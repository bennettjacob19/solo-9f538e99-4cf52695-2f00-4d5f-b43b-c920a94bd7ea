"""End-to-end tests for the release-review / logical-subscription feature.

Requires two PostgreSQL instances with logical replication enabled:

* ``TEST_PRIMARY_URL``  - a superuser/admin URL on the primary publisher
* ``TEST_REPLICA_URL``  - a superuser/admin URL on the replica subscriber

The suite creates dedicated ``review_test`` databases on both sides and lets
the application build its own publication and logical subscription (the
replica connects back to the primary, so the two instances must be mutually
reachable on the URLs given).

    TEST_PRIMARY_URL=postgresql://postgres@localhost:55432/postgres \
    TEST_REPLICA_URL=postgresql://postgres@localhost:55433/postgres \
    pytest tests/test_review.py
"""

from __future__ import annotations

import importlib
import os
import sys
import time

import psycopg
import pytest
from fastapi.testclient import TestClient

PRIMARY_ADMIN = os.environ.get("TEST_PRIMARY_URL")
REPLICA_ADMIN = os.environ.get("TEST_REPLICA_URL")
pytestmark = pytest.mark.skipif(
    not (PRIMARY_ADMIN and REPLICA_ADMIN),
    reason="TEST_PRIMARY_URL/TEST_REPLICA_URL not set; review tests skipped",
)

DB = "review_test"
ROLE = "drill"
# Dedicated object names so the suite never collides with a manually-run app
# (which uses the default release_review_pub/sub names) or with cross-database
# catalog leftovers carrying the same name but a different connection target.
PUB_NAME = "review_test_pub"
SUB_NAME = "review_test_sub"


def _admin_dsn(url: str, dbname: str) -> str:
    head = url.rsplit("/", 1)[0]
    return f"{head}/{dbname}"


def _ensure_database(admin_url: str) -> None:
    with psycopg.connect(admin_url, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (DB,)
        ).fetchone()
        if exists is None:
            conn.execute(f"CREATE DATABASE {DB}")


def _clean_database(primary_dsn: str, replica_dsn: str) -> None:
    """Remove every review-test replication object from both databases.

    A previous run can leave an *orphan* subscription row when its database was
    dropped and recreated (the row then points at a stale database OID, so
    ``ALTER/DROP SUBSCRIPTION`` cannot see it while its apply worker keeps
    reconnecting to the publisher). Handle both the normal and orphan paths.
    The databases themselves are reused across runs to avoid that race.
    """
    # 1. Disable (then drop) every subscription first. Disabling stops the
    # apply worker from reconnecting, which otherwise keeps re-acquiring the
    # publisher-side slot while we try to drop it.
    with psycopg.connect(replica_dsn, autocommit=True) as conn:
        names = [r[0] for r in conn.execute("SELECT subname FROM pg_subscription")]
        for name in names:
            try:
                conn.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
            except psycopg.errors.UndefinedObject:
                pass  # orphan row pointing at a stale database OID
            try:
                conn.execute(f'DROP SUBSCRIPTION IF EXISTS "{name}"')
            except psycopg.errors.UndefinedObject:
                # Orphan row: delete the catalog entry directly.
                conn.execute("DELETE FROM pg_subscription WHERE subname = %s", (name,))
        conn.execute("DROP TABLE IF EXISTS release_revision")

    # 2. Stop any walsender still holding a slot, then drop remaining slots.
    with psycopg.connect(primary_dsn, autocommit=True) as conn:
        for _ in range(20):
            rows = conn.execute(
                "SELECT slot_name, active_pid FROM pg_replication_slots"
            ).fetchall()
            if not rows:
                break
            for slot_name, active_pid in rows:
                if active_pid:
                    conn.execute("SELECT pg_terminate_backend(%s)", (active_pid,))
            time.sleep(0.3)
        for (slot_name,) in conn.execute(
            "SELECT slot_name FROM pg_replication_slots"
        ).fetchall():
            conn.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        conn.execute(f"DROP PUBLICATION IF EXISTS {PUB_NAME}")
        conn.execute("DROP TABLE IF EXISTS release_revision, release_service")


@pytest.fixture(scope="module")
def urls() -> dict[str, str]:
    primary_admin_dsn = _admin_dsn(PRIMARY_ADMIN, DB)
    replica_admin_dsn = _admin_dsn(REPLICA_ADMIN, DB)

    # Reuse stable databases; only the schema/replication objects are reset so
    # a previous module run's worker cannot race a DROP DATABASE.
    _ensure_database(PRIMARY_ADMIN)
    _ensure_database(REPLICA_ADMIN)
    _clean_database(primary_admin_dsn, replica_admin_dsn)

    yield {"primary": primary_admin_dsn, "replica": replica_admin_dsn}

    _clean_database(primary_admin_dsn, replica_admin_dsn)


@pytest.fixture()
def web(urls, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", urls["primary"])
    monkeypatch.setenv("REPLICA_DATABASE_URL", urls["replica"])
    monkeypatch.setenv("REVIEW_PUBLICATION_NAME", PUB_NAME)
    monkeypatch.setenv("REVIEW_SUBSCRIPTION_NAME", SUB_NAME)
    monkeypatch.setenv("REVIEW_STATEMENT_TIMEOUT_MS", "1500")
    monkeypatch.setenv("REVIEW_PROVISION_WAIT_SECONDS", "0.2")
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.main
    import app.review

    importlib.reload(app.config)
    importlib.reload(app.db)
    app.drill = importlib.reload(app.drill)
    app.review = importlib.reload(app.review)
    app.main = importlib.reload(app.main)
    client = TestClient(app.main.app)
    with client:
        yield client, app.review
    app.drill.store.shutdown()


def _register(client, service: str, version: str) -> dict:
    res = client.post(
        "/api/review/register",
        data={"service": service, "artifact_version": version},
    )
    assert res.status_code == 200, res.text
    return res.json()["registration"]


def _lookup(client, service: str, revision: int) -> dict:
    res = client.get(f"/api/review/lookup/{service}/{revision}")
    assert res.status_code == 200, res.text
    return res.json()["result"]


def _wait_source(client, service, revision, expected, timeout=15.0) -> dict:
    deadline = time.monotonic() + timeout
    result = {}
    while time.monotonic() < deadline:
        result = _lookup(client, service, revision)
        if result["source"] == expected:
            return result
        time.sleep(0.2)
    raise AssertionError(
        f"source never became {expected}; last={result['source']}"
    )


def test_provision_creates_publication_and_subscription(web):
    client, review = web
    sub = client.get("/api/review/subscription").json()["subscription"]
    assert sub["configured"] is True
    assert sub["enabled"] is True

    with psycopg.connect(review.settings.replica_url, autocommit=True) as conn:
        assert conn.execute(
            "SELECT 1 FROM pg_subscription WHERE subname = %s",
            (review.settings.review_subscription,),
        ).fetchone() is not None
    with psycopg.connect(review.settings.database_url, autocommit=True) as conn:
        assert conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s",
            (review.settings.review_publication,),
        ).fetchone() is not None


def test_revisions_increment_and_old_artifact_gets_new_revision(web):
    client, _ = web
    r1 = _register(client, "billing", "1.0.0")
    r2 = _register(client, "billing", "2.0.0")
    r3 = _register(client, "billing", "1.0.0")  # re-send the OLD artifact
    assert [r["revision"] for r in (r1, r2, r3)] == [1, 2, 3]
    assert r3["artifact_version"] == "1.0.0"
    assert r1["review_path"] == "/review/billing/1"

    # Revisions are per-service.
    other = _register(client, "other-service", "0.1.0")
    assert other["revision"] == 1


def test_register_form_returns_link_with_service_and_revision(web):
    client, _ = web
    res = client.post(
        "/review/register",
        data={"service": "web-form", "artifact_version": "3.1.4"},
        follow_redirects=False,
    )
    assert res.status_code == 303
    assert res.headers["location"] == "/review/web-form/1?registered=1"


def test_replica_serves_record_first_after_catchup(web):
    client, _ = web
    reg = _register(client, "catalog", "5.0.0")
    result = _wait_source(client, "catalog", reg["revision"], "replica")
    assert result["record"]["artifact_version"] == "5.0.0"
    assert result["record"]["revision"] == 1
    assert result["source_label"] == "逻辑订阅库"
    # Replica hit means the primary was never queried.
    assert result["primary_highest"] is None


def test_paused_subscription_link_reads_primary_then_replica(web):
    client, review = web
    # Pause the real subscription.
    paused = client.post("/api/review/subscription/pause").json()["subscription"]
    assert paused["enabled"] is False

    reg = _register(client, "checkout", "7.7.7")
    # The replica is behind: the link must fall back to the primary.
    result = _lookup(client, "checkout", reg["revision"])
    assert result["source"] == "primary"
    assert result["record"]["artifact_version"] == "7.7.7"
    assert result["replica_highest"] is None
    assert "订阅库" in (result["replica_skipped_reason"] or "")

    # Resume and wait until the same link is served by the replica.
    resumed = client.post("/api/review/subscription/resume").json()["subscription"]
    assert resumed["enabled"] is True
    caught = _wait_source(client, "checkout", reg["revision"], "replica")
    assert caught["record"]["artifact_version"] == "7.7.7"
    assert caught["source_label"] == "逻辑订阅库"


def test_replica_failure_falls_back_to_primary(web, urls):
    client, _ = web
    reg = _register(client, "search", "1.2.3")
    _wait_source(client, "search", reg["revision"], "replica")

    # Point the replica DSN at a closed port to simulate a read failure. The
    # settings object is a frozen dataclass, so bypass the frozen __setattr__
    # and restore the original value explicitly.
    from app.config import settings
    original = settings.replica_url
    object.__setattr__(settings, "replica_url", "postgresql://drill@127.0.0.1:1/drill")
    try:
        result = _lookup(client, "search", reg["revision"])
        assert result["source"] == "primary"
        assert result["record"]["artifact_version"] == "1.2.3"
        assert result["replica_error"] is not None
    finally:
        object.__setattr__(settings, "replica_url", original)


def test_neither_side_has_record_is_unconfirmed(web):
    client, _ = web
    result = _lookup(client, "ghost-service", 42)
    assert result["source"] == "unconfirmed"
    assert result["record"] is None
    assert result["primary_error"] is not None


def test_invalid_inputs_rejected(web):
    client, _ = web
    bad_service = client.post(
        "/api/review/register",
        data={"service": "bad name", "artifact_version": "1.0.0"},
    )
    assert bad_service.status_code == 400
    bad_version = client.post(
        "/api/review/register",
        data={"service": "ok-svc", "artifact_version": ""},
    )
    assert bad_version.status_code in (400, 422)
    bad_rev = client.get("/api/review/lookup/ok-svc/0")
    assert bad_rev.status_code == 400


def test_review_page_renders_source_and_subscription_controls(web):
    client, _ = web
    reg = _register(client, "page-svc", "4.0.0")
    _wait_source(client, "page-svc", reg["revision"], "replica")
    html = client.get(f"/review/page-svc/{reg['revision']}").text
    assert "复核结果" in html
    assert "逻辑订阅库" in html
    assert "4.0.0" in html
    assert "/review/subscription/pause" in html

    form_page = client.get("/review").text
    assert "登记一版制品" in form_page
    assert "暂停订阅" in form_page


def test_rebuilt_subscription_clears_stale_replica_rows(web, urls):
    """If a previous subscription left rows in the replica table, recreating it
    must truncate them first so the initial COPY does not hit a key conflict."""
    client, review = web
    reg = _register(client, "rebuild-svc", "1.0.0")
    _wait_source(client, "rebuild-svc", reg["revision"], "replica")

    # Simulate a broken/failed previous deployment: drop the subscription
    # without cleaning the replica-side table (the publisher slot is removed by
    # DROP SUBSCRIPTION), and leave an extra stale row behind.
    with psycopg.connect(urls["replica"], autocommit=True) as conn:
        conn.execute(
            f'ALTER SUBSCRIPTION "{review.settings.review_subscription}" DISABLE'
        )
        conn.execute(
            f'DROP SUBSCRIPTION "{review.settings.review_subscription}"'
        )
        conn.execute(
            "INSERT INTO release_revision (service, revision, artifact_version) "
            "VALUES ('stale-leftover', 1, '0.0.1')"
        )

    # A fresh application boot rebuilds everything. The stale row must be gone
    # and a new registration must replicate normally.
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.main
    import app.review

    importlib.reload(app.config)
    importlib.reload(app.db)
    app.drill = importlib.reload(app.drill)
    app.review = importlib.reload(app.review)
    app.main = importlib.reload(app.main)
    with TestClient(app.main.app) as client2:
        with psycopg.connect(urls["replica"], autocommit=True) as conn:
            stale = conn.execute(
                "SELECT 1 FROM release_revision WHERE service = 'stale-leftover'"
            ).fetchone()
        assert stale is None  # truncated before the initial COPY
        reg2 = client2.post(
            "/api/review/register",
            data={"service": "rebuild-svc", "artifact_version": "2.0.0"},
        ).json()["registration"]
        result = _wait_source(
            client2, "rebuild-svc", reg2["revision"], "replica"
        )
        assert result["record"]["artifact_version"] == "2.0.0"
    app.drill.store.shutdown()


def test_restart_keeps_links_and_reuses_subscription(web, urls, monkeypatch):
    client, review = web
    reg = _register(client, "restart-svc", "9.0.1")
    _wait_source(client, "restart-svc", reg["revision"], "replica")

    # Simulate an application restart by rebuilding every app module; all link
    # state lives in PostgreSQL, so provisioning must be idempotent.
    with psycopg.connect(review.settings.replica_url, autocommit=True) as conn:
        subs_before = {
            r[0] for r in conn.execute("SELECT subname FROM pg_subscription")
        }
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.main
    import app.review

    importlib.reload(app.config)
    importlib.reload(app.db)
    app.drill = importlib.reload(app.drill)
    app.review = importlib.reload(app.review)
    app.main = importlib.reload(app.main)
    with TestClient(app.main.app) as client2:
        result = client2.get(
            f"/api/review/lookup/restart-svc/{reg['revision']}"
        ).json()["result"]
        assert result["source"] == "replica"
        assert result["record"]["artifact_version"] == "9.0.1"
        sub = client2.get("/api/review/subscription").json()["subscription"]
        assert sub["configured"] is True
        assert sub["enabled"] is True
    with psycopg.connect(review.settings.replica_url, autocommit=True) as conn:
        subs_after = {
            r[0] for r in conn.execute("SELECT subname FROM pg_subscription")
        }
    assert subs_after == subs_before  # no duplicate subscription created
    app.drill.store.shutdown()
