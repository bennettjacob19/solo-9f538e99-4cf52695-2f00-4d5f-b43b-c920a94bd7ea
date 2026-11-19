"""Release-review registry backed by a primary and a logical-subscription replica.

The primary owns a monotonically increasing revision number per service.  Every
registration (including re-registering an *older* artifact) is committed to the
primary and produces a new revision.  A PostgreSQL logical publication streams
the registry table to a second database whose subscription this application can
pause and resume for the drill.

Review links are stable across application restarts because every piece of link
state lives in PostgreSQL, not in process memory.

Read strategy for a review link (:func:`lookup_revision`):

1. read the replica first, with a bounded statement timeout;
2. fall back to the primary when the replica fails, lacks the row, or is behind
   (its highest revision for the service is smaller than the requested one);
3. if neither side can serve the qualifying record, report "unconfirmed".
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any
from urllib.parse import quote

import psycopg
from psycopg.sql import SQL, Identifier, Literal

from .config import settings
from .db import connect_url, redacted_dsn

logger = logging.getLogger("lockdrill.review")

SERVICE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
ARTIFACT_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,127}$")

SOURCE_PRIMARY = "primary"
SOURCE_REPLICA = "replica"
SOURCE_NONE = "unconfirmed"


class ReviewError(Exception):
    """Invalid review input."""

    status_code = 400


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_service(service: str) -> str:
    service = (service or "").strip()
    if not SERVICE_NAME_RE.match(service):
        raise ReviewError(
            "服务名需为 1-128 个字符，以字母或数字开头，仅含字母、数字、点、下划线与连字符。"
        )
    return service


def validate_version(version: str) -> str:
    version = (version or "").strip()
    if not ARTIFACT_VERSION_RE.match(version):
        raise ReviewError(
            "制品版本需为 1-128 个字符，以字母或数字开头，仅含字母、数字、点、下划线、连字符、冒号与加号。"
        )
    return version


def review_path(service: str, revision: int) -> str:
    """URL path of a review link (percent-encoded, stable across restarts)."""
    return f"/review/{quote(service, safe='')}/{int(revision)}"


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_PRIMARY_DDL = """
CREATE TABLE IF NOT EXISTS release_service (
    service        text PRIMARY KEY,
    next_revision  integer NOT NULL CHECK (next_revision >= 1)
);

CREATE TABLE IF NOT EXISTS release_revision (
    service        text NOT NULL,
    revision       integer NOT NULL,
    artifact_version text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service, revision)
);
"""

# The replica copies only the registry rows; the sequence bookkeeping table is
# not needed there.  Column types must match the primary exactly so the logical
# apply worker can insert replicated rows.
_REPLICA_DDL = """
CREATE TABLE IF NOT EXISTS release_revision (
    service        text NOT NULL,
    revision       integer NOT NULL,
    artifact_version text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service, revision)
);
"""


def _timeout_ms() -> int:
    # SET cannot take a bind parameter; the value comes from our own integer
    # setting, so inlining it is safe and the int() cast rejects anything else.
    return max(1, int(settings.review_statement_timeout_ms))


def _set_statement_timeout(conn: psycopg.Connection[Any]) -> None:
    # SET LOCAL is scoped to the current transaction; in autocommit each
    # statement is its own transaction, so use plain SET there.
    keyword = "SET LOCAL" if not conn.autocommit else "SET"
    conn.execute(f"{keyword} statement_timeout = {_timeout_ms()}")


# ---------------------------------------------------------------------------
# Startup provisioning (idempotent; safe to re-run after an app restart)
# ---------------------------------------------------------------------------


def provision() -> dict[str, Any]:
    """Create schema, publication and (on the replica) the subscription.

    Returns a small status dict describing what is available.  Never raises for
    replica-side problems so the row-lock drill still starts when the replica is
    down; review pages degrade to primary-only reads.
    """
    status: dict[str, Any] = {
        "primary_ready": False,
        "replica_configured": False,
        "subscription_exists": False,
        "subscription_enabled": None,
        "error": None,
    }

    # Primary: registry tables + logical publication.
    with connect_url(
        settings.database_url,
        application_name=f"{settings.session_prefix}:review-setup",
    ) as conn:
        conn.execute(_PRIMARY_DDL)
        if conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s",
            (settings.review_publication,),
        ).fetchone() is None:
            conn.execute(
                SQL("CREATE PUBLICATION {} FOR TABLE release_revision").format(
                    Identifier(settings.review_publication)
                )
            )
        conn.commit()
    status["primary_ready"] = True

    if not settings.replica_url:
        status["error"] = "未配置 REPLICA_DATABASE_URL，复核读取将只使用主库。"
        return status

    # Replica: matching table + subscription.  All replication DDL must run in
    # autocommit mode (PostgreSQL forbids it inside transaction blocks).
    try:
        with connect_url(
            settings.replica_url,
            application_name=f"{settings.session_prefix}:review-setup",
            autocommit=True,
        ) as conn:
            conn.execute(_REPLICA_DDL)
            row = conn.execute(
                "SELECT oid FROM pg_subscription WHERE subname = %s",
                (settings.review_subscription,),
            ).fetchone()
            if row is None:
                # A brand-new subscription performs an initial table copy. The
                # replica table is a read-only copy owned by this app, so any
                # rows left behind by a dropped/failed earlier subscription
                # must be cleared first: otherwise the COPY fails on the
                # primary-key conflict and the apply worker never starts.
                conn.execute("TRUNCATE TABLE release_revision")
                _create_subscription_with_retry(conn)
            info = _subscription_status_conn(conn)
            status["subscription_exists"] = True
            status["subscription_enabled"] = info["enabled"]
            status["replica_configured"] = True
    except Exception as exc:  # replica unavailable / DDL failed
        message = f"{type(exc).__name__}: {exc}"
        logger.warning("logical replica provisioning failed: %s", message)
        status["error"] = message
    return status


def _create_subscription_with_retry(conn: psycopg.Connection[Any]) -> None:
    """Create the subscription, retrying while the primary cannot be reached.

    CREATE SUBSCRIPTION connects back to the publisher to build its slot; in
    docker compose the replica may accept connections before the primary is
    routable, so the create is retried with a bounded wait.
    """
    last_exc: Exception | None = None
    for attempt in range(1, settings.review_provision_attempts + 1):
        try:
            conn.execute(f"SET statement_timeout = {_timeout_ms()}")
            # Let PostgreSQL create and own the replication slot on the
            # publisher: dropping the subscription later drops the slot too.
            # DDL does not accept bind parameters, so build the statement with
            # psycopg's safe identifier/literal composables.
            conn.execute(
                SQL(
                    "CREATE SUBSCRIPTION {} CONNECTION {} PUBLICATION {}"
                ).format(
                    Identifier(settings.review_subscription),
                    Literal(_replica_origin_dsn()),
                    Identifier(settings.review_publication),
                )
            )
            logger.info(
                "created logical subscription %s", settings.review_subscription
            )
            return
        except Exception as exc:  # publisher not reachable yet, etc.
            last_exc = exc
            logger.warning(
                "CREATE SUBSCRIPTION attempt %s/%s failed: %s",
                attempt,
                settings.review_provision_attempts,
                exc,
            )
            time.sleep(settings.review_provision_wait_seconds)
    raise RuntimeError(
        f"无法在限定次数内创建逻辑订阅：{last_exc}"
    ) from last_exc


def _replica_origin_dsn() -> str:
    """libpq keyword/value DSN pointing at the primary (for CREATE SUBSCRIPTION).

    Built from the parsed URL so a URL-style connection string is converted to
    the keyword form that CREATE SUBSCRIPTION expects, without logging secrets.
    """
    from urllib.parse import urlparse

    parsed = urlparse(settings.database_url)
    parts = [
        f"host={parsed.hostname}",
        f"port={parsed.port or 5432}",
        f"user={parsed.username}",
        f"dbname={parsed.path.lstrip('/') or 'postgres'}",
        f"connect_timeout={settings.connect_timeout}",
    ]
    if parsed.password is not None:
        parts.append(f"password={parsed.password}")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Registration (primary write)
# ---------------------------------------------------------------------------


def register_release(service: str, artifact_version: str) -> dict[str, Any]:
    """Commit one release registration to the primary and return its revision.

    Revision numbers increase strictly per service in primary commit order.
    Re-registering an older artifact still allocates a fresh revision.
    """
    service = validate_service(service)
    artifact_version = validate_version(artifact_version)
    with connect_url(
        settings.database_url,
        application_name=f"{settings.session_prefix}:review-write",
    ) as conn:
        try:
            # Row lock on the service sequence makes revision allocation
            # serialisable; a new service gets its first revision.
            row = conn.execute(
                """
                INSERT INTO release_service (service, next_revision)
                VALUES (%s, 1)
                ON CONFLICT (service) DO UPDATE
                    SET next_revision = release_service.next_revision
                RETURNING next_revision
                """,
                (service,),
            ).fetchone()
            revision = row["next_revision"]
            conn.execute(
                """
                INSERT INTO release_revision
                    (service, revision, artifact_version)
                VALUES (%s, %s, %s)
                """,
                (service, revision, artifact_version),
            )
            conn.execute(
                "UPDATE release_service SET next_revision = next_revision + 1 "
                "WHERE service = %s",
                (service,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    logger.info(
        "registered release service=%s revision=%s version=%s",
        service,
        revision,
        artifact_version,
    )
    return {
        "service": service,
        "revision": revision,
        "artifact_version": artifact_version,
        "review_path": review_path(service, revision),
    }


# ---------------------------------------------------------------------------
# Bounded reads
# ---------------------------------------------------------------------------


def _read_source(
    url: str, service: str, revision: int, *, application_name: str
) -> dict[str, Any]:
    """Read one qualifying record plus the source's highest revision.

    Raises on any failure (connection, timeout, ...); callers decide fallback.
    Both the connection and the statements are bounded so a dead source fails
    fast instead of eating the whole request.
    """
    connect_timeout = max(1, settings.review_statement_timeout_ms // 1000)
    with connect_url(
        url,
        application_name=application_name,
        autocommit=True,
        connect_timeout=connect_timeout,
    ) as conn:
        _set_statement_timeout(conn)
        record = conn.execute(
            """
            SELECT service, revision, artifact_version, created_at
            FROM release_revision
            WHERE service = %s AND revision = %s
            """,
            (service, revision),
        ).fetchone()
        highest = conn.execute(
            "SELECT max(revision) AS revision FROM release_revision WHERE service = %s",
            (service,),
        ).fetchone()["revision"]
    return {
        "record": dict(record) if record is not None else None,
        "highest_revision": highest,
    }


def lookup_revision(service: str, revision: int) -> dict[str, Any]:
    """Resolve a review link: replica first, primary as fallback.

    The returned dict always contains ``source`` (``replica`` / ``primary`` /
    ``unconfirmed``), the displayed record (or ``None``) and per-source error
    details so the page can show why a source was skipped.
    """
    service = validate_service(service)
    revision = int(revision)
    if revision < 1:
        raise ReviewError("修订号必须为正整数。")

    result: dict[str, Any] = {
        "service": service,
        "requested_revision": revision,
        "record": None,
        "source": SOURCE_NONE,
        "source_label": "无法确认",
        "replica_highest": None,
        "primary_highest": None,
        "replica_error": None,
        "primary_error": None,
        "replica_skipped_reason": None,
    }

    # 1. Replica, bounded.
    if settings.replica_url:
        try:
            replica = _read_source(
                settings.replica_url,
                service,
                revision,
                application_name=f"{settings.session_prefix}:review-read-sub",
            )
            result["replica_highest"] = replica["highest_revision"]
            record = replica["record"]
            if record is not None:
                result["record"] = _display_record(record)
                result["source"] = SOURCE_REPLICA
                result["source_label"] = settings.replica_label
                return result
            # Missing row, or the replica is behind the requested revision:
            # remember why and fall through to the primary.
            if replica["highest_revision"] is None:
                result["replica_skipped_reason"] = "订阅库尚无该服务的任何登记行。"
            elif replica["highest_revision"] < revision:
                result["replica_skipped_reason"] = (
                    f"订阅库修订落后（最高 {replica['highest_revision']} "
                    f"< 请求 {revision}），改查主库。"
                )
            else:
                result["replica_skipped_reason"] = "订阅库缺少该修订行，改查主库。"
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            result["replica_error"] = message
            result["replica_skipped_reason"] = f"订阅库限时读取失败：{message}"
    else:
        result["replica_skipped_reason"] = "未配置逻辑订阅库，直接查主库。"

    # 2. Primary, bounded.
    try:
        primary = _read_source(
            settings.database_url,
            service,
            revision,
            application_name=f"{settings.session_prefix}:review-read-main",
        )
        result["primary_highest"] = primary["highest_revision"]
        if primary["record"] is not None:
            result["record"] = _display_record(primary["record"])
            result["source"] = SOURCE_PRIMARY
            result["source_label"] = settings.primary_label
            return result
        result["primary_error"] = (
            "主库也没有该服务/修订的达标记录。"
            if primary["highest_revision"] is None
            else f"主库该服务最高修订为 {primary['highest_revision']}，无修订 {revision}。"
        )
    except Exception as exc:
        result["primary_error"] = f"{type(exc).__name__}: {exc}"

    # 3. Neither side can confirm the record.
    return result


def _display_record(row: dict[str, Any]) -> dict[str, Any]:
    created = row.get("created_at")
    return {
        "service": row["service"],
        "revision": row["revision"],
        "artifact_version": row["artifact_version"],
        "created_at": created.isoformat(sep=" ", timespec="microseconds")
        if hasattr(created, "isoformat")
        else str(created),
    }


# ---------------------------------------------------------------------------
# Subscription pause / resume (real logical subscription control)
# ---------------------------------------------------------------------------


def subscription_status() -> dict[str, Any]:
    """Current state of the logical subscription from the replica's catalogs."""
    if not settings.replica_url:
        return {
            "configured": False,
            "enabled": None,
            "error": "未配置 REPLICA_DATABASE_URL。",
            "sub_name": settings.review_subscription,
        }
    try:
        with connect_url(
            settings.replica_url,
            application_name=f"{settings.session_prefix}:review-subctl",
            autocommit=True,
        ) as conn:
            return _subscription_status_conn(conn)
    except Exception as exc:
        return {
            "configured": False,
            "enabled": None,
            "error": f"{type(exc).__name__}: {exc}",
            "sub_name": settings.review_subscription,
        }


def _subscription_status_conn(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT s.subname, s.subenabled,
               (SELECT count(*) FROM pg_replication_origin_status
                 WHERE left(external_id, 3) = 'pg_') AS origin_rows
        FROM pg_subscription s
        WHERE s.subname = %s
        """,
        (settings.review_subscription,),
    ).fetchone()
    if row is None:
        return {
            "configured": False,
            "enabled": None,
            "error": f"订阅 {settings.review_subscription} 不存在。",
            "sub_name": settings.review_subscription,
        }
    return {
        "configured": True,
        "enabled": bool(row["subenabled"]),
        "error": None,
        "sub_name": row["subname"],
        "origin_rows": row["origin_rows"],
    }


def set_subscription_paused(paused: bool) -> dict[str, Any]:
    """Pause (DISABLE) or resume (ENABLE) the real logical subscription."""
    if not settings.replica_url:
        raise ReviewError("未配置逻辑订阅库，无法暂停/恢复订阅。")
    action = "DISABLE" if paused else "ENABLE"
    verb = "暂停" if paused else "恢复"
    try:
        with connect_url(
            settings.replica_url,
            application_name=f"{settings.session_prefix}:review-subctl",
            autocommit=True,
        ) as conn:
            conn.execute(f"SET statement_timeout = {_timeout_ms()}")
            exists = conn.execute(
                "SELECT 1 FROM pg_subscription WHERE subname = %s",
                (settings.review_subscription,),
            ).fetchone()
            if exists is None:
                raise ReviewError(
                    f"订阅 {settings.review_subscription} 不存在，无法{verb}。"
                )
            conn.execute(
                SQL("ALTER SUBSCRIPTION {} ").format(
                    Identifier(settings.review_subscription)
                )
                + SQL(action)
            )
            status = _subscription_status_conn(conn)
    except ReviewError:
        raise
    except Exception as exc:
        raise ReviewError(f"{verb}订阅失败：{type(exc).__name__}: {exc}") from exc
    logger.info("logical subscription %sd (enabled=%s)", verb, status["enabled"])
    return status


def wait_until_caught_up(
    service: str, revision: int, *, timeout: float = 15.0, interval: float = 0.2
) -> bool:
    """Poll the replica until it holds the requested revision (test helper)."""
    if not settings.replica_url:
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with connect_url(
                settings.replica_url,
                application_name=f"{settings.session_prefix}:review-wait",
                autocommit=True,
            ) as conn:
                _set_statement_timeout(conn)
                row = conn.execute(
                    "SELECT 1 FROM release_revision "
                    "WHERE service = %s AND revision = %s",
                    (service, revision),
                ).fetchone()
            if row is not None:
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def connection_summary() -> dict[str, Any]:
    """Redacted endpoints for the status panel / health checks."""
    return {
        "primary": redacted_dsn(settings.database_url),
        "replica": redacted_dsn(settings.replica_url)
        if settings.replica_url
        else None,
        "primary_label": settings.primary_label,
        "replica_label": settings.replica_label,
    }
