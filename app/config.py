"""Configuration for the row-lock drill app."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover - startup misconfiguration
        raise RuntimeError(f"{name} must be a number, got {raw!r}") from exc


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - startup misconfiguration
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    # Primary (writable) PostgreSQL used by both the row-lock drill and the
    # release-review registry.
    database_url: str
    # Read replica fed by a PostgreSQL *logical* subscription from the primary.
    replica_url: str | None
    # Object names created on the two databases for logical replication.
    review_publication: str
    review_subscription: str
    # Human-readable labels shown on the review page ("actual source").
    primary_label: str
    replica_label: str
    session_prefix: str
    connect_timeout: int
    # Statement timeout for every bounded review read / subscription DDL.
    review_statement_timeout_ms: int
    # Bounded retry while the logical subscription is being (re)built at startup.
    review_provision_attempts: int
    review_provision_wait_seconds: float
    # How long to wait for the observed blocking edge to appear.
    block_confirm_seconds: float
    # How long to let the blocked UPDATE finish after a successful terminate.
    release_wait_seconds: float
    # How long to wait for an already-running UPDATE when nothing was terminated.
    refused_wait_seconds: float
    # Abandoned active drills are cleaned up after this many seconds.
    drill_ttl_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        replica_url = os.environ.get("REPLICA_DATABASE_URL")
        if replica_url is not None and replica_url.strip() == "":
            replica_url = None
        return cls(
            database_url=os.environ.get(
                "DATABASE_URL", "postgresql://drill:drill@localhost:5432/drill"
            ),
            replica_url=replica_url,
            review_publication=os.environ.get(
                "REVIEW_PUBLICATION_NAME", "release_review_pub"
            ),
            review_subscription=os.environ.get(
                "REVIEW_SUBSCRIPTION_NAME", "release_review_sub"
            ),
            primary_label=os.environ.get("PRIMARY_DB_LABEL", "主库"),
            replica_label=os.environ.get("REPLICA_DB_LABEL", "逻辑订阅库"),
            session_prefix=os.environ.get("DRILL_SESSION_PREFIX", "lockdrill"),
            connect_timeout=_int_env("DB_CONNECT_TIMEOUT", 5),
            review_statement_timeout_ms=_int_env(
                "REVIEW_STATEMENT_TIMEOUT_MS", 1500
            ),
            review_provision_attempts=_int_env("REVIEW_PROVISION_ATTEMPTS", 30),
            review_provision_wait_seconds=_float_env(
                "REVIEW_PROVISION_WAIT_SECONDS", 1.0
            ),
            block_confirm_seconds=_float_env("BLOCK_CONFIRM_SECONDS", 5.0),
            release_wait_seconds=_float_env("RELEASE_WAIT_SECONDS", 10.0),
            refused_wait_seconds=_float_env("REFUSED_WAIT_SECONDS", 2.0),
            drill_ttl_seconds=_float_env("DRILL_TTL_SECONDS", 1800.0),
        )


settings = Settings.from_env()
