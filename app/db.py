"""Database helpers."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse, urlunparse

import psycopg
from psycopg.rows import dict_row

from .config import settings

DRILL_ROW_ID = 1


def connect(
    *,
    application_name: str | None = None,
    autocommit: bool = False,
) -> psycopg.Connection[Any]:
    """Open a connection to the primary with the drill's standard settings.

    Rows are returned as dicts so callers can address columns by name.
    """
    return connect_url(
        settings.database_url,
        application_name=application_name,
        autocommit=autocommit,
    )


def connect_url(
    url: str,
    *,
    application_name: str | None = None,
    autocommit: bool = False,
    connect_timeout: int | None = None,
) -> psycopg.Connection[Any]:
    """Open a dict-row connection to an arbitrary PostgreSQL URL."""
    kwargs: dict[str, Any] = {
        "connect_timeout": settings.connect_timeout
        if connect_timeout is None
        else connect_timeout,
        "autocommit": autocommit,
        "row_factory": dict_row,
    }
    if application_name is not None:
        kwargs["application_name"] = application_name
    return psycopg.connect(url, **kwargs)


def redacted_dsn(url: str) -> str:
    """Return a connection string with the password masked (for display/logs)."""
    parsed = urlparse(url)
    if parsed.password is None:
        return url
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    auth = parsed.username or ""
    netloc = f"{auth}:***@{host}"
    return urlunparse(parsed._replace(netloc=netloc))


def ensure_schema() -> None:
    """Create and seed the single-row table used by the drill."""
    with connect(application_name=f"{settings.session_prefix}:setup") as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lock_drill_row (
                id integer PRIMARY KEY,
                value integer NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            INSERT INTO lock_drill_row (id, value)
            VALUES (%s, 0)
            ON CONFLICT (id) DO NOTHING
            """,
            (DRILL_ROW_ID,),
        )
        conn.commit()
