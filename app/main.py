"""FastAPI application for the PostgreSQL row-lock drill."""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import review
from .config import settings
from .db import connect, ensure_schema
from .drill import DrillError, store

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("lockdrill.web")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    for attempt in range(1, 31):
        try:
            ensure_schema()
            logger.info("drill schema ready")
            break
        except Exception as exc:  # database may still be starting
            logger.warning("database not ready (attempt %s): %s", attempt, exc)
            if attempt == 30:
                raise
            time.sleep(1)
    # Release-review registry + logical publication/subscription. The primary
    # must already be reachable (ensure_schema above); a missing replica only
    # downgrades review reads to primary-only and never blocks startup.
    try:
        review_status = review.provision()
        logger.info("release review provisioning: %s", review_status)
    except Exception:
        logger.exception("release review provisioning failed")
    # Any session naming this app instance is a leftover from a previous
    # process (a restart loses the in-memory registry): end it and start clean.
    try:
        with connect(application_name="lockdrill:startup", autocommit=True) as conn:
            rows = conn.execute(
                """
                SELECT pid FROM pg_stat_activity
                WHERE application_name LIKE %s
                  AND pid <> pg_backend_pid()
                """,
                (f"{settings.session_prefix}:drill:%",),
            ).fetchall()
            for row in rows:
                conn.execute("SELECT pg_terminate_backend(%s)", (row["pid"],))
            if rows:
                logger.info("terminated %s orphaned drill session(s)", len(rows))
    except Exception:
        logger.exception("could not clean up orphaned drill sessions")
    try:
        yield
    finally:
        logger.info("shutting down: releasing drill sessions")
        store.shutdown()


app = FastAPI(title="PostgreSQL 行锁演练台", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


def _json_error(exc: DrillError) -> JSONResponse:
    return JSONResponse(
        {"ok": False, "error": str(exc), "kind": type(exc).__name__},
        status_code=getattr(exc, "status_code", 400),
    )


def _resolve(drill_id: str | None):
    if drill_id is None:
        return store.active()
    return store.get(drill_id)


def _render(
    request: Request,
    *,
    drill_id: str | None = None,
    message: str | None = None,
    error: str | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    drill = _resolve(drill_id)
    snapshot = store.snapshot(drill) if drill is not None else None
    context: dict[str, Any] = {
        "request": request,
        "snapshot": snapshot,
        # The page token is bound to this drill round; old pages carry old
        # tokens and are rejected by the server.
        "token": drill.token if drill is not None else None,
        "recent": [store.snapshot(item) for item in store.recent()],
        "message": message,
        "error": error,
    }
    return templates.TemplateResponse(request, "index.html", context, status_code=status_code)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return _render(request)


@app.get("/drill/{drill_id}", response_class=HTMLResponse)
async def drill_page(request: Request, drill_id: str) -> HTMLResponse:
    if store.get(drill_id) is None:
        return _render(
            request,
            error="该演练记录已不存在（服务可能已重启或记录已清理），页面不再可操作。",
            status_code=410,
        )
    return _render(request, drill_id=drill_id)


@app.post("/drill/start")
async def start_drill(request: Request) -> HTMLResponse:
    try:
        drill = store.start()
    except DrillError as exc:
        return _render(request, error=f"未能开始演练：{exc}", status_code=409)
    return RedirectResponse(url=f"/drill/{drill.id}", status_code=303)


@app.post("/drill/{drill_id}/release", response_class=HTMLResponse)
async def release_form(request: Request, drill_id: str, token: str = Form(...)) -> HTMLResponse:
    try:
        store.release(drill_id, token)
    except DrillError as exc:
        return _render(request, drill_id=drill_id, error=str(exc), status_code=exc.status_code)
    return _render(
        request,
        drill_id=drill_id,
        message="解锁操作已完成，下方为以受阻更新真实提交结果作出的判定。",
    )


@app.post("/drill/{drill_id}/end", response_class=HTMLResponse)
async def end_form(request: Request, drill_id: str, token: str = Form(...)) -> HTMLResponse:
    try:
        store.end(drill_id, token)
    except DrillError as exc:
        return _render(request, drill_id=drill_id, error=str(exc), status_code=exc.status_code)
    return RedirectResponse(url="/", status_code=303)


# ---------------------------------------------------------------------------
# JSON API (used by the live poller and by automated checks)
# ---------------------------------------------------------------------------


@app.get("/api/state", response_class=JSONResponse)
async def api_state(drill_id: str | None = None) -> JSONResponse:
    active = store.active()
    drill = store.get(drill_id) if drill_id is not None else active
    return JSONResponse(
        {
            "ok": True,
            "active_drill_id": active.id if active else None,
            "drill": store.snapshot(drill) if drill is not None else None,
            "recent": [store.snapshot(item) for item in store.recent()],
        }
    )


@app.get("/api/drills/{drill_id}", response_class=JSONResponse)
async def api_drill(drill_id: str) -> JSONResponse:
    drill = store.get(drill_id)
    if drill is None:
        return _json_error(
            DrillError("该演练记录已不存在，未执行任何操作。")
        )
    return JSONResponse({"ok": True, "drill": store.snapshot(drill)})


@app.post("/api/drills/{drill_id}/release", response_class=JSONResponse)
async def api_release(drill_id: str, token: str = Form(...)) -> JSONResponse:
    try:
        result = store.release(drill_id, token)
    except DrillError as exc:
        return _json_error(exc)
    return JSONResponse(
        {
            "ok": True,
            "release": result,
            "drill": store.snapshot(store.require(drill_id)),
        }
    )


@app.post("/api/drills/{drill_id}/end", response_class=JSONResponse)
async def api_end(drill_id: str, token: str = Form(...)) -> JSONResponse:
    try:
        store.end(drill_id, token)
    except DrillError as exc:
        return _json_error(exc)
    return JSONResponse({"ok": True, "drill": store.snapshot(store.require(drill_id))})


# ---------------------------------------------------------------------------
# Release review (logical-subscription drill)
# ---------------------------------------------------------------------------


def _review_context(
    request: Request,
    *,
    form: dict[str, str] | None = None,
    message: str | None = None,
    error: str | None = None,
    result: dict[str, Any] | None = None,
    registered: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "request": request,
        "form": form or {},
        "review_message": message,
        "review_error": error,
        "review_result": result,
        "registered": registered,
        "subscription": review.subscription_status(),
        "connections": review.connection_summary(),
    }


@app.get("/review", response_class=HTMLResponse)
async def review_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request, "review.html", _review_context(request)
    )


@app.post("/review/register")
async def review_register(
    request: Request,
    service: str = Form(...),
    artifact_version: str = Form(...),
) -> HTMLResponse:
    try:
        registered = review.register_release(service, artifact_version)
    except review.ReviewError as exc:
        return templates.TemplateResponse(
            request,
            "review.html",
            _review_context(
                request,
                form={"service": service, "artifact_version": artifact_version},
                error=str(exc),
            ),
            status_code=exc.status_code,
        )
    # PRG: after the primary commit the response carries the review link with
    # service name and revision.
    return RedirectResponse(
        url=f"{registered['review_path']}?registered=1", status_code=303
    )


@app.get("/review/{service}/{revision}", response_class=HTMLResponse)
async def review_link_page(
    request: Request, service: str, revision: int
) -> HTMLResponse:
    try:
        result = review.lookup_revision(service, revision)
    except review.ReviewError as exc:
        return templates.TemplateResponse(
            request,
            "review.html",
            _review_context(request, error=str(exc)),
            status_code=exc.status_code,
        )
    message = None
    if request.query_params.get("registered") == "1":
        message = "主库提交成功，本次登记的复核链接已生成（见下方）。"
    return templates.TemplateResponse(
        request,
        "review.html",
        _review_context(request, result=result, message=message),
    )


@app.post("/review/subscription/{action}")
async def review_subscription_form(
    request: Request, action: str
) -> HTMLResponse:
    if action not in ("pause", "resume"):
        return templates.TemplateResponse(
            request,
            "review.html",
            _review_context(request, error="不支持的订阅操作。"),
            status_code=404,
        )
    try:
        review.set_subscription_paused(paused=(action == "pause"))
    except review.ReviewError as exc:
        return templates.TemplateResponse(
            request,
            "review.html",
            _review_context(request, error=str(exc)),
            status_code=exc.status_code,
        )
    return RedirectResponse(url="/review", status_code=303)


# JSON API for automated checks ------------------------------------------------


@app.get("/api/review/register", response_class=JSONResponse)
async def api_review_register_get() -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "error": "请使用 POST 提交 service 与 artifact_version 表单字段。",
        },
        status_code=405,
    )


@app.post("/api/review/register", response_class=JSONResponse)
async def api_review_register(
    service: str = Form(...), artifact_version: str = Form(...)
) -> JSONResponse:
    try:
        registered = review.register_release(service, artifact_version)
    except review.ReviewError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc), "kind": type(exc).__name__},
            status_code=exc.status_code,
        )
    return JSONResponse({"ok": True, "registration": registered})


@app.get("/api/review/lookup/{service}/{revision}", response_class=JSONResponse)
async def api_review_lookup(service: str, revision: int) -> JSONResponse:
    try:
        result = review.lookup_revision(service, revision)
    except review.ReviewError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc), "kind": type(exc).__name__},
            status_code=exc.status_code,
        )
    return JSONResponse({"ok": True, "result": result})


@app.get("/api/review/subscription", response_class=JSONResponse)
async def api_review_subscription() -> JSONResponse:
    return JSONResponse(
        {"ok": True, "subscription": review.subscription_status()}
    )


@app.post("/api/review/subscription/{action}", response_class=JSONResponse)
async def api_review_subscription_action(action: str) -> JSONResponse:
    if action not in ("pause", "resume"):
        return JSONResponse(
            {"ok": False, "error": "action 必须是 pause 或 resume。"}, status_code=404
        )
    try:
        status = review.set_subscription_paused(paused=(action == "pause"))
    except review.ReviewError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc), "kind": type(exc).__name__},
            status_code=exc.status_code,
        )
    return JSONResponse({"ok": True, "subscription": status})


@app.get("/healthz", response_class=JSONResponse)
async def healthz() -> JSONResponse:
    try:
        with connect(application_name="lockdrill:health", autocommit=True) as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=503)
    active = store.active()
    return JSONResponse(
        {
            "ok": True,
            "active_drill_id": active.id if active else None,
            "review": {
                "subscription": review.subscription_status(),
                "connections": review.connection_summary(),
            },
        }
    )
