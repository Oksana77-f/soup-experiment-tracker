import asyncio
import hashlib
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict
from time import perf_counter_ns
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from psycopg import Error, OperationalError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import ValidationError
from sse_starlette import EventSourceResponse, JSONServerSentEvent, ServerSentEvent
from starlette.datastructures import MutableHeaders

from . import db
from .auth import COOKIE, Sessions, api_key, browser_origin
from .models import MAX_BODY_BYTES, MAX_INTEGER, Batch
from .settings import Settings

log = logging.getLogger("tracker")
timing_log = logging.getLogger("uvicorn.error.tracker.timing")


class PrivateResponses:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope["path"].startswith(("/runs", "/api/session")):
            await self.app(scope, receive, send)
            return

        async def no_store(message):
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)["Cache-Control"] = "no-store"
            await send(message)

        await self.app(scope, receive, no_store)


def checked_page(page):
    if page is None:
        raise HTTPException(401, "Invalid session")
    if page["id"] is None:
        raise HTTPException(404, "Run not found")
    return page


def stream_cursor(request):
    raw = request.headers.get("Last-Event-ID") or request.query_params.get("after", "0")
    if not raw.isascii() or not raw.isdecimal() or len(raw) > 16 or int(raw) > MAX_INTEGER:
        raise HTTPException(400, "Invalid event cursor")
    return int(raw)


async def batch_body(request):
    raw = bytearray()
    async for chunk in request.stream():
        if len(raw) + len(chunk) > MAX_BODY_BYTES:
            raise HTTPException(413, "Batch too large")
        raw.extend(chunk)
    try:
        return Batch.model_validate_json(raw), hashlib.sha256(raw).hexdigest()
    except ValidationError:
        raise HTTPException(422, "Invalid batch") from None


def create_app(settings=None):
    settings = settings or Settings.from_env()
    sessions = Sessions(settings)
    pool = AsyncConnectionPool(
        settings.database_url,
        kwargs={"row_factory": dict_row, "autocommit": True},
        min_size=1, max_size=16, timeout=5, open=False,
    )

    @asynccontextmanager
    async def lifespan(_app):
        async with pool:
            await pool.wait(timeout=15)
            await db.initialize(pool)
            yield

    app = FastAPI(title="Soup Experiment Tracker", lifespan=lifespan)
    app.add_middleware(PrivateResponses)
    app.state.pool = pool

    @app.exception_handler(Error)
    @app.exception_handler(PoolTimeout)
    async def database_error(_request, exc):
        log.error("Database operation failed (%s, %s)", type(exc).__name__, getattr(exc, "sqlstate", None))
        status = 503 if isinstance(exc, (OperationalError, PoolTimeout)) else 500
        return JSONResponse({"detail": "Database operation failed"}, status_code=status)

    @app.get("/health", include_in_schema=False)
    async def health():
        async with pool.connection() as conn:
            await conn.execute("SELECT 1")
        return {"ok": True}

    @app.post("/api/session")
    async def login(request: Request):
        browser_origin(request)
        key = await api_key(request, "read")
        response = Response(status_code=204)
        response.set_cookie(
            COOKIE, sessions.issue(key["id"]), max_age=settings.session_seconds,
            httponly=True, samesite="strict", secure=request.url.scheme == "https",
        )
        return response

    @app.post("/api/session/logout")
    async def logout(request: Request):
        browser_origin(request, required=True)
        response = Response(status_code=204)
        response.delete_cookie(COOKIE, httponly=True, samesite="strict", secure=request.url.scheme == "https")
        return response

    def read_key_id(request):
        key_id = sessions.key_id(request)
        if key_id is None:
            raise HTTPException(401, "Invalid session")
        return key_id

    @app.get("/runs")
    async def runs(request: Request):
        result = await db.list_runs(pool, read_key_id(request))
        if result is None:
            raise HTTPException(401, "Invalid session")
        return result

    @app.get("/runs/{run_id}")
    async def run(request: Request, run_id: UUID):
        return checked_page(await db.run_page(pool, read_key_id(request), run_id))

    @app.get("/runs/{run_id}/access")
    async def access(request: Request, run_id: UUID):
        checked_page(await db.run_page(pool, read_key_id(request), run_id, limit=0))
        return Response(status_code=204)

    @app.post("/runs/{run_id}/metrics")
    async def metrics(request: Request, run_id: UUID):
        started = perf_counter_ns() if settings.log_post_timings else 0
        key = await api_key(request, "write")
        batch, digest = await batch_body(request)
        timings = db.BatchWriteTimings() if settings.log_post_timings else None
        outcome = "error"
        try:
            acknowledgment = await db.save_batch(pool, key["project_id"], run_id, batch, digest, timings=timings)
            outcome = "accepted"
            return acknowledgment
        except db.MissingRun:
            outcome = "missing_run"
            raise HTTPException(404, "Run not found") from None
        except db.Conflict:
            outcome = "conflict"
            raise HTTPException(409, "Batch conflicts with stored data") from None
        finally:
            if timings is not None:
                sample = {"outcome": outcome, "total_ms": db.elapsed_ms(started), **asdict(timings)}
                timing_log.info("post_metrics_timing %s", json.dumps(sample))

    @app.get("/runs/{run_id}/events")
    async def events(request: Request, run_id: UUID):
        cursor = stream_cursor(request)
        key_id = read_key_id(request)
        initial = checked_page(await db.run_page(pool, key_id, run_id, cursor, settings.page_size))
        if cursor > initial["last_seq"]:
            raise HTTPException(400, "Invalid event cursor")

        async def read():
            after = cursor
            page = initial
            yield ServerSentEvent(retry=1000)
            while page is not None and page["id"] is not None:
                if sessions.key_id(request) is None:
                    return
                for point in page["points"]:
                    yield JSONServerSentEvent(data=point, event="metric", id=str(point["seq"]))
                    after = point["seq"]
                if not page["points"]:
                    await asyncio.sleep(settings.poll_seconds)
                try:
                    page = await db.run_page(pool, key_id, run_id, after, settings.page_size)
                except (Error, PoolTimeout) as exc:
                    log.warning("Stream read interrupted (%s)", type(exc).__name__)
                    return

        return EventSourceResponse(
            read(), send_timeout=settings.send_timeout, ping=15,
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    if (settings.static_dir / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=settings.static_dir / "assets"), name="assets")

    @app.get("/", include_in_schema=False)
    @app.get("/view/{_run_id}", include_in_schema=False)
    async def browser(_run_id: str | None = None):
        index = settings.static_dir / "index.html"
        if not index.is_file():
            return Response("Build the browser UI with npm run build.", status_code=503)
        return FileResponse(index, headers={"Cache-Control": "no-store"})

    return app
