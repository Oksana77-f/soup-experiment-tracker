import asyncio
import os
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4

import httpx
import psycopg
import pytest
import uvicorn
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from sse_starlette.sse import AppStatus

from tracker.admin import ensure_project, ensure_run, rotate_key
from tracker.app import create_app
from tracker.settings import Settings


@dataclass(frozen=True)
class Seed:
    project: object
    name: str
    run: object
    read: str
    write: str


@pytest.fixture(scope="session")
def settings():
    dsn = os.environ["DATABASE_URL"]
    name = conninfo_to_dict(dsn)["dbname"]
    if not name.endswith("_tests"):
        pytest.exit("Use a separate database whose name ends in _tests.")
    try:
        with psycopg.connect(dsn):
            pass
    except psycopg.OperationalError:
        with psycopg.connect(make_conninfo(dsn, dbname="postgres"), autocommit=True) as conn:
            if conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone() is None:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    return Settings(dsn, "test-only-signing-secret-long-enough-for-the-tracker",
                    poll_seconds=0.05)


async def seed(dsn):
    async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
        name = "Test " + uuid4().hex[:10]
        project = await ensure_project(conn, name)
        run = await ensure_run(conn, project, "Training run")
        read = await rotate_key(conn, project, "read")
        write = await rotate_key(conn, project, "write")
    return Seed(project, name, run, read, write)


async def query(dsn, statement, params=()):
    async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
        return await (await conn.execute(statement, params)).fetchall()


@pytest.fixture
async def api(settings):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://tracker.test") as client:
            yield app, client


@asynccontextmanager
async def live_server(app, port=0, *, send_buffer_size: int | None = None):
    # Each fixture server models a fresh process, including the SSE shutdown flag.
    AppStatus.should_exit = False
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if send_buffer_size is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buffer_size)
    sock.bind(("127.0.0.1", port))
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, access_log=False, log_level="warning",
                            timeout_graceful_shutdown=1, lifespan="on")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(15):
            while not server.started:
                if task.done():
                    await task
                    raise RuntimeError("Uvicorn exited during startup")
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 8)
        finally:
            sock.close()
            AppStatus.should_exit = False


def auth(token):
    return {"Authorization": "Bearer " + token}


def point(step, value=1.25, name="loss"):
    return {"name": name, "step": step, "value": value}


def batch(*points, batch_id=None):
    return {"batch_id": str(batch_id or uuid4()), "points": list(points)}


async def post(client, target, payload, key=None):
    return await client.post(f"/runs/{target.run}/metrics",
                             headers=auth(key or target.write), json=payload)


async def login(client, target):
    response = await client.post("/api/session", headers=auth(target.read))
    assert response.status_code == 204


async def frames(response):
    event = {}
    async for line in response.aiter_lines():
        if not line:
            if "data" in event:
                yield event
            event = {}
        elif ": " in line:
            field, value = line.split(": ", 1)
            event[field] = value
