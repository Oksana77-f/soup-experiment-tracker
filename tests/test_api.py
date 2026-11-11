import asyncio
import json
from dataclasses import replace
from time import perf_counter
from uuid import uuid4

import httpx
import psycopg
import pytest
from itsdangerous import URLSafeTimedSerializer
from psycopg.rows import dict_row

from tracker import db
from tracker.admin import rotate_key
from tracker.app import create_app, timing_log
from tracker.auth import COOKIE
from tracker.models import MAX_BODY_BYTES, MAX_INTEGER, Batch
from tracker.send import post_batch
from tracker.settings import Settings

from conftest import auth, batch, frames, live_server, login, point, post, query, seed


async def test_batches_retry_overlap_conflict_and_scope(api, settings):
    _, client = api
    a = await seed(settings.database_url)
    b = await seed(settings.database_url)
    first = batch(point(20), point(20))
    ack = await post(client, a, first)
    assert ack.status_code == 200 and ack.json()["inserted"] == 1
    assert (await post(client, a, first)).json() == ack.json()
    assert (await post(client, b, first)).status_code == 200
    assert (await post(client, a, batch(point(20)))).json()["inserted"] == 0
    mixed = batch(point(20), point(10))
    assert (await post(client, a, mixed)).json()["inserted"] == 1
    assert (await post(client, a, {**mixed, "points": [point(10, 9)]})).status_code == 409
    assert (await post(client, a, batch(point(40), point(20, 9)))).status_code == 409
    assert (await post(client, a, batch(point(50), point(50, 9)))).status_code == 409
    await login(client, a)
    state = (await client.get(f"/runs/{a.run}")).json()
    assert state["last_seq"] == 2
    assert [(p["step"], p["seq"]) for p in state["points"]] == [(20, 1), (10, 2)]


async def test_batch_write_timings_measure_separate_phases(api, settings, monkeypatch):
    app, _ = api
    target = await seed(settings.database_url)
    clock = iter((0, 1_000_000, 10_000_000, 12_000_000, 100_000_000, 103_000_000))
    monkeypatch.setattr(db, "perf_counter_ns", lambda: next(clock))
    timings = db.BatchWriteTimings()
    payload = Batch.model_validate(batch(point(1)))

    acknowledgment = await db.save_batch(
        app.state.pool, target.project, target.run, payload, "test-digest", timings=timings
    )

    assert acknowledgment["inserted"] == 1
    assert timings == db.BatchWriteTimings(pool_wait_ms=1, run_lock_ms=2, commit_ms=3)
    assert next(clock, None) is None


@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled"])
async def test_post_timing_log_preserves_batch_outcomes(settings, monkeypatch, enabled):
    messages = []
    monkeypatch.setattr(timing_log, "info", lambda message, sample: messages.append(message % sample))
    app = create_app(replace(settings, log_post_timings=enabled))
    async with app.router.lifespan_context(app):
        target = await seed(settings.database_url)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://tracker.test") as client:
            payload = batch(point(1))
            first = await post(client, target, payload)
            repeated = await post(client, target, payload)
            conflict = await post(client, target, {**payload, "points": [point(1, 2)]})
            denied = await client.post(f"/runs/{target.run}/metrics", json=payload)
    assert first.status_code == repeated.status_code == 200
    assert first.json() == repeated.json()
    assert conflict.status_code == 409
    assert denied.status_code == 401

    if not enabled:
        assert messages == []
        return

    prefix = "post_metrics_timing "
    assert len(messages) == 3
    assert all(message.startswith(prefix) for message in messages)
    samples = [json.loads(message.removeprefix(prefix)) for message in messages]
    assert [sample["outcome"] for sample in samples] == ["accepted", "accepted", "conflict"]
    for sample in samples:
        assert set(sample) == {"outcome", "total_ms", "pool_wait_ms", "run_lock_ms", "commit_ms"}
        for key in ("total_ms", "pool_wait_ms", "run_lock_ms"):
            assert isinstance(sample[key], (int, float)) and sample[key] >= 0
    assert all(isinstance(sample["commit_ms"], (int, float)) and sample["commit_ms"] >= 0
               for sample in samples[:2])
    assert samples[2]["commit_ms"] is None


async def test_batch_input_limits_reject_without_side_effects(api, settings):
    _, client = api
    target = await seed(settings.database_url)
    url = f"/runs/{target.run}/metrics"
    headers = {**auth(target.write), "Content-Type": "application/json"}
    oversized = await client.post(url, headers=headers, content=b" " * (MAX_BODY_BYTES + 1))
    assert oversized.status_code == 413
    assert oversized.json() == {"detail": "Batch too large"}

    invalid_batches = [
        batch(),
        batch(*(point(step) for step in range(257))),
        batch(point(-1)),
        batch(point(True)),
        batch({**point(1), "unexpected": 1}),
    ]
    for invalid in invalid_batches:
        response = await post(client, target, invalid)
        assert response.status_code == 422
        assert response.json() == {"detail": "Invalid batch"}

    overflow = json.dumps(batch(point(1, float("inf")))).replace("Infinity", "1e999")
    response = await client.post(url, headers=headers, content=overflow)
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid batch"}

    await login(client, target)
    state = (await client.get(f"/runs/{target.run}")).json()
    assert (state["last_seq"], state["points"]) == (0, [])
    receipts = await query(settings.database_url, "SELECT 1 FROM receipts WHERE run_id = %s", (target.run,))
    assert receipts == []


async def test_invalid_sse_cursor_is_rejected_before_opening_stream(api, settings):
    _, client = api
    target = await seed(settings.database_url)
    assert (await post(client, target, batch(point(1)))).status_code == 200
    await login(client, target)
    url = f"/runs/{target.run}/events"

    async with asyncio.timeout(5):
        for cursor in ("-1", "invalid", str(MAX_INTEGER + 1), "2"):
            response = await client.get(url, params={"after": cursor})
            assert response.status_code == 400
            assert response.json() == {"detail": "Invalid event cursor"}

        response = await client.get(url, params={"after": "0"}, headers={"Last-Event-ID": "invalid"})
        assert response.status_code == 400
        assert response.json() == {"detail": "Invalid event cursor"}

    assert (await client.get("/runs/not-a-uuid/events")).status_code == 422


async def test_failure_after_insert_rolls_back_receipt_points_and_cursor(api, settings, monkeypatch):
    _, client = api
    target = await seed(settings.database_url)
    payload = batch(point(1), point(2))
    insert = db.insert_points

    async def fail(conn, *args):
        await insert(conn, *args)
        raise psycopg.OperationalError("simulated after metric insert")

    with monkeypatch.context() as patch:
        patch.setattr(db, "insert_points", fail)
        assert (await post(client, target, payload)).status_code == 503
    await login(client, target)
    state = (await client.get(f"/runs/{target.run}")).json()
    assert state["last_seq"] == 0 and state["points"] == []
    receipts = await query(settings.database_url, "SELECT * FROM receipts WHERE run_id = %s", (target.run,))
    assert receipts == []
    assert (await post(client, target, payload)).json()["last_seq"] == 2


async def test_project_isolation_and_csrf(api, settings):
    _, client = api
    a = await seed(settings.database_url)
    b = await seed(settings.database_url)
    await login(client, b)
    owned = await client.get(f"/runs/{b.run}")
    assert owned.status_code == 200 and owned.headers["cache-control"] == "no-store"
    await login(client, a)
    listing = await client.get("/runs")
    assert listing.json() == {"project": a.name, "runs": [{"id": str(a.run), "name": "Training run"}]}
    for suffix in ("", "/access", "/events?after=0"):
        foreign = await client.get(f"/runs/{b.run}{suffix}")
        missing = await client.get(f"/runs/{uuid4()}{suffix}")
        assert foreign.status_code == missing.status_code == 404
        assert foreign.content == missing.content
        assert foreign.headers["cache-control"] == "no-store"
    payload = batch(point(2))
    foreign_write = await post(client, b, payload, key=a.write)
    missing_write = await client.post(f"/runs/{uuid4()}/metrics", headers=auth(a.write), json=payload)
    assert foreign_write.status_code == missing_write.status_code == 404
    assert foreign_write.content == missing_write.content
    assert (await client.post(f"/runs/{a.run}/metrics", json=batch(point(1)))).status_code == 401
    assert (await post(client, a, batch(point(2)), key=a.read)).status_code == 401
    cross_origin = {"Origin": "https://other.invalid"}
    denied = await client.post("/api/session", headers={**auth(b.read), **cross_origin})
    assert denied.status_code == 403 and "set-cookie" not in denied.headers
    assert (await client.post("/api/session/logout", headers=cross_origin)).status_code == 403
    assert (await client.post("/api/session/logout")).status_code == 403
    assert (await client.get("/runs")).json()["project"] == a.name


async def test_logout_revokes_captured_session_and_open_stream(settings):
    app = create_app(replace(settings, poll_seconds=Settings.poll_seconds))
    async with live_server(app) as url:
        target = await seed(settings.database_url)
        async with httpx.AsyncClient(base_url=url, timeout=5) as owner, \
                httpx.AsyncClient(base_url=url, timeout=5) as peer:
            await login(owner, target)
            await login(peer, target)
            captured = {"Cookie": f"{COOKIE}={owner.cookies[COOKIE]}"}
            assert (await post(owner, target, batch(point(1)))).status_code == 200
            path = f"/runs/{target.run}"
            async with owner.stream("GET", path + "/events") as response:
                assert response.status_code == 200
                messages = frames(response)
                assert (await anext(messages))["id"] == "1"
                assert (await owner.post("/api/session/logout", headers={"Origin": url})).status_code == 204
                assert (await post(owner, target, batch(point(2)))).status_code == 200
                with pytest.raises(StopAsyncIteration):
                    await asyncio.wait_for(anext(messages), 2)
            assert (await peer.get(path)).status_code == 200
            await login(owner, target)
            assert (await owner.get(path)).status_code == 200

    async with live_server(create_app(settings)) as url:
        async with httpx.AsyncClient(base_url=url, timeout=5) as restarted:
            for protected in ("/runs", path, path + "/access", path + "/events"):
                assert (await restarted.get(protected, headers=captured)).status_code == 401
            assert (await restarted.post("/api/session/logout", headers={**captured, "Origin": url})).status_code == 204


async def test_logout_revokes_legacy_cookie(api, settings):
    _, client = api
    target = await seed(settings.database_url)
    [key] = await query(settings.database_url,
                        "SELECT id FROM api_keys WHERE project_id = %s AND scope = 'read'", (target.project,))
    signer = URLSafeTimedSerializer(settings.session_secret, salt="soup-read-v1")
    captured = {"Cookie": f"{COOKIE}={signer.dumps(str(key['id']))}"}
    assert (await client.get("/runs", headers=captured)).status_code == 200
    assert (await client.post("/api/session/logout",
                              headers={**captured, "Origin": "http://tracker.test"})).status_code == 204
    assert (await client.get("/runs", headers=captured)).status_code == 401


async def test_concurrent_batches_share_one_point(api, settings, monkeypatch):
    import os

    _, client = api
    target = await seed(settings.database_url)
    insert = db.insert_points
    first_checked = asyncio.Event()
    second_checked = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    mutant = os.environ.get("TRACKER_TEST_MUTATION") == "no-run-lock"

    async def insert_at_barrier(conn, run_id, last_seq, new_points):
        nonlocal calls
        calls += 1
        if calls == 1:
            first_checked.set()
            await release.wait()
        elif mutant:
            assert new_points, "Both writers should see a new point before release"
            second_checked.set()
            await release.wait()
        return await insert(conn, run_id, last_seq, new_points)

    async def unlocked(conn, run_id, project_id):
        cursor = await conn.execute("SELECT last_seq FROM runs WHERE id = %s AND project_id = %s",
                                    (run_id, project_id))
        return await cursor.fetchone()

    monkeypatch.setattr(db, "insert_points", insert_at_barrier)
    if mutant:
        monkeypatch.setattr(db, "lock_run", unlocked)
    first = asyncio.create_task(post(client, target, batch(point(3))))
    second = None
    try:
        await asyncio.wait_for(first_checked.wait(), 5)
        second = asyncio.create_task(post(client, target, batch(point(3))))
        async with asyncio.timeout(5):
            if mutant:
                await second_checked.wait()
            else:
                while not await query(settings.database_url, """
                    SELECT 1 FROM pg_stat_activity
                    WHERE datname = current_database() AND wait_event_type = 'Lock'
                      AND query LIKE 'SELECT last_seq FROM runs WHERE%%'
                    """):
                    await asyncio.sleep(0.02)
    finally:
        release.set()
        responses = await asyncio.gather(first, *([second] if second is not None else []))
    assert [response.status_code for response in responses] == [200, 200], "Both duplicate writes must succeed"
    assert sorted(response.json()["inserted"] for response in responses) == [0, 1]
    metrics = await query(settings.database_url, "SELECT seq, step FROM metrics WHERE run_id = %s", (target.run,))
    assert metrics == [{"seq": 1, "step": 3}]


async def test_rotation_stops_open_stream_and_old_keys(settings):
    app = create_app(replace(settings, poll_seconds=Settings.poll_seconds))
    async with live_server(app) as url:
        target = await seed(settings.database_url)
        async with httpx.AsyncClient(base_url=url, timeout=5) as client:
            await login(client, target)
            await post(client, target, batch(point(7)))
            async with client.stream("GET", f"/runs/{target.run}/events") as stream:
                assert stream.headers["cache-control"] == "no-store"
                messages = frames(stream)
                assert (await anext(messages))["id"] == "1"
                async with await psycopg.AsyncConnection.connect(settings.database_url, row_factory=dict_row) as conn:
                    new_read = await rotate_key(conn, target.project, "read")
                    new_write = await rotate_key(conn, target.project, "write")
                rotated_at = perf_counter()
                async with asyncio.timeout(2):
                    assert (await post(client, target, batch(point(8)))).status_code == 401
                    assert (await post(client, target, batch(point(8)), key=new_write)).status_code == 200
                    with pytest.raises(StopAsyncIteration):
                        await anext(messages)
                print(f"Rotation commit to observed SSE EOF: {perf_counter() - rotated_at:.3f} s (idle poll 1 s)")
            assert (await client.get("/runs")).status_code == 401
            assert (await client.post("/api/session", headers=auth(target.read))).status_code == 401
            assert (await client.post("/api/session", headers=auth(new_read))).status_code == 204
            assert (await client.get("/runs")).status_code == 200


def test_sender_keeps_exact_body_on_retry_and_stops_on_409():
    seen = []

    def reply(request):
        seen.append(request.content)
        return httpx.Response(502 if len(seen) == 1 else 409)

    with httpx.Client(base_url="http://tracker.test", transport=httpx.MockTransport(reply)) as client:
        with pytest.raises(httpx.HTTPStatusError, match="409"):
            post_batch(client, uuid4(), b'{"batch_id":"fixed"}', pause=lambda _delay: None)
    assert seen == [b'{"batch_id":"fixed"}'] * 2
