import asyncio
import importlib
import os
from dataclasses import replace
from urllib.parse import urlsplit

import httpx
import psycopg
import pytest
from playwright.async_api import Route, async_playwright, expect
from psycopg.rows import dict_row
from sse_starlette import JSONServerSentEvent
from sse_starlette.sse import SendTimeoutError
from starlette.datastructures import Headers

from tracker.admin import rotate_key
from tracker.app import create_app

from conftest import batch, frames, live_server, login, point, post, query, seed

app_module = importlib.import_module("tracker.app")


@pytest.fixture
async def browser():
    async with async_playwright() as playwright:
        instance = await playwright.chromium.launch(ignore_default_args=["--disable-back-forward-cache"])
        try:
            yield instance
        finally:
            await instance.close()


class StreamGate:
    def __init__(self, app, bad_gateway):
        self.app = app
        self.bad_gateway = bad_gateway
        self.requests = []
        self.held = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].endswith("/events"):
            self.requests.append((Headers(scope=scope).get("last-event-id"), scope["query_string"]))
            if len(self.requests) == 2:
                self.held.set()
                await asyncio.wait_for(self.release.wait(), 30)
                if self.bad_gateway:
                    await send({"type": "http.response.start", "status": 502, "headers": []})
                    await send({"type": "http.response.body", "body": b"test gateway failure"})
                    return
        await self.app(scope, receive, send)


async def sign_in(page, url, target):
    await page.goto(url + f"/view/{target.run}")
    await page.get_by_label("Project read key").fill(target.read)
    await page.get_by_role("button", name="Sign in").click()
    await expect(page.get_by_test_id("connection")).to_have_text("Live", timeout=8000)


@pytest.mark.parametrize("bad_gateway", [False, True], ids=["native-reconnect", "closed-after-502"])
async def test_browser_matches_input_and_database_after_disconnect(
    settings, browser, monkeypatch, bad_gateway
):
    gate = StreamGate(create_app(settings), bad_gateway)
    response_type = app_module.EventSourceResponse
    parse_cursor = app_module.stream_cursor

    def end_first_stream(content, **kwargs):
        if len(gate.requests) == 1:
            async def one_metric():
                try:
                    async for message in content:
                        yield message
                        if isinstance(message, JSONServerSentEvent):
                            # Exercise the UI's duplicate handling before closing.
                            yield message
                            return
                finally:
                    await content.aclose()
            return response_type(one_metric(), **kwargs)
        return response_type(content, **kwargs)

    def skip_replay(request):
        if request.headers.get("Last-Event-ID"):
            return 3
        return parse_cursor(request)

    monkeypatch.setattr(app_module, "EventSourceResponse", end_first_stream)
    if os.environ.get("TRACKER_TEST_MUTATION") == "skip-replay":
        monkeypatch.setattr(app_module, "stream_cursor", skip_replay)
    async with live_server(gate) as url:
        target = await seed(settings.database_url)
        page = await browser.new_page()
        try:
            await sign_in(page, url, target)
            rows = page.get_by_test_id("metric-row")
            async with httpx.AsyncClient(base_url=url, timeout=5) as sender:
                first = point(20, 2.0)
                late = point(10, 1.0)
                newer = point(30, 3.0)
                assert (await post(sender, target, batch(first))).status_code == 200
                await expect(rows).to_have_count(1, timeout=12000)
                await asyncio.wait_for(gate.held.wait(), 15)
                assert gate.requests[1] == ("1", b"after=0")
                second = batch(newer, first, late)
                assert (await post(sender, target, second)).json()["inserted"] == 2
                assert (await post(sender, target, second)).json()["inserted"] == 2
                assert (await post(sender, target, batch(late))).json()["inserted"] == 0
                gate.release.set()
                await expect(rows).to_have_count(3, timeout=5000)
                assert (await post(sender, target, batch(point(40, 4.0)))).status_code == 200
                await expect(rows).to_have_count(4)
                if bad_gateway:
                    assert gate.requests[2] == (None, b"after=1")
            stored = await query(settings.database_url,
                                 "SELECT seq, step, name, value FROM metrics WHERE run_id = %s ORDER BY step",
                                 (target.run,))
            expected = [{**p, "seq": seq} for p, seq in [(late, 3), (first, 1), (newer, 2), (point(40, 4.0), 4)]]
            assert stored == expected
            displayed = await rows.evaluate_all("(rows) => rows.map(r => [...r.cells].map(c => c.textContent))")
            assert displayed == [[str(p["step"]), p["name"], format(p["value"], "g"), str(p["seq"])]
                                 for p in expected]
        finally:
            gate.release.set()
            await page.close()


async def test_same_browser_does_not_reuse_previous_project_data(settings, browser):
    app = create_app(settings)
    async with live_server(app) as url:
        a = await seed(settings.database_url)
        b = await seed(settings.database_url)
        async with httpx.AsyncClient(base_url=url) as sender:
            assert (await post(sender, b, batch(point(71, 1.2, "private_loss")))).status_code == 200
        page = await browser.new_page()
        try:
            await page.add_init_script("""window.addEventListener('pageshow', event => {
              if (event.persisted) sessionStorage.setItem('soup-test-bfcache', 'restored');
            });""")
            await sign_in(page, url, b)
            await expect(page.get_by_text("private_loss")).to_be_visible()
            await page.get_by_role("link", name="All runs").click()
            await expect(page.get_by_role("heading", name="Training runs")).to_be_visible()
            await page.get_by_role("button", name="Sign out").click()
            await page.get_by_label("Project read key").fill(a.read)
            await page.get_by_role("button", name="Sign in").click()
            await expect(page.get_by_text(a.name, exact=True).first).to_be_visible()
            await page.go_back(wait_until="commit")
            await expect(page).to_have_url(url + f"/view/{b.run}")
            await expect(page.get_by_role("alert")).to_contain_text("Could not load")
            await expect(page.get_by_text("private_loss")).to_have_count(0)
            history = await page.evaluate("""() => ({
              restored: sessionStorage.getItem('soup-test-bfcache') === 'restored',
              reasons: performance.getEntriesByType('navigation')[0]?.notRestoredReasons?.reasons
                ?.map(reason => reason.reason) ?? []
            })""")
            print(f"Chromium history navigation: {history}")
            await page.goto(url + f"/view/{b.run}")
            await expect(page.get_by_role("alert")).to_contain_text("Could not load")
            await expect(page.get_by_text("private_loss")).to_have_count(0)
        finally:
            await page.close()


async def test_logout_clears_run_and_list_in_other_tabs(settings, browser):
    async with live_server(create_app(settings)) as url:
        target = await seed(settings.database_url)
        async with httpx.AsyncClient(base_url=url) as sender:
            assert (await post(sender, target, batch(point(1, name="private_loss")))).status_code == 200
        async with await browser.new_context() as context:
            run_tab = await context.new_page()
            await sign_in(run_tab, url, target)
            await expect(run_tab.get_by_text("private_loss")).to_be_visible()

            list_tab = await context.new_page()
            await list_tab.goto(url)
            await expect(list_tab.get_by_role("heading", name="Training runs")).to_be_visible()

            logout_tab = await context.new_page()
            await logout_tab.goto(url)
            await logout_tab.get_by_role("button", name="Sign out").click()

            for page in (run_tab, list_tab):
                await expect(page.get_by_role("heading", name="Sign in")).to_be_visible()
                await expect(page.get_by_text(target.name, exact=True)).to_have_count(0)
            await expect(run_tab.get_by_text("private_loss")).to_have_count(0)


async def test_snapshot_cursor_replays_multiple_pages_before_first_subscription(settings, browser):
    async with live_server(create_app(settings)) as url:
        target = await seed(settings.database_url)
        held = asyncio.Event()
        release = asyncio.Event()
        page = await browser.new_page()

        async def hold_first_stream(route: Route) -> None:
            held.set()
            await release.wait()
            await route.continue_()

        try:
            async with httpx.AsyncClient(base_url=url, timeout=5) as sender:
                assert (await post(sender, target, batch(point(101)))).status_code == 200
                stream = f"{url}/runs/{target.run}/events?after=1"
                await page.route(lambda address: address == stream, hold_first_stream)
                await page.goto(url + f"/view/{target.run}")
                await page.get_by_label("Project read key").fill(target.read)
                await page.get_by_role("button", name="Sign in").click()
                await asyncio.wait_for(held.wait(), 10)
                rows = page.get_by_test_id("metric-row")
                await expect(rows).to_have_count(1)

                late = batch(*(point(step) for step in range(101)))
                assert (await post(sender, target, late)).json()["inserted"] == 101
                release.set()
                await expect(rows).to_have_count(102, timeout=10000)

            expected = [{"step": step, "seq": step + 2} for step in range(101)] + [{"step": 101, "seq": 1}]
            stored = await query(settings.database_url, "SELECT step, seq FROM metrics WHERE run_id = %s ORDER BY step",
                                 (target.run,))
            assert stored == expected
            displayed = await rows.evaluate_all("rows => rows.map(r => [r.cells[0].textContent, r.cells[3].textContent])")
            assert displayed == [[str(item["step"]), str(item["seq"])] for item in expected]
        finally:
            release.set()
            await page.close()


async def test_browser_recovers_across_application_restart(settings, browser):
    page = await browser.new_page()
    try:
        async with live_server(create_app(settings)) as url:
            target = await seed(settings.database_url)
            await sign_in(page, url, target)
            async with httpx.AsyncClient(base_url=url) as sender:
                assert (await post(sender, target, batch(point(4)))).status_code == 200
            await expect(page.get_by_test_id("metric-row")).to_have_count(1)
        # Simulate a committed point arriving while the web process is unavailable.
        from tracker import db
        from tracker.models import Batch
        from psycopg.rows import dict_row
        from psycopg_pool import AsyncConnectionPool

        async with AsyncConnectionPool(settings.database_url, open=False, min_size=1,
                                       kwargs={"row_factory": dict_row, "autocommit": True}) as pool:
            await db.save_batch(pool, target.project, target.run, Batch.model_validate(batch(point(5))),
                                "restart-fixture")
        async with live_server(create_app(settings), port=urlsplit(url).port):
            await expect(page.get_by_test_id("metric-row")).to_have_count(2, timeout=12000)
            await expect(page.get_by_test_id("connection")).to_have_text("Live")
            await page.close()
    finally:
        if not page.is_closed():
            await page.close()


@pytest.mark.parametrize("stalled_frame", ["data", "heartbeat", "final"])
async def test_stalled_send_times_out_and_allows_replay(api, settings, monkeypatch, stalled_frame):
    app, client = api
    target = await seed(settings.database_url)
    await login(client, target)
    points = [point(1), point(2)] if stalled_frame == "data" else [point(1)]
    assert (await post(client, target, batch(*points))).status_code == 200

    if stalled_frame == "heartbeat":
        response_type = app_module.EventSourceResponse

        def frequent_heartbeats(content, **kwargs):
            kwargs["ping"] = 0.01
            return response_type(content, **kwargs)

        monkeypatch.setattr(app_module, "EventSourceResponse", frequent_heartbeats)

    received_ids = []
    request_sent = False
    send_blocked = False
    read_target = target

    async def receive():
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        nonlocal send_blocked, read_target
        if message["type"] != "http.response.body":
            return
        body = message.get("body", b"")
        if b"id: 1" in body:
            received_ids.append(1)
            if stalled_frame == "final":
                async with await psycopg.AsyncConnection.connect(settings.database_url, row_factory=dict_row) as conn:
                    read_key = await rotate_key(conn, target.project, "read")
                read_target = replace(target, read=read_key)
        if stalled_frame == "data" and b"id: 2" in body:
            stats = app.state.pool.get_stats()
            assert stats["pool_available"] == stats["pool_size"]
            send_blocked = True
        elif stalled_frame == "heartbeat" and body.startswith(b":") and received_ids:
            send_blocked = True
        elif stalled_frame == "final" and not message["more_body"]:
            send_blocked = True
        if send_blocked:
            await asyncio.Event().wait()

    path = f"/runs/{target.run}/events"
    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"}, "http_version": "1.1",
        "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode(),
        "root_path": "", "query_string": b"after=0", "client": ("127.0.0.1", 9000),
        "server": ("127.0.0.1", 8000),
        "headers": [(b"cookie", ("soup_session=" + client.cookies["soup_session"]).encode())],
    }
    timed_out = False
    try:
        await asyncio.wait_for(app(scope, receive, send), 6)
    except* SendTimeoutError:
        timed_out = True
    assert send_blocked and timed_out and received_ids == [1]

    if stalled_frame != "data":
        assert (await post(client, target, batch(point(2)))).status_code == 200
    async with live_server(create_app(settings)) as url:
        async with httpx.AsyncClient(base_url=url, timeout=3) as viewer:
            await login(viewer, read_target)
            async with viewer.stream("GET", path + "?after=1") as response:
                assert (await anext(frames(response)))["id"] == "2"
