"""Compare fixed-rate writes, PostgreSQL rows, SSE delivery and one real browser."""
import argparse
import asyncio
import json
import math
import os
import platform
from collections import Counter
from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

import httpx
import psycopg
from playwright.async_api import async_playwright, expect
from psycopg.rows import dict_row

from tracker.admin import ensure_project, ensure_run, rotate_key
from tracker.send import MAX_ATTEMPTS, RETRYABLE, retry_delay

RUNS = 50
RATE = 10


def auth(key):
    return {"Authorization": "Bearer " + key}


def latency(values):
    ordered = sorted(values)
    if not ordered:
        return {}
    return {f"p{p}_ms": round(1000 * ordered[math.ceil(len(ordered) * p / 100) - 1], 2)
            for p in (50, 95, 99)}


def point(step):
    return {"name": "loss", "step": step, "value": (step + 1) / 1000}


def expected(step):
    return (step + 1, "loss", step, point(step)["value"])


async def prepare(dsn):
    async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
        project = await ensure_project(conn, "Benchmark " + uuid4().hex[:10])
        runs = [await ensure_run(conn, project, f"Run {index:02d}") for index in range(RUNS)]
        read = await rotate_key(conn, project, "read")
        write = await rotate_key(conn, project, "write")
    return runs, read, write


async def storage(dsn, runs):
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        result = await conn.execute(
            """SELECT run_id, seq, name, step, value FROM metrics
               WHERE run_id = ANY(%s::uuid[]) ORDER BY run_id, seq""", (runs,),
        )
        rows = {run: [] for run in runs}
        for run, *fields in await result.fetchall():
            rows[run].append(tuple(fields))
        return rows


async def run_load(args):
    dsn = os.environ["DATABASE_URL"]
    runs, read, write = await prepare(dsn)
    count = RATE * args.seconds
    expected_rows = [expected(step) for step in range(count)]
    expected_points = {row[1:] for row in expected_rows}
    observers = args.viewers * RUNS
    sent_at = {}
    read_at = {}
    dom_at = {}
    post_times = []
    schedule_lags = []
    retries = []
    received = {}
    errors = []
    ready = asyncio.Queue()
    loop = asyncio.get_running_loop()
    print(json.dumps({"started_utc": datetime.now(timezone.utc).isoformat(),
                      "runs": RUNS, "metrics_per_run_per_second": RATE,
                      "seconds": args.seconds, "viewers_per_run": args.viewers,
                      "browser_viewers": 1, "http_sse_viewers": observers - 1,
                      "metrics_per_batch": RATE, "schedule": "one synchronized batch per run per second",
                      "driver_platform": platform.platform()}), flush=True)

    async with async_playwright() as playwright, \
            httpx.AsyncClient(base_url=args.url, timeout=6, limits=httpx.Limits(max_connections=RUNS + 5, max_keepalive_connections=RUNS)) as sender, \
            httpx.AsyncClient(base_url=args.url, timeout=httpx.Timeout(15, read=None),
                              limits=httpx.Limits(max_connections=observers + 5)) as viewer:
        (await viewer.post("/api/session", headers=auth(read))).raise_for_status()
        browser = await playwright.chromium.launch()
        print(json.dumps({"browser_version": browser.version}), flush=True)
        page = await browser.new_page()
        await page.expose_function("reportVisibleMetric", lambda step: dom_at.setdefault(step, perf_counter()))
        await page.goto(args.url + f"/view/{runs[0]}")
        await page.get_by_label("Project read key").fill(read)
        await page.get_by_role("button", name="Sign in").click()
        await expect(page.get_by_test_id("connection")).to_have_text("Live", timeout=12000)
        await page.evaluate("""() => {
            const seen = new Set();
            new MutationObserver(() => {
                for (const row of document.querySelectorAll('[data-testid="metric-row"]')) {
                    const step = Number(row.cells[0].textContent);
                    if (!seen.has(step)) {
                        seen.add(step);
                        window.reportVisibleMetric(step);
                    }
                }
            }).observe(document.querySelector('main'), {childList: true, subtree: true});
        }""")

        async def consume(run, index):
            entries = []
            received[(run, index)] = entries
            try:
                async with viewer.stream("GET", f"/runs/{run}/events?after=0") as response:
                    response.raise_for_status()
                    ready.put_nowait(True)
                    lines = response.aiter_lines()
                    while True:
                        try:
                            async with asyncio.timeout(1.3 if len(entries) >= count else 15):
                                line = await anext(lines)
                        except TimeoutError:
                            if len(entries) >= count:
                                break
                            raise
                        if line.startswith("data: "):
                            row = json.loads(line[6:])
                            item = (row["seq"], row["name"], row["step"], row["value"])
                            entries.append(item)
                            read_at.setdefault((run, index, row["step"]), perf_counter())
            except (StopAsyncIteration, TimeoutError, httpx.HTTPError, ValueError) as exc:
                errors.append(f"stream {run}/{index}: {type(exc).__name__}")

        streams = [asyncio.create_task(consume(run, index))
                   for run in runs for index in range(args.viewers) if (run, index) != (runs[0], 0)]
        try:
            async with asyncio.timeout(25):
                for _ in streams:
                    await ready.get()
            start = loop.time() + 0.15

            async def send_batch(run, tick, payload):
                for attempt in range(MAX_ATTEMPTS):
                    try:
                        response = await sender.post(
                            f"/runs/{run}/metrics",
                            headers={**auth(write), "Content-Type": "application/json"},
                            content=payload,
                        )
                    except httpx.TransportError:
                        if attempt + 1 == MAX_ATTEMPTS:
                            raise
                    else:
                        if response.status_code not in RETRYABLE or attempt + 1 == MAX_ATTEMPTS:
                            response.raise_for_status()
                            return response.json()
                    retries.append((run, tick))
                    await asyncio.sleep(retry_delay(attempt))


            async def write_run(run):
                for tick in range(args.seconds):
                    await asyncio.sleep(max(0, start + tick - loop.time()))
                    steps = range(tick * RATE, (tick + 1) * RATE)
                    schedule_lags.append(max(0, loop.time() - (start + tick)))
                    began = perf_counter()
                    sent_at.update({(run, step): began for step in steps})
                    try:
                        payload = json.dumps({"batch_id": str(uuid4()), "points": [point(step) for step in steps]}).encode()
                        acknowledgment = await send_batch(run, tick, payload)
                        if acknowledgment["inserted"] != RATE:
                            errors.append(f"unexpected acknowledgment for {run}/{tick}")
                    except (httpx.HTTPError, ValueError) as exc:
                        errors.append(f"write {run}/{tick}: {type(exc).__name__}")
                    post_times.append(perf_counter() - began)

            await asyncio.gather(*(write_run(run) for run in runs))
            write_window = loop.time() - start
            await asyncio.gather(*streams)
            visible = await page.get_by_test_id("metric-row").evaluate_all(
                "(rows) => rows.map(r => [...r.cells].map(c => c.textContent))"
            )
            received[(runs[0], 0)] = [(int(seq), name, int(step), float(value))
                                      for step, name, value, seq in visible]
        finally:
            for stream in streams:
                stream.cancel()
            await asyncio.gather(*streams, return_exceptions=True)
            await browser.close()

    stored = await storage(dsn, runs)
    database_mismatches = sum(stored[run] != expected_rows for run in runs)
    subscriber_missing = sum(len(expected_points - {entry[1:] for entry in entries}) for entries in received.values())
    subscriber_duplicates = sum(sum(value - 1 for value in Counter((entry[1], entry[2]) for entry in entries).values())
                                for entries in received.values())
    subscriber_mismatches = sum(entries != expected_rows for entries in received.values())
    sse_latency = latency(stamp - sent_at[(run, step)] for (run, _, step), stamp in read_at.items()
                          if (run, step) in sent_at)
    dom_latency = latency(stamp - sent_at[(runs[0], step)] for step, stamp in dom_at.items()
                          if (runs[0], step) in sent_at)
    within_schedule = write_window <= args.seconds + 1
    passed = (not errors and not database_mismatches and not subscriber_mismatches
              and len(dom_at) == count and dom_latency.get("p99_ms", math.inf) < 2000
              and within_schedule)
    print(json.dumps({"attempted_metrics": len(sent_at), "stored_metrics": sum(map(len, stored.values())),
                      "database_mismatches": database_mismatches, "subscriber_missing": subscriber_missing,
                      "subscriber_duplicates": subscriber_duplicates, "subscriber_mismatches": subscriber_mismatches,
                      "dom_samples": len(dom_at), "write_window_s": round(write_window, 3), "transport_retries": len(retries),
                      "post": latency(post_times), "schedule_lag": latency(schedule_lags),
                      "post_to_sse": sse_latency, "post_to_dom": dom_latency,
                      "errors": errors, "verdict": "SHIP" if passed else "DON'T SHIP"}, indent=2))
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--viewers", type=int, default=1)
    args = parser.parse_args()
    if args.seconds < 1 or args.viewers < 1:
        parser.error("seconds and viewers must be positive")
    raise SystemExit(0 if asyncio.run(run_load(args)) else 1)


if __name__ == "__main__":
    main()
