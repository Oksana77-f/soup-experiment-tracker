import pytest
from sse_starlette import JSONServerSentEvent, ServerSentEvent

from tracker import sse_diagnostics
from tracker.sse_diagnostics import SseDiagnostics, SseSendTiming, TimingHistogram


def test_histogram_returns_upper_bounds_and_handles_overflow():
    timing = TimingHistogram()
    assert timing.summary()["p99_upper_ms"] is None
    for elapsed in (500_000, 1_200_000, 9_000_000_000):
        timing.record(elapsed)
    assert timing.summary() == {
        "samples": 3,
        "p50_upper_ms": 1.5,
        "p95_upper_ms": 9000,
        "p99_upper_ms": 9000,
        "max_ms": 9000,
    }


async def test_sse_page_timing_preserves_result_and_error(monkeypatch):
    diagnostics = SseDiagnostics()
    ticks = iter((0, 800_000, 1_000_000, 6_000_000))
    monkeypatch.setattr(sse_diagnostics, "perf_counter_ns", lambda: next(ticks))

    async def read(*, fail: bool) -> str:
        if fail:
            raise RuntimeError("read failed")
        return "page"

    timed_read = diagnostics.time_reads(read)
    assert await timed_read(fail=False) == "page"
    with pytest.raises(RuntimeError, match="read failed"):
        await timed_read(fail=True)
    assert diagnostics.page_read.summary()["samples"] == 2
    assert diagnostics.page_read.summary()["max_ms"] == 5


async def test_sse_send_timing_excludes_ping_and_non_stream_routes(monkeypatch):
    timing = TimingHistogram()
    ticks = iter((0, 500_000))
    monkeypatch.setattr(sse_diagnostics, "perf_counter_ns", lambda: next(ticks))
    frames = [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": ServerSentEvent(comment="ping").encode(), "more_body": True},
        {
            "type": "http.response.body",
            "body": JSONServerSentEvent(data={"seq": 1}, event="metric", id="1").encode(),
            "more_body": True,
        },
        {"type": "http.response.body", "body": b"", "more_body": False},
    ]
    received = []

    async def app(scope, receive, send):
        for frame in frames:
            await send(frame)

    async def send(frame):
        received.append(frame)

    timing_app = SseSendTiming(app, timing)
    await timing_app({"type": "http", "method": "GET", "path": "/runs/one"}, None, send)
    assert timing.summary()["samples"] == 0
    await timing_app({"type": "http", "method": "GET", "path": "/runs/one/events"}, None, send)
    assert received == frames + frames
    assert timing.summary()["samples"] == 1
    assert timing.summary()["max_ms"] == 0.5
