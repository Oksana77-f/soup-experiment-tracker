"""Opt-in timings for the SSE path, with constant memory per process."""

import asyncio
import json
import logging
from bisect import bisect_left
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from math import ceil
from time import perf_counter_ns
from typing import ParamSpec, TypeVar

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_BUCKET_MS = (
    0.01, 0.025, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1, 1.5, 2, 3, 5, 8,
    12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 4096,
)
_BUCKET_NS = tuple(round(ms * 1_000_000) for ms in _BUCKET_MS)
_LOOP_PROBE_SECONDS = 0.02
log = logging.getLogger("uvicorn.error.tracker.sse_diagnostics")
P = ParamSpec("P")
T = TypeVar("T")


class TimingHistogram:
    def __init__(self) -> None:
        self._counts = [0] * (len(_BUCKET_NS) + 1)
        self._samples = 0
        self._max_ns = 0

    def record(self, elapsed_ns: int) -> None:
        self._counts[bisect_left(_BUCKET_NS, elapsed_ns)] += 1
        self._samples += 1
        self._max_ns = max(self._max_ns, elapsed_ns)

    def _upper_bound_ms(self, percentile: int) -> float | None:
        if not self._samples:
            return None
        rank = ceil(self._samples * percentile / 100)
        seen = 0
        for index, count in enumerate(self._counts[:-1]):
            seen += count
            if seen >= rank:
                return _BUCKET_MS[index]
        return self._max_ns / 1_000_000

    def summary(self) -> dict[str, int | float | None]:
        return {
            "samples": self._samples,
            "p50_upper_ms": self._upper_bound_ms(50),
            "p95_upper_ms": self._upper_bound_ms(95),
            "p99_upper_ms": self._upper_bound_ms(99),
            "max_ms": self._max_ns / 1_000_000 if self._samples else None,
        }


class SseDiagnostics:
    def __init__(self) -> None:
        self.loop_delay = TimingHistogram()
        self.page_read = TimingHistogram()
        self.data_send = TimingHistogram()

    def time_reads(self, read: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        async def timed(*args: P.args, **kwargs: P.kwargs) -> T:
            started = perf_counter_ns()
            try:
                return await read(*args, **kwargs)
            finally:
                self.page_read.record(perf_counter_ns() - started)

        return timed

    async def _probe_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            deadline = loop.time() + _LOOP_PROBE_SECONDS
            await asyncio.sleep(_LOOP_PROBE_SECONDS)
            delay_ns = round((loop.time() - deadline) * 1_000_000_000)
            self.loop_delay.record(max(delay_ns, 0))

    @asynccontextmanager
    async def monitor(self) -> AsyncIterator[None]:
        task = asyncio.create_task(self._probe_loop())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            log.info("sse_diagnostics %s", json.dumps({
                "loop_probe_ms": _LOOP_PROBE_SECONDS * 1000,
                "loop_delay": self.loop_delay.summary(),
                "page_read": self.page_read.summary(),
                "data_send": self.data_send.summary(),
            }))


class SseSendTiming:
    def __init__(self, app: ASGIApp, timings: TimingHistogram) -> None:
        self.app = app
        self.timings = timings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (scope["type"] != "http" or scope["method"] != "GET"
                or not scope["path"].startswith("/runs/") or not scope["path"].endswith("/events")):
            await self.app(scope, receive, send)
            return

        async def timed_send(message: Message) -> None:
            if message["type"] != "http.response.body" or b"\nevent: metric\r\n" not in message.get("body", b""):
                await send(message)
                return
            started = perf_counter_ns()
            try:
                await send(message)
            finally:
                self.timings.record(perf_counter_ns() - started)

        await self.app(scope, receive, timed_send)
