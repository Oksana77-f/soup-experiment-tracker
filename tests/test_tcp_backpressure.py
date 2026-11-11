import asyncio
import socket
import sys
from urllib.parse import urlsplit

import httpx
import pytest
from sse_starlette.sse import SendTimeoutError

from tracker import app as app_module

from conftest import batch, frames, live_server, login, point, post, seed


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Requires Linux loopback TCP buffer behavior.",
)
async def test_nonreading_loopback_client_times_out_and_can_replay(settings, monkeypatch):
    timed_out = asyncio.Event()
    completed_metrics = 0

    class ObservedResponse(app_module.EventSourceResponse):
        async def __call__(self, scope, receive, send):
            async def observe_send(message):
                nonlocal completed_metrics
                await send(message)
                if b"event: metric" in message.get("body", b""):
                    completed_metrics += 1

            try:
                await super().__call__(scope, receive, observe_send)
            except* SendTimeoutError:
                timed_out.set()
                raise

    monkeypatch.setattr(app_module, "EventSourceResponse", ObservedResponse)

    point_count = 2048
    batch_size = 256
    metric_name = "metric_" + "x" * 57
    socket_buffer = 4096
    async with live_server(app_module.create_app(settings), send_buffer_size=socket_buffer) as url:
        target = await seed(settings.database_url)
        path = f"/runs/{target.run}/events?after=0"
        async with httpx.AsyncClient(base_url=url, timeout=5) as client:
            for start in range(0, point_count, batch_size):
                points = [point(step, name=metric_name) for step in range(start, start + batch_size)]
                response = await post(client, target, batch(*points))
                assert response.status_code == 200 and response.json()["inserted"] == batch_size
            await login(client, target)

            endpoint = urlsplit(url)
            assert endpoint.hostname == "127.0.0.1" and endpoint.port is not None
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {endpoint.netloc}\r\n"
                f"Cookie: soup_session={client.cookies['soup_session']}\r\n\r\n"
            )
            loop = asyncio.get_running_loop()
            with socket.socket() as slow_client:
                slow_client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, socket_buffer)
                slow_client.setblocking(False)
                async with asyncio.timeout(8):
                    await loop.sock_connect(slow_client, (endpoint.hostname, endpoint.port))
                    await loop.sock_sendall(slow_client, request.encode("ascii"))
                    # Leave the TCP connection open without draining the receive buffer.
                    await timed_out.wait()
                assert 0 < completed_metrics < point_count

        async with httpx.AsyncClient(base_url=url, timeout=5) as viewer:
            await login(viewer, target)
            async with viewer.stream("GET", path) as response:
                assert response.status_code == 200
                messages = frames(response)
                async with asyncio.timeout(10):
                    for seq in range(1, point_count + 1):
                        assert int((await anext(messages))["id"]) == seq
