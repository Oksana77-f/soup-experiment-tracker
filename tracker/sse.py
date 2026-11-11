from asyncio import timeout
from collections.abc import AsyncGenerator, Mapping
from contextlib import aclosing

from sse_starlette import EventSourceResponse as StarletteEventSourceResponse
from sse_starlette import ServerSentEvent
from sse_starlette.sse import SendTimeoutError
from starlette.types import Message, Receive, Scope, Send


class EventSourceResponse(StarletteEventSourceResponse):
    """Bound every ASGI send, including heartbeats and the final body."""

    def __init__(
        self,
        content: AsyncGenerator[ServerSentEvent, None],
        *,
        send_timeout: float,
        ping: float,
        headers: Mapping[str, str],
    ) -> None:
        super().__init__(content, send_timeout=None, ping=ping, headers=headers)
        self._content = content
        self._frame_timeout = send_timeout

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def send_with_timeout(message: Message) -> None:
            try:
                async with timeout(self._frame_timeout):
                    await send(message)
            except TimeoutError as exc:
                raise SendTimeoutError() from exc

        async with aclosing(self._content):
            await super().__call__(scope, receive, send_with_timeout)
