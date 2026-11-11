import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from fastapi import HTTPException, Request
from itsdangerous import BadData, URLSafeTimedSerializer

from . import db

COOKIE = "soup_session"


@dataclass(frozen=True)
class ReadSession:
    key_id: UUID
    digest: str
    expires_at: datetime


def bearer(request):
    kind, _, token = request.headers.get("Authorization", "").partition(" ")
    if kind.lower() != "bearer" or not token or len(token) > 128:
        raise HTTPException(401, "Invalid credentials")
    return token


async def api_key(request, scope):
    key = await db.find_key(request.app.state.pool, bearer(request), scope)
    if key is None:
        raise HTTPException(401, "Invalid credentials")
    return key


def browser_origin(request, required=False):
    origin = request.headers.get("Origin")
    if origin is None and not required:
        return
    expected = request.url
    if origin != f"{expected.scheme}://{expected.netloc}":
        raise HTTPException(403, "Invalid origin")


class Sessions:
    def __init__(self, settings):
        self.signer = URLSafeTimedSerializer(settings.session_secret, salt="soup-read-v1")
        self.max_age = settings.session_seconds

    def issue(self, key_id: UUID) -> str:
        return self.signer.dumps([str(key_id), str(uuid4())])

    def read(self, request: Request) -> ReadSession | None:
        cookie = request.cookies.get(COOKIE)
        if not cookie or len(cookie) > 256:
            return None
        try:
            payload, issued_at = self.signer.loads(cookie, max_age=self.max_age, return_timestamp=True)
            if isinstance(payload, str):
                key_id = UUID(payload)
                identity = f"legacy:{key_id}:{int(issued_at.timestamp())}"
            elif isinstance(payload, list) and len(payload) == 2:
                raw_key, raw_session = payload
                key_id = UUID(raw_key)
                identity = f"session:{key_id}:{UUID(raw_session)}"
            else:
                return None
        except (BadData, ValueError, TypeError, AttributeError):
            return None
        digest = hashlib.sha256(identity.encode()).hexdigest()
        # The signer accepts the final whole second of its max age.
        return ReadSession(key_id, digest, issued_at + timedelta(seconds=self.max_age + 1))
