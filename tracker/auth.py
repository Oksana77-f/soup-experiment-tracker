from uuid import UUID

from fastapi import HTTPException
from itsdangerous import BadData, URLSafeTimedSerializer

from . import db

COOKIE = "soup_session"


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

    def issue(self, key_id):
        return self.signer.dumps(str(key_id))

    def key_id(self, request):
        cookie = request.cookies.get(COOKIE)
        if not cookie or len(cookie) > 256:
            return None
        try:
            return UUID(self.signer.loads(cookie, max_age=self.max_age))
        except (BadData, ValueError, TypeError, AttributeError):
            return None
