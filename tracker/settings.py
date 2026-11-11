import os
import secrets
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str = field(repr=False)
    session_secret: str = field(repr=False)
    static_dir: Path = Path("frontend/dist")
    session_seconds: int = 8 * 60 * 60
    poll_seconds: float = 1
    send_timeout: float = 3
    page_size: int = 100
    log_post_timings: bool = False
    log_sse_diagnostics: bool = False

    @classmethod
    def from_env(cls):
        secret = os.environ.get("SESSION_SECRET")
        if not secret:
            path = Path(os.environ.get("SESSION_SECRET_FILE", ".env-session-secret"))
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as file:
                    file.write(secrets.token_urlsafe(48))
            except FileExistsError:
                pass
            secret = path.read_text(encoding="utf-8").strip()
        if len(secret) < 32:
            raise ValueError("Set a session signing secret of at least 32 characters.")
        return cls(
            database_url=os.environ["DATABASE_URL"],
            session_secret=secret,
            log_post_timings=os.environ.get("SOUP_LOG_POST_TIMINGS") == "1",
            log_sse_diagnostics=os.environ.get("SOUP_LOG_SSE_DIAGNOSTICS") == "1",
        )
