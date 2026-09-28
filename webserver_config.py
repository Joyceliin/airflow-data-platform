from __future__ import annotations

import os
from pathlib import Path


# Flask-Limiter/FAB rate limit storage.
# In production, keep this backed by Redis instead of per-process memory.
def _read_secret_file(path: str | None) -> str | None:
    if not path:
        return None
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _ratelimit_storage_uri() -> str:
    explicit_uri = os.getenv("RATELIMIT_STORAGE_URI")
    if explicit_uri:
        return explicit_uri

    redis_password = _read_secret_file(os.getenv("REDIS_PASSWORD_FILE"))
    if not redis_password:
        return "memory://"

    redis_host = os.getenv("REDIS_HOST", "redis")
    return f"redis://:{redis_password}@{redis_host}:6379/1"


RATELIMIT_STORAGE_URI = _ratelimit_storage_uri()
