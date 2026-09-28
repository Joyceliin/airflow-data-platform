"""Runtime hardening for the Airflow API server."""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from collections import defaultdict, deque
from urllib.parse import parse_qs


def _is_airflow_api_server() -> bool:
    return any(arg in {"api-server", "fastapi-api"} for arg in sys.argv)


class AuthTokenSecurityMiddleware:
    """Rate limit and temporarily lock authentication attempts."""

    def __init__(self, app):
        self.app = app
        self.paths = {
            path.strip()
            for path in os.getenv("AIRFLOW_AUTH_RATE_LIMIT_PATHS", "/auth/token").split(",")
            if path.strip()
        }
        self.max_requests = int(os.getenv("AIRFLOW_AUTH_RATE_LIMIT_MAX_REQUESTS", "5"))
        self.window_seconds = int(os.getenv("AIRFLOW_AUTH_RATE_LIMIT_WINDOW_SECONDS", "60"))
        self.max_failures = int(os.getenv("AIRFLOW_AUTH_LOCKOUT_MAX_FAILURES", "5"))
        self.lockout_seconds = int(os.getenv("AIRFLOW_AUTH_LOCKOUT_SECONDS", "900"))
        self.request_events = defaultdict(deque)
        self.failed_attempts = {}
        self.locked_until = {}

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or scope.get("path") not in self.paths:
            await self.app(scope, receive, send)
            return

        now = time.monotonic()
        client_id = self._client_id(scope)
        events = self.request_events[client_id]
        while events and now - events[0] >= self.window_seconds:
            events.popleft()

        if len(events) >= self.max_requests:
            await self._reject(
                send,
                429,
                b'{"detail":"Too many authentication attempts"}',
                retry_after=self.window_seconds,
            )
            return
        events.append(now)

        body, messages = await self._read_body(receive)
        username = self._username_from_body(scope, body)
        if username and self.locked_until.get(username, 0) > now:
            await self._reject(send, 423, b'{"detail":"User temporarily locked"}', retry_after=self.lockout_seconds)
            return

        response_status = None

        async def replay_receive():
            if messages:
                return messages.pop(0)
            return {"type": "http.request", "body": b"", "more_body": False}

        async def capture_send(message):
            nonlocal response_status
            if message.get("type") == "http.response.start":
                response_status = message.get("status")
            await send(message)

        await self.app(scope, replay_receive, capture_send)

        if username:
            if response_status in {400, 401, 403}:
                failures = self.failed_attempts.get(username, (0, now))
                count = failures[0] + 1 if now - failures[1] < self.lockout_seconds else 1
                self.failed_attempts[username] = (count, now)
                if count >= self.max_failures:
                    self.locked_until[username] = now + self.lockout_seconds
            elif response_status and response_status < 400:
                self.failed_attempts.pop(username, None)
                self.locked_until.pop(username, None)

    @staticmethod
    async def _read_body(receive):
        body_parts = []
        messages = []
        while True:
            message = await receive()
            messages.append(message)
            if message.get("type") != "http.request":
                break
            body_parts.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        return b"".join(body_parts), messages

    @staticmethod
    def _client_id(scope) -> str:
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        forwarded_for = headers.get(b"x-forwarded-for", b"").decode("latin1").split(",", 1)[0].strip()
        if forwarded_for:
            return forwarded_for
        client = scope.get("client") or ("unknown", 0)
        return str(client[0])

    @staticmethod
    def _username_from_body(scope, body: bytes) -> str | None:
        if not body:
            return None
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_type = headers.get(b"content-type", b"").decode("latin1").lower()
        try:
            if "application/json" in content_type:
                payload = json.loads(body.decode("utf-8"))
            else:
                parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
                payload = {key: values[-1] for key, values in parsed.items()}
        except Exception:
            return None

        for key in ("username", "login", "email"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
        return None

    @staticmethod
    async def _reject(send, status: int, body: bytes, retry_after: int):
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"retry-after", str(retry_after).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def _wrap_airflow_app(app):
    if getattr(app, "_airflow_auth_hardened", False):
        return app

    if isinstance(app, str):
        if app != "airflow.api_fastapi.main:app":
            return app
        module_name, app_name = app.split(":", 1)
        module = importlib.import_module(module_name)
        app = getattr(module, app_name)

    wrapped = AuthTokenSecurityMiddleware(app)
    wrapped._airflow_auth_hardened = True
    return wrapped


if _is_airflow_api_server():
    try:
        import uvicorn
    except Exception:
        uvicorn = None

    if uvicorn is not None and not getattr(uvicorn.run, "_airflow_no_server_header", False):
        _original_uvicorn_run = uvicorn.run

        def _run_without_server_header(*args, **kwargs):
            kwargs.setdefault("server_header", False)
            if args:
                args = (_wrap_airflow_app(args[0]), *args[1:])
            elif "app" in kwargs:
                kwargs["app"] = _wrap_airflow_app(kwargs["app"])
            return _original_uvicorn_run(*args, **kwargs)

        _run_without_server_header._airflow_no_server_header = True
        uvicorn.run = _run_without_server_header
