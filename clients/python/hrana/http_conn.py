"""
HTTP pipeline connection.

Mirrors ``clients/hranago/conn.go``:
- Maintains a baton string that rotates with every pipeline call.
- Follows base_url redirects returned by the server.
- Thread-safe baton / base_url updates protected by a lock.
"""

from __future__ import annotations

import json
import threading
from typing import Any, Optional

import urllib.request
import urllib.error

from .types import Config, DatabaseError, OperationalError, build_stmt, from_wire_value


class HttpConn:
    """Single Hrana HTTP pipeline stream."""

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._lock = threading.Lock()
        self._baton: Optional[str] = None
        self._base_url: str = cfg.base_url
        self._closed: bool = False

    # ─── Public protocol ─────────────────────────────────────────────────────

    def exec_statement(self, stmt: dict) -> dict:
        """Send a single execute request through the pipeline and return the result dict."""
        resp = self._send_pipeline([{"type": "execute", "stmt": stmt}])

        results = resp.get("results", [])
        if not results:
            raise DatabaseError("hrana: empty pipeline response")

        r = results[0]
        if r.get("type") == "error":
            err = r.get("error") or {}
            raise OperationalError(f"hrana: {err.get('message', 'unknown error')}")

        response = r.get("response") or {}
        result = response.get("result")
        if result is None:
            raise DatabaseError("hrana: missing result in pipeline response")

        return result

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            has_baton = self._baton is not None

        if has_baton:
            # Best-effort: release the server-side stream.
            try:
                self._send_pipeline([{"type": "close"}])
            except Exception:
                pass

    # ─── Internal ────────────────────────────────────────────────────────────

    def _send_pipeline(self, requests: list) -> dict:
        with self._lock:
            baton = self._baton
            base_url = self._base_url

        body: dict = {"requests": requests}
        if baton is not None:
            body["baton"] = baton

        data = json.dumps(body).encode()
        endpoint = f"{base_url}/{self._cfg.api_version}/pipeline"

        req = urllib.request.Request(
            endpoint,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        if self._cfg.auth_token:
            req.add_header("Authorization", f"Bearer {self._cfg.auth_token}")

        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            try:
                err_body = json.loads(exc.read())
                msg = err_body.get("error", "")
            except Exception:
                msg = ""
            if msg:
                raise OperationalError(
                    f"hrana: server returned {exc.code}: {msg}"
                ) from exc
            raise OperationalError(
                f"hrana: server returned {exc.code}"
            ) from exc
        except urllib.error.URLError as exc:
            raise OperationalError(f"hrana: http error: {exc.reason}") from exc

        try:
            pipeline_resp = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DatabaseError(f"hrana: decode response: {exc}") from exc

        # Rotate baton and optionally follow base_url redirect.
        with self._lock:
            self._baton = pipeline_resp.get("baton")
            new_base = pipeline_resp.get("base_url")
            if new_base:
                self._base_url = new_base.rstrip("/")

        return pipeline_resp
