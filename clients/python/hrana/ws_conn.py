"""
WebSocket connection.

Mirrors ``clients/hranago/ws_conn.go``:
1. Dials the server and negotiates the hrana subprotocol.
2. Sends a ``hello`` message with optional JWT.
3. Reads ``hello_ok`` / ``hello_error`` synchronously.
4. Opens stream 0.
5. Starts a background reader thread that routes responses to waiting callers.
6. Each request is assigned a monotonically increasing ``request_id``.

Requires the ``websocket-client`` package (``pip install websocket-client``).
"""

from __future__ import annotations

import json
import threading
from typing import Any, Optional

from .types import Config, DatabaseError, OperationalError, InterfaceError

try:
    import websocket  # type: ignore[import]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "websocket-client is required for WebSocket transport: "
        "pip install websocket-client"
    ) from exc


class WsConn:
    """Single Hrana WebSocket stream (stream_id = 0)."""

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._lock = threading.Lock()
        self._req_id = 0
        self._pending: dict[int, threading.Event] = {}
        self._results: dict[int, Any] = {}
        self._closed = False
        self._close_err: Optional[Exception] = None

        subproto = "hrana" + cfg.api_version[1:]  # "v3" → "hrana3"

        headers = {}
        if cfg.auth_token:
            headers["Authorization"] = f"Bearer {cfg.auth_token}"

        self._ws = websocket.WebSocket()
        self._ws.connect(
            cfg.base_url,
            subprotocols=[subproto],
            header=headers,
        )

        # Send hello.
        hello: dict = {"type": "hello"}
        if cfg.auth_token:
            hello["jwt"] = cfg.auth_token
        self._ws_send(hello)

        # Read hello_ok / hello_error synchronously.
        raw = self._ws.recv()
        msg = json.loads(raw)
        if msg.get("type") == "hello_error":
            err = msg.get("error") or {}
            self._ws.close()
            raise OperationalError(
                f"hrana: ws auth rejected: {err.get('message', 'unknown')}"
            )
        if msg.get("type") != "hello_ok":
            self._ws.close()
            raise DatabaseError(
                f"hrana: ws unexpected message {msg.get('type')!r} during hello"
            )

        # Start background reader.
        reader = threading.Thread(target=self._read_loop, daemon=True)
        reader.start()

        # Open stream 0.
        self._send_request({"type": "open_stream", "stream_id": 0})

    # ─── Public protocol ─────────────────────────────────────────────────────

    def exec_statement(self, stmt: dict) -> dict:
        """Send a single execute request over stream 0 and return the result dict."""
        resp = self._send_request({
            "type": "execute",
            "stream_id": 0,
            "stmt": stmt,
        })
        return resp.get("result", {})

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True

        try:
            self._send_request({"type": "close_stream", "stream_id": 0})
        except Exception:
            pass

        try:
            self._ws.close()
        except Exception:
            pass

    # ─── Internal ────────────────────────────────────────────────────────────

    def _send_request(self, payload: dict) -> Any:
        with self._lock:
            if self._closed:
                raise OperationalError("hrana: ws connection is closed")
            req_id = self._req_id + 1
            self._req_id = req_id
            ev = threading.Event()
            self._pending[req_id] = ev

        msg = {
            "type": "request",
            "request_id": req_id,
            "request": payload,
        }
        self._ws_send(msg)

        ev.wait()

        with self._lock:
            result = self._results.pop(req_id, None)

        if isinstance(result, Exception):
            raise result
        return result

    def _ws_send(self, obj: dict) -> None:
        data = json.dumps(obj)
        self._ws.send(data)

    def _read_loop(self) -> None:
        """Background thread: read responses and dispatch to waiting callers."""
        try:
            while True:
                try:
                    raw = self._ws.recv()
                except Exception as exc:
                    self._fail_all(exc)
                    return

                if not raw:
                    self._fail_all(OperationalError("hrana: ws connection closed"))
                    return

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError as exc:
                    self._fail_all(DatabaseError(f"hrana: ws decode: {exc}"))
                    return

                msg_type = msg.get("type")
                req_id = msg.get("request_id")

                if msg_type == "response_ok":
                    self._resolve(req_id, msg.get("response", {}))
                elif msg_type == "response_error":
                    err = msg.get("error") or {}
                    self._resolve(
                        req_id,
                        OperationalError(
                            f"hrana: ws error: {err.get('message', 'unknown')}"
                        ),
                    )
                # Ignore server-initiated messages (e.g. "hello_ok" already handled).
        except Exception as exc:
            self._fail_all(exc)

    def _resolve(self, req_id: Optional[int], value: Any) -> None:
        if req_id is None:
            return
        with self._lock:
            ev = self._pending.pop(req_id, None)
            self._results[req_id] = value
        if ev is not None:
            ev.set()

    def _fail_all(self, exc: Exception) -> None:
        with self._lock:
            pending = dict(self._pending)
            self._pending.clear()
            for req_id in pending:
                self._results[req_id] = exc
        for ev in pending.values():
            ev.set()
