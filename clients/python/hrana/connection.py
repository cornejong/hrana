"""
Connection – PEP 249 Connection implementation.

:func:`connect` is the module-level factory function.  It parses the DSN and
creates either an HTTP or WebSocket connection.
"""

from __future__ import annotations

from typing import Optional

from .dsn import parse_dsn
from .types import Config, InterfaceError, OperationalError
from .cursor import Cursor


class Connection:
    """PEP 249 Connection."""

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._impl = _make_impl(cfg)
        self._closed = False
        self._in_transaction = False

    # ─── PEP 249 factory ─────────────────────────────────────────────────────

    def cursor(self) -> Cursor:
        """Return a new :class:`Cursor` object."""
        self._check_open()
        return Cursor(self._impl)

    # ─── Transaction control ─────────────────────────────────────────────────

    def commit(self) -> None:
        """Commit the current transaction."""
        self._check_open()
        if self._in_transaction:
            cur = self.cursor()
            cur.execute("COMMIT")
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._check_open()
        if self._in_transaction:
            try:
                cur = self.cursor()
                cur.execute("ROLLBACK")
            except Exception:
                pass
            self._in_transaction = False

    def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return
        self._closed = True
        try:
            self._impl.close()
        except Exception:
            pass

    # ─── Context manager ─────────────────────────────────────────────────────

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()

    # ─── Helpers ─────────────────────────────────────────────────────────────

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("hrana: connection is closed")

    # ─── Convenience helpers (not part of DB-API 2.0 but useful) ─────────────

    def execute(self, sql: str, parameters=None) -> Cursor:
        """Shortcut: create a cursor, execute, and return it."""
        cur = self.cursor()
        cur.execute(sql, parameters)
        return cur

    def executemany(self, sql: str, seq_of_parameters) -> Cursor:
        """Shortcut: create a cursor, executemany, and return it."""
        cur = self.cursor()
        cur.executemany(sql, seq_of_parameters)
        return cur


def connect(dsn: str) -> Connection:
    """
    Open a Hrana connection from a DSN string.

    Examples::

        con = connect("http://localhost:8080?token=secret")
        con = connect("https://my-db.example.com?token=secret&version=v2")
        con = connect("ws://localhost:8080?token=secret")
        con = connect("wss://my-db.example.com?token=secret")
    """
    cfg = parse_dsn(dsn)
    return Connection(cfg)


def _make_impl(cfg: Config):
    """Instantiate the correct low-level connection based on transport."""
    if cfg.transport == "ws":
        from .ws_conn import WsConn
        return WsConn(cfg)
    # Default: HTTP pipeline.
    from .http_conn import HttpConn
    return HttpConn(cfg)
