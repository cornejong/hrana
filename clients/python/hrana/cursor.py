"""
Cursor – PEP 249 Cursor implementation.

Wraps either an :class:`HttpConn` or :class:`WsConn` and exposes the
standard ``execute``, ``executemany``, ``fetchone``, ``fetchmany``,
``fetchall`` interface.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence, Union

from .types import (
    InterfaceError,
    DatabaseError,
    build_stmt,
    from_wire_value,
)


class Cursor:
    """PEP 249 Cursor."""

    arraysize: int = 1

    def __init__(self, conn_impl: Any) -> None:
        # conn_impl is HttpConn or WsConn
        self._conn = conn_impl
        self._description: Optional[list] = None
        self._rows: list[tuple] = []
        self._pos: int = 0
        self._rowcount: int = -1
        self._lastrowid: Optional[int] = None
        self._closed: bool = False

    # ─── PEP 249 attributes ──────────────────────────────────────────────────

    @property
    def description(self) -> Optional[list]:
        """
        Read-only sequence of 7-item sequences:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> Optional[int]:
        return self._lastrowid

    # ─── Execution ───────────────────────────────────────────────────────────

    def execute(self, operation: str, parameters: Any = None) -> "Cursor":
        """Execute a database operation (query or command)."""
        self._check_open()
        want_rows = True  # always request rows; we'll ignore them for DML
        stmt = build_stmt(operation, parameters or [], want_rows)
        result = self._conn.exec_statement(stmt)
        self._apply_result(result)
        return self

    def executemany(self, operation: str, seq_of_parameters: Sequence) -> "Cursor":
        """Execute a database operation against all parameter sequences."""
        self._check_open()
        for params in seq_of_parameters:
            self.execute(operation, params)
        return self

    def callproc(self, procname: str, parameters: Any = None) -> None:
        raise InterfaceError("hrana: stored procedures are not supported")

    # ─── Fetch ───────────────────────────────────────────────────────────────

    def fetchone(self) -> Optional[tuple]:
        """Fetch the next row, or None if exhausted."""
        self._check_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> list[tuple]:
        """Fetch up to *size* rows (default: :attr:`arraysize`)."""
        self._check_open()
        n = size if size is not None else self.arraysize
        rows = self._rows[self._pos: self._pos + n]
        self._pos += len(rows)
        return rows

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows."""
        self._check_open()
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

    def setinputsizes(self, sizes: Any) -> None:  # noqa: D401
        pass  # no-op per PEP 249

    def setoutputsize(self, size: Any, column: Any = None) -> None:  # noqa: D401
        pass  # no-op per PEP 249

    def close(self) -> None:
        self._closed = True
        self._rows = []
        self._description = None

    # ─── Iterator protocol ───────────────────────────────────────────────────

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> tuple:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    # ─── Internal helpers ────────────────────────────────────────────────────

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("hrana: cursor is closed")

    def _apply_result(self, result: dict) -> None:
        """Populate internal state from a Hrana statement result dict."""
        cols = result.get("cols", [])
        raw_rows = result.get("rows", [])
        affected = result.get("affected_row_count", -1)
        last_rowid = result.get("last_insert_rowid")

        # Build PEP 249 description tuples.
        if cols:
            self._description = [
                (
                    c.get("name") or "",   # name
                    None,                  # type_code
                    None,                  # display_size
                    None,                  # internal_size
                    None,                  # precision
                    None,                  # scale
                    True,                  # null_ok
                )
                for c in cols
            ]
        else:
            self._description = None

        # Convert rows.
        self._rows = [
            tuple(from_wire_value(v) for v in row)
            for row in raw_rows
        ]
        self._pos = 0

        self._rowcount = int(affected) if affected >= 0 else -1
        self._lastrowid = int(last_rowid) if last_rowid is not None else None
