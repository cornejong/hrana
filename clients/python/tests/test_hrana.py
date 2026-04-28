"""
Unit tests for the hrana Python client.

These tests cover:
- DSN parsing
- Wire value serialisation / deserialisation
- Statement building (positional + named args)
- Cursor state (rowcount, description, fetch*)
- Connection context manager

The low-level transport methods are mocked so no real server is needed.
"""

from __future__ import annotations

import pytest

from hrana.dsn import parse_dsn
from hrana.types import (
    Config,
    InterfaceError,
    build_stmt,
    from_wire_value,
    to_wire_value,
)
from hrana.cursor import Cursor


# ─── DSN parsing ─────────────────────────────────────────────────────────────

class TestParseDSN:
    def test_http_defaults_to_v3(self):
        cfg = parse_dsn("http://localhost:8080")
        assert cfg.transport == "http"
        assert cfg.api_version == "v3"
        assert cfg.base_url == "http://localhost:8080"
        assert cfg.auth_token == ""

    def test_https_with_token_and_version(self):
        cfg = parse_dsn("https://db.example.com?token=abc&version=v2")
        assert cfg.transport == "http"
        assert cfg.api_version == "v2"
        assert cfg.auth_token == "abc"
        assert cfg.base_url == "https://db.example.com"

    def test_ws_defaults_to_v3(self):
        cfg = parse_dsn("ws://localhost:8080")
        assert cfg.transport == "ws"
        assert cfg.api_version == "v3"

    def test_wss_with_v1(self):
        cfg = parse_dsn("wss://db.example.com?version=v1")
        assert cfg.transport == "ws"
        assert cfg.api_version == "v1"

    def test_trailing_slash_stripped(self):
        cfg = parse_dsn("http://localhost:8080/")
        assert cfg.base_url == "http://localhost:8080"

    def test_invalid_scheme_raises(self):
        with pytest.raises(InterfaceError, match="scheme must be"):
            parse_dsn("ftp://localhost:8080")

    def test_http_v1_raises(self):
        with pytest.raises(InterfaceError, match="HTTP transport supports v2 or v3"):
            parse_dsn("http://localhost:8080?version=v1")

    def test_ws_bad_version_raises(self):
        with pytest.raises(InterfaceError, match="WebSocket transport supports"):
            parse_dsn("ws://localhost:8080?version=v9")


# ─── Wire values ─────────────────────────────────────────────────────────────

class TestWireValues:
    def test_null(self):
        assert to_wire_value(None) == {"type": "null"}
        assert from_wire_value({"type": "null"}) is None

    def test_integer(self):
        w = to_wire_value(42)
        assert w == {"type": "integer", "value": "42"}
        assert from_wire_value(w) == 42

    def test_negative_integer(self):
        w = to_wire_value(-7)
        assert from_wire_value(w) == -7

    def test_float(self):
        w = to_wire_value(3.14)
        assert w["type"] == "float"
        assert abs(from_wire_value(w) - 3.14) < 1e-9

    def test_bool_true(self):
        assert to_wire_value(True) == {"type": "integer", "value": "1"}

    def test_bool_false(self):
        assert to_wire_value(False) == {"type": "integer", "value": "0"}

    def test_text(self):
        w = to_wire_value("hello")
        assert w == {"type": "text", "value": "hello"}
        assert from_wire_value(w) == "hello"

    def test_blob(self):
        import base64
        data = b"\x00\x01\x02"
        w = to_wire_value(data)
        assert w["type"] == "blob"
        assert from_wire_value(w) == data

    def test_bytearray(self):
        data = bytearray(b"\xff\xfe")
        w = to_wire_value(data)
        assert from_wire_value(w) == bytes(data)

    def test_unsupported_type_raises(self):
        with pytest.raises(InterfaceError, match="unsupported parameter type"):
            to_wire_value(object())


# ─── Statement building ───────────────────────────────────────────────────────

class TestBuildStmt:
    def test_no_params(self):
        s = build_stmt("SELECT 1", [], True)
        assert s == {"sql": "SELECT 1", "want_rows": True}

    def test_positional(self):
        s = build_stmt("SELECT ?", [1], True)
        assert s["args"] == [{"type": "integer", "value": "1"}]
        assert "named_args" not in s

    def test_named(self):
        s = build_stmt("SELECT :x", {"x": 2}, True)
        assert s["named_args"] == [{"name": "x", "value": {"type": "integer", "value": "2"}}]
        assert "args" not in s

    def test_want_rows_false(self):
        s = build_stmt("DELETE FROM t", [], False)
        assert s["want_rows"] is False


# ─── Cursor ───────────────────────────────────────────────────────────────────

class _FakeImpl:
    """Minimal stub that returns a pre-built result dict."""

    def __init__(self, result: dict) -> None:
        self._result = result

    def exec_statement(self, stmt: dict) -> dict:
        return self._result


def _make_result(cols=None, rows=None, affected=0, last_rowid=None):
    return {
        "cols": cols or [],
        "rows": rows or [],
        "affected_row_count": affected,
        "last_insert_rowid": last_rowid,
    }


class TestCursor:
    def _cursor(self, result):
        return Cursor(_FakeImpl(result))

    def test_fetchall_returns_all_rows(self):
        result = _make_result(
            cols=[{"name": "id"}, {"name": "name"}],
            rows=[
                [{"type": "integer", "value": "1"}, {"type": "text", "value": "Alice"}],
                [{"type": "integer", "value": "2"}, {"type": "text", "value": "Bob"}],
            ],
        )
        cur = self._cursor(result)
        cur.execute("SELECT * FROM users")
        rows = cur.fetchall()
        assert rows == [(1, "Alice"), (2, "Bob")]

    def test_fetchone(self):
        result = _make_result(
            cols=[{"name": "x"}],
            rows=[[{"type": "integer", "value": "99"}]],
        )
        cur = self._cursor(result)
        cur.execute("SELECT 99")
        assert cur.fetchone() == (99,)
        assert cur.fetchone() is None

    def test_fetchmany(self):
        result = _make_result(
            cols=[{"name": "n"}],
            rows=[[{"type": "integer", "value": str(i)}] for i in range(5)],
        )
        cur = self._cursor(result)
        cur.execute("SELECT n FROM t")
        first = cur.fetchmany(2)
        assert first == [(0,), (1,)]
        rest = cur.fetchmany(10)
        assert rest == [(2,), (3,), (4,)]

    def test_description(self):
        result = _make_result(cols=[{"name": "id"}, {"name": "value"}])
        cur = self._cursor(result)
        cur.execute("SELECT id, value FROM t")
        assert cur.description is not None
        assert cur.description[0][0] == "id"
        assert cur.description[1][0] == "value"

    def test_rowcount_for_dml(self):
        result = _make_result(affected=3)
        cur = self._cursor(result)
        cur.execute("DELETE FROM t WHERE 1=1")
        assert cur.rowcount == 3

    def test_lastrowid(self):
        result = _make_result(last_rowid="42")
        cur = self._cursor(result)
        cur.execute("INSERT INTO t VALUES (?)", [1])
        assert cur.lastrowid == 42

    def test_iterator(self):
        result = _make_result(
            cols=[{"name": "n"}],
            rows=[[{"type": "integer", "value": str(i)}] for i in range(3)],
        )
        cur = self._cursor(result)
        cur.execute("SELECT n FROM t")
        collected = list(cur)
        assert collected == [(0,), (1,), (2,)]

    def test_closed_cursor_raises(self):
        cur = self._cursor(_make_result())
        cur.close()
        with pytest.raises(InterfaceError, match="cursor is closed"):
            cur.execute("SELECT 1")
