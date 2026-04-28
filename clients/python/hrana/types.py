"""
Wire types shared by the HTTP and WebSocket transports.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any, Optional


# ─── Exceptions (PEP 249) ─────────────────────────────────────────────────────


class HranaError(Exception):
    """Base exception for all Hrana errors."""


class InterfaceError(HranaError):
    """Errors related to the database interface rather than the database itself."""


class DatabaseError(HranaError):
    """Errors related to the database."""


class OperationalError(DatabaseError):
    """Errors related to the database's operation."""


class ProgrammingError(DatabaseError):
    """Errors related to the programming of the database interface."""


# ─── Value ────────────────────────────────────────────────────────────────────

# Mapping from Python types to Hrana wire values.
# null → None
# integer → int
# float → float
# text → str
# blob → bytes


def to_wire_value(v: Any) -> dict:
    """Convert a Python value to a Hrana wire-format dict."""
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        # bool before int – bool is a subclass of int in Python
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    if isinstance(v, str):
        return {"type": "text", "value": v}
    if isinstance(v, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(bytes(v)).decode()}
    raise InterfaceError(f"hrana: unsupported parameter type {type(v)!r}")


def from_wire_value(v: dict) -> Any:
    """Convert a Hrana wire-format dict to a Python value."""
    t = v.get("type")
    if t == "null":
        return None
    if t == "integer":
        return int(v["value"])
    if t == "float":
        return float(v["value"])
    if t == "text":
        return str(v["value"])
    if t == "blob":
        return base64.b64decode(v["base64"])
    raise DatabaseError(f"hrana: unknown value type {t!r}")


# ─── Statement ────────────────────────────────────────────────────────────────


def build_stmt(sql: str, parameters: Any, want_rows: bool) -> dict:
    """
    Build a Hrana statement dict from SQL and parameters.

    Supports:
    - No parameters → positional or named args empty.
    - Sequence (list/tuple) → positional ``args``.
    - Mapping (dict) → ``named_args`` (keys must be :name style without colon).
    """
    stmt: dict = {"sql": sql, "want_rows": want_rows}

    if not parameters:
        return stmt

    if isinstance(parameters, dict):
        stmt["named_args"] = [
            {"name": k, "value": to_wire_value(v)}
            for k, v in parameters.items()
        ]
    else:
        stmt["args"] = [to_wire_value(p) for p in parameters]

    return stmt


# ─── Configuration ────────────────────────────────────────────────────────────


@dataclass
class Config:
    base_url: str
    auth_token: str
    api_version: str  # "v1", "v2", or "v3"
    transport: str    # "http" or "ws"
