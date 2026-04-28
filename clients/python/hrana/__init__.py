"""
hrana – Python DB-API 2.0 client for the Hrana protocol.

Usage::

    import hrana

    con = hrana.connect("http://localhost:8080?token=secret")
    cur = con.cursor()
    cur.execute("SELECT 1")
    print(cur.fetchall())
    con.close()

WebSocket transport::

    con = hrana.connect("ws://localhost:8080?token=secret")

"""

from .connection import connect, Connection
from .types import HranaError

__all__ = ["connect", "Connection", "HranaError"]

# PEP 249 required module globals
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "qmark"
