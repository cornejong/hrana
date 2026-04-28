"""
DSN parsing – converts a URL string into a Config.

Supported schemes:
    http://host:port?token=secret               (HTTP pipeline, v3)
    https://host:port?token=secret&version=v2   (HTTP pipeline, v2)
    ws://host:port?token=secret                 (WebSocket, v3)
    wss://host:port?token=secret&version=v1     (WebSocket, v1)
"""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse, parse_qs

from .types import Config, InterfaceError


def parse_dsn(dsn: str) -> Config:
    """Parse a Hrana DSN string and return a :class:`Config`."""
    try:
        parsed = urlparse(dsn)
    except Exception as exc:
        raise InterfaceError(f"hrana: invalid DSN {dsn!r}: {exc}") from exc

    scheme = parsed.scheme.lower()
    if scheme in ("http", "https"):
        transport = "http"
    elif scheme in ("ws", "wss"):
        transport = "ws"
    else:
        raise InterfaceError(
            f"hrana: DSN scheme must be http, https, ws, or wss, got {scheme!r}"
        )

    query = parse_qs(parsed.query, keep_blank_values=True)
    token = query.get("token", [""])[0]
    version = query.get("version", ["v3"])[0]

    if transport == "http":
        if version not in ("v2", "v3"):
            raise InterfaceError(
                f"hrana: HTTP transport supports v2 or v3, got {version!r}"
            )
    else:
        if version not in ("v1", "v2", "v3"):
            raise InterfaceError(
                f"hrana: WebSocket transport supports v1, v2, or v3, got {version!r}"
            )

    # Strip query string from base URL.
    base_url = urlunparse(parsed._replace(query="", fragment="")).rstrip("/")

    return Config(
        base_url=base_url,
        auth_token=token,
        api_version=version,
        transport=transport,
    )
