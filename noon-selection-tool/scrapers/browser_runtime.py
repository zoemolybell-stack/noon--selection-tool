from __future__ import annotations

import socket
from urllib.parse import urlsplit, urlunsplit


def resolve_browser_cdp_endpoint(endpoint: str) -> str:
    raw = str(endpoint or "").strip()
    if not raw:
        return ""

    parts = urlsplit(raw)
    hostname = parts.hostname
    if not hostname:
        return raw

    try:
        infos = socket.getaddrinfo(hostname, parts.port or 0, family=socket.AF_INET, type=socket.SOCK_STREAM)
    except OSError:
        return raw

    if not infos:
        return raw

    ipv4 = infos[0][4][0]
    netloc = ipv4
    if parts.port:
        netloc = f"{ipv4}:{parts.port}"
    if parts.username:
        auth = parts.username
        if parts.password:
            auth = f"{auth}:{parts.password}"
        netloc = f"{auth}@{netloc}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def page_looks_access_denied(*, title: str = "", body_text: str = "") -> bool:
    title_text = " ".join(str(title or "").split()).strip().lower()
    body = " ".join(str(body_text or "").split()).strip().lower()
    haystack = f"{title_text}\n{body}"
    return (
        "access denied" in haystack
        or "you don't have permission to access" in haystack
        or "errors.edgesuite.net" in haystack
        or "akamai ghost" in haystack
        or "cloudflare tunnel error" in haystack
        or "bad gateway" in haystack
        or "error code 502" in haystack
        or "error code 503" in haystack
        or "error code 504" in haystack
        or "host error" in haystack
    )
