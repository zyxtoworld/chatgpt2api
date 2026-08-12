from __future__ import annotations

import ipaddress
import re
from urllib.parse import urlsplit, urlunsplit


_DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def _canonical_public_hostname(hostname: str) -> str:
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        try:
            ascii_hostname = hostname.removesuffix(".").encode("idna").decode("ascii").lower()
        except (UnicodeError, ValueError):
            return ""
        if not ascii_hostname or len(ascii_hostname) > 253:
            return ""
        labels = ascii_hostname.split(".")
        if any(not _DNS_LABEL_RE.fullmatch(label) for label in labels):
            return ""
        return ascii_hostname
    if "%" in hostname:
        return ""
    return address.compressed


def normalize_public_http_url(value: object) -> str:
    """Return a canonical, credential-free HTTP(S) link without a fragment."""
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if (
        not text
        or "\\" in text
        or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        return ""
    try:
        parsed = urlsplit(text)
        scheme = parsed.scheme.lower()
        if scheme not in {"http", "https"} or not parsed.netloc:
            return ""
        if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
            return ""
        port = parsed.port
        hostname = _canonical_public_hostname(parsed.hostname or "")
        if not hostname:
            return ""
        authority = f"[{hostname}]" if ":" in hostname else hostname
        if port is not None:
            authority = f"{authority}:{port}"
        return urlunsplit((scheme, authority, parsed.path, parsed.query, ""))
    except (TypeError, ValueError):
        return ""


def redact_url_credentials(value: object) -> str:
    """结构化移除 URL userinfo；无法安全解析时返回空字符串。"""
    raw_text = str(value or "")
    text = raw_text.strip()
    if not text:
        return ""
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in text):
        return ""
    try:
        parsed = urlsplit(text)
        if not parsed.scheme or not parsed.netloc:
            return ""
        _ = parsed.port
        authority = parsed.netloc.rsplit("@", 1)[-1]
        if authority.endswith(":"):
            return ""
        has_userinfo = "@" in parsed.netloc or parsed.username is not None or parsed.password is not None
        if not has_userinfo:
            return raw_text
        hostname = parsed.hostname or ""
        if not hostname:
            return ""
        if parsed.username is not None and "@" in parsed.path:
            return ""
        host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        return urlunsplit((parsed.scheme, f"[REDACTED]@{host}", parsed.path, parsed.query, parsed.fragment))
    except (TypeError, ValueError):
        return ""
