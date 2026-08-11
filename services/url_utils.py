from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit


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
