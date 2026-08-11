from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import re
import socket
from pathlib import PurePosixPath
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

from curl_cffi import CurlOpt, requests
from fastapi import HTTPException
from services.image_payload import (
    DEFAULT_MAX_IMAGE_PIXELS,
    IMAGE_MIME_FORMATS,
    ImagePayloadError,
    validate_image_payload,
)


DEFAULT_REMOTE_IMAGE_TIMEOUT_SECONDS = 30
DEFAULT_REMOTE_IMAGE_MAX_REDIRECTS = 3
DEFAULT_REMOTE_IMAGE_MAX_PIXELS = DEFAULT_MAX_IMAGE_PIXELS

_REMOTE_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
_REMOTE_MIME_FORMATS = IMAGE_MIME_FORMATS
_DNS_LABEL_RE = re.compile(r"^[a-z0-9-]+$")


@dataclass(frozen=True)
class _RemoteURL:
    source: str
    request_url: str
    scheme: str
    resolve_hostname: str
    port: int
    path: str


def _remote_error(message: str) -> HTTPException:
    return HTTPException(status_code=400, detail={"error": message})


def _extension_from_mime(mime_type: str) -> str:
    subtype = mime_type.split("/", 1)[1].split("+", 1)[0] if "/" in mime_type else "png"
    if subtype == "jpeg":
        return "jpg"
    return re.sub(r"[^a-z0-9]+", "", subtype.lower()) or "png"


def _safe_filename(name: str, mime_type: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    if not cleaned:
        cleaned = fallback
    if "." not in cleaned:
        cleaned = f"{cleaned}.{_extension_from_mime(mime_type)}"
    return cleaned


def _parse_remote_url(source: str) -> _RemoteURL:
    """解析远程图片 URL；只接受无 userinfo 的规范 HTTP(S) authority。"""
    if not source or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in source):
        raise _remote_error("image_url must be an http or https URL")
    try:
        parsed = urlsplit(source)
        scheme = parsed.scheme.lower()
        if scheme not in {"http", "https"} or not parsed.netloc or parsed.fragment:
            raise _remote_error("image_url must be an http or https URL")
        if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
            raise _remote_error("image_url must not contain credentials")
        if parsed.netloc.endswith(":") or (not parsed.netloc.startswith("[") and parsed.netloc.count(":") > 1):
            raise _remote_error("image_url must be an http or https URL")
        hostname = parsed.hostname
        if not hostname:
            raise _remote_error("image_url must be an http or https URL")
        parsed_port = parsed.port
        port = parsed_port if parsed_port is not None else (443 if scheme == "https" else 80)
        if not 1 <= port <= 65535:
            raise _remote_error("image_url must be an http or https URL")
    except HTTPException:
        raise
    except (TypeError, ValueError, UnicodeError) as exc:
        raise _remote_error("image_url must be an http or https URL") from exc

    try:
        literal = ipaddress.ip_address(hostname)
    except ValueError:
        raw_hostname = hostname
        if raw_hostname.endswith("."):
            raw_hostname = raw_hostname[:-1]
            if raw_hostname.endswith("."):
                raise _remote_error("image_url must be an http or https URL")
        try:
            resolve_hostname = raw_hostname.encode("idna").decode("ascii").lower()
        except (UnicodeError, UnicodeEncodeError):
            raise _remote_error("image_url must be an http or https URL")
        labels = resolve_hostname.split(".")
        if (
            not resolve_hostname
            or len(resolve_hostname) > 253
            or any(
                not 1 <= len(label) <= 63
                or not _DNS_LABEL_RE.fullmatch(label)
                or not label[0].isalnum()
                or not label[-1].isalnum()
                for label in labels
            )
        ):
            raise _remote_error("image_url must be an http or https URL")
        is_ipv6 = False
    else:
        resolve_hostname = str(literal).lower()
        is_ipv6 = isinstance(literal, ipaddress.IPv6Address)
    if not resolve_hostname or resolve_hostname == "localhost" or resolve_hostname.endswith(".localhost"):
        raise _remote_error("image_url target is not allowed")
    if "%" in resolve_hostname:
        raise _remote_error("image_url target is not allowed")
    authority = f"[{resolve_hostname}]" if is_ipv6 else resolve_hostname
    default_port = 443 if scheme == "https" else 80
    if port != default_port:
        authority = f"{authority}:{port}"
    return _RemoteURL(
        source=source,
        request_url=urlunsplit((scheme, authority, parsed.path, parsed.query, "")),
        scheme=scheme,
        resolve_hostname=resolve_hostname,
        port=port,
        path=parsed.path,
    )


def _resolve_remote_addresses(remote: _RemoteURL) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]:
    """解析全部 A/AAAA，并在连接前拒绝任何非公网地址。"""
    try:
        records = socket.getaddrinfo(
            remote.resolve_hostname,
            remote.port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except (OSError, TypeError, ValueError) as exc:
        raise _remote_error("image_url fetch failed") from exc

    addresses: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    seen: set[tuple[int, str]] = set()
    for family, _socktype, _proto, _canonname, sockaddr in records:
        if family not in {socket.AF_INET, socket.AF_INET6} or not sockaddr:
            continue
        try:
            address = ipaddress.ip_address(str(sockaddr[0]).split("%", 1)[0])
        except ValueError as exc:
            raise _remote_error("image_url fetch failed") from exc
        checked = getattr(address, "ipv4_mapped", None) or address
        if (
            not checked.is_global
            or checked.is_loopback
            or checked.is_private
            or checked.is_link_local
            or checked.is_multicast
            or checked.is_reserved
            or checked.is_unspecified
        ):
            raise _remote_error("image_url target is not allowed")
        key = (family, str(address))
        if key not in seen:
            seen.add(key)
            addresses.append(address)
    if not addresses:
        raise _remote_error("image_url fetch failed")
    return tuple(addresses)


def _curl_resolve_entries(
    remote: _RemoteURL,
    addresses: tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...],
) -> list[str]:
    entries: list[str] = []
    for address in addresses:
        rendered = str(address)
        if isinstance(address, ipaddress.IPv6Address):
            rendered = f"[{rendered}]"
        entries.append(f"{remote.resolve_hostname}:{remote.port}:{rendered}")
    return entries


def _open_remote_response(
    remote: _RemoteURL,
    addresses: tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...],
    *,
    timeout_seconds: float,
) -> tuple[requests.Session, requests.Response]:
    """直连并用 CURLOPT_RESOLVE 固定到已审过的地址；不继承应用或环境代理。"""
    session = requests.Session(
        verify=True,
        trust_env=False,
        curl_options={
            CurlOpt.RESOLVE: _curl_resolve_entries(remote, addresses),
        },
    )
    try:
        response = session.get(
            remote.request_url,
            headers={"Accept": "image/*", "User-Agent": "chatgpt2api image fetcher"},
            timeout=timeout_seconds,
            allow_redirects=False,
            stream=True,
        )
    except Exception:
        session.close()
        raise
    return session, response


def _response_content_type(response: requests.Response) -> str:
    content_type = str(response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    if content_type not in _REMOTE_MIME_FORMATS:
        raise _remote_error("image_url must point to a supported image")
    return content_type


def _read_remote_body(response: requests.Response, max_bytes: int) -> bytes:
    """流式读取响应，Content-Length 与实际字节数都受同一上限约束。"""
    content_length = str(response.headers.get("content-length") or "").strip()
    if content_length:
        try:
            declared_size = int(content_length)
        except (TypeError, ValueError) as exc:
            raise _remote_error("image_url fetch failed") from exc
        if declared_size < 0 or declared_size > max_bytes:
            raise _remote_error("image_url exceeds size limit")
    data = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise _remote_error("image_url fetch failed")
        data.extend(chunk)
        if len(data) > max_bytes:
            raise _remote_error("image_url exceeds size limit")
    if not data:
        raise _remote_error("image_url returned empty content")
    return bytes(data)


def _validate_remote_image(data: bytes, mime_type: str, max_pixels: int) -> None:
    try:
        validate_image_payload(data, mime_type, max_pixels=max_pixels)
    except ImagePayloadError as exc:
        raise _remote_error("image_url image data is invalid") from exc


def _filename_from_url(parsed_path: str, mime_type: str) -> str:
    raw_name = PurePosixPath(unquote(parsed_path)).name
    return _safe_filename(raw_name, mime_type, "image_url")


def _download_remote_image(
    source: str,
    *,
    max_bytes: int,
    timeout_seconds: float,
    max_redirects: int,
    max_pixels: int,
) -> tuple[bytes, str, str]:
    current = source
    previous_scheme = ""
    for redirect_count in range(max_redirects + 1):
        remote = _parse_remote_url(current)
        if previous_scheme == "https" and remote.scheme == "http":
            raise _remote_error("image_url redirect is not allowed")
        previous_scheme = remote.scheme
        addresses = _resolve_remote_addresses(remote)
        session = None
        response = None
        try:
            session, response = _open_remote_response(remote, addresses, timeout_seconds=timeout_seconds)
            status_code = int(response.status_code)
            if status_code in _REMOTE_REDIRECT_STATUS_CODES:
                if redirect_count >= max_redirects:
                    raise _remote_error("image_url redirect limit exceeded")
                location = str(response.headers.get("location") or "").strip()
                if not location:
                    raise _remote_error("image_url fetch failed")
                current = urljoin(current, location)
                continue
            if not 200 <= status_code < 300:
                raise _remote_error("image_url fetch failed")
            mime_type = _response_content_type(response)
            data = _read_remote_body(response, max_bytes)
            _validate_remote_image(data, mime_type, max_pixels)
            return data, _filename_from_url(remote.path, mime_type), mime_type
        finally:
            if response is not None:
                response.close()
            if session is not None:
                session.close()
    raise _remote_error("image_url redirect limit exceeded")


def download_remote_image(
    source: str,
    *,
    max_bytes: int,
    timeout_seconds: float = DEFAULT_REMOTE_IMAGE_TIMEOUT_SECONDS,
    max_redirects: int = DEFAULT_REMOTE_IMAGE_MAX_REDIRECTS,
    max_pixels: int = DEFAULT_REMOTE_IMAGE_MAX_PIXELS,
) -> tuple[bytes, str, str]:
    """下载并验证一张远程图片；所有失败均只返回固定安全错误。"""
    try:
        return _download_remote_image(
            source,
            max_bytes=max_bytes,
            timeout_seconds=timeout_seconds,
            max_redirects=max_redirects,
            max_pixels=max_pixels,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise _remote_error("image_url fetch failed") from exc
