"""Sub2API integration for browsing and importing ChatGPT OAuth accounts from a sub2api admin."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from curl_cffi.requests import Session

from services.account_service import account_service
from services.config import DATA_DIR, parse_public_url
from services.protocol.error_response import (
    ImportJobActiveError,
    PublicSafeValueError,
    canonicalize_import_job_errors,
    exception_log_message,
    validate_import_job_errors,
)
from services.remote_response import parse_json_response
from services.secure_file import atomic_write_bytes, read_checked_file_bytes
from services.storage.base import (
    StorageConflictError,
    StorageDataError,
    canonical_path_write_lock,
    make_storage_snapshot,
)
from services.task_executor import reserve_background_task, run_with_timeout
from utils.log import logger


SUB2API_CONFIG_FILE = DATA_DIR / "sub2api_config.json"

# Cached JWT per server to avoid re-login on every list/import call.
# Token lifetime on sub2api defaults to 24h; we refresh 5 min before expiry.
_TOKEN_REFRESH_SKEW = 5 * 60
SUB2API_REMOTE_BROWSE_TIMEOUT_SECS = 90.0
SUB2API_IMPORT_TIMEOUT_SECS = 30 * 60.0
SUB2API_MAX_REMOTE_ITEMS = 5000
SUB2API_MAX_REMOTE_PAGES = 25
_SUB2API_PUBLIC_TEXT_MAX_LENGTH = 256


def _remaining_import_timeout(deadline: float | None, maximum: float) -> float:
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Sub2API import timed out")
    return min(maximum, remaining)


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: object) -> str:
    return str(value or "").strip()


def _clean_remote_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _clean_public_text(value: object) -> str:
    text = _clean_remote_text(value)
    return text if len(text) <= _SUB2API_PUBLIC_TEXT_MAX_LENGTH else ""


def _require_base_url(value: object) -> str:
    normalized = parse_public_url(value)
    if not normalized:
        raise PublicSafeValueError(
            "Sub2API base URL must use http or https without credentials, query, or fragment"
        )
    return normalized


def _response_json(
    response,
    operation: str,
    *,
    invalidate_server_id: str = "",
) -> object:
    if invalidate_server_id and getattr(response, "status_code", None) in {401, 403}:
        _invalidate_token_cache(invalidate_server_id)
    return parse_json_response(response, operation)


def _clean_remote_id(value: object) -> str:
    if isinstance(value, str):
        text = value.strip()
        return text if len(text) <= _SUB2API_PUBLIC_TEXT_MAX_LENGTH else ""
    if type(value) is int and value >= 0:
        text = str(value)
        return text if len(text) <= _SUB2API_PUBLIC_TEXT_MAX_LENGTH else ""
    return ""


def _parse_nonnegative_int(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("invalid non-negative integer")
    return value


def _remaining_timeout(deadline: float, maximum: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("sub2api remote browse timed out")
    return min(maximum, remaining)


def _validate_remote_page(data: list, total: int | None, current_count: int, page: int) -> None:
    if page > SUB2API_MAX_REMOTE_PAGES:
        raise ValueError("remote item limit exceeded")
    received_count = current_count + len(data)
    if total is not None and (total < received_count or (not data and received_count < total)):
        raise ValueError("invalid sub2api pagination payload")
    if (total is not None and total > SUB2API_MAX_REMOTE_ITEMS) or received_count > SUB2API_MAX_REMOTE_ITEMS:
        raise ValueError("remote item limit exceeded")


def _merge_declared_total(
    total: int | None,
    expected_total: int | None,
    total_seen: bool,
    page: int,
) -> tuple[int | None, bool]:
    """Keep one total snapshot for a single paginated read."""
    if total_seen:
        if total is None or total != expected_total:
            raise ValueError("invalid sub2api pagination payload")
        return expected_total, True
    if total is not None:
        if page != 1:
            raise ValueError("invalid sub2api pagination payload")
        return total, True
    return None, False


def _normalize_import_job(raw: object, *, fail_unfinished: bool) -> dict | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise StorageDataError()

    raw_status = raw.get("status")
    if not isinstance(raw_status, str) or not raw_status.strip():
        raise StorageDataError()
    status = raw_status.strip()
    if status not in {"pending", "running", "completed", "failed"}:
        raise StorageDataError()
    if fail_unfinished and status in {"pending", "running"}:
        status = "failed"

    counters: dict[str, int] = {}
    for name in ("total", "completed", "added", "skipped", "refreshed", "failed"):
        value = raw.get(name, 0)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise StorageDataError()
        counters[name] = value

    text_fields: dict[str, str] = {}
    for name in ("job_id", "created_at", "updated_at"):
        value = raw.get(name)
        if not isinstance(value, str) or not value.strip():
            raise StorageDataError()
        text_fields[name] = value.strip()

    if "errors" not in raw:
        raise StorageDataError()
    raw_errors = validate_import_job_errors(raw["errors"])
    if (
        counters["completed"] > counters["total"]
        or counters["failed"] > counters["completed"]
        or counters["added"] + counters["skipped"] > counters["total"]
        or counters["refreshed"] > counters["total"]
    ):
        raise StorageDataError()
    return {
        "job_id": text_fields["job_id"],
        "status": status,
        "created_at": text_fields["created_at"],
        "updated_at": text_fields["updated_at"],
        **counters,
        "errors": raw_errors,
    }


def _normalize_server(raw: dict, *, fail_unfinished: bool = True) -> dict:
    if not isinstance(raw, dict):
        raise StorageDataError()

    values: dict[str, str] = {}
    for name in ("id", "name", "base_url", "email", "password", "api_key", "group_id"):
        if name not in raw:
            values[name] = ""
            continue
        value = raw[name]
        if not isinstance(value, str):
            raise StorageDataError()
        values[name] = value.strip()

    if not values["id"]:
        raise StorageDataError()

    return {
        "id": values["id"],
        "name": values["name"],
        "base_url": values["base_url"],
        "email": values["email"],
        "password": values["password"],
        "api_key": values["api_key"],
        "group_id": values["group_id"],
        "import_job": _normalize_import_job(raw.get("import_job"), fail_unfinished=fail_unfinished),
    }


class Sub2APIConfig:
    def __init__(self, store_file: Path):
        self._store_file = store_file
        self._lock = Lock()
        self._path_write_lock = canonical_path_write_lock(store_file)
        self._servers: list[dict] = self._load(fail_unfinished=False)
        self._snapshot_revision = make_storage_snapshot(self._servers).revision
        recovered_servers: list[dict] = []
        recovered = False
        for server in self._servers:
            next_server = dict(server)
            import_job = server.get("import_job")
            if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=True)
                recovered = True
            recovered_servers.append(next_server)
        if recovered:
            self._commit_locked(recovered_servers)

    def _load(self, *, fail_unfinished: bool = True) -> list[dict]:
        if not self._store_file.exists():
            return []
        try:
            raw = json.loads(read_checked_file_bytes(self._store_file, self._store_file.parent).decode("utf-8"))
            if not isinstance(raw, list):
                raise StorageDataError()
            servers: list[dict] = []
            seen_ids: set[str] = set()
            for item in raw:
                if not isinstance(item, dict):
                    raise StorageDataError()
                server = _normalize_server(item, fail_unfinished=fail_unfinished)
                if not server["base_url"] or not parse_public_url(server["base_url"]) or server["id"] in seen_ids:
                    raise StorageDataError()
                seen_ids.add(server["id"])
                servers.append(server)
            return servers
        except StorageDataError:
            raise
        except Exception as exc:
            raise StorageDataError() from exc

    def _reload_locked(self) -> None:
        servers = self._load(fail_unfinished=False)
        self._servers = servers
        self._snapshot_revision = make_storage_snapshot(servers).revision

    def _commit_locked(self, servers: list[dict]) -> None:
        previous = self._servers
        self._servers = servers
        try:
            self._save()
        except Exception:
            self._servers = previous
            raise

    def _save(self) -> None:
        parent = self._store_file.parent
        try:
            parent_stat = parent.stat()
            expected_root_identity = (parent_stat.st_dev, parent_stat.st_ino)
        except FileNotFoundError:
            expected_root_identity = None
        with self._path_write_lock:
            parent.mkdir(parents=True, exist_ok=True)
            if expected_root_identity is None:
                parent_stat = parent.stat()
                expected_root_identity = (parent_stat.st_dev, parent_stat.st_ino)
            current_revision = make_storage_snapshot(self._load(fail_unfinished=False)).revision
            if current_revision != self._snapshot_revision:
                raise StorageConflictError()
            payload = (json.dumps(self._servers, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
            atomic_write_bytes(
                self._store_file,
                parent,
                payload,
                expected_root_identity=expected_root_identity,
            )
            self._snapshot_revision = make_storage_snapshot(self._servers).revision

    def list_servers(self) -> list[dict]:
        with self._lock:
            self._reload_locked()
            return [dict(server) for server in self._servers]

    def get_server(self, server_id: str) -> dict | None:
        with self._lock:
            self._reload_locked()
            for server in self._servers:
                if server["id"] == server_id:
                    return dict(server)
        return None

    def add_server(
        self,
        *,
        name: str,
        base_url: str,
        email: str,
        password: str,
        api_key: str,
        group_id: str = "",
    ) -> dict:
        server = _normalize_server({
            "id": _new_id(),
            "name": name,
            "base_url": _require_base_url(base_url),
            "email": email,
            "password": password,
            "api_key": api_key,
            "group_id": group_id,
        })
        with self._lock:
            self._reload_locked()
            self._commit_locked([*self._servers, server])
        _invalidate_token_cache(server["id"])
        return dict(server)

    def update_server(self, server_id: str, updates: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                merged = {**server, **{k: v for k, v in updates.items() if v is not None}, "id": server_id}
                if "base_url" in updates and updates["base_url"] is not None:
                    merged["base_url"] = _require_base_url(updates["base_url"])
                next_servers = list(self._servers)
                next_servers[index] = _normalize_server(merged)
                self._commit_locked(next_servers)
                result = dict(next_servers[index])
                break
            else:
                return None
        _invalidate_token_cache(server_id)
        return result

    def delete_server(self, server_id: str) -> bool:
        with self._lock:
            self._reload_locked()
            for server in self._servers:
                if server["id"] == server_id:
                    import_job = server.get("import_job")
                    if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                        raise ImportJobActiveError("import is already running")
                    break
            before = len(self._servers)
            next_servers = [server for server in self._servers if server["id"] != server_id]
            removed = len(next_servers) < before
            if removed:
                self._commit_locked(next_servers)
        if removed:
            _invalidate_token_cache(server_id)
        return removed

    def set_import_job(self, server_id: str, import_job: dict | None) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                next_server = dict(server)
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=False)
                next_servers = list(self._servers)
                next_servers[index] = next_server
                self._commit_locked(next_servers)
                return dict(next_server)
        return None

    def begin_import_job(self, server_id: str, import_job: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                current_job = server.get("import_job")
                if isinstance(current_job, dict) and current_job.get("status") in {"pending", "running"}:
                    raise ImportJobActiveError("import is already running")
                next_server = dict(server)
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=False)
                next_servers = list(self._servers)
                next_servers[index] = next_server
                self._commit_locked(next_servers)
                return dict(next_server)
        return None

    def get_import_job(self, server_id: str) -> dict | None:
        with self._lock:
            self._reload_locked()
            for server in self._servers:
                if server["id"] == server_id:
                    job = server.get("import_job")
                    return dict(job) if isinstance(job, dict) else None
        return None


# Per-server cached access token: {server_id: (jwt, expires_at_epoch, auth_generation)}
_token_cache: dict[str, tuple[str, float, int]] = {}
_token_cache_generations: dict[str, int] = {}
_token_cache_lock = Lock()


def _invalidate_token_cache(server_id: str) -> None:
    if not server_id:
        return
    with _token_cache_lock:
        _token_cache.pop(server_id, None)
        _token_cache_generations[server_id] = _token_cache_generations.get(server_id, 0) + 1


def _login(
    base_url: str,
    email: str,
    password: str,
    *,
    deadline: float | None = None,
) -> tuple[str, float]:
    url = f"{base_url.rstrip('/')}/api/v1/auth/login"
    session = Session(verify=True)
    try:
        response = session.post(
            url,
            json={"email": email, "password": password},
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=_remaining_timeout(deadline, 30.0) if deadline is not None else 30.0,
            stream=True,
        )
        payload = _response_json(response, "sub2api login failed")
    finally:
        session.close()

    if deadline is not None and time.monotonic() >= deadline:
        raise RuntimeError("sub2api browse timed out")

    body = _unwrap_envelope(payload)
    if not isinstance(body, dict):
        raise RuntimeError("sub2api login payload is invalid")

    token = _clean_remote_text(body.get("access_token"))
    if not token:
        raise RuntimeError("sub2api login did not return access_token")

    raw_expires_in = body.get("expires_in")
    if raw_expires_in is None:
        expires_in = 3600
    else:
        try:
            expires_in = _parse_nonnegative_int(raw_expires_in)
        except ValueError as exc:
            raise RuntimeError("sub2api login payload is invalid") from exc
    expires_at = time.time() + max(60, expires_in) - _TOKEN_REFRESH_SKEW
    return token, expires_at


def _auth_headers(server: dict, *, deadline: float | None = None) -> dict[str, str]:
    api_key = _clean(server.get("api_key"))
    if api_key:
        return {"x-api-key": api_key, "Accept": "application/json"}

    email = _clean(server.get("email"))
    password = _clean(server.get("password"))
    if not email or not password:
        raise RuntimeError("sub2api server requires email+password or api_key")

    server_id = _clean(server.get("id"))
    base_url = _clean(server.get("base_url"))

    with _token_cache_lock:
        generation = _token_cache_generations.get(server_id, 0)
        cached = _token_cache.get(server_id)
        if cached and cached[2] == generation and cached[1] > time.time():
            return {"Authorization": f"Bearer {cached[0]}", "Accept": "application/json"}

    token, expires_at = _login(base_url, email, password, deadline=deadline)
    with _token_cache_lock:
        if _token_cache_generations.get(server_id, 0) == generation:
            _token_cache[server_id] = (token, expires_at, generation)
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _password_auth_cache_id(server: dict) -> str:
    if _clean(server.get("api_key")):
        return ""
    return _clean(server.get("id"))


def _extract_access_token(credentials: object) -> str:
    if not isinstance(credentials, dict):
        return ""
    for key in ("access_token", "accessToken", "token"):
        value = _clean_remote_text(credentials.get(key))
        if value:
            return value
    return ""


def _build_codex_import_value(account: object, credentials: object, token: str) -> str | dict:
    """Keep account metadata that is not recoverable from an opaque token."""
    account = account if isinstance(account, dict) else {}
    credentials = credentials if isinstance(credentials, dict) else {}
    item: dict[str, str] = {"access_token": token, "source_type": "codex"}
    for field in ("plan_type", "type", "refresh_token", "id_token"):
        value = _clean_remote_text(credentials.get(field))
        if not value:
            value = _clean_remote_text(account.get(field))
        if value:
            item[field] = value
    return item if len(item) > 2 else token


def _unwrap_envelope(payload: object) -> object:
    """Peel sub2api's `{code, message, data}` envelope, returning the inner `data` field
    when present. Also handles unwrapped responses from older/alt versions."""
    if isinstance(payload, dict) and "data" in payload and "code" in payload:
        return payload.get("data")
    return payload


def _extract_paged_items(payload: object) -> tuple[list, int | None]:
    """Return (items, total) from a paginated sub2api response.

    Handles both the wrapped shape `{code,data:{items,total,...}}` and a few looser
    variants (`{data:[...]}`, `[...]`, `{items:[...],total:N}`)."""
    inner = _unwrap_envelope(payload)
    if isinstance(inner, list):
        return inner, None
    if isinstance(inner, dict):
        for key in ("items", "data", "list"):
            value = inner.get(key)
            if isinstance(value, list):
                raw_total = inner.get("total")
                if raw_total is None:
                    total = None
                elif type(raw_total) is int and raw_total >= len(value):
                    total = raw_total
                else:
                    raise ValueError("invalid sub2api pagination payload")
                return value, total
    raise ValueError("invalid sub2api pagination payload")


def list_remote_accounts(server: dict) -> list[dict]:
    """Return a flat list of OpenAI OAuth accounts from a sub2api server."""
    base_url = _clean(server.get("base_url"))
    if not base_url:
        return []

    deadline = time.monotonic() + SUB2API_REMOTE_BROWSE_TIMEOUT_SECS
    headers = _auth_headers(server, deadline=deadline)
    invalidate_server_id = _password_auth_cache_id(server)
    group_id = _clean(server.get("group_id"))

    session = Session(verify=True)
    items: list[dict] = []
    received_count = 0
    expected_total: int | None = None
    total_seen = False
    try:
        page = 1
        while True:
            if page > SUB2API_MAX_REMOTE_PAGES:
                raise ValueError("remote item limit exceeded")
            params: dict[str, object] = {
                "platform": "openai",
                "type": "oauth",
                "page": page,
                "page_size": 200,
            }
            if group_id:
                params["group"] = group_id
            response = session.get(
                f"{base_url.rstrip('/')}/api/v1/admin/accounts",
                headers=headers,
                params=params,
                timeout=_remaining_timeout(deadline, 30.0),
                stream=True,
            )
            payload = _response_json(
                response,
                "sub2api list failed",
                invalidate_server_id=invalidate_server_id,
            )

            data, total = _extract_paged_items(payload)
            expected_total, total_seen = _merge_declared_total(total, expected_total, total_seen, page)
            _validate_remote_page(data, total, received_count, page)
            if not data:
                break
            received_count += len(data)

            for account in data:
                if not isinstance(account, dict):
                    continue
                credentials = account.get("credentials") if isinstance(account.get("credentials"), dict) else {}
                account_id = _clean_remote_id(account.get("id"))
                if not account_id:
                    continue
                items.append({
                    "id": account_id,
                    "name": _clean_public_text(account.get("name")),
                    "email": _clean_public_text(credentials.get("email")) or _clean_public_text(account.get("name")),
                    "plan_type": _clean_public_text(credentials.get("plan_type")),
                    "status": _clean_public_text(account.get("status")),
                    "expires_at": _clean_public_text(credentials.get("expires_at")),
                    "has_refresh_token": bool(_clean_remote_text(credentials.get("refresh_token"))),
                })

            if total is not None:
                if received_count >= total:
                    break
            elif len(data) < 200:
                break
            page += 1
    finally:
        session.close()

    return items


def list_remote_groups(server: dict) -> list[dict]:
    """Return OpenAI account groups from a sub2api server."""
    base_url = _clean(server.get("base_url"))
    if not base_url:
        return []

    deadline = time.monotonic() + SUB2API_REMOTE_BROWSE_TIMEOUT_SECS
    headers = _auth_headers(server, deadline=deadline)
    invalidate_server_id = _password_auth_cache_id(server)

    session = Session(verify=True)
    items: list[dict] = []
    received_count = 0
    expected_total: int | None = None
    total_seen = False
    try:
        page = 1
        while True:
            if page > SUB2API_MAX_REMOTE_PAGES:
                raise ValueError("remote item limit exceeded")
            response = session.get(
                f"{base_url.rstrip('/')}/api/v1/admin/groups",
                headers=headers,
                params={
                    "page": page,
                    "page_size": 200,
                },
                timeout=_remaining_timeout(deadline, 30.0),
                stream=True,
            )
            payload = _response_json(
                response,
                "sub2api groups failed",
                invalidate_server_id=invalidate_server_id,
            )

            data, total = _extract_paged_items(payload)
            expected_total, total_seen = _merge_declared_total(total, expected_total, total_seen, page)
            _validate_remote_page(data, total, received_count, page)
            if not data:
                break
            received_count += len(data)

            for group in data:
                if not isinstance(group, dict):
                    continue
                group_id = _clean_remote_id(group.get("id"))
                if not group_id:
                    continue
                try:
                    account_count = _parse_nonnegative_int(group.get("account_count", 0))
                    active_account_count = _parse_nonnegative_int(
                        group.get("active_account_count", 0)
                    )
                except ValueError as exc:
                    raise ValueError("invalid sub2api group payload") from exc
                items.append({
                    "id": group_id,
                    "name": _clean_public_text(group.get("name")),
                    "description": _clean_public_text(group.get("description")),
                    "platform": _clean_public_text(group.get("platform")),
                    "status": _clean_public_text(group.get("status")),
                    "account_count": account_count,
                    "active_account_count": active_account_count,
                })

            if total is not None:
                if received_count >= total:
                    break
            elif len(data) < 200:
                break
            page += 1
    finally:
        session.close()

    return items


def _fetch_access_tokens_for_accounts(
        server: dict,
        account_ids: list[str],
        *,
        deadline: float | None = None,
) -> tuple[list[str | dict], list[dict]]:
    """Return exported access tokens and per-account errors from sub2api."""
    base_url = _clean(server.get("base_url"))
    if not isinstance(account_ids, list) or any(not isinstance(item, str) for item in account_ids):
        raise ValueError("invalid account ids")
    ids = list(dict.fromkeys(item.strip() for item in account_ids if item.strip()))
    if not ids:
        return [], []
    headers = _auth_headers(server, deadline=deadline)
    invalidate_server_id = _password_auth_cache_id(server)

    session = Session(verify=True)
    requested_ids = set(ids)
    matched_ids: set[str] = set()
    try:
        response = session.get(
            f"{base_url.rstrip('/')}/api/v1/admin/accounts/data",
            headers=headers,
            params={"ids": ",".join(ids), "timezone": "Asia/Shanghai"},
            timeout=_remaining_import_timeout(deadline, 30.0),
            stream=True,
        )
        payload = _response_json(
            response,
            "sub2api account export failed",
            invalidate_server_id=invalidate_server_id,
        )
    finally:
        session.close()

    data = _unwrap_envelope(payload)
    accounts = data.get("accounts") if isinstance(data, dict) else None
    if not isinstance(accounts, list):
        raise RuntimeError("invalid export payload")

    values: list[str | dict] = []
    errors: list[dict] = []
    for account in accounts:
        if not isinstance(account, dict):
            continue
        account_id = _clean_remote_id(account.get("id"))
        if not account_id or account_id not in requested_ids or account_id in matched_ids:
            continue
        matched_ids.add(account_id)
        credentials = account.get("credentials") if isinstance(account.get("credentials"), dict) else {}
        token = _extract_access_token(credentials)
        if token:
            values.append(_build_codex_import_value(account, credentials, token))
        else:
            errors.append({"name": account_id, "error": "missing access_token"})

    missing_count = len(requested_ids - matched_ids)
    if missing_count:
        errors.append({"name": "Sub2API", "error": f"missing {missing_count} selected accounts"})

    return values, errors


def _normalize_import_values(values: list[str | dict]) -> tuple[list[dict], list[str], bool]:
    items: list[dict] = []
    tokens: list[str] = []
    has_metadata = False
    for value in values:
        if isinstance(value, str) and value.strip():
            token = value.strip()
            items.append({"access_token": token, "source_type": "codex"})
            tokens.append(token)
        elif isinstance(value, dict):
            token = _clean_remote_text(value.get("access_token"))
            if not token:
                continue
            item = dict(value)
            item["access_token"] = token
            item.setdefault("source_type", "codex")
            items.append(item)
            tokens.append(token)
            has_metadata = has_metadata or any(
                _clean_remote_text(item.get(field))
                for field in ("plan_type", "type", "refresh_token", "id_token")
            )
    return items, tokens, has_metadata


class Sub2APIImportService:
    def __init__(self, sub2api_config: Sub2APIConfig):
        self._config = sub2api_config

    def start_import(self, server: dict, account_ids: list[str]) -> dict:
        if not isinstance(account_ids, list) or any(not isinstance(item, str) for item in account_ids):
            raise PublicSafeValueError("account ids are required")
        ids = [item.strip() for item in account_ids if item.strip()]
        if not ids:
            raise PublicSafeValueError("account ids is required")
        if len(ids) > SUB2API_MAX_REMOTE_ITEMS:
            raise PublicSafeValueError("account ids limit exceeded")

        server_id = _clean(server.get("id"))
        reservation = reserve_background_task()
        job = {
            "job_id": uuid.uuid4().hex,
            "status": "pending",
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "total": len(ids),
            "completed": 0,
            "added": 0,
            "skipped": 0,
            "refreshed": 0,
            "failed": 0,
            "errors": [],
        }
        try:
            saved = self._config.begin_import_job(server_id, job)
        except Exception:
            reservation.cancel()
            raise
        if saved is None:
            reservation.cancel()
            raise PublicSafeValueError("server not found")
        try:
            reservation.submit(self._run_import, server_id, dict(saved), ids)
        except Exception:
            self._config.set_import_job(
                server_id,
                {
                    **job,
                    "status": "failed",
                    "completed": len(ids),
                    "failed": len(ids),
                    "errors": [],
                },
            )
            raise
        return dict(saved.get("import_job") or job)

    def _update_job(self, server_id: str, **updates) -> None:
        current = self._config.get_import_job(server_id)
        if current is None:
            return
        next_job = {**current, **updates, "updated_at": _now_iso()}
        self._config.set_import_job(server_id, next_job)

    def _append_error(self, server_id: str, account_id: str, message: str) -> None:
        current = self._config.get_import_job(server_id)
        if current is None:
            return
        errors = list(current.get("errors") or [])
        errors.append({"name": account_id, "error": message})
        self._update_job(server_id, errors=canonicalize_import_job_errors(errors))

    def _run_import(self, server_id: str, server: dict, account_ids: list[str]) -> None:
        try:
            self._update_job(server_id, status="running")
        except Exception:
            try:
                self._update_job(
                    server_id,
                    status="failed",
                    completed=len(account_ids),
                    failed=len(account_ids),
                    errors=canonicalize_import_job_errors([
                        {"name": "Sub2API", "error": "import failed"},
                    ]),
                )
            except Exception:
                logger.error({
                    "event": "sub2api_import_initial_state_persist_failed",
                    "stage": "running",
                })
            return
        deadline = time.monotonic() + SUB2API_IMPORT_TIMEOUT_SECS
        current = self._config.get_import_job(server_id) or {}
        failed_count = int(current.get("failed") or 0)

        try:
            values, errors = _fetch_access_tokens_for_accounts(
                server,
                account_ids,
                deadline=deadline,
            )
        except Exception as exc:
            message = exception_log_message(exc)
            for account_id in account_ids:
                self._append_error(server_id, account_id, message)
                failed_count += 1
            values = []
        else:
            for error in errors:
                self._append_error(server_id, _clean(error.get("name")), _clean(error.get("error")) or "unknown error")
                failed_count += 1

        account_items, tokens, has_metadata = _normalize_import_values(values)
        current = self._config.get_import_job(server_id) or {}
        total = int(current.get("total") or len(account_ids))
        failed_count = min(total, failed_count)
        self._update_job(
            server_id,
            completed=len(account_ids),
            failed=failed_count,
        )

        if not tokens:
            current = self._config.get_import_job(server_id) or {}
            self._update_job(
                server_id,
                status="failed",
                completed=total,
                failed=failed_count,
            )
            return

        try:
            _remaining_import_timeout(deadline, float("inf"))
        except TimeoutError:
            self._update_job(
                server_id,
                status="failed",
                completed=total,
                failed=total,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "Sub2API", "error": "import failed"}],
                ),
            )
            return

        try:
            add_result = (
                account_service.add_account_items(account_items)
                if has_metadata
                else account_service.add_accounts(tokens, source_type="codex")
            )
            if not isinstance(add_result, dict) or not add_result:
                raise ValueError("invalid account import result")
            added = _parse_nonnegative_int(add_result["added"])
            skipped = _parse_nonnegative_int(add_result["skipped"])
            if added + skipped > len(tokens):
                raise ValueError("invalid account import count")
        except Exception:
            self._update_job(
                server_id,
                status="failed",
                completed=total,
                added=0,
                skipped=0,
                refreshed=0,
                failed=total,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "Sub2API", "error": "import failed"}],
                ),
            )
            return
        try:
            _remaining_import_timeout(deadline, float("inf"))
        except TimeoutError:
            self._update_job(
                server_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=0,
                failed=total,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "Sub2API", "error": "import failed"}],
                ),
            )
            return
        try:
            refresh_result = run_with_timeout(
                account_service.refresh_accounts,
                tokens,
                timeout=_remaining_import_timeout(deadline, float("inf")),
                timeout_message="Sub2API account refresh timed out",
                deadline=deadline,
            )
            _remaining_import_timeout(deadline, float("inf"))
            if not isinstance(refresh_result, dict) or not refresh_result:
                raise ValueError("invalid account refresh result")
            refreshed = _parse_nonnegative_int(refresh_result["refreshed"])
            if refreshed > len(tokens):
                raise ValueError("invalid account refresh count")
            refresh_errors = refresh_result.get("errors", [])
            if not isinstance(refresh_errors, list):
                raise ValueError("invalid account refresh errors")
        except TimeoutError:
            self._update_job(
                server_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=0,
                failed=total,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "Sub2API", "error": "import failed"}],
                ),
            )
            return
        except Exception:
            self._update_job(
                server_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=0,
                failed=failed_count,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "Sub2API", "error": "import failed"}],
                ),
            )
            return
        if refresh_errors:
            self._update_job(
                server_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=refreshed,
                failed=min(total, failed_count + len(refresh_errors)),
                errors=canonicalize_import_job_errors([*(current.get("errors") or []), *refresh_errors]),
            )
            return
        current = self._config.get_import_job(server_id) or {}
        self._update_job(
            server_id,
            status="completed",
            completed=total,
            added=added,
            skipped=skipped,
            refreshed=refreshed,
            failed=failed_count,
        )


sub2api_config = Sub2APIConfig(SUB2API_CONFIG_FILE)
sub2api_import_service = Sub2APIImportService(sub2api_config)
