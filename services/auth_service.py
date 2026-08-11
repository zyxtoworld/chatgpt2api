from __future__ import annotations

import hashlib
import hmac
import secrets
import uuid
from datetime import datetime, timezone
from threading import Lock
from typing import Literal

from services.config import config
from services.protocol.error_response import PublicSafeValueError
from services.storage.base import (
    StorageBackend,
    StorageConflictError,
    StorageDataError,
    StorageSnapshot,
    make_storage_snapshot,
)

AuthRole = Literal["admin", "user"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class AuthService:
    def __init__(self, storage: StorageBackend):
        self.storage = storage
        self._lock = Lock()
        self._auth_reload_generation = 0
        self._auth_reload_invalidated = False
        self._items = self._load()
        self._last_used_flush_at: dict[str, datetime] = {}

    @staticmethod
    def _clean(value: object) -> str:
        return str(value or "").strip()

    @staticmethod
    def _default_name(role: object) -> str:
        return "管理员密钥" if str(role or "").strip().lower() == "admin" else "普通用户"

    @staticmethod
    def _is_timestamp(value: object) -> bool:
        if not isinstance(value, str):
            return False
        candidate = value.strip()
        if not candidate:
            return False
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            datetime.fromisoformat(candidate)
        except (TypeError, ValueError, OverflowError):
            return False
        return True

    def _normalize_item(self, raw: object) -> dict[str, object] | None:
        if not isinstance(raw, dict):
            return None

        raw_role = raw.get("role")
        if not isinstance(raw_role, str):
            return None
        role = raw_role.strip().lower()
        if role not in {"admin", "user"}:
            return None

        raw_key_hash = raw.get("key_hash")
        if not isinstance(raw_key_hash, str):
            return None
        key_hash = raw_key_hash.strip()
        if not key_hash:
            return None

        if "id" in raw:
            raw_id = raw.get("id")
            if not isinstance(raw_id, str) or not raw_id.strip():
                return None
            item_id = raw_id.strip()
        else:
            item_id = uuid.uuid4().hex[:12]

        name = self._clean(raw.get("name")) or self._default_name(role)

        if "created_at" in raw:
            raw_created_at = raw.get("created_at")
            if not self._is_timestamp(raw_created_at):
                return None
            created_at = raw_created_at.strip()
        else:
            created_at = _now_iso()

        if "last_used_at" not in raw or raw.get("last_used_at") is None:
            last_used_at = None
        else:
            raw_last_used_at = raw.get("last_used_at")
            if not self._is_timestamp(raw_last_used_at):
                return None
            last_used_at = raw_last_used_at.strip()

        if "enabled" in raw:
            enabled = raw.get("enabled")
            if type(enabled) is not bool:
                return None
        else:
            # Missing fields are the only legacy migration default.
            enabled = True

        return {
            "id": item_id,
            "name": name,
            "role": role,
            "key_hash": key_hash,
            "enabled": enabled,
            "created_at": created_at,
            "last_used_at": last_used_at,
        }

    def _load(self) -> list[dict[str, object]]:
        load_snapshot = getattr(self.storage, "load_auth_keys_snapshot", None)
        if callable(load_snapshot):
            snapshot = load_snapshot()
            if not isinstance(snapshot, StorageSnapshot):
                raise StorageDataError()
            items = snapshot.records
        else:
            items = self.storage.load_auth_keys()
            snapshot = make_storage_snapshot(items)
        if not isinstance(items, list):
            raise StorageDataError()
        normalized_items: list[dict[str, object]] = []
        seen_ids: set[str] = set()
        seen_key_hashes: set[str] = set()
        for item in items:
            normalized = self._normalize_item(item)
            if normalized is None:
                raise StorageDataError()
            item_id = self._clean(normalized.get("id"))
            key_hash = self._clean(normalized.get("key_hash"))
            if (
                (item_id and item_id in seen_ids)
                or (key_hash and key_hash in seen_key_hashes)
            ):
                raise StorageDataError()
            if item_id:
                seen_ids.add(item_id)
            if key_hash:
                seen_key_hashes.add(key_hash)
            normalized_items.append(normalized)
        self._auth_keys_snapshot = snapshot
        return normalized_items

    def _save(self) -> None:
        auth_keys = list(self._items)
        expected_auth_keys = getattr(self, "_auth_keys_snapshot", None)
        save_if_revision = getattr(self.storage, "save_auth_keys_if_revision", None)
        if callable(save_if_revision) and isinstance(expected_auth_keys, StorageSnapshot):
            saved_snapshot = save_if_revision(expected_auth_keys, auth_keys)
            self._auth_keys_snapshot = (
                saved_snapshot
                if isinstance(saved_snapshot, StorageSnapshot)
                else make_storage_snapshot(auth_keys)
            )
        else:
            # Structural fakes used by integrations may not implement the CAS helper.
            self.storage.load_auth_keys()
            self.storage.save_auth_keys(auth_keys)
            self._auth_keys_snapshot = make_storage_snapshot(auth_keys)

    def _reload_locked(self) -> None:
        self._items = self._load()

    @staticmethod
    def _public_item(item: dict[str, object]) -> dict[str, object]:
        return {
            "id": item.get("id"),
            "name": item.get("name"),
            "role": item.get("role"),
            "enabled": bool(item.get("enabled", True)),
            "created_at": item.get("created_at"),
            "last_used_at": item.get("last_used_at"),
        }

    def list_keys(self, role: AuthRole | None = None) -> list[dict[str, object]]:
        with self._lock:
            self._reload_locked()
            items = [item for item in self._items if role is None or item.get("role") == role]
            return [self._public_item(item) for item in items]

    def _has_key_hash_locked(self, key_hash: str, *, exclude_id: str = "") -> bool:
        for item in self._items:
            item_id = self._clean(item.get("id"))
            if exclude_id and item_id == exclude_id:
                continue
            stored_hash = self._clean(item.get("key_hash"))
            if stored_hash and hmac.compare_digest(stored_hash, key_hash):
                return True
        return False

    def _build_key_hash_locked(self, raw_key: str, *, exclude_id: str = "") -> str:
        candidate = self._clean(raw_key)
        if not candidate:
            raise PublicSafeValueError("请输入新的专用密钥")
        admin_key = self._clean(config.auth_key)
        if admin_key and hmac.compare_digest(candidate, admin_key):
            raise PublicSafeValueError("这个密钥和管理员密钥冲突了，请换一个新的密钥")
        key_hash = _hash_key(candidate)
        if self._has_key_hash_locked(key_hash, exclude_id=exclude_id):
            raise PublicSafeValueError("这个专用密钥已经存在，请换一个新的密钥")
        return key_hash

    def _has_name_locked(self, name: str, *, role: AuthRole | None = None, exclude_id: str = "") -> bool:
        candidate = self._clean(name)
        if not candidate:
            return False
        for item in self._items:
            item_id = self._clean(item.get("id"))
            if exclude_id and item_id == exclude_id:
                continue
            if role is not None and item.get("role") != role:
                continue
            if self._clean(item.get("name")) == candidate:
                return True
        return False

    def _build_default_name_locked(self, role: AuthRole, *, exclude_id: str = "") -> str:
        base_name = self._default_name(role)
        if not self._has_name_locked(base_name, role=role, exclude_id=exclude_id):
            return base_name
        suffix = 2
        while True:
            candidate = f"{base_name} {suffix}"
            if not self._has_name_locked(candidate, role=role, exclude_id=exclude_id):
                return candidate
            suffix += 1

    def _build_name_locked(self, name: str, *, role: AuthRole, exclude_id: str = "") -> str:
        candidate = self._clean(name)
        if not candidate:
            return self._build_default_name_locked(role, exclude_id=exclude_id)
        if self._has_name_locked(candidate, role=role, exclude_id=exclude_id):
            raise PublicSafeValueError("这个名称已经在使用中了，换一个更容易区分的名称吧")
        return candidate

    def create_key(self, *, role: AuthRole, name: str = "") -> tuple[dict[str, object], str]:
        with self._lock:
            self._reload_locked()
            normalized_name = self._build_name_locked(name, role=role)
            while True:
                raw_key = f"sk-{secrets.token_urlsafe(24)}"
                try:
                    key_hash = self._build_key_hash_locked(raw_key)
                    break
                except ValueError:
                    continue
            item = {
                "id": uuid.uuid4().hex[:12],
                "name": normalized_name,
                "role": role,
                "key_hash": key_hash,
                "enabled": True,
                "created_at": _now_iso(),
                "last_used_at": None,
            }
            self._items.append(item)
            self._save()
            return self._public_item(item), raw_key

    def update_key(
        self,
        key_id: str,
        updates: dict[str, object],
        *,
        role: AuthRole | None = None,
    ) -> dict[str, object] | None:
        normalized_id = self._clean(key_id)
        if not normalized_id:
            return None
        with self._lock:
            self._reload_locked()
            for index, item in enumerate(self._items):
                if item.get("id") != normalized_id:
                    continue
                if role is not None and item.get("role") != role:
                    return None
                next_item = dict(item)
                next_role = "admin" if str(next_item.get("role") or "").strip().lower() == "admin" else "user"
                if "name" in updates and updates.get("name") is not None:
                    next_item["name"] = self._build_name_locked(
                        str(updates.get("name") or ""),
                        role=next_role,
                        exclude_id=normalized_id,
                    )
                if "enabled" in updates and updates.get("enabled") is not None:
                    next_item["enabled"] = bool(updates.get("enabled"))
                if "key" in updates and updates.get("key") is not None:
                    next_item["key_hash"] = self._build_key_hash_locked(str(updates.get("key") or ""), exclude_id=normalized_id)
                self._items[index] = next_item
                self._save()
                return self._public_item(next_item)
        return None

    def delete_key(self, key_id: str, *, role: AuthRole | None = None) -> bool:
        normalized_id = self._clean(key_id)
        if not normalized_id:
            return False
        with self._lock:
            self._reload_locked()
            before = len(self._items)
            self._items = [
                item
                for item in self._items
                if not (item.get("id") == normalized_id and (role is None or item.get("role") == role))
            ]
            if len(self._items) == before:
                return False
            self._save()
            return True

    def authenticate(self, raw_key: str) -> dict[str, object] | None:
        candidate = self._clean(raw_key)
        if not candidate:
            return None
        candidate_hash = _hash_key(candidate)
        # Requests that were already queued behind the same snapshot read may
        # authenticate against that completed read. A request arriving after
        # the wave observes the new generation and reloads again, so revoke
        # and disable changes remain visible on the next authentication.
        observed_reload_generation = self._auth_reload_generation
        with self._lock:
            if (
                self._auth_reload_invalidated
                or observed_reload_generation == self._auth_reload_generation
            ):
                self._reload_locked()
                self._auth_reload_generation += 1
                self._auth_reload_invalidated = False
            for index, item in enumerate(self._items):
                if not bool(item.get("enabled", True)):
                    continue
                stored_hash = self._clean(item.get("key_hash"))
                if not stored_hash or not hmac.compare_digest(stored_hash, candidate_hash):
                    continue
                next_item = dict(item)
                now = datetime.now(timezone.utc)
                next_item["last_used_at"] = now.isoformat()
                self._items[index] = next_item
                item_id = self._clean(next_item.get("id"))
                last_flush_at = self._last_used_flush_at.get(item_id)
                if last_flush_at is None or (now - last_flush_at).total_seconds() >= 60:
                    try:
                        self._save()
                        self._last_used_flush_at[item_id] = now
                    except StorageDataError:
                        self._auth_reload_invalidated = True
                        raise
                    except StorageConflictError:
                        pass
                    except Exception:
                        pass
                return self._public_item(next_item)
        return None


auth_service = AuthService(config.get_storage_backend())
