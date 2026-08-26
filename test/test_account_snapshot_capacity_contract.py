from __future__ import annotations

import json
import os
import threading
import time
from unittest import mock

import pytest

import services.storage.json_storage as json_storage_module
from services.account_snapshot import (
    ACCOUNT_SNAPSHOT_MAX_BYTES,
    ACCOUNT_SNAPSHOT_MAX_DEPTH,
    ACCOUNT_SNAPSHOT_MAX_KEY_BYTES,
    ACCOUNT_SNAPSHOT_MAX_NODES,
    ACCOUNT_SNAPSHOT_MAX_OBJECT_FIELDS,
    ACCOUNT_SNAPSHOT_MAX_RECORDS,
    ACCOUNT_SNAPSHOT_MAX_STRING_BYTES,
    AccountSnapshotLimitError,
    validate_account_snapshot_bytes,
)
from services.storage.base import StorageDataError
from services.storage.json_storage import JSONStorageBackend


PRODUCTION_ACCOUNT_COUNT = 1_491
PRODUCTION_ACCOUNT_BYTES = 8_027_088


def _padded_empty_snapshot(size: int) -> bytes:
    payload = b'{"items":[],"cumulative_total":0}'
    assert size >= len(payload)
    return payload + (b" " * (size - len(payload)))


def _production_shaped_snapshot() -> bytes:
    records = [
        {
            "access_token": f"production-token-{index}",
            "account_id": f"production-account-{index}",
            "refresh_token": "r" * 5_000,
            "status": "正常",
            "quota": 1,
            "type": "free",
        }
        for index in range(PRODUCTION_ACCOUNT_COUNT)
    ]
    payload = json.dumps(
        {"items": records, "cumulative_total": PRODUCTION_ACCOUNT_COUNT},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    assert 4 * 1024 * 1024 < len(payload) < PRODUCTION_ACCOUNT_BYTES
    return payload + (b" " * (PRODUCTION_ACCOUNT_BYTES - len(payload)))


def _maximum_legal_structural_snapshot() -> bytes:
    records = [
        {
            "access_token": f"token-{index}",
            **{f"f{field}": "v" * 63 for field in range(23)},
        }
        for index in range(ACCOUNT_SNAPSHOT_MAX_RECORDS)
    ]
    payload = json.dumps(
        {"items": records, "cumulative_total": ACCOUNT_SNAPSHOT_MAX_RECORDS},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    assert len(payload) == 16_758_926
    return payload + (b" " * (ACCOUNT_SNAPSHOT_MAX_BYTES - len(payload)))


@pytest.mark.parametrize(
    ("size", "accepted"),
    (
        (ACCOUNT_SNAPSHOT_MAX_BYTES - 1, True),
        (ACCOUNT_SNAPSHOT_MAX_BYTES, True),
        (ACCOUNT_SNAPSHOT_MAX_BYTES + 1, False),
    ),
)
def test_account_snapshot_byte_boundary_is_enforced_before_json_dom(
    tmp_path,
    size: int,
    accepted: bool,
) -> None:
    accounts_path = tmp_path / "accounts.json"
    accounts_path.write_bytes(_padded_empty_snapshot(size))
    backend = JSONStorageBackend(accounts_path)

    if accepted:
        assert backend.load_accounts() == []
    else:
        with mock.patch.object(json_storage_module.json, "loads", side_effect=AssertionError("DOM")):
            with pytest.raises(StorageDataError):
                backend.load_accounts()


def test_account_snapshot_node_bomb_is_rejected_before_json_dom(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    accounts_path.write_bytes(
        b'{"items":['
        + (b"0," * ACCOUNT_SNAPSHOT_MAX_NODES)
        + b'0],"cumulative_total":0}'
    )
    backend = JSONStorageBackend(accounts_path)

    with mock.patch.object(json_storage_module.json, "loads", side_effect=AssertionError("DOM")):
        with pytest.raises(StorageDataError):
            backend.load_accounts()


def test_account_snapshot_record_limit_is_shared_by_python_and_rust(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    accounts_path.write_text(
        json.dumps([{"access_token": f"token-{index}"} for index in range(ACCOUNT_SNAPSHOT_MAX_RECORDS + 1)]),
        encoding="utf-8",
    )
    backend = JSONStorageBackend(accounts_path)

    with mock.patch.object(json_storage_module.json, "loads", side_effect=AssertionError("DOM")):
        with pytest.raises(StorageDataError):
            backend.load_accounts()


def test_account_snapshot_scanner_enforces_structural_boundaries_before_dom() -> None:
    def nested(depth: int) -> bytes:
        containers = depth - 1
        return (b"[" * containers) + b"0" + (b"]" * containers)

    def object_with_fields(count: int) -> bytes:
        return (
            b"{"
            + b",".join(f'"k{index}":0'.encode() for index in range(count))
            + b"}"
        )

    def array_with_items(count: int) -> bytes:
        return b"[" + (b",".join([b"0"] * count)) + b"]"

    accepted = (
        nested(ACCOUNT_SNAPSHOT_MAX_DEPTH),
        array_with_items(ACCOUNT_SNAPSHOT_MAX_RECORDS),
        object_with_fields(ACCOUNT_SNAPSHOT_MAX_OBJECT_FIELDS),
        b'{"' + (b"k" * ACCOUNT_SNAPSHOT_MAX_KEY_BYTES) + b'":0}',
        b'"' + (b"s" * ACCOUNT_SNAPSHOT_MAX_STRING_BYTES) + b'"',
    )
    rejected = (
        nested(ACCOUNT_SNAPSHOT_MAX_DEPTH + 1),
        array_with_items(ACCOUNT_SNAPSHOT_MAX_RECORDS + 1),
        object_with_fields(ACCOUNT_SNAPSHOT_MAX_OBJECT_FIELDS + 1),
        b'{"' + (b"k" * (ACCOUNT_SNAPSHOT_MAX_KEY_BYTES + 1)) + b'":0}',
        b'"' + (b"s" * (ACCOUNT_SNAPSHOT_MAX_STRING_BYTES + 1)) + b'"',
    )
    for payload in accepted:
        assert validate_account_snapshot_bytes(payload) is None
    for payload in rejected:
        with pytest.raises(AccountSnapshotLimitError):
            validate_account_snapshot_bytes(payload)


def test_production_sized_snapshot_primes_a_bounded_health_hot_path(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_bytes(_production_shaped_snapshot())
    auth_keys_path.write_text('{"items":[]}', encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)

    assert len(backend.load_accounts()) == PRODUCTION_ACCOUNT_COUNT
    assert backend.load_auth_keys() == []
    with (
        mock.patch.object(
            backend,
            "_load_json_document",
            side_effect=AssertionError("health must not reload accounts"),
        ),
        mock.patch.object(
            backend,
            "load_auth_keys",
            side_effect=AssertionError("health must not reload auth keys"),
        ),
    ):
        assert backend.health_check()["status"] == "healthy"


def test_maximum_legal_structural_snapshot_supports_health_and_compact_rmw(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_bytes(_maximum_legal_structural_snapshot())
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)

    accounts = backend.load_accounts()
    assert len(accounts) == ACCOUNT_SNAPSHOT_MAX_RECORDS
    assert backend.load_auth_keys() == []
    assert backend.health_check()["status"] == "healthy"
    accounts[0]["f0"] = "w" * 63
    backend.save_accounts(accounts)

    assert accounts_path.stat().st_size <= ACCOUNT_SNAPSHOT_MAX_BYTES
    assert backend.health_check()["status"] == "healthy"
    reloaded = backend.load_accounts()
    assert len(reloaded) == ACCOUNT_SNAPSHOT_MAX_RECORDS
    assert reloaded[0]["f0"] == "w" * 63


def test_external_snapshot_replacement_stays_unhealthy_until_successfully_revalidated(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    original = b'[{"access_token":"original"}]'
    accounts_path.write_bytes(original)
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.load_auth_keys() == []
    assert backend.health_check()["status"] == "healthy"

    corrupt = tmp_path / "corrupt.json"
    corrupt.write_bytes(b"{broken")
    os.replace(corrupt, accounts_path)
    assert backend.health_check()["status"] == "unhealthy"

    restored = tmp_path / "restored.json"
    restored.write_bytes(original)
    os.replace(restored, accounts_path)
    assert backend.health_check()["status"] == "unhealthy"
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.health_check()["status"] == "healthy"


def test_failed_atomic_account_write_invalidates_health_until_revalidation(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text('[{"access_token":"original"}]', encoding="utf-8")
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.load_auth_keys() == []

    with mock.patch.object(json_storage_module, "atomic_write_bytes", side_effect=OSError("write failed")):
        with pytest.raises(OSError):
            backend.save_accounts([{"access_token": "replacement"}])

    assert backend.health_check()["status"] == "unhealthy"
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.health_check()["status"] == "healthy"


def test_failed_account_serialization_invalidates_health_before_atomic_write(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text('[{"access_token":"original"}]', encoding="utf-8")
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.load_auth_keys() == []

    with mock.patch.object(json_storage_module.json, "dumps", side_effect=TypeError("encode failed")):
        with pytest.raises(StorageDataError):
            backend.save_accounts([{"access_token": "replacement"}])

    assert backend.health_check()["status"] == "unhealthy"
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.health_check()["status"] == "healthy"


def test_concurrent_successful_rmw_does_not_create_transient_health_failure(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text('[{"access_token":"original"}]', encoding="utf-8")
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.load_auth_keys() == []
    entered = threading.Event()
    release = threading.Event()
    original_atomic_write = json_storage_module.atomic_write_bytes
    failures: list[BaseException] = []

    def gated_atomic_write(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=2)
        return original_atomic_write(*args, **kwargs)

    def writer() -> None:
        try:
            backend.save_accounts([{"access_token": "replacement"}])
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    with mock.patch.object(json_storage_module, "atomic_write_bytes", side_effect=gated_atomic_write):
        thread = threading.Thread(target=writer)
        thread.start()
        assert entered.wait(timeout=2)
        assert backend.health_check()["status"] == "healthy"
        release.set()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert failures == []
    assert backend.health_check()["status"] == "healthy"
    assert backend.load_accounts() == [{"access_token": "replacement"}]


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
@pytest.mark.parametrize("replacement", ("corrupt", "oversized"))
def test_health_lock_contention_still_rejects_changed_snapshot_without_reparse(
    tmp_path,
    kind: str,
    replacement: str,
) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text('[{"access_token":"original"}]', encoding="utf-8")
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.load_auth_keys() == []
    target = accounts_path if kind == "accounts" else auth_keys_path
    entered = threading.Event()
    release = threading.Event()

    def hold_state_lock() -> None:
        with backend._health_state_lock:
            entered.set()
            assert release.wait(timeout=2)

    holder = threading.Thread(target=hold_state_lock)
    holder.start()
    assert entered.wait(timeout=2)
    changed = tmp_path / f"{kind}-changed.json"
    changed.write_bytes(
        b"{broken"
        if replacement == "corrupt"
        else b"x" * (ACCOUNT_SNAPSHOT_MAX_BYTES + 1)
    )
    os.replace(changed, target)
    started = time.monotonic()
    try:
        with (
            mock.patch.object(
                backend,
                "_load_json_document",
                side_effect=AssertionError("health must not parse changed accounts"),
            ),
            mock.patch.object(
                backend,
                "load_auth_keys",
                side_effect=AssertionError("health must not parse changed auth keys"),
            ),
        ):
            result = backend.health_check()
    finally:
        release.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    assert time.monotonic() - started < 0.5
    assert result == {
        "status": "unhealthy",
        "backend": "json",
        "error": "存储后端健康检查失败",
    }


def test_health_lock_contention_accepts_only_unchanged_validated_identities(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text('[{"access_token":"original"}]', encoding="utf-8")
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    assert backend.load_accounts() == [{"access_token": "original"}]
    assert backend.load_auth_keys() == []
    entered = threading.Event()
    release = threading.Event()

    def hold_state_lock() -> None:
        with backend._health_state_lock:
            entered.set()
            assert release.wait(timeout=2)

    holder = threading.Thread(target=hold_state_lock)
    holder.start()
    assert entered.wait(timeout=2)
    try:
        assert backend.health_check()["status"] == "healthy"
    finally:
        release.set()
        holder.join(timeout=2)
    assert not holder.is_alive()
