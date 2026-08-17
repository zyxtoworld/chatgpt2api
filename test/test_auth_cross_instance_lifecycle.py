from __future__ import annotations

import tempfile
import threading
from pathlib import Path

import pytest

from services.auth_service import AuthService
from services.storage.base import StorageConflictError
from services.storage.json_storage import JSONStorageBackend


def _make_service(root: Path) -> AuthService:
    return AuthService(
        JSONStorageBackend(root / "accounts.json", root / "auth_keys.json")
    )


@pytest.mark.parametrize("mutation", ["delete", "disable", "rotate"])
@pytest.mark.parametrize("save_failure", ["cas", "oserror"])
def test_late_auth_audit_never_authorizes_key_changed_by_other_instance(
    mutation: str,
    save_failure: str,
) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        seed = _make_service(root)
        item, raw_key = seed.create_key(role="user", name="shared")
        service_a = _make_service(root)
        service_b = _make_service(root)
        entered_save = threading.Event()
        release_save = threading.Event()
        auth_result: list[dict[str, object] | None] = []
        auth_errors: list[BaseException] = []
        original_save = service_a._save

        def blocked_save() -> None:
            entered_save.set()
            assert release_save.wait(5), "A audit save was not released"
            if save_failure == "oserror":
                raise OSError("audit write failed")
            original_save()

        service_a._save = blocked_save

        def authenticate_a() -> None:
            try:
                auth_result.append(service_a.authenticate(raw_key))
            except BaseException as exc:
                auth_errors.append(exc)

        thread = threading.Thread(target=authenticate_a)
        thread.start()
        assert entered_save.wait(5), "A did not enter the audit save"

        if mutation == "delete":
            assert service_b.delete_key(str(item["id"]), role="user")
        elif mutation == "disable":
            assert service_b.update_key(
                str(item["id"]), {"enabled": False}, role="user"
            ) is not None
        else:
            assert service_b.update_key(
                str(item["id"]), {"key": "sk-rotated-key"}, role="user"
            ) is not None

        release_save.set()
        thread.join(5)
        assert not thread.is_alive()
        assert not auth_errors
        assert auth_result == [None]

        fresh = _make_service(root)
        if mutation == "rotate":
            assert fresh.authenticate("sk-rotated-key") is not None
            assert fresh.authenticate(raw_key) is None
        else:
            assert fresh.authenticate(raw_key) is None
