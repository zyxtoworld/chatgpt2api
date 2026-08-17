from __future__ import annotations

import json
import shutil
import sys
import tempfile
import uuid
from pathlib import Path
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

from git import Repo
from git.remote import PushInfo

from services.protocol.error_response import exception_log_message
from services.secure_file import atomic_write_bytes, authorized_root, read_checked_file_bytes
from services.storage.base import (
    STORAGE_HEALTH_ERROR_MESSAGE,
    StorageConflictError,
    StorageBackend,
    StorageDataError,
    StorageSnapshot,
    make_storage_snapshot,
    validate_storage_records,
)
from services.url_utils import redact_url_credentials


_MISSING = object()
_PENDING_PUSH_MARKER = ".pending-push"
_PUSH_FAILURE_FLAGS = (
    PushInfo.ERROR
    | PushInfo.REJECTED
    | PushInfo.REMOTE_REJECTED
    | PushInfo.REMOTE_FAILURE
    | PushInfo.NO_MATCH
)
_PUSH_SUCCESS_FLAGS = PushInfo.NEW_HEAD | PushInfo.FAST_FORWARD | PushInfo.UP_TO_DATE
GIT_OPERATION_TIMEOUT_SECS = 30.0


def _git_process_timeout_kwargs() -> dict[str, float]:
    # GitPython's kill_after_timeout uses POSIX process-group termination and
    # explicitly rejects the option on Windows.
    if sys.platform == "win32":
        return {}
    return {"kill_after_timeout": GIT_OPERATION_TIMEOUT_SECS}


def _validate_repo_file_path(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a relative file path")
    normalized = value.replace("\\", "/")
    if PurePosixPath(normalized).is_absolute() or PureWindowsPath(value).is_absolute():
        raise ValueError(f"{name} must be a relative file path")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"{name} must be a normalized relative file path")
    return normalized


class GitStorageBackend(StorageBackend):
    """Git 私有仓库存储后端"""

    supports_cumulative_snapshot = True

    def __init__(
        self,
        repo_url: str,
        token: str,
        branch: str = "main",
        file_path: str = "accounts.json",
        auth_keys_file_path: str = "auth_keys.json",
        local_cache_dir: Path | None = None,
    ):
        self.repo_url = repo_url
        self.token = token
        self.branch = branch
        self.file_path = _validate_repo_file_path(file_path, "file_path")
        self.auth_keys_file_path = _validate_repo_file_path(auth_keys_file_path, "auth_keys_file_path")
        
        # 本地缓存目录
        if local_cache_dir is None:
            local_cache_dir = Path(tempfile.gettempdir()) / "chatgpt2api_git_cache"
        self.local_cache_dir = authorized_root(Path(local_cache_dir))
        self.local_cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 构建带认证的 Git URL
        self.auth_repo_url = self._build_auth_url(repo_url, token)

    def _mutation_identity(self) -> str:
        return f"git:{self.repo_url}:{self.branch}:{self.file_path}:{self.auth_keys_file_path}"

    @staticmethod
    def _build_auth_url(repo_url: str, token: str) -> str:
        """构建带认证的 Git URL"""
        if not token:
            return repo_url
        
        # 支持 HTTPS 格式：https://github.com/user/repo.git
        if repo_url.startswith("https://"):
            # 插入 token
            return repo_url.replace("https://", f"https://{token}@")
        
        # 支持 git@ 格式：git@github.com:user/repo.git
        # 转换为 HTTPS 格式
        if repo_url.startswith("git@"):
            repo_url = repo_url.replace("git@", "https://")
            repo_url = repo_url.replace(".com:", ".com/")
            return repo_url.replace("https://", f"https://{token}@")
        
        return repo_url

    @staticmethod
    def _close_repo(repo: object) -> None:
        close = getattr(repo, "close", None)
        if not callable(close):
            return
        try:
            close()
        except Exception:
            pass

    def _clone_or_pull(self) -> Repo:
        """克隆或拉取仓库"""
        self.local_cache_dir = authorized_root(self.local_cache_dir)
        repo_path = self.local_cache_dir / "repo"
        pending_push = self.local_cache_dir / _PENDING_PUSH_MARKER

        if pending_push.exists() and not (repo_path.exists() and (repo_path / ".git").exists()):
            raise StorageDataError()
        
        if repo_path.exists() and (repo_path / ".git").exists():
            # 拉取失败必须保留最后一个有效缓存并向上报错。删除后重克隆会在
            # 网络故障时把可恢复快照变成永久缺失。
            repo = Repo(repo_path)
            if pending_push.exists():
                try:
                    self._restore_repo_from_remote(repo)
                    pending_push.unlink(missing_ok=True)
                except Exception as exc:
                    self._close_repo(repo)
                    raise StorageDataError() from exc
                return repo
            try:
                origin = repo.remote("origin")
                origin.pull(self.branch, **_git_process_timeout_kwargs())
            except Exception:
                self._close_repo(repo)
                raise
            return repo

        if repo_path.exists():
            raise StorageDataError()

        # 首次克隆只在完整成功后发布为授权缓存；失败的临时工作树不能污染
        # 后续读取，也不能覆盖已有路径。
        clone_path = self.local_cache_dir / f".repo.clone.{uuid.uuid4().hex}.tmp"
        try:
            Repo.clone_from(
                self.auth_repo_url,
                clone_path,
                branch=self.branch,
                **_git_process_timeout_kwargs(),
            )
            clone_path.replace(repo_path)
            return Repo(repo_path)
        finally:
            if clone_path.exists():
                shutil.rmtree(clone_path, ignore_errors=True)

    def load_accounts(self) -> list[dict[str, Any]]:
        """从 Git 仓库加载账号数据"""
        try:
            records, _cumulative_total = self._load_accounts_document()
            return records
        except Exception as exc:
            print(f"[git-storage] load failed: {exception_log_message(exc)}")
            raise

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存账号数据到 Git 仓库"""
        accounts = validate_storage_records(accounts)
        try:
            with self._mutation_lock("accounts"):
                _current_records, cumulative_total = self._load_accounts_document()
                value: object = accounts
                if cumulative_total is not None:
                    value = {"items": accounts, "cumulative_total": cumulative_total}
                self._save_json_file(self.file_path, value, "Update accounts data")
        except Exception as exc:
            print(f"[git-storage] save failed: {exception_log_message(exc)}")
            raise

    def save_accounts_if_revision(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
    ) -> StorageSnapshot:
        records = validate_storage_records(accounts)
        next_snapshot = make_storage_snapshot(records)
        try:
            with self._mutation_lock("accounts"):
                current_records, cumulative_total = self._load_accounts_document()
                if make_storage_snapshot(current_records).revision != expected.revision:
                    raise StorageConflictError()
                value: object = records
                if cumulative_total is not None:
                    value = {"items": records, "cumulative_total": cumulative_total}
                self._save_json_file(
                    self.file_path,
                    value,
                    "Update accounts data",
                    expected_revision=expected.revision,
                )
                return next_snapshot
        except Exception as exc:
            print(f"[git-storage] save failed: {exception_log_message(exc)}")
            raise

    def load_cumulative_total(self) -> int | None:
        _records, cumulative_total = self._load_accounts_document()
        return cumulative_total

    def save_accounts_with_cumulative_total(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
        cumulative_total: int,
    ) -> StorageSnapshot:
        records = validate_storage_records(accounts)
        if type(cumulative_total) is not int or cumulative_total < 0:
            raise StorageDataError()
        next_snapshot = make_storage_snapshot(records)
        try:
            with self._mutation_lock("accounts"):
                current_records, current_total = self._load_accounts_document()
                current = make_storage_snapshot(current_records)
                if current.revision != expected.revision:
                    raise StorageConflictError()
                if current_total is not None and current_total > cumulative_total:
                    raise StorageConflictError()
                self._save_json_file(
                    self.file_path,
                    {"items": records, "cumulative_total": cumulative_total},
                    "Update accounts data and cumulative total",
                    expected_revision=expected.revision,
                    minimum_cumulative_total=cumulative_total,
                )
                return next_snapshot
        except Exception as exc:
            print(f"[git-storage] save failed: {exception_log_message(exc)}")
            raise

    def load_auth_keys(self) -> list[dict[str, Any]]:
        """从 Git 仓库加载鉴权密钥数据"""
        try:
            return self._parse_auth_keys_document(
                self._load_json_value(self.auth_keys_file_path)
            )
        except Exception as exc:
            print(f"[git-storage] load failed: {exception_log_message(exc)}")
            raise

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存鉴权密钥数据到 Git 仓库"""
        auth_keys = validate_storage_records(auth_keys)
        try:
            with self._mutation_lock("auth_keys"):
                self._save_json_file(self.auth_keys_file_path, {"items": auth_keys}, "Update auth keys data")
        except Exception as exc:
            print(f"[git-storage] save failed: {exception_log_message(exc)}")
            raise

    def save_auth_keys_if_revision(
        self,
        expected: StorageSnapshot,
        auth_keys: list[dict[str, Any]],
    ) -> StorageSnapshot:
        records = validate_storage_records(auth_keys)
        next_snapshot = make_storage_snapshot(records)
        try:
            with self._mutation_lock("auth_keys"):
                current = self.load_auth_keys_snapshot()
                if current.revision != expected.revision:
                    raise StorageConflictError()
                self._save_json_file(
                    self.auth_keys_file_path,
                    {"items": records},
                    "Update auth keys data",
                    expected_revision=expected.revision,
                )
                return next_snapshot
        except Exception as exc:
            print(f"[git-storage] save failed: {exception_log_message(exc)}")
            raise

    def _load_json_file(self, file_path: str) -> list[dict[str, Any]]:
        data = self._load_json_value(file_path)
        if data is _MISSING:
            return []
        return validate_storage_records(data)

    def _load_accounts_document(self) -> tuple[list[dict[str, Any]], int | None]:
        data = self._load_json_value(self.file_path)
        return self._parse_accounts_document(data)

    @staticmethod
    def _parse_accounts_document(data: Any) -> tuple[list[dict[str, Any]], int | None]:
        if data is _MISSING:
            return [], None
        cumulative_total = None
        if isinstance(data, dict):
            raw_total = data.get("cumulative_total")
            if type(raw_total) is not int or raw_total < 0:
                raise StorageDataError()
            cumulative_total = raw_total
            data = data.get("items")
        return validate_storage_records(data), cumulative_total

    @staticmethod
    def _parse_auth_keys_document(data: Any) -> list[dict[str, Any]]:
        if data is _MISSING:
            return []
        if isinstance(data, dict):
            data = data.get("items")
        return validate_storage_records(data)

    def _parse_document_for_revision(
        self,
        file_path: str,
        data: Any,
    ) -> tuple[list[dict[str, Any]], int | None]:
        if file_path == self.file_path:
            return self._parse_accounts_document(data)
        if file_path == self.auth_keys_file_path:
            return self._parse_auth_keys_document(data), None
        raise StorageDataError()

    def _load_json_value(self, file_path: str) -> Any:
        with self._mutation_lock("repository"):
            repo = self._clone_or_pull()
            try:
                return self._load_json_value_from_repo(repo, file_path)
            finally:
                self._close_repo(repo)

    def _save_json_file(
        self,
        file_path: str,
        items: Any,
        message: str,
        *,
        expected_revision: str | None = None,
        minimum_cumulative_total: int | None = None,
    ) -> None:
        with self._mutation_lock("repository"):
            repo = self._clone_or_pull()
            file_full_path = Path(repo.working_dir) / file_path
            pending_push = self.local_cache_dir / _PENDING_PUSH_MARKER
            try:
                if expected_revision is not None:
                    current_data = self._load_json_value_from_repo(repo, file_path)
                    current_records, current_total = self._parse_document_for_revision(
                        file_path,
                        current_data,
                    )
                    if make_storage_snapshot(current_records).revision != expected_revision:
                        raise StorageConflictError()
                    if (
                        minimum_cumulative_total is not None
                        and current_total is not None
                        and current_total > minimum_cumulative_total
                    ):
                        raise StorageConflictError()
                atomic_write_bytes(pending_push, self.local_cache_dir, b"pending\n")
                file_full_path.parent.mkdir(parents=True, exist_ok=True)
                worktree_root = Path(repo.working_dir)
                try:
                    worktree_stat = worktree_root.stat()
                except OSError as exc:
                    raise StorageDataError() from exc
                atomic_write_bytes(
                    file_full_path,
                    worktree_root,
                    (json.dumps(items, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
                    expected_root_identity=(worktree_stat.st_dev, worktree_stat.st_ino),
                )
                repo.index.add([file_path])
                if repo.is_dirty():
                    repo.index.commit(message)
                    push_result = repo.remote("origin").push(self.branch)
                    if self._push_result_failed(push_result):
                        raise StorageDataError()
                pending_push.unlink(missing_ok=True)
            except StorageConflictError:
                raise
            except Exception as exc:
                try:
                    self._restore_repo_from_remote(repo)
                    pending_push.unlink(missing_ok=True)
                except Exception as recovery_exc:
                    raise StorageDataError() from recovery_exc
                raise StorageDataError() from exc
            finally:
                self._close_repo(repo)

    @staticmethod
    def _push_result_failed(push_result: object) -> bool:
        try:
            infos = list(push_result)  # type: ignore[arg-type]
        except Exception as exc:
            raise StorageDataError() from exc
        if not infos:
            raise StorageDataError()
        for info in infos:
            flags = int(getattr(info, "flags", 0))
            if flags & _PUSH_FAILURE_FLAGS or not flags & _PUSH_SUCCESS_FLAGS:
                return True
        return False

    def _restore_repo_from_remote(self, repo: Repo) -> None:
        """Discard an unpushable local commit and pin the cache to origin."""
        origin = repo.remote("origin")
        origin.fetch(self.branch)
        repo.git.reset("--hard", f"origin/{self.branch}")
        repo.git.clean("-fd", "--", self.file_path, self.auth_keys_file_path)

    def health_check(self) -> dict[str, Any]:
        """健康检查"""
        try:
            with self._mutation_lock("repository"):
                repo = self._clone_or_pull()
                try:
                    account_data = self._load_json_value_from_repo(repo, self.file_path)
                    auth_key_data = self._load_json_value_from_repo(repo, self.auth_keys_file_path)
                    if account_data is not _MISSING:
                        if isinstance(account_data, dict):
                            raw_total = account_data.get("cumulative_total")
                            if type(raw_total) is not int or raw_total < 0:
                                raise StorageDataError()
                            account_data = account_data.get("items")
                        validate_storage_records(account_data)
                    if auth_key_data is not _MISSING:
                        if isinstance(auth_key_data, dict):
                            auth_key_data = auth_key_data.get("items")
                        validate_storage_records(auth_key_data)
                    return {
                        "status": "healthy",
                        "backend": "git",
                        "repo_url": self._mask_token(self.repo_url),
                        "branch": self.branch,
                        "file_path": self.file_path,
                        "auth_keys_file_path": self.auth_keys_file_path,
                        "last_commit": repo.head.commit.hexsha[:8],
                    }
                finally:
                    self._close_repo(repo)
        except Exception:
            return {
                "status": "unhealthy",
                "backend": "git",
                "error": STORAGE_HEALTH_ERROR_MESSAGE,
            }

    @staticmethod
    def _load_json_value_from_repo(repo: Repo, file_path: str) -> Any:
        file_full_path = Path(repo.working_dir) / file_path
        try:
            payload = read_checked_file_bytes(file_full_path, Path(repo.working_dir))
        except FileNotFoundError:
            return _MISSING
        try:
            return json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise StorageDataError() from exc

    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        return {
            "type": "git",
            "description": "Git 私有仓库存储",
            "repo_url": self._mask_token(self.repo_url),
            "branch": self.branch,
            "file_path": self.file_path,
            "auth_keys_file_path": self.auth_keys_file_path,
        }

    @staticmethod
    def _mask_token(url: str) -> str:
        """隐藏 URL 中的完整 userinfo。"""
        return redact_url_credentials(url)
