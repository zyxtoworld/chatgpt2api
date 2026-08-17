from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


@dataclass
class OpenedFile:
    file: BinaryIO
    filename: str
    stat_result: os.stat_result


def _lexical_absolute(path: Path) -> Path:
    """Return an absolute lexical path without following links."""
    return Path(os.path.abspath(os.fspath(Path(path))))


def authorized_root(root: Path) -> Path:
    """Normalize an authorization root without allowing it to become a link target."""
    root = _lexical_absolute(Path(root))
    if has_link(root, ()):
        raise OSError("authorized root must not be a link or reparse point")
    if root.exists() and not root.is_dir():
        raise OSError("authorized root is not a directory")
    return root


def resolve_under_root(root: Path, relative_path: str | Path) -> Path:
    root = authorized_root(root)
    relative = Path(relative_path)
    if relative.is_absolute():
        raise OSError("file path must be relative")
    lexical_path = root / relative
    try:
        relative_parts = lexical_path.relative_to(root).parts
    except ValueError as exc:
        raise OSError("file is outside the configured root") from exc
    if (
        not relative_parts
        or any(part in {"", ".", ".."} or "/" in part or "\\" in part for part in relative_parts)
    ):
        raise OSError("file path is invalid")
    if has_link(root, relative_parts):
        raise OSError("linked file path is not downloadable")
    return lexical_path


def has_link(root: Path, relative_parts: tuple[str, ...]) -> bool:
    current = _lexical_absolute(Path(root))
    while True:
        if current.is_symlink():
            return True
        is_junction = getattr(current, "is_junction", None)
        if os.name == "nt" and not callable(is_junction):
            raise ValueError("reparse-point inspection is unavailable")
        if callable(is_junction) and is_junction():
            return True
        parent = current.parent
        if parent == current:
            break
        current = parent

    current = _lexical_absolute(Path(root))
    for part in relative_parts:
        current = current / part
        if current.is_symlink():
            return True
        is_junction = getattr(current, "is_junction", None)
        if os.name == "nt" and not callable(is_junction):
            raise ValueError("reparse-point inspection is unavailable")
        if callable(is_junction) and is_junction():
            return True
    return False


def normalize_windows_handle_path(value: str) -> str:
    if value.startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return os.path.normcase(os.path.normpath(value))


def validate_windows_handle_path(final_path: str, expected_path: Path, expected_dir: Path) -> None:
    normalized_final = normalize_windows_handle_path(final_path)
    normalized_expected = normalize_windows_handle_path(str(expected_path))
    normalized_dir = normalize_windows_handle_path(str(expected_dir))
    try:
        Path(normalized_final).relative_to(Path(normalized_dir))
    except ValueError as exc:
        raise OSError("Windows file escaped the owner task directory") from exc
    if normalized_final != normalized_expected:
        raise OSError("Windows file escaped the validated task directory")


def _open_posix_file(path: Path, root: Path) -> BinaryIO:
    try:
        relative_parts = path.relative_to(root).parts
    except ValueError as exc:
        raise OSError("file is outside the configured root") from exc
    if (
        not relative_parts
        or any(part in {"", ".", ".."} or "/" in part or "\\" in part for part in relative_parts)
    ):
        raise OSError("file path is invalid")
    no_follow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if no_follow is None or directory is None or os.open not in os.supports_dir_fd:
        raise OSError("safe relative file open is unavailable")
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    root_flags = os.O_RDONLY | directory | no_follow | close_on_exec
    child_dir_flags = os.O_RDONLY | directory | no_follow | close_on_exec
    file_flags = os.O_RDONLY | no_follow | close_on_exec
    current_fd = os.open(root, root_flags)
    try:
        for part in relative_parts[:-1]:
            next_fd = os.open(part, child_dir_flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        file_descriptor = os.open(relative_parts[-1], file_flags, dir_fd=current_fd)
        try:
            return os.fdopen(file_descriptor, "rb", closefd=True)
        except Exception:
            os.close(file_descriptor)
            raise
    finally:
        os.close(current_fd)


def _open_windows_file(path: Path, expected_dir: Path) -> BinaryIO:
    import ctypes
    import msvcrt

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle_type = ctypes.c_void_p
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
    ]
    create_file.restype = handle_type
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [handle_type]
    close_handle.restype = ctypes.c_int
    get_file_information = getattr(kernel32, "GetFileInformationByHandleEx", None)
    get_final_path = getattr(kernel32, "GetFinalPathNameByHandleW", None)
    if get_file_information is None or get_final_path is None:
        raise OSError("safe Windows file open is unavailable")
    get_file_information.argtypes = [handle_type, ctypes.c_int, handle_type, ctypes.c_uint32]
    get_file_information.restype = ctypes.c_int
    get_final_path.argtypes = [handle_type, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
    get_final_path.restype = ctypes.c_uint32

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", ctypes.c_uint32),
            ("reparse_tag", ctypes.c_uint32),
        ]

    generic_read = 0x80000000
    share_read = 0x00000001
    share_write = 0x00000002
    share_delete = 0x00000004
    open_existing = 3
    file_attribute_normal = 0x00000080
    file_flag_open_reparse_point = 0x00200000
    file_attribute_tag_info = 9
    file_attribute_reparse_point = 0x00000400
    invalid_handle = ctypes.c_void_p(-1).value
    handle = create_file(
        str(path),
        generic_read,
        share_read | share_write | share_delete,
        None,
        open_existing,
        file_attribute_normal | file_flag_open_reparse_point,
        None,
    )
    if handle in {None, invalid_handle}:
        error_code = ctypes.get_last_error()
        raise OSError(error_code, "safe Windows file open failed")
    file_descriptor = None
    try:
        tag_info = FileAttributeTagInfo()
        if not get_file_information(
            handle,
            file_attribute_tag_info,
            ctypes.byref(tag_info),
            ctypes.sizeof(tag_info),
        ):
            raise OSError(ctypes.get_last_error(), "Windows file attributes unavailable")
        if tag_info.file_attributes & file_attribute_reparse_point:
            raise OSError("reparse-point file is not downloadable")

        buffer = ctypes.create_unicode_buffer(32768)
        length = get_final_path(handle, buffer, len(buffer), 0)
        if not length or length >= len(buffer):
            raise OSError("Windows final file path unavailable")
        validate_windows_handle_path(buffer.value[:length], path, expected_dir)

        file_descriptor = msvcrt.open_osfhandle(int(handle), os.O_RDONLY | os.O_BINARY)
        handle = None
        try:
            return os.fdopen(file_descriptor, "rb", closefd=True)
        except Exception:
            os.close(file_descriptor)
            raise
    finally:
        if handle not in {None, invalid_handle}:
            close_handle(handle)


def open_no_follow_file(path: Path, root: Path, expected_dir: Path) -> BinaryIO:
    path = _lexical_absolute(Path(path))
    root = authorized_root(root)
    expected_dir = _lexical_absolute(Path(expected_dir))
    if not path.is_absolute() or not expected_dir.is_absolute():
        raise OSError("safe file open requires absolute paths")
    try:
        path.relative_to(expected_dir)
        expected_dir.relative_to(root)
    except ValueError as exc:
        raise OSError("file is outside its authorized directory") from exc
    relative_parts = path.relative_to(root).parts
    if not relative_parts or has_link(root, relative_parts):
        raise OSError("linked file path is not downloadable")
    if os.name == "nt":
        return _open_windows_file(path, expected_dir)
    return _open_posix_file(path, root)


def open_checked_file(path: Path, root: Path, expected_dir: Path) -> OpenedFile:
    path = _lexical_absolute(Path(path))
    root = authorized_root(root)
    expected_dir = _lexical_absolute(Path(expected_dir))
    try:
        path.relative_to(expected_dir)
        expected_dir.relative_to(root)
    except ValueError as exc:
        raise OSError("file is outside its authorized directory") from exc
    try:
        validated_stat = os.stat(path, follow_symlinks=False)
    except OSError:
        raise
    if not stat.S_ISREG(validated_stat.st_mode) or not validated_stat.st_ino:
        raise OSError("file is not a regular file")
    file = open_no_follow_file(path, root, expected_dir)
    try:
        opened_stat = os.fstat(file.fileno())
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or not opened_stat.st_ino
            or validated_stat.st_dev != opened_stat.st_dev
            or validated_stat.st_ino != opened_stat.st_ino
        ):
            raise OSError("file changed during secure open")
        return OpenedFile(file=file, filename=path.name, stat_result=opened_stat)
    except Exception:
        file.close()
        raise


def _open_posix_append_file(
    path: Path,
    root: Path,
    expected_root_identity: tuple[int, int] | None = None,
) -> BinaryIO:
    try:
        relative_parts = path.relative_to(root).parts
    except ValueError as exc:
        raise OSError("file is outside the configured root") from exc
    if (
        not relative_parts
        or any(part in {"", ".", ".."} or "/" in part or "\\" in part for part in relative_parts)
    ):
        raise OSError("file path is invalid")
    no_follow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if no_follow is None or directory is None or os.open not in os.supports_dir_fd:
        raise OSError("safe relative file append is unavailable")
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    current_fd = os.open(root, os.O_RDONLY | directory | no_follow | close_on_exec)
    try:
        if expected_root_identity is not None:
            root_stat = os.fstat(current_fd)
            if (root_stat.st_dev, root_stat.st_ino) != expected_root_identity:
                raise OSError("authorized root changed")
        for part in relative_parts[:-1]:
            next_fd = os.open(part, os.O_RDONLY | directory | no_follow | close_on_exec, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        try:
            expected_stat = os.stat(relative_parts[-1], dir_fd=current_fd, follow_symlinks=False)
            existed = True
        except FileNotFoundError:
            expected_stat = None
            existed = False
        flags = os.O_WRONLY | os.O_APPEND | no_follow | close_on_exec
        if not existed:
            flags |= os.O_CREAT | os.O_EXCL
        file_descriptor = os.open(relative_parts[-1], flags, 0o600, dir_fd=current_fd)
        try:
            opened_stat = os.fstat(file_descriptor)
            if expected_stat is not None and (
                expected_stat.st_dev != opened_stat.st_dev or expected_stat.st_ino != opened_stat.st_ino
            ):
                raise OSError("file changed during safe append open")
            if not stat.S_ISREG(opened_stat.st_mode) or opened_stat.st_nlink != 1:
                raise OSError("file is not a regular file")
            return os.fdopen(file_descriptor, "ab", closefd=True)
        except Exception:
            os.close(file_descriptor)
            raise
    finally:
        os.close(current_fd)


def _open_windows_append_file(
    path: Path,
    root: Path,
    expected_dir: Path,
    expected_root_identity: tuple[int, int] | None = None,
) -> BinaryIO:
    import ctypes
    import msvcrt

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle_type = ctypes.c_void_p
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32, handle_type,
        ctypes.c_uint32, ctypes.c_uint32, handle_type,
    ]
    create_file.restype = handle_type
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [handle_type]
    close_handle.restype = ctypes.c_int
    get_file_information = getattr(kernel32, "GetFileInformationByHandleEx", None)
    get_final_path = getattr(kernel32, "GetFinalPathNameByHandleW", None)
    if get_file_information is None or get_final_path is None:
        raise OSError("safe Windows file append is unavailable")
    get_file_information.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
    get_file_information.restype = ctypes.c_int
    get_final_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
    get_final_path.restype = ctypes.c_uint32

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [("file_attributes", ctypes.c_uint32), ("reparse_tag", ctypes.c_uint32)]

    generic_read = 0x80000000
    generic_write = 0x40000000
    share_all = 0x00000001 | 0x00000002 | 0x00000004
    share_read_write = 0x00000001 | 0x00000002
    open_existing = 3
    create_new = 1
    file_attribute_normal = 0x00000080
    file_flag_backup_semantics = 0x02000000
    file_flag_open_reparse_point = 0x00200000
    file_attribute_tag_info = 9
    file_attribute_reparse_point = 0x00000400
    invalid_handle = ctypes.c_void_p(-1).value
    try:
        directory_parts = expected_dir.relative_to(root).parts
    except ValueError as exc:
        raise OSError("safe Windows append directory is outside the root") from exc
    locked_directories = []
    current_dir = root
    handle = None
    file_descriptor = None
    try:
        for part in (None, *directory_parts):
            if part is not None:
                current_dir = current_dir / part
            directory_handle = _windows_open_directory_handle(
                create_file,
                current_dir,
                generic_read,
                share_read_write,
                open_existing,
                file_flag_backup_semantics | file_flag_open_reparse_point,
            )
            if directory_handle in {None, invalid_handle}:
                raise OSError(ctypes.get_last_error(), "safe Windows append directory open failed")
            locked_directories.append(directory_handle)
            directory_info = FileAttributeTagInfo()
            if not get_file_information(
                directory_handle,
                file_attribute_tag_info,
                ctypes.byref(directory_info),
                ctypes.sizeof(directory_info),
            ):
                raise OSError(ctypes.get_last_error(), "Windows append directory attributes unavailable")
            if directory_info.file_attributes & file_attribute_reparse_point:
                raise OSError("reparse-point directory is not appendable")
            validate_windows_handle_path(_windows_final_path(kernel32, directory_handle), current_dir, root)
            if part is None and expected_root_identity is not None:
                if not _windows_identity_matches(
                    expected_root_identity,
                    _windows_handle_identity(kernel32, directory_handle),
                ):
                    raise OSError("authorized root changed")

        try:
            expected_stat = os.stat(path, follow_symlinks=False)
            existed = True
        except FileNotFoundError:
            expected_stat = None
            existed = False
        creation = open_existing if existed else create_new
        handle = create_file(
            str(path), generic_write, share_all, None, creation,
            file_attribute_normal | file_flag_open_reparse_point, None,
        )
        if handle in {None, invalid_handle}:
            raise OSError(ctypes.get_last_error(), "safe Windows file append failed")
        tag_info = FileAttributeTagInfo()
        if not get_file_information(handle, file_attribute_tag_info, ctypes.byref(tag_info), ctypes.sizeof(tag_info)):
            raise OSError(ctypes.get_last_error(), "Windows file attributes unavailable")
        if tag_info.file_attributes & file_attribute_reparse_point:
            raise OSError("reparse-point file is not appendable")
        if _windows_handle_link_count(kernel32, handle) != 1:
            raise OSError("hard-linked file is not appendable")
        buffer = ctypes.create_unicode_buffer(32768)
        length = get_final_path(handle, buffer, len(buffer), 0)
        if not length or length >= len(buffer):
            raise OSError("Windows final file path unavailable")
        validate_windows_handle_path(buffer.value[:length], path, expected_dir)
        file_descriptor = msvcrt.open_osfhandle(int(handle), os.O_WRONLY | os.O_APPEND | os.O_BINARY)
        handle = None
        if expected_stat is not None:
            opened_stat = os.fstat(file_descriptor)
            if expected_stat.st_dev != opened_stat.st_dev or expected_stat.st_ino != opened_stat.st_ino:
                os.close(file_descriptor)
                file_descriptor = None
                raise OSError("file changed during safe append open")
        stream = os.fdopen(file_descriptor, "ab", closefd=True)
        file_descriptor = None
        return stream
    finally:
        if handle not in {None, invalid_handle}:
            close_handle(handle)
        elif file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError:
                pass
        for directory_handle in reversed(locked_directories):
            close_handle(directory_handle)


def append_checked_file_bytes(
    path: Path,
    root: Path,
    payload: bytes,
    *,
    expected_root_identity: tuple[int, int] | None = None,
) -> None:
    """Append bytes through an authorized, no-follow file handle."""
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    path = _lexical_absolute(Path(path))
    root = authorized_root(root)
    expected_dir = path.parent
    try:
        expected_dir.relative_to(root)
    except ValueError as exc:
        raise OSError("file is outside its authorized directory") from exc
    relative_parts = path.relative_to(root).parts
    if not relative_parts or has_link(root, relative_parts):
        raise OSError("linked file path is not appendable")
    file = (
        _open_windows_append_file(path, root, expected_dir, expected_root_identity)
        if os.name == "nt"
        else _open_posix_append_file(path, root, expected_root_identity)
    )
    try:
        file.write(payload)
        file.flush()
    finally:
        file.close()


def read_checked_file_bytes(path: Path, root: Path, *, max_bytes: int | None = None) -> bytes:
    """Read an existing regular file through one fixed, no-follow handle."""
    path = _lexical_absolute(Path(path))
    root = authorized_root(root)
    opened = open_checked_file(path, root, path.parent)
    try:
        if max_bytes is None:
            return opened.file.read()
        if type(max_bytes) is not int or max_bytes < 0:
            raise ValueError("max_bytes must be a non-negative integer")
        payload = opened.file.read(max_bytes + 1)
        if len(payload) > max_bytes:
            raise OSError("file exceeds the configured read limit")
        return payload
    finally:
        opened.file.close()


def _relative_file_parts(path: Path, root: Path) -> tuple[str, ...]:
    path = _lexical_absolute(path)
    root = _lexical_absolute(root)
    try:
        parts = path.relative_to(root).parts
    except ValueError as exc:
        raise OSError("file is outside the configured root") from exc
    if (
        not parts
        or any(part in {"", ".", ".."} or "/" in part or "\\" in part for part in parts)
    ):
        raise OSError("file path is invalid")
    return parts


def _open_posix_directory(
    root: Path,
    relative_parts: tuple[str, ...],
    expected_root_identity: tuple[int, int] | None = None,
) -> int:
    no_follow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if no_follow is None or directory is None or os.open not in os.supports_dir_fd:
        raise OSError("safe relative directory open is unavailable")
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    current_fd = os.open(root, os.O_RDONLY | directory | no_follow | close_on_exec)
    try:
        if expected_root_identity is not None:
            root_stat = os.fstat(current_fd)
            if (root_stat.st_dev, root_stat.st_ino) != expected_root_identity:
                raise OSError("authorized root changed")
        for part in relative_parts:
            next_fd = os.open(
                part,
                os.O_RDONLY | directory | no_follow | close_on_exec,
                dir_fd=current_fd,
            )
            os.close(current_fd)
            current_fd = next_fd
        return current_fd
    except Exception:
        os.close(current_fd)
        raise


def _relative_directory_parts(directory: Path, root: Path) -> tuple[str, ...]:
    directory = _lexical_absolute(directory)
    root = _lexical_absolute(root)
    try:
        parts = Path(directory).relative_to(Path(root)).parts
    except ValueError as exc:
        raise OSError("directory is outside the configured root") from exc
    if any(part in {"", ".", ".."} or "/" in part or "\\" in part for part in parts):
        raise OSError("directory path is invalid")
    return parts


def _ensure_posix_directory(root: Path, relative_parts: tuple[str, ...]) -> None:
    current_fd = _open_posix_directory(root, ())
    try:
        for part in relative_parts:
            try:
                next_fd = os.open(
                    part,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY")
                    | getattr(os, "O_NOFOLLOW")
                    | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=current_fd,
                )
            except FileNotFoundError:
                try:
                    os.mkdir(part, 0o700, dir_fd=current_fd)
                except FileExistsError:
                    pass
                next_fd = os.open(
                    part,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY")
                    | getattr(os, "O_NOFOLLOW")
                    | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=current_fd,
                )
            os.close(current_fd)
            current_fd = next_fd
    finally:
        os.close(current_fd)


def _ensure_windows_directory(root: Path, directory: Path) -> None:
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle_type = ctypes.c_void_p
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
    ]
    create_file.restype = handle_type
    create_directory = kernel32.CreateDirectoryW
    create_directory.argtypes = [ctypes.c_wchar_p, handle_type]
    create_directory.restype = ctypes.c_int
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [handle_type]
    close_handle.restype = ctypes.c_int
    get_file_information = getattr(kernel32, "GetFileInformationByHandleEx", None)
    if get_file_information is None:
        raise OSError("safe Windows directory creation is unavailable")

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", ctypes.c_uint32),
            ("reparse_tag", ctypes.c_uint32),
        ]

    get_file_information.argtypes = [handle_type, ctypes.c_int, handle_type, ctypes.c_uint32]
    get_file_information.restype = ctypes.c_int
    generic_read = 0x80000000
    share_read_write = 0x00000001 | 0x00000002
    open_existing = 3
    file_flag_open_reparse_point = 0x00200000
    file_flag_backup_semantics = 0x02000000
    file_attribute_tag_info = 9
    file_attribute_reparse_point = 0x00000400
    error_already_exists = 183
    invalid_handle = ctypes.c_void_p(-1).value

    parts = _relative_directory_parts(directory, root)
    locked_directories = []
    current_dir = root
    try:
        for part in (None, *parts):
            if part is not None:
                current_dir = current_dir / part
                if not create_directory(str(current_dir), None):
                    error_code = ctypes.get_last_error()
                    if error_code != error_already_exists:
                        raise OSError(error_code, "safe Windows directory creation failed")
            directory_handle = create_file(
                str(current_dir),
                generic_read,
                share_read_write,
                None,
                open_existing,
                file_flag_backup_semantics | file_flag_open_reparse_point,
                None,
            )
            if directory_handle in {None, invalid_handle}:
                raise OSError(ctypes.get_last_error(), "safe Windows directory open failed")
            locked_directories.append(directory_handle)
            directory_info = FileAttributeTagInfo()
            if not get_file_information(
                directory_handle,
                file_attribute_tag_info,
                ctypes.byref(directory_info),
                ctypes.sizeof(directory_info),
            ):
                raise OSError(ctypes.get_last_error(), "Windows directory attributes unavailable")
            if directory_info.file_attributes & file_attribute_reparse_point:
                raise OSError("reparse-point directory is not writable")
            validate_windows_handle_path(_windows_final_path(kernel32, directory_handle), current_dir, root)
    finally:
        for directory_handle in reversed(locked_directories):
            close_handle(directory_handle)


def ensure_directory(root: Path, directory: Path) -> None:
    root = authorized_root(root)
    directory = _lexical_absolute(Path(directory))
    if not root.is_absolute() or not directory.is_absolute():
        raise OSError("safe directory creation requires absolute paths")
    if not root.exists():
        parent = root.parent
        if not parent.is_dir():
            raise OSError("safe directory root is unavailable")
        ensure_directory(parent, root)
    parts = _relative_directory_parts(directory, root)
    if has_link(root, parts):
        raise OSError("linked directory is not writable")
    if os.name == "nt":
        _ensure_windows_directory(root, directory)
    else:
        _ensure_posix_directory(root, parts)


def _apply_file_metadata(
    temp_fd: int,
    mode: int,
    owner: tuple[int, int] | None,
) -> None:
    fchmod = getattr(os, "fchmod", None)
    if not callable(fchmod):
        raise OSError("safe POSIX file mode restoration is unavailable")
    fchmod(temp_fd, mode)

    if owner is None:
        return
    fchown = getattr(os, "fchown", None)
    if not callable(fchown):
        raise OSError("safe POSIX file owner restoration is unavailable")
    fchown(temp_fd, owner[0], owner[1])


def _atomic_write_posix(
    path: Path,
    root: Path,
    payload: bytes | Iterable[bytes],
    mode: int = 0o600,
    owner: tuple[int, int] | None = None,
    expected_root_identity: tuple[int, int] | None = None,
) -> None:
    parts = _relative_file_parts(path, root)
    parent_fd = _open_posix_directory(root, parts[:-1], expected_root_identity)
    temp_name = f".{parts[-1]}.{secrets.token_hex(12)}.tmp"
    temp_fd = None
    temp_created = False
    try:
        temp_fd = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        temp_created = True
        chunks = (payload,) if isinstance(payload, bytes) else payload
        for chunk in chunks:
            if not isinstance(chunk, bytes):
                raise OSError("secure file chunks must be bytes")
            view = memoryview(chunk)
            while view:
                written = os.write(temp_fd, view)
                if not written:
                    raise OSError("short secure file write")
                view = view[written:]
        _apply_file_metadata(temp_fd, mode, owner)
        os.fsync(temp_fd)
        os.close(temp_fd)
        temp_fd = None
        os.replace(temp_name, parts[-1], src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.fsync(parent_fd)
    finally:
        if temp_fd is not None:
            os.close(temp_fd)
        if temp_created:
            try:
                os.unlink(temp_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
        os.close(parent_fd)


def _delete_windows_file(path: Path, root: Path) -> bool:
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle_type = ctypes.c_void_p
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
    ]
    create_file.restype = handle_type
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [handle_type]
    close_handle.restype = ctypes.c_int
    get_file_information = getattr(kernel32, "GetFileInformationByHandleEx", None)
    set_file_information = getattr(kernel32, "SetFileInformationByHandle", None)
    if get_file_information is None or set_file_information is None:
        raise OSError("safe Windows file delete is unavailable")

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", ctypes.c_uint32),
            ("reparse_tag", ctypes.c_uint32),
        ]

    get_file_information.argtypes = [handle_type, ctypes.c_int, handle_type, ctypes.c_uint32]
    get_file_information.restype = ctypes.c_int
    set_file_information.argtypes = [handle_type, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
    set_file_information.restype = ctypes.c_int

    generic_read = 0x80000000
    delete_access = 0x00010000
    share_read_write = 0x00000001 | 0x00000002
    open_existing = 3
    file_flag_open_reparse_point = 0x00200000
    file_flag_backup_semantics = 0x02000000
    file_attribute_tag_info = 9
    file_attribute_reparse_point = 0x00000400
    file_disposition_info = 4
    invalid_handle = ctypes.c_void_p(-1).value

    try:
        directory_parts = path.parent.relative_to(root).parts
    except ValueError as exc:
        raise OSError("safe Windows file is outside the root") from exc
    locked_directories = []
    current_dir = root
    file_handle = None
    try:
        for part in (None, *directory_parts):
            if part is not None:
                current_dir = current_dir / part
            directory_handle = create_file(
                str(current_dir),
                generic_read,
                share_read_write,
                None,
                open_existing,
                file_flag_backup_semantics | file_flag_open_reparse_point,
                None,
            )
            if directory_handle in {None, invalid_handle}:
                raise OSError(ctypes.get_last_error(), "safe Windows directory open failed")
            locked_directories.append(directory_handle)
            directory_info = FileAttributeTagInfo()
            if not get_file_information(
                directory_handle,
                file_attribute_tag_info,
                ctypes.byref(directory_info),
                ctypes.sizeof(directory_info),
            ):
                raise OSError(ctypes.get_last_error(), "Windows directory attributes unavailable")
            if directory_info.file_attributes & file_attribute_reparse_point:
                raise OSError("reparse-point directory is not deletable")
            validate_windows_handle_path(_windows_final_path(kernel32, directory_handle), current_dir, root)

        file_handle = create_file(
            str(path),
            generic_read | delete_access,
            share_read_write,
            None,
            open_existing,
            file_flag_open_reparse_point,
            None,
        )
        if file_handle in {None, invalid_handle}:
            raise FileNotFoundError(str(path))
        file_info = FileAttributeTagInfo()
        if not get_file_information(
            file_handle,
            file_attribute_tag_info,
            ctypes.byref(file_info),
            ctypes.sizeof(file_info),
        ):
            raise OSError(ctypes.get_last_error(), "Windows file attributes unavailable")
        if file_info.file_attributes & file_attribute_reparse_point:
            raise OSError("reparse-point file is not deletable")
        validate_windows_handle_path(_windows_final_path(kernel32, file_handle), path, path.parent)
        disposition = ctypes.c_ubyte(1)
        if not set_file_information(
            file_handle,
            file_disposition_info,
            ctypes.byref(disposition),
            ctypes.sizeof(disposition),
        ):
            raise OSError(ctypes.get_last_error(), "safe Windows file delete failed")
        return True
    finally:
        if file_handle not in {None, invalid_handle}:
            close_handle(file_handle)
        for directory_handle in reversed(locked_directories):
            close_handle(directory_handle)


def delete_checked_file(path: Path, root: Path) -> bool:
    path = _lexical_absolute(Path(path))
    root = authorized_root(root)
    if not path.is_absolute():
        raise OSError("safe file delete requires an absolute path")
    parts = _relative_file_parts(path, root)
    if has_link(root, parts[:-1]):
        raise OSError("linked parent directory is not deletable")
    if os.name == "nt":
        return _delete_windows_file(path, root)
    parent_fd = _open_posix_directory(root, parts[:-1])
    file_fd = None
    try:
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise OSError("safe file delete is unavailable")
        file_fd = os.open(parts[-1], os.O_RDONLY | no_follow, dir_fd=parent_fd)
        file_stat = os.fstat(file_fd)
        if not stat.S_ISREG(file_stat.st_mode) or not file_stat.st_ino:
            raise OSError("file is not a regular file")
        os.close(file_fd)
        file_fd = None
        os.unlink(parts[-1], dir_fd=parent_fd)
        return True
    except FileNotFoundError:
        return False
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(parent_fd)


def _windows_final_path(kernel32, handle) -> str:
    import ctypes

    get_final_path = getattr(kernel32, "GetFinalPathNameByHandleW", None)
    if get_final_path is None:
        raise OSError("safe Windows final path check is unavailable")
    get_final_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
    get_final_path.restype = ctypes.c_uint32
    buffer = ctypes.create_unicode_buffer(32768)
    length = get_final_path(handle, buffer, len(buffer), 0)
    if not length or length >= len(buffer):
        raise OSError("Windows final path unavailable")
    return buffer.value[:length]


def _windows_handle_identity(kernel32, handle) -> tuple[int, int]:
    """Return the native Windows volume serial and file-index identity."""
    import ctypes

    get_file_information = getattr(kernel32, "GetFileInformationByHandle", None)
    if get_file_information is None:
        raise OSError("Windows file identity is unavailable")

    class FileTime(ctypes.Structure):
        _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    class ByHandleInformation(ctypes.Structure):
        _fields_ = [
            ("file_attributes", ctypes.c_uint32),
            ("creation_time", FileTime),
            ("last_access_time", FileTime),
            ("last_write_time", FileTime),
            ("volume_serial_number", ctypes.c_uint32),
            ("file_size_high", ctypes.c_uint32),
            ("file_size_low", ctypes.c_uint32),
            ("number_of_links", ctypes.c_uint32),
            ("file_index_high", ctypes.c_uint32),
            ("file_index_low", ctypes.c_uint32),
        ]

    get_file_information.argtypes = [ctypes.c_void_p, ctypes.POINTER(ByHandleInformation)]
    get_file_information.restype = ctypes.c_int
    info = ByHandleInformation()
    if not get_file_information(handle, ctypes.byref(info)):
        raise OSError(ctypes.get_last_error(), "Windows file identity unavailable")
    file_index = (int(info.file_index_high) << 32) | int(info.file_index_low)
    return int(info.volume_serial_number), file_index


def _windows_handle_link_count(kernel32, handle) -> int:
    """Return the hard-link count for one already-open Windows file handle."""
    import ctypes

    get_file_information = getattr(kernel32, "GetFileInformationByHandle", None)
    if get_file_information is None:
        raise OSError("Windows file link metadata unavailable")

    class FileTime(ctypes.Structure):
        _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    class ByHandleInformation(ctypes.Structure):
        _fields_ = [
            ("file_attributes", ctypes.c_uint32),
            ("creation_time", FileTime),
            ("last_access_time", FileTime),
            ("last_write_time", FileTime),
            ("volume_serial_number", ctypes.c_uint32),
            ("file_size_high", ctypes.c_uint32),
            ("file_size_low", ctypes.c_uint32),
            ("number_of_links", ctypes.c_uint32),
            ("file_index_high", ctypes.c_uint32),
            ("file_index_low", ctypes.c_uint32),
        ]

    get_file_information.argtypes = [ctypes.c_void_p, ctypes.POINTER(ByHandleInformation)]
    get_file_information.restype = ctypes.c_int
    info = ByHandleInformation()
    if not get_file_information(handle, ctypes.byref(info)):
        raise OSError(ctypes.get_last_error(), "Windows file link metadata unavailable")
    return int(info.number_of_links)


def _windows_identity_matches(
    expected: tuple[int, int],
    actual: tuple[int, int],
) -> bool:
    """Match a Python Windows stat identity to native handle identity."""
    # CPython keeps the volume serial in the low 32 bits of st_dev while
    # GetFileInformationByHandle returns that serial directly.
    return (int(expected[0]) & 0xFFFFFFFF, int(expected[1])) == actual


def _windows_open_directory_handle(
    create_file,
    path: Path,
    access: int,
    share_mode: int,
    open_existing: int,
    flags: int,
):
    """Open one directory by handle; callers keep the handle until the operation ends."""
    return create_file(
        str(path),
        access,
        share_mode,
        None,
        open_existing,
        flags,
        None,
    )


def _atomic_write_windows(
    path: Path,
    root: Path,
    expected_dir: Path,
    payload: bytes | Iterable[bytes],
    mode: int,
    owner: tuple[int, int] | None,
    expected_root_identity: tuple[int, int] | None = None,
) -> None:
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle_type = ctypes.c_void_p
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
        ctypes.c_uint32,
        ctypes.c_uint32,
        handle_type,
    ]
    create_file.restype = handle_type
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [handle_type]
    close_handle.restype = ctypes.c_int
    get_file_information = getattr(kernel32, "GetFileInformationByHandleEx", None)
    write_file = getattr(kernel32, "WriteFile", None)
    flush_file_buffers = getattr(kernel32, "FlushFileBuffers", None)
    move_file = getattr(kernel32, "MoveFileExW", None)
    delete_file = getattr(kernel32, "DeleteFileW", None)
    if any(item is None for item in (get_file_information, write_file, flush_file_buffers, move_file, delete_file)):
        raise OSError("safe Windows file write is unavailable")

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", ctypes.c_uint32),
            ("reparse_tag", ctypes.c_uint32),
        ]

    get_file_information.argtypes = [handle_type, ctypes.c_int, handle_type, ctypes.c_uint32]
    get_file_information.restype = ctypes.c_int
    write_file.argtypes = [handle_type, ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32), handle_type]
    write_file.restype = ctypes.c_int
    flush_file_buffers.argtypes = [handle_type]
    flush_file_buffers.restype = ctypes.c_int
    move_file.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
    move_file.restype = ctypes.c_int
    delete_file.argtypes = [ctypes.c_wchar_p]
    delete_file.restype = ctypes.c_int

    generic_read = 0x80000000
    generic_write = 0x40000000
    share_read_write = 0x00000001 | 0x00000002
    share_all = share_read_write | 0x00000004
    open_existing = 3
    create_new = 1
    file_attribute_normal = 0x00000080
    file_flag_open_reparse_point = 0x00200000
    file_flag_backup_semantics = 0x02000000
    file_attribute_tag_info = 9
    file_attribute_reparse_point = 0x00000400
    movefile_replace_existing = 0x00000001
    movefile_write_through = 0x00000008
    invalid_handle = ctypes.c_void_p(-1).value

    try:
        directory_parts = expected_dir.relative_to(root).parts
    except ValueError as exc:
        raise OSError("safe Windows directory is outside the root") from exc
    locked_directories = []
    current_dir = root
    temp_handle = None
    temp_created = False
    temp_path = expected_dir / f".{path.name}.{secrets.token_hex(12)}.tmp"
    renamed = False
    try:
        for part in (None, *directory_parts):
            if part is not None:
                current_dir = current_dir / part
            directory_handle = _windows_open_directory_handle(
                create_file,
                current_dir,
                generic_read,
                share_read_write,
                open_existing,
                file_flag_backup_semantics | file_flag_open_reparse_point,
            )
            if directory_handle in {None, invalid_handle}:
                raise OSError(ctypes.get_last_error(), "safe Windows directory open failed")
            locked_directories.append(directory_handle)
            directory_info = FileAttributeTagInfo()
            if not get_file_information(
                directory_handle,
                file_attribute_tag_info,
                ctypes.byref(directory_info),
                ctypes.sizeof(directory_info),
            ):
                raise OSError(ctypes.get_last_error(), "Windows directory attributes unavailable")
            if directory_info.file_attributes & file_attribute_reparse_point:
                raise OSError("reparse-point directory is not writable")
            validate_windows_handle_path(_windows_final_path(kernel32, directory_handle), current_dir, root)
            if part is None and expected_root_identity is not None:
                if not _windows_identity_matches(
                    expected_root_identity,
                    _windows_handle_identity(kernel32, directory_handle),
                ):
                    raise OSError("authorized root changed")

        temp_handle = create_file(
            str(temp_path),
            generic_read | generic_write,
            share_all,
            None,
            create_new,
            file_attribute_normal | file_flag_open_reparse_point,
            None,
        )
        if temp_handle in {None, invalid_handle}:
            raise OSError(ctypes.get_last_error(), "safe Windows temporary file open failed")
        temp_created = True
        validate_windows_handle_path(_windows_final_path(kernel32, temp_handle), temp_path, expected_dir)

        chunks = (payload,) if isinstance(payload, bytes) else payload
        for raw_chunk in chunks:
            if not isinstance(raw_chunk, bytes):
                raise OSError("secure file chunks must be bytes")
            remaining = memoryview(raw_chunk)
            while remaining:
                chunk = remaining[:1024 * 1024]
                buffer = ctypes.create_string_buffer(bytes(chunk))
                written = ctypes.c_uint32()
                if not write_file(temp_handle, buffer, len(chunk), ctypes.byref(written), None) or written.value != len(chunk):
                    raise OSError(ctypes.get_last_error(), "safe Windows file write failed")
                remaining = remaining[written.value:]
        chmod = getattr(os, "chmod", None)
        if not callable(chmod):
            raise OSError("safe Windows file mode restoration is unavailable")
        chmod(temp_path, mode)
        if owner is not None:
            chown = getattr(os, "chown", None)
            if not callable(chown):
                raise OSError("safe Windows file owner restoration is unavailable")
            chown(temp_path, owner[0], owner[1])
        if not flush_file_buffers(temp_handle):
            raise OSError(ctypes.get_last_error(), "safe Windows file flush failed")
        close_handle(temp_handle)
        temp_handle = None
        if not move_file(
            str(temp_path),
            str(path),
            movefile_replace_existing | movefile_write_through,
        ):
            raise OSError(ctypes.get_last_error(), "safe Windows file replace failed")
        renamed = True
    finally:
        if temp_handle not in {None, invalid_handle}:
            close_handle(temp_handle)
        if not renamed and temp_created:
            delete_file(str(temp_path))
        for directory_handle in reversed(locked_directories):
            close_handle(directory_handle)


def atomic_write_stream(
    path: Path,
    root: Path,
    chunks: Iterable[bytes],
    *,
    mode: int = 0o600,
    owner: tuple[int, int] | None = None,
    expected_root_identity: tuple[int, int] | None = None,
) -> None:
    path = _lexical_absolute(Path(path))
    root = authorized_root(root)
    expected_dir = path.parent
    if not path.is_absolute() or not expected_dir.is_absolute():
        raise OSError("safe file write requires absolute paths")
    _relative_file_parts(path, root)
    if expected_dir != root:
        expected_dir.relative_to(root)
    ensure_directory(root, expected_dir)
    if has_link(root, path.relative_to(root).parts):
        raise OSError("linked file path is not writable")
    if os.name == "nt":
        _atomic_write_windows(path, root, expected_dir, chunks, mode, owner, expected_root_identity)
    else:
        _atomic_write_posix(path, root, chunks, mode, owner, expected_root_identity)


def atomic_write_bytes(
    path: Path,
    root: Path,
    payload: bytes,
    *,
    mode: int = 0o600,
    owner: tuple[int, int] | None = None,
    expected_root_identity: tuple[int, int] | None = None,
) -> None:
    atomic_write_stream(
        path,
        root,
        (bytes(payload),),
        mode=mode,
        owner=owner,
        expected_root_identity=expected_root_identity,
    )
