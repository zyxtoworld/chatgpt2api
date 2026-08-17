from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from pathlib import Path
from threading import Lock, RLock

from services.storage.base import canonical_path_write_lock


_LOCKS_GUARD = Lock()
_LOCKS: dict[tuple[str, str], tuple[RLock, int]] = {}


def _rel_os_lock_path(index_file: Path, rel: str) -> Path:
    index_path = Path(index_file).absolute()
    identity = f"{os.path.normcase(str(index_path))}\x00{rel}".encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()
    return index_path.parent / f".{index_path.name}.{digest}.rel"


@contextmanager
def image_rel_lock(index_file: Path, rel: str):
    """Serialize all publication and cleanup operations for one image rel."""
    key = (str(Path(index_file).absolute()), rel)
    with _LOCKS_GUARD:
        entry = _LOCKS.get(key)
        if entry is None:
            entry = (RLock(), 0)
        lock, refs = entry
        _LOCKS[key] = (lock, refs + 1)
    try:
        with lock:
            with canonical_path_write_lock(_rel_os_lock_path(index_file, rel)):
                yield
    finally:
        with _LOCKS_GUARD:
            current = _LOCKS.get(key)
            if current is not None:
                current_lock, current_refs = current
                if current_refs <= 1:
                    _LOCKS.pop(key, None)
                else:
                    _LOCKS[key] = (current_lock, current_refs - 1)
