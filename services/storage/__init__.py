from __future__ import annotations

from pathlib import Path

from services.storage.base import StorageBackend


def create_storage_backend(data_dir: Path) -> StorageBackend:
    # Keep factory imports lazy: storage backends may use protocol error helpers,
    # so importing the factory while the package is initializing creates a cycle.
    from services.storage.factory import create_storage_backend as _create_storage_backend

    return _create_storage_backend(data_dir)

__all__ = ["create_storage_backend"]
