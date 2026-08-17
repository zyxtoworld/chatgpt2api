from __future__ import annotations

from datetime import datetime

from services.storage.base import StorageDataError


_TASK_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def canonical_task_timestamp(value: object, default: str) -> str:
    """Validate the canonical timestamp persisted by task services."""
    if value is None or value == "":
        return default
    if not isinstance(value, str) or value != value.strip() or len(value) != 19:
        raise StorageDataError()
    try:
        parsed = datetime.strptime(value, _TASK_TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise StorageDataError() from exc
    if parsed.strftime(_TASK_TIMESTAMP_FORMAT) != value:
        raise StorageDataError()
    return value
