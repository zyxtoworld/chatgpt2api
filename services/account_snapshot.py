from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, cast


class AccountSnapshotLimitError(ValueError):
    """The account snapshot exceeds its executable resource contract."""


_CONTRACT_PATH = Path(__file__).resolve().parent.parent / "account_snapshot_contract.json"
_CONTRACT_FIELDS = (
    "max_bytes",
    "max_records",
    "max_depth",
    "max_nodes",
    "max_object_fields",
    "max_key_bytes",
    "max_string_bytes",
)


def _load_contract() -> dict[str, int]:
    try:
        value = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("account snapshot contract is unavailable") from exc
    if (
        not isinstance(value, dict)
        or set(value) != set(_CONTRACT_FIELDS)
        or any(type(value.get(field)) is not int or value[field] <= 0 for field in _CONTRACT_FIELDS)
    ):
        raise RuntimeError("account snapshot contract is invalid")
    return cast(dict[str, int], value)


_CONTRACT = _load_contract()
ACCOUNT_SNAPSHOT_MAX_BYTES = _CONTRACT["max_bytes"]
ACCOUNT_SNAPSHOT_MAX_RECORDS = _CONTRACT["max_records"]
ACCOUNT_SNAPSHOT_MAX_DEPTH = _CONTRACT["max_depth"]
ACCOUNT_SNAPSHOT_MAX_NODES = _CONTRACT["max_nodes"]
ACCOUNT_SNAPSHOT_MAX_OBJECT_FIELDS = _CONTRACT["max_object_fields"]
ACCOUNT_SNAPSHOT_MAX_KEY_BYTES = _CONTRACT["max_key_bytes"]
ACCOUNT_SNAPSHOT_MAX_STRING_BYTES = _CONTRACT["max_string_bytes"]


class _JsonBudgetScanner:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.length = len(payload)
        self.index = 0
        self.nodes = 0

    def scan(self) -> None:
        self._skip_whitespace()
        self._scan_value(1)
        self._skip_whitespace()
        if self.index != self.length:
            raise AccountSnapshotLimitError("invalid trailing JSON data")

    def _skip_whitespace(self) -> None:
        payload = self.payload
        index = self.index
        length = self.length
        while index < length and payload[index] in (0x20, 0x09, 0x0A, 0x0D):
            index += 1
        self.index = index

    def _consume(self, byte: int) -> None:
        if self.index >= self.length or self.payload[self.index] != byte:
            raise AccountSnapshotLimitError("invalid JSON syntax")
        self.index += 1

    def _count_node(self) -> None:
        self.nodes += 1
        if self.nodes > ACCOUNT_SNAPSHOT_MAX_NODES:
            raise AccountSnapshotLimitError("account snapshot has too many JSON nodes")

    def _scan_value(self, depth: int) -> None:
        if depth > ACCOUNT_SNAPSHOT_MAX_DEPTH:
            raise AccountSnapshotLimitError("account snapshot nesting is too deep")
        self._count_node()
        if self.index >= self.length:
            raise AccountSnapshotLimitError("truncated JSON value")
        byte = self.payload[self.index]
        if byte == 0x7B:
            self._scan_object(depth)
        elif byte == 0x5B:
            self._scan_array(depth)
        elif byte == 0x22:
            self._scan_string(ACCOUNT_SNAPSHOT_MAX_STRING_BYTES)
        elif byte == 0x74:
            self._scan_literal(b"true")
        elif byte == 0x66:
            self._scan_literal(b"false")
        elif byte == 0x6E:
            self._scan_literal(b"null")
        elif byte == 0x2D or 0x30 <= byte <= 0x39:
            self._scan_number()
        else:
            raise AccountSnapshotLimitError("invalid JSON value")

    def _scan_object(self, depth: int) -> None:
        self.index += 1
        self._skip_whitespace()
        if self.index < self.length and self.payload[self.index] == 0x7D:
            self.index += 1
            return
        fields = 0
        while True:
            fields += 1
            if fields > ACCOUNT_SNAPSHOT_MAX_OBJECT_FIELDS:
                raise AccountSnapshotLimitError("account snapshot object has too many fields")
            self._scan_string(ACCOUNT_SNAPSHOT_MAX_KEY_BYTES)
            self._skip_whitespace()
            self._consume(0x3A)
            self._skip_whitespace()
            self._scan_value(depth + 1)
            self._skip_whitespace()
            if self.index >= self.length:
                raise AccountSnapshotLimitError("truncated JSON object")
            delimiter = self.payload[self.index]
            self.index += 1
            if delimiter == 0x7D:
                return
            if delimiter != 0x2C:
                raise AccountSnapshotLimitError("invalid JSON object delimiter")
            self._skip_whitespace()

    def _scan_array(self, depth: int) -> None:
        self.index += 1
        self._skip_whitespace()
        if self.index < self.length and self.payload[self.index] == 0x5D:
            self.index += 1
            return
        items = 0
        while True:
            items += 1
            if items > ACCOUNT_SNAPSHOT_MAX_RECORDS:
                raise AccountSnapshotLimitError("account snapshot array has too many items")
            self._scan_value(depth + 1)
            self._skip_whitespace()
            if self.index >= self.length:
                raise AccountSnapshotLimitError("truncated JSON array")
            delimiter = self.payload[self.index]
            self.index += 1
            if delimiter == 0x5D:
                return
            if delimiter != 0x2C:
                raise AccountSnapshotLimitError("invalid JSON array delimiter")
            self._skip_whitespace()

    @staticmethod
    def _hex_value(byte: int) -> int:
        if 0x30 <= byte <= 0x39:
            return byte - 0x30
        if 0x41 <= byte <= 0x46:
            return byte - 0x41 + 10
        if 0x61 <= byte <= 0x66:
            return byte - 0x61 + 10
        raise AccountSnapshotLimitError("invalid JSON unicode escape")

    def _scan_u_escape(self) -> int:
        if self.index + 4 > self.length:
            raise AccountSnapshotLimitError("truncated JSON unicode escape")
        value = 0
        for byte in self.payload[self.index : self.index + 4]:
            value = (value << 4) | self._hex_value(byte)
        self.index += 4
        return value

    def _scan_string(self, max_bytes: int) -> None:
        self._consume(0x22)
        decoded_bytes = 0
        while self.index < self.length:
            byte = self.payload[self.index]
            self.index += 1
            if byte == 0x22:
                return
            if byte < 0x20:
                raise AccountSnapshotLimitError("unescaped control byte in JSON string")
            if byte == 0x5C:
                if self.index >= self.length:
                    raise AccountSnapshotLimitError("truncated JSON escape")
                escape = self.payload[self.index]
                self.index += 1
                if escape in b'"\\/bfnrt':
                    decoded_bytes += 1
                elif escape == 0x75:
                    codepoint = self._scan_u_escape()
                    if 0xD800 <= codepoint <= 0xDBFF:
                        if (
                            self.index + 2 > self.length
                            or self.payload[self.index] != 0x5C
                            or self.payload[self.index + 1] != 0x75
                        ):
                            raise AccountSnapshotLimitError("unpaired JSON surrogate")
                        self.index += 2
                        low = self._scan_u_escape()
                        if not 0xDC00 <= low <= 0xDFFF:
                            raise AccountSnapshotLimitError("unpaired JSON surrogate")
                        decoded_bytes += 4
                    elif 0xDC00 <= codepoint <= 0xDFFF:
                        raise AccountSnapshotLimitError("unpaired JSON surrogate")
                    elif codepoint <= 0x7F:
                        decoded_bytes += 1
                    elif codepoint <= 0x7FF:
                        decoded_bytes += 2
                    else:
                        decoded_bytes += 3
                else:
                    raise AccountSnapshotLimitError("invalid JSON escape")
            elif byte < 0x80:
                decoded_bytes += 1
            else:
                width = self._utf8_width(byte)
                end = self.index + width - 1
                if end > self.length:
                    raise AccountSnapshotLimitError("truncated UTF-8 sequence")
                sequence = self.payload[self.index - 1 : end]
                try:
                    sequence.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise AccountSnapshotLimitError("invalid UTF-8 sequence") from exc
                self.index = end
                decoded_bytes += width
            if decoded_bytes > max_bytes:
                raise AccountSnapshotLimitError("account snapshot string is too large")
        raise AccountSnapshotLimitError("truncated JSON string")

    @staticmethod
    def _utf8_width(lead: int) -> int:
        if 0xC2 <= lead <= 0xDF:
            return 2
        if 0xE0 <= lead <= 0xEF:
            return 3
        if 0xF0 <= lead <= 0xF4:
            return 4
        raise AccountSnapshotLimitError("invalid UTF-8 lead byte")

    def _scan_literal(self, literal: bytes) -> None:
        if self.payload[self.index : self.index + len(literal)] != literal:
            raise AccountSnapshotLimitError("invalid JSON literal")
        self.index += len(literal)

    def _scan_number(self) -> None:
        payload = self.payload
        length = self.length
        index = self.index
        if payload[index] == 0x2D:
            index += 1
            if index >= length:
                raise AccountSnapshotLimitError("truncated JSON number")
        if payload[index] == 0x30:
            index += 1
        elif 0x31 <= payload[index] <= 0x39:
            index += 1
            while index < length and 0x30 <= payload[index] <= 0x39:
                index += 1
        else:
            raise AccountSnapshotLimitError("invalid JSON number")
        if index < length and payload[index] == 0x2E:
            index += 1
            start = index
            while index < length and 0x30 <= payload[index] <= 0x39:
                index += 1
            if index == start:
                raise AccountSnapshotLimitError("invalid JSON fraction")
        if index < length and payload[index] in (0x65, 0x45):
            index += 1
            if index < length and payload[index] in (0x2B, 0x2D):
                index += 1
            start = index
            while index < length and 0x30 <= payload[index] <= 0x39:
                index += 1
            if index == start:
                raise AccountSnapshotLimitError("invalid JSON exponent")
        self.index = index


def validate_account_snapshot_bytes(payload: bytes) -> None:
    if not isinstance(payload, bytes) or len(payload) > ACCOUNT_SNAPSHOT_MAX_BYTES:
        raise AccountSnapshotLimitError("account snapshot byte limit exceeded")
    _JsonBudgetScanner(payload).scan()


def validate_account_records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > ACCOUNT_SNAPSHOT_MAX_RECORDS:
        raise AccountSnapshotLimitError("account snapshot record limit exceeded")
    stack: list[tuple[object, int]] = [(value, 1)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        if depth > ACCOUNT_SNAPSHOT_MAX_DEPTH:
            raise AccountSnapshotLimitError("account snapshot nesting is too deep")
        nodes += 1
        if nodes > ACCOUNT_SNAPSHOT_MAX_NODES:
            raise AccountSnapshotLimitError("account snapshot has too many JSON nodes")
        if isinstance(current, dict):
            if len(current) > ACCOUNT_SNAPSHOT_MAX_OBJECT_FIELDS:
                raise AccountSnapshotLimitError("account snapshot object has too many fields")
            for key, child in current.items():
                if not isinstance(key, str) or len(key.encode("utf-8")) > ACCOUNT_SNAPSHOT_MAX_KEY_BYTES:
                    raise AccountSnapshotLimitError("account snapshot key is invalid")
                stack.append((child, depth + 1))
        elif isinstance(current, list):
            if len(current) > ACCOUNT_SNAPSHOT_MAX_RECORDS:
                raise AccountSnapshotLimitError("account snapshot array has too many items")
            stack.extend((child, depth + 1) for child in current)
        elif isinstance(current, str):
            if len(current.encode("utf-8")) > ACCOUNT_SNAPSHOT_MAX_STRING_BYTES:
                raise AccountSnapshotLimitError("account snapshot string is too large")
        elif current is None or isinstance(current, (bool, int)):
            continue
        elif isinstance(current, float) and math.isfinite(current):
            continue
        else:
            raise AccountSnapshotLimitError("account snapshot contains a non-JSON value")
    if any(not isinstance(item, dict) for item in value):
        raise AccountSnapshotLimitError("account snapshot records must be objects")
    return cast(list[dict[str, Any]], value)
