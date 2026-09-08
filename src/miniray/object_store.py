"""A small, in-memory object store with immutable sealed objects.

The store owns *physical bytes on one node*.  Logical ownership, lineage, and
distributed references deliberately live in :mod:`miniray.ownership`.  Keeping
the two responsibilities separate is one of mini-Ray's central teaching
boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Hashable
from uuid import uuid4

from .errors import (
    ObjectAlreadySealedError,
    ObjectNotFoundError,
    ObjectNotSealedError,
    ObjectStoreError,
)
from .ids import ObjectID


class ObjectAlreadyExistsError(ObjectStoreError):
    """Raised when ``create`` is called for an existing local replica."""


class ObjectStoreFullError(ObjectStoreError):
    """Raised when a create reservation would exceed store capacity."""


class IncompleteObjectError(ObjectStoreError):
    """Raised when an object is sealed before every byte was written."""


class InvalidWriteError(ObjectStoreError):
    """Raised for an out-of-bounds or otherwise invalid write."""


@dataclass(frozen=True)
class ObjectStoreEntrySnapshot:
    """Read-only diagnostic information for one local replica."""

    object_id: ObjectID
    size_bytes: int
    sealed: bool
    pin_count: int


@dataclass
class _StoreEntry:
    size_bytes: int
    buffer: bytearray | None
    sealed_data: bytes | None = None
    written_ranges: list[tuple[int, int]] = field(default_factory=list)
    pin_tokens: set[Hashable] = field(default_factory=set)

    @property
    def sealed(self) -> bool:
        return self.sealed_data is not None

    def record_write(self, start: int, end: int) -> None:
        if start == end:
            return

        merged: list[tuple[int, int]] = []
        for old_start, old_end in sorted((*self.written_ranges, (start, end))):
            if not merged or old_start > merged[-1][1]:
                merged.append((old_start, old_end))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], old_end))
        self.written_ranges = merged

    @property
    def completely_written(self) -> bool:
        if self.size_bytes == 0:
            return True
        return self.written_ranges == [(0, self.size_bytes)]


class ObjectStore:
    """A capacity-bounded store for immutable local object replicas.

    ``create`` reserves the complete object size immediately.  Partial writes
    remain invisible; ``get`` succeeds only after an atomic ``seal``.  Pins are
    token based so duplicate pin/unpin messages are harmless.
    """

    def __init__(self, capacity_bytes: int) -> None:
        if (
            not isinstance(capacity_bytes, int)
            or isinstance(capacity_bytes, bool)
            or capacity_bytes < 0
        ):
            raise ValueError("capacity_bytes must be a non-negative integer")
        self._capacity_bytes = capacity_bytes
        self._used_bytes = 0
        self._entries: dict[ObjectID, _StoreEntry] = {}
        self._lock = RLock()

    @property
    def capacity_bytes(self) -> int:
        return self._capacity_bytes

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes

    @property
    def available_bytes(self) -> int:
        with self._lock:
            return self._capacity_bytes - self._used_bytes

    def create(self, object_id: ObjectID, size_bytes: int) -> None:
        """Reserve space for an unsealed replica."""

        _require_object_id(object_id)
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise ValueError("size_bytes must be a non-negative integer")

        with self._lock:
            if object_id in self._entries:
                raise ObjectAlreadyExistsError(f"object already exists: {object_id!r}")
            if self._used_bytes + size_bytes > self._capacity_bytes:
                raise ObjectStoreFullError(
                    f"cannot reserve {size_bytes} bytes; "
                    f"only {self._capacity_bytes - self._used_bytes} available"
                )
            self._entries[object_id] = _StoreEntry(
                size_bytes=size_bytes, buffer=bytearray(size_bytes)
            )
            self._used_bytes += size_bytes

    def write(
        self, object_id: ObjectID, data: bytes | bytearray | memoryview, *, offset: int = 0
    ) -> None:
        """Write one chunk into an unsealed replica."""

        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise InvalidWriteError("offset must be a non-negative integer")
        payload = _as_bytes(data)

        with self._lock:
            entry = self._entry(object_id)
            if entry.sealed:
                raise ObjectAlreadySealedError(f"object is already sealed: {object_id!r}")
            end = offset + len(payload)
            if end > entry.size_bytes:
                raise InvalidWriteError(
                    f"write [{offset}, {end}) exceeds object size {entry.size_bytes}"
                )
            assert entry.buffer is not None
            entry.buffer[offset:end] = payload
            entry.record_write(offset, end)

    def seal(self, object_id: ObjectID) -> None:
        """Atomically make a completely written replica readable."""

        with self._lock:
            entry = self._entry(object_id)
            if entry.sealed:
                raise ObjectAlreadySealedError(f"object is already sealed: {object_id!r}")
            if not entry.completely_written:
                raise IncompleteObjectError(
                    f"object has unwritten bytes: {object_id!r}; "
                    f"written ranges={entry.written_ranges!r}"
                )
            assert entry.buffer is not None
            entry.sealed_data = bytes(entry.buffer)
            entry.buffer = None
            entry.written_ranges.clear()

    def put(self, object_id: ObjectID, data: bytes | bytearray | memoryview) -> None:
        """Convenience wrapper for one-shot ``create/write/seal``."""

        payload = _as_bytes(data)
        self.create(object_id, len(payload))
        try:
            self.write(object_id, payload)
            self.seal(object_id)
        except BaseException:
            # Only this call created the entry, so rollback cannot delete a
            # pre-existing replica.
            self.abort(object_id)
            raise

    def get(self, object_id: ObjectID) -> bytes:
        """Return immutable bytes for a sealed local replica."""

        with self._lock:
            entry = self._entry(object_id)
            if not entry.sealed:
                raise ObjectNotSealedError(f"object is not sealed: {object_id!r}")
            assert entry.sealed_data is not None
            return entry.sealed_data

    def contains(self, object_id: ObjectID, *, sealed_only: bool = True) -> bool:
        with self._lock:
            _require_object_id(object_id)
            entry = self._entries.get(object_id)
            return entry is not None and (entry.sealed or not sealed_only)

    def pin(self, object_id: ObjectID, token: Hashable | None = None) -> Hashable:
        """Pin a sealed replica and return its idempotency token."""

        pin_token: Hashable = uuid4().hex if token is None else token
        _require_hashable(pin_token, "pin token")
        with self._lock:
            entry = self._entry(object_id)
            if not entry.sealed:
                raise ObjectNotSealedError(f"object is not sealed: {object_id!r}")
            entry.pin_tokens.add(pin_token)
        return pin_token

    def unpin(self, object_id: ObjectID, token: Hashable) -> bool:
        """Release a pin; duplicate releases return ``False``."""

        with self._lock:
            entry = self._entry(object_id)
            existed = token in entry.pin_tokens
            entry.pin_tokens.discard(token)
            return existed

    def delete(self, object_id: ObjectID) -> bool:
        """Delete an unpinned replica, returning whether it was removed."""

        with self._lock:
            _require_object_id(object_id)
            entry = self._entries.get(object_id)
            if entry is None or entry.pin_tokens:
                return False
            self._remove_entry(object_id, entry)
            return True

    def abort(self, object_id: ObjectID) -> bool:
        """Discard an incomplete create and release its reservation."""

        with self._lock:
            _require_object_id(object_id)
            entry = self._entries.get(object_id)
            if entry is None:
                return False
            if entry.sealed:
                raise ObjectAlreadySealedError(
                    f"cannot abort sealed object: {object_id!r}"
                )
            self._remove_entry(object_id, entry)
            return True

    def snapshot(self, object_id: ObjectID) -> ObjectStoreEntrySnapshot:
        with self._lock:
            entry = self._entry(object_id)
            return ObjectStoreEntrySnapshot(
                object_id=object_id,
                size_bytes=entry.size_bytes,
                sealed=entry.sealed,
                pin_count=len(entry.pin_tokens),
            )

    def object_ids(self, *, sealed_only: bool = True) -> tuple[ObjectID, ...]:
        with self._lock:
            return tuple(
                object_id
                for object_id, entry in self._entries.items()
                if entry.sealed or not sealed_only
            )

    def _entry(self, object_id: ObjectID) -> _StoreEntry:
        _require_object_id(object_id)
        try:
            return self._entries[object_id]
        except KeyError as exc:
            raise ObjectNotFoundError(f"unknown object: {object_id!r}") from exc

    def _remove_entry(self, object_id: ObjectID, entry: _StoreEntry) -> None:
        del self._entries[object_id]
        self._used_bytes -= entry.size_bytes
        assert self._used_bytes >= 0


def _require_hashable(value: object, label: str) -> None:
    try:
        hash(value)
    except TypeError as exc:
        raise TypeError(f"{label} must be hashable") from exc


def _require_object_id(value: object) -> None:
    if not isinstance(value, ObjectID):
        raise TypeError("object_id must be an ObjectID")


def _as_bytes(data: bytes | bytearray | memoryview) -> bytes:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise InvalidWriteError("data must be bytes-like")
    try:
        return bytes(data)
    except (TypeError, ValueError) as exc:
        raise InvalidWriteError("data must be bytes-like") from exc
