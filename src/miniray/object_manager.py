"""Pure pull decisions and node-local object-transfer coordination.

``choose_object_location`` and ``decide_pull`` are policy functions: they inspect
immutable inputs and never mutate an object store.  ``ObjectManager`` owns the
complementary state machine.  It coalesces concurrent demand for one ObjectID,
buffers/checks chunks, and advertises readiness only after the target
``ObjectStore`` has atomically sealed the complete replica.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
from threading import RLock
from typing import Hashable, Iterable, Mapping
from uuid import uuid4


ObjectIDLike = Hashable
ObjectLocation = Hashable
AttemptIDLike = Hashable
WaiterToken = Hashable


class ObjectManagerError(RuntimeError):
    """Base class for pull-coordinator failures."""


class UnknownPullError(ObjectManagerError, LookupError):
    """No pull record exists for the requested object."""


class PullNotReadyError(ObjectManagerError):
    """A local replica has not completed and sealed yet."""


class PullStateError(ObjectManagerError):
    """A transfer operation is illegal in the current state."""


class ConflictingPullError(ObjectManagerError):
    """Duplicate pull messages disagree about immutable transfer metadata."""


class IncompletePullError(ObjectManagerError):
    """Completion was requested before every byte had arrived."""


class PullChecksumError(ObjectManagerError):
    """The complete transfer does not match its advertised digest."""


class StalePullError(ObjectManagerError):
    """The owner fenced the attempt associated with a transferred replica."""


class PullAction(str, Enum):
    LOCAL_READY = "LOCAL_READY"
    START_PULL = "START_PULL"
    JOIN_PULL = "JOIN_PULL"
    WAIT_FOR_LOCATION = "WAIT_FOR_LOCATION"
    FAILED = "FAILED"


class PullState(str, Enum):
    WAITING_FOR_LOCATION = "WAITING_FOR_LOCATION"
    PULLING = "PULLING"
    READY = "READY"
    FAILED = "FAILED"


@dataclass(frozen=True)
class PullDecision:
    """A side-effect-free answer to one local object demand."""

    object_id: ObjectIDLike
    action: PullAction
    source_location: ObjectLocation | None = None
    transfer_id: str | None = None
    waiter_token: WaiterToken | None = None
    reason: str = ""


@dataclass(frozen=True)
class PullSnapshot:
    object_id: ObjectIDLike
    state: PullState
    source_location: ObjectLocation | None
    transfer_id: str | None
    waiter_tokens: frozenset[WaiterToken]
    expected_size: int | None
    received_size: int
    expected_checksum: str | None
    attempt_id: AttemptIDLike | None
    error: str | None

    @property
    def ready(self) -> bool:
        return self.state is PullState.READY


@dataclass(frozen=True)
class PullCompletion:
    object_id: ObjectIDLike
    local_location: ObjectLocation
    size_bytes: int
    checksum: str
    waiter_tokens: frozenset[WaiterToken]


@dataclass
class _PullRecord:
    object_id: ObjectIDLike
    state: PullState = PullState.WAITING_FOR_LOCATION
    source_location: ObjectLocation | None = None
    transfer_id: str | None = None
    waiters: set[WaiterToken] = field(default_factory=set)
    expected_size: int | None = None
    expected_checksum: str | None = None
    attempt_id: AttemptIDLike | None = None
    buffer: bytearray | None = None
    written_ranges: list[tuple[int, int]] = field(default_factory=list)
    target_created: bool = False
    error: str | None = None

    @property
    def received_size(self) -> int:
        return sum(end - start for start, end in self.written_ranges)


def choose_object_location(
    locations: Iterable[ObjectLocation] | Mapping[ObjectLocation, object],
    *,
    requester_location: ObjectLocation | None = None,
    excluded_locations: Iterable[ObjectLocation] = (),
) -> ObjectLocation | None:
    """Choose a deterministic remote source without changing any state.

    The requester is excluded because this function is called only after its
    local store reported no sealed replica.  A same-node owner entry is thus a
    stale routing hint, not a usable transfer source.
    """

    raw_locations = locations.keys() if isinstance(locations, Mapping) else locations
    excluded = set(excluded_locations)
    if requester_location is not None:
        excluded.add(requester_location)
    candidates = {location for location in raw_locations if location not in excluded}
    if not candidates:
        return None
    return min(candidates, key=_stable_location_key)


def decide_pull(
    object_id: ObjectIDLike,
    *,
    local_ready: bool,
    locations: Iterable[ObjectLocation] | Mapping[ObjectLocation, object],
    requester_location: ObjectLocation | None = None,
    active_transfer_id: str | None = None,
    active_source_location: ObjectLocation | None = None,
    failed: bool = False,
    excluded_locations: Iterable[ObjectLocation] = (),
) -> PullDecision:
    """Compute whether to use, start, join, or defer a pull."""

    if local_ready:
        return PullDecision(object_id, PullAction.LOCAL_READY, reason="sealed locally")
    if failed:
        return PullDecision(object_id, PullAction.FAILED, reason="prior pull failed")
    if active_transfer_id is not None:
        return PullDecision(
            object_id,
            PullAction.JOIN_PULL,
            active_source_location,
            active_transfer_id,
            reason="joined existing object transfer",
        )
    source = choose_object_location(
        locations,
        requester_location=requester_location,
        excluded_locations=excluded_locations,
    )
    if source is None:
        return PullDecision(
            object_id,
            PullAction.WAIT_FOR_LOCATION,
            reason="no usable sealed source location",
        )
    return PullDecision(
        object_id, PullAction.START_PULL, source, reason="remote source selected"
    )


# Readable aliases for callers that use source-oriented terminology.
select_object_location = choose_object_location
choose_source_location = choose_object_location
plan_pull = decide_pull


class ObjectManager:
    """Coordinate pulls into one node's immutable object store.

    Networking is intentionally outside this class.  A transport consumes the
    returned ``START_PULL`` decision, pins/reads that source, and delivers bytes
    through :meth:`receive_chunk` or :meth:`receive_object`.
    """

    def __init__(
        self,
        local_location: ObjectLocation,
        object_store: object,
        owner_table: object | None = None,
    ) -> None:
        _require_hashable(local_location, "local_location")
        self.local_location = local_location
        self.object_store = object_store
        self.owner_table = owner_table
        self._pulls: dict[ObjectIDLike, _PullRecord] = {}
        self._lock = RLock()

    def request_pull(
        self,
        object_id: ObjectIDLike,
        *,
        locations: Iterable[ObjectLocation] | Mapping[ObjectLocation, object] | None = None,
        waiter_token: WaiterToken | None = None,
        expected_size: int | None = None,
        expected_checksum: str | None = None,
        attempt_id: AttemptIDLike | None = None,
        excluded_locations: Iterable[ObjectLocation] = (),
    ) -> PullDecision:
        """Register demand and coalesce it with an existing per-object pull."""

        _require_hashable(object_id, "object_id")
        token: WaiterToken = uuid4().hex if waiter_token is None else waiter_token
        _require_hashable(token, "waiter_token")
        resolved_locations, owner_attempt = self._owner_metadata(object_id, locations)
        if attempt_id is None:
            attempt_id = owner_attempt
        elif owner_attempt is not None and attempt_id != owner_attempt:
            raise StalePullError("requested attempt is no longer current at the owner")
        normalized_checksum = (
            _normalize_checksum(expected_checksum)
            if expected_checksum is not None
            else None
        )

        with self._lock:
            if self._store_contains(object_id, sealed_only=True):
                record = self._pulls.get(object_id)
                if record is not None and record.state is PullState.FAILED:
                    record.waiters.add(token)
                    return PullDecision(
                        object_id,
                        PullAction.FAILED,
                        record.source_location,
                        record.transfer_id,
                        token,
                        record.error or "prior pull failed",
                    )
                if (
                    record is not None
                    and record.attempt_id is not None
                    and attempt_id is not None
                    and record.attempt_id != attempt_id
                ):
                    raise StalePullError("local replica belongs to another attempt")
                if record is not None:
                    self._check_metadata(record, expected_size, normalized_checksum)
                self._validate_local_replica(
                    object_id, expected_size, normalized_checksum
                )
                if record is None:
                    record = _PullRecord(
                        object_id,
                        state=PullState.READY,
                        expected_size=expected_size,
                        expected_checksum=normalized_checksum,
                        attempt_id=attempt_id,
                    )
                    self._pulls[object_id] = record
                record.state = PullState.READY
                record.waiters.add(token)
                return PullDecision(
                    object_id,
                    PullAction.LOCAL_READY,
                    transfer_id=record.transfer_id,
                    waiter_token=token,
                    reason="sealed locally",
                )

            old_record = self._pulls.get(object_id)
            if (
                old_record is not None
                and old_record.attempt_id is not None
                and attempt_id is not None
                and old_record.attempt_id != attempt_id
            ):
                raise ConflictingPullError(
                    "concurrent requests name different producing attempts"
                )
            if old_record is not None:
                self._check_metadata(
                    old_record, expected_size, normalized_checksum
                )
                record = old_record
            else:
                record = _PullRecord(object_id)

            # Descriptor and epoch validation above has no side effects.  Bind
            # immutable metadata before admitting this waiter so a conflicting
            # replay cannot partially alter the transfer record.
            if record.attempt_id is None:
                record.attempt_id = attempt_id
            if record.expected_size is None:
                record.expected_size = expected_size
            if record.expected_checksum is None:
                record.expected_checksum = normalized_checksum
            if old_record is None:
                self._pulls[object_id] = record
            record.waiters.add(token)

            decision = decide_pull(
                object_id,
                local_ready=False,
                locations=resolved_locations,
                requester_location=self.local_location,
                active_transfer_id=(
                    record.transfer_id if record.state is PullState.PULLING else None
                ),
                active_source_location=record.source_location,
                failed=record.state is PullState.FAILED,
                excluded_locations=excluded_locations,
            )
            if decision.action is PullAction.START_PULL:
                record.state = PullState.PULLING
                record.source_location = decision.source_location
                record.transfer_id = uuid4().hex
                try:
                    if (
                        record.expected_size is not None
                        or record.expected_checksum is not None
                    ):
                        self._configure_transfer(
                            record, record.expected_size, record.expected_checksum
                        )
                except Exception as exc:
                    self._fail_record(record, exc)
                    raise
                return PullDecision(
                    object_id,
                    PullAction.START_PULL,
                    record.source_location,
                    record.transfer_id,
                    token,
                    decision.reason,
                )
            return PullDecision(
                object_id,
                decision.action,
                decision.source_location,
                decision.transfer_id,
                token,
                decision.reason,
            )

    # ``request`` is useful in small node-manager fixtures.
    request = request_pull

    def begin_transfer(
        self,
        object_id: ObjectIDLike,
        *,
        size_bytes: int,
        checksum: str,
        transfer_id: str | None = None,
        source_location: ObjectLocation | None = None,
    ) -> PullSnapshot:
        """Attach immutable object metadata and reserve the target store."""

        with self._lock:
            record = self._pull_record(object_id)
            self._require_pulling(record, transfer_id, source_location)
            try:
                self._configure_transfer(record, size_bytes, checksum)
            except Exception as exc:
                self._fail_record(record, exc)
                raise
            return self._snapshot(record)

    begin_pull = begin_transfer

    def receive_chunk(
        self,
        object_id: ObjectIDLike,
        data: bytes | bytearray | memoryview,
        *,
        offset: int,
        transfer_id: str | None = None,
        source_location: ObjectLocation | None = None,
    ) -> int:
        """Write a verified-range chunk; duplicates with the same bytes are safe."""

        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("pull chunk must be bytes-like")
        payload = bytes(data)
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("chunk offset must be a non-negative integer")

        with self._lock:
            record = self._pull_record(object_id)
            self._require_pulling(record, transfer_id, source_location)
            if record.buffer is None or record.expected_size is None:
                raise PullStateError("begin_transfer must precede chunk delivery")
            end = offset + len(payload)
            if end > record.expected_size:
                raise ValueError(
                    f"chunk [{offset}, {end}) exceeds object size {record.expected_size}"
                )
            self._require_overlap_matches(record, offset, payload)
            try:
                self.object_store.write(object_id, payload, offset=offset)
            except Exception as exc:
                self._fail_record(record, exc)
                raise
            record.buffer[offset:end] = payload
            record.written_ranges = _merge_range(
                record.written_ranges, offset, end
            )
            return record.received_size

    def finish_pull(
        self,
        object_id: ObjectIDLike,
        *,
        transfer_id: str | None = None,
        source_location: ObjectLocation | None = None,
    ) -> PullCompletion:
        """Verify, seal, publish the location, and only then wake waiters."""

        with self._lock:
            record = self._pull_record(object_id)
            self._require_pulling(record, transfer_id, source_location)
            if (
                record.buffer is None
                or record.expected_size is None
                or record.expected_checksum is None
            ):
                raise PullStateError("transfer metadata is incomplete")
            if record.received_size != record.expected_size:
                raise IncompletePullError(
                    f"received {record.received_size} of {record.expected_size} bytes"
                )
            actual_checksum = hashlib.sha256(record.buffer).hexdigest()
            if actual_checksum != record.expected_checksum.lower():
                error = PullChecksumError(
                    "transferred object checksum does not match its descriptor"
                )
                self._fail_record(record, error)
                raise error

            try:
                self.object_store.seal(object_id)
                if not self._store_contains(object_id, sealed_only=True):
                    raise PullStateError("object store did not expose the sealed replica")
                self._publish_local_location(record)
            except Exception as exc:
                # A replica rejected by the logical owner is quarantined: the
                # record remains FAILED, so local-ready lookup cannot expose it.
                # Best-effort deletion avoids leaking capacity when it is not
                # pinned; failure to delete must not resurrect the stale bytes.
                if self._store_contains(object_id, sealed_only=True):
                    delete = getattr(self.object_store, "delete", None)
                    if callable(delete):
                        delete(object_id)
                self._fail_record(record, exc)
                raise

            record.state = PullState.READY
            record.error = None
            return PullCompletion(
                object_id,
                self.local_location,
                record.expected_size,
                actual_checksum,
                frozenset(record.waiters),
            )

    complete_transfer = finish_pull

    def receive_object(
        self,
        object_id: ObjectIDLike,
        data: bytes | bytearray | memoryview,
        *,
        checksum: str | None = None,
        transfer_id: str | None = None,
        source_location: ObjectLocation | None = None,
    ) -> PullCompletion:
        """One-shot adapter for transports that receive the complete payload."""

        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("object payload must be bytes-like")
        payload = bytes(data)
        digest = hashlib.sha256(payload).hexdigest() if checksum is None else checksum
        self.begin_transfer(
            object_id,
            size_bytes=len(payload),
            checksum=digest,
            transfer_id=transfer_id,
            source_location=source_location,
        )
        self.receive_chunk(
            object_id,
            payload,
            offset=0,
            transfer_id=transfer_id,
            source_location=source_location,
        )
        return self.finish_pull(
            object_id,
            transfer_id=transfer_id,
            source_location=source_location,
        )

    complete_pull = receive_object

    def fail_pull(self, object_id: ObjectIDLike, error: object) -> bool:
        """Fail one in-flight pull; duplicate failure reports are harmless."""

        with self._lock:
            record = self._pull_record(object_id)
            if record.state is PullState.FAILED:
                return False
            if record.state is PullState.READY:
                return False
            self._fail_record(record, error)
            return True

    def reset_failed_pull(self, object_id: ObjectIDLike) -> bool:
        """Remove a retryable FAILED record only when no replica remains.

        A new lease may then allocate a fresh transfer ID.  Refusing to reset
        while either a sealed or incomplete local replica exists prevents a
        retry from overwriting or adopting bytes from the failed transfer.
        """

        _require_hashable(object_id, "object_id")
        with self._lock:
            record = self._pulls.get(object_id)
            if record is None or record.state is not PullState.FAILED:
                return False
            if self._store_contains(object_id, sealed_only=False):
                raise PullStateError(
                    "cannot reset a failed pull while a local replica exists"
                )
            del self._pulls[object_id]
            return True

    def forget_local_replica(
        self,
        object_id: ObjectIDLike,
        *,
        attempt_id: AttemptIDLike | None = None,
    ) -> bool:
        """Forget node-local pull state after its physical bytes are gone.

        A completed pull retains immutable transfer metadata so duplicate
        demand can take the local-ready fast path.  Once the node deliberately
        deletes that replica, retaining the ``READY`` record would make a later
        pull reuse a stale transfer buffer.  Attempt fencing prevents a delayed
        drop for an old producer epoch from clearing a newer pull.

        Physical deletion remains the ObjectStore/NodeManager's responsibility;
        this method refuses to forget state while any local entry still exists.
        """

        _require_hashable(object_id, "object_id")
        with self._lock:
            record = self._pulls.get(object_id)
            if record is None:
                return False
            if (
                attempt_id is not None
                and record.attempt_id is not None
                and record.attempt_id != attempt_id
            ):
                return False
            if record.state is PullState.PULLING:
                raise PullStateError(
                    "cannot forget a replica while its pull is in flight"
                )
            if self._store_contains(object_id, sealed_only=False):
                raise PullStateError(
                    "cannot forget pull state while a local replica exists"
                )
            del self._pulls[object_id]
            return True

    def is_ready(self, object_id: ObjectIDLike) -> bool:
        with self._lock:
            record = self._pulls.get(object_id)
            return bool(
                self._store_contains(object_id, sealed_only=True)
                and (record is None or record.state is PullState.READY)
            )

    def get_local(self, object_id: ObjectIDLike) -> bytes:
        with self._lock:
            if not self.is_ready(object_id):
                raise PullNotReadyError(f"object is not ready locally: {object_id!r}")
            return self.object_store.get(object_id)

    def snapshot(self, object_id: ObjectIDLike) -> PullSnapshot:
        with self._lock:
            return self._snapshot(self._pull_record(object_id))

    def _owner_metadata(
        self,
        object_id: ObjectIDLike,
        explicit_locations: Iterable[ObjectLocation] | Mapping[ObjectLocation, object] | None,
    ) -> tuple[tuple[ObjectLocation, ...], AttemptIDLike | None]:
        snapshot = (
            self.owner_table.snapshot(object_id)
            if self.owner_table is not None
            else None
        )
        if explicit_locations is not None:
            source = (
                explicit_locations.keys()
                if isinstance(explicit_locations, Mapping)
                else explicit_locations
            )
            resolved_locations = tuple(source)
        elif snapshot is not None:
            resolved_locations = tuple(getattr(snapshot, "locations", ()))
        else:
            resolved_locations = ()
        return resolved_locations, getattr(snapshot, "current_attempt", None)

    def _validate_local_replica(
        self,
        object_id: ObjectIDLike,
        expected_size: int | None,
        expected_checksum: str | None,
    ) -> None:
        if expected_size is None and expected_checksum is None:
            return
        payload = self.object_store.get(object_id)
        if expected_size is not None:
            _validate_size(expected_size)
            if len(payload) != expected_size:
                raise ConflictingPullError(
                    "sealed local replica has a different object size"
                )
        if expected_checksum is not None:
            checksum = _normalize_checksum(expected_checksum)
            if hashlib.sha256(payload).hexdigest() != checksum:
                raise ConflictingPullError(
                    "sealed local replica has a different checksum"
                )

    def _check_metadata(
        self,
        record: _PullRecord,
        size_bytes: int | None,
        checksum: str | None,
    ) -> None:
        if size_bytes is not None:
            _validate_size(size_bytes)
            if record.expected_size not in (None, size_bytes):
                raise ConflictingPullError("concurrent pulls disagree about object size")
        if checksum is not None:
            normalized = _normalize_checksum(checksum)
            if record.expected_checksum not in (None, normalized):
                raise ConflictingPullError("concurrent pulls disagree about checksum")

    def _configure_transfer(
        self,
        record: _PullRecord,
        size_bytes: int | None,
        checksum: str | None,
    ) -> None:
        if size_bytes is None or checksum is None:
            raise ValueError("both size_bytes and checksum are required")
        self._check_metadata(record, size_bytes, checksum)
        if record.buffer is not None:
            return
        if self._store_contains(record.object_id, sealed_only=False):
            raise ConflictingPullError(
                "an unrelated incomplete local replica already exists"
            )
        self.object_store.create(record.object_id, size_bytes)
        record.target_created = True
        record.expected_size = size_bytes
        record.expected_checksum = _normalize_checksum(checksum)
        record.buffer = bytearray(size_bytes)

    def _require_pulling(
        self,
        record: _PullRecord,
        transfer_id: str | None,
        source_location: ObjectLocation | None,
    ) -> None:
        if record.state is not PullState.PULLING:
            raise PullStateError(
                f"object {record.object_id!r} is in state {record.state.value}"
            )
        if transfer_id is not None and transfer_id != record.transfer_id:
            raise StalePullError("message belongs to a superseded transfer")
        if (
            source_location is not None
            and source_location != record.source_location
        ):
            raise StalePullError("message came from a non-selected source")

    def _require_overlap_matches(
        self, record: _PullRecord, offset: int, payload: bytes
    ) -> None:
        assert record.buffer is not None
        end = offset + len(payload)
        for old_start, old_end in record.written_ranges:
            overlap_start = max(offset, old_start)
            overlap_end = min(end, old_end)
            if overlap_start >= overlap_end:
                continue
            new_start = overlap_start - offset
            new_end = overlap_end - offset
            if record.buffer[overlap_start:overlap_end] != payload[new_start:new_end]:
                raise ConflictingPullError(
                    "duplicate chunks contain conflicting bytes"
                )

    def _publish_local_location(self, record: _PullRecord) -> None:
        if self.owner_table is None:
            return
        publish = getattr(self.owner_table, "publish_stored", None)
        if not callable(publish):
            publish = getattr(self.owner_table, "add_location", None)
        if not callable(publish):
            raise TypeError("owner table cannot publish an object location")
        accepted = publish(
            record.object_id, record.attempt_id, self.local_location
        )
        if accepted is False:
            raise StalePullError("owner rejected a stale replica location")

    def _fail_record(self, record: _PullRecord, error: object) -> None:
        if record.target_created and self._store_contains(
            record.object_id, sealed_only=False
        ) and not self._store_contains(record.object_id, sealed_only=True):
            abort = getattr(self.object_store, "abort", None)
            if callable(abort):
                abort(record.object_id)
        record.target_created = False
        record.buffer = None
        record.written_ranges.clear()
        record.state = PullState.FAILED
        record.error = str(error)

    def _pull_record(self, object_id: ObjectIDLike) -> _PullRecord:
        try:
            return self._pulls[object_id]
        except KeyError:
            raise UnknownPullError(f"unknown object pull: {object_id!r}") from None

    def _store_contains(self, object_id: ObjectIDLike, *, sealed_only: bool) -> bool:
        contains = getattr(self.object_store, "contains")
        try:
            return bool(contains(object_id, sealed_only=sealed_only))
        except TypeError:
            # A minimal duck-typed store may expose only sealed ``contains``.
            return bool(contains(object_id)) if sealed_only else bool(contains(object_id))

    @staticmethod
    def _snapshot(record: _PullRecord) -> PullSnapshot:
        return PullSnapshot(
            record.object_id,
            record.state,
            record.source_location,
            record.transfer_id,
            frozenset(record.waiters),
            record.expected_size,
            record.received_size,
            record.expected_checksum,
            record.attempt_id,
            record.error,
        )


PullCoordinator = ObjectManager


def _merge_range(
    ranges: list[tuple[int, int]], start: int, end: int
) -> list[tuple[int, int]]:
    if start == end:
        return list(ranges)
    merged: list[tuple[int, int]] = []
    for candidate_start, candidate_end in sorted((*ranges, (start, end))):
        if not merged or candidate_start > merged[-1][1]:
            merged.append((candidate_start, candidate_end))
        else:
            merged[-1] = (
                merged[-1][0], max(merged[-1][1], candidate_end)
            )
    return merged


def _stable_location_key(location: ObjectLocation) -> tuple[str, str]:
    value = getattr(location, "value", location)
    if isinstance(value, bytes):
        rendered = value.hex()
    else:
        rendered = repr(value)
    return type(location).__qualname__, rendered


def _validate_size(size_bytes: int) -> None:
    if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
        raise ValueError("size_bytes must be a non-negative integer")


def _validate_checksum(checksum: str) -> None:
    if not isinstance(checksum, str) or len(checksum) != 64:
        raise ValueError("checksum must be a SHA-256 hexadecimal digest")
    try:
        bytes.fromhex(checksum)
    except ValueError as exc:
        raise ValueError("checksum must be a SHA-256 hexadecimal digest") from exc


def _normalize_checksum(checksum: str) -> str:
    _validate_checksum(checksum)
    return checksum.lower()


def _require_hashable(value: object, label: str) -> None:
    try:
        hash(value)
    except TypeError as exc:
        raise TypeError(f"{label} must be hashable") from exc
