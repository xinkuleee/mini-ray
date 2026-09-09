"""Immutable messages shared by mini-Ray processes.

This module is intentionally transport-neutral.  A local multiprocessing transport,
a deterministic simulator, or a future socket transport can carry the same values.
No message contains a callback or mutable runtime object.
"""

from __future__ import annotations

from dataclasses import dataclass, fields as dataclass_fields
from enum import Enum
import hashlib
import time
import uuid
from typing import Optional, Tuple, TYPE_CHECKING, Union

from .errors import ProtocolError
from .contained_edges import ContainedReferenceHold, IncomingContainedReferenceHold
from .ids import (
    ActorID,
    ActorGeneration,
    AttemptID,
    JobID,
    LeaseID,
    NodeID,
    ObjectID,
    PlacementGroupID,
    TaskID,
    WorkerID,
)
from .resources import AllocationToken, ResourceVector

if TYPE_CHECKING:
    from .output_publication import (
        OutputPublicationCompleteWitness, OutputPublicationEnvelope,
        OutputPublicationNodeIncarnation,
    )
    from .ownership import StoredContainedReferenceDisposition
    from .publication_sources import PreparedContainedTransfer


def _validate_bound_address(value: object, name: str) -> Tuple[str, int]:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or not isinstance(value[0], str)
        or not value[0]
        or isinstance(value[1], bool)
        or not isinstance(value[1], int)
        or not 1 <= value[1] <= 65535
    ):
        raise ProtocolError(
            "{} must be a bound (host, port) tuple".format(name)
        )
    return value[0], value[1]


def _validate_lease_execution_identity(
    lease_id: object,
    task_id: object,
    attempt_id: object,
    worker_id: object,
    operation: str,
) -> None:
    if not isinstance(lease_id, LeaseID):
        raise ProtocolError("{} lease_id must be a LeaseID".format(operation))
    if not isinstance(task_id, TaskID):
        raise ProtocolError("{} task_id must be a TaskID".format(operation))
    if not isinstance(attempt_id, AttemptID):
        raise ProtocolError("{} attempt_id must be an AttemptID".format(operation))
    if attempt_id.task_id != task_id:
        raise ProtocolError(
            "{} attempt_id must belong to task_id".format(operation)
        )
    if not isinstance(worker_id, WorkerID):
        raise ProtocolError("{} worker_id must be a WorkerID".format(operation))


def _validate_acceptance_error(
    accepted: bool, error: Optional[str], operation: str
) -> None:
    if not isinstance(accepted, bool):
        raise ProtocolError("{} accepted flag must be a bool".format(operation))
    if accepted and error is not None:
        raise ProtocolError("an accepted {} cannot contain an error".format(operation))
    if not accepted and (not isinstance(error, str) or not error):
        raise ProtocolError("a rejected {} must contain an error".format(operation))


def _validate_transfer_id(value: object, operation: str) -> None:
    if not isinstance(value, str) or not value:
        raise ProtocolError("{} transfer_id must be a non-empty string".format(operation))


def _validate_non_negative_integer(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolError("{} must be a non-negative integer".format(name))


def _validate_sha256(value: object, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ProtocolError("{} must be a lowercase SHA-256 hex digest".format(name))


def _validate_reference_token(value: object, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ProtocolError("{} must be a non-empty string".format(name))


def _normalize_contained_reference_hold(value: object, operation: str) -> IncomingContainedReferenceHold:
    if type(value) is not ContainedReferenceHold:
        raise ProtocolError('{} requires a complete ContainedReferenceHold'.format(operation))
    return ContainedReferenceHold(value.container_object_id, value.container_owner_worker_id, value.transfer_token)


class PlacementGroupParticipantPhase(str, Enum):
    PREPARE = "PREPARE"
    COMMIT = "COMMIT"
    ABORT = "ABORT"


class PlacementGroupPhaseStatus(str, Enum):
    PLANNING = "PLANNING"
    PENDING = "PENDING"
    INFEASIBLE = "INFEASIBLE"
    PREPARING = "PREPARING"
    COMMITTING = "COMMITTING"
    ABORTING = "ABORTING"
    CREATED = "CREATED"
    LOST = "LOST"
    REMOVING = "REMOVING"
    REMOVED = "REMOVED"


@dataclass(frozen=True, order=True)
class PlacementGroupSchedulingKey:
    """Immutable PG bundle identity carried on every task/lease message."""

    placement_group_id: PlacementGroupID
    attempt: int
    bundle_index: int
    node_id: NodeID
    plan_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise ProtocolError("placement_group_id must be a PlacementGroupID")
        _validate_non_negative_integer(self.attempt, "placement group attempt")
        _validate_non_negative_integer(self.bundle_index, "bundle_index")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("placement group node_id must be a NodeID")
        _validate_sha256(self.plan_digest, "placement group plan_digest")


@dataclass(frozen=True, order=True)
class PlacementGroupBundle:
    bundle_index: int
    resources: ResourceVector

    def __post_init__(self) -> None:
        _validate_non_negative_integer(self.bundle_index, "bundle_index")
        if not isinstance(self.resources, ResourceVector):
            raise ProtocolError("placement group bundle resources must be a ResourceVector")


def _validate_pg_bundles(values: object) -> Tuple[PlacementGroupBundle, ...]:
    try:
        bundles = tuple(values)  # type: ignore[arg-type]
    except TypeError as exc:
        raise ProtocolError("placement group bundles must be iterable") from exc
    if not bundles or any(not isinstance(bundle, PlacementGroupBundle) for bundle in bundles):
        raise ProtocolError("placement group bundles must be non-empty PlacementGroupBundle values")
    indexes = tuple(bundle.bundle_index for bundle in bundles)
    if len(indexes) != len(set(indexes)) or indexes != tuple(sorted(indexes)):
        raise ProtocolError("placement group bundle indexes must be unique and sorted")
    return bundles


def _validate_pg_identity(
    placement_group_id: object, attempt: object, node_id: object,
    plan_digest: object, phase: object, operation: str,
) -> None:
    if not isinstance(placement_group_id, PlacementGroupID):
        raise ProtocolError("{} placement_group_id must be a PlacementGroupID".format(operation))
    _validate_non_negative_integer(attempt, "{} attempt".format(operation))
    if not isinstance(node_id, NodeID):
        raise ProtocolError("{} node_id must be a NodeID".format(operation))
    _validate_sha256(plan_digest, "{} plan_digest".format(operation))
    if not isinstance(phase, PlacementGroupParticipantPhase):
        raise ProtocolError("{} phase must be a PlacementGroupParticipantPhase".format(operation))


@dataclass(frozen=True)
class CreatePlacementGroupRequest:
    placement_group_id: PlacementGroupID
    attempt: int
    bundles: Tuple[PlacementGroupBundle, ...]
    strategy: str

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise ProtocolError("create PG placement_group_id must be a PlacementGroupID")
        _validate_non_negative_integer(self.attempt, "create PG attempt")
        object.__setattr__(self, "bundles", _validate_pg_bundles(self.bundles))
        if len(self.bundles) > 2:
            raise ProtocolError("mini-Ray placement groups support at most two bundles")
        if self.strategy not in {"STRICT_PACK", "STRICT_SPREAD"}:
            raise ProtocolError("create PG strategy must be STRICT_PACK or STRICT_SPREAD")


@dataclass(frozen=True)
class CreatePlacementGroupReply:
    placement_group_id: PlacementGroupID
    attempt: int
    accepted: bool
    phase: PlacementGroupPhaseStatus
    placements: Tuple[PlacementGroupSchedulingKey, ...] = ()
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise ProtocolError("create PG reply placement_group_id is invalid")
        _validate_non_negative_integer(self.attempt, "create PG reply attempt")
        if not isinstance(self.phase, PlacementGroupPhaseStatus):
            raise ProtocolError(
                "create PG reply phase must be a PlacementGroupPhaseStatus"
            )
        placements = tuple(self.placements)
        if any(not isinstance(key, PlacementGroupSchedulingKey) for key in placements):
            raise ProtocolError("create PG reply placements must contain scheduling keys")
        if len({key.bundle_index for key in placements}) != len(placements):
            raise ProtocolError("create PG reply bundle placements must be unique")
        if any(
            key.placement_group_id != self.placement_group_id
            or key.attempt != self.attempt
            for key in placements
        ):
            raise ProtocolError("create PG reply placement identity mismatch")
        _validate_acceptance_error(self.accepted, self.error, "create PG reply")
        if self.accepted and self.phase is not PlacementGroupPhaseStatus.CREATED:
            raise ProtocolError(
                "an accepted create PG reply must report CREATED"
            )
        if self.accepted and not placements:
            raise ProtocolError("an accepted create PG reply needs placements")
        if placements and not self.accepted:
            raise ProtocolError("a rejected create PG reply cannot contain placements")
        object.__setattr__(self, "placements", placements)


@dataclass(frozen=True)
class GetPlacementGroupRequest:
    placement_group_id: PlacementGroupID

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise ProtocolError("get PG placement_group_id must be a PlacementGroupID")


@dataclass(frozen=True)
class GetPlacementGroupReply:
    placement_group_id: PlacementGroupID
    found: bool
    attempt: Optional[int] = None
    phase: Optional[PlacementGroupPhaseStatus] = None
    placements: Tuple[PlacementGroupSchedulingKey, ...] = ()
    error: Optional[str] = None

    def __post_init__(self) -> None:
        GetPlacementGroupRequest(self.placement_group_id)
        placements = tuple(self.placements)
        if not isinstance(self.found, bool):
            raise ProtocolError("get PG reply found must be bool")
        if not self.found:
            if any(value is not None for value in (self.attempt, self.phase)) or placements:
                raise ProtocolError("missing get PG reply cannot expose state")
            if not isinstance(self.error, str) or not self.error:
                raise ProtocolError("missing get PG reply must contain an error")
            return
        _validate_non_negative_integer(self.attempt, "get PG reply attempt")
        if not isinstance(self.phase, PlacementGroupPhaseStatus):
            raise ProtocolError(
                "found get PG reply must contain a PlacementGroupPhaseStatus"
            )
        if self.error is not None:
            raise ProtocolError("found get PG reply cannot contain an error")
        if any(
            not isinstance(key, PlacementGroupSchedulingKey)
            or key.placement_group_id != self.placement_group_id
            or key.attempt != self.attempt
            for key in placements
        ):
            raise ProtocolError("get PG reply placement identity mismatch")
        object.__setattr__(self, "placements", placements)


@dataclass(frozen=True)
class RemovePlacementGroupRequest:
    placement_group_id: PlacementGroupID
    attempt: int

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise ProtocolError("remove PG placement_group_id is invalid")
        _validate_non_negative_integer(self.attempt, "remove PG attempt")


@dataclass(frozen=True)
class RemovePlacementGroupReply:
    placement_group_id: PlacementGroupID
    attempt: int
    accepted: bool
    removed: bool
    phase: PlacementGroupPhaseStatus
    error: Optional[str] = None

    def __post_init__(self) -> None:
        RemovePlacementGroupRequest(self.placement_group_id, self.attempt)
        if not isinstance(self.phase, PlacementGroupPhaseStatus):
            raise ProtocolError(
                "remove PG reply phase must be a PlacementGroupPhaseStatus"
            )
        if not isinstance(self.accepted, bool) or not isinstance(self.removed, bool):
            raise ProtocolError("remove PG reply flags must be bools")
        if self.removed:
            if (
                not self.accepted
                or self.phase is not PlacementGroupPhaseStatus.REMOVED
                or self.error is not None
            ):
                raise ProtocolError(
                    "successful remove PG reply must be accepted REMOVED without error"
                )
            return
        if self.accepted:
            if (
                self.phase is not PlacementGroupPhaseStatus.REMOVING
                or self.error is not None
            ):
                raise ProtocolError(
                    "in-progress remove PG reply must be accepted REMOVING without error"
                )
            return
        if not isinstance(self.error, str) or not self.error:
            raise ProtocolError(
                "rejected remove PG reply must contain an error"
            )


@dataclass(frozen=True)
class DrainPlacementGroupsRequest:
    """Advance one idempotent GCS placement-group cleanup epoch."""

    request_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise ProtocolError(
                "placement-group drain request_id must be a non-empty string"
            )

    @classmethod
    def create(cls) -> "DrainPlacementGroupsRequest":
        return cls(uuid.uuid4().hex)


@dataclass(frozen=True)
class DrainPlacementGroupsReply:
    """Typed progress result for an independent GCS PG-drain barrier."""

    request_id: str
    accepted: bool
    clean: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        DrainPlacementGroupsRequest(self.request_id)
        if not isinstance(self.clean, bool):
            raise ProtocolError(
                "placement-group drain clean flag must be a bool"
            )
        _validate_acceptance_error(
            self.accepted, self.error, "placement-group drain reply"
        )
        if not self.accepted and self.clean:
            raise ProtocolError(
                "a rejected placement-group drain cannot report clean"
            )


@dataclass(frozen=True)
class DrainOwnerDeathFences:
    """Advance one bounded global publication owner-death cleanup round."""

    request_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise ProtocolError(
                "publication owner-death drain request_id must be non-empty"
            )


@dataclass(frozen=True)
class DrainOwnerDeathFencesReply:
    request_id: str
    clean: bool
    active_fences: int

    def __post_init__(self) -> None:
        DrainOwnerDeathFences(self.request_id)
        if not isinstance(self.clean, bool):
            raise ProtocolError("owner-death drain clean flag must be a bool")
        _validate_non_negative_integer(
            self.active_fences, "active_fences"
        )
        if self.clean != (self.active_fences == 0):
            raise ProtocolError(
                "owner-death drain clean flag must match active publications"
            )


@dataclass(frozen=True)
class DrainActorsRequest:
    """Advance one idempotent GCS Actor-control cleanup epoch."""

    request_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise ProtocolError("actor drain request_id must be a non-empty string")

    @classmethod
    def create(cls) -> "DrainActorsRequest":
        return cls(uuid.uuid4().hex)


@dataclass(frozen=True)
class DrainActorsReply:
    """Typed progress for the independent GCS Actor-drain barrier."""

    request_id: str
    accepted: bool
    clean: bool
    active_actor_ids: Tuple[ActorID, ...] = ()
    error: Optional[str] = None

    def __post_init__(self) -> None:
        DrainActorsRequest(self.request_id)
        if not isinstance(self.accepted, bool) or not isinstance(self.clean, bool):
            raise ProtocolError("actor drain accepted and clean flags must be bools")
        actor_ids = tuple(self.active_actor_ids)
        if any(not isinstance(actor_id, ActorID) for actor_id in actor_ids):
            raise ProtocolError("actor drain active_actor_ids must contain ActorIDs")
        if len(actor_ids) != len(set(actor_ids)):
            raise ProtocolError("actor drain active_actor_ids must be unique")
        if actor_ids != tuple(sorted(actor_ids, key=lambda value: value.hex)):
            raise ProtocolError("actor drain active_actor_ids must be sorted")
        object.__setattr__(self, "active_actor_ids", actor_ids)
        _validate_acceptance_error(self.accepted, self.error, "actor drain reply")
        if self.clean and actor_ids:
            raise ProtocolError("a clean actor drain cannot report active Actors")
        if not self.clean and self.accepted and not actor_ids:
            raise ProtocolError("a pending actor drain must report active Actors")
        if not self.accepted and (self.clean or actor_ids):
            raise ProtocolError(
                "a rejected actor drain cannot report clean or active Actors"
            )


@dataclass(frozen=True)
class PlacementGroupParticipantRequest:
    placement_group_id: PlacementGroupID
    attempt: int
    node_id: NodeID
    plan_digest: str
    phase: PlacementGroupParticipantPhase
    bundles: Tuple[PlacementGroupBundle, ...]

    def __post_init__(self) -> None:
        _validate_pg_identity(self.placement_group_id, self.attempt, self.node_id, self.plan_digest, self.phase, "PG participant request")
        object.__setattr__(self, "bundles", _validate_pg_bundles(self.bundles))


@dataclass(frozen=True)
class PlacementGroupParticipantReply:
    placement_group_id: PlacementGroupID
    attempt: int
    node_id: NodeID
    plan_digest: str
    phase: PlacementGroupParticipantPhase
    accepted: bool
    applied: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_pg_identity(self.placement_group_id, self.attempt, self.node_id, self.plan_digest, self.phase, "PG participant reply")
        _validate_acceptance_error(self.accepted, self.error, "PG participant reply")
        if not isinstance(self.applied, bool) or (self.applied and not self.accepted):
            raise ProtocolError("PG participant reply applied flag is invalid")


class PreparePlacementGroupRequest(PlacementGroupParticipantRequest):
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.phase is not PlacementGroupParticipantPhase.PREPARE:
            raise ProtocolError("prepare PG request must echo PREPARE phase")


class CommitPlacementGroupRequest(PlacementGroupParticipantRequest):
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.phase is not PlacementGroupParticipantPhase.COMMIT:
            raise ProtocolError("commit PG request must echo COMMIT phase")


class AbortPlacementGroupRequest(PlacementGroupParticipantRequest):
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.phase is not PlacementGroupParticipantPhase.ABORT:
            raise ProtocolError("abort PG request must echo ABORT phase")


class PreparePlacementGroupReply(PlacementGroupParticipantReply):
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.phase is not PlacementGroupParticipantPhase.PREPARE:
            raise ProtocolError("prepare PG reply must echo PREPARE phase")


class CommitPlacementGroupReply(PlacementGroupParticipantReply):
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.phase is not PlacementGroupParticipantPhase.COMMIT:
            raise ProtocolError("commit PG reply must echo COMMIT phase")


class AbortPlacementGroupReply(PlacementGroupParticipantReply):
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.phase is not PlacementGroupParticipantPhase.ABORT:
            raise ProtocolError("abort PG reply must echo ABORT phase")


def _validate_pg_key(value: object, operation: str) -> None:
    if value is not None and not isinstance(value, PlacementGroupSchedulingKey):
        raise ProtocolError(
            "{} scheduling_key must be a PlacementGroupSchedulingKey or None"
            .format(operation)
        )


def _validate_owned_object_reply(
    *,
    accepted: bool,
    state: object,
    data: object,
    error: object,
    descriptor: object,
    detail: object,
    object_id: ObjectID,
    owner_worker_id: WorkerID,
    operation: str,
) -> None:
    """Validate the common payload shape of borrower and retained reads."""

    if not isinstance(accepted, bool):
        raise ProtocolError("{} accepted must be a bool".format(operation))
    if not accepted:
        if any(value is not None for value in (state, data, error, descriptor)):
            raise ProtocolError(
                "rejected {} cannot expose object state".format(operation)
            )
        if not isinstance(detail, str) or not detail:
            raise ProtocolError(
                "rejected {} must contain a detail".format(operation)
            )
        return
    if not isinstance(state, OwnedObjectState):
        raise ProtocolError(
            "accepted {} must contain an object state".format(operation)
        )
    if detail is not None:
        raise ProtocolError(
            "accepted {} cannot contain rejection detail".format(operation)
        )
    if state is OwnedObjectState.READY_INLINE:
        if not isinstance(data, bytes) or error is not None or descriptor is not None:
            raise ProtocolError("inline owned object must contain only bytes")
    elif state is OwnedObjectState.READY_STORED:
        if (
            not isinstance(descriptor, ObjectStoreDescriptor)
            or data is not None
            or error is not None
        ):
            raise ProtocolError("stored owned object must contain only a descriptor")
        if (
            descriptor.object_id != object_id
            or descriptor.owner_worker_id != owner_worker_id
        ):
            raise ProtocolError(
                "stored owned descriptor must match object and owner identity"
            )
    elif state is OwnedObjectState.ERROR:
        if (
            not isinstance(error, RemoteErrorInfo)
            or data is not None
            or descriptor is not None
        ):
            raise ProtocolError("failed owned object must contain only an error")
    elif data is not None or error is not None or descriptor is not None:
        raise ProtocolError(
            "pending or lost owned object cannot expose result payload"
        )


def _normalize_child_status(
    *,
    operation: str,
    child_pid: Optional[int],
    child_exitcode: Optional[int],
    child_clean: bool,
    child_pids: Tuple[int, ...],
    child_exitcodes: Tuple[Optional[int], ...],
    child_cleans: Tuple[bool, ...],
) -> tuple[Tuple[int, ...], Tuple[Optional[int], ...], Tuple[bool, ...]]:
    """Validate and normalize one-or-many child-process diagnostics.

    The tuple form is authoritative.  The singular fields are a compatibility
    view of the first configured child and may be used to construct the
    historical one-child form when no tuples are supplied.
    """

    if not isinstance(child_clean, bool):
        raise ProtocolError("{} child_clean must be a bool".format(operation))

    pids = tuple(child_pids)
    exitcodes = tuple(child_exitcodes)
    cleans = tuple(child_cleans)
    if not pids and child_pid is not None:
        pids = (child_pid,)
        exitcodes = (child_exitcode,)
        cleans = (child_clean,)
    if not (len(pids) == len(exitcodes) == len(cleans)):
        raise ProtocolError("{} child tuples must align".format(operation))
    if not pids and (exitcodes or cleans):
        raise ProtocolError("{} child tuples must align".format(operation))
    if any(
        isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0
        for pid in pids
    ):
        raise ProtocolError("{} child PIDs must be positive".format(operation))
    if len(set(pids)) != len(pids):
        raise ProtocolError("{} child PIDs must be unique".format(operation))
    if any(
        code is not None and (isinstance(code, bool) or not isinstance(code, int))
        for code in exitcodes
    ):
        raise ProtocolError(
            "{} child exitcodes must be integers or None".format(operation)
        )
    if any(not isinstance(value, bool) for value in cleans):
        raise ProtocolError("{} child clean flags must be bools".format(operation))
    if pids:
        if child_pid is not None and child_pid != pids[0]:
            raise ProtocolError(
                "{} singular child PID must name the first child".format(operation)
            )
        if child_exitcode is not None and child_exitcode != exitcodes[0]:
            raise ProtocolError(
                "{} singular child exitcode must name the first child".format(
                    operation
                )
            )

    return pids, exitcodes, cleans


@dataclass(frozen=True, order=True)
class FunctionKey:
    job_id: JobID
    module: str
    qualname: str
    version: str

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, JobID):
            raise ProtocolError("function job_id must be a JobID")
        if not self.module or not self.qualname or not self.version:
            raise ProtocolError("module, qualname, and version must be non-empty")


@dataclass(frozen=True)
class FunctionDefinition:
    key: FunctionKey
    payload: bytes
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise ProtocolError("serialized function payload must be bytes")
        if hashlib.sha256(self.payload).hexdigest() != self.sha256:
            raise ProtocolError("function payload checksum mismatch")

    @classmethod
    def from_payload(
        cls, key: FunctionKey, payload: bytes
    ) -> "FunctionDefinition":
        return cls(key, payload, hashlib.sha256(payload).hexdigest())


@dataclass(frozen=True)
class RegisterFunction:
    definition: FunctionDefinition


@dataclass(frozen=True)
class FunctionRegistrationReply:
    key: FunctionKey
    accepted: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class GetFunction:
    key: FunctionKey


@dataclass(frozen=True)
class FunctionReply:
    key: FunctionKey
    definition: Optional[FunctionDefinition] = None
    error: Optional[str] = None


class NodeMembershipState(str, Enum):
    ALIVE = "ALIVE"
    DEAD = "DEAD"


class NodeDeathReason(str, Enum):
    EXPECTED = "EXPECTED"
    PROCESS_EXIT = "PROCESS_EXIT"


class NodeDeathDisposition(str, Enum):
    APPLIED = "APPLIED"
    ALREADY_DEAD = "ALREADY_DEAD"
    UNKNOWN = "UNKNOWN"
    CONFLICT = "CONFLICT"


class WorkerMembershipState(str, Enum):
    """GCS-observed lifetime of one ordinary Worker incarnation."""

    ALIVE = "ALIVE"
    DEAD = "DEAD"


class WorkerDeathReason(str, Enum):
    """Why an ordinary Worker incarnation became terminal.

    Consumers must not infer owner-loss semantics from an exit code alone:
    direct process failure, enclosing Node loss, and orderly finalization have
    different ownership consequences.
    """

    PROCESS_EXIT = "PROCESS_EXIT"
    NODE_EXIT = "NODE_EXIT"
    EXPECTED = "EXPECTED"


class WorkerDeathDisposition(str, Enum):
    """Result of reducing one ordinary Worker exit proof."""

    APPLIED = "APPLIED"
    ALREADY_DEAD = "ALREADY_DEAD"
    UNKNOWN = "UNKNOWN"
    CONFLICT = "CONFLICT"


def _validate_node_pid(value: object, operation: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProtocolError("{} node_pid must be a positive integer".format(operation))


def _validate_positive_epoch(value: object, operation: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProtocolError("{} must be a positive integer".format(operation))


@dataclass(frozen=True)
class NodeDeathRecord:
    detection_id: str
    node_id: NodeID
    node_pid: int
    registration_epoch: int
    death_epoch: int
    exit_code: int
    reason: NodeDeathReason
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("node death detection_id must be non-empty")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("node death node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "node death")
        _validate_positive_epoch(self.registration_epoch, "registration_epoch")
        _validate_positive_epoch(self.death_epoch, "death_epoch")
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise ProtocolError("node death exit_code must be an integer")
        if not isinstance(self.reason, NodeDeathReason):
            raise ProtocolError("node death reason must be a NodeDeathReason")
        if not isinstance(self.detail, str) or not self.detail:
            raise ProtocolError("node death detail must be non-empty")


@dataclass(frozen=True)
class WorkerIncarnation:
    """Physical identity of one ordinary Worker supervised by one Node.

    ``WorkerID`` already identifies an incarnation, so there is deliberately
    no second Worker epoch.  The Node PID and registration epoch fence reports
    from another physical NodeManager lifetime.
    """

    node_id: NodeID
    node_pid: int
    node_registration_epoch: int
    worker_id: WorkerID
    worker_pid: int

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("worker incarnation node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "worker incarnation")
        _validate_positive_epoch(
            self.node_registration_epoch,
            "worker incarnation node_registration_epoch",
        )
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("worker incarnation worker_id must be a WorkerID")
        if (
            isinstance(self.worker_pid, bool)
            or not isinstance(self.worker_pid, int)
            or self.worker_pid <= 0
        ):
            raise ProtocolError("worker incarnation worker_pid must be positive")


@dataclass(frozen=True)
class WorkerDeathRecord:
    """Immutable, globally ordered fact that one ordinary Worker exited."""

    detection_id: str
    incarnation: WorkerIncarnation
    death_epoch: int
    exit_code: int
    reason: WorkerDeathReason

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("worker death detection_id must be non-empty")
        if not isinstance(self.incarnation, WorkerIncarnation):
            raise ProtocolError(
                "worker death incarnation must be a WorkerIncarnation"
            )
        _validate_positive_epoch(self.death_epoch, "worker death_epoch")
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise ProtocolError("worker death exit_code must be an integer")
        if not isinstance(self.reason, WorkerDeathReason):
            raise ProtocolError(
                "worker death reason must be a WorkerDeathReason"
            )

    @property
    def node_id(self) -> NodeID:
        return self.incarnation.node_id

    @property
    def node_pid(self) -> int:
        return self.incarnation.node_pid

    @property
    def node_registration_epoch(self) -> int:
        return self.incarnation.node_registration_epoch

    @property
    def worker_id(self) -> WorkerID:
        return self.incarnation.worker_id

    @property
    def worker_pid(self) -> int:
        return self.incarnation.worker_pid


@dataclass(frozen=True)
class RegisterWorkerIncarnation:
    """Register one ordinary Worker after its Node incarnation is known."""

    incarnation: WorkerIncarnation

    def __post_init__(self) -> None:
        if not isinstance(self.incarnation, WorkerIncarnation):
            raise ProtocolError(
                "worker registration requires a WorkerIncarnation"
            )


@dataclass(frozen=True)
class RegisterWorkerIncarnationReply:
    incarnation: WorkerIncarnation
    accepted: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        RegisterWorkerIncarnation(self.incarnation)
        _validate_acceptance_error(
            self.accepted, self.error, "worker incarnation registration"
        )


@dataclass(frozen=True)
class ReportWorkerDeath:
    """A Node supervisor's proof about one exact ordinary Worker."""

    detection_id: str
    incarnation: WorkerIncarnation
    exit_code: int
    reason: WorkerDeathReason

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("worker death detection_id must be non-empty")
        if not isinstance(self.incarnation, WorkerIncarnation):
            raise ProtocolError(
                "worker death report requires a WorkerIncarnation"
            )
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise ProtocolError("worker death exit_code must be an integer")
        if not isinstance(self.reason, WorkerDeathReason):
            raise ProtocolError(
                "worker death report reason must be a WorkerDeathReason"
            )

    @property
    def node_id(self) -> NodeID:
        return self.incarnation.node_id

    @property
    def node_pid(self) -> int:
        return self.incarnation.node_pid

    @property
    def node_registration_epoch(self) -> int:
        return self.incarnation.node_registration_epoch

    @property
    def worker_id(self) -> WorkerID:
        return self.incarnation.worker_id

    @property
    def worker_pid(self) -> int:
        return self.incarnation.worker_pid


@dataclass(frozen=True)
class ReportWorkerDeathReply:
    detection_id: str
    worker_id: WorkerID
    disposition: WorkerDeathDisposition
    watermark: int
    death: Optional[WorkerDeathRecord] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("worker death reply detection_id must be non-empty")
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("worker death reply worker_id must be a WorkerID")
        if not isinstance(self.disposition, WorkerDeathDisposition):
            raise ProtocolError("worker death reply disposition is invalid")
        _validate_non_negative_integer(self.watermark, "worker death watermark")
        acknowledged = self.disposition in (
            WorkerDeathDisposition.APPLIED,
            WorkerDeathDisposition.ALREADY_DEAD,
        )
        if acknowledged:
            if (
                not isinstance(self.death, WorkerDeathRecord)
                or self.death.detection_id != self.detection_id
                or self.death.worker_id != self.worker_id
                or self.death.death_epoch > self.watermark
                or self.error is not None
            ):
                raise ProtocolError(
                    "acknowledged worker death requires only its ordered record"
                )
        elif (
            self.death is not None
            or not isinstance(self.error, str)
            or not self.error
        ):
            raise ProtocolError(
                "unapplied worker death reply requires only an error"
            )


@dataclass(frozen=True)
class GetWorkerState:
    worker_id: WorkerID

    def __post_init__(self) -> None:
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("get worker state worker_id must be a WorkerID")


@dataclass(frozen=True)
class GetWorkerStateReply:
    worker_id: WorkerID
    found: bool
    watermark: int
    state: Optional[WorkerMembershipState] = None
    incarnation: Optional[WorkerIncarnation] = None
    death: Optional[WorkerDeathRecord] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        GetWorkerState(self.worker_id)
        if not isinstance(self.found, bool):
            raise ProtocolError("get worker state found must be a bool")
        _validate_non_negative_integer(self.watermark, "worker death watermark")
        if not self.found:
            if (
                self.state is not None
                or self.incarnation is not None
                or self.death is not None
                or not isinstance(self.error, str)
                or not self.error
            ):
                raise ProtocolError(
                    "missing worker state must contain only an error"
                )
            return
        if not isinstance(self.state, WorkerMembershipState):
            raise ProtocolError(
                "found worker state requires WorkerMembershipState"
            )
        if (
            not isinstance(self.incarnation, WorkerIncarnation)
            or self.incarnation.worker_id != self.worker_id
            or self.error is not None
        ):
            raise ProtocolError(
                "found worker state requires its matching incarnation"
            )
        if self.state is WorkerMembershipState.DEAD:
            if (
                not isinstance(self.death, WorkerDeathRecord)
                or self.death.incarnation != self.incarnation
                or self.death.death_epoch > self.watermark
            ):
                raise ProtocolError(
                    "DEAD worker state requires its ordered death record"
                )
        elif self.death is not None:
            raise ProtocolError("ALIVE worker state cannot contain a death record")


@dataclass(frozen=True)
class GetWorkerDeaths:
    """Read every global Worker-death fact after one consumed epoch."""

    after_epoch: int = 0

    def __post_init__(self) -> None:
        _validate_non_negative_integer(
            self.after_epoch, "worker deaths after_epoch"
        )


@dataclass(frozen=True)
class GetWorkerDeathsReply:
    after_epoch: int
    watermark: int
    deaths: Tuple[WorkerDeathRecord, ...]

    def __post_init__(self) -> None:
        GetWorkerDeaths(self.after_epoch)
        _validate_non_negative_integer(self.watermark, "worker death watermark")
        deaths = tuple(self.deaths)
        if any(not isinstance(death, WorkerDeathRecord) for death in deaths):
            raise ProtocolError(
                "worker death journal must contain WorkerDeathRecord values"
            )
        if self.after_epoch > self.watermark:
            raise ProtocolError(
                "worker death after_epoch cannot exceed the current watermark"
            )
        expected_epochs = tuple(range(self.after_epoch + 1, self.watermark + 1))
        if tuple(death.death_epoch for death in deaths) != expected_epochs:
            raise ProtocolError(
                "worker death journal must be the complete ordered suffix"
            )
        object.__setattr__(self, "deaths", deaths)


@dataclass(frozen=True)
class NodeInfo:
    """One live physical Node incarnation used only as a scheduling hint."""

    node_id: NodeID
    node_pid: int
    registration_epoch: int
    address: Tuple[str, int]
    total_resources: ResourceVector
    available_resources: ResourceVector
    state: NodeMembershipState = NodeMembershipState.ALIVE

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("node info node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "node info")
        _validate_positive_epoch(self.registration_epoch, "registration_epoch")
        _validate_bound_address(self.address, "node info address")
        if not isinstance(self.total_resources, ResourceVector):
            raise ProtocolError("node total_resources must be a ResourceVector")
        if not isinstance(self.available_resources, ResourceVector):
            raise ProtocolError("node available_resources must be a ResourceVector")
        if not self.available_resources.fits_in(self.total_resources):
            raise ProtocolError("node available resources cannot exceed total resources")
        if self.state is not NodeMembershipState.ALIVE:
            raise ProtocolError("NodeInfo is a live scheduling record")


@dataclass(frozen=True)
class RegisterNode:
    node_id: NodeID
    node_pid: int
    address: Tuple[str, int]
    total_resources: ResourceVector
    available_resources: Optional[ResourceVector] = None

    def __post_init__(self) -> None:
        available = (
            self.total_resources
            if self.available_resources is None
            else self.available_resources
        )
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("register node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "register")
        _validate_bound_address(self.address, "register address")
        if not isinstance(self.total_resources, ResourceVector):
            raise ProtocolError("register total_resources must be a ResourceVector")
        if not isinstance(available, ResourceVector):
            raise ProtocolError("register available_resources must be a ResourceVector")
        if not available.fits_in(self.total_resources):
            raise ProtocolError("register available resources cannot exceed total")


@dataclass(frozen=True)
class RegisterNodeReply:
    node_id: NodeID
    node_pid: int
    accepted: bool
    registration_epoch: int
    membership_epoch: int
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("register reply node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "register reply")
        _validate_positive_epoch(self.registration_epoch, "registration_epoch")
        _validate_non_negative_integer(self.membership_epoch, "membership_epoch")
        _validate_acceptance_error(self.accepted, self.error, "node registration")


@dataclass(frozen=True)
class UpdateNodeResources:
    node_id: NodeID
    node_pid: int
    registration_epoch: int
    report_seq: int
    available_resources: ResourceVector

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("resource update node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "resource update")
        _validate_positive_epoch(self.registration_epoch, "registration_epoch")
        _validate_non_negative_integer(self.report_seq, "resource report_seq")
        if not isinstance(self.available_resources, ResourceVector):
            raise ProtocolError("available_resources must be a ResourceVector")


@dataclass(frozen=True)
class UpdateNodeResourcesReply:
    node_id: NodeID
    node_pid: int
    registration_epoch: int
    report_seq: int
    updated: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        UpdateNodeResources(
            self.node_id, self.node_pid, self.registration_epoch, self.report_seq,
            ResourceVector.empty(),
        )
        _validate_acceptance_error(
            self.updated, self.error, "resource update reply"
        )


@dataclass(frozen=True)
class ReportNodeDeath:
    detection_id: str
    node_id: NodeID
    node_pid: int
    expected_registration_epoch: int
    exit_code: int
    reason: NodeDeathReason
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("node death detection_id must be non-empty")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("node death node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "node death report")
        _validate_positive_epoch(
            self.expected_registration_epoch, "expected_registration_epoch"
        )
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise ProtocolError("node death exit_code must be an integer")
        if not isinstance(self.reason, NodeDeathReason):
            raise ProtocolError("node death reason must be a NodeDeathReason")
        if not isinstance(self.detail, str) or not self.detail:
            raise ProtocolError("node death detail must be non-empty")


@dataclass(frozen=True)
class ReportNodeDeathReply:
    detection_id: str
    node_id: NodeID
    node_pid: int
    disposition: NodeDeathDisposition
    membership_epoch: int
    live_nodes: Tuple[NodeInfo, ...]
    death: Optional[NodeDeathRecord] = None
    error: Optional[str] = None
    actor_state_converged: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("node death reply detection_id must be non-empty")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("node death reply node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "node death reply")
        if not isinstance(self.disposition, NodeDeathDisposition):
            raise ProtocolError("node death reply disposition is invalid")
        if not isinstance(self.actor_state_converged, bool):
            raise ProtocolError(
                "node death actor_state_converged must be a bool"
            )
        _validate_non_negative_integer(self.membership_epoch, "membership_epoch")
        live_nodes = tuple(self.live_nodes)
        if any(not isinstance(node, NodeInfo) for node in live_nodes):
            raise ProtocolError("node death reply live_nodes must contain NodeInfo")
        if len({node.node_id for node in live_nodes}) != len(live_nodes):
            raise ProtocolError("node death reply live_nodes must be unique")
        object.__setattr__(self, "live_nodes", live_nodes)
        if self.disposition in (
            NodeDeathDisposition.APPLIED, NodeDeathDisposition.ALREADY_DEAD
        ):
            if not isinstance(self.death, NodeDeathRecord) or self.error is not None:
                raise ProtocolError("acknowledged node death requires only a death record")
        elif (
            not isinstance(self.error, str)
            or not self.error
            or not self.actor_state_converged
        ):
            raise ProtocolError(
                "unapplied node death reply requires an error and no pending Actor state installation"
            )


@dataclass(frozen=True)
class GetNodeState:
    node_id: NodeID

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("get node state node_id must be a NodeID")


@dataclass(frozen=True)
class GetNodeStateReply:
    node_id: NodeID
    found: bool
    membership_epoch: int
    state: Optional[NodeMembershipState] = None
    node_pid: Optional[int] = None
    registration_epoch: Optional[int] = None
    death: Optional[NodeDeathRecord] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        GetNodeState(self.node_id)
        _validate_non_negative_integer(self.membership_epoch, "membership_epoch")
        if not isinstance(self.found, bool):
            raise ProtocolError("get node state found must be a bool")
        if not self.found:
            if any(value is not None for value in (
                self.state, self.node_pid, self.registration_epoch, self.death
            )) or not isinstance(self.error, str) or not self.error:
                raise ProtocolError("missing node state must contain only an error")
            return
        if not isinstance(self.state, NodeMembershipState):
            raise ProtocolError("found node state requires NodeMembershipState")
        _validate_node_pid(self.node_pid, "get node state reply")
        _validate_positive_epoch(self.registration_epoch, "registration_epoch")
        if self.error is not None:
            raise ProtocolError("found node state cannot contain an error")
        if self.state is NodeMembershipState.DEAD:
            if not isinstance(self.death, NodeDeathRecord):
                raise ProtocolError("DEAD node state requires death record")
        elif self.death is not None:
            raise ProtocolError("ALIVE node state cannot contain death record")


@dataclass(frozen=True)
class UnregisterNode:
    node_id: NodeID
    node_pid: int
    registration_epoch: int
    detection_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("unregister node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "unregister")
        _validate_positive_epoch(self.registration_epoch, "registration_epoch")
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("unregister detection_id must be non-empty")


@dataclass(frozen=True)
class UnregisterNodeReply:
    node_id: NodeID
    node_pid: int
    registration_epoch: int
    detection_id: str
    removed: bool
    membership_epoch: int
    death: Optional[NodeDeathRecord] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        UnregisterNode(
            self.node_id, self.node_pid, self.registration_epoch, self.detection_id
        )
        if not isinstance(self.removed, bool):
            raise ProtocolError("unregister removed must be a bool")
        _validate_non_negative_integer(self.membership_epoch, "membership_epoch")
        if self.removed:
            if not isinstance(self.death, NodeDeathRecord) or self.error is not None:
                raise ProtocolError("removed node reply requires only death record")
        elif self.error is not None and (not isinstance(self.error, str) or not self.error):
            raise ProtocolError("unregister error must be non-empty")


@dataclass(frozen=True)
class GetNodes:
    pass


@dataclass(frozen=True)
class GetNodesReply:
    membership_epoch: int
    nodes: Tuple[NodeInfo, ...]

    def __post_init__(self) -> None:
        _validate_non_negative_integer(self.membership_epoch, "membership_epoch")
        object.__setattr__(self, "nodes", tuple(self.nodes))
        if any(not isinstance(node, NodeInfo) for node in self.nodes):
            raise ProtocolError("get-nodes reply must contain NodeInfo values")


@dataclass(frozen=True)
class GetNodeAddress:
    node_id: NodeID

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("address lookup node_id must be a NodeID")


@dataclass(frozen=True)
class GetNodeAddressReply:
    node_id: NodeID
    found: bool
    address: Optional[Tuple[str, int]] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("address reply node_id must be a NodeID")
        if self.found:
            if self.address is None:
                raise ProtocolError("found address reply must contain an address")
            _validate_bound_address(self.address, "node address")
            if self.error is not None:
                raise ProtocolError("found address reply cannot contain an error")
        elif self.address is not None or not self.error:
            raise ProtocolError("missing address reply must contain only an error")


@dataclass(frozen=True)
class InstallClusterSnapshot:
    """Install one immutable, content-addressed cluster scheduling view.

    The message is sent during bootstrap (and may later be reused for explicit
    refreshes).  Ordinary lease requests consume the NodeManager's local copy;
    they do not synchronously query the GCS.
    """

    membership_epoch: int
    snapshot_id: str
    nodes: Tuple[NodeInfo, ...]

    def __post_init__(self) -> None:
        _validate_non_negative_integer(self.membership_epoch, "membership_epoch")
        if not isinstance(self.snapshot_id, str) or not self.snapshot_id:
            raise ProtocolError("cluster snapshot_id must be a non-empty string")
        object.__setattr__(self, "nodes", tuple(self.nodes))
        if any(not isinstance(node, NodeInfo) for node in self.nodes):
            raise ProtocolError("cluster snapshot must contain NodeInfo values")
        node_ids = tuple(node.node_id for node in self.nodes)
        if len(node_ids) != len(set(node_ids)):
            raise ProtocolError("cluster snapshot node IDs must be unique")


@dataclass(frozen=True)
class InstallClusterSnapshotReply:
    membership_epoch: int
    snapshot_id: str
    node_id: NodeID
    installed: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_non_negative_integer(self.membership_epoch, "membership_epoch")
        if not isinstance(self.snapshot_id, str) or not self.snapshot_id:
            raise ProtocolError("snapshot reply ID must be a non-empty string")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("snapshot reply node_id must be a NodeID")
        if self.installed and self.error is not None:
            raise ProtocolError("installed snapshot reply cannot contain an error")
        if not self.installed and not self.error:
            raise ProtocolError("rejected snapshot reply must contain an error")


class TaskReferenceHoldKind(str, Enum):
    """Owner-side lifetime reason transferred with one nested Task ref."""

    SUBMITTED = "SUBMITTED"
    RETAINED = "RETAINED"


@dataclass(frozen=True, order=True)
class TaskReferenceHold:
    """Stable credential for one logical Task-hold incarnation."""

    kind: TaskReferenceHoldKind
    submitting_worker_id: WorkerID
    task_id: TaskID
    origin_attempt_id: AttemptID

    def __post_init__(self) -> None:
        if not isinstance(self.kind, TaskReferenceHoldKind):
            raise ProtocolError(
                "task reference hold kind must be a TaskReferenceHoldKind"
            )
        if not isinstance(self.submitting_worker_id, WorkerID):
            raise ProtocolError(
                "task reference hold submitting_worker_id must be a WorkerID"
            )
        if not isinstance(self.task_id, TaskID):
            raise ProtocolError(
                "task reference hold task_id must be a TaskID"
            )
        if not isinstance(self.origin_attempt_id, AttemptID):
            raise ProtocolError(
                "task reference hold origin_attempt_id must be an AttemptID"
            )
        if self.origin_attempt_id.task_id != self.task_id:
            raise ProtocolError(
                "task reference hold origin attempt must belong to task_id"
            )


def _validate_retained_task_hold(
    value: object, borrower_worker_id: WorkerID, operation: str
) -> None:
    """Validate the complete capability used by retained-task RPCs."""

    if not isinstance(value, TaskReferenceHold):
        raise ProtocolError(
            "{} hold must be a TaskReferenceHold".format(operation)
        )
    if value.kind is not TaskReferenceHoldKind.RETAINED:
        raise ProtocolError(
            "{} hold must have RETAINED kind".format(operation)
        )
    if value.submitting_worker_id != borrower_worker_id:
        raise ProtocolError(
            "{} hold submitting worker must match borrower_worker_id"
            .format(operation)
        )


@dataclass(frozen=True, order=True)
class NestedReferenceTransfer:
    """Immutable owner route and Task-hold proof for a nested ObjectRef."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    owner_address: Tuple[str, int]
    hold: TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError(
                "nested reference transfer object_id must be an ObjectID"
            )
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError(
                "nested reference transfer owner_worker_id must be a WorkerID"
            )
        object.__setattr__(
            self,
            "owner_address",
            _validate_bound_address(
                self.owner_address, "nested reference transfer owner_address"
            ),
        )
        if not isinstance(self.hold, TaskReferenceHold):
            raise ProtocolError(
                "nested reference transfer hold must be a TaskReferenceHold"
            )
        if (
            self.hold.kind is TaskReferenceHoldKind.SUBMITTED
            and self.hold.submitting_worker_id != self.owner_worker_id
        ):
            raise ProtocolError(
                "a submitted nested-reference hold must belong to its owner"
            )


@dataclass(frozen=True, order=True)
class ContainedTransferSource:
    """Exact proof installed by a result-reference serializer.

    New runtime messages carry a :class:`ContainedReferenceHold`, binding the
    logical outer object and its owner Worker incarnation to the token.  A raw
    string remains the compatibility spelling and is normalized to an
    explicitly ownerless legacy hold; it can never alias the typed identity.
    """

    hold: IncomingContainedReferenceHold

    def __post_init__(self) -> None:
        hold = _normalize_contained_reference_hold(
            self.hold, "contained transfer source"
        )
        object.__setattr__(self, "hold", hold)

    @property
    def transfer_token(self) -> str:
        """Compatibility projection; never use it as authority identity."""

        token = self.hold.transfer_token
        assert isinstance(token, str)
        return token


@dataclass(frozen=True, order=True)
class TaskHoldSource:
    """Proof that an admitted logical Task keeps the object live."""

    hold: TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.hold, TaskReferenceHold):
            raise ProtocolError("task hold source must contain a TaskReferenceHold")


BorrowSource = Union[ContainedTransferSource, TaskHoldSource]


@dataclass(frozen=True)
class InlineArg:
    data: bytes
    serializer: str = "pickle"
    nested_refs: Tuple[NestedReferenceTransfer, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes):
            raise ProtocolError("inline argument data must be bytes")
        if not self.serializer:
            raise ProtocolError("serializer must be non-empty")
        nested_refs = tuple(self.nested_refs)
        if any(
            not isinstance(reference, NestedReferenceTransfer)
            for reference in nested_refs
        ):
            raise ProtocolError(
                "inline argument nested_refs must contain "
                "NestedReferenceTransfer values"
            )
        by_object: dict[ObjectID, NestedReferenceTransfer] = {}
        for reference in nested_refs:
            previous = by_object.get(reference.object_id)
            if previous is not None:
                if previous == reference:
                    raise ProtocolError(
                        "inline argument nested-reference manifest must be unique"
                    )
                raise ProtocolError(
                    "one nested ObjectID cannot name conflicting transfers"
                )
            by_object[reference.object_id] = reference
        object.__setattr__(self, "nested_refs", nested_refs)


@dataclass(frozen=True)
class RefArg:
    object_id: ObjectID
    owner_worker_id: WorkerID


TaskArg = Union[InlineArg, RefArg]


@dataclass(frozen=True)
class ObjectStoreDescriptor:
    """Byte-free identity and integrity metadata for one sealed replica.

    ``node_id`` names the node on which this particular replica is sealed.
    The remaining fields describe the logical object and therefore stay
    unchanged while a pull creates another replica on a different node.
    """

    object_id: ObjectID
    owner_worker_id: WorkerID
    producer_attempt_id: AttemptID
    node_id: NodeID
    size_bytes: int
    checksum: str

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("object descriptor object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError(
                "object descriptor owner_worker_id must be a WorkerID"
            )
        if not isinstance(self.producer_attempt_id, AttemptID):
            raise ProtocolError(
                "object descriptor producer_attempt_id must be an AttemptID"
            )
        if self.producer_attempt_id.task_id != self.object_id.task_id:
            raise ProtocolError(
                "object descriptor producing attempt must belong to object_id"
            )
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("object descriptor node_id must be a NodeID")
        _validate_non_negative_integer(
            self.size_bytes, "object descriptor size_bytes"
        )
        _validate_sha256(self.checksum, "object descriptor checksum")


def _validate_dependencies(
    values: object, operation: str
) -> Tuple[ObjectStoreDescriptor, ...]:
    try:
        dependencies = tuple(values)  # type: ignore[arg-type]
    except TypeError as exc:
        raise ProtocolError(
            "{} dependencies must be an iterable".format(operation)
        ) from exc
    if any(not isinstance(item, ObjectStoreDescriptor) for item in dependencies):
        raise ProtocolError(
            "{} dependencies must contain ObjectStoreDescriptor values".format(
                operation
            )
        )
    object_ids = tuple(item.object_id for item in dependencies)
    if len(object_ids) != len(set(object_ids)):
        raise ProtocolError(
            "{} dependencies must contain unique ObjectIDs".format(operation)
        )
    return dependencies


@dataclass(frozen=True)
class TaskSpec:
    job_id: JobID
    task_id: TaskID
    attempt_id: AttemptID
    function: FunctionKey
    args: Tuple[TaskArg, ...]
    num_returns: int
    resources: ResourceVector
    owner_worker_id: WorkerID
    parent_task_id: Optional[TaskID] = None
    actor_generation: Optional[ActorGeneration] = None
    function_definition: Optional[FunctionDefinition] = None
    kwargs: Tuple[Tuple[str, TaskArg], ...] = ()
    max_retries: int = 0
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", tuple(self.kwargs))
        if self.attempt_id.task_id != self.task_id:
            raise ProtocolError("attempt_id must belong to task_id")
        if (
            isinstance(self.num_returns, bool)
            or not isinstance(self.num_returns, int)
            or self.num_returns != 1
        ):
            raise ProtocolError("mini-Ray tasks require num_returns=1")
        if any(
            not isinstance(arg, (InlineArg, RefArg))
            for arg in self.args
        ):
            raise ProtocolError(
                "all task arguments must be InlineArg or RefArg"
            )
        keyword_names = []
        for item in self.kwargs:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ProtocolError("each keyword argument must be a (name, value) pair")
            name, value = item
            if not isinstance(name, str) or not name:
                raise ProtocolError("keyword argument names must be non-empty strings")
            if not isinstance(value, (InlineArg, RefArg)):
                raise ProtocolError(
                    "keyword argument values must be InlineArg or RefArg"
                )
            keyword_names.append(name)
        if len(keyword_names) != len(set(keyword_names)):
            raise ProtocolError("keyword argument names must be unique")
        if (
            self.function_definition is not None
            and self.function_definition.key != self.function
        ):
            raise ProtocolError("function_definition key must match function")
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or self.max_retries < 0
        ):
            raise ProtocolError("max_retries must be a non-negative integer")
        _validate_pg_key(
            self.scheduling_key, "task spec"
        )
        arguments = self.args + tuple(value for _, value in self.kwargs)
        owners_by_object: dict[ObjectID, WorkerID] = {}
        for argument in arguments:
            if isinstance(argument, RefArg):
                previous_owner = owners_by_object.setdefault(
                    argument.object_id, argument.owner_worker_id
                )
                if previous_owner != argument.owner_worker_id:
                    raise ProtocolError(
                        "TaskSpec arguments name conflicting owners for one object"
                    )
        nested_by_object: dict[ObjectID, NestedReferenceTransfer] = {}
        for argument in arguments:
            if not isinstance(argument, InlineArg):
                continue
            for transfer in argument.nested_refs:
                hold = transfer.hold
                if hold.task_id != self.task_id:
                    raise ProtocolError(
                        "nested reference hold must belong to TaskSpec.task_id"
                    )
                if hold.submitting_worker_id != self.owner_worker_id:
                    raise ProtocolError(
                        "nested reference hold submitter must match TaskSpec owner"
                    )
                previous_owner = owners_by_object.setdefault(
                    transfer.object_id, transfer.owner_worker_id
                )
                if previous_owner != transfer.owner_worker_id:
                    raise ProtocolError(
                        "TaskSpec arguments name conflicting owners for one object"
                    )
                previous = nested_by_object.setdefault(transfer.object_id, transfer)
                if previous != transfer:
                    raise ProtocolError(
                        "one nested object cannot carry conflicting task holds"
                    )

    def return_ids(self) -> Tuple[ObjectID, ...]:
        return (ObjectID(self.task_id, 0),)


@dataclass(frozen=True)
class DependencyOwnerRoute:
    """Frozen logical-owner endpoint and real submission hold for one input.

    A replica's physical source Node is not its logical owner's endpoint.
    Retaining this route lets that Node hand custody back after the submitting
    Worker dies, without manufacturing another borrower or changing ownership.
    """

    object_id: ObjectID
    owner_worker_id: WorkerID
    owner_address: Tuple[str, int]
    hold: TaskReferenceHold

    def __post_init__(self) -> None:
        from .output_publication import _attempt, _object_id, _opaque, _require_type

        try:
            object_id = _object_id(self.object_id)
            owner = _opaque(self.owner_worker_id, WorkerID, "dependency owner WorkerID")
            _require_type(self.owner_address, tuple, "dependency owner address")
            address = _validate_bound_address(self.owner_address, "dependency owner address")
            _require_type(self.hold, TaskReferenceHold, "dependency owner hold")
            _require_type(self.hold.kind, TaskReferenceHoldKind, "dependency owner hold kind")
            hold = TaskReferenceHold(
                self.hold.kind, _opaque(self.hold.submitting_worker_id, WorkerID, "dependency submitter"),
                _opaque(self.hold.task_id, TaskID, "dependency consumer task"), _attempt(self.hold.origin_attempt_id),
            )
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid dependency owner route: {exc}") from exc
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "owner_worker_id", owner)
        object.__setattr__(self, "owner_address", address)
        object.__setattr__(self, "hold", hold)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.object_id, self.owner_worker_id, self.owner_address, self.hold,
        ))


@dataclass(frozen=True)
class RequestWorkerLease:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    resources: ResourceVector
    requester_node_id: NodeID
    requester_worker_id: WorkerID
    preferred_node_id: Optional[NodeID] = None
    # ``None`` denotes the first scheduling hop.  After spillback the
    # submitter retries with the selected NodeID here, turning routing into a
    # final-validation request and preventing redirect loops.
    target_node_id: Optional[NodeID] = None
    dependencies: Tuple[ObjectStoreDescriptor, ...] = ()
    return_ids: Tuple[ObjectID, ...] = ()
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None
    dependency_owner_routes: Tuple[DependencyOwnerRoute, ...] = ()
    requester_owner_address: Optional[Tuple[str, int]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.lease_id, LeaseID):
            raise ProtocolError("lease_id must be a LeaseID")
        if not isinstance(self.task_id, TaskID):
            raise ProtocolError("lease task_id must be a TaskID")
        if not isinstance(self.attempt_id, AttemptID):
            raise ProtocolError("lease attempt_id must be an AttemptID")
        if self.attempt_id.task_id != self.task_id:
            raise ProtocolError("lease attempt_id must belong to task_id")
        if not isinstance(self.resources, ResourceVector):
            raise ProtocolError("lease resources must be a ResourceVector")
        if not isinstance(self.requester_node_id, NodeID):
            raise ProtocolError("requester_node_id must be a NodeID")
        if not isinstance(self.requester_worker_id, WorkerID):
            raise ProtocolError("requester_worker_id must be a WorkerID")
        if self.preferred_node_id is not None and not isinstance(
            self.preferred_node_id, NodeID
        ):
            raise ProtocolError("preferred_node_id must be a NodeID or None")
        if self.target_node_id is not None and not isinstance(
            self.target_node_id, NodeID
        ):
            raise ProtocolError("target_node_id must be a NodeID or None")
        object.__setattr__(
            self,
            "dependencies",
            _validate_dependencies(self.dependencies, "lease request"),
        )
        return_ids = tuple(self.return_ids)
        if any(not isinstance(object_id, ObjectID) for object_id in return_ids):
            raise ProtocolError("lease return_ids must contain ObjectID values")
        if any(object_id.task_id != self.task_id for object_id in return_ids):
            raise ProtocolError("lease return_ids must belong to task_id")
        # Empty manifests are only for lease probes that never execute a TaskSpec.
        if return_ids and return_ids != (ObjectID(self.task_id, 0),):
            raise ProtocolError("lease return_ids must contain the single task return at index 0")
        object.__setattr__(self, "return_ids", return_ids)
        key = self.scheduling_key
        _validate_pg_key(key, "lease request")
        if key is not None and self.target_node_id != key.node_id:
            raise ProtocolError(
                "placement-group lease request must target its planned node"
            )
        try:
            routes = tuple(self.dependency_owner_routes)
            if any(type(route) is not DependencyOwnerRoute for route in routes):
                raise ProtocolError("dependency owner routes require exact route values")
            routes = tuple(DependencyOwnerRoute(route.object_id, route.owner_worker_id, route.owner_address, route.hold)
                           for route in routes)
            if routes:
                if len(routes) != len(self.dependencies):
                    raise ProtocolError("dependency owner routes must cover the complete ordered dependency manifest")
                for descriptor, route in zip(self.dependencies, routes):
                    if (route.object_id, route.owner_worker_id) != (descriptor.object_id, descriptor.owner_worker_id):
                        raise ProtocolError("dependency owner route order or object identity changed")
                    expected_kind = (TaskReferenceHoldKind.SUBMITTED if route.owner_worker_id == self.requester_worker_id
                                     else TaskReferenceHoldKind.RETAINED)
                    if (route.hold.kind is not expected_kind
                            or route.hold.submitting_worker_id != self.requester_worker_id
                            or route.hold.task_id != self.task_id
                            or route.hold.origin_attempt_id.attempt_number > self.attempt_id.attempt_number):
                        raise ProtocolError("dependency owner route does not belong to the submitting task hold")
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid lease dependency owner routes: {exc}") from exc
        object.__setattr__(self, "dependency_owner_routes", routes)
        if self.requester_owner_address is not None:
            object.__setattr__(self, "requester_owner_address", _validate_bound_address(
                self.requester_owner_address, "requester owner address"
            ))


@dataclass(frozen=True)
class LeaseDependencyInventory:
    """A Node's exact localized subset for one immutable lease request.

    This is custody evidence, including an explicitly empty subset. It carries
    no Worker or allocation capability and cannot authorize execution. The
    reporting Node may differ from a rejected request's target; consumers must
    independently bind the response to the actual Node they contacted.
    """

    lease_request: RequestWorkerLease
    node_id: NodeID
    descriptors: Tuple[ObjectStoreDescriptor, ...]

    def __post_init__(self) -> None:
        from .output_publication import _opaque, _sequence

        try:
            request = revalidate_worker_lease_request(self.lease_request)
            node_id = _opaque(self.node_id, NodeID, "inventory node_id")
            descriptors = tuple(
                _revalidate_lease_descriptor(item)
                for item in _sequence(self.descriptors, "inventory descriptors")
            )
            expected = {
                item.object_id: (index, ObjectStoreDescriptor(
                    item.object_id, item.owner_worker_id, item.producer_attempt_id,
                    node_id, item.size_bytes, item.checksum,
                ))
                for index, item in enumerate(request.dependencies)
            }
            previous_index = -1
            for descriptor in descriptors:
                original = expected.get(descriptor.object_id)
                if original is None or descriptor != original[1]:
                    raise ProtocolError("inventory descriptor does not match its original lease dependency")
                if original[0] <= previous_index:
                    raise ProtocolError("inventory must be a unique, ordered dependency subset")
                previous_index = original[0]
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid lease dependency inventory: {exc}") from exc
        object.__setattr__(self, "lease_request", request)
        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "descriptors", descriptors)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.lease_request, self.node_id, self.descriptors,
        ))


ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER = "ack_lease_dependency_custody"


@dataclass(frozen=True)
class AckLeaseDependencyCustody:
    """The original submitter acknowledges one exact inventory handoff."""

    requester_worker_id: WorkerID
    inventory: LeaseDependencyInventory

    def __post_init__(self) -> None:
        from .output_publication import _opaque

        try:
            requester = _opaque(self.requester_worker_id, WorkerID, "custody ACK requester")
            inventory = revalidate_lease_dependency_inventory(self.inventory)
            if requester != inventory.lease_request.requester_worker_id:
                raise ProtocolError("custody ACK requester does not own the lease request")
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid lease dependency custody ACK: {exc}") from exc
        object.__setattr__(self, "requester_worker_id", requester)
        object.__setattr__(self, "inventory", inventory)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.requester_worker_id, self.inventory,
        ))


@dataclass(frozen=True)
class AckLeaseDependencyCustodyReply:
    request: AckLeaseDependencyCustody
    accepted: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if type(self.request) is not AckLeaseDependencyCustody:
            raise ProtocolError("custody ACK reply must echo an exact typed request")
        request = AckLeaseDependencyCustody(self.request.requester_worker_id, self.request.inventory)
        _validate_acceptance_error(self.accepted, self.error, "lease dependency custody ACK")
        object.__setattr__(self, "request", request)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.request, self.accepted, self.error,
        ))


@dataclass(frozen=True)
class GrantWorkerLease:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    node_id: NodeID
    worker_id: WorkerID
    worker_address: Tuple[str, int]
    allocation_token: AllocationToken
    dependencies: Tuple[ObjectStoreDescriptor, ...] = ()
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None

    def __post_init__(self) -> None:
        if not isinstance(self.lease_id, LeaseID):
            raise ProtocolError("grant lease_id must be a LeaseID")
        if not isinstance(self.task_id, TaskID):
            raise ProtocolError("grant task_id must be a TaskID")
        if not isinstance(self.attempt_id, AttemptID):
            raise ProtocolError("grant attempt_id must be an AttemptID")
        if self.attempt_id.task_id != self.task_id:
            raise ProtocolError("grant attempt_id must belong to task_id")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("grant node_id must be a NodeID")
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("grant worker_id must be a WorkerID")
        _validate_bound_address(self.worker_address, "worker_address")
        if not isinstance(self.allocation_token, AllocationToken):
            raise ProtocolError("grant allocation_token must be an AllocationToken")
        dependencies = _validate_dependencies(self.dependencies, "lease grant")
        if any(item.node_id != self.node_id for item in dependencies):
            raise ProtocolError(
                "lease grant dependencies must be sealed on the granting node"
            )
        object.__setattr__(self, "dependencies", dependencies)
        key = self.scheduling_key
        _validate_pg_key(key, "lease grant")
        if key is not None and key.node_id != self.node_id:
            raise ProtocolError(
                "placement-group lease grant must come from its planned node"
            )


def _revalidate_lease_scheduling_key(
    value: object,
) -> Optional[PlacementGroupSchedulingKey]:
    if value is None:
        return None
    from .output_publication import _opaque, _require_type

    _require_type(value, PlacementGroupSchedulingKey, "lease scheduling_key")
    _require_type(value.attempt, int, "placement group attempt")
    _require_type(value.bundle_index, int, "bundle_index")
    _require_type(value.plan_digest, str, "placement group plan_digest")
    return PlacementGroupSchedulingKey(
        _opaque(value.placement_group_id, PlacementGroupID, "placement group ID"),
        value.attempt, value.bundle_index,
        _opaque(value.node_id, NodeID, "placement group node_id"),
        value.plan_digest,
    )


def _revalidate_lease_descriptor(value: object) -> ObjectStoreDescriptor:
    from .output_publication import _attempt, _object_id, _opaque, _require_type

    _require_type(value, ObjectStoreDescriptor, "lease dependency")
    _require_type(value.size_bytes, int, "dependency size_bytes")
    _require_type(value.checksum, str, "dependency checksum")
    return ObjectStoreDescriptor(
        _object_id(value.object_id),
        _opaque(value.owner_worker_id, WorkerID, "dependency owner"),
        _attempt(value.producer_attempt_id),
        _opaque(value.node_id, NodeID, "dependency node_id"),
        value.size_bytes, value.checksum,
    )


def revalidate_worker_lease_request(value: object) -> RequestWorkerLease:
    """Deeply validate and detach the complete pre-effect request identity.

    Reconstruct resource milli-units without float conversion or normalizing a
    malformed sparse vector. Empty return manifests remain valid for explicit
    never-Pushed probes; executable tasks require the single return at index 0.
    """
    from .output_publication import _attempt, _object_id, _opaque, _require_type, _sequence

    try:
        _require_type(value, RequestWorkerLease, "worker lease request")
        _require_type(value.resources, ResourceVector, "lease resources")
        items = value.resources._items
        _require_type(items, tuple, "resource entries")
        last_name = None
        resources = {}
        for entry in items:
            _require_type(entry, tuple, "resource entry")
            if len(entry) != 2:
                raise ProtocolError("resource entries require a name and integer milli-units")
            name, units = entry
            _require_type(name, str, "resource name")
            _require_type(units, int, "resource milli-units")
            if not name or name.strip() != name or units <= 0:
                raise ProtocolError("resource entries must be normalized and strictly positive")
            if last_name is not None and name <= last_name:
                raise ProtocolError("resource entries must be unique and ordered")
            resources[name] = units
            last_name = name
        return RequestWorkerLease(
            lease_id=_opaque(value.lease_id, LeaseID, "lease_id"),
            task_id=_opaque(value.task_id, TaskID, "lease task_id"),
            attempt_id=_attempt(value.attempt_id),
            resources=ResourceVector._from_units(resources),
            requester_node_id=_opaque(value.requester_node_id, NodeID, "lease requester node_id"),
            requester_worker_id=_opaque(value.requester_worker_id, WorkerID, "lease requester worker_id"),
            preferred_node_id=(None if value.preferred_node_id is None
                               else _opaque(value.preferred_node_id, NodeID, "preferred node_id")),
            target_node_id=(None if value.target_node_id is None
                            else _opaque(value.target_node_id, NodeID, "target node_id")),
            dependencies=tuple(_revalidate_lease_descriptor(item)
                               for item in _sequence(value.dependencies, "request dependencies")),
            return_ids=tuple(_object_id(item) for item in _sequence(value.return_ids, "lease return_ids")),
            scheduling_key=_revalidate_lease_scheduling_key(value.scheduling_key),
            dependency_owner_routes=_sequence(value.dependency_owner_routes, "lease dependency owner routes"),
            requester_owner_address=value.requester_owner_address,
        )
    except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
        raise ProtocolError(f"invalid worker lease request: {exc}") from exc


def revalidate_lease_dependency_inventory(value: object) -> LeaseDependencyInventory:
    """Validate and detach an inventory, never mint an execution grant."""
    if type(value) is not LeaseDependencyInventory:
        raise ProtocolError("lease dependency inventory must have its exact wire type")
    try:
        return LeaseDependencyInventory(value.lease_request, value.node_id, value.descriptors)
    except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
        raise ProtocolError(f"invalid lease dependency inventory: {exc}") from exc


def revalidate_worker_lease_grant(value: object) -> GrantWorkerLease:
    """Return a detached, deeply validated grant without a pickle round trip.

    Both an ordinary grant and a retired-grant inventory cross this same
    boundary. Rebuilding every nested value prevents frozen dataclass mutation
    or shared in-process reply objects from changing retained replica custody.
    Validation alone never proves that the lease is still executable.
    """
    from .output_publication import _attempt, _opaque, _require_type, _sequence

    try:
        _require_type(value, GrantWorkerLease, "worker lease grant")
        dependencies = tuple(_revalidate_lease_descriptor(descriptor)
                             for descriptor in _sequence(value.dependencies, "grant dependencies"))
        _require_type(value.allocation_token, AllocationToken, "grant allocation_token")
        _require_type(value.allocation_token.value, str, "allocation token value")
        if not value.allocation_token.value:
            raise ProtocolError("grant allocation token must be non-empty")
        address = _validate_bound_address(value.worker_address, "worker_address")
        _require_type(value.worker_address, tuple, "worker_address")
        _require_type(address[0], str, "worker address host")
        _require_type(address[1], int, "worker address port")
        return GrantWorkerLease(
            _opaque(value.lease_id, LeaseID, "grant lease_id"),
            _opaque(value.task_id, TaskID, "grant task_id"),
            _attempt(value.attempt_id),
            _opaque(value.node_id, NodeID, "grant node_id"),
            _opaque(value.worker_id, WorkerID, "grant worker_id"),
            address, AllocationToken(value.allocation_token.value),
            tuple(dependencies),
            _revalidate_lease_scheduling_key(value.scheduling_key),
        )
    except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
        raise ProtocolError(f"invalid worker lease grant: {exc}") from exc


@dataclass(frozen=True)
class SpillbackWorkerLease:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    target_node_id: NodeID
    reason: str = "better-node"
    # This is a cache of the GCS NodeID -> address lookup, never node identity.
    # It is optional so callers can fall back to a typed GetNodeAddress request.
    target_address: Optional[Tuple[str, int]] = None
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None

    def __post_init__(self) -> None:
        if self.attempt_id.task_id != self.task_id:
            raise ProtocolError("spillback attempt_id must belong to task_id")
        if not isinstance(self.target_node_id, NodeID):
            raise ProtocolError("spillback target_node_id must be a NodeID")
        if self.target_address is not None:
            _validate_bound_address(self.target_address, "target_address")
        _validate_pg_key(self.scheduling_key, "spillback")
        if self.scheduling_key is not None:
            raise ProtocolError(
                "placement-group worker leases cannot spill back"
            )


class LeaseRejectReason(str, Enum):
    INFEASIBLE = "INFEASIBLE"
    PENDING_CAPACITY = "PENDING_CAPACITY"
    NODE_DRAINING = "NODE_DRAINING"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    STALE_ATTEMPT = "STALE_ATTEMPT"
    WRONG_TARGET = "WRONG_TARGET"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"


class LeaseExecutionState(str, Enum):
    """NodeManager-owned lifecycle of one granted worker lease."""

    GRANTED = "GRANTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ABANDONED = "ABANDONED"
    WORKER_LOST = "WORKER_LOST"


@dataclass(frozen=True)
class RejectWorkerLease:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    reason: LeaseRejectReason
    detail: str = ""
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None

    def __post_init__(self) -> None:
        if not isinstance(self.lease_id, LeaseID):
            raise ProtocolError("reject lease_id must be a LeaseID")
        if not isinstance(self.task_id, TaskID):
            raise ProtocolError("reject task_id must be a TaskID")
        if not isinstance(self.attempt_id, AttemptID) or (
            self.attempt_id.task_id != self.task_id
        ):
            raise ProtocolError("reject attempt_id must belong to task_id")
        if not isinstance(self.reason, LeaseRejectReason):
            raise ProtocolError("reject reason must be a LeaseRejectReason")
        _validate_pg_key(self.scheduling_key, "lease rejection")


@dataclass(frozen=True)
class StartWorkerLease:
    """Bind a granted lease to the task attempt about to execute."""

    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None

    def __post_init__(self) -> None:
        _validate_lease_execution_identity(
            self.lease_id, self.task_id, self.attempt_id, self.worker_id, "start"
        )
        _validate_pg_key(self.scheduling_key, "start")


@dataclass(frozen=True)
class StartWorkerLeaseReply:
    lease_id: LeaseID
    state: LeaseExecutionState
    accepted: bool
    error: Optional[str] = None
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None
    node_incarnation: Optional[OutputPublicationNodeIncarnation] = None

    def __post_init__(self) -> None:
        if not isinstance(self.lease_id, LeaseID):
            raise ProtocolError("start reply lease_id must be a LeaseID")
        if not isinstance(self.state, LeaseExecutionState):
            raise ProtocolError("start reply state must be a LeaseExecutionState")
        if not isinstance(self.accepted, bool):
            raise ProtocolError("start reply accepted must be a bool")
        _validate_acceptance_error(self.accepted, self.error, "start reply")
        if self.accepted and self.state is not LeaseExecutionState.RUNNING:
            raise ProtocolError("an accepted start must report RUNNING")
        _validate_pg_key(self.scheduling_key, "start reply")
        if self.node_incarnation is not None:
            # A local import avoids reversing protocol/model import order.
            # Accepted runtime Start replies bind subsequent output discovery
            # to the Node's actual registered process incarnation.
            from .output_publication import OutputPublicationNodeIncarnation

            value = self.node_incarnation
            if type(value) is not OutputPublicationNodeIncarnation:
                raise ProtocolError(
                    "start reply node_incarnation must be an OutputPublicationNodeIncarnation"
                )
            if not self.accepted:
                raise ProtocolError(
                    "rejected start cannot expose a publication incarnation"
                )
            if type(value.node_id) is not NodeID:
                raise ProtocolError("start publication incarnation must name a NodeID")
            try:
                copied = OutputPublicationNodeIncarnation(
                    NodeID(bytes(value.node_id)), value.node_pid,
                    value.registration_epoch,
                )
            except (TypeError, ValueError) as exc:
                raise ProtocolError(
                    "start reply has an invalid publishing Node incarnation"
                ) from exc
            object.__setattr__(self, "node_incarnation", copied)


@dataclass(frozen=True)
class NotifyWorkerBlocked:
    """Yield CPU for one blocking episode of a running task attempt.

    Production Ray identifies the Worker through an ordered raylet IPC
    connection.  mini-Ray uses independent request/reply connections, so the
    complete execution identity and an explicit monotonically increasing
    episode sequence form the idempotency and stale-message fence.
    """

    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    sequence: int

    def __post_init__(self) -> None:
        _validate_lease_execution_identity(
            self.lease_id, self.task_id, self.attempt_id, self.worker_id,
            "notify blocked",
        )
        _validate_non_negative_integer(
            self.sequence, "blocking episode sequence"
        )


@dataclass(frozen=True)
class NotifyWorkerUnblocked:
    """Reacquire CPU for the matching blocking episode."""

    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    sequence: int

    def __post_init__(self) -> None:
        _validate_lease_execution_identity(
            self.lease_id, self.task_id, self.attempt_id, self.worker_id,
            "notify unblocked",
        )
        _validate_non_negative_integer(
            self.sequence, "blocking episode sequence"
        )


def _validate_worker_block_reply(
    lease_id: object,
    task_id: object,
    attempt_id: object,
    worker_id: object,
    sequence: object,
    state: object,
    accepted: object,
    changed: object,
    error: Optional[str],
    operation: str,
) -> None:
    _validate_lease_execution_identity(
        lease_id, task_id, attempt_id, worker_id, operation
    )
    _validate_non_negative_integer(sequence, "blocking episode sequence")
    if not isinstance(state, LeaseExecutionState):
        raise ProtocolError(
            "{} state must be a LeaseExecutionState".format(operation)
        )
    if not isinstance(changed, bool):
        raise ProtocolError("{} changed flag must be a bool".format(operation))
    _validate_acceptance_error(accepted, error, operation)
    if not accepted and changed:
        raise ProtocolError("a rejected {} cannot change state".format(operation))
    if accepted and state is not LeaseExecutionState.RUNNING:
        raise ProtocolError(
            "an accepted {} must report a RUNNING lease".format(operation)
        )


@dataclass(frozen=True)
class NotifyWorkerBlockedReply:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    sequence: int
    state: LeaseExecutionState
    accepted: bool
    changed: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_worker_block_reply(
            self.lease_id, self.task_id, self.attempt_id, self.worker_id,
            self.sequence, self.state, self.accepted, self.changed, self.error,
            "notify blocked reply",
        )


@dataclass(frozen=True)
class NotifyWorkerUnblockedReply:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    sequence: int
    state: LeaseExecutionState
    accepted: bool
    changed: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_worker_block_reply(
            self.lease_id, self.task_id, self.attempt_id, self.worker_id,
            self.sequence, self.state, self.accepted, self.changed, self.error,
            "notify unblocked reply",
        )


@dataclass(frozen=True)
class PushTask:
    lease_id: LeaseID
    worker_id: WorkerID
    spec: TaskSpec
    dependencies: Tuple[ObjectStoreDescriptor, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.lease_id, LeaseID):
            raise ProtocolError("push lease_id must be a LeaseID")
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("push worker_id must be a WorkerID")
        if not isinstance(self.spec, TaskSpec):
            raise ProtocolError("push spec must be a TaskSpec")
        dependencies = _validate_dependencies(self.dependencies, "push task")
        references = tuple(
            argument
            for argument in (
                self.spec.args + tuple(value for _, value in self.spec.kwargs)
            )
            if isinstance(argument, RefArg)
        )
        reference_owners = {}
        for reference in references:
            previous_owner = reference_owners.setdefault(
                reference.object_id, reference.owner_worker_id
            )
            if previous_owner != reference.owner_worker_id:
                raise ProtocolError(
                    "push task dependency arguments name conflicting owners "
                    "for one object"
                )
        dependency_owners = {
            dependency.object_id: dependency.owner_worker_id
            for dependency in dependencies
        }
        if dependency_owners != reference_owners:
            raise ProtocolError(
                "push task dependencies must exactly cover its top-level "
                "RefArgs"
            )
        object.__setattr__(self, "dependencies", dependencies)


class ResultStorage(str, Enum):
    INLINE = "INLINE"
    OBJECT_STORE = "OBJECT_STORE"


@dataclass(frozen=True)
class ResultDescriptor:
    object_id: ObjectID
    storage: ResultStorage
    size_bytes: int
    owner_worker_id: WorkerID
    node_id: NodeID
    checksum: str
    inline_data: Optional[bytes] = None

    def __post_init__(self) -> None:
        if not isinstance(self.storage, ResultStorage):
            raise ProtocolError("result storage must be a ResultStorage")
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("result object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("result owner_worker_id must be a WorkerID")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("result node_id must be a NodeID")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ProtocolError("result size must be a non-negative integer")
        if self.storage is ResultStorage.INLINE:
            if not isinstance(self.inline_data, bytes):
                raise ProtocolError("inline result data must be bytes")
            if len(self.inline_data) != self.size_bytes:
                raise ProtocolError("inline result must contain exactly size_bytes bytes")
            if hashlib.sha256(self.inline_data).hexdigest() != self.checksum:
                raise ProtocolError("inline result checksum mismatch")
        elif self.inline_data is not None:
            raise ProtocolError("object-store descriptors cannot carry inline bytes")
        if not isinstance(self.checksum, str) or len(self.checksum) != 64:
            raise ProtocolError("result checksum must be a SHA-256 hex digest")
        try:
            int(self.checksum, 16)
        except ValueError as exc:
            raise ProtocolError("result checksum must be a SHA-256 hex digest") from exc


def _validate_output_publication_envelope(
    envelope: object, *, task_id: TaskID, attempt_id: AttemptID,
    worker_id: WorkerID, operation: str, lease_id: Optional[LeaseID] = None,
    output_ids: Optional[Tuple[ObjectID, ...]] = None,
    owner_worker_id: Optional[WorkerID] = None, node_id: Optional[NodeID] = None,
) -> "OutputPublicationEnvelope":
    """Rebuild the entire data handoff before binding it to a reply identity."""

    from dataclasses import replace
    from .output_publication import (
        OutputPublicationEnvelope, _attempt, _object_id, _opaque,
    )
    from .task_outputs import TaskExecutionKey

    if type(envelope) is not OutputPublicationEnvelope:
        raise ProtocolError(f"{operation} output_publication must be an OutputPublicationEnvelope")
    try:
        envelope = replace(envelope)
        task_id = _opaque(task_id, TaskID, "reply task_id")
        attempt_id = _attempt(attempt_id)
        worker_id = _opaque(worker_id, WorkerID, "reply worker_id")
        if lease_id is not None:
            lease_id = _opaque(lease_id, LeaseID, "reply lease_id")
        if output_ids is not None:
            output_ids = tuple(_object_id(value) for value in output_ids)
        if owner_worker_id is not None:
            owner_worker_id = _opaque(owner_worker_id, WorkerID, "reply owner")
        if node_id is not None:
            node_id = _opaque(node_id, NodeID, "reply node_id")
    except (TypeError, ValueError, AttributeError) as exc:
        raise ProtocolError(f"invalid {operation} output publication: {exc}") from exc
    publication_id = envelope.publication_id
    header = envelope.manifest.header
    if (publication_id.task_id != task_id or publication_id.attempt_id != attempt_id
            or header.executor_worker_id != worker_id
            or lease_id is not None and publication_id.lease_id != lease_id
            or owner_worker_id is not None and header.owner_worker_id != owner_worker_id
            or node_id is not None and header.node_incarnation.node_id != node_id):
        raise ProtocolError(f"{operation} output publication changed lease, task, attempt, executor, owner, or Node")
    if type(publication_id.execution) is not TaskExecutionKey:
        raise ProtocolError(f"{operation} output publication requires an exact task execution")
    if output_ids is not None and publication_id.output_ids != output_ids:
        raise ProtocolError(f"{operation} output publication changed its single task output")
    return envelope


def _validate_output_completion_witness(
    witness: object, *, lease_id: LeaseID, task_id: TaskID, attempt_id: AttemptID,
    operation: str,
    output_ids: Optional[Tuple[ObjectID, ...]] = None,
) -> "OutputPublicationCompleteWitness":
    """Validate metadata Complete after the Node has retired reply payloads."""

    from dataclasses import replace
    from .output_publication import (
        OutputPublicationCompleteWitness, _attempt, _object_id, _opaque,
    )
    from .task_outputs import TaskExecutionKey

    if type(witness) is not OutputPublicationCompleteWitness:
        raise ProtocolError(f"{operation} output_completion must be an OutputPublicationCompleteWitness")
    try:
        witness = replace(witness)
        lease_id = _opaque(lease_id, LeaseID, "reply lease_id")
        task_id = _opaque(task_id, TaskID, "reply task_id")
        attempt_id = _attempt(attempt_id)
        if output_ids is not None:
            output_ids = tuple(_object_id(value) for value in output_ids)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ProtocolError(f"invalid {operation} output completion: {exc}") from exc
    identity = witness.publication_id
    if (identity.lease_id != lease_id or identity.task_id != task_id
            or identity.attempt_id != attempt_id):
        raise ProtocolError(f"{operation} output completion changed lease, task, or attempt")
    if type(identity.execution) is not TaskExecutionKey:
        raise ProtocolError(f"{operation} output completion requires an exact task execution")
    if output_ids is not None and identity.output_ids != output_ids:
        raise ProtocolError(f"{operation} output completion changed its single task output")
    return witness


class TaskReplyStatus(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    APPLICATION_ERROR = "APPLICATION_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class CompleteWorkerLease:
    """Atomically record a task terminal reply and release its allocation."""

    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    status: TaskReplyStatus
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None

    def __post_init__(self) -> None:
        _validate_lease_execution_identity(
            self.lease_id,
            self.task_id,
            self.attempt_id,
            self.worker_id,
            "complete",
        )
        if not isinstance(self.status, TaskReplyStatus):
            raise ProtocolError("completion status must be a TaskReplyStatus")
        _validate_pg_key(self.scheduling_key, "complete")


@dataclass(frozen=True)
class CompleteWorkerLeaseReply:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    status: TaskReplyStatus
    state: LeaseExecutionState
    accepted: bool
    released: bool
    error: Optional[str] = None
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None
    output_publication: Optional["OutputPublicationEnvelope"] = None
    # Exact success metadata remains available after Node reply-cache retirement.
    # It contains no payload and cannot reconstruct a result for a cold caller.
    output_completion: Optional["OutputPublicationCompleteWitness"] = None

    def __post_init__(self) -> None:
        _validate_lease_execution_identity(
            self.lease_id,
            self.task_id,
            self.attempt_id,
            self.worker_id,
            "completion reply",
        )
        if not isinstance(self.status, TaskReplyStatus):
            raise ProtocolError(
                "completion reply status must be a TaskReplyStatus"
            )
        if not isinstance(self.state, LeaseExecutionState):
            raise ProtocolError(
                "completion reply state must be a LeaseExecutionState"
            )
        if not isinstance(self.accepted, bool):
            raise ProtocolError("completion reply accepted must be a bool")
        if not isinstance(self.released, bool):
            raise ProtocolError("completion reply released must be a bool")
        _validate_acceptance_error(
            self.accepted, self.error, "completion reply"
        )
        if self.accepted and self.state is not LeaseExecutionState.COMPLETED:
            raise ProtocolError("an accepted completion must report COMPLETED")
        if not self.accepted and self.released:
            raise ProtocolError("a rejected completion cannot release resources")
        _validate_pg_key(
            self.scheduling_key, "completion reply"
        )
        if self.output_completion is not None:
            if self.output_publication is not None:
                raise ProtocolError("metadata output completion cannot coexist with publication envelopes")
            if (not self.accepted or self.state is not LeaseExecutionState.COMPLETED
                    or self.status is not TaskReplyStatus.SUCCEEDED):
                raise ProtocolError("output completion witness requires an accepted successful COMPLETED reply")
            witness = _validate_output_completion_witness(
                self.output_completion, lease_id=self.lease_id, task_id=self.task_id,
                attempt_id=self.attempt_id,
                operation="completion reply",
            )
            object.__setattr__(self, "output_completion", witness)
        if self.output_publication is not None:
            if (not self.accepted or self.state is not LeaseExecutionState.COMPLETED
                    or self.status is not TaskReplyStatus.SUCCEEDED):
                raise ProtocolError("output publication requires an accepted successful COMPLETED reply")
            envelope = _validate_output_publication_envelope(
                self.output_publication, lease_id=self.lease_id, task_id=self.task_id,
                attempt_id=self.attempt_id, worker_id=self.worker_id,
                operation="completion reply",
            )
            object.__setattr__(self, "output_publication", envelope)

        # ``released`` is deliberately independent for accepted replays:
        # True means this request performed the RUNNING -> COMPLETED resource
        # release; False means an identical completion was already terminal.

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        values = tuple(
            getattr(self, definition.name)
            for definition in dataclass_fields(self)
        )
        return _rebuild_validated_wire_message, (type(self), values)


@dataclass(frozen=True)
class GetWorkerLeaseOutcome:
    """Query the Node authority after a direct Worker reply is ambiguous.

    The request deliberately names both the physical executor and the logical
    result owner.  ``object_ids`` is the exact set of statically declared
    returns whose node-local stored descriptors the owner is asking about.
    No object bytes or inline task reply travel through this control message.
    """

    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    executor_worker_id: WorkerID
    owner_worker_id: WorkerID
    object_ids: Tuple[ObjectID, ...]
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None

    def __post_init__(self) -> None:
        _validate_lease_execution_identity(
            self.lease_id,
            self.task_id,
            self.attempt_id,
            self.executor_worker_id,
            "worker lease outcome query",
        )
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError(
                "worker lease outcome owner_worker_id must be a WorkerID"
            )
        object_ids = tuple(self.object_ids)
        if any(not isinstance(object_id, ObjectID) for object_id in object_ids):
            raise ProtocolError(
                "worker lease outcome object_ids must contain ObjectID values"
            )
        if any(object_id.task_id != self.task_id for object_id in object_ids):
            raise ProtocolError(
                "worker lease outcome objects must belong to task_id"
            )
        if len(object_ids) != len(set(object_ids)):
            raise ProtocolError(
                "worker lease outcome object_ids must be unique"
            )
        if object_ids and object_ids != (ObjectID(self.task_id, 0),):
            raise ProtocolError("worker lease outcome requires the single task return at index 0")
        object.__setattr__(self, "object_ids", object_ids)
        _validate_pg_key(
            self.scheduling_key, "worker lease outcome query"
        )


@dataclass(frozen=True)
class GetWorkerLeaseOutcomeReply:
    """Node-authoritative outcome with metadata replicas and an optional data handoff."""

    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    executor_worker_id: WorkerID
    owner_worker_id: WorkerID
    object_ids: Tuple[ObjectID, ...]
    node_id: NodeID
    found: bool
    worker_alive: bool
    state: Optional[LeaseExecutionState] = None
    completion_status: Optional[TaskReplyStatus] = None
    descriptors: Tuple[ObjectStoreDescriptor, ...] = ()
    orphan_descriptors: Tuple[ObjectStoreDescriptor, ...] = ()
    error: Optional[str] = None
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None
    # Recovery carries the same single-output handoff as ordinary success,
    # or a byte-free Complete witness after payload retirement.
    output_publication: Optional["OutputPublicationEnvelope"] = None
    output_completion: Optional["OutputPublicationCompleteWitness"] = None
    # Execution/resource truth can be terminal before publication compensation
    # ACKs converge. Owners must not retry while this exact cleanup is pending.
    cleanup_pending: bool = False

    def __post_init__(self) -> None:
        request = GetWorkerLeaseOutcome(
            self.lease_id,
            self.task_id,
            self.attempt_id,
            self.executor_worker_id,
            self.owner_worker_id,
            self.object_ids, self.scheduling_key,
        )
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError(
                "worker lease outcome reply node_id must be a NodeID"
            )
        if not isinstance(self.found, bool) or not isinstance(
            self.worker_alive, bool
        ):
            raise ProtocolError(
                "worker lease outcome found/worker_alive must be bools"
            )
        descriptors = tuple(self.descriptors)
        orphan_descriptors = tuple(self.orphan_descriptors)
        if type(self.cleanup_pending) is not bool:
            raise ProtocolError("worker outcome cleanup_pending must be a bool")
        if self.cleanup_pending and (
            not self.found
            or self.state not in (LeaseExecutionState.COMPLETED, LeaseExecutionState.WORKER_LOST, LeaseExecutionState.ABANDONED)
            or self.completion_status is TaskReplyStatus.SUCCEEDED
            or descriptors or orphan_descriptors
            or self.output_publication is not None or self.output_completion is not None
        ):
            raise ProtocolError("pending cleanup requires a failed terminal outcome without result payload")

        def validate_descriptors(
            values: Tuple[ObjectStoreDescriptor, ...], label: str
        ) -> Tuple[ObjectID, ...]:
            if any(
                not isinstance(descriptor, ObjectStoreDescriptor)
                for descriptor in values
            ):
                raise ProtocolError(
                    "worker lease outcome {} must be ObjectStoreDescriptor values".format(
                        label
                    )
                )
            object_ids = tuple(descriptor.object_id for descriptor in values)
            if len(object_ids) != len(set(object_ids)):
                raise ProtocolError(
                    "worker lease outcome {} must name unique objects".format(
                        label
                    )
                )
            return object_ids

        descriptor_ids = validate_descriptors(descriptors, "descriptors")
        orphan_ids = validate_descriptors(
            orphan_descriptors, "orphan_descriptors"
        )
        if set(descriptor_ids).intersection(orphan_ids):
            raise ProtocolError(
                "recoverable and orphan descriptors must be disjoint"
            )
        requested = set(request.object_ids)
        for descriptor in descriptors + orphan_descriptors:
            if descriptor.object_id not in requested:
                raise ProtocolError(
                    "worker lease outcome descriptor was not requested"
                )
            if (
                descriptor.object_id.task_id != self.task_id
                or descriptor.producer_attempt_id != self.attempt_id
                or descriptor.owner_worker_id != self.owner_worker_id
                or descriptor.node_id != self.node_id
            ):
                raise ProtocolError(
                    "worker lease outcome descriptor identity does not match reply"
                )
        for object_ids, label in (
            (descriptor_ids, "descriptors"),
            (orphan_ids, "orphan_descriptors"),
        ):
            expected_order = tuple(
                object_id
                for object_id in request.object_ids
                if object_id in set(object_ids)
            )
            if object_ids != expected_order:
                raise ProtocolError(
                    "worker lease outcome {} must preserve return-manifest order".format(
                        label
                    )
                )
        if self.found:
            if self.error is not None:
                raise ProtocolError(
                    "found worker lease outcome cannot contain an error"
                )
            if not isinstance(self.state, LeaseExecutionState):
                raise ProtocolError(
                    "found worker lease outcome must contain a lease state"
                )
            if self.state is LeaseExecutionState.WORKER_LOST and self.worker_alive:
                raise ProtocolError(
                    "WORKER_LOST outcome cannot report a live executor"
                )
            if self.state is LeaseExecutionState.COMPLETED:
                if not isinstance(self.completion_status, TaskReplyStatus):
                    raise ProtocolError(
                        "COMPLETED outcome must contain completion_status"
                    )
            elif self.completion_status is not None:
                raise ProtocolError(
                    "only COMPLETED outcome may contain completion_status"
                )
            if descriptors and (
                self.state is not LeaseExecutionState.COMPLETED
                or self.completion_status is not TaskReplyStatus.SUCCEEDED
            ):
                raise ProtocolError(
                    "stored descriptors require a successful COMPLETED outcome"
                )
            if (descriptors and descriptor_ids != request.object_ids
                    and self.output_publication is None):
                raise ProtocolError(
                    "recoverable descriptors must contain the complete execution output manifest"
                )
            terminal_orphan_state = self.state in (
                LeaseExecutionState.COMPLETED,
                LeaseExecutionState.WORKER_LOST,
                LeaseExecutionState.ABANDONED,
            )
            if orphan_descriptors and not terminal_orphan_state:
                raise ProtocolError(
                    "orphan descriptors require a terminal lease outcome"
                )
            if (
                orphan_descriptors
                and self.state is LeaseExecutionState.COMPLETED
                and self.completion_status is TaskReplyStatus.SUCCEEDED
                and orphan_ids == request.object_ids
            ):
                raise ProtocolError(
                    "a complete successful replica set must be recoverable, not orphaned"
                )
        else:
            if not self.error:
                raise ProtocolError(
                    "missing worker lease outcome must contain an error"
                )
            if (
                self.state is not None
                or self.completion_status is not None
                or descriptors
                or orphan_descriptors
                or self.worker_alive
            ):
                raise ProtocolError(
                    "missing worker lease outcome cannot contain authority state"
                )
        if self.output_completion is not None:
            if self.output_publication is not None or descriptors or orphan_descriptors:
                raise ProtocolError("metadata output completion cannot carry envelopes or replica descriptors")
            if (not self.found or self.state is not LeaseExecutionState.COMPLETED
                    or self.completion_status is not TaskReplyStatus.SUCCEEDED):
                raise ProtocolError("output completion witness requires a found successful COMPLETED outcome")
            witness = _validate_output_completion_witness(
                self.output_completion, lease_id=self.lease_id, task_id=self.task_id,
                attempt_id=self.attempt_id,
                output_ids=request.object_ids, operation="worker lease outcome",
            )
            object.__setattr__(self, "output_completion", witness)
        if self.output_publication is not None:
            if orphan_descriptors:
                raise ProtocolError("output publication outcome cannot contain orphan replicas")
            if (not self.found or self.state is not LeaseExecutionState.COMPLETED
                    or self.completion_status is not TaskReplyStatus.SUCCEEDED):
                raise ProtocolError("output publication requires a found successful COMPLETED outcome")
            envelope = _validate_output_publication_envelope(
                self.output_publication, lease_id=self.lease_id, task_id=self.task_id,
                attempt_id=self.attempt_id, worker_id=self.executor_worker_id,
                output_ids=request.object_ids,
                owner_worker_id=self.owner_worker_id, node_id=self.node_id,
                operation="worker lease outcome",
            )
            expected_descriptors = tuple(
                ObjectStoreDescriptor(
                    result.object_id, result.owner_worker_id, self.attempt_id,
                    result.node_id, result.size_bytes, result.checksum,
                )
                for result in envelope.results if result.storage is ResultStorage.OBJECT_STORE
            )
            if descriptors != expected_descriptors:
                raise ProtocolError("output publication outcome must retain its exact STORED descriptor projection")
            # Retain only the freshly reconstructed, payload-free projections.
            descriptors = expected_descriptors
            object.__setattr__(self, "output_publication", envelope)
        object.__setattr__(self, "object_ids", request.object_ids)
        object.__setattr__(self, "descriptors", descriptors)
        object.__setattr__(self, "orphan_descriptors", orphan_descriptors)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        # Outcome recovery can now carry a completed-publication hand-off.
        # Re-enter every exact-identity check after transport unpickling.
        values = tuple(
            getattr(self, definition.name)
            for definition in dataclass_fields(self)
        )
        return _rebuild_validated_wire_message, (type(self), values)


@dataclass(frozen=True)
class RemoteErrorInfo:
    type_name: str
    message: str
    traceback: str = ""


@dataclass(frozen=True)
class TaskReply:
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    status: TaskReplyStatus
    results: Tuple[ResultDescriptor, ...] = ()
    error: Optional[RemoteErrorInfo] = None
    output_publication: Optional["OutputPublicationEnvelope"] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "results", tuple(self.results))
        if self.attempt_id.task_id != self.task_id:
            raise ProtocolError("reply attempt_id must belong to task_id")
        if any(result.object_id.task_id != self.task_id for result in self.results):
            raise ProtocolError("reply result IDs must belong to task_id")
        if self.status is TaskReplyStatus.SUCCEEDED and self.error is not None:
            raise ProtocolError("a successful task reply cannot contain an error")
        if self.status is not TaskReplyStatus.SUCCEEDED and self.error is None:
            raise ProtocolError("a failed task reply must contain an error")
        if self.status is not TaskReplyStatus.SUCCEEDED and self.results:
            raise ProtocolError("a failed task reply cannot publish results")
        if self.status is TaskReplyStatus.SUCCEEDED:
            if tuple(result.object_id for result in self.results) != (ObjectID(self.task_id, 0),):
                raise ProtocolError("successful task reply must publish the single return at index 0")
        if self.output_publication is not None:
            if self.status is not TaskReplyStatus.SUCCEEDED:
                raise ProtocolError("output publication requires a successful task reply")
            envelope = _validate_output_publication_envelope(
                self.output_publication, task_id=self.task_id,
                attempt_id=self.attempt_id, worker_id=self.worker_id,
                output_ids=tuple(result.object_id for result in self.results),
                operation="task reply",
            )
            if self.results != envelope.results:
                raise ProtocolError("output publication task reply must publish exactly its results")
            object.__setattr__(self, "output_publication", envelope)
            object.__setattr__(self, "results", envelope.results)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        values = tuple(
            getattr(self, definition.name)
            for definition in dataclass_fields(self)
        )
        return _rebuild_validated_wire_message, (type(self), values)


class OwnedObjectState(str, Enum):
    """Owner-visible state returned without routing object bytes via GCS."""

    PENDING = "PENDING"
    READY_INLINE = "READY_INLINE"
    READY_STORED = "READY_STORED"
    ERROR = "ERROR"
    LOST = "LOST"


@dataclass(frozen=True)
class AcquireBorrowedObject:
    """Convert an exported transfer pin into one borrower token."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    source: BorrowSource
    borrower_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("borrow acquire object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("borrow acquire owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("borrow acquire borrower must be a WorkerID")
        # A raw string is the compatibility spelling for the original result
        # contained-reference protocol.  Normalize it once so every owner-side
        # check and acknowledgement uses the typed source union.
        source = self.source
        if not isinstance(source, (ContainedTransferSource, TaskHoldSource)):
            raise ProtocolError(
                "borrow acquire source must be a contained transfer or task hold"
            )
        _validate_reference_token(self.borrower_token, "borrower_token")

@dataclass(frozen=True)
class AcquireBorrowedObjectReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    source: BorrowSource
    borrower_token: str
    accepted: bool
    acquired: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("borrow acquire reply object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("borrow acquire reply owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("borrow acquire reply borrower must be a WorkerID")
        source = self.source
        if not isinstance(source, (ContainedTransferSource, TaskHoldSource)):
            raise ProtocolError(
                "borrow acquire reply source must be a contained transfer or task hold"
            )
        _validate_reference_token(self.borrower_token, "borrower_token")
        _validate_acceptance_error(self.accepted, self.error, "borrow acquire reply")
        if not isinstance(self.acquired, bool):
            raise ProtocolError("borrow acquire reply acquired must be a bool")
        if not self.accepted and self.acquired:
            raise ProtocolError("a rejected borrow acquire cannot add a token")

@dataclass(frozen=True)
class ReleaseBorrowedObject:
    """Release a token while retaining an owner-side no-resurrection fence."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    borrower_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("borrow release object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("borrow release owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("borrow release borrower must be a WorkerID")
        _validate_reference_token(self.borrower_token, "borrower_token")


@dataclass(frozen=True)
class ReleaseBorrowedObjectReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    borrower_token: str
    accepted: bool
    released: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("borrow release reply object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("borrow release reply owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("borrow release reply borrower must be a WorkerID")
        _validate_reference_token(self.borrower_token, "borrower_token")
        _validate_acceptance_error(self.accepted, self.error, "borrow release reply")
        if not isinstance(self.released, bool):
            raise ProtocolError("borrow release reply released must be a bool")
        if not self.accepted and self.released:
            raise ProtocolError("a rejected borrow release cannot remove a token")


@dataclass(frozen=True)
class RetainOwnedObjectForTask:
    """Promote one active borrower handle into a task-lifetime hold."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    borrower_token: str
    hold: TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("task retain object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("task retain owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("task retain borrower must be a WorkerID")
        _validate_reference_token(self.borrower_token, "borrower_token")
        _validate_retained_task_hold(
            self.hold, self.borrower_worker_id, "task retain"
        )


@dataclass(frozen=True)
class RetainOwnedObjectForTaskReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    borrower_token: str
    hold: TaskReferenceHold
    accepted: bool
    retained: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        RetainOwnedObjectForTask(
            self.object_id, self.owner_worker_id, self.borrower_worker_id,
            self.borrower_token, self.hold,
        )
        _validate_acceptance_error(self.accepted, self.error, "task retain reply")
        if not isinstance(self.retained, bool):
            raise ProtocolError("task retain reply retained must be a bool")
        if not self.accepted and self.retained:
            raise ProtocolError("a rejected task retain cannot add a hold")


@dataclass(frozen=True)
class GetRetainedOwnedObject:
    """Read owner state through an independent task-lifetime hold."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    hold: TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("retained get object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("retained get owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("retained get borrower must be a WorkerID")
        _validate_retained_task_hold(
            self.hold, self.borrower_worker_id, "retained get"
        )


@dataclass(frozen=True)
class GetRetainedOwnedObjectReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    hold: TaskReferenceHold
    accepted: bool
    state: Optional[OwnedObjectState] = None
    data: Optional[bytes] = None
    error: Optional[RemoteErrorInfo] = None
    descriptor: Optional[ObjectStoreDescriptor] = None
    detail: Optional[str] = None
    current_attempt: Optional[AttemptID] = None

    def __post_init__(self) -> None:
        GetRetainedOwnedObject(
            self.object_id, self.owner_worker_id, self.borrower_worker_id,
            self.hold,
        )
        _validate_owned_object_reply(
            accepted=self.accepted, state=self.state, data=self.data,
            error=self.error, descriptor=self.descriptor, detail=self.detail,
            object_id=self.object_id, owner_worker_id=self.owner_worker_id,
            operation="retained get reply",
        )
        if not self.accepted and self.current_attempt is not None:
            raise ProtocolError(
                "rejected retained get reply cannot expose current attempt"
            )
        if self.current_attempt is not None:
            if not isinstance(self.current_attempt, AttemptID):
                raise ProtocolError(
                    "retained get current attempt must be an AttemptID or None"
                )
            if self.current_attempt.task_id != self.object_id.task_id:
                raise ProtocolError(
                    "retained get current attempt must belong to the object"
                )
        if (
            self.accepted
            and self.state is OwnedObjectState.READY_STORED
            and self.current_attempt is not None
            and self.descriptor is not None
            and self.descriptor.producer_attempt_id != self.current_attempt
        ):
            raise ProtocolError(
                "retained stored descriptor must match current attempt"
            )


@dataclass(frozen=True)
class ReportRetainedObjectLocation:
    """Report one target-sealed replica through an active task hold.

    The descriptor is deliberately byte-free.  A Node grant proves that the
    replica was sealed; this retained owner credential decides whether that
    physical fact belongs to the owner's current producer epoch.
    """

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    hold: TaskReferenceHold
    descriptor: ObjectStoreDescriptor

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError(
                "retained location report object_id must be an ObjectID"
            )
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError(
                "retained location report owner must be a WorkerID"
            )
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError(
                "retained location report borrower must be a WorkerID"
            )
        _validate_retained_task_hold(
            self.hold, self.borrower_worker_id, "retained location report"
        )
        if not isinstance(self.descriptor, ObjectStoreDescriptor):
            raise ProtocolError(
                "retained location report descriptor must be an "
                "ObjectStoreDescriptor"
            )
        if (
            self.descriptor.object_id != self.object_id
            or self.descriptor.owner_worker_id != self.owner_worker_id
        ):
            raise ProtocolError(
                "retained location report descriptor must match object and owner"
            )


class RetainedLocationReportStatus(str, Enum):
    """Owner-authoritative outcome of a retained location report."""

    ADDED = "ADDED"
    ALREADY_RECORDED = "ALREADY_RECORDED"
    CUSTODY_ONLY = "CUSTODY_ONLY"  # replica tracked, but this consumer may not execute
    STALE_PRODUCER = "STALE_PRODUCER"
    RETIRED = "RETIRED"  # owner retained exact cleanup; consumer must cancel, not Push
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class ReportRetainedObjectLocationReply:
    """Echo the complete report identity so ambiguous ACKs are replayable."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    hold: TaskReferenceHold
    descriptor: ObjectStoreDescriptor
    status: RetainedLocationReportStatus
    error: Optional[str] = None

    def __post_init__(self) -> None:
        # Reuse the request validator for the complete credential/descriptor
        # binding rather than maintaining a weaker reply-only copy.
        ReportRetainedObjectLocation(
            self.object_id, self.owner_worker_id, self.borrower_worker_id,
            self.hold, self.descriptor,
        )
        if not isinstance(self.status, RetainedLocationReportStatus):
            raise ProtocolError(
                "retained location report reply status is invalid"
            )
        if self.status in (
            RetainedLocationReportStatus.ADDED,
            RetainedLocationReportStatus.ALREADY_RECORDED,
        ):
            if self.error is not None:
                raise ProtocolError(
                    "accepted retained location report cannot contain an error"
                )
        elif not isinstance(self.error, str) or not self.error:
            raise ProtocolError(
                "rejected retained location report must contain an error"
            )

    @property
    def accepted(self) -> bool:
        return self.status in (
            RetainedLocationReportStatus.ADDED,
            RetainedLocationReportStatus.ALREADY_RECORDED,
        )

    @property
    def custody_transferred(self) -> bool:
        """Whether the owner tracks the replica or owns its exact cleanup.

        Custody alone is not permission to PushTask. A rejected consumer must
        still cancel its grant even after this physical handoff completed.
        """
        return self.status in (
            RetainedLocationReportStatus.ADDED,
            RetainedLocationReportStatus.ALREADY_RECORDED,
            RetainedLocationReportStatus.CUSTODY_ONLY,
            RetainedLocationReportStatus.RETIRED,
        )

    @property
    def added(self) -> bool:
        return self.status is RetainedLocationReportStatus.ADDED

    @property
    def stale(self) -> bool:
        return self.status is RetainedLocationReportStatus.STALE_PRODUCER


REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER = "report_abandoned_dependency_replica"


@dataclass(frozen=True)
class ReportAbandonedDependencyReplica:
    """A Node offers exact replica custody after its submitter has died.

    The route remains the original owner's route and hold. It is historical
    identity, not an active borrower or permission to execute the dead task.
    The receiver independently checks the death's authority before accepting
    custody; constructing this DTO is not itself proof of a process exit.
    """

    inventory: LeaseDependencyInventory
    descriptor: ObjectStoreDescriptor
    submitter_death: WorkerDeathRecord

    def __post_init__(self) -> None:
        try:
            inventory = revalidate_lease_dependency_inventory(self.inventory)
            descriptor = _revalidate_lease_descriptor(self.descriptor)
            if descriptor not in inventory.descriptors:
                raise ProtocolError("abandoned replica is not an exact witnessed inventory member")
            if not inventory.lease_request.dependency_owner_routes:
                raise ProtocolError("abandoned replica requires the original dependency owner routes")
            if type(self.submitter_death) is not WorkerDeathRecord or type(self.submitter_death.incarnation) is not WorkerIncarnation:
                raise ProtocolError("abandoned replica requires an exact Worker death proof")
            death = _revalidate_worker_death(self.submitter_death)
            if (death.worker_id != inventory.lease_request.requester_worker_id
                    or death.reason not in (WorkerDeathReason.PROCESS_EXIT, WorkerDeathReason.NODE_EXIT)):
                raise ProtocolError("abandoned replica death does not identify the original submitting Worker")
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid abandoned dependency replica: {exc}") from exc
        object.__setattr__(self, "inventory", inventory)
        object.__setattr__(self, "descriptor", descriptor)
        object.__setattr__(self, "submitter_death", death)

    @property
    def owner_route(self) -> DependencyOwnerRoute:
        return next(route for route in self.inventory.lease_request.dependency_owner_routes
                    if route.object_id == self.descriptor.object_id)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.inventory, self.descriptor, self.submitter_death,
        ))


@dataclass(frozen=True)
class ReportAbandonedDependencyReplicaReply:
    """Custody-only acknowledgement; no status grants task execution."""

    request: ReportAbandonedDependencyReplica
    status: RetainedLocationReportStatus
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if type(self.request) is not ReportAbandonedDependencyReplica:
            raise ProtocolError("abandoned replica reply requires an exact request")
        request = ReportAbandonedDependencyReplica(
            self.request.inventory, self.request.descriptor, self.request.submitter_death,
        )
        if type(self.status) is not RetainedLocationReportStatus or self.status not in (
            RetainedLocationReportStatus.CUSTODY_ONLY, RetainedLocationReportStatus.RETIRED,
            RetainedLocationReportStatus.REJECTED, RetainedLocationReportStatus.STALE_PRODUCER,
        ):
            raise ProtocolError("abandoned replica reply cannot authorize execution")
        if self.custody_transferred:
            if self.error is not None and (type(self.error) is not str or not self.error):
                raise ProtocolError("abandoned custody explanation must be a non-empty string or None")
        elif type(self.error) is not str or not self.error:
            raise ProtocolError("abandoned replica rejection must explain its failure")
        object.__setattr__(self, "request", request)

    @property
    def custody_transferred(self) -> bool:
        return self.status in (RetainedLocationReportStatus.CUSTODY_ONLY, RetainedLocationReportStatus.RETIRED)

    @property
    def accepted(self) -> bool:
        """Acceptance of custody only, never permission to Push or execute."""
        return self.custody_transferred

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (self.request, self.status, self.error))


@dataclass(frozen=True)
class ReleaseOwnedObjectForTask:
    """Release a task hold without releasing its source transfer pin."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    hold: TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("task release object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("task release owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("task release borrower must be a WorkerID")
        _validate_retained_task_hold(
            self.hold, self.borrower_worker_id, "task release"
        )


@dataclass(frozen=True)
class ReleaseOwnedObjectForTaskReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    hold: TaskReferenceHold
    accepted: bool
    released: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        ReleaseOwnedObjectForTask(
            self.object_id, self.owner_worker_id, self.borrower_worker_id,
            self.hold,
        )
        _validate_acceptance_error(
            self.accepted, self.error, "task release reply"
        )
        if not isinstance(self.released, bool):
            raise ProtocolError("task release reply released must be a bool")
        if not self.accepted and self.released:
            raise ProtocolError("a rejected task release cannot remove a hold")


@dataclass(frozen=True)
class ReleaseContainedReference:
    """Release one transfer pin at the contained object's logical owner."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    hold: IncomingContainedReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("contained release object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("contained release owner must be a WorkerID")
        object.__setattr__(
            self, "hold",
            _normalize_contained_reference_hold(
                self.hold, "contained release"
            ),
        )

    @property
    def transfer_token(self) -> str:
        """Compatibility projection; ``hold`` is the wire authority."""

        token = self.hold.transfer_token
        assert isinstance(token, str)
        return token


@dataclass(frozen=True)
class ReleaseContainedReferenceReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    hold: IncomingContainedReferenceHold
    accepted: bool
    released: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("contained release reply object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("contained release reply owner must be a WorkerID")
        object.__setattr__(
            self, "hold",
            _normalize_contained_reference_hold(
                self.hold, "contained release reply"
            ),
        )
        _validate_acceptance_error(
            self.accepted, self.error, "contained release reply"
        )
        if not isinstance(self.released, bool):
            raise ProtocolError("contained release reply released must be a bool")
        if not self.accepted and self.released:
            raise ProtocolError("a rejected contained release cannot remove a pin")

    @property
    def transfer_token(self) -> str:
        """Compatibility projection; ``hold`` is the echoed identity."""

        token = self.hold.transfer_token
        assert isinstance(token, str)
        return token


@dataclass(frozen=True)
class GetOwnedObject:
    """Read logical state from the owner using an active borrower token."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    borrower_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("owned get object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("owned get owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("owned get borrower must be a WorkerID")
        _validate_reference_token(self.borrower_token, "borrower_token")


@dataclass(frozen=True)
class GetOwnedObjectReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    borrower_token: str
    accepted: bool
    state: Optional[OwnedObjectState] = None
    current_attempt: Optional[AttemptID] = None
    data: Optional[bytes] = None
    error: Optional[RemoteErrorInfo] = None
    descriptor: Optional[ObjectStoreDescriptor] = None
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("owned get reply object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("owned get reply owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("owned get reply borrower must be a WorkerID")
        _validate_reference_token(self.borrower_token, "borrower_token")
        if not isinstance(self.accepted, bool):
            raise ProtocolError("owned get reply accepted must be a bool")
        if not self.accepted:
            if (
                self.state is not None
                or self.current_attempt is not None
                or self.data is not None
                or self.error is not None
                or self.descriptor is not None
            ):
                raise ProtocolError("rejected owned get cannot expose object state")
            if not isinstance(self.detail, str) or not self.detail:
                raise ProtocolError("rejected owned get must contain a detail")
            return
        if not isinstance(self.state, OwnedObjectState):
            raise ProtocolError("accepted owned get must contain an object state")
        if (
            self.current_attempt is not None
            and not isinstance(self.current_attempt, AttemptID)
        ):
            raise ProtocolError(
                "owned get current attempt must be an AttemptID or None"
            )
        if (
            isinstance(self.current_attempt, AttemptID)
            and self.current_attempt.task_id != self.object_id.task_id
        ):
            raise ProtocolError(
                "owned get current attempt must belong to the object"
            )
        if self.detail is not None:
            raise ProtocolError("accepted owned get cannot contain rejection detail")
        if self.state is OwnedObjectState.READY_INLINE:
            if (
                not isinstance(self.data, bytes)
                or self.error is not None
                or self.descriptor is not None
            ):
                raise ProtocolError("inline owned object must contain only bytes")
        elif self.state is OwnedObjectState.READY_STORED:
            if (
                not isinstance(self.descriptor, ObjectStoreDescriptor)
                or self.data is not None
                or self.error is not None
            ):
                raise ProtocolError(
                    "stored owned object must contain only a descriptor"
                )
            if (
                self.descriptor.object_id != self.object_id
                or self.descriptor.owner_worker_id != self.owner_worker_id
                or (
                    self.current_attempt is not None
                    and self.descriptor.producer_attempt_id
                    != self.current_attempt
                )
            ):
                raise ProtocolError(
                    "stored owned descriptor must match object, owner, and "
                    "current-attempt identity"
                )
        elif self.state is OwnedObjectState.ERROR:
            if (
                not isinstance(self.error, RemoteErrorInfo)
                or self.data is not None
                or self.descriptor is not None
            ):
                raise ProtocolError("failed owned object must contain only an error")
        elif (
            self.data is not None
            or self.error is not None
            or self.descriptor is not None
        ):
            raise ProtocolError(
                "pending or lost owned object cannot expose result payload"
            )


class OwnedObjectReconstructionDisposition(str, Enum):
    """Owner-authoritative outcome of a foreign reconstruction request."""

    STARTED = "STARTED"
    JOINED = "JOINED"
    FAILED = "FAILED"


class OwnedObjectReconstructionFailure(str, Enum):
    """Typed reason an owner did not start or join reconstruction.

    These values deliberately separate lifetime, lineage, retry-budget, and
    liveness facts.  In particular, a transport timeout is not OWNER_DEAD; that
    value may be produced only after an authoritative Worker-death record has
    been consumed.
    """

    WRONG_OWNER = "WRONG_OWNER"
    OWNER_DEAD = "OWNER_DEAD"
    UNKNOWN_OBJECT = "UNKNOWN_OBJECT"
    INACTIVE_CREDENTIAL = "INACTIVE_CREDENTIAL"
    RELEASED_CREDENTIAL = "RELEASED_CREDENTIAL"
    CREDENTIAL_MISMATCH = "CREDENTIAL_MISMATCH"
    REQUEST_CONFLICT = "REQUEST_CONFLICT"
    EXPECTED_ATTEMPT_MISMATCH = "EXPECTED_ATTEMPT_MISMATCH"
    NOT_LOST = "NOT_LOST"
    PUT_OBJECT = "PUT_OBJECT"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    UNRECONSTRUCTABLE = "UNRECONSTRUCTABLE"
    COLLECTION_IN_PROGRESS = "COLLECTION_IN_PROGRESS"
    AUTHORITY_REJECTED = "AUTHORITY_REJECTED"


@dataclass(frozen=True, order=True)
class BorrowedCredential:
    """An active ordinary borrower plus its immutable export binding."""

    source: BorrowSource
    borrower_token: str

    def __post_init__(self) -> None:
        source = self.source
        if not isinstance(source, (ContainedTransferSource, TaskHoldSource)):
            raise ProtocolError(
                "borrowed reconstruction source must be a contained transfer "
                "or task hold"
            )
        _validate_reference_token(self.borrower_token, "borrower_token")


@dataclass(frozen=True, order=True)
class RetainedCredential:
    """An independently active retained Task hold used after handle close."""

    hold: TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.hold, TaskReferenceHold):
            raise ProtocolError(
                "retained reconstruction credential requires a TaskReferenceHold"
            )
        if self.hold.kind is not TaskReferenceHoldKind.RETAINED:
            raise ProtocolError(
                "retained reconstruction credential hold must have RETAINED kind"
            )


ReconstructionCredential = Union[
    BorrowedCredential, RetainedCredential
]


@dataclass(frozen=True, init=False)
class RequestOwnedObjectReconstruction:
    """Ask the logical owner to reconstruct one foreign object.

    ``credential`` is either an ordinary borrower plus its export binding, or
    an independently active retained Task hold. ``borrower_token`` remains as
    the Phase-2A compatibility echo and is ``None`` for retained credentials.

    ``expected_owner_attempt`` is the request's idempotency epoch.  STARTED and
    JOINED replies always name a strictly newer physical attempt while the
    logical ObjectID remains stable.
    """

    object_id: ObjectID
    owner_worker_id: WorkerID
    requester_worker_id: WorkerID
    credential: ReconstructionCredential | BorrowSource
    borrower_token: Optional[str]
    expected_owner_attempt: AttemptID

    def __init__(
        self,
        object_id: ObjectID,
        owner_worker_id: WorkerID,
        requester_worker_id: WorkerID,
        credential: ReconstructionCredential | BorrowSource | None = None,
        borrower_token: Optional[str] = None,
        expected_owner_attempt: Optional[AttemptID] = None,
        *,
        source: BorrowSource | None = None,
    ) -> None:
        # ``source=...`` is the Phase-2A keyword spelling. Keep accepting it
        # while normalizing both old and new calls to one credential field.
        if credential is None:
            credential = source
        elif source is not None and source != credential:
            raise ProtocolError(
                "owned reconstruction source conflicts with credential"
            )
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "owner_worker_id", owner_worker_id)
        object.__setattr__(self, "requester_worker_id", requester_worker_id)
        object.__setattr__(self, "credential", credential)
        object.__setattr__(self, "borrower_token", borrower_token)
        object.__setattr__(
            self, "expected_owner_attempt", expected_owner_attempt
        )
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError(
                "owned reconstruction object_id must be an ObjectID"
            )
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError(
                "owned reconstruction owner must be a WorkerID"
            )
        if not isinstance(self.requester_worker_id, WorkerID):
            raise ProtocolError(
                "owned reconstruction requester must be a WorkerID"
            )
        credential = self.credential
        if isinstance(credential, (str, ContainedTransferSource, TaskHoldSource)):
            if self.borrower_token is None:
                raise ProtocolError(
                    "borrowed reconstruction compatibility form requires "
                    "borrower_token"
                )
            credential = BorrowedCredential(
                credential, self.borrower_token
            )
            object.__setattr__(self, "credential", credential)
        if not isinstance(
            credential,
            (BorrowedCredential, RetainedCredential),
        ):
            raise ProtocolError(
                "owned reconstruction credential must be borrowed or retained"
            )
        if isinstance(credential, BorrowedCredential):
            if self.borrower_token != credential.borrower_token:
                raise ProtocolError(
                    "borrowed reconstruction token must match its credential"
                )
        elif self.borrower_token is not None:
            raise ProtocolError(
                "retained reconstruction credential cannot contain borrower_token"
            )
        if (
            isinstance(credential, RetainedCredential)
            and credential.hold.submitting_worker_id != self.requester_worker_id
        ):
            raise ProtocolError(
                "retained reconstruction hold must belong to requester"
            )
        if not isinstance(self.expected_owner_attempt, AttemptID):
            raise ProtocolError(
                "owned reconstruction expected attempt must be an AttemptID"
            )
        if self.expected_owner_attempt.task_id != self.object_id.task_id:
            raise ProtocolError(
                "owned reconstruction expected attempt must belong to object"
            )

    @property
    def source(self) -> BorrowSource | None:
        """Compatibility projection for ordinary borrower callers."""

        credential = self.credential
        return (
            credential.source
            if isinstance(credential, BorrowedCredential)
            else None
        )


@dataclass(frozen=True, init=False)
class RequestOwnedObjectReconstructionReply:
    """Exact identity echo plus a START/JOIN attempt or typed failure."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    requester_worker_id: WorkerID
    credential: ReconstructionCredential | BorrowSource
    borrower_token: Optional[str]
    expected_owner_attempt: AttemptID
    disposition: OwnedObjectReconstructionDisposition
    reconstruction_attempt: Optional[AttemptID] = None
    failure: Optional[OwnedObjectReconstructionFailure] = None
    detail: Optional[str] = None

    def __init__(
        self,
        object_id: ObjectID,
        owner_worker_id: WorkerID,
        requester_worker_id: WorkerID,
        credential: ReconstructionCredential | BorrowSource | None = None,
        borrower_token: Optional[str] = None,
        expected_owner_attempt: Optional[AttemptID] = None,
        disposition: Optional[OwnedObjectReconstructionDisposition] = None,
        reconstruction_attempt: Optional[AttemptID] = None,
        failure: Optional[OwnedObjectReconstructionFailure] = None,
        detail: Optional[str] = None,
        *,
        source: BorrowSource | None = None,
    ) -> None:
        if credential is None:
            credential = source
        elif source is not None and source != credential:
            raise ProtocolError(
                "owned reconstruction reply source conflicts with credential"
            )
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "owner_worker_id", owner_worker_id)
        object.__setattr__(self, "requester_worker_id", requester_worker_id)
        object.__setattr__(self, "credential", credential)
        object.__setattr__(self, "borrower_token", borrower_token)
        object.__setattr__(
            self, "expected_owner_attempt", expected_owner_attempt
        )
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(
            self, "reconstruction_attempt", reconstruction_attempt
        )
        object.__setattr__(self, "failure", failure)
        object.__setattr__(self, "detail", detail)
        self.__post_init__()

    def __post_init__(self) -> None:
        # Reuse the request validator so a reply cannot weaken or partially
        # echo the capability and expected-attempt identity.
        request = RequestOwnedObjectReconstruction(
            self.object_id,
            self.owner_worker_id,
            self.requester_worker_id,
            self.credential,
            self.borrower_token,
            self.expected_owner_attempt,
        )
        if request.credential != self.credential:
            object.__setattr__(self, "credential", request.credential)
        if not isinstance(
            self.disposition, OwnedObjectReconstructionDisposition
        ):
            raise ProtocolError(
                "owned reconstruction reply disposition is invalid"
            )
        if self.disposition in (
            OwnedObjectReconstructionDisposition.STARTED,
            OwnedObjectReconstructionDisposition.JOINED,
        ):
            if not isinstance(self.reconstruction_attempt, AttemptID):
                raise ProtocolError(
                    "started or joined reconstruction requires an attempt"
                )
            if (
                self.reconstruction_attempt.task_id
                != self.expected_owner_attempt.task_id
                or self.reconstruction_attempt.attempt_number
                <= self.expected_owner_attempt.attempt_number
            ):
                raise ProtocolError(
                    "reconstruction attempt must be newer than expected attempt"
                )
            if self.failure is not None or self.detail is not None:
                raise ProtocolError(
                    "started or joined reconstruction cannot contain a failure"
                )
            return
        if self.reconstruction_attempt is not None:
            raise ProtocolError(
                "failed reconstruction cannot contain an attempt"
            )
        if not isinstance(self.failure, OwnedObjectReconstructionFailure):
            raise ProtocolError(
                "failed reconstruction requires a typed failure"
            )
        if not isinstance(self.detail, str) or not self.detail:
            raise ProtocolError(
                "failed reconstruction requires a non-empty detail"
            )

    @property
    def source(self) -> BorrowSource | None:
        """Compatibility projection for ordinary borrower callers."""

        credential = self.credential
        return (
            credential.source
            if isinstance(credential, BorrowedCredential)
            else None
        )


class ReplaceRetainedObjectDisposition(str, Enum):
    REPLACED = "REPLACED"
    ALREADY_REPLACED = "ALREADY_REPLACED"
    FAILED = "FAILED"


class ReplaceRetainedObjectFailure(str, Enum):
    WRONG_OWNER = "WRONG_OWNER"
    # The endpoint itself authoritatively fenced owner protocol admission.
    # This is a shutdown fact, not a Worker-death proof.
    OWNER_STOPPED = "OWNER_STOPPED"
    DEAD_BORROWER = "DEAD_BORROWER"
    UNKNOWN_OBJECT = "UNKNOWN_OBJECT"
    INACTIVE_OLD_HOLD = "INACTIVE_OLD_HOLD"
    RELEASED_OLD_HOLD = "RELEASED_OLD_HOLD"
    REPLACEMENT_RELEASED = "REPLACEMENT_RELEASED"
    OLD_HOLD_BUSY = "OLD_HOLD_BUSY"
    CONFLICT = "CONFLICT"
    COLLECTION_IN_PROGRESS = "COLLECTION_IN_PROGRESS"


@dataclass(frozen=True)
class ReplaceRetainedObjectForTask:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    expected_hold: TaskReferenceHold
    replacement_hold: TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("retained replacement object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("retained replacement owner must be a WorkerID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("retained replacement borrower must be a WorkerID")
        _validate_retained_task_hold(
            self.expected_hold, self.borrower_worker_id, "retained replacement"
        )
        _validate_retained_task_hold(
            self.replacement_hold, self.borrower_worker_id,
            "retained replacement",
        )
        if (
            self.expected_hold.submitting_worker_id
            != self.replacement_hold.submitting_worker_id
            or self.expected_hold.task_id != self.replacement_hold.task_id
        ):
            raise ProtocolError(
                "retained replacement holds must share submitter and task"
            )
        if (
            self.replacement_hold.origin_attempt_id.attempt_number
            <= self.expected_hold.origin_attempt_id.attempt_number
        ):
            raise ProtocolError(
                "replacement hold origin attempt must increase"
            )


@dataclass(frozen=True)
class ReplaceRetainedObjectForTaskReply:
    object_id: ObjectID
    owner_worker_id: WorkerID
    borrower_worker_id: WorkerID
    expected_hold: TaskReferenceHold
    replacement_hold: TaskReferenceHold
    disposition: ReplaceRetainedObjectDisposition
    failure: Optional[ReplaceRetainedObjectFailure] = None
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        ReplaceRetainedObjectForTask(
            self.object_id, self.owner_worker_id, self.borrower_worker_id,
            self.expected_hold, self.replacement_hold,
        )
        if not isinstance(self.disposition, ReplaceRetainedObjectDisposition):
            raise ProtocolError("retained replacement disposition is invalid")
        if self.disposition in (
            ReplaceRetainedObjectDisposition.REPLACED,
            ReplaceRetainedObjectDisposition.ALREADY_REPLACED,
        ):
            if self.failure is not None or self.detail is not None:
                raise ProtocolError(
                    "successful retained replacement cannot contain failure"
                )
            return
        if not isinstance(self.failure, ReplaceRetainedObjectFailure):
            raise ProtocolError(
                "failed retained replacement requires a typed failure"
            )
        if not isinstance(self.detail, str) or not self.detail:
            raise ProtocolError(
                "failed retained replacement requires a non-empty detail"
            )


class DropOwnedObjectDisposition(str, Enum):
    """Owner-authoritative result of one borrower debug-drop request."""

    DROPPED = "DROPPED"
    ALREADY_DROPPED = "ALREADY_DROPPED"
    FAILED = "FAILED"


class DropOwnedObjectFailure(str, Enum):
    WRONG_OWNER = "WRONG_OWNER"
    UNKNOWN_OBJECT = "UNKNOWN_OBJECT"
    INACTIVE_CREDENTIAL = "INACTIVE_CREDENTIAL"
    RELEASED_CREDENTIAL = "RELEASED_CREDENTIAL"
    CREDENTIAL_MISMATCH = "CREDENTIAL_MISMATCH"
    EXPECTED_ATTEMPT_MISMATCH = "EXPECTED_ATTEMPT_MISMATCH"
    NOT_STORED = "NOT_STORED"
    UNKNOWN_REPLICA = "UNKNOWN_REPLICA"
    NODE_REJECTED = "NODE_REJECTED"
    OWNER_STOPPED = "OWNER_STOPPED"
    REQUEST_CONFLICT = "REQUEST_CONFLICT"


@dataclass(frozen=True)
class RequestDropOwnedObject:
    """Ask an owner to delete one replica using an active capability."""

    operation_id: str
    object_id: ObjectID
    owner_worker_id: WorkerID
    requester_worker_id: WorkerID
    source: BorrowSource
    borrower_token: str
    expected_owner_attempt: AttemptID
    node_id: Optional[NodeID] = None

    def __post_init__(self) -> None:
        _validate_reference_token(self.operation_id, "drop operation_id")
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("owned drop object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("owned drop owner must be a WorkerID")
        if not isinstance(self.requester_worker_id, WorkerID):
            raise ProtocolError("owned drop requester must be a WorkerID")
        source = self.source
        if not isinstance(source, (ContainedTransferSource, TaskHoldSource)):
            raise ProtocolError(
                "owned drop source must be a contained transfer or task hold"
            )
        _validate_reference_token(self.borrower_token, "borrower_token")
        if (
            not isinstance(self.expected_owner_attempt, AttemptID)
            or self.expected_owner_attempt.task_id != self.object_id.task_id
        ):
            raise ProtocolError(
                "owned drop expected attempt must belong to the object"
            )
        if self.node_id is not None and not isinstance(self.node_id, NodeID):
            raise ProtocolError("owned drop node_id must be a NodeID or None")


@dataclass(frozen=True)
class RequestDropOwnedObjectReply:
    """Exact request echo plus the owner-selected replica and outcome."""

    operation_id: str
    object_id: ObjectID
    owner_worker_id: WorkerID
    requester_worker_id: WorkerID
    source: BorrowSource
    borrower_token: str
    expected_owner_attempt: AttemptID
    requested_node_id: Optional[NodeID]
    disposition: DropOwnedObjectDisposition
    dropped_node_id: Optional[NodeID] = None
    failure: Optional[DropOwnedObjectFailure] = None
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        request = RequestDropOwnedObject(
            self.operation_id, self.object_id, self.owner_worker_id,
            self.requester_worker_id, self.source, self.borrower_token,
            self.expected_owner_attempt, self.requested_node_id,
        )
        if request.source != self.source:
            object.__setattr__(self, "source", request.source)
        if not isinstance(self.disposition, DropOwnedObjectDisposition):
            raise ProtocolError("owned drop reply disposition is invalid")
        if self.disposition in (
            DropOwnedObjectDisposition.DROPPED,
            DropOwnedObjectDisposition.ALREADY_DROPPED,
        ):
            if (
                self.disposition is DropOwnedObjectDisposition.DROPPED
                and not isinstance(self.dropped_node_id, NodeID)
            ):
                raise ProtocolError(
                    "newly dropped owned object must name the selected node"
                )
            if (
                self.dropped_node_id is not None
                and not isinstance(self.dropped_node_id, NodeID)
            ):
                raise ProtocolError(
                    "owned drop selected node must be a NodeID or None"
                )
            if (
                self.requested_node_id is not None
                and self.dropped_node_id != self.requested_node_id
            ):
                raise ProtocolError(
                    "owned drop reply selected a different requested node"
                )
            if self.failure is not None or self.detail is not None:
                raise ProtocolError(
                    "acknowledged owned drop cannot contain a failure"
                )
            return
        if self.dropped_node_id is not None:
            raise ProtocolError("failed owned drop cannot name a dropped node")
        if not isinstance(self.failure, DropOwnedObjectFailure):
            raise ProtocolError("failed owned drop requires a typed failure")
        if not isinstance(self.detail, str) or not self.detail:
            raise ProtocolError("failed owned drop requires detail")


@dataclass(frozen=True)
class ActorClassDefinition:
    """Serialized Actor class plus its explicitly exported method surface."""

    key: FunctionKey
    payload: bytes
    sha256: str
    method_names: Tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.key, FunctionKey):
            raise ProtocolError("actor class key must be a FunctionKey")
        if not isinstance(self.payload, bytes):
            raise ProtocolError("serialized actor class payload must be bytes")
        if hashlib.sha256(self.payload).hexdigest() != self.sha256:
            raise ProtocolError("actor class payload checksum mismatch")
        object.__setattr__(self, "method_names", tuple(self.method_names))
        if not self.method_names:
            raise ProtocolError("actor class must export at least one method")
        if any(
            not isinstance(name, str) or not name for name in self.method_names
        ):
            raise ProtocolError("actor method names must be non-empty strings")
        if len(self.method_names) != len(set(self.method_names)):
            raise ProtocolError("actor method names must be unique")


def _validate_actor_identity(
    actor_id: object, generation: object, operation: str
) -> None:
    if not isinstance(actor_id, ActorID):
        raise ProtocolError("{} actor_id must be an ActorID".format(operation))
    if not isinstance(generation, ActorGeneration):
        raise ProtocolError(
            "{} generation must be an ActorGeneration".format(operation)
        )
    if generation.actor_id != actor_id:
        raise ProtocolError(
            "{} generation must belong to actor_id".format(operation)
        )


def _validate_actor_endpoint_reply(
    *,
    actor_id: object,
    generation: object,
    accepted: object,
    node_id: object,
    worker_id: object,
    worker_address: object,
    worker_pid: object,
    error: Optional[str],
    operation: str,
) -> None:
    _validate_actor_identity(actor_id, generation, operation)
    if not isinstance(accepted, bool):
        raise ProtocolError("{} accepted must be a bool".format(operation))
    endpoint = (node_id, worker_id, worker_address, worker_pid)
    if accepted:
        if any(value is None for value in endpoint):
            raise ProtocolError("accepted {} must contain a complete endpoint".format(operation))
        if not isinstance(node_id, NodeID):
            raise ProtocolError("{} node_id must be a NodeID".format(operation))
        if not isinstance(worker_id, WorkerID):
            raise ProtocolError("{} worker_id must be a WorkerID".format(operation))
        _validate_bound_address(worker_address, "{} worker_address".format(operation))
        if isinstance(worker_pid, bool) or not isinstance(worker_pid, int) or worker_pid <= 0:
            raise ProtocolError("{} worker_pid must be positive".format(operation))
        if error is not None:
            raise ProtocolError("accepted {} cannot contain an error".format(operation))
    else:
        if any(value is not None for value in endpoint):
            raise ProtocolError("rejected {} cannot contain an endpoint".format(operation))
        if not isinstance(error, str) or not error:
            raise ProtocolError("rejected {} must contain an error".format(operation))


class ActorState(str, Enum):
    """GCS-authoritative lifecycle of one logical Actor.

    ``PENDING`` and ``FAILED`` are compatibility aliases for the old K0 names.
    The canonical names make the two non-routable phases explicit: creation has
    not published an incarnation yet, while restart has fenced the old route but
    has not published the next incarnation.
    """

    CREATING = "CREATING"
    PENDING = "CREATING"
    ALIVE = "ALIVE"
    RESTARTING = "RESTARTING"
    DEAD = "DEAD"
    FAILED = "DEAD"


class ActorWorkerExitDisposition(str, Enum):
    """Result of reducing one physical Actor-worker exit proof."""

    APPLIED = "APPLIED"
    ALREADY_APPLIED = "ALREADY_APPLIED"
    RETRYABLE = "RETRYABLE"
    UNKNOWN = "UNKNOWN"
    STALE = "STALE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class ActorWorkerExitRecord:
    """Immutable proof that one exact Actor incarnation exited."""

    detection_id: str
    actor_id: ActorID
    generation: ActorGeneration
    route_epoch: int
    node_id: NodeID
    node_pid: int
    registration_epoch: int
    worker_id: WorkerID
    worker_pid: int
    exit_code: int

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ProtocolError("actor exit detection_id must be non-empty")
        _validate_actor_identity(self.actor_id, self.generation, "actor exit")
        _validate_non_negative_integer(self.route_epoch, "actor exit route_epoch")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("actor exit node_id must be a NodeID")
        _validate_node_pid(self.node_pid, "actor exit")
        _validate_positive_epoch(
            self.registration_epoch, "actor exit registration_epoch"
        )
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("actor exit worker_id must be a WorkerID")
        if (
            isinstance(self.worker_pid, bool)
            or not isinstance(self.worker_pid, int)
            or self.worker_pid <= 0
        ):
            raise ProtocolError("actor exit worker_pid must be positive")
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise ProtocolError("actor exit exit_code must be an integer")


@dataclass(frozen=True)
class ActorSnapshot:
    """Payload-free authoritative view of one logical Actor.

    A physical endpoint is deliberately exposed only while ``state`` is
    ``ALIVE``.  ``route_epoch`` is the owner's monotonic compare-and-install
    token; ``generation`` alone is insufficient because the non-routable
    RESTARTING publication and the following ALIVE publication are separate
    commits.
    """

    actor_id: ActorID
    generation: ActorGeneration
    state: ActorState
    route_epoch: int
    restarts_used: int
    max_restarts: int
    last_exit: Optional[ActorWorkerExitRecord] = None
    node_id: Optional[NodeID] = None
    worker_id: Optional[WorkerID] = None
    worker_address: Optional[Tuple[str, int]] = None
    worker_pid: Optional[int] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_actor_identity(self.actor_id, self.generation, "actor snapshot")
        if not isinstance(self.state, ActorState):
            raise ProtocolError("actor snapshot state must be an ActorState")
        _validate_non_negative_integer(self.route_epoch, "actor route_epoch")
        _validate_non_negative_integer(self.restarts_used, "actor restarts_used")
        _validate_non_negative_integer(self.max_restarts, "actor max_restarts")
        if self.restarts_used > self.max_restarts:
            raise ProtocolError("actor restarts_used cannot exceed max_restarts")
        if self.generation.generation != self.restarts_used:
            raise ProtocolError(
                "actor generation must equal the number of consumed restarts"
            )
        if self.last_exit is not None:
            if (
                not isinstance(
                    self.last_exit, ActorWorkerExitRecord
                )
                or self.last_exit.actor_id != self.actor_id
            ):
                raise ProtocolError(
                    "actor snapshot last_exit must belong to actor_id"
                )
            if self.last_exit.generation.generation > self.generation.generation:
                raise ProtocolError("actor snapshot last_exit is from the future")
            if self.route_epoch <= self.last_exit.route_epoch:
                raise ProtocolError(
                    "actor snapshot route_epoch must advance beyond last_exit"
                )
        endpoint = (
            self.node_id, self.worker_id, self.worker_address, self.worker_pid
        )
        if self.state is ActorState.ALIVE:
            if any(value is None for value in endpoint):
                raise ProtocolError("ALIVE actor snapshot requires a complete endpoint")
            if not isinstance(self.node_id, NodeID):
                raise ProtocolError("actor snapshot node_id must be a NodeID")
            if not isinstance(self.worker_id, WorkerID):
                raise ProtocolError("actor snapshot worker_id must be a WorkerID")
            _validate_bound_address(
                self.worker_address, "actor snapshot worker_address"
            )
            if (
                isinstance(self.worker_pid, bool)
                or not isinstance(self.worker_pid, int)
                or self.worker_pid <= 0
            ):
                raise ProtocolError("actor snapshot worker_pid must be positive")
            if self.error is not None:
                raise ProtocolError("ALIVE actor snapshot cannot contain an error")
        elif any(value is not None for value in endpoint):
            raise ProtocolError(
                "only an ALIVE actor snapshot may expose an endpoint"
            )
        if self.state is ActorState.CREATING:
            if (
                self.generation.generation != 0
                or self.restarts_used != 0
                or self.route_epoch != 0
                or self.last_exit is not None
                or self.error is not None
            ):
                raise ProtocolError(
                    "CREATING actor snapshot must be the empty generation-zero route"
                )
        if self.state is ActorState.RESTARTING:
            if self.last_exit is None:
                raise ProtocolError("RESTARTING actor snapshot requires last_exit")
            if self.last_exit.generation.next() != self.generation:
                raise ProtocolError(
                    "RESTARTING generation must immediately follow last_exit"
                )
            if self.error is not None:
                raise ProtocolError("RESTARTING actor snapshot cannot contain an error")
        if (
            self.state is ActorState.ALIVE
            and self.generation.generation == 0
            and self.last_exit is not None
        ):
            raise ProtocolError("initial ALIVE actor cannot contain last_exit")
        if (
            self.state is ActorState.ALIVE
            and self.generation.generation == 0
            and self.route_epoch == 0
        ):
            raise ProtocolError(
                "initial ALIVE actor route_epoch must be positive"
            )
        if (
            self.state is ActorState.ALIVE
            and self.generation.generation > 0
            and self.last_exit is None
        ):
            raise ProtocolError(
                "restarted ALIVE actor requires its preceding exit"
            )
        if (
            self.state is ActorState.ALIVE
            and self.last_exit is not None
            and self.last_exit.generation.next() != self.generation
        ):
            raise ProtocolError(
                "ALIVE actor last_exit must immediately precede its generation"
            )
        if self.state is ActorState.DEAD and not self.error:
            raise ProtocolError("DEAD actor snapshot requires an error")


@dataclass(frozen=True)
class CreateActorRequest:
    actor_id: ActorID
    generation: ActorGeneration
    class_definition: ActorClassDefinition
    constructor_payload: bytes
    resources: ResourceVector
    owner_worker_id: WorkerID
    max_restarts: int = 0
    owner_address: Optional[Tuple[str, int]] = None

    def __post_init__(self) -> None:
        _validate_actor_identity(self.actor_id, self.generation, "create actor")
        if not isinstance(self.class_definition, ActorClassDefinition):
            raise ProtocolError("class_definition must be an ActorClassDefinition")
        if not isinstance(self.constructor_payload, bytes):
            raise ProtocolError("constructor_payload must be bytes")
        if not isinstance(self.resources, ResourceVector):
            raise ProtocolError("actor resources must be a ResourceVector")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("actor owner_worker_id must be a WorkerID")
        _validate_non_negative_integer(self.max_restarts, "actor max_restarts")
        if self.generation.generation != 0:
            raise ProtocolError("actor creation must start at generation zero")
        if self.owner_address is not None:
            object.__setattr__(
                self, "owner_address",
                _validate_bound_address(self.owner_address, "actor owner_address"),
            )
        if self.max_restarts > 0 and self.owner_address is None:
            raise ProtocolError(
                "a restartable actor requires a bound owner_address"
            )


@dataclass(frozen=True)
class CreateActorReply:
    actor_id: ActorID
    generation: ActorGeneration
    accepted: bool
    node_id: Optional[NodeID] = None
    worker_id: Optional[WorkerID] = None
    worker_address: Optional[Tuple[str, int]] = None
    worker_pid: Optional[int] = None
    error: Optional[str] = None
    retryable: bool = False
    route_epoch: int = 0

    def __post_init__(self) -> None:
        _validate_actor_endpoint_reply(
            actor_id=self.actor_id, generation=self.generation, accepted=self.accepted,
            node_id=self.node_id, worker_id=self.worker_id,
            worker_address=self.worker_address, worker_pid=self.worker_pid,
            error=self.error, operation="create actor reply",
        )
        if not isinstance(self.retryable, bool):
            raise ProtocolError("create actor reply retryable must be a bool")
        if self.accepted and self.retryable:
            raise ProtocolError("accepted actor creation cannot be retryable")
        _validate_non_negative_integer(self.route_epoch, "actor route_epoch")
        if self.accepted and self.route_epoch == 0:
            raise ProtocolError(
                "accepted actor creation requires a positive route_epoch"
            )


@dataclass(frozen=True)
class ReserveActorWorkerRequest:
    actor_id: ActorID
    generation: ActorGeneration
    class_definition: ActorClassDefinition
    constructor_payload: bytes
    resources: ResourceVector
    owner_worker_id: WorkerID
    target_node_id: NodeID
    route_epoch: int = 0
    restart: Optional[ActorWorkerExitRecord] = None

    def __post_init__(self) -> None:
        _validate_actor_identity(
            self.actor_id, self.generation, "reserve actor worker"
        )
        if not isinstance(self.class_definition, ActorClassDefinition):
            raise ProtocolError("class_definition must be an ActorClassDefinition")
        if not isinstance(self.constructor_payload, bytes):
            raise ProtocolError("constructor_payload must be bytes")
        if not isinstance(self.resources, ResourceVector):
            raise ProtocolError("actor resources must be a ResourceVector")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("actor owner_worker_id must be a WorkerID")
        if not isinstance(self.target_node_id, NodeID):
            raise ProtocolError("actor target_node_id must be a NodeID")
        _validate_non_negative_integer(self.route_epoch, "actor route_epoch")
        if self.generation.generation == 0:
            if self.restart is not None:
                raise ProtocolError("initial actor reservation cannot contain restart proof")
            return
        if not isinstance(self.restart, ActorWorkerExitRecord):
            raise ProtocolError("restart actor reservation requires a Worker exit proof")
        if (
            self.restart.actor_id != self.actor_id
            or self.restart.generation.next() != self.generation
        ):
            raise ProtocolError("actor restart proof does not authorize this incarnation")
        if self.route_epoch <= self.restart.route_epoch:
            raise ProtocolError("actor restart route_epoch must advance beyond the old route")
        if self.restart.node_id != self.target_node_id:
            raise ProtocolError("actor Worker-exit proof authorizes only same-Node restart")


class ActorWorkerFailure(str, Enum):
    CAPACITY_UNAVAILABLE = "CAPACITY_UNAVAILABLE"
    CONSTRUCTOR_FAILED = "CONSTRUCTOR_FAILED"
    STARTUP_FAILED = "STARTUP_FAILED"
    INVALID_REQUEST = "INVALID_REQUEST"
    NODE_STOPPING = "NODE_STOPPING"


@dataclass(frozen=True)
class ActorWorkerStartupFailure:
    failure: ActorWorkerFailure
    error: str

    def __post_init__(self) -> None:
        if self.failure not in (
            ActorWorkerFailure.CONSTRUCTOR_FAILED, ActorWorkerFailure.STARTUP_FAILED
        ) or not isinstance(self.failure, ActorWorkerFailure):
            raise ProtocolError("Actor startup failure must identify constructor or startup")
        if not isinstance(self.error, str) or not self.error:
            raise ProtocolError("Actor startup failure requires an error")


@dataclass(frozen=True)
class ReserveActorWorkerReply:
    actor_id: ActorID
    generation: ActorGeneration
    accepted: bool
    node_id: Optional[NodeID] = None
    worker_id: Optional[WorkerID] = None
    worker_address: Optional[Tuple[str, int]] = None
    worker_pid: Optional[int] = None
    error: Optional[str] = None
    failure: Optional[ActorWorkerFailure] = None

    def __post_init__(self) -> None:
        _validate_actor_endpoint_reply(
            actor_id=self.actor_id, generation=self.generation, accepted=self.accepted,
            node_id=self.node_id, worker_id=self.worker_id,
            worker_address=self.worker_address, worker_pid=self.worker_pid,
            error=self.error, operation="reserve actor worker reply",
        )
        if self.accepted:
            if self.failure is not None:
                raise ProtocolError("accepted Actor reservation cannot have a failure")
        elif not isinstance(self.failure, ActorWorkerFailure):
            raise ProtocolError("rejected Actor reservation requires a typed failure")


@dataclass(frozen=True)
class ReportActorWorkerExit:
    record: ActorWorkerExitRecord

    def __post_init__(self) -> None:
        if not isinstance(self.record, ActorWorkerExitRecord):
            raise ProtocolError(
                "actor worker exit report must contain ActorWorkerExitRecord"
            )


@dataclass(frozen=True)
class ReportActorWorkerExitReply:
    record: ActorWorkerExitRecord
    disposition: ActorWorkerExitDisposition
    snapshot: Optional[ActorSnapshot] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.record, ActorWorkerExitRecord):
            raise ProtocolError("actor worker exit reply requires an exit record")
        if not isinstance(self.disposition, ActorWorkerExitDisposition):
            raise ProtocolError("actor worker exit reply disposition is invalid")
        if self.snapshot is not None:
            if (
                not isinstance(self.snapshot, ActorSnapshot)
                or self.snapshot.actor_id != self.record.actor_id
            ):
                raise ProtocolError(
                    "actor worker exit reply snapshot must belong to actor_id"
                )
        acknowledged = self.disposition in (
            ActorWorkerExitDisposition.APPLIED,
            ActorWorkerExitDisposition.ALREADY_APPLIED,
        )
        if acknowledged:
            if (
                self.snapshot is None
                or self.snapshot.last_exit != self.record
                or self.snapshot.route_epoch <= self.record.route_epoch
                or self.error is not None
            ):
                raise ProtocolError(
                    "acknowledged actor exit requires a snapshot that consumed it"
                )
        elif not isinstance(self.error, str) or not self.error:
            raise ProtocolError(
                "unapplied actor exit reply requires an error"
            )


@dataclass(frozen=True)
class GetActorState:
    actor_id: ActorID

    def __post_init__(self) -> None:
        if not isinstance(self.actor_id, ActorID):
            raise ProtocolError("get actor state actor_id must be an ActorID")


@dataclass(frozen=True)
class GetActorStateReply:
    actor_id: ActorID
    found: bool
    snapshot: Optional[ActorSnapshot] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        GetActorState(self.actor_id)
        if not isinstance(self.found, bool):
            raise ProtocolError("get actor state found must be a bool")
        if self.found:
            if (
                not isinstance(self.snapshot, ActorSnapshot)
                or self.snapshot.actor_id != self.actor_id
                or self.error is not None
            ):
                raise ProtocolError(
                    "found actor state requires only its matching snapshot"
                )
        elif self.snapshot is not None or not isinstance(self.error, str) or not self.error:
            raise ProtocolError("missing actor state requires only an error")


@dataclass(frozen=True)
class InstallActorState:
    owner_worker_id: WorkerID
    snapshot: ActorSnapshot

    def __post_init__(self) -> None:
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("actor state owner_worker_id must be a WorkerID")
        if not isinstance(self.snapshot, ActorSnapshot):
            raise ProtocolError("install actor state requires an ActorSnapshot")


@dataclass(frozen=True)
class InstallActorStateReply:
    owner_worker_id: WorkerID
    snapshot: ActorSnapshot
    installed: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        InstallActorState(self.owner_worker_id, self.snapshot)
        _validate_acceptance_error(
            self.installed, self.error, "install actor state reply"
        )


@dataclass(frozen=True)
class ActorWorkerStartup:
    actor_id: ActorID
    generation: ActorGeneration
    worker_id: WorkerID
    worker_pid: int
    worker_address: Tuple[str, int]

    def __post_init__(self) -> None:
        _validate_actor_identity(self.actor_id, self.generation, "actor worker startup")
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("actor startup worker_id must be a WorkerID")
        if isinstance(self.worker_pid, bool) or not isinstance(self.worker_pid, int) or self.worker_pid <= 0:
            raise ProtocolError("actor startup worker_pid must be positive")
        _validate_bound_address(self.worker_address, "actor startup worker_address")


@dataclass(frozen=True)
class ActorCallRequest:
    actor_id: ActorID
    generation: ActorGeneration
    caller_worker_id: WorkerID
    sequence: int
    method_name: str
    task_id: TaskID
    attempt_id: AttemptID
    owner_worker_id: WorkerID
    arguments: bytes
    target_worker_id: Optional[WorkerID] = None
    route_epoch: int = 0

    def __post_init__(self) -> None:
        _validate_actor_identity(self.actor_id, self.generation, "actor call")
        if not isinstance(self.caller_worker_id, WorkerID):
            raise ProtocolError("actor caller_worker_id must be a WorkerID")
        _validate_non_negative_integer(self.sequence, "actor call sequence")
        if not isinstance(self.method_name, str) or not self.method_name:
            raise ProtocolError("actor method_name must be non-empty")
        if not isinstance(self.task_id, TaskID):
            raise ProtocolError("actor call task_id must be a TaskID")
        if not isinstance(self.attempt_id, AttemptID):
            raise ProtocolError("actor call attempt_id must be an AttemptID")
        if self.attempt_id.task_id != self.task_id:
            raise ProtocolError("actor call attempt_id must belong to task_id")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("actor call owner_worker_id must be a WorkerID")
        if not isinstance(self.arguments, bytes):
            raise ProtocolError("actor call arguments must be bytes")
        if self.target_worker_id is not None and not isinstance(
            self.target_worker_id, WorkerID
        ):
            raise ProtocolError("actor call target_worker_id must be a WorkerID")
        _validate_non_negative_integer(self.route_epoch, "actor call route_epoch")


@dataclass(frozen=True)
class ActorCallReply:
    actor_id: ActorID
    generation: ActorGeneration
    caller_worker_id: WorkerID
    sequence: int
    task_reply: TaskReply
    route_epoch: int = 0

    def __post_init__(self) -> None:
        _validate_actor_identity(self.actor_id, self.generation, "actor call reply")
        if not isinstance(self.caller_worker_id, WorkerID):
            raise ProtocolError("actor reply caller_worker_id must be a WorkerID")
        _validate_non_negative_integer(self.sequence, "actor reply sequence")
        if not isinstance(self.task_reply, TaskReply):
            raise ProtocolError("actor call reply must contain a TaskReply")
        _validate_non_negative_integer(self.route_epoch, "actor reply route_epoch")


@dataclass(frozen=True)
class SealObject:
    object_id: ObjectID
    attempt_id: AttemptID
    owner_worker_id: WorkerID
    data: bytes
    checksum: str

    def __post_init__(self) -> None:
        if self.object_id.task_id != self.attempt_id.task_id:
            raise ProtocolError("sealed result must belong to its producing attempt")
        if not isinstance(self.data, bytes):
            raise ProtocolError("object data must be bytes")
        if hashlib.sha256(self.data).hexdigest() != self.checksum:
            raise ProtocolError("object checksum mismatch")

    @classmethod
    def from_data(
        cls,
        object_id: ObjectID,
        attempt_id: AttemptID,
        owner_worker_id: WorkerID,
        data: bytes,
    ) -> "SealObject":
        return cls(
            object_id,
            attempt_id,
            owner_worker_id,
            data,
            hashlib.sha256(data).hexdigest(),
        )


@dataclass(frozen=True)
class SealObjectReply:
    object_id: ObjectID
    sealed: bool
    node_id: NodeID
    size_bytes: int
    checksum: str
    error: Optional[str] = None
    absence_fenced: bool = False

    def __post_init__(self) -> None:
        if type(self.sealed) is not bool or type(self.absence_fenced) is not bool:
            raise ProtocolError("seal flags must be booleans")
        if self.sealed and self.absence_fenced:
            raise ProtocolError("sealed bytes cannot also be fenced absent")
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("seal reply object_id must be an ObjectID")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("seal reply node_id must be a NodeID")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ProtocolError("seal reply size must be a non-negative integer")
        if self.sealed and self.error is not None:
            raise ProtocolError("a successful seal reply cannot contain an error")
        if not self.sealed and not self.error:
            raise ProtocolError("a rejected seal reply must contain an error")


@dataclass(frozen=True)
class GetObject:
    object_id: ObjectID
    requester_node_id: NodeID
    expected_attempt_id: Optional[AttemptID] = None
    expected_owner_worker_id: Optional[WorkerID] = None
    expected_size_bytes: Optional[int] = None
    expected_checksum: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("get object_id must be an ObjectID")
        if not isinstance(self.requester_node_id, NodeID):
            raise ProtocolError("get requester_node_id must be a NodeID")
        expectations = (
            self.expected_attempt_id,
            self.expected_owner_worker_id,
            self.expected_size_bytes,
            self.expected_checksum,
        )
        supplied = tuple(value is not None for value in expectations)
        if any(supplied) and not all(supplied):
            raise ProtocolError(
                "get object expectations must be either all supplied or all omitted"
            )
        if not any(supplied):
            return
        if not isinstance(self.expected_attempt_id, AttemptID):
            raise ProtocolError("expected_attempt_id must be an AttemptID")
        if self.expected_attempt_id.task_id != self.object_id.task_id:
            raise ProtocolError(
                "expected_attempt_id must produce the requested object"
            )
        if not isinstance(self.expected_owner_worker_id, WorkerID):
            raise ProtocolError("expected_owner_worker_id must be a WorkerID")
        _validate_non_negative_integer(
            self.expected_size_bytes, "expected_size_bytes"
        )
        _validate_sha256(self.expected_checksum, "expected_checksum")


@dataclass(frozen=True)
class GetObjectReply:
    object_id: ObjectID
    node_id: NodeID
    found: bool
    sealed: bool
    data: Optional[bytes] = None
    checksum: Optional[str] = None
    error: Optional[str] = None
    producer_attempt_id: Optional[AttemptID] = None
    owner_worker_id: Optional[WorkerID] = None
    size_bytes: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("get reply object_id must be an ObjectID")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("get reply node_id must be a NodeID")
        if type(self.found) is not bool or type(self.sealed) is not bool:
            raise ProtocolError(
                "get reply found and sealed flags must be booleans"
            )
        if self.data is not None and not (self.found and self.sealed):
            raise ProtocolError("only a sealed object may expose bytes")
        if self.found and self.sealed and not isinstance(self.data, bytes):
            raise ProtocolError("a sealed object reply must contain bytes")
        if not self.found and self.sealed:
            raise ProtocolError("a missing object cannot be sealed")
        metadata = (
            self.producer_attempt_id,
            self.owner_worker_id,
            self.size_bytes,
        )
        supplied = tuple(value is not None for value in metadata)
        if any(supplied) and not all(supplied):
            raise ProtocolError(
                "get reply object metadata must be all supplied or all omitted"
            )
        if all(supplied):
            if not (self.found and self.sealed):
                raise ProtocolError(
                    "only a sealed object reply may expose producer metadata"
                )
            if not isinstance(self.producer_attempt_id, AttemptID):
                raise ProtocolError(
                    "get reply producer_attempt_id must be an AttemptID"
                )
            if self.producer_attempt_id.task_id != self.object_id.task_id:
                raise ProtocolError(
                    "get reply producer attempt must belong to object_id"
                )
            if not isinstance(self.owner_worker_id, WorkerID):
                raise ProtocolError(
                    "get reply owner_worker_id must be a WorkerID"
                )
            _validate_non_negative_integer(
                self.size_bytes, "get reply size_bytes"
            )
            if self.data is not None and len(self.data) != self.size_bytes:
                raise ProtocolError(
                    "get reply data length must match size_bytes"
                )
        if self.data is not None and self.checksum is not None:
            _validate_sha256(self.checksum, "get reply checksum")
            if hashlib.sha256(self.data).hexdigest() != self.checksum:
                raise ProtocolError("returned object checksum mismatch")


@dataclass(frozen=True)
class PinObjectForTransfer:
    """Open an idempotent, pinned source-replica transfer session."""

    transfer_id: str
    descriptor: ObjectStoreDescriptor
    requester_node_id: NodeID

    def __post_init__(self) -> None:
        from .output_publication import _opaque, _require_type

        _validate_transfer_id(self.transfer_id, "pin object")
        try:
            _require_type(self.transfer_id, str, "pin transfer_id")
            descriptor = _revalidate_lease_descriptor(self.descriptor)
            requester = _opaque(self.requester_node_id, NodeID, "pin requester_node_id")
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid transfer pin request: {exc}") from exc
        object.__setattr__(self, "descriptor", descriptor)
        object.__setattr__(self, "requester_node_id", requester)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.transfer_id, self.descriptor, self.requester_node_id,
        ))


@dataclass(frozen=True)
class PinObjectForTransferReply:
    transfer_id: str
    descriptor: ObjectStoreDescriptor
    pinned: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        from .output_publication import _require_type

        _validate_transfer_id(self.transfer_id, "pin object reply")
        try:
            _require_type(self.transfer_id, str, "pin reply transfer_id")
            descriptor = _revalidate_lease_descriptor(self.descriptor)
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid transfer pin reply: {exc}") from exc
        _validate_acceptance_error(self.pinned, self.error, "pin object reply")
        object.__setattr__(self, "descriptor", descriptor)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.transfer_id, self.descriptor, self.pinned, self.error,
        ))


@dataclass(frozen=True)
class GetObjectChunk:
    """Read one bounded range from an already-pinned transfer session."""

    transfer_id: str
    object_id: ObjectID
    requester_node_id: NodeID
    offset: int
    size_bytes: int

    def __post_init__(self) -> None:
        _validate_transfer_id(self.transfer_id, "get object chunk")
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("chunk object_id must be an ObjectID")
        if not isinstance(self.requester_node_id, NodeID):
            raise ProtocolError("chunk requester_node_id must be a NodeID")
        _validate_non_negative_integer(self.offset, "chunk offset")
        _validate_non_negative_integer(self.size_bytes, "chunk size_bytes")
        if self.size_bytes == 0:
            raise ProtocolError("chunk size_bytes must be positive")


@dataclass(frozen=True)
class GetObjectChunkReply:
    transfer_id: str
    object_id: ObjectID
    node_id: NodeID
    offset: int
    data: bytes = b""
    ok: bool = True
    error: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_transfer_id(self.transfer_id, "get object chunk reply")
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("chunk reply object_id must be an ObjectID")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("chunk reply node_id must be a NodeID")
        _validate_non_negative_integer(self.offset, "chunk reply offset")
        if not isinstance(self.data, bytes):
            raise ProtocolError("chunk reply data must be bytes")
        _validate_acceptance_error(self.ok, self.error, "get object chunk reply")
        if not self.ok and self.data:
            raise ProtocolError("a failed chunk reply cannot contain object bytes")


@dataclass(frozen=True)
class ReleaseObjectPin:
    """Close one source transfer session and release its physical pin."""

    transfer_id: str
    object_id: ObjectID
    requester_node_id: NodeID

    def __post_init__(self) -> None:
        from .output_publication import _object_id, _opaque, _require_type

        _validate_transfer_id(self.transfer_id, "release object pin")
        try:
            _require_type(self.transfer_id, str, "release transfer_id")
            object_id = _object_id(self.object_id)
            requester = _opaque(self.requester_node_id, NodeID, "release requester_node_id")
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid transfer pin release: {exc}") from exc
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "requester_node_id", requester)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.transfer_id, self.object_id, self.requester_node_id,
        ))


@dataclass(frozen=True)
class ReleaseObjectPinReply:
    transfer_id: str
    object_id: ObjectID
    node_id: NodeID
    accepted: bool
    released: bool
    error: Optional[str] = None

    def __post_init__(self) -> None:
        from .output_publication import _object_id, _opaque, _require_type

        _validate_transfer_id(self.transfer_id, "release object pin reply")
        try:
            _require_type(self.transfer_id, str, "release reply transfer_id")
            object_id = _object_id(self.object_id)
            node_id = _opaque(self.node_id, NodeID, "release reply node_id")
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid transfer pin release reply: {exc}") from exc
        if not isinstance(self.released, bool):
            raise ProtocolError("release pin reply released flag must be a bool")
        _validate_acceptance_error(
            self.accepted, self.error, "release object pin reply"
        )
        if not self.accepted and self.released:
            raise ProtocolError("a rejected pin release cannot release a pin")
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "node_id", node_id)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        return _rebuild_validated_wire_message, (type(self), (
            self.transfer_id, self.object_id, self.node_id, self.accepted, self.released, self.error,
        ))


@dataclass(frozen=True)
class DropObjectReplica:
    """Delete exactly one physical replica, fenced by immutable metadata.

    ``ObjectID`` alone is deliberately insufficient: the same logical return
    slot may be produced by a later reconstruction attempt.  The node deletes
    bytes only when the target node, producer epoch, owner, and checksum all
    still match its sealed metadata.
    """

    object_id: ObjectID
    producer_attempt_id: AttemptID
    owner_worker_id: WorkerID
    node_id: NodeID
    checksum: str

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("drop object_id must be an ObjectID")
        if not isinstance(self.producer_attempt_id, AttemptID):
            raise ProtocolError(
                "drop producer_attempt_id must be an AttemptID"
            )
        if self.producer_attempt_id.task_id != self.object_id.task_id:
            raise ProtocolError(
                "drop producer attempt must belong to object_id"
            )
        if not isinstance(self.owner_worker_id, WorkerID):
            raise ProtocolError("drop owner_worker_id must be a WorkerID")
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("drop node_id must be a NodeID")
        _validate_sha256(self.checksum, "drop checksum")

    @property
    def attempt_id(self) -> AttemptID:
        """Readable alias matching :class:`SealObject` terminology."""

        return self.producer_attempt_id


class DropObjectReplicaStatus(str, Enum):
    """Node-authoritative outcome of one epoch-fenced replica drop.

    Only ``DROPPED`` and ``ALREADY_DROPPED`` discharge a durable drop
    obligation.  In particular, ``PINNED`` is retryable and
    ``STALE_EPOCH`` requires the logical owner to re-check its producer epoch;
    neither is an acknowledgement that this request deleted the replica.
    """

    DROPPED = "DROPPED"
    ALREADY_DROPPED = "ALREADY_DROPPED"
    PINNED = "PINNED"
    STALE_EPOCH = "STALE_EPOCH"
    NODE_DRAINING = "NODE_DRAINING"
    INCONSISTENT = "INCONSISTENT"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class DropObjectReplicaReply:
    """Typed reply that echoes the complete immutable replica identity."""

    object_id: ObjectID
    producer_attempt_id: AttemptID
    owner_worker_id: WorkerID
    node_id: NodeID
    checksum: str
    status: DropObjectReplicaStatus
    error: Optional[str] = None

    def __post_init__(self) -> None:
        # Reuse the request validator so an ACK cannot carry a weaker identity
        # than the destructive operation it purports to acknowledge.
        DropObjectReplica(
            object_id=self.object_id,
            producer_attempt_id=self.producer_attempt_id,
            owner_worker_id=self.owner_worker_id,
            node_id=self.node_id,
            checksum=self.checksum,
        )
        if not isinstance(self.status, DropObjectReplicaStatus):
            raise ProtocolError("drop reply status is invalid")
        if self.status in (
            DropObjectReplicaStatus.DROPPED,
            DropObjectReplicaStatus.ALREADY_DROPPED,
        ):
            if self.error is not None:
                raise ProtocolError(
                    "an acknowledged replica drop cannot contain an error"
                )
        elif not isinstance(self.error, str) or not self.error:
            raise ProtocolError(
                "an unacknowledged replica drop must contain an error"
            )

    @property
    def attempt_id(self) -> AttemptID:
        """Readable alias matching :class:`SealObject` terminology."""

        return self.producer_attempt_id

    @property
    def accepted(self) -> bool:
        """Compatibility view; typed GC code must inspect ``status``."""

        return self.status in (
            DropObjectReplicaStatus.DROPPED,
            DropObjectReplicaStatus.ALREADY_DROPPED,
        )

    @property
    def dropped(self) -> bool:
        """Whether this call performed the physical deletion."""

        return self.status is DropObjectReplicaStatus.DROPPED

    @property
    def deleted(self) -> bool:
        """Alias for callers that use object-store deletion wording."""

        return self.dropped


class _ValidatedOwnerDeathFenceWireMessage:
    """Re-enter validation when an authority-bearing fence crosses pickle."""

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        values = tuple(
            getattr(self, definition.name)
            for definition in dataclass_fields(self)
        )
        return type(self), values


class OwnerDeathReplicaStatus(str, Enum):
    """State of one replica while an owner fence effect is processed.

    Publication-exact fences are observational.  Owner-wide sweeps may turn a
    matching ``PRESENT`` replica into ``ABSENT`` by deleting it.  ``PINNED``
    retains retryable work, and ``CONFLICT`` preserves every inconsistent state
    for explicit repair rather than guessing ownership.
    """

    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    CONFLICT = "CONFLICT"
    PINNED = "PINNED"


class OwnerDeathFenceScope(str, Enum):
    """Why a Node is installing an owner-death fence.

    Publication cleanup already owns an exact immutable replica manifest and
    must only observe that manifest.  The independent owner-wide fence has a
    different job: after permanently closing late writes, it sweeps every
    remaining ordinary stored replica discoverable from Node-local sealed
    metadata.  Keeping this distinction on the wire prevents an empty
    publication manifest from accidentally becoming an unbounded sweep.
    """

    PUBLICATION_EXACT = "PUBLICATION_EXACT"
    OWNER_WIDE_SWEEP = "OWNER_WIDE_SWEEP"


@dataclass(frozen=True)
class OwnerDeathReplicaObservation(_ValidatedOwnerDeathFenceWireMessage):
    """Typed Node-local result for one physical replica identity."""

    descriptor: ObjectStoreDescriptor
    status: OwnerDeathReplicaStatus
    pin_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.descriptor, ObjectStoreDescriptor):
            raise ProtocolError(
                "owner-death replica observation requires an object descriptor"
            )
        # Re-enter validation for authority-bearing nested wire data.
        descriptor = ObjectStoreDescriptor(
            self.descriptor.object_id,
            self.descriptor.owner_worker_id,
            self.descriptor.producer_attempt_id,
            self.descriptor.node_id,
            self.descriptor.size_bytes,
            self.descriptor.checksum,
        )
        object.__setattr__(self, "descriptor", descriptor)
        if not isinstance(self.status, OwnerDeathReplicaStatus):
            raise ProtocolError(
                "owner-death replica observation status is invalid"
            )
        _validate_non_negative_integer(
            self.pin_count, "owner-death replica pin_count"
        )
        if self.status is OwnerDeathReplicaStatus.PINNED:
            if self.pin_count == 0:
                raise ProtocolError(
                    "a PINNED owner-death replica requires a positive pin_count"
                )
        elif self.pin_count != 0:
            raise ProtocolError(
                "only a PINNED owner-death replica may report pins"
            )

    @property
    def drop_request(self) -> DropObjectReplica:
        """Project the exact identity into the existing deletion protocol."""

        descriptor = self.descriptor
        return DropObjectReplica(
            object_id=descriptor.object_id,
            producer_attempt_id=descriptor.producer_attempt_id,
            owner_worker_id=descriptor.owner_worker_id,
            node_id=descriptor.node_id,
            checksum=descriptor.checksum,
        )


@dataclass(frozen=True)
class InstallOwnerDeathFence(_ValidatedOwnerDeathFenceWireMessage):
    """Install one permanent Worker-owner fence and process its scope.

    ``request_id`` and the complete request are immutable idempotency identity.
    The first request permanently fences the Worker owner.  Further requests
    for other publications may scan other replica sets only when they carry the
    same death proof.  An explicit owner-wide scope instead discovers and
    deletes ordinary replicas from Node-local sealed metadata.  One request ID,
    however, can bind only one exact request.
    """

    request_id: str
    owner_death: WorkerDeathRecord
    node_id: NodeID
    expected_replicas: Tuple[ObjectStoreDescriptor, ...] = ()
    scope: OwnerDeathFenceScope = OwnerDeathFenceScope.PUBLICATION_EXACT

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise ProtocolError(
                "owner-death fence request_id must be a non-empty string"
            )
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError(
                "owner-death fence node_id must be a NodeID"
            )
        try:
            death = _revalidate_worker_death(self.owner_death)
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(
                "owner-death fence requires a valid WorkerDeathRecord: {}"
                .format(exc)
            ) from exc
        if death.reason not in (
            WorkerDeathReason.PROCESS_EXIT, WorkerDeathReason.NODE_EXIT,
        ):
            raise ProtocolError(
                "owner-death fence requires PROCESS_EXIT or NODE_EXIT"
            )
        object.__setattr__(self, "owner_death", death)
        if not isinstance(self.scope, OwnerDeathFenceScope):
            raise ProtocolError("owner-death fence scope is invalid")
        try:
            raw_replicas = tuple(self.expected_replicas)
        except TypeError as exc:
            raise ProtocolError(
                "owner-death expected_replicas must be iterable"
            ) from exc
        replicas: list[ObjectStoreDescriptor] = []
        for value in raw_replicas:
            if not isinstance(value, ObjectStoreDescriptor):
                raise ProtocolError(
                    "owner-death expected_replicas must contain object descriptors"
                )
            descriptor = ObjectStoreDescriptor(
                value.object_id, value.owner_worker_id,
                value.producer_attempt_id, value.node_id, value.size_bytes,
                value.checksum,
            )
            if descriptor.owner_worker_id != death.worker_id:
                raise ProtocolError(
                    "owner-death expected replica belongs to another owner"
                )
            if descriptor.node_id != self.node_id:
                raise ProtocolError(
                    "owner-death expected replica belongs to another node"
                )
            replicas.append(descriptor)
        object_ids = tuple(item.object_id for item in replicas)
        if len(object_ids) != len(set(object_ids)):
            raise ProtocolError(
                "owner-death expected replica ObjectIDs must be unique"
            )
        if (
            self.scope is OwnerDeathFenceScope.OWNER_WIDE_SWEEP
            and replicas
        ):
            raise ProtocolError(
                "owner-wide sweep discovers replicas at the Node and cannot "
                "carry an expected replica manifest"
            )
        object.__setattr__(self, "expected_replicas", tuple(replicas))

    @property
    def owner_worker_id(self) -> WorkerID:
        return self.owner_death.worker_id


class OwnerDeathFenceDisposition(str, Enum):
    FENCED = "FENCED"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class InstallOwnerDeathFenceReply(_ValidatedOwnerDeathFenceWireMessage):
    """Exact cached fence result, including its atomic replica witness."""

    request: InstallOwnerDeathFence
    disposition: OwnerDeathFenceDisposition
    observations: Tuple[OwnerDeathReplicaObservation, ...] = ()
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.request, InstallOwnerDeathFence):
            raise ProtocolError(
                "owner-death fence reply must echo its exact request"
            )
        request = InstallOwnerDeathFence(
            self.request.request_id, self.request.owner_death,
            self.request.node_id, self.request.expected_replicas,
            self.request.scope,
        )
        object.__setattr__(self, "request", request)
        if not isinstance(self.disposition, OwnerDeathFenceDisposition):
            raise ProtocolError(
                "owner-death fence reply disposition is invalid"
            )
        try:
            observations = tuple(self.observations)
        except TypeError as exc:
            raise ProtocolError(
                "owner-death fence observations must be iterable"
            ) from exc
        if any(
            not isinstance(item, OwnerDeathReplicaObservation)
            for item in observations
        ):
            raise ProtocolError(
                "owner-death fence reply requires typed replica observations"
            )
        observations = tuple(
            OwnerDeathReplicaObservation(
                item.descriptor, item.status, item.pin_count
            )
            for item in observations
        )
        object.__setattr__(self, "observations", observations)
        if self.disposition is OwnerDeathFenceDisposition.FENCED:
            if self.error is not None:
                raise ProtocolError(
                    "an installed owner-death fence cannot contain an error"
                )
            observed_descriptors = tuple(
                item.descriptor for item in observations
            )
            if request.scope is OwnerDeathFenceScope.PUBLICATION_EXACT:
                if observed_descriptors != request.expected_replicas:
                    raise ProtocolError(
                        "owner-death observations must exactly preserve request order"
                    )
            else:
                if (
                    len(observed_descriptors)
                    != len(set(item.object_id for item in observed_descriptors))
                    or any(
                        descriptor.owner_worker_id
                        != request.owner_worker_id
                        or descriptor.node_id != request.node_id
                        for descriptor in observed_descriptors
                    )
                    or observed_descriptors != tuple(
                        sorted(
                            observed_descriptors,
                            key=lambda descriptor: descriptor.object_id,
                        )
                    )
                ):
                    raise ProtocolError(
                        "owner-wide sweep observations must be unique, ordered, "
                        "and belong to the fenced owner and Node"
                    )
            return
        if observations:
            raise ProtocolError(
                "a conflicting owner-death fence cannot report observations"
            )
        if not isinstance(self.error, str) or not self.error:
            raise ProtocolError(
                "a conflicting owner-death fence requires an error"
            )

    @property
    def accepted(self) -> bool:
        return self.disposition is OwnerDeathFenceDisposition.FENCED

    @property
    def complete(self) -> bool:
        """Whether this acknowledgement is a terminal fence effect.

        Publication-exact replies freeze observations for a later publication
        saga, so installation itself is terminal even when a replica is pinned.
        Owner-wide sweeps are terminal only after every frozen candidate is
        proven absent; PINNED/CONFLICT observations remain replayable work.
        """

        if not self.accepted:
            return False
        if self.request.scope is OwnerDeathFenceScope.PUBLICATION_EXACT:
            return True
        return all(
            observation.status is OwnerDeathReplicaStatus.ABSENT
            for observation in self.observations
        )

    @property
    def retryable(self) -> bool:
        """Whether exact replay may converge without external repair."""

        return bool(
            self.accepted
            and not self.complete
            and self.request.scope is OwnerDeathFenceScope.OWNER_WIDE_SWEEP
            and self.observations
            and all(
                observation.status in (
                    OwnerDeathReplicaStatus.ABSENT,
                    OwnerDeathReplicaStatus.PINNED,
                )
                for observation in self.observations
            )
            and any(
                observation.status is OwnerDeathReplicaStatus.PINNED
                for observation in self.observations
            )
        )


@dataclass(frozen=True)
class CancelWorkerLease:
    """Fence an unresolved lease without knowing its Worker allocation.

    A submitter may lose the reply to ``RequestWorkerLease`` and therefore not
    know the granted Worker or allocation token.  The original logical and
    requester identities are sufficient for the NodeManager to serialize this
    cancellation against the request and prevent a late grant.
    """

    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    requester_node_id: NodeID
    requester_worker_id: WorkerID
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None
    # Bind the complete request even when cancellation reaches the Node first.
    # None preserves the narrow identity-only surface, not proof of no replicas.
    lease_request: Optional[RequestWorkerLease] = None

    def __post_init__(self) -> None:
        from .output_publication import _attempt, _opaque

        try:
            identity = (
                _opaque(self.lease_id, LeaseID, "cancel lease_id"),
                _opaque(self.task_id, TaskID, "cancel task_id"),
                _attempt(self.attempt_id),
                _opaque(self.requester_node_id, NodeID, "cancel requester_node_id"),
                _opaque(self.requester_worker_id, WorkerID, "cancel requester_worker_id"),
                _revalidate_lease_scheduling_key(self.scheduling_key),
            )
            if identity[2].task_id != identity[1]:
                raise ProtocolError("cancel attempt_id must belong to task_id")
            request = (None if self.lease_request is None
                       else revalidate_worker_lease_request(self.lease_request))
            if request is not None:
                if identity != (
                    request.lease_id, request.task_id, request.attempt_id,
                    request.requester_node_id, request.requester_worker_id, request.scheduling_key,
                ):
                    raise ProtocolError("cancellation identity does not match its full lease request")
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise ProtocolError(f"invalid lease cancellation: {exc}") from exc
        for name, component in zip(
            ("lease_id", "task_id", "attempt_id", "requester_node_id",
             "requester_worker_id", "scheduling_key"), identity,
        ):
            object.__setattr__(self, name, component)
        object.__setattr__(self, "lease_request", request)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        values = tuple(getattr(self, definition.name) for definition in dataclass_fields(self))
        return _rebuild_validated_wire_message, (type(self), values)


@dataclass(frozen=True)
class CancelWorkerLeaseReply:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    requester_node_id: NodeID
    requester_worker_id: WorkerID
    state: LeaseExecutionState
    accepted: bool
    cancelled: bool
    released: bool
    error: Optional[str] = None
    scheduling_key: Optional[PlacementGroupSchedulingKey] = None
    # Historical committed grant / localized-replica inventory, not permission
    # to execute. None does not prove that partial localization made no replica.
    retired_grant: Optional[GrantWorkerLease] = None
    dependency_inventory: Optional[LeaseDependencyInventory] = None

    def __post_init__(self) -> None:
        cancellation = CancelWorkerLease(
            self.lease_id, self.task_id, self.attempt_id,
            self.requester_node_id, self.requester_worker_id,
            self.scheduling_key,
        )
        for name in ("lease_id", "task_id", "attempt_id", "requester_node_id",
                     "requester_worker_id", "scheduling_key"):
            object.__setattr__(self, name, getattr(cancellation, name))
        if not isinstance(self.state, LeaseExecutionState):
            raise ProtocolError("cancel reply state must be a LeaseExecutionState")
        if not isinstance(self.cancelled, bool) or not isinstance(self.released, bool):
            raise ProtocolError("cancel reply flags must be bools")
        _validate_acceptance_error(self.accepted, self.error, "cancel reply")
        if self.cancelled and (
            not self.accepted or self.state is not LeaseExecutionState.ABANDONED
        ):
            raise ProtocolError("a cancelled lease must be accepted and ABANDONED")
        if self.released and not self.cancelled:
            raise ProtocolError("only a cancelled lease may release resources")
        if self.retired_grant is not None:
            if not (
                self.accepted and self.cancelled
                and self.state is LeaseExecutionState.ABANDONED
                or not self.accepted and self.state is LeaseExecutionState.WORKER_LOST
            ):
                raise ProtocolError(
                    "retired grant requires cancelled ABANDONED or rejected WORKER_LOST"
                )
            grant = revalidate_worker_lease_grant(self.retired_grant)
            from .output_publication import _attempt, _opaque

            try:
                identity = (
                    _opaque(self.lease_id, LeaseID, "cancel lease_id"),
                    _opaque(self.task_id, TaskID, "cancel task_id"),
                    _attempt(self.attempt_id),
                    _opaque(self.requester_node_id, NodeID, "cancel requester_node_id"),
                    _opaque(self.requester_worker_id, WorkerID, "cancel requester_worker_id"),
                    _revalidate_lease_scheduling_key(self.scheduling_key),
                )
            except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
                raise ProtocolError(f"invalid retired-grant cancellation identity: {exc}") from exc
            if (grant.lease_id, grant.task_id, grant.attempt_id, grant.scheduling_key) != (
                identity[0], identity[1], identity[2], identity[5],
            ):
                raise ProtocolError("retired grant identity does not match cancellation reply")
            for name, component in zip(
                ("lease_id", "task_id", "attempt_id", "requester_node_id",
                 "requester_worker_id", "scheduling_key"),
                identity,
            ):
                object.__setattr__(self, name, component)
            object.__setattr__(self, "retired_grant", grant)
        if self.dependency_inventory is not None:
            if not (
                self.accepted and self.cancelled and self.state is LeaseExecutionState.ABANDONED
                or not self.accepted and self.state is LeaseExecutionState.WORKER_LOST
            ):
                raise ProtocolError("dependency inventory requires cancelled ABANDONED or rejected WORKER_LOST")
            inventory = revalidate_lease_dependency_inventory(self.dependency_inventory)
            cancellation = CancelWorkerLease(
                self.lease_id, self.task_id, self.attempt_id, self.requester_node_id,
                self.requester_worker_id, self.scheduling_key, inventory.lease_request,
            )
            grant = self.retired_grant
            if grant is not None:
                request = inventory.lease_request
                complete = tuple(ObjectStoreDescriptor(
                    item.object_id, item.owner_worker_id, item.producer_attempt_id,
                    inventory.node_id, item.size_bytes, item.checksum,
                ) for item in request.dependencies)
                if (grant.node_id != inventory.node_id
                        or request.target_node_id not in (None, grant.node_id)
                        or grant.lease_id != request.lease_id
                        or grant.task_id != request.task_id
                        or grant.attempt_id != request.attempt_id
                        or grant.scheduling_key != request.scheduling_key
                        or grant.dependencies != complete
                        or inventory.descriptors != complete):
                    raise ProtocolError("retired grant and dependency inventory disagree")
            for name in ("lease_id", "task_id", "attempt_id", "requester_node_id",
                         "requester_worker_id", "scheduling_key"):
                object.__setattr__(self, name, getattr(cancellation, name))
            object.__setattr__(self, "dependency_inventory", inventory)

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        values = tuple(
            getattr(self, definition.name)
            for definition in dataclass_fields(self)
        )
        return _rebuild_validated_wire_message, (type(self), values)


@dataclass(frozen=True)
class ReleaseWorkerLease:
    """Abandon a GRANTED lease that never entered RUNNING.

    A terminal replay may be acknowledged as a no-op.  A RUNNING lease must
    instead use :class:`CompleteWorkerLease`, which couples the task terminal
    status with exactly-once resource release.
    """

    lease_id: LeaseID
    worker_id: WorkerID
    allocation_token: AllocationToken

    def __post_init__(self) -> None:
        if not isinstance(self.lease_id, LeaseID):
            raise ProtocolError("release lease_id must be a LeaseID")
        if not isinstance(self.worker_id, WorkerID):
            raise ProtocolError("release worker_id must be a WorkerID")
        if not isinstance(self.allocation_token, AllocationToken):
            raise ProtocolError("release allocation_token must be an AllocationToken")


@dataclass(frozen=True)
class ReleaseObject:
    object_id: ObjectID
    borrower_worker_id: WorkerID
    reference_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ProtocolError("release object_id must be an ObjectID")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise ProtocolError("borrower_worker_id must be a WorkerID")
        if not isinstance(self.reference_token, str) or not self.reference_token:
            raise ProtocolError("reference token must be non-empty")


@dataclass(frozen=True)
class ReleaseReply:
    released: bool
    detail: str = ""


@dataclass(frozen=True)
class Ack:
    message: str = ""


@dataclass(frozen=True)
class Shutdown:
    request_id: str
    reason: str = "requested"
    graceful: bool = True

    def __post_init__(self) -> None:
        if not self.request_id:
            raise ProtocolError("shutdown request_id must be non-empty")

    @classmethod
    def create(cls, reason: str = "requested", graceful: bool = True) -> "Shutdown":
        return cls(uuid.uuid4().hex, reason, graceful)


@dataclass(frozen=True)
class BeginDrain:
    """Fence new cluster work without stopping cleanup endpoints."""

    request_id: str
    reason: str = "requested"

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise ProtocolError("drain request_id must be non-empty")
        if not isinstance(self.reason, str):
            raise ProtocolError("drain reason must be a string")

    @classmethod
    def create(cls, reason: str = "requested") -> "BeginDrain":
        return cls(uuid.uuid4().hex, reason)


@dataclass(frozen=True)
class FinalizeShutdown:
    """Cross the cluster barrier and stop one prepared component."""

    request_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise ProtocolError("finalize request_id must be non-empty")


@dataclass(frozen=True)
class DrainStatus:
    """Idempotent drain progress for a Worker or a Node and its pool."""

    request_id: str
    component: str
    drain_started: bool
    clean: bool
    resources_clean: bool = True
    detail: str = ""
    child_pids: Tuple[int, ...] = ()
    child_cleans: Tuple[bool, ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.request_id, str)
            or not self.request_id
            or not isinstance(self.component, str)
            or not self.component
        ):
            raise ProtocolError("drain status identity must be non-empty")
        for name in ("drain_started", "clean", "resources_clean"):
            if not isinstance(getattr(self, name), bool):
                raise ProtocolError("drain status {} must be a bool".format(name))
        pids = tuple(self.child_pids)
        cleans = tuple(self.child_cleans)
        if len(pids) != len(cleans):
            raise ProtocolError("drain status child tuples must align")
        if any(
            isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0
            for pid in pids
        ):
            raise ProtocolError("drain status child PIDs must be positive integers")
        if len(pids) != len(set(pids)):
            raise ProtocolError("drain status child PIDs must be unique")
        if any(not isinstance(value, bool) for value in cleans):
            raise ProtocolError("drain status child clean flags must be bools")
        if self.clean and (
            not self.drain_started
            or not self.resources_clean
            or any(not value for value in cleans)
        ):
            raise ProtocolError(
                "clean drain status requires started, resource-clean children"
            )
        object.__setattr__(self, "child_pids", pids)
        object.__setattr__(self, "child_cleans", cleans)

    @property
    def children_clean(self) -> bool:
        return all(self.child_cleans)


@dataclass(frozen=True)
class ShutdownAck:
    request_id: str
    component: str
    clean: bool
    detail: str = ""
    child_pid: Optional[int] = None
    child_exitcode: Optional[int] = None
    child_clean: bool = True
    forced: bool = False
    resources_clean: bool = True
    child_pids: Tuple[int, ...] = ()
    child_exitcodes: Tuple[Optional[int], ...] = ()
    child_cleans: Tuple[bool, ...] = ()
    child_forced: Tuple[bool, ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.request_id, str)
            or not self.request_id
            or not isinstance(self.component, str)
            or not self.component
        ):
            raise ProtocolError("shutdown ACK identity must be non-empty")
        for name in ("clean", "forced", "resources_clean"):
            if not isinstance(getattr(self, name), bool):
                raise ProtocolError("shutdown ACK {} must be a bool".format(name))
        pids, exitcodes, cleans = _normalize_child_status(
            operation="shutdown ACK",
            child_pid=self.child_pid,
            child_exitcode=self.child_exitcode,
            child_clean=self.child_clean,
            child_pids=self.child_pids,
            child_exitcodes=self.child_exitcodes,
            child_cleans=self.child_cleans,
        )
        forced = tuple(self.child_forced)
        if not forced and pids and not self.child_pids and self.child_pid is not None:
            forced = (self.forced,)
        if len(forced) != len(pids):
            raise ProtocolError("shutdown ACK child tuples must align")
        if any(not isinstance(value, bool) for value in forced):
            raise ProtocolError("shutdown ACK child flags must be bools")
        if pids:
            object.__setattr__(self, "child_pid", pids[0])
            object.__setattr__(self, "child_exitcode", exitcodes[0])
            object.__setattr__(self, "child_clean", cleans[0])
        # ``forced`` is the component-wide summary used by the Driver, while
        # ``child_forced`` retains exact slot diagnostics.  A forced child must
        # therefore make the aggregate true even if an older sender supplied
        # only the tuple form.
        object.__setattr__(self, "forced", self.forced or any(forced))
        object.__setattr__(self, "child_pids", pids)
        object.__setattr__(self, "child_exitcodes", exitcodes)
        object.__setattr__(self, "child_cleans", cleans)
        object.__setattr__(self, "child_forced", forced)

    @property
    def children_clean(self) -> bool:
        """Whether every reported child stopped without forced termination."""

        return all(self.child_cleans) and not any(self.child_forced)


@dataclass(frozen=True)
class ShutdownStatusRequest:
    """Inspect a prepared shutdown and optionally let the node exit."""

    request_id: str
    finalize: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise ProtocolError("shutdown status request_id must be non-empty")
        if not isinstance(self.finalize, bool):
            raise ProtocolError("shutdown status finalize must be a bool")


@dataclass(frozen=True)
class ShutdownStatus:
    request_id: str
    component: str
    shutdown_requested: bool
    child_pid: Optional[int]
    child_exitcode: Optional[int]
    child_clean: bool
    finalized: bool
    resources_clean: bool = True
    child_pids: Tuple[int, ...] = ()
    child_exitcodes: Tuple[Optional[int], ...] = ()
    child_cleans: Tuple[bool, ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.request_id, str)
            or not self.request_id
            or not isinstance(self.component, str)
            or not self.component
        ):
            raise ProtocolError("shutdown status identity must be non-empty")
        for name in ("shutdown_requested", "finalized", "resources_clean"):
            if not isinstance(getattr(self, name), bool):
                raise ProtocolError("shutdown status {} must be a bool".format(name))
        pids, exitcodes, cleans = _normalize_child_status(
            operation="shutdown status",
            child_pid=self.child_pid,
            child_exitcode=self.child_exitcode,
            child_clean=self.child_clean,
            child_pids=self.child_pids,
            child_exitcodes=self.child_exitcodes,
            child_cleans=self.child_cleans,
        )
        if pids:
            object.__setattr__(self, "child_pid", pids[0])
            object.__setattr__(self, "child_exitcode", exitcodes[0])
            object.__setattr__(self, "child_clean", cleans[0])
        object.__setattr__(self, "child_pids", pids)
        object.__setattr__(self, "child_exitcodes", exitcodes)
        object.__setattr__(self, "child_cleans", cleans)

    @property
    def children_clean(self) -> bool:
        return all(self.child_cleans)


@dataclass(frozen=True)
class GCSStartup:
    """Identity published after the spawned GCS is accepting RPCs."""

    gcs_pid: int
    gcs_address: Tuple[str, int]

    def __post_init__(self) -> None:
        if isinstance(self.gcs_pid, bool) or not isinstance(self.gcs_pid, int):
            raise ProtocolError("GCS process ID must be an integer")
        if self.gcs_pid <= 0:
            raise ProtocolError("GCS process ID must be positive")
        _validate_bound_address(self.gcs_address, "GCS address")


@dataclass(frozen=True)
class NodeStartup:
    """The exact processes and sockets created by one NodeManager.

    Worker tuples are authoritative and preserve deterministic slot order.
    Singular properties retain the original one-Worker teaching API and refer
    only to the first ordinary Worker; Actor Workers are never included.
    """

    node_id: NodeID
    node_pid: int
    node_address: Tuple[str, int]
    worker_ids: Tuple[WorkerID, ...]
    worker_pids: Tuple[int, ...]
    worker_addresses: Tuple[Tuple[str, int], ...]

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ProtocolError("startup node_id must be a NodeID")
        if isinstance(self.node_pid, bool) or not isinstance(self.node_pid, int):
            raise ProtocolError("startup node PID must be an integer")
        if self.node_pid <= 0:
            raise ProtocolError("startup node PID must be positive")
        _validate_bound_address(self.node_address, "startup node_address")
        worker_ids = tuple(self.worker_ids)
        worker_pids = tuple(self.worker_pids)
        worker_addresses = tuple(self.worker_addresses)
        if not 1 <= len(worker_ids) <= 2:
            raise ProtocolError("startup must contain one or two ordinary Workers")
        if not (len(worker_ids) == len(worker_pids) == len(worker_addresses)):
            raise ProtocolError("startup Worker identity tuples must align")
        if len(set(worker_ids)) != len(worker_ids):
            raise ProtocolError("startup WorkerIDs must be unique")
        for index, (worker_id, worker_pid, worker_address) in enumerate(
            zip(worker_ids, worker_pids, worker_addresses)
        ):
            if not isinstance(worker_id, WorkerID):
                raise ProtocolError("startup worker_id must be a WorkerID")
            if (
                isinstance(worker_pid, bool)
                or not isinstance(worker_pid, int)
                or worker_pid <= 0
            ):
                raise ProtocolError("startup worker_pid must be positive")
            _validate_bound_address(
                worker_address, "startup worker_addresses[{}]".format(index)
            )
        object.__setattr__(self, "worker_ids", worker_ids)
        object.__setattr__(self, "worker_pids", worker_pids)
        object.__setattr__(self, "worker_addresses", worker_addresses)

    @property
    def worker_id(self) -> WorkerID:
        return self.worker_ids[0]

    @property
    def worker_pid(self) -> int:
        return self.worker_pids[0]

    @property
    def worker_address(self) -> Tuple[str, int]:
        return self.worker_addresses[0]


@dataclass(frozen=True)
class TraceRecord:
    """One event in a causal, per-process ordered trace."""

    event_id: str
    timestamp_ns: int
    process_id: str
    process_sequence: int
    component: str
    event: str
    entity_kind: str
    entity_id: str
    cause_event_id: Optional[str] = None
    fields: Tuple[Tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if (
            not self.event_id or not self.process_id or not self.component
            or not self.event or not self.entity_kind or not self.entity_id
        ):
            raise ProtocolError("trace identity fields must be non-empty")
        if self.process_sequence < 0:
            raise ProtocolError("process_sequence must be non-negative")
        object.__setattr__(self, "fields", tuple(sorted(tuple(self.fields))))

    @classmethod
    def create(
        cls,
        *,
        process_id: str,
        process_sequence: int,
        component: str,
        event: str,
        entity_kind: str,
        entity_id: str,
        cause_event_id: Optional[str] = None,
        fields: Tuple[Tuple[str, str], ...] = (),
    ) -> "TraceRecord":
        return cls(
            uuid.uuid4().hex,
            time.monotonic_ns(),
            process_id,
            process_sequence,
            component,
            event,
            entity_kind,
            entity_id,
            cause_event_id,
            tuple(fields),
        )


@dataclass(frozen=True)
class TraceBatch:
    """A best-effort side-channel batch from one trace source."""

    source_id: str
    records: Tuple[TraceRecord, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ProtocolError("trace source_id must be a non-empty string")
        object.__setattr__(self, "records", tuple(self.records))
        if not self.records:
            raise ProtocolError("trace batch must contain at least one record")
        if any(not isinstance(record, TraceRecord) for record in self.records):
            raise ProtocolError("trace batch records must be TraceRecord values")


@dataclass(frozen=True)
class TraceBatchAck:
    source_id: str
    accepted: int
    deduplicated: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ProtocolError("trace ack source_id must be a non-empty string")
        _validate_non_negative_integer(self.accepted, "trace accepted count")
        _validate_non_negative_integer(
            self.deduplicated, "trace deduplicated count"
        )


def _rebuild_validated_wire_message(
    message_type: type, values: tuple[object, ...]
) -> object:
    """Re-enter dataclass construction when a message crosses pickle.

    Normal dataclass unpickling writes ``__dict__`` directly and therefore
    skips ``__post_init__``.  These recovery messages are authority-bearing,
    so a corrupted in-memory value must not become valid merely by taking a
    transport round trip.
    """

    # dev runtimes communicate with the same checkout. After retiring an old
    # wire field, accepting a shorter/longer positional record would silently
    # reinterpret its values as another authority. Reject a different schema
    # instead of filling defaults or reviving a legacy publication envelope.
    if type(values) is not tuple or len(values) != len(dataclass_fields(message_type)):
        raise ProtocolError("wire field count does not match the current message schema")
    return message_type(*values)


class _ValidatedPublicationWireMessage:
    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        values = tuple(
            getattr(self, definition.name)
            for definition in dataclass_fields(self)
        )
        return _rebuild_validated_wire_message, (type(self), values)


class StoredPublicationRPCErrorKind(str, Enum):
    """Stable error categories; exception class names never become API."""

    CONFLICT = "CONFLICT"
    INVALID_STATE = "INVALID_STATE"
    CYCLE = "CYCLE"
    INVALID_REQUEST = "INVALID_REQUEST"
    UNAVAILABLE = "UNAVAILABLE"
    INTERNAL = "INTERNAL"


class StoredPublicationQueryDisposition(str, Enum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    REJECTED = "REJECTED"


def _validate_publication_wire_error(
    *,
    succeeded: bool,
    error_kind: Optional[StoredPublicationRPCErrorKind],
    error: Optional[str],
    operation: str,
) -> None:
    if succeeded:
        if error_kind is not None or error is not None:
            raise ProtocolError(
                "successful {} cannot contain an error".format(operation)
            )
        return
    if not isinstance(error_kind, StoredPublicationRPCErrorKind):
        raise ProtocolError(
            "failed {} requires StoredPublicationRPCErrorKind".format(
                operation
            )
        )
    if not isinstance(error, str) or not error:
        raise ProtocolError(
            "failed {} requires a non-empty error".format(operation)
        )


def _revalidate_opaque_id(value: object, expected: type, label: str) -> object:
    if not isinstance(value, expected):
        raise TypeError("{} must be a {}".format(label, expected.__name__))
    return expected(value.value)


def _revalidate_worker_death(value: object) -> WorkerDeathRecord:
    if not isinstance(value, WorkerDeathRecord):
        raise TypeError("owner_death must be a WorkerDeathRecord")
    incarnation = value.incarnation
    if not isinstance(incarnation, WorkerIncarnation):
        raise TypeError("worker death incarnation is invalid")
    incarnation = WorkerIncarnation(
        _revalidate_opaque_id(
            incarnation.node_id, NodeID, "worker death node_id"
        ),
        incarnation.node_pid, incarnation.node_registration_epoch,
        _revalidate_opaque_id(
            incarnation.worker_id, WorkerID, "dead worker_id"
        ),
        incarnation.worker_pid,
    )
    return WorkerDeathRecord(
        value.detection_id, incarnation, value.death_epoch,
        value.exit_code, value.reason,
    )


@dataclass(frozen=True)
class PrepareStoredContainedPin(_ValidatedPublicationWireMessage):
    transfer: "PreparedContainedTransfer"
    authority_worker_id: WorkerID

    def __post_init__(self) -> None:
        from .publication_sources import (
            PreparedContainedTransfer,
            prepared_contained_transfer_fingerprint,
        )

        if not isinstance(self.transfer, PreparedContainedTransfer):
            raise ProtocolError(
                "stored pin prepare transfer has an invalid type"
            )
        try:
            prepared_contained_transfer_fingerprint(self.transfer)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProtocolError(
                "stored pin prepare transfer is invalid: {}".format(exc)
            ) from exc
        if not isinstance(self.authority_worker_id, WorkerID):
            raise ProtocolError(
                "stored pin prepare authority must be a WorkerID"
            )
        if self.transfer.contained_owner_worker_id != self.authority_worker_id:
            raise ProtocolError(
                "stored pin prepare authority must be the child owner"
            )


@dataclass(frozen=True)
class PromoteStoredContainedPin(_ValidatedPublicationWireMessage):
    transfer: "PreparedContainedTransfer"
    authority_worker_id: WorkerID

    def __post_init__(self) -> None:
        from .publication_sources import (
            PreparedContainedTransfer,
            prepared_contained_transfer_fingerprint,
        )

        if not isinstance(self.transfer, PreparedContainedTransfer):
            raise ProtocolError(
                "stored pin promotion transfer has an invalid type"
            )
        try:
            prepared_contained_transfer_fingerprint(self.transfer)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProtocolError(
                "stored pin promotion transfer is invalid: {}".format(exc)
            ) from exc
        if not isinstance(self.authority_worker_id, WorkerID):
            raise ProtocolError(
                "stored pin promotion authority must be a WorkerID"
            )
        if self.transfer.contained_owner_worker_id != self.authority_worker_id:
            raise ProtocolError(
                "stored pin promotion authority must be the child owner"
            )


StoredContainedPinRequest = Union[
    PrepareStoredContainedPin, PromoteStoredContainedPin
]


@dataclass(frozen=True)
class StoredContainedPinReply(_ValidatedPublicationWireMessage):
    request: StoredContainedPinRequest
    disposition: Optional["StoredContainedReferenceDisposition"] = None
    error_kind: Optional[StoredPublicationRPCErrorKind] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        from .ownership import StoredContainedReferenceDisposition

        if not isinstance(
            self.request, (PrepareStoredContainedPin, PromoteStoredContainedPin)
        ):
            raise ProtocolError(
                "stored contained-pin reply must echo its exact request"
            )
        succeeded = self.disposition is not None
        _validate_publication_wire_error(
            succeeded=succeeded, error_kind=self.error_kind, error=self.error,
            operation="stored contained-pin transition",
        )
        if not succeeded:
            return
        if not isinstance(
            self.disposition, StoredContainedReferenceDisposition
        ):
            raise ProtocolError(
                "stored contained-pin disposition has an invalid type"
            )
        allowed = (
            {StoredContainedReferenceDisposition.PREPARED,
             StoredContainedReferenceDisposition.ALREADY_PREPARED}
            if isinstance(self.request, PrepareStoredContainedPin)
            else {StoredContainedReferenceDisposition.PROMOTED,
                  StoredContainedReferenceDisposition.ALREADY_PROMOTED}
        )
        if self.disposition not in allowed:
            raise ProtocolError(
                "stored contained-pin disposition contradicts its request"
            )

    @property
    def accepted(self) -> bool:
        return self.disposition is not None


LeaseGrant = GrantWorkerLease
LeaseSpillback = SpillbackWorkerLease
LeaseReject = RejectWorkerLease

ProtocolMessage = Union[
    PrepareStoredContainedPin,
    PromoteStoredContainedPin,
    StoredContainedPinReply,
    CreatePlacementGroupRequest,
    CreatePlacementGroupReply,
    GetPlacementGroupRequest,
    GetPlacementGroupReply,
    RemovePlacementGroupRequest,
    RemovePlacementGroupReply,
    DrainPlacementGroupsRequest,
    DrainPlacementGroupsReply,
    DrainOwnerDeathFences,
    DrainOwnerDeathFencesReply,
    DrainActorsRequest,
    DrainActorsReply,
    PreparePlacementGroupRequest,
    PreparePlacementGroupReply,
    CommitPlacementGroupRequest,
    CommitPlacementGroupReply,
    AbortPlacementGroupRequest,
    AbortPlacementGroupReply,
    NodeDeathRecord,
    ReportNodeDeath,
    ReportNodeDeathReply,
    GetNodeState,
    GetNodeStateReply,
    RegisterNode,
    RegisterNodeReply,
    UpdateNodeResources,
    UpdateNodeResourcesReply,
    UnregisterNode,
    UnregisterNodeReply,
    GetNodes,
    GetNodesReply,
    GetNodeAddress,
    GetNodeAddressReply,
    WorkerIncarnation,
    WorkerDeathRecord,
    RegisterWorkerIncarnation,
    RegisterWorkerIncarnationReply,
    ReportWorkerDeath,
    ReportWorkerDeathReply,
    GetWorkerState,
    GetWorkerStateReply,
    GetWorkerDeaths,
    GetWorkerDeathsReply,
    InstallClusterSnapshot,
    InstallClusterSnapshotReply,
    ActorClassDefinition,
    ActorWorkerExitRecord,
    ActorSnapshot,
    CreateActorRequest,
    CreateActorReply,
    ReserveActorWorkerRequest,
    ReserveActorWorkerReply,
    ReportActorWorkerExit,
    ReportActorWorkerExitReply,
    GetActorState,
    GetActorStateReply,
    InstallActorState,
    InstallActorStateReply,
    ActorWorkerStartup,
    ActorWorkerStartupFailure,
    ActorCallRequest,
    ActorCallReply,
    RegisterFunction,
    FunctionRegistrationReply,
    GetFunction,
    FunctionReply,
    DependencyOwnerRoute,
    RequestWorkerLease,
    LeaseDependencyInventory,
    AckLeaseDependencyCustody,
    AckLeaseDependencyCustodyReply,
    GrantWorkerLease,
    SpillbackWorkerLease,
    RejectWorkerLease,
    StartWorkerLease,
    StartWorkerLeaseReply,
    NotifyWorkerBlocked,
    NotifyWorkerBlockedReply,
    NotifyWorkerUnblocked,
    NotifyWorkerUnblockedReply,
    CompleteWorkerLease,
    CompleteWorkerLeaseReply,
    GetWorkerLeaseOutcome,
    GetWorkerLeaseOutcomeReply,
    PushTask,
    TaskReply,
    AcquireBorrowedObject,
    AcquireBorrowedObjectReply,
    ReleaseBorrowedObject,
    ReleaseBorrowedObjectReply,
    RetainOwnedObjectForTask,
    RetainOwnedObjectForTaskReply,
    GetRetainedOwnedObject,
    GetRetainedOwnedObjectReply,
    ReplaceRetainedObjectForTask,
    ReplaceRetainedObjectForTaskReply,
    ReportRetainedObjectLocation,
    ReportRetainedObjectLocationReply,
    ReportAbandonedDependencyReplica,
    ReportAbandonedDependencyReplicaReply,
    ReleaseOwnedObjectForTask,
    ReleaseOwnedObjectForTaskReply,
    ReleaseContainedReference,
    ReleaseContainedReferenceReply,
    GetOwnedObject,
    GetOwnedObjectReply,
    RequestOwnedObjectReconstruction,
    RequestOwnedObjectReconstructionReply,
    RequestDropOwnedObject,
    RequestDropOwnedObjectReply,
    SealObject,
    SealObjectReply,
    GetObject,
    GetObjectReply,
    PinObjectForTransfer,
    PinObjectForTransferReply,
    GetObjectChunk,
    GetObjectChunkReply,
    ReleaseObjectPin,
    ReleaseObjectPinReply,
    DropObjectReplica,
    DropObjectReplicaReply,
    OwnerDeathReplicaObservation,
    InstallOwnerDeathFence,
    InstallOwnerDeathFenceReply,
    CancelWorkerLease,
    CancelWorkerLeaseReply,
    ReleaseWorkerLease,
    ReleaseObject,
    ReleaseReply,
    Ack,
    Shutdown,
    BeginDrain,
    DrainStatus,
    FinalizeShutdown,
    ShutdownAck,
    ShutdownStatusRequest,
    ShutdownStatus,
    GCSStartup,
    NodeStartup,
    TraceRecord,
    TraceBatch,
    TraceBatchAck,
]
