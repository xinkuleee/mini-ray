from __future__ import annotations

import hashlib

import cloudpickle
import pytest

from miniray.actor_arguments import (
    ActorArgumentAuthority, ActorArgumentConflictError,
    ActorArgumentDependencyError, ActorArgumentFencedError, ActorArgumentScope,
    ActorArgumentSpec, ActorArgumentsNotReady,
)
from miniray.actor_state import ActorCallKey
from miniray.ids import (
    ActorGeneration, ActorID, AttemptID, NodeID, ObjectID, TaskID, WorkerID,
)
from miniray.protocol import (
    AcquireBorrowedObjectReply, GetOwnedObjectReply, InlineArg,
    NestedReferenceTransfer, ObjectStoreDescriptor, OwnedObjectState, RefArg,
    RemoteErrorInfo, TaskHoldSource, TaskReferenceHold, TaskReferenceHoldKind,
)


pytestmark = pytest.mark.unit


def _worker(seed: int) -> WorkerID:
    return WorkerID(bytes([seed]) * 16)


def _task(seed: int) -> TaskID:
    return TaskID(bytes([seed]) * 16)


def _actor(seed: int = 90) -> ActorID:
    return ActorID(bytes([seed]) * 16)


def _transfer(
    object_id: ObjectID, owner: WorkerID, submitter: WorkerID, task_id: TaskID,
    *, port: int, kind: TaskReferenceHoldKind = TaskReferenceHoldKind.RETAINED,
) -> NestedReferenceTransfer:
    attempt = AttemptID(task_id, 0)
    return NestedReferenceTransfer(
        object_id, owner, ("127.0.0.1", port),
        TaskReferenceHold(kind, submitter, task_id, attempt),
    )


def _constructor_fixture():
    actor_id = _actor()
    creator = _worker(1)
    task_id = _task(2)
    top_object = ObjectID.for_task(_task(3))
    nested_object = ObjectID.for_task(_task(4))
    top_owner = _worker(5)
    nested_owner = _worker(6)
    top = _transfer(top_object, top_owner, creator, task_id, port=15005)
    nested = _transfer(
        nested_object, nested_owner, creator, task_id, port=15006
    )
    inline = InlineArg(
        cloudpickle.dumps(("nested-marker",)),
        serializer="cloudpickle", nested_refs=(nested,),
    )
    spec = ActorArgumentSpec(
        actor_id=actor_id, owner_worker_id=creator, task_id=task_id,
        attempt_id=AttemptID(task_id, 0),
        args=(RefArg(top_object, top_owner), inline, RefArg(top_object, top_owner)),
        top_level_transfers=(top,),
    )
    return spec, top, nested


def _acquired(
    transfer: NestedReferenceTransfer, target: WorkerID, token: str,
    *, accepted: bool = True,
) -> AcquireBorrowedObjectReply:
    return AcquireBorrowedObjectReply(
        transfer.object_id, transfer.owner_worker_id, target,
        TaskHoldSource(transfer.hold), token, accepted, accepted,
        None if accepted else "owner rejected acquire",
    )


def _stored_observation(
    transfer: NestedReferenceTransfer, target: WorkerID, token: str,
    attempt_number: int, *, payload: bytes = b"stored", node_seed: int = 30,
) -> GetOwnedObjectReply:
    attempt = AttemptID(transfer.object_id.task_id, attempt_number)
    descriptor = ObjectStoreDescriptor(
        transfer.object_id, transfer.owner_worker_id, attempt,
        NodeID(bytes([node_seed]) * 16), len(payload),
        hashlib.sha256(payload).hexdigest(),
    )
    return GetOwnedObjectReply(
        transfer.object_id, transfer.owner_worker_id, target, token, True,
        OwnedObjectState.READY_STORED, current_attempt=attempt,
        descriptor=descriptor,
    )


def _inline_observation(
    transfer: NestedReferenceTransfer, target: WorkerID, token: str, payload: bytes,
) -> GetOwnedObjectReply:
    return GetOwnedObjectReply(
        transfer.object_id, transfer.owner_worker_id, target, token, True,
        OwnedObjectState.READY_INLINE, data=payload,
    )


def _prepare_inputs(
    top: NestedReferenceTransfer, nested: NestedReferenceTransfer,
    worker: WorkerID, *, top_token: str, nested_token: str,
    top_observation: GetOwnedObjectReply,
):
    return (
        {
            (top.object_id, top.owner_worker_id): _acquired(
                top, worker, top_token
            ),
            (nested.object_id, nested.owner_worker_id): _acquired(
                nested, worker, nested_token
            ),
        },
        {(top.object_id, top.owner_worker_id): top_observation},
    )


def test_constructor_spec_deduplicates_occurrences_and_preserves_full_holds() -> None:
    spec, top, nested = _constructor_fixture()

    assert spec.scope is ActorArgumentScope.ACTOR_LIFETIME
    assert spec.top_level_references == (
        RefArg(top.object_id, top.owner_worker_id),
    )
    assert spec.nested_transfers == (nested,)
    assert spec.reference_transfers == (top, nested)
    assert spec.logical_holds == (top.hold, nested.hold)
    assert all(isinstance(value, TaskReferenceHold) for value in spec.logical_holds)
    # Physical dependency descriptors never become constructor lineage.
    assert not hasattr(spec, "dependencies")


def test_spec_rejects_incomplete_manifest_conflicts_and_hold_drift() -> None:
    spec, top, nested = _constructor_fixture()
    with pytest.raises(ValueError, match="exactly cover"):
        ActorArgumentSpec(
            spec.actor_id, spec.owner_worker_id, spec.task_id, spec.attempt_id,
            spec.args, top_level_transfers=(),
        )

    wrong_owner = _worker(42)
    with pytest.raises(ValueError, match="exact transfer"):
        ActorArgumentSpec(
            spec.actor_id, spec.owner_worker_id, spec.task_id, spec.attempt_id,
            (
                RefArg(top.object_id, top.owner_worker_id),
                InlineArg(b"x", nested_refs=(NestedReferenceTransfer(
                    top.object_id, top.owner_worker_id, ("127.0.0.1", 15999),
                    nested.hold,
                ),)),
            ),
            top_level_transfers=(top,),
        )

    bad = _transfer(
        top.object_id, top.owner_worker_id, wrong_owner, spec.task_id, port=15005
    )
    with pytest.raises(ValueError, match="submitter"):
        ActorArgumentSpec(
            spec.actor_id, spec.owner_worker_id, spec.task_id, spec.attempt_id,
            (RefArg(top.object_id, top.owner_worker_id),),
            top_level_transfers=(bad,),
        )


def test_constructor_preparation_keeps_stored_ref_and_exact_descriptor() -> None:
    spec, top, nested = _constructor_fixture()
    generation = ActorGeneration(spec.actor_id, 0)
    worker = _worker(10)
    authority = ActorArgumentAuthority(spec)
    authority.install_generation(generation, worker)
    observation = _stored_observation(top, worker, "top-g0", 3)
    acquisitions, observations = _prepare_inputs(
        top, nested, worker, top_token="top-g0", nested_token="nested-g0",
        top_observation=observation,
    )

    prepared = authority.prepare_constructor(
        acquisitions=acquisitions, observations=observations
    )
    replay = authority.prepare_constructor(
        acquisitions=acquisitions, observations=observations
    )

    assert replay is prepared
    assert prepared.args[0] == RefArg(top.object_id, top.owner_worker_id)
    assert prepared.args[2] == prepared.args[0]
    assert prepared.args[1] == spec.args[1]
    assert prepared.dependencies == (observation.descriptor,)
    assert tuple(value.source for value in prepared.acquisitions) == (
        TaskHoldSource(top.hold), TaskHoldSource(nested.hold),
    )
    assert tuple(value.borrower_worker_id for value in prepared.acquisitions) == (
        worker, worker,
    )
    assert tuple(value.borrower_token for value in prepared.acquisitions) == (
        "top-g0", "nested-g0",
    )
    assert tuple(value.borrower_token for value in prepared.execution_releases) == (
        "top-g0", "nested-g0",
    )


def test_constructor_restart_refreshes_descriptor_and_execution_borrowers() -> None:
    spec, top, nested = _constructor_fixture()
    authority = ActorArgumentAuthority(spec)
    generation0 = ActorGeneration(spec.actor_id, 0)
    worker0 = _worker(10)
    authority.install_generation(generation0, worker0)
    observation0 = _stored_observation(top, worker0, "top-g0", 0, node_seed=31)
    acquire0, observed0 = _prepare_inputs(
        top, nested, worker0, top_token="top-g0", nested_token="nested-g0",
        top_observation=observation0,
    )
    prepared0 = authority.prepare_constructor(
        acquisitions=acquire0, observations=observed0
    )

    generation1 = generation0.next()
    worker1 = _worker(11)
    abandoned = authority.install_generation(generation1, worker1)
    assert abandoned == (prepared0,)
    observation1 = _stored_observation(top, worker1, "top-g1", 4, node_seed=32)
    acquire1, observed1 = _prepare_inputs(
        top, nested, worker1, top_token="top-g1", nested_token="nested-g1",
        top_observation=observation1,
    )
    prepared1 = authority.prepare_constructor(
        acquisitions=acquire1, observations=observed1
    )

    assert prepared1.dependencies == (observation1.descriptor,)
    assert prepared1.dependencies != prepared0.dependencies
    assert prepared1.spec is prepared0.spec is spec
    assert prepared1.spec.logical_holds == prepared0.spec.logical_holds
    assert prepared1.spec.logical_holds[0].origin_attempt_id == AttemptID(
        spec.task_id, 0
    )
    assert {value.borrower_token for value in prepared1.acquisitions} == {
        "top-g1", "nested-g1"
    }
    assert authority.constructor_lifetime_transfers == (top, nested)


def test_restart_rejects_stale_or_reused_constructor_execution_identity() -> None:
    spec, top, nested = _constructor_fixture()
    authority = ActorArgumentAuthority(spec)
    generation0 = ActorGeneration(spec.actor_id, 0)
    worker0 = _worker(10)
    authority.install_generation(generation0, worker0)
    acquire0, observed0 = _prepare_inputs(
        top, nested, worker0, top_token="top", nested_token="nested",
        top_observation=_stored_observation(top, worker0, "top", 0),
    )
    authority.prepare_constructor(acquisitions=acquire0, observations=observed0)

    with pytest.raises(ActorArgumentConflictError, match="target Worker"):
        authority.install_generation(generation0, _worker(11))
    authority.install_generation(generation0.next(), _worker(11))
    with pytest.raises(ActorArgumentFencedError, match="already fenced"):
        authority.install_generation(generation0, worker0)

    # Even though the physical Worker changed, each reference also needs a fresh
    # borrower token; a lost old release must not alias the new import.
    acquire1, observed1 = _prepare_inputs(
        top, nested, _worker(11), top_token="top", nested_token="nested",
        top_observation=_stored_observation(top, _worker(11), "top", 1),
    )
    with pytest.raises(ActorArgumentConflictError, match="fresh borrower"):
        authority.prepare_constructor(
            acquisitions=acquire1, observations=observed1
        )


def test_inline_refresh_substitutes_value_but_nested_handle_is_unchanged() -> None:
    spec, top, nested = _constructor_fixture()
    worker = _worker(10)
    authority = ActorArgumentAuthority(spec)
    authority.install_generation(ActorGeneration(spec.actor_id, 0), worker)
    payload = cloudpickle.dumps({"value": 7})
    observation = _inline_observation(top, worker, "top-inline", payload)
    acquisitions, observations = _prepare_inputs(
        top, nested, worker, top_token="top-inline",
        nested_token="nested-inline", top_observation=observation,
    )

    prepared = authority.prepare_constructor(
        acquisitions=acquisitions, observations=observations
    )

    assert prepared.args[0] == InlineArg(payload, serializer="cloudpickle")
    assert prepared.args[2] == prepared.args[0]
    assert prepared.args[1] == spec.args[1]
    assert prepared.dependencies == ()


def test_pending_and_failed_owner_facts_do_not_partially_prepare() -> None:
    spec, top, nested = _constructor_fixture()
    worker = _worker(10)
    authority = ActorArgumentAuthority(spec)
    generation = ActorGeneration(spec.actor_id, 0)
    authority.install_generation(generation, worker)
    acquisitions, _ = _prepare_inputs(
        top, nested, worker, top_token="top", nested_token="nested",
        top_observation=_stored_observation(top, worker, "top", 0),
    )
    pending = GetOwnedObjectReply(
        top.object_id, top.owner_worker_id, worker, "top", True,
        OwnedObjectState.PENDING,
    )
    with pytest.raises(ActorArgumentsNotReady) as info:
        authority.prepare_constructor(
            acquisitions=acquisitions,
            observations={(top.object_id, top.owner_worker_id): pending},
        )
    assert info.value.missing == (top.object_id,)
    assert authority.prepared_constructor(generation) is None

    failed = GetOwnedObjectReply(
        top.object_id, top.owner_worker_id, worker, "top", True,
        OwnedObjectState.ERROR,
        error=RemoteErrorInfo("ValueError", "upstream failed"),
    )
    with pytest.raises(ActorArgumentDependencyError, match="upstream failed"):
        authority.prepare_constructor(
            acquisitions=acquisitions,
            observations={(top.object_id, top.owner_worker_id): failed},
        )
    assert authority.prepared_constructor(generation) is None


def _method_spec(
    constructor: ActorArgumentSpec, generation: ActorGeneration, sequence: int,
    *, task_seed: int, value: bytes = b"value",
) -> ActorArgumentSpec:
    task_id = _task(task_seed)
    return ActorArgumentSpec(
        constructor.actor_id, constructor.owner_worker_id, task_id,
        AttemptID(task_id, 0),
        args=(InlineArg(value, serializer="cloudpickle"),),
        generation=generation,
        call_key=ActorCallKey(constructor.owner_worker_id, sequence),
    )


def test_method_key_exact_replay_conflict_and_generation_fencing() -> None:
    constructor, _top, _nested = _constructor_fixture()
    authority = ActorArgumentAuthority(constructor)
    generation0 = ActorGeneration(constructor.actor_id, 0)
    worker0 = _worker(10)
    authority.install_generation(generation0, worker0)
    method0 = _method_spec(constructor, generation0, 0, task_seed=60)

    first = authority.prepare_method(
        method0, acquisitions={}, observations={}
    )
    replay = authority.prepare_method(
        method0, acquisitions={}, observations={}
    )
    assert replay is first
    assert first.spec.scope is ActorArgumentScope.METHOD_CALL

    conflicting = _method_spec(
        constructor, generation0, 0, task_seed=61, value=b"different"
    )
    with pytest.raises(ActorArgumentConflictError, match="call key"):
        authority.register_method(conflicting)

    generation1 = generation0.next()
    abandoned = authority.install_generation(generation1, _worker(11))
    assert first in abandoned
    with pytest.raises(ActorArgumentFencedError, match="non-current"):
        authority.prepare_method(method0, acquisitions={}, observations={})

    # Sequence 0 is legal again only with a fresh Task identity in generation 1.
    method1 = _method_spec(constructor, generation1, 0, task_seed=62)
    assert authority.prepare_method(
        method1, acquisitions={}, observations={}
    ).generation == generation1


def test_method_task_identity_cannot_cross_generation_and_retirement_is_exact() -> None:
    constructor, top, nested = _constructor_fixture()
    authority = ActorArgumentAuthority(constructor)
    generation0 = ActorGeneration(constructor.actor_id, 0)
    authority.install_generation(generation0, _worker(10))
    original = _method_spec(constructor, generation0, 0, task_seed=70)
    authority.register_method(original)
    generation1 = generation0.next()
    authority.install_generation(generation1, _worker(11))
    rebound = ActorArgumentSpec(
        original.actor_id, original.owner_worker_id, original.task_id,
        original.attempt_id, original.args, generation=generation1,
        call_key=ActorCallKey(original.owner_worker_id, 0),
    )
    with pytest.raises(ActorArgumentConflictError, match="TaskID"):
        authority.register_method(rebound)

    assert authority.retire_actor() == (top, nested)
    assert authority.retire_actor() == (top, nested)
    with pytest.raises(ActorArgumentFencedError, match="retired"):
        authority.install_generation(generation1.next(), _worker(12))
