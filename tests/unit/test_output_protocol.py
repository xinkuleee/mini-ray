"""Bounded pure wire checks; no RPC, runtime threads, or process execution."""

from dataclasses import fields, replace
import pickle
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, LeaseID, NodeID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationManifest,
)
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationRollbackPlan,
    OutputPublicationRollbackTombstone, OutputPublicationSlotCleanupProof,
)
from miniray.output_recovery import (
    OutputPublicationRecoveryAuthority, OutputRecoveryAck,
    OutputRecoveryDisposition as Disposition, OutputRecoverySnapshot,
    OutputRecoveryStage as Stage,
)
from miniray.task_outputs import TargetExecutionKey, TargetOutputManifest, TaskOutputManifest
from tests.unit.test_output_publication import _Fixture, _assert_metadata, _id


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("output wire contract attempted runtime work")

    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Thread, "join", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _metadata_roundtrip(value, fixture):
    _assert_metadata(value)
    encoded = pickle.dumps(value)
    assert all(payload not in encoded for payload in fixture.payloads)
    assert pickle.loads(encoded) == value


def _identity(f):
    return wire.OutputPublicationRequestIdentity(f.publication_id, f.manifest.manifest_digest)


def _adoption(f, commit="owner-commit"):
    return OutputPublicationAdoptionProof(f.witness, f.owner, commit)


def _collection(f, index=0, cleanup="slot-cleanup"):
    return OutputPublicationSlotCleanupProof(
        f.witness, f.owner, index, f.publication_id.output_ids[index], cleanup,
    )


def _rollback(f):
    return OutputPublicationRollbackTombstone(OutputPublicationRollbackPlan(
        f.publication_id, f.manifest.manifest_digest, "rollback-before-effects", (),
    ), ())


@pytest.mark.parametrize("target,refs", ((False, False), (False, True), (True, True)))
def test_prepare_is_the_only_payload_message_and_reply_is_identity_only(target, refs):
    f = _Fixture(target=target, refs=refs)
    request = wire.PrepareOutputPublication(f.manifest, list(f.payloads))
    assert request.slot_payloads == f.payloads
    assert request.request_identity == _identity(f)
    assert pickle.loads(pickle.dumps(request)) == request
    reply = wire.PreparedOutputPublicationReply(request.request_identity, True)
    _metadata_roundtrip(reply, f)
    assert not hasattr(reply, "request") and not hasattr(reply, "manifest")
    assert not hasattr(reply, "slot_payloads")


@pytest.mark.parametrize("case", ("missing", "extra", "order", "size", "hash", "bytearray", "generator"))
def test_prepare_rejects_inexact_slot_data_before_effects(case):
    f = _Fixture()
    payloads = {
        "missing": f.payloads[:1], "extra": f.payloads + (b"extra",),
        "order": tuple(reversed(f.payloads)),
        "size": (f.payloads[0] + b"x", f.payloads[1]),
        "hash": (b"X" * len(f.payloads[0]), f.payloads[1]),
        "bytearray": (bytearray(f.payloads[0]), f.payloads[1]),
        "generator": iter(f.payloads),
    }[case]
    with pytest.raises(ProtocolError):
        wire.PrepareOutputPublication(f.manifest, payloads)


def test_recovery_stages_and_queries_echo_exact_metadata_facts():
    f = _Fixture(target=True)
    authority = OutputPublicationRecoveryAuthority()
    pairs = [(wire.ReportOutputPublicationIntent(f.manifest), authority.report_intent(f.manifest))]
    pairs.append((wire.ArmOutputPublication(f.publication_id, f.manifest.manifest_digest),
                  authority.arm_complete(f.publication_id, f.manifest.manifest_digest)))
    pairs.append((wire.ReportOutputPublicationTerminal(f.witness), authority.report_terminal(f.witness)))
    proof = _adoption(f)
    pairs.append((wire.ReportOutputPublicationAdopted(proof), authority.report_adopted(proof)))
    collection = _collection(f)
    pairs.append((wire.ReportOutputPublicationSlotCollected(collection), authority.report_slot_collected(collection)))
    for request, ack in pairs:
        reply = wire.OutputRecoveryReply(request, ack)
        assert reply.accepted and reply.ack == ack
        assert request.request_identity == _identity(f)
        _metadata_roundtrip((request, reply), f)
    query = wire.GetOutputPublicationRecovery(f.publication_id)
    _metadata_roundtrip(wire.GetOutputPublicationRecoveryReply(
        query, True, authority.snapshot(f.publication_id),
    ), f)
    _metadata_roundtrip(wire.GetOutputPublicationRecoveryReply(query, False), f)
    retirement = wire.AckOutputPublicationAdopted(proof)
    _metadata_roundtrip(wire.AckOutputPublicationAdoptedReply(retirement, True), f)
    assert retirement.request_identity == _identity(f)
    rollback_authority = OutputPublicationRecoveryAuthority()
    tombstone = _rollback(f)
    rollback = wire.ReportOutputPublicationRollback(tombstone, f.manifest)
    ack = rollback_authority.report_rollback(tombstone, manifest=f.manifest)
    _metadata_roundtrip(wire.OutputRecoveryReply(rollback, ack), f)


def test_recovery_rejects_stage_digest_manifest_and_proof_rebinding():
    f = _Fixture()
    intent = wire.ReportOutputPublicationIntent(f.manifest)
    snapshot = OutputRecoverySnapshot(f.manifest, armed=True)
    with pytest.raises(ProtocolError, match="stage"):
        wire.OutputRecoveryReply(intent, OutputRecoveryAck(Stage.ARM_COMPLETE, Disposition.APPLIED, snapshot))
    with pytest.raises(ProtocolError, match="identity"):
        wire.OutputRecoveryReply(
            wire.ArmOutputPublication(f.publication_id, "0" * 64),
            OutputRecoveryAck(Stage.ARM_COMPLETE, Disposition.APPLIED, snapshot),
        )
    changed = OutputPublicationManifest.create(
        f.header, (replace(f.slots[0], size_bytes=f.slots[0].size_bytes + 1), f.slots[1]),
    )
    with pytest.raises(ProtocolError):
        wire.OutputRecoveryReply(intent, OutputRecoveryAck(
            Stage.INTENT, Disposition.APPLIED, OutputRecoverySnapshot(changed),
        ))
    proof = _adoption(f)
    adopted = OutputRecoverySnapshot(f.manifest, armed=True, complete=f.witness, adopted=proof)
    with pytest.raises(ProtocolError, match="proof"):
        wire.OutputRecoveryReply(wire.ReportOutputPublicationAdopted(_adoption(f, "other-commit")),
                                OutputRecoveryAck(Stage.ADOPTED, Disposition.APPLIED, adopted))
    with pytest.raises(ProtocolError, match="owner"):
        wire.OutputRecoveryReply(wire.ReportOutputPublicationAdopted(replace(
            proof, owner_worker_id=_id(WorkerID, 40),
        )), OutputRecoveryAck(Stage.ADOPTED, Disposition.APPLIED, adopted))
    collected = OutputRecoverySnapshot(f.manifest, armed=True, complete=f.witness,
                                      slot_collections=(_collection(f, 0),))
    with pytest.raises(ProtocolError, match="exact proof"):
        wire.OutputRecoveryReply(wire.ReportOutputPublicationSlotCollected(_collection(f, 1)),
                                OutputRecoveryAck(Stage.SLOT_COLLECTED, Disposition.APPLIED, collected))


def test_fenced_ack_is_typed_non_authorizing_and_does_not_invent_terminal_fact():
    f = _Fixture()
    node = f.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "node-death", f.node, node.node_pid, node.registration_epoch, 8, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "test process exit",
    )
    snapshot = OutputRecoverySnapshot(f.manifest, armed=True, frozen_node_death=death)
    reports = (
        (wire.ArmOutputPublication(f.publication_id, f.manifest.manifest_digest), Stage.ARM_COMPLETE),
        (wire.ReportOutputPublicationTerminal(f.witness), Stage.TERMINAL),
        (wire.ReportOutputPublicationAdopted(_adoption(f)), Stage.ADOPTED),
        (wire.ReportOutputPublicationSlotCollected(_collection(f)), Stage.SLOT_COLLECTED),
    )
    for request, stage in reports:
        reply = wire.OutputRecoveryReply(request, OutputRecoveryAck(stage, Disposition.FENCED, snapshot))
        assert not reply.accepted and reply.ack.snapshot.complete is None
        _metadata_roundtrip(reply, f)


@pytest.mark.parametrize("factory", ("prepare", "recover", "retire", "query"))
def test_errors_require_typed_kind_and_nonempty_detail(factory):
    f = _Fixture()
    constructors = {
        "prepare": lambda **kwargs: wire.PreparedOutputPublicationReply(_identity(f), False, **kwargs),
        "recover": lambda **kwargs: wire.OutputRecoveryReply(wire.ReportOutputPublicationIntent(f.manifest), **kwargs),
        "retire": lambda **kwargs: wire.AckOutputPublicationAdoptedReply(wire.AckOutputPublicationAdopted(_adoption(f)), False, **kwargs),
        "query": lambda **kwargs: wire.GetOutputPublicationRecoveryReply(wire.GetOutputPublicationRecovery(f.publication_id), False, **kwargs),
    }
    construct = constructors[factory]
    reply = construct(error_kind=wire.OutputPublicationRPCErrorKind.UNAVAILABLE, error="unavailable")
    _metadata_roundtrip(reply, f)
    with pytest.raises(ProtocolError):
        construct(error_kind="UNAVAILABLE", error="unavailable")
    with pytest.raises(ProtocolError):
        construct(error_kind=wire.OutputPublicationRPCErrorKind.UNAVAILABLE, error="")
    with pytest.raises(ProtocolError):
        construct(error="unavailable")


def test_queries_and_retirement_do_not_accept_payload_values_or_other_identity():
    f = _Fixture()
    query = wire.GetOutputPublicationRecovery(f.publication_id)
    with pytest.raises(ProtocolError):
        wire.GetOutputPublicationRecoveryReply(query, True, f.envelope)
    with pytest.raises(ProtocolError):
        wire.GetOutputPublicationRecoveryReply(query, False, OutputRecoverySnapshot(f.manifest))
    other = replace(f.publication_id, lease_id=_id(LeaseID, 40))
    with pytest.raises(ProtocolError, match="another publication"):
        wire.GetOutputPublicationRecoveryReply(wire.GetOutputPublicationRecovery(other), True, OutputRecoverySnapshot(f.manifest))
    with pytest.raises(ProtocolError):
        wire.ReportOutputPublicationIntent(f.envelope)
    with pytest.raises(ProtocolError):
        wire.AckOutputPublicationAdopted(f.envelope)
    with pytest.raises(ProtocolError):
        wire.ReportOutputPublicationTerminal(f.envelope)
    with pytest.raises(ProtocolError):
        wire.PreparedOutputPublicationReply(wire.PrepareOutputPublication(f.manifest, f.payloads), True)


def _task_reply(f, envelope=None):
    envelope = f.envelope if envelope is None else envelope
    return protocol.TaskReply(
        f.task, f.attempt, f.executor, protocol.TaskReplyStatus.SUCCEEDED,
        envelope.results, target_execution=f.execution if type(f.execution) is TargetExecutionKey else None,
        output_publication=envelope,
    )


def _complete_reply(f, envelope=None):
    return protocol.CompleteWorkerLeaseReply(
        f.lease, f.task, f.attempt, f.executor, protocol.TaskReplyStatus.SUCCEEDED,
        protocol.LeaseExecutionState.COMPLETED, True, True,
        target_execution=f.execution if type(f.execution) is TargetExecutionKey else None,
        output_publication=f.envelope if envelope is None else envelope,
    )


def _outcome_reply(f, envelope=None):
    envelope = f.envelope if envelope is None else envelope
    descriptors = tuple(protocol.ObjectStoreDescriptor(
        result.object_id, result.owner_worker_id, f.attempt, result.node_id,
        result.size_bytes, result.checksum,
    ) for result in envelope.results if result.storage is protocol.ResultStorage.OBJECT_STORE)
    return protocol.GetWorkerLeaseOutcomeReply(
        f.lease, f.task, f.attempt, f.executor, f.owner, f.publication_id.output_ids,
        f.node, True, True, protocol.LeaseExecutionState.COMPLETED,
        protocol.TaskReplyStatus.SUCCEEDED, descriptors,
        target_execution=f.execution if type(f.execution) is TargetExecutionKey else None,
        output_publication=envelope,
    )


@pytest.mark.parametrize("state", (protocol.LeaseExecutionState.COMPLETED,
                                    protocol.LeaseExecutionState.WORKER_LOST,
                                    protocol.LeaseExecutionState.ABANDONED))
def test_cleanup_pending_keeps_terminal_truth_but_never_returns_result_custody(state):
    f = _Fixture(target=True)
    reply = protocol.GetWorkerLeaseOutcomeReply(
        f.lease, f.task, f.attempt, f.executor, f.owner, f.publication_id.output_ids,
        f.node, True, state is not protocol.LeaseExecutionState.WORKER_LOST, state,
        protocol.TaskReplyStatus.SYSTEM_ERROR if state is protocol.LeaseExecutionState.COMPLETED else None,
        target_execution=f.execution, cleanup_pending=True,
    )
    _metadata_roundtrip(reply, f)
    assert reply.cleanup_pending and reply.state is state
    assert not reply.descriptors and not reply.orphan_descriptors
    for changes in ({"cleanup_pending": 1}, {"output_publication": f.envelope},
                    {"output_completion": f.witness},
                    {"descriptors": _outcome_reply(f).descriptors},
                    {"state": protocol.LeaseExecutionState.RUNNING, "completion_status": None},
                    {"state": protocol.LeaseExecutionState.COMPLETED, "completion_status": protocol.TaskReplyStatus.SUCCEEDED}):
        with pytest.raises(ProtocolError):
            replace(reply, **changes)


@pytest.mark.parametrize("target", (False, True))
def test_mixed_unified_envelopes_work_on_all_terminal_boundaries(target):
    f = _Fixture(target=target)
    for reply in (_task_reply(f), _complete_reply(f), _outcome_reply(f)):
        restored = pickle.loads(pickle.dumps(reply))
        assert restored == reply
        assert restored.output_publication == f.envelope
        assert restored.output_publication is not f.envelope
        assert not hasattr(reply, "stored_publication") and not hasattr(reply, "inline_publication")
        assert not hasattr(restored, "stored_publication") and not hasattr(restored, "inline_publication")
    outcome = _outcome_reply(f)
    assert tuple(item.object_id for item in outcome.descriptors) == (f.slots[1].object_id,)
    assert outcome.output_publication.results[0].inline_data == f.payloads[0]


def test_all_inline_outcome_uses_empty_stored_projection_and_no_orphans():
    f = _Fixture(refs=False)
    slots = tuple(replace(slot, tier=protocol.ResultStorage.INLINE) for slot in f.slots)
    manifest = OutputPublicationManifest.create(f.header, slots)
    results = tuple(replace(result, storage=protocol.ResultStorage.INLINE, inline_data=payload)
                    for result, payload in zip(f.results, f.payloads))
    envelope = OutputPublicationEnvelope(manifest, OutputPublicationCompleteWitness.for_manifest(manifest), results)
    outcome = _outcome_reply(f, envelope)
    assert outcome.descriptors == () and outcome.orphan_descriptors == ()
    assert pickle.loads(pickle.dumps(outcome)) == outcome
    with pytest.raises(ProtocolError):
        replace(_outcome_reply(f), descriptors=())
    with pytest.raises(ProtocolError):
        replace(_outcome_reply(f), orphan_descriptors=_outcome_reply(f).descriptors, descriptors=())
    with pytest.raises(ProtocolError):
        replace(_outcome_reply(f), descriptors=(replace(_outcome_reply(f).descriptors[0], size_bytes=99),))


@pytest.mark.parametrize("factory", (_task_reply, _complete_reply, _outcome_reply))
def test_terminal_replies_reject_incompatible_authority_and_deep_envelope_mutation(factory):
    f = _Fixture()
    reply = factory(f)
    for field in ("stored_publication", "inline_publication"):
        assert not hasattr(reply, field)
        constructor_fields = {item.name: getattr(reply, item.name) for item in fields(reply)}
        assert field not in constructor_fields
        with pytest.raises(TypeError, match="unexpected keyword argument.*" + field):
            type(reply)(**constructor_fields, **{field: object()})
        with pytest.raises(TypeError, match="unexpected keyword argument.*" + field):
            replace(reply, **{field: object()})
    invalid_envelope = replace(f.envelope)
    object.__setattr__(invalid_envelope.results[0], "inline_data", b"changed")
    with pytest.raises(ProtocolError):
        replace(reply, output_publication=invalid_envelope)
    # Existing outer reply unpickling must not trust a frozen nested envelope.
    object.__setattr__(reply, "output_publication", invalid_envelope)
    with pytest.raises(ProtocolError):
        pickle.loads(pickle.dumps(reply))


@pytest.mark.parametrize("factory", (_task_reply, _complete_reply, _outcome_reply))
def test_terminal_envelopes_bind_executor_attempt_full_and_selected_manifest(factory):
    f = _Fixture(target=True)
    reply = factory(f)
    executor_field = "executor_worker_id" if factory is _outcome_reply else "worker_id"
    with pytest.raises(ProtocolError):
        replace(reply, **{executor_field: _id(WorkerID, 40)})
    with pytest.raises(ProtocolError):
        replace(reply, attempt_id=AttemptID(f.task, f.attempt.attempt_number + 1))
    with pytest.raises(ProtocolError, match="target"):
        replace(reply, target_execution=None)
    other_full = TaskOutputManifest.for_task(f.task, 5)
    changed_full = TargetExecutionKey(TargetOutputManifest(
        other_full, f.publication_id.output_ids,
    ), f.attempt)
    with pytest.raises(ProtocolError, match="manifest"):
        replace(reply, target_execution=changed_full)
    if factory is not _task_reply:
        with pytest.raises(ProtocolError):
            replace(reply, lease_id=_id(LeaseID, 40))
    if factory is _outcome_reply:
        with pytest.raises(ProtocolError):
            replace(reply, owner_worker_id=_id(WorkerID, 40))
        with pytest.raises(ProtocolError):
            replace(reply, node_id=_id(NodeID, 40))


@pytest.mark.parametrize("factory", (_task_reply, _complete_reply, _outcome_reply))
def test_retired_wire_layout_is_rejected_instead_of_shifting_positional_authorities(factory):
    from dataclasses import fields

    reply = factory(_Fixture())
    values = tuple(getattr(reply, field.name) for field in fields(reply))
    assert protocol._rebuild_validated_wire_message(type(reply), values) == reply
    # Legacy replies had two tier-specific fields immediately before the
    # unified envelope. New participants reject that complete old layout.
    envelope_index = next(index for index, field in enumerate(fields(reply))
                          if field.name == "output_publication")
    old_values = values[:envelope_index] + (None, None) + values[envelope_index:]
    for incompatible in (old_values, values[:-1]):
        with pytest.raises(ProtocolError, match="field count"):
            protocol._rebuild_validated_wire_message(type(reply), incompatible)


def test_task_results_and_success_status_cannot_disagree_with_envelope():
    f = _Fixture()
    reply = _task_reply(f)
    with pytest.raises(ProtocolError):
        replace(reply, results=(f.results[0],))
    with pytest.raises(ProtocolError):
        replace(reply, results=(f.results[0], replace(f.results[1], size_bytes=99)))
    assert not hasattr(reply, "contained_edges")
    with pytest.raises(TypeError, match="unexpected keyword argument.*contained_edges"):
        protocol.TaskReply(
            f.task, f.attempt, f.executor, protocol.TaskReplyStatus.SUCCEEDED,
            f.envelope.results, output_publication=f.envelope,
            contained_edges=f.manifest.ordered_edges,
        )
    with pytest.raises(TypeError, match="unexpected keyword argument.*contained_edges"):
        replace(reply, contained_edges=f.manifest.ordered_edges)
    with pytest.raises(ProtocolError):
        replace(reply, status=protocol.TaskReplyStatus.SYSTEM_ERROR, results=(),
                error=protocol.RemoteErrorInfo("Failed", "failed"))
    with pytest.raises(ProtocolError):
        replace(_complete_reply(f), status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    with pytest.raises(ProtocolError):
        replace(_complete_reply(f), accepted=False, released=False, error="rejected")
    with pytest.raises(ProtocolError):
        replace(_outcome_reply(f), completion_status=protocol.TaskReplyStatus.SYSTEM_ERROR)


def test_prepare_and_recovery_deep_revalidate_tampered_values_on_roundtrip():
    f = _Fixture()
    request = wire.PrepareOutputPublication(f.manifest, f.payloads)
    object.__setattr__(request, "slot_payloads", (b"changed", f.payloads[1]))
    with pytest.raises(ProtocolError):
        pickle.loads(pickle.dumps(request))
    manifest = replace(f.manifest)
    object.__setattr__(manifest.slots[0], "size_bytes", -1)
    with pytest.raises(ProtocolError):
        wire.ReportOutputPublicationIntent(manifest)
    f = _Fixture()
    ack = OutputRecoveryAck(Stage.INTENT, Disposition.APPLIED, OutputRecoverySnapshot(f.manifest))
    object.__setattr__(ack, "stage", Stage.TERMINAL)
    with pytest.raises(ProtocolError):
        wire.OutputRecoveryReply(wire.ReportOutputPublicationIntent(f.manifest), ack)


@pytest.mark.parametrize("target", (False, True))
def test_retired_payload_completion_is_exact_metadata_only(target):
    f = _Fixture(target=target)
    complete = replace(_complete_reply(f), output_publication=None, output_completion=f.witness)
    outcome = replace(_outcome_reply(f), output_publication=None, descriptors=(),
                      output_completion=f.witness)
    for reply in (complete, outcome):
        _metadata_roundtrip(reply, f)
        assert reply.output_publication is None and reply.output_completion == f.witness
        with pytest.raises(ProtocolError):
            replace(reply, output_publication=f.envelope)
        assert not hasattr(reply, "inline_publication") and not hasattr(reply, "stored_publication")
        with pytest.raises(TypeError, match="unexpected keyword argument.*inline_publication"):
            replace(reply, inline_publication=object())
        with pytest.raises(TypeError, match="unexpected keyword argument.*stored_publication"):
            replace(reply, stored_publication=object())
        with pytest.raises(ProtocolError):
            replace(reply, lease_id=_id(LeaseID, 40))
        with pytest.raises(ProtocolError):
            replace(reply, output_completion=f.envelope)
    with pytest.raises(ProtocolError):
        replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    with pytest.raises(ProtocolError):
        replace(complete, accepted=False, released=False, error="rejected")
    with pytest.raises(ProtocolError):
        replace(outcome, completion_status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    with pytest.raises(ProtocolError):
        replace(outcome, descriptors=_outcome_reply(f).descriptors)
    with pytest.raises(ProtocolError):
        replace(outcome, orphan_descriptors=_outcome_reply(f).descriptors)


def test_metadata_completion_rejects_changed_full_target_manifest_and_nested_tampering():
    f = _Fixture(target=True)
    full = TaskOutputManifest.for_task(f.task, 5)
    changed = TargetExecutionKey(TargetOutputManifest(full, f.publication_id.output_ids), f.attempt)
    for original in (_complete_reply(f), _outcome_reply(f)):
        fields = {"output_publication": None, "output_completion": f.witness}
        if type(original) is protocol.GetWorkerLeaseOutcomeReply:
            fields["descriptors"] = ()
        reply = replace(original, **fields)
        with pytest.raises(ProtocolError, match="manifest"):
            replace(reply, target_execution=changed)
        with pytest.raises(ProtocolError, match="target"):
            replace(reply, target_execution=None)
        invalid = replace(f.witness)
        object.__setattr__(invalid, "status", protocol.TaskReplyStatus.SYSTEM_ERROR)
        with pytest.raises(ProtocolError):
            replace(reply, output_completion=invalid)
