"""Finite single-output wire checks; no RPC, Store, threads or child processes.

Owner table transitions consume explicit local facts. They do not model GCS
transactions, execute a Task, perform owner CAS, or prove remote cleanup.
"""

from dataclasses import fields, replace
import pickle
import socket
import subprocess
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_handoff import (
    OutputHandoffConflictError, OutputHandoffPhase, OutputHandoffStateError,
    OutputHandoffTable,
)
from miniray.output_publication import OutputPublicationManifest
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationRollbackPlan,
    OutputPublicationRollbackTombstone,
)
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
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _metadata_roundtrip(value, fixture):
    _assert_metadata(value)
    encoded = pickle.dumps(value)
    assert (fixture.payload not in encoded)
    assert pickle.loads(encoded) == value


def _identity(f):
    return wire.OutputPublicationRequestIdentity(f.publication_id, f.manifest.manifest_digest)


def _adoption(f, commit="owner-commit"):
    return OutputPublicationAdoptionProof(f.witness, f.owner, commit)


def _rollback(f):
    return OutputPublicationRollbackTombstone(OutputPublicationRollbackPlan(
        f.publication_id, f.manifest.manifest_digest, "rollback-before-effects", (),
    ), ())


@pytest.mark.parametrize("stored,refs", ((False, False), (False, True), (True, True)))
def test_prepare_carries_the_single_payload_and_reply_is_identity_only(stored, refs):
    f = _Fixture(stored=stored, refs=refs)
    request = wire.PrepareOutputPublication(f.manifest, (f.payload))
    assert (request.payload) == (f.payload) and (type(request.payload) is bytes)
    assert not hasattr(request, "slot_payloads")
    for wrapped in ((f.payload,), [f.payload]):
        with pytest.raises(ProtocolError):
            wire.PrepareOutputPublication(f.manifest, wrapped)
    assert request.request_identity == _identity(f)
    assert pickle.loads(pickle.dumps(request)) == request
    reply = wire.PreparedOutputPublicationReply(request.request_identity, True)
    _metadata_roundtrip(reply, f)
    assert not hasattr(reply, "request") and not hasattr(reply, "manifest")
    assert (not hasattr(reply, "payload") and not hasattr(reply, "slot_payloads"))


@pytest.mark.parametrize("case", ("missing", "extra", "size", "hash", "bytearray", "generator"))
def test_prepare_rejects_inexact_single_payload_before_effects(case):
    f = _Fixture()
    payload = {
        "missing": (), "extra": ((f.payload, b"extra")),
        "size": (f.payload + b"x"),
        "hash": (b"X" * len(f.payload)),
        "bytearray": (bytearray(f.payload)),
        "generator": iter(((f.payload,))),
    }[case]
    with pytest.raises(ProtocolError):
        wire.PrepareOutputPublication(f.manifest, payload)


def test_owner_handoff_and_retirement_echo_exact_metadata_facts():
    f = _Fixture()
    table = OutputHandoffTable()
    query = wire.GetOutputHandoff(f.publication_id)
    _metadata_roundtrip(wire.OutputHandoffReply(query, True, table.query(f.publication_id)), f)
    register = wire.RegisterOutputHandoff(f.manifest)
    registered = table.register(register.manifest, f.attempt)
    complete = wire.ReportOutputHandoffComplete(f.witness)
    completed = table.record_complete(complete.witness)
    adopted = table.adopt(_adoption(f))
    assert registered.complete is None and registered.adoption is None
    assert completed.complete == f.witness and completed.adoption is None
    assert adopted.adoption == _adoption(f)
    for request, snapshot in ((register, registered), (complete, completed), (query, adopted)):
        reply = wire.OutputHandoffReply(request, True, snapshot)
        _metadata_roundtrip((request, reply), f)
        assert reply.snapshot == snapshot and reply.snapshot is not snapshot
    retirement = wire.AckOutputPublicationAdopted(_adoption(f))
    assert retirement.request_identity == _identity(f)
    _metadata_roundtrip(wire.AckOutputPublicationAdoptedReply(retirement, True), f)
    # This empty rollback is an explicit pre-effect input fact, not remote proof.
    rollback = wire.ReportOutputHandoffRollback(f.manifest, _rollback(f))
    aborted = OutputHandoffTable().abort_manifest(f.manifest, "before-effects")
    _metadata_roundtrip(wire.OutputHandoffReply(rollback, True, aborted), f)


def test_handoff_rejects_digest_manifest_and_adoption_proof_rebinding():
    f = _Fixture()
    table = OutputHandoffTable()
    registered = table.register(f.manifest, f.attempt)
    changed = OutputPublicationManifest.create(
        f.header, (replace(f.manifest.value, size_bytes=f.manifest.value.size_bytes + 1)),
    )
    with pytest.raises(ProtocolError, match="manifest"):
        wire.OutputHandoffReply(wire.RegisterOutputHandoff(changed), True, registered)
    with pytest.raises(ProtocolError, match="manifest"):
        wire.ReportOutputHandoffRollback(changed, _rollback(f))
    # Complete digest binding belongs to the actual frozen owner receipt table.
    with pytest.raises(OutputHandoffConflictError):
        table.record_complete(replace(f.witness, manifest_digest="0" * 64))
    assert table.query(f.publication_id) == registered
    table.record_complete(f.witness)
    with pytest.raises(OutputHandoffConflictError):
        table.adopt(replace(_adoption(f), owner_worker_id=_id(WorkerID, 40)))
    adopted = table.adopt(_adoption(f))
    with pytest.raises(OutputHandoffConflictError):
        table.adopt(_adoption(f, "other-commit"))
    assert table.query(f.publication_id) == adopted


def test_aborted_handoff_fences_forward_work_without_inventing_complete_or_cleanup():
    f = _Fixture()
    table = OutputHandoffTable()
    table.register(f.manifest, f.attempt)
    assert table.abort(f.publication_id, "owner-cancelled")
    snapshot = table.query(f.publication_id)
    assert snapshot.phase is OutputHandoffPhase.ABORTED
    assert snapshot.complete is None and snapshot.adoption is None
    assert not hasattr(snapshot, "cleaned") and not hasattr(snapshot, "cleanup_receipt")
    for transition in (lambda: table.register(f.manifest, f.attempt),
                       lambda: table.record_complete(f.witness),
                       lambda: table.adopt(_adoption(f))):
        with pytest.raises((OutputHandoffStateError, OutputHandoffConflictError)):
            transition()
        assert table.query(f.publication_id) == snapshot
    rejected = wire.OutputHandoffReply(
        wire.ReportOutputHandoffComplete(f.witness), False, snapshot, "handoff aborted",
    )
    assert not rejected.accepted and rejected.snapshot.complete is None
    _metadata_roundtrip(rejected, f)


@pytest.mark.parametrize("factory", ("prepare", "retire"))
def test_errors_require_typed_kind_and_nonempty_detail(factory):
    f = _Fixture()
    constructors = {
        "prepare": lambda **kwargs: wire.PreparedOutputPublicationReply(_identity(f), False, **kwargs),
        "retire": lambda **kwargs: wire.AckOutputPublicationAdoptedReply(
            wire.AckOutputPublicationAdopted(_adoption(f)), False, **kwargs),
    }
    construct = constructors[factory]
    reply = construct(error_kind=wire.OutputPublicationRPCErrorKind.UNAVAILABLE, error="unavailable")
    _metadata_roundtrip(reply, f)
    for values in ({"error_kind": "UNAVAILABLE", "error": "unavailable"},
                   {"error_kind": wire.OutputPublicationRPCErrorKind.UNAVAILABLE, "error": ""},
                   {"error": "unavailable"}):
        with pytest.raises(ProtocolError):
            construct(**values)
    with pytest.raises(ProtocolError):
        replace(reply, accepted=True)
    with pytest.raises(ProtocolError):
        replace(reply, accepted=1)


@pytest.mark.parametrize("factory", (wire.RegisterOutputHandoff, wire.GetOutputHandoff))
def test_handoff_rejections_require_detail_and_acceptance_requires_a_receipt(factory):
    f = _Fixture()
    request = factory(f.manifest if factory is wire.RegisterOutputHandoff else f.publication_id)
    rejected = wire.OutputHandoffReply(request, False, error="unavailable")
    _metadata_roundtrip(rejected, f)
    for error in (None, "", 1):
        with pytest.raises(ProtocolError):
            wire.OutputHandoffReply(request, False, error=error)
    with pytest.raises(ProtocolError):
        replace(rejected, accepted=True)
    with pytest.raises(ProtocolError):
        replace(rejected, accepted=1)
    if factory is wire.RegisterOutputHandoff:
        with pytest.raises(ProtocolError, match="receipt"):
            wire.OutputHandoffReply(request, True)


def test_queries_and_retirement_reject_payload_values_and_other_identity():
    f = _Fixture()
    table = OutputHandoffTable()
    snapshot = table.register(f.manifest, f.attempt)
    query = wire.GetOutputHandoff(f.publication_id)
    with pytest.raises(ProtocolError):
        wire.OutputHandoffReply(query, True, f.envelope)
    other = replace(f.publication_id, lease_id=_id(LeaseID, 40))
    with pytest.raises(ProtocolError, match="identity"):
        wire.OutputHandoffReply(wire.GetOutputHandoff(other), True, snapshot)
    for constructor in (wire.RegisterOutputHandoff, wire.GetOutputHandoff,
                        wire.AckOutputPublicationAdopted, wire.ReportOutputHandoffComplete):
        with pytest.raises(ProtocolError):
            constructor(f.envelope)
    with pytest.raises(ProtocolError):
        wire.PreparedOutputPublicationReply(wire.PrepareOutputPublication(f.manifest, (f.payload)), True)


def _task_reply(f):
    return protocol.TaskReply(
        f.task, f.attempt, f.executor, protocol.TaskReplyStatus.SUCCEEDED,
        ((f.envelope.result,)), output_publication=f.envelope,
    )


def _complete_reply(f):
    return protocol.CompleteWorkerLeaseReply(
        f.lease, f.task, f.attempt, f.executor, protocol.TaskReplyStatus.SUCCEEDED,
        protocol.LeaseExecutionState.COMPLETED, True, True,
        output_publication=f.envelope,
    )


def _outcome_reply(f):
    result = f.envelope.result
    descriptors = ((protocol.ObjectStoreDescriptor(
        result.object_id, result.owner_worker_id, f.attempt, result.node_id,
        result.size_bytes, result.checksum,
    ),) if result.storage is protocol.ResultStorage.OBJECT_STORE else ())
    return protocol.GetWorkerLeaseOutcomeReply(
        f.lease, f.task, f.attempt, f.executor, f.owner, ((f.publication_id.object_id,)),
        f.node, True, True, protocol.LeaseExecutionState.COMPLETED,
        protocol.TaskReplyStatus.SUCCEEDED, descriptors, output_publication=f.envelope,
    )


@pytest.mark.parametrize("state", (protocol.LeaseExecutionState.COMPLETED,
                                    protocol.LeaseExecutionState.WORKER_LOST,
                                    protocol.LeaseExecutionState.ABANDONED))
def test_cleanup_pending_keeps_terminal_truth_but_never_returns_result_custody(state):
    f = _Fixture(stored=True)
    reply = protocol.GetWorkerLeaseOutcomeReply(
        f.lease, f.task, f.attempt, f.executor, f.owner, ((f.publication_id.object_id,)),
        f.node, True, state is not protocol.LeaseExecutionState.WORKER_LOST, state,
        protocol.TaskReplyStatus.SYSTEM_ERROR if state is protocol.LeaseExecutionState.COMPLETED else None,
        cleanup_pending=True,
    )
    _metadata_roundtrip(reply, f)
    assert reply.cleanup_pending and reply.state is state
    assert not reply.descriptors and not reply.orphan_descriptors
    for changes in ({"cleanup_pending": 1}, {"output_publication": f.envelope},
                    {"output_completion": f.witness},
                    {"descriptors": _outcome_reply(f).descriptors},
                    {"orphan_descriptors": _outcome_reply(f).descriptors},
                    {"found": False, "error": "missing"},
                    {"state": protocol.LeaseExecutionState.RUNNING, "completion_status": None},
                    {"state": protocol.LeaseExecutionState.COMPLETED, "completion_status": protocol.TaskReplyStatus.SUCCEEDED}):
        with pytest.raises(ProtocolError):
            replace(reply, **changes)


@pytest.mark.parametrize("stored", (False, True), ids=("inline", "stored"))
def test_single_output_envelopes_work_on_all_terminal_boundaries(stored):
    f = _Fixture(stored=stored)
    for reply in (_task_reply(f), _complete_reply(f), _outcome_reply(f)):
        restored = pickle.loads(pickle.dumps(reply))
        assert restored == reply and restored.output_publication == f.envelope
        assert restored.output_publication is not f.envelope
        assert not hasattr(restored, "stored_publication") and not hasattr(restored, "inline_publication")
    outcome = _outcome_reply(f)
    assert tuple(item.object_id for item in outcome.descriptors) == (((f.publication_id.object_id,)) if stored else ())
    assert (outcome.output_publication.result).inline_data == (None if stored else (f.payload))


def test_inline_outcome_has_no_stored_projection_and_stored_projection_is_exact():
    inline = _outcome_reply(_Fixture(refs=False))
    assert inline.descriptors == () and inline.orphan_descriptors == ()
    assert pickle.loads(pickle.dumps(inline)) == inline
    stored = _outcome_reply(_Fixture(stored=True))
    for changes in ({"descriptors": ()},
                    {"orphan_descriptors": stored.descriptors, "descriptors": ()},
                    {"descriptors": (replace(stored.descriptors[0], size_bytes=99),)}):
        with pytest.raises(ProtocolError):
            replace(stored, **changes)
    with pytest.raises(ProtocolError):
        replace(inline, descriptors=stored.descriptors)


@pytest.mark.parametrize("factory", (_task_reply, _complete_reply, _outcome_reply))
def test_terminal_replies_reject_incompatible_authority_and_deep_envelope_mutation(factory):
    f = _Fixture()
    reply = factory(f)
    for field in ("stored_publication", "inline_publication", "target_execution"):
        assert not hasattr(reply, field)
        with pytest.raises(TypeError, match="unexpected keyword argument.*" + field):
            replace(reply, **{field: object()})
    invalid_envelope = replace(f.envelope)
    object.__setattr__((invalid_envelope.result), "inline_data", b"changed")
    with pytest.raises(ProtocolError):
        replace(reply, output_publication=invalid_envelope)
    object.__setattr__(reply, "output_publication", invalid_envelope)
    with pytest.raises(ProtocolError):
        pickle.loads(pickle.dumps(reply))


@pytest.mark.parametrize("factory", (_task_reply, _complete_reply, _outcome_reply))
def test_terminal_envelopes_bind_executor_attempt_and_single_output(factory):
    f = _Fixture()
    reply = factory(f)
    executor_field = "executor_worker_id" if factory is _outcome_reply else "worker_id"
    for changes in ({executor_field: _id(WorkerID, 40)},
                    {"attempt_id": f.attempt.next()},
                    {"task_id": _id(TaskID, 40), "attempt_id": AttemptID(_id(TaskID, 40), 0)}):
        with pytest.raises(ProtocolError):
            replace(reply, **changes)
    if factory is not _task_reply:
        with pytest.raises(ProtocolError):
            replace(reply, lease_id=_id(LeaseID, 40))
    if factory is _outcome_reply:
        for changes in ({"owner_worker_id": _id(WorkerID, 40)},
                        {"node_id": _id(NodeID, 40)},
                        {"object_ids": ()},
                        {"object_ids": (ObjectID(f.task, 1),)}):
            with pytest.raises(ProtocolError):
                replace(reply, **changes)


@pytest.mark.parametrize("factory", (_task_reply, _complete_reply, _outcome_reply))
def test_terminal_wire_rebuild_rejects_wrong_field_count_and_shifted_values(factory):
    reply = factory(_Fixture())
    values = tuple(getattr(reply, field.name) for field in fields(reply))
    assert protocol._rebuild_validated_wire_message(type(reply), values) == reply
    for invalid in (values[:-1], values + (None,), list(values)):
        with pytest.raises(ProtocolError, match="field count"):
            protocol._rebuild_validated_wire_message(type(reply), invalid)
    shifted = list(values)
    names = [field.name for field in fields(reply)]
    envelope_index = names.index("output_publication")
    identity_index = names.index("task_id")
    shifted[envelope_index], shifted[identity_index] = shifted[identity_index], shifted[envelope_index]
    with pytest.raises(ProtocolError):
        protocol._rebuild_validated_wire_message(type(reply), tuple(shifted))


def test_task_results_and_success_status_cannot_disagree_with_envelope():
    f = _Fixture(stored=True)
    reply = _task_reply(f)
    for results in ((), ((f.envelope.result,)) * 2, (replace((f.envelope.result), size_bytes=99),)):
        with pytest.raises(ProtocolError):
            replace(reply, results=results)
    with pytest.raises(TypeError, match="unexpected keyword argument.*contained_edges"):
        replace(reply, contained_edges=(f.manifest.value).edges)
    with pytest.raises(ProtocolError):
        replace(reply, status=protocol.TaskReplyStatus.SYSTEM_ERROR, results=(),
                error=protocol.RemoteErrorInfo("Failed", "failed"))
    with pytest.raises(ProtocolError):
        replace(_complete_reply(f), status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    with pytest.raises(ProtocolError):
        replace(_complete_reply(f), accepted=False, released=False, error="rejected")
    with pytest.raises(ProtocolError):
        replace(_outcome_reply(f), completion_status=protocol.TaskReplyStatus.SYSTEM_ERROR)


def test_prepare_and_handoff_deep_revalidate_tampered_values_on_roundtrip():
    f = _Fixture()
    request = wire.PrepareOutputPublication(f.manifest, (f.payload))
    object.__setattr__(request, ("payload"), (b"changed"))
    with pytest.raises(ProtocolError):
        pickle.loads(pickle.dumps(request))
    manifest = replace(f.manifest)
    object.__setattr__((manifest.value), "size_bytes", -1)
    with pytest.raises(ProtocolError):
        wire.RegisterOutputHandoff(manifest)
    f = _Fixture()
    snapshot = OutputHandoffTable().register(f.manifest, f.attempt)
    object.__setattr__(snapshot, "phase", "ADOPTED")
    with pytest.raises(ProtocolError):
        wire.OutputHandoffReply(wire.RegisterOutputHandoff(f.manifest), True, snapshot)
    f = _Fixture()
    report = wire.ReportOutputHandoffComplete(f.witness)
    object.__setattr__((report.witness.publication_id.execution.attempt_id),
                       ("attempt_number"), (-1))
    with pytest.raises(ValueError):
        pickle.loads(pickle.dumps(report))


@pytest.mark.parametrize("stored", (False, True), ids=("inline", "stored"))
def test_retired_payload_completion_is_exact_metadata_only(stored):
    f = _Fixture(stored=stored)
    complete = replace(_complete_reply(f), output_publication=None, output_completion=f.witness)
    outcome = replace(_outcome_reply(f), output_publication=None, descriptors=(), output_completion=f.witness)
    for reply in (complete, outcome):
        _metadata_roundtrip(reply, f)
        assert reply.output_publication is None and reply.output_completion == f.witness
        with pytest.raises(ProtocolError):
            replace(reply, output_publication=f.envelope)
        with pytest.raises(ProtocolError):
            replace(reply, output_completion=f.envelope)
        with pytest.raises(ProtocolError):
            replace(reply, lease_id=_id(LeaseID, 40))
    with pytest.raises(ProtocolError):
        replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    with pytest.raises(ProtocolError):
        replace(complete, accepted=False, released=False, error="rejected")
    with pytest.raises(ProtocolError):
        replace(outcome, completion_status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    descriptors = _outcome_reply(_Fixture(stored=True)).descriptors
    for changes in ({"descriptors": descriptors}, {"orphan_descriptors": descriptors}):
        with pytest.raises(ProtocolError):
            replace(outcome, **changes)


def test_metadata_completion_rejects_changed_execution_and_nested_tampering():
    f = _Fixture()
    for original in (_complete_reply(f), _outcome_reply(f)):
        changes = {"output_publication": None, "output_completion": f.witness}
        if type(original) is protocol.GetWorkerLeaseOutcomeReply:
            changes["descriptors"] = ()
        reply = replace(original, **changes)
        with pytest.raises(ProtocolError):
            replace(reply, attempt_id=f.attempt.next())
        if type(original) is protocol.GetWorkerLeaseOutcomeReply:
            with pytest.raises(ProtocolError):
                replace(reply, object_ids=())
        invalid = replace(f.witness)
        object.__setattr__(invalid, "status", protocol.TaskReplyStatus.SYSTEM_ERROR)
        with pytest.raises(ProtocolError):
            replace(reply, output_completion=invalid)
        object.__setattr__(reply, "output_completion", invalid)
        with pytest.raises(ValueError):
            pickle.loads(pickle.dumps(reply))
