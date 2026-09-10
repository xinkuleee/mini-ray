"""Single-output wire and Worker contracts, with no sockets or background work."""

from dataclasses import fields, replace
import hashlib
import importlib.util
import inspect

from types import SimpleNamespace
import cloudpickle
import pytest

from miniray import dependency, protocol, task_outputs, worker
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, PlacementGroupID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_protocol import PrepareOutputPublication, PreparedOutputPublicationReply
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationEnvelope, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation,
)
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecution


pytestmark = pytest.mark.unit


def _spec(value=None):
    job = JobID(b"j" * 16)
    task = TaskID.derive(job, TaskID.for_driver(job), 1)
    key = protocol.FunctionKey(job, __name__, "single_result", "v1")
    return protocol.TaskSpec(
        job, task, AttemptID(task, 0), key, (), 1, ResourceVector.of(CPU=1),
        WorkerID(b"o" * 16),
        function_definition=protocol.FunctionDefinition.from_payload(
            key, cloudpickle.dumps(lambda: value),
        ),
    )


def _header(spec=None):
    spec = spec or _spec()
    return OutputPublicationHeader(
        OutputPublicationID(LeaseID(b"l" * 16), TaskExecution.from_task_spec(spec)),
        spec.job_id, WorkerID(b"w" * 16), spec.owner_worker_id,
        OutputPublicationNodeIncarnation(NodeID(b"n" * 16), 1234, 1),
    )


def _envelope(manifest, payload):
    value = (manifest.value)
    result = protocol.ResultDescriptor(
        (manifest.publication_id).object_id, value.tier, value.size_bytes, manifest.header.owner_worker_id,
        manifest.header.node_incarnation.node_id, value.checksum, payload,
    )
    return OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), result,
    )


def test_task_spec_has_one_stable_output_across_attempts():
    spec = _spec()
    assert spec.return_ids() == (ObjectID(spec.task_id, 0),)
    assert replace(spec, attempt_id=spec.attempt_id.next()).return_ids() == spec.return_ids()
    for count in (0, 2, -1, True, 1.0):
        with pytest.raises(ProtocolError, match="num_returns=1"):
            replace(spec, num_returns=count)


def test_task_manifest_rejects_empty_multiple_and_nonzero_outputs():
    spec = _spec()
    execution = (TaskExecution.from_task_spec(spec))
    assert (tuple(field.name for field in fields(execution))) == (("attempt_id",))
    assert execution == TaskExecution(spec.attempt_id)
    assert execution.task_id == spec.task_id
    assert execution.for_attempt(spec.attempt_id.next()).object_id == spec.return_ids()[0]
    assert not hasattr(task_outputs, "TaskOutputManifest")
    assert not hasattr(task_outputs, "TaskExecutionKey")
    for name in (("manifest"), ("output_ids"),
                    ("num_returns")):
        assert not hasattr(execution, name)
    class EqualTaskID:
        def __eq__(self, other):
            return True

    invalid_task = SimpleNamespace(
        task_id=EqualTaskID(), attempt_id=spec.attempt_id,
        return_ids=spec.return_ids,
    )
    with pytest.raises(TypeError, match="task_spec.task_id"):
        TaskExecution.from_task_spec(invalid_task)
    for outputs in ((), (ObjectID(spec.task_id, 1),),
                    (ObjectID(spec.task_id, 0), ObjectID(spec.task_id, 1))):
        invalid = SimpleNamespace(
            task_id=spec.task_id, attempt_id=spec.attempt_id,
            return_ids=lambda: outputs,
        )
        with pytest.raises(ValueError, match="canonical return index zero"):
            TaskExecution.from_task_spec(invalid)


def test_publication_revalidates_a_mutated_multislot_execution():
    header = _header()
    execution = header.publication_id.execution
    object.__setattr__((execution.attempt_id), ("attempt_number"), (-1))
    with pytest.raises(ValueError, match=("attempt_number")):
        OutputPublicationID(header.publication_id.lease_id, execution)


def test_success_envelope_requires_its_single_output_and_exact_complete():
    header = _header()
    discovery = OutputDiscoverySession(header, inline_threshold=10000)
    output = discovery.discover((("left", "right")))
    envelope = _envelope(output.manifest, (output.payload))
    reply = protocol.TaskReply(
        header.publication_id.task_id, header.publication_id.attempt_id,
        header.executor_worker_id, protocol.TaskReplyStatus.SUCCEEDED,
        ((envelope.result,)), output_publication=envelope,
    )
    assert cloudpickle.loads(reply.results[0].inline_data) == ("left", "right")
    assert reply.output_publication.complete == envelope.complete
    for values in ((), ((output.manifest.value,)),
                  ((output.manifest.value,) * 2)):
        with (pytest.raises(TypeError, match="OutputValue")):
            OutputPublicationManifest.create(header, values)
    with pytest.raises(TypeError, match="unexpected keyword argument.*object_id"):
        replace(output.manifest.value, object_id=ObjectID(reply.task_id, 1))
    with pytest.raises(OutputPublicationConflictError, match="descriptor"):
        replace(envelope, result=replace(envelope.result, object_id=ObjectID(reply.task_id, 1)))
    for results in ((), reply.results * 2):
        with pytest.raises(ProtocolError, match="single return"):
            replace(reply, results=results, output_publication=None)
    changed_complete = replace(envelope.complete, manifest_digest="0" * 64)
    with pytest.raises(OutputPublicationConflictError, match="exact publication"):
        replace(envelope, complete=changed_complete)
    discovery.release_sources_after_promotions()


def test_lease_probe_stays_empty_but_execution_requires_index_zero():
    spec = _spec()
    node = NodeID(b"n" * 16)
    lease = protocol.RequestWorkerLease(
        LeaseID(b"l" * 16), spec.task_id, spec.attempt_id, spec.resources,
        node, spec.owner_worker_id, target_node_id=node,
    )
    assert protocol.revalidate_worker_lease_request(lease).return_ids == ()
    execution = replace(lease, return_ids=spec.return_ids())
    assert protocol.revalidate_worker_lease_request(execution).target_node_id == node
    for outputs in ((ObjectID(spec.task_id, 1),), spec.return_ids() * 2):
        with pytest.raises(ProtocolError, match="single task return"):
            replace(lease, return_ids=outputs)


def test_wire_no_longer_exposes_targeted_execution_identity():
    for wire_type in (
        protocol.RequestWorkerLease, protocol.GrantWorkerLease,
        protocol.SpillbackWorkerLease, protocol.RejectWorkerLease,
        protocol.StartWorkerLease, protocol.StartWorkerLeaseReply, protocol.PushTask,
        protocol.CompleteWorkerLease, protocol.CompleteWorkerLeaseReply,
        protocol.GetWorkerLeaseOutcome, protocol.GetWorkerLeaseOutcomeReply, protocol.TaskReply,
    ):
        assert "target_execution" not in inspect.signature(wire_type).parameters
    assert "target_node_id" in inspect.signature(protocol.RequestWorkerLease).parameters


def test_automatic_stored_arguments_and_inactive_alias_modules_are_retired():
    assert not hasattr(protocol, "StoredArg")
    assert importlib.util.find_spec("miniray.actor_arguments") is None
    assert importlib.util.find_spec("miniray.stored_publication") is None
    for decoder in (dependency.decode_task_argument, dependency.decode_task_arguments,
                    dependency.resolve_task_arguments):
        assert "materialize_stored" not in inspect.signature(decoder).parameters


def test_explicit_refarg_keeps_exact_stored_materialization_and_bytes_value(monkeypatch):
    spec = _spec()
    object_id = ObjectID(TaskID(b"d" * 16), 0)
    argument = protocol.RefArg(object_id, spec.owner_worker_id)
    node_id = NodeID(b"n" * 16)
    attempt = AttemptID(object_id.task_id, 0)
    value = b"user bytes are a decoded value"
    payload = cloudpickle.dumps(value)
    checksum = hashlib.sha256(payload).hexdigest()
    descriptor = protocol.ObjectStoreDescriptor(
        object_id, argument.owner_worker_id, attempt, node_id, len(payload), checksum,
    )
    push = protocol.PushTask(LeaseID(b"l" * 16), WorkerID(b"w" * 16),
                             replace(spec, args=(argument,)), (descriptor,))
    observed = []

    def get_object(address, handler, request):
        assert address == ("127.0.0.1", 31002) and handler == worker.GET_OBJECT_HANDLER
        assert request == protocol.GetObject(
            object_id, node_id, expected_attempt_id=attempt,
            expected_owner_worker_id=argument.owner_worker_id,
            expected_size_bytes=len(payload), expected_checksum=checksum,
        )
        observed.append(request)
        return protocol.GetObjectReply(
            object_id=object_id, node_id=node_id, found=True, sealed=True,
            data=payload, checksum=checksum, producer_attempt_id=attempt,
            owner_worker_id=argument.owner_worker_id, size_bytes=len(payload),
        )

    monkeypatch.setattr(worker, "rpc_request", get_object)
    decoded = worker._decode_argument(push.spec.args[0], dependencies={object_id: descriptor},
                                      node_id=node_id, node_address=("127.0.0.1", 31002))
    assert decoded == value and type(decoded) is bytes
    assert len(observed) == 1
    assert dependency.decode_task_argument(argument, materialize_ref=lambda obj, owner: decoded) == value
    assert dependency.top_level_dependencies((argument,)) == (object_id,)
    with pytest.raises(RuntimeError, match="not local"):
        worker._decode_argument(argument, dependencies={object_id: descriptor},
                                node_id=NodeID(b"x" * 16), node_address=("127.0.0.1", 31002))
    assert len(observed) == 1


@pytest.mark.parametrize("value", [("left", "right"), ["left", "right"]], ids=["tuple", "list"])
def test_worker_executes_and_serializes_sequence_as_one_result(monkeypatch, value):
    # Replace only the transport boundary: run actual admission, decoding,
    # discovery, completion validation, caching, and exact PushTask replay.
    class UnstartedTransport:
        address = ("127.0.0.1", 31001)

        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(worker, "TCPServer", UnstartedTransport)
    spec = _spec(value)
    header = _header(spec)
    phases = []
    prepared = []

    def node_request(address, handler, request, **kwargs):
        phases.append(handler)
        if handler == worker.START_WORKER_LEASE_HANDLER:
            return protocol.StartWorkerLeaseReply(
                request.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                node_incarnation=header.node_incarnation,
            )
        if handler == worker.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            assert type(request) is PrepareOutputPublication
            prepared.append(request)
            return PreparedOutputPublicationReply(request.request_identity, True)
        assert handler == worker.COMPLETE_WORKER_LEASE_HANDLER
        publication = prepared[0]
        return protocol.CompleteWorkerLeaseReply(
            request.lease_id, request.task_id, request.attempt_id, request.worker_id,
            request.status, protocol.LeaseExecutionState.COMPLETED, True, True,
            output_publication=_envelope(publication.manifest, (publication.payload)),
        )

    monkeypatch.setattr(worker, "rpc_request", node_request)
    server = worker.WorkerServer(
        header.executor_worker_id, node_id=header.node_incarnation.node_id,
        node_address=("127.0.0.1", 31002),
    )
    server._worker_core_enabled = False
    push = protocol.PushTask(header.publication_id.lease_id, server.worker_id, spec)
    reply = server._handle_push_task(push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert (len(reply.results) == 1)
    assert type(prepared[0].payload) is bytes
    restored = cloudpickle.loads(reply.results[0].inline_data)
    assert type(restored) is type(value) and restored == value
    assert server._handle_push_task(push) == reply
    assert phases == [worker.START_WORKER_LEASE_HANDLER,
                      worker.PREPARE_OUTPUT_PUBLICATION_HANDLER, worker.COMPLETE_WORKER_LEASE_HANDLER]
    assert server._embedded_core is None
    assert not server._prepared_output_replies


def test_placement_group_wire_accepts_only_the_two_strict_strategies():
    bundles = tuple(protocol.PlacementGroupBundle(i, ResourceVector.of(CPU=1)) for i in range(2))
    for strategy in ("STRICT_PACK", "STRICT_SPREAD"):
        request = protocol.CreatePlacementGroupRequest(PlacementGroupID(b"p" * 16), 0, bundles, strategy)
        assert request.bundles == bundles
    for strategy in ("PACK", "SPREAD"):
        with pytest.raises(ProtocolError, match="STRICT_PACK or STRICT_SPREAD"):
            protocol.CreatePlacementGroupRequest(PlacementGroupID(b"p" * 16), 0, bundles, strategy)


def test_placement_group_wire_rejects_more_than_two_bundles():
    bundles = tuple(protocol.PlacementGroupBundle(i, ResourceVector.of(CPU=1)) for i in range(3))
    with pytest.raises(ProtocolError, match="at most two bundles"):
        protocol.CreatePlacementGroupRequest(PlacementGroupID(b"p" * 16), 0, bundles, "STRICT_PACK")
