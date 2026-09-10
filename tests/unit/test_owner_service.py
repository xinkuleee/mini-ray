"""Pure contracts for the Driver owner TCP adapter."""

from __future__ import annotations

import pytest

from miniray import output_protocol as wire, owner_service, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import (
    JobID, ObjectID, TaskID, WorkerID,
)
from miniray.ownership import StoredContainedReferenceDisposition
from miniray.publication_sources import (
    OwnedContainedSource, PreparedContainedTransfer,
)


pytestmark = pytest.mark.unit


class _Core:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def _call(self, name: str, request: object) -> object:
        self.calls.append((name, request))
        return name, request

    def acquire_exported_reference(self, request):
        return self._call("acquire", request)

    def release_borrowed_reference(self, request):
        return self._call("release", request)

    def get_owned_object(self, request):
        return self._call("get", request)

    def request_owned_object_reconstruction(self, request):
        return self._call("reconstruct", request)

    def request_drop_owned_object(self, request):
        return self._call("drop_owned", request)

    def retain_owned_object_for_task(self, request):
        return self._call("retain", request)

    def get_retained_owned_object(self, request):
        return self._call("get_retained", request)

    def report_retained_object_location(self, request):
        return self._call("report_location", request)

    def release_owned_object_for_task(self, request):
        return self._call("release_retained", request)

    def replace_retained_object_for_task(self, request):
        return self._call("replace_retained", request)

    def release_contained_reference(self, request):
        return self._call("release_contained", request)

    def install_actor_state(self, request):
        return self._call("install_actor_state", request)

    def register_output_handoff(self, request):
        return self._call("register_handoff", request)

    def report_output_handoff_complete(self, request):
        return self._call("report_complete", request)

    def report_output_handoff_rollback(self, request):
        return self._call("report_rollback", request)

    def get_output_handoff(self, request):
        return self._call("get_handoff", request)

    def prepare_stored_contained_pin(self, request):
        return self._call("prepare_stored", request)

    def promote_stored_contained_pin(self, request):
        return self._call("promote_stored", request)

    def report_abandoned_dependency_replica(self, request):
        return self._call("abandoned", request)


class _Server:
    def __init__(self, handlers, **configuration) -> None:
        self.handlers = dict(handlers)
        self.configuration = configuration
        self.address = ("127.0.0.1", 29001)
        self.is_running = False
        self.stop_calls = 0

    def start(self):
        self.is_running = True
        return self.address

    def stop(self):
        self.is_running = False
        self.stop_calls += 1


def test_owner_service_exposes_only_live_core_handlers(monkeypatch) -> None:
    created = []

    def server(handlers, **configuration):
        result = _Server(handlers, **configuration)
        created.append(result)
        return result

    monkeypatch.setattr(owner_service, "TCPServer", server)
    core = _Core()
    service = owner_service.OwnerService(core)
    implementation = created[0]

    expected = {
        owner_service.ACQUIRE_BORROWED_OBJECT_HANDLER: "acquire",
        owner_service.RELEASE_BORROWED_OBJECT_HANDLER: "release",
        owner_service.GET_OWNED_OBJECT_HANDLER: "get",
        owner_service.REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER:
            "reconstruct",
        owner_service.REQUEST_DROP_OWNED_OBJECT_HANDLER: "drop_owned",
        owner_service.RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER: "retain",
        owner_service.GET_RETAINED_OWNED_OBJECT_HANDLER: "get_retained",
        owner_service.REPORT_RETAINED_OBJECT_LOCATION_HANDLER: "report_location",
        owner_service.RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER: "release_retained",
        owner_service.REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER:
            "replace_retained",
        owner_service.RELEASE_CONTAINED_REFERENCE_HANDLER: "release_contained",
        owner_service.INSTALL_ACTOR_STATE_HANDLER: "install_actor_state",
        wire.REGISTER_OUTPUT_HANDOFF_HANDLER: "register_handoff",
        wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER: "report_complete",
        wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER: "report_rollback",
        wire.GET_OUTPUT_HANDOFF_HANDLER: "get_handoff",
        owner_service.PREPARE_STORED_CONTAINED_PIN_HANDLER: "prepare_stored",
        owner_service.PROMOTE_STORED_CONTAINED_PIN_HANDLER: "promote_stored",
    }
    assert set(implementation.handlers) == set(expected) | {protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER}
    expected_calls = []
    for handler, operation in expected.items():
        request = object()
        assert implementation.handlers[handler](request) == (operation, request)
        expected_calls.append((operation, request))
    assert core.calls == expected_calls
    assert service.address == implementation.address
    assert service.start() == implementation.address
    assert service.is_running
    service.stop()
    assert not service.is_running and implementation.stop_calls == 1


def _stored_transfer():
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 10)
    child_task = TaskID.derive(job, TaskID.for_driver(job), 11)
    outer = ObjectID.for_task(task)
    child = ObjectID.for_task(child_task)
    owner = WorkerID.random()
    return owner, PreparedContainedTransfer(
        child, owner, ("127.0.0.1", 29002), OwnedContainedSource(owner),
        ContainedReferenceHold(outer, WorkerID.random(), "stored:pin"),
        ContainedReferenceHold(outer, WorkerID.random(), "stored:pin"),
    )


def test_missing_required_owner_handler_fails_before_server_creation(monkeypatch):
    monkeypatch.setattr(owner_service, "TCPServer", lambda *_a, **_k: pytest.fail("incomplete owner created a server"))
    class MissingPins(_Core):
        prepare_stored_contained_pin = None
    with pytest.raises(TypeError, match="complete current owner"):
        owner_service.OwnerService(MissingPins())


def test_required_stored_pin_handlers_echo_exact_requests_and_dispositions(
    monkeypatch,
) -> None:
    created = []
    monkeypatch.setattr(
        owner_service, "TCPServer",
        lambda handlers, **configuration: created.append(
            _Server(handlers, **configuration)
        ) or created[-1],
    )

    class CoreWithStoredPins(_Core):
        def prepare_stored_contained_pin(self, request):
            self.calls.append(("prepare_stored", request))
            return StoredContainedReferenceDisposition.PREPARED

        def promote_stored_contained_pin(self, request):
            self.calls.append(("promote_stored", request))
            return StoredContainedReferenceDisposition.PROMOTED

    core = CoreWithStoredPins()
    owner_service.OwnerService(core)
    handlers = created[0].handlers
    owner, transfer = _stored_transfer()
    prepare = protocol.PrepareStoredContainedPin(transfer, owner)
    promote = protocol.PromoteStoredContainedPin(transfer, owner)

    assert handlers[
        owner_service.PREPARE_STORED_CONTAINED_PIN_HANDLER
    ](prepare) is StoredContainedReferenceDisposition.PREPARED
    assert handlers[
        owner_service.PROMOTE_STORED_CONTAINED_PIN_HANDLER
    ](promote) is StoredContainedReferenceDisposition.PROMOTED
    assert core.calls == [
        ("prepare_stored", prepare),
        ("promote_stored", promote),
    ]


def test_retired_inline_pin_is_not_advertised_even_if_a_core_has_that_method(monkeypatch):
    created = []
    monkeypatch.setattr(owner_service, "TCPServer", lambda handlers, **configuration: created.append(
        _Server(handlers, **configuration)
    ) or created[-1])

    class StaleCore(_Core):
        def install_inline_contained_pin(self, _request):
            pytest.fail("retired inline facade was called")

    service = owner_service.OwnerService(StaleCore())
    assert "install_inline_contained_pin" not in created[0].handlers
    assert not hasattr(owner_service, "INSTALL_INLINE_CONTAINED_PIN_HANDLER")
    assert not service.is_running
