"""Finite spawned-Node checkpoint after real preparation, before Complete.

The historical base ARM interval is now preparation-complete: base has no
central ARM authority. Enhanced additionally checks its actual ARMED receipt.
No reply is fabricated and no Node/journal/resource state is mutated. The
original preparation handler runs first; its successful reply is withheld for
at most ten seconds. One metadata frame and one connection are shared by any
replayed preparation requests. The parent crashes this exact managed Node.
"""
from __future__ import annotations

from dataclasses import replace
import socket
import threading
import time

from miniray import api as api_module, node as node_module, output_protocol as wire, protocol
from miniray.output_publication_journal import OutputPublicationJournalState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateArrival,
    OutputPublicationGatePhase,
)
from miniray.resources import AllocationState

_ACTUAL_NODE_PROCESS_MAIN = api_module._node_process_main
PREPARED_FRAME_PREFIX = b"MRPRC001"
_SECONDS = 10.0


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("prepared-before-Complete fixture exceeded its deadline")
    return remaining


class _PreparedBeforeComplete:
    def __init__(self, address):
        self.address = address
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.arrival = self.deadline = self.error = None

    def checkpoint(self, node, request, reply):
        assert type(reply) is wire.PreparedOutputPublicationReply
        assert reply.request_identity == request.request_identity and reply.accepted
        manifest = replace(request.manifest)
        identity = manifest.publication_id

        def validate():
            journal = node._output_publication_journal
            with journal.linearize(identity), node._state_lock:
                snapshot = journal.snapshot(identity)
                record = node._output_lease_record_locked(manifest)
                assert snapshot.manifest == manifest
                assert snapshot.state is OutputPublicationJournalState.ACTIVE
                assert snapshot.ready_to_complete and snapshot.complete is None
                assert snapshot.rollback is None and snapshot.rollback_tombstone is None
                assert record.output_publication_id == identity
                assert record.state is protocol.LeaseExecutionState.RUNNING
                assert record.completion is None and record.output_complete_inflight is None
                allocation = node._ledger.record(record.allocation_token)
                assert allocation is not None and allocation.state is AllocationState.ACTIVE
                assert node._workers[manifest.header.executor_worker_id].active_lease_id == identity.lease_id
            return OutputPublicationGateArrival.from_manifest(
                manifest, OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK,
            )

        arrival = validate()
        with self.lock:
            leader = self.arrival is None
            if leader:
                self.arrival = arrival
                self.deadline = time.monotonic() + _SECONDS
            else:
                assert self.arrival == arrival, "prepared replay changed publication or incarnation"
            deadline = self.deadline
        if not leader:
            if not self.done.wait(_remaining(deadline)):
                raise TimeoutError("prepared checkpoint leader did not finish")
            if self.error is not None:
                raise RuntimeError(self.error)
            return
        try:
            assert validate() == arrival
            with socket.create_connection(self.address, timeout=_remaining(deadline)) as connection:
                connection.settimeout(_remaining(deadline))
                connection.sendall(PREPARED_FRAME_PREFIX + arrival.to_bytes())
                connection.settimeout(_remaining(deadline))
                if connection.recv(1) != OUTPUT_PUBLICATION_GATE_RELEASE:
                    raise RuntimeError("prepared checkpoint closed without release")
            assert validate() == arrival
        except BaseException as exc:
            self.error = str(exc) or type(exc).__name__
            raise
        finally:
            self.done.set()


def node_process_with_prepared_checkpoint(address, survivor_resource, *args):
    if args[1].get(survivor_resource, 0):
        return _ACTUAL_NODE_PROCESS_MAIN(*args)
    node_type = node_module.NodeServer
    original = node_type._handle_prepare_output_publication
    gate = _PreparedBeforeComplete(address)

    def prepare(node, request):
        reply = original(node, request)
        if type(reply) is wire.PreparedOutputPublicationReply and reply.accepted:
            gate.checkpoint(node, request, reply)
        return reply

    node_type._handle_prepare_output_publication = prepare
    try:
        _ACTUAL_NODE_PROCESS_MAIN(*args)
    finally:
        node_type._handle_prepare_output_publication = original
