"""Pure wire contracts for bounded ordinary-Worker diagnostics."""

from __future__ import annotations

import pickle

import pytest

from miniray import protocol
from miniray.errors import ProtocolError
from miniray.ids import NodeID, WorkerID


pytestmark = pytest.mark.unit


def _startup() -> protocol.NodeStartup:
    return protocol.NodeStartup(
        node_id=NodeID.random(),
        node_pid=4000,
        node_address=("127.0.0.1", 19000),
        worker_ids=(WorkerID.random(), WorkerID.random()),
        worker_pids=(4101, 4102),
        worker_addresses=(("127.0.0.1", 19101), ("127.0.0.1", 19102)),
    )


def test_node_startup_worker_tuples_are_authoritative_and_singular_is_first() -> None:
    startup = _startup()
    restored = pickle.loads(pickle.dumps(startup))

    assert restored == startup
    assert restored.worker_ids == startup.worker_ids
    assert restored.worker_pids == (4101, 4102)
    assert restored.worker_addresses == (
        ("127.0.0.1", 19101),
        ("127.0.0.1", 19102),
    )
    assert restored.worker_id == restored.worker_ids[0]
    assert restored.worker_pid == restored.worker_pids[0]
    assert restored.worker_address == restored.worker_addresses[0]


def test_node_startup_rejects_empty_oversized_misaligned_or_duplicate_pool() -> None:
    startup = _startup()
    common = dict(
        node_id=startup.node_id,
        node_pid=startup.node_pid,
        node_address=startup.node_address,
    )
    with pytest.raises(ProtocolError, match="one or two"):
        protocol.NodeStartup(**common, worker_ids=(), worker_pids=(), worker_addresses=())
    with pytest.raises(ProtocolError, match="one or two"):
        protocol.NodeStartup(
            **common,
            worker_ids=startup.worker_ids + (WorkerID.random(),),
            worker_pids=(4101, 4102, 4103),
            worker_addresses=startup.worker_addresses
            + (("127.0.0.1", 19103),),
        )
    with pytest.raises(ProtocolError, match="align"):
        protocol.NodeStartup(
            **common,
            worker_ids=startup.worker_ids,
            worker_pids=(4101,),
            worker_addresses=startup.worker_addresses,
        )
    with pytest.raises(ProtocolError, match="unique"):
        protocol.NodeStartup(
            **common,
            worker_ids=(startup.worker_ids[0], startup.worker_ids[0]),
            worker_pids=startup.worker_pids,
            worker_addresses=startup.worker_addresses,
        )


def test_shutdown_ack_preserves_ordered_child_diagnostics_over_pickle() -> None:
    ack = protocol.ShutdownAck(
        request_id="shutdown-1",
        component="node:test",
        clean=False,
        # Aggregate normalization must notice a forced non-first child even
        # when an older sender leaves the summary flag false.
        forced=False,
        resources_clean=True,
        child_pids=(4101, 4102),
        child_exitcodes=(0, -9),
        child_cleans=(True, False),
        child_forced=(False, True),
    )
    restored = pickle.loads(pickle.dumps(ack))

    assert restored == ack
    assert restored.child_pids == (4101, 4102)
    assert restored.child_exitcodes == (0, -9)
    assert restored.child_cleans == (True, False)
    assert restored.child_forced == (False, True)
    assert restored.child_pid == 4101
    assert restored.child_exitcode == 0
    assert restored.child_clean
    assert not restored.clean
    assert restored.forced
    assert not restored.children_clean


def test_shutdown_ack_rejects_misalignment_and_singular_reordering() -> None:
    with pytest.raises(ProtocolError, match="align"):
        protocol.ShutdownAck(
            "shutdown-1",
            "node:test",
            False,
            child_pids=(4101, 4102),
            child_exitcodes=(0,),
            child_cleans=(True, False),
            child_forced=(False, True),
        )
    with pytest.raises(ProtocolError, match="first child"):
        protocol.ShutdownAck(
            "shutdown-1",
            "node:test",
            False,
            child_pid=4101,
            child_pids=(4102, 4101),
            child_exitcodes=(0, 0),
            child_cleans=(True, True),
            child_forced=(False, False),
        )
    with pytest.raises(ProtocolError, match="unique"):
        protocol.ShutdownAck(
            "shutdown-1",
            "node:test",
            False,
            child_pids=(4101, 4101),
            child_exitcodes=(0, 0),
            child_cleans=(True, True),
            child_forced=(False, False),
        )


def test_shutdown_status_preserves_order_and_rejects_tuple_drift() -> None:
    status = protocol.ShutdownStatus(
        request_id="shutdown-1",
        component="node:test",
        shutdown_requested=True,
        child_pid=None,
        child_exitcode=None,
        child_clean=True,
        finalized=True,
        resources_clean=True,
        child_pids=(4101, 4102),
        child_exitcodes=(0, -9),
        child_cleans=(True, False),
    )
    restored = pickle.loads(pickle.dumps(status))
    assert restored == status
    assert restored.child_pids == (4101, 4102)
    assert restored.child_exitcodes == (0, -9)
    assert restored.child_cleans == (True, False)
    assert restored.child_pid == 4101
    assert restored.child_exitcode == 0
    assert restored.child_clean
    assert not restored.children_clean

    with pytest.raises(ProtocolError, match="align"):
        protocol.ShutdownStatus(
            "shutdown-1",
            "node:test",
            True,
            None,
            None,
            True,
            True,
            child_pids=(4101, 4102),
            child_exitcodes=(0,),
            child_cleans=(True, False),
        )
    with pytest.raises(ProtocolError, match="first child"):
        protocol.ShutdownStatus(
            "shutdown-1",
            "node:test",
            True,
            4101,
            0,
            True,
            True,
            child_pids=(4102, 4101),
            child_exitcodes=(0, 0),
            child_cleans=(True, True),
        )
    with pytest.raises(ProtocolError, match="unique"):
        protocol.ShutdownStatus(
            "shutdown-1",
            "node:test",
            True,
            None,
            None,
            True,
            True,
            child_pids=(4101, 4101),
            child_exitcodes=(0, 0),
            child_cleans=(True, True),
        )
