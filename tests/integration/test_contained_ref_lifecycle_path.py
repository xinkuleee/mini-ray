"""Two real foreign borrowers outlive their collected INLINE container.

The original parent, running on the first ordinary Worker, submits one child
and returns its ObjectRef.  Two separate Driver deserializations acquire two
different tokens at that Worker owner.  Before either child read, public
outer.close plus actual collection must release the container's exact hold.
After the first borrower release ACK, the second borrower remains readable.

Bounds: four children (one GCS, one Node, two Workers), two CPUs, one 1 MiB
store, two tiny tasks, three public handles, no Actors, faults, test threads,
or test server.  The Driver owner service adds the fifth tracked endpoint.
Public gets and early closes share one 15-second work deadline; each early
close also has a three-second cap.  Final second.close, release convergence,
and finally's handle/release pass reuse one three-second cleanup epoch.
Condition observations are capped at 128 per wait; the passive lifecycle
observer retains at most 16 real RPC exchanges and adds no calls.  Runtime
shutdown retains its own production bounds.  Nested owner RPCs retain their original
three-attempt policy; a caller timeout does not cancel those RPCs.  The exact
30-second external runner remains the whole-process bound.

Evidence stops at outer metadata COLLECTED, exact remote contained/borrower
release ACKs, and clean shutdown.  A public close receipt is only local intent;
the final foreign child's owner-metadata GC is not independently observed.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.ownership import ObjectCollectionState, ObjectState


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 16
_MAX_POLLS = 128


@ray.remote(num_cpus=1)
def contained_child(value: int) -> tuple[str, int, int]:
    return "contained-inline", value + 1, os.getpid()


@ray.remote(num_cpus=1)
def return_contained_child_ref(value: int) -> object:
    return contained_child.remote(value)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remaining_current(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("contained borrower work or cleanup deadline expired")
    return remaining


def _wait_current(core, predicate, deadline):
    # Predicates only inspect local state; never hold this lock across RPC/close.
    with core._completion:
        for index in range(_MAX_POLLS):
            if predicate():
                return
            if index + 1 == _MAX_POLLS:
                break
            core._completion.wait(min(0.1, _remaining_current(deadline)))
    raise TimeoutError("contained borrower owner transition did not converge")


def _close_current(reference, deadline):
    # Retain the original receipt even if a synchronous close clears a binding.
    # Repeated close must check that receipt too; closed alone is not completion.
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _borrow_key_current(reference, core):
    return (
        reference.owner_worker_id, reference.object_id,
        core.worker_id, reference.borrower_token,
    )


def test_two_borrowers_outlive_their_inline_container() -> None:
    context = report = runtime = core = None
    original_borrow = None
    outer = None
    first = None
    second = None
    cleanup_deadline = None
    cleanup_errors = []
    observations = []
    observation_lock = threading.Lock()
    observation_failed = overflow = False
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    expected_worker_pids: tuple[int, ...] = ()

    def inspect_borrow(address, handler, request):
        nonlocal observation_failed, overflow
        reply = original_borrow(address, handler, request)
        try:
            if handler in ("acquire_borrowed_object",
                           "release_contained_reference",
                           "release_borrowed_object"):
                with observation_lock:
                    if len(observations) < _MAX_OBSERVATIONS:
                        observations.append((address, handler, request, reply))
                    else:
                        overflow = True
        except Exception:
            observation_failed = True
        # No observer assertions, replacement ACKs, or changed RPC arguments.
        return reply

    def current_exchanges():
        with observation_lock:
            exchanges = tuple(observations)
            failed, exceeded = observation_failed, overflow
        assert not failed and not exceeded
        return exchanges

    def accepted_borrower_tokens():
        tokens = set()
        for address, handler, request, reply in current_exchanges():
            if handler != "release_borrowed_object":
                continue
            assert isinstance(request, protocol.ReleaseBorrowedObject)
            assert isinstance(reply, protocol.ReleaseBorrowedObjectReply)
            assert address == first.owner_address
            assert request.object_id == reply.object_id == first.object_id
            assert request.owner_worker_id == reply.owner_worker_id == first.owner_worker_id
            assert request.borrower_worker_id == reply.borrower_worker_id == core.worker_id
            assert request.borrower_token == reply.borrower_token
            assert request.borrower_token in (first.borrower_token, second.borrower_token)
            assert reply.accepted and reply.error is None
            tokens.add(request.borrower_token)
        return tokens

    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=2,
            num_workers_per_node=2,
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        # Register every startup identity before any main-path assertion.
        managed_pids.update(
            (context.gcs_pid, *context.node_pids, *context.worker_pids)
        )
        managed_addresses.update(
            (context.gcs_address, *context.node_addresses, *context.worker_addresses)
        )
        runtime = _get_runtime()
        core = runtime.core_worker
        if runtime.owner_service is not None:
            managed_addresses.add(runtime.owner_service.address)
        node = context.nodes[0]
        expected_worker_pids = context.worker_pids
        assert runtime.owner_service is not None
        assert context.trace_address is None
        assert len(context.nodes) == 1
        assert len(node.worker_ids) == len(node.worker_pids) == 2
        assert len(node.worker_addresses) == 2
        assert len(managed_pids) == 4
        assert len(managed_addresses) == 5
        assert os.getpid() not in managed_pids
        original_borrow = core._borrow_rpc
        core._borrow_rpc = inspect_borrow

        _remaining_current(deadline)
        outer = return_contained_child_ref.remote(41)
        first = ray.get(outer, timeout=_remaining_current(deadline))
        second = ray.get(outer, timeout=_remaining_current(deadline))

        assert isinstance(outer, ray.ObjectRef)
        assert isinstance(first, ray.ObjectRef)
        assert isinstance(second, ray.ObjectRef)
        assert first is not second
        assert first.object_id == second.object_id
        assert first.owner_worker_id == second.owner_worker_id == node.worker_id
        assert first.owner_address == second.owner_address == node.worker_address
        assert first.borrower_token is not None
        assert second.borrower_token is not None
        assert first.borrower_token != second.borrower_token
        assert first.owner_worker_id != core.worker_id
        assert not core.owner_table.contains(first.object_id)
        first_key = _borrow_key_current(first, core)
        second_key = _borrow_key_current(second, core)
        assert first_key != second_key

        outer_before = core.owner_table.snapshot(outer.object_id)
        assert outer_before.state is ObjectState.READY_INLINE
        membership = outer_before.output_publication
        assert membership is not None and membership.slot.object_id == outer.object_id
        assert membership.slot.tier is protocol.ResultStorage.INLINE
        transfer, = membership.slot.transfers
        hold = transfer.final_hold
        assert transfer.contained_object_id == first.object_id
        assert transfer.contained_owner_worker_id == first.owner_worker_id
        assert transfer.contained_owner_address == first.owner_address
        assert hold.container_object_id == outer.object_id
        assert hold.container_owner_worker_id == outer.owner_worker_id == core.worker_id
        acquires = tuple(exchange for exchange in current_exchanges()
                         if exchange[1] == "acquire_borrowed_object")
        assert len(acquires) == 2
        acquired_tokens = set()
        for address, handler, request, reply in acquires:
            assert isinstance(request, protocol.AcquireBorrowedObject)
            assert isinstance(reply, protocol.AcquireBorrowedObjectReply)
            assert address == first.owner_address
            assert request.object_id == reply.object_id == first.object_id
            assert request.owner_worker_id == reply.owner_worker_id == first.owner_worker_id
            assert request.borrower_worker_id == reply.borrower_worker_id == core.worker_id
            assert request.borrower_token == reply.borrower_token
            assert isinstance(request.source, protocol.ContainedTransferSource)
            assert request.source == reply.source == first.borrow_source == second.borrow_source
            assert request.source.hold == hold
            assert reply.accepted and reply.acquired and reply.error is None
            acquired_tokens.add(request.borrower_token)
        assert acquired_tokens == {first.borrower_token, second.borrower_token}

        # Public close is local intent; observe actual outer GC and its exact
        # remote ReleaseContainedReference ACK before resolving either borrower.
        _remaining_current(deadline)
        _close_current(outer, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _wait_current(
            core,
            lambda: core.owner_table.collection_state(outer.object_id)
            is ObjectCollectionState.COLLECTED,
            deadline,
        )
        contained_releases = tuple(exchange for exchange in current_exchanges()
                                   if exchange[1] == "release_contained_reference")
        assert contained_releases
        for address, handler, request, reply in contained_releases:
            assert isinstance(request, protocol.ReleaseContainedReference)
            assert isinstance(reply, protocol.ReleaseContainedReferenceReply)
            assert address == first.owner_address
            assert request.object_id == reply.object_id == first.object_id
            assert request.owner_worker_id == reply.owner_worker_id == first.owner_worker_id
            assert request.hold == reply.hold == hold
            assert reply.accepted and reply.error is None
        expected = ("contained-inline", 42)
        first_value = ray.get(first, timeout=_remaining_current(deadline))
        second_value = ray.get(second, timeout=_remaining_current(deadline))
        assert first_value[:2] == expected
        assert second_value[:2] == expected
        assert first_value == second_value
        assert first_value[2] in node.worker_pids

        _remaining_current(deadline)
        _close_current(first, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _wait_current(core, lambda: first_key not in core._borrowed_release_obligations, deadline)
        assert accepted_borrower_tokens() == {first.borrower_token}
        with core._completion:
            remaining_borrower = core._borrowed_release_obligations[second_key]
            assert not remaining_borrower.release_requested
        # A real first-token ACK, not just its local close, precedes this get.
        assert ray.get(second, timeout=_remaining_current(deadline)) == second_value
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        _close_current(second, cleanup_deadline)
        _wait_current(
            core,
            lambda: first_key not in core._borrowed_release_obligations
            and second_key not in core._borrowed_release_obligations,
            cleanup_deadline,
        )
        assert accepted_borrower_tokens() == acquired_tokens
        # Owner death could also discharge an obligation; no such fence applies.
        assert core.owner_table.dead_worker_record(first.owner_worker_id) is None
        # Remote token release is proven, but its best-effort child GC is not
        # independently observable through the existing protocol.
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        cleanup_keys = []
        try:
            for label, reference in (("first", first), ("second", second), ("outer", outer)):
                if not isinstance(reference, ray.ObjectRef):
                    continue
                if core is not None and reference.borrower_token is not None:
                    cleanup_keys.append(_borrow_key_current(reference, core))
                try:
                    _close_current(reference, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append((label + " close", repr(exc)))
            if core is not None and cleanup_keys:
                try:
                    _wait_current(
                        core,
                        lambda: all(key not in core._borrowed_release_obligations
                                    for key in cleanup_keys),
                        cleanup_deadline,
                    )
                except Exception as exc:
                    cleanup_errors.append(("remote release convergence", repr(exc)))
        finally:
            try:
                report = ray.shutdown()
            except Exception as exc:
                cleanup_errors.append(("shutdown", repr(exc)))
            finally:
                if core is not None and original_borrow is not None:
                    core._borrow_rpc = original_borrow

        # Gather every hygiene observation before reporting any cleanup error.
        if report is not None:
            managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
        surviving_pids = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
        active_pids = tuple(child.pid for child in mp.active_children()
                            if child.pid in managed_pids)
        open_addresses = []
        for address in sorted(managed_addresses):
            try:
                with socket.create_connection(address, timeout=0.1):
                    open_addresses.append(address)
            except OSError:
                pass
        checks = [
            (not ray.is_initialized(), "runtime remains initialized"),
            (not surviving_pids, ("surviving PIDs", surviving_pids)),
            (not active_pids, ("active children", active_pids)),
            (not open_addresses, ("open endpoints", open_addresses)),
            (not observation_failed and not overflow, "lifecycle observer failed or overflowed"),
        ]
        if context is not None:
            checks.append((report is not None, "missing shutdown report"))
        if report is not None and context is not None:
            checks.extend((
                (report.core_stopped, "Core did not stop"),
                (report.gcs_pid == context.gcs_pid, "GCS identity changed"),
                (tuple(report.node_pids) == context.node_pids, "Node identities changed"),
                (tuple(report.worker_pids) == expected_worker_pids, "Worker identities changed"),
                (report.gcs_clean and report.gcs_exitcode == 0, "unclean GCS exit"),
                (report.node_clean and report.worker_clean, "unclean Node/Worker exit"),
                (report.resources_clean and report.finalized, "resources not finalized"),
                (report.shutdown_ack_clean and not report.forced, "forced or unacknowledged exit"),
                (tuple(report.node_exitcodes) == (0,), "unexpected Node exitcodes"),
                (tuple(report.worker_exitcodes) == (0, 0), "unexpected Worker exitcodes"),
                (tuple(report.worker_cleans) == (True, True), "unexpected Worker clean flags"),
                (tuple(report.worker_forced) == (False, False), "unexpected Worker forced flags"),
            ))
        cleanup_errors.extend(message for passed, message in checks if not passed)
        assert not cleanup_errors, cleanup_errors

    assert context is not None and report is not None
