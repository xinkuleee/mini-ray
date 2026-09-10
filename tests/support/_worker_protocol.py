"""Explicit unstarted Worker protocol state; no server/Core/process is constructed."""

import threading


def initialize_worker_protocol(worker):
    """Initialize a newly allocated test Worker, never reconstruct accepted work."""
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._active_tasks = 0
    worker._accepting_tasks = True
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._prepared_output_replies = {}
    worker._cached_output_manifests = {}
    worker._owner_abandoned_outputs = {}
    worker._execution_lock = threading.Lock()
    worker._embedded_core_lock = threading.Lock()
    worker._embedded_core_drain_lock = threading.Lock()
    worker._embedded_core = None
    worker._embedded_core_job_id = None
    worker._embedded_core_stopped = False
    worker._owner_retain_admission_open = True
    worker._drain_request_id = None
    worker._drain_clean = False
    worker._finalize_exit_scheduled = False
    worker._stop_event = threading.Event()
    worker._worker_core_enabled = False


def complete_boundary(peer, request):
    """Real prepared success; explicit terminal-error transport reply only."""
    from miniray import protocol
    if request.status is protocol.TaskReplyStatus.SUCCEEDED:
        return peer.complete(request)
    return protocol.CompleteWorkerLeaseReply(request.lease_id, request.task_id,
        request.attempt_id, request.worker_id, request.status,
        protocol.LeaseExecutionState.COMPLETED, True, True,
        scheduling_key=request.scheduling_key)
