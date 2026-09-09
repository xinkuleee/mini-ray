"""Revalidate exact membership death records at effect boundaries.

These functions preserve an already committed fact. They never infer death
from a failed RPC and do not coordinate output publication.
"""
from .ids import NodeID, WorkerID
from .output_publication import _opaque, _require_type, _string, _uint
from .protocol import NodeDeathRecord, NodeDeathReason, WorkerDeathRecord, WorkerDeathReason, WorkerIncarnation


def node_death(value):
    _require_type(value, NodeDeathRecord, "node death")
    _require_type(value.reason, NodeDeathReason, "node death reason")
    _uint(value.node_pid, "node_pid", positive=True)
    _uint(value.registration_epoch, "registration_epoch", positive=True)
    _uint(value.death_epoch, "node death_epoch", positive=True)
    _require_type(value.exit_code, int, "node exit_code")
    return NodeDeathRecord(
        _string(value.detection_id, "node detection_id"),
        _opaque(value.node_id, NodeID, "node_id"), value.node_pid,
        value.registration_epoch, value.death_epoch, value.exit_code, value.reason,
        _string(value.detail, "node death detail"),
    )


def owner_death(value):
    _require_type(value, WorkerDeathRecord, "owner death")
    _require_type(value.incarnation, WorkerIncarnation, "owner incarnation")
    incarnation = value.incarnation
    _uint(incarnation.node_pid, "owner node_pid", positive=True)
    _uint(incarnation.node_registration_epoch, "owner node epoch", positive=True)
    _uint(incarnation.worker_pid, "owner worker_pid", positive=True)
    _uint(value.death_epoch, "owner death_epoch", positive=True)
    _require_type(value.exit_code, int, "owner exit_code")
    _require_type(value.reason, WorkerDeathReason, "owner death reason")
    return WorkerDeathRecord(
        _string(value.detection_id, "owner detection_id"),
        WorkerIncarnation(
            _opaque(incarnation.node_id, NodeID, "owner node_id"),
            incarnation.node_pid, incarnation.node_registration_epoch,
            _opaque(incarnation.worker_id, WorkerID, "owner worker_id"), incarnation.worker_pid,
        ), value.death_epoch, value.exit_code, value.reason,
    )
