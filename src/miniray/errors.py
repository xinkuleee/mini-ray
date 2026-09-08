"""Exception hierarchy for mini-Ray.

The hierarchy intentionally separates errors caused by user code from failures in
the runtime.  A scheduler or retry policy can therefore make a decision without
matching error-message strings.
"""

from __future__ import annotations


class MiniRayError(Exception):
    """Base class for all public mini-Ray exceptions."""


class InvalidIDError(MiniRayError, ValueError):
    """An ID has an invalid representation or component."""


class ProtocolError(MiniRayError):
    """A protocol message is malformed or violates an invariant."""


class ResourceError(MiniRayError):
    """Base class for resource-accounting errors."""


class InvalidResourceError(ResourceError, ValueError):
    """A resource name or quantity cannot be represented."""


class InsufficientResourcesError(ResourceError):
    """A local ledger cannot satisfy an allocation."""


class AllocationTokenError(ResourceError):
    """An allocation token was reused inconsistently."""


class AllocationAlreadyReleasedError(AllocationTokenError):
    """A delayed acquire tried to resurrect a released token."""


class SchedulingError(MiniRayError):
    """Base class for scheduling failures."""


class InfeasibleTaskError(SchedulingError):
    """No live node has enough total resources for a task."""


class PendingCapacityError(SchedulingError):
    """A task is feasible, but every feasible node is currently busy."""


class LeaseRejectedError(SchedulingError):
    """A node rejected a worker-lease request."""


class FunctionRegistryError(MiniRayError):
    """Base class for function-registry failures."""


class FunctionNotRegisteredError(FunctionRegistryError, LookupError):
    """A task refers to a function unknown to the registry."""


class TaskError(MiniRayError):
    """A user function raised an exception.  This is not a system failure."""


class SystemTaskError(MiniRayError):
    """A task attempt failed because of a runtime or process failure."""


class WorkerDiedError(SystemTaskError):
    """The worker executing an attempt died."""


class NodeDiedError(SystemTaskError):
    """The node executing an attempt died."""


class ObjectStoreError(MiniRayError):
    """Base class for immutable-object-store failures."""


class ObjectNotFoundError(ObjectStoreError, LookupError):
    """The local store does not know the requested object."""


class ObjectNotSealedError(ObjectStoreError):
    """An object exists locally but is not atomically visible yet."""


class ObjectAlreadySealedError(ObjectStoreError):
    """An immutable object was sealed or overwritten a second time."""


class ObjectLostError(ObjectStoreError):
    """No physical replica of a logical object remains."""


class OwnerDiedError(ObjectStoreError):
    """The owner of an ObjectRef died; mini-Ray does not transfer ownership."""


class OwnerUnavailableError(ObjectStoreError):
    """The owner route is unavailable without an authoritative death fact."""


class BorrowedObjectUnavailableError(ObjectStoreError):
    """A foreign ObjectRef no longer has an active owner capability."""


class UnreconstructableObjectError(ObjectLostError):
    """An object has no replayable producer lineage (for example, ray.put)."""


class ActorError(MiniRayError):
    """Base class for actor lifecycle and invocation failures."""


class ActorDiedError(ActorError):
    """An actor exhausted its restart budget or was explicitly killed."""


class ActorUnavailableError(ActorError):
    """An actor's authoritative live route could not be reached."""


class StaleGenerationError(ActorError):
    """A message targets an actor incarnation that is no longer current."""


class PlacementGroupError(MiniRayError):
    """A placement-group plan or resource transaction failed."""


class PlacementGroupLostError(PlacementGroupError):
    """A committed participant death made an immutable PG attempt unusable."""


class RuntimeShuttingDownError(MiniRayError):
    """The destination is shutting down and accepts no new work."""
