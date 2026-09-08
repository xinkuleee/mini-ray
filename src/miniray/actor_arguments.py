"""Pure argument authority for restartable Actor invocations.

Actor arguments have two lifetimes which must not be conflated:

* an Actor constructor is immutable logical lineage.  Its Task-reference holds
  survive every Actor generation so a replacement Worker can resolve the same
  values again; and
* a method invocation belongs to one ``(generation, caller, sequence)`` key.
  Restart fences it instead of replaying it against fresh constructor state.

This module deliberately performs no RPC and imports no Actor runtime.  The
caller supplies owner-authoritative acquire acknowledgements and object-state
observations.  The reducer validates the complete identities, freezes one
prepared execution, and returns transport-neutral protocol values.

Top-level ``RefArg`` values are readiness dependencies.  Ready inline values
are substituted into the prepared arguments; ready stored values remain
``RefArg`` values and carry a byte-free ``ObjectStoreDescriptor``.  Nested
``ObjectRef`` values remain opaque handles and therefore create lifetime/import
work but never participate in the readiness gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Mapping, Optional, Tuple

from .actor_state import ActorCallKey
from .ids import (
    ActorGeneration, ActorID, AttemptID, ObjectID, TaskID, WorkerID,
)
from .protocol import (
    AcquireBorrowedObject, AcquireBorrowedObjectReply, GetOwnedObjectReply,
    InlineArg, NestedReferenceTransfer, ObjectStoreDescriptor, OwnedObjectState,
    RefArg, ReleaseBorrowedObject, StoredArg, TaskArg, TaskHoldSource,
    TaskReferenceHold,
)


ReferenceKey = Tuple[ObjectID, WorkerID]


class ActorArgumentError(RuntimeError):
    """Base class for Actor argument authority failures."""


class ActorArgumentConflictError(ActorArgumentError):
    """A stable invocation or execution identity changed its contents."""


class ActorArgumentFencedError(ActorArgumentError):
    """An operation targets a non-current Actor generation."""


class ActorArgumentsNotReady(ActorArgumentError):
    """At least one top-level dependency is still pending at its owner."""

    def __init__(self, missing: Tuple[ObjectID, ...]) -> None:
        self.missing = tuple(missing)
        super().__init__(
            "Actor arguments are waiting for {} top-level dependencies"
            .format(len(self.missing))
        )


class ActorArgumentDependencyError(ActorArgumentError):
    """An owner rejected a capability or reported a terminal dependency."""


class ActorArgumentScope(str, Enum):
    ACTOR_LIFETIME = "ACTOR_LIFETIME"
    METHOD_CALL = "METHOD_CALL"


def _reference_key(value: object) -> ReferenceKey:
    if not isinstance(value, (RefArg, NestedReferenceTransfer)):
        raise TypeError("reference must be a RefArg or NestedReferenceTransfer")
    if not isinstance(value.object_id, ObjectID):
        raise TypeError("Actor argument reference object_id must be an ObjectID")
    if not isinstance(value.owner_worker_id, WorkerID):
        raise TypeError(
            "Actor argument reference owner_worker_id must be a WorkerID"
        )
    return value.object_id, value.owner_worker_id


def _all_arguments(
    args: Tuple[TaskArg, ...], kwargs: Tuple[Tuple[str, TaskArg], ...]
) -> Tuple[TaskArg, ...]:
    return args + tuple(value for _name, value in kwargs)


def _ordered_top_level_references(
    arguments: Tuple[TaskArg, ...],
) -> Tuple[RefArg, ...]:
    ordered = []  # type: list[RefArg]
    by_object = {}  # type: dict[ObjectID, RefArg]
    for argument in arguments:
        if not isinstance(argument, RefArg):
            continue
        _reference_key(argument)
        previous = by_object.get(argument.object_id)
        if previous is None:
            by_object[argument.object_id] = argument
            ordered.append(argument)
        elif previous.owner_worker_id != argument.owner_worker_id:
            raise ValueError(
                "one top-level ObjectID cannot name conflicting owners"
            )
    return tuple(ordered)


def _ordered_nested_transfers(
    arguments: Tuple[TaskArg, ...],
) -> Tuple[NestedReferenceTransfer, ...]:
    ordered = []  # type: list[NestedReferenceTransfer]
    by_object = {}  # type: dict[ObjectID, NestedReferenceTransfer]
    for argument in arguments:
        if not isinstance(argument, (InlineArg, StoredArg)):
            continue
        for transfer in argument.nested_refs:
            _reference_key(transfer)
            previous = by_object.get(transfer.object_id)
            if previous is None:
                by_object[transfer.object_id] = transfer
                ordered.append(transfer)
            elif previous != transfer:
                raise ValueError(
                    "one nested ObjectID cannot name conflicting transfers"
                )
    return tuple(ordered)


@dataclass(frozen=True)
class ActorArgumentSpec:
    """Immutable logical argument lease for a constructor or one method.

    ``top_level_transfers`` supplies the full owner route and Task hold omitted
    by ``RefArg`` itself.  Nested transfers already live inside ``InlineArg``.
    Their ordered union is the complete logical lifetime manifest.

    A constructor intentionally contains no generation, target Worker, current
    producer attempt, location, or stored descriptor.  Those are physical facts
    refreshed separately for each incarnation.
    """

    actor_id: ActorID
    owner_worker_id: WorkerID
    task_id: TaskID
    attempt_id: AttemptID
    args: Tuple[TaskArg, ...]
    kwargs: Tuple[Tuple[str, TaskArg], ...] = ()
    top_level_transfers: Tuple[NestedReferenceTransfer, ...] = ()
    generation: Optional[ActorGeneration] = None
    call_key: Optional[ActorCallKey] = None
    _top_level_references: Tuple[RefArg, ...] = field(
        init=False, repr=False, compare=False
    )
    _nested_transfers: Tuple[NestedReferenceTransfer, ...] = field(
        init=False, repr=False, compare=False
    )
    _reference_transfers: Tuple[NestedReferenceTransfer, ...] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise TypeError("owner_worker_id must be a WorkerID")
        if not isinstance(self.task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        if not isinstance(self.attempt_id, AttemptID):
            raise TypeError("attempt_id must be an AttemptID")
        if self.attempt_id.task_id != self.task_id:
            raise ValueError("attempt_id must belong to task_id")

        args = tuple(self.args)
        kwargs = tuple(self.kwargs)
        transfers = tuple(self.top_level_transfers)
        if any(not isinstance(value, (InlineArg, RefArg)) for value in args):
            raise TypeError("Actor positional arguments must be TaskArg values")
        names = []  # type: list[str]
        for item in kwargs:
            if not isinstance(item, tuple) or len(item) != 2:
                raise TypeError(
                    "Actor keyword arguments must be (name, TaskArg) pairs"
                )
            name, value = item
            if not isinstance(name, str) or not name:
                raise ValueError("Actor keyword argument names must be non-empty")
            if not isinstance(value, (InlineArg, RefArg)):
                raise TypeError(
                    "Actor keyword argument values must be TaskArg values"
                )
            names.append(name)
        if len(names) != len(set(names)):
            raise ValueError("Actor keyword argument names must be unique")
        if any(
            not isinstance(value, NestedReferenceTransfer) for value in transfers
        ):
            raise TypeError(
                "top_level_transfers must contain NestedReferenceTransfer values"
            )

        constructor = self.generation is None and self.call_key is None
        method = self.generation is not None and self.call_key is not None
        if not constructor and not method:
            raise ValueError(
                "generation and call_key must be absent for a constructor or "
                "present together for a method"
            )
        if constructor:
            if self.attempt_id.attempt_number != 0:
                raise ValueError(
                    "constructor logical lineage must originate at creation attempt 0"
                )
        else:
            assert self.generation is not None and self.call_key is not None
            if (
                not isinstance(self.generation, ActorGeneration)
                or self.generation.actor_id != self.actor_id
            ):
                raise ValueError("method generation must belong to actor_id")
            if not isinstance(self.call_key, ActorCallKey):
                raise TypeError("call_key must be an ActorCallKey")
            if not isinstance(self.call_key.caller_id, WorkerID):
                raise TypeError("Actor call caller_id must be a WorkerID")
            if self.call_key.caller_id != self.owner_worker_id:
                raise ValueError(
                    "method call caller must own its Task result and holds"
                )
            if (
                isinstance(self.call_key.sequence, bool)
                or not isinstance(self.call_key.sequence, int)
                or self.call_key.sequence < 0
            ):
                raise ValueError("Actor call sequence must be non-negative")

        object.__setattr__(self, "args", args)
        object.__setattr__(self, "kwargs", kwargs)
        object.__setattr__(self, "top_level_transfers", transfers)
        arguments = _all_arguments(args, kwargs)
        top_level = _ordered_top_level_references(arguments)
        nested = _ordered_nested_transfers(arguments)

        expected_top_keys = tuple(_reference_key(value) for value in top_level)
        actual_top_keys = tuple(_reference_key(value) for value in transfers)
        if actual_top_keys != expected_top_keys:
            raise ValueError(
                "top_level_transfers must exactly cover unique RefArgs in "
                "first-appearance order"
            )

        by_key = {}  # type: dict[ReferenceKey, NestedReferenceTransfer]
        by_object = {}  # type: dict[ObjectID, NestedReferenceTransfer]
        complete = []  # type: list[NestedReferenceTransfer]
        for transfer in transfers + nested:
            key = _reference_key(transfer)
            object_previous = by_object.setdefault(
                transfer.object_id, transfer
            )
            if object_previous.owner_worker_id != transfer.owner_worker_id:
                raise ValueError(
                    "one Actor argument ObjectID cannot name conflicting owners"
                )
            previous = by_key.get(key)
            if previous is None:
                by_key[key] = transfer
                complete.append(transfer)
            elif previous != transfer:
                raise ValueError(
                    "top-level and nested occurrences require one exact transfer"
                )
            hold = transfer.hold
            if not isinstance(hold, TaskReferenceHold):
                raise TypeError("Actor reference transfer requires a full Task hold")
            if (
                hold.task_id != self.task_id
                or hold.origin_attempt_id != self.attempt_id
                or hold.submitting_worker_id != self.owner_worker_id
            ):
                raise ValueError(
                    "every Actor reference hold must preserve the invocation's "
                    "submitter, TaskID, and origin AttemptID"
                )

        object.__setattr__(self, "_top_level_references", top_level)
        object.__setattr__(self, "_nested_transfers", nested)
        object.__setattr__(self, "_reference_transfers", tuple(complete))

    @property
    def scope(self) -> ActorArgumentScope:
        return (
            ActorArgumentScope.ACTOR_LIFETIME
            if self.generation is None
            else ActorArgumentScope.METHOD_CALL
        )

    @property
    def is_constructor(self) -> bool:
        return self.scope is ActorArgumentScope.ACTOR_LIFETIME

    @property
    def top_level_references(self) -> Tuple[RefArg, ...]:
        return self._top_level_references

    @property
    def nested_transfers(self) -> Tuple[NestedReferenceTransfer, ...]:
        return self._nested_transfers

    @property
    def reference_transfers(self) -> Tuple[NestedReferenceTransfer, ...]:
        """Complete, deduplicated logical lifetime manifest."""

        return self._reference_transfers

    @property
    def logical_holds(self) -> Tuple[TaskReferenceHold, ...]:
        """One full hold per referenced object, never a token projection."""

        return tuple(value.hold for value in self.reference_transfers)


@dataclass(frozen=True)
class PreparedActorArguments:
    """One fully fenced physical preparation of an argument spec."""

    spec: ActorArgumentSpec
    generation: ActorGeneration
    target_worker_id: WorkerID
    args: Tuple[TaskArg, ...]
    kwargs: Tuple[Tuple[str, TaskArg], ...]
    dependencies: Tuple[ObjectStoreDescriptor, ...]
    acquisitions: Tuple[AcquireBorrowedObject, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.spec, ActorArgumentSpec):
            raise TypeError("spec must be an ActorArgumentSpec")
        if (
            not isinstance(self.generation, ActorGeneration)
            or self.generation.actor_id != self.spec.actor_id
        ):
            raise ValueError("prepared generation must belong to the Actor")
        if (
            not self.spec.is_constructor
            and self.spec.generation != self.generation
        ):
            raise ValueError("method arguments cannot cross Actor generations")
        if not isinstance(self.target_worker_id, WorkerID):
            raise TypeError("target_worker_id must be a WorkerID")

        args = tuple(self.args)
        kwargs = tuple(self.kwargs)
        dependencies = tuple(self.dependencies)
        acquisitions = tuple(self.acquisitions)
        object.__setattr__(self, "args", args)
        object.__setattr__(self, "kwargs", kwargs)
        object.__setattr__(self, "dependencies", dependencies)
        object.__setattr__(self, "acquisitions", acquisitions)

        if len(args) != len(self.spec.args):
            raise ValueError("prepared positional argument shape changed")
        if tuple(name for name, _value in kwargs) != tuple(
            name for name, _value in self.spec.kwargs
        ):
            raise ValueError("prepared keyword argument shape changed")

        prepared_by_key = {}  # type: dict[ReferenceKey, TaskArg]
        for original, prepared in zip(self.spec.args, args):
            _validate_prepared_argument(original, prepared, prepared_by_key)
        for (_name, original), (_prepared_name, prepared) in zip(
            self.spec.kwargs, kwargs
        ):
            _validate_prepared_argument(original, prepared, prepared_by_key)

        stored_keys = []  # type: list[ReferenceKey]
        seen = set()  # type: set[ReferenceKey]
        for original in _all_arguments(self.spec.args, self.spec.kwargs):
            if not isinstance(original, RefArg):
                continue
            key = _reference_key(original)
            prepared = prepared_by_key[key]
            if isinstance(prepared, RefArg) and key not in seen:
                seen.add(key)
                stored_keys.append(key)
        if len(dependencies) != len(stored_keys):
            raise ValueError(
                "stored descriptors must exactly cover prepared RefArgs"
            )
        for key, descriptor in zip(stored_keys, dependencies):
            if not isinstance(descriptor, ObjectStoreDescriptor):
                raise TypeError(
                    "dependencies must contain ObjectStoreDescriptor values"
                )
            if (descriptor.object_id, descriptor.owner_worker_id) != key:
                raise ValueError(
                    "stored descriptor does not match its exact RefArg identity"
                )

        transfers = self.spec.reference_transfers
        if len(acquisitions) != len(transfers):
            raise ValueError(
                "execution acquisitions must exactly cover logical references"
            )
        tokens = []  # type: list[str]
        for transfer, acquire in zip(transfers, acquisitions):
            if not isinstance(acquire, AcquireBorrowedObject):
                raise TypeError(
                    "acquisitions must contain AcquireBorrowedObject values"
                )
            if (
                acquire.object_id != transfer.object_id
                or acquire.owner_worker_id != transfer.owner_worker_id
                or acquire.borrower_worker_id != self.target_worker_id
                or acquire.source != TaskHoldSource(transfer.hold)
            ):
                raise ValueError(
                    "execution acquisition changed object, owner, borrower, or hold"
                )
            tokens.append(acquire.borrower_token)
        if len(tokens) != len(set(tokens)):
            raise ValueError(
                "one prepared execution must use unique borrower tokens"
            )

    @property
    def execution_releases(self) -> Tuple[ReleaseBorrowedObject, ...]:
        """Exact idempotent cleanup work for this physical incarnation."""

        return tuple(
            ReleaseBorrowedObject(
                value.object_id, value.owner_worker_id,
                value.borrower_worker_id, value.borrower_token,
            )
            for value in self.acquisitions
        )


def _validate_prepared_argument(
    original: TaskArg,
    prepared: TaskArg,
    by_key: dict[ReferenceKey, TaskArg],
) -> None:
    if isinstance(original, InlineArg):
        if prepared != original:
            raise ValueError(
                "inline and nested Actor argument contents are immutable"
            )
        return
    if not isinstance(original, RefArg):
        raise TypeError("original Actor argument must be a TaskArg")
    key = _reference_key(original)
    if isinstance(prepared, RefArg):
        if prepared != original:
            raise ValueError("prepared RefArg changed object or owner identity")
    elif isinstance(prepared, InlineArg):
        if isinstance(original, StoredArg):
            if (
                prepared.serializer != original.serializer
                or prepared.nested_refs != original.nested_refs
            ):
                raise ValueError(
                    "resolved StoredArg must preserve serializer and nested manifest"
                )
        elif prepared.serializer != "cloudpickle" or prepared.nested_refs:
            raise ValueError(
                "resolved top-level inline values must be plain cloudpickle bytes"
            )
    else:
        raise TypeError("prepared Actor argument must be a TaskArg")
    previous = by_key.setdefault(key, prepared)
    if previous != prepared:
        raise ValueError(
            "duplicate top-level ObjectRef occurrences resolved differently"
        )


class ActorArgumentAuthority:
    """Serialize generation installation and exact argument preparation.

    The immutable constructor spec is retained until :meth:`retire_actor`; an
    Actor restart discards only physical execution borrowers.  Method keys are
    indexed by generation, so reusing a caller sequence after a restart is a
    new key while every old-generation preparation is fenced.
    """

    def __init__(self, constructor: ActorArgumentSpec) -> None:
        if not isinstance(constructor, ActorArgumentSpec):
            raise TypeError("constructor must be an ActorArgumentSpec")
        if not constructor.is_constructor:
            raise ValueError("authority requires an Actor constructor spec")
        self.actor_id = constructor.actor_id
        self.constructor = constructor
        self._generation = None  # type: Optional[ActorGeneration]
        self._worker_id = None  # type: Optional[WorkerID]
        self._constructor_prepared = (
            {}
        )  # type: dict[ActorGeneration, PreparedActorArguments]
        self._method_specs = (
            {}
        )  # type: dict[tuple[ActorGeneration, ActorCallKey], ActorArgumentSpec]
        self._method_task_keys = (
            {}
        )  # type: dict[TaskID, tuple[ActorGeneration, ActorCallKey]]
        self._method_prepared = (
            {}
        )  # type: dict[tuple[ActorGeneration, ActorCallKey], PreparedActorArguments]
        self._retired = False
        self._retirement = None  # type: Optional[Tuple[NestedReferenceTransfer, ...]]
        self._lock = RLock()

    @property
    def current_generation(self) -> Optional[ActorGeneration]:
        with self._lock:
            return self._generation

    @property
    def current_worker_id(self) -> Optional[WorkerID]:
        with self._lock:
            return self._worker_id

    @property
    def constructor_lifetime_transfers(
        self,
    ) -> Tuple[NestedReferenceTransfer, ...]:
        """Logical holds retained across restarts until Actor retirement."""

        return self.constructor.reference_transfers

    def install_generation(
        self, generation: ActorGeneration, worker_id: WorkerID
    ) -> Tuple[PreparedActorArguments, ...]:
        """Install a fresh incarnation and return fenced physical work.

        Returned preparations expose exact ``execution_releases``.  They do
        not include the constructor's logical transfers, which remain owned by
        this authority across the transition.
        """

        if (
            not isinstance(generation, ActorGeneration)
            or generation.actor_id != self.actor_id
        ):
            raise ValueError("generation must belong to this Actor")
        if not isinstance(worker_id, WorkerID):
            raise TypeError("worker_id must be a WorkerID")
        with self._lock:
            self._require_live()
            current = self._generation
            if current is not None:
                if generation == current:
                    if worker_id != self._worker_id:
                        raise ActorArgumentConflictError(
                            "one Actor generation cannot change target Worker"
                        )
                    return ()
                if generation.generation <= current.generation:
                    raise ActorArgumentFencedError(
                        "Actor generation is already fenced"
                    )
                if worker_id == self._worker_id:
                    raise ActorArgumentConflictError(
                        "a fresh Actor generation requires a fresh Worker"
                    )

            abandoned = []  # type: list[PreparedActorArguments]
            if current is not None:
                prepared = self._constructor_prepared.get(current)
                if prepared is not None:
                    abandoned.append(prepared)
                method_values = [
                    value for (item_generation, _key), value
                    in self._method_prepared.items()
                    if item_generation == current
                ]
                method_values.sort(
                    key=lambda value: (
                        value.spec.call_key.caller_id.hex,
                        value.spec.call_key.sequence,
                    )
                )
                abandoned.extend(method_values)
            self._generation = generation
            self._worker_id = worker_id
            return tuple(abandoned)

    def register_method(self, spec: ActorArgumentSpec) -> ActorArgumentSpec:
        if not isinstance(spec, ActorArgumentSpec):
            raise TypeError("spec must be an ActorArgumentSpec")
        if spec.is_constructor:
            raise ValueError("register_method requires a method argument spec")
        assert spec.generation is not None and spec.call_key is not None
        with self._lock:
            self._require_current(spec.generation)
            if spec.actor_id != self.actor_id:
                raise ValueError("method spec belongs to another Actor")
            identity = spec.generation, spec.call_key
            old_identity = self._method_task_keys.get(spec.task_id)
            if old_identity is not None and old_identity != identity:
                raise ActorArgumentConflictError(
                    "a method TaskID cannot be replayed in another call or "
                    "Actor generation"
                )
            previous = self._method_specs.get(identity)
            if previous is not None:
                if previous != spec:
                    raise ActorArgumentConflictError(
                        "Actor method call key was rebound to another spec"
                    )
                return previous
            self._method_specs[identity] = spec
            self._method_task_keys[spec.task_id] = identity
            return spec

    def prepare_constructor(
        self,
        *,
        acquisitions: Mapping[ReferenceKey, AcquireBorrowedObjectReply],
        observations: Mapping[ReferenceKey, GetOwnedObjectReply],
    ) -> PreparedActorArguments:
        with self._lock:
            generation, worker_id = self._current_execution()
            candidate = _prepare_arguments(
                self.constructor, generation, worker_id, acquisitions, observations
            )
            previous = self._constructor_prepared.get(generation)
            if previous is not None:
                if previous != candidate:
                    raise ActorArgumentConflictError(
                        "one constructor generation changed its prepared identity"
                    )
                return previous
            self._validate_fresh_constructor_borrowers(candidate)
            self._validate_token_collisions(candidate)
            self._constructor_prepared[generation] = candidate
            return candidate

    def prepare_method(
        self,
        spec: ActorArgumentSpec,
        *,
        acquisitions: Mapping[ReferenceKey, AcquireBorrowedObjectReply],
        observations: Mapping[ReferenceKey, GetOwnedObjectReply],
    ) -> PreparedActorArguments:
        with self._lock:
            registered = self.register_method(spec)
            assert registered.generation is not None
            assert registered.call_key is not None
            generation, worker_id = self._current_execution()
            candidate = _prepare_arguments(
                registered, generation, worker_id, acquisitions, observations
            )
            identity = generation, registered.call_key
            previous = self._method_prepared.get(identity)
            if previous is not None:
                if previous != candidate:
                    raise ActorArgumentConflictError(
                        "one method call changed its prepared identity"
                    )
                return previous
            self._validate_token_collisions(candidate)
            self._method_prepared[identity] = candidate
            return candidate

    def prepared_constructor(
        self, generation: ActorGeneration
    ) -> Optional[PreparedActorArguments]:
        with self._lock:
            return self._constructor_prepared.get(generation)

    def prepared_method(
        self, generation: ActorGeneration, call_key: ActorCallKey
    ) -> Optional[PreparedActorArguments]:
        with self._lock:
            return self._method_prepared.get((generation, call_key))

    def retire_actor(self) -> Tuple[NestedReferenceTransfer, ...]:
        """Fence all preparation and release constructor logical lineage.

        The exact replay returns the same full transfers.  A runtime adapter can
        use each transfer's owner route and ``TaskReferenceHold`` to issue an
        idempotent logical release; no token-only reconstruction is required.
        """

        with self._lock:
            if self._retirement is not None:
                return self._retirement
            self._retired = True
            self._retirement = self.constructor.reference_transfers
            return self._retirement

    def _current_execution(self) -> tuple[ActorGeneration, WorkerID]:
        self._require_live()
        if self._generation is None or self._worker_id is None:
            raise ActorArgumentFencedError(
                "Actor has no installed physical generation"
            )
        return self._generation, self._worker_id

    def _require_current(self, generation: ActorGeneration) -> None:
        current, _worker = self._current_execution()
        if generation != current:
            raise ActorArgumentFencedError(
                "method arguments target a non-current Actor generation"
            )

    def _require_live(self) -> None:
        if self._retired:
            raise ActorArgumentFencedError(
                "Actor argument authority is retired"
            )

    def _validate_fresh_constructor_borrowers(
        self, candidate: PreparedActorArguments
    ) -> None:
        current_tokens = {
            (value.object_id, value.owner_worker_id): value.borrower_token
            for value in candidate.acquisitions
        }
        for generation, previous in self._constructor_prepared.items():
            if generation == candidate.generation:
                continue
            previous_tokens = {
                (value.object_id, value.owner_worker_id): value.borrower_token
                for value in previous.acquisitions
            }
            for key, token in current_tokens.items():
                if previous_tokens.get(key) == token:
                    raise ActorArgumentConflictError(
                        "constructor restart must import every reference with "
                        "a fresh borrower token"
                    )

    def _validate_token_collisions(
        self, candidate: PreparedActorArguments
    ) -> None:
        identities = {
            (
                value.object_id, value.owner_worker_id,
                value.borrower_worker_id, value.borrower_token,
            )
            for value in candidate.acquisitions
        }
        existing = tuple(self._constructor_prepared.values()) + tuple(
            self._method_prepared.values()
        )
        for prepared in existing:
            if prepared == candidate:
                continue
            other = {
                (
                    value.object_id, value.owner_worker_id,
                    value.borrower_worker_id, value.borrower_token,
                )
                for value in prepared.acquisitions
            }
            if identities.intersection(other):
                raise ActorArgumentConflictError(
                    "execution borrower identity was reused by another invocation"
                )


def _prepare_arguments(
    spec: ActorArgumentSpec,
    generation: ActorGeneration,
    worker_id: WorkerID,
    acquisition_replies: Mapping[ReferenceKey, AcquireBorrowedObjectReply],
    observations: Mapping[ReferenceKey, GetOwnedObjectReply],
) -> PreparedActorArguments:
    """Validate all owner facts before constructing one immutable result."""

    acquisition_values = dict(acquisition_replies)
    observation_values = dict(observations)
    reference_keys = tuple(
        _reference_key(value) for value in spec.reference_transfers
    )
    top_level_keys = tuple(
        _reference_key(value) for value in spec.top_level_references
    )
    if set(acquisition_values) != set(reference_keys):
        raise ActorArgumentDependencyError(
            "acquire acknowledgements must exactly cover logical references"
        )
    if set(observation_values) != set(top_level_keys):
        raise ActorArgumentDependencyError(
            "owner observations must exactly cover top-level references"
        )

    acquisitions = []  # type: list[AcquireBorrowedObject]
    acquire_by_key = {}  # type: dict[ReferenceKey, AcquireBorrowedObject]
    for transfer in spec.reference_transfers:
        key = _reference_key(transfer)
        reply = acquisition_values[key]
        if not isinstance(reply, AcquireBorrowedObjectReply):
            raise TypeError(
                "acquisition values must be AcquireBorrowedObjectReply values"
            )
        expected_source = TaskHoldSource(transfer.hold)
        if (
            reply.object_id != transfer.object_id
            or reply.owner_worker_id != transfer.owner_worker_id
            or reply.borrower_worker_id != worker_id
            or reply.source != expected_source
        ):
            raise ActorArgumentDependencyError(
                "acquire acknowledgement changed object, owner, borrower, or hold"
            )
        if not reply.accepted:
            raise ActorArgumentDependencyError(
                reply.error or "object owner rejected execution borrower"
            )
        acquire = AcquireBorrowedObject(
            reply.object_id, reply.owner_worker_id, reply.borrower_worker_id,
            reply.source, reply.borrower_token,
        )
        acquisitions.append(acquire)
        acquire_by_key[key] = acquire

    replacement = {}  # type: dict[ReferenceKey, TaskArg]
    dependencies = []  # type: list[ObjectStoreDescriptor]
    missing = []  # type: list[ObjectID]
    for reference in spec.top_level_references:
        key = _reference_key(reference)
        acquire = acquire_by_key[key]
        reply = observation_values[key]
        if not isinstance(reply, GetOwnedObjectReply):
            raise TypeError(
                "observation values must be GetOwnedObjectReply values"
            )
        if (
            reply.object_id != reference.object_id
            or reply.owner_worker_id != reference.owner_worker_id
            or reply.borrower_worker_id != worker_id
            or reply.borrower_token != acquire.borrower_token
        ):
            raise ActorArgumentDependencyError(
                "owner observation does not match the execution borrower"
            )
        if not reply.accepted:
            raise ActorArgumentDependencyError(
                reply.detail or "object owner rejected dependency read"
            )
        if reply.state is OwnedObjectState.PENDING:
            missing.append(reference.object_id)
            continue
        if reply.state is OwnedObjectState.READY_INLINE:
            assert reply.data is not None
            replacement[key] = (
                InlineArg(
                    reply.data, serializer=reference.serializer,
                    nested_refs=reference.nested_refs,
                )
                if isinstance(reference, StoredArg)
                else InlineArg(reply.data, serializer="cloudpickle")
            )
            continue
        if reply.state is OwnedObjectState.READY_STORED:
            descriptor = reply.descriptor
            if (
                descriptor is None
                or reply.current_attempt is None
                or descriptor.producer_attempt_id != reply.current_attempt
                or (descriptor.object_id, descriptor.owner_worker_id) != key
            ):
                raise ActorArgumentDependencyError(
                    "stored owner observation lacks an exact current descriptor"
                )
            replacement[key] = reference
            dependencies.append(descriptor)
            continue
        if reply.state is OwnedObjectState.ERROR:
            detail = reply.error
            raise ActorArgumentDependencyError(
                "Actor dependency failed at owner: {}: {}".format(
                    getattr(detail, "type_name", "error"),
                    getattr(detail, "message", "unknown failure"),
                )
            )
        if reply.state is OwnedObjectState.LOST:
            raise ActorArgumentDependencyError(
                "Actor dependency is LOST at its owner"
            )
        raise ActorArgumentDependencyError(
            "object owner returned an unsupported dependency state"
        )
    if missing:
        raise ActorArgumentsNotReady(tuple(missing))

    def prepare(value: TaskArg) -> TaskArg:
        if isinstance(value, RefArg):
            return replacement[_reference_key(value)]
        return value

    prepared_args = tuple(prepare(value) for value in spec.args)
    prepared_kwargs = tuple(
        (name, prepare(value)) for name, value in spec.kwargs
    )
    return PreparedActorArguments(
        spec=spec,
        generation=generation,
        target_worker_id=worker_id,
        args=prepared_args,
        kwargs=prepared_kwargs,
        dependencies=tuple(dependencies),
        acquisitions=tuple(acquisitions),
    )


__all__ = [
    "ActorArgumentAuthority",
    "ActorArgumentConflictError",
    "ActorArgumentDependencyError",
    "ActorArgumentError",
    "ActorArgumentFencedError",
    "ActorArgumentScope",
    "ActorArgumentSpec",
    "ActorArgumentsNotReady",
    "PreparedActorArguments",
    "ReferenceKey",
]
