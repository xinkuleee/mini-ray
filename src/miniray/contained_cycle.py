"""Cycle policy for the logical contained-ObjectRef graph.

Python serialization has its own object graph and memo table.  A list that
contains itself is therefore unrelated to this module.  This authority sees
only explicit logical edges of the form ``outer ObjectID -> contained
ObjectID``.

Distributed reference counting cannot collect a strongly connected component:
every object in the component keeps another one pinned.  mini-Ray deliberately
does not teach a second, distributed tracing collector.  Instead it keeps the
contained-object graph a DAG by rejecting a publication that would introduce a
cycle.

``prepare`` is a reservation, not a read-only check.  Prepared edges participate
in later checks, so concurrent ``A -> B`` and ``B -> A`` publications cannot
both pass preflight and then commit.  A caller commits after the matching result
publication becomes authoritative, or aborts while compensating its prepared
contained pins.  All batch transitions are atomic under one job-scoped lock.
Collection keeps the committed graph edge until the matching incoming pin
release has acknowledged; only then does ``release_container`` retire it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Iterable

from .contained_edges import ContainedReferenceEdge
from .ids import ObjectID, WorkerID


class ContainedGraphError(RuntimeError):
    """Base class for contained-reference graph policy failures."""


class ContainedReferenceCycleError(ContainedGraphError):
    """A complete candidate batch would make the ObjectID graph cyclic."""

    def __init__(self, cycle_path: tuple[ObjectID, ...]) -> None:
        if (
            len(cycle_path) < 2
            or cycle_path[0] != cycle_path[-1]
            or any(not isinstance(value, ObjectID) for value in cycle_path)
        ):
            raise ValueError(
                "cycle_path must be a closed sequence of ObjectIDs"
            )
        self.cycle_path = tuple(cycle_path)
        rendered = " -> ".join(str(value) for value in self.cycle_path)
        super().__init__(
            "contained ObjectRef publication would create an ObjectID "
            f"cycle: {rendered}"
        )


class ContainedGraphTransactionConflictError(ContainedGraphError):
    """One transaction identity was rebound to a different edge batch."""


class ContainedGraphTransactionStateError(ContainedGraphError):
    """The requested transition contradicts a terminal transaction state."""


class ContainedContainerBusyError(ContainedGraphError):
    """A container cannot be collected while its outgoing batch is prepared."""


class ContainedGraphTransactionState(str, Enum):
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


@dataclass(frozen=True)
class ContainedGraphManifest:
    """Durable GCS identity for one prepared publication manifest.

    ``publication_id`` remains opaque here to avoid coupling the job-scoped
    cycle authority to the Node journal module.  The full value is nevertheless
    retained, so a lost Node can recover the exact owner and edge obligations
    instead of relying on an edge-only transaction projection.
    """

    transaction_id: str
    publication_id: object
    outer_owner_worker_id: WorkerID
    manifest_digest: str
    ordered_edges: tuple[ContainedReferenceEdge, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.transaction_id, str) or not self.transaction_id:
            raise ValueError("transaction_id must be a non-empty string")
        try:
            hash(self.publication_id)
        except TypeError as exc:
            raise TypeError("publication_id must be hashable") from exc
        if not isinstance(self.outer_owner_worker_id, WorkerID):
            raise TypeError("outer_owner_worker_id must be a WorkerID")
        if (
            not isinstance(self.manifest_digest, str)
            or len(self.manifest_digest) != 64
        ):
            raise ValueError("manifest_digest must be a SHA-256 hex digest")
        try:
            int(self.manifest_digest, 16)
        except ValueError as exc:
            raise ValueError(
                "manifest_digest must be a SHA-256 hex digest"
            ) from exc
        object.__setattr__(self, "manifest_digest", self.manifest_digest.lower())
        edges = tuple(self.ordered_edges)
        if any(not isinstance(edge, ContainedReferenceEdge) for edge in edges):
            raise TypeError(
                "ordered_edges must contain ContainedReferenceEdge values"
            )
        if len(edges) != len(set(edges)):
            raise ValueError("ordered_edges must be unique")
        object.__setattr__(self, "ordered_edges", edges)

    @property
    def transaction(self) -> "ContainedGraphTransaction":
        return ContainedGraphTransaction(self.transaction_id, self.ordered_edges)


class ContainedGraphManifestDisposition(str, Enum):
    APPLIED = "APPLIED"
    ALREADY_PREPARED = "ALREADY_PREPARED"
    ALREADY_COMMITTED = "ALREADY_COMMITTED"
    ALREADY_ABORTED = "ALREADY_ABORTED"
    RELEASED = "RELEASED"
    ALREADY_RELEASED = "ALREADY_RELEASED"


@dataclass(frozen=True)
class ContainedGraphManifestReceipt:
    manifest: ContainedGraphManifest
    state: ContainedGraphTransactionState
    disposition: ContainedGraphManifestDisposition
    released_edges: tuple[ContainedReferenceEdge, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, ContainedGraphManifest):
            raise TypeError("manifest must be a ContainedGraphManifest")
        if not isinstance(self.state, ContainedGraphTransactionState):
            raise TypeError("state must be a ContainedGraphTransactionState")
        if not isinstance(self.disposition, ContainedGraphManifestDisposition):
            raise TypeError(
                "disposition must be a ContainedGraphManifestDisposition"
            )
        edges = tuple(self.released_edges)
        if any(not isinstance(edge, ContainedReferenceEdge) for edge in edges):
            raise TypeError(
                "released_edges must contain ContainedReferenceEdge values"
            )
        if len(edges) != len(set(edges)):
            raise ValueError("released_edges must be unique")
        edge_set = set(edges)
        if edges != tuple(
            edge for edge in self.manifest.ordered_edges if edge in edge_set
        ):
            raise ValueError(
                "released_edges must be an ordered subset of the manifest"
            )
        release_dispositions = (
            ContainedGraphManifestDisposition.RELEASED,
            ContainedGraphManifestDisposition.ALREADY_RELEASED,
        )
        if self.disposition in release_dispositions:
            if (
                self.state is not ContainedGraphTransactionState.COMMITTED
                or not edges
                or len({edge.container_object_id for edge in edges}) != 1
            ):
                raise ValueError(
                    "a graph release receipt requires one non-empty "
                    "committed container edge subset"
                )
        elif edges:
            raise ValueError(
                "only a graph release receipt may contain released_edges"
            )
        expected_state = {
            ContainedGraphManifestDisposition.ALREADY_PREPARED:
                ContainedGraphTransactionState.PREPARED,
            ContainedGraphManifestDisposition.ALREADY_COMMITTED:
                ContainedGraphTransactionState.COMMITTED,
            ContainedGraphManifestDisposition.ALREADY_ABORTED:
                ContainedGraphTransactionState.ABORTED,
        }.get(self.disposition)
        if expected_state is not None and self.state is not expected_state:
            raise ValueError(
                "manifest receipt disposition contradicts transaction state"
            )
        object.__setattr__(self, "released_edges", edges)


@dataclass(frozen=True)
class ContainedGraphTransaction:
    """Immutable identity of one atomic contained-edge publication.

    A batch may name several containers so a future multi-return publication can
    reserve its whole manifest at once.  Parallel edges with distinct transfer
    tokens are retained as distinct release obligations, while cycle detection
    projects them to ObjectID pairs.
    """

    transaction_id: str
    edges: tuple[ContainedReferenceEdge, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.transaction_id, str) or not self.transaction_id:
            raise ValueError("transaction_id must be a non-empty string")
        edges = tuple(self.edges)
        if not edges:
            raise ValueError("a contained graph transaction requires an edge")
        if any(not isinstance(edge, ContainedReferenceEdge) for edge in edges):
            raise TypeError(
                "transaction edges must contain ContainedReferenceEdge values"
            )
        if len(edges) != len(set(edges)):
            raise ValueError("transaction edges must be unique")
        object.__setattr__(self, "edges", tuple(sorted(edges)))


@dataclass(frozen=True)
class ContainedGraphSnapshot:
    """Immutable diagnostic view; Python object identities never appear here."""

    committed_edges: tuple[ContainedReferenceEdge, ...] = ()
    prepared_edges: tuple[ContainedReferenceEdge, ...] = ()
    transactions: tuple[
        tuple[str, ContainedGraphTransactionState], ...
    ] = ()
    admission_closed: bool = False
    manifests: tuple["ContainedGraphManifestSnapshot", ...] = ()


@dataclass(frozen=True)
class ContainedGraphManifestSnapshot:
    """Exact diagnostic state for one durable publication manifest."""

    manifest: ContainedGraphManifest
    state: ContainedGraphTransactionState
    active_edges: tuple[ContainedReferenceEdge, ...] = ()
    released_container_ids: tuple[ObjectID, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, ContainedGraphManifest):
            raise TypeError("manifest must be a ContainedGraphManifest")
        if not isinstance(self.state, ContainedGraphTransactionState):
            raise TypeError("state must be a ContainedGraphTransactionState")
        active = tuple(self.active_edges)
        if any(not isinstance(edge, ContainedReferenceEdge) for edge in active):
            raise TypeError(
                "active_edges must contain ContainedReferenceEdge values"
            )
        if not set(active).issubset(self.manifest.ordered_edges):
            raise ValueError("active_edges must belong to the exact manifest")
        released = tuple(self.released_container_ids)
        if any(not isinstance(value, ObjectID) for value in released):
            raise TypeError("released_container_ids must contain ObjectIDs")
        if len(released) != len(set(released)):
            raise ValueError("released_container_ids must be unique")
        manifest_containers = {
            edge.container_object_id for edge in self.manifest.ordered_edges
        }
        if not set(released).issubset(manifest_containers):
            raise ValueError(
                "released_container_ids must belong to the exact manifest"
            )
        object.__setattr__(self, "active_edges", active)
        object.__setattr__(
            self, "released_container_ids", tuple(sorted(released))
        )


@dataclass
class _TransactionRecord:
    transaction: ContainedGraphTransaction
    state: ContainedGraphTransactionState
    # Collection can discharge one container from a committed multi-container
    # batch.  Commit replay must not resurrect those already-released edges.
    active_edges: set[ContainedReferenceEdge] = field(default_factory=set)


class ContainedReferenceGraphAuthority:
    """Job-scoped authority that keeps contained ObjectID edges acyclic.

    The graph uses tri-colour DFS.  Preparing a batch costs ``O(V + E)`` in the
    committed-plus-prepared graph; commit, abort, and exact replay are bounded
    state transitions.  This intentionally favors one visible teaching
    invariant over a production distributed SCC protocol.
    """

    def __init__(self) -> None:
        self._transactions: dict[str, _TransactionRecord] = {}
        self._manifests: dict[str, ContainedGraphManifest] = {}
        # A future atomic publication may contain one result per container.
        # Collection is therefore terminal per (manifest, container), not per
        # manifest.  Keeping this exact tombstone makes a lost RELEASE reply
        # replayable without preventing another container in the same batch
        # from being collected later.
        self._released_manifest_containers: set[
            tuple[str, ObjectID]
        ] = set()
        self._admission_closed = False
        self._lock = RLock()

    def prepare_manifest(
        self, manifest: ContainedGraphManifest
    ) -> ContainedGraphManifestReceipt:
        """Reserve and retain a complete publication manifest in GCS."""

        self._require_manifest(manifest)
        with self._lock:
            previous = self._matching_manifest(manifest)
            if previous is not None:
                record = self._transactions[manifest.transaction_id]
                if record.state is ContainedGraphTransactionState.ABORTED:
                    raise ContainedGraphTransactionStateError(
                        "an aborted contained graph manifest cannot prepare"
                    )
                disposition = (
                    ContainedGraphManifestDisposition.ALREADY_COMMITTED
                    if record.state is ContainedGraphTransactionState.COMMITTED
                    else ContainedGraphManifestDisposition.ALREADY_PREPARED
                )
                return ContainedGraphManifestReceipt(
                    manifest, record.state, disposition
                )
            self._require_admission_open()
            self._raise_if_cyclic(manifest.ordered_edges)
            transaction = manifest.transaction
            self._transactions[manifest.transaction_id] = _TransactionRecord(
                transaction, ContainedGraphTransactionState.PREPARED
            )
            self._manifests[manifest.transaction_id] = manifest
            return ContainedGraphManifestReceipt(
                manifest, ContainedGraphTransactionState.PREPARED,
                ContainedGraphManifestDisposition.APPLIED,
            )

    def commit_manifest(
        self, manifest: ContainedGraphManifest
    ) -> ContainedGraphManifestReceipt:
        """Commit the exact prepared publication manifest."""

        self._require_manifest(manifest)
        with self._lock:
            if self._matching_manifest(manifest) is None:
                raise ContainedGraphTransactionStateError(
                    "contained graph manifest was not prepared"
                )
            record = self._transactions[manifest.transaction_id]
            if record.state is ContainedGraphTransactionState.ABORTED:
                raise ContainedGraphTransactionStateError(
                    "an aborted contained graph manifest cannot commit"
                )
            if record.state is ContainedGraphTransactionState.COMMITTED:
                return ContainedGraphManifestReceipt(
                    manifest, record.state,
                    ContainedGraphManifestDisposition.ALREADY_COMMITTED,
                )
            record.state = ContainedGraphTransactionState.COMMITTED
            record.active_edges = set(manifest.ordered_edges)
            return ContainedGraphManifestReceipt(
                manifest, record.state,
                ContainedGraphManifestDisposition.APPLIED,
            )

    def abort_manifest(
        self, manifest: ContainedGraphManifest
    ) -> ContainedGraphManifestReceipt:
        """Abort one exact reservation and retain its identity tombstone."""

        self._require_manifest(manifest)
        with self._lock:
            if self._matching_manifest(manifest) is None:
                # ABORT is itself an authoritative write.  Recording an unseen
                # identity fences a PREPARE that was sent before a Node crash
                # but reaches GCS after its compensation path.
                self._transactions[manifest.transaction_id] = _TransactionRecord(
                    manifest.transaction, ContainedGraphTransactionState.ABORTED
                )
                self._manifests[manifest.transaction_id] = manifest
                return ContainedGraphManifestReceipt(
                    manifest, ContainedGraphTransactionState.ABORTED,
                    ContainedGraphManifestDisposition.APPLIED,
                )
            record = self._transactions[manifest.transaction_id]
            if record.state is ContainedGraphTransactionState.COMMITTED:
                raise ContainedGraphTransactionStateError(
                    "a committed contained graph manifest cannot abort"
                )
            if record.state is ContainedGraphTransactionState.ABORTED:
                return ContainedGraphManifestReceipt(
                    manifest, record.state,
                    ContainedGraphManifestDisposition.ALREADY_ABORTED,
                )
            record.state = ContainedGraphTransactionState.ABORTED
            return ContainedGraphManifestReceipt(
                manifest, record.state,
                ContainedGraphManifestDisposition.APPLIED,
            )

    def release_manifest_container(
        self, manifest: ContainedGraphManifest,
        container_object_id: ObjectID,
    ) -> ContainedGraphManifestReceipt:
        """Retire exact committed edges after all final pins are released.

        Unlike the legacy ``release_container`` empty-tuple replay, this reply
        echoes the complete manifest identity and digest.  It can therefore be
        trusted after response loss or GCS failover.
        """

        self._require_manifest(manifest)
        if not isinstance(container_object_id, ObjectID):
            raise TypeError("container_object_id must be an ObjectID")
        with self._lock:
            if self._matching_manifest(manifest) is None:
                raise ContainedGraphTransactionStateError(
                    "contained graph manifest was not prepared"
                )
            expected = tuple(
                edge for edge in manifest.ordered_edges
                if edge.container_object_id == container_object_id
            )
            if not expected:
                raise ContainedGraphTransactionConflictError(
                    "release container is not present in the manifest"
                )
            record = self._transactions[manifest.transaction_id]
            if record.state is ContainedGraphTransactionState.PREPARED:
                raise ContainedContainerBusyError(
                    "container has a prepared contained-edge publication"
                )
            if record.state is ContainedGraphTransactionState.ABORTED:
                raise ContainedGraphTransactionStateError(
                    "an aborted manifest has no committed container"
                )
            release_identity = (
                manifest.transaction_id, container_object_id
            )
            if release_identity in self._released_manifest_containers:
                return ContainedGraphManifestReceipt(
                    manifest, record.state,
                    ContainedGraphManifestDisposition.ALREADY_RELEASED,
                    expected,
                )
            released = tuple(
                edge for edge in expected
                if edge in record.active_edges
            )
            if released != expected:
                raise ContainedGraphTransactionConflictError(
                    "manifest container was released through another identity"
                )
            record.active_edges.difference_update(released)
            self._released_manifest_containers.add(release_identity)
            return ContainedGraphManifestReceipt(
                manifest, record.state,
                ContainedGraphManifestDisposition.RELEASED, released,
            )

    def close_admission(self) -> None:
        """Fence new graph publications while terminal replays stay legal."""

        with self._lock:
            self._admission_closed = True

    def has_active_obligations(self) -> bool:
        """Whether shutdown would abandon reserved or live graph edges."""

        with self._lock:
            return any(
                record.state is ContainedGraphTransactionState.PREPARED
                or (
                    record.state is ContainedGraphTransactionState.COMMITTED
                    and bool(record.active_edges)
                )
                for record in self._transactions.values()
            )

    def get_manifest(
        self, transaction_id: str
    ) -> ContainedGraphManifest | None:
        """Return the durable full manifest for ACK recovery/Node loss."""

        if not isinstance(transaction_id, str) or not transaction_id:
            raise ValueError("transaction_id must be a non-empty string")
        with self._lock:
            return self._manifests.get(transaction_id)

    def prepare(self, transaction: ContainedGraphTransaction) -> bool:
        """Reserve a complete batch, or replay its exact prior prepare.

        Returns ``True`` only for the first successful reservation.  Cycle and
        conflict failures leave the authority unchanged.
        """

        self._require_transaction(transaction)
        with self._lock:
            record = self._matching_record(transaction)
            if record is not None:
                if record.state is ContainedGraphTransactionState.ABORTED:
                    raise ContainedGraphTransactionStateError(
                        "an aborted contained graph transaction cannot be prepared"
                    )
                return False
            self._require_admission_open()
            self._raise_if_cyclic(transaction.edges)
            self._transactions[transaction.transaction_id] = _TransactionRecord(
                transaction, ContainedGraphTransactionState.PREPARED
            )
            return True

    def commit(self, transaction: ContainedGraphTransaction) -> bool:
        """Commit an exact prepared batch without a second race window."""

        self._require_transaction(transaction)
        with self._lock:
            record = self._matching_record(transaction)
            if record is None:
                raise ContainedGraphTransactionStateError(
                    "contained graph transaction was not prepared"
                )
            if record.state is ContainedGraphTransactionState.ABORTED:
                raise ContainedGraphTransactionStateError(
                    "an aborted contained graph transaction cannot commit"
                )
            if record.state is ContainedGraphTransactionState.COMMITTED:
                return False
            # The PREPARED reservation has participated in every later cycle
            # check, so this transition cannot create a new cycle.
            record.state = ContainedGraphTransactionState.COMMITTED
            record.active_edges = set(transaction.edges)
            return True

    def publish(self, transaction: ContainedGraphTransaction) -> bool:
        """Atomically prepare and commit a one-phase publication.

        A prepared exact replay is committed.  An unseen transaction is checked
        against all other committed and prepared edges before becoming visible.
        """

        self._require_transaction(transaction)
        with self._lock:
            record = self._matching_record(transaction)
            if record is not None:
                if record.state is ContainedGraphTransactionState.ABORTED:
                    raise ContainedGraphTransactionStateError(
                        "an aborted contained graph transaction cannot publish"
                    )
                if record.state is ContainedGraphTransactionState.COMMITTED:
                    return False
                record.state = ContainedGraphTransactionState.COMMITTED
                record.active_edges = set(transaction.edges)
                return True
            self._require_admission_open()
            self._raise_if_cyclic(transaction.edges)
            self._transactions[transaction.transaction_id] = _TransactionRecord(
                transaction,
                ContainedGraphTransactionState.COMMITTED,
                set(transaction.edges),
            )
            return True

    def abort(self, transaction: ContainedGraphTransaction) -> bool:
        """Abort a reservation and retain an identity tombstone.

        An unseen transaction is not mutated: publication cannot be known to
        have begun, so a caller must not invent an ABORTED fact.  Once PREPARED
        exists, its tombstone prevents delayed work from resurrecting a
        compensated transfer-pin transaction.  The return is ``True`` only when
        an existing PREPARED reservation was removed.
        """

        self._require_transaction(transaction)
        with self._lock:
            record = self._matching_record(transaction)
            if record is None:
                return False
            if record.state is ContainedGraphTransactionState.COMMITTED:
                raise ContainedGraphTransactionStateError(
                    "a committed contained graph transaction cannot abort"
                )
            if record.state is ContainedGraphTransactionState.ABORTED:
                return False
            record.state = ContainedGraphTransactionState.ABORTED
            return True

    def release_container(
        self, container_object_id: ObjectID
    ) -> tuple[ContainedReferenceEdge, ...]:
        """Retire a container's graph edges after its pins were released.

        The owner collection path must already have frozen these same full edge
        identities and received every matching pin-release ACK.  Keeping edges
        here until that point makes the authority graph match actual contained
        holds.  The return value lets the composition layer validate its frozen
        plan; exact terminal replay returns an empty tuple.
        """

        if not isinstance(container_object_id, ObjectID):
            raise TypeError("container_object_id must be an ObjectID")
        with self._lock:
            if any(
                record.state is ContainedGraphTransactionState.PREPARED
                and any(
                    edge.container_object_id == container_object_id
                    for edge in record.transaction.edges
                )
                for record in self._transactions.values()
            ):
                raise ContainedContainerBusyError(
                    "container has a prepared contained-edge publication"
                )
            released: set[ContainedReferenceEdge] = set()
            for transaction_id, record in self._transactions.items():
                # Full manifests require an exact manifest-bearing release.
                # Letting the legacy ObjectID-only API touch them would erase
                # the identity needed to prove an ambiguous RPC replay.
                if transaction_id in self._manifests:
                    continue
                if record.state is not ContainedGraphTransactionState.COMMITTED:
                    continue
                matches = {
                    edge for edge in record.active_edges
                    if edge.container_object_id == container_object_id
                }
                if matches:
                    released.update(matches)
                    record.active_edges.difference_update(matches)
            return tuple(sorted(released))

    def snapshot(self) -> ContainedGraphSnapshot:
        with self._lock:
            committed = {
                edge
                for record in self._transactions.values()
                if record.state is ContainedGraphTransactionState.COMMITTED
                for edge in record.active_edges
            }
            prepared = {
                edge
                for record in self._transactions.values()
                if record.state is ContainedGraphTransactionState.PREPARED
                for edge in record.transaction.edges
            }
            manifest_snapshots = tuple(
                ContainedGraphManifestSnapshot(
                    manifest=manifest,
                    state=self._transactions[transaction_id].state,
                    active_edges=tuple(
                        edge for edge in manifest.ordered_edges
                        if edge in self._transactions[transaction_id].active_edges
                    ),
                    released_container_ids=tuple(
                        sorted(
                            container_id
                            for released_transaction_id, container_id in
                            self._released_manifest_containers
                            if released_transaction_id == transaction_id
                        )
                    ),
                )
                for transaction_id, manifest in sorted(
                    self._manifests.items()
                )
            )
            return ContainedGraphSnapshot(
                committed_edges=tuple(sorted(committed)),
                prepared_edges=tuple(sorted(prepared)),
                transactions=tuple(sorted(
                    (transaction_id, record.state)
                    for transaction_id, record in self._transactions.items()
                )),
                admission_closed=self._admission_closed,
                manifests=manifest_snapshots,
            )

    @staticmethod
    def _require_transaction(transaction: object) -> None:
        if not isinstance(transaction, ContainedGraphTransaction):
            raise TypeError(
                "transaction must be a ContainedGraphTransaction"
            )

    def _matching_record(
        self, transaction: ContainedGraphTransaction
    ) -> _TransactionRecord | None:
        if transaction.transaction_id in self._manifests:
            raise ContainedGraphTransactionConflictError(
                "edge-only transaction API cannot mutate a full manifest"
            )
        record = self._transactions.get(transaction.transaction_id)
        if record is not None and record.transaction != transaction:
            raise ContainedGraphTransactionConflictError(
                "contained graph transaction identity names another edge batch"
            )
        return record

    @staticmethod
    def _require_manifest(manifest: object) -> None:
        if not isinstance(manifest, ContainedGraphManifest):
            raise TypeError("manifest must be a ContainedGraphManifest")

    def _matching_manifest(
        self, manifest: ContainedGraphManifest
    ) -> ContainedGraphManifest | None:
        previous = self._manifests.get(manifest.transaction_id)
        if previous is not None and previous != manifest:
            raise ContainedGraphTransactionConflictError(
                "contained graph manifest identity names another publication"
            )
        # The original transaction API remains supported.  Never allow a full
        # manifest identity to alias a pre-existing edge-only transaction.
        if previous is None and manifest.transaction_id in self._transactions:
            raise ContainedGraphTransactionConflictError(
                "manifest transaction_id aliases an edge-only transaction"
            )
        return previous

    def _require_admission_open(self) -> None:
        if self._admission_closed:
            raise ContainedGraphTransactionStateError(
                "contained graph publication admission is closed"
            )

    def _raise_if_cyclic(
        self, candidate_edges: Iterable[ContainedReferenceEdge]
    ) -> None:
        edges = set(candidate_edges)
        for record in self._transactions.values():
            if record.state is ContainedGraphTransactionState.PREPARED:
                edges.update(record.transaction.edges)
            elif record.state is ContainedGraphTransactionState.COMMITTED:
                edges.update(record.active_edges)
        cycle = _find_cycle(edges)
        if cycle is not None:
            raise ContainedReferenceCycleError(cycle)


def _find_cycle(
    edges: Iterable[ContainedReferenceEdge],
) -> tuple[ObjectID, ...] | None:
    """Return one deterministic closed cycle using iterative tri-colour DFS."""

    adjacency: dict[ObjectID, set[ObjectID]] = {}
    vertices: set[ObjectID] = set()
    for edge in edges:
        source = edge.container_object_id
        target = edge.contained_object_id
        adjacency.setdefault(source, set()).add(target)
        vertices.add(source)
        vertices.add(target)

    # 0 = white, 1 = grey (on the current DFS path), 2 = black.
    colour: dict[ObjectID, int] = {}
    for start in sorted(vertices):
        if colour.get(start, 0) != 0:
            continue
        path: list[ObjectID] = [start]
        positions: dict[ObjectID, int] = {start: 0}
        colour[start] = 1
        frames: list[tuple[ObjectID, object]] = [
            (start, iter(sorted(adjacency.get(start, ()))))
        ]
        while frames:
            node, children = frames[-1]
            try:
                child = next(children)  # type: ignore[arg-type]
            except StopIteration:
                frames.pop()
                colour[node] = 2
                positions.pop(node)
                path.pop()
                continue
            child_colour = colour.get(child, 0)
            if child_colour == 0:
                colour[child] = 1
                positions[child] = len(path)
                path.append(child)
                frames.append(
                    (child, iter(sorted(adjacency.get(child, ()))))
                )
            elif child_colour == 1:
                return tuple(path[positions[child]:] + [child])
    return None


__all__ = [
    "ContainedContainerBusyError",
    "ContainedGraphError",
    "ContainedGraphManifest",
    "ContainedGraphManifestDisposition",
    "ContainedGraphManifestReceipt",
    "ContainedGraphManifestSnapshot",
    "ContainedGraphSnapshot",
    "ContainedGraphTransaction",
    "ContainedGraphTransactionConflictError",
    "ContainedGraphTransactionState",
    "ContainedGraphTransactionStateError",
    "ContainedReferenceCycleError",
    "ContainedReferenceGraphAuthority",
]
