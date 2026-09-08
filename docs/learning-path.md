# mini-ray executable learning path

> This is the learning map for the existing backend, not a verified release
> or a second simplified runtime. Read [handoff.md](handoff.md) before running
> anything: this snapshot retains unverified test drafts and a classification
> mismatch. The [correction plan](correction-plan.md) is pending confirmation;
> historical example results below do not approve execution or certify this tree.

mini-ray is best read as an executable systems textbook. Each example isolates
one architectural question, uses at most two logical Nodes and two ordinary
Workers per Node, and explicitly closes every returned `ObjectRef`. Run one at a
time from the repository root after installing the package; PID values vary.

All seven original mains now have individual bounded-runner evidence: a 1 MiB
store per Node, shared get budget, finite public `ObjectRef.close(timeout=...)`
and unconditional shutdown. The receipt timeout does not cancel cleanup or
prove physical GC. Actor/PG synchronous create/remove retain exact replay and
have no per-call cancellation timeout; the 30-second process-tree runner is
the whole-experiment bound, whose expiry is failure rather than a clean ACK.
On the constrained local machine, run the exact reviewed case instead of an
unbounded standalone script. For example (choose one example01–example07 ID):

```bash
python scripts/run_bounded_test.py 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

The source scripts below are the same mains exercised by that test, not a
parallel implementation. Each is loaded without executing its main during
import; the harness observes real init/close/shutdown, original assertions and
output, including all managed PID/owner/trace/Actor endpoint cleanup. Exact
results and limits live in `testing.md`; they do not certify arbitrary changes.

```bash
python examples/01_task_path.py
python examples/02_spillback_direct_submission.py
python examples/03_cross_node_object_pull.py
python examples/04_actor_control_direct.py
python examples/05_nested_get_cpu_yield.py
python examples/06_lineage_reconstruction.py
python examples/07_placement_group.py
```

## Four authorities

| Component | Authoritative for | Deliberately not authoritative for |
|---|---|---|
| `CoreWorker` | logical Task/Object IDs, owner metadata, dependencies, direct submission | node-local resources |
| GCS-lite | membership summaries, Actor/PG coordination, mini-specific publication and contained-graph metadata | ordinary Task placement and object bytes |
| `NodeServer` | local resource ledger, Worker leases, ObjectStore and replica transfer | user execution and logical references |
| Worker / Actor Worker | function execution, result encoding, completion handshake | cluster placement |

GCS does not place ordinary Tasks, but every ordinary Task success currently
depends on synchronous GCS INTENT/ARM ACKs and terminal/adopted reports during
publication. Successful local `Complete` releases lease resources without
waiting for the terminal outbox; full adoption/finish still needs those reports.
GCS retains only manifest/digest/graph metadata, never result bytes. This extra
publication protocol is mini-specific, not production Ray's exact hot path.
The [advanced sections 3A–3D](#advanced-publication-custody-and-recovery), placed
after example 7, separate the current implementation from historical test evidence.

The recurring rule is that logical identity is stable while physical
incarnations may change: `TaskID` survives retries but `AttemptID` does not;
`ObjectID` names a logical return slot while replicas live in Node ObjectStores;
`ActorID` survives the supported Worker restart and Node-loss migration while generation,
route epoch, WorkerID and PID advance.

## Recommended reading order

First pass: [1. Tasks](#1-task-objectref-and-identity) →
[2. Spillback](#2-hybrid-spillback-and-direct-submission) →
[3. Object pull](#3-stored-objects-and-cross-node-pull) →
[4. Actors](#4-actor-control-path-versus-direct-calls) →
[5. CPU yield](#5-blocking-get-as-a-resource-transition) →
[6. Reconstruction](#6-lineage-reconstruction-and-attempt-fencing) →
[7. Placement groups](#7-placement-groups-as-plan-prepare-and-commit).
Then read [3A–3D](#advanced-publication-custody-and-recovery) for publication,
custody and recovery details. This is a reading order, not a runtime switch:
all seven examples use the same single real backend described there. No
alternate teaching backend or omitted protocol guarantee makes the tour work.
The optional [2A locality experiment](#2a-optional-locality-chooses-the-first-lease-hop)
can follow examples 2 and 3; it does not replace or renumber that first pass.

For a reproducible reviewed pure subset, use `python scripts/run_reviewed_pure.py
--list` to inspect its explicit scope, then `python scripts/run_reviewed_pure.py`.
The list operation imports no tests. The execution has a 30-second deadline
and bounded cleanup; it is not the complete default unit gate, and changed
fixtures/imports still require safety review. Examples and real process tests
remain separate, individually reviewed runs.
Bare pytest and directory selectors now fail explicitly before standard test
collection; this guard does not silently shrink the complete gate to a subset.

### 1. Task, ObjectRef, and identity

Start with [`01_task_path.py`](../examples/01_task_path.py). `.remote()` returns a
pending logical handle; `get()` is the separate synchronization operation. Read
[`RemoteFunction.remote` and `get`](../src/miniray/api.py),
[`CoreWorker.submit`](../src/miniray/core.py), ID derivation in
[`ids.py`](../src/miniray/ids.py), then lease and execution entry points
[`NodeServer._handle_request_lease`](../src/miniray/node.py) and
[`WorkerServer._handle_push_task`](../src/miniray/worker.py).

Asynchronous execution does not make submission instantaneous: before returning,
`.remote()` serializes arguments, seals any lifted by-value data and acquires
the required reference holds. These preparation steps may block on their RPCs
or fail synchronously at `.remote()`. It does not wait for task dependencies
to become ready or for user execution; failures of accepted execution are
reported when its results are observed. See the argument-lifting discussion in
[section 3](#3-stored-objects-and-cross-node-pull) and its
[seal-failure/rollback contracts](../tests/unit/test_large_argument_lift.py).

Evidence: [`test_task_path.py`](../tests/integration/test_task_path.py),
[`test_ids_resources.py`](../tests/unit/test_ids_resources.py), and
[`test_cross_process_trace.py`](../tests/integration/test_cross_process_trace.py).
The example also renders the repository's
[`ordinary_task_success.json`](../src/miniray/golden_traces/ordinary_task_success.json)
contract. Its sequence intentionally omits concrete PID, timestamp, event ID and RPC
ID values; inspect `ray.trace()` when the raw diagnostic record is needed.
The success contract now expands the **actual mini-specific publication path**:
Worker Prepare encloses Node→GCS INTENT/ARM; local Complete releases the lease;
after Push returns, owner terminal ACK precedes owner CAS/wake, then adopted
and Node reply-custody retirement. Request/reply rows are separate, so a Push
request is not mistaken for a completed call before Worker execution.

Three observations mean different things: `output_lease_completed` records the
real Node completion reply's resource release; `output_owner_ready` observes
the first owner CAS/wake before adopted; `output_payload_retired` acknowledges
retired Node reply custody, not deleted ObjectStore bytes or a collected ref.
The older `task_finished`/`object_ready` events remain completion-tail
notifications, not the first instant when a result becomes readable or proof
that the logical-task finalizer has drained.

Each publication ACK binds TaskID, AttemptID, LeaseID and manifest digest to
its own real RPC reply; transport `ok` alone is insufficient. Shared-handler
stages cannot reuse one request, and Prepare/Push must contain their own
matching business facts. A Node ACK event observes receipt; the adapter still
performs its original stage/forward-permission/journal validation afterward.
The event itself does not prove that local journal commit has completed.
Node's background terminal report remains an
independent, repeatable branch: the contract does not require its ACK before
local Complete or before the owner's report. This is one isolated no-retry,
reference-free INLINE success path, not every multi-task/failure interleaving.
Missing observations fail the bounded demonstration; they never cause a Task
retry. Making this dependency visible does not remove the GCS/Ray fidelity
difference or change any publication/recovery guarantee.

`ray.export_trace(path)` atomically writes the Driver's current collector
snapshot as deterministically ordered JSONL. This preserves real IDs and causal
fields; it neither normalizes independent runs nor waits for a distributed trace
drain. Its focused pure contracts live in
[`test_trace_export.py`](../tests/unit/test_trace_export.py). The sibling
[`ordinary_task_application_error.json`](../src/miniray/golden_traces/ordinary_task_application_error.json)
shows why a user exception is a successful transport round trip but a terminal task
failure with no system retry.

### 2. Hybrid spillback and direct submission

[`02_spillback_direct_submission.py`](../examples/02_spillback_direct_submission.py)
puts a custom resource only on node 2. The home Node returns a spillback; the
Core preserves lease/task/attempt identity for the targeted hop, then pushes
directly to the granted Worker. Read [`HybridPolicy.schedule`](../src/miniray/resources.py),
[`NodeServer._handle_request_lease_serialized`](../src/miniray/node.py),
[`CoreWorker._execute`](../src/miniray/core.py), and Worker lease completion in
[`worker.py`](../src/miniray/worker.py).

Evidence: [`test_two_node_spillback.py`](../tests/integration/test_two_node_spillback.py),
[`test_two_node_spillback_contract.py`](../tests/unit/test_two_node_spillback_contract.py),
and [`test_spillback_runtime.py`](../tests/unit/test_spillback_runtime.py).
Here one Task owns one LeaseID and a spillback preserves it across two Node hops;
each Node has 1–2 prestarted ordinary Worker slots. Production Ray instead has a
dynamic job/language/runtime-env Worker pool and can pipeline multiple tasks over
one leased Worker, so this example teaches lease authority rather than production
pooling throughput.

`StartLease` also returns the registered publishing Node incarnation. Follow
`WorkerServer._start_worker_lease` into `_output_discovery_session`: this is
where execution permission becomes the identity for selected-output discovery,
without a guessed PID/epoch or another Driver/GCS lookup. Every selected return
is serialized once with its original return index and per-slot storage choice.
The result budget is per slot; the argument budget in section 3 is cumulative.

### 2A. Optional: locality chooses the first lease hop

Read [`preferred_lease_node`](../src/miniray/lease_policy.py), then
`CoreWorker._first_lease_route` and `_execute` in [`core.py`](../src/miniray/core.py).
Before a new ordinary lease, Core scores each Node by distinct stored dependency
bytes: each ObjectID contributes its serialized size at most once per Node.
Positive ties prefer home, then NodeID; no positive score keeps the home first hop.
Current-epoch canonical local-owner metadata may contribute multiple known
replicas; a foreign dependency contributes only its retained descriptor's source.
This does not rewrite dependency sources
or prove that bytes are still present.

Locality answers “which Node should receive the first request?”, not “where must
the task run?”. It does not filter total or available resources. The request's
`preferred_node_id` names the actual first hop, `requester_node_id` still names
home, and `target_node_id` is still `None`. That Node applies Hybrid scheduling
and its local ledger, including spillback back to home. Only the explicit second
hop is targeted. PG bundle routing and already-frozen lease/capacity/cancel/Push,
custody and publication continuations bypass locality re-scoring.

Driver uses its installed snapshot without an address lookup. A Worker without
that snapshot first checks its positive address cache; on a cold miss, this
selection may issue one existing `GetNodeAddress` call with a 0.75-second budget
(or the shorter enclosing deadline). Failure loses only the hint and falls back
to home; it is not Node death. After lookup, Core rechecks authoritative death
and any newly installed snapshot before caching or using the address. This is
not a new mandatory GCS lookup for every task; the separate synchronous GCS
publication dependency remains unchanged.

Evidence entry: [`test_lease_locality_path.py`](../tests/integration/test_lease_locality_path.py).
Its single public-API experiment keeps Driver/owner on A and runs three tasks:

1. A producer requiring B's custom resource requests A, spills back to B, and
   stores a result containing 32 KiB of data there.
2. An unconstrained consumer asks B first and executes on B, without first
   fetching the producer to Driver or creating an A replica.
3. A consumer requiring A's custom resource still asks B first. B spills it
   back to A; A pulls/seals the input, the unchanged owner accepts custody,
   and only then does Core push directly to A's Worker.

The path checks real request/grant/Push identities and normal collection on the
same backend, not a simulated scheduler. Run only its reviewed exact selector with
the bounded runner, following [`testing.md`](testing.md), one case at a time;
run/version evidence lives in [`current-status.md`](current-status.md). This is
a Driver installed-snapshot slice, not evidence for Worker cold-lookup failures,
all multi-replica races, throughput, or complete Ray fidelity. Reusing a warm
Worker process here still requires a new LeaseID for a new task attempt; it does
not implement leased-Worker reuse or production task pipelining. Locality does
not remove the global contained DAG or phase-specific publication guarantees.

The separate [Worker cold-route experiment](../tests/integration/test_worker_lease_locality_path.py)
keeps a Driver-owned 32 KiB source on B, passes its reference nested to a zero-CPU
parent on A, and lets that Worker's real embedded Core submit two children to B.
Its first-hop scopes observe one cold GetNodeAddress query followed by a cache
hit, without installing a snapshot or choosing a route in the test. Count
`(1, 0)` only within those scoring scopes: adoption still has independent
address queries. It also follows both child outputs' foreign lineage Release
ACKs, the parent's borrowed-handle release, then Driver parent/source GC while
both Workers remain alive. This is a bounded successful cold-cache path, not
cold-query failure, concurrent miss, or node-loss coverage.

### 3. Stored objects and cross-node pull

[`03_cross_node_object_pull.py`](../examples/03_cross_node_object_pull.py) forces a
64 KiB result into the source store and consumes it on node 2. Keep three forms
separate: an `ObjectRef` is a logical handle, an `ObjectStoreDescriptor` is
byte-free metadata, and sealed bytes belong to a Node. Read
[`CoreWorker._prepare_task_dependencies`](../src/miniray/core.py),
[`NodeServer._localize_one_dependency`](../src/miniray/node.py), the pull state
machine in [`object_manager.py`](../src/miniray/object_manager.py), physical
storage in [`object_store.py`](../src/miniray/object_store.py), and logical owner
state in [`ownership.py`](../src/miniray/ownership.py).

Evidence: [`test_cross_node_dependency_pull.py`](../tests/integration/test_cross_node_dependency_pull.py),
[`test_cross_node_dependency_pull_contract.py`](../tests/unit/test_cross_node_dependency_pull_contract.py),
and [`test_node_dependency_pull.py`](../tests/unit/test_node_dependency_pull.py).

The same storage path also carries over-budget by-value arguments. Read
`StoredArg` in [`protocol.py`](../src/miniray/protocol.py) and the cumulative
argument budget in [`CoreWorker.submit`](../src/miniray/core.py): the serialized
container becomes one storage dependency, while its nested ObjectRefs remain
separate lifetime/import edges. Worker decoding reuses one attempt-wide
`NestedReferenceImportSession`. The nested-argument increment has focused
contracts in [`test_large_argument_lift.py`](../tests/unit/test_large_argument_lift.py)
and a dedicated bounded witness in
[`test_nested_large_argument_path.py`](../tests/integration/test_nested_large_argument_path.py).
This is separate evidence from the existing object-pull smoke.

Materializing a top-level RefArg can itself produce a container of ObjectRefs.
Those serialized result reducers have no explicit TaskArg nested manifest.
Follow Worker's scoped `importing_references` and
`NestedReferenceImportSession.resolve_exported`: they acquire through the
original contained hold and join the same attempt lifetime as manifest imports,
without pretending that their credential is a Task hold. Positional/keyword
duplicates share one imported handle per exact source; later decode failure
releases in reverse order. Returned children stay live through output promotion.
The importer is absent during user code and result serialization. Plain values
do not invoke this lazy embedded-Core callback. Imported parameter handles are
attempt-scoped even if user code stores an alias in a global.

### 4. Actor control path versus direct calls

[`04_actor_control_direct.py`](../examples/04_actor_control_direct.py) shows the
split: creation is coordinated through GCS; invocation uses the returned Actor
Worker endpoint directly. Read [`ActorClass` and `ActorHandle`](../src/miniray/api.py),
[`CoreWorker.create_actor` and `submit_actor_call`](../src/miniray/core.py),
[`ActorCoordinator.create`](../src/miniray/control.py), Actor reservation in
[`node.py`](../src/miniray/node.py), and the generation-fenced FIFO mailbox in
[`actor_worker.py`](../src/miniray/actor_worker.py).

Evidence: [`test_actor_k0_path.py`](../tests/integration/test_actor_k0_path.py),
[`test_actor_restart_path.py`](../tests/integration/test_actor_restart_path.py),
[`test_actor_node_loss_migration_path.py`](../tests/integration/test_actor_node_loss_migration_path.py),
[`test_actor_public_runtime_contract.py`](../tests/unit/test_actor_public_runtime_contract.py),
[`test_actor_restart_control.py`](../tests/unit/test_actor_restart_control.py),
and [`test_actor_protocol.py`](../tests/unit/test_actor_protocol.py). The restart
same-Node slice replaces a dedicated Actor Worker on a still-live Node; the
Node-loss slice migrates it to a survivor. Both preserve ActorID, advance
generation/route/Worker incarnation, fail old in-flight calls without replay,
and reset constructor state. Method retry, named/detached lifetime and PG Actor
remain omitted.

### 5. Blocking get as a resource transition

[`05_nested_get_cpu_yield.py`](../examples/05_nested_get_cpu_yield.py) would
deadlock with one CPU if its parent merely blocked. The parent returns only
CPU to the Node and the child runs in the second Worker slot. On unblock the
parent immediately restores its logical allocation, allowing signed CPU debt
instead of waiting for free capacity again; the scheduler sees availability
clamped to zero. Read Worker-local Core creation in
[`worker.py`](../src/miniray/worker.py), [`BlockingNotifier`](../src/miniray/blocking.py),
the Node's blocked/unblocked handlers in [`node.py`](../src/miniray/node.py), and
parent execution binding in [`runtime_binding.py`](../src/miniray/runtime_binding.py).

Evidence: [`test_blocking_get_cpu_yield_path.py`](../tests/integration/test_blocking_get_cpu_yield_path.py),
[`test_cpu_yield_accounting.py`](../tests/unit/test_cpu_yield_accounting.py), and
[`test_worker_blocking_binding.py`](../tests/unit/test_worker_blocking_binding.py).

There is another lock boundary when a LOST result waits for its old finalizer
or a dependency's finalizer. Read the two LOST branches of `CoreWorker.get`:
inspect the predicate under the condition lock, enter the notifier outside it,
then reacquire and recheck before `Condition.wait`. Recompute the remaining
time from the original deadline after notification entry. Unblock also runs
after leaving the condition lock. The
[pure lock-order regressions](../tests/unit/test_core_lost_blocking_lock_order.py)
use actual publication/drop/lineage state and inert waits, not a running OS
deadlock. They also check completion during notifier entry and exhausted wait
budget. Sharing a notifier across user threads requires explicitly binding the
same execution context; a new thread does not inherit it automatically.
This is not a new total timeout/cancellation guarantee for notifier RPCs.

The ordinary local PENDING wait and foreign PENDING/retryable-LOST polls also
subtract notification-entry time from the original get budget. With no time
left, a local already-signalled event permits the usual owner-state recheck;
it does not declare the object successful by itself. Foreign gets cannot infer
readiness locally and start neither another poll nor another owner query after
this boundary expires. The
[deadline regressions](../tests/unit/test_get_notification_deadline.py) preserve
inline/error precedence and one aggregate episode with simulated time. This
bounds the next wait, not the total duration of notification/Unblock RPCs.

Read notification entry as a small transaction too: failed lock entry must not
leave a false nested depth, and a Block that was never constructed must not
spend a Node episode sequence. Groups retain an exit obligation only after
successful entry. The [entry-failure contracts](../tests/unit/test_blocking_notifier_entry_failure.py)
exercise these boundaries with injected local exceptions and typed reply
callbacks, not delivered OS signals or memory exhaustion. Once Block may have
been sent, the original matching Unblock compensation still applies.

### 6. Lineage reconstruction and attempt fencing

[`06_lineage_reconstruction.py`](../examples/06_lineage_reconstruction.py) uses
`drop_object()` only as a teaching failpoint. Dropping the last replica makes
the logical object LOST; `get()` starts or joins a replay with a new AttemptID
while TaskID/ObjectID stay stable. Read [`CoreWorker.drop_object` and
`_start_or_join_reconstruction`](../src/miniray/core.py),
[`ReconstructionCoordinator`](../src/miniray/reconstruction_runtime.py),
[`RecoveryManager`](../src/miniray/recovery.py), and stale-publication fencing in
[`ownership.py`](../src/miniray/ownership.py).

The example prints the actual attempt transition as well as the stable IDs.
Its producer and `first == second` assertion are unchanged. Only after both
gets succeed does `_observed_attempts` match the Driver's `task_submitted` and
`object_reconstruction_started` records for that exact Task/Object, validate
attempts 0/1 and process-local order, then print their recorded values. At most
201 snapshots and two seconds of delivery observation fit inside the same
ten-second work budget. Missing trace is an observation failure, not evidence
that reconstruction failed; no complete distributed trace is required.

Evidence: [`test_lineage_reconstruction_path.py`](../tests/integration/test_lineage_reconstruction_path.py),
[`test_core_reconstruction_runtime.py`](../tests/unit/test_core_reconstruction_runtime.py),
[`test_reconstruction_runtime.py`](../tests/unit/test_reconstruction_runtime.py),
and [`test_drop_object_replica.py`](../tests/unit/test_drop_object_replica.py).

For the next dependency question, inspect the original
[three-level recursive experiment](../tests/integration/test_recursive_lineage_reconstruction_path.py).
It drops leaf, middle and root, confirms all three are LOST at attempt 0, then
uses only one root get to reconstruct the graph. Each producer advances once
to attempt 1 while canonical lineage and Task/ObjectIDs remain unchanged.
This is an explicitly approved three-drop/three-reconstruction composite
experiment on one two-CPU Node, not a single-fault smoke or a benchmark.
Follow its exact-scope exception in [testing](testing.md) and run it alone;
normal owner GC must collect all three after public closes.

Two related boundaries use the same runtime, with separate exact experiments:
[foreign-owner reconstruction](../tests/integration/test_foreign_reconstruction_path.py)
keeps lineage at the Worker owner while the Driver requests reconstruction;
[foreign-input reconstruction](../tests/integration/test_foreign_input_lineage_reconstruction_path.py)
keeps a retained input alive after the original handles close, then replaces
its hold from origin attempt 0 to 1 before replaying the consumer. The latter
checks final local-output/foreign-lineage cleanup while the source owner lives,
not a directly observed remote source collection. Internal foreign RPC retries
keep their own finite deadlines under the outer runner; a public timeout is
not transaction cancellation. Follow each test's reviewed scope in testing.

### 7. Placement groups as plan, prepare, and commit

[`07_placement_group.py`](../examples/07_placement_group.py) creates two one-CPU
bundles with `STRICT_SPREAD`, then binds one task to each committed bundle. The
public create is synchronous: its immutable handle is exposed only after every
participant commit ACK, and each task carries the exact bundle scheduling key
back to the planned Node. Read the pure planner and child resource ledgers in
[`placement.py`](../src/miniray/placement.py), the obligation reducer in
[`placement_group_runtime.py`](../src/miniray/placement_group_runtime.py), GCS
coordination in [`control.py`](../src/miniray/control.py), and Node reservation
handling in [`node.py`](../src/miniray/node.py).

Before submitting either Task, the example prints the actual committed PGID,
attempt and bundle→NodeID mapping from that public handle, then checks the
two distinct Nodes against the real runtime context. `STRICT_SPREAD` makes
different Nodes a hard constraint, not a preference. This two-one-CPU-Node
example cannot distinguish it from PACK: two one-CPU bundles already require
both Nodes. No intermediate PREPARE/COMMIT event log is fabricated, and this
synchronous success example does not observe visibility during PREPARE. Read
the pure coordinator tests for that boundary. Tracing remains disabled; no
extra Task, control RPC, fault or observation wait was added.

Evidence: [`test_placement_group_path.py`](../tests/integration/test_placement_group_path.py),
[`test_placement_group_runtime.py`](../tests/unit/test_placement_group_runtime.py),
[`test_placement_child_ledgers.py`](../tests/unit/test_placement_child_ledgers.py),
and [`test_placement_group_public_api.py`](../tests/unit/test_placement_group_public_api.py).

## Advanced: publication, custody, and recovery

After the seven-example tour, revisit the object path through sections 3A–3D.
Their existing labels and section anchors are retained for earlier links.
For per-slot recovery, the existing
[partial multi-return experiment](../tests/integration/test_multi_return_partial_reconstruction_path.py)
compares a full three-slot publication with a later target-only publication
of slot 1. Both user invocations compute all returns; healthy slots 0/2 keep
their original owner snapshots and attempt 0. Final collection removes all
three only after their public references close. This is one drop/reconstruction,
not selective execution of just part of the function.

### 3A. Advanced: one publication for all selected outputs

The actual ordinary-Task success path now uses the same protocol for plain and
contained results, single and multiple returns, and targeted reconstruction.
INLINE versus STORED only changes per-slot materialization. Read this path after
the introductory examples; it is not an eighth introductory example.

Read it in this order:

1. [`OutputDiscoverySession`](../src/miniray/output_discovery.py) discovers and
   serializes every selected slot once before external effects. Worker
   `_PreparedOutputReply` retains streams, source handles and the argument import
   transaction; `_resume_discovered_outputs` sends one `PrepareOutputPublication`.
2. Follow its registered handler in [`node.py`](../src/miniray/node.py) into
   [`OutputPublicationNodeAdapter`](../src/miniray/output_publication_node.py) and
   [`OutputPublicationJournal`](../src/miniray/output_publication_journal.py):
   INTENT ACK, child prepare, one graph reservation, per-slot materialization,
   child promotion and ARM ACK. Only needed effects run; even plain success
   still uses INTENT/ARM.
3. [`output_recovery.py`](../src/miniray/output_recovery.py) owns metadata-only GCS
   facts; [`contained_cycle.py`](../src/miniray/contained_cycle.py) atomically
   rejects the complete edge batch if it would create a cycle.
   `PublicationControlAdapter` in [`control.py`](../src/miniray/control.py) owns
   their composition, recovery tickets and effect replay. GCSLite supplies
   current local membership observers and fence-ready owner deaths through
   explicit calls; it does not reach into the adapter's private cleanup state.
   This boundary does not remove the mini-specific synchronous GCS dependency.
4. Node `_handle_complete_output_worker_lease` composes local journal Complete
   with lease/resource release, without a GCS round trip. Worker may cache the
   exact Complete envelope only after local source/import custody has drained.
5. [`CoreWorker._drive_output_publication_adoption`](../src/miniray/core.py)
   reports terminal, commits the graph when present, and calls the selected-set
   owner CAS in [`ownership.py`](../src/miniray/ownership.py). It then reports
   adopted and acknowledges Node payload retirement. GCS report failures retain
   a finish obligation, not a new user execution.
6. Follow `_reference_released` into per-slot collection: child release, graph
   `RELEASE_CONTAINER`, replica cleanup, then owner metadata. The final sibling
   releases task lineage; collecting one slot must not invalidate the other.

The central invariant is that local Complete cannot be rolled back. Preparation
and completion ambiguity reuse retained bytes and exact identities. Successful
remote promotion is not proof that local source/import release finished; failed
Complete is not accepted until compensation converges. Read the contracts in
[`test_worker_unified_output.py`](../tests/unit/test_worker_unified_output.py),
[`test_output_publication_node_server.py`](../tests/unit/test_output_publication_node_server.py),
and [`test_core_output_publication.py`](../tests/unit/test_core_output_publication.py).
These are source references, not a new pass report; verification history lives
in `current-status.md`.

When following the dispatcher, separate a queue turn from the Task's current
state. `_ReadyTask.kind` is derived from exactly one retained continuation (or
FRESH when there is none). Only FRESH checks new PG admission; output adoption
and output-loss work may still need to run after the owner leaves PENDING.
The tag does not replace the current protocol marker or its authority: an old
queued cancellation can legitimately meet a newer custody record. The small
[envelope contracts](../tests/unit/test_dispatch_kinds.py) teach this routing
boundary; the publication/PG tests above validate real protocol effects.

An output owner can also be its executor: after a remote Node dies, a surviving
Worker may run the child it originally submitted. Read the token construction
in [`OutputDiscoverySession._export`](../src/miniray/output_discovery.py) and the
validator in [`publication_sources.py`](../src/miniray/publication_sources.py).
For this case only, the provisional token is `provisional:<final-token>`; the
final serialized hold remains unchanged. Distinct owners retain the original
shared-token rule. The two lifetimes still need an atomic prepare/promote
exchange, even though WorkerID does not change. The
[`same-owner custody contracts`](../tests/unit/test_same_owner_output_custody.py)
exercise real child tables, discovery, publication and per-slot collection;
they do not add another publication backend or authorize an expired borrower.

### 3B. Advanced: per-slot Node-loss recovery without GCS bytes

Read the actions in [`output_recovery.py`](../src/miniray/output_recovery.py),
the registered handlers in [`control.py`](../src/miniray/control.py), and
`CoreWorker._drive_output_node_loss` in [`core.py`](../src/miniray/core.py).

Intent without ARM permits pre-Complete rollback. ARM without a successful
witness is `COMPLETION_UNKNOWN`, not proof that Complete never happened; a
known successful terminal leads to `POSTCOMPLETE_RESOLVE`. Frozen metadata
contains no bytes. The owner chooses per slot: INLINE KEEP requires retained
data; STORED KEEP requires an already-adopted slot with an owner-advertised,
grant-backed secondary, excluding the publisher and known-dead Nodes. A lost
STORED replica cannot be manufactured from a descriptor.
Exact cleanup fences late delivery before resolving DROP slots to LOST.
Final application uses the current replica set, not a saved decision-time
route. A secondary lost after KEEP leaves LOST plus its publication membership
for explicit retirement; it does not change KEEP or revive old bytes. See the
[adoption-tail secondary test](../tests/integration/test_output_surviving_replica_path.py)
for real pin/pull/grant/contained import, unchanged producer attempt and GC.
Follow late reports through `ObjectOwnerTable.retired_output_replica` into
[`ReplicaCleanupQueue`](../src/miniray/replica_cleanup.py). Rejection prevents
execution, while the exact retained Drop obligation prevents forgetting the
already-sealed bytes. Consumer cancellation only unpins; Node deletion ACK or
installed death releases cleanup custody. A Node deletion watermark also
prevents old dependency bytes from being pulled back afterward. The
[late-DROP test](../tests/integration/test_late_output_replica_cleanup_path.py)
separates these events using two existing dispatch lanes, with no fake reply.
For multiple owners, follow `_LocationReportState`: permission to execute and
custody of sealed bytes are different facts. CUSTODY_ONLY rejects execution
while retaining a current replica; RETIRED transfers deletion to its owner.
The first failure cancels the grant promptly, but later owners still receive
their reports. Exact receipt/cancel/death progress survives ACK loss and old
queued work. A conflicting reply without custody proof is quarantined, not
blindly treated as permission to delete. The
[two-foreign-owner failure test](../tests/integration/test_multi_owner_handoff_failure_path.py)
kills only the first owner Worker and proves the healthy second owner still
receives custody. Local reports now use the same driver: the pure inventory
builder records all foreign reports and the actual grant before any local
mutation. A shared owner helper checks real SUBMITTED versus RETAINED holds;
local receipt progress survives route/CAS exceptions and only missing effects
replay. The [local route-failure test](../tests/integration/test_local_replica_handoff_failure_path.py)
shows checkpoint, cancel, foreign handoff and local repair in that order,
without executing the consumer. Its separate executor-exit case demonstrates
why a rejected Cancel is not a cancellation ACK: the living Node's exact
WORKER_LOST outcome fences execution while both input owners keep their bytes.

Now consider losing **every** Grant reply. The Node has already sealed input
replicas, but the submitter does not yet know its Worker or local descriptors.
Cancellation therefore returns `retired_grant`, the Node's historical committed
inventory, not new execution permission. Follow `_resolve_lease_cancellation`
back into the same `_LocationReportState`: the input owners take custody before
the consumer exposes its original error. A retained valid Cancel reply can
survive a report-builder failure without another cancellation RPC. Read
[`test_ambiguous_grant_custody.py`](../tests/unit/test_ambiguous_grant_custody.py)
and the pure unlock interleavings in
[`test_lease_cancel_handoff_interleavings.py`](../tests/unit/test_lease_cancel_handoff_interleavings.py).
The [real lost-Grant test](../tests/integration/test_ambiguous_grant_custody_path.py)
discards twelve actual cached Grant replies, then verifies both owner handoffs
and final physical GC through the existing runtime; its pass is recorded in
`current-status.md`.
Missing/corrupt Grant fields require exact replay, never invented replica facts.
An absent historical grant still does **not** prove that an earlier partial
localization created no bytes. Follow the independent `LeaseDependencyInventory`
through [`lease_dependencies.py`](../src/miniray/lease_dependencies.py): one
request records candidates before effects and witnesses each sealed/reused
replica, even if a later dependency or source-release ACK fails. Cancel freezes
that subset; the same owner handoff works without inventing a Worker grant.
Only the final explicit custody ACK releases the Node's pending obligation.
The [pre-grant example test](../tests/integration/test_pregrant_custody_path.py)
really drops the second source after the request is frozen, then verifies the
first replica is handed off and later collected.

Read [`transfer_pins.py`](../src/miniray/transfer_pins.py) as a separate lifetime:
an active source reader is not eligible for background release, and losing a
Pin ACK does not prove the pin never existed. The exact close is retained
before send, then driven after the reader leaves; source close tombstones make
Release-before-Pin safe. A typed source-Node death can discharge the target's
obligation; target-Node death instead requires the living source to unpin its
session. The [requester-death test](../tests/integration/test_transfer_pin_requester_death_path.py)
keeps a second same-object reader alive to demonstrate why these scopes differ.
The [ACK-loss cases](../tests/integration/test_transfer_pin_ack_loss_path.py)
exercise actual spawned Nodes while discarding validated real replies; Pin
loss leaves no target bytes, while Release loss keeps a sealed target replica
and a separately pending source-close obligation.
The submitting Worker is a different authority from either Node or input owner.
Follow the Request's frozen `DependencyOwnerRoute` into Node's abandoned
inventory driver: after confirmed submitter death, it offers each replica to
the original owner with custody-only semantics. A running executor keeps its
pins; only its normal completion can release them. If a put was already
collected, compact owner-originated history authorizes exact late cleanup
without recreating the object. The [submitter-death test](../tests/integration/test_abandoned_dependency_custody_path.py)
exits the real submitting Worker after Grant and demonstrates this handoff,
including unchanged ownership and final physical GC.

The last Core lock before `push_send` is an execution-admission boundary, not a
network lock. Cancellation selected before it prevents Push; a later cancel
cannot retract bytes already admitted and must rely on Node Start arbitration.

A known Complete can therefore leave a mixed result: the received INLINE slot
remains readable while the STORED slot is LOST. Explicit `get` requests
reconstruction only for lost slots. Unknown Complete instead uses budgeted
SYSTEM retry after cleanup. Reconstruction and last-reference GC stay behind
the old logical-task finalizer until input holds and accepted accounting are
settled; a late old finalizer must not retire the successor execution.

The new [`mixed Node-loss test`](../tests/integration/test_multi_output_node_loss_path.py)
specifies a real publisher crash after Complete delivery, retained INLINE data,
and selected STORED reconstruction on the survivor. Its current bounded run is
recorded in `current-status.md`, alongside mixed-contained reconstruction and
the adopted-owner-death cleanup path. Earlier single-return INLINE KEEP/DROP
and stored Node-loss passes do not certify other unified failure windows.
The single [`output gate`](../src/miniray/publication_gate.py) now exposes four
phases with full selected-output identity: INTENT before effects, promotions
before ARM, ARM before observed Complete, and successful Complete before either
Node result exit. The migrated [`stored fault tests`](../tests/integration/test_stored_outer_node_loss_path.py)
exercise all four, including one executor-owned-child `COMPLETION_UNKNOWN`
case. The [`INLINE tests`](../tests/integration/test_inline_node_loss_path.py)
use the same gate for received KEEP and unreceived DROP. Subsequent F1/F2 cases
cover shared-borrowed mixed outputs and targeted selected slots. F3 observes a
real local Complete while withholding terminal/delivery, without feeding the
observer's witness to Core. Their bounded evidence and exact remaining scope
live in [`current-status.md`](current-status.md) and
[`acceptance-matrix.md`](acceptance-matrix.md), not an assumed full cross-product.

Node-death propagation also reaches a Worker-side owner. Follow Driver
`_install_recovery_snapshot` and `_publish_installed_node_deaths` in
[`api.py`](../src/miniray/api.py): every survivor must first ACK the same installed
snapshot. [`InstalledNodeDeathView`](../src/miniray/node_death_view.py) retains
that ACK vector, snapshot and cumulative exact GCS death records on each Node.
Worker `_embedded_core_for` reads it before exposing a lazy Core; the existing
Core coordinator polls the same local Node afterward, including during drain.
There is no new detector or per-Task GCS death query. Missing/old replies never
create a death, and successor validation cannot forget or resurrect a Node.
Partial local application keeps retry work; the applied-view marker advances
only after all records are consumed. The
[`Worker-owner Node-loss test`](../tests/integration/test_worker_owner_node_loss_path.py)
keeps owner A alive, crashes publisher B at ARM, then observes A resolve UNKNOWN
and retry on itself before the Driver requests the result. Owner identity stays A.

### 3C. Targeted contained output and historical-contract coverage

[`OutputOwnerPublicationPlan`](../src/miniray/ownership.py) publishes selected
slots atomically. Each owner entry keeps only its own bytes/descriptor and
metadata membership, not a payload-bearing copy of all siblings. The targeted
path first retires old selected membership, then installs a new attempt for the
same logical return indices. Healthy siblings remain unchanged. The function
still runs in full: targeting restricts publication, not computation.

The new [`mixed contained-output test`](../tests/integration/test_multi_contained_output_path.py)
specifies two different storage tiers sharing one borrowed child, distinct
per-slot holds, loss/reconstruction of return index 1, and independent GC.
Read it beside [`test_output_owner_retirement.py`](../tests/unit/test_output_owner_retirement.py)
and [`targeted_reconstruction.py`](../src/miniray/targeted_reconstruction.py).
Multi-return and targeted-contained wiring has the narrow bounded acceptance
recorded in `current-status.md`; the wider fault matrix is still unfinished.

Core, Node and GCS no longer select or construct the legacy INLINE/STORED
publication lifecycles. `PublicationControlAdapter` owns one graph/output
recovery composition boundary; membership death freezes that work before any
remote cleanup. Tier-neutral capabilities now live in `publication_sources.py`.
Owner legacy publication/retirement/GC associations have also been removed.
Standalone old lifecycle models are archived; their wire fields, InlineInstall
facade and raw reply-edge cleanup have been removed. A TaskReply's only
publication authority is its unified manifest/envelope. Do not learn three
protocols as the intended final architecture. The Worker-side
stored coordinator and two old gate implementations were also removed.
Archived contracts and active replacements are mapped in
`history/retired-core-publication`, `history/retired-node-publication` and
`history/retired-gcs-publication`, `history/retired-owner-publication` and
`history/retired-protocol-family`. Run
only reviewed exact integration IDs through the bounded runner, one at a time.

### 3D. Owner death before Complete and real-wire cycle rejection

The [F4/F5 cases](../tests/integration/test_precomplete_output_owner_death_path.py)
kill only the actual outer-owner Worker at INTENT or PROMOTIONS. Both Nodes,
the executor Worker and the Driver-owned child survive. Watch the exact Node
fences and child-release replies before opening the held Prepare gate; waiting
for `owner_cleaned` first would deadlock against the adapter ticket still held
by Prepare. After release, forward publication is rejected and the same live
executor must acknowledge finalization before GCS records `owner_cleaned`.
No successful Complete, owner takeover or child-owner death is invented.
These are independently exercised single-slot boundaries, not all combinations.

The [F7 case](../tests/integration/test_contained_cycle_control_path.py) deliberately
separates two questions. Metadata-only A→B and B→A proposals name a real
registered publisher and pass through unified INTENT and real GCS TCP handlers.
The second PREPARE is rejected atomically; exact ABORT receipts close those
control obligations and fence late requests. These model ObjectIDs are not
publicly created references or evidence of acquired child custody. A separate
ordinary task returns a genuine Driver child; its actual Complete/adoption and
owner GC provide the positive COMMIT/RELEASE evidence. `GetContainedGraph` may
still return a manifest after ABORT/RELEASE: retained identity is not active edges.
This is a control-protocol test plus an ordinary GC path, not a public-API cycle
construction or scheduler-concurrency test.

## Implemented boundary and upcoming K1 work

The seven examples exercise implemented vertical paths: ordinary Tasks and stable
IDs, two-node spillback/direct submission, stored dependency pull, K0 Actor
creation/direct FIFO calls, blocking-get CPU yield/reacquire, and
single-output reconstruction, including a local-owner recursive DAG, and a
two-bundle placement group with synchronous commit visibility. Returned nested ObjectRef
ownership and stored physical collection have focused acceptance tests
([`test_contained_ref_lifecycle_path.py`](../tests/integration/test_contained_ref_lifecycle_path.py)
and [`test_stored_physical_gc_path.py`](../tests/integration/test_stored_physical_gc_path.py));
they are advanced lifecycle slices rather than extra introductory examples.
Before reading those runtime paths, compare the two meanings of a cycle in
[`contained_cycle.py`](../src/miniray/contained_cycle.py) and
[`test_contained_cycle_policy.py`](../tests/unit/test_contained_cycle_policy.py).
A Python list that contains itself is one serializer-local value and creates no
logical edge. An outer result containing an `ObjectRef` creates an
`ObjectID -> ObjectID` lifetime edge; mini-Ray's chosen teaching policy keeps
that graph a DAG through an atomic prepare/commit reservation. The unified
runtime reserves all selected INLINE/STORED edges together and releases each
container's edges during reverse GC.
Driver-owned ObjectRefs inside Task argument containers now have a separate
public acceptance path in
[`test_nested_task_argument_path.py`](../tests/integration/test_nested_task_argument_path.py):
the logical Task hold is a complete `(kind, submitter, TaskID, origin AttemptID)`
credential installed before the source handle may close. SYSTEM retry preserves
that incarnation, while reconstruction creates one from its new AttemptID. The
execution Attempt imports and releases its own borrower without turning the
nested handle into a readiness dependency.
This particular acceptance test uses an already-READY put: its two blockers
prove sender-close-before-Push, not timing with a pending source. The separate
[local nested reconstruction test](../tests/integration/test_local_nested_reconstruction_path.py)
keeps the container itself INLINE while source/result bytes are STORED. Its
preflight contains only the producer result, not the nested source. Lowering
the threshold enough to lift that container instead creates a real StoredArg
storage dependency, which correctly enters the DFS; these are distinct cases.

Additional bounded paths cover Driver-local Node route migration, local nested
and foreign-owner/foreign-input reconstruction, multi-return all-lost and
partial-loss reconstruction, Actor Node-loss migration, PG terminal `LOST`, and
four ordinary-task cross-PID RPC causal edges. These are historical bounded
results, not an automatic same-version unified-backend regression. F1–F5 now
have their specified narrow scenarios; F7 has the explicitly separated
control-only cycle and public GC evidence described above. The separate
[F6 foreign late-replica case](../tests/integration/test_foreign_late_output_replica_cleanup_path.py)
holds a real foreign consumer's grant until the original Worker owner latches
DROP, then checks RETIRED/custody, cancel-without-Push and autonomous physical
cleanup. It observes absence before replaying any Drop, and uses public get to
reconstruct before proving old report/Drop replay leaves the new epoch and
healthy sibling untouched. Source credentials settle through a real lifetime
predicate before full-snapshot comparison; GCS adopted alone is not that barrier.
Remaining K0/K1 work includes
broader mixed/targeted ownership/failure combinations, historical-contract
replacement coverage, asynchronous-queue traces, safe test classification, and
all required same-version gates. No counters here claim those gates passed.
The K0/K1 exit criteria, global fail-fast DAG and phase-specific recovery
contracts are unchanged. Placement-group bundle
rescheduling remains explicitly omitted; the implemented Node-loss behavior is
terminal LOST plus survivor cleanup. Pure planners or state machines do not make
these end-to-end features. Continue with
[`design.md`](design.md), [`roadmap.md`](roadmap.md), then [`testing.md`](testing.md).
