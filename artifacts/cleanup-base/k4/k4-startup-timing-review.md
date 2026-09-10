# K4 startup timing review

## Conclusion

The reviewed K4 changes do **not** introduce a demonstrated `Node.worker_id`
property deadlock, pre-lock property access, or registration-under-state-lock
cycle. Several failed attempts reached a complete, valid `NodeStartup` and
tried to send success after the Driver's five-second deadline had expired.
That is evidence of missed readiness timing, not a permanently blocked new
property. Other attempts timed out earlier while waiting for the Worker pipe;
those logs do not identify the delayed Worker subphase.

K4 nevertheless has a material timing/reliability problem in this batch. It
must not be accepted by labeling every timeout flaky or retrying until green.
The existing evidence supports **an independently demonstrated environment/
time-dependent slowdown plus a preexisting tight nested-startup deadline**,
while it does not yet quantify any additional K4 contribution. A controlled
same-environment cache experiment is justified; the five-second startup and
30-second process-tree limits should remain unchanged.

## Inputs and cutoff

Read-only comparison used the actual `project.tar` bytes from:

- `audit/two-version-cleanup/execution/base-k3p-01`, original runs 000–040.
- `audit/two-version-cleanup/execution/base-k4d-01`, runs 000–047 as visible
  during this review. Later appended runs are outside these statistics.
- Root's newly appended control: K3p `041-smoke.log` for example04.

The K4 archive belongs to the working-tree candidate based on
`42ed62d7231d17d3b7f703670a357c509183e853`, not an alleged clean commit. Its
archive SHA is `3a200dec3c9017740b399ea65c0b0a33136d598960305f8d63cc734e61b9269d`.
Current Node/Worker/Core/OwnerService files match the corresponding K4 frozen
source used below. Archive members were read in memory; none were executed.

Recorded Python, WSL kernel and package versions match between the batches:
Python 3.12.13, WSL2 kernel 6.18.33.2, pytest 8.4.2, cloudpickle 3.1.2. Those
facts do not establish equal host load, CPU scheduling, filesystem/cache state,
memory pressure or concurrent audit activity; the records contain no such
telemetry.

## Actual timing evidence

| Observation | K3p original | K4d through run 047 |
|---|---:|---:|
| First attempts of the same 32 smoke selectors | 31 pass / 1 fail | 21 pass / 11 fail |
| Smoke attempts including retries | 33 | 46 |
| Failed smoke attempts | 1 | 17 |
| Failure classification | 1 late Node readiness | 6 late Node success sends, 8 Worker-ready timeouts, 3 outer 30-s timeouts |
| Complete same-selector successful pairs available | 29 | 29 |
| Median outer elapsed for those pairs | 12.446 s | 18.501 s |
| Median pytest elapsed for those pairs | 10.65 s | 14.85 s |

The median paired ratio is 1.617 for outer elapsed and 1.558 for pytest
elapsed. These are whole-case measurements, not startup-only profiles.
K4 pure gate also rose from 4.17 to 6.81 seconds inside pytest (343 passed,
one deselected in each); this provides a broader slowdown signal but is not
alone a controlled performance comparison.

The root's fresh control re-ran **unchanged frozen K3p example04**:

| Same K3 selector | Outer | Pytest |
|---|---:|---:|
| Original `013-smoke.log` | 9.666 s | 8.70 s |
| New `041-smoke.log` | 16.328 s | 14.20 s |

The old source became 1.689× slower outside / 1.632× inside pytest without
K4. This establishes a time/environment component. It neither proves all K4
effects harmless nor converts the remaining K4 failures into passes. The
remaining selectors at cutoff have no successful K4 record: example04,
startup rollback, and pre-Complete owner death.

### Failure locations

- Late valid Node success after the Driver deadline: K4 logs
  `005,028,030,035,042,047`. These show
  `node_main: ready_connection.send((True, startup))` and `BrokenPipeError`.
  Driver `api.py:2113–2118` has already closed the receive pipe following its
  readiness timeout. K3p `020-smoke.log` has the same causal shape at the old
  Node line 7304; its later retry passed.
- Worker-ready wait expired: K4 `008,022,023,029,031,033,036,046`. The trace is
  `NodeServer.start → _start_worker_pool → _start_worker_slot →
  _spawn_worker_process`, ending at `node.py:2410–2411`. Some Driver timeouts
  occur first and turn the later failure report into another BrokenPipe.
  No log contains a blocked Python stack from inside that Worker before
  readiness, so import, constructor, socket start, trace emission, scheduling
  or another delay cannot be individually apportioned.
- Outer timeouts: `012,014,045` report the 30-second process-tree boundary.
  Log 012 already printed a pytest pass dot before the outer timeout; the
  existing output cannot identify whether exit/teardown or another post-test
  phase consumed the remaining time. Do not classify these as five-second
  startup failures.

The classifications above were checked against the explicit Worker-timeout
and success-send text in every failed log at the 047 cutoff.

## Lock and initialization analysis

All line references in this section are current K4 source.

1. `NodeServer.__init__` creates a local `first_worker_id`, then
   `_worker_order` and `_workers` at node.py 368–375. `_WorkerSlot` is the
   same dataclass AST as K3, with the same `None` process/address/pid and
   remaining default fields. It invokes no Node property.
2. `_state_lock = threading.RLock()` is created at line 506. An AST scan of
   the entire constructor finds **no `self.worker_id`, `worker_ids`,
   `worker_pids` or `worker_addresses` access before it**. The constructor
   no longer assigns the read-only property.
3. `_make_output_publication_adapter()` is called at 466 before that lock,
   but it defines/binds callbacks only. Its adapter constructor validates
   callable objects and stores them; it does not invoke the callbacks, read
   `worker_id`, acquire the Node state lock, or perform RPC. This method's
   AST is unchanged from K3.
4. The new `worker_id` getter (566–569) acquires the reentrant lock and reads
   `_worker_order[0]`. There is no current internal `self.worker_id` use in
   node.py. `node_main` reads the already-existing plural properties only
   after `server.start()` completes, each with a short lock scope and no RPC.
5. `start()` (614–635) starts the server, snapshots its own address under the
   state lock, releases it, registers the Node, starts Workers, and starts
   supervisors. That method is AST-identical to K3.
6. `_register_with_gcs()` (698–737) holds `_gcs_lifecycle_lock`, briefly
   snapshots resources under `_state_lock`, **releases `_state_lock` before
   the RegisterNode RPC**, then reacquires it to install the acknowledged
   epoch. It neither calls the new first-worker getter nor waits for a
   state-locked callback.
7. `_start_worker_pool()` copies order and checks each slot under brief
   state locks. `_start_worker_slot()` holds the existing lifecycle lock,
   checks drain under state lock, **releases state lock before spawn and
   pipe wait**, registers the ready Worker, and only then publishes
   process/address/pid/incarnation under state lock. The only K4 AST
   differences in these two methods are removal of the old slot-importer
   and first-worker mirror synchronization calls.
8. `_register_unpublished_worker()` (2330–2377) snapshots Node identity under
   state lock, releases it, and issues exact RegisterWorkerIncarnation RPC.
   The Worker is unleaseable until its actual ACK arrives. No startup
   registration RPC was moved under `_state_lock`.

The following methods are AST-identical between the frozen K3/K4 snapshots:
Node `start`, `_register_with_gcs`, `_spawn_worker_process`,
`_register_unpublished_worker`, both supervisor-start methods,
`_make_output_publication_adapter`, and `node_main`; Worker constructor,
`start`, and `worker_main`; Core constructor.

API, control.py, transport.py, `scripts/run_baseline.py`, and
`scripts/_test_process.py` are byte-identical between the two archives.

The Driver constructs Core and OwnerService only **after every NodeStartup
has arrived and the cluster snapshot barrier completed** (api.py 2157–2193).
Therefore K4's mandatory OwnerService handler lookup/check cannot directly
cause the earlier Driver Node-readiness timeout. Worker startup explicitly
sets embedded Core to None; its Core is created lazily during task execution,
not before the initial Worker-ready pipe message. Module import cost remains
part of startup even though those constructors have not run.

## Existing deadline composition

Driver starts a Node process and waits
`_START_TIMEOUT_SECONDS * workers_per_node` (five seconds for one Worker).
Inside that same window, the Node must import modules, bind/start its TCP
server, register itself with GCS, spawn a Worker, await Worker readiness for
up to **another five seconds**, register the ready Worker with GCS, start
supervisors, build NodeStartup, and send it to the Driver.

The inner Worker window starts later than the outer Driver window and does
not include Node imports, Node registration or final Worker registration.
Even if the inner wait returns within five seconds, the total outer startup
can exceed five seconds. This composition is preexisting and unchanged. It
explains how a real success can arrive at a closed pipe, but does not tell us
which phase ran slowly in the current batch.

## Bytecode/cache hypothesis and controlled next step

The audit freeze script excludes `__pycache__`, `.pyc` and `.pyo`. Both frozen
archives contain **zero pyc files**. `run_cleanup_candidate.py` explicitly
sets `PYTHONDONTWRITEBYTECODE=1`; the bounded child environment preserves it.
`spawn` starts fresh interpreters, so memory import caches are not inherited.
Importing `miniray` executes `__init__ → api → Core/Node/Worker` and their
substantial protocol/ownership dependency closure in each child.

| Frozen source footprint | K3p | K4d |
|---|---:|---:|
| Python modules under src/miniray | 55 | 55 |
| Combined source bytes | 2,580,688 | 2,559,971 |
| core.py bytes | 616,236 | 610,656 |
| node.py bytes | 358,893 | 354,778 |
| protocol.py bytes | 271,978 | 271,978 |

Repeated parsing/compilation of this uncached source can consume a meaningful
part of a five-second spawn budget under load. This is a plausible,
inspectable mechanism, **not a measured attribution yet**. K4's source is
slightly smaller, and its cold Worker constructor/entrypoint AST is unchanged;
the available data does not support assuming K4 added a new large compile
burden. K4 import ordering changes still preclude asserting zero overhead
without measurements.

`PYTHONDONTWRITEBYTECODE=1` prevents automatic cache writes; it does not forbid
reading a valid precompiled cache. Root can compare exact K3/K4 frozen
selectors before/after generating normal **checked-hash pyc** with the same
Linux Python 3.12 interpreter, at the same optimization level and source
paths. Do not use Windows-created bytecode, unchecked source-independent
caches, altered source, optimized assertions, or increased timeouts. Keep
cache outputs outside the immutable archive and document their provenance.

Record source hashes, interpreter/cache tag, checked-hash policy, compile
command, cache location, no-write setting, load/concurrency conditions and
both outer/pytest elapsed for each arm. Compare K3 and K4 under the same cache
policy; do not compare warmed K4 only against the original cold K3 timings.
The already obtained unchanged-K3 control is the first measurement, not the
complete cache experiment. Compile-only prewarming does not execute test
code, while the later selected smoke must still use the existing reviewed
runner and unchanged five-/30-second limits.

If matched caches do not resolve the startup failures, the next useful
evidence is bounded phase timing at Node entry/constructor, Node register
start/end, Worker spawn/entry/ready, Worker registration and NodeStartup send
in an isolated diagnostic copy. A timestamp for the parent timeout alone
cannot distinguish compile, scheduling, socket/trace startup or RPC delay.
Neither path justifies ignoring a timeout or accepting a partially observed
startup/cleanup result.

## Review boundary

This review executed only read-only filesystem/archive inspection, AST
comparison and arithmetic over recorded logs. It did not run pytest, WSL,
runtime imports, bytecode compilation or a new timing experiment; it changed
only this report. Runtime causality beyond the demonstrated observations
remains explicitly unmeasured.

## Root's subsequent controlled-cache observations

After the 047 diagnostic cutoff, the root performed the matched-interpreter
checked-hash cache experiment without changing source or five-/30-second
limits. These later results are separate observations, not replacements for
the failed cold runs or changes to the statistics above.

| Selector / snapshot | No-write uncached control | Checked-hash cache |
|---|---:|---:|
| K3p example04 | 16.328 s, log 041 | 9.159 s, log 042 |
| K4d example04 | 16.129 s, log 048 | 14.504 s, log 049 |
| K4d startup rollback | Prior failures retained | 15.309 s, log 050, pass |
| K4d pre-Complete owner death | Prior failures retained | 11.970 s, log 051, pass |

The first two comparisons are single observations: K3 improves by 7.169 s
(43.9%), while K4 improves by 1.625 s (10.1%). They establish that the cache
policy can matter in this environment but do not justify a general speedup,
performance parity, or an exact fraction of startup time attributable to
compilation. Root reports the 55-source compile steps took 0.937/0.970 s.
Those preparation times and checked-hash policy require their own environment
record; whole-case timings alone do not certify cache provenance.

All four cached smoke records above have exit code zero in the subsequently
read results files. Thus the previously remaining three K4 selectors now
have passing evidence under the recorded cache strategy. The defensible
acceptance statement is conditional on that validated environment; the cold
startup failures remain material evidence and are not relabeled passes.
