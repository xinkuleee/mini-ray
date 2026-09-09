# 当前实现状态与历史记录

日期：2026-09-09。**第一阶段基础版已独立通过约定验收；本地提交/标记为`teaching-base-v0.1`，第二阶段尚未实施。**
两阶段顺序以 [redesign-plan.md](redesign-plan.md) 为准；当前实际结果、证据层级和有限剩余项以 [acceptance-baseline.md](acceptance-baseline.md) 为唯一验收账本。
本页不累计旧 pass 数表示完成度，也不把不同修改时点的结果当成同一最终版本通过。

## 当前基础实现

| 领域 | 当前已接入的范围 | 尚不能据此声称 |
|---|---|---|
| Task / 执行 | 单输出；真实 spawn；1–2 逻辑 Node；dependency gate、lease/spillback、Core→Worker direct Push；动态子任务与 CPU yield | K0/K1 全部历史组合或生产 Ray API 兼容 |
| owner-led 普通结果 | owner 待交接清单与精确收据；Node 物化/Complete；child owner holds；owner 原子可见与独立托管退休 | 无 ACK/补偿，或 Node Complete 就等于当前 bytes 可读 |
| GCS 边界 | 成员、死亡事实、owner-wide Node fence、Actor 与 PG；普通结果 GCS 阶段事务和全局 contained graph 已退出活动实现 | 第二阶段两项增强已完成 |
| 对象与引用 | INLINE/STORED、immutable bytes、pin/pull、typed hold/source、独立 borrower、nested refs；显式含 Ref put 接线与补偿 | close/shutdown 已证明所有实体 GC，或 put 可以 lineage 重建 |
| 恢复 | 单输出 whole retry/reconstruction；B1 真实 START/JOIN 准入事实、B2 完整 drop 请求绑定；准确 Complete 与 UNKNOWN/LOST 分开 | 精确一次外部副作用、任意故障交错或 owner 接管 |
| Actor / PG | 串行 Actor、同 Node 有限 restart、typed 构造/启动失败、Node loss 终态；PG 至多两个 bundle、STRICT_PACK/STRICT_SPREAD、2PC/LOST | Actor migration、引用参数/值内 Ref、soft PG 优化或 bundle 重排 |

独立多返回槽、targeted/sibling 恢复、自动 StoredArg lift、Actor migration、全局图及 GCS 普通发布权威已从基础版范围退出。
对应旧测试的共享不变量按验收账本迁移，退役协议专属断言不自动成为新门禁；历史 manifest 不是当前基础回归入口。

最终snapshot03归档SHA256为`42fa8b6406ae5672b434b1b479aa08ca3c36faab131429ea287970bf135cd5d0`。
同版Linux结果为**318 passed / 1 deselected，32个exact smoke全部通过**；七个原main的stdout和canonical trace已保存。
证据见[结果](../artifacts/stage1-baseline/results.json)、[环境](../artifacts/stage1-baseline/environment.json)、[示例产物](../artifacts/stage1-baseline/example-output/)。
B04 adoption ACK-loss、B06 Task foreign nested replay的有限缺口均在本版复验闭合。
Linux/Windows已分别完成相同锁文件的frozen安装和import；安装不扩大Windows运行时支持，远端CI尚未执行。

2026-09-09活动源码为56个Python文件、59,386物理行、48,555代码行，比原始代码行减少12.03%。
Core、Node、wire责任仍集中，紧凑代码量目标尚未达成；成本及低置信度预算校准见[两阶段计划§8](redesign-plan.md)。

## 阶段交界

基础版以本地提交/标记`teaching-base-v0.1`固定源码、依赖和证据；通过该标记独立检出，之后进入第二阶段。
第二阶段尚未实现，接下来按计划E0–E4加入两项增强并验收，不恢复退出能力、不长期保留双后端。
基础标记持续作为首次学习入口，新增保证和组合测试不回写成本版交付条件。

阅读当前运行路径从 [learning-path.md](learning-path.md) 开始；职责与简化对照见 [production-ray-mapping.md](production-ray-mapping.md)。
[handoff.md](handoff.md)、旧 correction-plan、roadmap、acceptance-matrix、design/testing 的历史段落及下方记录保留来源，不覆盖本轮实施授权或当前事实。

---

## 历史 checkpoint 归档：以下不是当前工作树状态

**以下原始记录全部属于 owner-led 基础改造前的历史。** 其中“current”“now”“remaining”、旧 GCS INTENT/ARM/terminal/adopted、global DAG、multi-return/targeted、Actor migration 和累计测试数字，都必须按各自当时源码解释。
这些结果不能认证当前工作树；其中旧源文件/行号可能已删除或移动。保留内容用于追溯原断言、故障来源和语义变化，不恢复旧滚动任务队列。
当前是否通过只查顶部链接的验收账本，不能从下面任一历史绿色或未勾 checkbox 推导。

## Last recorded focused verification (2026-09-08; before retained drafts)

The foreign-reconstruction-ack-contracts checkpoint fixes the first owner
reconstruction ACK when a newly admitted, same-attempt whole-task execution
has already reached a matching terminal state. The initial five pure cases produced **3 failed / 2 passed in
0.23s**: success and application error cleared `active_recovery` before the
reducer formed STARTED, causing an incorrect AUTHORITY_REJECTED; the metadata
probe also observed owner/recovery reads outside the Core composition lock.
The uncommitted-preview and wrong-epoch controls already passed.

Core now supplies one paired snapshot callback from both normal and lazy
reducer construction. The callback is saved, not invoked during construction,
and holds `_state_lock` only over the two metadata reads, never admission or
RPC. Both preflight and post-admission checks use that pair. A real START/JOIN
outcome must still identify the same current attempt in owner and recovery.
Accepted states are PENDING with an active RETRY_PENDING/RUNNING attempt,
READY with SUCCEEDED and no active marker, or ERROR with APPLICATION_FAILED/
SYSTEM_FAILED and no active marker. Exact cached replies still win before
capability revalidation; failed/throwing callbacks never become successful ACKs
merely because terminal state exists.

The original five plus incompatible-pair/failed-callback controls and a real
terminal-system-error case passed **10 in 0.33s**. Independent review then
identified a new exception-classification issue in the paired-read change:
an existing owner whose recovery lookup failed was classified UNKNOWN_OBJECT.
Two additional cases produced **1 failed / 1 passed in 0.21s**. Only a genuine
UnknownObjectError now means UNKNOWN_OBJECT; other lookup failures return
AUTHORITY_REJECTED before admission, claims or reply caching. The final twelve
new cases plus owner/deferred/targeted and runner/classification regressions
passed **153 in 0.57s**. Added negative/system cases are not reported as
observed failures against the original implementation.

The new fixture owns two threadless Cores, one logical Task/slot, at most two
attempts/publications and one 4 KiB store with at most 128 bytes per slot. It
really publishes, finishes and drops the original stored result, then admits
the retry and completes it synchronously before returning its outcome. The
success race publishes INLINE; errors use Core's actual terminal reducer.
Borrowing uses a real Acquire from an explicit legacy export hold; cleanup
releases real capabilities but does not fabricate completion/GC for pending
controls. Callbacks and receipt observations are capped at 32. No user Task,
physical Worker, scheduler, socket or real wait executes. The lock probe
observes composition ownership, not an OS scheduling race.

Four original foreign-reconstruction heavy functions are now pure, preserving
all five original names/eight expanded cases and passing **8 in 0.26s** during
migration. Their success paths use a canonical STORED first publication, real
drop and owner START/JOIN, then the original INLINE retry, real borrower
Release/export-hold release and actual GC. They use two threadless Cores, one
Task, at most two publications and one 1 KiB store. Transport unavailability
does not prove death: the original explicit owner-death fact remains separate.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/unit/test_owner_reconstruction.py::test_concurrent_exact_requests_commit_once_and_replay_one_reply` | **1 in 0.13s** | original two concurrent callers, one admission and identical cached reply; metadata reducer model |
| `tests/integration/test_foreign_reconstruction_path.py::test_driver_reconstructs_worker_owned_stored_object_through_owner` | **1 in 1.40s** | real Worker-owned stored output, owner-local drop, foreign reconstruction and stable borrower/ObjectID |

Both were fully reviewed, individually approved and run alone through the
30s runner after the source fix. The original concurrent case was converted
to L1 (earlier bounded pass: 0.20s), retaining real reducer/coordinator/owner
locks with two owned daemon threads, a 1s barrier and shared 2s normal/1s
failure joins. No canonical publication or runtime shutdown is claimed for
that model. The process regression retains five children/six endpoints,
three logical Tasks/four executions, two 1 MiB stores and a 64 KiB result per
producer attempt. Work shares 15s; body and finally each give their closes a
shared budget of at most 3s. Internal RPC/startup/shutdown retain their own
bounds. Cleanup captures all known handles before
shape assertions and checks PIDs/endpoints and uninitialized state in finally.
This is a normal process path, not a forced completion-before-ACK interleave
or proof that the first foreign observation was PENDING.

The post-evidence reviewed-pure run passed **2910 tests, 12 deselected in
9.17s** (first expanded run: 9.29s).
Compilation passed for `conftest.py src tests scripts examples`. The manifest
is **201 whole files + 254 exact selectors in 36 other files**, 455 selectors,
with the same twelve L1 exclusions. Owner reconstruction stays eleven exact
pure functions/fourteen cases plus its separate L1; it is not whole-selected.
Independent inventory is **240 unit files / 1926 functions**, **2910 pure /
98 heavy / 35 L1**. Allowlists hold **88 MP / 45 L1**, 133 IDs across 94 files.
Original guard/name/parameter mappings align, with no pure omissions, overlap
or heavy selection. Counts/allowlists remain a reviewed snapshot, not future
safety certificates or a complete same-version gate.

**Remaining ACK boundary:** this is not a history of every admitted execution.
Targeted failure legitimately leaves the selected owner ERROR while the
logical task stays SUCCEEDED to preserve healthy siblings; that combination
still lacks an explicit joint terminal-failure receipt. Targeted completion
can also remove its session before Core has formed an outcome at all. A
completed attempt which becomes LOST again, or advances through another retry
before the first ACK, remains outside this same-attempt proof. These require
separate contracts; no arbitrary state widening or guarantee removal was used.
K0/K1, remaining historical runtime repairs, broader fault combinations and
the GCS-fidelity decision remain open. Synchronous GCS publication, global
contained DAG and phase-specific recovery are unchanged.

### Previous blocking-entry-worker-drain-contracts checkpoint (2026-09-08)

The blocking-entry-worker-drain-contracts checkpoint fixes failure-atomic
notification entry. Six initial pure cases produced **4 failed / 2 passed in
0.16s**: interrupted episode-lock entry and failed pre-RPC Block construction
left thread-local depth nonzero, so a later scope entered without a Block;
native/fallback groups also cached a failed entry and skipped a new Block when
the same group retried. Outward exception/unwind controls already passed.

`BlockingNotifier` now restores depth through an outer finally covering lock
entry, construction, notification and Unblock/lock exit. The candidate sequence
is committed only after Block construction; once sending may start, it is not
rolled back and the existing exact Unblock obligation is preserved. Native
groups, Core fallback and all three manually entered foreign scopes now save
their exit obligation only after successful entry. Scope failure propagates
normally; an unentered context is not exited. The same-group catch/retry check
is a helper contract, not a claim that ordinary get_many silently retries.

The initial six plus prior notification/deadline/LOST regressions passed
**33 in 0.33s** after the fix. A seventh pure case then verified the foreign
PENDING entry-failure path with a real owner capability: exact error identity
and deadline restoration, one owner query, no poll/fetch or failed-context
exit, and real borrower release. The seven plus prior cases passed
**34 in 0.28s**. This representative foreign case was added after the fix and
was not reported as an observed pre-fix failure; the other two foreign entry
sites are the same corrected source shape, not three new process tests.
The lock interrupt and MemoryError are one-shot local boundary injections,
not OS signal, contention, memory-exhaustion or general OOM-recovery evidence.
RPC callbacks provide typed values, not Node CPU accounting.

The last three original Worker-side heavy functions are now L1; their names,
original lifecycle assertions and real Condition waits remain. The task/drain
case owns exactly two non-daemon threads, preventing accidental creation of
the daemon-handler finalizer thread. It keeps the original admission-only
fixture seam (no reply cache, admitted handler returns a string), not user
function/Start/Complete/publication execution. The two timeout cases retain
the original active-task-count premise and execute real 1ms Condition waits
on the calling thread; there is no actual parent thread or forced clean drain.
Fake `_DrainCore` routing/fencing is not actual Core shutdown, which is tested
separately by the existing pure owner cases.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_drains_task_before_embedded_core_and_clean_ack` | **1 in 0.21s** | two real lifecycle handlers; task exit precedes Core seam shutdown and clean ACK |
| `tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_timeout_keeps_core_open_for_inflight_parent` | **1 in 0.13s** | real 1ms timeout with original active-count premise; Core remains open |
| `tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_fences_only_new_owner_retains` | **1 in 0.17s** | real 1ms timeout preserves original owner route and new-retain fence |
| `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` | **1 in 1.01s** | original real Worker Block/Unblock and one-CPU/two-Worker resource closure |

Each exact was fully reviewed, individually approved and run alone through the
30s execution runner. The two-thread case has a 2s task gate, 1s entry/wait
observations, the original 1s shutdown wait, and shared 2s normal/failure joins.
Only the two owned threads can start/join. Errors and real-wait observations
are bounded at 16 and cannot disappear through production exception handling.
Failure releases the gate, joins exact started threads, then retries actual
Worker shutdown only if real `_end_task` drained the count; no count, reply or
authority table is cleared. The two timeout cases start no thread or runtime.
The process regression retains four children/six endpoints, two tiny Tasks,
one 1 MiB store and its original 10s work/3s reference cleanup budgets. It is
a normal-path regression, not a process-level entry-fault test.

The post-evidence reviewed-pure run passed **2894 tests, 12 deselected in
8.31s** (first expanded run: 8.39s). Combined focused checks passed
**121 in 0.41s**. Compilation passed
for `conftest.py src tests scripts examples`. The manifest is **199 whole files
+ 255 exact selectors in 37 other files**, 454 selectors, retaining the same
twelve L1 exclusions. Worker-side Core remains 14 exact pure functions/16
cases plus four separately reviewed L1s; absence of heavy functions in that
file does not make it a pure whole-file selector. Independent inventory is
**239 unit files / 1917 functions**, **2894 pure / 103 heavy / 34 L1**.
Allowlists hold **88 MP / 44 L1**, 132 exact IDs across 93 files; original
guard/name/parameter mappings match current source, with no pure omissions,
duplicate/overlap or heavy selection. No heavy/default/directory
or complete same-version gate ran. K0/K1, remaining historical runtime repairs,
wider fault combinations and the GCS-fidelity decision remain open. No change
was made to synchronous GCS publication, global contained DAG or phase-specific
recovery guarantees.

### Previous notification-deadline-owner-contracts checkpoint (2026-09-08)

The notification-deadline-owner-contracts checkpoint fixes stale wait budgets
after notification entry in three paths: local PENDING get, foreign PENDING
polling and foreign retryable-LOST polling. The seven new pure cases first
produced **5 failed / 2 passed in 0.20s** against the prior Core: a 5ms budget
still scheduled a 5ms wait after 3ms of entry, and also after entry had already
used 10ms. The two local terminal-precedence cases already passed and remain
regression protection, not additional defects.

All three paths now recompute the remaining time from the original deadline
after aggregate/inner notification entry. An expired local wait only checks
whether its Event is already signalled and then uses the normal owner-state
loop; that preserves an inline value or error published during entry without
equating an Event with successful output. Foreign gets have no local readiness
fact and start no further poll/query after this expired boundary. Existing
Unblock error precedence and deadline-context restoration remain. This bounds
the next wait, not total wall-clock get duration or cancellation of notifier
control RPCs. The earlier two local LOST lock fixes are unchanged. New cases
plus prior LOST/Core-blocking/notifier regressions passed **27 in 0.29s**.

The new fixture uses at most two real registered Tasks, one 4 KiB store and
128-byte output slots; time/wait/notifier entry are explicit finite callbacks.
The foreign capability is actually acquired from an explicitly installed
legacy export hold, not a fabricated serialized outer. The LOST case really
publishes/drops parent and child outputs, finishes only the parent and obtains
retryable NOT_LOST from the owner because the child's finalizer remains.
Capabilities are released through actual owner APIs; unfinished Task/barrier
state is not erased or reported as completed GC. No user Task executes.

Three original Worker-side functions migrated from heavy to pure and passed
**3 in 0.19s**. Wrapper cloudpickle exercises two real threadless Core definition
caches and distinct locks, retaining the original local `_plus_one` call but
not dispatching a Task. Retain fencing uses a real existing owner and original
PENDING metadata: first Worker shutdown fences only new retains, exact replay/
query/release still work, then a quiescent actual Core shutdown closes the owner
protocol. Only the instance Condition deadline boundary is modelled (two calls,
three real predicate evaluations); no actual timed wait or thread shutdown is
claimed. Stored-pin proxy keeps missing-owner rejection, real provisional/
final holds and release. Its metadata remains PENDING/ACTIVE through actual
Core finalization, rather than fabricating GC. Both owner cases consume three
exact empty Worker-death suffix replies; no lazy Node-death bootstrap, listener
or live GCS is created. Three original Worker-side functions remain heavy.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/unit/test_node_blocking_get_authority.py::test_concurrent_block_and_completion_linearize_without_leaking` | **1 in 0.21s** | original two real Block/Complete callers; exact release and terminal report, without owner adoption/GC |
| `tests/integration/test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get` | **1 in 1.16s** | original escaped ref, two distinct borrowers and actual Release ACKs |
| `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` | **1 in 1.01s** | original real Worker get/CPU yield/reacquire and resource cleanup |

Each exact was fully reviewed, individually approved and run alone through the
30s execution runner. The Node L1 preserves its 3-party barrier with 1s waits,
shared 2s normal joins/1s finally joins, two result slots and a 1 KiB store.
Errors are capped/preserved, only its two owned threads may start, and failure
cleanup never reenters a potentially held authority lock. Its six original
pure cases and shared fixture bodies are unchanged. Real terminal ACK settles
the outbox; both reply slots and 13B stored data deliberately remain awaiting
an owner. This is not a leaked-allocation or fabricated-clean-GC assertion.

The Worker-owned-ref process case retains five children/seven endpoints, two
tiny Tasks and two 1 MiB stores; body gets share 10s, reference cleanup 3s,
and finally checks recorded PID/endpoints. Its gets are Driver-side and may
already be ready, so it proves neither a Worker notification nor budget expiry.
The CPU-yield case retains four children/six endpoints, two Tasks, one 1 MiB
store and its existing budgets/finally checks. It tests normal Block/Unblock,
not an injected notification timeout. Public get/close budgets do not cancel
inner owner/control RPCs or replace the runner's failure-cleanup bound.

The post-evidence reviewed-pure run passed **2887 tests, 12 deselected in
8.59s** (first expanded run: 8.61s); the combined focused set passed
**134 in 0.46s**. Compilation passed
for `conftest.py src tests scripts examples`. Selection is **198 whole files +
255 exact selectors in 37 other files**, 453 selectors, with the same twelve
L1 exclusions. All current pure cases remain selected, without selecting the
new Node L1. Independent AST inventory is **238 unit files / 1912 functions**,
**2887 pure / 106 heavy / 31 L1**; allowlists contain **88 MP / 41 L1**, 129
exact IDs across 93 files. Worker-side Core remains exact-selected (14 pure
functions/16 expanded cases), not whole-selected with its three heavy and one
L1. Node blocking keeps six exact pure selectors and its separate L1.
All pytest was serial; no heavy/default/directory/full same-version
gate ran. K0/K1, the remaining historical runtime repairs, wider failure
combinations and GCS-fidelity design decision remain open. Synchronous GCS
publication, global contained DAG and phase-specific recovery are unchanged.

### Previous lost-blocking-drain-contracts checkpoint (2026-09-08)

The lost-blocking-drain-contracts checkpoint fixes an actual lock-order bug in
the two local LOST branches of `CoreWorker.get`. Four new pure regressions
failed against the previous implementation (**4 failed in 0.40s**): own and
descendant finish waits entered notifier scopes while holding the real Core
RLock; a publication completed during scope entry was followed by a stale
wait; and notification entry could exhaust the deadline while the old remaining
budget was still used. These are deterministic method/lock observations, not
an executed OS deadlock. A cross-thread wait cycle requires user threads to
explicitly bind the same execution context/notifier; no inheritance is implied.

Both branches now check their predicate, leave the condition lock before
entering group/notifier scope, then reacquire it and recheck before the atomic
Condition wait. Remaining time is recomputed from the original deadline after
notification entry. Unblock exits outside the condition lock. Zero-timeout
behavior and aggregate single-episode semantics remain; a LOST result without
a barrier can still admit one reconstruction before its new PENDING timeout.
The four regressions plus eleven unchanged finish-barrier cases passed
**15 in 0.41s**. This does not change all PENDING/foreign timeout behavior or
cancel the notifier's independent control RPCs when a get budget expires.

All six original Core blocking-notification functions/seven cases are now
pure and passed **7 in 0.19s**. They retain the original ready puts, pending
waits, exceptions, nested and aggregate episodes. Successful fixture outcomes
use real discovery/Node journal/adoption/finish/GC, not direct legacy inline
publication. They register at most two Tasks with the original `_enqueue=False`;
they do not prove Worker execution or lease admission. Pending-only fixture
termination happens explicitly after the original assertions, not as a
consequence inferred from get timeout. Actual waits are bounded inert callbacks
or an already-set receipt check; runtime/thread/socket/Timer entry is forbidden.

Worker binding preserves three original functions: Driver-only API rejection
keeps all init/shutdown/trace parameters as pure checks; child-spec context
cleanup registers two canonical Tasks, then performs actual terminal/finish/GC;
one real thread still proves binding isolation. Its first focused pure run was
**3 passed / 1 failed in 0.19s** because an added fixture-tail assertion expected
two coordinator wakes instead of the real four (terminal plus finish for each
Task). The corrected exact expectation, without clearing or ignoring events,
passed **4 in 0.18s**. Six remaining Worker-side functions stay heavy.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/unit/test_node_shutdown_drain.py::test_inflight_localization_blocks_drain_and_cannot_late_grant` | **1 in 0.14s** | one actual request thread pauses the original empty-dependency localizer; BeginDrain prevents a late grant |
| `tests/unit/test_node_shutdown_drain.py::test_exact_cached_replays_are_counted_and_balance_after_begin_drain` | **1 in 0.14s** | two actual callers wait behind one LeaseID lock, retain the cached grant and then retire it through real Cancel |
| `tests/unit/test_worker_side_core_contract.py::test_runtime_binding_isolates_worker_thread_and_restores_driver` | **1 in 0.12s** | one explicitly bound Worker thread, Driver binding unchanged/restored; no constructed Core runtime |
| `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` | **1 in 1.02s** | original one-CPU/two-Worker parent-child execution and clean resource accounting, not a LOST lock-cycle reproduction |

Each exact was fully reviewed, individually approved and run alone through the
30s runner. Node L1s preserve the original 1/2-thread interleavings, with 1s
event/count limits, shared 2s normal joins and 1s failure joins. They use an
unstarted Node/inert Worker slot and empty dependencies: no byte pull, pending
replica custody or real Worker-drain claim. The original cached Worker-clean
status is an explicit first-case premise. Thread failures are retained;
failure cleanup releases test gates, joins exact started threads, then uses
real Cancel only if needed, never clearing authority state. Binding L1 has
one thread/two 1s gates, 2s normal join/1s failure join and a main-thread error
check. The CPU-yield process case keeps four children/six endpoints, two tiny
Tasks, one 1 MiB store and the original 10s work/3s reference cleanup budgets.
Known PIDs/endpoints are now captured before structure assertions and checked
in unconditional shutdown finally. Inner RPC/lock/shutdown budgets remain their
own bounds; no timeout is described as distributed cancellation.

The post-evidence reviewed-pure run passed **2877 tests, 12 deselected in
8.45s** (the first expanded run independently also took 8.45s).
The combined focused set passed **113 in 0.56s**, and the separately
updated Worker classification passed **17 in 0.04s**. Compilation passed for
`conftest.py src tests scripts examples`. Independent AST inventory is
**237 unit files / 1906 functions**, **2877 pure / 110 heavy / 30 L1**. The
manifest contains **197 whole files + 252 exact selectors in 37 other files**,
449 selectors, with all pure cases selected and the same twelve L1 exclusions.
No duplicate/overlap/heavy selection was found. Allowlists are **88 MP / 40 L1**,
128 exact IDs across 92 files; original classification and replacement mappings
remain checked. All pytest was serial; no heavy/default/directory/full
same-version gate ran. Complete K0/K1, remaining historical runtime contracts,
wider fault combinations and the GCS-fidelity design decision remain open.
Synchronous publication, global contained DAG and phase-specific recovery
guarantees are unchanged.

### Previous publication-trace-contracts checkpoint (2026-09-08)

The publication-trace-contracts checkpoint makes the existing ordinary-success
publication path visible in the first teaching example. Its post-evidence
reviewed-pure run passed **2862 tests, 12 deselected in 8.93s** (first expanded
run: 8.89s): **195 whole files + 250 exact selectors in 37 other files**,
445 selectors. Compilation
passed for `conftest.py src tests scripts examples`. All pytest runs were
serial; no heavy, default, directory-wide or full same-version gate ran.

Core/Node add best-effort observations of actual publication ACKs, first owner
CAS/wake, local Complete/resource release and Node reply-custody retirement.
Existing RPCs, typed acceptance checks, authority state and recovery guarantees
are unchanged. ACK scopes bind observations to their actual received replies.
Node ACK events observe receipt; the adapter still performs the original
stage/forward-permission/journal checks, so the event alone proves no local
journal commit.
The owner READY observation is outside both Core and owner-table locks; Node
Complete is observed after its inner handler releases the local locks.

The success golden contract now checks ten separate RPC round trips, including
Prepare, Node INTENT/ARM, Core terminal/adopted and Node payload retirement.
Four same-handler phases bind TaskID/AttemptID/LeaseID/manifest digest to their
own received ACK; `transport_ok=true` alone is not a business acknowledgement.
Explicit handler cause chains anchor Prepare/Push/Complete, and shared semantic
anchors stay fixed across nested rules even with an independent repeated ARM
report. Request/reply endpoints are rendered separately. Node's terminal outbox
remains an independent branch, not a prerequisite for local resource release.
The old `task_finished`/`object_ready` tail is not first readiness or final GC.

Synthetic matcher/renderer cases passed **61 in 0.25s** (first expansion:
54 in 0.28s). Six new pure observation cases passed **6 in 0.25s**; the combined
matcher/observation/runner contracts passed **98 in 0.36s**. The observation
cases use actual Node factory/outer Complete and Core publication/GC reducers
with two 13-byte slots and one 1 KiB store. Normal, RuntimeError and custom
BaseException sinks cannot alter publication/ledger/retirement/GC outcomes.
This tests throwing sinks, not the liveness of an arbitrarily blocking sink.
Their client reply observations are explicitly inert test boundaries, not
network-delivery evidence. Calls/samples/errors have pre-effect/capped limits;
tripwire failures remain visible even if an observational exception is caught.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/integration/test_cross_process_trace.py::test_one_task_emits_cross_process_golden_trace_and_cleans_up` | **1 in 0.93s** | final strengthened ten-RPC success path, four distinct phase request IDs, owner READY before adopted/retirement; first pre-hardening run also 1 in 0.93s |
| `tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]` | **1 in 0.93s** | original main and actual canonical output containing all four GCS stages |
| `tests/integration/test_cross_process_trace.py::test_application_error_trace_is_terminal_without_system_retry` | **1 in 0.94s** | original terminal application-error path and no system retry |

Each exact was fully reviewed, individually approved and run alone. Each keeps
three managed children, five endpoints, one tiny Task and one 1 MiB store. Work
shares 10s; trace observation is at most 2s within that budget; close uses 3s
before unconditional shutdown, under the 30s execution runner. PID/endpoint
hygiene now runs in finally even if a trace or main assertion fails; the first
shutdown report cannot be replaced by a fallback result. No extra Task, fault
or test thread was introduced. Other six example mains were not rerun here.

Independent static inventory is **236 unit files / 1903 functions**,
**2862 pure / 124 heavy / 27 L1**; all current pure cases are selected with the
same twelve exclusions and no duplicate/overlap/heavy selection. Allowlists
remain **88 multiprocess / 37 L1**, 125 exact IDs across 90 files. Classification
guards and historical replacement mappings are unchanged. These are scoped
observations, not future safety certificates or complete K0/K1 exits. The
single-Task no-retry success contract is not a general concurrent-trace model
checker. Exposing synchronous GCS publication does not resolve its Ray-fidelity
design difference; global DAG and phase-specific recovery guarantees remain.

### Previous sibling-spillback-lifecycle-contracts checkpoint (2026-09-08)

The sibling-spillback-lifecycle-contracts checkpoint's first expanded run
passed **2800 tests, 12 deselected in 8.86s**: **194 whole files + 250 exact
selectors in 37 other files**, 444 selectors. Compilation passed for
`conftest.py src tests scripts examples`. Only the leading docstrings of
core/node/worker changed in production sources: they now describe Worker
embedded ownership, the fixed 1–2 Worker pool, and the existing publication
coordination. No executable algorithm or protocol changed. All pytest runs
were serial; no heavy/default/directory-wide gate ran.

The remaining four original multi-return lifetime functions are now three
pure functions/five expanded cases and one true-thread L1. The pure five
passed **5 in 0.42s**. Task lifecycle keys use the actual TaskID after real
publication/finish, without fabricated accepted counts. Dependency cases
retain the original submission order: producer PENDING, then its three-return
consumer with submitted/lineage holds, then real producer publication/finish
before consumer dependency preparation and successful publication. This uses
two actual Task lineages, not a replacement put or permanently PENDING
producer. All three original close orders preserve the Task lineage until
the final sibling, release the producer hold once, then collect the producer
after its last real handle closes. The STORED-first case uses an actual
ObjectStore pin, real Node PINNED reply/frozen GC plan, and actual unpin plus
the same Drop request before final lineage release; an old saved retry event
delivered afterward makes no new RPC. Two canonical publications and a 1 KiB
store are bounded in memory; no Worker executes a user function.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/unit/test_function_registry.py::test_concurrent_identical_registration_creates_exactly_once` | **1 in 0.26s** | two actual callers, one unchanged registry/RLock, one 7-byte payload and exactly one insertion |
| `tests/unit/test_spillback_runtime.py::test_concurrent_duplicate_spillback_uses_one_cached_snapshot` | **1 in 0.23s** | two overlapping Node requests, actual installed-view copy/Hybrid decision, one cached spillback, changed-request rejection |
| `tests/unit/test_public_multi_return_runtime.py::test_concurrent_sibling_closes_claim_task_lineage_exactly_once` | **1 in 0.17s** | original three close threads plus one actual reference consumer, task-scoped last-sibling lineage/GC |
| `tests/integration/test_nested_large_argument_path.py::test_nested_large_argument_pulls_without_gating_on_pending_handle_and_collects` | **1 in 1.32s** | original public StoredArg lift, pending nested handle, true cross-node pull/import/get and three-object physical/metadata GC |

Every selector was fully reviewed, individually approved and run alone through
the 30s runner. Registry keeps two threads and its actual lock/algorithm, not
sequential calls or a GCS service. Its Barrier is at most 1s, joins share 2s and
finally joins share 1s; bounded outcomes/errors and exact thread cleanup replace
the old unbounded wait. No particular low-level lock-acquisition schedule or
Worker function-cache path is claimed.

Spillback keeps the real per-LeaseID/scheduling/state locks. Two registered
Node hints are installed in one unstarted Node; the snapshot accessor copies
that local view before the gate pauses it. Both actual handlers have entered
with inflight count 2 and no cached outcome before release. The real Hybrid
policy runs once, both replies share the one cached result, and a changed
request is rejected without a second decision. This does not instantiate a
remote Node, allocate resources, create a Core/store or query GCS. Two threads,
1s waits, shared 2s normal joins and 1s finally joins remain the bounded scope.

Sibling-close L1 preserves three genuine close threads feeding one genuine
reference-event consumer; it does not make three parallel GC commits or use
a synchronous fake mailbox. The callback invokes real collection, records
errors before the GC loop could suppress them, and signals only after actual
owner metadata collection. The one lineage release is observed on that
reference thread. Producer closure/GC follows the three consumer siblings;
real stop/join proves FIFO empty, zero unfinished events, closed timer
admission and a detached runtime finalizer. Barrier/close/event waits are at
most 1s, closer joins share 2s, and failure cleanup shares 2s using only the
owned thread/mailbox objects, not possibly held owner locks or cleared state.
The original 24 pure parameter cases remain exact-selected alongside this L1.

Nested large-argument acceptance retains two resource-pinned Tasks on two
Nodes/Workers, five child PIDs/seven endpoints, two 1 MiB stores and the same
64 KiB lifted container. A pending source handle is closed before consumer
Push; only the lifted object is a readiness/pull dependency. Two nested
occurrences become one real imported borrower/TaskHoldSource, and user get
is observed while the producer is still PENDING before its gate is released.
Source-only bytes still choose source as the first lease hop; target-only
resources cause the original strict source→target spillback. Actual Task/
Attempt/Lease identities, byte-free control data and two replica-absence
reads remain checked. Original step/work/Worker-gate limits are 5/10/10s;
final gate/handle cleanup, all three actual COLLECTED states and both physical
reads share `min(work deadline, now + 3s)`, reused by finally. Listener setup
and all accepted sockets/handles have failure cleanup, observation is capped
at 32 records with error flags checked outside callbacks, and metadata
traversal has node/depth bounds. No extra Task, fault, thread or fake ACK was
added. Internal RPCs/shutdown keep their own bounds; public deadlines are not
distributed cancellation.

Five focused pure cases plus legacy/runner/collection/mode checks passed
**105 in 0.54s**; the separate registry-classification/mode check passed
**56 in 0.09s**. Inventory is **235 unit files, 2800 pure / 124 heavy /
27 L1**. All current pure cases are selected; only the same twelve selected
L1 exclusions apply, with no heavy, duplicates or whole/exact overlap.
Guards are legacy **188/0/6**, placement **151/9/4**, reference **93/36/4**;
allowlists are **88 multiprocess / 37 L1**. New L1 cases did not enter the
pure selection. These are reviewed snapshots, not future safety certificates
or full same-version gates. K0/K1, remaining historical repairs, wider fault
matrices and the GCS-fidelity decision remain open; original GCS publication/
global DAG/phase guarantees are unchanged.

### Previous replay-foreign-lifecycle-contracts checkpoint (2026-09-08)

The replay-foreign-lifecycle-contracts checkpoint passed **2795 tests,
12 deselected in 8.53s** after the manifest evidence update (first expanded
run: 8.81s): **194 whole files + 247 exact selectors in 37 other files**,
441 selectors. Compilation passed for
`conftest.py src tests scripts examples`. Production sources are unchanged.
All pytest runs were serial and explicitly scoped; no heavy/default/full
directory gate ran, and the complete K0/K1 exits remain unverified.

Three original multi-return functions retain all eleven parameter cases and
passed **11 in 0.41s** after pure migration. Five descriptor-drift fields still
use two actual STORED siblings, with slot 0 healthy and slot 1 changed. A real
Node Complete envelope remains present: wire reconstruction and the independent
Core decoder/canonical-replay preflight are tested separately. Three malformed
ordered manifests leave all three owner slots pending and unmodified; these
are format/decoder guards, not claims that owner CAS ran on invalid data.
Success preflight failure now returns False with the real retained adoption
continuation, rather than the obsolete escaping RuntimeError: GCS terminal
metadata may advance, but owner/Task recovery/Node bytes do not. Replaying the
same continuation performs no new lease, user execution or Complete; all eight
control calls, including repeated terminal, are recorded. The application and
local-terminal branches retain their original error objects and no-success-
publication path before real finish/GC. Both stored siblings and the original
three-slot cases are preserved, not replaced by a smaller success fixture.

The original already-acknowledged-owner-death case now uses two real put
owners, canonical consumer foreign lineage and actual Node pull/grant. First
owner's ADDED receipt is retained; second owner's real report effect loses its
ACK. An exact registered Worker death consumed from the GCS journal makes the
replay Cancel its grant before reporting to the healthy owner again. Only
matching receipts/death delegation permit the inventory ACK and original
OwnerDiedError. Task finish still retains healthy input lineage until output
GC, then the healthy owner's final local close collects its two replicas.

The old `test_typed_stale_waits_for_exact_grant_cancellation_before_hold_release`
is explicitly replaced by
`test_noncustody_stale_is_quarantined_until_authoritative_owner_death`. STALE
is an explicitly injected typed protocol response, not a claim that an actual
put reconstructed or the owner generated stale history. The real first Cancel
takes effect before its ACK is lost; its exact replay fences execution but
cannot release lineage, acknowledge custody, or unpark quarantine. A separately
supplied registered owner-death fact then wakes the actual retained record,
authorizes inventory handoff and preserves the original STALE error. No
normal dead-owner release/GC is fabricated. Both cases drive exactly two
frozen GCS owner-wide sweep effects through real Node handlers; only the TCP
endpoint constructor is inert, because real GCS construction otherwise binds
a socket even without start. The original seventeen pure cases/helpers are
unchanged, and the whole file remains exact-selected.

The death test first failed **1 in 0.32s** on an incorrect expectation that
source transfer-history tables disappear. Real Node close intentionally keeps
released/closed tombstones. The corrected test records actual ReleasePin ACKs,
checks exact terminal sessions/close fences and empty active outboxes, while
preserving all physical-byte/hold/lineage cleanup checks. It then passed
**1 in 0.28s**; the STALE replacement passed **1 in 0.15s**. No production
cleanup was weakened or history cleared. Dead-owner metadata stays unchanged
through main assertions; final fixture fencing only closes local test handles,
leaving retained tokens and queued GC unprocessed in that non-live image.
An earlier focused run's classification guard observed an in-progress mode
edit before its mapping was updated (**54 passed/1 failed in 0.47s**); that run did not execute the
new foreign cases. The final thirteen focused cases plus legacy/runner
contracts passed **57 in 0.50s**.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects` | **1 in 1.05s** | original Worker-owned child in STORED outer, forward adoption/witness and inverse graph/hold/replica/metadata proofs |
| `tests/integration/test_contained_ref_lifecycle_path.py::test_two_borrowers_outlive_their_inline_container` | **1 in 1.04s** | original two independent foreign borrowers, actual outer GC before reads and exact first-token release before the surviving-token read |

Each was fully reviewed, individually approved and run alone with the 30s
runner. Both retain two tiny Tasks, one Node/two Workers, four children/five
endpoints and a 1 MiB store, with no fault or extra probe Task. STORED outer
keeps parent 0 CPU/child 1 CPU, 64 KiB padding, the original 10s work deadline
and all graph/Node witness/GCS metadata/physical-before-and-after probes. Public
closes, actual borrower-ACK convergence and inverse GC/replays share
`min(original work deadline, now + 3s)`; finally reuses that epoch. Metadata
inspection traverses the complete value with node/depth budgets rather than
truncating it. The imported legacy close helper is unchanged.

Two-borrower acceptance retains its original two-CPU configuration and exact
two outer deserializations/three child reads; its resource exception is
documented per exact ID. Actual outer COLLECTED and contained-hold Release
precede child reads, and a matching first-borrower Release ACK precedes the
last second-borrower read. At most sixteen completed Core borrow-RPC calls
are passively observed, not sixteen underlying transport attempts. Gets and
early closes share 15s; each early close is also capped at 3s, while the final
second close/release and finally share one 3s epoch. Neither process case
infers final foreign-child owner-metadata COLLECTED from a release tombstone
or clean shutdown. Inner borrow RPCs keep their finite three-attempt policy;
caller deadlines do not cancel it. PID/endpoints are checked in failure
cleanup, with original resource/exit reporting retained.

Independent static inventory is **235 unit files, 2795 pure / 132 heavy /
24 L1**; all current pure cases are selected, with the same twelve exact
selected-L1 exclusions and no heavy, duplicates or whole/exact overlap.
Guards are legacy **183/7/4**, placement **151/9/4**, reference **93/36/4**;
allowlists remain **88 multiprocess / 34 L1**, all 122 exact mappings checked.
The pure increment is eleven original parameter cases, one original death
case and one explicitly replaced STALE contract. These counts do not certify
future fixtures. Remaining historical repairs, complete same-version exits,
wider matrices and the GCS-fidelity decision remain open; existing GCS
publication/global DAG/phase guarantees remain unchanged.

### Previous admission-reference-concurrency-contracts checkpoint (2026-09-07)

The admission-reference-concurrency-contracts checkpoint passed **2782 tests,
12 deselected in 8.36s** after the manifest evidence update (first expanded
run: 8.41s): **194 whole files + 242 exact selectors in 37 other files**,
436 selectors. Compilation passed for
`conftest.py src tests scripts examples`. No production source changed. All
pytest executions were serial; no heavy/default/directory-wide gate ran.

Four original public-submission/retry cases passed **4 in 0.24s** after pure
migration. The actual public remote path returns one versus three ordered
references and enqueues the corresponding canonical Tasks. Three-output
registration preserves owner tokens, full lineage and one Task finish count.
Real owner/recovery retry CAS advances all siblings once; an old finish/retry
cannot alter the successor. An injected error after real owner preflight
preserves all owner/recovery/queue/finish-barrier state and budget. Only after
the main assertions, an explicit local fixture error ends each never-leased
Task through the real terminal/finish/GC path. That is not a Worker failure,
success execution or public shutdown claim; failure-finally does not invent a
terminal state or clear obligations.

The obsolete `test_pending_or_stored_zero_reference_metadata_is_not_inline_collected`
contract is formally replaced by
`test_pending_closed_output_waits_for_finish_then_stored_publication_is_collected`
and passed **1 in 0.27s**. A real early close leaves zero local tokens, first
PENDING and then canonically published READY_STORED, while the actual finish
barrier still forbids collection. Only real finish permits the exact Node
Drop, slot-cleanup proof and owner/lineage GC. One unstarted Node/1 KiB store,
one tiny result/no child graph and no thread or user function are involved.
The old blanket STORED exclusion is not reported as a passing assertion.
The five pure cases plus legacy/runner/mode contracts passed **80 in 0.36s**.

| Exact L1 selector | Result | Scope |
|---|---|---|
| `tests/unit/test_worker_export_pin_rollback.py::test_worker_drain_stays_unclean_while_export_release_cannot_converge` | **1 in 0.21s** | actual three-thread Core and Worker drain composition; persistent generic-export failure, true drain release/GC, stale FIFO retry and finalization |
| `tests/unit/test_contained_edge_runtime.py::test_publish_installs_edge_before_wake_and_same_owner_release_does_not_deadlock` | **1 in 0.27s** | one real reference consumer, same Core/RLock, edge-before-wake and recursive contained/lineage GC |
| `tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_lost_requests_merge_and_old_attempt_is_fenced` | **1 in 0.20s** | two request threads/three internal Core calls, one stored result, contended retirement followed by one START/enqueue and real JOIN |
| `tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_multi_return_sibling_requests_start_once_and_join` | **1 in 0.19s** | same two-thread interleaving across a canonical three-sibling manifest; exact old-envelope fencing |

Every L1 selector was fully reviewed, separately approved and executed alone
through the 30s runner; none starts a live cluster or socket. The Worker-drain
case keeps its original ID but no longer uses an incomplete Core fixture whose
shutdown exception could be swallowed as unclean. Its real constructor starts
three owned threads; shutdown return values and exceptions are recorded. An
actual put/generic export pin survives the initial Release and first drain
failures. The local handle closes while that pin still protects it; the second
real Worker drain performs the successful Release/tombstone/GC. The saved
original retry event then reaches the real reference consumer and claims
False without another Release. Preserved-owner drain keeps the owner protocol
and reference thread alive until explicit Core.finalize_shutdown; no fake
Worker stop flag or cleared obligation is counted as success. Normal waits/
joins are at most 1s and failure cleanup shares 2s; Timer/network/Worker
publication races are not covered. Its first approval expired before starting
any process; a permitted retry executed the one test run.

Same-owner acceptance keeps one actual reference thread plus the main
publisher, not two separate owners or full Core constructor/dispatch. The
original reference initializer is installed on the one Core before real put,
canonical nested input, Grant/Start/selected publication and finish. The
reference consumer calls the same Core's Release outside its own Core-lock
ownership; child lineage survives until outer GC, then child GC converges.
The first run passed in 0.16s. Static review subsequently found that a callback
observation-bound assertion could be swallowed by the production GC loop;
the test now caps and records callback failures for main-thread checks, and
the final 0.27s run includes that correction. Failure cleanup only stops the
independent mailbox and joins the exact thread, avoiding possibly held Core
locks; it does not pretend to prove arbitrary-schedule deadlock freedom.

The two reconstruction L1 cases now create their initial state through real
Node registration/Grant/Start/discovery/Prepare/Complete, Core adoption and
finish, saving the actual successful envelope. Main deletes one or three tiny
in-memory replicas with real Drop, leaving LOST publication memberships for
the request threads to retire. First holds the production slot-0 retirement
ticket at the Drop boundary without Core/Node/journal locks; second genuinely
defers. Main releases that gate; first performs all old-slot retirement and
the actual enqueue, after which second's one additional request genuinely
JOINs. Two threads make exactly three internal Core requests, not public get
calls or user executions; no test mutex serializes the pipeline or moves
retirement out of it. Initial DROPPED versus threaded ALREADY_DROPPED replies
are checked with complete identities and GCS per-slot cleanup proofs. The
original envelope is then rejected as stale without mutating successor
owner/recovery/session/queue state. Main explicitly terminates the unexecuted
new attempt only after those assertions, then verifies real finish and GC.
Barrier/events use 1s waits, normal joins share 2s and finally joins share 1s;
one unstarted 1 KiB store and at most 32 bridge calls/8 drop receipts are the
static bound. The three-memory-drop setup is an exact documented exception,
not permission for a broader multiprocess fault experiment.

Independent static pure inventory is **235 unit files, 2782 pure / 145 heavy /
24 L1**. All current unit cases are selected, with twelve exact selected-L1
exclusions and no heavy, duplicates or whole/exact overlap. Contained and
Worker-export files each remain six exact pure selectors plus one separate
L1, not whole-file selections. Guards are legacy **170/20/4**, placement
**151/9/4**, reference **93/36/4**; allowlists are **88 multiprocess / 34 L1**.
The pure increment is four original migrations plus one explicit replacement;
two other original cases moved from heavy to L1. Wider matrices, remaining
historical repairs, same-version full exits and GCS-fidelity decisions remain
open. K0/K1 remains incomplete; all existing GCS/global-DAG/phase guarantees
are unchanged.

### Previous mixed-contained-terminal-contracts checkpoint (2026-09-07)

The mixed-contained-terminal-contracts checkpoint passed **2777 tests,
12 deselected in 8.21s** after the manifest evidence update (first expanded
run: 8.28s): **194 whole files + 237 exact selectors in 37 other files**,
431 selectors. Compilation passed for
`conftest.py src tests scripts examples`. No production source changed. All
pytest runs were serial and explicitly reviewed; this is not the complete
default gate or a same-version all-ID acceptance.

Two original three-sibling terminal cases now use one pure Core, one real
unstarted Node and a 1 KiB store. Canonical submission/Grant/Start precede
discovery/Prepare/Complete and the existing Core batch adoption. A bounded
serializer decodes the middle result to the original 22 but crosses the real
64-byte threshold, preserving decoded values 11/22/33 and INLINE/STORED/INLINE.
Before each real wake, all three owner results, the stored route/bytes and
Recovery state must already be committed. The application-error case instead
uses a real failed Complete, no success envelope or discovery, and one shared
TaskError object across all siblings. Real finish, duplicate Complete without
resource release, per-slot GC/one actual stored Drop and final task-lineage
retirement follow. These two cases passed **2 in 0.24s**; no Worker/user Task
executes in this pure Core/Node composition.

Two obsolete Worker multi-return-refusal contracts are explicitly replaced:

- `test_multi_return_contained_ref_rejects_before_any_seal_and_unpins_all` →
  `test_multi_return_contained_refs_use_one_unified_publication_and_slot_scoped_gc`.
- `test_target_single_slot_cannot_bypass_multi_return_contained_ref_rule` →
  `test_target_single_contained_slot_keeps_full_identity_without_publishing_other_slot`.

They passed **2 in 0.23s**. The real Worker handler invokes a controlled
callable once; replay reuses the same reply. Discovery has no pin/graph/store
effects before Prepare; actual journal/pin promotion/seal/Complete and owner
CAS/selected-slot GC then converge. Start ACK and the initial CPU allocation
remain typed fixture boundaries, not Node lease admission. The target case
keeps its unselected slot PENDING, releases only its test token and does not
claim LOST→reconstruction, a healthy READY sibling, or whole-owner cleanup.
Generic export is forbidden in these current-Worker cases; the four existing
generic compatibility cases remain separate and unchanged. Their actual
threaded drain case is still heavy. Both edited unit files remain exact-selected.
The four new pure selections plus legacy/runner contracts passed **48 in 0.35s**.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | **1 in 0.93s** | original shared-child mixed-tier publication, one stored-slot drop/targeted replay, healthy snapshot and staged GC |
| `tests/integration/test_multi_output_node_loss_path.py::test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored` | **1 in 1.21s** | original publisher loss before graph COMMIT, actual INLINE KEEP/STORED DROP, explicit-get targeted replay and survivor GC |

Each was fully reviewed, individually approved and run alone through the
30s runner. The shared-child case keeps one Node/CPU/Worker, three children/
four endpoints, one 1 MiB store, one put/one two-return Task, 8 KiB padding
and one explicit Drop. Up to eight passive Push exchanges verify the real
full-two/selected-one manifests and fresh TaskHoldSource origin; owner
snapshots verify old slot-1 hold retirement, slot-0 hold survival and the new
slot-1 hold. Six handles, outer-0/outer-1/source GC share one final 3s epoch;
get/finish use 15s and Drop cannot exceed its original RPC budget. All imported
legacy helpers are unchanged; only this case uses the new bounded helpers.
Its first approval request expired before any process started; the explicitly
permitted retry executed once. This was not a test failure or a second run.

The Node-loss case retains two Nodes/one CPU and Worker each, five children/
six endpoints, two 1 MiB stores and the same one put/one two-return Task. The
first graph-COMMIT hook observes actual received custody, pending owner
entries and two child holds, releases every observation/Core lock, then
crashes only the verified original publisher process group. Terminal and
Node-loss replies are observed, not replaced: complete-known KEEP(0)/DROP(1)
preserves success without consuming retry budget; the old stored child hold
has an actual release tombstone. Only the later public get starts attempt 1
on the survivor with full-two/selected-one identity and a new contained hold.
The INLINE sibling's complete owner snapshot remains unchanged. At most
eight Push and 64 control observations, 256 predicate waits per observation,
15s work waiting and one final 3s five-handle/GC/PID epoch are explicit.
Worker embedded Cores add no OS processes; the Driver owner remains alive.

Neither process test independently counts user invocations beyond observed
physical attempts, and timeout is not cancellation of synchronous recovery
RPCs. Failure finally attempts all closes, unconditionally shuts down and
checks known PIDs/endpoints; staged GC and resource-clean reports certify
the successful path. The crashed Node deliberately remains unclean in the
report, while survivor resources/Finalize/ACK/Worker exit are checked.

Independent static inventory is **235 unit files, 2777 pure / 152 heavy /
22 L1**; all current unit cases are selected with exactly twelve selected-L1
exclusions and no heavy, duplicates or whole/exact overlap. Guards are legacy
**165/27/2**, placement **151/9/4**, reference **93/36/4**; allowlists remain
**88 multiprocess / 32 L1**. The pure increment is two original migrations
and two explicitly mapped contract replacements, not the old refusal
assertions passing. K0/K1, remaining historical repairs, wider fault matrices,
complete same-version exits and the GCS-fidelity decision remain open. All
existing GCS publication/global DAG/phase guarantees remain unchanged.

### Previous multi-return-lifecycle-contracts checkpoint (2026-09-07)

The multi-return-lifecycle-contracts checkpoint passed **2773 tests,
12 deselected in 8.32s** after the manifest evidence update (first expanded
run: 8.30s): **194 whole files + 233 exact selectors in 37 other files**,
427 selectors. Compilation passed for
`conftest.py src tests scripts examples`. No production source changed. This
is the reviewed pure selection, not a complete default gate or same-version
all-ID acceptance; all pytest runs were serial.

One obsolete contract is explicitly replaced, not reported as an original
assertion passing: `test_stale_reply_edges_are_released_as_orphan_obligations`
becomes `test_stale_reply_cannot_release_committed_publication_edges`. A real
canonical Task/selected-output publication is adopted and finished while the
outer handle remains live. Exact and detached duplicate envelopes leave
owner/child holds, task lineage, graph, Node/GCS publication snapshots and RPC
history unchanged. Only the later real outer close triggers child Release,
graph retirement, slot report and child/outer GC; a post-GC duplicate cannot
resurrect them. This is a post-finish duplicate of the same attempt, not a
prior-attempt reconstruction or unadopted-orphan cleanup test. Such cleanup
still needs Node/GCS authority, never raw TaskReply edges. The guard records
the explicit old-to-new mapping.

Two original generic-export cases now use real put/export pins, release
obligations, original FIFO events and tombstones in a threadless Core. One
checks the synchronous shutdown GC precheck: failed Release preserves the
pin; a later precheck succeeds, making the old queued round inapplicable.
The other drives failed round 1 into round 2, rejects the old claim without
disturbing round 2, then obtains a real Release/tombstone and normal child GC.
Neither clears retained obligations in teardown. These remain compatibility
primitives, not public shutdown, Timer/ref-thread races or the current Worker
publication path. The three changed pure cases passed **3 in 0.32s**; with
the legacy guard and reviewed-runner contracts, **47 in 0.32s**. Both mixed
files remain exact-selected, with five and four pure cases respectively.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_multi_return_path.py::test_public_multi_return_mixed_outputs_retry_dependencies_and_gc` | **1 in 0.97s** | original predecode SYSTEM_ERROR then full mixed-tier retry, two downstream consumers and staged lineage GC |
| `tests/integration/test_multi_return_reconstruction_path.py::test_multi_return_all_outputs_lost_reconstructs_once_from_nonzero_sibling` | **1 in 0.92s** | original two-sibling loss, nonzero public-get trigger, real START/JOIN, one whole-producer replay and final sibling GC |

Both were fully reviewed and individually approved/run through the 30-second
runner. Each has one Node/CPU/Worker, three managed children/four endpoints
and a 1 MiB store. Mixed-output acceptance retains three logical Tasks/four
attempts and one predecode failure, not lineage reconstruction. At most eight
passive actual Push replies verify the producer's two-tier full envelope,
stable logical IDs/new lease and canonical INLINE/STORED consumer inputs.
All four handles are protected before later assertions/submission failures;
the three staged close/GC observations share one final 3s epoch. The finite
GC observer rechecks actual COLLECTED after its last wait.

Whole-manifest reconstruction retains one Task/two user invocations, a
two-byte external invocation journal and two heterogeneous results below
32 KiB each. Its two explicit sibling drop injections are an exact reviewed
composite setup, not one fault or the total count of Drop RPCs: old-replica
retirement replays and final new-replica GC also occur. Slot 1's public get
triggers START; one injected sibling request at the real commit boundary
returns JOIN before Core enqueues the single replay. The observer was moved
from the obsolete START request hook to commit_prepared, with nested JOIN
recorded once. This is not two concurrent public gets. Both stable ObjectIDs,
attempt 0→1/one retry and task-scoped lineage survival/removal are checked.
Gets/drop/finite finish observations share a 15s waiting budget; synchronous
reconstruction RPCs are not cancelled by that deadline. Close/actual GC and
finally reuse one 3s epoch. In both process tests, failures still reach
shutdown and PID/endpoint checks; ledger-clean assertions certify the normal
successful path, not every failure path.

Independent static inventory is **235 unit files, 2773 pure / 156 heavy /
22 L1**. Every current unit case is selected; only twelve exact selected-L1
cases are excluded, with no heavy, duplicates or whole/exact overlap. Guards
are legacy **161/31/2**, placement **151/9/4**, reference **93/36/4**;
allowlists remain **88 multiprocess / 32 L1**. The pure increment is two
original migrations plus one explicit contract replacement. These counts
do not certify future fixtures. K0/K1, wider matrices, remaining historical
repairs, complete same-version exits and the GCS-fidelity decision remain
open; existing GCS publication/global DAG/phase guarantees are unchanged.

### Previous replica-retirement-contracts checkpoint (2026-09-07)

The replica-retirement-contracts checkpoint passed **2770 tests, 12 deselected
in 8.13s** after the manifest evidence update (first expanded run: 8.23s):
**194 whole files + 230 exact selectors
in 37 other files**, 424 selectors. Compilation passed for
`conftest.py src tests scripts examples`. No production source changed. This
is the reviewed pure selection, not a complete default gate or same-version
all-ID acceptance.

The original contained-edge shutdown-GC-precheck case now uses the existing
canonical selected-output fixture. Publication and real Task finish occur
while the outer handle is live; close then triggers the original pre-effect
Release failure. A frozen collection plan, child hold, graph and task lineage
remain until `_retry_gc_obligations_for_shutdown` performs one successful
additional Release (two total), followed by real child/graph/slot/outer GC.
Delivering the retained original GC event afterward changes no protocol
history and issues no extra RPC. The case calls a synchronous cleanup reducer,
not public Core/ray shutdown. It and the two previous edge cases passed
**3 in 0.24s**; the other three live/raw-edge cases remain heavy.

The original failed-unpin case now has a pure canonical put and a real generic
ReferenceExportSession pin. Core's existing `request_export_pin_release`
retains its obligation after one pre-effect table failure; the original
round-1 mailbox event is manually delivered to the real claim/release reducer,
which records the tombstone before child close/GC. It passed **1 in 0.15s**.
This explicitly tests a generic export compatibility primitive, not the
current Worker's zero-effect output discovery or Node publication rollback.
No fake connection between those two paths was introduced; the historical
multi-return rejection cases remain heavy and semantically obsolete. Combined
with the edge cases and legacy guard, **17 passed in 0.29s**; runner contracts
passed **31 in 0.08s**. Both mixed files remain exact-selected.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_stored_physical_gc_path.py::test_foreign_stored_dependency_collects_source_and_target_replicas` | **1 in 1.36s** | original two-replica GC, real owner-local collection probe, two exact Drop receipts and causal trace |
| `tests/integration/test_multi_return_partial_reconstruction_path.py::test_one_lost_return_reconstructs_without_changing_healthy_siblings` | **1 in 0.85s** | original single-slot reconstruction, actual full/selected envelopes, unchanged healthy slots and final GC |

Both were fully reviewed and run alone with approval through the 30-second
runner. Physical GC retains five children, eight endpoints including gate/
trace/Driver owner, two 1 MiB stores, four logical Tasks and a 64 KiB data
field. Gets/gate/owner-probe/trace/direct observations share 15s; the original
probe/trace caps remain 5s/2s with finite polling and at most 128 passive
owner-RPC records. It still observes source-owner COLLECTED, removed local
metadata/descriptor/waiter/obligation/lineage, two real replica Drop ACKs,
physical absence on both Nodes and exact ALREADY_DROPPED replays. Final
gate/reference cleanup shares 3s, and failures also reach unconditional
shutdown/PID/endpoint checks. Trace delivery does not authorize GC.

Partial reconstruction retains one Node/CPU/Worker, three children/four
endpoints, one 1 MiB store and one logical three-return Task. One 16 KiB slot
is dropped; both user invocations still compute all 12/16/20 KiB values, while
the second publication selects only return index 1. Initial and post-recovery
finish barriers define the exact healthy-sibling baseline: slots 0/2 keep
their full snapshots, descriptors and attempt 0 while slot 1 reaches attempt
1 with one retry. Up to eight passive Push exchanges verify the full
TaskExecutionKey versus TargetExecutionKey/full-three/selected-one envelopes.
All gets/finish use 15s; the real local Drop uses a no-longer-than-original
RPC deadline. Three closes and actual per-slot owner/lineage GC share one 3s
cleanup epoch, reused by failure finally. No additional fault or task was added.

Inventory is **235 unit files, 2770 pure / 159 heavy / 22 L1**. All current
unit cases are selected, with twelve exact selected-L1 exclusions and no
heavy, duplicates or whole/exact overlap. The pure increment is two original
case migrations. Guards are legacy **158/34/2**, placement **151/9/4**, reference
**93/36/4**; allowlists remain **88 multiprocess / 32 L1**. These counts are
not future fixture certification. Wider matrices, remaining historical
repairs, complete same-version gates and the explicit GCS-fidelity decision
remain open. K0/K1 is incomplete; all existing GCS/DAG/phase guarantees remain.

### Previous nested-shutdown-contracts checkpoint (2026-09-07)

The nested-shutdown-contracts checkpoint passed **2768 tests, 12 deselected in
7.60s** after the manifest evidence update (first expanded run: 7.71s):
**194 whole files + 228 exact selectors in
37 other files**, 422 selectors. Compilation passed for
`conftest.py src tests scripts examples`. The only production-source edit
corrects an outdated ObjectRef comment: direct construction/no-exporter pickle
starts detached, whereas exported foreign restore already binds an owner-ACKed
borrower capability. No executable runtime statement or guarantee changed.
This is the reviewed pure selection, not a complete default/all-ID gate.

The original pending-outer-close case now uses the same real selected-output
fixture as the failed-release case, with its new fault flag disabled. The
existing default and failure-case body are unchanged. An actual local finalizer
closes the pending outer; Node Grant/Start/discovery/child promotion/Complete
precedes Core adoption, which installs the exact edge before calling the real
wake. READY notices still cannot cross the Task finish barrier. Real finish
then permits one child Release, graph retirement, slot report and child/outer
GC. Two threadless Cores, one 1 KiB empty store and one bounded INLINE slot;
no user function, timer, wait or live shutdown. Both original edge cases
passed **2 in 0.19s**, and with the legacy guard **15 in 0.25s**. The remaining
four live/raw-edge cases stay heavy.

The two original borrower shutdown cases now run as L1 with a real borrower
Core constructor, coordinator, dispatcher, reference consumer and
`CoreWorker.shutdown`. The owner-side child/outer publication stays threadless
and transport invokes real owner handlers in-process. One scheduled retry
event is retained instead of a Timer, explicitly excluding timer/network races.
One case's pre-effect Release outage makes the first shutdown return False
while the owner token and exact obligation survive; the second real shutdown
obtains Release and stops all three threads. The other leaves its actual
borrowed handle open so shutdown itself requests Release; later close does
not issue another RPC. Original IDs and all pure bodies remain. Normal joins
are at most 1s, failure-only cleanup shares 2s under the 30s outer runner, and
fallback cleanup cannot substitute for successful normal-path assertions.
These are Core-level shutdown tests, not `ray.shutdown()` multi-process proof.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/unit/test_borrowed_object_refs.py::test_shutdown_retries_unresolved_borrowed_release_before_clean` | **1 in 0.21s** | real three-thread Core; failed first drain preserves token, second releases and stops |
| `tests/unit/test_borrowed_object_refs.py::test_shutdown_releases_live_borrowed_handle` | **1 in 0.13s** | real shutdown converts live borrowed obligation into one Release; late close is idempotent |
| `tests/integration/test_local_nested_reconstruction_path.py::test_local_nested_handle_survives_single_return_reconstruction` | **1 in 0.89s** | original nested lifetime/non-DFS edge and fresh attempt borrower; corrected inline-container precondition |
| `tests/integration/test_nested_task_argument_path.py::test_nested_argument_survives_sender_close_before_worker_push` | **1 in 1.00s** | original two-blocker sender-close-before-Push, deduplicated nested manifest and actual Worker import |

All four were fully reviewed and run alone with approval through the bounded
runner. The nested-reconstruction case first failed **1 in 0.92s**: threshold
one lifted the container into a StoredArg, whose separate READY put correctly
appeared in the dependency DFS. The fix keeps the actual container INLINE
with a 1 KiB budget, while 2 KiB source/result padding preserves both stored
paths. The original graph-only-result/no-nested-source assertion is retained,
not weakened and not implemented by filtering a valid StoredArg dependency.
It remains one put/one logical Task/two executions/one drop and reconstruction,
three children, four endpoints, one 1 MiB store, 15s Driver work and shared
3s final close/GC. Eight passive Acquire records and finite state checks
observe real hold origins 0/1 and eventual normal collection.

Nested argument acceptance retains one two-CPU Node/two Workers, four children,
six endpoints, one 1 MiB store, three tiny Tasks and no fault. Two blockers
occupy both slots while a ready put's source handle closes after submission.
The actual single nested transfer, submitted hold/lineage, unchanged bytes and
absence of an executor borrower before release are checked. This is not a
pending-source timing test. Its first run failed **1 in 1.02s** because the
edited remote helper captured an unpickleable struct.Struct; using the
original serializable struct.pack operation fixed the test, not the runtime.
All barrier/get work shares 15s, final four-handle/gate cleanup shares 3s,
and failure-finally checks all recorded PIDs/endpoints. The exact two-CPU
resource exception is explicit in testing policy.

Runner/mode/legacy guard tests passed **75 in 0.17s**. Inventory is
**235 unit files, 2768 pure / 161 heavy / 22 L1**. All current unit cases are
selected, with only twelve selected L1 exclusions and no heavy/duplicate/
whole-exact overlap. The borrower file remains exact-selected, so its two
new L1 cases do not enter the pure gate or add exclusions. Legacy guard now
supports explicit L1 entries while preserving all IDs/parameter counts:
**156 pure / 36 heavy / 2 L1**; placement **151/9/4**, reference **93/36/4**.
Allowlists are **88 multiprocess / 32 L1**. These counts do not certify future
fixtures. Remaining historical repairs, full same-version exits, wider fault
matrices and the GCS-fidelity decision remain open. K0/K1 is incomplete.

### Previous foreign-lifetime-contracts checkpoint (2026-09-07)

The foreign-lifetime-contracts checkpoint passed **2767 tests, 12 deselected
in 7.66s** after the manifest evidence update (first expanded run: 7.74s):
**194 whole files + 227 exact selectors
in 37 other files**, 421 selectors. Compilation passed for
`conftest.py src tests scripts examples`. No production source changed. This
remains the reviewed pure selection, not a complete default gate or same-version
all-ID acceptance.

Three original borrower cases now use the real child put/INLINE outer fixture:

- Pre-effect Release outage keeps the owner's actual borrower token and full
  snapshot unchanged. Local close retains its round-1 obligation; the real
  shutdown-GC precheck later obtains the first true Release ACK, and delivery
  of the already-scheduled event makes no additional RPC. This is a pure
  precheck, not public process shutdown.
- Owner unreachability uses the actual Core transport error conversion: get,
  wait, drop and submission fail before owner effects, without installing
  death. A failed Retain leaves its real rollback obligation until an exact
  Release-before-Retain tombstone ACK arrives; the original borrower remains
  usable afterward.
- Closing owner admission rejects a new Acquire while the previously acquired
  token can still Get/Release. Normal contained/lineage/child GC converges
  before the metadata finalization precheck becomes true.

The three passed **3 in 0.21s**. Existing delivery-helper defaults and prior
case bodies are unchanged; the two actual-shutdown cases remain heavy.

The original failed-contained-release case now uses canonical Task submission,
real Node Grant/Start, discovery/child pins/graph, Complete and owner adoption
instead of the obsolete raw TaskReply edge projection. Its outer handle still
closes before publication. After normal finish, the first Release fails before
its effect; one frozen collection ID, manifest, edge, child hold, task lineage
and graph remain until the real retry event is delivered. Actual child Release
and GC precede graph retirement, slot cleanup report and outer collection.
Two pure Cores, one empty 1 KiB store, one Task/one put/one selected INLINE slot
within a 4 KiB discovery threshold; no user execution or runtime wait. It
passed **1 in 0.20s**. Other raw-edge/threaded cases remain heavy rather than
being counted as current protocol evidence. All seven reviewed borrower
functions, this edge case and the legacy guard passed **26 in 0.38s**; runner
contracts passed **31 in 0.07s**.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_foreign_reconstruction_path.py::test_driver_reconstructs_worker_owned_stored_object_through_owner` | **1 in 1.13s** | original Worker-owner-controlled drop and borrower-requested reconstruction, stable owner/ObjectID/token |
| `tests/integration/test_foreign_wait_drop_path.py::test_foreign_wait_drop_replay_then_owner_reconstruction` | **1 in 1.09s** | original metadata-only wait, one real drop with one lost ACK, exact replay/reconstruction, physical GC observation |
| `tests/integration/test_foreign_input_lineage_reconstruction_path.py::test_foreign_input_hold_replaced_before_consumer_reconstruction` | **1 in 1.03s** | original early foreign-handle close, retained hold 0→1 before consumer reconstruction, actual local/foreign-lineage GC |

Each was fully reviewed and run alone with explicit approval through the
30-second runner. Foreign-owner reconstruction keeps two Nodes/one Worker
each, five children, six endpoints, two 1 MiB stores, three logical Tasks/four
executions and one 64 KiB payload per producer attempt. Small control metadata
uses the original owner-local Task. Work/get/Node-drop observations use a 15s
budget, records are capped at 64 per list, and final reference closes share
3s before unconditional shutdown and failure-finally PID/endpoint checks.

Wait/drop preserves two Tasks/three executions, the same topology and one
physical drop plus one lost successful ACK, not two physical drops. The
observer forwards all original transport timeouts/requests and caps relevant
records at 256; the deliberate ACK loss is separate from passive bookkeeping.
A finite metadata probe accepts the owner's legitimate finish-barrier PENDING
projection until actual LOST@0. After the single STARTED@1 and public fetch,
independent read-only Node probes observe matching attempt-1 bytes and then
true absence after close. Driver outer collection and borrower Release
convergence are checked; foreign owner metadata collection is not inferred
from byte absence. Public gets/waits/polls share 15s, while raw owner drop/
borrow RPCs retain their own finite three-attempt policy under the outer bound.
No test-only timeout rewrite changes that policy. Final closes share 3s.

Foreign-input lineage keeps one two-CPU Node/two Workers, four children, five
endpoints, one 1 MiB store, three Tasks/four executions and one drop/reconstruction.
The two foreign/outer handles still close immediately after consumer submission,
before the first get. Actual finish, retained origin 0, LOST@0 without budget
consumption, replacement origin 1, SUCCEEDED@1 and stable producer spec are
checked. Up to 32 passive replacement records and 256 local-state checks use
15s work; internal renewal RPCs retain their own finite retries. Final public
closes and actual local-owner/foreign-lineage collection share 3s, with the
source owner alive. This does not claim the remote source metadata was directly
observed collected. Its exact two-CPU exception and the wait/drop ACK-loss
combination are explicit in testing policy; no general stress/fault permission
is implied.

Independent AST inventory is **235 unit files, 2767 pure / 164 heavy / 20 L1**.
All current unit cases are selected, only twelve selected L1 cases are excluded,
and there is no heavy, duplicate or whole/exact overlap. Both edited mixed
files remain exact-selected; the pure increment is four original migrations,
not new case IDs. Guards are legacy **155/39**, placement **151/9/4**, reference
**93/36/4**. Allowlists remain **88 multiprocess / 30 L1**. Wider matrices,
remaining historical runtime migrations, same-version full exits and the
explicit GCS-fidelity decision remain open. K0/K1 is incomplete; no heavy or
unreviewed test ran, and all GCS/DAG/phase-specific guarantees are unchanged.

### Previous release-concurrency-contracts checkpoint (2026-09-07)

The release-concurrency-contracts checkpoint passed **2763 tests, 12 deselected
in 7.67s** after the manifest evidence update (first expanded run: 7.80s):
**194 whole files + 223 exact selectors
in 37 other files**, 417 selectors. No production source changed. This remains
the explicitly reviewed pure selection, not a complete default gate or
same-version all-ID acceptance. Compilation passed for
`conftest.py src tests scripts examples`.

Two original borrower functions/seven expanded cases now use the existing
real child put/INLINE outer publication fixture. The owner applies Release
before the first ACK is lost or altered in the six original field variants.
The borrower retains its exact obligation and the real scheduler's round-1
event. One explicit delivery of that event receives the owner's true
`accepted=True, released=False` tombstone ACK, then normal GC completes.
The failed-restore case loses both Acquire and Release ACKs without installing
owner death. The close receipt only acknowledges local intent, not remote
release. No timer, user function or public shutdown runs in this composition.
The two exact selectors passed **7 in 0.24s**; five remaining live cases in
the borrowed file stay heavy and outside its exact selection.

The original late-outbound-admission case now consumes an actual
NodeRegistry/WorkerRegistry death suffix through Core's normal observer.
Borrow registration, orphan retention, nested import, retained acquire and
exported restore all fence the dead owner before transport or new obligations.
Late IDs/holds are only request metadata; no foreign result, admitted Task,
ObjectRef or missing GC proof is fabricated. One real wake is consumed and an
empty suffix replay preserves the watermark. Its exact case passed **1 in
0.14s**; it does not prove OS failure detection. The whole Worker-death file,
four borrower functions and legacy guard passed **41 in 0.28s**; runner
contracts passed **31 in 0.09s**.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_two_worker_pool_path.py::test_one_node_two_workers_execute_two_tasks_concurrently` | **1 in 0.94s** | original same-Node two-Worker overlap, four children, one 1 MiB store, six endpoints |
| `tests/integration/test_parallel_task_lanes.py::test_two_nodes_execute_resource_pinned_tasks_concurrently` | **1 in 1.05s** | original two-Node resource-pinned overlap, five children, two 1 MiB stores, seven endpoints |
| `tests/integration/test_recursive_lineage_reconstruction_path.py::test_recursive_lineage_reconstructs_leaf_to_root` | **1 in 1.01s** | original three-level DAG, three drops and three producer reconstructions; explicitly approved composite budget |

Each was fully reviewed, approved and run alone through the 30-second runner.
The two concurrency cases keep their original proof: identify both blocked Worker arrivals and
observe neither result READY before sending either release byte. No elapsed-
time comparison, extra Task or fault supplies concurrency. References are
retained immediately after each submission, including partial-submit failure;
barrier/get work shares ten seconds for the same-Node case and fifteen for
the two-Node case. Gate/reference cleanup shares three seconds before
unconditional shutdown and failure-finally PID/owner/endpoint checks. The
same-Node case's two logical CPUs/two Workers are an explicitly documented
exact-scope resource exception, with the same total CPU/Worker count as two
one-CPU Nodes; this is not a performance benchmark.

The recursive-lineage exact case was separately reviewed and approved for its
explicit composite budget: one two-CPU Node/two Workers, four children, five
endpoints, one 1 MiB store, three tiny objects, three distinct drops and one
reconstruction per producer. It is not a single-fault smoke. Before any drop
all three initial finish barriers converge; after all three are LOST at
attempt 0, only one root get drives their recursive recovery. All original
Task/ObjectIDs and canonical dependency lineage survive, while each actual
attempt advances to 1 and consumes its own one-retry budget. Subsequent
leaf/middle reads only verify the already-reconstructed values. Work/get/drop
share 15s; existing real drop RPC deadlines are scoped and restored. Three
public closes and actual owner/lineage GC share 3s, with finite condition
observations and failure-finally shutdown/PID/endpoint checks. No three-node
scale, stress workload, new fault or production policy change was introduced.

Independent AST inventory is **235 unit files, 2763 pure / 168 heavy / 20 L1**.
All current unit cases are selected; only twelve selected L1 cases are exactly
excluded, with no heavy selection, duplicate or whole/exact overlap. The
Worker-death file's fifteen exact selectors become one whole file; the
borrower file gains two exact selectors. Guards are legacy **151/43**,
placement **151/9/4**, reference **93/36/4**. Allowlists remain **88 multiprocess
/ 30 L1**, with all 118 exact IDs mapped. Counts/markers are not future safety
certificates. Wider faults, remaining historical runtime migrations, full
same-version gates and the GCS-fidelity decision remain open. K0/K1 is
incomplete, and existing GCS/DAG/phase-specific guarantees are unchanged.

### Previous borrower-startup-contracts checkpoint (2026-09-07)

The borrower-startup-contracts checkpoint passed **2755 tests, 12 deselected in
7.65s** after the manifest evidence update (first expanded run: 7.52s):
**193 whole files + 236 exact selectors in
38 other files**, 429 selectors. Compilation passed for
`conftest.py src tests scripts examples`. The only production-source change is
the submit docstring: `.remote()` does not wait for user execution, but its
serialization, lifted-argument seal and reference handoffs finish synchronously
before admission and may fail there. No runtime algorithm or guarantee changed.
The reviewed pure selection is not the complete default gate or same-version
all-ID acceptance.

Two original borrower cases now use threadless, real ownership/publication
composition. A canonical child put and nested-argument outer Task preserve
lineage and its retry budget; one real Grant/Start, INLINE discovery, child
pins, graph reservation, Complete and owner adoption produce the shared bytes.
Two loads acquire distinct borrower tokens through the real owner API. Outer
GC releases its contained and lineage holds while the second borrower remains
usable; only its final release allows child GC. The ambiguity case takes
effect at the owner, loses its Acquire ACK, then creates a real Release
tombstone that rejects the same Acquire replay. Its historical OwnerDiedError
injection is a transport-failure simulation, not an installed death fact.
One empty 1 KiB store, two tiny outputs, no runtime threads or reconstruction
execution. Both exact cases passed **2 in 0.21s**, and with the legacy guard
**15 in 0.20s**. The seven remaining live functions/twelve expanded cases in
that file remain heavy; it is still exact-selected, not whole-selected.

Three original Core-startup cases now preserve their real threads as bounded
L1, rather than pretending to be pure. Thread objects/starts/joins are tracked
by exact identity, and successful constructor rollback is asserted before
the fallback. The first two start only the reference thread and first lane:
one fails before lane 1 starts, the other after lane 0 really starts. The
idempotent-abort case starts all three threads; only the coordinator's periodic
Worker-death poll is explicitly deferred in the fixture. GCS remains configured,
the real loop runs, and all sync/RPC attempts have pre-constructor tripwires
whose retained records must be empty on the main thread. Abort is not faked.
The second abort performs no new join or sink close. Production join budgets
are one second; failure-only signals/joins share two seconds and never erase
task/owner/recovery state. Internal startup waits/locks still require the
outer 30-second runner. The original two pure functions are unchanged.

| Exact bounded selector | Result | Scope |
|---|---|---|
| `tests/unit/test_core_startup_rollback.py::test_lane_start_failure_stops_all_started_local_threads_without_rpc` | **1 in 0.13s** | four constructed thread objects, two started, two joined; original pre-start failure |
| `tests/unit/test_core_startup_rollback.py::test_lane_start_raise_after_start_is_still_joined` | **1 in 0.12s** | three constructed objects, two started/joined; original post-start failure |
| `tests/unit/test_core_startup_rollback.py::test_unpublished_abort_is_idempotent_and_never_syncs_gcs` | **1 in 0.12s** | three real threads; explicit background-poll isolation, real idempotent abort |
| `tests/integration/test_worker_death_ownership_path.py::test_dead_attempt_borrower_is_swept_while_logical_hold_spans_retry` | **1 in 1.06s** | original nested-import crash/retry; dead borrower removed while logical hold/lineage survive |
| `tests/integration/test_worker_owner_death_path.py::test_confirmed_worker_owner_death_fences_foreign_get_wait_and_release` | **1 in 1.28s** | original owner death; typed get/wait error and death-authorized Release convergence |

All five were fully reviewed and run individually, with approval, through the
30-second runner. The consumer-death process case has one 1 MiB store, one
logical Task/two attempts, three managed children at peak/four lifetime PIDs
and at most six endpoints including gate/owner/replacement. Shared 15s work,
64 passive RPC/four Acquire records, finite cleanup polls and shared 3s final
cleanup replace independent/unbounded waits. The real death journal and query
use scoped RPC deadlines without changing dispatcher deadlines or reply values.
All recorded endpoints and PIDs are checked in failure finally.

Owner death retains two Tasks/one SIGKILL, two 1 MiB stores, five managed
children at peak/six lifetime PIDs and at most seven endpoints. Before killing
the exact GCS-validated owner incarnation, the test now waits for the original
outer Task finish and actual collection, not just a local close receipt. The
installed death still produces OwnerDiedError for get/wait and discharges
unacknowledgeable Release; ownership is never reassigned. An additional
zero-resource lease-only probe observes the replacement's actual endpoint/PID
without Start/Push, then receives Cancel and empty custody ACK. This is an
extra unexecuted lease, not a third user Task or read-only operation. Shared
15s work/3s final cleanup and finite observation caps preserve the one-fault
scope. Initial and newly observed PIDs/endpoints are checked on failure too.

Runner/mode/placement guard contracts passed **78 in 0.18s**. Independent AST
inventory is **235 unit files, 2755 pure / 176 heavy / 20 L1**. All current unit
cases are selected, only the twelve existing L1 exclusions are selected then
deselected, with no heavy, duplicate or overlap. The three new L1 selectors
are outside the pure selection entirely. Guards are legacy **143/51**,
placement **151/9/4**, reference **93/36/4**. Allowlists are now **88 multiprocess
/ 30 L1**, with all 118 exact IDs statically mapped. Counts/markers do not
certify future fixture changes.

Wider fault matrices, historical runtime migrations, same-version full exit
gates and the explicit GCS-fidelity decision remain open. GCS publication,
global contained DAG and phase-specific recovery guarantees are unchanged;
K0/K1 is incomplete. No heavy or unreviewed test ran.

### Previous node-lifecycle-contracts checkpoint (2026-09-07)

The node-lifecycle-contracts checkpoint passed **2753 tests, 12 deselected in
7.39s** after the manifest evidence update (first expanded run: 7.54s):
**193 whole files + 234 exact selectors in
38 other files**, 427 selectors. Compilation passed for
`conftest.py src tests scripts examples`. No production source changed. This
is the explicitly reviewed pure selection, not the complete default gate or
same-version all-ID runtime acceptance.

Two original heavy cases now use real, guarded, threadless authorities:

- `test_core_drop_marks_put_lost_and_get_reports_unreconstructable` uses
  canonical Core put/get/drop and actual Node Seal/Get/Drop with one 1 KiB
  store and three exact RPC boundaries. Only the real Drop ACK authorizes
  owner LOST; the put remains without producer lineage, a repeated drop makes
  no RPC, and get raises the original typed error without retry. Normal local
  close/GC forgets the put. The exact case passed **1 in 0.16s**.
- `test_worker_exit_reclaims_running_lease_and_fences_late_completion` now
  obtains a real Grant/Start and prepares one tiny INLINE output through
  INTENT/ARM before the passive exit input reaches the actual Node reclaim
  reducer. Late successful Complete is rejected because the lease is no
  longer live RUNNING, not merely because preparation is missing. One
  resource release, one real SLOT_DROP/rollback ACK and a second no-op driver
  round leave no retained publication or pending cleanup. The exact case
  passed **1 in 0.14s**. It does not call the thread-starting Worker-stop
  helper or claim real process detection/restart.

Both whole files and the two fixed classification guards passed **59 in
0.27s**; runner contracts passed **31 in 0.07s**. Their eighteen previous
exact selectors become two whole-file selectors. No new unit file or case was
added. Independent AST inventory is **235 unit files, 2753 pure / 181 heavy /
17 L1**; all current unit cases are selected, exactly twelve L1 cases are
excluded, with no heavy selection, duplicate or whole/exact overlap. Guard
totals are legacy **141/53**, placement **151/12/1**, reference **93/36/4**.
Inventory and marker checks do not certify future fixture changes.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_node_crash_recovery_path.py::test_remote_node_death_retries_task_on_survivor_and_reports_crash` | **1 in 1.16s** | original remote executor loss, survivor capacity gate, stable task/object and new attempt/lease |
| `tests/integration/test_startup_rollback_path.py::test_second_node_ready_failure_rolls_back_every_started_process` | **1 in 7.03s** | original second-Node-ready failure, init's own rollback verified before fallback shutdown |
| `tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example07]` | **1 in 1.07s** | original PG example with actual committed placement and explicit strategy limitation |

Each exact case was fully reviewed and run alone through the 30-second runner.
Remote Node recovery retains five children, three logical Tasks and one kill/
retry, now with two 1 MiB stores, seven endpoints including owner/gate, one
15s work budget, at most 64 passive lease records and shared 3s final reference/
gate cleanup. Its actual small control arguments remain INLINE. The test no
longer assumes a timely GCS resource hint; Node-local capacity remains the
placement authority. Failure finally checks every recorded PID/endpoint, and
the report still distinguishes the crashed victim from graceful survivors.

Startup rollback retains five children, no Tasks/Actors/trace, two 1 MiB
stores and at most eight passive callback records. The original checkpoint
and Process/startup identity assertions remain. All five known PIDs/endpoints
must already be gone before fallback shutdown; finally also handles an
unexpectedly successful init and closes successful diagnostic connections.
The external runner bounds startup/rollback, not a fabricated per-call timeout.

Example 7 prints the committed public PGID/attempt and both bundle→NodeIDs
before submitting its original two Tasks. It checks distinct actual Nodes and
keeps the original executor-PID assertions. STRICT_SPREAD is explained as a
hard constraint, while two one-CPU Nodes/two one-CPU bundles also force PACK
to use both Nodes. This run therefore does not distinguish those policies or
observe PREPARE-time visibility. No synthetic event log, trace collector, new
fault, Task, control RPC or observation wait was added.

Allowlists remain **88 multiprocess / 27 L1**, with all 115 exact IDs statically
mapped. The wider fault matrices, remaining historical runtime migrations,
same-version full exit gates and explicit GCS-fidelity decision remain open.
No GCS publication, global contained-DAG or phase-specific guarantee changed;
K0/K1 is incomplete, and no heavy or unreviewed test ran.

### Previous recovery-contracts checkpoint (2026-09-07)

The recovery-contracts checkpoint passed **2751 tests, 12 deselected in
7.42s** after the final monitor-comment correction (first expanded run: 7.61s;
post-evidence run: 7.62s): **191 whole files + 252 exact selectors in
40 other files**, 443 selectors. Syntax compilation passed for
`conftest.py src tests scripts examples`. This is the reviewed pure selection,
not the complete default gate or same-version all-ID runtime acceptance.
No production algorithm changed: Core shutdown and API monitor comments now
describe the existing unresolved-work and graceful-finalization behavior.
Timeout is not transaction cancellation or a strict whole-method return bound;
unresolved work can still adopt a late valid result.

Two original heavy cases now use guarded, threadless composition:

- Worker-loss dependency hold: real producer publication and canonical consumer
  submission, actual Node Grant/Start/reclaim/outcome, retry on an existing
  passive survivor slot, selected-output adoption and normal finish/GC. The
  submitted hold survives retry; finish leaves lineage until consumer output
  GC permits producer GC. One empty 1 KiB store, two Tasks, three leases and
  two tiny INLINE outputs. The exact case passed **1 in 0.25s**; it does not
  execute user code or prove real process detection/replacement.
- Queued PG participant loss: real two-Node STRICT_SPREAD PREPARE/COMMIT,
  NodeRegistry death input, full-group LOST, survivor ABORT, installed snapshot
  and Core observer. The original queued survivor-bundle Task fails before any
  Lease/Push, with unchanged attempt and retry budget, then closes/collects
  normally. Two empty 1 KiB stores. The exact case passed **1 in 0.20s**; the
  dead fixture ledger is not erased to pretend to be OS destruction.

The whole Worker file, that PG exact case and both classification guards passed
**46 in 0.36s**. The manifest runner contracts passed **31 in 0.07s**. No new
unit case or file was added: the pure increment is two original-case migrations.
The Worker file's twelve former selectors become one whole-file selector;
the PG file gains one exact selector and still excludes its four heavy cases.
Independent AST inventory is **235 unit files, 2751 pure / 183 heavy / 17 L1**.
All current unit cases are selected, exactly twelve L1 cases are excluded, and
there is no heavy selection, duplicate or whole/exact overlap. Guard totals
are legacy **140/54**, placement **150/13/1**, reference **93/36/4**. These are
inventory/classification facts, not future fixture safety certificates.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_driver_local_node_recovery_path.py::test_driver_local_node_death_migrates_home_and_retries_on_survivor` | **1 in 1.14s** | original home-loss retry, stable owner/Task/Object identity, stored results and migrated put |
| `tests/integration/test_worker_crash_recovery_path.py::test_after_complete_worker_crash_recovers_output_without_reexecution` | **1 in 0.95s** | successful Complete survives executor loss at attempt 0; independent unexecuted lease observes replacement |
| `tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example06]` | **1 in 0.85s** | original lineage example plus observed Task/Object identity and attempt 0→1 |

Each was fully reviewed and run alone through the 30-second runner. These
three process results precede only the last documentation-only monitor
clarification; no executable runtime statement changed afterward. Driver home
recovery initially failed **1 in 11.19s**: the old threshold of one lifted
its control arguments into home-only puts without lineage, so the killed Node
also destroyed unreconstructable inputs. A 1 KiB inline budget now keeps those
three actual TaskSpec arguments INLINE; the two Worker results and migrated
put use 2 KiB payloads to preserve their STORED paths. The original Node death,
retry/lease identities and five-PID/seven-endpoint teardown remain checked,
with 15s work, 32 passive lease records and shared 3s final cleanup. This fixes
the test precondition, not put reconstruction or the production retry policy.

The historical Worker test first failed **1 in 0.95s**, observing attempt 0
where its stale contract required attempt 1. CRASH occurs after successful
Complete, so the surviving Node supplies the original envelope and user code
must not rerun. The renamed exact ID replaces the old
`test_after_complete_worker_crash_retries_on_fresh_worker`; its historical
retry assertion is not reported as passing. GCS records exit 23; original
Task/lease/attempt, Node adoption and payload retirement are checked separately
from replacement. A new, zero-resource, empty-input/return lease-only probe
obtains the replacement endpoint/PID, never sends Start/Push, and receives real
Cancel plus empty custody ACK. It is an extra unexecuted lease, not a second
user Task or read-only query. One 1 MiB store, three managed children at peak,
four lifetime PIDs, at most five distinct endpoints, 15s work/3s final cleanup.

Example 6 keeps its original producer, equal values, single drop and single
reconstruction. After both gets succeed it observes only the Driver's
`task_submitted` and `object_reconstruction_started` records, validates exact
IDs and local ordering, then prints the actual attempts. At most 201 snapshots
and two seconds of delivery observation fit inside the original 10s budget;
trace never decides runtime progress or claims a complete distributed trace.

Allowlists remain **88 multiprocess / 27 L1**, with one explicit Worker ID
replacement. Wider fault combinations, remaining historical runtime migrations,
same-version exit gates and the GCS-fidelity decision remain open. The global
contained DAG and phase-specific publication guarantees are unchanged. K0/K1
is incomplete; no heavy or unreviewed test ran.

### Previous dispatch-continuations checkpoint (2026-09-07)

The dispatch-continuations checkpoint passed **2749 tests, 12 deselected in
7.47s** after the manifest evidence update (first expanded run: 7.40s):
**190 whole files + 263 exact selectors in 41 other files**, 453 selectors.
Syntax compilation passed for `conftest.py src tests scripts examples`. This
is still the explicitly reviewed pure selection, not the full historical/
default gate or same-version all-ID runtime acceptance.

`_ReadyTask` now derives an immutable `_DispatchKind` from its existing
continuation fields. At most one top-level lease/cancel/Push/custody/output/
system continuation may be present; a nonzero ambiguity round requires the
original lease, while zero-round lease/capacity work remains legal. The
dispatcher reads this tag instead of duplicating long absence checks. Only
FRESH receives new PG admission, and only the two OUTPUT kinds keep the
existing non-PENDING exception. No `_execute` argument, RPC, payload, retry
authority or cleanup order changed. The tag describes queued work, not the
current unresolved marker: old cancellation work may still encounter newer
custody and must defer to the existing resolver.

Thirteen new envelope/routing cases use opaque payloads and synchronous
spies, not fabricated protocol validity. They verify eight-way dispatch,
mutual exclusion, exact forwarding, ambiguity validation, re-derivation after
replace and the original non-PENDING filter. Combined with the real pure
publication/PG continuation paths, **19 passed in 0.21s**. Independent static
inspection found all thirteen production constructors select exactly one
kind; no legal pair of top-level continuation fields was removed. Nested
orphan cleanup, cancellation replies and custody inventories are not subjected
to a new recursive exclusivity rule. This is a teaching-clarity and invalid-
construction guard, not a claim that a new public race was reproduced.

Three original spillback/Push-replay cases migrated from heavy to pure with
real NodeRegistry registration, Hybrid/Grant/Start/Complete and selected
INLINE publication/adoption. The timeout leaves a real RUNNING lease and CPU
held; explicit Worker rejection retains the GRANTED lease. One manual replay
uses identical Push bytes without a new lease or submitter-side release.
Normal finish, local finalizers and owner GC converge afterward. The cases
passed **3 in 0.20s**. Their Node helper and other test bodies are unchanged;
the original two-thread duplicate-spillback case remains heavy.

Three original foreign-STORED report cases also migrated to pure, now using
canonical Core submission and TaskID-scoped foreign lineage. Two tiny real
Nodes seal/pull the input; one real borrower is acquired before submission,
then its handle closes while the retained credential survives. Report ACK
loss, exact report replay, custody ACK and either real INLINE publication or
definite Push failure use the current authorities. Importantly, normal Task
finish does **not** release the retained lineage hold; output collection and
its exact owner Release ACK do. The prior direct fixture did not express that
normal lifetime. The unresolved shutdown test calls the real early-return
path without threads or joins, then explicitly cancels the known grant and
collects through normal authorities. It is not a public process-shutdown test.
All three passed **3 in 0.20s**; the legacy classification guard plus all new
and migrated cases passed **32 in 0.23s**. Two historical owner-death/stale
cases remain heavy and unchanged rather than manufacturing lifecycle proofs.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_foreign_stored_ref_path.py::test_inline_outer_restores_foreign_ref_then_driver_fetches_stored_bytes_from_node` | **1 in 1.01s** | original INLINE outer/foreign stored child, independent borrower after outer close, real Node byte fetch |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[terminal]` | **1 in 1.10s** | retained completed output adopts/retires after peer-PG loss and terminal ACK loss |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[adopted]` | **1 in 1.06s** | READY output still retires Node payload and finishes after adopted ACK loss |

Each was reviewed and run alone with the 30-second runner after the dispatch
change. The foreign-ref case now has one 10s work deadline, a 16-reply passive
cap and shared 3s failure reference cleanup; four child PIDs/five endpoints
are checked in failure finally. Its topology stays one Node/two Workers, one
1 MiB store and two Tasks/64 KiB data. The returning parent keeps its child
handle alive through result discovery and only closes it on its error path;
closing a successful returned handle before discovery would change semantics.
The PG pair retains its existing five children/two 1 MiB stores, 15s/3s bounds
and expected unclean victim diagnostics. These are three old exact cases,
not a new all-fault matrix.

The pure increment is **13 new envelope cases + 6 original-case migrations**.
The two mixed migrated files remain exact-selected, with their three heavy
cases excluded. Independent AST inventory counts **235 top-level unit files,
2749 unit / 185 heavy / 17 L1 cases**. All current unit cases are selected;
the only selected non-unit cases are exactly the 12 named L1 exclusions. No
heavy case, duplicate or whole/exact overlap is present. The fixed legacy
classification guard now covers 139 pure / 55 heavy; reference and placement
guard totals are unchanged. This is inventory, not future safety certification.
Allowlists remain **88 multiprocess / 27 L1**. The learning
path/design now distinguish dispatch turns from execution/owner state without
adding a second runtime. Remaining historical runtime migrations, wider fault
combinations, all-ID same-version gates and the GCS-fidelity contract decision
remain open. No heavy tests ran; K0/K1 is incomplete.

### Previous Worker-locality/admission checkpoint (2026-09-07)

The Worker-locality/admission checkpoint passed **2730 tests, 12 deselected
in 7.52s** after the manifest evidence update (first expanded run: 7.63s).
The scope is **189 whole files + 257 exact selectors in 41 other files**,
446 selectors. Compilation passed for `conftest.py src tests scripts examples`.
No production source changed in this checkpoint; it adds a missing real
Worker path and migrates eleven original protocol cases into reviewed pure
composition. These results are not the complete historical/default gate.

One new process case exercises a real embedded Core without an installed
snapshot. Driver owns a 32 KiB stored source on B; a zero-CPU parent on A
imports its nested handle and submits two sequential, unconstrained children.
Their locality scoring scopes perform **one cold GetNodeAddress query, then
zero on the cached selection**, and both first leases/direct Pushes target B.
The test does not install a snapshot, choose a route or substitute an ACK.
Adoption has separate address queries, explicitly counted outside scoring;
this is not a claim that the whole runtime makes only one GCS call.

Child outputs remain owned by the parent Worker, while the source remains
Driver-owned. Each child's real retained-owner read/report, custody ACK and
Push order are checked. Child collection must also clear foreign-lineage
receipts/registry and receive both exact Release ACKs before closing the
parent's attempt-borrowed handle. Driver independently checks released
borrower/retained tokens and its one remaining parent lineage root, then
collects parent and source. A real Node DROP ACK and a bounded GetObject
absence observation confirm physical source cleanup while both Workers are
still alive; no owner death or test-driven GC operation supplies that proof.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_worker_lease_locality_path.py::test_worker_without_snapshot_caches_cold_locality_for_foreign_stored_dependency` | **1 in 1.26s** | new cold-cache Worker/foreign-reference path, four Task executions, source GC without owner death |
| `tests/integration/test_foreign_stored_dependency_path.py::test_foreign_stored_dependency_pulls_node_to_node_before_push` | **1 in 1.16s** | original pending Worker-owned source, closed Driver input, target pull and owner report before Push |
| `tests/integration/test_worker_nested_task_path.py::test_worker_submits_child_task_and_gets_plain_result` | **1 in 1.17s** | original no-stored-dependency Worker child and causal trace |
| `tests/integration/test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get` | **1 in 1.12s** | original escaping Worker-owned ref and two distinct borrower lifetimes |

Every exact case was statically reviewed and executed alone through the
30-second runner, with five managed children and two 1 MiB stores. The new
case has six endpoints, 15s work, a shared parent close/GC subdeadline of up
to 3s within work, and a shared Driver close/GC deadline of 3s. Parent/Driver
passive observation caps are 64/24. It covers cold success and positive-cache
reuse, not cold-query failures, concurrent misses or a Node-death matrix.

The original foreign-stored dependency test now acquires its listener inside
the failure boundary, passes one 15s work deadline through its existing gate
and get operations, bounds public closes, and checks all five PIDs/seven
endpoints in failure finally. Its at-most-64 records and Driver-byte-fetch
flag are passive; no observer changes a real response to satisfy the test.
The original three Tasks, 64 KiB payload, foreign owner and report-before-Push
assertions remain. A close receipt is not relabeled as a remote release ACK.

Five original Core cancellation functions/eight expanded cases now use one
threadless Core and real Node Grant/Release/Cancel/Outcome reducers. Lost
Cancel ACK follows the actual cancellation effect; a negative Release reply
comes from a real release plus its duplicate, with the original advisory
text retained. PENDING-before-ACK, unchanged attempt/budget, sticky error
identity and exactly-once resource release remain checked. Direct cancellation
also ACKs its actual empty inventory; the known-Grant scalar path retains its
existing no-extra-custody-RPC optimization. Each case finishes and collects
through the real local finalizer/owner authorities, without pretending to
exercise public runtime shutdown. The whole file passed **11 in 0.19s**.

Three original PG cases now use a real attempt-3 bundle, Node lease and
selected-output publication. They preserve direct targeted submission,
capacity replay of the same lease, and Task attempts 0/1/2 across SYSTEM
retry and reconstruction. Only a physical replica is dropped; the PG remains
CREATED, so reconstruction is legal. The first run had **1 failed, 2 passed
in 0.24s** because the old fixture expected Python `is` identity for a key
copied by the real publication snapshot. The test now checks every immutable
capability field via value equality; no production identity rule changed.
The three passed **3 in 0.21s**. Combined with cancellation and the placement
classification guard, the reviewed scope passed **30 in 0.31s**. All eighteen
other PG test bodies, including the five heavy expanded cases, are unchanged.

The pure increment is **8 migrated cancellation + 3 migrated PG**, not new
duplicate tests. The fixed placement guard now covers 149 pure / 14 heavy /
1 L1; reference classification is unchanged. Independent AST inventory counts
234 top-level unit-test files, **2730 unit / 191 heavy / 17 L1** cases. Every
current unit case is selected; the only selected non-unit cases are the exact
12 named L1 exclusions, with no heavy case or whole/exact overlap. The mixed
PG file remains exact-selected, not whole-selected. This proves inventory,
not future fixture safety or the unrun runtime contracts.

Allowlists are **88 multiprocess
/ 27 L1**, not 115 current-version passes. Broader Worker cold-route/failure
coverage, remaining historical runtime migrations, all-ID same-version exit
gates and GCS-fidelity decisions remain open. Lease-locality, publication,
global contained DAG and phase-specific guarantees are unchanged. No heavy
test ran; K0/K1 remains incomplete.

### Previous lease-locality checkpoint (2026-09-07)

The lease-locality checkpoint passed **2719 tests, 12 deselected in 7.19s**
after the manifest evidence update (first expanded run: 7.18s). Its explicit
scope is **188 whole files + 257 exact selectors in 42 other files**,
445 selectors total. The previous 2695 selection passed on the new routing
implementation in 7.31s. Syntax compilation passed for
`conftest.py src tests scripts examples`. These are reviewed subsets, not the
complete historical/default gate or all-ID runtime acceptance.

Ordinary fresh leases now have a small metadata-only locality policy. It
counts each stored dependency's size once per known replica Node, prefers the
largest byte total, and breaks positive ties by home then NodeID. It does not
filter total/available resources or allocate anything: the first Node still
owns Hybrid scheduling and may spill back. requester_node_id remains the
captured home and the owner WorkerID is unchanged; preferred_node_id names
the actual first hop, and only the second hop is
targeted. Self-spillback now compares the actual receiving Node, not the
requester, so data-Node -> home is a legitimate resource override.

Core projects local-owner locations only from a matching current-attempt
READY_STORED canonical result outside collection/retirement. An original
publisher need not still hold a replica; a healthy secondary remains useful.
Foreign objects contribute only their existing retained descriptor source,
not an invented remote location table. Input descriptors, holds, selected
output identities and the Node's physical localization checks are unchanged.
Installed snapshots supply routes without control I/O. A Core without one
uses a positive address cache and, on a cold miss, the existing address lookup
with at most a 0.75s/enclosing deadline. Invalid/unavailable lookup loses only
the hint; success is validated and rechecked against current snapshot/death
facts before caching. No new membership/death authority or mandatory GCS
availability prerequisite was introduced.

The 24 new pure cases cover byte scoring/deduplication/ties and Core route
projection, current-version checks, positive cache/deadline restoration,
malformed lookup fallback, death/snapshot during lookup, real B -> home
spillback, ACK-loss/capacity exact-hop replay and PG bypass. Core cases use
two unstarted 1 KiB Nodes, actual sealed put identities and real
Grant/custody/Start/SYSTEM_ERROR Complete reducers, without executing user
code or fabricating successful Task outputs. The first Core run was
**1 failed, 11 passed in 0.32s** because the fixture's manual put reused
Core.put's sequence zero; reserving the existing put sequence fixed that
fixture, not runtime semantics. The two files then passed **24 in 0.18s**.
The original spillback DTO and two empty-dependency/migrated-home selectors
also passed **6 in 0.17s**, without changing their assertions.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_lease_locality_path.py::test_stored_dependency_selects_data_first_hop_and_resources_can_spill_back_home` | **1 in 1.14s** | three public Tasks: producer A -> B, ordinary consumer first asks B, resource-constrained consumer B -> A; owner custody and final replica GC |
| `tests/integration/test_cross_node_dependency_pull.py::test_store_backed_dependency_pulls_to_consumer_node_before_direct_push` | **1 in 1.14s** | original unready dependency and resource-forced cross-Node pull, unchanged assertions |
| `tests/integration/test_two_node_spillback.py::test_custom_resource_spills_task_to_second_node_and_cleans_cluster` | **1 in 1.10s** | original no-dependency home fallback and custom-resource placement |
| `tests/integration/test_placement_group_path.py::test_strict_spread_tasks_use_committed_bundles_and_remove_restores_resources` | **1 in 1.06s** | original committed PG route and explicit removal, unaffected by locality |

Each process selector was statically reviewed and run alone through the
30-second runner, with one GCS/two Nodes/one Worker each and two 1 MiB stores.
The new case has six endpoints, a 32 KiB payload (serialized object <=64 KiB),
15s work and one shared 3s close/GC deadline. Its at-most-32 passive
observations record real lease/grant/custody-before-Push ordering and both
exact replica DROP ACKs before complete owner/recovery collection. It does
not get the stored producer in Driver, rewrite the scheduler, or send repair
RPCs. It observes Driver messages and owner state, not a direct Node-to-Node
wire trace or a general zero-copy guarantee. The real case exercises Driver
installed-snapshot routing; Worker cold lookup and its failure races have
only the stated pure evidence here.

Independent static inventory is **234 top-level unit-test files, 2719 unit /
202 heavy / 17 L1 cases**. All current unit cases are selected; exactly the
12 selected non-unit cases are named L1 exclusions, with no heavy case in the
selection. Fixed historical guard totals are unchanged. The runner now has
**87 multiprocess / 27 L1** exact entries, not 114 same-version passes.
README, the optional learning-path 2A and production mapping explain the
first-hop/placement distinction without changing the original seven-example
tour, diagrams or historical results. Cross-task leased-Worker reuse, broader
locality/failure coverage, historical runtime migrations, all-ID same-version
gates and GCS-publication fidelity remain open. The synchronous publication,
global contained DAG and phase-specific K1 guarantees are unchanged.
No heavy tests ran; K0/K1 remains incomplete.

### Previous PG-retry-atomicity checkpoint (2026-09-07)

The PG-retry-atomicity checkpoint passed **2695 tests, 12 deselected in
7.38s** after the manifest evidence was updated (first expanded run: 7.85s):
**186 whole files + 257 exact selectors in 42 other files**, 443 selectors
total. Syntax compilation passed for `conftest.py src tests scripts examples`.
This remains a reviewed pure subset, not the complete historical/default gate.
The same Core source was used for the seven individually bounded process
results below; no process tests ran in parallel.

The PG phase check previously released `_state_lock` before owner/recovery
retry commits. A committed peer-Node death could make the PG LOST in that gap,
yet the failed Task would still advance AttemptID and consume retry budget.
The new pure composition first failed with the exact observed order
`pg_lost -> owner_retry -> recovery_retry` (**1 failed, 2 passed in 0.16s**).
It uses two actual in-memory Node reducers, real STRICT_SPREAD prepare/commit,
canonical Core submission and Grant/Start/SYSTEM_ERROR Complete; a one-shot
synchronous callback runs after the real outer Core lock releases. Death is
typed reducer input, not an OS event. No thread, socket or OS process starts;
no user callable or successful-output publication runs.

The phase check now shares the existing retry critical section. Death first
means no retry; retry first may advance once, but the subsequent fresh-PG gate
must reject it without another Lease or Push. The three cases passed **3 in
0.21s**, including death-before-entry and unrelated-PG ordinary retry. PG
terminal precedence, late-replica cleanup rules and budget-exhaustion error
publication are unchanged. Error normalization remains after the PG branch;
normal callers supply internal SystemTaskError values, while the private
non-SystemTaskError fallback now formats under that same lock. No RPC or new
coordinator was added.

Two original test migrations are now included: multi-return submission
rollback retains all six original functions/eight expanded cases, real
owner/recovery aborts and saved-finalizer detachment; its unexecuted producer
stays PENDING after handle release. The first three ordinary retry cases now
use real submission/finish barriers, finite manual FIFOs, public close and
explicit owner GC; the last three reconstruction bodies are unchanged. Their
14-case scope increment is **8 migrated + 3 migrated + 3 new**, not duplicate
replacement tests. The three files plus both classification guards passed
**49 in 0.33s**.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_task_retry_path.py::test_explicit_worker_system_error_retries_once` | **1 in 0.92s** | real SYSTEM_ERROR attempt 0, one user execution on attempt 1, stable TaskID and different LeaseIDs |
| `tests/integration/test_placement_group_path.py::test_strict_spread_tasks_use_committed_bundles_and_remove_restores_resources` | **1 in 1.14s** | two tiny bundle-bound Tasks, explicit removal and clean resources |
| `tests/integration/test_placement_group_path.py::test_shutdown_removes_committed_group_without_explicit_remove` | **1 in 1.08s** | committed PG drained by shutdown, no application Task |
| `tests/integration/test_placement_group_prepare_failure_path.py::test_second_participant_prepare_rejection_aborts_first_and_restores_roots` | **1 in 1.19s** | original second-PREPARE rejection, both real ABORT ACKs and causal trace |
| `tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg` | **1 in 1.15s** | original running-task Node loss and ordinary survivor probe |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[terminal]` | **1 in 1.07s** | retained Complete still adopts and retires after peer loss/ACK loss |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[adopted]` | **1 in 1.07s** | READY still retains its Node-retirement and finish obligations |

The participant-loss regression initially failed **1 in 1.11s** at its
immediate GCS-availability assertion after the survivor probe succeeded.
Unified Complete intentionally releases the Node ledger before the supervisor
reports its versioned resource hint. The test now passively polls GetNodes
within the original 15-second deadline and 1024-read cap, validating the exact
survivor incarnation and unchanged total on every reply. It retains the final
full-availability assertion; no Node flush, metadata repair, additional Task
or longer timeout makes it pass. Failure-finally cleanup also ran on the
failed invocation. This fixes an observation assumption, not a production
resource-release defect.

The first four process cases now have fully bounded work/reference cleanup
and failure-finally PID/port checks. Every store is 1 MiB; PG cases start five
children, ordinary retry starts three. Original synchronous PG create/remove
still rely on the 30-second outer runner, not timeout-as-cancellation. The
Node-loss cases retain their existing 15s work/3s reference-close bounds and
expected unclean victim diagnostics. They do not dynamically reproduce the
new lock-window race; that evidence is the pure interleaving above.

README now leads with project positioning and the seven-example learning
entry. Its earlier progress prose is preserved verbatim in a historical
details section. This changes navigation, not the single backend or any
GCS/global-DAG/phase-specific guarantee. Independent AST inventory counts
232 top-level unit-test files: 2695 unit, 202 heavy and 17 L1 cases. Every
current unit case is selected; the only selected non-unit cases are exactly
the 12 named L1 exclusions. This checks inventory, not future fixture safety
or runtime correctness. The fixed reference guard covers 93 pure / 36 heavy /
4 L1; the placement guard covers 138 pure / 25 heavy / 1 L1. The new atomic
file is separate from those fixed historical inventories.

Allowlists remain **86 multiprocess /
27 L1**. Broader failure combinations, remaining historical runtime migrations,
same-version all-ID exit gates and the GCS-fidelity contract decision remain
open. No heavy test was run; K0/K1 is incomplete.

### Previous publication-continuations checkpoint (2026-09-07)

The publication-continuations checkpoint passed **2681 tests, 12 deselected
in 7.90s** after the final manifest evidence/scope and runner allowlists were
updated (first expanded run: 7.82s): **183 whole files + 260 exact selectors
in 43 other files**, 443 selectors total. The prior 2668 selection passed on
the dispatcher fix in 7.80s. Syntax compilation passed for
`conftest.py src tests scripts examples`. This is an explicit reviewed scope,
not the complete historical/default/runtime gate.

Core dispatch incorrectly treated retained publication continuations as new
PG admission. After a real Complete, losing another bundle Node could make
the PG LOST while a terminal/adopted ACK retry was queued. Before owner CAS,
the shortcut published ERROR over the completed task; after owner CAS, it
could mark the task finished without retiring the Node payload. The new two
pure phase cases failed at those exact assertions before the fix. The initial
PG gate now excludes output-adoption, output-Node-loss and deferred-system
continuations, alongside the existing lease/cancel/Push/custody continuations.
Its PG exception handler is scoped only to fresh admission. Existing protocol
authorities retain responsibility; no new backend, ERROR discard shortcut,
cleanup saga or retry semantics was introduced.

Six new pure cases cover both adoption cuts, known/unknown Node-loss cleanup
ACK replay, fresh PG rejection and deferred-system routing. The loss pair
uses real in-memory GCS metadata reduction, not physical replica/child GC;
deferred-system coverage is routing-only, with no pending late replicas.
Seven original owner-integration cases were also migrated to threadless
composition and real selected-output replies. Their first six-pass/one-fail
iteration exposed an invalid fixture cut: injecting ERROR without a remote
fence and then inventing a successful same-attempt publication. The corrected
case first obtains real Node cancellation/custody ACKs, preserves
ERROR-before-finish, and rejects LateStart/Complete plus a descriptor-only
false success. It does not discard a legitimate unadopted publication. The
seven migrated plus six new cases passed **13 in 0.28s**; the 13-case scope
increase preserves every original owner test ID.

Two new exact process cases reproduce the actual continuation bug with one
tiny PG Task on a surviving Node. After the real terminal/adopted GCS report
ACK, the original dispatcher crashes only the idle peer Node through the
existing full death barrier, then discards that one ACK. No ReadyTask, owner
state or cleanup ACK is fabricated. The original runtime must replay, retain
the same publication/attempt, finish, and receive a real Node payload-retirement
ACK. A metadata-only Node outcome independently confirms no retained envelope
and the same Complete witness. Each case has five children, two 1 MiB stores,
six endpoints, 15s work / 3s public close and the 30-second outer runner; there
are **two controlled events**, ACK loss plus peer death, not a generic
single-fault claim. No test-owned thread/listener or user-code retry is added.

| Exact process selector | Result | Scope |
|---|---|---|
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[terminal]` | **1 in 1.24s** | owner still PENDING; existing Complete adopts and retires after PG loss |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[adopted]` | **1 in 1.12s** | owner already READY; Node payload and finish tail still must converge |
| `tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg` | **1 in 1.28s** | unchanged running-task participant-loss regression after dispatcher fix |

Four original object/Worker paths were bounded and run separately before the
dispatcher fix: public put **1 in 0.84s**, stored result **1 in 0.90s**, Worker
nested task **1 in 1.21s**, and Worker-owned escaping ref **1 in 1.14s**. Their
exact selectors are in `testing.md`. They retain the 64 KiB/1 MiB data limits,
0-CPU parent arrangement, original trace/owner assertions and no-retry scope.
All observed PID/owner/trace endpoints are checked in failure finally. The
escaping-ref test distinguishes public-close receipt from the remote borrower
release before the second deserialization. These results are not silently
upgraded to a post-dispatch-fix run.

Four original local-reference tests now remain **real-thread L1**, not pure:
distinct handle tokens, close idempotency, FIFO stop/late close, and detached
pickle handle each use one reference consumer, one PENDING object, at most
two handles, finite close/join and a pre-start fixture finally. Their exact
bounded runs passed respectively **0.15s / 0.13s / 0.14s / 0.14s** before the
Core dispatch change. The two process-wide `gc.collect()` cases remain heavy.
No fake mailbox replaced this runtime evidence.

An independent AST inventory matches all 2681 currently unit-labelled cases
to the explicit manifest. The 231 top-level unit files also contain 213 heavy
and 17 L1 cases; four moved from heavy to L1, seven moved to pure. This is an
inventory assertion, not certification of arbitrary future imports/fixtures.
Allowlists are now **86 multiprocess / 27 L1**. Broader ownership/fault
combinations, remaining historical runtime migrations, all-ID same-version
exit gates and the GCS-fidelity contract decision remain open. No heavy test
was run; K0/K1 is incomplete.

### Previous replica-integrity/lifetimes checkpoint (2026-09-07)

The replica-integrity/lifetimes checkpoint passed **2668 tests, 12 deselected
in 8.23s** after manifest evidence and the scope contract were updated (first
expanded run: 8.07s): **180 whole files + 260 exact selectors in 43 other
files**, 440 selectors total. The previous 2655 selection passed separately
on the new Node code in 7.87s. Syntax compilation passed for
`conftest.py src tests scripts examples`. These remain explicit reviewed
selections, not a complete default/runtime gate.

The shared sealed-replica deletion tail and owner-death observation now check
actual byte length and SHA-256 before accepting a live replica as consistent.
Previously an injected same-length corruption could be reported PRESENT or
deleted with a successful receipt despite disagreeing with sealed metadata.
The first three pure regressions failed before the fix. Seven final cases
cover generic Drop, owner-wide sweep, publication-exact observation, read
exceptions, actual-length drift and a retained real pin. They first seal a
legal eight-byte value into one 1 KiB store, then explicitly inject private
byte damage or a read failure. No supported public fail-stop path was shown
to create that damage: this is a defensive integrity-contract repair, not a
claim of ordinary application data loss or automatic corruption recovery.

Unreadable/corrupt bytes preserve metadata and do not mint a deletion
watermark/receipt. After explicit repair a real pin still blocks deletion;
after unpin the same request completes. Publication-exact fences retain their
frozen CONFLICT witness, whereas an owner-wide sweep remains incomplete.
An already-completed receipt still returns before any byte read, and the
absent-bytes + exact-watermark path still retries ObjectManager forget. The
change deliberately does not skip all CONFLICT observations, which would
strand a real deletion whose later forget failed. New integrity cases plus
the existing cross-authority/owner-finalize contracts passed **66 in 0.36s**.

Six original lifecycle cases were migrated, not replaced: the two local
lineage-hold tests now use real selected-output discovery/adoption and
explicit leaf/producer finish plus manual GC; four nested-argument tests use
threadless Core authorities and real owner retain/release handlers. They
preserve PENDING nested-ref readiness, Task hold versus lineage lifetime,
SYSTEM retry identity, retain-ACK-loss compensation and one shared
top-level/nested foreign hold. The original assertions remain; no user code,
background consumer or real wait is hidden in the fixtures. The two
attempt-borrow/timing cases remain heavy and unchanged. The six migrated
cases plus the reference classification lock passed **22 in 0.27s**. The
13-case manifest increment is these six plus seven integrity cases; it does
not add duplicate tests for the six preserved IDs.

Four original Actor gates now use 1 MiB per Node, shared work deadlines,
finite public reference close and unconditional shutdown. Every observed
startup/Actor/replacement PID and owner/trace/gate endpoint is checked even
after a work error. A create failing before any physical route reaches Core
still relies on Node shutdown and the outer process-tree bound, not a guessed
Actor endpoint. Generation observers forward the real install first and
record test failures without injecting another business-RPC failure. No
per-call Actor cancellation semantics or transparent method retry was added.

Each exact case below was fully reviewed and run separately through the
30-second runner after the Node integrity fix; no pytest runs overlapped:

| Exact case | Result | Bound |
|---|---|---|
| `tests/integration/test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker` | **1 in 1.50s** | 6 children, two 1 MiB stores, 3 calls, 10s work / 3s cleanup, 8 endpoints |
| `tests/integration/test_actor_cross_process_trace.py::test_actor_creation_uses_control_plane_and_method_call_bypasses_gcs` | **1 in 1.18s** | 4 children, one 1 MiB store, 1 call, 10s work / at most 2s trace within it / 3s cleanup, 6 endpoints |
| `tests/integration/test_actor_restart_path.py::test_actor_crash_restarts_once_fences_inflight_call_and_resets_state` | **1 in 1.55s** | peak 4 children / 5 lifetime PIDs, one 1 MiB store, 4 calls, one Actor exit/restart, 15s / 3s, 7 endpoints |
| `tests/integration/test_actor_node_loss_migration_path.py::test_actor_migrates_after_remote_node_loss_and_resets_generation` | **1 in 1.79s** | peak 6 children / 7 lifetime PIDs, two 1 MiB stores, 1 Task + 2 Actor calls, one Node loss/migration, 15s / 3s, 9 endpoints |
| `tests/integration/test_precomplete_output_owner_death_path.py::test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds` | **1 in 1.56s** | 5 startup children + one Worker replacement, two 1 MiB stores, 8 KiB output, one owner exit, 15s / 3s |
| `tests/integration/test_cross_cleanup_receipt_path.py::test_publication_rollback_receipt_replays_after_same_object_retry_seals` | **1 in 0.94s** | 3 children, one 1 MiB store, 8 KiB output, one post-seal error/retry, 15s / 3s, 4 endpoints |

The last two exercise live owner-wide sealed cleanup and publication
rollback -> generic old-epoch receipt -> unaffected successor bytes. Their
local references now use the existing finite public-close helper, with no
shared helper change. The cross-cleanup test also checks observed PID/ports
in its failure finally. Corruption itself was injected only in pure tests.
Actor migration retains the expected victim-Node unclean diagnostics; it
does not pretend the deliberately crashed cluster exited normally.
Allowlists remain **84 multiprocess / 23 L1**. Remaining historical runtime
migrations, broader fault combinations, all-ID same-version acceptance and
the GCS fidelity decision are still open. No heavy test was run; K0/K1 is
incomplete.

### Previous PG-cancellation/classification checkpoint (2026-09-07)

The PG-cancellation/classification checkpoint passed **2655 tests, 12
deselected in 7.69s** through the final frozen reviewed entry (first expanded
run: 7.80s; post-evidence update: 8.18s): **178 whole files and
256 exact selectors in 42 other files**, 434 selectors total. The earlier
2187 selection was also rerun after shared fixture changes and passed in
7.36s, before the PG fix. Syntax compilation passed for
`conftest.py src tests scripts examples`. This is still an explicitly reviewed
subset, not the complete default gate or all-ID process acceptance.

Migrating an old cancellation fixture to real Node Cancel/inventory/ACK
handlers exposed a production bug: a Task whose PG was already LOST published
the `_LeaseRequestAmbiguous` wrapper instead of the selected
`PlacementGroupLostError` after custody converged. Core now retains that same
typed cause when this Task has a PG key, its current phase is LOST, and the
ambiguity cause is already PG loss. Non-PG wrappers and earlier latched
owner/local errors are unchanged. Error publication still waits for the exact
cancellation and custody obligations; no retry budget is consumed.
The original PG cancellation case first failed, then the whole Node-death
file passed **16 in 0.28s**. Four new pure regressions cover lost Cancel ACK,
lost custody ACK, a non-PG wrapper with a PG-typed cause, and a known Grant
whose resource release must not repeat. Their combined run with Node-death
and the classification guard passed **33 in 0.30s**; seven existing
cancel/custody/reference files also passed **73 in 0.40s**. These are real
in-memory handler compositions, not concurrent or public PG-loss-ACK evidence.

Four static classification locks cover 62 fixed historical files. The new
468-case increment is 77 reference + 130 placement/Node + 87 Worker + 108
previously unselected legacy-runtime cases, plus 62 AST checks and four new
PG tests. Mixed files use exact pure selectors; the original non-pure bodies
and IDs remain. Classification checks preserve an inventory, not future
safety or semantic correctness. Thread/socket tests were not silently made
pure to increase the count.
An independent read-only AST inventory of the 228 top-level unit-test files
matched all 2655 currently unit-marked cases to this explicit selection, with
no missing or multiply classified test functions. It also counted 230 heavy
and 13 loopback cases; 12 loopback cases in selected whole files are the exact
manifest exclusions, and the remaining localizer is run separately. This
closes the label/selection inventory, not safety after future source changes,
the retained historical runtime contracts or a complete default-gate claim.

Old successful fixtures now use the one actual selected-output path: Core
reconstruction, Push replay, Worker completion/supervisor, Node leases/pools/PG
and large stored results no longer bypass it with descriptor-only success.
STORED remains a real physical store (the large-result case retains its
64 KiB payload and 128 KiB store); a wrong late epoch cannot delete its
successor. Two earlier fixture failures were corrected without changing the
runtime: PG local Complete schedules a resource-report outbox rather than
synchronously reporting GCS; unpin completion does not acknowledge pre-grant
custody. Reconstruction validation is observed on both sides of retirement
before the owner/recovery commit. This checkpoint also corrected the
supervisor deletion-watermark expectation, foreign-report routing by semantic
guard identity, and the scoped-importer error assertion. The old multi-return
export rejection is explicitly a low-level injected rollback, not a current
Worker restriction. These statements cover migrated selectors, not all old
tests in those domains.

After complete static review, the following exact cases ran independently
through the 30-second runner on the PG-fix source; no pytest runs overlapped:

| Exact case | Result | Bound |
|---|---|---|
| `tests/integration/test_two_node_spillback.py::test_custom_resource_spills_task_to_second_node_and_cleans_cluster` | **1 in 1.26s** | 5 children, two 1 MiB stores, 10s work / 3s reference close, 7 endpoints |
| `tests/integration/test_cross_node_dependency_pull.py::test_store_backed_dependency_pulls_to_consumer_node_before_direct_push` | **1 in 1.38s** | 5 children, two 1 MiB stores, 64 KiB value, 10s / 3s, 8 endpoints |
| `tests/integration/test_lineage_reconstruction_path.py::test_stored_task_output_reconstructs_with_same_object_id` | **1 in 0.95s** | 3 children, one 1 MiB store, one reconstruction, 10s / 3s, 5 endpoints |
| `tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg` | **1 in 1.24s** | 5 children, two 1 MiB stores, one Node crash, 15s / 3s, 7 endpoints |

The first three had also passed separately before the PG fix (1.22s, 1.27s,
0.90s); those are not additional scenarios. PG-loss setup/listener and all
reference cleanup are now protected by finally, including owner/gate endpoint
checks on failure. The expected victim crash remains unclean in the shutdown
report; survivor cleanup is independently clean. This test is running-task
participant loss, not an integration reproduction of the ambiguous
surviving-lease ACK window. The separately reviewed two-thread localizer
`tests/unit/test_node_dependency_pull.py::test_concurrent_localizers_pull_once_and_second_uses_local_replica`
passed **1 in 0.17s** before the Core-only PG fix: no cluster or real network,
one 16 KiB-plus-three-byte transfer, bounded gates/joins and physical cleanup.

The learning path now keeps introductory examples 1–7 contiguous, with
publication/custody chapters 3A–3D afterwards. This changes reading order, not
the backend, K1 guarantees or acceptance claims. Allowlists remain **84
multiprocess / 23 L1**. The wider fault matrix, safe review of remaining
runtime/legacy tests, same-version full exit gates and the GCS fidelity
decision remain open. No heavy test was run; K0/K1 is incomplete.

### Previous bounded-reference-close checkpoint (2026-09-07)

The bounded-reference-close increment passed **2187 tests, 12 deselected in
7.29s** through the final reviewed entry (first expanded run: 7.22s): 144 whole
files and 126 exact selectors in 30 other files. Syntax compilation passed for
`conftest.py src tests scripts examples`. This remains a reviewed subset, not
the complete default unit gate.

`ObjectRef.close(*, timeout=None)` now supports a finite non-negative receipt
wait. Timeout leaves the handle closed and its original release obligation
pending; another close waits for the same receipt without a second enqueue.
Default `None` remains unchanged, invalid values fail before mutation, and
detached handles remain inert. The receipt describes local release/event intent,
not necessarily the remote owner's ACK or physical object/lineage GC. The
existing runtime still owns those unfinished effects. Twenty-six pure tests
passed separately in 0.22s using real finalizers and scripted wait/clock values;
they do not claim real timing behavior.

All seven original example mains now use a 1 MiB store per Node, shared get
budgets, finite public close and unconditional shutdown. A parameterized
acceptance loads each original script without running main at import, then
calls that main with passive init/close/shutdown observations. The actual
Actor endpoint is recorded on creation, and the real PG handle is explicitly
removed once on its normal path. The first shutdown result cannot be replaced
by a later fallback. Every exact case passed independently on the final source:

| `test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster` parameter | Result | Children |
|---|---|---|
| `[example01]` | **1 passed in 0.92s** | 3 |
| `[example02]` | **1 passed in 1.25s** | 5 |
| `[example03]` | **1 passed in 1.21s** | 5 |
| `[example04]` | **1 passed in 1.35s** | 6, including its Actor |
| `[example05]` | **1 passed in 1.03s** | 4 |
| `[example06]` | **1 passed in 0.92s** | 3 |
| `[example07]` | **1 passed in 1.16s** | 5 |

The separate CPU-yield semantic gate also passed (**1 in 1.07s**) with public
finite close on both Driver and Worker. Actor and PG synchronous control calls
retain their existing exact-replay semantics; only the 30-second outer runner
bounds the whole experiment. Timeout means failure/abort, not cancellation or
a clean reservation/owner ACK. No new cancellation protocol was introduced.

Twelve more original observability/membership/startup test files were reviewed:
eight whole files (66 pure cases) and four mixed files (24 pure / 9 retained
heavy cases), plus four AST classification checks. A stale bare-Node stop
fixture now declares its empty transfer registry; the production stop behavior
was not weakened. These 94 cases plus the 26 close cases account for the
120-case increment.

Allowlists now contain **84 multiprocess / 23 L1** exact IDs, not all-ID
acceptance. Full safety classification, remaining old smoke lifecycle fixes,
same-version complete gates and the ordinary-success GCS fidelity decision
remain open. No heavy test was run; K0/K1 is incomplete.

### Previous control-boundary checkpoint (2026-09-07)

The control-boundary increment passed **2067 tests, 12 deselected in 7.10s**
through the reviewed entry after the final manifest metadata update (the first
expanded run was 7.19s): 134 whole files and 102 exact selectors in 26 other
files. The selection remains explicit, not the complete default unit gate.
Syntax compilation passed for `conftest.py src tests scripts examples`.

`PublicationControlAdapter` now owns publisher admission, Node-loss decisions
and progress, owner-death publication cleanup, tickets and graph/recovery
composition. GCSLite supplies current local membership observers, current RPC
callbacks and owner-wide-fence-ready deaths; it no longer reads the adapter's
private lock or cleanup collections. No second registry, backend, thread or
wire protocol was added. The owner-fence → publication → membership lock order,
lock-free remote effects, exact replay and per-publication progress bounds
remain. This improves responsibility boundaries but does **not** remove the
mini-specific synchronous GCS path or resolve its Ray-fidelity tradeoff.

Four new pure boundary contracts passed in 0.22s. They substitute a public-only
adapter, verify fresh per-call callbacks, reenter progress during a real child
release, and commit owner death during the last child ACK. The stale driver
cannot mutate graph/resolve; its finally-released ticket lets owner cleanup
resume against the same child tombstones. These are synchronous interleavings,
not concurrent-thread acceptance. Existing control contracts also passed.

Safety review expanded over 24 original files. Seventeen whole pure files
contributed 205 cases; seven mixed files contributed 46 pure cases, while their
29 original real-thread/runtime cases were preserved as heavy pending bounded
review. Two new AST classification files contribute seven cases. The four
boundary contracts complete the 262-case increment over the previous 1805
selection. Pure Actor argument models do not imply public Actor ObjectRef
argument support; the historical `runtime_state` facade is not current runtime
wiring evidence. Actor's separate descriptor result path was not changed.

The baseline Task and two trace tests retain their original exact IDs and
semantic assertions. Each now uses one 1 MiB store, a shared ten-second work
deadline, three seconds for the real local finalizer, unconditional shutdown
and all three managed PID/five endpoint checks, including owner and trace.
On the updated control runtime, six scenarios passed individually:

| Exact case | Result |
|---|---|
| `test_task_path.py::test_one_node_one_worker_task_path` | **1 passed in 0.93s** |
| `test_cross_process_trace.py::test_one_task_emits_cross_process_golden_trace_and_cleans_up` | **1 passed in 0.94s** |
| `test_cross_process_trace.py::test_application_error_trace_is_terminal_without_system_retry` | **1 passed in 0.92s** |
| `test_foreign_late_output_replica_cleanup_path.py::test_foreign_late_replica_is_collected_and_old_messages_preserve_reconstructed_epoch` | **1 passed in 1.53s** |
| `test_precomplete_output_owner_death_path.py::test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds` | **1 passed in 1.49s** |
| `test_contained_cycle_control_path.py::test_registered_unified_graph_rejects_cycle_then_real_owner_gc_releases_container` | **1 passed in 0.92s** |

Allowlists remain **77 multiprocess / 23 L1**, not all-ID same-version
acceptance. The unchanged F4 and Worker-owner tests were not rerun after this
control refactor; their prior results retain their original version scope.
Full test safety classification, other teaching-example lifecycle bounds,
same-version exit gates and the GCS fidelity contract remain open. No heavy
test was run. K0/K1 is incomplete.

### Previous foreign-custody/foundations checkpoint (2026-09-06)

The foreign-custody/foundations selection passed **1805 tests, 12 deselected
in 6.10s** through the frozen entry after the final manifest/runner changes
(the first expanded run was 6.15s): 114 whole files and 63 exact selectors
in 19 other files. This adds reviewed Actor/PG/Worker-pool/pull/hold protocols,
typed borrowing and multi-return owner contracts; eight unchanged pure files
also passed separately (93 cases, 0.25s).

The targeted-protocol file's three historical descriptor-only success fixtures
were migrated, not excluded to obtain a green selection. They now prepare and
complete a real in-memory unified publication for noncontiguous slots 0/2,
retain the healthy slot 1, and query the actual envelope after fake Worker exit.
Mixed and INLINE success recover retained bytes instead of falsely classifying
them as orphan-only results. A new negative contract rejects a successful
outcome with no prepared publication. All 10 cases passed in 0.18s; existing
SYSTEM_ERROR partial-seal orphan assertions remain.

Trace-export safety classification now separates four original real-file
contracts (retained as heavy, not executed) from two pure rejection functions
(four parameter-expanded cases). The rejection checks use no tmp_path or file
write and have scoped runtime/I/O tripwires; an AST contract preserves all six
functions and their explicit markers. This closes one classification gap,
not the complete default gate.

F6 passed independently through the bounded runner:
`tests/integration/test_foreign_late_output_replica_cleanup_path.py::test_foreign_late_replica_is_collected_and_old_messages_preserve_reconstructed_epoch`
— **1 passed in 1.55s**. A real Worker owner on A has its mixed producer execute
on B. A real Driver consumer grant seals/pins a secondary on A, but reports it
only after publisher loss and the owner's latched DROP. The owner returns
RETIRED/custody, the consumer cancels without Push, and the owner's existing
mailbox collects the old bytes. Physical absence is observed before the test
replays any Drop. A public get then reconstructs the stored slot on A; the old
foreign report and exact Drop receipt leave the new epoch, bytes, healthy
INLINE sibling and settled source-reference snapshot unchanged.

The test uses five children, two 1 MiB stores, 8 KiB padding, one Node crash
and one explicit reconstruction. The final source-lifetime predicate avoids
mistaking ordinary finish-tail releases after GCS adopted for replay damage.
No production runtime was changed for this acceptance; no owner metadata or
cleanup ACK was manufactured. The first permission review expired before
process creation; the one permitted approval retry launched the successful
run. This was not a failed runtime test.

Syntax compilation passed for `conftest.py src tests scripts examples`.
Allowlists are now **77 multiprocess / 23 L1**. F1–F7 each have their stated
bounded scenario evidence, not the entire fault matrix or all-ID same-version
acceptance. Full safety classification, the required complete gates and the
ordinary-success GCS fidelity contract remain unresolved. K0/K1 is incomplete.

### Worker-owner death-view checkpoint

The Worker-owner death-view increment passed **1697 tests, 12 deselected in
6.12s** through the frozen reviewed-pure entry after its manifest and scope
contract were updated: 105 whole files and 61 exact selectors in 18 other
files. The earlier explicit selection passed in 6.09s. Neither run is the
complete default unit gate; changed imports and fixtures still require review.

The Driver now retains actual survivor snapshot-install ACKs before publishing
a cumulative, metadata-only Node-death certificate. Each embedded Worker Core
reads it from its local Node through the existing coordinator, including before
lazy Core publication. This repairs the earlier gap where only the Driver Core
installed Node deaths. It introduces no owner takeover, extra failure detector,
new thread, or claim about remote hosts/network partitions. Partial death
application retains exact removal work for replay rather than losing routes
or waiters after an effect-then-error.

The real Worker-owner recovery case initially exposed a second runtime defect:
when the same Worker owned and executed the retry, provisional and final child
holds were identical. Discovery now gives only that case a distinct
`provisional:` token namespace. Both owned and borrowed sources use the existing
prepare/promote/release path; different-owner tokens and digests are unchanged.
The focused same-owner custody file contributes 12 pure cases. The first real
failure was not counted as a pass; the repaired case and the final-source rerun
passed.

Four exact scenarios were completely re-reviewed and rerun individually on
this runtime, with the 30-second process-tree runner and bounded cleanup grace:

| Exact case | Result | Boundary |
|---|---|---|
| `test_precomplete_output_owner_death_path.py::test_owner_death_after_intent_fences_unmaterialized_output_and_cleans_live_executor` | **1 passed in 1.53s** | F4: real outer-owner Worker death after INTENT; no materialization or ARM; same live executor acknowledges finalization |
| `test_precomplete_output_owner_death_path.py::test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds` | **1 passed in 1.44s** | F5: real sealed output and promoted holds, live Driver child owner, exact releases and replica cleanup before owner-cleaned |
| `test_contained_cycle_control_path.py::test_registered_unified_graph_rejects_cycle_then_real_owner_gc_releases_container` | **1 passed in 0.91s** | F7: registered metadata-only INTENT/PREPARE cycle rejection; a separate public task supplies genuine graph COMMIT and owner-GC RELEASE evidence |
| `test_worker_owner_node_loss_path.py::test_live_worker_owner_retries_armed_child_after_certified_remote_node_death` | **1 passed in 1.30s** | Surviving embedded Worker owner autonomously resolves UNKNOWN and retries on itself with stable logical IDs; Driver performs read-only queries until READY |

F4/F5 release the existing gate after a real child-release ACK proves owner-wide
Node fences are installed, not after owner-cleaned: the live Prepare ticket
must first be released so executor finalization can finish. The supervisor no
longer admits ordinary rollback while an owner-fenced publication awaits that
finalization. Both tests observe the replacement Worker's actual new endpoint
and verify its closure. F7's two cycle inputs do not fabricate public ObjectRefs,
child-prepare ACKs, or Complete witnesses. Its graph-only rollback records only
the real ABORT replies.

The runner listed **76 multiprocess / 23 L1** exact IDs, not all-ID
acceptance at this checkpoint. F1–F5 and F7 had their stated bounded slices;
F6 was then open and is now covered by the separate case above.
The new Worker-owner recovery case is a prerequisite, not foreign late-replica
custody/GC evidence. Full safety classification, same-version all-allowlist
verification, and the ordinary-success GCS fidelity design contract also remain
open. K0/K1 is not complete. Final compilation and selection evidence for
this continuation are recorded above.

### Previous mixed-UNKNOWN checkpoint (historical)

The mixed-UNKNOWN increment passed **1590 tests, 12 deselected in 5.97s**
through the fixed reviewed-pure entry: 101 whole files and 51 exact selectors
in 17 other files. This remains a reviewed subset, not a complete default gate.
Syntax compilation passed, including the root collection guard.

Two Core interleaving contracts now exercise both previously reported Complete
and ARM-only frozen knowledge. An actual envelope received before owner choice
can supply its Complete witness and preserve INLINE bytes without rewriting
the GCS frozen work. An envelope after latched DROP cannot reopen custody; the
UNKNOWN path consumes exactly one system retry and fences subsequent old
adoption. These pure tests add no runtime wait, thread or transport.

Four individually reviewed process scenarios passed:

| Exact case | Result | Boundary |
|---|---|---|
| `test_mixed_borrowed_output_unknown_path.py::test_armed_unknown_mixed_borrowed_outputs_release_each_old_slot_before_one_retry` | **1 passed in 1.51s** | F1: two mixed slots share one live child; both DROP without Complete, all old holds release before one retry, new siblings collect independently |
| `test_unreported_complete_node_loss_path.py::test_locally_completed_unreported_output_crash_is_unknown_then_cleans_before_retry` | **1 passed in 1.42s** | F3: real local Complete/resource release, terminal suppressed before send and both delivery exits held, then exact Node crash; GCS remains UNKNOWN, not known success |
| `test_targeted_borrowed_output_unknown_path.py::test_targeted_borrowed_arm_loss_retries_only_lost_slot_and_preserves_healthy_sibling` | **1 passed in 1.41s** | Final shared harness: one targeted STORED slot retries at attempt 2 while healthy INLINE sibling remains unchanged |
| `test_targeted_borrowed_output_unknown_path.py::test_targeted_mixed_borrowed_arm_loss_retries_selected_batch_and_preserves_healthy_sibling` | **1 passed in 1.74s** | F2: selected original slots 1/2 form mixed INLINE/STORED batch; one healthy slot 0 retains its exact owner snapshot/hold across loss, targeted retry and independent selected GC |

F1 and F3 use five children, two 1 MiB stores, a tiny live Driver-owned child
and one 8 KiB padded result. The F3 test alone owns the temporary partition
and metadata-only observation gate; production gate/protocol behavior is
unchanged, no ACK is fabricated and no observer witness enters Core custody.
It is explicitly a two-event partition-then-crash scenario, not a generic
single-fault claim. All real runs use the 30-second runner plus bounded cleanup.
F2 uses the same five-child/two-store bounds, at most two 8 KiB initial replicas,
public drops as reconstruction setup and one Node crash. max_retries=2 counts
one explicit reconstruction plus one SYSTEM retry. The mixed producer openly
changes slot 1's payload size after attempt zero to exercise per-attempt tier
selection; it is not a deterministic-output claim. Both exact IDs share one
workflow. No production runtime/protocol was changed in this increment.

Further safety classification preserves nine existing runtime cases as heavy
(Core blocking tests, Node blocking race and contained-graph thread race).
The six pure Node blocking tests now prepare real tiny output publication
before successful Complete; their resource/episode assertions remain intact.
Other newly reviewed files cover Worker blocking binding/protocol and PG's
pure transaction reducer. Unreviewed runtime tests were neither run nor deleted.

F1–F3 now have their specified bounded scenario evidence. F4–F7, same-version all-allowlist
verification and full default safety classification remain open. K0/K1 is
not complete; the ordinary-success GCS fidelity contract is unchanged.
The runner contains **72 multiprocess / 23 L1** exact IDs, not all-ID acceptance.

### Previous safe-entry/foundations checkpoint (historical)

The safe-entry/foundations increment passed **1539 tests, 12 deselected in
5.82s** through `scripts/run_reviewed_pure.py`: 98 reviewed whole files plus
36 exact selectors in 15 other files. This remains a subset, not the complete
default gate. The added foundation contracts cover identities/Hybrid, PG/Actor
state, retry/reconstruction, dependency/pull, trace and CPU-yield accounting.
Notifier retry tests now use a synchronous delay recorder, not real Event waits.
Syntax compilation, including the root collection guard, passed.
The requirement-to-evidence map and next finite failure batch are recorded in
[`acceptance-matrix.md`](acceptance-matrix.md), without replacing roadmap exits.

The root `conftest.py` rejects missing/directory/package selectors before
standard test-module collection; it does not silently substitute the manifest.
Two diagnostic invocations (bare pytest and directory `--collect-only`) both
returned usage error 4 before collection, so no complete test suite ran. Explicit
files still require safety review, and this is not a plugin/import sandbox.
Three previously mixed-marked files preserve all 30 original cases: 16 pure
cases and 14 quarantined heavy cases. Their original runtime tests were not
run or deleted; AST checks preserve that classification without claiming to
prove future fixture safety. Only the 16 exact pure IDs enter the manifest.

One new real process case passed independently:
`test_borrowed_output_unknown_path.py::test_armed_unknown_borrowed_output_releases_old_holds_before_retrying_same_live_child`
— **1 passed in 1.34s**. Five children, two 1 MiB stores, one tiny live
Driver-owned child and one 8 KiB stored output use the existing ARM-before-
Complete gate. After one managed publisher-Node crash, real resolution and
the live child's old contained-hold releases precede system retry. The logical
Task hold and lineage span retry unchanged; the ordered death consumer retires
the dead executor's borrower independently. Retry returns the same live child
with new per-attempt output holds; final GC and PID/port cleanup converge. This
is a single borrowed-child STORED case, not mixed/targeted UNKNOWN acceptance.

The existing CPU-yield process case was tightened and rerun independently:
`test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker`
— **1 passed in 1.07s**. It keeps the same actual single-CPU/two-Worker proof,
now with a 1 MiB store, one shared ten-second API/gate deadline, bounded parent
and child reference finalizers, partial-setup cleanup and all four PIDs/six
addresses checked. No retries or injected failures are used in this case.

Allowlists currently contain **68 multiprocess / 23 L1** exact IDs. The complete
default classification, full same-version exit gate and remaining explicit
failure combinations remain open; K0/K1 is not complete. Ordinary-success GCS
coordination is still the existing mini-specific contract, not Ray-equivalent.

The live-metadata quarantine concern was rechecked: the illustrative stale-
producer test directly changes a no-lineage/no-publication fixture's epoch.
It is not a demonstrated public-path recovery defect. Supported Task retirement
retains its publication history; put stays at attempt zero and collection keeps
compact history. Quarantine remains fail-closed for unexplained conflicting
metadata, with no claim of arbitrary corruption repair. A real missing-history
path, if found, must be fixed at its owner transition rather than accepting an
incoming descriptor as deletion authority.

### Previous cross-cleanup checkpoint (historical)

The cross-cleanup increment passed **1436 tests, 12 deselected in 5.74s**:
90 reviewed whole files and 20 exact selectors in 13 other files. This is an
explicit reviewed subset, not the complete default unit gate. No heavy, scale
or GPU test ran. Syntax compilation passed. The final selected run used the
new reviewed-pure entry and included its own reviewed contract tests.

Ordinary replica GC, publication rollback and owner-wide sealed cleanup now
share one physical completion tail and the existing exact receipt set. They
still validate their own authority first. A real old completion can answer
both generic and publication replays after newer seal/deletion without reading
or mutating the new replica. A higher epoch alone is not completion proof.

The deletion watermark and completed receipt are separate facts. Sealed cleanup
fences before physical deletion, retains metadata across delete/manager errors,
and only records completion after absence, manager cleanup and claim retirement.
Uncommitted or never-created publication writes retain the existing typed claim
while cleanup is unknown; they cannot be overwritten by a new Seal or pull.
Old Seal fast paths and new source readers also respect the deletion fence.
Owner-death Finalize contributes to the same receipt set for exact partial
STORED custody, waits for manager cleanup, and does not fabricate INLINE replica
history or a publication rollback ACK. A lost Worker-finalize ACK replays no
completed physical work.

`scripts/run_reviewed_pure.py --list` now exposes the fixed scope and 12 exact
non-unit exclusions without importing tests. Its default mode starts one
isolated pytest child with plugin autoload disabled, the unit filter and a
30-second execution deadline; process-snapshot cleanup calls are also bounded.
The JSON manifest is review scope, not a permanent safety certificate. Changes
to tests, imports, fixtures or runtime still require review; it is not a glob
over tests/unit. The new runner initially verified the historical 1322 subset
(12 deselected, 6.04s) before explicit enrollment of this increment.

Three process cases passed separately, using the final runtime source:

| Exact case | Result | Scope |
|---|---|---|
| `test_cross_cleanup_receipt_path.py::test_publication_rollback_receipt_replays_after_same_object_retry_seals` | **1 passed in 0.95s** | Three children, one 1 MiB store, one 8 KiB result; real post-seal failure, rollback/GCS ACK, one system retry, then first old generic Drop without priming; newer bytes preserved and full GC/PID/port cleanup |
| `test_pregrant_custody_path.py::test_second_source_loss_hands_off_first_replica_without_a_grant` | **1 passed in 1.15s** | Existing two-node partial localization/source-loss/custody path with the earlier source deletion fence |
| `test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | **1 passed in 1.44s** | Existing exact owner-Worker exit, shared sealed sweep and Node/Worker finalization; executor remains alive |

Allowlists now contain **67 multiprocess / 23 L1** exact IDs, not an all-ID
acceptance claim. Each run has a 30-second execution deadline followed by
bounded cleanup grace. Partial-write finalization and both directions of
cross-authority replays have pure evidence; the new process case covers only
publication-to-generic completion after a normal system retry.

Still open: wider multi-owner/GC/owner-death/node-death/unknown-publication
combinations and persistent conflicting-metadata repair; full safe default
fixture classification and requirement-by-requirement K0/K1 acceptance; the
ordinary-success GCS fidelity decision. The local completion proof does not
certify all cleanup interleavings or give a new owner to dead-owner objects.
K0/K1 remains incomplete.

### Previous abandoned-submitter checkpoint (historical)

The abandoned-submitter custody increment passed **1322 tests, 12 deselected
in 5.47s**: 86 reviewed whole files and 20 exact selectors in 13 other files.
Syntax compilation passed. This supersedes the overlapping 1269 selection,
not a complete default gate. No heavy, scale or GPU test ran.

Every real stored-dependency Request now freezes the original owner endpoint
and real SUBMITTED/RETAINED hold. After independently confirmed submitter
Worker death, the Node fences new work and hands its witnessed replicas back
to their unchanged owners. Pre-grant state is frozen under the request lock;
GRANTED may be abandoned, while RUNNING keeps pins/resources until its normal
execution terminal. No dead-submitter normal ACK or new borrower is invented.

The owner's ordered death consumer verifies the offered proof before the
existing custody helper runs with execution permission disabled. Current
replicas receive CUSTODY_ONLY; retired replicas use the existing physical
cleanup queue. Generic stored collection now retains only six byte-free
identity fields so a put collected before a late report can authorize exact
cleanup without reviving metadata. Task outputs retain their manifest-based
history. A dead input owner uses its own exact owner-wide death fence, never
the unrelated submitter's death.

Node progression handles at most one lease/one replica each round, rotating
past failures. Failure of the first owner's report and GCS lookup does not
starve later owners. Normal delayed ACKs may race the autonomous path for the
same inventory, but active tickets still fence drain. Owner handler admission
is retained across its death-sync RPC so final shutdown cannot close it early.

Three process cases passed independently through the bounded runner:

| Case | Result | Scope |
|---|---|---|
| Submitter exits after real child Grant | **1 passed in 1.35s** | Five startup children + one replacement; 2×1 MiB, one 8 KiB put, two logical tasks and one nonexecuting probe; original owner survives, child never Pushes, custody/GC/PID/port cleanup |
| Existing pre-grant second-source loss | **1 passed in 1.16s** | Frozen owner routes remain compatible with partial inventory handoff |
| Existing requester-Node pin cleanup | **1 passed in 1.27s** | Same supervisor still closes only the dead peer's read pin |

The new submitter case was rerun after the final one-replica driver/terminal
checks. The requester-Node case also ran at the final source; the pre-grant
result preceded the last narrow changes. Allowlists are **66 multiprocess /
23 L1**, not all-ID acceptance. Every process run has a 30-second execution
deadline followed by bounded timeout cleanup grace.

Still open: wider multi-owner/GC/owner-death/node-death/unknown-publication
combinations, generic cross-cleanup absence proof and pending conflicting
metadata repair; safe default fixture classification; a requirement-by-
requirement K0/K1 gate; and the ordinary-success GCS fidelity decision. The
live-owner GRANTED submitter-loss process slice does not certify the pure-only
RUNNING, pre-grant or collected-put combinations. K0/K1 remains incomplete.

### Previous source-transfer checkpoint (historical)

The source-transfer-pin increment passed **1269 tests, 12 deselected in
5.40s**: 80 reviewed complete files and 20 exact selectors in 13 other files.
Syntax compilation passed. This supersedes the overlapping 1226 selection,
not the unsafe complete default unit gate. No heavy, scale or GPU test ran.

Before Pin can send, the target retains its exact source endpoint and transfer
identity in a small outbox. Active readers are not eligible for background
Release. Finally closes and claims one ticket atomically, retaining it across
three short attempts; failure hands the same obligation to the existing Node
supervisor/drain, not a new thread. Release uses 0.25/0.5-second connect/request
limits plus an absolute 0.75-second deadline. Source close fences are installed
before physical unpin, with distinct CLOSING/CLOSED state. Even Release-before-
Pin leaves a permanent transfer-ID fence; no late Pin can reopen it.

Exact source-Node death can discharge only its target outbox. Requester-Node
death instead makes the living source close only that requester's sessions.
Malformed/unknown/ALIVE/Worker-death replies and timeouts do not prove Node
death. Active and in-flight tickets survive the death observation until their
proper exit boundary. Local Pin/unpin effect-then-error replays the saved token,
never another session's pin. Pin/Release wire fields are unchanged and now
deeply validated on construction and pickle reconstruction.

Four process cases passed independently through the bounded runner:

| Case | Result | Scope |
|---|---|---|
| Real Pin effect, lost ACK | **1 passed in 1.18s** | Five children, one input and nonexecuting consumer; exact source close, empty custody inventory, GC |
| First three real Release ACKs lost | **1 passed in 1.21s** | Same bounds; Node outbox delivers the fourth exact ACK, retained target replica is handed off and collected |
| Requester Node dies with a live same-object peer | **1 passed in 1.30s** | Five startup children, no user task; automatic source cleanup precedes test Release; surviving session stays readable |
| Existing pre-grant source-loss path | **1 passed in 1.14s** | Real missing-source Pin now also receives its exact close fence |

The two ACK-loss tests use a test-module spawn entry wrapper that calls the
original process entry, including its process-group isolation. It only drops
validated real transport replies on one marked Node. Child exit assertions
prove exact Pin/Chunk/Release counts; the Driver requires successful managed
exit and full GC/PID/port cleanup. There is no new production failpoint, test
thread or listener. Each exact ID ran separately; allowlists are now **65
multiprocess / 23 L1**, not all-ID acceptance.

Still unfinished: autonomous replica custody after a submitting Worker dies
(source-pin cleanup does not replace owner handoff); persistent/quarantined
metadata repair, broader source/target/owner/GC interleavings and generic
cross-cleanup absence proofs; safe default fixture classification; and the
ordinary-success GCS fidelity decision. K0/K1 remains incomplete.

### Previous pre-grant checkpoint (historical)

The pre-grant inventory increment passed **1226 tests, 12 deselected in
5.32s**: 75 reviewed complete files and 20 exact selectors in 13 other files.
Syntax compilation passed. This is one overlapping selection, not a complete
default unit gate; no heavy, scale or GPU test ran.

Node now binds the full Request before localization and records an ordered
subset of witnessed replicas independently of any Worker grant. Candidates
are retained before effects; sealing and LOCAL_READY reuse both enter this
inventory before source-release finally can fail. Temporary metadata/snapshot
failures keep pending evidence; exact Cancel rechecks physical bytes and the
saved seal witness under the normal object/state locks. No failed request
deletes a replica merely because it pulled it.

Terminal rejection and capacity-budget exhaustion with stored dependencies
now freeze the partial inventory through Cancel and reuse the same owner
handoff with `grant=None`. Core holds the original inputs/error until every
replica is accounted for, then sends an exact custody ACK. Node cannot report
clean until that ACK arrives. Ordinary stored-dependency grants use the same
ACK; tasks without stored dependencies add no such RPC. ACK loss replays only
the retained metadata, not owner effects or user code.

The final handoff-to-Push check is now atomic with cancellation selection.
An already selected failure or terminal result prevents a stale success lane
from sending; a cancellation after `push_send` admission still relies on Node
Start arbitration. Core rechecks local holds after the added custody-ACK RPC.
A delayed capacity request also bypasses the pre-lease PG-loss shortcut so
the custody-aware lane, not a generic terminal branch, owns its reconciliation.
The PG test is a pure dispatcher-routing check, not full PG/death acceptance.

Three process cases passed independently through the bounded runner:

| Case | Result | Scope |
|---|---|---|
| Second source disappears before first Grant | **1 passed in 1.14s** | Five children, two tasks, 2×1 MiB; real public source drop, partial inventory, no Grant/Push/retry, exact ACK and full GC/PID/port cleanup |
| All twelve committed Grant replies lost | **1 passed in 1.26s** | Existing five-child slice with the added custody ACK |
| Normal dependent task and healthy secondary | **1 passed in 1.29s** | Existing five-child contained-output survivor path; real successful Push/GC |

The new first case was rerun after the final entry/identity and reconciliation
checks. The other two preceded the final narrow changes and are not a same-
revision all-ID acceptance. Allowlists are **62 multiprocess / 23 L1**. The
30-second execution deadline is followed by bounded timeout cleanup grace.

Open boundaries remain substantive: a dead submitter cannot drive the owner
handoff and Node does not yet retain autonomous owner-routing capabilities;
its inventory stays unclean, not silently discarded. Source Pin ACK ambiguity,
source-release persistent retry and target-death source-pin cleanup need their
own exact obligations. A test where Release truly happened before ACK loss
does not prove those cases. Wider physical/death/GC races, generic absence
proof, default test safety and the GCS fidelity decision remain unfinished.
K0/K1 is not complete.

### Previous committed-grant checkpoint (historical)

The committed-grant custody increment passed **1155 tests, 12 deselected in
5.02s**: 67 reviewed complete files plus 20 exact selectors in 13 other files.
`compileall -q src tests scripts examples` passed. This supersedes the
overlapping 1099 selection; it is not the unsafe complete default unit gate.
No heavy, scale or GPU test ran.

When every reply to a real committed Grant is lost, Cancel now returns the
Node's detached, deeply validated `retired_grant`. It is historical dependency
custody, not execution permission. Core keeps the exact original request and
terminal error, hands all local/foreign replicas into the existing location
driver, and only then terminates the consumer. A valid saved Cancel reply is
reused after a pure builder failure; it does not cause user-code reexecution.
Duplicate Cancel replies retain inventory even when `released=False`.

Ordinary and retired grants share deep validation. Missing fields, corrupt
nested IDs, duplicate or incomplete dependency manifests trigger exact hop
replay, not a no-grant terminal cleanup. Cancellation selection and marker
updates are atomic with newer Location progress. Pure unlock interleavings
exercise actual local receipts and foreign ACK loss; an active handoff merges
later sticky cancellation errors instead of restoring old execution permission.
Target death after cancellation was selected preserves the original error.

A living Node may have already reclaimed the exited executor as WORKER_LOST,
so Cancel legitimately rejects. Core retains a separate exact, empty
GetWorkerLeaseOutcome proof: executor, owner, output manifest and all lease/
attempt/PG/target identities must match, with no pending output cleanup. No
`cancelled=True` is synthesized. The previously pure-only local-route/executor
collision now has an independent real-process test.

Four process cases passed independently through the bounded runner:

| Case | Result | Bound / evidence |
|---|---|---|
| All twelve real target Grant replies lost | **1 passed in 1.33s** | Five children, two tasks, 2×1 MiB; real source drop, Cancel inventory, both owners, no Push/retry and complete GC |
| Granted executor exits before route-failure Cancel | **1 passed in 1.34s** | Five startup children + one replacement; only verified target Worker exits; real rejected Cancel/outcome, both input owners survive |
| Original local route-failure baseline | **1 passed in 1.25s** | Five children, two tasks, no process kill; same local/foreign custody driver |
| First foreign owner dies, second retains custody | **1 passed in 1.35s** | Five startup children + one replacement; existing multi-owner regression |

Allowlists are **61 multiprocess / 23 L1**, not all-ID acceptance. Each process
test uses an execution deadline of 30 seconds, followed on timeout by bounded
process-tree termination/reaping; internal finalizer budgets do not include the
full cluster shutdown budget. Tests verify exact managed PIDs and endpoints.

Still open: `retired_grant=None` proves no committed Grant, **not** absence of
pre-grant partial-localization replicas. A pure Node test keeps such real bytes
visible explicitly; cleanup for that path is unfinished. A permanently
unusable inventory has no general reconciliation/absence proof. The final
handoff-to-Push cancellation interleaving, wider owner/Node/ACK matrix, generic
deletion authority, safe default test classification and GCS fidelity remain
separate work. K0/K1 is not complete.

### Previous local/foreign checkpoint (historical)

The local/foreign custody increment ultimately passed **1099 tests, 12
deselected in 4.62s**: 64 reviewed whole files and 19 selectors in 12 other
files. This added four pure executor-loss cases and three exact migrated cases
to the earlier 1092 checkpoint. No complete default unit gate ran.

`_build_location_reports` is now a pure inventory builder. `_LocationReportState`
retains the actual grant, complete foreign reports and lease identity **before**
any local owner/location/route mutation. Local and foreign consumers use one
owner custody transaction; the former checks its real SUBMITTED hold and the
latter its real RETAINED hold. StoredArg uses the same top-level dependency
hold, and SYSTEM retries may retain its original hold attempt. No fake foreign
credential, extra RPC or second handoff backend is introduced for local work.

Local receipts now join the existing post-grant record. Unexpected route/CAS
failures leave the receipt unknown, preserve the first error, cancel the grant
and still hand off foreign replicas. Exact replay repairs only missing local
progress. An owner CAS which took effect before raising is not mistaken for
no mutation, and a newly valid route is not rolled back. Clear epoch/canonical
conflicts remain typed rejections, not permission to delete newer data. Local
hold/protected-dependency identity is checked on replay; loss of a formerly
active hold downgrades permission to CUSTODY_ONLY rather than allowing Push.

Four process cases passed independently through the 30-second runner:

| Case | Result | Bound / evidence |
|---|---|---|
| Local route write fails before foreign handoff | **1 passed in 1.07s** | Five children, two tasks, 2×1 MiB, two 8 KiB puts; rerun after executor-loss proof change |
| Late sealed secondary after DROP | **1 passed in 1.14s** | Existing five-child Node-loss/cancel/drop/GC slice, gate migrated to pure builder |
| First foreign owner dies, second retains custody | **1 passed in 1.28s** | Five startup children + one replacement, three tasks, one nonexecuting probe |
| Healthy contained secondary KEEP | **1 passed in 1.12s** | Five children, two tasks, actual SUBMITTED hold and selected-output GC |

The new local test uses public source-replica deletion to make route repair
necessary, then a one-shot dictionary-write fault without discarding live
entries. It proves checkpoint-before-effect, exact cancel, foreign report
before local repair, preserved original error, no consumer execution, readable
surviving inputs and complete reference/physical/PID/port cleanup. Allowlists
are **59 multiprocess / 23 L1**, not all-ID acceptance. No heavy tests ran.

Remaining work includes full grant-inventory validation and malformed input
reconciliation, wider custody/death/ACK interleavings, generic deletion proof
across cleanup authorities, default fixture safety/migration and the GCS
fidelity decision. At this checkpoint the specific executor-loss collision was
pure-only; the other three process results above preceded that proof change.
Two spillback exact cases used new pure fixtures and the foreign route-write
exact case was reviewed and run; their entire mixed files were not run.
Subsequent grant work is recorded in the current section, not retroactively
counted as evidence for this checkpoint. K0/K1 remained unfinished.

### Previous multi-owner checkpoint (historical)

The multi-owner post-grant handoff increment passed **1083 tests, 12
deselected in 4.61s**: the reviewed 1044 selection below plus two pure files.
`compileall -q src tests scripts examples` passed. No full default unit gate,
heavy, scale or GPU test ran.

A report now distinguishes **execution permission** from **replica custody**.
An exact current replica reported with an inactive task hold becomes
CUSTODY_ONLY: the owner tracks its location and schedules normal GC, but the
consumer may not Push. RETIRED still transfers exact deletion responsibility;
neither result is confused with ADDED/ALREADY_RECORDED execution permission.
Conflicting or unknown metadata grants no deletion authority.

One retained `_LocationReportState` now carries the complete foreign reports,
their typed receipts, the first terminal error, installed owner-death records
and the exact cancellation ACK. The first definitive failure cancels promptly
to release target pins, while subsequent owners still receive their own
sealed-replica handoffs. A missing ACK does not skip later owners, and replay
does not repeat accepted receipts or forget cancellation. The same state is
in the Core marker and queued work; a nonblocking ticket plus canonical marker
selection prevents old/reentrant lanes from rolling progress backward.

Only after cancellation and every valid custody handoff converge does the
consumer expose its original error and finish its holds. An installed owner
death delegates its replica cleanup to the existing GCS owner-wide fence; it
is not Node death or ownership takeover. Target death cannot turn an already
rejected consumer into a new execution. A definitive rejection without a
custody proof is quarantined with exact evidence, no repeated rejection RPC,
and an unclean finalization fence; a later installed owner/target death can
wake it. Final parking/death rechecks share the Core lock, avoiding a lost wake.

Five process slices passed separately through the 30-second runner:

| Case | Result | Bound / evidence |
|---|---|---|
| First foreign owner Worker dies; second owner still receives replica | **1 passed in 1.56s** | Five startup children + one replacement, 2×1 MiB, three tasks, two 8 KiB puts, one nonexecuting probe lease |
| Late sealed secondary after DROP | **1 passed in 1.12s** | Five children, one publisher loss, exact cancel/drop/GC |
| Healthy contained secondary KEEP | **1 passed in 1.12s** | Five children, two tasks, one publisher loss, no producer replay |
| Ordinary foreign stored dependency | **1 passed in 1.06s** | Five children, three tasks, 64 KiB, real owner ACK before Push |
| Mixed Node loss / selected reconstruction | **1 passed in 1.10s** | Five children, one loss and one reconstruction |

The new real test kills only the GCS-verified owner Worker, keeps both Nodes
and the second owner alive, observes cancel before the healthy report, proves
no consumer Push/retry, and verifies GCS cleanup of the dead owner's copies.
It also checks healthy-object/consumer-lineage GC and exact replacement
identity, PID/port cleanup. Allowlists are **58 multiprocess / 23 L1**, not
all-ID acceptance.

Remaining boundaries include local pre-record exceptions before the complete
handoff record is constructed, malformed/quarantined input reconciliation,
the broader owner/Node/ACK matrix, generic deletion absence proof across other
cleanup authorities, safe default-test classification and the GCS fidelity
decision. Pure tests do not certify arbitrary network failures or the full
foreign-lineage lifecycle; K0/K1 remains unfinished.

### Previous late-replica checkpoint (historical)

The late-replica cleanup increment passed **1044 tests, 12 deselected in
4.55s**: the reviewed 971-test selection below plus five reviewed pure files
and five exact selectors (eight expanded cases) from the mixed drop file.
Syntax compilation passed. This is not the unsafe full default unit gate.

Late local grants and foreign location reports now pass an owner-side
retirement check against the exact retained publication manifest. A latched
DROP, active slot collection/retirement, or completed retirement/GC admits
**cleanup only**, including after the original task hold was released.
`ReplicaCleanupQueue` retains the immutable Drop request before the foreign
RETIRED reply or local consumer cancellation. It shares the existing reference
event consumer; it does not own publication or logical GC. PINNED, ambiguous
or malformed ACKs remain pending. Only an exact completed Node deletion receipt
or installed Node death releases custody, and in-flight RPCs remain a fence
even after a concurrent death proof arrives.

Every local dependency and every foreign RETIRED report in a grant is processed
before the consumer is cancelled; later healthy dependencies keep their normal
location handoff. Known cleanup blocks relevant explicit reconstruction START,
system retry, final retirement, metadata GC and clean finalization, without
blocking the consumer's cancellation/unpin. Node dependency pull admission,
final seal and lease pin now consume the same deletion watermark, preventing
old bytes from being pulled back after cleanup. Generic successful deletion
receipts survive newer replicas; exact old replay returns ALREADY_DROPPED without
touching them. A never-completed old request is still not an ACK.

Preserved-owner drain now keeps its reference consumer alive until the final
owner admission fence. A timed-out post-fence join is retried without reopening
the owner or losing Node-death cutover. Forced cluster exit stops local
transport separately and never turns pending cleanup into successful GC.

The following process cases passed one at a time through the 30-second runner:

| Case | Result | Scope |
|---|---|---|
| Late sealed secondary after latched DROP | **1 passed in 1.10s** | Five children, two tasks, 1 MiB/Node, one publisher exit; real grant, cancel, exact drop and GC |
| Adopted mixed-output healthy secondary | **1 passed in 1.10s** | Existing two-slot KEEP/contained import/GC slice |
| Mixed-output Node loss and selected reconstruction | **1 passed in 1.10s** | Existing KEEP/LOST slice, one reconstruction |
| Mixed contained selected reconstruction | **1 passed in 0.83s** | Three children, 1 MiB, one reconstruction |
| Actor creation/direct calls | **1 passed in 1.25s** | Six children, one Actor, three tiny calls |
| Adopted owner death | **1 passed in 1.39s** | Five startup children plus one replacement, two tasks |

Allowlists are **57 multiprocess / 23 L1**, not all-ID acceptance. The final
small forced-finalize recheck was exercised by the last owner-death run, not
a fresh all-ID rerun. No heavy, scale or GPU tests ran.

Still open: non-RETIRED foreign report rejection/owner-death in a multi-owner
grant can abandon later sealed dependencies before their custody handoff;
historical deletion by another cleanup authority followed by a newer epoch
has no universal typed absence proof; full late-report/GC/death interleavings
and explicit debug-drop ACK-loss liveness remain incomplete. The new real
late-DROP acceptance is local-owner; foreign report/cancel is currently pure
composition evidence. GCS fidelity and the safe complete default gate remain
separate K0/K1 work, not resolved by this cleanup queue.

### Previous surviving-output checkpoint (historical)

The surviving-output/argument-materialization increment passed **971 tests,
12 deselected in 4.26s**: the reviewed 887-test selection below plus five
reviewed pure files. This is one overlapping selection, not an additive total
or the unsafe complete default unit gate.
`compileall -q src tests scripts examples` also passed; no heavy test ran.

Publishing-Node loss now preserves an already-adopted STORED slot when its
owner has a current, grant-backed secondary location. The owner checks the
exact manifest receipt, membership, canonical result and every location epoch
before Core chooses KEEP. Final application filters the **current** locations
against the publishing Node and installed death fences; it never reinstalls
a saved replica set. If the secondary disappears after KEEP, the slot remains
LOST with its canonical identity/contained membership for later retirement.
Decision/cleanup ACK loss and owner-CAS effect-then-error replay have pure
contracts. Descriptors without adopted custody still do not authorize KEEP.

The new real secondary test exposed a separate missing path: a materialized
Task dependency could contain exported ObjectRefs but Worker deserialization
had no scoped importer. RefArg bytes and ready-INLINE dependency bytes now
use the same attempt-wide import session as explicit nested arguments, while
keeping their original contained-hold and Task-hold credentials distinct.
Imports are acquired before exposure, rolled back after later decode failure,
and held through output promotion/replay. Plain arguments do not introduce
an extra embedded-Core requirement. Local session closure hands off existing
Core release obligations; it does not prove all remote Release ACKs arrived.

Four exact process cases passed independently through the 30-second runner:

| Case | Result | Bound |
|---|---|---|
| Adopted mixed outputs, healthy STORED secondary | **1 passed in 1.09s** | Five children, two tasks, 1 MiB/Node, one publisher exit, no retry |
| Received mixed result, missing STORED slot | **1 passed in 1.10s** | Five children, one selected reconstruction, 1 MiB/Node |
| Worker-only loss with dead child | **1 passed in 1.00s** | Three startup children plus one replacement, one retry, 1 MiB |
| Mixed contained selected reconstruction | **1 passed in 0.82s** | Three children, one reconstruction, 1 MiB |

The new secondary case holds the first **real Adopted ACK**, lets the second
dispatch lane consume the contained STORED dependency, and then kills the
exact publisher. It checks both KEEP slots, unchanged attempt, the real
surviving bytes, independent child holds, per-slot graph/physical GC and
process/port cleanup. Allowlists are now **56 multiprocess / 23 L1**, not
all-ID acceptance.

Still open: a grant/location report arriving **after DROP is latched** can
introduce an old-attempt physical secondary which the final DROP forgets.
Simply rejecting that report is insufficient: cancelling its consumer lease
unpins but does not delete the replica. This needs exact late-replica cleanup
custody and race acceptance, not a second publication backend or a relaxed
KEEP rule. Explicit debug-drop effect/ACK-loss liveness is also not proven by
the healthy-secondary tests. The GCS fidelity choice, safe default gate and
remaining K0/K1 failure matrix remain unfinished.

### Previous protocol-retirement checkpoint (historical)

The legacy protocol-family retirement increment passed **887 tests,
12 deselected in 3.98s**: 50 reviewed files plus eleven exact cases in nine
other files. It supersedes the overlapping 767-test checkpoint, not an
additional total. Syntax compilation passed; the unsafe complete default unit
gate was not run.

Fourteen old lifecycle source modules are now non-executable history resources
(the old `stored_publication.py` body included). That module now only re-exports
shared source/Node identity classes for historical pickle names. The old
OPEN/claim/recovery/finalize wire types, InlineInstall facade, old Worker
encoders and StoredReferenceExportSession are removed. TaskReply no longer
accepts raw contained edges or tier-specific publication envelopes; Core's
separate raw-edge disposal/retry authority is removed too. Physical orphan
replica cleanup, generic export-pin cleanup and unified output GC remain.

Fourteen obsolete active test files were archived with hashes; a fifteenth
(`test_inline_recovery.py`) retains its name but now uses unified recovery,
including its two separately bounded L1 races. Shared graph/source and direct
Worker tests were migrated rather than removed. Generic ReferenceExportSession
still has standalone lifecycle contracts, not a normal Worker-path claim.
Positional wire rebuild rejects a mismatched field count instead of silently
shifting old fields into new authority. Shared-source pickle compatibility is
preserved; mixed runtime revisions are not supported.

Five exact process cases passed separately after the final source changes:

| Case | Result | Bound |
|---|---|---|
| Mixed contained selected reconstruction | **1 passed in 0.79s** | Three children, one reconstruction, 1 MiB |
| Worker-only loss with dead child | **1 passed in 0.99s** | Three startup children plus one replacement, one retry, 1 MiB |
| Actor creation/direct calls | **1 passed in 1.22s** | Six children, one Actor, three tiny calls |
| ARM-UNKNOWN publishing-Node loss | **1 passed in 1.14s** | Five children, one retry, 1 MiB/Node |
| Adopted owner death | **1 passed in 1.30s** | Five startup children plus one replacement, two tasks |

The migrated two-thread intent/owner-death and KEEP/DROP races also passed
independently (**0.12s / 0.11s**) through the bounded runner. Allowlists remain
**55 multiprocess / 23 L1** IDs, not all-ID acceptance. No heavy test ran.
Historical mapping gaps, default fixture safety/migration and the synchronous
GCS publication design choice remain open; K0/K1 is not complete.

### Previous owner-publication checkpoint (historical)

The owner-publication retirement increment passed **767 tests, 10 deselected
in 3.78s**: 44 reviewed files plus five exact cases in three other files. It
supersedes the overlapping 653-test checkpoint; counts are not additive.
`compileall -q src tests scripts examples` passed. No complete default gate,
GPU, heavy or scale test was run.

`ObjectOwnerTable` no longer retains the old INLINE/STORED publication plans,
receipts, retirement records, snapshot fields or GC wrappers. Only the unified
output membership/lifecycle remains. Shared plain publication, borrower/
contained/retained/lineage behavior is preserved; retirement guards and the
collection digest were renamed without changing their protected invariants
or digest framing. Standalone legacy modules and reply/protocol fields still
exist and require coordinated retirement, not compatibility properties on
owner snapshots. Four old owner test sources are archived byte-for-byte with
coverage mappings in `history/retired-owner-publication/README.md`.

Additional owner contracts now cover pristine no-membership PENDING
resolution, rejecting every bad later slot/lineage before any batch mutation,
cross-entry/cross-tier retired-attempt fences, newer-epoch GC integrity, and
the full owner's post-GC reachable state containing no result/argument/function
bytes or TaskSpec. Caller-held collection plans still replay exactly, and
altered payload/function/collection identities are rejected.

Five reviewed process cases ran individually through the 30-second runner:

| Case | Result | Bound |
|---|---|---|
| Mixed contained selected reconstruction | **1 passed in 0.85s** | Three children, one reconstruction, 1 MiB |
| Unreceived INLINE Node loss / DROP | **1 passed in 1.21s** | Five children, one reconstruction, 1 MiB/Node |
| Received INLINE Node loss / KEEP | **1 passed in 1.09s** | Five children, no original-result recomputation, 1 MiB/Node |
| Adopted owner death | **1 passed in 1.45s** | Five startup children plus one replacement, two tasks |
| Stored outer adoption/GC | **1 passed in 0.98s** | Four children, two tasks, 64 KiB padding, 1 MiB |

The allowlists remain **55 multiprocess / 23 L1** IDs, not all-ID acceptance.
The ordinary-success synchronous GCS dependency and broader recovery/safe-test
acceptance remain open; this refactor does not establish full K0/K1 completion.

### Previous GCS/shared-source checkpoint (historical)

The GCS retirement/shared-source/dead-child increment passed **653 tests,
10 deselected in 3.52s**: 38 reviewed files plus five exact cases in three
other files. The ten exclusions are three socketpair gate checks and seven
explicit thread tests. This supersedes overlapping prior selections, not an
additional total. `compileall -q src tests scripts examples` also passed.
The complete default unit gate remains unsafe and was not run.

GCS now constructs `PublicationControlAdapter` only: one contained graph, one
output recovery registry and their shared composition lock. The old tier
registries, graph aliases, Node-loss and owner-death saga assemblies/RPCs were
removed; membership, Actor and PG state remain unchanged. Owner-scoped
progress, global drain and the background driver operate on unified output
cleanup plus owner-wide fences. Typed Get/Decide/Progress NodeLoss dispatch is
also registered. Source capabilities and Node incarnation have one neutral
definition in `publication_sources.py`, preserving fingerprint bytes and
historical pickle globals through same-class exports. Owner legacy publication
state and standalone old model/protocol families still remain.

Node compensation can use an exact GCS child-owner death proof after Release
fails. Both Node and adapter validate the registered incarnation, watermark
and PROCESS_EXIT/NODE_EXIT fact, never manufacturing a Release ACK; other
live child holds remain obligations. The pure matrix and the following real
Worker-only-loss slice cover this binding, not every possible death race.

Each process case below ran independently through the 30-second runner:

| Case | Result | Bound / evidence |
|---|---|---|
| Worker-only loss with owned child | **1 passed in 1.26s** | One live Node; three startup children plus one replacement, two task attempts, 1 MiB; observed cleanup_pending, exact rollback ACK before retry, unchanged Node PID/epoch |
| ARM-UNKNOWN publishing-Node loss | **1 passed in 1.28s** | Five children, one survivor retry, 1 MiB/Node |
| Adopted owner death | **1 passed in 1.46s** | Five startup children plus one replacement; still-live executor cleanup |
| Mixed contained selected reconstruction | **1 passed in 0.91s** | Three children, one reconstruction, 1 MiB |
| Migrated stored outer adoption/GC | **1 passed in 1.05s** | Four children, two tasks, 64 KiB padding, 1 MiB; unified metadata-only outcome after retirement |

Three GCS L1 checks also passed separately: unified background failure in the
fence/publication domain (**0.31s / 0.27s**) and retry of a nonterminal owner
sweep (**0.41s**). The allowlists now contain **55 multiprocess / 23 L1** IDs;
these counts are not all-ID acceptance. No GPU, heavy, stress or scale run was
performed. Historical sources and outstanding mappings are in
`history/retired-gcs-publication/README.md`.

### Previous Core/Node checkpoint (historical)

The earlier Core/Node retirement increment passed **558 tests, 7 deselected in
3.21s**: 30 explicitly reviewed files and five exact cases in three other
files. The seven excluded cases are three socketpair gate checks and four
opt-in Node thread probes. This replaces, rather than adds to, the overlapping
378- and 462-test checkpoints. It is not the complete default unit gate.
`compileall -q src tests scripts examples` also passed.

This increment removes the legacy INLINE/STORED publication dispatch from Core
and the two journal/runtime/handler/supervisor assemblies from Node. Ordinary
successful Complete/outcome requires the unified prepared publication. Generic
store/pull/pin, early task failure, put and Actor paths remain. GCS/owner legacy
assembly and shared types have **not** been removed. Node drain now derives
publication cleanliness from the existing journal/adapter and refuses to
finalize while payload retirement, exact rollback/report ACKs or owner cleanup
remain. Nonblocking lock observation avoids reversing publication lock order.

Reconstruction now preflights the full graph and foreign renewals before
retiring old memberships, revalidates all renewals before any producer START,
and retires only explicitly selected slots. Targeted admission defers unknown
cleanup/renewal ACKs without claiming an authority failure; a fresh credential
can JOIN the selected PENDING attempt, while an unrelated queued-next loss
cannot. A terminal OPEN
failure is latched, cleans the old slot before ERROR, and remains authoritative
if other siblings later become LOST. Whole-DAG admission cannot bypass an
active targeted producer. Pure tests cover these boundaries and final GC;
they do not establish the complete reconstruction failure matrix.

The following exact process tests ran separately through the 30-second runner
after the Core/Node source changes:

| Case | Result | Resource/semantic bound |
|---|---|---|
| Recursive lineage | **1 passed in 1.10s** | Four children, three producers/reconstructions, one 1 MiB store |
| Mixed contained selected reconstruction | **1 passed in 0.92s** | Three children, two slots, one reconstruction, one 1 MiB store |
| Adopted owner death | **1 passed in 1.52s** | Five startup children plus one replacement; live executor custody cleanup |
| Actor creation/direct calls | **1 passed in 1.44s** | Six children, one Actor, three tiny method calls |

Four migrated Node thread probes also passed independently in
**0.16s / 0.15s / 0.16s / 0.16s**. The runner now lists **54 multiprocess** and
**22 L1** exact IDs, not a claim that every ID was rerun. No default suite,
GPU, pressure, performance or scale test was executed.

### Prior unified-gate checkpoint (historical)

The earlier reviewed selection passed **378 tests, 3 deselected in 1.42s**:
16 reviewed pure files plus four exact pure cases in three other files. The
three deselections are the explicit socketpair L1 cases, each run separately.
Coverage includes unified publication/owner cleanup, full/targeted pre-Push
Node loss and cancellation, one output fault gate, and partial-store failure
with PINNED / lost Drop ACK / lost GCS rollback ACK before retry. This is not a
complete unit gate; it overlaps and supersedes the prior 176-test selection,
not an additional count. Commands are in `testing.md`.
`compileall src tests scripts examples` also passed.
The final three added cases cover a metadata-only successful Node outcome:
Core resumes adoption from retained bytes or already-ready owner slots, never
treating a valid `output_completion` witness as an ordinary failed-task retry.
If no local payload is available, it retains exact replay rather than making
bytes from control metadata; wider fault/liveness acceptance remains open.

Current private fault gates are unified in `publication_gate.py`; the former
INLINE gate, Node-local STORED gate implementation and two startup options
were replaced, not layered beneath a third gate. All four phases carry the
whole/selected execution identity and manifest digest. Complete and outcome
share the delivery gate. No configured gate means no socket/wait overhead.

This round ran the following exact process tests independently through the
30-second runner:

| Window / case | Result | What was observed |
|---|---|---|
| INTENT ACK, before child effects | **1 passed in 1.37s** | No replica/graph; cleanup then one SYSTEM retry |
| All promotions ACKed, before ARM | **1 passed in 1.36s** | Real sealed bytes and child effects; pre-Complete cleanup before retry |
| ARM ACK, Complete not observed | **1 passed in 1.38s** | GCS `COMPLETION_UNKNOWN`; exact cleanup ACK before consuming retry budget |
| Complete, STORED result not delivered | **1 passed in 1.43s** | Task SUCCEEDED / object LOST first; explicit `get` alone reconstructs |
| Complete, INLINE not delivered | **1 passed in 1.40s** | Both Node exits gated; DROP, then explicit reconstruction with borrowed child |
| Complete, INLINE actually received | **1 passed in 1.26s** | KEEP retained bytes without rerunning the old task |
| Ordinary Node loss before publication | **1 passed in 1.34s** | Exact absent-intent recovery, stable logical identity and survivor retry |
| Mixed contained / selected reconstruction | **1 passed in 0.94s** | Current Worker path after legacy coordinator retirement |

These span adjacent revisions: routing/compensation edits were rechecked by
ARM-UNKNOWN and mixed-contained runs; the final successful-witness change by
the last STORED post-Complete run, not a fresh full allowlist run. Three gate socketpair L1 cases passed separately in
**0.16s / 0.16s / 0.17s** (full identity, noncontiguous target bitmap, truncated
frame). The runner now lists **54 multiprocess** and **21 L1** exact IDs.

The previous adjacent revision also has these three independently bounded
records; only mixed-contained was rerun above after the latest edits:

| Case | Result | Evidence boundary |
|---|---|---|
| Mixed contained outputs and selected reconstruction | **1 passed in 0.98s** | Three children, one 1 MiB store; common publication, distinct child holds, return-index-1 reconstruction, healthy sibling and reverse GC |
| Mixed result publisher Node loss | **1 passed in 1.32s** | Five children; actual received INLINE custody survives, STORED becomes LOST and reconstructs on the survivor |
| Adopted output owner Worker death | **1 passed in 1.65s** | Five startup children plus one replacement; exact owner death, source-hold cleanup, missing stored bytes, unchanged live executor, GCS owner-cleaned barrier and clean shutdown |

The last case uses two tasks and 8 KiB padding, not a stress workload. Its
owner-cleaned fact is recorded only after the GCS/Node/Worker cleanup chain;
the test does not manufacture death or invoke cleanup RPCs as observations.
It covers **already-adopted** publication, not pre-Complete/unknown owner death.
Neither the prior records nor the current selection closes the whole allowlist
or the remaining fault matrix.

## Verification evidence (historical)

The counts below predate the latest unified NodeServer/GCS/Core publication
wiring. They are retained as historical evidence, not a pass for this checkout.
These counts do not include the focused verification above. No full default
unit suite, GPU, heavy, scale or performance test was run in this update.

| Gate | Recorded evidence | Scope at the tested revision |
|---|---|---|
| Pure unit gate | Historical complete baseline: **1668 passed, 54 deselected in 11.04s** | Includes Hybrid-policy boundary tests, startup rollback, semantic trace contracts, INLINE/STORED publication, owner-wide replica cleanup, owner-death convergence, plain large by-value argument lift, and the previous reference/recovery baseline; subsequent increments require a new complete run |
| Recorded reviewed pure subset | **428 passed, 3 deselected in 1.29s**, 24 selected files | Worker discovery/custody and legacy publication integration, unified output pure components and local Node storage bridge, Complete replay, threadless Core dispatch/shutdown/reconstruction/foreign lineage and runner contracts; three explicit NodeServer L1 cases excluded. Not a complete unit gate; overlaps the earlier 29-file **607/12** subset and must not be added to it |
| Loopback smoke | Last recorded run: **7 passed in 0.28s** | Bounded single-process socket paths, run outside the sandbox because loopback binding is prohibited inside it; not a rerun after every subsequent edit |
| Multiprocess smoke | Earlier **50-entry allowlist**; recorded reruns of ordinary Task, mixed multi-return, INLINE lifecycle, STORED outer, targeted partial reconstruction and INLINE Node-loss KEEP/DROP | Each ran separately across adjacent revisions, not all 50 at one revision. KEEP (**1.29s**) and DROP (**1.25s**) cover reported successful Complete and one TaskHoldSource borrowed child; this is not the current allowlist size or unified-backend acceptance |
| Reclassified thread/socket/timer L1 | **21 exact node IDs**: previous 13, three timer checks, two dispatch concurrency/drain checks and three shutdown races, each passed independently through the bounded runner | Bounded local concurrency, no live cluster; the default unit safety classification remains incomplete |
| Static compatibility | Last recorded `compileall src tests scripts examples` passed | Predates subsequent edits; syntax checks are not runtime acceptance |
| Read-only review | A prior review found no remaining P0/P1/P2 in the then-repaired stored-replica, deadline, final-fence and owner-death report paths | Not a review of the current unified backend or the complete stored-owner fault matrix |
| Heavy/scale/performance | Not run locally | No throughput, scale, soak, fuzz, production-Ray differential, or real-machine claim |

The completed binding migration represents each logical Task reference hold as
`TaskReferenceHold(kind, submitting_worker_id, task_id, origin_attempt_id)`. The
origin attempt must belong to the Task; SYSTEM retry preserves that hold, while
lineage reconstruction creates a new hold incarnation from its reconstruction
AttemptID. Retained RPCs echo the complete hold, and owner active/tombstone state
is keyed by that complete value rather than a raw token. The full-unit row
records the historical complete baseline; the combined subset and recorded
static check do not establish a current full-suite pass.
The `StoredArg` nested-argument lift has focused pure contracts and a dedicated
bounded multiprocess witness; public `export_trace()` has eight focused pure
tests. Neither increment nor the metadata-only INLINE recovery change is
included in the complete baseline above. The INLINE Node/GCS/Core synchronous
composition suite passed seven pure cases; its combined Core/Node/full-flow run
passed 61 cases with one real-thread test deselected. These are focused evidence,
not a refreshed complete unit gate; the separate Node-loss multiprocess evidence
is recorded below.
The earlier 29-file **607/12** increment superseded the 19-file **383/1** and
11-file **224/11** runs; the recorded **428/3** subset is a different overlapping
selection, not a replacement full-suite count. Another three pure owner-death
files were separately verified as **31 passed in 0.18s**. The prior 13 L1 entries
include two reconstruction-concurrency cases; the three newly bounded timer
checks passed in **0.16s**, **0.15s**, **0.19s**; two dispatch concurrency/drain
checks passed separately in **0.16s**, **0.17s**. Three shutdown L1 cases then
passed individually in **0.20s**, **0.16s**, **0.17s**, bringing that allowlist to 21.
Six dispatch cases now use pure fixtures; combined with fifteen reconstruction
and five foreign-lineage cases they passed **26 tests in 0.21s**. Three shutdown
protocol cases were also made threadless (**3 passed in 0.21s**); these four
files are now included in the 24-file subset, not added again. The original
shutdown races remain separate opt-in L1 coverage. The safety audit found
other historical `unit` cases that start real threads or sockets
and some unbounded waits; the default marker is not proof of pure L0 safety.
Until classification/cleanup is repaired, only explicitly reviewed subsets are
run locally. See `testing.md` for exact commands and the remaining safety work.
Multiprocess evidence combines the earlier 42-test baseline, four separately run
stored-outer smokes, the secondary-replica Node-loss smoke, nested-large
argument smoke and two INLINE Node-loss smokes; the INLINE two-borrower rerun
is an existing allowlisted case.
The ordinary stored-result reconstruction smoke was also rerun through its exact
bounded runner entry (**1 passed in 0.99s**) after the task-finish barrier change.
The recorded single-test reruns covered ordinary Task **0.97s**, mixed multi-return
**0.98s**, INLINE two-borrower lifecycle **1.14s**, STORED outer happy/GC **1.27s**,
targeted partial reconstruction **0.87s**, INLINE KEEP **1.29s** and DROP **1.25s**.
These were run between adjacent small edits and do not constitute same-revision
acceptance of the whole allowlist. Exact commands remain in `testing.md`.

Ordinary borrowed references now freeze the owner route, exact Acquire/source,
and Release identity before the first Acquire RPC. Handle close, ambiguous-Acquire
compensation, invalid/lost Release ACKs, and shutdown retain one replayable
obligation until an exact accepted ACK arrives. A pending obligation is a shutdown
barrier; it is never discarded after a fixed retry count.
Worker process death is now represented by a separately registered physical
`WorkerID` incarnation, a Node-owned exact exit outbox, an ordered GCS journal,
and per-Core owner-table consumption. Transport failure yields an unavailable
route, not a death fact. `PROCESS_EXIT`/`NODE_EXIT` install reference fences;
`EXPECTED` advances the journal cursor without sweeping references. This model
has historical unit evidence. Its recorded multiprocess smoke covered: attempt 0
crashes after importing a nested borrower but before user code, attempt 1 runs on
a fresh Worker while preserving the logical Task hold, and the ordered death
journal removes only the dead attempt borrower (**1 passed in 1.34s**).

The two five-process Placement Group checks were run through the
30-second allowlisted runner:

- explicit create, two bundle-bound tasks, remove and teardown:
  **1 passed in 1.08s**;
- committed group with no explicit remove, followed by cluster PG-drain and
  shutdown: **1 passed in 0.82s**.

The application-error golden trace passes in **1.16s** and proves one user
exception remains one physical attempt with no system retry, while `push_task`
is a successful transport reply carrying an application failure. The startup
rollback path passes in **7.35s** and proves that a failure after the second Node
and Worker are ready, but before publication, removes all five child PIDs and
five endpoints without exposing a runtime.

The remote-Node crash slice also passed through the same 30-second runner:
**1 passed in 1.80s**. It proves an exact managed Process sentinel, a GCS DEAD
tombstone, a live-only membership snapshot acknowledged by the survivor, Core
location/attempt fencing, stable TaskID/ObjectID with a new AttemptID/LeaseID,
retry on the survivor, and typed per-Node shutdown reporting. Phase A is
deliberately limited to a Driver-owned ordinary Task on a remote managed Node.

The same-Node Actor Worker restart slice passed in **2.61s**; the K0 Actor smoke
passed in **4.67s**. The restart check uses one
lifetime CPU, so generation 1 cannot start until generation 0 releases its
allocation. It proves stable ActorID, incremented generation and route epoch, a
fresh WorkerID/PID, constructor-state reset, an old in-flight call fenced with
`ActorDiedError` and no transparent replay, and clean teardown. It does not
prove Actor migration after Node loss. A separate Node-loss migration smoke
passes in **5.53s** and proves a stable ActorID, generation/route advance, fresh
survivor Worker, constructor reset, old-call failure without replay, and sequence
zero for the new generation.

## Unified ordinary-Task output publication

All successful ordinary Task results now use one registered Worker/Node/GCS/Core
backend, including mixed INLINE/STORED, multi-return contained and targeted
contained results. The focused current evidence above covers representative
flows, not full K0/K1 acceptance.

`OutputDiscoverySession` serializes every selected slot once before any external
effect, preserves original return indices, and chooses storage per slot. The
accepted `StartWorkerLeaseReply` supplies the registered Node PID/epoch. One
`_PreparedOutputReply` retains the streams, source handles and argument
`NestedReferenceImportSession`; ambiguous preparation or Complete resumes this
record rather than executing user code or serializing again. Source/import
release remains an independent obligation. A failed Complete ACK must follow
exact compensation; an exact successful witness overrides a proposed abort.
Local ObjectStore failures are now typed preparation rejections, not endless
unknown preparation replay. CPU/Worker resource release can precede cleanup,
so outcome replies expose `cleanup_pending` while retaining the real terminal
lease state. Core cannot spend retry budget until all Node rollback effects
and the GCS rollback report have exact ACKs.

`PrepareOutputPublication` is registered on NodeServer. Its
`OutputPublicationNodeAdapter` and `OutputPublicationJournal` drive one full
INTENT, child prepare, one optional contained-graph reservation, per-slot
materialization, child promotion, and ARM. The local store bridge uses the
existing ObjectStore, sealed metadata, write claims and deletion tombstones;
INLINE is a materialization choice, not a separate success lifecycle. Complete
locally commits the journal and lease/resource transition with no GCS RPC. The
Node retains a metadata-only terminal outbox for later reporting.

GCS does not place ordinary Tasks, but it is a synchronous dependency of their
current successful publication: preparation waits for INTENT and ARM ACKs even
when no child graph exists. `CoreWorker._drive_output_publication_adoption`
synchronously reports terminal, commits the optional graph, and performs one
selected-output owner batch CAS before notifying readers. It then reports
adopted to GCS and obtains Node payload-retirement ACK. Failure in these later
steps retains an adoption/finish obligation; it does not undo local Complete.
GCS retains manifest/digest/graph metadata only, never result bytes. This
protocol is mini-ray's explicit teaching model, not production Ray's protocol.

Owner entries keep only their own slot bytes/descriptor and metadata membership.
Per-slot GC releases that container's child holds, graph edges and replicas;
the final sibling releases task lineage. Before targeted reconstruction, old
selected publication membership is retired; the new CAS changes only selected
LOST slots and preserves original return indices and healthy siblings. The
function still executes in full: selective publication is not partial computation.

## Unified recovery and acceptance boundary

The registered metadata-only recovery authority and Core/Node handlers include
publisher Node-loss and owner-death progression. Intent without ARM proves
pre-Complete rollback; ARM without a successful witness is
`COMPLETION_UNKNOWN`, not proof that execution failed. A known successful
terminal leads to `POSTCOMPLETE_RESOLVE`. Owner decisions are per slot: retained
INLINE bytes can be kept, while a lost STORED replica is not recoverable from
its descriptor. Exact cleanup precedes loss resolution. Known success with LOST
slots retains lineage for explicit `get`; unknown Complete uses budgeted SYSTEM
retry after cleanup. Reconstruction and last-reference GC still wait for the
old logical-task finalizer to release input holds and settle accepted accounting.
Two publication paths cannot retire the same custody concurrently: the Node
adapter uses one nonblocking ticket across forward effects and owner cleanup,
without holding local state/journal locks across Worker RPC. Worker freezes the
full owner-death request before releasing custody, so a lost cleanup ACK or an
already-admitted Push cannot restart that abandoned publication.

STORED KEEP is now also supported for an already-adopted slot with a
grant-backed secondary in the owner's current location set. Neither a GCS
descriptor nor an unadopted result envelope establishes that custody. The
canonical descriptor continues to name the original publisher; only the
mutable fetch route moves to the survivor. Replica loss after immutable KEEP
can leave a LOST slot with retained publication membership, which the existing
explicit reconstruction/GC paths must retire. This is not a replication policy,
background health probe or a promise that a prior grant proves current bytes.

An old targeted execution can still owe metadata adoption ACKs when a disjoint
sibling starts its next attempt. Its exact receipt plus execution/hold finish
barrier preserves only that cleanup tail; it must not mark the successor
successful, republish its slots, or reinsert work after terminal convergence.
Focused pure tests cover publisher-alive and publisher-dead variants, including
capacity retry metadata that does not change execution identity.

The three bounded cases recorded above are implemented in:

- `tests/integration/test_multi_contained_output_path.py` specifies one
  mixed-tier batch sharing a borrowed child, distinct per-slot holds, selected
  reconstruction of return index 1, healthy sibling preservation and reverse GC.
- `tests/integration/test_multi_output_node_loss_path.py` specifies publisher
  loss after real Complete delivery, preserving received INLINE bytes while the
  STORED slot becomes LOST and is reconstructed on the surviving Node.
- `tests/integration/test_output_owner_death_path.py` checks adopted stored
  publication owned by a killed Worker, with a live executor and surviving
  Driver-owned nested child.

The new ARM-UNKNOWN case covers one stored result with an executor-owned child;
it does not close lost-terminal-after-real-Complete or the full unknown-result
matrix. Wider lost/invalid ACK and borrowed-child cases, pre-Complete owner death, or competing cleanup
fault combinations. Runtime wiring, safe test classification and current-revision acceptance
must be audited separately before K0/K1 closure.

## Retained legacy protocols and historical fault evidence

Core's old publication branches, Node's legacy INLINE/STORED journals/adapters,
GCS's tier-specific registries/handlers/sagas and owner publication/GC/retirement
associations have been removed. Standalone lifecycle sources, old wire families
and reply fields have also been retired into non-executable history resources.
Shared source capabilities have neutral definitions and only a same-type
historical pickle export remains; Node/GCS no longer instantiate old models. Do not
count old and unified protocols as three required teaching mechanisms.
The unused `worker_stored_publication.py` coordinator has been removed after
its discovery/order/replay/Complete contracts were migrated to the unified
Worker and Node fixtures. New ordinary lease/location/cancellation candidates
are `OutputPublicationID` from lease creation onward; they do not probe an
INLINE registry and then fall back to STORED on pre-Push Node loss. A candidate
is local identity only, not an early GCS publication intent.

Four old Core test files are preserved as non-executable historical resources;
two old Node test sources are similarly archived, while their active filenames
now exercise unified fixtures. See `history/retired-core-publication/README.md`
and `history/retired-node-publication/README.md` for direct/partial/replaced
coverage and outstanding contracts. Three old GCS test modules are now archived
and a fourth rewritten for unified owner cleanup, with mappings in
`history/retired-gcs-publication/README.md`. Archiving is not equivalent coverage
or a passing gate. Four owner sources are additionally mapped in
`history/retired-owner-publication/README.md`. The unified Node dead-child fallback now has pure and one
same-live-Node Worker-loss acceptance record; broader death/interleaving and
default fixture migrations remain unfinished.
The final protocol/source-family archive and shared-contract mappings are in
`history/retired-protocol-family/README.md`; archived test counts are not current
passing coverage.

Earlier stored-outer happy/GC and pre-/post-Complete Node-loss smokes, and the
reported-Complete INLINE KEEP/DROP checks, remain historical evidence for the
then-current single-return backend. The INLINE checks covered a TaskHoldSource
borrowed child and the rule that missing bytes become LOST before explicit
reconstruction. Those five fault scenarios have now been migrated and rerun
on the unified gate; the records at the top, not old pass counts, establish
their new scope. The complete
publication owner-death and Node-loss fault matrices remain open.

## Capability matrix

In the matrix, “evidence” means historical individually bounded runs unless
explicitly stated otherwise; no row claims a same-revision full regression.

| Capability | Implemented | Pure/unit evidence | Multiprocess evidence | Important remaining boundary |
|---|---:|---:|---:|---|
| Ordinary Task, lease, direct submission | Yes | Yes | Yes | Fixed 1–2-slot pool; unlike production Ray there is no dynamic job/language/runtime-env Worker pool or multi-task leased-Worker pipeline |
| Unified selected-output publication and retained custody | Yes: registered Node/GCS/Core backend, including multi-return/targeted contained results | Current focused selection above | Mixed-contained, mixed Node-loss, adopted-owner-death and Worker-only-loss cases above | Complete historical-contract mappings and bounded ACK/death/GC fault acceptance |
| Two-node Hybrid spillback | Yes | Yes | Yes | One bounded redirect model; not a production cluster scheduler |
| Inline/stored result and Node-to-Node dependency pull | Yes | Yes | Yes | No Plasma, spilling, zero-copy, RDMA, or transfer pressure testing |
| Surviving replica promotion before lineage replay | Yes for the two-node slice, including unfinished adopted output publication | Exact-custody/KEEP/CAS replay and late-replica cleanup contracts | Source→target pull; contained adoption-tail KEEP and local late-DROP cleanup above | Broader multi-owner failure handoff/absence proofs remain; no replication policy, background health probing, or scale coverage |
| Public `put`, `get`, `wait`, `drop_object` | Yes for the current object slice | Yes | Local/foreign representative paths | Unified Task output reserves one selected-output DAG when needed; `put` has no producer lineage; no arbitrary cross-ObjectID cycles or production memory-pressure handling |
| Large by-value Task argument lift | Yes, including `StoredArg` with serializer and nested-reference manifest | Focused contracts | Dedicated nested-large argument smoke (**1.28s**) | Storage dependency is top-level; nested handles remain lifetime/import edges; broader combinations and a full unit rerun remain |
| Actor creation, direct FIFO calls, Worker restart and Node-loss migration | Yes for the teaching slice | Yes | K0 calls, same-Node restart, cross-Node migration and control/direct trace | ObjectRef arguments, method replay, named/detached lifetime, concurrency groups and PG Actor are explicitly omitted |
| Worker-side Core and blocking-`get` CPU yield | Yes | Yes | Yes | Deliberately bounded Worker topology |
| Owner/borrower/contained references and stored physical GC | Partial: unified per-slot contained publication/GC integrated | Current focused cleanup and replay contracts plus historical evidence | Three current narrow cases above, plus historical single-return slices | UNKNOWN Complete, wider borrowed-child/owner-death combinations, competing cleanup and full safe regression remain open |
| Lineage reconstruction | Yes for the K1 teaching slice | Yes | Local single/recursive/nested, foreign-owner/input, and multi-return all-lost/partial-loss | Broader failure matrix remains |
| Worker/Node crash recovery | Partial | Yes | Worker, remote/Driver-local ordinary Task, PG LOST, Actor migration | Broader failure windows missing |
| Placement Group planner and 2PC | Yes | Yes | Happy path, shutdown cleanup and participant Node-loss LOST | Actor PG and bundle rescheduling omitted |
| Cross-process trace and public JSONL export | Yes for the ordinary-task path; Driver `export_trace(path)` is implemented | Yes, including eight focused export tests | Cross-PID trace path has evidence; export tests use collector snapshots in-process | Golden trace proves lease, PushTask, StartLease and CompleteLease cross-PID edges; export is an observational snapshot, not a distributed drain; wider asynchronous queue causality remains future work |
| Cluster shutdown barrier | Yes for current topology | Yes | Exercised by every smoke | Does not imply production distributed shutdown tolerance |

## Placement Group slice

The integrated teaching path is:

```text
placement_group()
→ GCS shadow plan
→ all Node prepare ACKs
→ all Node commit ACKs
→ immutable per-bundle scheduling keys become visible
→ Task requests the planned Node and allocates only from its child ledger
→ remove/shutdown aborts every participant
→ root reservation is released only after every child lease is terminal
```

The Node root ledger remains the physical allocation authority. Prepare charges
it once; committed child ledgers divide already-reserved capacity, so a PG Task
never debits root twice. CPU-yielded and zero-resource live tasks still fence
root release. Create, remove, task publication and cluster shutdown use explicit
admission fences and exact replay identities. GCS resource summaries are
versioned retryable hints, never allocation proof.
The planner expresses the four Ray placement semantics with bounded backtracking
for PACK/SPREAD and augmenting-path matching for STRICT_SPREAD; it is not a
line-for-line copy of production Ray's scarce-resource-ordered greedy policies.

## Not complete

K0 and K1 remain open although Node crash recovery now covers representative
remote and Driver-local slices; the complete ownership/failure matrix remains
incomplete. Actor
recovery covers both a dedicated Worker crash on a live Node and migration after
participant Node loss; calls from every revoked route fail and are not transparently
replayed. The current
status must not be described as API-compatible, production-ready, real-multinode
Ray or a complete Ray Core implementation.

The cyclic-reference boundary is now explicit but remains partial. Ordinary
Python container cycles are serializer-local and supported. Cross-ObjectID
contained edges use a job-scoped DAG authority with atomic batch
prepare/commit/abort, prepared-edge cycle detection, exact replay, and a typed
cycle path error. The unified runtime reserves the complete selected-output
graph for INLINE/STORED slots together, commits before owner batch publication,
and calls `RELEASE_CONTAINER` per slot after child releases during GC. This is
a stronger teaching policy than production Ray, not a distributed tracing
collector. Multi-return and targeted-contained publication are integrated, but
the two new test files, historical single-return results and pure contracts do
not establish their complete fault acceptance. Legacy protocol retirement, the
remaining ownership/failure matrix and safe current-revision regression must
still be completed before K0/K1 closure; the exit criteria are not narrowed.
