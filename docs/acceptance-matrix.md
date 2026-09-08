# K0＋K1 验收矩阵

> **历史实现／验证索引。** 后续需求级基线见
> [correction-plan.md](correction-plan.md)，当前未验证草稿见 [handoff.md](handoff.md)。
> 本页旧“全部 allowlist 同版／宽矩阵”等表述保留作来源，不构成新增实施授权；
> 发布门禁须在语义确认后按需求一次性落实，不能随白名单自动增长。

**尚未完成：本页用于跟踪，不是完成证明。**

本页把有限的下一批工作映射到 [roadmap](roadmap.md)，不替换其规格、缩小出口条件，
也不把测试文件存在、历史通过或当前纯子集通过等同于整项能力验收。运行结果及版本边界
以 [current-status](current-status.md) 为准；执行安全要求见 [testing](testing.md)。

最新foreign-reconstruction-ack-contracts修复真实START后同attempt
已完成导致首次ACK误拒绝：Core pre/post同锁pairedsnapshot，strict
active/terminal组合且保FAILED/异常与exactcachedreply。新pure先红
后绿，并修pairedread异常误当unknown；foreign原4heavy迁pure，
原two-caller变L1和原foreignMP分别有界复验。未覆盖targeted
ERROR＋SUCCEEDED、Outcome生成前session消失、再次LOST/跨attempt
历史，不把normalMP当强制race或全K0K1证据；原GCS/globalDAG/phase不变。

此前blocking-entry-worker-drain-contracts修复notifier锁/消息入口失败
残留depth/sequence以及helper缓存failedscope的问题；原Block可能发送
后exactUnblock保证保留，新pure先红后绿。WorkerSide最后3heavy
保真实Condition/两handler原层次转L1逐项验收，正常CPUyield MP回归
另有证据；不是OS中断/OOM/真实entryfault多进程复现，完整K0K1开放。

此前notification-deadline-owner-contracts修复localPENDING和foreign
PENDING/可重试LOST在notifier进入后沿用旧等待预算；7pure中5红→
全部绿，READY/ERROR原优先级保留，不冒总墙钟取消。Worker3原
cache/retain/existingpin合同迁pure保真实owner，NodeBlock/Complete
原race转真threadL1，escapedref/CPUyield两个原MP分别复验。
它们不替代全部K0K1/同版gate/宽故障矩阵，GCS/globalDAG/phase不变。

此前lost-blocking-drain-contracts checkpoint修复Core两LOST等待持
authority锁进入notifier的问题；真实publication/Drop/barrier的四pure
先红后绿，通知后predicate/deadline重查及锁外Unblock得到验证，不冒
OS死锁再现。原Coreblocking7case与Workerbinding4case迁pure，Node
drain两原threadcase和Workerbinding单thread转L1逐项通过；原CPUyield
MP回归也独立通过。完整K0/K1仍开放，既有GCS/globalDAG/phase不变。

此前publication-trace-contracts checkpoint把原同步GCS发布接入例1的
真实黄金trace，而不只是增加架构文案。十RPC/四stage各自绑定
identity+ACK cause，区分Node资源释放、首次ownerREADY与Node回复
custody退休；pure抛异常sink验证不扰动原协议（不证明任意阻塞sink活性），real success/example01/
application-error三exact分别复验并在失败finally检查PID/端口。
这是教学可观察性的完成切片，不解决GCS还原度设计差异，也不完成
全部同版门禁/故障矩阵；原globalDAG/phase-specific合同保持。

此前sibling-spillback-lifecycle-contracts checkpoint闭合原三sibling
lineage/真实storedPINNED后GC；保producerPENDING时先提交consumer，
再真producer成功后执行consumer协议，不用put替Task。原并发close
保3closer＋1实际refconsumer转L1；registry/spillback各2真线程
有界，原锁/实际snapshot/policy/cache不变。原nestedlargeMP保
pendinghandle非readiness依赖、真实lift/pull/import/userget/三GC。
生产只更正模块首职责文案，不改原协议、故障或GCS取舍。

此前replay-foreign-lifecycle-contracts checkpoint保原2STORED replay与
3slotmanifest/preflight的11参数；wire拒绝和ownerCAS层次分明，
successpreflight改真实retainedadoption续行不冒旧escapingerror。
foreigndeath保2owner真实report/cancel/death/fence；旧STALE自动
release要求正式改quarantine至独立死亡授权，真实sourcePin已闭
墓碑不当泄漏清空。原twoforeignborrower及storedouter两个process
exact有界复验，保原Task/跨owner关系及Release/GC证据；未独立
观测foreignchildmetadataGC，不以ACK或shutdown代证。

此前admission-reference-concurrency-contracts checkpoint迁4原public
提交/retry到pure真实CAS，并正式替换旧STORED不回收合同，zeroRef
finishbarrier后真Drop/GC。两个原sameowner/Workerdrain保真实1/3
线程转L1；后者排除残缺Core异常被吞的假unclean，前者补callback
异常主线程核验。两原并发reconstruction用真Node初始publication/
drop，再由2真实thread在retirement竞争真defer→START/1enqueue→
JOIN与oldEnvelopefence；3内部请求不冒publicget/用户执行。

此前mixed-contained-terminal-contracts checkpoint将两个原3slot
success/error合同迁真实pureCore/Node；wake前全batch/route已提交，
error无成功envelope。另一Worker文件的旧contained-ref拒绝合同正式
替换为full/selected正常publication；target只是PENDING未选槽
投影，不当真实reconstruction。原sharedchild mixed与publisherloss
两exact进程有界复验，真childholds/KEEP0-DROP1/full2-selected1/
healthy0与阶段GC；未更改sharedlegacyhelpers或原publication保证。

此前multi-return-lifecycle-contracts checkpoint正式用postfinish重复
不得释放live committed edge的真实合同替换旧rawstale-orphan假设；
另外两generic export原case迁pure，真pin/FIFO/round/tombstone/GC，
不冒Worker发布或publicshutdown。原mixed多返回retry/依赖及whole
重建两个进程exact均有界复验；后者明确2显式drop/1重建例外，
真commit START＋注入siblingrequest JOIN，不称并发publicget。
正常分阶段GC与failure PID/端点清理区分，完整K0/K1仍开放。

此前replica-retirement-contracts checkpoint将原contained cleanup
precheck与generic export重试迁pure；后者明确是兼容原语，不把
当前Worker发布重新接入旧后端。原双副本physicalGC以实际owner
probe/两DropACK/trace/absence及replay核验，原partial单槽重建
验证full/selected envelope与healthy0/2完全不变、真实最后GC。
资源/期限/失败清理已受限并逐exact复验，完整K0/K1仍开放。

此前nested-shutdown-contracts checkpoint迁原pending-outerclose为
currentselectedoutput纯合同，edge安装在wake前且finish前不得GC。
两原borrower shutdown保真实三线程Core转L1并逐项通过，transport/
Timer隔离不伪称网络/多进程关停。localnested原INLINE容器前提与
nestedargument测试helper序列化分别先失败后修正，保原语义/Task图；
后者输入是READYput，不能当PENDING输入的时序证明。完整出口仍开放。

此前foreign-lifetime-contracts checkpoint迁原borrower pre-effect outage、
不可达/关闭准入与contained release冻结合同到currentselectedoutput
纯组合；owner未生效时token必须保活、原retry/精确ACK后才GC。
foreign-owner重建、foreignwait/drop ACKloss、foreigninput retained
换代三个原进程路径分别复验，保持owner/ObjectID/原earlyclose与
实际GC边界，内层RPC有限重试不伪称全call15s取消。完整出口仍开放。

此前release-concurrency-contracts checkpoint迁原Release补偿/六种坏ACK
与lateowner准入到纯authority：真实Release墓碑与原scheduled event、
真实WorkerRegistry死亡suffix，未伪造外部值/死亡或运行时timer。
原同Node双Worker和两Node并发gate分别复验，仍双arrival后才放行；
原三层DAG按显式2CPU/3drop/3重建复合预算单独批准并验证全@0→@1、
仅root触发递归、全lineage/ownerGC。它不冒充单故障或全矩阵。

此前borrower-startup-contracts checkpoint将两个原borrower加载/ACKloss
合同迁pure：真实put/outer canonical lineage/selected-output与owner
Acquire/Get/Release、最后borrower才childGC，不冒称实际重建。三个
Core startup原case保真实线程转L1并逐exact验收，不假装pure；成功
ctor的周期poll隔离在fixture，abort/RPC禁令仍真实检查。两个原
Worker consumer/owner死亡进程用例补共享期限、actual outerGC与
replacement端点清理后独立复验；准确范围见状态页。提交准备同步
seal/hold与异步执行的教学措辞也已纠正，完整K0/K1仍开放。

此前node-lifecycle-contracts checkpoint复验原远端Node恢复、第二Node
ready失败回滚与例7；前两者收紧真实进程/对象/期限和失败清理边界，
例7展示实际committed handle映射，明确本容量下无法区分PACK和
STRICT_SPREAD、不把成功示例称为PREPARE期间可见性证明。原put
丢失与Node WorkerLoss late-Complete两case迁pure，分别真GC无lineage
put和真ARM→reclaim→SLOT_DROP/rollbackACK，完整出口仍开放。

此前recovery-contracts checkpoint修正两个旧进程测试的合同/前置条件：
home-loss控制参数必须INLINE，不能丢失无lineage的lifted put后仍要求
重试成功；Worker在successful Complete后退出则从Node envelope采用
原attempt0，不应重跑用户函数。后者显式改名，另一个不Start/Push的
lease-only探针观察replacement，未增加第二个用户任务或故障。原Worker
dependency hold及queued PG loss两case安全迁pure，真实finish/GC保持；
例6在相同后端/故障预算中展示实际Task/Object稳定与Attempt0→1。
精确通过、先失败证据和边界见状态页，完整出口仍开放。

此前dispatch-continuations checkpoint显式区分新准入与既有协议续行：
ReadyTask派生互斥kind，PG/非PENDING过滤保原规则，kind不替代当前
unresolved marker。原3spillback/Push和3foreign报告合同安全迁pure，
后者改用canonical foreign lineage，finish不提前释放retained hold，
到output GC才真实Release。13新封套合同只证路由、不证payload有效性；
原PG双phase进程回归和foreign-stored-ref路径在新dispatcher分别复验。
完整出口/剩余heavy/真实故障组合仍开放，精确范围见状态页。

此前Worker-locality/admission checkpoint补上真实无snapshot内嵌Core
冷查询→命中正缓存的成功路径，区分评分范围(1,0)与adoption独立lookup；
源owner不变，foreign lineage/borrower/真实GC在Worker存活时收敛。
原5Core取消/8展开及3PG首跳、容量、重试/重建合同已安全迁pure，
保原ID/参数与现selected-output/精确资源/引用语义，不冒充运行时并发。
原foreign-stored gate的setup、共享deadline、被动记录上限和failure
cleanup已收紧并复验。具体范围与未完组合见状态页。

此前lease-locality checkpoint接通普通新lease的bytes-first首跳，
与Node Hybrid/本地账本最终放置分开。当前epoch本owner多副本与foreign
单source提示、无snapshot的可选冷查询正缓存、原身份重放/PG bypass
均有纯composition入口；三个公开Task另验证B首跳与B→home spillback、
custody先于Push及真实副本GC。具体执行结果见状态页，不外推Worker冷
查询的真实进程故障或完整多副本矩阵。GCS publication/DAG合同不变。

此前PG-retry-atomicity checkpoint把PG LOST检查与owner/recovery retry
提交合在同一Core锁；纯真实authority交错先复现死亡先提交却消耗预算，
修后验证death-first和retry-first的合法边界。原multi-return rollback
8case及retry前三3case安全迁pure；三个文件与分类锁、固定受审选择
均有实跑记录。七个原/已有PG及retry exact MP在此Core修复后分别通过，
不是该锁窗口的真实线程复现，也不是全部allowlist同版验收。
PG participant-loss另修正get后立即要求GCS资源hint已到达的观察假设：
仅在原工作期限内被动查询，保最终资源相等与身份/cleanup断言。
README入口重排不改变K0/K1范围、单后端或GCS/DAG合同。

此前publication-continuations checkpoint修复PG初始准入误截断已有发布
续行的问题。两个真实MP参数验证terminal/adopted ACK丢失加idlepeer
Node故障后仍完成同一publication/Node payload退休/finish；已知Complete
不会因另一bundle死亡被抹成ERROR。此为明确复合故障切片，不替代全部
F矩阵；deferred-system pure仅证明路由，原late-replica规则未扩大。

此前replica-integrity/lifetimes checkpoint修复共享sealed清理的防御性
bytes一致性，保旧receipt/absence恢复；损坏只在tiny pure fixture显式注入，
不称public fail-stop反例。六个原lineage/nested生命周期case迁到pure，
四个原Actor gate和F5/cross-cleanup在该实现后逐项复验。它们仍不替代
同版本全allowlist、完整fault矩阵或GCS fidelity决策。

2026-09-07 的 PG-cancellation/classification checkpoint 扩展并实际复验了
受审pure范围；62个旧文件的分类、陈旧成功fixture迁移与四个新PG取消合同
均有明确选择记录。已LOST PG保留typed cause，Cancel/custody未收齐前仍
PENDING；普通先选错误不变。E4 spillback、E5 pull、首个lineage及E11
participant-loss也已在该修复后逐项复验。它们不等于全部E表/F矩阵或
surviving ambiguous lease ACK窗口的进程验收，以下完整出口仍开放。

## 1. 已接线机制与尚缺证据

表内“已接线”表示实现参与当前运行时，不表示全部故障组合已验证。E 编号指下一节
的精确进程选择器；它们是代表性证据入口，不是完整 allowlist。

| roadmap 类别 | 实现／算法证据 | 测试入口与剩余要求 |
|---|---|---|
| K0.1 身份、规格、可观测性 | [ids.py](../src/miniray/ids.py)、[protocol.py](../src/miniray/protocol.py)、[trace_contract.py](../src/miniray/trace_contract.py)：逻辑 ID 与 attempt/generation 分离、因果 trace | [test_ids_resources.py](../tests/unit/test_ids_resources.py)、[test_trace_contract.py](../tests/unit/test_trace_contract.py)、E1。成功／用户异常黄金语义合约已保存；其余出口仍需同版核验 |
| K0.2 进程拓扑、生命周期 | [api.py](../src/miniray/api.py) 的 spawn 启动／回滚／shutdown；[node.py](../src/miniray/node.py) 的固定 Worker pool；Worker 内嵌 Core | E2；Worker 子任务与 owner 路径见 E3。部分启动回滚、两节点账本与每次 PID／端口清理仍须逐项验收 |
| K0.3 Task、依赖、direct submission | [core.py](../src/miniray/core.py)、[dependency.py](../src/miniray/dependency.py)、[worker.py](../src/miniray/worker.py)：pending ref、先 gate 后 lease、direct Push、Start/Complete、exact replay | E1、E3、E4；[test_handoff_push_admission.py](../tests/unit/test_handoff_push_admission.py)。未知 lease/Push/cancel 与 shutdown 保留义务不能由 happy path 代证 |
| K0.4 资源与 Hybrid | [resources.py](../src/miniray/resources.py) 的 ResourceLedger、HybridPolicy：feasible/available、非 GPU 优先、利用率阈值、locality、seeded top-k；Node 最终扣账 | [test_ids_resources.py](../tests/unit/test_ids_resources.py)、E4。已实现，不因旧 checkbox 未勾选重复开发；仍需同版防超卖／teardown 证据 |
| K0.5 ObjectStore 与跨节点 pull | [object_store.py](../src/miniray/object_store.py)、[object_manager.py](../src/miniray/object_manager.py)、[transfer_pins.py](../src/miniray/transfer_pins.py)：immutable seal、源 pin、完整校验后 ready | E5；[test_cross_cleanup_receipts.py](../tests/unit/test_cross_cleanup_receipts.py)。并发 pull、失效与 GC 组合没有被单对象 smoke 全部覆盖 |
| K0.6 Actor | [actor_state.py](../src/miniray/actor_state.py)、[actor_client.py](../src/miniray/actor_client.py)、[control.py](../src/miniray/control.py)：GCS 创建、专属 Worker、方法直达、caller FIFO | E6；[test_placement_actor_state.py](../tests/unit/test_placement_actor_state.py)。同版准入、缓存重放、drain 核验仍开放 |
| K1.1 引用、DAG、publication | [ownership.py](../src/miniray/ownership.py)、[contained_cycle.py](../src/miniray/contained_cycle.py)：独立存活边、含 PREPARED edges 的三色 DFS；[output_recovery.py](../src/miniray/output_recovery.py)：PRECOMPLETE／UNKNOWN／POSTCOMPLETE | E7、E8、第 3 节 F1–F7；[test_output_recovery.py](../tests/unit/test_output_recovery.py)。保留全部 tier／contained／mixed／targeted 合同；单槽 borrowed ARM 只是窄证据 |
| K1.2 Lineage 与 attempt fencing | [recovery.py](../src/miniray/recovery.py)、[reconstruction_runtime.py](../src/miniray/reconstruction_runtime.py)、[targeted_reconstruction.py](../src/miniray/targeted_reconstruction.py)：producer START/JOIN、预检后 CAS、依赖优先、退休后换 epoch | E7、E9；[test_recovery.py](../tests/unit/test_recovery.py)、[test_targeted_retirement_admission.py](../tests/unit/test_targeted_retirement_admission.py)。F2 补 targeted 故障，不把无故障重建视为全部重建恢复证据 |
| K1.3 Blocking get CPU yield | [resources.py](../src/miniray/resources.py) 的 yield_cpu/reacquire_cpu 与 [blocking.py](../src/miniray/blocking.py)：仅让出 CPU、episode fencing、signed debt、终态一次清账 | E10；[test_node_blocking_get_authority.py](../tests/unit/test_node_blocking_get_authority.py)。算法已接通；reacquire 是账本恢复，不等物理空闲，仍需同版成功／失败／取消核验 |
| K1.4 Placement Group | [placement.py](../src/miniray/placement.py)、[placement_group_runtime.py](../src/miniray/placement_group_runtime.py)：shadow view、bounded backtracking／增广路匹配、prepare/commit/abort、独立 bundle pool | E11；[test_placement_group_runtime.py](../tests/unit/test_placement_group_runtime.py)。要求全 commit ACK 前不可见、回滚所有已 prepare 节点；Node loss 的合同是 LOST，不是自动重排 bundle |
| K1.5 Node loss、Actor restart | [node_monitor.py](../src/miniray/node_monitor.py)、[control.py](../src/miniray/control.py)、Node/Core：受管进程退出证明、普通 Task 预算重试、Actor generation／route fencing | E12；[test_core_node_death_recovery.py](../tests/unit/test_core_node_death_recovery.py)。同版死亡节点 locations／lease／allocation 清理及旧消息 fencing 仍须核验；不新增透明 Actor method retry |

K0.4的locality现在包含独立[lease首跳策略](../src/miniray/lease_policy.py)，
不只是Hybrid的preferred-node等分规则；其[纯Core合同](../tests/unit/test_core_lease_locality.py)
与[真实三任务路径](../tests/integration/test_lease_locality_path.py)分别记证。
资源裁决仍只在Node，当前不实现跨任务lease复用，也不将这些切片当完整K0.4验收。

## 2. 代表性精确进程选择器

以下全部相对 `tests/integration/`，**不是整文件运行指令**。先完整审查所选函数及
imports/fixtures，再通过 `scripts/run_bounded_test.py` 每次只运行一个 exact ID；
执行上限 30 秒，随后有界终止／回收。不能并行 pytest 或由此清单推断运行安全。

| 编号 | 文件与精确函数 |
|---|---|
| E1 | `test_cross_process_trace.py::test_one_task_emits_cross_process_golden_trace_and_cleans_up`；`test_cross_process_trace.py::test_application_error_trace_is_terminal_without_system_retry` |
| E2 | `test_startup_rollback_path.py::test_second_node_ready_failure_rolls_back_every_started_process`；`test_two_worker_pool_path.py::test_one_node_two_workers_execute_two_tasks_concurrently` |
| E3 | `test_worker_nested_task_path.py::test_worker_submits_child_task_and_gets_plain_result`；`test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get` |
| E4 | `test_two_node_spillback.py::test_custom_resource_spills_task_to_second_node_and_cleans_cluster`；`test_task_retry_path.py::test_explicit_worker_system_error_retries_once` |
| E5 | `test_cross_node_dependency_pull.py::test_store_backed_dependency_pulls_to_consumer_node_before_direct_push`；`test_cross_cleanup_receipt_path.py::test_publication_rollback_receipt_replays_after_same_object_retry_seals` |
| E6 | `test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker` |
| E7 | `test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` |
| E8 | `test_stored_outer_node_loss_path.py::test_armed_unknown_stored_outer_node_loss_cleans_then_retries`；`test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` |
| E9 | `test_recursive_lineage_reconstruction_path.py::test_recursive_lineage_reconstructs_leaf_to_root`；`test_foreign_input_lineage_reconstruction_path.py::test_foreign_input_hold_replaced_before_consumer_reconstruction` |
| E10 | `test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` |
| E11 | `test_placement_group_prepare_failure_path.py::test_second_participant_prepare_rejection_aborts_first_and_restores_roots`；`test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg` |
| E12 | `test_node_crash_recovery_path.py::test_remote_node_death_retries_task_on_survivor_and_reports_crash`；`test_actor_restart_path.py::test_actor_crash_restarts_once_fences_inflight_call_and_resets_state`；`test_actor_node_loss_migration_path.py::test_actor_migrates_after_remote_node_loss_and_resets_generation` |

七个教学脚本另有 [original-main 验收](../tests/integration/test_teaching_examples_path.py)：
`test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]` 至
`[example07]` 均已分别运行，精确资源边界和版本见状态页。它们不替代 E 表的
故障／因果合同或完整 allowlist；Actor/PG 只有外层30秒实验上限，没有新增
超时即取消的公共语义。

## 3. 有限的下一批语义故障验收

这七项把下一批工作具体化，**不是完整笛卡尔积，也不替代 roadmap 的完整范围或出口**。
“开放”指缺该真实组合的闭合证据，不先验断言算法未实现；发现反例后再做最小修复。

| 编号 | 单场景边界与必须观察的区别 | 状态 |
|---|---|---|
| F1 | shared-borrowed mixed-tier multi-return，在 ARM 后、Complete 前 publisher loss；同一 live child 的各槽旧 holds 全清理后才 retry | 此场景已验收：[mixed borrowed UNKNOWN](../tests/integration/test_mixed_borrowed_output_unknown_path.py)，两槽 DROP、一次 retry、逐槽 GC 与最后 sibling lineage 释放；非整个故障矩阵 |
| F2 | F1 的故障发生在 targeted reconstruction；旧 selected custody 先收敛再 retry，健康 sibling 的 ID／attempt／descriptor／holds 不变 | 此场景已验收：[targeted borrowed UNKNOWN](../tests/integration/test_targeted_borrowed_output_unknown_path.py) 的 mixed-selected exact ID；原indices1/2、健康INLINE0不变、budget明确计重建＋系统重试，逐槽GC |
| F3 | Complete 实际生效，但 terminal 与 TaskReply 都不可得后 publisher loss；不能把 ARM-only 当从未执行 Complete | 此场景已验收：[unreported Complete](../tests/integration/test_unreported_complete_node_loss_path.py)。test-local 隔离发送／交付后崩溃，真实本地成功但 GCS/owner 仍 UNKNOWN；迟到 envelope 决策前后另有 pure 合同 |
| F4 | outer-owner 在 INTENT 后、物化／promotion 前死亡；执行方最终清理并拒绝迟到成功，不能接管 owner | 此场景已验收：[precomplete owner-death](../tests/integration/test_precomplete_output_owner_death_path.py) 的 INTENT exact ID；真实 owner 死亡、Node fence 后开 gate、同一活 executor Finalize ACK，无物化／ARM／owner 接管 |
| F5 | outer-owner 在 promotions 后、Complete 前死亡，foreign child owner 仍活；准确释放已建立的 child holds、graph 与 replica | 此场景已验收：同文件 PROMOTIONS exact ID；真实 sealed output、live Driver child 的 release ACK、graph/replica 清理及替换 Worker 新端口退出，不以 dead child 代证 |
| F6 | 已 latched DROP 后，foreign-owner 收到迟到 secondary replica；先 custody／cancel，原 owner 历史授权 exact GC，旧消息不删新 epoch | 此场景已验收：[foreign late-replica](../tests/integration/test_foreign_late_output_replica_cleanup_path.py)；活 Worker owner 返回真实 RETIRED/custody，consumer 真 cancel/no Push，自治删除后才回放旧 Drop；public get 重建后旧 report/Drop 不改新 bytes/epoch、健康 INLINE 或 child holds |
| F7 | 两个经真实 GCS 的 PREPARE 构成 ObjectID 环；第二个原子拒绝，ABORT／RELEASE 后旧 transaction 仍被 fencing | 此场景已验收：[contained-cycle control](../tests/integration/test_contained_cycle_control_path.py)；两个注册 metadata-only publication 验证反环／ABORT，独立 public Task 的 owner GC 验证 COMMIT／RELEASE，不伪造 child／Complete |

新增窄证据：
`test_borrowed_output_unknown_path.py::test_armed_unknown_borrowed_output_releases_old_holds_before_retrying_same_live_child`
（[源码](../tests/integration/test_borrowed_output_unknown_path.py)）本轮已单独有界通过；运行记录见状态页。
它证明 live Driver-owned child 的旧 transfer/final contained holds 在系统 retry 前收敛；
dead-executor borrower cleanup 是独立生命周期，不虚构其必须早于 retry 的顺序。
范围只有一个 STORED slot，不覆盖 F1–F7 完整目标。
F1–F7 的限定场景进程证据及完整 exact selectors 见状态页；这不是全部故障矩阵。
[Worker-owner Node-loss](../tests/integration/test_worker_owner_node_loss_path.py) 另证明
活 Worker Core 消费本地 certified view 并自主重试，是 F6 的恢复前提；
迟到副本的真实 custody／GC 证据来自独立 F6，而不由前提用例代证。
不能把有限场景完成外推为全部 K1 失效组合或同版出口通过。

每个新场景固定检查：最强已知 phase、稳定逻辑 ID、合法的新 attempt、预算、
旧消息 fencing、精确 holds/graph/replica 清理及最终账本/PID/端口/未 seal 对象。
所有推进使用语义 event/failpoint，不以 sleep 猜时序，不由测试伪造清理 ACK。

## 4. Quarantine：诊断边界，不是已证实的公共 API 缺陷

[test_local_replica_handoff.py](../tests/unit/test_local_replica_handoff.py) 的
`test_new_local_producer_epoch_before_replay_is_quarantined_without_overwrite_or_deletion`
直接 register/publish_stored 一个无 producer/publication history 的输入，再直接
advance_attempt 和重新发布。它证明此不一致组合 fail-closed，不能作为正常恢复 bug 的复现。

正常 Task 先保存 publication manifest，whole/targeted 重建先退休旧 membership 再 advance；
迟到 report 先查 `retired_output_replica`。真实 put 固定 attempt 0、无重建 lineage，
收集时保存 `retired_stored_replica` 所需的 compact history。实现见
[ownership.py](../src/miniray/ownership.py) 与 [core.py](../src/miniray/core.py)，
回归见 [test_core_late_replica_cleanup.py](../tests/unit/test_core_late_replica_cleanup.py)。

目前未找到受支持公共路径中“合法推进 epoch 却缺原 owner 历史”的反例。现有 quarantine
保留冲突证据并拒绝破坏性动作；应先诊断，不承诺自动修复任意 metadata 损坏或新增修复协议。
若出现合法路径反例，应补原 owner 的 retirement/collection 历史，复用既有 cleanup，
不能让来访 descriptor 自证权限、猜 ACK 或更换 owner。

## 5. 不可由本页替代的出口

- [ ] K0 对应纯单测及 K1 各机制纯状态机门禁在当前版本验证；安全分类未完成时不得运行全默认 suite。
- [ ] 全部 allowlisted 有界多进程 smoke 在同一版本经审查后逐个独立通过；E 表和累计历史不能替代。
- [x] 成功／用户异常黄金 trace 语义合约已保存；本项沿用 roadmap，不推导本版全部进程通过。
- [ ] 每类 K1 机制至少一个受限单故障真实 smoke；所有旧 attempt/generation/transaction 注入稳定 fencing。
- [ ] 每次 teardown 核验两节点账本、自己创建的 PID／端口及未 seal 对象，不用宽泛进程清理。
- [ ] README 最小 API 在当前版本实际验证后，才按 roadmap 完成相应出口文案。
- [ ] 完整默认测试安全分类与所要求的完整单元 gate 仍开放。[conftest.py](../conftest.py) 仅阻止误用广泛收集；显式文件不是安全认证。

[reviewed-pure 入口](../scripts/run_reviewed_pure.py) 和显式 manifest 只证明已审查选择范围，
不自动认证新 imports/fixtures，也不把未运行用例计为通过。

普通成功的逐任务 GCS INTENT/ARM/terminal/adopted 依赖仍是 Ray 还原度设计差异。
保留 [production-ray-mapping](production-ray-mapping.md) 的区分；global fail-fast DAG
与 phase-specific 恢复是现有 K1 合同，未经设计取舍确认不得取消。增加测试、移动权威
所在模块或完成本页七项，都不自动解决这一差异，也不自动完成 K0/K1。
