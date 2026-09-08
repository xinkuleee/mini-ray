# 测试策略与 Mac M4 安全边界

> **归档边界（2026-09-08）**：本次整理／推送没有执行测试。以下是历史 checkpoint
> 的结果，不是当前快照通过声明；两份未验证草稿及分类冲突见 [handoff](handoff.md)。
> 需求冻结与执行清单冻结的区别见 [纠偏方案](correction-plan.md)。
> 白名单只表示受限执行入口，不定义发布范围，也不免除逐项成本审查。

> **本轮安全复核：不要直接运行完整默认 gate。** 历史混标用例中的真实 socket、
> Core 线程、无超时 join 和 live coordinator 队列路径已分别保留为非纯测试；
> marker 仍不是永久 L0 证明。当前只运行已逐项确认的固定纯集合，以及 allowlist
> 中逐个审查的30秒进程隔离smoke。原runtime合同的安全迁移与完整出口仍待完成。

## 最近一次已记录验收（2026-09-08；早于保留的测试草稿）

foreign-reconstruction-ack-contracts 更新证据后最终复验：**2910 passed,
12 deselected in9.17s**（首次扩展9.29s）；**201 whole files＋另36文件254 exact selectors**，
共455。compileall覆盖`conftest.py src tests scripts examples`通过；所有
pytest串行，无heavy/default/目录级/全部同版gate。

新首ACK合同最初5pure **3 failed/2 passed in0.23s**：真实START已入队，
同attempt success/application-error先完成清active，原reducer误回
AUTHORITY_REJECTED；两metadata读取也未持Core composition锁。preview
未commit及错epoch控制原已通过。normal/lazy构造均接延迟callback，
pre/post分别同锁读owner＋recovery，锁不跨admit/RPC；真实START/JOIN
且双current同attempt才检查严格state组合。active必须PENDING＋
RETRY_PENDING/RUNNING；无active仅READY＋SUCCEEDED，或ERROR＋
APPLICATION_FAILED/SYSTEM_FAILED。exactcached reply仍优先，
FAILED/throwingcallback不得凭末态升级为成功ACK。

新5＋不兼容pair/失败callback/真实terminalsystem控制 **10 in0.33s**。
独立审查发现本次paired修改把recovery读取异常误归UNKNOWN_OBJECT；
新增2pure **1 failed/1 passed in0.21s** 后修为只有真实
UnknownObjectError才unknown，其余AUTHORITY_REJECTED，无admit/
claims/cache/epoch变化。最终12新case＋owner/deferred/targeted/
runner/classification回归 **153 in0.57s**；追加negative/systemcase
不冒原实现已观察到的红测。

新fixture2threadlessCore/1Task1slot、≤2attempt/publication、1×4KiB
store/≤128Bslot，真stored发布→finish→debugdrop→ownerSTART；
retry在callback返回前真INLINE发布或Core terminalerror。显式legacy
exporthold/Acquire/Release不冒serializedouter；未完成的control
不伪finish/GC，callback/receipt每channel≤32，禁runtime/用户Task/
socket/线程/Timer/真实wait。pair锁probe非OS并发时序证明。

foreign原4heavy迁pure，5原函数8case保留，迁移单文件 **8 in0.26s**：
真canonicalSTORED原输出/drop/STARTJOIN、原INLINEretry、borrower
Release/exportholdrelease/真实GC；2pureCore/1Task/≤2publication/
1KiBstore。timeout仅unavailable，显式committedownerdeath另行安装，
不因RPC失败推断死亡。

| Exact bounded selector | 结果 | 受限范围 |
|---|---|---|
| `tests/unit/test_owner_reconstruction.py::test_concurrent_exact_requests_commit_once_and_replay_one_reply` | 1 in0.13s | 原2真实请求线程/1START/同cached对象，metadata模型非发布runtime |
| `tests/integration/test_foreign_reconstruction_path.py::test_driver_reconstructs_worker_owned_stored_object_through_owner` | 1 in1.40s | 原5children6端点/3Task4执行、2×1MiBstore/64KiB结果，真实foreign重建 |

两exact全读/分别批准/30srunner逐项，均在最终production上执行。
原concurrent由heavy→L1，先前本轮还曾1in0.20s；2owneddaemonthread，
barrier1s、normaljoin共享2s/finallyjoin共享1s，保真实原锁与cache。
MP work共享15s，body与finally各自close共享至多3s，内部RPC/startup/shutdown另有限界；
先捕获knownPID/端点再做形状assert，failurefinally查无残留/未初始化。
正常MP不冒强制completion-before-ACK或首次foreignPENDING证据。

静态 **240files1926fn、2910pure98heavy35L1**；allow **88MP45L1**
共133exactIDs跨94files。manifest同12L1排除；owner文件仍11exact
pure函数14case＋单独L1，foreign/newrace才whole。guards/逐名/参数
映射对齐，无pure遗漏/duplicate/overlap/heavy误选，非永久安全认证。
targetedERROR＋taskSUCCEEDED需独立jointreceipt；targeted在形成
Outcome前session消失、首次ACK前再次LOST或advance也未覆盖。
不扩宽状态接受来绕过证明；完整K0K1/同版gate/宽矩阵/GCSfidelity
开放，原同步GCS/globalDAG/phase保证不变。

### 先前 blocking-entry-worker-drain-contracts checkpoint（2026-09-08）

blocking-entry-worker-drain-contracts 更新证据后最终复验：**2894 passed,
12 deselected in8.31s**（首次扩展8.39s）；**199 whole files＋另37文件255 exact selectors**，
共454。compileall覆盖`conftest.py src tests scripts examples`通过；所有
pytest串行，无heavy/default/目录级/全部同版gate。

初始6pure在旧code **4 failed/2 passed in0.16s**：lockenter中断/
Block构造失败残留depth导致下一scope不发Block；native/fallback组
缓存failedscope使同group catch/rebegin跳过新Block。两外传exception
control原已通过，不冒额外bug。fix为outerfinally保证depth在Unblock/
lock退出后复位，candidate Block成功构造才commitsequence（可能发送后
不回滚）；native/fallback/三foreign手工scope成功enter才保存exit义务。
原exactUnblock及错误优先级保留，无新retrywire/权威。new6+prior27
**33 in0.33s**。之后增加foreignPENDING代表1pure，new7+prior27
**34 in0.28s**：真ownerborrowercap/单query/原exceptionidentity/deadline
restore/noexit-unentered/nopollfetch/真实Release，不声称此第7项已跑
旧code红，也不将一个PENDINGcase外推另两foreign运行证据。

入口测试的KeyboardInterrupt/MemoryError是一回localboundary注入，
不是实际signal/OOM或线程竞争。锁wrapper只非阻塞取得真实Lock最多2次，
typedRPC最多4，无真实NodeCPU账本；组内catch/retry是helper合同，
普通get_many错误仍外传。追加foreigncase只1PENDINGTask/2纯Core、
真实legacyexportpin/Acquire/Release，pendingbarrier不人工清除，
两个receipt仅实例is_set验证，globalwait仍禁。

| Exact bounded selector | 结果 | 受限范围 |
|---|---|---|
| `tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_drains_task_before_embedded_core_and_clean_ack` | 1 in0.21s | 原admission-only task/shutdown两真实non-daemon线程，先_end_task再Core seam |
| `tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_timeout_keeps_core_open_for_inflight_parent` | 1 in0.13s | callingthread真实1msConditiontimeout，原activecount1保持 |
| `tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_fences_only_new_owner_retains` | 1 in0.17s | 真实1msConditiontimeout后newretain fence及原Core route |
| `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` | 1 in1.01s | 原4children6端点/2Task/1MiB，正常真实Workerget/CPUyield回归 |

四exact全审/分别批准/30srunner逐项。首L1只2ownedThreads可start/join，
taskgate2s/entry和waitobservations1s、shutdown原1s、normal/finaljoin
各共享2s；异常/realwait观察cap16，waitwrapper调用原Condition.wait，
wait_for原deadline不改。shutdown线程保non-daemon避免额外finalizer
thread；fixture无_replies、admitted返回string，不冒userfunction/
Start/Complete/publication。失败先release+exactjoin，线程已停且真
_end_task归0后才必要真实Worker shutdown，不手写active0/clear。
后两L1无新thread，只callingthread真实wait、原fakeCore/count前提，
不证明真实parent或Corethread teardown。MP保原10swork/3sclose和
failurefinallyPID/端点检查，正常path不冒entryfault。

focused **121 in0.41s**；manifest199whole255exact/37other=454保12
原L1exclude，WorkerSide14exactpurefn/16cases＋4L1不升whole，
最后3heavy迁L1不意味着全部K0K1/安全门禁已完成。剩余历史runtime/
宽矩阵/同版出口/GCSfidelity开放，原publication/globalDAG/phase保证不改。
静态 **239files1917fn、2894pure103heavy34L1**；allow **88MP44L1**
共132exactIDs跨93files，旧guards/逐名/参数映射对齐，无pure遗漏/
重复/overlap/heavy误选。这仍只是当前受审snapshot，非future安全认证。

### 先前 notification-deadline-owner-contracts checkpoint（2026-09-08）

notification-deadline-owner-contracts 更新证据后最终复验：**2887 passed,
12 deselected in8.59s**（首次扩展8.61s）；**198 whole files＋另37文件255 exact selectors**，
共453。compileall覆盖`conftest.py src tests scripts examples`通过；全部
pytest串行，未执行heavy/default/目录级/全部同版gate。

新通知预算7pure先 **5 failed/2 passed in0.20s**：localPENDING/foreign
PENDING/foreign可重试LOST在通知耗3ms后仍wait5ms而非2ms；耗10ms后
仍wait5ms。两READY/ERROR优先级case原本通过，非额外bug。修复3处在
group/inner scope进入后按原deadline重算：过期local只is_set许可重读
owner状态，不发新wait；foreign不再poll/额外owner查询。原Unblock
异常优先级/context恢复保留，不改前两LOST锁序。new7＋priorLOST4/
Coreblocking7/notifier9 **27 in0.29s**，只约束下次wait非总get墙钟
硬期限或已开始控制RPC取消。

新fixture≤2canonicalTask/2slot、1×4KiBstore、128Bslot，时钟/wait/
notifier均有限callback。borrower realAcquire真实owner legacyexporthold
再绑定credential，不冒serializedouter；foreignLOST为真stored发布/
drop、parentfinish而childbarrier保留，owner真回retryableNOT_LOST。
真实Release但不伪unfinishedTask/GC完成，runtime/socket/进程/Timer
禁令和每channel32cap持久违规检查保留。

Worker侧原3heavy→pure **3 in0.19s**：cloudpickle真实双Core cache/
独立lock及原local_plus_one调用(非Taskdispatch)；retain原PENDING/
activeparent下先fence-new、真replay/query/release、后真实quiescent
Core.shutdown/finalize；只model instanceCondition deadline，两call
三个真实predicate F/F/T，无真实wait。storedproxy原missing拒绝、
existingowner真provisional/finalpin/release，PENDINGmetadata保持，
不以GC清表换clean。后两case各3个真实typedemptyWorkerdeathsuffix，
不是GCS网络/Nodebootstrap/线程退出证据。尚3原Worker函数heavy。

| Exact bounded selector | 结果 | 受限范围 |
|---|---|---|
| `tests/unit/test_node_blocking_get_authority.py::test_concurrent_block_and_completion_linearize_without_leaking` | 1 in0.21s | 原2handler真thread竞争＋terminalACK；1KiB/13B保留pendingowner，不假adopt/GC |
| `tests/integration/test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get` | 1 in1.16s | 原5children7端点、2Task、两1MiB/2borrower/真实Release ACK |
| `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` | 1 in1.01s | 原4children6端点、2Task/1MiB，真Worker get/Block/Unblock/debt清零 |

三exact全审/分别批准/30srunner逐项。NodeL1 barrier3party≤1s、
normaljoin共2s/final共1s，只允许2ownedthreads，2replies/16errors/
1terminalACK留账；异常先存，failure只abort barrier+exactjoin，不
重入可能持有的Node锁。原6pure/所有共享helper正文保留。真single
Node progress收terminal，replyslots(0,1)/13B副本故意仍待owner，
不把资源释放当owneradoption/physicalGC。两个MP保原10swork/3s
引用cleanup及finallyPID/端点检查。Worker-owned所有get在Driver，
child可能已ready，因此不是通知/预算耗尽证据；CPUyield证明正常
路径不冒timeout注入。innerRPC/关闭预算仍原有，publictimeout非取消。

合并focused **134 in0.46s**；manifest198whole255exact/37other=453，
同12原selectedL1排除，新L1不入pure。K0K1/其余历史repair/全部
同版出口/宽矩阵/GCSfidelity仍开放，原同步publication/globalDAG/
phase-specific保证不改。
独立AST **238files1912fn、2887pure106heavy31L1**；allow **88MP41L1**
共129IDs跨93files。worker_side仍14exactpurefn/16展开＋3heavy1L1，
不升whole；Nodeblocking6exactpure＋单独L1。无pure遗漏/重复/overlap/
heavy误选，scope仍不认证未来fixture。

### 先前 lost-blocking-drain-contracts checkpoint（2026-09-08）

lost-blocking-drain-contracts 更新证据后最终复验：**2877 passed, 12 deselected
in 8.45s**（首次独立扩展运行也为8.45s）；**197 whole files＋另37文件252 exact selectors**，共449。
compileall覆盖`conftest.py src tests scripts examples`通过；全部pytest串行，
无heavy/default/目录级/全部同版出口执行。

先红后绿：`test_core_lost_blocking_lock_order.py`四个新pure在旧Core
**4 failed in0.40s**，分别观察own/descendant LOST时scope持Core锁、
scope-enter真实finish/republication后仍旧predicate等待、以及通知消耗
deadline后仍使用旧remaining。修复仅Core.get两LOST等待点：predicate
快检后离condition锁进入group/episode，再持锁重查，原deadline重算，
Condition.wait仍在锁内，Unblock在其外。new4＋原finishbarrier11
**15 passed in0.41s**；原零超时准入/无丢醒测试不改并通过。

纯回归单Core真实RLock/Condition、2Task/2publication最多、单4KiB/
每slot≤128B、真实stored发布+Core.drop_object→LOST；notifier是锁spy，
Condition.wait是单次sentinel，无真实线程/RPC/CPUyield。它证明锁边界
与二次复查，不声称OS死锁已被运行复现；跨线程环需显式绑定同context/
notifier，新thread不继承。普通PENDING/foreign期限及Unblock独立控制
RPC不在修复保证内。tripwire持久16flag、scope16、backend入场32、
admission入场2、receipt32，不能靠吞异常放行未知工作。

原Coreblocking6fn7case全部pure，**7 in0.19s**；readyput/readyerror/
timeout/error/nested/get_many单episode保原语义，成功从真实discovery/
Nodeadapter→Coreadoption/finish/GC，pending-only主断言后显式fixture
error。原_enqueue=False不伪accepted/lease执行；wait只有inertcallback/
is_set检查。Worker侧原3fn：childspec＋Driveronly3参数共4pure，
threadlocal保1真thread L1。纯首跑 **3 passed/1 failed in0.19s**：
新增cleanup错预期2WAKE，实际两Task各terminal+finish共4；改精确4
并逐item消费后 **4 in0.18s**，不清表/放宽事件类型，不改production。
其余6个Workerheavy函数未运行、未宣称完成。

| Exact bounded selector | 结果 | 边界 |
|---|---|---|
| `tests/unit/test_node_shutdown_drain.py::test_inflight_localization_blocks_drain_and_cannot_late_grant` | 1 in0.14s | 1真requestthread/emptydeps localizer/BeginDrain/no lategrant |
| `tests/unit/test_node_shutdown_drain.py::test_exact_cached_replays_are_counted_and_balance_after_begin_drain` | 1 in0.14s | 2真replaythreads/原perLease锁与cache/grant/realCancel释放 |
| `tests/unit/test_worker_side_core_contract.py::test_runtime_binding_isolates_worker_thread_and_restores_driver` | 1 in0.12s | 1真thread，两绑定隔离/恢复；无Core构造或集群 |
| `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` | 1 in1.02s | 原4children6端点/2tinyTasks/1MiB，PENDING parent-child CPUyield回归，非LOST死锁再现 |

四exact全审/分别批准/30srunner独立执行。Node两个L1的Event/count
各1s、normaljoin共享2s/finaljoin共享1s，失败先放gate再join准确已
启动线程，确认停止后才必要Cancel；errors16条留账。不启动Node/Core
运行时，不建Store/socket/进程，first原typedWorker-clean缓存是前提，不代证真drain；
emptydeps不冒真实pull/pendingcustody。bindingL1两个1sgate、2sjoin/
1sfinally、单异常转main，未伪并发调度。CPUyield保原10swork/3sclose/
真实两Worker，登记PID/端口在结构assert前，卫生检查在shutdown finally，
原账本/debt/单报告不变；内层RPC/锁/shutdown仍原预算非用户取消。

合并focused **113 in0.56s**，另Workerclassification **17 in0.04s**。
静态 **237files1906fn、2877pure110heavy30L1**；197whole252exact
/37other=449覆盖所有unit、12原L1排除，无重/漏/overlap/heavy。
allow **88MP40L1=128**跨92files，新3L1不入pure。guard及replacement
逐名/参数映射已核，这不是future安全证书/全部同版K0K1；剩余历史
runtime、宽故障矩阵与GCS fidelity仍开放，原publication/globalDAG/
phase-specific保证未改变。

### 先前 publication-trace-contracts checkpoint（2026-09-08）

publication-trace-contracts 更新证据后最终复验：**2862 passed, 12 deselected
in 8.93s**（首次扩展8.89s）；**195 whole files＋另37文件250 exact selectors**，共445。
compileall覆盖`conftest.py src tests scripts examples`通过；全部pytest串行，
未运行heavy/default/目录级/全部同版出口。

普通单Task成功黄金trace现在展开十个真实RPC往返：原lease/push/start/complete
之外，包含Prepare、Node INTENT/ARM、Core terminal/adopted和Node回复托管
退休。四阶段同handler必须分别匹配Task/Attempt/Lease/manifest及真实reply
后的业务ACK事件；不能用transport ok冒accepted，不能复用一条request满足
两个阶段。Prepare/Push/Complete还检查显式handler cause链，共享ARM anchor
冻结为同一事件。请求/回复端点分开，Node后台terminal不成为localComplete
或Core terminal的前提。旧task_finished/object_ready是尾部通知，不是首次
ownerREADY、逻辑finalizer完成或物理GC。

Core/Node新观察点仅best-effort metadata，无新业务RPC/权威表。真实ACK
放causal_scope后绑定收到的reply；ownerREADY在首次CAS/wake后锁外发，
NodeComplete在inner返回后锁外发。原GCS同步依赖/globalDAG/phase恢复不变。

synthetic matcher **61 in 0.25s**（首次扩展54 in0.28s）；新观察
pure **6 in0.25s**；matcher/observation/runner合并 **98 in0.36s**。
新观察6case复用真实Node factory/outerComplete和Core publication/GC；
两13B槽/1KiBstore，无用户函数/线程/进程/socket/wait。记录/RuntimeError/
自定义BaseException sink三种情况保持原ACK、ledger、no-retry、custody与
GC；只验证抛异常的sink，不证明任意阻塞sink的活性。
Core/ownerTable/Node/journal四锁在READY/retired观察时均未持有。
client reply事件由inert bridge明确模拟，不是网络验证。Core calls入场
32cap＋持久violation、probe errors32cap＋overflow，runtime tripwire
在fixture teardown复查，不能被观测异常处理吞为通过。

| Exact bounded selector | 结果 | 范围 |
|---|---|---|
| `tests/integration/test_cross_process_trace.py::test_one_task_emits_cross_process_golden_trace_and_cleans_up` | 1 passed in0.93s | final十RPC/四独立stage request；hardening前也单独1 in0.93s |
| `tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]` | 1 passed in0.93s | 原main真实输出含四GCS阶段、READY和custody退休 |
| `tests/integration/test_cross_process_trace.py::test_application_error_trace_is_terminal_without_system_retry` | 1 passed in0.94s | 原用户异常语义，无成功重试 |

三个exact各完整审查/分别批准/30srunner逐项：每次3children/5端点、
1tinyTask/1MiB，原10swork、min(work,now2s)trace、3sclose后shutdown。
PID/端口检查移到finally，失败也检查已捕获资源，保首次shutdown报告；
没有新增Task/fault/thread。其它6个example main未在此checkpoint重跑。

静态 **236files/1903fn、2862pure/124heavy/27L1**；195whole250exact
/37other=445，所有currentunit纳入、12原L1排除、无重复/overlap/heavy。
guard及历史replacement mappings未变；allow **88MP/37L1**，125IDs
跨90文件。这不认证未来fixture、全部同版gate或K0/K1完成；单Task成功
trace不是任意并发模型检查，GCS fidelity仍是待决设计取舍。

### 先前 sibling-spillback-lifecycle-contracts checkpoint（2026-09-08）

sibling-spillback-lifecycle-contracts 首次扩展复验：**2800 passed,
12 deselected in 8.86s**；**194 whole files＋另37文件250 exact selectors**，
共444 selectors。compileall覆盖`conftest.py src tests scripts examples`
通过。生产仅core/node/worker三个首docstring纠正当前职责，无可执行
算法/协议变更；全部pytest串行，未跑heavy/default/目录级gate。

multi-return最后4原函数现3pure共5展开＋1L1，pure **5 in 0.42s**。
TaskID生命周期键在真publication/finish后验证，不造count。producer
PENDING时先register3返回consumer建立SUBMITTED+LINEAGE；真producer
pub/finish后再consumer准备InlineArg/pub/finish，不换put或假永久
PENDING源。3原close_order保到lastsibling才单次releaseLineage，
再闭producer正常GC。storedfirst真store.pin→NodePINNED保plan/
lineage，inline2/1先GC也不得迁移Task责任，真unpin/同Drop成功才
收最后slot，晚交原retryevent不再RPC。1KiBstore/2Task4refs/
两pub，全pure不跑用户函数，最多12controlcalls含两Drop尝试。

| Exact bounded selector | 结果 | 有界范围 |
|---|---|---|
| `tests/unit/test_function_registry.py::test_concurrent_identical_registration_creates_exactly_once` | 1 passed in 0.26s | 2线程/原registry-RLock/7Bpayload/0GCS网络；Barrier1s/normaljoins2s/final1s |
| `tests/unit/test_spillback_runtime.py::test_concurrent_duplicate_spillback_uses_one_cached_snapshot` | 1 passed in 0.23s | 2真requestthreads/1未启动Node/2hints；snapshot1/policy1/cache1，无Core/store/RPC；wait1s/joins2s/final1s |
| `tests/unit/test_public_multi_return_runtime.py::test_concurrent_sibling_closes_claim_task_lineage_exactly_once` | 1 passed in 0.17s | 原3closer＋1refconsumer、2Task4refs/1KiB、全INLINE；close/barrier≤1s/closerjoins2s/failure2s |
| `tests/integration/test_nested_large_argument_path.py::test_nested_large_argument_pulls_without_gating_on_pending_handle_and_collects` | 1 passed in 1.32s | 原2Task/5children7端点/两1MiB/64KiBlift/0fault；step5s/work10s/Worker gate10s，final min(work,now3s) |

四exact全审、分别批准、30srunner逐项。registry保原真正锁/
并发调用，不称特定C-level acquire竞争或GCS服务hotpath。spillback
先真实NodeRegistry注册/Installsnapshot，first真实copy后暂停，
两个handler都entered/inflight2/cache空、原lease/schedulinglock
held才放行；真Hybrid一次/twoequalreply，changedrequest仍STALE
无再调policy。只有hint无remoteallocation，finally不重入Node锁。

siblingL1保真实3close→真实FIFO/refthread(并非3GC并行)，同
canonical2Task和3consumer元数据，lastsibling只在refthread真
releaseLineage一次，再producerclose/GC。callbackErrors16/
GCobservations32cap且main检查，避免finally/continue吞断言。
正常停refthread后FIFO空/unfinished0/timers关闭/runtimefinalizer
detached全部真查，failure只独立mailbox/ownedthreads不clear
authority或卡住时重入ownerlock。public文件仍24pure各exact＋1L1。

nestedlarge保原64KiB StoredArg＋同pendingref两occurrences，
target只拉hiddencontainer、不等nested值；sourceclose-beforePush/
actualoneAcquire TaskHoldSource/用户get仍PENDING后才放producer。
locality仍sourcebytes首跳、targetresource强制spillback，原两hop
并补完整Lease/Task/Attempt/requester/preferred身份不放宽。
三object真正COLLECTED/metadata-lineage消失、hidden两副本原
两typedGetObject(absent)全保；probe不加Task/故障/线程/RPC。
step/work/Worker gate5/10/10不扩，sourceearlyclose≤3纳work；
finalgates/refs/GC/两个typedread共min(work,now3)，finally复用
不续期。socketsetup入try/五PID七endpoint早记录，case-owned
patch早undo；32observerrecords及flags不造用户taskerror，元数据
遍历2048nodes/32depth预算不截断成假bytefree。内部原timeout/
shutdown不是publicdeadline取消，30souter只作实验兜底。

5pure＋legacy/runner/collection/mode **105 in 0.54s**，另registry
classification/mode **56 in 0.09s**。静态235files、2800pure/
124heavy/27L1；194whole250exact/37other=444，无漏/重复/overlap/
heavy，currentunit全纳，12原selectedL1精确排除。legacy188/0/6、
placement151/9/4、reference93/36/4；allow **88MP/37L1**。public
24pure1L1仍14exact，spillback11pure1L1仍11exact，registry7pure
1L1仍7exact；无whole升格或newL1混入。完整K0/K1/余历史repair/
同版出口/宽矩阵/GCSfidelity仍开放，不以标签/count替future安全，
原publication/globalDAG/phase保证不变。

### 先前 replay-foreign-lifecycle-contracts checkpoint（2026-09-08）

replay-foreign-lifecycle-contracts 更新证据后最终复验：**2795 passed, 12 deselected
in 8.53s**（首次8.81s）；**194 whole files＋另37文件247 exact selectors**，共441 selectors。
compileall覆盖`conftest.py src tests scripts examples`通过。生产未改，
全部pytest串行，未跑heavy/default/目录级或整套同版门禁。

3原multi-return函数保5fields/3modes/3status共11参数，**11 in
0.41s**。storeddrift仍2真实STORED/只index1变，wire重验与独立
Coredecoder/canonical预检分层；invalid3slot保真Completeenvelope，
非missing-envelope假拒绝，allPENDING/noWake/不变性与合法后续
adopt/GC保留。successpreflight原escapingRuntimeError已obsolete，
实际False+同custody/DelayedReady adoption续行；GCS可增terminal
但owner/TaskRecovery/Nodebytes不变，真_execute续行无新lease/
Complete/userfn，8actualcall含重复terminal全部记录。另两error保
原error对象、无成功publication后真finish/GC。每case1KiBstore、
两stored总≤256B或3slot总≤512B、0thread/userfn/网络。

foreigndeath原ID保两owner、2真实put/Acquire/canonicalforeignlineage/
actualpull grant。第1owner真实ADDED保receipt，第2ownereffect后
ACKloss；真registrationdeath→Core._sync suffix后先exactCancel再
healthyowner ALREADY_RECORDED，inventoryACK→OwnerDied。healthy
retained不在finish就释放，outputGC后才Release，再原sourcehandle
GC两副本。另一旧`test_typed_stale_waits_for_exact_grant_cancellation_before_hold_release`
正式替换为`test_noncustody_stale_is_quarantined_until_authoritative_owner_death`：
明确typedSTALE是协议fault，不给put造重建epoch；真实Cancel首
effectACKloss/secondreplay只撤执行，noncustody仍quarantine/no
inventoryACK/noholdrelease。之后独立实际owner死亡日志才唤醒并
授权handoff，保最先STALEerror，不称Cancel自己允许终结。

两例最后用真实GCS冻结的两OWNER_WIDE_SWEEP effects→真Node
handler删除deadowner source/targetbytes，不能fakeACK/清表。
仅GCS TCP构造边界是inert（真实ctor即bind socket，不start不等
于pure），真实registry/handler/outbox保。deadimage主assert阶段
不变，finally只闭testlocalref/留retained+未驱GC，不冒正常dead
owner回收。2/3pureCore+两1KiBNode、1consumer1grant、3/6transfer、
最多3actualreport/2Cancel/2fence/healthy2Drop，全部同步无userfn。

death首跑 **1 failed in 0.32s**：测试错要求source transfer历史
dict为空，实际released/closed墓碑必须保留。修为真实ReleasePin
ACK ledger与source session/closedrequest逐项匹配、targetactive
outbox无pending，仍actualbytes0/GC不削；后 **1 in 0.28s**，
stale **1 in 0.15s**。无生产修补。此前guard读到并发编辑中marker
先变、mapping未更新，**54 passed/1 failed in 0.47s**，未执行
foreign新body；最终13focused＋legacy/runner **57 in 0.50s**。

| Exact process selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects` | 1 passed in 1.05s | 4children/5端点、1Node1CPU2Workers/1MiB、2Task(parent0CPU)、64KiBpadding/0fault；原10s/单3s交集 |
| `tests/integration/test_contained_ref_lifecycle_path.py::test_two_borrowers_outlive_their_inline_container` | 1 passed in 1.04s | 同4children5端点/1MiB、原Node2CPU2Workers/2Task/2tokens/0fault；15swork与final3s |

两exact全审/分别批准/30srunner逐项，原函数/调用数不裁、不加
probeTask。storedouter保OwnedContainedSource、GCSmetadata与
Node退休后只有witness、graph ALREADY_COMMITTED、physicalSHA；
childpublicclose后真borrowobligation空/noownerdeath，outerGC后
原graph/childtombstone/physicalabsence/GCSslotproof全部保留。
逆向2close/GC/replay共min(原10swork,now+3)，finally不续期；
metadata全遍历受1024values/depth24约束不截尾。旧共享_close_reference
不改，新case才publicclose；提前保护borrower/ownerendpoint。

twoborrower保原2outerdeserialize/3childget与两foreigntoken，§2
twoCPU exact例外明确；outerCOLLECTED+actualcontainedRelease在
childget前，firstborrower匹配ReleaseACK/key清空后second仍能get，
最后secondclose及Release义务/finally共single3s。earlyclose各≤3s
且同15swork，16records只completedCore._borrow_rpc不是底层网络
重试次数；internal原3attempt不可由publicdeadline取消。两test均
不拿foreignchildholdRelease墓碑/clean shutdown冒foreignchildmetadata
COLLECTED。失败finally仍核PID/端点，原resource/exit报告保留。

独立AST235files、2795pure/132heavy/24L1；194whole247exact/37other
=441，无漏/重复/overlap/heavy混入，currentunit全纳/12原selectedL1
exclude准确。legacy183/7/4、placement151/9/4、reference93/36/4，
allowlist **88MP/34L1**，122exact映射核对。foreign19pure仍19exact
不升whole，public19pure6heavy只11exact。纯增量11原展开＋1death
迁移＋1正式stale替换，不认证futurefixture/fullK0K1；余历史修复/
同版出口/宽矩阵/GCSfidelity仍开放，原发布/globalDAG/phase保证不变。

### 先前 admission-reference-concurrency-contracts checkpoint（2026-09-07）

admission-reference-concurrency-contracts 更新证据后最终复验：**2782 passed,
12 deselected in 8.36s**（首次8.41s）；**194 whole files＋另37文件242 exact selectors**，
共436 selectors。compileall覆盖`conftest.py src tests scripts examples`
通过。生产source未改，全部pytest串行，未跑heavy/default/目录级gate。

4原public提交/retry case迁pure，**4 in 0.24s**：真API分别返回
1/3ObjectRef、canonical queue/count/barrier；3slot真实owner/recovery
CAS只advance一次，oldfinish/retry不改successor；原ownerpreflight
成功后注入同RuntimeError，完整owner/recovery/queue/预算不变。
关键断言后对未leaseTask施加明确fixture-local终态error，再真
finish/refrelease/GC；非Worker异常/执行成功/publicshutdown，失败
finally不造terminal或清义务。

旧`test_pending_or_stored_zero_reference_metadata_is_not_inline_collected`
正式替换为`test_pending_closed_output_waits_for_finish_then_stored_publication_is_collected`，
**1 in 0.27s**。zero-localroot实际close，PENDING→realcanonical
STORED publication仍因finishbarrier不能GC，realfinish之后才
NodeDrop/typedACK/slotproof/owner-lineageGC。无childgraph、1KiB
store/≤128B结果/0thread；不说旧STORED排除断言通过。5pure＋
legacy/runner/mode **80 in 0.36s**。

| Exact L1 selector | 结果 | 有界范围 |
|---|---|---|
| `tests/unit/test_worker_export_pin_rollback.py::test_worker_drain_stays_unclean_while_export_release_cannot_converge` | 1 passed in 0.21s | 3真实Core线程、1tinyput/exportpin、0Task/网络/Timer；wait≤1s/failure共2s |
| `tests/unit/test_contained_edge_runtime.py::test_publish_installs_edge_before_wake_and_same_owner_release_does_not_deadlock` | 1 passed in 0.27s | 1真reference线程+main、同Core/RLock、1put1不执行Task、1KiB空store；close≤.5s/join≤1s |
| `tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_lost_requests_merge_and_old_attempt_is_fenced` | 1 passed in 0.20s | 2真requestthreads/3内部Corecalls、1STORED/1KiBstore、1Drop/1准入；wait≤1s/joins2s＋finally1s |
| `tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_multi_return_sibling_requests_start_once_and_join` | 1 passed in 0.19s | 同界限，原3siblings/总<128B/3内存Drop/1准入；非3次执行或集群故障 |

四exact全审/独立批准/30srunner串行，无livecluster/socket。Worker
drain保原ID但改真实Core构造与3thread，避免残缺fakeCore异常被
Worker吞为False。真实put/genericpin在request/firstdrain两次
Release失败后保活；先close本地ref、secondrealdrain成功Release/
tombstone/GC，原savedround1交真正refFIFO后claimFalse无新release。
preserve drain仍owner协议/refthreadlive、worker未伪stop，最终
Core.finalize才stop/FIFO0/sinkonce。不coverTimerrace/Workerpublish/
ray.shutdown；审批首次过期未启动，许可重试才运行一次。

sameowner实际单Core/mainpublish＋1refconsumer，canonicalnested
child/lineage/edge-before-wake，callbackthread外Core锁ownership
调用同Core Release，outerGC才lineage退场并childGC。first **1 in
0.16s** 后静审发现GCloop会吞callback上界assert，测试已改cap
并留存callbackerrors/violations供main核，最终 **1 in 0.27s**
包括此观测修正；非生产bug修复。finally只独立mailbox.stop＋准确
threadjoin不重入潜在Core锁，不用clear伪造GC/任意调度无死锁。

并发两case原LOST fixture和raw旧reply已换成真实Node注册/Grant/
Start/discovery/Prepare/Complete/Coreadopt/finish保存的成功envelope。
main仅真实drop内存bytes，保LOSTmembership；first在原retirement
ticket的NodeDrop边界暂停且不持Core/Node/journal锁，second真None
defer；main放行后first退休全部1/3slot、真enqueue，second仅再
一次内部Core调用得真JOIN，twoThreads3calls/no全局testmutex。
实际DROPPED与threadedALREADY_DROPPED身份/线程/顺序、GCSslot
proof都检查；原合法oldEnvelope必须false且successorowner/Recovery/
session/queue不变。主assert后显式未执行attempt终结/真实GC，
finally只有close/purefence；活thread则不重入state锁且必fail。
上限32bridgecalls/8receipts/8event/8violations，回调失败不被
best-effort吞掉变假绿。不是publicget/Worker执行、未把retirement
挪主线程以便测试。3内存drop前置例外见§2，非压力/扩故障授权。

静态235files、2782pure/145heavy/24L1；194whole242exact/37other
=436，全currentunit纳入、无heavy/漏/重复/overlap，selected
非unit仍12原L1exactexclude。contained/Workerexport各6pureexact
和1L1未升whole；legacy170/20/4、placement151/9/4、reference93/36/4，
allowlists **88MP/34L1**。纯增量4原迁移＋1正式替换，另两原case
heavy→L1。完整K0/K1/余历史repair/同版出口/宽矩阵/GCSfidelity仍
开放，所有原发布/globalDAG/phase保证保留，不以标签/计数认证未来安全。

### 先前 mixed-contained-terminal-contracts checkpoint（2026-09-07）

mixed-contained-terminal-contracts 更新证据后最终复验：**2777 passed, 12 deselected
in 8.21s**（首次8.28s）；**194 whole files＋另37文件237 exact selectors**，共431 selectors。
compileall覆盖`conftest.py src tests scripts examples`通过；生产source未改，
全部pytest串行，不是完整default gate或同版all-ID验收。

原三槽mixed success/application error迁pure，**2 in 0.24s**。真
canonical注册→Node Grant/Start，success discovery真实64B阈值生成
INLINE/STORED/INLINE→Prepare/Complete→Corebatch adoption；中值
serializer仅把22编码变长，decode仍11/22/33，store1KiB/总payload
≤512B。每个realwake前核全3owner/result/route/Recovery已提交，
error真failedComplete，无discovery/envelope，全3同一TaskError。
真finish/duplicateComplete不再次release、3slotGC/1storedDrop及
最后lineage收尾；每FIFO≤16、successRPC≤7/error0，无Worker/用户
Task执行。两原ID保留，其余mixed旧body不运行。

Worker旧拒绝合同正式替换，**2 in 0.23s**，不称旧断言通过：

- `test_multi_return_contained_ref_rejects_before_any_seal_and_unpins_all` →
  `test_multi_return_contained_refs_use_one_unified_publication_and_slot_scoped_gc`。
- `test_target_single_slot_cannot_bypass_multi_return_contained_ref_rule` →
  `test_target_single_contained_slot_keeps_full_identity_without_publishing_other_slot`。

真Worker handler调用受控函数一次/cache replay，discovery前无pin/
graph/store effects；Node journal/promotion/seal/Complete与ownerCAS/
selectedGC为实际authority。Start ACK/初始CPU allocation为typed
fixture边界，非Node租约准入或CoreE2E；原backend128KiB容量/实际
≤2KiB输出/≤2childput。target保full2/selected0，未选slot1保持
PENDING只release测试token，不造历史成功、不声称重建/healthyREADY/
全owner清理。现Worker禁入genericexport，原4genericpure及唯一
live-drain heavy保留不变。4exact＋legacy/runner **48 in 0.35s**。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | 1 passed in 0.93s | 3children/4端点/1CPU/1MiB、1put1Task2attempt/8KiB、单storedslotdrop/target重建；15s/3s |
| `tests/integration/test_multi_output_node_loss_path.py::test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored` | 1 passed in 1.21s | 5children/6端点/两Node各CPU1/两1MiB、同1put1Task2attempt、单publishercrash；15s/3s |

两exact完整审查/独立批准/30srunner逐项。mixed保sharedDriverchild
两slot独立finalhold，≤8realPush核full2→selected1，真实owner旧1
hold退休/新1建立/healthy0完整snapshot不变；get按原顺序拆开以
及时保存3restoredchild，6refs和三阶段GC共final3s。新helper不改
其它约20文件导入的原_close_local/_wait/_pid_exists/_TIMEOUT；
debugDrop使用min(workdeadline,now+原4sRPC,enclosing)，未扩大
原transportpolicy。最初审批超时且未启动，获准单次重试后执行
成功，不记录为测试失败或两次运行。

Node-loss仍在第一个graphCOMMIT前、realTerminalACK后crash，
先留真实envelope/PENDINGowner/两holds快照、释放全部锁，再用
header核exactpublisher并调用原managedprocessgroup故障。actual
controls≤64/Push≤8，NodeLoss解析兼容query已带resolution，真
KEEP0/DROP1保knownComplete/SUCCEEDED@0/noRetry；旧storedhold
真实Release墓碑，公开get才targetSTART@1/survivor，full2selected1/
新hold和healthy0不变。5handles/两outer及source真实GC/PID观察
共single3s，predicate每次≤256；WorkerembeddedCore不增OS进程，
Driverowner路由身份未接管。仅观测两个physicalattempt，不凭
RPC去重冒称独立用户调用计数器。

两case的15s是工作等待预算，不取消同步recoveryRPC；shutdown
另有原协议预算和30souter兜底。failurefinally逐ref尝试/必shutdown/
准确PID端点核验，三阶段GC与ledgerclean只成功主体证明；crash
report故意victimunclean，只要求survivor资源/Finalize/ACK/exit0。

独立AST235files、2777pure/152heavy/22L1；194whole237exact/37other
=431，无漏/重复/overlap/heavy混入，全currentunit纳入，selected
非unit仍12原L1 exclusions。legacy165/27/2、placement151/9/4、
reference93/36/4；allowlists **88MP/32L1**。纯增量2原迁移＋2正式
合同替换，非全defaultgate/future安全证书。K0/K1、剩余历史修复、
同版出口/宽故障矩阵/GCSfidelity仍开放，发布/globalDAG/phase保证不变。

### 先前 multi-return-lifecycle-contracts checkpoint（2026-09-07）

multi-return-lifecycle-contracts 更新证据后最终复验：**2773 passed, 12 deselected
in 8.32s**（首次8.30s）；**194 whole files＋另37文件233 exact selectors**，共427 selectors。
compileall覆盖`conftest.py src tests scripts examples`通过；生产source未改，
所有pytest串行，不是完整default gate或同版all-ID验收。

旧`test_stale_reply_edges_are_released_as_orphan_obligations`正式替换为
`test_stale_reply_cannot_release_committed_publication_edges`，不是旧
raw orphan-release断言“迁移通过”。realcanonical/selectedoutput完成
adoption和finish后仍有outer localref，原envelope及deepcopy重复投递
必须不改owner/childholds/lineage/graph/Node-GCS快照/RPC history。
lastref真close才单Release→child/graph/slot/outerGC，GC后重复无
复活；是同attempt postfinish duplicate，不priorattempt或unadopted
清理。旧→新映射已写guard，真正orphan清理仍需Node/GCS authority。

两原generic export case保真实put/ReferenceExportSession pin及义务：
shutdown同步GCprecheck失败保pin，恢复后真Release/tombstone使旧
queuedround失效；另一round1驱动失败→round2，旧claim不得破坏
新round，恢复后真release再normalchildGC。原events经真实mailbox
FIFO/get_nowait/task_done，最多两event/三release尝试，不用finally
clear掩盖未收敛。仅generic兼容原语/同步precheck，不publicshutdown、
Timer竞争或currentWorker publication。3pure **3 in 0.32s**；加legacy
和reviewedrunner **47 in 0.32s**。两mixed仍exact选择，不扩whole。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_multi_return_path.py::test_public_multi_return_mixed_outputs_retry_dependencies_and_gc` | 1 passed in 0.97s | 3children/4端点/1CPU/1MiB、3Task/4attempt、单predecodeSYSTEM_ERROR、32KiB字段；15s/3s |
| `tests/integration/test_multi_return_reconstruction_path.py::test_multi_return_all_outputs_lost_reconstructs_once_from_nonzero_sibling` | 1 passed in 0.92s | 同拓扑/store、1Task2invocations、两个小于32KiB结果、2显式drop/1whole重建；15s/3s |

两个exact完整审阅、独立批准、30srunner逐项执行。mixed保原用户
producer只运行一次、两distinctconsumer；≤8被动真实PushReply核
producer0SYSTEM_ERROR→1fullmixedEnvelope、同Task/Object新lease、
真实InlineArg/byte-free RefArg+attempt1descriptor。不是lineage
reconstruction/targeted，也不称已观察中间半发布。4refs及时纳入
finally，3段close/真实COLLECTED共单3s，末次wait后仍真predicate
复查，不能以transientQueue空或后续shutdown当GC证据。

whole保两次用户执行计数b"XX"，两siblings先全LOST@0才由slot1
publicget重建，START真commit后注入slot0request取得真JOIN，
再仅一次Coreenqueue；不冒充并发publicget。旧observer.request
已不覆盖START，现观察真实commit_prepared，嵌套JOIN只记一次，
最多8记录；stableObjectIDs/attempt1/retry1及先收一slot保Task
最后收另一slot删lineage均真实验证。两个显式drop是§2限exact
复合例外，非单fault或全程DropRPC计数；retirement/最后GC另有
调用。get/drop/finish观察共15s等待预算，不取消同步reconstruction
RPC；close/GC/finally共单3s。失败仍shutdown/PID/端点检查，正常
主体成功才额外宣称ledgerclean，不泛化失败分支。

独立AST库存235files、2773pure/156heavy/22L1；194whole233exact/
37other=427，全currentunit选中、无heavy/重复/overlap，selected
非unit恰12原L1 exclusions。legacy161/31/2、placement151/9/4、
reference93/36/4；allowlists **88MP/32L1**。纯增量为2迁移＋1合同
正式替换。计数不是futurefixture安全证书，完整K0/K1/余历史
repair/同版出口/宽矩阵/GCSfidelity仍开放，原发布/DAG/phase保证不变。

### 先前 replica-retirement-contracts checkpoint（2026-09-07）

replica-retirement-contracts 更新证据后最终复验：**2770 passed, 12 deselected in
8.13s**（首次8.23s）；**194 whole files＋另37文件230 exact selectors**，共424 selectors。
compileall覆盖`conftest.py src tests scripts examples`通过。生产未改；
全部pytest串行，不是完整default gate/同版all-ID验收。

原contained shutdown-GC-precheck迁pure，复用真实canonical selected
output：localref保到publish/finish后close，firstRelease pre-effect
失败保原plan/childhold/graph/lineage；真实_retry_gc_obligations_for_shutdown
一轮新增1Release(total2)→child/graph/slot/outerGC，晚交原event无
新RPC/history变化。只cleanup reducer，不publicshutdown。新+两原
edge **3 in 0.24s**；余3live/rawedge仍heavy。

原failed-unpin迁pure，**1 in 0.15s**，只验证Core genericexport
兼容原语：真实put/ReferenceExportSession pin，pre-effect失败保
round1义务，原mailbox event手动claim/drive→真tombstone→childGC。
不把currentWorker discovery接回旧Coreexport，不当作Node journal
rollback证据；旧multi-return拒绝假设已obsolete，仍heavy不运行。
4focused＋legacy **17 in 0.29s**，runner **31 in 0.08s**。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_stored_physical_gc_path.py::test_foreign_stored_dependency_collects_source_and_target_replicas` | 1 passed in 1.36s | 5children/8端点(含gate/trace/owner)、两1MiB、4Task/64KiB字段、无故障；15s/3s |
| `tests/integration/test_multi_return_partial_reconstruction_path.py::test_one_lost_return_reconstructs_without_changing_healthy_siblings` | 1 passed in 0.85s | 3children/4端点/1CPU/1MiB、1Task3returns2invocations、单index1drop/重建；15s/3s |

两exact全审、单独授权、30秒runner执行。physicalGC保原owner-local
probe直接检查COLLECTED/metadata/descriptor/waiter/obligation/lineage，
trace同collectionID两实际DropACK后才completed、两Node真实absence
再exactALREADY_DROPPED重放。observer≤128、ownerprobe≤512次且≤5s、
trace≤201且≤2s均纳入原15swork，trace不作为删除authority。
finalgate/ref共3s，failure必shutdown并检查PID全部端点，保原4Task。

partial保用户函数两次都算全部3个值，只selected1重发布；真实
full/selectedEnvelope observer≤8，基准前和重建后finish观察≤128
各纳15s；healthy0/2完整snapshot/descriptor/attempt0不变，target1
attempt1/retry1且originalmanifest完整。localDrop只缩原RPCdeadline
并restoreContextVar，不改协议；最后3refsclose+真COLLECTED/lineage
忘Task共连续3s，finally不重置，不能由shutdown修复当GC证据。
无额外Task/fault/线程/外网/压力。

静态235files、2770pure/159heavy/22L1；manifest194whole230exact/
37other=424，currentunit全纳，无heavy/duplicate/overlap，selected
L1恰12精确排除。legacy158/34/2 placement151/9/4 reference93/36/4；
allowlists **88MP/32L1**。两mixed仍exact选择，纯增量2原case。
完整K0/K1、同版exits/余历史repair/宽矩阵/GCSfidelity仍开放，原
发布/globalDAG/phase合同未改，未运行heavy/未知成本用例。

### 先前 nested-shutdown-contracts checkpoint（2026-09-07）

nested-shutdown-contracts 更新证据后最终复验：**2768 passed, 12 deselected in
7.60s**（首次7.71s）；**194 whole files＋另37文件228 exact selectors**，共422 selectors。
compileall覆盖`conftest.py src tests scripts examples`通过。生产仅
ObjectRef注释纠正：直接构造/无exporter pickle是detached，真正
exported restore已有owner ACK后borrower绑定，不再说是未来功能。
无可执行runtime变化；全pytest串行，不是完整default/all-ID gate。

原pendingclose迁pure，复用selected-output fixture的zero-failure模式，
默认及原failedrelease body不变：真finalizer close仍PENDING/noedge，
NodePrepare/Complete→owner安装edge-before-real-wake→finish前不能
GC→realfinish→单Release/childGC/graph/slot/outerGC。两个原edge
case **2 in 0.19s**，加legacy **15 in 0.25s**。2pureCore/1Node/
1KiB空store/1put1Task1edge/≤4KiBINLINE，无线程/timer/user函数，
原其它4heavy不运行。

原borrowed两shutdown保**真实Core构造及三个线程**转L1，不用pure
precheck代替。owner侧原真实child/outer/lineage保持threadless，
transport只调用真owner，原retry事件保留不创建Timer。首例close
pre-effect失败，第一真shutdownFalse保token/义务/refthread；
第二真shutdown收到Release停止全线程，再投旧event无新RPC。
另例open handle由shutdown主动Release，lateclose不重发。正常
join≤1s，failurecleanup共享2s；internalwait/锁非取消，仍外层30s。
不宣称测试了网络/timer竞争或多进程ray.shutdown。pure bodies不改。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/unit/test_borrowed_object_refs.py::test_shutdown_retries_unresolved_borrowed_release_before_clean` | 1 passed in 0.21s | 3真实Core线程/0liveTask/0网络/0子进程，纯ownerfixture1KiB；exactRelease最多3次 |
| `tests/unit/test_borrowed_object_refs.py::test_shutdown_releases_live_borrowed_handle` | 1 passed in 0.13s | 同界限，一次真Release，ownerGC正常闭合 |
| `tests/integration/test_local_nested_reconstruction_path.py::test_local_nested_handle_survives_single_return_reconstruction` | 1 passed in 0.89s | 3children/4端点/1MiB，1put1Task2exec/1drop1recon；Driver15s/最后closeGC3s |
| `tests/integration/test_nested_task_argument_path.py::test_nested_argument_survives_sender_close_before_worker_push` | 1 passed in 1.00s | 4children/6端点/1MiB，1Node2CPU2Workers/3Task0fault，15s/四refs-gate共3s |

四exact全审且单独授权，30秒runner逐项。localnested先 **1 failed in
0.92s**：threshold1导致container变StoredArg，正确DFS出现额外
READY_SKIP put；改1KiB阈值保实际INLINE容器、source/result2KiB
padding保STORED，原graph只有result/nested非依赖断言不削。
保原8条最多被动Acquire/每观察≤256，实际hold origin0/1与最终GC；
早期sourceclose纳入work，只有最终close/GC共连续3s。
nestedargument先 **1 failed in 1.02s**：安全编辑把不可pickle的
Struct捕获进remote函数；改回struct.pack，不改runtime。原两
blocker全到齐→submitconsumer→sourceclose→再G/Workerimport
证明不变；增加实际单nestedmanifest/hold/lineage/sourcebytes
及执行前无borrower。源是READYput，不冒称pending-input test。
twoCPU例外限这个exact；failed路径也核全部记录PID/端点。

runner/mode/legacy **75 in 0.17s**；静态235files、2768pure/
161heavy/22L1，全unit选中、无heavy/重复/overlap，selectedL1仍
12exactexclude，新两L1完全不在pure范围。legacy guard扩展显式
third L1 count不丢原ID/参数：156/36/2；placement151/9/4、
reference93/36/4。allowlists **88MP/32L1**。计数不认证futurefixture。
完整K0/K1、同版exit/宽矩阵/历史repair/GCS fidelity仍开放，原
发布/globalcontainedDAG/phase恢复保证未改。未运行heavy/未知测试。

### 先前 foreign-lifetime-contracts checkpoint（2026-09-07）

foreign-lifetime-contracts 更新证据后最终复验：**2767 passed, 12 deselected in
7.66s**（首次7.74s）；**194 whole files＋另37文件227 exact selectors**，共421 selectors。
compileall覆盖`conftest.py src tests scripts examples`通过。生产source
未改；全部pytest串行，不是default完整gate或同版本all-ID验收。

原borrower三case迁pure，**3 in 0.21s**：

- before-effect Release outage保真正owner token/snapshot；close只
  留本地intent，真实shutdown-GC precheck后才首次Release ACK，
  原scheduled event晚交付不新增RPC，不称publicshutdown完成。
- owner不可达用真实Core错误转换/有限重试，Get/Wait/Drop/submit
  都无owner新effect；失效Retain的rollback仍须真Release-before-Retain
  tombstone ACK，不造死亡；恢复后原borrower可用并normalGC。
- owner关闭新准入仍允许既有token Get/Release，最后metadata
  finalization预检查前完成child/outer/lineage收尾。

旧delivery默认/此前case body不改，两个actualshutdown仍heavy。
原contained failed-release迁currentselectedoutput，**1 in 0.20s**：
两pureCore/1Node/1KiB空store，1Task/1put/1edge，Discovery阈4KiB
单INLINE。保outer在publish前close、首次Release pre-effect RuntimeError；
真实finish后GC冻结同ID/manifest/edge/childhold/lineage/graph，
手动交付原唯一retry→真childRelease/GC→graph/slotproof→outerGC。
不再用已废rawTaskReply edge或伪released=False当成功；其余5heavy
原body不运行。七borrow函数＋此case＋legacyguard **26 in 0.38s**；
runner **31 in 0.07s**。纯增量仅4原case，两个mixed文件都不升whole。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_foreign_reconstruction_path.py::test_driver_reconstructs_worker_owned_stored_object_through_owner` | 1 passed in 1.13s | 5children/6端点、两1MiB、3Task/4exec、原owner-local drop/1reconstruction、64KiBpayload；15swork/3sclose |
| `tests/integration/test_foreign_wait_drop_path.py::test_foreign_wait_drop_replay_then_owner_reconstruction` | 1 passed in 1.09s | 同拓扑/store，2Task/3exec、1physicaldrop+1ACKloss/1recon；256records、64LOST/physicalGCpolls |
| `tests/integration/test_foreign_input_lineage_reconstruction_path.py::test_foreign_input_hold_replaced_before_consumer_reconstruction` | 1 passed in 1.03s | 4children/5端点、1Node2CPU2Workers/1MiB、3Task4exec/1drop1recon；32records/256statechecks |

三exact均root全审、单独明确批准、30秒runner逐项执行。所有reference
finiteclose与failurefinally无条件shutdown、PID/owner/端点清理加强；
不是只成功末尾检查。foreignrecon仅owner-local Node drop设置/恢复
既有deadline，observer≤64每list且不抛/assert改变原reply。

wait/drop不改transport kwargs或三次重试：15s只publicget/wait/
有限观察预算，raw owner RPC保自身有限重试/outer30。原单次真实
DROPPED后丢ACK、第二同请求/缓存回执、oneSTARTED@1、wait不fetch/
onepublicfetch不变。LOSTprobe允许已知finishbarrier临时PENDING，
只等真LOST0不驱动重建。恢复后raw Node检查matching@1 bytes，
close后真absent error/无metadata，区分epoch冲突/corrupt。实际
Driverouter COLLECTED与borrowerRelease空另外检查，不以此推断
foreignowner metadata已GC。probe是额外只读fetch，不混public计数。

foreigninput保submit consumer后、首次get前关闭原foreign/outer，
纠正文案而不改变流程；真finish/原retained0→dropLOST0→真实Replace
hold1→SUCCEEDED1/retry1/原producer spec。最后3refs/真localownerGC/
foreignlineagerelease共3s，sourceowner仍存活；不称直接观测了远端
source metadata。get/drop/work15s，内部renewalRPC原有限重试不被
ContextVar取消，外层30s。twoCPU与drop+ACKloss的限exact例外见§2。

静态235files、2767pure/164heavy/20L1，currentunit全纳、selected
非unit恰12L1精确排除，无heavy/overlap/重复。legacy155/39、
placement151/9/4、reference93/36/4；allowlists **88MP/30L1**。
计数/marker不认证futurefixture；完整K0/K1、剩余runtime迁移/宽矩阵/
同版exit及GCS fidelity取舍仍开放，现发布/DAG/phase合同不改。

### 先前 release-concurrency-contracts checkpoint（2026-09-07）

release-concurrency-contracts 更新证据后最终复验：**2763 passed, 12 deselected in
7.67s**（首次7.80s）；**194 whole files＋另37文件223 exact selectors**，共417 selectors。
生产source未改，不是完整default gate/同版本all-ID验收；所有pytest串行。
`compileall -q conftest.py src tests scripts examples`通过。

两个原borrower函数7展开迁pure：复用真实child put/INLINE outer发布，
owner第一次Release已生效但ACK丢失或被原六字段变异。borrower保持
原义务/round1调度事件，以容量2FIFO手动交付原event→真实墓碑ACK
accepted=True/released=False→正常GC，不伪release/no-resurrection或
模拟定时器。failed-restore先丢真实Acquire再丢真实Release ACK，
原OwnerDiedError异常不被当作死亡事实。**7 in 0.24s**，原参数/ID保；
该文件剩5live case仍heavy，不升whole。

原late-outbound迁pure，**1 in 0.14s**：实际Node/Worker registry
register→typeddeath→deaths_after→Core observer装owner fence/cursor，
五类late入口在RPC/新义务前拒绝/no-op。Task/Object/hold只是合法
请求metadata，不注册假外部值/未accepted Task；无Ref/store/GC
等待。实际1wake＋empty suffix replay均明确消费，无真OS死亡声明。
worker-death整文件＋四borrow函数＋legacy **41 in 0.28s**；runner
**31 in 0.09s**。两个文件guard/manifest对应8个原case迁移。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_two_worker_pool_path.py::test_one_node_two_workers_execute_two_tasks_concurrently` | 1 passed in 0.94s | 4children、1Node/2CPU/2Workers、1MiB/6端点、2Task无故障；10s work/3s finally |
| `tests/integration/test_parallel_task_lanes.py::test_two_nodes_execute_resource_pinned_tasks_concurrently` | 1 passed in 1.05s | 5children、两Node各1CPU/Worker、两1MiB/7端点、2Task无故障；15s/3s |
| `tests/integration/test_recursive_lineage_reconstruction_path.py::test_recursive_lineage_reconstructs_leaf_to_root` | 1 passed in 1.01s | 4children、1Node/2CPU/2Workers、1MiB/5端点；原3drop/3reconstruction明确复合预算；15s/3s |

两个原exact完整审阅、单独授权、30秒runner逐项执行。原两arrival
marker/PID全部到齐且ray.wait(timeout0)两ref未ready后才release，
不靠耗时阈值证明并发。每次提交立即保存ref以覆盖第二提交失败；
barrier/listener setup在try内，finally所有gate/ref共3s并必达shutdown，
failure也核所有已记录PID/owner/端点，成功诊断socket同样关闭。
same-Node两个逻辑CPU/Worker的exact资源例外见§2，集群总量未增加，
不能把它降为单CPU排队来“通过”并行合同。

三层recursive exact另外完整审阅并单独批准2CPU/3drop/3重建预算，
不是单故障：原3Task与3个≤1KiB结果，最多6次小函数执行。首次root
成功后等全3finish，记录实际STORED@0/producer依赖lineage；三次
真实drop后全LOST@0且预算未动，只有一次root get驱动全部重建，
随后实际均SUCCEEDED@1/各retry1/原TaskID/ObjectID/lineage不变。
不是先分别get叶/中间来代替递归。所有get/drop/finish共15s，drop
用既有ContextVar绝对deadline并reset；3refs close及真GC共3s，
每个condition观察≤256次、每wait≤min(.1s,remaining)并重读authority，
不把超时当成功也不依赖必有notify。failurefinally核4PID/5端点与
实际shutdown报告；无新增Task/外网/GPU/压力或故障机制。

静态235unit文件、2763pure/168heavy/20L1；whole194+exact223/37other，
417unique，currentunit无漏/无heavy/overlap，selected L1恰12并全
exact排除。legacy151/43、placement151/9/4、reference93/36/4；
allowlists **88MP/30L1**，118exact映射。marker/count不认证未来body。
完整K0/K1、其它runtime迁移/宽fault矩阵/同版gate/GCS fidelity均开放；
既有发布、global contained DAG和phase-specific保证未改。

### 先前 borrower-startup-contracts checkpoint（2026-09-07）

borrower-startup-contracts 更新证据后最终复验：**2755 passed, 12 deselected in
7.65s**（首次7.52s）；**193 whole files＋另38文件236 exact selectors**，共429 selectors。
compileall覆盖`conftest.py src tests scripts examples`通过。生产source
仅submit docstring明确同步提交准备/异步用户执行的区别，没有算法变化。
全pytest串行，未执行完整default gate或全部allowlist同版验收。

原borrowed文件两case迁pure：canonical child put/outer nested Task
保真正lineage/maxretry，真实NodeGrant/Start/INLINE Prepare/graph/
Complete→owner adoption，不使用旧raw成功backend。两次load真Acquire
不同token，outerGC释放contained/lineage后第二borrower仍可get，最后
Release才childGC。ACKloss用真Acquire效果→旧异常注入→真Release
tombstone拒重放；OwnerDiedError异常在此不是installed死亡证明。
一1KiB空store，无用户执行/threads/socket/实际重建，2exact **2 in 0.21s**；
加legacy guard **15 in 0.20s**。其余7live函数/12展开heavy不运行，
该文件不升whole，只加2exact。

原Core startup三case保真实constructor/线程，改为L1而非pure。
原2pure body不改，L1不在pure manifest中。所有RPC/_sync/Task/
timer等在constructor前tripwire记录+raise，并由主线程断言记录为空，
不让后台吞异常制造通过。每个正常abort先证明清洁，再finally
只对准确captured线程投STOP/join共享2s，不抹count/owner/recovery。
真实abort一秒join预算不是整个方法墙钟保证，仍须外层30秒。
第三仅测试隔离coordinator周期poll：记录实际线程、延后nextpoll5s
避免busy-loop，真实loop/3threads仍运行、GCS仍配置、没有伪sync成功。
原两次abort第二次零新增join/close。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/unit/test_core_startup_rollback.py::test_lane_start_failure_stops_all_started_local_threads_without_rpc` | 1 passed in 0.13s | 4Thread objects/2started/2joined，原lane1 start前抛错；无任务/对象/网络/子进程 |
| `tests/unit/test_core_startup_rollback.py::test_lane_start_raise_after_start_is_still_joined` | 1 passed in 0.12s | 3objects/2started/2joined，原lane0真实start后抛错 |
| `tests/unit/test_core_startup_rollback.py::test_unpublished_abort_is_idempotent_and_never_syncs_gcs` | 1 passed in 0.12s | 3real threads，周期poll显式隔离，真实abort/join/close幂等 |
| `tests/integration/test_worker_death_ownership_path.py::test_dead_attempt_borrower_is_swept_while_logical_hold_spans_retry` | 1 passed in 1.06s | 1Node/1MiB/1Task2attempt/1crash，peak3children/lifetime4PID/≤6端点；15s/3s |
| `tests/integration/test_worker_owner_death_path.py::test_confirmed_worker_owner_death_fences_foreign_get_wait_and_release` | 1 passed in 1.28s | 两1MiB/2Task1SIGKILL，peak5children/lifetime6PID/≤7端点；另1noStart/Push lease探针；15s/3s |

5个exact全部root完整审阅、单独授权、30秒runner逐项执行，不并行。
consumer-death：64被动RPC/4Acquire记录与有限cleanup观察，所有get/
gate纳入同15s；实际_sync死亡suffix与GCSstate用scoped deadline，
不更改dispatcher或假造reply；reference/gate finally共享3s，所有
已记录PID/端点(含replacement/owner)即使失败也检查。
owner-death：kill前真实outer finish+COLLECTED，而非close receipt
冒充远端收尾；真实GCS核ownerincarnation后仅该PID SIGKILL。
get/wait仍typed OwnerDiedError，Release由installed death discharge，
不更换owner。两个local gate各≤256、death≤128、probe容量≤64；
额外零资源empty lease只观replacement，真Cancel/fullrequest/emptyACK
即unknownGrant也保留；不是第三用户Task/只读查询。所有cleanup失败
仍达shutdown，不能只在成功末尾查资源。

runner/mode/placement guard **78 in 0.18s**；独立AST235文件、
2755pure/176heavy/20L1，全unit纳入、无heavy/重复/overlap。只原12
selected L1被exact排除，新增3L1完全不在purescope，无需新增exclusions。
legacy143/51、placement151/9/4、reference93/36/4；allowlists
**88MP/30L1**，118exact静态映射。计数/marker不保证未来fixture安全。

教学说明同步纠正：.remote()返回前可因序列化、argument seal、
reference hold交接而阻塞/抛错；不等依赖ready或用户执行。
这不是改变接纳/回滚保证；已有lift失败合同仍是证据。完整K0/K1、
余runtime迁移/更宽故障及GCS fidelity未完成，现发布/DAG/phase合同不变。

### 先前 node-lifecycle-contracts checkpoint（2026-09-07）

node-lifecycle-contracts 更新证据后最终复验：**2753 passed, 12 deselected in
7.39s**（首次7.54s）；**193 whole files＋另38文件234 exact selectors**，共427 selectors。
`compileall -q conftest.py src tests scripts examples` 通过。生产source未改；
以下pytest全部串行，不是完整default gate或同版本all-ID验收。

两个原heavy case安全迁pure，不增unit文件/用例：

- `test_drop_object_replica.py::test_core_drop_marks_put_lost_and_get_reports_unreconstructable`：
  **1 in 0.16s**。真实Core put/get/drop、Node Seal/Get/Drop三exact边界，
  1KiB store/≤128B值。物理Drop ACK前owner不变，后LOST；无producer
  lineage、不retry，重复drop不RPC，get为原typed unreconstructable；
  真close/GC忘put，消费有限FIFO，不造Task finish或发布协议。
- `test_node_lease_execution.py::test_worker_exit_reclaims_running_lease_and_fences_late_completion`：
  **1 in 0.14s**。真实Grant/Start、≤32B单INLINE经INTENT/ARM，再向
  实际reclaim输入被动exit。late Complete因非live RUNNING被拒，
  不是缺Prepare导致的伪证据；ledger只release一次，真实driver一轮
  SLOT_DROP/rollback ACK后cleanup_pending为false，重复不再次释放/报告。
  1KiB空store，无stop-worker线程、进程死亡/supervisor/restart声明。

两whole＋classification guards：**59 in 0.27s**；runner合同：**31 in 0.07s**。
原18exact合并为两whole；独立AST235unit文件、2753pure/181heavy/17L1。
全current unit纳入，selected非unit恰12L1并全exact排除，无heavy、
重复/whole-exact重叠。legacy141/53，placement151/12/1，reference93/36/4；
这是库存/分类证据，不证明未来fixture安全。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_node_crash_recovery_path.py::test_remote_node_death_retries_task_on_survivor_and_reports_crash` | 1 passed in 1.16s | 5children、两1MiB、7端点、3logicalTask/1kill/1retry；15s work/3s finally、≤64被动lease记录 |
| `tests/integration/test_startup_rollback_path.py::test_second_node_ready_failure_rolls_back_every_started_process` | 1 passed in 7.03s | 5children、两1MiB、无Task/Actor/trace；≤8被动callback、原5PID/5端点先核init rollback再fallback |
| `tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example07]` | 1 passed in 1.07s | 原5children、两1MiB/6端点、2bundle/2Task；10s/3s，PG同步控制仍仅外层实验上限 |

三个原exact均完整审查、单独授权，通过30秒runner逐项执行。remote-Node
case保原blocker/victim/retry gate，控制参数实际INLINE，所有get、
membership/PID观察共用期限，故意victim unclean报告保留；不再把GCS
hint及时到达当作前提，placement仍由Node本地账本裁决。failure finally
逐一关闭gate/ref并核全部已记录PID/owner/端点，不只在成功末尾检查。

startup case不新增故障：原第二Node ready checkpoint仍在committed
prefix前抛同异常。callback只有限记录，不以assert阻断真正清理。
五PID/端点必须在fallback之前已消失，防止shutdown代偿掩盖init缺陷；
意外init成功也补context/owner端点后无条件shutdown。成功socket探测
同样用contextmanager关闭。startup/rollback无新per-call timeout语义。

例7在Task前输出真实public PGID/attempt及bundle映射，并校验2distinct
Node/context，原执行PID断言保持。解释STRICT_SPREAD硬约束，同时指出
本容量PACK也需2Node；没有用成功结果宣称策略差异、PREPARE期不可见
或采集了虚构日志。不启trace/新RPC/Task/fault/wait，原example07
验收新增对应输出fragment。

allowlists仍 **88MP/27L1**，115exact静态映射完整。完整K0/K1出口、剩余
历史runtime迁移、更宽故障矩阵及GCS fidelity决定未完成；同步发布、
global contained DAG与phase-specific合同不变。未运行heavy/未知成本测试。

### 先前 recovery-contracts checkpoint（2026-09-07）

recovery-contracts 固定入口最后monitor文案纠正后复验：**2751 passed, 12 deselected in
7.42s**（首次7.61s，更新证据后7.62s）；
**191 whole files＋另40文件252 exact selectors**，共443 selectors。
`compileall -q conftest.py src tests scripts examples` 通过。以下所有pytest均
串行；这不是完整default gate或全部allowlist同版本验收。生产变化只有
Core.shutdown docstring/API monitor注释，执行、发布与恢复算法未改。

两个原heavy合同安全迁pure，没有新增unit文件/用例：

- `test_core_worker_crash_recovery.py::test_dependency_hold_survives_worker_loss_retry`：
  **1 in 0.25s**。canonical producer/consumer、实际Node Grant/Start/
  reclaim/Outcome→retry→selected-output，submitted hold跨retry，finish
  只释放submitted，output GC才释放lineage并允许producer GC。1Core/
  1未启动Node/2被动slot/1KiB空store，2Task/3leases/4Push调用/2tinyINLINE，
  控制/队列/GC均有限；无用户函数、真实Worker死亡或supervisor验证。
- `test_core_placement_group_scheduling.py::test_committed_participant_death_fences_complete_manifest_and_queued_task`：
  **1 in 0.20s**。两1KiB Node真实STRICT_SPREAD PREPARE/COMMIT，
  registry typed death→PG LOST/survivor ABORT→完整snapshot安装→Core
  拒绝原FIFO Task；零Lease/Push、不推进attempt/预算，真实finish/close/GC。
  死Node被动fixture账本保留，不伪造成真实OS回收。

Worker整文件＋PG该exact＋两个classification guard：**46 in 0.36s**；
runner合同：**31 in 0.07s**。Worker原12exact合并为whole，PG仍mixed，
仅增加1exact。独立AST库存235unit文件、2751pure/183heavy/17L1；
全current unit纳入，selected非unit恰12L1且全部exact排除，无heavy、
重复或whole/exact重叠。legacy guard140/54，placement150/13/1，
reference93/36/4。库存/marker不证明未来fixture安全或全部语义通过。

| Exact selector | 结果 | 有界范围 |
|---|---|---|
| `tests/integration/test_driver_local_node_recovery_path.py::test_driver_local_node_death_migrates_home_and_retries_on_survivor` | 1 passed in 1.14s | 5children、两1MiB、7端点、原1retry＋1probe＋1put，各成功payload2KiB；15s/3s、≤32被动lease记录 |
| `tests/integration/test_worker_crash_recovery_path.py::test_after_complete_worker_crash_recovers_output_without_reexecution` | 1 passed in 0.95s | 1Node/1slot/1MiB、peak3children/lifetime4PID/≤5端点；1用户Task/1CRASH，另1未执行lease探针；15s/3s |
| `tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example06]` | 1 passed in 0.85s | 原3children/1MiB/5端点、1drop/1reconstruction；10s/3s，≤201次/2s的Driver trace观察计入work |

三个exact均重新完整审查、单独授权，以30秒外层runner执行；之后仅补正
monitor相关注释，没有再改运行时可执行语句。
Driver旧fixture先 **1 failed in 11.19s**：threshold1让控制参数被lift成
home-only put，Node丢失后无lineage，第二gate不能到达。现threshold1024
并在kill前核实际3个参数为INLINE；结果/put在原位置生成2KiB以保STORED。
不补造put lineage或更改retry算法；owner/Task/Object/lease/home迁移与
故意victim unclean诊断保留，已记录PID/端点在failure finally检查。

Worker旧exact `test_after_complete_worker_crash_retries_on_fresh_worker`
先 **1 failed in 0.95s**：实际attempt0而旧断言要求1。它已显式重命名为
表中ID，不将旧“重执行成功”合同伪称通过。真实Complete后CRASH不等于
SYSTEM_ERROR：Node envelope保存初始PID/attempt0，零retry、无成功
direct TaskReply，adoption后仅保留witness。独立GCS死亡事实核exit23；
replacement继承failpoint，因此额外probe只Request/Cancel/empty-custody
ACK，不Start/Push，不能当成第二用户Task或只读查询。原Core记录≤64，
finish观察≤256，old-death与同probe容量轮询各≤64。未知Grant也保full
Cancel请求；failure finally共享3s清理后必达shutdown，不造ACK。

例6保原producer、first==second/ObjectID及资源/故障预算。两次get成功后
只等Driver task_submitted/reconstruction_started，校验实际TaskID、
ObjectID、AttemptID0→1和进程内顺序；缺trace明确是观察交付失败，不
变成重建或全trace收齐证明。新的输出fragment已在原example06验收检查。

Core.shutdown文案现区分协作等待预算与RPC本身期限；未决协议早退可
保owner状态并采用晚结果，不能说所有unfinished都ERROR。API monitor
clean路径贯穿Node finalize/graceful退出，force前才stop/reconcile。
allowlist仍 **88MP/27L1**，仅Worker exact ID替换；完整K0/K1、剩余
runtime迁移、更宽故障组合及GCS fidelity取舍均未完成。

### 先前 dispatch-continuations checkpoint（2026-09-07）

dispatch-continuations 固定入口最终复验：
**2749 passed, 12 deselected in 7.47s**（首次7.40s）；
**190 whole files＋另41文件263 exact selectors**，共453 selectors。
`compileall -q conftest.py src tests scripts examples` 通过。
未执行完整历史/default gate；所有pytest串行，无heavy/未知成本case。

生产变化仅ReadyTask派生DispatchKind与dispatcher三项分支判断。
7份顶层continuation互斥，非零ambiguity_round必须原lease，0轮lease
合法。仅FRESH走PG新准入、仅两OUTPUT免原current-PENDING快捷过滤；
payload内部state/GC/owner权限由既有authority核验，_execute转发不变。
13项新envelope测试用opaque payload和routing spy，不能视为有效
publication/owner状态证据；加真实pure PG续行 **19 in 0.21s**。
旧CANCEL队列遇新CUSTODY marker仍合法，kind不冒充当前protocol phase。
这是显式表达/拒无效组合，不宣称新public故障先失败复现。

原spillback前三Core函数3case安全迁pure：NodeRegistry真登记、
Hybrid spillback/Grant/Start、selected INLINE Prepare/Complete/adoption
与finally本地ref/ownerGC。Timeout保持RUNNING/CPU，Worker拒绝保持
GRANTED，一次手动完全相同Push bytes重放，无新Lease或Submitter
release；**3 in 0.20s**。最多两Nodes、1tinyTask、2Push、1份1KiB
store，FIFO快照/GC各≤8，无user代码/线程/等待。原并发duplicate
spillback仍heavy，其余body及原Nodehelper不变，不能升whole运行。

原foreign-stored报告三case也安全迁pure：真实put/export/borrower
Acquire→canonical consumer submission→输入handle close但retained
hold仍活，Node3call pin/chunk/release与真实Grant/owner report/custody；
成功用现selected-output，失败用原definite Push failure。finish后
仍保foreign lineage，output COLLECTED后才真Release retained，再
source owner GC删除两个副本，不能沿旧directfixture提前释放hold。
一项实际调用Core.shutdown的未决协议早退，无threads/join/wait，
后续exactCancel/custody后正常GC；不宣称公共进程关停。**3 in 0.20s**；
全部13新＋6迁移＋legacyguard **32 in 0.23s**。原owner-death和
typed-stale两heavy正文不改，本轮不以虚构death/retirement替代证明。

| Exact selector | 结果 | 有界资源/语义 |
|---|---|---|
| `tests/integration/test_foreign_stored_ref_path.py::test_inline_outer_restores_foreign_ref_then_driver_fetches_stored_bytes_from_node` | 1 passed in 1.01s | 4children、1Node/2Workers、1MiB、5端口、2Task/64KiB、10s work/3s finally close |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[terminal]` | 1 passed in 1.10s | 原5children、两1MiB、6端口、1tinyTask、15s/3s；terminal ACKloss＋idlepeer death |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[adopted]` | 1 passed in 1.06s | 同边界；adopted ACKloss仍需payload退休/finish |

三个原exact重新完整静态审查、单独授权，通过30秒runner逐项执行。
foreign-stored-ref所有get共享work期限，工作期close各≤min(work剩余,3s)，
finally两refs共享3s且close异常必达shutdown/PID/端口检查；最多16条
owner/fetch观测只记实际reply，不从observer改变RPC。成功parent返回的
child句柄保留到discovery，只有异常分支finiteclose，避免早关破坏
contained发布。原outer close后独立borrower取值与Node bytes校验保留。
PG双phase未加新的故障或线程，故意crash诊断仍unclean，不冒充全矩阵。

本轮净增19pure=13新＋6迁移；两mixed文件仅增加六exact选择，
12既有L1 exclusions不变。独立AST库存235unit文件、2749pure/185heavy/
17L1；所有当前unit被选中且无重复/whole-exact重叠，selected非unit
恰12L1全部精确排除，无heavy混入。legacy分类锁现139pure/55heavy，
reference和placement固定库存计数不变；库存不替代future safety。
allowlist仍 **88MP/27L1**，不视为全部同版
通过。设计/学习路径补queue-kind与authority差别，无第二后端；
历史runtime迁移、同版all-ID出口、宽故障组合/GCSfidelity仍开放。

### 先前 Worker-locality/admission checkpoint（2026-09-07）

Worker-locality/admission 固定入口最终复验：
**2730 passed, 12 deselected in 7.52s**（首次 7.63s）；
**189 whole files＋另41文件257 exact selectors**，共446 selectors。
`compileall -q conftest.py src tests scripts examples` 通过。
本checkpoint未改生产source；新增真实Worker冷缓存验收与11项原合同
安全迁移，不等于完整历史/default gate。全部pytest串行，无heavy。

| Exact selector | 结果 | 有界资源/语义 |
|---|---|---|
| `tests/integration/test_worker_lease_locality_path.py::test_worker_without_snapshot_caches_cold_locality_for_foreign_stored_dependency` | 1 passed in 1.26s | 新无snapshot Worker/Core，5children、两1MiB、6端口、4次Task、32KiB、15s work/共享3s close-GC |
| `tests/integration/test_foreign_stored_dependency_path.py::test_foreign_stored_dependency_pulls_node_to_node_before_push` | 1 passed in 1.16s | 原foreign源/输入先关/目标pull，5children、两1MiB、7端口、3Task、64KiB、15s/3s |
| `tests/integration/test_worker_nested_task_path.py::test_worker_submits_child_task_and_gets_plain_result` | 1 passed in 1.17s | 原2tinyTask/0CPU parent/Worker trace、5children、两1MiB、7端口、10s/3s |
| `tests/integration/test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get` | 1 passed in 1.12s | 原2tinyTask/两次borrower/escaping ref、5children、两1MiB、7端口、10s/3s |

四exact全部重新静态审查、单独授权并逐项通过30秒runner。新Worker
case不造snapshot/route/ACK：Driver-owned source在B，0CPU parent在A
接nested borrowed handle，嵌入Core连发两个顺序children到B。
pass-through首跳评分scope使用thread-local标签，仅评分内冷查询
计数(1,0)；adoption仍有各自GetNodeAddress，不称整个运行只查询一次。
父/Driver observers最多64/24条，超限只记flag，观察锁不跨原RPC。

children GC还等待foreign lineage activated/prepared receipts与registry
清空，实际owner Release ACK2次以后才borrower Release1次。Driver
独立核源tokens/parent lineage，再parent/source GC，实Node Drop ACK
及deadline内GetObject absence确认物理删除，两个Worker仍存活。
父close/GC共用min(work deadline,now+3s)，Driver所有close/GC共用3s
且finally不重置已开始预算；startup/shutdown依旧外层runner有界。
它仅证明冷查询成功→正缓存复用及所有权收尾，不覆盖冷失败、
同时miss、失效或Node-death矩阵，不把pure当真实wire。

原foreign-stored测试只收紧安全生命周期：listener setup移入try、
原gate I/O与get共享15s（单次gate仍≤10s）。工作期public close
受work剩余及每轮3s限制，finally全部refs另共用3s释放预算；
五PID/七端口（含owner/gate）在失败finally检查。64条原消息观察
有cap，Driver get_object改为被动flag而非从dispatcher抛pytest.fail；
主线程仍assert未取bytes。原3Task/64KiB/foreign hold先保留再关输入/
sealed grant→owner report→Push等断言保留，不因helper改动增加新故障。

纯增量11来自迁移而非新增：lease_cancellation原5Core函数8展开→
pure，原3Node正文与release_outcome/detail参数不改；真实Release
先执行、精确重复后false/或丢ACK，Cancel真实提交后可丢ACK；
Core仍PENDING直到真实回执，原error对象/attempt/资源version保留。
direct取消经empty inventory/custody，known-Grant scalar仍用原
inventory=None优化，不凭空加custody RPC。最终真实finish/finalizer/
手动owner GC，非公共runtime shutdown测试。整文件 **11 in 0.19s**；
加classification guard曾 **27 in 0.21s**。

原PG三例改为单Node/一1KiB store、真实PG attempt3 reservation与
Grant/Start/selected-output Prepare/Complete/adoption；容量case只
手动一次原replay，无clock/wait，重建case为Task attempts0/1/2，
先真实drop_object失副本、旧publication retirement后重建，PG不死。
首跑 **1 failed / 2 passed in 0.24s** 是原key `is`期望不适用真实
publication snapshot deepcopy，修成完整value identity `==`，
不改变生产key/attempt/owner合同；随后 **3 in 0.21s**，
cancel＋PG三exact＋classification最终 **30 in 0.31s**。
剩余18个PG函数正文不改，5个heavy展开继续不运行。

manifest把cancel原3exact提升whole11，PG增加3exact，其余选择/
12 exclusions不变。placement固定锁149pure/14heavy/1L1，reference
不变。独立AST库存234个顶层unit文件、2730pure/191heavy/17L1；
所有当前unit均入显式选择，无重复/遗漏/whole-exact重叠，selected
nonunit恰12L1且逐个排除，无heavy混入。PG仍混标文件，只选exact，
不因3项迁移升whole。库存检查不代替运行证据或future safety。
runner为 **88MP/27L1**，不是115项同版通过；完整runtime
迁移、all-ID gate、更宽故障组合与GCS fidelity取舍仍开放。

### 先前 lease-locality checkpoint（2026-09-07）

lease-locality 固定入口在历史 evidence 更新后复验：
**2719 passed, 12 deselected in 7.19s**（首次 7.18s）；
**188 whole files＋另42文件257 exact selectors**，共445 selectors。
旧2695范围在新routing实现上先通过 **2695/12 in 7.31s**；
`compileall -q conftest.py src tests scripts examples` 通过。
新增两whole均已完整静态阅读，全部pytest串行，无heavy/未知成本执行。

新增24pure为12项独立bytes评分＋12项Core组合：本owner当前epoch/
canonical多副本、foreign单source、snapshot/death过滤、可选冷查询
正缓存/0.75s deadline与失败fallback、真实B→home spillback、
ACKloss/capacity原hop冻结重放、PG bypass。Core fixture用2×1KiB
unstarted Nodes和实际sealed put，不执行user callable，不造成功Task
publication；执行测试真实Grant/custody/Start/SYSTEM_ERROR Complete。
它不运行GC/shutdown线程，finally只执行真实本地ref finalizer。
Core首跑 **1 failed / 11 passed in 0.32s** 为fixture手动put与
Core.put复用序号0；改用Core既有put序号后两文件 **24 in 0.18s**。
未为该fixture冲突改生产协议。原spillback DTO/两empty-dependency
home-route exact另 **6 in 0.17s**，原断言保留。

旧scope的只读调用链审查确认：原fresh普通_execute均空stored依赖，
其余stored/foreign case显式lease_state/location_state或只测prepare，
不会因本次optional coldlookup暗中触发新网络。此结论针对当前调用点，
并非旧bare helper永久安全；新增fresh非空依赖必须重新审查/mock RPC。

| Exact selector | 结果 | 有界资源/语义 |
|---|---|---|
| `tests/integration/test_lease_locality_path.py::test_stored_dependency_selects_data_first_hop_and_resources_can_spill_back_home` | 1 passed in 1.14s | 5children、2×1MiB、6端口、3Tasks/32KiB、15s work＋共享3s close/GC；A→B/B/B→A |
| `tests/integration/test_cross_node_dependency_pull.py::test_store_backed_dependency_pulls_to_consumer_node_before_direct_push` | 1 passed in 1.14s | 原5children、两1MiB、64KiB、2Tasks、8端口；unready ref/custom target/pull原断言 |
| `tests/integration/test_two_node_spillback.py::test_custom_resource_spills_task_to_second_node_and_cleans_cluster` | 1 passed in 1.10s | 原无依赖Task、5children、两1MiB、7端口；home fallback与资源spillback |
| `tests/integration/test_placement_group_path.py::test_strict_spread_tasks_use_committed_bundles_and_remove_restores_resources` | 1 passed in 1.06s | 原两bundle Tasks、5children、两1MiB、6端口；PG路线不被locality改写 |

四exact均独立授权、逐项通过30秒runner。新case无trace/gate/listener/
test线程/故障，Driver不get producer bytes；<=32条被动RPC观察
验证requester/lease/task/attempt不变、first preferred=B/targetNone、
B→A仅第二hop定向、真实owner custody ACK先于Push。两consumer的真实
GC先释放source lineage，source close后等两Node exact Drop ACK及
owner/recovery最终collection；不主动flush、Drop或修复metadata。
所有已观测PID/端口/精确report在新case失败finally检查。原三个MP
保其已审资源/清理约束与原验收断言，不称全部历史入口已同版清理验收。

新MP直接观测的是Driver消息和owner状态，不是Node间wire trace或
所有无拷贝路径证明；它只覆盖Driver installed-snapshot首跳，Worker
冷lookup/失效和多副本评分的证据限于pure。该优化不改变GCS同步
publication、global contained DAG、phase-specific恢复或frozen replay。

静态库存：234顶层unit文件，2719pure/202heavy/17L1；全部当前unit
在188whole/257exact里，无重复/遗漏/whole-exact重叠，选中的12L1均
精确排除，无heavy。固定reference/placement分类锁计数不变。
runner为 **87MP/27L1**，仅允许清单，不是114项同版通过。
学习路径新增可选2A，基础1→7、原图和历史证据不变；完整历史runtime
迁移、全部同版出口、更多故障组合及GCS fidelity取舍仍开放。

### 先前 PG-retry-atomicity checkpoint（2026-09-07）

PG-retry-atomicity 固定入口在更新历史 evidence 后复验：
**2695 passed, 12 deselected in 7.38s**（首次 7.85s）；
**186 whole files＋另42文件257 exact selectors**，共443 selectors。
`compileall -q conftest.py src tests scripts examples` 通过。
这是当前受审 pure 子集，不是完整历史/default gate。全部 pytest 串行，
下表每个进程用例均重新静态审查、单独授权、通过30秒runner执行。

新增PG原子性反例先 **1 failed / 2 passed in 0.16s**：真实内存
NodeRegistry/PG prepare/commit、Grant/Start/SYSTEM_ERROR Complete后，
Core原PG检查解锁触发单次同步死亡回调，观察到
`pg_lost -> owner_retry -> recovery_retry`。修复把PG判定与原owner/recovery
retry提交放进同一已有锁，三例 **3 passed in 0.21s**；retry-first合法
推进一次后仍须被fresh-PG准入拒绝，不能再Lease/Push。此为纯authority
交错，不是OS并发/真实进程死亡复现。普通终态/late-cleanup顺序不变，
没有新增RPC；non-SystemTaskError私有fallback仍在PG分支后格式化，
现处同锁内，实际生产调用链使用内部SystemTaskError族。

manifest净增14case：multi-return原6函数/8展开安全迁到pure，保真实
owner/recovery rollback与已存finalizer detach；原retry前三3case迁
pure，保submitted/lineage hold、有限FIFO、finish/public close/owner GC；
再加3个新atomic。原retry后三正文未改。三个文件＋两分类锁实跑
**49 passed in 0.33s**；runner scope合同另 **31 in 0.08s**。
原retry三exact升whole，不重复选择；12个既有非pure exclusions不变。

| Exact selector | 结果 | 有界资源/语义 |
|---|---|---|
| `tests/integration/test_task_retry_path.py::test_explicit_worker_system_error_retries_once` | 1 passed in 0.92s | 3children、1MiB、5端口、1Task/2attempt、10s work/3s close；真实finish后检查 |
| `tests/integration/test_placement_group_path.py::test_strict_spread_tasks_use_committed_bundles_and_remove_restores_resources` | 1 passed in 1.14s | 5children、两1MiB、6端口、2tinyTasks、10s/3s；显式PG remove |
| `tests/integration/test_placement_group_path.py::test_shutdown_removes_committed_group_without_explicit_remove` | 1 passed in 1.08s | 5children、两1MiB、6端口、零Task；shutdown原PG drain |
| `tests/integration/test_placement_group_prepare_failure_path.py::test_second_participant_prepare_rejection_aborts_first_and_restores_roots` | 1 passed in 1.19s | 5children、两1MiB、7端口、零Task；原一次PREPARE拒绝和真实双ABORT |
| `tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg` | 1 passed in 1.15s | 5children、两1MiB、7端口、1PG Task＋1survivor probe、15s/3s |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[terminal]` | 1 passed in 1.07s | 原Complete/terminal ACKloss＋idlepeer death、15s/3s |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[adopted]` | 1 passed in 1.07s | 原READY/adopted ACKloss＋idlepeer death、15s/3s |

participant-loss 首次 **1 failed in 1.11s**，断点是survivor probe
get成功后立即要求GCS available==total。Node已本地Complete释放CPU，
但GCS尚可见上一次Grant的空available hint；既有supervisor异步outbox
独立报告，get不提供该ACK顺序。仅将观察改为原15s期限内≤1024次
GetNodes/10ms被动Event等待，每次检查唯一survivor ID/PID/epoch/address/
ALIVE与total，保最终equal及超时失败。不主动flush、不修改metadata、
不发新Task、不吞RPC/identity错误；失败调用也经过finally清理。
这不是生产资源算法修复，也不是PG原子竞态的进程复现。

前四原process case只收紧生命周期与被动观测：Task retry的最多16条
lease/Push记录不改变回复；PREPARE trace≤1024 polls/4096records。
每个已观测PID/端口在失败finally核验，close异常仍必达shutdown。
PG同步创建/删除、startup/shutdown的总上限仍来自30秒runner，
public close只限制receipt等待，不取消事务或提前确认物理GC。
两个peer-loss case均5children/两1MiB/6端口/1tinyTask，
ACKloss＋death继续按两个受控事件记录，不外推完整fault矩阵。

README首屏现优先给定位、七例入口与安全运行链接；原故障增量逐字
保留到历史折叠段，既有图和协议/证据边界未改。独立AST库存核对
232个顶层unit测试文件、2695pure/202heavy/17L1；全部当前unit被显式
选择，selected非unit恰为12个原L1并全部exact排除，无heavy混入。
reference分类锁93pure/36heavy/4L1，placement锁138pure/25heavy/1L1；
新atomic不扩其固定历史文件范围。库存核对不替代future safety或实跑。
runner allowlist仍
**86MP/27L1**；历史runtime迁移、同版all-ID gate、完整K1组合和
GCS fidelity取舍未完成，未运行heavy或未知成本用例。

### 先前 publication-continuations checkpoint（2026-09-07）

publication-continuations 固定入口在最终evidence/scope/allowlist更新后
复验：**2681 passed, 12 deselected in 7.90s**（首次7.82s）；
**183 whole files＋另43文件260 exact selectors**，共443selectors。
原2668选择在dispatcher修复后另通过 **2668/12 in 7.80s**；
`compileall -q conftest.py src tests scripts examples` 通过。
全部pytest串行，未运行heavy/未知成本case，完整历史gate仍禁止。

新增可达故障：真实Complete后的adoption retry被dispatcher的初始PG
准入检查截断。另一bundleNode死亡令PG LOST，terminal ACK丢失时会
在owner CAS前错误发布ERROR；adopted ACK丢失时会提前finish却未退休
Node payload。新两phase纯反例先 **2 failed / 7 passed in 0.27s**
（7为迁移后的owner文件），修复仅让output_adoption、output_node_loss、
system_failure续行进入原authority，PG catch只围fresh admission。
不新建协议、不对ERROR blanket early-return、不以取消PG代替结果清理。

纯增量13：7个原owner案例安全迁移＋6个新PG参数case。原owner
case6首次 **1 failed / 6 passed in 0.22s** 是fixture把无远端fence的
ERROR与凭空构造的成功Complete拼接。现改为真实Grant→Core取消→
Node Cancel/空inventory ACK后ERROR（仍未finish），LateStart与
无Prepare成功Complete拒绝；descriptor-only假成功按当前wire合同
抛SystemTaskError，原ERROR/recovery/event不变且无新增义务。真实
publication的故障由新PG路径验证，不能用忽略所有ERROR回包掩盖其custody。
最终这13项 **13 passed in 0.28s**；新Node-loss pair只做真实GCS
metadata清理/终态，不声称物理GC；deferred-system仅原样路由，不声称
等待全部late-replica queue。

以下新MP每次独立授权、完整静态审查后经30秒runner执行：

| Exact selector | 结果 | 场景 |
|---|---|---|
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[terminal]` | 1 passed in 1.24s | Complete已真提交，owner尚PENDING，terminal ACK丢失 |
| `tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[adopted]` | 1 passed in 1.12s | owner已READY，adopted ACK丢失，仍须Node payload退休/finish |
| `tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg` | 1 passed in 1.28s | dispatcher修后原运行中PG participant-loss回归 |

新case：5children、2×1MiB、1PG/2bundle、1tinyTask（无执行重试）、
6端口、15swork/3spublicclose，零test线程/listener。原Core dispatcher
在真实GCS ACK后、无Core锁时调用既有exact NodeID crash barrier，再
丢该一个ACK；**ACKloss与idlepeercrash是两个受控事件**，非generic
单故障证明。main等真实cut完成及finishbarrier，再核实际Node retirement
ACK与metadata-onlyoutcome（同Complete witness/无retained envelope），
不另发retirement修复测试。旧PG两bundle都拒新Task，generation/attempt
不重写；故意victim crash依旧为unclean，survivor正常收尾。

另四个原MP先完整收紧后在dispatcher修前分别通过（不冒充修后复验）：

| Exact selector | 结果 | 有界资源 |
|---|---|---|
| `tests/integration/test_public_put_path.py::test_public_put_inline_and_stored_values_without_worker_execution` | 1 passed in 0.84s | 3children、1MiB、2put/零Task、64KiB、5端口 |
| `tests/integration/test_large_object_path.py::test_one_node_large_result_uses_object_store` | 1 passed in 0.90s | 3children、1MiB、1Task/64KiB、5端口 |
| `tests/integration/test_worker_nested_task_path.py::test_worker_submits_child_task_and_gets_plain_result` | 1 passed in 1.21s | 5children、两1MiB、2tinyTasks/0CPUparent、7端口 |
| `tests/integration/test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get` | 1 passed in 1.14s | 5children、两1MiB、2tinyTasks/两次borrower、7端口 |

这四项work共享postinit10s、finally refs共3s、全部setup在try、必达
shutdown及failure PID/owner/trace端口核验；nested child get另≤5s且
不超Driver期限，trace≤min(2s,workremaining)。foreign ref用通用公开
close，不套local-only helper，并等真实borrower release义务消失后
第二次get，close receipt不是远端ACK。同步put/create不新增取消。

四原local-reference L1也先完整审查再分别运行，每次仅一个exact：

| `tests/unit/test_local_reference_lifecycle.py::` 后缀 | 结果 |
|---|---|
| `test_each_python_handle_has_a_distinct_local_token` | 1 passed in 0.15s |
| `test_close_releases_exactly_once_and_invalidates_only_that_handle` | 1 passed in 0.13s |
| `test_shutdown_drains_accepted_releases_and_late_close_is_a_noop` | 1 passed in 0.14s |
| `test_unpickled_logical_handle_is_detached_until_borrower_protocol_exists` | 1 passed in 0.14s |

1真实reference线程/1PENDING对象/最多2handles，无socket/集群/用户函数；
最多2release+2GC+1STOP。close共享1s、stop/join1s，fixture在start前
安装finally，NEW thread不错误join，真实stop后校验stopped/emptyFIFO/
零unfinished/timer。它们不是pure，两项gc.collect继续heavy原样。

独立AST库存：231个顶层unit文件，2681pure/213heavy/17L1；当前pure
全在显式manifest内，12whole内L1仍精确deselect，local4不入purewhole。
reference分类锁现90pure/39heavy/4L1。runner现 **86MP/27L1**，只是
allowlist并非全部同版通过。更宽fault组合、历史runtime迁移和GCS
fidelity合同取舍未完成。

### 先前 replica-integrity/lifetimes checkpoint（2026-09-07）

replica-integrity/lifetimes 固定入口更新evidence/scope后复验：
**2668 passed, 12 deselected in 8.23s**（首次8.07s）；
**180 whole files＋另43文件260 exact selectors**，共440selectors。
旧2655范围在新Node实现上先复验 **2655/12 in 7.87s**；
`compileall -q conftest.py src tests scripts examples` 通过。不是完整
默认/runtime gate，没有运行任何heavy或不确定成本测试。

Node sealed-replica共享删除与owner-death观察现在核对实际bytes长度和
SHA-256。原先same-size损坏会被当成PRESENT甚至成功删除，新增三pure
反例先 **3 failed in 0.22s**；实现修复后加上read异常/length drift与
live pin组合，最终7cases＋现cross-authority/ownerFinalize回归
**66 passed in 0.36s**。每case先真Seal8B到1KiB store，再明确注入
私有bytes损坏/读取异常；未证明正常public fail-stop路径可达该损坏，
不声称新增磁盘可靠性或自动repair。

损坏/不可读不写删除watermark或receipt，metadata保留；显式repair后
仍须先解除真实pin才能同request收敛。PUBLICATION_EXACT的CONFLICT
是冻结观察且可complete，不是GC；OWNER_WIDE则不complete、不缓存。
旧completed receipt仍先于读取，delete已生效/forget失败的absence恢复
仍在新校验之前；不能跳过全部CONFLICT导致旧清理永久卡住。正常sweep
最多分别在观察/删除做两次本地校验，没有新的RPC、状态表或后台线程。

原两项lineage＋四项nested-argument从heavy安全迁到pure，IDs/语义
断言保留；真实discovery/adoption、owner hold/release、finalizer与
手动FIFO/GC组成同一运行逻辑。嵌套PENDING handle不门控执行、SYSTEM
retry保持logical hold、foreign retain生效后丢ACK补偿、顶层/嵌套只
占一hold与最后GC后release均保留。lineage首轮因未先prepare顶层
RefArg而 **1 failed / 17 passed**，补真实dependency preparation后
两项＋分类锁 **18 in 0.23s**；加入四nested最终 **22 in 0.27s**。
没有为通过改生产协议。nested另两项live attempt-borrow/timing测试
仍heavy且正文未改。reference锁相应从77pure/56heavy变为83/50；
本次总增13是6项原case迁移＋7新integritycase，不能重复加总。

以下六个原exact均先完整静态审查，再独立授权、逐个经30秒runner运行；
全部发生在Node修复后：

| 精确selector | 结果 | 规模 |
|---|---|---|
| `tests/integration/test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker` | 1 passed in 1.50s | 6children、两1MiB、1Actor/3calls、10s work/3s close、8端口 |
| `tests/integration/test_actor_cross_process_trace.py::test_actor_creation_uses_control_plane_and_method_call_bypasses_gcs` | 1 passed in 1.18s | 4children、1MiB、1call、10s/3s、6端口；trace≤min(2s,work剩余)/4096原始records |
| `tests/integration/test_actor_restart_path.py::test_actor_crash_restarts_once_fences_inflight_call_and_resets_state` | 1 passed in 1.55s | 峰4/累计5PID、1MiB、4calls、一次Actor退出/重启、15s/3s、7端口 |
| `tests/integration/test_actor_node_loss_migration_path.py::test_actor_migrates_after_remote_node_loss_and_resets_generation` | 1 passed in 1.79s | 峰6/累计7PID、两1MiB、1Task/2Actorcalls、一次Node故障/迁移、15s/3s、9端口 |
| `tests/integration/test_precomplete_output_owner_death_path.py::test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds` | 1 passed in 1.56s | 5startup+1replacement、两1MiB、8KiB输出、一次owner退出、15s/3s |
| `tests/integration/test_cross_cleanup_receipt_path.py::test_publication_rollback_receipt_replays_after_same_object_retry_seals` | 1 passed in 0.94s | 3children、1MiB、8KiB输出、一次seal后错误/retry、15s/3s、4端口 |

四Actor保全部原业务与trace断言：创建GCS/call直达、FIFO、generation/
route fencing、constructor reset、不透明重放。setup与publicclose有限，
shutdown后finally检查所有**已观测**PID/endpoint和gate句柄；restart/
migration最终PID检查全组2秒，端口每个0.1秒。未返回任何物理route的
create失败仍依Node shutdown与outer进程树，不能猜ActorPID。被动route
observer先转发原install，异常只记flag，不额外注入RPC失败。
Actorcreate/debug/method控制原语义不变，无per-call超时即取消承诺。
F5/cross-cleanup只换有限publicclose helper导入，sharedhelper未改；
cross-cleanup额外将失败卫生检查移入finally。故意Node死亡继续报告
victim unclean，不能因为survivor清理成功抹去故障。

allowlists仍84MP/23L1，不代表全ID同版通过；其它历史runtime安全迁移、
更宽故障矩阵与GCS fidelity仍未闭合。

### 先前 PG-cancellation/classification checkpoint（2026-09-07）

PG-cancellation/classification 固定入口最终复验：**2655 passed, 12 deselected
in 7.69s**（首次扩展7.80s，更新manifest evidence与scope合同后8.18s）。
范围是 **178 whole files＋另42文件256 exact selectors**，共434 selectors。
原2187选择在共享fixture更新后、PG修复前另复验 **2187/12 in 7.36s**。
最终 `compileall -q conftest.py src tests scripts examples` 通过。仍禁止完整
默认gate；这一结果不是所有MP/L1在同版通过或永久安全认证。

四个固定分类锁完整静态审查62个旧文件，分类与语义通过分开处理：

| 组 | 文件数 | pure展开 | 保留heavy展开 | 原L1 | 分类锁自身 |
|---|---:|---:|---:|---:|---:|
| reference | 16 | 77 | 56 | 0 | 16 |
| placement/Node | 16 | 130 | 33 | 1 | 16 |
| Worker | 17 | 87 | 22 | 0 | 17 |
| legacy runtime（含Node-death） | 13 | 133 | 61 | 0 | 13 |

原manifest已含最后一组25个pure cases；因此净增
`77 + 130 + 87 + 108 + 62 + 4 = 468`，最后4项是新PG取消回归。
172个heavy保留原body/ID且未运行；不能按数千条pure通过推导这些旧runtime
用例已验证。AST仅读取固定源码，锁定原IDs/参数/分类；它不证明将来import、
fixture或运行时代码变化的成本。保留旧成功fixture的pure测试没有改heavy
来隐藏失败，而是迁到实际唯一selected-output路径。
独立AST库存核验228个顶层unit目录测试文件：当前unit标签展开2655项，
均被manifest覆盖，无漏标/多重模式函数；另有230 heavy和13 loopback。
所选whole内12个loopback均由known exclusions精确排除，剩余NodePull
loopback独立运行。这是标签/选择库存闭合，不认证未来改动或未跑runtime
合同，不能据此解除目录级collection guard或把默认历史gate宣称完成。

主要修复与可复验边界：

- Push重放、Worker completion/supervisor、Node lease/PG/pool、Core
  reconstruction和large-result成功现在经过真实discovery/journal/adapter；
  小STORED fixture为1KiB store，large保持64KiB payload＋128KiB store。
  控制RPC仅metadata；Node Complete/owner adoption/每槽GC各自有真实状态断言。
- Node unpin完成不等于pre-grant custody ACK；资源report在local Complete后
  由outbox驱动；两次lineage validation均早于owner/recovery推进。对应旧
  断言曾失败，修正fixture期望而非削弱生产协议。
- 本轮pull11＋supervisor13首次 **1 failed, 23 passed**：旧seal错误文案
  忽略原删除watermark。现在同时验证新attempt bytes/metadata及旧watermark
  不变，最终 **24 passed in 0.23s**。foreign报告不再依赖复制前guard的
  Python `is`，scoped importer匹配当前拒绝边界。旧multi-return拒绝注明
  仅为底层export的注入rollback，不称当前Worker限制。
- 真实Node Cancel/inventory/ACK迁移暴露PG生产问题：原PG-loss selector
  **1 failed / 1 passed**，已LOST却返回transport wrapper。Core只在PG key、
  当前LOST、typed PG cause三条件满足时保留同cause；普通wrapper与既有
  sticky error不改，先清理后终态、不重试。整Node-death **16 in 0.28s**；
  新4项＋该文件＋13项AST **33 in 0.30s**，既有7个相关文件 **73 in 0.40s**。
  新case每项1Node、1KiB空store、最多两次手动推进/三RPC；丢失的是实际
  Node效果后的ACK，任意ACK未收齐均观察到PENDING。它不代表真实线程竞态
  或公共PG同时故障的集成验收。

四组另有单独纯运行：Worker含guard **104 in 0.39s**；reference含guard
**93 in 0.39s**；placement/Node含guard **146 in 0.50s**；前12 legacy＋
13项guard **130 in 0.40s**。这些选择有交集，只作为调试证据，不重复加总。

下列完整静态审查后逐项经30秒runner，所有运行串行、独立授权。前三项先前
分别1.22/1.27/0.90s，表内为Core PG修复后的复验：

| 精确selector | 结果 | 规模与清理 |
|---|---|---|
| `tests/integration/test_two_node_spillback.py::test_custom_resource_spills_task_to_second_node_and_cleans_cluster` | 1 passed in 1.26s | 5children、两1MiB store、10s work/3s close、7端口 |
| `tests/integration/test_cross_node_dependency_pull.py::test_store_backed_dependency_pulls_to_consumer_node_before_direct_push` | 1 passed in 1.38s | 5children、两1MiB、64KiB值、10s/3s、8端口 |
| `tests/integration/test_lineage_reconstruction_path.py::test_stored_task_output_reconstructs_with_same_object_id` | 1 passed in 0.95s | 3children、1MiB、一次drop/reconstruction、10s/3s、5端口 |
| `tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg` | 1 passed in 1.24s | 5children、两1MiB、一次Node crash、15s/3s、7端口 |

以上均检查owner与gate/trace实际端口、全部managed PID；PG旧listener创建
移入try，失败路径仍close/shutdown并检查端口/PID。PGcreate/remove同步
事务仍只有外层实验硬界，deadline不等于取消ACK。预期victim crash仍为
unclean，不把survivor正常退出改写成全局clean。该PG集成检查运行中
participant loss，不冒充surviving ambiguous lease ACK窗口的进程证据。

`tests/unit/test_node_dependency_pull.py::test_concurrent_localizers_pull_once_and_second_uses_local_replica`
另单独 **1 passed in 0.17s**（Core-only PG修复前）：两真实线程、零集群/
网络，16387B对象、两份512KiB容量store、真实锁串行一次pull；2s gate/
锁界、3s join共享deadline＋1s finally，最后unpin/delete。

学习路径基础1→7现连续，3A–3D在之后，所有合同和原章节锚保留。
allowlists仍 **84 MP/23 L1**；其余runtime/legacy安全闭环、全部同版
出口、完整故障组合和GCS fidelity仍开放。本轮无heavy测试。

### 先前 bounded-reference-close checkpoint（2026-09-07）

bounded-reference-close 固定入口最终复验：**2187 passed, 12 deselected in
7.29s**（首次7.22s），144 whole files＋另30文件126 exact selectors。
最终 `compileall -q conftest.py src tests scripts examples` 通过，仍非默认全gate。

新增 `ObjectRef.close(*, timeout=None)` 只限制本地释放receipt等待；超时
raise TimeoutError、handle保持closed且同一release继续由runtime推进，再次
close不重复enqueue。它不保证remote owner ACK或physical GC。26个纯合同
单独 **26 passed in 0.22s**：真实weakref.finalize、tinyowner、fakeclock/Event
最多6次脚本wait，覆盖local/foreign/attempt、默认None、zero、非法预检、
detached、receipt先捕获、同一deadline及平台wait上限分段。无真实等待。

另完整审查12个旧observability/membership/startup文件：8whole **66 passed
in 0.23s**；4mixed只运行24pure exact＋4新ASTguard **28 passed in 0.23s**。
9个真线程/Pipe/TCP/文件用例保留原body与IDs，显式heavy，未运行。
`test_node_membership_snapshot.py` 的generic stop bareNode fixture缺空
transfer registry已补，原stop断言保留；未绕过生产cleanup。

七个原教学main分别由唯一harness运行，无重复实现、无mockruntime或额外
测试线程/子进程。每Node1MiB；最多64KiB应用数据；除了06一次physicaldrop
＋一次重建，均无故障。get在初始化后共享10秒，引用close共享3秒；05的
Worker也使用public close(timeout=3)。01trace额外上限2秒且不超workdeadline。
04 Actor/07 PG同步控制没有per-call取消deadline，整个实验只由30秒runner
硬界控制；超时不能当作取消成功、normal shutdown或资源清理ACK。

以下参数均先完整静态审查，再单独调用 runner（不能整文件或参数集合运行）：

`tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster`

| exact 参数后缀 | 结果 | 资源／身份观察 |
|---|---|---|
| `[example01]` | 1 passed in 0.92s | 3 children／1MiB／1 Task／5 endpoints；最终harness复验 |
| `[example02]` | 1 passed in 1.25s | 5 children／2MiB／1 spillback Task／7 endpoints |
| `[example03]` | 1 passed in 1.21s | 5 children／2MiB／64KiB＋2 Tasks／7 endpoints |
| `[example04]` | 1 passed in 1.35s | 6 children／2MiB／1 Actor＋3 calls／8 endpoints；创建后立即记ActorPID |
| `[example05]` | 1 passed in 1.03s | 4 children／1MiB／parent＋child／6 endpoints |
| `[example06]` | 1 passed in 0.92s | 3 children／1MiB／1 Task两次执行／5 endpoints |
| `[example07]` | 1 passed in 1.16s | 5 children／2MiB／2 bundles＋2 Tasks／6 endpoints，无trace，真实remove一次 |

调用形式（一次仅替换一个参数ID）：

```bash
python scripts/run_bounded_test.py 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

观察器转发真实init/shutdown/close并限制次数，首个shutdownreport单独保留，
fallback不能替它证明clean；完整PID及GCS/Node/Worker/owner/trace/Actor端口
按实际context/endpoint核验退出。runpy用非__main__名，函数通过cloudpickle
by-value送Worker，spawn不重跑main。01初版harness0.93s为历史，不替代上表。

另 `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker`
使用公开close的语义gate回归 **1 passed in 1.07s**（4children、1MiB、2tinyTasks、
10秒work/3秒cleanup，30秒外界）；仍检查CPUyield/accounting及6端口/PID。
allowlists现 **84 MP/23 L1**，不是整套同版验收；本轮无重型测试。完整安全
分类、其它smoke生命周期修复、全部gate和GCS fidelity仍开放。

### 先前 control-boundary checkpoint（2026-09-07）

control-boundary 固定选择最终复跑：**2067 passed, 12 deselected in 7.10s**
（首次扩展为 7.19s），已包含最终 manifest evidence 更新；134 完整文件＋另
26 文件的 102 exact selectors，仍不是完整默认 gate，清单不是运行结果。
最终 `compileall -q conftest.py src tests scripts examples` 通过。

本轮只重构 `control.py` 的职责封装：GCSLite 委托既有
PublicationControlAdapter 进行注册发布／Node-loss／owner-death publication
推进，不再操作其私有锁、tickets、cleanup。membership／owner-wide fence
仍归服务，observer/RPC callback 每次传入；所有远程效果在 composition lock
外，图与终态在锁内。没有第二后端、新线程／协议或改变 GCS 同步发布合同。

安全审查覆盖两批共 24 个旧文件：17 whole pure **205 cases**；7 mixed 文件
中的 **46 pure cases** 用 39 exact 函数选择，原 **29 runtime/thread cases**
保留正文与 IDs，去继承 unit 后逐项 heavy。它们尚无完整有界生命周期审查，
不是功能失败，也没有用排除陈旧断言获得通过。

- whole recovery/publication 的十文件单独 **154 passed in 0.95s**，最多
  1 KiB store／2 slots／4 transfers、4-producer DFS、12 个手动效果，无 runtime。
- whole Actor 的七文件单独 **51 passed in 0.23s**，纯 registry/route/arguments
  与同步 callbacks，无 socket/线程启动；Actor argument 模型不是公共 API 接线。
- 两组 mixed 的 39 exact 函数和两份新 AST guard 合计 **53 passed in 0.32s**。
  guard 只固定已审分类，不认证未来 imports/fixtures。
- `test_publication_control_boundary.py` **4 passed in 0.22s**，至多2份Node/Worker
  metadata、1 publication、1真实child owner表，8手动轮次；动态callback/票据
  重入、最后child ACK触发owner death后旧driver不可graph/resolve，再精确收尾。
  只模型化同步交错，不冒充真实线程竞态。

基础 Task／trace 三个旧 exact IDs 已收紧为 3 children、1 MiB、10秒共享工作、
3秒真实finalizer，再无条件shutdown。全3 PID和5端口（含Driver owner/trace）
核验。成功仍检查task_finished→object_ready及RPC因果；用户异常仍只有attempt0，
不因配置max_retries=3而系统重试。golden不枚举所有GCS发布RPC，不能据此推断
普通成功不依赖GCS。以下各项静态完整审查后分别运行，外层30秒＋有界回收：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_task_path.py::test_one_node_one_worker_task_path` | 1 passed in 0.93s |
| `tests/integration/test_cross_process_trace.py::test_one_task_emits_cross_process_golden_trace_and_cleans_up` | 1 passed in 0.94s |
| `tests/integration/test_cross_process_trace.py::test_application_error_trace_is_terminal_without_system_retry` | 1 passed in 0.92s |
| `tests/integration/test_foreign_late_output_replica_cleanup_path.py::test_foreign_late_replica_is_collected_and_old_messages_preserve_reconstructed_epoch` | 1 passed in 1.53s |
| `tests/integration/test_precomplete_output_owner_death_path.py::test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds` | 1 passed in 1.49s |
| `tests/integration/test_contained_cycle_control_path.py::test_registered_unified_graph_rejects_cycle_then_real_owner_gc_releases_container` | 1 passed in 0.92s |

F5/F6/F7沿用各自上轮固定成本，不并行pytest、无重型测试。allowlists仍
**77 MP/23 L1**，不是全部同版通过。F4与Worker-owner旧结果不自动更新到本次
control重构；完整安全分类与其它教学入口的有界close/创建/teardown仍待完成。

### 先前 foreign-custody/foundations checkpoint（2026-09-06）

foreign-custody/foundations 固定选择最终复跑：**1805 passed, 12 deselected in 6.10s**
（首次扩展为 6.15s），已包括最终 manifest/runner 变更，
114 完整文件＋另 19 文件的 63 exact selectors；不是完整默认 gate。
新增完整静态审查的八份纯文件为 Actor trace/protocol、cross-node pull protocol、
Task hold protocol、typed borrower sources、Worker pool protocol、PG protocol、
multi-return owner model。它们单独 **93 passed in 0.25s**；最大 16 个 return IDs、
4 个 borrower，仅小型 DTO/owner/recovery 模型，无实际 runtime。

`test_targeted_reconstruction_protocol.py` 的三个陈旧成功 fixture 不再依赖
无 publication 的 SUCCEEDED/descriptor-only 终态：现在真实调用内存
Prepare→ARM→Complete→Query，selected 原 indices 0/2 与 healthy 1 不混淆，
mixed/INLINE 从完整 envelope 恢复，CPU 真实释放；fake Worker 只提供 liveness。
新增明确拒绝旧 fixture 的用例，原 SYSTEM_ERROR partial-seal orphan 合同未变。
全文件 **10 passed in 0.18s**；1 KiB store、至多三份 tiny replicas，局部
tripwire 禁止 thread/process/socket/等待。没有改生产协议来迁就旧断言。

`test_trace_export.py` 去掉继承 unit，四个原始真实文件写入函数分别标 heavy，
本轮不运行或替换其 I/O。tmp_path 小不等于 fsync 时间有界。两个拒绝函数
分别标 unit（参数展开共 4），无 tmp_path／文件探测，写入口和 runtime
tripwire 只包围该函数；invalid path 仍执行真实 writer 的前置参数验证。
新增 AST 分类合同只保持已审决定，不自动认证未来 body/import。两个 exact
函数与完整 classification file 合计 **29 passed in 0.19s**。

F6 新场景完整静态复审后独立通过 30 秒 runner：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_foreign_late_output_replica_cleanup_path.py::test_foreign_late_replica_is_collected_and_old_messages_preserve_reconstructed_epoch` | 1 passed in 1.55s |

5 children、2×1 MiB、1 tiny Driver put、factory＋producer＋不执行的consumer，
producer 初始／重建两次，总 3 次用户执行；8 KiB padding、1 次 Node crash＋
1 次 public get 显式 targeted reconstruction。工作 18 秒、两个 semantic gate
各至多 8 秒、共享 3 秒引用 finally；1 listener/控制连接，无新测试线程。
先截取真实 GCS adopted ACK，再截取真实 foreign consumer Grant，latched
DROP 后才发送原 location report；原 owner 的 RETIRED/custody 先于 cancel，
测试观测自治 GC 的物理不存在后才重放旧 Drop，不以测试主动删除制造通过。
重建后再重放原 foreign report/Drop，验证新 epoch/bytes 与 healthy sibling
保持；child snapshot 在真实 retained/local/contained 和 borrower/submitted
收尾屏障后比较，GCS adopted 不冒充 finish 完成。所有 PID/端口退出已核验。

首次权限审核超时，未创建测试进程；按工具允许重试一次审核后实际运行通过，
不计为测试失败。生产 runtime 未改动，无 fake ACK 或直接 owner-state 注入。
最终 `compileall -q conftest.py src tests scripts examples` 通过。allowlists
**77 MP/23 L1**，并非全部 exact IDs 同版通过；F1–F7 仅各自指定的切片完成，
完整安全分类、roadmap 出口和普通成功 GCS fidelity 合同仍开放。

### Worker-owner death-view checkpoint

Worker-owner death-view 增量的冻结入口已在 manifest／scope 合同更新后复跑：
**1697 passed, 12 deselected in 6.12s**。范围为 105 完整文件＋另 18 文件中
61 exact selectors；此前同一显式选择为 6.09s。两者均非完整默认 gate。
新增死亡视图协议、Worker Core 消费、Driver 发布及同 owner custody 的纯合同，
包括同 membership epoch 的晚到死亡事实、真实 survivor 安装 ACK 校验、部分
应用后异常重放与同 owner provisional/final token 区分；没有运行时线程或等待。

以下四项完整复审后分别经 30 秒 runner 在当前 runtime 复验，无并行 pytest：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_precomplete_output_owner_death_path.py::test_owner_death_after_intent_fences_unmaterialized_output_and_cleans_live_executor` | 1 passed in 1.53s |
| `tests/integration/test_precomplete_output_owner_death_path.py::test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds` | 1 passed in 1.44s |
| `tests/integration/test_contained_cycle_control_path.py::test_registered_unified_graph_rejects_cycle_then_real_owner_gc_releases_container` | 1 passed in 0.91s |
| `tests/integration/test_worker_owner_node_loss_path.py::test_live_worker_owner_retries_armed_child_after_certified_remote_node_death` | 1 passed in 1.30s |

F4/F5：5 startup children＋1 Worker replacement，2×1 MiB store，2 logical
Tasks、零 retry、1 tiny Driver child、8 KiB output；1 次精确 owner Worker
SIGKILL。工作 15 秒、已有 publication gate 10 秒、引用/gate finally 3 秒。
真实 child-release ACK 证明全 Node owner fences 后即开 gate，使同一存活
executor 能返回 Finalize ACK；不先等待 owner-cleaned 造成测试自锁。被观察的
Node entry 只核验原效果，不驱动清理；启动端口、替换 Worker 的新端口和所有
PID 都核验退出。supervisor owner-fenced 分支另有三项纯 phase 回归。

F7：3 children、1 MiB、1 tiny put＋1 Task、无 kill／监听器／新测试线程，
工作 12 秒、finally 3 秒。两个额外 publication 是明确的 metadata-only
控制输入，使用真实注册身份／INTENT／PREPARE／ABORT；不声称它们已有
public refs、child holds、ARM 或 Complete。正向 COMMIT／RELEASE 由独立
真实 public Task 与 owner GC 提供，不能将其描述为用户代码构造了 ObjectID 环。

Worker-owner：5 children、2×1 MiB、1 tiny put、factory＋child 两 logical
Tasks／3 次用户执行、8 KiB、一次 Node crash 与一次 SYSTEM retry。1 listener
两连接，无额外测试线程；工作 15 秒、gate 10 秒、finally 3 秒。factory
持有 A 的 CPU，先握手后启动 child 以保证真实 spillback B；A 从本地 Node
读取 Driver-certified view 自主恢复，Driver 在 READY 前只读 owner metadata。
初次真实验证发现 same-owner retry 的 provisional/final hold 相同，修复其
命名空间后通过；不能说初稿一次通过。此用例不是 F6 的迟到副本验收。

该 checkpoint allowlists **76 MP/23 L1**，仍非整套同版验收。F6 当时开放，
现由上方独立场景补齐；安全分类、完整出口及 GCS fidelity 取舍仍开放。
本 continuation 最终编译与固定子集记录见上方。

### 先前 mixed-UNKNOWN checkpoint（历史）

mixed-UNKNOWN 增量：**1590 passed, 12 deselected in 5.97s**，由固定 reviewed-pure
入口执行，101 个完整文件＋另 17 文件中的 51 个 exact selectors，仍非全 gate。
本轮增加 Core envelope 迟到的 UNKNOWN 两个纯分支；新审 Worker blocking
binding/protocol、PG reducer；Node 的六个纯 CPU 账本合同改用真实 Prepare/ARM
再 Complete，保留原成功释放／代际／CPU debt 断言。9 个原运行时用例仍保留为
heavy（Core blocking 7、Node race 1、contained graph race 1），不运行也不删除。
分类 AST 守卫只锁当前决策，不给未来代码自动安全认证。

以下 exact 场景先完整静态审查、再分别经 30 秒 runner 运行：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_mixed_borrowed_output_unknown_path.py::test_armed_unknown_mixed_borrowed_outputs_release_each_old_slot_before_one_retry` | 1 passed in 1.51s |
| `tests/integration/test_unreported_complete_node_loss_path.py::test_locally_completed_unreported_output_crash_is_unknown_then_cleans_before_retry` | 1 passed in 1.42s |
| `tests/integration/test_targeted_borrowed_output_unknown_path.py::test_targeted_borrowed_arm_loss_retries_only_lost_slot_and_preserves_healthy_sibling` | 1 passed in 1.41s（最终共享harness） |
| `tests/integration/test_targeted_borrowed_output_unknown_path.py::test_targeted_mixed_borrowed_arm_loss_retries_selected_batch_and_preserves_healthy_sibling` | 1 passed in 1.74s |

F1：5 children、2×1 MiB、1 tiny child、两 logical Tasks、8 KiB padding，
单 Node crash＋最多一次 retry；12 秒工作/10 秒 gate/3 秒 finally，6 次物理查询。
两槽共享 child 但各有 pro/final hold；无 Complete 不 KEEP INLINE，真实 cleanup
后 retry，新 sibling 分开 GC，最后 sibling 才释放 lineage。

F3：相同 5 children/2×1 MiB/小对象，明确两个受控事件：发布通道隔离，然后
一次 Node crash。只在 test-module spawn wrapper 中阻断 terminal 发送及
Complete/outcome 交付；收到 `LOCAL_COMPLETE_UNREPORTED` metadata 帧前已经
断言真实 journal Complete、lease COMPLETED、CPU 已释放及实际 terminal 尝试。
GCS 独立查询仍为 ARM-only，observer witness 不送 Core。gate/工作各 10 秒，
finally 3 秒；无 fake ACK、新生产开关或额外测试线程。

F2共享harness：初始成功＋公开drop预置LOST＋targeted重建＋Node crash＋SYSTEM
retry，max_retries=2计数准确；5children/2×1MiB、3次producer执行、
15秒工作/10秒gate/3秒finally。两个exact IDs分别复验。narrow用两return和
5次物理查询；mixed用三return、初始两个8KiB存储对象，丢失slots1/2后
公开wait在Core锁内合并OPEN（不手改owner状态），再形成mixed selected batch，
11次物理查询。测试函数明确根据真实attempt去掉slot1后续padding，以覆盖
合法tier变化，不把它描述为确定性函数。健康slot0 snapshot/hold始终不变，
selected ordinal0/1对应原index1/2；两target分开GC后仍留健康hold/lineage，
最后健康sibling才释放lineage。初稿单target 1.49s只是历史记录。
上述均检查真实清理与 PID/端口退出，不并行 pytest、不运行重型测试。
最终源码 `compileall -q conftest.py src tests scripts examples` 通过；
allowlists **72 MP/23 L1**，并非整套allowlist同版已验收。

### 先前 safe-entry/foundations checkpoint（历史）

safe-entry/foundations 增量：**1539 passed, 12 deselected in 5.82s**。
已审清单为 98 完整文件＋另 15 文件中 36 exact selectors，仍不是全套 gate。
本轮在原清单上完整静态审查并纳入 8 个基础文件（ID/Hybrid、PG/Actor、recovery、
dependency/pull、blocking notifier、trace、CPU yield、collection safety），再纳入
function-registry 的 7 个和 Worker-side Core 的 9 个纯 exact IDs。

默认误运行现在显式失败：根 conftest 在标准测试模块 collection 之前拒绝无文件
选择器、目录或 `--pyargs`，提示使用固定清单；**不把默认全量静默替换成子集**。
裸 pytest 和目录 `--collect-only` 两个诊断都以 usage error 4 返回，未收集测试。
这不是插件或 conftest 导入沙箱；显式文件仍须逐项审查。正常使用：

```bash
python scripts/run_reviewed_pure.py --list
python scripts/run_reviewed_pure.py
```

三份混标文件保留全部 30 个原用例/ID：function-registry 7 pure/1 heavy；
Worker-side Core 9 pure/11 heavy；Node shutdown 2 heavy。heavy 是未验收的
真实线程/Core/等待，未运行、未删除，也未用 fake 替换原合同。AST 测试只锁分类，
不自动证明未来代码安全。blocking-notifier 的 9 个纯合同原先会真实 backoff，
现使用同步 delay recorder 并安装 wait/socket/thread tripwire。

新增一项进程验收，完整静态审查后单独经 30 秒 runner 运行：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_borrowed_output_unknown_path.py::test_armed_unknown_borrowed_output_releases_old_holds_before_retrying_same_live_child` | 1 passed in 1.34s |
| `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker` | 1 passed in 1.07s |

5 startup children、2×1 MiB、1 tiny Driver put、blocker＋producer 两 logical Tasks、
8 KiB padding、1 次受管 Node crash、最多 1 retry。已有 AFTER_ARM gate 证明
物理 seal/promotions，GCS 仍无 Complete；真实 resolution 和 live child owner
的 old contained release ACK 先于 Core retry。独立 death consumer 自然删除旧
borrower，原 Task hold/lineage 跨 retry；新 output hold 不复用旧 token。
10 秒 post-init 工作、3 秒引用/gate finally，30 秒外层截止另有有界清理；
5 PID/7 地址（含 gate、Driver owner）全部退出。没有新测试线程、合成 death、
伪造 Release/清账，未修改生产故障路径。只覆盖单 borrowed STORED UNKNOWN，
不等于 mixed/targeted 或 terminal-lost-after-Complete。allowlists **68 MP/23 L1**。

CPU-yield 原 exact ID 保留，重新完整审查并收紧：4 子进程、1 MiB、2 tiny Tasks、
max_retries=0；共享 10 秒 deadline 从 Driver 传至父子 Worker，包括各次 socket
收发/API。父子引用均以真实 finalizer＋最多 3 秒等待关闭，listener setup 位于
finally 保护内；检查 4 PID/6 地址和无 CPU debt/活跃 lease。没有更改生产 CPU
yield 算法。两项进程验证分别运行；最终新清单回归及
`compileall -q conftest.py src tests scripts examples` 均通过，未运行重型测试。

### 先前 cross-cleanup checkpoint（历史）

cross-cleanup 增量：**1436 passed, 12 deselected in 5.74s**。
范围已冻结为 [reviewed manifest](../scripts/reviewed_pure_manifest.json)：90 个完整
文件＋另 13 个文件中的 20 个 exact selectors。它是已审查子集，不是完整默认 gate。

在已安装测试依赖的项目虚拟环境中，从仓库根目录使用：

```bash
python scripts/run_reviewed_pure.py --list
python scripts/run_reviewed_pure.py
```

`--list` 只读取 JSON 和所列文件路径，不导入或收集测试；执行模式仅起一个隔离
pytest 子进程，保留 `-m unit`，禁止 plugin autoload/cache 和环境额外 pytest
选项，并显式排除已知 12 个非纯用例。执行截止 30 秒，复用有界进程树清理；
清理中的每次 `ps` 快照也有 0.25 秒上限。没有目录 glob 或任意参数透传。
清单记录审查范围和历史证据，不是永久安全认证；修改模块、fixture、import 或
运行时代码后仍需审查，再手工纳入新范围，不能自动扩成整个 tests/unit。

本轮新增/复审的独立文件：

```text
tests/unit/test_output_replica_node.py
tests/unit/test_cross_cleanup_receipts.py
tests/unit/test_owner_finalize_replica_receipts.py
tests/unit/test_reviewed_pure_runner.py
```

分别为 22/45/14/31 个展开用例；最多一个 1 KiB 内存 store、两代 output、
实际 journal/Node 处理器；owner finalize 另有一个 fake Worker 和纯资源账本。
无实际线程/socket/子进程/等待；runner 契约中的 Popen、wait、信号及清理均 fake。
现有 runner 与 Node 删除测试各增加一例；source pin 拒绝断言改为更精确的
删除 fence 原因，保留原有 close/无 chunk/完整收敛断言。
跨 authority、旧 receipt 与新 epoch、delete/forget 生效前后异常、partial/
never-created claim、owner-finalize Worker ACK 丢失均有明确边界，不能外推全矩阵。
新入口在纳入增量前曾独立跑过历史 1322 子集：1322 passed/12 deselected/6.04s。
默认全 unit、重型或规模不明测试仍禁止运行。
最终 1436 选择集已由新入口完整运行；`compileall -q src tests scripts examples`
也通过。首次扩展回归仅一例失败：source Pin 现先命中删除 fence，旧测试仍断言
descriptor mismatch 字符串；改为精确 fence 断言后复跑通过，未放宽生命周期验证。

以下进程用例均先完整静态审查，再经 30 秒 runner 各自运行，无并行 pytest：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_cross_cleanup_receipt_path.py::test_publication_rollback_receipt_replays_after_same_object_retry_seals` | 1 passed in 0.95s |
| `tests/integration/test_pregrant_custody_path.py::test_second_source_loss_hands_off_first_replica_without_a_grant` | 1 passed in 1.15s |
| `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | 1 passed in 1.44s |

新增 case 仅 3 子进程、1 MiB store、1 个 8 KiB 结果、1 次系统 retry；
test-module spawn wrapper 在真实 seal 后抛一次 ObjectStoreError，实际 rollback
与 GCS ACK 完成后才 seal attempt 1。第一次 generic old Drop 只能消费已有
publication receipt，不准预先 generic Drop“预热”；随即检查原始新副本，不让
重复 get 的重建掩盖误删。子进程正常退出时验证精确事件计数，3 PID/4 端口清理。
工作截止 15 秒、引用 finally 3 秒，外层 30 秒包含启动和 shutdown，另有有界
清理宽限。已有两项回归各为 5 startup（owner-death 另有 1 replacement）、
2×1 MiB、固定任务/故障；详情保留在各 test 文件头。所有结果基于最终 runtime。
allowlists **67 MP/23 L1**；不把本轮三项通过当作全部 allowlist 或完整 K0/K1。

### 先前 abandoned-submitter checkpoint（历史）

abandoned submitter增量：**1322 passed, 12 deselected in 5.47s**。
下方1269选择集加6个完整pure文件，共86完整文件＋另13文件20exactselectors。
`compileall -q src tests scripts examples`通过；未跑默认完整unit/重型/GPU测试。

```text
tests/unit/test_collected_replica_history.py
tests/unit/test_abandoned_dependency_protocol.py
tests/unit/test_abandoned_replica_owner.py
tests/unit/test_abandoned_dependency_node.py
tests/unit/test_abandoned_dependency_interleavings.py
tests/unit/test_abandoned_dependency_registry.py
```

分别11/20/6/7/3/6展开case，最大2pure ownerCore、2×1KiB、2tinyinputs、
1真实Nodegrant/3轮手动driver；wire/registry只有metadata。死亡由typed
GCS输入经实际proof/deathconsumer验证，不冒充真正进程退出；Node/owner
Drop与hold退役真实。RUNNING样例保留pins/CPU到实际Complete reducer；
GC-first put保6字段compacthistory且不复活；首owner报告＋GCS都失败后
下一轮仍交接第二owner；正常ACK并发不隐去active driver。
旧dead-child回归仅适配新增短RPC options；partial源丢失fixture修正为5次
source调用（含失败Pin的exactClose），没有放宽原authority/GC断言。

| Exact process node ID | 结果 |
|---|---|
| `tests/integration/test_abandoned_dependency_custody_path.py::test_dead_submitter_hands_granted_input_back_to_live_owner_without_child_execution` | 1 passed in 1.35s |
| `tests/integration/test_pregrant_custody_path.py::test_second_source_loss_hands_off_first_replica_without_a_grant` | 1 passed in 1.16s |
| `tests/integration/test_transfer_pin_requester_death_path.py::test_requester_node_death_closes_only_its_source_transfer_pin` | 1 passed in 1.27s |

新case：5startup＋1replacement、2×1MiB、1Driver8KiBput、2logicaltasks。
S Worker parent经nested输入取得真实borrower，embeddedCore提交B child，
在真实Grant到达但未返Core前仅自身os._exit(17)，不任意killPID。NodeB
真实GetWorkerState确认S死→ABANDONED→ReportAbandoned→活Driverowner
CUSTODY_ONLY；真实child outcome证明无Complete/结果/Push执行。
一份empty未Push probe识别现replacement，finally也exactCancel；18swork/
3srefs及probe cleanup，再正常shutdown，完整GC/6PID/7端口检查。
外层runner每项30s执行截止+有界清理宽限，互不并行；新case与pin回归
最终源下运行，pregrant在最后窄改动前。allowlists **66 MP/23 L1**。

没有以这个GRANTED活owner纵切片外推RUNNING/GC-first等pure组合已E2E。
更宽死亡/发布未知/多owner回收矩阵、typedabsence、默认安全gate和GCS
还原度仍未完成。

### 先前source-transfer checkpoint（历史）

source-transfer-pin增量：**1269 passed, 12 deselected in 5.40s**。
下方1226选择集加5个完整pure文件，共80完整文件＋另13文件20exactselectors。
`compileall -q src tests scripts examples`通过；未跑全unit或重型/GPU测试。

```text
tests/unit/test_transfer_pin_protocol.py
tests/unit/test_transfer_pin_outbox.py
tests/unit/test_transfer_pin_cleanup.py
tests/unit/test_transfer_pin_death.py
tests/unit/test_transfer_pin_close_race.py
```

分别19/8/9/6/1展开case：wire深验证、纯outbox、实际Node Pin/Release和
先Release后Pin墓碑、effect-then-error、ALIVE/unknown/wrong Node death拒绝、
active/inflight死亡和首次解锁抢close ticket。最多3passive Nodes/3×1KiB
（其余最多2×1KiB）、2sessions、固定smallbadproof表、至多3前台尝试和
1～2手动后台drive，无runtime/GCS/线程/socket/等待。typeddeath明确不等于
fixture物理退出；finally正常释放真实pin。outbox rounds按关闭轮次计，
三次前台RPC共用ticket且只settle一次，因此不等于RPC次数。

旧pregrant source-missing用例增加一次真实Release-before-Pin关闭回执（4→5
调用），原sourceRelease ACKloss用例现在必须手动取得第四个真实ACK才clean，
不把已确认副本custody冒充源pin确认。RPCfixtures支持并由新测试断言短timeout
和绝对deadline，未恢复长超时或绕过生产调用。

| Exact process node ID | 结果 |
|---|---|
| `tests/integration/test_transfer_pin_ack_loss_path.py::test_real_pin_ack_loss_closes_unknown_source_session_before_rejection` | 1 passed in 1.18s |
| `tests/integration/test_transfer_pin_ack_loss_path.py::test_three_real_release_ack_losses_retry_from_the_existing_node_outbox` | 1 passed in 1.21s |
| `tests/integration/test_transfer_pin_requester_death_path.py::test_requester_node_death_closes_only_its_source_transfer_pin` | 1 passed in 1.30s |
| `tests/integration/test_pregrant_custody_path.py::test_second_source_loss_hands_off_first_replica_without_a_grant` | 1 passed in 1.14s |

均完整静态审查后各自通过30srunner，无并行pytest。ACKloss每项5children、
2×1MiB、1Driver8KiB put＋1不执行consumer；仅testmodule顶级spawn wrapper
在B Node验证真实回包后丢1Pin或3Release ACK，保留原setsid入口。child正常
退出前检查Pin/Chunk/Release计数分别1/0/1或1/1/4，Driver验证exitcode0、
原错误/noPush/noRetry、真实custody/GC及5PID/6端口消失，无额外观察通道。
requester-death用5startup、1put、0用户task、两显式Node协议sessions；只
退出B受管进程组，先观察source自动拒旧chunk/新Pin，再Release必须releasedFalse，
存活sameobject reader仍可读、仍阻Drop，最后正常Release/GC。该例不是
真实target reader pipeline。18swork/3sfinally不含startup/shutdown，runner
30s执行截止后有TERM/KILL/reap有界宽限。当前allowlists **65 MP/23 L1**。

仍未完成：提交者死亡自主副本交接、更宽三方死亡/GC交错和typed absence、
默认安全gate与GCS还原度。上述sourcepin验收不代表全部ownership出口通过。

### 先前pre-grant checkpoint（历史）

pre-grant inventory增量：**1226 passed, 12 deselected in 5.32s**。
下方1155选择集加8个完整pure文件，共75完整文件＋另13文件20exactselectors。
`compileall -q src tests scripts examples`通过；未跑默认全gate或重型/GPU测试。

```text
tests/unit/test_handoff_push_admission.py
tests/unit/test_lease_dependency_inventory.py
tests/unit/test_lease_dependency_registry.py
tests/unit/test_pregrant_dependency_custody.py
tests/unit/test_pregrant_reconciliation.py
tests/unit/test_lease_custody_frontier.py
tests/unit/test_pregrant_shared_replica.py
tests/unit/test_pregrant_pg_lost.py
```

分别3/39/13/6/3/5/1/1展开case。wire/registry仅小metadata；Node/Core组合最多
2threadless Core、2×1KiB store、2tiny输入，最多6次source transfer、2手工
replay，无runtime/用户执行。真实sealed副本、Cancel、owner receipts和Node
ACK均经实际reducer；metadata/snapshot连续两异常在精确Cancel中恢复，
不靠测试清authority。共享副本用两个独立真实请求验证首取消不删除/不unpin
第二lease；PG只做单Ready＋STOP的同步dispatcher路由，不冒称PG故障E2E。

旧local/ambiguous/multi-owner/late-replica纯fixture迁移到真实Node库存ACK；
不返回fake成功、不移除quarantine/owner-death断言。中间审查选择出现45失败
主要是旧RPCfixture尚不识别新ACK及调用数变化；逐项修正后当前选择全部通过。

| Exact process node ID | 结果 |
|---|---|
| `tests/integration/test_pregrant_custody_path.py::test_second_source_loss_hands_off_first_replica_without_a_grant` | 1 passed in 1.14s |
| `tests/integration/test_ambiguous_grant_custody_path.py::test_lost_grant_replies_cancel_and_transfer_both_input_replicas` | 1 passed in 1.26s |
| `tests/integration/test_output_surviving_replica_path.py::test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss` | 1 passed in 1.29s |

每项先完整静态审查，独立30s runner，未并行pytest。新pregrant：5startup
children、2user tasks、2×1MiB、两个8KiB put；target Request发送前8s gate，
main公开drop测试自己的foreign源，Node先seal本地输入再真实Reject，无Grant/
Push/retry。Cancel返回唯一partial descriptor；ACK前Node不能clean、ACK后
clean，最后GC与5PID/6端口清理。18s post-init work＋3s引用finally不包括
cluster shutdown；runner到30s开始TERM/KILL/reap及有界宽限。最终两项小
身份/恢复检查后只复跑新pregrant，另外两项不是全部同版覆盖。
runner **62 MP/23 L1**。

未完成：提交者死亡后的自主owner交接、source Pin ACK未知与持久release重驱、
target死亡后的sourcepin处理、广泛owner/GC/death组合、安全默认gate及GCS
还原度。Node保留unclean inventory是安全性，不是这些活性需求已完成。

### 先前committed-Grant checkpoint（历史）

committed-Grant custody增量：**1155 passed, 12 deselected in 5.02s**。
下方1099选择集加3个完整pure文件和1个exact selector，共67完整文件＋另13
文件20个exact selectors；不是完整默认unit。未运行重型、规模或GPU测试。
`compileall -q src tests scripts examples`通过。

```text
tests/unit/test_cancelled_grant_inventory.py
tests/unit/test_ambiguous_grant_custody.py
tests/unit/test_lease_cancel_handoff_interleavings.py
tests/unit/test_core_node_death_recovery.py::test_cancellation_consumes_death_before_rpc
```

新增分别40/12/3个展开case与1个exact迁移：深层wire与Node reducer、真实
双输入Grant最多12次ACK丢失、Cancel ACKloss/坏inventory/builder重放、
exact WORKER_LOST outcome、旧queued与解锁后单次同步重入、原错误/Node死亡。
最多2threadless Core、2×1KiB内存store、6次同步source transfer、3次后续
manual replay；无真实线程/socket/process/wait/usercode。正常终态case通过
实际owner collector删除3个剩余副本；typed Node死亡case明确不把fixture
残存bytes伪装成进程物理消失。每项有runtime tripwire。legacy Node-death
文件仅迁移指定函数，不能运行整个文件。首次exact迁移运行发现put自身wake
事件未被消费，改为验证并消费该事件；复跑通过，未清队列或隐藏计数。

本轮以下exact process IDs分别执行，互不并行：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_ambiguous_grant_custody_path.py::test_lost_grant_replies_cancel_and_transfer_both_input_replicas` | 1 passed in 1.33s |
| `tests/integration/test_local_replica_handoff_failure_path.py::test_exited_granted_executor_uses_outcome_fence_and_preserves_input_custody` | 1 passed in 1.34s |
| `tests/integration/test_local_replica_handoff_failure_path.py::test_local_route_failure_replays_custody_without_executing_consumer` | 1 passed in 1.25s |
| `tests/integration/test_multi_owner_handoff_failure_path.py::test_dead_first_owner_cancels_grant_but_hands_off_second_foreign_replica` | 1 passed in 1.35s |

全部先静态审查再由runner单项运行。新Grant丢ACK场景：5startup children、
2user tasks、两个8KiB put、2×1MiB；首真实Grant gate≤8s、public source
drop、12次真实缓存Grant回包后抛TransportTimeout、1次真实Cancel、两owner
接管与GC，无Push/kill/replacement。executor-exit场景复用route故障基线，
只终止经GCS incarnation/Node epoch/GRANTED outcome核验的B Worker，两个
输入owner不死；5startup＋1replacement、2user tasks＋1不执行的空probe。
probe在finally也exact Cancel。两新场景均18s post-init work、3s引用/probe
finalizer；cluster shutdown有自己的预算，30s runner到期开始TERM/KILL/reap，
并有有界清理宽限，不宣称所有内部timeout之和小于30s。runner **61 MP/23 L1**。

范围限制：只完成committed Grant的库存交接；无Grant但partial localization
已seal的bytes仍缺清理闭环，None库存不是absence proof。更宽取消/Push交错、
typed deletion absence、安全默认gate与GCS还原度均未因此验收。

### 先前local/foreign checkpoint（历史）

最终 **1099 passed, 12 deselected in 4.62s**：先前1083selection全部参数，
加local-handoff8case、shared-owner CAS effect-then-error1case、executor-loss
4case与3个exact迁移，共64完整文件＋另12文件19个exact selectors。后3个是
spillback文件的stored依赖准备和death-before-local-report，以及foreign stored
文件的route-write UNKNOWN case；两个mixed文件均未整份运行。

新pure文件8case：2pureCore、2×1KiB store、1真实dual-input grant、
6次source transfer RPC、真实public source drop、route/owner before/after
异常、foreignACKloss和2次CancelACKloss（最多2轮手工重放）、旧origin hold、
newepoch quarantine及中途SUBMITTED release。无GCS/真实threads/waits。
两个旧spillback exact case已迁移purefixture，foreign exact按真实UNKNOWN
预期验证；不恢复旧descriptor-success协议来使fixture通过。executor-loss
4pure当时只证明fake退出状态驱动真实Node reducer，不代表真实进程碰撞。

本轮以下exact process IDs分别执行，互不并行：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_local_replica_handoff_failure_path.py::test_local_route_failure_replays_custody_without_executing_consumer` | 1 passed in 1.07s |
| `tests/integration/test_late_output_replica_cleanup_path.py::test_late_sealed_secondary_is_rejected_then_cleaned_after_real_consumer_cancellation` | 1 passed in 1.14s |
| `tests/integration/test_multi_owner_handoff_failure_path.py::test_dead_first_owner_cancels_grant_but_hands_off_second_foreign_replica` | 1 passed in 1.28s |
| `tests/integration/test_output_surviving_replica_path.py::test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss` | 1 passed in 1.12s |

新无kill真实test：5child、2tasks、两个8KiB puts、2×1MiB；8秒grantgate、
18秒工作deadline、3秒finally、30秒runner。public drop只删精确source
副本，one-shot route字典保留原内容和运行期间的新entries，finally只disarm，
不恢复旧cache。没有测试线程/listener，没有用户函数重执行；验证cancel/
foreignACK/localrepair/error原对象及全部实际GC。runner**59 MP/23 L1**。
local exact在executor-loss production增量后复跑；其余三个结果在该增量前，
不是当前revision同版acceptance。

### 先前multi-owner checkpoint（历史）

multi-owner post-grant增量：**1083 passed, 12 deselected in 4.61s**。
下方1044项selection全部参数加以下2个reviewed pure文件，共62文件+另10文件
16个exact selectors；不是完整默认unit gate。compileall通过，未运行重型测试。

```text
tests/unit/test_location_report_custody.py
tests/unit/test_multi_owner_location_handoff.py
```

前者26个展开case：单tiny owner、current/retired metadata、CUSTODY_ONLY、GC
enqueue failure精确重放、请求/回执隔离；后者13个case：3threadless Core、
2×1KiB actual Node store、双foreign依赖真实grant共6source RPC、真实取消、
ACKloss/已注册GCS死亡/ticket与旧marker重放、quarantine前后death唤醒。
均有完整runtime tripwires，无真正线程、等待或进程；foreignhold是明确的
pre-Push isolated fixture，不冒充normal foreign-lineage全部GC。

以下process exact ID分别通过runner（30秒、从不并行pytest）：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_multi_owner_handoff_failure_path.py::test_dead_first_owner_cancels_grant_but_hands_off_second_foreign_replica` | 1 passed in 1.56s |
| `tests/integration/test_late_output_replica_cleanup_path.py::test_late_sealed_secondary_is_rejected_then_cleaned_after_real_consumer_cancellation` | 1 passed in 1.12s |
| `tests/integration/test_output_surviving_replica_path.py::test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss` | 1 passed in 1.12s |
| `tests/integration/test_foreign_stored_dependency_path.py::test_foreign_stored_dependency_pulls_node_to_node_before_push` | 1 passed in 1.06s |
| `tests/integration/test_multi_output_node_loss_path.py::test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored` | 1 passed in 1.10s |

新test：5启动children+1replacement、每Node1MiB、3user tasks/2×8KiB
Worker put、一个只观察replacement地址的不执行probelease并exactcancel；
SIGKILL前核GCS worker incarnation，Node不死；8秒grantACK gate、18秒shared
work、3秒finally、无testthread/listener。验证typed OwnerDiedError、consumer
noPush/noRetry、后续healthy owner ACK、GCS owner-wide实际物理清理、正常
foreignlineage最后输出GC及replacement PID/port。runner **58 MP/23 L1**。
最后仅增加cancelreply防别名deepcopy，未由此宣称全部allowlist同版重跑。

### 先前late-replica checkpoint（历史）

late-replica cleanup增量：**1044 passed, 12 deselected in 4.55s**，
下方971选择集全部参数，加以下5个pure文件与5个exact selectors（8个展开case）：
共60个完整文件、另10文件16个exact selectors；不是完整默认unit。
`compileall -q src tests scripts examples`通过，没有重型测试。

```text
tests/unit/test_replica_cleanup.py
tests/unit/test_node_replica_deletion_fence.py
tests/unit/test_core_late_replica_cleanup.py
tests/unit/test_late_cleanup_shutdown.py
tests/unit/test_cluster_shutdown_barrier.py
tests/unit/test_drop_object_replica.py::test_node_drop_is_idempotent_and_fences_reconstructed_attempt
tests/unit/test_drop_object_replica.py::test_node_drop_rejects_mismatched_replica_capability
tests/unit/test_drop_object_replica.py::test_missing_replica_without_matching_tombstone_is_not_acknowledged
tests/unit/test_drop_object_replica.py::test_higher_deleted_epoch_fences_stale_drop
tests/unit/test_drop_object_replica.py::test_forget_failure_is_inconsistent_then_exact_replay_finishes_cleanup
```

新queue/owner20例、Node12例、Core9例、shutdown5例；所有为同步reducers/小
内存store和有界fake RPC，完整runtime tripwires。cluster-shutdown历史fixture
原本也会创建实际Thread；本轮改成同步执行相同callback的InlineThread、有限
Event wait stub和无真实processgroup检查，并新增owner切换/join重放与unclean
本地transport停止测试。未运行drop文件其它Core/真实引用fixture。

以下process smoke分别执行（无并行pytest），单项由runner硬限时30秒：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_late_output_replica_cleanup_path.py::test_late_sealed_secondary_is_rejected_then_cleaned_after_real_consumer_cancellation` | 1 passed in 1.10s |
| `tests/integration/test_output_surviving_replica_path.py::test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss` | 1 passed in 1.10s |
| `tests/integration/test_multi_output_node_loss_path.py::test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored` | 1 passed in 1.10s |
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | 1 passed in 0.83s |
| `tests/integration/test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker` | 1 passed in 1.25s |
| `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | 1 passed in 1.39s |

新late-DROP：5children、2×1MiB、8KiB、1put＋producer＋不执行consumer；
2个现有dispatch lane上的≤8秒语义gate共用18秒工作deadline，finally两gate
放行、还原hooks、3秒ref关闭再shutdown。真实Node退出后只延迟已有grant的
report，不从死Node继续造副本；检查真实Cancel、Drop receipt、bytes不存在、
无GPU运行或重型压力、无新test线程/listener。runner为**57 MP/23 L1**。
最后的forced finalize二次校验后只复跑owner-death，未宣称同版全部allowlist通过。

未完成：foreign真实late-DROP smoke、non-RETIRED多owner失败后续副本交接、
其它cleanup authority旧删除与新epoch的typed absence证明、完整竞争矩阵。

### 先前surviving-output checkpoint（历史）

surviving-output/物化参数增量：**971 passed, 12 deselected in 4.26s**。
下方887项选择集全部参数，加以下五个已审查pure文件；55个完整文件及另9文件
11个exact case，重叠checkpoint不相加，未运行完整默认unit或重型测试。
`compileall -q src tests scripts examples`通过。

```text
tests/unit/test_output_owner_surviving_replica.py
tests/unit/test_core_output_surviving_replica.py
tests/unit/test_worker_materialized_contained_arguments.py
tests/unit/test_worker_nested_task_arguments.py
tests/unit/test_nested_argument_manifest.py
```

前三新增文件分别34/5/7个case：owner纯metadata；Core两个1KiB真内存store、
真实pin/chunk/seal/grant/location/GetObject和同步GCS；Worker最多两个child
import、8KiB输入、128KiB内存store。最后两个原文件经重新审查：Task manifest
与Worker decode全为同步fixture；Worker文件补完整runtime tripwires，显式crash
用例在执行前替换os._exit。默认marker仍不能认证其它文件。

本轮以下exact ID分别经30秒runner验证，互不并行：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_output_surviving_replica_path.py::test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss` | 1 passed in 1.09s |
| `tests/integration/test_multi_output_node_loss_path.py::test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored` | 1 passed in 1.10s |
| `tests/integration/test_output_child_owner_worker_loss_path.py::test_dead_executor_child_cleanup_precedes_retry_on_same_live_node` | 1 passed in 1.00s |
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | 1 passed in 0.82s |

新secondary case：五children、每Node1MiB、8KiB payload、1put＋2tasks、
max_retries=0、单publisher退出；真实Adopted ACK后的8秒Event gate、共享18秒
work deadline、finally无条件放行/恢复RPC/关闭refs/shutdown；无新增test线程
或listener。仅这一个exact ID加入runner，当前**56 MP/23 L1**，并非全部同版通过。
首次收集修复了测试的transport别名import；sandbox内bind被拒后按权限流程重跑。

这不覆盖DROP锁定后迟到location report的物理清理；已发现其grant cancellation
仅unpin的缺口，未以拒绝report或无条件KEEP隐藏。也不证明debug drop的
effect已执行但ACK丢失时的完整活性。

### 先前protocol-retirement checkpoint（历史）

旧协议族/源码退役增量：**887 passed, 12 deselected in 3.98s**，50个reviewed
files＋另9文件11个exact case。替代重叠767项checkpoint，不相加；compileall通过。
未运行默认完整unit、GPU或任何重型测试。选择为下方owner checkpoint参数全集，
再追加6个pure文件和6个已逐项审查的pure selector：

```text
tests/unit/test_owner_service.py
tests/unit/test_output_discovery.py
tests/unit/test_output_publication.py
tests/unit/test_multi_container_graph_protocol.py
tests/unit/test_inline_recovery.py
tests/unit/test_contained_graph_manifest_boundaries.py
tests/unit/test_contained_edge_runtime.py::test_task_reply_edge_validation_and_worker_commit_names_outer
tests/unit/test_large_object_runtime.py::test_worker_large_result_reply_contains_descriptor_not_bytes
tests/unit/test_borrowed_object_refs.py::test_worker_plain_result_does_not_require_server_or_embedded_core
tests/unit/test_worker_export_pin_rollback.py::test_worker_session_hands_failed_rollback_to_core_before_returning
tests/unit/test_public_multi_return_runtime.py::test_static_multi_return_contained_refs_require_unified_publication
tests/unit/test_core_worker_death_consumer.py::test_death_cleanup_discharges_owned_incoming_pins_without_raw_reply_authority
```

这些mixed文件的其它case不因此获得pure安全认证。新增Worker exactcase通过真实
Discovery→NodeAdapter/Journal/Store，最大2slots、128KiB；通用ReferenceExportSession
rollback契约明确不代表当前Worker发布主链。12个deselect为3socketpair＋9个L1。
旧source14份与旧test原文15份已归档（其中inline_recovery活动文件重写保留），
共享graph/source/pin/Worker合同有明确映射，不把归档当全量覆盖完成。

本轮final source下逐项运行如下30秒runner测试：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | 1 passed in 0.79s |
| `tests/integration/test_output_child_owner_worker_loss_path.py::test_dead_executor_child_cleanup_precedes_retry_on_same_live_node` | 1 passed in 0.99s |
| `tests/integration/test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker` | 1 passed in 1.22s |
| `tests/integration/test_stored_outer_node_loss_path.py::test_armed_unknown_stored_outer_node_loss_cleans_then_retries` | 1 passed in 1.14s |
| `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | 1 passed in 1.30s |
| `tests/unit/test_inline_recovery.py::test_owner_death_and_intent_admission_linearize_atomically` | 1 passed in 0.12s |
| `tests/unit/test_inline_recovery.py::test_owner_keep_drop_race_has_one_winner_and_no_payload_in_authority` | 1 passed in 0.11s |

前5项资源上界沿用下方相同smoke定义（3–6children、1MiB/Node，最多1retry/replacement）；
后2项各2线程、1秒Barrier、共享2秒join＋finally1秒cleanup，无socket/子进程。
runner仍55 MP/23 L1，未作all-ID同版通过声明。

### 先前owner-publication checkpoint（历史）

owner旧publication/retirement/GC关联退役增量：**767 passed, 10 deselected in
3.78s**，44个已审查文件＋另3文件5个exact case。取代重叠653项checkpoint，不相加。
compileall通过；仍未运行完整默认unit或重型测试。选择是下方GCS checkpoint的
全部pytest参数再追加以下6个pure文件：

```text
tests/unit/test_output_owner_resolution_preflight.py
tests/unit/test_output_owner_retired_fencing.py
tests/unit/test_output_owner_publication.py
tests/unit/test_output_owner_retirement.py
tests/unit/test_output_owner_terminal_metadata.py
tests/unit/test_object_ownership.py
```

新增fixture最多2个selected slots、小型metadata/bytes与单共享child，无运行时/
线程/网络/真实等待。覆盖bad后槽整批零mutation、pristine known/UNKNOWN正向基线、
retired attempt跨入口与跨tier fence、旧墓碑不能豁免新epoch GC、全owner状态无
TaskSpec/函数/参数/结果bytes，以及caller原plan精确重放与篡改拒绝。

以下每项经30秒runner单独执行（不是并行pytest）：

| Exact node ID | 结果 |
|---|---|
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | 1 passed in 0.85s |
| `tests/integration/test_inline_node_loss_path.py::test_unreceived_inline_result_is_lost_until_explicit_get_reconstructs` | 1 passed in 1.21s |
| `tests/integration/test_inline_node_loss_path.py::test_received_inline_result_survives_publisher_node_loss` | 1 passed in 1.09s |
| `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | 1 passed in 1.45s |
| `tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects` | 1 passed in 0.98s |

拓扑/资源上界与下方同名历史切片一致：3–5启动children、owner death最多1次replacement、
1MiB/Node、最多一次重建或2项tiny任务。snapshot旧字段断言已迁移，TaskReply同名旧
wire字段未偷偷删除；四原owner tests完整归档及SHA-256映射见retired-owner-publication。
runner仍55 MP/23 L1，10 deselected仍为3 socketpair+7显式线程case，本轮未重跑它们。

### 先前GCS/shared-source checkpoint（历史）

GCS旧后端退役、共享source抽取和dead-child补偿增量：**653 passed, 10 deselected
in 3.52s**，38个已审查文件＋另3文件的5个exact case，替代重叠旧checkpoint而非
叠加。10项排除是3个socketpair gate与7个明确L1线程测试。compileall通过；
没有运行完整默认unit、GPU、压力或规模测试。

可复现选择是下方Core/Node checkpoint命令的全部参数，再追加：

```text
tests/unit/test_publication_sources.py
tests/unit/test_output_dead_child_cleanup.py
tests/unit/test_contained_graph_protocol.py
tests/unit/test_owner_death_fence_control.py
tests/unit/test_output_control_shutdown.py
tests/unit/test_publication_owner_death_control.py
tests/unit/test_control.py
tests/unit/test_stored_contained_owner_table.py
```

上面是pytest参数列表，不是8条独立shell命令。新增pure检查包含：同一中性类型
及旧pickle/fingerprint不变、Node与adapter消费双层death-proof校验、GCS构造无旧
runtime/RPC、真实shutdown保留精确cleanup、membership已提交而freeze失败后
ALREADY_DEAD完整重放两项publication、COMMITTED/all-DROP与UNKNOWN/KEEP图效果、
owner-filtered progress/global drain及child/finalize ACK歧义。

以下exact node ID均**逐项**通过30秒runner（非并行pytest）：

| Exact node ID | 结果 | 范围 |
|---|---|---|
| `tests/integration/test_output_child_owner_worker_loss_path.py::test_dead_executor_child_cleanup_precedes_retry_on_same_live_node` | 1 passed in 1.26s | 3启动children＋1replacement、1Node保持存活、2attempts、1MiB；真实cleanup_pending→精确rollback→retry |
| `tests/integration/test_stored_outer_node_loss_path.py::test_armed_unknown_stored_outer_node_loss_cleans_then_retries` | 1 passed in 1.28s | 5children、1retry、1MiB/Node |
| `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | 1 passed in 1.46s | 5启动children＋1replacement、2tasks、1MiB/Node |
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | 1 passed in 0.91s | 3children、1reconstruction、1MiB |
| `tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects` | 1 passed in 1.05s | 4children、2tasks、64KiB、1MiB；已迁移unified的adoption/GC/outcome |

Worker-only测试初次已通过恢复主断言，但末尾错误地期望正常shutdown没有NodeDeath
记录；已改为核对同一Node incarnation的EXPECTED/exit0记录后复跑。该修正不把
PROCESS_EXIT当正常退出，也不改变恢复期Node ALIVE断言。

GCS L1同样逐项执行：

| Exact node ID | 结果 |
|---|---|
| `tests/unit/test_publication_owner_death_control.py::test_live_background_converges_owner_wide_and_publication_sagas[fence]` | 1 passed in 0.31s |
| `tests/unit/test_publication_owner_death_control.py::test_live_background_converges_owner_wide_and_publication_sagas[publication]` | 1 passed in 0.27s |
| `tests/unit/test_owner_death_fence_control.py::test_live_progress_thread_retries_nonterminal_owner_sweep` | 1 passed in 0.41s |

前两项只有1个后台线程、一次domain故障、Event gate、3秒收敛与finally有限join；
最后一项1线程、2秒Event/最多100次10ms观察、finally停止。均无socket/子进程。
runner当前**55 multiprocess / 23 L1**，不是全部同轮通过。旧测试完整归档与
覆盖/缺口映射见history/retired-gcs-publication，不能把删除旧收集项当覆盖完成。

### 先前Core/Node checkpoint（历史）

当前 Core/Node 旧发布路径退役与重建准入增量：**558 passed, 7 deselected in
3.21s**。30个明确审查文件＋另3文件中的5个exact case，构造器/线程/网络/等待
使用小型同步fixture或tripwire；不是完整默认unit gate。7项排除为3个socketpair
gate及4个Node并发探测。它取代重叠的378/462项checkpoint，不能相加。
`python -m compileall -q src tests scripts examples`通过。

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -p no:cacheprovider \
  tests/unit/test_actor_result_publication.py \
  tests/unit/test_worker_inline_publication.py \
  tests/unit/test_core_output_lease_domain.py \
  tests/unit/test_core_output_node_loss.py \
  tests/unit/test_core_output_receipt_loss.py \
  tests/unit/test_core_output_publication.py \
  tests/unit/test_core_stored_publication_adoption.py \
  tests/unit/test_stored_publication_node_death_gc.py \
  tests/unit/test_task_finish_barrier.py \
  tests/unit/test_core_concurrent_dispatch.py \
  tests/unit/test_core_shutdown_unresolved.py \
  tests/unit/test_targeted_output_publication.py \
  tests/unit/test_output_node_loss_control.py \
  tests/unit/test_output_owner_death_node.py \
  tests/unit/test_output_publication_control.py \
  tests/unit/test_output_publication_node_server.py \
  tests/unit/test_worker_unified_output.py \
  tests/unit/test_inline_publication_gate.py \
  tests/unit/test_stored_intent_gate.py \
  tests/unit/test_bounded_runner.py \
  tests/unit/test_bounded_test_modes.py \
  tests/unit/test_worker_stored_publication.py \
  tests/unit/test_worker_output_discovery.py \
  tests/unit/test_output_protocol.py \
  tests/unit/test_targeted_reconstruction.py \
  tests/unit/test_targeted_retirement_admission.py \
  tests/unit/test_targeted_owner_defer.py \
  tests/unit/test_stored_publication_node_server.py \
  tests/unit/test_inline_publication_node_server.py \
  tests/unit/test_node_publication_owner_death_finalize.py \
  tests/unit/test_multi_return_partial_seal_cleanup.py::test_partial_seal_orphan_requires_exact_drop_ack_before_retry \
  tests/unit/test_lease_completion_handshake.py::test_worker_retries_cached_completion_without_rerunning_callable \
  tests/unit/test_core_worker_crash_recovery.py::test_completed_stored_result_publishes_while_worker_remains_alive \
  tests/unit/test_core_worker_crash_recovery.py::test_completed_without_local_bytes_and_malformed_query_keep_exact_replay \
  tests/unit/test_core_worker_crash_recovery.py::test_cleanup_pending_outcome_keeps_original_attempt_until_exact_ack
```

新增合同包括：whole graph全量renewal预检、targeted退休前prerequisite、单槽退休
隔离、owner端deferred→同请求START、新凭据JOIN与queued-next非JOIN、失败锁定后的ERROR/GC、all-LOST和parent
lineage不能绕过targeted session；Node无Prepare成功拒绝，以及retained payload/
rollback/owner-death未收尾时禁止clean drain、忙锁非阻塞观察。

以下真实进程测试均以一个exact node ID单独执行，命令统一为
`python scripts/run_bounded_test.py <exact node ID>`，30秒硬超时：

| Exact node ID | 结果 | 上界/范围 |
|---|---|---|
| `tests/integration/test_recursive_lineage_reconstruction_path.py::test_recursive_lineage_reconstructs_leaf_to_root` | 1 passed in 1.10s | 4children、3tasks＋3reconstructions、1MiB |
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | 1 passed in 0.92s | 3children、2slots、1reconstruction、1MiB |
| `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | 1 passed in 1.52s | 5启动children＋1replacement、2tasks、1MiB/Node |
| `tests/integration/test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker` | 1 passed in 1.44s | 6children、1Actor、3小调用 |

同样逐项运行4个Node L1（前三各1个helper thread，最后2个thread；无socket）：

| Exact node ID | 结果 |
|---|---|
| `tests/unit/test_stored_publication_node_server.py::test_stored_complete_releases_state_lock_around_external_terminal_ack` | 1 passed in 0.16s |
| `tests/unit/test_stored_publication_node_server.py::test_stored_outcome_validates_under_lock_then_queries_adapter_lock_free` | 1 passed in 0.15s |
| `tests/unit/test_stored_publication_node_server.py::test_publication_step_external_effect_does_not_hold_node_state_lock` | 1 passed in 0.16s |
| `tests/unit/test_inline_publication_node_server.py::test_pending_terminal_rpc_does_not_lock_out_complete_or_outcome` | 1 passed in 0.16s |

前三使用0.1s锁探测、finally有限join；最后使用1s Event gate和finally共享join
deadline。最后一项原有L1迁移后加入exact allowlist及AST分类校验。runner当前
为**54 multiprocess、22 L1**，并非全部同轮通过。Core/Node旧publication已退役，
GCS/owner旧协议仍需清理；historical fixture迁移、安全分类、完整故障矩阵均未完成。
未运行默认suite、GPU、压力、性能或规模测试。

### 先前统一gate checkpoint（历史）

统一后端已接入 ordinary Task 成功主路径，含 mixed-tier contained multi-return
与 targeted publication；该checkpoint时旧Node/GCS/owner INLINE/STORED实现尚待退役。下列
16个纯文件与另外三文件中的4个 exact pure case 经静态审查后串行执行，得到
**378 passed, 3 deselected in 1.42s**，不是完整 unit gate。构造器、
RPC、时钟和等待边界使用小型同步 fixture；runner 测试仅使用假进程和 AST，
不启动被审查的 smoke。旧默认 `unit` marker 仍不能证明其余文件安全。

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -p no:cacheprovider \
  tests/unit/test_core_output_lease_domain.py \
  tests/unit/test_core_output_node_loss.py \
  tests/unit/test_core_output_publication.py \
  tests/unit/test_targeted_output_publication.py \
  tests/unit/test_output_node_loss_control.py \
  tests/unit/test_output_owner_death_node.py \
  tests/unit/test_output_publication_control.py \
  tests/unit/test_output_publication_node_server.py \
  tests/unit/test_worker_unified_output.py \
  tests/unit/test_inline_publication_gate.py \
  tests/unit/test_stored_intent_gate.py \
  tests/unit/test_bounded_runner.py \
  tests/unit/test_bounded_test_modes.py \
  tests/unit/test_worker_stored_publication.py \
  tests/unit/test_worker_output_discovery.py \
  tests/unit/test_output_protocol.py \
  tests/unit/test_multi_return_partial_seal_cleanup.py::test_partial_seal_orphan_requires_exact_drop_ack_before_retry \
  tests/unit/test_lease_completion_handshake.py::test_worker_retries_cached_completion_without_rerunning_callable \
  tests/unit/test_stored_publication_node_server.py::test_complete_gate_runs_after_node_commit_before_reply \
  tests/unit/test_stored_publication_node_server.py::test_complete_gate_failure_is_sticky_and_never_rolls_back
```

新增纯契约覆盖：child/finalize ACK 的严格重建校验；Node 非阻塞 publication
ticket 内的 owner 清理；Worker 完整死亡墓碑、pending/cached/source/import
精确释放、已准入 Push 与 drain fencing；Core 在查询中收到 bytes、DROP 后
迟到 bytes、旧 RPC 在另一 lane 完成后返回，以及 disjoint targeted successor
启动后旧 publication 仍须完成自己的 adoption/Node-loss 收尾。其余未审查
故障组合仍开放。`compileall src tests scripts examples` 也通过，但不是运行验收。

该集合包含并取代先前176项集合，不相加。新增迁移契约验证真实ObjectStore容量
失败、PINNED、Drop ACK丢失、GCS rollback ACK丢失时 `cleanup_pending=True`，
Node仍报告真实terminal/worker_alive，而Core不启动新attempt。未被列出的
同文件其他case没有因此自动获得pure安全认证。
最后补充3项witness-only outcome契约：有真实custody或已READY owner slots时
恢复exact adoption，无本地bytes时保持原身份重放，不能降为普通SYSTEM retry。

当前统一gate使用单一 `_test_output_publication_gate`，同时覆盖Complete与outcome
结果出口；pre-ARM阶段严格要求ARM intent尚未发生，ARM已开始的重放不能重新
报告更早阶段。完整/targeted身份均包含manifest digest，不逐槽冒充一个batch。

以下六个故障exact node ID **分别执行**，每次只有一个runner：

| Exact node ID | 最新结果 | 范围 |
|---|---|---|
| `tests/integration/test_stored_outer_node_loss_path.py::test_precomplete_stored_outer_node_loss_rolls_back_then_retries_survivor` | **1 passed in 1.37s** | INTENT ACK后、child effects前，清理后SYSTEM retry |
| `tests/integration/test_stored_outer_node_loss_path.py::test_post_effect_precomplete_stored_outer_node_loss_compensates_then_retries` | **1 passed in 1.36s** | 已seal/promote、ARM前，补偿后retry |
| `tests/integration/test_stored_outer_node_loss_path.py::test_armed_unknown_stored_outer_node_loss_cleans_then_retries` | **1 passed in 1.38s** | ARM后无已知Complete；GCS UNKNOWN、cleanup ACK先于retry预算 |
| `tests/integration/test_stored_outer_node_loss_path.py::test_postcomplete_stored_outer_node_loss_retires_then_reconstructs` | **1 passed in 1.43s** | Complete未交付，先SUCCEEDED+LOST，再explicit get |
| `tests/integration/test_inline_node_loss_path.py::test_unreceived_inline_result_is_lost_until_explicit_get_reconstructs` | **1 passed in 1.40s** | INLINE双出口未放行，DROP后显式重建borrowed child |
| `tests/integration/test_inline_node_loss_path.py::test_received_inline_result_survives_publisher_node_loss` | **1 passed in 1.26s** | 真实已收到bytes，KEEP不重跑旧Task |

每项五启动子进程、1 MiB/Node、至多一个重试/重建；STORED使用64 KiB padding，
INLINE使用微小对象。原测试名保留但协议与断言已迁移，不再执行旧publication
状态机。UNKNOWN是额外用例，没有替换掉promotions-before-ARM的已知失败窗口。

同轮还逐项通过 ordinary Node loss
`tests/integration/test_node_crash_recovery_path.py::test_remote_node_death_retries_task_on_survivor_and_reports_crash`
（**1 passed in 1.34s**）与mixed-contained（**1 passed in 0.94s**，exact ID见下表）。
最后的PrePush/补偿屏障修改后又复验了ARM-UNKNOWN与mixed-contained；其他记录
横跨相邻修订，不是同一revision全量复验。witness-only成功处理修改后另复验了
STORED post-Complete场景，最新结果已列入上表。

三项socketpair L1同样各自经runner执行（原文件名保留）：

| `tests/unit/test_stored_intent_gate.py::` 后的 exact 名称 | 结果 |
|---|---|
| `test_arrival_frame_round_trips_exact_incarnation_and_publication` | 1 passed in 0.16s |
| `test_targeted_arrival_frame_preserves_full_manifest_and_selected_indices` | 1 passed in 0.16s |
| `test_receiver_rejects_truncated_frame_before_peer_exit` | 1 passed in 0.17s |

这些用例在默认pure选择中是上述3个deselected；每项最多一对socket，单次发送
119字节frame，无测试线程，1秒socket timeout，30秒runner硬限时。

前一相邻revision的三条统一发布证据（mixed-contained现已复验为0.94s）：

| Exact node ID | 结果 | 资源与覆盖 |
|---|---|---|
| `tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot` | **1 passed in 0.98s** | 3 children、1 MiB、8 KiB padding；统一两槽、一个重建、per-slot GC |
| `tests/integration/test_multi_output_node_loss_path.py::test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored` | **1 passed in 1.32s** | 5 children、1 MiB/Node；真实已收到INLINE保留、STORED丢失后survivor重建 |
| `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds` | **1 passed in 1.65s** | 5启动children＋1 replacement、2 tasks、1 MiB/Node；已adopted owner death，live executor清理、source holds、physical deletion、clean shutdown |

命令格式为 `python scripts/run_bounded_test.py <一个完整 node ID>`，需要
loopback 端口权限。脚本拒绝目录、文件级选择和额外 pytest 参数，30秒硬超时
只清理它自己的进程树。当前 allowlist 为 **54 multiprocess、21 L1**，不是
全部同轮通过数。这里没有运行完整默认 gate、GPU、压力、规模或性能测试。

owner-death 新测试通过 GCS `owner_cleaned` 的因果屏障和存活 executor 身份
验证 Node→Worker ACK 链；只读 graph query 会保留历史 manifest，不能把
FOUND 当成仍有 active edges。该验收不覆盖 pre-Complete/UNKNOWN owner death。
旧故障gate已替换并完成上述迁移；完整K0/K1故障矩阵、旧publication runtime
退役、默认测试安全分类仍待收敛。已删除无生产或测试消费者的旧
`worker_stored_publication.py`，其重要契约迁至统一fixture，未通过恢复legacy
toggle让旧测试假设继续成立。

## 历史验收记录（不代表当前后端完整通过）

> 当前状态：v0.1 实现中。最近一次完整历史基线得到
> **1668 passed, 54 deselected in 11.04s**；它不是后续增量的当前完整 gate。该阶段覆盖
> `TaskReferenceHold` 绑定、普通 borrowed-reference 持久 Release obligation，以及
> Worker/Node/GCS/Core normal-owner stored-publication、reverse-GC 与
> intent-before-effect Node-loss recovery fence、GCS takeover runtime 与 Core
> owner-pull/ACK/progress full flow 接通后的轻量证据。同轮的
> `compileall src tests scripts examples` 也已通过。pytest 的项目默认配置已限制为
> `-m unit`。7 项单进程
> loopback smoke 的历史整组记录为 **7 passed in 0.28s**；沙箱内禁止绑定
> loopback 监听端口。Python 3.9 已完成静态兼容性检查和包导入
> 验证。30 秒 bounded runner allowlist 现有五十个 multiprocess smoke，并有十三项
> 独立 thread/socket L1 allowlist：其中四十二个 multiprocess 是
> **stored-outer 接线之前的 multiprocess 基线**；本轮另有专用 stored-outer smoke
> `tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects`
> 在最终 gate 改动后复验得到 **1 passed in 0.96s**。另外两个 pre-Complete
> publishing-Node-loss smoke 分别在 intent ACK 后、任一 effect 前，以及 replica seal＋
> 全部 promotion ACK 后、Complete 前触发，得到 **1.27s** 和 **1.17s**；post-Complete/
> pre-TaskReply retirement＋reconstruction 得到 **1.32s**；secondary-replica Node-loss
> promotion 最新得到 **1.09s**。同次受影响路径还逐项复验了 cross-node pull
> **1.19s**、public put/get **0.80s**、foreign stored dependency **1.04s**、foreign
> stored get/final owner fence **0.87s**。这些记录来自
> 不同验证轮次，不能表述为同轮全量复验。既有
> 三十七项此前在当前 checkout 逐项通过，新增 ownership、application-error trace、
> startup rollback、local/foreign/multi-return reconstruction、PG Node-loss、foreign
> wait/drop、public multi-return、Actor Node-loss migration、Driver-local Node recovery
> 十一项及受影响旧路径在最新改动后再次通过。
> 通过：单节点普通 Task **1 passed in 1.08s**、单节点大对象 **1 passed in 0.95s**、两节点 Hybrid
> spillback **1 passed in 0.78s**、跨节点依赖 pull **1 passed in 0.84s**、K0 Actor
> **1 passed in 0.99s**、同 Node Actor Worker restart **1 passed in 1.02s**、public put **1 passed in 0.52s**、SYSTEM_ERROR retry
> **1 passed in 0.49s**、跨进程事件汇聚 **1 passed in 0.62s**、两节点并发执行
> **1 passed in 0.90s**、Worker 内嵌 Core 子任务 trace **1 passed in 0.94s**、Worker-owned
> inline ObjectRef **1 passed in 0.96s**、lineage reconstruction
> **1 passed in 0.77s**、单节点双 Worker 并发 **1 passed in 0.57s**、单 CPU blocking-get
> yield/reacquire **1 passed in 0.52s**、inline contained-reference 生命周期
> **1 passed in 0.57s**、foreign stored ref 直取 Node **1 passed in 0.57s**、foreign INLINE
> Task dependency **1 passed in 0.60s**、跨节点 foreign STORED Task dependency
> **1 passed in 0.84s**、stored physical GC **1 passed in 0.97s**、普通 Worker crash
> recovery **1 passed in 0.89s**、Driver-owned nested Task argument
> **1 passed in 0.87s**、三层 recursive lineage **1 passed in 0.85s**、PG execution/remove
> **1 passed in 1.08s**、PG shutdown 自动清理 **1 passed in 0.82s**、远端 Node crash
> survivor retry **1 passed in 0.96s**；PG prepare rollback **1 passed in 1.06s**、
> foreign-input lineage reconstruction **1 passed in 0.80s**、multi-return partial-loss
> reconstruction **1 passed in 0.76s**。PG bundle rescheduling 等仍缺，不能据此推导
> K0 或 K1 已完成。
> 最近报告的 stored replica P1 已修复并重新通过完整 unit gate；最终聚焦复审仍在进行。stored Node-loss takeover 的 runtime/full-flow 已有
> unit 覆盖、两个 pre-Complete 与一个 post-Complete bounded multiprocess 故障 smoke。

本项目的本地基线设备是 MacBook Air M4、16 GB 内存。默认策略是：

> 只自动运行确定为轻量的纯单元测试；真实多进程测试先静态审查，再逐个串行
> 运行；无法确认资源边界的测试按重型处理，不在本机运行。

### 历史：统一输出发现与发布迁移验证（2026-09-05）

当时 Worker 已将 single/multi/targeted 的 selected values 接到同一个
`OutputDiscoverySession`，只序列化一次，再投影至原单返回 Node 发布协议。
未知 OPEN/prepare/Complete ACK 保留原 stream、source handles 和 nested import
session；成功 promotion 或失败补偿的精确 ACK 才允许清理。新 batch journal、
metadata-only recovery、owner CAS/per-slot GC 和 Node effect adapter 已有纯组合
测试，当时尚未启用为 NodeServer 的新发布后端。这是迁移前快照，当前
后端已启用；旧测试/gate 未迁移之前，其历史通过不能扩大为新故障窗口验收。

本轮经静态审查、串行运行的 24 文件集合：**428 passed, 3 deselected in
1.29s**。此前 19 文件集合为 **372/3 in 1.25s**，23 文件集合为
**404/3 in 1.31s**；新集合加入纯 Core dispatch/shutdown/reconstruction/
foreign-lineage、batch authority 组合、安全分类和 Node 本地存储桥接断言。
三项排除均是 NodeServer 原有显式 L1 线程测试；没有运行完整默认
unit，也没有运行 GPU/压力/性能测试。更早的 607 与本集合有交集，不能相加。

```bash
python -m pytest -q -p no:cacheprovider \
  tests/unit/test_output_publication.py \
  tests/unit/test_output_publication_journal.py \
  tests/unit/test_output_recovery.py \
  tests/unit/test_multi_container_graph_protocol.py \
  tests/unit/test_output_publication_node.py \
  tests/unit/test_output_replica_node.py \
  tests/unit/test_output_owner_publication.py \
  tests/unit/test_output_discovery.py \
  tests/unit/test_stored_publication_node.py \
  tests/unit/test_stored_publication_node_server.py \
  tests/unit/test_bounded_test_modes.py \
  tests/unit/test_worker_output_discovery.py \
  tests/unit/test_worker_nested_task_arguments.py \
  tests/unit/test_worker_completion_paths.py \
  tests/unit/test_worker_stored_publication.py \
  tests/unit/test_worker_local_dependencies.py \
  tests/unit/test_lease_completion_handshake.py \
  tests/unit/test_targeted_worker_execution.py \
  tests/unit/test_worker_retry_failpoint.py \
  tests/unit/test_multi_return_partial_seal_cleanup.py \
  tests/unit/test_core_concurrent_dispatch.py \
  tests/unit/test_core_shutdown_unresolved.py \
  tests/unit/test_core_reconstruction_runtime.py \
  tests/unit/test_core_foreign_lineage_runtime.py
```

其中新增故障契约验证：STORED journal 已 Complete 后，terminal report 发出前
失败或生效但 ACK 丢失，精确 Complete 重放均可重新报告；不会被只允许首次
完成的 readiness 判定卡住。另验证本地 custody release 生效后抛错时，重放
必须先完成清理，不能因已有 prepared/success witness 就越过这项义务。

另选 Worker INLINE 文件排除其真实 drain wait 用例：**6 passed, 1 deselected
in 0.19s**。先前外部 lineage 五项已迁至 threadless fixture，与已有十五项
reconstruction 合并曾通过 **20 passed in 0.18s**；本次 24 文件集合已包含这两文件。

该历史阶段 Node 物理桥接尚未启用为 RPC 后端；其 22 项纯测试确认：新 journal effect
复用现有 ObjectStore、sealed metadata、删除墓碑和 ObjectManager；未完成
write claim 只认领本次 create/write/seal 的残留。其他 attempt 或未知残留
不会被误删；partial write、seal 后 metadata 前失败、manager cleanup ACK
丢失、pin、晚到旧 seal 与新 attempt 均有固定小对象契约。与 Node effect
adapter 的实际本地 helper 组合合计 **55 passed in 0.70s**，已包含于总集合，
不能作为完整多进程 batch 发布验收。

以下受影响真实进程路径均通过 runner **每次只运行一个 exact node ID**：

| Exact node ID | 本轮结果 | 范围 |
|---|---|---|
| `tests/integration/test_task_path.py::test_one_node_one_worker_task_path` | 1 passed in 0.97s | 真实 Node incarnation Start 握手与普通结果 |
| `tests/integration/test_multi_return_path.py::test_public_multi_return_mixed_outputs_retry_dependencies_and_gc` | 1 passed in 0.98s | 普通 mixed multi-return、retry、依赖与 GC |
| `tests/integration/test_contained_ref_lifecycle_path.py::test_two_borrowers_outlive_their_inline_container` | 1 passed in 1.14s | INLINE contained 双 borrower 生命周期 |
| `tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects` | 1 passed in 1.27s | 旧单返回 STORED 发布/图/owner/回收路径 |
| `tests/integration/test_multi_return_partial_reconstruction_path.py::test_one_lost_return_reconstructs_without_changing_healthy_siblings` | 1 passed in 0.87s | selected-only 序列化与 healthy sibling 保留 |
| `tests/integration/test_inline_node_loss_path.py::test_received_inline_result_survives_publisher_node_loss` | 1 passed in 1.29s | reported-Complete KEEP，实际已收到 bytes |
| `tests/integration/test_inline_node_loss_path.py::test_unreceived_inline_result_is_lost_until_explicit_get_reconstructs` | 1 passed in 1.25s | reported-Complete DROP→LOST→显式 get 重建 |

这些验证横跨本轮相邻小修订，不是全 allowlist 在同一 revision 的完整复验。
首次普通任务在沙箱内因 loopback bind 权限失败，随后同一有界测试经批准在
沙箱外通过；没有通过沙箱外运行未知/重型测试。

三个真实 Timer 契约已从 `tests/unit` 移至
`tests/integration/test_core_gc_retry_timer_lifecycle.py`，原测试名保留，按
`loopback_smoke` 逐项执行。每项最多两个 Timer 对象、一个实际 daemon 线程；
启动前登记、finally 取消/放行，join 共用一秒 deadline。

| Exact test name | 单独 runner 结果 |
|---|---|
| `test_fired_reference_timer_removes_itself_after_enqueue` | 1 passed in 0.16s |
| `test_timer_teardown_fences_cancelled_and_future_callbacks` | 1 passed in 0.15s |
| `test_timed_out_teardown_retains_running_callback_for_next_pass` | 1 passed in 0.19s |

dispatch 的六项协议/依赖/capacity 用例随后改为 threadless Core、显式队列和
fake clock；与十五项 reconstruction 和五项 foreign-lineage 合并得到
**26 passed in 0.21s**。真实双 lane 与后台 shutdown drain 保留在
`tests/integration/test_core_dispatch_concurrency.py`，每项零 Node/Worker、
最多四个 Core 线程/六项任务，RPC/socket/timer tripwire，引用关闭和 teardown
都有有限期限；两个 exact runner 用例分别通过 **0.16s**、**0.17s**。

shutdown 的三项协议契约也已纯化，单文件 **3 passed in 0.21s**，现已包含于
上面的 24 文件集合。原线程存活与 send/grant-vs-shutdown race 保留在
`tests/integration/test_core_shutdown_concurrency.py`：每项三 Core 线程、最多一
请求线程，无真实 RPC；补齐原来遗漏的 outcome fake，构造异常可收尾，finally
不通过清零 accepted count 伪造干净状态。三个 exact runner 用例分别通过：

| Exact test name | 单独 runner 结果 |
|---|---|
| `test_short_shutdown_keeps_runtime_alive_until_ambiguous_push_resolves` | 1 passed in 0.20s |
| `test_push_is_marked_unresolved_before_send_can_race_shutdown` | 1 passed in 0.16s |
| `test_lease_is_marked_unresolved_before_grant_reply_races_shutdown` | 1 passed in 0.17s |

L1 allowlist 因此由 13 增至 21，MP allowlist 仍为 50。PG、worker-supervisor
等其他历史 unit 安全分类仍待完成，不能运行整套默认 gate；上面的 pure
计数是不同选择集，不应直接相加。

### 较早增量验证（2026-09-05）

较早合并门禁：两个已审查集合去重后的 **29 文件，607 passed, 12 deselected in
0.72s**；不是完整默认 gate。下面保留早先分组结果以便追溯。完整命令如下：

```bash
python -m pytest -q -p no:cacheprovider \
  tests/unit/test_nested_argument_manifest.py \
  tests/unit/test_worker_nested_task_arguments.py \
  tests/unit/test_reconstruction_runtime.py \
  tests/unit/test_large_argument_lift.py \
  tests/unit/test_worker_local_dependencies.py \
  tests/unit/test_cross_node_pull_protocol.py \
  tests/unit/test_task_reference_hold_protocol.py \
  tests/unit/test_owner_reconstruction_deferred.py \
  tests/unit/test_foreign_task_finish_barrier.py \
  tests/unit/test_trace_export.py \
  tests/unit/test_inline_data_plane_fullflow.py \
  tests/unit/test_inline_recovery_runtime.py \
  tests/unit/test_inline_recovery_control.py \
  tests/unit/test_unreceived_inline_owner_retirement.py \
  tests/unit/test_inline_recovery_protocol.py \
  tests/unit/test_inline_owner_publication.py \
  tests/unit/test_inline_publication_node_server.py \
  tests/unit/test_task_finish_barrier.py \
  tests/unit/test_core_inline_publication.py \
  tests/unit/test_core_reconstruction_runtime.py \
  tests/unit/test_inline_publication_gate.py \
  tests/unit/test_output_publication.py \
  tests/unit/test_stored_intent_gate.py \
  tests/unit/test_inline_recovery.py \
  tests/unit/test_publication_owner_death_control.py \
  tests/unit/test_stored_publication_node_server.py \
  tests/unit/test_node_dependency_pull.py \
  tests/unit/test_bounded_test_modes.py \
  tests/unit/test_bounded_runner.py
```

已审查的 19 文件纯集合：**383 passed, 1 deselected in 0.55s**。排除的一项是
Node 双线程并发测试；没有把它算作纯执行证据。集合覆盖 `StoredArg`、metadata-only
INLINE recovery、Node/GCS/Core 同步回调接线、owner receipt 的 byte-free GC、
task-finish barrier 与 foreign reconstruction deferral。此结果不替代完整 K0/K1 gate。
另三个纯 owner-death reducer/runtime/arbitration 文件独立复验为 **31 passed in 0.18s**。
全目录 `compileall src tests scripts examples` 通过；它不执行测试。

```bash
python -m pytest -q -p no:cacheprovider \
  tests/unit/test_nested_argument_manifest.py \
  tests/unit/test_worker_nested_task_arguments.py \
  tests/unit/test_reconstruction_runtime.py \
  tests/unit/test_large_argument_lift.py \
  tests/unit/test_worker_local_dependencies.py \
  tests/unit/test_cross_node_pull_protocol.py \
  tests/unit/test_task_reference_hold_protocol.py \
  tests/unit/test_owner_reconstruction_deferred.py \
  tests/unit/test_foreign_task_finish_barrier.py \
  tests/unit/test_trace_export.py \
  tests/unit/test_inline_data_plane_fullflow.py \
  tests/unit/test_inline_recovery_runtime.py \
  tests/unit/test_inline_recovery_control.py \
  tests/unit/test_unreceived_inline_owner_retirement.py \
  tests/unit/test_inline_recovery_protocol.py \
  tests/unit/test_inline_owner_publication.py \
  tests/unit/test_inline_publication_node_server.py \
  tests/unit/test_task_finish_barrier.py \
  tests/unit/test_core_inline_publication.py
```

本轮真实进程验收均通过下面的 runner **单独**执行；沙箱禁止 loopback bind，
因此使用已批准的沙箱外执行，不涉及外网或重型测试：

| Exact node ID | 结果 | 范围 |
|---|---|---|
| `tests/integration/test_nested_large_argument_path.py::test_nested_large_argument_pulls_without_gating_on_pending_handle_and_collects` | 1 passed in 1.28s | 新增：64 KiB keyword 容器自动 lift、pending nested handle 不 gating、两副本和三对象 GC |
| `tests/integration/test_contained_ref_lifecycle_path.py::test_two_borrowers_outlive_their_inline_container` | 1 passed in 1.10s | driver 票据改动后复验既有 INLINE happy/双 borrower/GC 路径，不是 borrowed-source publication 或 Node-loss 证明 |
| `tests/integration/test_lineage_reconstruction_path.py::test_stored_task_output_reconstructs_with_same_object_id` | 1 passed in 0.99s | 既有 single-result reconstruction 复验，task-finish barrier 不阻断合法重建 |

```bash
python scripts/run_bounded_test.py tests/integration/test_nested_large_argument_path.py::test_nested_large_argument_pulls_without_gating_on_pending_handle_and_collects
```

上述 happy-path 不证明 INLINE publishing-Node-loss；该故障路径的新增专用验收
单独记录在下一节，不能混用证据。

### 后续收敛：INLINE 故障验收与测试安全分类

新测试 `test_inline_node_loss_path.py` 的两项已经各自通过 runner：

| Exact node ID | 结果 | 证明范围 |
|---|---|---|
| `tests/integration/test_inline_node_loss_path.py::test_received_inline_result_survives_publisher_node_loss` | 1 passed in 1.31s | 本地 Complete＋GCS terminal 已完成，owner 真正收到 bytes 后 Node 死亡；KEEP 保留旧 attempt 与精确数据 |
| `tests/integration/test_inline_node_loss_path.py::test_unreceived_inline_result_is_lost_until_explicit_get_reconstructs` | 1 passed in 1.25s | Complete/outcome 均未放行时 Node 死亡；DROP→LOST＋SUCCEEDED，旧任务收尾后由显式 get 重建一次 |

每项为两 Node、每 Node 一 CPU/一 Worker、五个受管子进程、最多三个物理 Task、
1 MiB/Node store、微小 INLINE 数据和一次精确 Node 故障。语义 gate 仅在显式私有
配置下启用，携带 Node incarnation 与 publication ID；不持运行时状态锁跨 socket，
terminal proof 和 gate 共用 deadline。默认 Complete/outcome 不受测试 gate 影响。
child 由 Driver 拥有、通过 nested 参数借给 Worker，因此还证明了单返回 INLINE 的
TaskHoldSource borrowed-source 路径；不外推为全部 STORED borrowed-child 或 owner-death 矩阵。

测试安全修复：五个混合模块采用逐函数 `unit`/`loopback_smoke`，真实 socket/线程
不再继承 `unit`。`test_core_reconstruction_runtime.py` 的十四个原非并发契约及新增
引用回收契约改为 threadless Core＋同步 mailbox，并以 guard 拒绝未模拟 RPC、线程、
socket 和阻塞等待；两个真实请求竞争用例移动到独立 L1 文件。

本次上述变更＋gate＋output batch 值模型＋runner 的十一文件纯增量门禁为
**224 passed, 11 deselected in 0.39s**；十一项排除项均为该集合内的独立 L1，
不是被删除的覆盖。以下十三个 L1 已经**每项单独**通过 runner（没有并行 pytest）：

| 文件 | 独立用例 | 每项结果 |
|---|---|---|
| `tests/integration/test_core_reconstruction_concurrency.py` | `test_concurrent_lost_requests_merge_and_old_attempt_is_fenced`、`test_concurrent_multi_return_sibling_requests_start_once_and_join` | 各 1 passed in 0.15s |
| `tests/unit/test_stored_intent_gate.py` | `test_arrival_frame_round_trips_exact_incarnation_and_publication`、`test_legacy_intent_frame_and_four_argument_constructor_remain_compatible`、`test_legacy_receiver_rejects_later_publication_phase` | 各 1 passed in 0.15s |
| `tests/unit/test_inline_recovery.py` | `test_owner_death_and_intent_admission_linearize_atomically`；`test_owner_keep_drop_race_has_one_winner_and_no_payload_in_authority` | 分别 1 passed in 0.15s、0.17s |
| `tests/unit/test_publication_owner_death_control.py` | `test_live_background_converges_owner_wide_and_publication_sagas[fence]`、`[publication]` | 各 1 passed in 0.21s |
| `tests/unit/test_stored_publication_node_server.py` | `test_stored_complete_releases_state_lock_around_external_terminal_ack`、`test_stored_outcome_validates_under_lock_then_queries_adapter_lock_free`、`test_publication_step_external_effect_does_not_hold_node_state_lock` | 各 1 passed in 0.15s |
| `tests/unit/test_node_dependency_pull.py` | `test_concurrent_localizers_pull_once_and_second_uses_local_replica` | 1 passed in 0.15s |

这些 L1 每项零实际集群、最多两线程；socketpair 各有一秒超时，线程采用 deadline
join 与 finally 清理；各自在隔离 pytest 子进程内受三十秒硬截止保护。runner 按
完整 node ID 推断 marker，没有任意 pytest passthrough：

```bash
python scripts/run_bounded_test.py tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_lost_requests_merge_and_old_attempt_is_fenced
```

其余完整 Core fixture 仍可能懒启动引用线程，默认安全分类**尚未全部完成**。
历史 marker 不是 L0 证明；不能只叠加 `loopback_smoke` 而继续继承模块级 `unit`。

## 1. 安全等级

| 等级 | 判定条件 | 本机策略 | Pytest marker |
|---|---|---|---|
| L0 明显轻量 | 单进程、纯函数/内存状态机；无 socket、共享内存、后台线程、子进程和真实等待 | 可运行 | `unit` |
| L1 有界 smoke | 仅 loopback；线程或固定两个逻辑节点；子进程、对象、任务和超时均有硬上限；确定性同步和完整 teardown | 代码审查后，每次只运行一个明确的 test node id | `loopback_smoke` / `multiprocess_smoke` |
| H 重型 | benchmark、压力、fuzz/chaos、长时运行、大对象、大量任务/Worker/故障、完整 production Ray 构建或测试 | 本机不运行 | `heavy` |
| U 未知 | 无法从代码确认进程数、内存、网络、超时、递归重试或清理行为 | 立即按 H 处理，不运行 | 标成 `heavy`，直到证明有界 |

仅仅“测试文件很短”不等于 L0。导入模块若可能隐式启动 runtime、后台线程或
子进程，也必须先归为 U。`pytest --collect-only` 同样会导入测试模块，在完成静态
审查前不能用它探测未知测试。

## 2. L1 的硬上限

一般L1测试必须同时满足下列条件。下面单列的固定教学实验只覆盖其
明确写出的资源差异；其余限制仍全部适用，不能由allowlist或历史通过
推导出新用例的安全性。

- 进程启动方式为 `spawn`，不依赖 `fork` 继承锁、线程或文件描述符；
- 逻辑节点不超过 2 个，每节点 1 个逻辑 CPU；
- 峰值子进程不超过 8；当前两节点拓扑是 1 GCS、2 NodeManager（各自内嵌
  ObjectStore）和 2 Worker，共 5 个受管子进程；
- Worker 总数不超过 2，Actor 不超过 1，PG bundle 不超过 2；
- 单测试 Task 不超过 20；
- 每节点 ObjectStore 容量不超过 16 MiB；
- 单对象不超过 256 KiB，同时存活对象总量不超过 1 MiB；
- 测试跨节点 pull 时把 inline threshold 调到 1 KiB，并使用约 64 KiB 对象，
  不通过“大对象”制造代码路径；
- 最多注入 1 次 kill/drop/failpoint，最多 1 次 retry/reconstruction/restart；
- 单测试 hard timeout 不超过 30 秒，teardown 额外最多 5 秒；timeout 只作保险，
  不作同步条件；
- 只绑定 `127.0.0.1` 和操作系统分配的临时端口，不访问外网；
- 使用 Event、barrier、ACK 或语义 failpoint，同步逻辑中禁止 `sleep()`；
- 固定调度 seed，使用 monotonic clock；纯状态机测试优先 fake clock；
- fixture 只操作自己记录的准确 PID；teardown 断言所有子进程退出、端口关闭、
  临时目录/共享内存清理、资源账本回到基线。

任何一项无法从代码证明，测试仍是 U/H。

### 已审查的固定资源例外

- `tests/integration/test_contained_ref_lifecycle_path.py::test_two_borrowers_outlive_their_inline_container`
  保原一个Node/两个逻辑CPU/两个Worker（四子进程、五端点），
  parent和child各占1CPU、仅两Task、1MiB store/两个tinyINLINE结果，
  无故障/retry/trace/测试线程。两次outer反序列化为同一Workerowner
  建两个独立borrower，不用Driver-localref或第三probeTask代替。
  原Task拓扑需要这个twoCPU配置，但不增加集群总Worker/CPU上限。
  gets/earlycloses/真正outerGC共15s；早期每close≤3s，最终second
  close/Release收敛/finally共一个3s epoch，outerrunner30s。
  运行须单独批准此exact twoCPU例外；child最终metadataGC未由
  现协议独立观测，不以ReleaseACK/clean shutdown外推。

- `tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_multi_return_sibling_requests_start_once_and_join`
  是两个真实请求线程的**内存authority实验**：一个不启动的Node、
  一个threadlessCore、1KiB store、一个逻辑Task/三个总计小于128B
  的STORED结果；无Worker执行/网络/集群进程。为保原三sibling全部
  LOST前提，主线程确实分别Drop三个内存副本，不能说单drop。
  重建在两个线程中运行完整retirement/prepare/commit/enqueue，
  first持slot0ticket时second真defer，first入队后second真JOIN，
  共三次内部Core request/一次重建准入，不是三次重执行或公开get。
  这不是productionfault/压力许可；物理删除和原receipt重放分别
  最多3次，每wait≤1s、normaljoin共享2s/finaljoin共享1s、外层30s。
  已入提交FIFO但尚未取得lease的重建在主断言后以显式fixture终态结束，真实finish/
  close/GC核验；不伪造success或缩掉线程内retirement。这个exact
  的三内存drop前置须单独批准，其余限制不放宽。

- `tests/integration/test_multi_return_reconstruction_path.py::test_multi_return_all_outputs_lost_reconstructs_once_from_nonzero_sibling`
  保留原**两个 sibling 的各一次显式 drop＋一个 producer 重建**，不称
  单故障。一个Node/CPU/Worker、三个受管子进程/四端点、1MiB store，
  一个逻辑Task/两次用户执行，两个结果各小于32KiB。两次注入是为
  进入全manifest LOST，而非把逐槽故障缩成单返回；退休阶段还会
  重放旧replica Drop，最终GC删除新attempt副本，不称全程只有两RPC。
  slot1由公开get触发真实重执行，commit后注入一次slot0 request取得
  真JOIN；不是两个并发公开get。外部两字节invocation journal、最多
  八条START/JOIN记录，状态观察最多256次。get/drop/finish共享15s
  等待预算，不取消同步重建退休RPC；close/实际GC与finally共单3s
  epoch，外层30s runner，失败必shutdown并检查PID/端点。仅正常
  主体成功才宣称资源账本clean。运行须单独批准这个exact的两drop
  复合设置，不扩大其它故障/对象/进程上限，也不授权未知case。

- `tests/integration/test_two_worker_pool_path.py::test_one_node_two_workers_execute_two_tasks_concurrently`
  使用**一个Node、两个逻辑CPU、两个Worker**，与常规两Node各一CPU的
  总CPU/Worker上限相同，而不是扩大并发规模。四个受管子进程、1MiB
  store、两个仅报告PID并等待屏障的小Task、无故障/重试；工作共享10s，
  gate/ref清理共享3s，外层30s。此差异用于保留原“同Node双Worker同时
  执行”合同，不能改成单CPU排队后仍宣称并行；不授权压力/吞吐测试。

- `tests/integration/test_recursive_lineage_reconstruction_path.py::test_recursive_lineage_reconstructs_leaf_to_root`
  是**三层DAG复合恢复实验**，不称单故障：一个Node/两个逻辑CPU/两个
  Worker、四个受管子进程、1MiB store、三个≤1KiB结果；三次独立drop，
  每producer最多一次重建，共三原执行＋三重建执行。三层是K1原合同，
  不能先分别get叶/中间节点或降成单层来替代递归。没有进程kill、
  额外fault、随机压力、GPU或外网；work共享15s，三refs/GC共享3s，
  外层仍30s。运行须单独明确批准这个exact的2CPU/3drop/3重建预算；
  其余安全要求不放宽，不能由这一例外推导其它复合故障也可运行。

- `tests/integration/test_foreign_wait_drop_path.py::test_foreign_wait_drop_replay_then_owner_reconstruction`
  保留原**一次真实drop＋其ACK丢失一次**，不是两个独立物理drop：
  重放同一operation ID和真实缓存回执。两个Node各1CPU/Worker、五个
  子进程、两1MiB store、两个逻辑Task/三次执行、一个约64KiB结果，
  最多一次producer重建。额外GC验证只读当前副本，不增加删除/Task。
  publicget/wait/有限观察共享15s，raw owner RPC仍自身有限三次重试，
  外层30s；close共享3s。执行须单独批准这个exact的drop/ACKloss组合，
  不能泛化为随机故障或扩大矩阵的许可。

- `tests/integration/test_foreign_input_lineage_reconstruction_path.py::test_foreign_input_hold_replaced_before_consumer_reconstruction`
  使用一个Node/两个逻辑CPU/两个Worker（四子进程、1MiB store），
  原三个逻辑Task/四次执行、一个小于16KiB的consumer结果、一次
  drop/一次重建。此exact用于区分关闭foreign句柄与长期retained
  lineage，并非增加并行规模；不添加新的故障。public操作/有限
  观察共享15s，内部foreign RPC保原有限重试/外层30s，最后close/
  实际GC共享3s。运行前须单独批准此two-CPU例外，其它限制不放宽。

- `tests/integration/test_nested_task_argument_path.py::test_nested_argument_survives_sender_close_before_worker_push`
  一个Node/两个逻辑CPU/两个Worker、四子进程/1MiB store、两个
  仅等待屏障的blocker与一个consumer、一个已READY的inline put。
  无故障/重建；原“占满两slot后close源，再放行consumer”需要此
  two-CPU配置，不扩大集群总CPU/Worker数量。工作共享15s、gate/
  四个handle清理共享3s、外层30s；不声称覆盖PENDING source。

## 3. 可以在本机运行的 L0

下面这些测试在确认没有导入副作用后属于 L0：

- TaskID/AttemptID/ObjectID/ActorID/generation 的构造和稳定性；
- 不可变 TaskSpec、参数编码和协议序列化；
- 资源向量加减、feasible/available、资源守恒和重复 release；
- Hybrid policy 的过滤、评分、GPU 避让和 seeded top-k；
- Task、Attempt、Lease、Replica、Actor、PG 的合法/非法状态转换；
- ObjectStore 的内存模型 `create → write → seal` 和二次 seal 拒绝；
- owner location add/remove 幂等；
- local/submitted/borrower/contained token 的幂等 acquire/release；
- lineage 决策和旧 attempt/generation fencing；
- blocking-get CPU yield/reacquire 的纯资源账本模型；
- PG planner 以及 prepare/commit/abort 的纯事务状态机；
- trace schema、单进程序号、因果边与状态机校验。

项目的 `pyproject.toml` 已把 `-m unit` 写入 pytest 默认参数，但历史 marker 分类
尚不完全满足 L0 定义。这只能排除多数集成测试，不能代替逐项安全审查。
**不要直接运行不带精确选择器的 pytest**；使用本页顶部的 reviewed-pure 入口。
不要通过 `-o addopts=...`、空 marker
表达式或其他方式绕过项目默认过滤。维护者最近一次验证使用了已有隔离环境的
解释器路径；该机器专用路径不是面向用户的安装或测试命令。

## 4. 只能逐个运行的 L1

以下是较早版本的累计记录，不是当前allowlist数量或同版本复验。当前范围和
结果以上方checkpoint及runner为准；已替换的Worker合同在原记录处明确注明。

较早一次 loopback 验证显式运行了当时全部 7 个 `loopback_smoke` 用例，整体结果为
**7 passed**。其中 5 项在沙箱内通过；另外 2 项需要创建真实 `TCPServer` 并绑定
loopback 监听端口，沙箱策略不允许该操作，因此改在沙箱外运行并通过。这里的
沙箱内外差异是执行环境的端口绑定权限限制，不是测试失败；这 7 项仍是单进程
loopback 测试，不构成 multiprocess 验收。

当时allowlist包含五十项真实进程测试；下列记录中的四十七项均经静态审查：四十二项是 stored-outer
接线之前的基线；专用 stored-outer happy/GC 以及三个 Node-loss smoke
当前分别为 **0.96s**、**1.27s**、**1.17s** 和 **1.32s**；secondary-replica
promotion 最新为 **1.09s**。这些不是同轮全量复验。
既有三十七项此前通过标准库
30 秒 bounded runner 逐项、串行执行，新增项及三项受影响旧路径在最新改动后复验。
下列标题使用 allowlist 中的完整 pytest node ID，
与 `scripts/run_bounded_test.py` 一一对应：
该历史轮次耗时依次为：task **1.08s**、large object **0.95s**、spillback **4.01s**、
cross-node pull **3.04s**、Actor **4.67s**、Actor restart **2.61s**、put **1.54s**、
system retry **1.74s**、causal trace **1.07s**、parallel lanes **2.90s**、Worker nested
**2.66s**、nested argument **1.16s**、Worker-owned ref **2.32s**、lineage **1.09s**、
recursive lineage **1.17s**、two-worker **2.08s**、CPU yield **1.57s**、contained refs
**1.95s**、foreign stored ref **1.17s**、foreign inline dependency **1.27s**、foreign
stored dependency **1.30s**、physical GC **1.61s**、Worker crash **2.28s**、PG remove
**1.46s**、PG shutdown **1.50s**、Node crash **1.80s**、Worker-death ownership
**1.34s**、application error trace **1.16s**、startup rollback **7.35s**、local nested
reconstruction **1.74s**、foreign reconstruction **3.35s**、PG Node-loss **2.23s**、
foreign wait/drop **5.87s**、public multi-return **2.35s**、Actor Node-loss migration
**5.53s**、multi-return reconstruction **2.60s**、Driver-local Node recovery **1.51s**；
stored outer publication 在最终改动后复验为 **0.96s**；before-effect 与 after-effect
pre-Complete Node-loss 分别为 **1.27s** 和 **1.17s**；post-Complete/pre-TaskReply 为
**1.32s**。
secondary-replica Node-loss promotion 最新为 **1.09s**。

- `tests/integration/test_task_path.py::test_one_node_one_worker_task_path`：
  **1 passed in 0.48s**，覆盖单节点 spawn、
  Worker lease、direct `PushTask`、Start/Complete 握手、结果获取与 teardown；
- `tests/integration/test_large_object_path.py::test_one_node_large_result_uses_object_store`：
  **1 passed in 0.48s**，覆盖单节点
  Worker→Node seal、descriptor-only reply、owner stored location 与 fetch/checksum；
- `tests/integration/test_two_node_spillback.py::test_custom_resource_spills_task_to_second_node_and_cleans_cluster`：
  **1 passed in 0.78s**，覆盖独立 GCS、两个 Node/Worker 的注册与发现，自定义资源
  触发的确定性 Hybrid spillback，提交者向远端 Worker direct `PushTask`、结果取回，
  以及 5 个 PID/端口和两节点资源的 teardown。调度读取启动期安装到 NodeManager
  的不可变集群快照，普通任务 hot path 不逐任务查询 GCS；
- `tests/integration/test_cross_node_dependency_pull.py::test_store_backed_dependency_pulls_to_consumer_node_before_direct_push`：
  **1 passed in 0.84s**，覆盖消费者在依赖 ready 前提交，源副本 pin、16 KiB chunks、
  目标 checksum 校验与 seal-before-grant、byte-free lease request/grant 和 `PushTask`，
  以及 Worker 通过目标本地 descriptor 物化 `RefArg`；
- `tests/integration/test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker`：
  **1 passed in 0.99s**，覆盖公开 Actor API、GCS 创建、Node lifetime resources、
  专属 Actor Worker、三次 direct FIFO 方法调用与 generation 校验，以及 GCS、两个
  Node、两个普通 Worker、一个 Actor Worker 共 6 个 PID/端口的干净 teardown；
- `tests/integration/test_actor_restart_path.py::test_actor_crash_restarts_once_fences_inflight_call_and_resets_state`：
  **1 passed in 1.02s**，覆盖同一存活 Node 内的 Actor Worker sentinel exit、单 CPU
  lifetime token 先释放后重获、ActorID 稳定、generation/route/Worker incarnation 更新、
  old in-flight call typed failure/no replay、构造器状态重置与 clean teardown；
- `tests/integration/test_public_put_path.py::test_public_put_inline_and_stored_values_without_worker_execution`：
  **1 passed in 0.52s**，覆盖 Driver owner 直接发布 inline/stored 值，大值 seal 到
  本地 Node，且不执行任务、不新增 Worker、producer TaskSpec 为 `None`；
- `tests/integration/test_task_retry_path.py::test_explicit_worker_system_error_retries_once`：
  **1 passed in 0.49s**，覆盖 failpoint
  产生 attempt 0 SYSTEM_ERROR、预算内 attempt 1 成功，TaskID/ObjectID 稳定而
  AttemptID/LeaseID 更新。application error 不进入这条 retry 路径。
- `tests/integration/test_cross_process_trace.py::test_one_task_emits_cross_process_golden_trace_and_cleans_up`：
  **1 passed in 1.07s**，
  覆盖 Driver collector 汇聚 CoreWorker、GCS、Node 和 Worker 事件、各进程序号单调，
  验证普通 Task 事件不进入 GCS，并以同一 `rpc_id` 证明 lease、PushTask、StartLease、
  CompleteLease 四条跨 PID send→receive `cause_event_id` 边。
- `tests/integration/test_parallel_task_lanes.py::test_two_nodes_execute_resource_pinned_tasks_concurrently`：
  **1 passed in 0.90s**，
  使用测试进程持有的 barrier 在释放任一任务前观察两个 Worker 同时到达，证明 Driver
  Core 的两条 dispatch lane 能让两个节点上的资源固定任务真实重叠，而非依赖计时猜测。
- `tests/integration/test_worker_nested_task_path.py::test_worker_submits_child_task_and_gets_plain_result`：
  **1 passed in 0.94s**，覆盖普通
  Worker 的 thread-local runtime binding 与懒创建内嵌 Core、attempt-scoped child ID，
  child 使用普通 lease/direct `PushTask` 在另一节点执行，parent 取得并返回 plain result；
  collector 同时观测到 `worker_core` 的 child submit、lease request/grant、push 和 finish。
  parent 明确请求 0 CPU，且没有 ObjectRef 逃逸，因此不覆盖 CPU yield、foreign-owner
  ref 或 borrower 生命周期。
- `tests/integration/test_nested_task_argument_path.py::test_nested_argument_survives_sender_close_before_worker_push`：
  **1 passed in 0.87s**，覆盖两个 Worker slot 被 blocker 占用时，Driver-owned nested
  `ObjectRef` 的完整 logical-Task hold 在 `remote()` 返回前建立；源 handle 随后关闭，
  consumer 延迟到 `PushTask` 后仍可恢复重复 nested handle，并完成 owner route 读取。
- `tests/integration/test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get`：
  **1 passed in 0.96s**，覆盖 Worker owner endpoint 路由与 WorkerID 身份分离、序列化前
  transfer contained pin、同步 Acquire ACK、每次恢复的唯一 borrower token、Release
  tombstone/精确 transfer binding、两次 outer `get()` 和 foreign inline `get()`。
- `tests/integration/test_lineage_reconstruction_path.py::test_stored_task_output_reconstructs_with_same_object_id`：
  **1 passed in 0.77s**，覆盖 single-return stored output 丢失后的 START/JOIN coordinator、
  同一 TaskID/ObjectID、新 AttemptID、旧 attempt fencing 和 `max_retries` 预算。
- `tests/integration/test_recursive_lineage_reconstruction_path.py::test_recursive_lineage_reconstructs_leaf_to_root`：
  **1 passed in 0.85s**，覆盖三个 stored 结果全部 LOST 后，从 root `get()` 触发
  `leaf → middle → root` 的 local-owner 依赖优先 reconstruction，并保持三个逻辑 ObjectID。
- `tests/integration/test_two_worker_pool_path.py::test_one_node_two_workers_execute_two_tasks_concurrently`：
  **1 passed in 0.57s**，覆盖
  单 Node 的两个固定 ordinary Worker slot；在释放任一任务前，Driver barrier 已收到两个
  不同 Worker PID，证明池内真实重叠执行，并验证完整 Worker tuple shutdown 与资源清理。
- `tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker`：
  **1 passed in 0.52s**，覆盖单 Node、
  单 CPU、两个固定 Worker；父任务占用 CPU 后提交 child，真实 `get()` 的 Blocked 通知只
  归还 CPU，child 在另一 Worker 运行，随后 Unblocked 恢复 parent，最终无 CPU debt。
- `tests/integration/test_contained_ref_lifecycle_path.py::test_two_borrowers_outlive_their_inline_container`：
  **1 passed in 0.57s**，覆盖 outer
  INLINE result 的 contained edge；两次反序列化得到独立 borrower，outer 关闭后仍可读取，
  最后 borrower 释放后 edge obligation 收敛并干净 shutdown。
- `tests/integration/test_foreign_stored_ref_path.py::test_inline_outer_restores_foreign_ref_then_driver_fetches_stored_bytes_from_node`：
  **1 passed in 0.57s**，覆盖 owner 仅返回 current-attempt descriptor，Driver 以完整
  attempt/owner/size/checksum expectations 直取 Node，并校验字节 SHA-256。
- `tests/integration/test_foreign_inline_dependency_path.py::test_foreign_inline_dependency_survives_input_handle_close`：
  **1 passed in 0.60s**，覆盖
  borrower 派生独立 logical-Task retained hold、retain ACK-before-submit、关闭 borrower/outer
  后继续保活、PENDING 不占 dispatch lane、READY_INLINE 改写以及 terminal release。
- `tests/integration/test_foreign_stored_dependency_path.py::test_foreign_stored_dependency_pulls_node_to_node_before_push`：
  **1 passed in 0.84s**，
  覆盖 retained READY_STORED descriptor、Node-to-Node pull/target pin、owner-authoritative
  target location report、report ACK-before-Push，以及 Driver/GCS byte-free 数据路径。
- `tests/integration/test_stored_physical_gc_path.py::test_foreign_stored_dependency_collects_source_and_target_replicas`：
  **1 passed in 0.97s**，
  覆盖冻结 collection plan、source/target typed Drop、精确 ACK replay，以及 metadata、
  descriptor、waiter、durable obligation 和 producer lineage 的最终收敛。
- `tests/integration/test_worker_crash_recovery_path.py::test_after_complete_worker_crash_retries_on_fresh_worker`：
  **历史 1 passed in 0.89s；该合同/ID已被替换，不能用于当前验收。**
  旧后端在Complete后丢结果并要求新Attempt/Lease；统一后端保留Node
  envelope，现应同attempt0采用原成功。替代ID为
  `test_after_complete_worker_crash_recovers_output_without_reexecution`，
  本次失败/修正/实跑及未执行replacement探针见顶部recovery-contracts。
- `tests/integration/test_worker_death_ownership_path.py::test_dead_attempt_borrower_is_swept_while_logical_hold_spans_retry`：
  **1 passed in 1.34s**，覆盖 attempt 0 在 nested borrower Acquire 后、用户代码前精确
  crash；Node/GCS 记录 `PROCESS_EXIT`，fresh Worker 执行 attempt 1，完整 logical Task
  hold 跨 SYSTEM retry 保持，Core journal barrier 只清理死亡 attempt borrower，最后 hold、
  borrower、对象元数据及进程/端口/资源全部收敛。
- `tests/integration/test_cross_process_trace.py::test_application_error_trace_is_terminal_without_system_retry`：
  **1 passed in 1.16s**，覆盖用户异常只产生 attempt 0、零 SYSTEM retry、owner ERROR 与
  Recovery APPLICATION_FAILED；`push_task` RPC 正常返回并携带业务失败，四条跨 PID
  send→receive 因果边及 clean teardown 保持成立。
- `tests/integration/test_startup_rollback_path.py::test_second_node_ready_failure_rolls_back_every_started_process`：
  **1 passed in 7.35s**，覆盖第二 Node/Worker 已报告 ready、但尚未进入 committed prefix 时
  注入失败；已提交第一 Node、未提交第二 Node 进程组和 GCS 的五个 PID/五个 endpoint
  全部回收，且 `_runtime` 从未暴露。
- `tests/integration/test_local_nested_reconstruction_path.py::test_local_nested_handle_survives_single_return_reconstruction`：
  **1 passed in 2.85s**，覆盖 nested handle 作为 lifetime edge 而非 readiness/DFS edge；
  B 的 stored output 丢失后保持 ObjectID、Attempt 0→1，以 fresh hold 和 attempt borrower
  重导入 A，最终 producer lineage、两对象副本与 metadata 全部收敛。
- `tests/integration/test_foreign_reconstruction_path.py::test_driver_reconstructs_worker_owned_stored_object_through_owner`：
  **1 passed in 2.16s**，覆盖 Driver active borrower 观察 Worker-owned stored object
  `LOST@0`，以完整 capability 请求 owner 唯一 START，随后轮询并取得同 ObjectID 的
  `READY_STORED@1`；borrower 不接收 lineage，也不在本地执行 producer。
- `tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg`：
  **1 passed in 2.23s**，覆盖两节点 STRICT_SPREAD participant `PROCESS_EXIT` 后 GCS/Core
  整组 `LOST`、旧 Task 不重试、两个旧 bundle 均拒绝新提交、survivor abort 后 root/child
  ledger 恢复以及 victim crash/survivor graceful teardown。
- `tests/integration/test_foreign_wait_drop_path.py::test_foreign_wait_drop_replay_then_owner_reconstruction`：
  **1 passed in 2.17s**，覆盖 foreign wait 只轮询 owner metadata、零对象 byte fetch；
  owner 已删除副本但首次 drop ACK 丢失时，同 operation ID 精确重放，随后 active borrower
  从 `LOST@0` 请求 owner reconstruction 到 attempt 1，并保持 ObjectID/token 与 clean teardown。
- `tests/integration/test_multi_return_path.py::test_public_multi_return_mixed_outputs_retry_dependencies_and_gc`：
  **1 passed in 2.35s**，覆盖 public two-return、SYSTEM retry 后 mixed inline/stored
  原子发布、两个 sibling 的独立下游依赖，以及最后 sibling 才释放 TaskID-scoped lineage。
- `tests/integration/test_actor_node_loss_migration_path.py::test_actor_migrates_after_remote_node_loss_and_resets_generation`：
  **1 passed in 5.53s**，覆盖 stable ActorID、跨 Node generation/route migration、旧 in-flight
  `ActorDiedError` 且不重放、新 generation sequence 0、构造器状态重置和 survivor cleanup。
- `tests/integration/test_multi_return_reconstruction_path.py::test_multi_return_all_outputs_lost_reconstructs_once_from_nonzero_sibling`：
  **1 passed in 2.60s**，覆盖非零 sibling 触发 whole-manifest START、另一 sibling JOIN、
  所有 ObjectID 稳定、Attempt 0→1、producer 仅重执行一次及逐 sibling GC。
- `tests/integration/test_driver_local_node_recovery_path.py::test_driver_local_node_death_migrates_home_and_retries_on_survivor`：
  **1 passed in 1.51s**，覆盖初始 home Node 上已进入用户代码的 attempt 0 随 Node
  crash 终止，Core 在 GCS tombstone 与 survivor snapshot ACK 后原子迁移 home route，
  attempt 1 以同 TaskID/ObjectID 在 survivor 执行；后续新 Task 与 large put/get 也使用
  survivor，而 Core WorkerID 与 OwnerService 地址保持稳定。
- `tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects`：
  当前记录为 **1 passed in 0.96s**，覆盖大 outer 内含 executor-owned child `ObjectRef` 的真实多进程
  publication，Node claim、promotion replay、GCS graph commit、owner descriptor+edges 原子
  adoption、READY 与 Node adopted ACK；关闭 outer/child 后按 child hold、graph container、
  replica、owner metadata 顺序收敛，并完成 clean shutdown。该结果已在 intent-before-effect
  改动后使用同一 exact node ID 复验；不表示此前四十二项在同轮全部复验。
- `tests/integration/test_stored_outer_node_loss_path.py::test_precomplete_stored_outer_node_loss_rolls_back_then_retries_survivor`：
  **1 passed in 1.27s**，在 exact intent ACK 后、首个 child effect 前杀死 publishing Node，
  验证 graph tombstone、final/provisional hold 补偿、无副本证明和 survivor retry；
- `tests/integration/test_stored_outer_node_loss_path.py::test_post_effect_precomplete_stored_outer_node_loss_compensates_then_retries`：
  **1 passed in 1.17s**，在 replica seal 与全部 final-hold promotion 获 GCS ACK 后、
  Complete admission 前杀死 publishing Node，验证 PREPARED graph abort、仅 final hold
  释放、dead replica proof 和稳定 TaskID/ObjectID 上的新 AttemptID/LeaseID。
- `tests/integration/test_stored_outer_node_loss_path.py::test_postcomplete_stored_outer_node_loss_retires_then_reconstructs`：
  **1 passed in 1.32s**，在 Node/GCS successful Complete 已提交而 TaskReply 尚未发送时
  杀死 publishing Node；验证 `POSTCOMPLETE_LOST_RESULT`、owner `PENDING→LOST`、
  graph/hold/replica retirement、零普通 system retry，以及显式 `get` 后才以新 AttemptID/
  LeaseID 在 survivor 做 lineage reconstruction。
- `tests/integration/test_secondary_replica_node_loss_path.py::test_dead_primary_promotes_surviving_replica_without_lineage_replay`：
  **1 passed in 1.09s**，先以跨节点依赖 pull 建立 source/target 两副本，再杀 source；
  owner 保持 `READY_STORED@attempt0`，fetch route 切到 target，完整 Attempt/owner/size/
  checksum expectations 从 target 取回字节，且 retry/reconstruction 计数均不变。
- `tests/integration/test_placement_group_path.py::test_strict_spread_tasks_use_committed_bundles_and_remove_restores_resources`：
  **1 passed in 1.08s**，覆盖两节点 STRICT_SPREAD、commit 后可见、bundle child ledger、
  计划节点执行、显式 remove 与资源/进程/端口清理；
- `tests/integration/test_placement_group_path.py::test_shutdown_removes_committed_group_without_explicit_remove`：
  **1 passed in 0.82s**，
  覆盖 Node admission fence 后、Node finalize 前的独立 GCS PG-drain barrier，以及未显式
  remove 时的 participant abort、root reservation 恢复和全进程 clean shutdown。
- `tests/integration/test_node_crash_recovery_path.py::test_remote_node_death_retries_task_on_survivor_and_reports_crash`：
  **1 passed in 0.96s**，覆盖远端 managed Node 的精确 Process sentinel、GCS DEAD
  tombstone、survivor live-only snapshot ACK、Core dead-location/attempt fencing、稳定
  TaskID/ObjectID 上的新 AttemptID/LeaseID、survivor retry/probe，以及 survivor graceful
  shutdown 与 victim typed crash 的逐槽报告。

第二项把 CoreWorker/owner 与单节点 ObjectStore 接线带入真实进程路径；第三项证明
远端执行和 inline 返回；第四项证明 store-backed `ObjectRef` 的真实 Node-to-Node
pull；第五项证明 K0 Actor 创建与调用。上述四十二项仍只是 stored-outer 之前
Gate B/C 的部分证据；新增四项提供 normal-owner stored-outer happy/adoption/GC、两个
pre-Complete 与一个 post-Complete Node-loss 窗口的真实进程证据；仍不覆盖完整 failure matrix。
foreign-input lineage 与 multi-return partial-loss 已有 bounded 代表切片，但尚不覆盖
完整 ownership/failure matrix、PG bundle rescheduling 或并发传输压力。Actor 单元契约还
覆盖停止接收后 drain mailbox、shutdown fencing、Core sequence 和幂等/冲突 replay。

pull 单元契约还覆盖相同对象的并发串行化、失败后 reset/new epoch、pin release 重试
与 shutdown sweep、owner source/target locations、`GetObject` 的 attempt/owner/size/
checksum fencing，以及 enqueue 时建立 submitted-reference。
Core 并发单元契约还覆盖：未就绪依赖停留在 coordinator 且不占 lane；
`PENDING_CAPACITY` 经过有界退避后保持 TaskID/AttemptID/LeaseID，以 $O(1)$ 瞬时 Node
重评重排；耗尽预算时发布 typed error。Worker Core 单元契约覆盖 runtime binding
隔离与恢复、按 job
懒创建和 drain、parent attempt 范围内的 child ID，以及 foreign job 在执行前被 fencing。
lease ambiguity 单元契约还覆盖 cancel-before-request tombstone、GRANTED cancel/replay
只释放一次、RUNNING 不可取消，以及 Core 在匹配 cancel ACK 前保持对象 pending。
PushTask replay 单元契约覆盖：`RemoteCallError` 被视为可能已经执行；Core 保留原 grant
并重放完全相同的 PushTask、Worker endpoint 和 LeaseID；进入不明状态后，即使后来连接
失败也不能释放原 lease。只有第一次 `TransportConnectionError` 能证明 Worker 未收到请求，
允许进入原 grant 的 release/cancel 路径。Worker 已缓存的完全相同重放只补发完成通知，
改变请求内容的同 attempt＋lease 重放会被拒绝。
accepted-Push/shutdown 单元契约还覆盖：Worker 关闭新准入后仍服务完全相同的已接受请求
重放，clean ACK 必须等待 reply cache 与 `CompleteLease` ACK；Core 在 Push send 前先登记
unresolved，避免 shutdown 越过 mark/send 竞态。Push 或 cancel 结果不明时，短 shutdown
返回 `False` 且保持 owner `PENDING`、dependency submitted tokens、coordinator/lanes、
reference thread 与 sink；有效 TaskReply 或 accepted cancel 收敛后，第二次 shutdown 成功。
borrower 单元契约覆盖 owner endpoint 与 WorkerID 双重 fencing、transfer pin 先于序列化、
Acquire ACK 前不暴露 handle、唯一 borrower token、release-before-acquire tombstone 与精确
transfer 绑定。outer INLINE result 的 contained edge 与 transfer pin 已接入两阶段 obligation
release，并通过两 borrower 生命周期 smoke；foreign stored ref 已支持 descriptor-only owner
lookup 与直接 Node fetch；foreign INLINE dependency 已接 retained protocol；stored physical
GC 已覆盖两副本删除和 owner metadata/lineage 收敛。foreign wait/drop 与递归
lineage 已接通。contained ObjectID cycle 已选择 fail-fast DAG policy，并由
`test_contained_cycle_policy.py` 覆盖 Python 容器自环的层次区分、批量原子拒绝、self-loop、
prepared-edge 并发竞态、exact replay、abort 与 container release。当前统一
runtime publication/GC 已接入图权威，但这些历史 pure tests 本身不能作为
完整端到端故障验收。
CPU yield Phase 2 单元契约覆盖 Node blocked/unblocked handlers 的完整 identity/sequence
fencing、$O(1)$ transition、unblock tombstone、只归还 CPU并保留 GPU/自定义资源，以及
unblock 后的 signed CPU debt；调度可见 availability clamp 为非负。completion/abandon/
worker loss 共享 terminal finalizer。Worker notifier、Core 真实等待点和单 CPU、双 Worker
blocking-get smoke 已接通；ready fast path不通知，`get_many()` 合并一个重入 episode。
reconstruction P0 单元契约还覆盖状态突变前预校验与 shutdown admission fence：拒绝路径
不得推进 attempt、消费 retry budget、清理 descriptor 或增加 accepted count。
cluster shutdown 纯单元契约覆盖单一 epoch、所有 Node BeginDrain 后的连续 clean barrier、
协议非法 clean claim 拒绝，以及 Driver Core commit-before-Node-finalize；Core preflight 或
commit 失败时不得发送 Node Finalize，GCS 始终最后停止。

K0 候选 smoke tests：

- 未就绪依赖先提交，ready 后才取得 CPU/Worker；
- 一次用户异常传播，且不发生系统 retry。

K1 候选 smoke tests：

- 一个 borrower 加一个 nested ref 的真实生命周期；
- 单 CPU 节点上 blocking `get` 让出 CPU 后完成一个子任务；
- 删除一个 task 输出副本后执行一次 lineage reconstruction；
- 精确杀死一个 Worker 或一个逻辑节点后执行一次有界恢复；
- 两 bundle PG 在第二节点 prepare failpoint 后全局 abort；
- 精确杀死一个 Actor Worker 后执行一次 generation restart（已验收）；
- Actor 所在 Node 死亡后迁移到 survivor（已验收）。

即使测试使用 `multiprocess_smoke` marker，也不能整组运行。先阅读 fixture 和进程
拓扑，再通过标准库有界 runner 运行一个完整且已静态审查的 node id，例如：

```bash
python scripts/run_bounded_test.py \
  tests/integration/test_task_path.py::test_one_node_one_worker_task_path
```

runner 不接受任意 pytest 参数，也不接受文件、目录或 marker 作为选择器；完整 node
id 必须先写入脚本中的 `ALLOWED_NODE_IDS`。它固定使用当前 `sys.executable -m
pytest`，为 pytest 创建独立进程组，并把 pytest 的正常退出码原样转发。单项测试
超过 30 秒时，runner 从只读进程表构造 pytest PID 的精确 descendant closure，连同
Node 自建的进程组逐个发送 `SIGTERM`，短暂重扫后才对仍存活的精确 PID/PGID 发送
`SIGKILL`，并以 124 退出；目标必须是正整数且排除 runner 自身组，禁止用 `pkill`、
进程名或其他宽泛条件清理。
若 runner 收到 `KeyboardInterrupt` 或其他非正常中断，也会先对同一精确 tracked tree
执行上述清理，再传播原异常；独立 session 不会因 Ctrl-C 成为孤儿集群。

allowlist 中现有四十七个 node id 均有历史逐项通过记录：四十二项属于 stored-outer 接线之前的
multiprocess 基线；四项 stored-outer smoke 当前分别为 **0.96s**、**1.27s**、**1.17s**、
**1.32s**；secondary promotion 最新为 **1.09s**。这不是四十七项同轮全量复验。既有十一项、PG prepare rollback、
foreign-input lineage reconstruction 和受最新 Worker/Core 修改影响的
三项已在本轮再次通过，其余项沿用本次相关改动前的当前-checkout记录。这不授权整组运行，
也不意味着未来修改后可以跳过静态审查。其余候选场景的
名称仍只是规划。

## 5. 本机禁止的 H/U

下列内容在 M4 16 GB 本机不运行：

- production Ray 的 Bazel 构建、`python/ray/tests` 或完整集群测试；
- 导入/启动 production Ray 做 differential test，除非另行证明资源边界；当前按 H；
- 超过 2 节点、2 Worker、1 Actor、2 PG bundle 或 20 Task；
- 绕过默认 `-m unit` 后收集全套测试、整个 integration 目录、pytest-xdist 或任何
  并行 pytest；
- 吞吐/延迟 benchmark、soak、race hunting、chaos、未严格限例数的 property/fuzz；
- 大对象、ObjectStore 压力、spilling、多轮 reconstruction/restart；
- Docker/Kubernetes、外部 Redis、真实多机、云服务、网络分区和 TLS；
- GPU/CUDA 测试、production Ray C++ core 编译；
- 无明确硬上限、无精确 teardown 或用随机 sleep 协调的任何脚本。

## 6. 分层 gate

### Gate A：纯模型

先验证 ID、状态机、资源守恒、不可变对象、幂等 token、scheduler pure function、
PG transaction 和 trace causal order。Gate A 目标只含 L0；当前默认 marker 与这个目标
尚有差距，见页首安全复核。最近完整历史记录为
**1668 passed, 54 deselected in 11.04s**，对应当时的引用协议、contained-cycle/Actor-argument/publication policy、startup rollback、semantic trace contracts、large by-value argument lift、owner-wide replica cleanup、owner-death convergence，以及其余带 `unit` marker 的基线。它不包含标记为
`loopback_smoke` 的 transport/trace 测试，也不包含 multiprocess integration test。
loopback 的当前通过记录为 **7 passed in 0.28s**（因端口绑定权限在沙箱外运行）。
五十个 multiprocess 测试有累计逐项通过记录：四十二项是 stored-outer 接线之前的基线；
四项 stored-outer bounded smoke 当前为 **0.96s**、**1.27s**、**1.17s** 和 **1.32s**。
secondary promotion 最新为 **1.09s**；它们来自不同验证轮次。
各测试的当前耗时记录见第 4 节的 exact node ID 清单。
它们均由 30 秒 bounded runner 单独执行，是 Gate B 的部分证据；不改变 Gate A 的
覆盖边界，也不构成完整 K0/K1。

### Gate B：K0 真实多进程

Gate A 通过且 runtime fixture 经静态审查后，逐个运行 K0 smoke。每个测试必须同时
断言用户结果、协议事件、资源回收和进程清理。

### Gate C：K1 单故障恢复

Gate B 稳定后，逐个运行 K1 smoke。每个测试只注入一个语义故障；必须断言 ID
不变量、旧消息 fencing、恢复预算和 teardown。不能因为设计为 smoke 就自动归类为
本机安全，每个测试实现后都要重新审查。

### Gate D：远程重型 CI

规模、性能、重复故障、fuzz、生产 Ray differential test 和真实多机只进入单独的
远程重型环境。它们永远不是本机默认 gate。

## 7. Python 3.9 兼容性状态

项目声明 `requires-python = ">=3.9"`。当前证据是源码静态兼容性检查以及在
Python 3.9 环境中的包导入验证；尚未宣称 Python 3.9 上跑过完整 Gate A，更没有
用它验收 loopback 或 multiprocess 路径。后续若修改语法、类型注解或 import-time
行为，必须重复这两项检查。

## 8. 测试必须验证的系统不变量

1. retry/reconstruction 不改变 TaskID 或 ObjectID，只增加 AttemptID；
2. Actor restart 不改变 ActorID，只增加 generation；
3. 本地资源账本永不超卖，资源只归还一次；
4. lease grant 后普通 Task 的执行路径不经过 GCS；
5. Worker 只在 StartLease ACK 后执行，缓存 reply 或 seal 结果后才 CompleteLease；
6. 模糊的 Core timeout 不释放 RUNNING lease；PushTask 只有首次连接建立失败可释放或
   取消未启动 grant，RemoteCallError/发送/接收不明后只能精确重放同一 PushTask＋LeaseID；
7. 未 seal 数据不可见，大对象不经 GCS/Driver/Task RPC 中转；
8. owner token 尚存或 transfer/reconstruction 进行中时，不删除最后一个副本；
9. 旧 attempt/generation/transaction 消息不能修改权威状态；
10. PG 全部 commit ACK 前不可见，prepare 失败后所有账本完全回滚；
11. 应用失败不被误判为系统失败而自动重试；
12. 每个测试退出后无孤儿进程、残留 socket、未 seal 对象或资源泄漏。

真实进程 smoke 的 teardown 必须用 `os.kill(pid, 0)` 对启动报告中记录的 Node 和
Worker PID 分别确认 `ProcessLookupError`，并断言 shutdown report 的 `finalized` 为
真；仅检查 `multiprocessing.active_children()` 不足以证明嵌套 Worker 已退出。

比起只断言 `ray.get(ref) == expected`，这些不变量和 trace 才是 mini-ray 作为教学
项目的主要验证结果。

当前 trace 的 schema、进程内序号、内存/JSONL sink、跨进程事件汇聚和普通 Task
四条跨 PID RPC 因果边已有测试覆盖；更宽的异步队列因果传播仍属于后续范围。
