# mini-ray K0＋K1 历史设计

> **归档说明（2026-09-09）**：下文是基础版改造前的 K0/K1 历史规格及当时记录，原始源码版本为
> `ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e`。其中“当前”“已接通”、旧文件/行号、GCS 发布、
> global DAG、multi-return/targeted 和 Actor migration 均按该历史上下文理解，不描述活动源码。
> 第一阶段 owner-led 基础版已在实现与验收，第二阶段两项增强是确定交付。
> 当前范围见 [两阶段计划](redesign-plan.md)，运行路径见 [学习入口](learning-path.md)，
> 实际证据及未完成项见 [验收账本](acceptance-baseline.md)。旧 correction-plan/handoff 不再决定实施授权。

> 状态：设计规格与当前实现说明，v0.1 建设中。本页显式标为“当前已接通”的路径
> 已有对应测试证据；其余“应”“必须”和流程图表示 K0＋K1 目标语义，不表示已经
> 实现。当前已接通两节点 GCS/Hybrid spillback/direct submission 和跨节点对象依赖
> pull、普通 Worker 内嵌 Core 子任务、K0 Actor 创建/直达调用和两 bundle Placement
> Group；远端普通 Task 的 Node crash Phase-A 恢复和同一存活 Node 内的 Actor Worker
> restart Phase B1 已接通；PG participant Node-loss 终态 LOST、Driver-local Node recovery
> 和 Actor Node-loss migration 也已接通；PG bundle rescheduling 按 §11 显式省略。
> 当前普通 Task 的所有成功结果已走统一 selected-output 后端：Worker discovery、
> Node batch journal、GCS metadata recovery 与 Core batch adoption/GC 已接线，涵盖
> ref-free、contained、mixed-tier multi-return 和 targeted outputs；不再将 contained
> 结果投影到旧单返回协议。接线不等于完整验收：历史 STORED/INLINE 单返回及大 nested
> 参数的 bounded 证据不能自动覆盖新后端与完整故障矩阵，K0/K1 仍未完成。
> GCS 不转发结果 bytes，但当前普通成功路径同步参与 INTENT/ARM 和 owner 的
> terminal/adopted 报告；Node local Complete/lease release 本身不等待 GCS terminal。
> 这是 mini-ray 的教学协议，不是 production Ray 的普通任务原始成功路径。
> 故障验收现只使用统一四阶段 OutputPublicationGate；旧双 gate/API 与 Worker
> 单返回 coordinator、Core/Node/GCS/owner旧关联、独立旧模型和旧结果线协议均已退役。统一后端的
> STORED/INLINE 阶段、mixed/targeted UNKNOWN、未报告 Complete 和 pre-Complete owner-death
> 均有各自限定的进程证据；存活 Worker owner 的 Node-loss retry 与真实 GCS 环拒绝也已有窄验收。
> F6 foreign late-replica 也有独立进程证据；更宽组合、同版完整门禁仍开放，不能外推 K0/K1 完成。
> 最新运行版本及 gate 状态统一记录在 [当前状态](current-status.md)，本页不复制易漂移的计数。

## 1. 设计目标

mini-ray 用尽可能小的 Python 代码回答五个问题：

1. `f.remote()` 为什么无需等待用户执行就能返回一个 `ObjectRef`？
2. 谁决定任务在哪个节点执行，谁真正把任务发送给 Worker？
3. 对象的逻辑身份、物理字节和位置元数据分别由谁拥有？
4. Worker、节点或对象丢失后，哪些 ID 保持不变，哪些 incarnation 必须更新？
5. 多个资源 bundle 如何做到“要么全部可用，要么全部不可见”？

答案不是一个中央任务队列，而是控制面、调度、执行和对象系统之间的协议。K0
建立 happy path 的真实进程边界；K1 在同一模型上加入引用生命周期、故障恢复和
gang scheduling。

## 2. 进程拓扑与职责

v0.1 当前可在一台机器上用 `multiprocessing` 的 `spawn` 方式启动一个独立 GCS、
一个或两个逻辑节点，以及每节点 1–2 个固定 ordinary Worker slot；Actor 使用独立的
专属 Worker。ObjectStore 由对应 NodeManager 进程
持有。逻辑节点用于保留网络、故障和资源所有权边界，不冒充真实性能或真实多机
验证。

| 组件 | 所在位置 | 权威职责 | 不负责什么 |
|---|---|---|---|
| Python API | Driver 与普通 Task 执行线程；Worker 线程通过 runtime binding 选择内嵌 Core | Driver：`remote/get/wait/init/shutdown`、RemoteFunction、ActorClass/ActorHandle；Worker 当前验收 `remote/get` 子任务 | 集群调度、对象字节转发 |
| CoreWorker | Driver 常驻；普通 Worker 按 job 懒创建一个 | 构造 `TaskSpec` 和逻辑 ID；提交任务；owner metadata；解析结果和错误 | 节点本地资源分配 |
| GCS | 独立控制面进程 | 当前：带 incarnation/epoch 的节点成员与 DEAD tombstone、资源摘要、Actor 创建/restart、PG 两阶段协调，以及统一输出的 metadata/graph/recovery authority | 普通 Task/Actor method 的逐调用放置调度；大对象字节传输 |
| NodeManager | 每个逻辑节点一个进程 | 当前：本地资源账本、1–2 个固定 ordinary Worker slot、Hybrid spillback、Node-owned ObjectStore、依赖 gate 与跨节点 pull | 用户函数执行；对象逻辑引用所有权 |
| Worker | 受 NodeManager 管理的普通或专属 Actor 进程 | 当前：执行 Task、一次性发现 selected outputs 并请求 Node batch publication；普通 Worker 绑定内嵌 Core 提交子任务；专属 Worker 持有 Actor 实例和串行 mailbox | 全局调度或全局对象目录 |
| ObjectStore | 当前嵌入每个 NodeManager 进程 | 本地对象的 `create → write → seal → read/delete`；只公开完整 sealed bytes | 引用计数、lineage、集群级放置决策 |

逻辑源码职责如下。当前保持平铺模块以便教学阅读；表中一行可以由数个平铺文件
共同承担，不暗示尚不存在的物理子目录：

| 当前文件 | 模块边界 |
|---|---|
| `api.py` | 用户 API、RemoteFunction、ActorClass/ActorHandle 与初始化入口 |
| `core.py`、`protocol.py`、`ids.py` | CoreWorker、不可变 specs、消息与身份 |
| `control.py` | GCS、节点表、Actor 创建/restart manager 与 PG plan/prepare/commit/abort coordinator |
| `node.py`、`resources.py` | NodeManager、资源账本、Hybrid policy、lease 与跨节点 pull |
| `node_death_view.py` | Driver 收齐 survivor snapshot 安装 ACK 后的累计死亡证明；Node 保留、内嵌 Core 本地读取，不另作 failure detection |
| `worker.py`、`actor_state.py`、`actor_client.py` | Task executor、Actor runtime/串行 mailbox，以及 owner 侧 generation/route fencing |
| `runtime_binding.py` | 当前执行线程的 CoreWorker 与 parent attempt 上下文 |
| `object_store.py`、`ownership.py`、`object_manager.py`、`recovery.py` | 物理对象、owner/ref、pull 与 lineage；当前教学切片已接通，完整故障矩阵尚未完成 |
| `output_discovery.py`、`output_publication.py`、`output_protocol.py` | 当前普通成功路径的单次 discovery、execution/selected-slot manifest、Complete witness 与统一 wire contract |
| `output_publication_journal.py`、`output_publication_node.py`、`output_recovery.py` | 当前 Node batch journal/adapter 与 GCS metadata-only recovery；Node/Core 接入 publication、adoption、per-slot GC 和故障收尾 |
| `publication_sources.py` | tier-neutral source capability、provisional/final hold、Node incarnation 与稳定 fingerprint；不包含 journal、RPC 或 mutation |
| `stored_publication.py` | 仅历史source/Node incarnation pickle名称的同对象re-export；无journal、recovery或旧运行时，旧源码在history/retired-protocol-family/source |
| `publication_gate.py` | 唯一私有四阶段 OutputPublicationGate；只观察精确 publication/manifest 身份，未配置时无等待，不承担发布权威 |
| `contained_cycle.py` | job-scoped contained ObjectID DAG authority；统一 manifest 对全部 selected slots 一次预留图边 |
| `placement.py`、`node_monitor.py`、`trace.py`、`trace_collector.py` | PG、精确 managed-Node sentinel observation 与跨进程 trace；`runtime_state.py` 仅保留 deprecated 教学 facade |

平铺不意味着合并权威职责。例如 ObjectStore 和 owner directory 必须维护不同状态
和接口，即使教学实现把相关适配代码放在同一进程中。
当前统一输出协议遵守这一数据边界：GCS recovery snapshot/work/ACK 只有
manifest、digest、incarnation 与 graph facts，完整 envelope 和结果 bytes 留在
Worker/Node journal/owner 的数据面缓存中。INTENT ACK 先于 child/store effects，
ARM ACK 先于 Node 本地 Complete/lease release。Node terminal outbox 可独立重试；
Core adoption 仍同步向 GCS 确认 terminal、提交所需 graph，并在 owner CAS 后报告 adopted。
因此“本地释放不等待 terminal”不能表述为“整个普通成功路径不访问 GCS”。
GCS 的 ARM 无 terminal 记录只证明 Complete 未知，不能推断未执行。Node 丢失后
owner 根据本地结果作 per-slot KEEP/DROP 决定；已知 Complete 但没有 bytes 的 slot
成为 LOST，再由显式 `get` 请求重建，不能从 GCS metadata 恢复 bytes。未知 Complete 的
清理遵守系统重试预算与原 execution 的 finalizer barrier。当前时序见 §8.1.3；
§8.1.1 仅记录迁移前的历史切片及其证据边界。
已adopted的STORED输出若存在grant-backed secondary，KEEP保留既有owner
membership，实际可读性由当前location集合决定；这不是由GCS或descriptor制造bytes。

## 3. 统一身份模型

所有重试和恢复都建立在四组不可混淆的概念上：

| 逻辑实体 | 物理 incarnation | 规则 |
|---|---|---|
| `TaskID` | `AttemptID` | 一个逻辑任务可以物理执行多次，attempt 单调增加 |
| `ObjectID` | `Replica` | `ObjectID = (TaskID, return_index)`；一个值可有多个 sealed 副本 |
| `ActorID` | `generation` | 重启保持 ActorID，generation 增加 |
| `PlacementGroupID` | reservation transaction | 只有全部 bundle 激活后 PG 才可见 |

消息至少携带对应的逻辑 ID 和 incarnation。接收者只接受当前 attempt、generation
或事务 epoch；旧消息是幂等 no-op 或显式 stale error，不能修改当前状态。

系统语义是：

> 唯一逻辑身份、至多一个权威结果、物理执行可能多次。

它不等于 exactly-once execution。系统重试可能重复运行用户函数，因此对外部世界
有副作用的 Task 必须幂等或自行去重。用户异常与系统失败也必须分开：用户异常
默认不重试；Worker/节点丢失才受系统重试预算控制。

## 4. K0 普通 Task 路径

当前已接通这条路径中的 pending `ObjectRef`、GCS 节点注册/发现、lease、一次
spillback、向本地或远端 Worker direct `PushTask` 和 inline 结果返回；单节点执行的
大结果也可在执行 Node seal 后由 Driver `get` 拉取。Task 以 store-backed `ObjectRef`
为输入时，Driver 保留逻辑 `RefArg`，由目标 Node 在 grant 前完成跨节点 pull。
普通成功结果现统一经过 §8.1.3 的 batch publication/adoption。
下图仅保留 placement、依赖传输与 direct submission 的示意；其旧成功收尾标签
不描述当前统一后端，也不表示 GCS 不参与成功发布。

```mermaid
sequenceDiagram
  participant U as User API
  participant C as Submitter CoreWorker
  participant N1 as Local NodeManager
  participant N2 as Remote NodeManager
  participant W as Executor Worker
  participant S2 as Target ObjectStore

  U->>C: f.remote(args)
  C-->>U: ObjectRef(PENDING)
  C->>N1: RequestWorkerLease(resources, byte-free dependencies)
  N1-->>C: SpillbackLease(N2 endpoint, if Hybrid selects N2)
  C->>N2: RequestWorkerLease(target=N2, descriptors)
  N2->>N1: Pin source replica
  loop 16 KiB chunks
    N2->>N1: GetObjectChunk
    N1-->>N2: bytes + identity/offset
  end
  N2->>S2: checksum + atomic seal
  N2->>N1: Release source pin
  N2-->>C: GrantLease(worker endpoint, target-local descriptor)
  C->>W: PushTask(RefArg + target-local descriptor)
  Note over C,W: direct submission，不再经过 GCS
  W->>N2: StartLease(lease/task/attempt)
  N2-->>W: StartLeaseAck
  W->>S2: materialize RefArg; seal large result (if stored)
  Note over W: ordinary: cache TaskReply; stored-contained: prepare envelope
  W->>N2: CompleteLease(outcome)
  Note over N2: atomically terminal + release allocation
  N2-->>W: CompleteLeaseAck
  W-->>C: TaskReply or ObjectAvailable
  C-->>U: ObjectRef READY or FAILED
```

必须满足以下顺序与所有权：

1. `.remote()` 不等待任务依赖就绪或用户函数执行结束，也不得隐式
   `get()`。返回句柄前仍同步完成参数序列化、必要的lifted-argument seal
   与引用存活交接，随后接纳并排队；这些准备可能耗时或直接抛错，
   “异步执行”不等于“提交只做内存构造”。顶层未就绪 `ObjectRef` 留在 Driver Core coordinator 的 dependency gate，
   不进入 ready queue，也不占 dispatch lane、Worker 或 CPU。
2. 依赖就绪后，提交者向 NodeManager 请求满足资源向量的 Worker lease。
3. 本地节点总容量不可行或策略选择远端时，lease 可以 spillback；被选择的
   NodeManager 必须用本地权威账本再次检查，过期集群摘要不能导致超卖。
4. lease 返回 Worker endpoint；CoreWorker 随后直接 `PushTask`，GCS 不转发 Task
   payload，也不逐任务决定这一步放置。Worker 必须先用完整 lease/task/attempt 身份
   向签发 NodeManager 完成 `StartLease`，得到含 Node incarnation 的 ACK 后才执行用户函数。
5. 所有成功结果先完整 discovery，再完成统一 Node/GCS batch prepare：每槽 INLINE
   或 STORED 只决定物化方式，不改变 publication identity。Worker 在 prepare ACK 后
   排空本地 source/import custody，再从 Node `CompleteLease` 取得权威 envelope 并
   缓存成功 `TaskReply`；
   Core 按统一 owner adoption 路径处理。执行或序列化早期错误仍可先缓存失败选择，
   但只在匹配的 completion ACK 后返回；已开始 publication 的失败还必须先确认补偿。
6. TaskReply 只回答结果是否到达 Core；CompleteLease 回答用户代码是否结束；Node
   lease 状态决定资源是否占用。Core 的 RPC timeout 或断连只表示结果未知，不能
   释放仍可能运行的 lease。Worker 丢失时，Node 必须确认进程退出后才能回收。

当前 Driver Core 把 submission、dependency gate、ready queue 和执行 lane 分离；两节点
运行时启用两条 lane，单节点仍为一条。Node 返回 `PENDING_CAPACITY` 时，任务不会占住
lane，而是进入有界延迟优先队列，随后保持 TaskID/AttemptID/LeaseID，以 $O(1)$ 的瞬时
Node 重评再次判断容量，让其他 ready Task 先行且不制造新的 lease 身份。普通 Worker 则按
job 懒创建单 lane CoreWorker，并在执行用户
函数时通过 thread-local runtime binding 暴露公共 `remote/get`。子任务使用与 Driver
提交相同的 Node lease、spillback 和 direct `PushTask` 路径；child TaskID 的派生包含
parent AttemptID 和提交序号，`TaskSpec.parent_task_id` 仍记录稳定的逻辑 parent。当前
plain-result nested smoke 只覆盖 parent 持有 0 CPU、child 返回普通值；另一条 smoke 已
接通 Worker-owned inline ObjectRef borrower；foreign stored ref 也由 owner 返回无字节
descriptor、borrower 直取 Node。stored physical GC 已接通 owner 冻结 collection plan、
source/target typed Drop、ACK 后 metadata/lineage 收敛；blocking-get CPU yield 的 Worker
notifier 和单 CPU、双 Worker 端到端路径已经接通。
nested trace smoke 还证明
`worker_core` 发出的 child submit、lease request/grant、direct push 和 finish 事件进入 Driver
的统一 collector；transport sidecar 现以独立 `rpc_id` 传播 `cause_event_id`，golden
trace 验证 lease、PushTask、StartLease 与 CompleteLease 的跨 PID send→receive 边。

当前两节点实现中，各 NodeManager 向 GCS 注册地址、总资源和可用资源；Driver 在
启动完成后从 GCS 取得并校验统一快照，再安装到各 NodeManager。首节点运行 Hybrid
policy 并向提交者返回 `SpillbackWorkerLease`；CoreWorker 保持同一 LeaseID、TaskID 和
AttemptID，向目标 NodeManager 发送带 `target_node_id` 的第二跳请求。目标本地账本是
最终分配权威；得到 grant 后，CoreWorker 直连远端 Worker。Worker 的 Start/Complete
消息发给实际签发 lease 的目标 Node，由目标 Node 作为
lease 和资源账本权威完成释放；Core 不发送正常完成 release。每次 lease 的 Hybrid
决策读取 NodeManager 本地安装的不可变集群快照，不逐任务请求 GCS；目标节点仍用
实时本地账本重新校验，避免陈旧快照导致超卖。GCS 不承载 `PushTask`；普通值和
STORED 结果 bytes 也不经 GCS。不过当前每个普通成功 execution（包括无 refs 的普通值）
都同步依赖 GCS 的 INTENT/ARM，以及 Core adoption 的 terminal/adopted 报告；有
contained edges 时还需要 graph PREPARE/COMMIT。只有 Node local Complete/lease release
这一局部步骤不包含 GCS I/O，terminal outbox、owner adoption 和故障恢复仍是独立义务。
这是教学后端的额外控制面依赖，不等同于 production Ray 的普通 Task 成功路径；同步
组合测试也不能替代当前 revision 的专用真实进程故障验收。

Lease 的当前最小状态机是：

```text
GRANTED → RUNNING → COMPLETED
       ↘ ABANDONED / START_EXPIRED
RUNNING → WORKER_LOST   （仅在确认 Worker 已退出后）
```

资源当且仅当 lease 处于 `GRANTED` 或 `RUNNING` 时被占用。`CompleteLease` 重放及
terminal 状态查询必须幂等，不能重复释放。
若 `RequestWorkerLease` 回复持续不明，Core 必须先以相同 LeaseID 重放，之后发送
`CancelWorkerLease`。Node 用 per-lease 串行化让 cancel 与 grant 竞争，并保留取消 tombstone：
cancel 可在 request 前阻止迟到 grant，也可将未启动的 `GRANTED` 原子转为 `ABANDONED`
并只释放一次；`RUNNING` 不可取消。Core 只有收到身份完全匹配的 accepted/cancelled ACK
后才能发布终态错误，否则对象保持 pending 并继续重试同一 cancel。

lease 已 grant 后，`PushTask` 的不明状态独立处理。只有首次连接 Worker 时的
`TransportConnectionError` 能证明请求字节没有到达，因此允许 Core 对原 grant 执行
release/cancel。`RemoteCallError` 不能作此证明：Worker 可能已经执行用户函数、缓存
`TaskReply`，随后在 `CompleteLease` 或 reply 路径失败；发送/接收错误也同样不明。任一
不明结果发生后，Core 保存原 grant、Worker endpoint 和完全相同的 `PushTask`，归还
dispatch lane 并延迟重放同一 LeaseID。即使后续得到连接失败，也不能申请新 lease 或
释放原 lease。Worker 以 `(AttemptID, LeaseID)` 查找缓存，同时校验重放的完整 `PushTask`
相等；pending discovery 重放原 prepare/Complete 与本地 custody cleanup，已有成功
reply 只需收敛 completion ACK，绝不再次调用用户函数或重新序列化。
Worker 的 admission fence 只拒绝新的逻辑 Push。已经接受的精确 PushTask 在 shutdown
期间仍有恢复义务：相同 attempt、LeaseID 和完整请求可以命中 durable reply cache，并在
必要时继续重试 `CompleteLease` ACK；请求内容冲突则被拒绝。Worker 只有在已接受任务完成
缓存、completion ACK 和内嵌 Core drain 后才返回 clean shutdown。

Core 同时以 `ObjectID → (protocol phase, pending task)` 跟踪不明 Push/cancel。第一次可能
到达 Worker 的 send 之前即登记，因此 shutdown 不能越过 mark/send 竞态；精确 Push replay
及 cancel 重试持续更新该 phase。shutdown deadline 到达时，只要协议仍不明，Core 返回
`False`，但不得发布 ERROR、释放 dependency submitted tokens、递减终态计数、停止
coordinator/dispatch lanes/reference thread 或关闭 trace sink。有效 TaskReply、明确的正向
release ACK，或身份匹配的 accepted cancel 才能清除 unresolved；恢复完成后的第二次
shutdown 再执行正常终结。

当前 Core 从 pre-Push lease 请求起就保留统一
`OutputPublicationID(LeaseID, execution)` candidate，并贯穿 target hop、grant、
location report、cancel 与 Push 的 unresolved/replay 状态。它只绑定潜在发布身份，
不提前创建 GCS INTENT，不表示 Worker 已开始执行或发生 child/store effect。
Node death 查询仍须根据同一身份取得精确 GCS 冻结事实，不能用 candidate 存在
推断发布已开始，也不能在不同阶段切回旧 INLINE/STORED 身份。

队列工作与上述执行/owner状态是两回事。`_ReadyTask` 的 `_DispatchKind`
只标记本条lane turn要恢复哪个既有authority：FRESH、LEASE、CANCEL、
PUSH、CUSTODY、OUTPUT_ADOPTION、OUTPUT_NODE_LOSS、SYSTEM_FAILURE。
构造时最多允许一份顶层continuation；lease的ambiguity_round只能随
原lease一起保留，零轮也合法。它不递归限制载荷里的inventory、
cancel ACK、orphan cleanup，不创造新的协议状态、backend或权威。

dispatcher仅对FRESH执行PG新准入；只有两类OUTPUT工作绕过
current-PENDING快捷过滤，继续由原发布/恢复authority判定过时与否。
SYSTEM_FAILURE仍保原过滤规则。kind也不能要求等于当前unresolved
marker：旧CANCEL队列可能遇到已推进的CUSTODY状态，必须让原resolver
保留实际receipts再继续，不能因两者名称不同就丢弃。明确标签使
“新执行准入”和“已接受工作收尾”的阅读边界可见，不改变消息顺序。

Node 处理统一 prepare 时把本地 `ObjectStoreError` 作为明确拒绝返回，Worker
冻结失败 Complete 并请求补偿，而不是永远重放一个确定失败的 seal。
`GetWorkerLeaseOutcomeReply.cleanup_pending` 区分真实执行终态与清理完成：CPU
可以已经释放、lease 已是 COMPLETED/WORKER_LOST/ABANDONED，但只要 child、graph、
replica rollback 或 GCS rollback-report ACK 未收敛，Core 就保留原 Push/outcome
恢复义务，不开始新 attempt。资源已释放不构成可以绕过补偿的证据。

集群 shutdown 使用一个稳定 epoch：所有 Node 先安装 `BeginDrain` fence，Driver Core 与
各 Worker/Core 再共同收敛，纯单元测试固定 barrier、协议校验与 replay 语义。Driver Core
必须在 cleanup endpoint 仍存活时通过 preflight 并 commit；commit 失败绝不发送 Node
`FinalizeShutdown`。Core commit 成功后才并行 finalize Nodes，GCS 最后退出。

Task/Attempt 的最小状态机为：

```text
Task:    CREATED → DEPENDENCY_WAIT → LEASE_PENDING → RUNNING
                                              ↘ SUCCEEDED | APP_FAILED | SYSTEM_FAILED
Attempt: CREATED → RUNNING → SUCCEEDED | APP_FAILED | SYSTEM_FAILED | STALE
```

## 5. K0 调度

资源是稀疏的非负标量向量，例如 `CPU`、`GPU`、`memory` 和自定义资源。实现
必须区分：

- **feasible**：节点总容量足以满足请求；
- **available**：节点当前空闲容量足以满足请求。

集群内没有 feasible 节点时为 `INFEASIBLE`；有 feasible 节点但都暂时繁忙时为
`PENDING_CAPACITY`。这两个状态不能合并。

简化 Hybrid policy 的步骤是：

1. 过滤死亡和总容量不可行的节点；
2. 优先当前 available 的节点；
3. 无 GPU 请求时优先非 GPU 节点；
4. 按关键资源利用率评分，低负载区允许 locality/warm Worker 优先；
5. 在最低分 top-k 中用可注入 seed 选择，避免热点且保持测试可复现；
6. 目标 NodeManager 用本地账本最终校验并签发 allocation token。

这套纯策略和自定义资源强制 spillback 已进入当前运行路径；更宽容量竞争与持续
调度场景仍需逐项验收。`warm Worker` 表示固定进程/函数缓存，不表示一个
Worker lease 可承载多项 Task；当前仍是每 attempt 一份 LeaseID。

Hybrid 前还有一个不同职责的普通新 lease 首跳选择：`lease_policy.py` 的
`preferred_lease_node` 按各 Node 已知持有的去重 stored 依赖字节总量评分。
同一 ObjectID 在每个 Node 上仅贡献一次，正分同分先 home 再 NodeID；没有
正分则沿原 home 路由。这个策略不筛 total/available，也不扣资源。数据 Node
仍可由 Hybrid 判定其它 Node 更合适，甚至 spillback 回 requester/home。
因此 `preferred_node_id` 是实际首跳、`requester_node_id` 仍是 home，
`target_node_id` 在首跳保持 None，只有明确 spillback 的第二跳才定向。
self-spillback 必须与实际收件 Node 比较，不能把“回到 requester”误判为环。

Core 仅在 fresh ordinary admission 提取提示：本 owner 必须是当前 attempt
的 READY_STORED、canonical 内容身份相符且不在 collection/retirement 中，
才使用 owner.locations 多副本。canonical 的原 publisher 可以已无副本，
不能为此忽略健康 secondary。foreign 只用已有 retained descriptor 的 source，
不请求完整位置表。INLINE 或仅 nested 的句柄不计 stored 执行依赖。
这些提示不改原 descriptor、holds、owner、逻辑 ID 或 Node 的 seal/grant 权威。

有已安装 cluster snapshot 时直接筛可达 live Node 并取地址；无完整快照的
Worker 先用正向地址缓存，冷 miss 仅以至多0.75秒（或更短外层期限）复用
已有 GetNodeAddress 查询。失败回退 home，不生成死亡事实；成功经地址格式
验证、同锁复核最新 snapshot/death 后才可缓存。后续 lease/容量/ACK歧义
重放和全部 custody/publication 收尾使用原冻结路由，PG 从不进入此策略。

[首跳实验](../tests/integration/test_lease_locality_path.py)用三个真实公开任务
区分 producer A→B、普通 consumer B 首跳与资源 override B→A，并检查
custody/Push/最终双副本 GC。它是 Driver 有快照路径，不证明 Worker 冷查询
的真实故障组合、全量位置优化或生产吞吐；精确版本结果见状态页。
另一个[Worker冷查询实验](../tests/integration/test_worker_lease_locality_path.py)
实际运行无snapshot的内嵌Core：同一foreign stored source的两个子Task
在评分范围内冷查询一次后命中正缓存；publication adoption自己的地址
查询独立记账。它观察child GC后的foreign lineage Release，再关闭parent
borrower、最后Driver收集parent/source且Worker仍活。成功路径不等于
已验证冷查询失败、并发miss或任意死亡/缓存交错。

NodeManager 的资源守恒必须始终可断言：

$$
T_r = F_r + A_r^{normal} + H_r^{prepared} + sum_b C_{b,r}
$$

其中 $T$ 是总量，$F$ 是空闲量，$A$ 是普通任务分配，$H$ 是尚未 commit 的
PG reservation，$C$ 是已 commit 的 bundle pool。

## 6. K0 对象系统

对象系统刻意拆成逻辑层和物理层：

- **ObjectStore**：节点本地的不可变 bytes。未 seal 的内容不可读，seal 后不可
  覆盖。
- **owner directory**：通常在创建 ObjectRef 的 CoreWorker 内，记录逻辑状态、
  locations、borrower 和 producer lineage；K0 先实现位置与基本生命周期。
- **ObjectManager/pull coordinator**：NodeManager 侧协调跨节点拉取与本地 seal。

跨节点消费当前遵循：读取 owner location → pin 源副本 → 16 KiB 分块传输/校验 → 目标
ObjectStore 原子 seal → 报告新 location → 唤醒依赖。GCS、Driver 和 TaskSpec 不
携带大对象 bytes。重复 pull 和重复 location add/remove 必须幂等。

当前实现中，lease request 只携带 byte-free dependency descriptor。Hybrid 选中目标
节点后，目标 NodeManager 在占用 Worker/CPU 并返回 grant 前 pin 源副本，按固定
16 KiB chunks 拉取；每块身份、offset 和长度都被校验，完整 checksum 通过并在目标
ObjectStore seal 后，grant 才携带目标本地 descriptor 返回。随后 direct `PushTask`
保留 `RefArg`，同样不含对象 bytes；Worker 根据目标本地 descriptor 从其 NodeManager
读取并反序列化参数。源 pin 在成功或失败路径都释放，未完整或校验失败的数据不会
成为 ready 副本。

普通 task 的 by-value 参数先按 `args`、再按 `kwargs` 插入顺序各序列化一次。mini-Ray
为教学性复用一个 `inline_threshold` 同时表达 Ray 的单参数 direct-call 上限与整份 task
RPC inline budget：加入下一个参数后累计 bytes 若仍 $\leq$ threshold 就保留
`InlineArg`，否则把这份已序列化 payload 强制 seal 到 ObjectStore，并在 `TaskSpec` 中
只留下 `StoredArg`。它与用户显式传入的 `RefArg` 分开：前者表示“对象存储中的参数
序列化流”，因此携带 serializer 和 byte-free nested manifest；Worker 拉取并校验本地
bytes 后重建临时 `InlineArg`，由共享 `NestedReferenceImportSession` 解码。于是 container
本身是 readiness dependency，contained ObjectRefs 仍只是 lifetime/import edges。提交事务
的 submitted hold 与 producer-lineage hold 接管生命周期后，Core 才关闭不向用户暴露的
临时 put handle；失败则先回滚 task holds，再关闭 handle 并沿同一 owner-driven GC 路径
删除副本。

前述跨节点对象 pull 以一个 64 KiB 对象、两个逻辑节点和两个任务验收；
`StoredArg` nested 参数增量已有定向单元测试和独立 bounded 多进程记录，当前
整体 gate 状态以 `current-status.md` 为准。已有 pull smoke 不自动证明 argument-lift 事务。项目不实现 Plasma
零拷贝、对象 spilling 或生产级流控/并发传输。

K0 只需形成 owner/location/replica 的正确边界；完整 borrower token、nested ref
和回收协议属于 K1。

为使 K1 reconstruction 可确定性演示，公开的教学 debug failpoint
`miniray.drop_object(ref, node_id=None)` 使用两层 fencing：NodeManager 仅删除匹配
`(ObjectID, AttemptID, owner, node, checksum)` 的未 pin 副本；CoreWorker 收到成功
回复后再以同一 AttemptID 从 owner location set 中删除节点。Node 的删除 tombstone
拒绝旧 attempt 的迟到 `SealObject`，但允许更高 attempt 使用相同逻辑 ObjectID
发布重建结果。它不是正常对象生命周期 API；`put()` 的最后副本丢失后，`get()`
明确抛出 `UnreconstructableObjectError`。

## 7. K0 Actor

Actor 当前使用两条不同路径：

1. 创建：CoreWorker 向 GCS 请求；GCS 选择节点并取得专属 Worker lease，建立
   `ActorID/generation=0`。
2. 方法：handle 得到 endpoint 后，调用者 CoreWorker 直接向 Actor Worker 发送
   `ActorCall`，GCS 不参与逐方法调度。

K0 默认单线程串行 mailbox。每个 caller/handle 的 sequence 保证 FIFO；不同 caller
之间只以 Actor endpoint 的接收顺序为准，不承诺客户端墙钟意义上的全局顺序。
公开 `@remote` class 返回 ActorClass，`.remote()` 经 GCS 创建 Actor；选定 Node 在
Actor 生命周期内持有 CPU/自定义资源，并启动区别于普通 task pool 的专属 Worker。
ActorHandle 缓存 endpoint，后续 method call 由 caller CoreWorker 直达该 Worker，GCS
不参与逐方法调用。Worker mailbox 串行执行并按 caller sequence 保证 FIFO，协议从
K0 起携带并校验 generation。当前 smoke 验证 Counter 三次结果为 1/2/3、专属 PID，
以及 6 个受管 PID/端点完整清理。

K1 Phase B1 已实现同一存活 Node 内的专属 Actor Worker restart。Node 只把受管子进程
sentinel 作为退出证明，精确释放旧 generation 的 lifetime allocation，再向 GCS 重放稳定
exit record。GCS 先发布不含 endpoint 的 `RESTARTING` snapshot，使 owner 撤销旧 route 和
失败所有 old in-flight calls；随后使用 frozen reservation 在原 Node 启动 fresh WorkerID/PID，
再发布递增 generation 和 route epoch 的 `ALIVE` snapshot。构造器重新执行，内存状态回到
初态；旧方法不会在新实例上透明重放。同一经 GCS 确认仍为 ALIVE 的 route 若持续不可达，
调用方返回 typed `ActorUnavailableError`，而不是伪造死亡。

公开 `max_restarts` 当前只允许 Driver-owned Actor。Actor 所在 Node 死亡后，GCS 会撤销旧
route，在 live survivor 上重新预留 lifetime resources，并以 fresh Worker incarnation 发布
递增 generation/route epoch；旧调用失败且不透明重放。当前不支持 method-call retry、
named/detached lifetime、concurrency groups 或 PG Actor。Actor drain 是独立
控制屏障：Node `BeginDrain` 先关闭新 restart admission，GCS 再收敛 frozen initial create、
RESTARTING 和待 owner ACK 的 terminal publication；这些 obligation clean 后才进入 Core/Node
finalize。

## 8. K1 引用、lineage 与故障恢复

当前运行时已提供两个相邻但不等同于完整 K1 恢复的机制：

- `put(value)` 由 Driver CoreWorker 创建无 producer TaskSpec 的 ObjectRef；小值 inline，
  大值 seal 到本地 Node 后发布 location，不占用 Worker。它可被 `get/wait` 和依赖
  pull 消费，但副本全失时明确不可 reconstruction。
- RemoteFunction 可配置 `max_retries`。明确的 Worker `SYSTEM_ERROR` 会在预算内创建
  新 AttemptID/LeaseID 并重提，TaskID/ObjectID 保持稳定；application error 默认终止，
  不消耗系统 retry 预算。显式 failpoint、普通 Worker crash 和远端 managed Node crash
  各有受限纵切面；这不代表完整故障矩阵。已接通的 lineage reconstruction
  切片及其边界见 §8.2。

Worker退出与Task失败必须分开判断：successful Complete已经在存活Node
保留envelope，即使执行Worker在TaskReply前退出，Core仍从原lease outcome
采用同attempt结果，不重执行也不消耗retry预算。adoption后Node只保留
成功witness，不由metadata再制造payload。实际回归见
[Worker-after-Complete](../tests/integration/test_worker_crash_recovery_path.py)；
replacement的未执行lease探针只验证slot/端点及取消，不代表第二次用户执行。
相反，home Node死亡时若Task依赖是该Node唯一副本的put/StoredArg，
无producer lineage不能恢复；home迁移不承诺修复所有输入数据。
[home-route回归](../tests/integration/test_driver_local_node_recovery_path.py)
明确保留小控制参数INLINE，并用2KiB Worker输出/put覆盖迁移后的存储路径。

Core.shutdown的timeout只预算线程join和协作等待；观察/清理RPC有各自
期限，不是整个方法的严格墙钟上限或远端取消。未决lease/Push/custody/
publication等保留owner和重放身份，迟到有效结果仍可能被采用；只有
协议状态允许的pending输出才尝试shutdown错误。Core可重复drain不等于
公开ray.shutdown可相同重试：后者清除公开runtime并推进集群finalization。
clean路径的sentinel monitor持续到Node graceful退出观察后，force/关闭
Process handles前才stop/reconcile；EXPECTED取决于精确Finalize ACK。

### 8.1 分布式引用计数

owner entry 至少区分 local、submitted-task、borrower 和 contained-reference credential。
logical Task 生命周期使用完整
`TaskReferenceHold(kind, submitting_worker_id, task_id, origin_attempt_id)`，不再投影成 raw
token 或 `(submitter, token)`；origin AttemptID 必须属于 TaskID。SYSTEM retry 保持原 hold，
lineage reconstruction 以新的 reconstruction AttemptID 建立新 incarnation。retained RPC
必须回显完整 hold，owner active/tombstone/cascade 也以完整值为键。其他引用协议同样使用
唯一 typed token，而不是脆弱的裸整数增减；重复 acquire/release 必须幂等。

`ObjectRef.close(*, timeout=None)` 复用 GC 的同一 finalizer，只发起一次释放。
默认 `None` 保持原等待语义；有限非负秒数限制本地 release receipt 等待，
`0` 是非阻塞检查。超时抛 `TimeoutError`，handle 仍 closed、不可再序列化，
原清理义务不取消；再次 close 等待同一 receipt。无绑定的 detached handle
仍为 no-op，非法 timeout 在任何状态变更前拒绝。receipt 只表示本地事件／
保留的 release intent，不保证远端 owner ACK 或 physical GC 已完成；后者
仍由既有 borrower/replica/lineage 协议及 shutdown barrier 判断。

第一条真实 borrower 纵切面已经接通 Worker-owned inline ObjectRef。`owner_address` 只是
访问 owner service 的路由 endpoint，`owner_worker_id` 才是稳定逻辑身份；Acquire/Get/
Release reply 同时校验 ObjectID、双方 WorkerID 和 token。Worker 对包含 ObjectRef
的外层结果只做一次无副作用 discovery serialization，保留源 handle 并生成唯一
transfer identity；随后由统一 selected-output publication 协议安装 contained hold，失败则
按已执行的 exact effects 回滚，不能把 discovery 当作 pin 已生效。
接收 Core 为每次反序列化生成唯一 borrower token，同步得到 owner Acquire ACK 后才暴露
Python handle。Acquire 必须精确匹配 transfer pin；Release 使用同一 borrower 身份并留下
tombstone，令 release-before-acquire 或迟到 Acquire 都不能复活引用。重复 outer `get()` 会
形成新的 borrower token，因此第一次 handle 释放后仍可再次安全恢复并读取同一 inline
logical object。

普通 contained-result borrower 也使用持久释放义务：在第一次 Acquire 之前冻结 owner
route、完整 Acquire/source 与配对 Release。成功句柄的 close、模糊 Acquire 的补偿、错误或
丢失的 Release ACK 都重放同一身份；只有完整匹配的 accepted ACK 才删除义务。shutdown
会把仍存活的 ordinary borrower 转成 Release intent，未收敛时拒绝 clean finalize。
普通网络失败只表示 owner route 暂时不可达，返回 `OwnerUnavailableError` 并保留原
obligation；它不能作为进程死亡证据。只有 Node 的受管子进程观察经 GCS Worker death
journal 提交后，各 Core 才安装 WorkerID death fence、清理该 borrower/submitter 的 owner
引用，并终止明确指向 dead owner 的出站义务。`EXPECTED` Worker exit 只推进 journal
游标，不以 death sweep 掩盖正常 shutdown 中应显式完成的 release。

INLINE outer result 在 graph reservation/commit 后将 contained edge 安装到 outer owner；
outer 最后引用消失后，owner 先冻结 metadata 并持久保留 release obligation，在全部
contained owner ACK、graph `RELEASE_CONTAINER` ACK 后才完成 inline metadata collection。
foreign stored ObjectRef 已通过 descriptor-only owner lookup 与 direct
Node fetch 接通。foreign INLINE ref 作为 Task dependency 也已接通：提交者先从原 borrower
取得 owner 对独立 retained hold 的 ACK，随后原 handle 可立即关闭；PENDING 查询停在
dependency gate，READY_INLINE 才改写为 `InlineArg`，逻辑 Task terminal 后按 ACK 释放 hold。
foreign READY_STORED dependency 保留 `RefArg` 和无字节 descriptor；target Node 在 grant 前
pin source、分块校验并 seal，grant 后 Core 以 retained credential 向真实 owner 报告 target
location，所有 ACK 收敛后才 Push。Driver/GCS 均不读取或转发对象 bytes。stored physical
GC 在最后 token 消失后冻结完整 replica/contained-release plan，持久化 Drop obligation，
只以身份匹配的 ACK 收敛 source/target replicas，随后删除 metadata、descriptor、waiter 和
producer lineage；对应有界真实进程 smoke 的当前结果见 `current-status.md`。foreign `wait()` 与公开
`drop_object()` 已接通；foreign-owner single-return 可由 borrower 请求 owner 重建。
foreign-input lineage 也已接通：重建先以 owner-side old-hold→new-hold 原子换代保住每条
外部依赖，再按 dependency-first 顺序恢复 LOST input，所有 ACK 收敛后才提交本地 attempt；
最终 output sibling GC 才释放这些 holds。对应 bounded smoke 的当前结果见 `current-status.md`。
multi-return partial-loss 已用 target-only execution identity 接通，并保持健康 sibling 的
attempt、descriptor 与 locations 不变；教学版仍省略 borrower tree 优化。
Worker 的 typed shutdown 只有在 incoming retained holds 全部释放后才返回 clean，并在此之前
继续服务 exact retain replay、query 和 release。`Worker.stop()` 仅是进程退出/异常兜底，
不具备这项协议收敛承诺，不能作为 clean-shutdown 证据。

#### 8.1.1 历史单返回后端：publication、adoption 与 reverse GC

本节保留迁移前 STORED/INLINE 分支的设计与历史验收记录，不描述当前普通 Task
成功路径。下文的单返回限制、旧 coordinator/RPC 与局部 smoke 证据均属于该历史
后端，相关源码已移至 `history/retired-protocol-family/source`；当前实现见 §8.1.3，
不能据此声称新统一后端已完成同版完整验收。

这一纵切片解决的是一个比“把 pickle bytes 放进 ObjectStore”更难的问题：stored outer
中的 child ObjectRef 必须和 outer bytes、contained graph、owner metadata 一起跨越故障边界，
且用户 reducer 不能为了判断 INLINE/STORED 被执行两次。当时运行时仅接受单返回、非
targeted execution，以及 executor Worker 直接拥有或持有效 borrower token/source 的 child；
当时明确拒绝 multi-return contained publication；该限制已由当前统一后端接线替代。

Worker 先用 `StoredReferenceExportSession` 做一次无副作用 discovery serialization。payload
较小则进入当时的 INLINE 分支；payload 较大时，现已删除的 Worker 单返回 coordinator 以同一
`StoredPublicationID(LeaseID, TaskID, AttemptID, outer ObjectID)` 驱动：

1. Node `OPEN` journal，先于所有远端副作用；
2. 将完整 manifest 与 publishing Node incarnation 上报 GCS，并取得 exact intent ACK；
3. 只有 ACK 后才对每个 child owner `PREPARE` provisional hold；
4. `FREEZE` 有序 transfer manifest、payload size/checksum 和 digest；
5. GCS 对完整 edge manifest 执行原子 graph `PREPARE`；
6. Node seal outer replica，再把 provisional holds `PROMOTE` 为 outer-owner final holds；
7. `CompleteLease` 令 Node 生成不可变 envelope，Worker 随后缓存只含 descriptor/envelope 的
   `TaskReply`。

`Complete` 是 rollback boundary。publishing Node 存活时，此前的失败由 Node recovery
record 驱动 reverse rollback；
成功后状态为 `COMPLETED_UNCLAIMED`，Node 不允许 clean finalize，也不因 reply 丢失回滚。Core
通过 lease outcome 或 TaskReply 取得同一 envelope 后，持久化 `_StoredAdoptionObligation`，再按
`claim Node → replay promotion fence → GCS COMMIT graph → atomic owner descriptor+edges commit
→ READY wake → Node ADOPTED ACK` 推进。任何 transport ambiguity 只重放第一个未完成的 exact
effect；不会普通发布 descriptor，也不会开启新的 attempt。

outer 最后一个引用释放时，Core 从 owner metadata 冻结 collection plan。顺序固定为：逐个
child exact release ACK，GCS `RELEASE_CONTAINER` 返回同一 edge set，删除 outer replica，最后
原子收集 owner metadata/descriptor/lineage。图权威不会早于真实 pins 被移除，bytes 也不会
早于图 release ACK 被删除。

存活 normal owner 由 Core adapter 驱动；owner death 则由 GCS 的独立 publication
owner-death authority、runtime、仲裁器与 Node fence/finalize 协议接管。INLINE 与 STORED
publication 都冻结 exact workset，并与并发 Node-loss 归并到唯一终态；Node publisher
以 `OWNER_DEATH_CLEANED` 记录这一独立生命周期终态。
普通 dead-owner STORED 副本由独立 `OWNER_WIDE_SWEEP` 扫描和删除，不要求存在
publication record；late seal/pin 受相同 owner-death fence 约束，暂被 pin 的副本保留
重试义务。GCS 后台推进普通 owner-wide sweep 与 publication saga 两类工作。
该历史模型阶段只有部分 pure/runtime 证据；当前统一后端的 borrowed-child 与
pre-Complete owner-death 多进程范围另见 §8.1.4，不从历史通过自动推导当前覆盖。
intent-before-effect 已接入 runtime：完整 manifest 与 Node incarnation 在首个 child effect 前
取得 GCS exact ACK。`stored_node_loss.py` saga、GCS takeover runtime 与 Core composition
已接通，PENDING/adopted 两条 full-flow 有 unit evidence。Complete 后 Node death 在 owner
commit/READY 前执行 dead fence，不复活 DEAD location，也不转入普通 retry；takeover 随后
驱动 pre-Complete rollback 或 post-Complete lost-result retirement。intent ACK 后、任一 child
effect 前的故障窗口与 replica sealed＋全部 promotions ACK 后、Complete 前的故障窗口，
当时已有 bounded multiprocess smoke。Node 已提交 successful Complete、但 TaskReply
尚未离开 Worker 的窗口也有历史记录，并验证
`SUCCEEDED + LOST` retirement 后仅由显式 `get` 启动 lineage reconstruction。
owner-death、Node-loss 与 normal adoption 的入口共享仲裁 gate；完整的真实进程故障矩阵
仍以 `current-status.md` 和有界测试清单为验收依据。

含 refs 的 INLINE 分支使用独立 byte-free recovery manifest，而不是把 STORED 协议或
完整 result envelope 搬进 GCS。Node journal 留存完整 envelope；GCS 在 child effect 前
ACK intent，graph PREPARE 与 child pins 完成后 ACK PREPARED。随后成功 `Complete` 仅在
Node 本地跨越 journal boundary、释放资源并返回同一 envelope；terminal report 由后台
outbox 重试。报告不持有 publication lock，因此 GCS 不可用时 exact Complete replay 和
outcome read 仍可读取 Node 保留的 bytes；未收敛的报告继续阻止 clean finalize。

publishing Node death 冻结三类证据：intent-only 对应 `PRECOMPLETE_ROLLBACK`；PREPARED
但无 terminal 对应 `COMPLETION_UNKNOWN`；已记录成功 terminal 对应 `POSTCOMPLETE_RESOLVE`。
后两类 work 都没有 bytes，必须由 exact owner 在本地 custody/delivery lock 下单调选择：

- KEEP：已有完整 completed envelope 或匹配的 committed owner receipt，才能继续 graph
  commit 与原子 owner adoption；若随后判定 stale，则走 retirement。KEEP 不是由 GCS
  重新构造 bytes，UNKNOWN 的冻结 terminal 也仍是 `None`。
- DROP：没有可保留的本地结果时先安装迟到结果 fence，再向 GCS 确认该决定；child holds
  与 graph cleanup 都取得 exact ACK 后，publication 才终态为 LOST。

已知成功的 DROP 令任务 `SUCCEEDED`、ObjectRef `LOST`，保留 lineage，只有显式 `get`
启动 reconstruction。UNKNOWN 的 DROP 不伪造任务成功；清理后按系统故障预算重试。
LOST 先可见不等于新 attempt 已可进入：旧 logical-task finalizer 必须先释放 input
holds、完成 accepted-task 计数，barrier 才允许 reconstruction 和 last-reference GC；
旧 finalizer 的迟到 replay 也不能收尾新 execution。除同步 Node/GCS/Core pure 测试外，
`test_inline_node_loss_path.py` 的两个 bounded 验收已覆盖报告成功 terminal 后的
KEEP/DROP：前者真实收到 envelope、在首个 owner graph COMMIT 前杀 Node；后者
Complete/outcome 均未放行即杀 Node，等 `SUCCEEDED + LOST` 与旧 finalizer 收尾后才
显式 `get` 重建一次。测试不在 Driver 丢弃 bytes；child 是通过 nested 参数借用的
Driver-owned ObjectRef，覆盖单个 TaskHoldSource INLINE borrowed source。这些文件
现已迁移到统一 gate/后端并分别复验；当前还新增了一项 OwnedChild STORED
ARM-UNKNOWN 验收。历史模型不自动覆盖其余 UNKNOWN、STORED shared-borrowed
和 owner-death 故障组合，当前范围见 §8.1.3。

#### 8.1.2 两张不同的“环”与教学版 DAG policy

Python value 内部的对象图与 Ray 的逻辑对象图不是同一层。比如 `xs = []; xs.append(xs)`
只有一个待序列化 value；pickle memo 可以恢复 `xs[0] is xs`，它没有产生任何
`ObjectID → ObjectID` 边。mini-Ray 保留这种普通 Python 容器自环。另一种环来自 outer
object 的序列化结果包含 `ObjectRef`：`A contains B` 会使 B 的 owner 持有 contained pin，
直到 A 的 metadata 被收集。若同时允许 `B contains A`，仅靠引用计数时两边都不能先归零。

生产 Ray 的 reference counter 保存 contained edges，但没有把 distributed ObjectID graph
交给一个 SCC/tracing collector；Python global GC 也只处理进程内 Python 对象环。mini-Ray
选择一个更适合教学项目的显式缩窄：contained ObjectID graph 必须始终是 DAG，不实现第二套
分布式 tracing GC。`contained_cycle.py` 提供 job-scoped pure authority：一个结果 manifest 的
所有 candidate edges 作为批次先 `PREPARE`，用三色 DFS 做 $O(V+E)$ cycle preflight，再随
结果发布 `COMMIT`，失败则 `ABORT` 并补偿 pins。self-loop 与任意回边返回带闭合 cycle path
的 `ContainedReferenceCycleError`，整批不产生部分状态。

`PREPARED` 边也参加后续 DFS，这一点关闭了两个并发发布各自只读检查的 TOCTOU 窗口：
`A → B` 与 `B → A` 最多一个能得到 reservation。transaction identity 的 exact replay
幂等，冲突 replay 被拒绝；container collection 通过 `RELEASE_CONTAINER` 原子取回完整 edge
identities。collection 必须先从 owner metadata 冻结同一批 release obligations，收到全部
contained-owner ACK 后才能 retire authority edges；因此 cycle graph 不会早于真实 pin 消失。
当前统一 publication 将全部 selected slots 的边作为同一个 graph manifest：Node 在
materialization 前取得 `PREPARE`，Core 在 owner CAS/READY 前取得 `COMMIT`，per-slot
collection 在 child ACK 后取得 `RELEASE_CONTAINER`。mixed-tier multi-return 与 targeted
contained 已使用这条路径，不再因单返回限制被拒绝。历史 STORED/INLINE 单返回、
INLINE 双 borrower 与已知成功 Node-loss KEEP/DROP 的 bounded 证据仍有参考价值，
但不能自动作为新 batch 后端的完整验收。DAG 约束是 mini-ray 更强的教学模型，
不是生产 Ray 已有的全局 cycle collector；分阶段及 shared-borrowed/mixed/targeted
故障证据见 §8.1.4，不将一项通过泛化成完整组合矩阵。

F7 的[真实控制面用例](../tests/integration/test_contained_cycle_control_path.py) 将两个
metadata-only proposal 绑定到实际注册 publisher，经 unified INTENT 和 GCS TCP handlers
依次 PREPARE `A → B`、拒绝形成环的 `B → A`，验证拒绝无部分 reservation；
真实 ABORT 后原请求可按合法状态推进，旧 transaction／INTENT／ARM 被 fencing。
这两个 ObjectID 是协议模型输入，不是公共 API 创建的对象，也不证明 child custody。
其 rollback 只记录真正收到的 GRAPH_ABORT ACK，不虚构 child prepare/release 或 Complete。
正向 COMMIT/RELEASE 另由一个公共任务的实际 publication、owner GC 和 slot-collected
记录证明；`GetContainedGraph` 的 FOUND 只是 retained manifest，不能当成 active edges。

#### 8.1.3 当前后端：一个 execution 的统一输出生命周期

当前普通 Task 成功路径让 INLINE/STORED 只决定每槽的 materialization：
同一个 execution 使用一个 manifest、一项包含全部 selected edges 的 graph reservation
（无 edges 时省略图操作）、一个成功 Complete 与一次 selected-output owner CAS。selected ordinal 用于
索引 manifest；ObjectID.return_index 始终保留原完整返回列表中的位置。

当前 Worker 已接入唯一 `OutputDiscoverySession`：先对所有 selected values 各
序列化一次，完整验证后才开始外部 effect。源 handle 与参数 import session
由一个 `_PreparedOutputReply` 保活；unknown ACK 重放原 stream 和 identity。
成功 facts 与本地 custody drain 是不同义务：即使已有 prepared result 或
成功 outcome，清理抛错后也要再次完成 drain 才能退休缓存。ref-free、contained、
mixed-tier multi-return 与 targeted outputs 都通过同一 `PrepareOutputPublication`
进入 Node；成功 `TaskReply` 和 lease outcome 使用同一统一 envelope/witness，
Core 使用 batch adoption/per-slot GC，不再投影到旧单返回 INLINE/STORED 后端。

当提交者在 survivor Worker 自己执行重试时，executor 与 output owner 可是同一
WorkerID。两个生命周期仍须分开：final hold 使用既有 token，provisional hold 使用
`provisional:<final-token>`。`PreparedContainedTransfer` 要求相同 outer 和这一精确
命名空间关系，不能仅“任意两个不同 token”就通过；不同 owner 保持原来的同 token、
不同 WorkerID 规则。promotion 在 child owner 锁内删除 provisional、安装其释放墓碑、
再建立 final；旧 provisional release 不得误删 final。序列化引用仍指向 final hold，
没有新增 wire 字段、后端或 owner 权限。真实 owned/borrowed 及 targeted shared-child
纯合同见 [test_same_owner_output_custody.py](../tests/unit/test_same_owner_output_custody.py)。

统一组件已接入当前 Worker/NodeServer/GCS/Core 路径，职责如下。已有纯组合与
局部运行时证据不等于完整 K0/K1 或全部真实进程故障组合已经验收：

- `output_publication.py`：execution/selected-slot identity、metadata manifest、
  Complete witness、含真实结果的 data-plane envelope；不执行网络或所有权转移。
- `output_publication_journal.py`：Node 唯一 intent/ACK phase authority。
  prepare/materialize/promote 前记录相应补偿身份，rollback 按可能发生的
  effects 而非仅已收到 ACK 的 effects 逆序收敛。
- `output_recovery.py`：GCS 只保存 metadata。INTENT ACK 允许开始 effects；
  ARM ACK 允许本地 Complete，ARM 自身不证明执行成功。Node death 冻结原
  work，晚到 terminal 不重写历史；owner 使用完整 per-slot KEEP/DROP vector，
  不因混合输出中一个 slot 丢失而销毁健康 siblings。
- `control.py` 中的 `PublicationControlAdapter`：在同一个 composition lock
  下组合 publisher admission、graph 与 recovery，并拥有 cleanup ticket／
  回执进度。GCSLite 只提供当前本地 membership observers、RPC callback 和
  owner-wide fence-ready 的死亡记录，不再操作 adapter 的私有锁或 cleanup
  字典。回调每次调用传入，外部 RPC 不持该锁；Node-loss 进度异常向调用者
  返回，owner-death 则保留该 publication 义务并继续其它 publication。
  每项 publication 每轮最多一次 child release，最后一次 release 后允许
  同轮 graph retirement 和 Node Finalize；票据始终在 finally 释放。
- `output_publication_node.py`：把 journal 的 effect 接到 child/graph/local-store
  callbacks，没有第二份 phase bitmap。成功 Complete、local lease convergence
  和异步 terminal outbox 分开；即使回复 payload 已退休，supervisor 仍可仅凭
  metadata 重试尚未完成的本地资源清账。rollback-report 也参与收尾检查。
- `ownership.py` 中的统一 batch CAS：先验证全部 selected slots 再一次提交，
  entry 只持有自身 payload 和共享 metadata membership；历史 receipt 不保
  payload/TaskSpec。GC 按单 slot 释放子引用和图边，最后 sibling 才取回 task
  lineage。旧 publication membership 未经 retirement，不能被 reconstruction
  静默覆盖。

当前正常顺序为：发现全部 slots → intent ACK → child provisional ACK
→ whole graph PREPARE → 全部 materialization → 全部 promotion → ARM ACK
→ Worker source/import drain → Node local Complete/lease release → Core terminal ACK → owner graph COMMIT
→ selected CAS/READY → GCS adopted ACK → Node payload-retirement ACK。
Node terminal outbox 独立重试，不是本地 Complete/资源释放的同步前提；但 Core
adoption 同步报告 terminal 与 adopted，INTENT/ARM 也同步参与每个普通成功 execution，
包括无 contained refs 的普通值。因此不能把本后端描述为 GCS 完全不在成功路径上，
也不能把它等同于 production Ray 原有的普通任务发布路径。
Node/owner death 仲裁、selected membership retirement、supervisor/drain 与 owner
cleanup RPC 已接线；Worker exact owner-death cleanup 会退休 pending discovery/cached
reply 并阻止迟到 Push/drain 重新发布。已迁移阶段和后续 F1–F5 的明确故障边界
均有独立有界记录，见 §8.1.4；这些运行不自动验证之后每次 runtime 修改。完整
mixed-tier/targeted 故障矩阵与同版全部门禁仍未完成，不能因此勾选 K0/K1 出口。

publishing Node loss不意味着所有STORED副本都丢失。`surviving_output_locations`
只从exact committed receipt、slot membership、canonical result与same-attempt
location中选secondary，并排除publisher及Core已安装的dead Nodes；没有adopted
membership的descriptor不能进入KEEP。最终owner CAS只读当时的location map，
canonical保留原publisher身份，fetch route另选survivor。KEEP之后secondary
被删除/确认死亡可以得到LOST＋membership：KEEP是已锁定的publication存活选择，
不是永久READY承诺。后续显式重建先走现有逐槽retirement。
这条路径复用owner、Node store、GCS recovery，不增加第二发布后端、复制策略
或探活协议。新的真实adoption-tail回归用第二dispatch lane形成真实secondary，
验证producer不重跑、子引用可读以及逐槽物理GC。DROP锁定后迟到location report
现先经过owner历史manifest校验，进入`replica_cleanup.py`的exact物理队列；
foreign收到RETIRED，local在完成其余grant依赖交接后取消consumer。仅cancel
只能unpin；queue在原reference event线程驱动Drop，PINNED/ACK未知保留，
exact Node删除receipt或installed Node death才清账。它不接管逻辑GC或publication，
没有新增线程/另一套发布协议。active collection/retirement及GC后晚到同epoch
report也只能进入cleanup；精确元数据来自retained manifest，不来自新attempt的
descriptor。已知pending queue挡相关START/system retry/retirement/GC final CAS。
Node的pull admission、finalseal、grantpin均消费删除水位，旧数据不能重新拉回。
普通GC、publication rollback与owner-wide sealed sweep共享
`_drop_sealed_replica_locked`物理尾部；partial/never-created的rollback及
owner-death Finalize也以同一`_finish_replica_drop_locked`完成证明。各入口
先验证自己的journal/manifest、owner fence或精确Drop请求，再读取共用的
`_replica_drop_receipts`；receipt只绑定ObjectID/attempt/owner/Node/checksum，
不保存bytes、不增加新registry或wire状态。
对仍存在的sealed副本，共享物理尾部和owner-death观察都检查实际长度与
SHA-256；只有snapshot大小或metadata checksum相符不足以证明bytes完整。
读取失败/损坏返回INCONSISTENT或CONFLICT，保留原metadata，不提前写删除
watermark/receipt。完成过的旧receipt仍先返回、不读取同ObjectID的新副本；
已删除但forget未完成的absence分支仍能精确收尾。此为显式corruption
fault model下的防御性一致性，不声称正常fail-stop路径产生损坏或自动修复。

删除watermark只禁止旧写入，不证明清理完成。sealed路径先fence再delete，
metadata留到ObjectManager确认forget；partial或尚未create的journal intent
使用原typed write claim保留未知清理义务。任何失败都不提前写receipt，新seal/
pull不能覆盖pending metadata/claim，删除fence后的新source reader也被拒绝。
只有physical absence、manager与相关metadata/claim都退休后才record完成；
其它authority可凭同一receipt关闭旧义务，即使新attempt已seal或又被删除，也
不读写新副本。无旧receipt的STALE仍非ACK。owner Finalize只处理STORED且
不伪造rollback ACK；Worker ACK未知不重复已经完成的物理清理。

多foreign后续副本交接现由同一post-grant record继续驱动：
execution permission与physical custody分开，inactive TaskHold但current
canonical匹配的报告返回CUSTODY_ONLY并触发原GC；RETIRED则原queue负责删除。
首个拒绝/owner死亡锁定原错误并尽早取消lease解除pin，后续owner照常交接；
任意ACK未知不跳过其它owner，也不重新申请lease。typed receipts、cancelACK、
death delegation及terminal error同时保存在Core marker与queued state。
ticket防并发重复driver，旧队列只可使用最新marker；final quarantine与
death重查同锁，已消费死亡不会遗漏唤醒。没有custody证明的确定冲突进入
quarantine：不盲删共享bytes、不反复请求相同拒绝，保持不完整/不clean证据。
不能据此推断正常Task重建必然留下不可清理副本：受支持的output重建先退休旧
membership并保留manifest，迟到report先查这份owner历史再决定RETIRED清理；
put没有lineage，不推进其attempt。无历史输入被测试直接advance的reducer组合
只证明不信任冲突metadata，尚未证明是公共API可达缺陷。对于无法对应原owner
历史的损坏状态，不承诺自动修复，也不能允许来访descriptor自行授权删除。
真实双foreign owner切片已验证首ownerWorker死亡（Node仍活）后，consumer
不Push而healthy owner仍接收报告，死owner由GCS既有owner-wide sweep清理。
local pre-record现不再做owner副作用：纯`_build_location_reports`完整构造
foreign inventory，实际grant也进同一checkpoint后才开始本地事务。local
receipt与foreign receipt共用post-grant driver；owner canonical/location/route
的事务由一个helper实现，但permission分别检查实际SUBMITTED/RETAINED hold。
route/CAS抛异常是UNKNOWN而不是无副作用REJECTED；先取消并完成其它owner
交接，重放只修缺失receipt。CAS已生效的exactlocation/route不被异常补偿误撤回。

完整Grant已在Node提交、但所有Grant回复丢失时，取消不再只释放lease就丢弃
目标副本：`CancelWorkerLeaseReply.retired_grant`返回Node原record中深校验且
脱离别名的历史inventory。它不是重新授权执行；首次释放与released=False的
重放都携带相同证据。Core保留原Request、原错误及已验证Cancel回执，再进入
同一个local/foreign handoff。所有custody完成前不发布ERROR、不释放输入hold；
builder失败只重试保留的inventory，不重复用户函数或另建发布后端。
Worker已退出而Node仍存活时，Cancel可拒绝为WORKER_LOST；Core还须取得匹配
lease/task/attempt/executor/owner/output/PG/target的无payload、无cleanup_pending
GetWorkerLeaseOutcome，单独保留execution_outcome，不能伪造cancelled=True。

普通Grant和取消inventory复用同一深校验，完整有序依赖、唯一ObjectID、owner、
producer、checksum与目标Node都须匹配。缺字段、损坏nested ID、缺项或重复项
只触发exact lease replay，不能进入“无Grant所以无副作用”的终结路径。首次
新请求前检查foreign guard结构；这是credential校验，不是假定其之后永不退休。
取消记录与新Location进度的选择/写入在同一个Core锁内；旧queue不能覆盖已确认
receipt，active handoff也合并后来锁定的原错误。已选取消终态后的目标死亡不
改成新attempt；真实死亡证明与fixture中的typed reducer输入仍是不同证据。

授权前副本现在由同一交接路径接管：`LeaseDependencyInventory`仅包含完整原
Request、实际Node和已经确认的有序descriptor子集，没有Worker/allocation。
`lease_dependencies.py`只管理请求绑定、pending candidate、见证子集和ACK，
不运行网络或决定删除。Node在每次localization之前保留candidate；新Seal和
LOCAL_READY复用都在object/state锁内确认完整owner/epoch/bytes后登记，先于
source pin释放的finally。metadata写入或snapshot临时连续失败后，精确Cancel
会在相同锁序下重新检查实物与保留的seal见证，不靠metadata制造结果bytes。

terminal Reject和容量预算耗尽有store依赖时先Cancel冻结inventory；取消先到
也携原Request以给出可信EMPTY。Core把grant=None的inventory送入同一个
local/foreign driver，持有原Task holds直到交接完毕；不造Grant、不Push。
owner receipts齐全后，Core显式AckLeaseDependencyCustody，Node才释放该请求
的custody责任；普通Grant也复用这个ACK，没有store依赖的普通值不增加RPC。
ACK丢失只重放库存，不重复owner操作。Node有pending candidate或未ACK副本
就不能报告clean；零pin不是独占证明，多个lease复用副本时分别确认，只有
真正owner的GC/退休/死亡流程有删除权。

handoff到Push现在有最后一次同Core锁的准入检查：PENDING、完整identity及
canonical terminal error都匹配才mark push_send。已选取消不能被旧成功lane
覆盖；mark之后才来的取消由Node Start/Cancel仲裁，不承诺撤回已准入网络
字节。新增Node custody ACK期间也可能失去hold，返回后再次检查本地权限。

source-read pin与副本custody另有独立责任：`transfer_pins.py`在发送Pin之前
保留精确source地址/descriptor/transfer ID，active reader期间后台不能Release。
finally将reader关闭与取得首个close ticket放在同一个Node锁内；三次即时
短RPC共用ticket，不被supervisor抢先误判失败。失败保留outbox，现有监督/
drain继续重放，没有新增线程或发布后端。每次Release connect/request/绝对
deadline分别0.25/0.5/0.75秒；source-release未确认时Node仍不clean。

source在ObjectStore.pin之前记下唯一token；effect-then-error也可精确unpin。
Release先安装CLOSING fence再unpin，成功才CLOSED；Release比Pin先到时
直接建立关闭墓碑，accepted=True/released=False表示持久关闭，不是假造一次
物理unpin。任何迟到同transferID Pin都不复活，另session同ObjectID不受影响。
Pin/Release四种DTO保持原字段，通过深验证和pickle重构阻止nested身份篡改。

源Node死亡消灭物理pin，目标只可用完整GCS NodeDeath证明解除对应outbox；
目标Node死亡则由存活source监督者对该requester的session执行exactRelease。
Worker死亡、timeout、未找到Node或snapshot缺席均不替代Node死亡。proof必须
node/pid/registration/membership一致，active/inflight责任仍等read/settle边界。
GCS拒绝DEAD NodeID再次注册，故已确认死亡缓存可作永久fence。

提交者死亡的副本交接现在也走既有owner custody/cleanup：最初Request冻结
`DependencyOwnerRoute`（owner地址、ObjectID、真实SUBMITTED/RETAINED hold）。
这只是历史路由与提交身份，不授予新borrower或执行权。Node监督者验证GCS
WorkerDeath后先fence该submitter；request锁冻结正在localize的库存，GRANTED
可转ABANDONED并精确释资源，RUNNING不能因submitter死亡而直接释pin或终止
活executor。每轮最多一个lease/一个副本，cursor确保故障owner不饿死后项。

`ReportAbandonedDependencyReplica`绑定实际inventory/descriptor和完整死亡
proof；活owner用其有序GCS死亡消费者再次确认，再以active_hold=False调用
同一个helper。current副本由CUSTODY_ONLY接管，retired副本进入同一个
ReplicaCleanupQueue；Node保存真实owner回执后记自治完成，不冒充死Worker
发送AckLeaseDependencyCustody。若输入owner本身已死，只能以其独立完整
death走已有owner-wide fence，不能把submitter死亡泛化到全部inputs。

generic stored put先GC时，owner在删metadata之前保存六字段无负载历史
（object、producer attempt、owner、size、checksum、collection ID），不保
TaskSpec/位置/引用/bytes。COLLECTING查冻结plan与canonical，COLLECTED查
这段历史，授权迟到副本的exactDrop而不复活对象或修改原GC locations。
Task output继续用已有publication manifest历史。缺路由、错误epoch/checksum
或未确认死亡仍保留unclean库存；更宽malformed/death矩阵及跨删除authority
的absence证明仍需继续，局部切片不等于任意故障自动恢复。

cluster preserve-owner drain不提前停止reference consumer；直到fresh最终检查
与owner admission fence在同锁提交，才关闭timers/stop并join。join超时只重试
本地停机，不重开owner。所有Node已退出后的forced transport teardown单独保留
未清义务，不伪造clean/finalized；这是本地线程资源收尾，不是owner takeover。

Worker参数物化与结果发布现在共享一条引用交接规则：TaskArg显式nested manifest
使用TaskHoldSource；已解析的RefArg或ready-INLINE结果bytes里的reducer使用原
ContainedTransferSource。两类credential分别去重，加入同一attempt import session，
全部参数解码成功才commit。`importing_references`只包参数decode，不包user code
或输出serialization。后续decode失败反向释放；返还child时由Prepared custody
持有到promotion确认。opaque reducer实际出现时才lazy取得Core；普通值不增加
Core/server要求。session.close是本地handle释放/责任交接，不是远端Release ACK
全部成功的证明；既有Core obligation负责重试。与显式nested参数一致，用户把
同一参数handle存到global不会延长其attempt生命周期，不承诺任意global逃逸语义。

Node 的本地存储桥复用现有 `_sealed_metadata`、`_dropped_metadata` 和
ObjectManager。journal 的 MATERIALIZE intent 并不证明任意已存在 bytes
由它创建，因此先确认 ABSENT，再在 create 前记录一个未提交 write claim。
claim 只覆盖 write/seal 与 metadata 提交之间的缺口，成功后删除；rollback
可据它清理本次 partial write，但不能删除其他 attempt 或身份未知的残留。
正常删除与精确重放仍使用同一删除墓碑；本地锁顺序固定为 journal→object
localization→Node state→store，不在这些锁内执行 RPC。这些桥接已被运行后端的
supervisor/drain/owner-death cleanup 使用；接线的存在不替代完整故障验收。

#### 8.1.4 统一故障观察点与验收边界

`publication_gate.py` 只保留 `OutputPublicationGate`；私有 publication-gate 初始化参数只有
`_test_output_publication_gate`。旧 STORED/INLINE gate、旧帧和双参数 API 已删除。
gate 用统一 `OutputPublicationID`、manifest digest、Node incarnation 与 phase
绑定同一发布，full/targeted 身份及原始 return indices 均保留。四个观察点为：

- `AFTER_INTENT_ACK`：GCS 已确认完整 intent，child/store effects 尚未开始。
- `AFTER_PROMOTIONS_ACK`：全部 materialization/promotions 已 ACK，但尚未 ARM。
- `AFTER_ARM_ACK_BEFORE_COMPLETE`：ARM 已 ACK、本地 Complete 尚未发生；Node
  丢失后 GCS 只能判定 UNKNOWN，不能借测试已知的时序假定 Complete 不可能发生。
- `AFTER_COMPLETE_BEFORE_TASK_REPLY`：本地 Complete 与资源释放完成，但尚未
  交付结果；Complete reply 和 outcome 两个出口共享一个 gate。只有配置该 gate
  时才在放行前同步确认 GCS terminal ACK，不能把测试观察点当成正常 Complete
  路径的同步前提。

gate 不持有 journal/Node/owner 权威锁跨 callback、socket 或等待；terminal proof、
连接、收发及同发布 follower 共用有界 deadline。未配置时不创建 gate，也不增加
这些等待。放行后再次检查 incarnation/owner fence 和 payload retirement，不能
把已退休结果重新交付。

`test_stored_outer_node_loss_path.py` 的原有阶段切片已迁移到统一后端并逐项通过，
新增 ARM-UNKNOWN 用例只证明一个 executor-owned child 的 STORED 发布在该窗口
丢失后先精确清理、再预算内系统重试。`test_inline_node_loss_path.py` 的 KEEP/DROP
也已迁移并逐项通过。`test_borrowed_output_unknown_path.py` 另验收一个存活
Driver-owned child：旧final/provisional contained释放及实际resolution先于retry，
Task hold/lineage跨SYSTEM retry保留，dead executor borrower由独立死亡consumer
自然退休，新output使用新contained token。该单槽STORED场景不依赖child owner
死亡来代替Release ACK；也不把borrower退休与retry入口强加为一个全序。
这些早期切片没有覆盖本地 Complete 后 terminal 丢失或mixed/targeted shared
borrowed；后续证据如下。各段仅说明本段场景，不是对其它交叉故障的验收声明。

新增F1实际验收mixed INLINE/STORED两槽共享一个活child：未收到Complete时
两槽均DROP，四个旧pro/final holds精确退休后才SYSTEMretry，重试后各槽独立
GC。F3另以test-module wrapper隔离terminal发送及Complete/outcome交付，先
验证本地journal成功与CPU释放，再让publisher退出；它不使用会同步确认
terminal的配置gate，不把观察者witness送给Core。GCS实际冻结仍是UNKNOWN，
不是“确定从未Complete”。对应测试见test_mixed_borrowed_output_unknown_path
和test_unreported_complete_node_loss_path；其它未完成组合由验收矩阵逐项跟踪。
F2的targeted共享harness也已分别验收单槽和mixed selected两槽：公开drop建立
LOST，重建origin1的Task hold跨SYSTEMretry到attempt2不变；原indices1/2
对应selected ordinals0/1，健康INLINE slot0的原snapshot与contained hold不变。
测试函数有意在重建时改变slot1 payload大小，从STORED变为INLINE，说明storage
tier是每次输出发现的结果，不是假定确定性用户输出或另走一套发布生命周期。
两target独立GC、健康sibling最后释放共享lineage；对应test_targeted_borrowed_output_unknown_path。

F4/F5 的 [pre-Complete owner-death 用例](../tests/integration/test_precomplete_output_owner_death_path.py)
分别在 INTENT ACK 后和 promotions ACK 后精确终止 outer-owner Worker A。Node A
仍活并补充 Worker；publisher Node B、原 executor B 与 Driver-owned child 全部存活。
前者没有物化 bytes／child promotion，后者已有 sealed output 和 final child hold，
都必须由 GCS owner-death 路径收敛，不能由 Driver 接管 A 的输出。
测试先观察全部 Node fence 之后的真实 child release，才释放占有 adapter ticket 的
Prepare gate；若等待 owner_cleaned 后才放行，会形成测试自身的循环等待。放行后
原 Prepare 被 owner fence 拒绝；同一存活 executor 的真实 Finalize ACK 收齐后，
GCS 才记录 owner_cleaned。没有 ARM／成功 Complete，也不把 finalization 当 rollback ACK。
活 child 保持原值及 owner，本地临时引用、graph 与 replica 全部精确退休。

F7 的 raw graph-cycle 与公共任务 GC 分别见 §8.1.2。存活 Worker owner 的真实
Node-loss 自动 retry 见 §8.4。[F6 foreign-owner 迟到副本](../tests/integration/test_foreign_late_output_replica_cleanup_path.py)
以 A Worker 为原 owner、B 为 publisher、Driver 为 consumer submitter：真实
secondary grant 先 seal/pin 但未上报位置；B 死亡且 A 锁定 DROP 后才上报，
返回真实 RETIRED/custody 并取消 grant，owner mailbox 自主清理。测试在观测
物理不存在后才回放旧 Drop，再以 public get 显式 targeted reconstruction，
旧 report/Drop 必须保留新 epoch/bytes、健康 INLINE sibling 及已稳定的 child
引用快照。这没有额外生产协议或新 owner。上述新增
单场景及历史运行结果统一链接 [当前状态](current-status.md)／[验收矩阵](acceptance-matrix.md)，
不证明全部 UNKNOWN、mixed/targeted、Node/owner/GC 交错或同版本完整回归。

Core的UNKNOWN纯交错测试也区分真正envelope的到达时机：owner choice之前
到达可以提供Complete证据并KEEP已收到的INLINE；DROP锁定后到达不能重开
custody或覆盖下一attempt。这并不把GCS冻结历史从UNKNOWN篡改为已提前报告。

Worker 旧单返回 coordinator 源文件已删除，Core 旧发布分支与 Node 的旧
journal/handler/supervisor 组装也已移除。Node 不接受未 Prepare 的普通成功 Complete，
更不会由 generic descriptor-only outcome 构造成功。共享的 put/Actor/object-store
与 child-hold 原语保留；GCS旧registry/handlers、owner旧plan/receipt/retirement/
GC关联也已移除。独立旧模型已归档；旧OPEN/claim/recovery/finalize wire、
InlineInstall facade、Worker孤立encoder及StoredReferenceExportSession均已删除。
TaskReply不再带raw contained_edges、inline_publication或stored_publication；
nested引用只通过统一manifest表达，错误reply的metadata不再触发另一套orphan-edge清理。
通用ReferenceExportSession仅供独立序列化/生命周期教学，非当前Worker发布主链。旧测试的
保留合同、替代语义和缺口见 `history/retired-core-publication` 与
`history/retired-node-publication`、`history/retired-gcs-publication`及
`history/retired-owner-publication`和`history/retired-protocol-family`，不是通过归档宣称验收完成。

运行进程必须使用同一checkout的dev wire schema。pickle重建先要求完整字段数匹配，
再执行现有构造/深层校验；旧布局不会通过默认值或位置偏移变成新publication权威。
只保留已约定的共享source/Node incarnation历史pickle名称，指向同一中性类型，
不恢复旧TaskReply或publication状态机的线协议兼容。

owner现只有每槽output membership与统一retirement/collection身份。Node-loss
resolution先预检全部selected槽：未交付槽必须是pristine PENDING并保有一致
producer lineage；不能把partial payload/error/location/edge当作可擦除的空状态。
已退休epoch跨普通replica、task-output或新Lease的统一publication均受fence，
新epoch合法发布不受旧墓碑影响。只有精确退休的当前epoch才能metadata-only GC，
旧墓碑不能豁免下一epoch的canonical校验。collection digest保持原字节规则，
终态历史不持有TaskSpec或结果/函数/参数bytes，仍接受caller保留的精确plan重放。

`PublicationControlAdapter` 不再兼容旧tier-specific RPC，只拥有同一composition
lock下的contained graph与统一output recovery；GCS death、progress/drain和后台
owner cleanup只驱动这套状态。Node/Worker membership commit与相应workset冻结
保持同一锁顺序；普通Task放置、Actor、PG不因此变化。该重构没有消除普通成功
对GCS INTENT/ARM/terminal/adopted的同步依赖，还原性债务仍独立存在。

Node rollback释放child hold失败后可以询问GCS：必须取得同一WorkerID、注册的
Node/Worker incarnation、有效watermark和非EXPECTED死亡记录，才能以typed
`GetWorkerStateReply`作为这一个cleanup effect的终态依据。adapter在消费时再次
重建校验，而不是伪造成功Release ACK；活着的其它child仍需各自精确ACK。
路由不可达、ALIVE、缺记录、EXPECTED退出及畸形证明均保留原释放失败。

Node drain 区分“本轮推进完毕”和“可以关机”：`_drive_output_publications`
推进本地资源清账、terminal outbox 和 rollback，不证明 retained payload 已交付。
`_output_publications_clean_locked` 从同一 journal/adapter 检查已退休的 payload、
Complete/rollback ACK、owner-death 收尾及 in-flight ticket，不另设 phase bitmap。
因调用者已持 Node state，检查 publication locks 使用非阻塞获取；忙则报告
unclean，避免与 journal→Node state 的发布顺序死锁。

### 8.2 Lineage reconstruction

task 输出失去最后一个副本时，owner 根据保存的 producer `TaskSpec` 重建：

1. 以 producer TaskID 合并重复恢复请求；
2. 递归确保依赖可用；
3. 用新 AttemptID 重提原 TaskSpec；
4. 保持原 TaskID 和所有 ObjectID；
5. 只接受当前 attempt 结果，清理迟到旧 attempt；
6. 超出预算、owner 死亡或对象来自 `put()` 时返回明确不可恢复错误。

Actor 方法默认不做 lineage replay，因为 Actor 状态可能已经前进。

当前运行时纵切面支持 single-return、local-owner 顶层 `RefArg` DAG、local nested handle、
foreign-owner single-return，以及 multi-return 全 manifest 同时 LOST 的重建。DFS 只从 canonical
producer `TaskSpec` 派生边，以三色标记拒绝 cycle、合并 shared child，并生成 dependency-first
后序；READY dependency（包括 `put`）直接跳过，LOST producer 启动新 attempt，已有 active
reconstruction 则 JOIN。长期 lineage hold 使 execution hold 释放后依赖仍能存活，直到
producer output/lineage 被最终收集。新 attempt 保持 TaskID/ObjectID，旧 result/location 继续
由 attempt fencing 拒绝。单 producer stored 与三层 `leaf → middle → root` recursive
smoke 的当前结果统一记录在 `current-status.md`。local nested、foreign-owner/
foreign-input 与 multi-return all-lost/partial-loss 另有独立 smoke；owner failure 的完整
真实进程矩阵仍未完整验收；已有 pre-Complete owner-death 与 Worker-owner Node-loss
切片不覆盖全部 reconstruction 组合。重建图在任何状态突变前预校验 output manifest、
owner、cycle、stored lineage 和 retry
预算等约束；前置检查等待/失败本身不会推进 attempt、消费预算或清理 descriptor。
确认为终态的 targeted OPEN failure 由下面的独立清理步骤处理。Core shutdown fence
同样位于 coordinator 状态修改之前，shutdown 后的 reconstruction 请求保持 owner LOST、
recovery inactive 和 accepted count 不变。

旧 selected-output membership 的退休与新 attempt 准入分开：

- whole DAG 先检查所有待重建 producer 的 finish barrier、lineage/budget，再在锁外
  renew foreign inputs；锁内重验通过后按槽退休。退休 RPC 返回后重新验证全部
  producer 的本地计划和 foreign renewal，随后才消费任何预算。
- targeted 使用不可执行的 `preview_start`，不会用一个未退休 owner CAS 绕过
  验证。它按同样的 prerequisite→renew→retire→revalidate 顺序，只清理选定槽，
  不扫其它 LOST siblings。外部调用期间合并的新 target 必须重新进入预检。
- 未知清理 ACK、renewal WAITING 或 finish barrier 是 deferred；owner-routed API
  复用可重试的 `NOT_LOST` 投影，不把它当成永久 `AUTHORITY_REJECTED`。
- OPEN 的确定性失败先锁定错误选择，重放原 cleanup；旧 child/graph/replica 精确
  退休后才发布 ERROR。否则清掉 canonical descriptor 却留 membership 会破坏 GC。
  清理期间其它 sibling 变 LOST，也不能转入 whole START 绕过已锁定的失败。
- 已有 targeted session 优先于 all-LOST 路由；whole DAG 对非 READY lineage vertex
  上的 active targeted producer 延后准入。READY 数据和 nested handles 不被扩展
  为额外 readiness 依赖。

这些局部准入/清理不变量已有纯契约与代表性有界 smoke；不证明完整并发故障矩阵，
也不改变“targeted 仍执行完整函数，仅选择发布返回槽”的语义。

### 8.3 Blocking get 的 CPU yield

Worker 内的 Task 若提交子任务后阻塞 `get`，单 CPU 节点会死锁。K1 中它必须暂时
归还 CPU allocation，让子任务取得 lease；unblock 立即恢复逻辑 CPU allocation，
不等待空闲容量再继续。GPU、
自定义资源和 Actor lifetime resource 不随之释放；成功和异常路径都只能 yield/
reacquire 一次。

Phase 2 已把 `NotifyWorkerBlocked/Unblocked` handlers 接入 Node 权威 lease state。消息携带
lease/task/attempt/worker identity 和单调 episode sequence；重复通知幂等、跳号/旧 identity
被拒绝，unblock tombstone 阻止同 sequence 的迟到 block。每次转换和账本变更均为 $O(1)$。
账本内部 CPU availability 可以为负：yield 后
空出的 CPU 若被 child 占用，parent unblock 仍立即恢复其逻辑 allocation，并形成 signed
CPU debt；公开给调度器的 `available` 始终 clamp 到 $0$ 以上。任一运行任务释放 CPU 时先
偿还 debt，因此调度器不会把负容量解释为新的可分配资源。completion、abandon 和 worker
loss 使用同一 terminal finalizer，依据 ACTIVE/CPU_YIELDED 实际 held resources 一次性清理。
当前每个 Node 可配置 1–2 个固定普通 Worker。Worker `ray.get()` 只在真实等待前发送
blocked，并在恢复用户代码前以相同 identity/sequence 精确发送 unblocked；ready fast path
不通知，`get_many()` 的多个内部等待合并为一个重入 episode。单 CPU、双 Worker 的 bounded
multiprocess smoke 已证明 parent yield 后 child 获得 CPU、parent 随后 reacquire，最终无 debt。

LOST 对象有两个额外等待点：自己的 publication/finish 尚未结束，以及递归
lineage 准入因某个依赖的 finish 暂缓。它们先在 Core condition 锁内检查谓词，
再离锁进入 blocking group/episode，随后重新持锁复查并按原 deadline 计算剩余
时间，最后才调用 Condition.wait。退出 condition 后才 Unblock。这样既不持有
publication/finalizer 需要的锁去等 notifier/RPC，也不会丢掉通知期间发生的唤醒。
[锁序回归](../tests/unit/test_core_lost_blocking_lock_order.py)使用真实发布、物理
Drop、owner/recovery/barrier 和模拟等待验证这两处边界；并不执行真实 OS 死锁。
若用户线程显式绑定同一个 ExecutionContext/notifier，episode 锁才会跨线程共享；
新线程不会自动继承绑定。这项锁序修复本身只覆盖两条LOST分支；notifier
既有控制RPC与Unblock收敛也不因get超时而取消。

普通PENDING的本地Event等待、foreign PENDING及可重试LOST的poll也在
blocking group/episode进入后按原deadline重算余量，避免把通知耗时之外
再授予完整等待预算。若已过期，本地仅检查Event是否已通知并重新读取owner
状态，保留通知期间到达的READY/ERROR优先级；否则超时而不再wait。foreign
没有本地ready事实，预算耗尽时不再poll或增加owner查询。
[七项pure deadline合同](../tests/unit/test_get_notification_deadline.py)用受限
时钟/等待和真实owner credential/defer验证下一次等待的预算，不证明
get的总墙钟硬上限，也不取消已经开始的notifier或Unblock控制RPC。

通知入口必须具备失败原子性：episode锁进入中断或Block消息构造失败时，
thread-local depth恢复为0，这两类入口失败不消耗序号；否则下一次get会
误当重入而跳过Block，或向Node发送跳号episode。正常取得锁并构造Block后
才提交sequence；一旦可能发送，原exact Unblock补偿仍必须执行，depth在
Unblock及episode锁退出后恢复。native group、Core fallback与foreign手工
scope都只在`__enter__`成功后保存待退出对象，失败不等于已进入scope。
[入口异常纯合同](../tests/unit/test_blocking_notifier_entry_failure.py)以一次
锁入口异常和一次合法身份下的本地构造异常验证重用，不证明真实信号或
OOM恢复；同group内部捕获再试是helper合同，普通get_many会传播入口错误。

### 8.4 节点与 Actor 故障

远端与 Driver-local ordinary-Task 的代表路径已经接通：Driver 只以受管
`multiprocessing.Process.sentinel` 证明物理退出；GCS 以 NodeID、PID、registration epoch
和稳定 detection ID 提交不可变 DEAD tombstone，并返回同一 membership epoch 的
live-only 快照。每个 survivor ACK 安装该快照后，Driver 将完整 ACK 向量、快照及
累计 GCS death facts 组合为 `InstalledNodeDeathView`，向所有存活 Node 保留成功后
再通知 Driver Core。证书不携带结果 bytes，不是重新选出的成员权威。
RPC timeout 从不等价于死亡证明。Core finalization在死亡事务锁内复查成员集与
尚未通知的已提交死亡，并在两轮clean后提交。monitor继续贯穿Node Finalize与
graceful退出观察；force/关闭Process handle前才stop、补收sentinel并join死亡事务。真实
multiprocess smoke 分别覆盖远端与初始 home Node。home Node 死亡时，Core 在 survivor
snapshot ACK 后原子更新带 epoch 的 route cell；已有物理 lease 保持冻结身份，新 Task 和
large put/get 使用 survivor。Actor migration 与 PG-keyed Task 的 LOST 终止另有独立路径；
foreign-owner object reconstruction 仍由其 owner 而非本地 fallback 执行。

Worker 内的 owner 通过同一死亡事实收敛。Node 的 publish handler 验证证书对应
本地 installed snapshot、survivor ACK 完整且 epoch／旧死亡事实不回退，再保留一个
累计 view。`get_installed_node_deaths` 在服务存活和 drain 期间可读；尚无证书、旧
证书或 RPC 失败都不是新增死亡证据。DTO 在构造／pickle 重建时深校验、拷贝嵌套身份，
既有死亡不能被丢弃或变成存活 Node，同一 epoch 仅允许补充未到达的精确死亡事实。

`WorkerServer._embedded_core_for` 用本地 Node 读取结果完成懒创建 bootstrap，随后
由 Core 既有 coordinator 周期读取；不是每个 Task 查询 GCS，也不新建 failure detector
或 owner 监听服务。Core 逐条调用 `handle_node_death`，先保留 location 清理工作，
再安装 dead fence、修正 owner locations／route／waiter 并排入任务分类。局部步骤失败
时重放同一 view，全部应用后才推进 applied-view marker。快照安装／证书发布失败
仍由原 observer 在有界 RPC 轮次间重试，不因 sentinel 只触发一次而遗失已提交事实。
shutdown cutover 保留未完成义务而不假报 clean；EXPECTED 退出不被冒充 crash。

[Worker-owned Node-loss 用例](../tests/integration/test_worker_owner_node_loss_path.py) 中，
A 是 child Task 的原 owner，B 在 ARM 后丢失；A 从本地 Node 消费 Driver-certified
view，执行 UNKNOWN/DROP 清理后自主在 A 重试。Driver 只查询 owner readiness，
待 READY 后才 get，不发起 reconstruction 或注入 owner 死亡状态。TaskID/ObjectID
与 owner A 保持，attempt／lease 推进；同 owner/executor 的 hold 分离规则见 §8.1.3。
该通过记录不证明全部 Worker-owner/node-loss/GC 组合，也不改变同步 GCS publication、
global fail-fast DAG 或 phase-specific 恢复合同。

Actor restart/migration 保持 ActorID、递增 generation/route epoch 并以 fresh Worker incarnation
重建构造器状态。旧 generation 的 endpoint、call、reply 和 mailbox ACK 被 fencing；旧
in-flight 方法失败且不透明重放；既覆盖 same-Node Worker replacement，也覆盖 Node-loss
后在 survivor 上迁移。

## 9. K1 Placement Group

PG 是 bundle 的 gang scheduling，而不只是 PACK/SPREAD 四个字符串。Planner 在
shadow resource view 上做确定性 bounded backtracking；`STRICT_SPREAD` 使用增广路匹配。
这些算法表达四种策略语义，但不逐行复刻 production Ray 的稀缺资源排序贪心策略，且
不得在 plan 阶段修改真实账本。

[例7](../examples/07_placement_group.py)在Task提交前展示已提交public handle
中的PGID/attempt/bundle→Node映射，再把它与实际执行PID对照。
STRICT_SPREAD的“必须不同Node”是硬约束；该例两节点各一CPU、两个
bundle各一CPU，所以PACK同样必须跨节点，不能用结果区分两种策略。
它不声称采集了plan/PREPARE/COMMIT日志或观察到PREPARE期间不可见；
中间可见性由下面协议及纯状态测试解释，成功示例不增加故障/轮询。

```text
ABSENT → PREPARED → COMMITTED → ACTIVE
       ↘ ABORTED
```

1. GCS coordinator 为全部 bundle 规划节点；
2. 每个 NodeManager 原子 prepare 本节点的全部 bundle；
3. 任一 prepare 失败，所有已 prepare 节点都必须 abort 并逐项恢复资源；
4. 全部 prepare 成功后幂等 commit；
5. 全部 commit ACK 后才发布 PG 为 `CREATED/ACTIVE`，此前任何 PG Task 都不能
   启动；
6. PG Task 只能使用指定 bundle pool，不得回退到普通 root pool。

这是单 GCS、fail-stop 假设下的近似两阶段预留，不宣称有持久日志或 coordinator
crash recovery。任一成员节点死亡时，v0.1 可以把整个 PG 标记为 `LOST`。
若Core因已知PG LOST而选择取消一个结果未知的lease，用户可见错误是原
`PlacementGroupLostError`；`_LeaseRequestAmbiguous`只描述尚需取消的传输状态，
不能在inventory交接后取代已选错误。该typed cause随同一取消/副本交接记录
重放，必须等精确Cancel与custody ACK收齐后才能发布，不消费新attempt预算。
这不把任意后来PG loss提升为先前owner/local错误的覆盖权：已有sticky错误
保持不变，普通非PG任务也保留其原ambiguity wrapper。

PG终态检查和SYSTEM retry的owner/recovery提交共享同一Core锁，不能
先取CREATED快照、解锁后再推进attempt。死亡先提交时保留原attempt与
预算；retry先提交时可以推进一次，但后续fresh-PG准入必须拒绝已LOST
capability，不再申请Lease或Push。这个局部原子边界不修改late-replica
清理、已知Complete的发布收尾或其它PG的普通任务重试规则。

dispatcher的PG初始准入检查只属于尚无远端义务的新任务。已有lease/
cancel/Push/custody、output adoption、output Node-loss与deferred system
failure必须进入各自原收尾authority。另一bundle死亡只撤销后续调度
capability，不能把已知Complete改成ERROR，也不能把READY误当Node payload
已经退休；owner CAS前后均须保留同一publication与finish barrier直到
原ACK收齐。该规则不保证任意PG故障后Task成功：Node-loss UNKNOWN仍在
原清理收敛后按PG终态规则决定，不通过dispatcher快捷清除义务。

## 10. Trace 是一等接口

每次消息收发和状态转换都应输出结构化事件，至少包含：

```text
event_id, cause_event_id, process_id, process_sequence, component,
entity_kind, entity_id, task_id, attempt_id, object_id,
message, src, dst, old_state, new_state, reason
```

跨进程不存在可靠的全局总时钟；`cause_event_id` 表达因果偏序，`process_sequence` 表达
单进程内顺序。测试应断言因果链和状态合法性，而不是依赖 `sleep()` 猜测时序。
当前已有 trace schema、内存/JSONL sink、异步远端 sink、Driver collector 和跨进程
事件汇聚 smoke；transport sidecar 使用独立物理 `rpc_id`，已验证普通 Task 的 lease、
PushTask、StartLease 和 CompleteLease 四条跨 PID `cause_event_id` 边。
公开 `trace()` 返回 Driver collector 的到达序快照；`export_trace(path)` 将当前快照
按确定性顺序原子写为 JSONL，保留真实 event/process/causal/entity 字段并替换原文件。
同一快照的导出顺序可复现，不代表不同运行的 ID 相同，也不是远端 sink 的 drain
barrier；导出接口已有八项 focused pure tests。

原始 `TraceRecord` 与黄金 trace 刻意分层：前者保留一次运行的 PID、event/RPC ID、
timestamp，服务精确诊断；后者是 `src/miniray/golden_traces/` 中可审阅、可执行的
语义合约。合约用 `$task_id`、`$attempt_id` 等符号绑定动态身份，只检查事件角色、
稳定字段、进程内 sequence 和真实 `cause_event_id` 边，不保存某次运行的随机值。
成功与应用异常各有一份合约；`trace_contract.render_trace_sequence()` 将匹配结果渲染为
适合示例阅读的参与者序列，而不是伪造跨进程全局时间线。
当前普通单Task INLINE成功合约还展开Prepare、INTENT、ARM、owner terminal、
adopted和Node回复custody退休，共十个不同RPC往返。四个同handler发布阶段
依靠真实typed ACK后的`output_publication_ack`及Task/Attempt/Lease/manifest
身份绑定，不依靠第几次调用或transport `ok=true`猜阶段。Node ACK观察只
表示收到回复，adapter仍继续原stage/forward/journal验证，不能拿此事件
当作本地journal已经提交的权威。新的`reply_event`
锚定实际`rpc_reply_received`直接cause，`server_event`沿显式cause链验证
Prepare/Push/Complete内的业务事实；嵌套与外层规则共用事实时固定同一event。
请求和回复分别展示，Node后台terminal重报不被强加为Complete的前提。
`output_owner_ready`在owner CAS/wake之后、adopted之前且authority锁外记录；
`output_payload_retired`仅是Node完成回复托管退休，不是ObjectStore物理删除。
旧`task_finished/object_ready`仍是收尾通知，不能冒称首次READY或最终GC。
观测丢失/投影异常不改变业务协议；合约只验收隔离的无重试成功切片，不是任意
并发trace的完整模型检查，也不完成同步GCS发布的Ray还原度取舍。

## 11. 与 production Ray 的边界

### Same

- Task/Actor/ObjectRef 的关键语义与普通任务 direct submission；
- 普通 Task 的 lease placement/direct `PushTask` 不由 GCS 逐调用调度或转发 bytes；
- NodeManager 的本地资源账本与 Worker lease；
- owner-based metadata 与本地不可变对象副本分离；
- Task/Attempt、Object/Replica、Actor/generation 分离；
- Actor 创建走 GCS、稳定后方法直达 Worker；
- Placement Group 全组成功后才可见。

### Simplified

- Python-only，用户函数由 `cloudpickle` 序列化；
- 单机 loopback 两逻辑节点、单 GCS、每 Node 1–2 个预启动 ordinary Worker slot；每个
  Task 使用一个 LeaseID，spillback 两跳保持同一 LeaseID。production Ray 的 raylet 则按
  job/language/runtime env 动态启动和复用 Worker，并可在 leased Worker 上流水发送多项工作；
- CPU/GPU/memory/自定义标量资源与确定性简化 Hybrid policy；
- ObjectStore 使用教学友好的进程间传输，不复刻 Plasma 零拷贝实现；
- borrower 直接向 owner 注册，不实现 borrower tree；
- 当前普通成功 publication 同步向 GCS 取得 INTENT/ARM，并在 Core adoption 中
  报告 terminal/adopted；contained graph 也由 GCS 预留/提交。这是额外的教学
  控制面依赖，不是 production Ray 的普通成功原路径；Node local Complete/lease
  release 本身仍不等待 GCS terminal；
- Actor 默认串行，支持同 Node Worker restart 与 Node-loss 后 survivor migration，旧调用
  不透明重放；PG 使用 bounded backtracking、匹配和
  近似两阶段预留；
- 故障模型是 crash-stop，恢复预算小而明确。

### Omitted

- GCS HA、共识、外部持久存储、coordinator crash recovery；
- 云/多机部署、Autoscaler、Dashboard、Jobs、runtime env 与多语言；
- Plasma 性能工程、对象 spilling、RDMA、多级缓存；
- 抢占、公平份额、deadline、复杂标签与 production scheduler 优化；
- Actor method retry、async/concurrency groups、named/detached actor
  和 PG Actor 的完整语义；
- PG bundle rescheduling（participant Node-loss 仅实现终态 LOST 与 survivor cleanup）；
- 安全多租户、认证、TLS、资源隔离；
- Data、Train、Tune、Serve、RLlib 的完整产品层。

这些省略项必须公开，不能把“同样的核心语义”描述为“生产 Ray 的完整缩小版”。
