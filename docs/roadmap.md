# K0＋K1 路线图

> **历史路线图，非新的自动任务队列。** 原 K0/K1 能力继续保留；下面后来增加的
> 协议增强、开放 checkbox 和滚动白名单门槛不自动成为用户新增要求。
> 当前归档边界见 [handoff](handoff.md)，后续按
> [纠偏方案及冻结交付表](correction-plan.md)等待一次性语义确认。
> 方案尚未实施；当前代码中的 GCS 发布／global DAG 等保证未被删除。

## 当前状态

当前 release train 是 **v0.1，实现中**。首批 API、运行时骨架和纯算法代码已经
落盘；最新 Gate A、compileall 与有界 smoke 结果统一记录在 `current-status.md` 和
`testing.md`，本页不复制易漂移的计数。K0 和 K1 都没有被
宣称完成；未勾选项保留历史来源，须按纠偏后的需求映射区分实现缺口、验证缺口与
自选增强，不能直接恢复逐项追加循环。文档、接口或
纯状态机存在
不等于端到端能力完成，单元测试通过也不能替代真实进程协议的验收。

状态只使用以下含义：

- **设计中**：规格可能调整，没有实现声明；
- **实现中**：已有代码，但还缺少某些验收证据；
- **已验收**：对应 gate 的代码、确定性测试和 trace 都已检查；
- **延期/省略**：明确不属于 K0＋K1，而不是悄悄缺失。

逐项实现证据、代表性 exact selectors 和下一批七个语义故障场景集中在
[验收矩阵](acceptance-matrix.md)。它用于消除模糊待办，不替代本页完整出口。

在 v0.1 发布前，应在本页为每个勾选项链接测试或 trace 证据。Gate A、loopback 与
multiprocess smoke 的 exact node ID、耗时和命令集中保存在 `current-status.md` 与
`testing.md`；有界测试必须逐项执行，历史局部记录不表示当前 allowlist 在同一轮重跑。
现有证据仍未覆盖完整
K0/K1，出口项保持未勾选。

此前已完成 `StoredArg` 大型 nested 参数接线及独立 bounded 验收；历史 INLINE recovery
已改为 GCS metadata-only、Node 本地 Complete 与异步 terminal outbox、owner
KEEP/DROP、结果丢失与重建/系统重试分离。普通任务的 finish barrier 将 reconstruction
与最后引用 GC 放到旧 execution 的输入 holds/计数收尾之后。已报告成功 Complete 的
KEEP/DROP 各有专用 bounded 验收，同时覆盖单个 TaskHoldSource INLINE borrowed source；
这些 gate/测试现已迁移到统一后端并分别复验。新增的一项 OwnedChild STORED
ARM-UNKNOWN 也已逐项通过，但不能外推其余 UNKNOWN、shared-borrowed 与
owner-preComplete 的完整故障验收。

本轮普通 Task 成功路径已接入统一 selected-output 后端：Worker discovery、Node
batch journal、GCS metadata recovery 与 Core batch adoption/per-slot GC 不再走旧
单返回投影。真实 Start ACK 提供 Node incarnation，每槽只序列化一次，一张 custody
记录保留 bytes、source handles 与 argument import transaction；未知 ACK 精确重放，
失败 Complete 先确认补偿，已知成功不回退为失败重试。ref-free、contained、mixed-tier
multi-return 和 targeted outputs 都已接线，完整组合验收仍未完成。
GCS 的 INTENT/ARM 与 Core 的 terminal/adopted 报告同步参与当前普通成功路径；
Node local Complete/lease release 本身不等待 GCS terminal，Node outbox 可独立重试。
这一区别属于教学协议，不应描述为 production Ray 的原始普通任务成功路径。
local custody cleanup、owner-death fencing 和已知 Complete 的资源清账独立保留
重试义务；当前 revision 的选择集与逐项 smoke 证据见 `current-status.md`。
本轮已取得 mixed-contained/targeted reconstruction、mixed publishing-Node loss
和已 adopted owner-death 的独立有界进程证据；后者保持 executor 存活，验证
GCS→Node→Worker 清理与 source holds/物理副本收敛。这些较早切片本身不覆盖 UNKNOWN
或 pre-Complete owner death；本轮新增的 STORED ARM-UNKNOWN 也只是单一
OwnedChild 场景，完整出口条件仍未满足。

Core 的 pre-Push lease、target hop、grant/location、cancel 与 Push 恢复现统一
携带 OutputPublicationID candidate；这只是身份，既不提前登记 GCS intent，也不
证明已有执行/副作用。Node 本地 ObjectStoreError 现在明确拒绝并进入失败补偿；
outcome 的 cleanup_pending 保留真实执行终态，但 Core 必须等全部 rollback 与
GCS ACK 后才能 retry，不能把 CPU 已释放误当成清理完成。

新增必须收敛的验收工作：

- [x] owner首次重建ACK接入Core同锁pairedsnapshot，真实START/JOIN后
  同attempt明确完成不再因active清除误拒绝；[新pure合同](../tests/unit/test_owner_reconstruction_completion_race.py)
  覆盖success/app/system末态、错epoch/未commit/不兼容pair/失败callback，
  并区分unknown与metadata读取错误。原foreign4heavy迁pure保实际
  发布/drop/GC，原并发reducerL1与foreignMP各自有界复验；只完成此切片。
- [ ] 完成owner首次ACK的其余生命周期窗口：targeted终态ERROR但task
  SUCCEEDED需来自真实联合commit的目标收据，不能凭状态拼接；queued
  late-loss sibling没参加START，不得借失败union授权。另查targeted
  在Core构造Outcome前session消失、再次LOST/系统retry推进后的历史
  START证据。当前same-attempt修复及normalMP均不替代这些合同；
  最小每Task当前targeted receipt方案仍待实现/GC与独立验证。
- [x] notifier入口失败恢复depth，Block构造成功才提交episode sequence，
  native/fallback/foreign只记录成功enter的scope；
  [入口pure回归](../tests/unit/test_blocking_notifier_entry_failure.py)保可能发送
  Block后的exactUnblock，不宣称OS信号或OOM完整恢复。WorkerSide最后
  3原heavy用真实Condition与原two-handler层次转L1逐项验收，CPUyield
  原MP正常回归通过；完整K0K1/更宽故障矩阵未据此完成。
- [x] localPENDING等待、foreignPENDING和可重试LOST poll在notification
  进入后扣除已耗原deadline预算；[七项pure合同](../tests/unit/test_get_notification_deadline.py)
  保留localREADY/ERROR优先级并限制下次wait，不宣称notifierRPC取消。
  Worker原cache/retain/existing-ownerpin三合同迁pure，Node原Block/
  Complete竞争L1及原Worker-ownedref/CPUyield MP各有独立有界证据，
  不因此完成其余runtime合同或全部K0/K1出口。
- [x] Core.get两LOST等待不再持condition锁进入blocking group/episode，
  通知后重查谓词及原deadline，Unblock在锁外；
  [四项pure先红后绿](../tests/unit/test_core_lost_blocking_lock_order.py)与原
  finishbarrier合同共同通过，非真实OS死锁执行证据。原Coreblocking
  6fn7case、Workerbinding4pure保原语义完成纯迁移，Node drain两原
  L1和binding L1逐exact验证，原CPUyield MP回归另有独立证据。
  不由此勾选全部原runtime合同、宽故障矩阵或K0/K1出口。
- [x] 例1原Task路径的黄金trace展开真实INTENT/ARM/terminal/adopted，
  以十条独立RPC/显式handler链区分资源释放、ownerREADY、回复custody退休。
  [pure匹配/负例](../tests/unit/test_trace_contract.py)和
  [观察不扰动协议](../tests/unit/test_publication_trace_observation.py)已验证；
  原success trace、example01和application-error三个exact各自有界通过。
  此项仅补教学可观察性，不改变同步GCS/globalDAG/phase保证，亦不宣称
  GCS fidelity取舍、其余示例同版复验或K0/K1全部出口完成。
- [x] 在未完成adoption收尾的publishing-Node loss中保留已adopted、
  grant-backed的健康STORED secondary，不因publisher死亡误丢副本。
  [真实mixed-contained双KEEP切片](../tests/integration/test_output_surviving_replica_path.py)
  已有逐项bounded证据；owner/Core pure覆盖KEEP后secondary丢失与ACK/CAS重放。
  同时补上Worker物化RefArg/readyINLINE结果中contained ObjectRef的scoped
  importer，复用同一attempt import生命周期，不伪造TaskHold。
- [x] DROP锁定后迟到local grant/foreign location report现用exact owner历史
  metadata进入独立物理cleanup queue，先保留义务再拒绝执行；consumer真实cancel
  只unpin，queue继续等待exact Drop ACK或installed Node death。Node旧epoch
  pull/finalseal/grant由同一删除水位fence，真实旧drop receipt可跨newepoch重放。
  [local-owner真实late-DROP切片](../tests/integration/test_late_output_replica_cleanup_path.py)
  和pure local/foreign＋ACKloss/PINNED/death/START/retry/GC/shutdown契约已验收。
- [x] 多owner grant的首个拒绝/死亡现保留sticky terminal并尽早exactcancel，
  后续sealed依赖继续交接；CUSTODY_ONLY区分无执行权与owner仍负责副本。
  完整state同时在marker/queued work，ticket与same-lock final停车防进度倒退
  及death失唤醒。[真实双foreign owner死亡切片](../tests/integration/test_multi_owner_handoff_failure_path.py)
  验证Node保持live、no consumerPush/retry、healthy owner报告与normal lineageGC；
  pure覆盖ACKloss、旧队列、whole_execute重入和quarantine唤醒。
- [x] local pre-record副作用已移入同一个post-grant driver，纯builder先保留
  完整grant/foreign inventory；shared custody helper分别验证实际SUBMITTED/
  RETAINED，local异常UNKNOWN保留receipt缺口并继续foreign交接。
  [真实one-shot route故障](../tests/integration/test_local_replica_handoff_failure_path.py)
  验证先checkpoint/取消，再foreign report和local exact repair；pure覆盖
  route/CAS生效前后、旧hold、新epoch与合并ACKloss，非全failure matrix。
- [x] Node仍存活而已GRANTED executor退出时，以真实WORKER_LOST outcome
  收敛执行fence，取消拒绝不伪造成ACK；双输入owner存活、原错误/noPush/noRetry
  与最终物理GC由local-handoff文件的独立executor-exit exact进程用例验证。
- [x] 验收Grant回复全部丢失后的历史inventory交接。已接线Cancel回传原record
  retired_grant、Core复用local/foreign driver、原错误与完整hold收尾；深校验、
  Cancel ACKloss、builder异常、旧queued/unlock重入与WorkerLost已有纯合同。
  [真实12次Grant丢ACK切片](../tests/integration/test_ambiguous_grant_custody_path.py)
  已有独立bounded证据，当前revision结果及范围见current-status；不能外推
  无committed Grant的partial-localization副本清理或任意冲突自动修复。
- [x] pre-grant partial-localization使用独立请求inventory而非伪造Grant，
  terminal Reject/容量预算取消后复用同owner交接和exact ACK。
  [第二源真实丢失切片](../tests/integration/test_pregrant_custody_path.py)证明无Grant
  也不忘首个已Seal副本；busy/双ACKloss/metadata与snapshot临时失败恢复、
  shared replica分别确认和Node drain fence有纯证据。
- [x] handoff到Push最后同锁检查已选取消/PENDING；ACK返回后重查本地hold。
  三个纯准入边界测试区分“取消先赢禁止发送”和“Push先准入由Node Start仲裁”。
- [x] source Pin ACK未知与source-release后台重驱已有独立outbox、active-reader
  fence及原子close ticket；Release-before-Pin永久关闭该session，effect-then-error
  只操作保存token。[真实Pin/Release ACKloss](../tests/integration/test_transfer_pin_ack_loss_path.py)
  两exactcase分别有界通过；短RPCdeadline与首次解锁后台抢占有纯回归。
- [x] [请求者Node死亡pin清理](../tests/integration/test_transfer_pin_requester_death_path.py)
  独立进程协议测试证明存活source自动关闭dead peer session、同对象其他reader
  不受影响；sourceNode死亡/active/inflight/错死亡proof另有纯证据，不冒称
  任意失效组合已验收。
- [x] 提交者Worker死亡后的Node自主owner交接已冻结原route/hold并使用
  custody-only报告；每轮一个副本，保存真实owner回执不伪造死Worker ACK。
  [真实GRANTED提交者死亡切片](../tests/integration/test_abandoned_dependency_custody_path.py)
  证明活Driver owner接管及最终GC；pregrant/RUNNING、GC-first put、
  多owner公平推进和正常ACK并发另有纯证据，不当作全部E2E。
- [x] generic stored collection在删除metadata前保六字段无负载身份；
  COLLECTING/已COLLECTED put的迟到副本由现ReplicaCleanupQueue删除，
  不复活对象或改原GCplan。完整owner/Node/GC交错仍是下项未完成矩阵。
- [ ] 完成上述late-replica的更宽交错验收；F6 的 foreign 真实进程切片已另行
  验收，但不勾选完整矩阵。明确冲突metadata的quarantine仅保证
  fail-closed且不反复RPC，不等于自动修复。当前无历史staleProducer样例直接
  变更reducer状态，尚未证明公共API可达；正常output先退休并保留manifest，
  put不推进attempt。不得以外来descriptor自行授权删除或用人为冲突替代真实故障。还需补其它
  真实多owner交错。Node各删除authority与newer epoch的typed absence/完成证明
  已复用同一receipt/物理尾部，并有
  [跨authority纯合同](../tests/unit/test_cross_cleanup_receipts.py)和
  [owner Finalize纯合同](../tests/unit/test_owner_finalize_replica_receipts.py)；
  [真实publication rollback→retry→旧generic Drop](../tests/integration/test_cross_cleanup_receipt_path.py)
  已逐项有界通过，且owner-death/pregrant两项回归均复验。
  STALE_EPOCH本身仍不是ACK。局部验收不勾选完整ownership failure矩阵。
- [x] 已报告成功 Complete 的 INLINE publishing-Node-loss KEEP/DROP 各有专用
  [有界真实进程证据](../tests/integration/test_inline_node_loss_path.py)：真实收到 envelope
  后保留；未收到时先 LOST，再由显式 `get` 重建。两项现均已迁移统一 gate 并逐项复验，
  不覆盖全部 UNKNOWN/owner-death 组合。
- [ ] 完成默认历史gate的安全闭环；真实socket/线程/无超时等待已明确保留为非纯，
  当前unit标签与显式选择的库存已对齐，但原runtime合同尚未全部安全迁移/复验。
  已增加 [固定reviewed-pure入口](../scripts/run_reviewed_pure.py)及显式manifest，
  可复现当前已审查子集；根conftest在标准collection前拒绝无文件/目录级执行，
  旧混标文件保留原合同并将未审运行时case标heavy；这不替代完整出口。
- [x] 另62个固定旧文件已完成逐项静态分类，四个AST锁保留原ID/参数；
  陈旧pure成功fixture迁到同一selected-output后端，新增受审范围已进入
  reviewed-pure manifest。分类、实际选择通过和完整runtime gate仍分别计证。
- [x] 已LOST的PG在处理ambiguous lease时，先保留已选typed PG cause，再经
  真实Cancel/inventory/custody ACK收敛后发布；不再误报transport wrapper。
  [四项纯回归](../tests/unit/test_pg_ambiguous_cancellation.py)验证ACKloss、
  non-PG wrapper与knownGrant释放；既有first-error sticky和retry预算不变。
  此项不是公共API surviving-lease ACKloss进程验收。
- [x] GCSLite 不再读写 PublicationControlAdapter 私有锁/tickets/cleanup；
  注册与恢复推进通过该既有adapter接口，service保留membership/fence-ready
  判定。新纯boundary合同覆盖callback替换、重入及owner-death竞争；这只是
  职责封装，不取消集中协调，不勾选下方GCS fidelity取舍。
- [x] 基础Task与成功／用户异常trace的3个原exact用例现有统一工作deadline、
  真实finalizer有界等待及完整PID／owner/trace端口核验，并在当前版本分别通过。
- [x] 七个 examples 已用同一原 main 分别通过 exact 30秒 runner；每 Node
  1MiB、共享get期限、public close(timeout)与无条件shutdown，Actor额外
  PID/endpoint及真实PG显式remove均检查。Actor/PG同步控制仍无per-call
  cancellation deadline，外层超时只表示实验失败，不伪称取消或clean。
- [x] 学习路径的基础1→7现连续，进阶3A–3D移至其后；仍是同一真实后端，
  不删除原章节锚、publication或K1保证。
- [x] 原spillback、跨节点pull、lineage和PG participant-loss exact smoke
  已收紧共享deadline、public close与owner/gate/trace/PID清理，并在本次
  PG修复后逐项复验；两线程localizer也单独通过。具体范围见状态页，
  不是所有allowlist同版验收；PG运行中participant loss不替代取消ACK窗口。
- [x] 四个原Actor K0/trace/restart/Node-migration exact gate已收紧1MiB store、
  共享deadline、publicclose与失败finally的已观测PID/端口检查，并分别复验；
  generation/reset/无透明retry及故意victim故障报告保留，不新增取消语义。
- [x] 修复dispatcher把publication/recovery续行当fresh PG准入的问题。
  [两phase真实进程回归](../tests/integration/test_pg_publication_peer_loss_path.py)
  分别在terminal/adopted真实ACK后失去idlepeer，再丢ACK，证明原publication
  继续adopt/Node退休/finish且不重跑任务；本切片有两个受控事件，不是全fault矩阵。
  两adoption切片加known/unknown Node-loss、freshPG拒绝与deferred routing
  合计另有6项pure。
- [x] PG LOST判定与SYSTEM retry的owner/recovery提交现同锁原子化；
  [三项纯交错](../tests/unit/test_pg_retry_atomicity.py)保留真实PG/Node
  reducer、Grant/Start/SYSTEM_ERROR Complete及完整死亡安装，先失败再
  通过；retry-first仍须被后续fresh准入拒绝，不新增Lease/Push。
  原8项multi-return rollback与3项retry安全迁pure并纳入固定选择。
  原PG创建/删除/shutdown/PREPARE拒绝、ordinary retry、participant-loss
  与两phase publication续行已在修后逐exact复验；participant-loss只
  修正异步GCS资源hint的被动观察，未扩deadline或更改Node资源协议。
  这些不代替完整fault/runtime矩阵，精确结果见状态页。
- [x] README首屏优先定位→七例学习入口→状态/安全边界，原故障进展
  逐字保留为历史折叠段；只是阅读导航，不创建另一个简化执行后端。
- [x] 普通新lease按stored依赖bytes选择首跳：本owner使用匹配epoch/
  canonical身份的多副本，foreign只用原descriptor source；Node仍作
  Hybrid/ledger裁决，真实B→home spillback合法。PG和全部冻结重放
  绕过评分。无快照Worker的冷地址查询是可选正缓存，不造death，
  不新增逐Task强制GCS可用性前提；同步GCS publication合同不变。
  [纯算法](../tests/unit/test_lease_locality.py)、
  [Core组合](../tests/unit/test_core_lease_locality.py)与
  [三个公开任务](../tests/integration/test_lease_locality_path.py)已分别
  运行；Worker冷查询/更多副本失效组合和lease复用不由此宣称完成。
- [x] [Worker无snapshot冷查询→缓存命中](../tests/integration/test_worker_lease_locality_path.py)
  已用4次真实Task执行独立有界验收：Driver source在B，0CPU parent在A
  经nested handle提交两个子Task到B；评分scope的查询数(1,0)与adoption
  其它lookup区分。child foreign lineage/borrower释放及Driver源GC发生
  在Worker存活时，不靠owner death清理；非冷查询故障/并发miss矩阵。
- [x] 原5个Core取消合同8展开安全迁pure，保真实Grant/Release/Cancel/
  Outcome、错误identity、ACK前PENDING和exact finalizer/ownerGC；
  原3个PG首跳/容量重放/retry＋reconstruction也迁pure。重建通过真实
  drop_object丢副本，PG仍CREATED且key完整值不变；不以旧descriptor-only
  success或Python对象is当跨publication协议身份。原live并发项仍heavy，
  这些不是公共shutdown或实时调度证据。
- [x] `_ReadyTask`现用派生且互斥的DispatchKind表达fresh/lease/cancel/
  Push/custody/output/system续行。只有fresh走PG新准入，只有两类
  output保原非PENDING特权；kind不是当前marker authority。
  [纯封套合同](../tests/unit/test_dispatch_kinds.py)验证路由/转发与
  歧义轮次约束，旧真实publication/PG/custody仍用同一协议，不新增后端。
- [x] 原Worker依赖hold重试及queued PG participant-loss合同安全迁pure：
  实际Node/owner/recovery/PG reducers和normal finish/GC，不冒充进程死亡。
  [home-route进程回归](../tests/integration/test_driver_local_node_recovery_path.py)
  修正threshold1误把控制参数lift为无lineage put的前提，保STORED结果和put；
  [Worker Complete回归](../tests/integration/test_worker_crash_recovery_path.py)
  显式替换旧retry断言为同attempt0恢复Node envelope，lease-only探针
  观察replacement而不运行第二用户Task；两项先失败再逐exact通过。
- [x] [原lineage示例](../examples/06_lineage_reconstruction.py)现在原值恢复
  后读取两条Driver事件，展示稳定TaskID/ObjectID与实际AttemptID0→1；
  单drop/重建及原资源/工作预算不变，不靠全trace收齐作为运行时权威。
  原example06 gate已单独复验，更多故障及其余示例同版出口仍开放。
- [x] 原[远端Node恢复](../tests/integration/test_node_crash_recovery_path.py)
  与[第二Node-ready启动回滚](../tests/integration/test_startup_rollback_path.py)
  收紧每store1MiB、有限被动观察、失败finally及精确PID/端点清理并
  分别复验。回滚在fallback前验证，不靠后续shutdown掩盖；恢复保
  原blocker/victim/retry与实际INLINE控制参数，不新增Task/fault。
- [x] 原[put丢失合同](../tests/unit/test_drop_object_replica.py)和
  [Node WorkerLost](../tests/unit/test_node_lease_execution.py)迁为真实
  pure composition。前者真实Seal/Get/Drop后无lineage且normal GC；
  后者先真ARM再reclaim，late Complete因非live RUNNING拒绝，
  SLOT_DROP/真实rollback ACK收敛，不调用stop-worker线程或伪造成功。
- [x] [原PG示例](../examples/07_placement_group.py)在提交Task前展示
  public PGID/attempt/bundle映射，并保实际执行PID对照；解释硬约束
  与容量使PACK也跨节点的限制。原example07已独立复验，不启trace
  或伪造PREPARE/COMMIT日志，不宣称中间可见性实验已完成。
- [x] [原borrower加载/Acquire ACKloss](../tests/unit/test_borrowed_object_refs.py)
  两case迁pure，真实child put/outer canonical lineage/统一发布，
  两token与outerGC后borrower独立保活，真Release tombstone拒重放。
  保原INLINE且不新增实际reconstruction，其余live case仍heavy。
- [x] [Core startup三个原线程合同](../tests/unit/test_core_startup_rollback.py)
  保真实constructor/Thread与原失败点，L1逐exact验收；正常abort在
  failure-finally前核验，不用pure或兜底清理冒充。成功ctor仅隔离
  coordinator周期poll，所有RPC/_sync预置记录型tripwire。
- [x] 原[consumer death](../tests/integration/test_worker_death_ownership_path.py)
  与[owner death](../tests/integration/test_worker_owner_death_path.py)
  补1MiB store/共享期限/finiteclose/failure PID与端口清理并逐项复验；
  owner在实际outerGC后才死，独立noStart/Push租约只观察replacement，
  get/wait及death授权release语义未变，不新增owner接管或故障矩阵声明。
- [x] `.remote()`入门文案区分同步参数/引用准备与异步用户执行：
  seal/hold可在返回前阻塞或抛错，不为“立即返回”错误措辞改变事务边界。
- [x] 原[Release补偿/六种坏ACK](../tests/unit/test_borrowed_object_refs.py)
  7展开与[lateowner准入](../tests/unit/test_core_worker_death_consumer.py)
  迁pure：owner已实际Release，原义务/round事件等真实墓碑ACK；
  late凭证由registry死亡suffix先fence，不造Task/Ref/GC。
- [x] 原[同Node双Worker](../tests/integration/test_two_worker_pool_path.py)
  与[两Node dispatch](../tests/integration/test_parallel_task_lanes.py)
  并发exact收紧共享期限/存储/失败清理并分别复验；双arrival+未ready
  后才放行的证明不变。同Node2CPU/2Worker在测试策略限exact说明，
  不用单CPU顺序执行或吞吐指标代替并行合同。
- [x] 原[三层recursive lineage](../tests/integration/test_recursive_lineage_reconstruction_path.py)
  按明确2CPU/3drop/3重建的复合预算逐exact批准验收，三层不缩减。
  真3finish→全LOST@0→仅root get→全SUCCEEDED@1/各retry1，
  stableTask/Object与依赖lineage不变、3close/GC共享3s且failure清理。
  不是单故障也不代表完整recursive/multi-owner故障矩阵完成。
- [x] 原[borrower pre-effect/不可达/关闭准入](../tests/unit/test_borrowed_object_refs.py)
  三case及[contained Release冻结](../tests/unit/test_contained_edge_runtime.py)
  一case迁pure，真实当前发布/childhold/graph/lineage/ACK/GC，不
  用旧rawedges或假死亡。另两actualshutdown和其它contained线程
  合同仍保未验收heavy，不能以pure代替。
- [x] [foreign owner重建](../tests/integration/test_foreign_reconstruction_path.py)、
  [foreignwait/drop](../tests/integration/test_foreign_wait_drop_path.py)、
  [foreigninput换代](../tests/integration/test_foreign_input_lineage_reconstruction_path.py)
  原exact已收紧期限/被动记录/close/失败清理并分别复验。wait/drop
  保一次drop+一次ACKloss及单reconstruction、physicalGC真实观察
  不推断foreignmetadataGC；input保submit后earlyclose与retained0→1/
  最后output GC。两个限exact预算例外已明确，不改内部RPC重试协议。
- [x] 原[pending outer close](../tests/unit/test_contained_edge_runtime.py)
  迁pure，真实selectedoutput/childhold/edge-before-wake/finish/单
  Release及GC；旧失败case默认不变，其余raw/线程合同未冒称通过。
- [x] 原[borrower Core shutdown](../tests/unit/test_borrowed_object_refs.py)
  两case保实际构造/3线程转L1逐项验收；firstFalse保owner token、
  secondTrue真Release停线程，另livehandle由shutdown主动release。
  明确仅transport/timer投递隔离，不是ray.shutdown多进程证明。
- [x] 原[localnested reconstruction](../tests/integration/test_local_nested_reconstruction_path.py)
  修正threshold1导致containerlift的错误前提，1KiBINLINE容器与
  2KiBSTORED值分开，保原nested非DFS依赖断言；
  [sender-close-before-Push](../tests/integration/test_nested_task_argument_path.py)
  保原3Task/readyput，新增hold/lineage/manifest实际验证并修测试
  Struct捕获序列化失败。两者先失败后bounded逐exact通过。
- [x] 原[contained shutdown-GC precheck](../tests/unit/test_contained_edge_runtime.py)
  迁currentselectedoutput纯组合，真实冻结义务由一次同步清理收敛，
  晚投原timer事件无新效果；不称publicshutdown。
  [generic export重试](../tests/unit/test_worker_export_pin_rollback.py)
  保Core兼容原语的真实pin/round/tombstone/GC，仅手动event投递，
  不伪接当前Worker discovery；当时obsolete拒绝case的后续替换见下。
- [x] [双副本physicalGC](../tests/integration/test_stored_physical_gc_path.py)
  原owner-localCOLLECTED probe、两DropACK因果trace、actualabsence
  与exactreplay均在共享期限/完整failurecleanup下复验；
  [单槽partial reconstruction](../tests/integration/test_multi_return_partial_reconstruction_path.py)
  保原1drop1recon、whole函数重执行/只selected1发布，actualfull/
  selectedEnvelope与healthy0/2不变、最后3refs/lineageGC受限复验。
- [x] [旧stale-orphan合同](../tests/unit/test_contained_edge_runtime.py)
  正式替换为postfinish重复envelope不得释放live committed edge，
  旧→新映射明确；最后真close才child/graph/slot/outerGC，不称旧
  rawrelease断言通过或priorattempt/unadopted清理覆盖。
  [generic export两原case](../tests/unit/test_worker_export_pin_rollback.py)
  迁pure，真pin/FIFO/round/tombstone及GC；不冒publicshutdown或
  currentWorker publication；旧拒绝合同与原真实drain后续闭环见下。
- [x] [原mixed多返回](../tests/integration/test_multi_return_path.py)
  保predecode系统重试、两下游及逐段GC；
  [原whole多返回重建](../tests/integration/test_multi_return_reconstruction_path.py)
  保两显式drop/一次producer重建，真实commit START后注入sibling
  request取得JOIN，外部两次invocation/稳定ID/最后TaskGC不变。
  两exact独立批准并bounded通过，共用期限/failurePID端点清理；
  不称并发publicget或完整故障矩阵。
- [x] [原三槽success/error](../tests/unit/test_public_multi_return_runtime.py)
  迁真实pureCore/Node，保11/22/33的I/S/I发布，每wake前全batch/
  route/Recovery提交；error无successenvelope且全3共享同一error。
  真finish/GC/一次physicalDrop不冒Worker执行或并发race。
  [Worker旧两contained拒绝合同](../tests/unit/test_worker_export_pin_rollback.py)
  正式替换为统一full/selected发布及slotGC，target未选槽原PENDING
  不造reconstruction历史，typedStartfixture不冒真实Node租约。
- [x] [原shared-child混合输出](../tests/integration/test_multi_contained_output_path.py)
  与[publisher Node-loss](../tests/integration/test_multi_output_node_loss_path.py)
  原exact分别有界复验，真两slotfinalholds/旧1退休新1建立、
  full2→selected1/healthy0完整snapshot和逐段GC。crash仍原
  TerminalACK后/graphCOMMIT前，实际KEEP0-DROP1不消耗retry直到
  explicitget；typedcontrols/passivePush无伪造。共享legacyhelper
  保留，失败PID/端点检查与正常survivorledgerclean声明分开。
- [x] [原public提交/3slotretry](../tests/unit/test_public_multi_return_runtime.py)
  四case迁pure，保真正publicremote/注册与owner-recoveryCAS、
  ownerpreflight故障预算/queue全不变；主体后仅对未leaseTask
  显式fixture终结再真finish/GC，不伪装Worker结果。
- [x] [原contained剩余合同](../tests/unit/test_contained_edge_runtime.py)
  旧STORED不GC正式替换为zeroRef/finishbarrier后真实DropGC；
  sameowner保同Core/RLock＋1真实refconsumer转L1，callback错误
  主线程留存避免被GCloop吞掉。
  [原Worker drain](../tests/unit/test_worker_export_pin_rollback.py)
  保真3threadCore构造/Workerdrain转L1，failedRelease保pin，
  第二drain真Release/GC，旧event真实consumer拒重放，finalize
  才stop；不拿残缺fixture异常当unclean或用pure冒线程。
- [x] [原并发reconstruction](../tests/integration/test_core_reconstruction_concurrency.py)
  两exact以真正Node publication/Drop建立1/3slot LOST，2thread
  完整retirement中ticket竞争使second真defer，first真enqueue
  后second真JOIN，共3内部请求；合法旧envelope不改successor。
  三内存Drop前置明确例外，未挪retirement到main或用testmutex
  串行化，也不声称publicget/第二次Worker函数或任意调度证明。
- [x] [原multi-return replay/预检](../tests/unit/test_public_multi_return_runtime.py)
  3函数11参数迁pure，2STORED及3slot保留，wire/decoder分层；
  成功预检失败保真实adoption续行，无新lease/Complete，error
  两分支保持原局部终态，不夸坏wireE2E。
- [x] [foreign两原剩余合同](../tests/unit/test_foreign_stored_task_dependencies.py)
  death保2owner真实receipt/Cancel/死亡授权/最后lineageGC；
  旧STALE自动release正式替换为noncustodyquarantine，随后
  独立死亡事实才允许终结。两GCSeffect实际删dead副本，活owner
  原GC两Drop；sourcePin终结历史保留、真实ACK而非空表为清洁。
  只隔离TCPctor，不假造GCSauthority或普通put重建历史。
- [x] [原两个foreignborrower](../tests/integration/test_contained_ref_lifecycle_path.py)
  与[原storedouter](../tests/integration/test_stored_outer_publication_path.py)
  各exact有界复验；前者两token在outer实际GC后仍活、首Release
  ACK后第二可读；后者forward/逆向graph/Node/GCS/physical证明全保。
  publicclose、共用清理deadline、失败PID/端点观察补齐，不声称
  foreignchildmetadataGC已独立验证；twoCPU仅原exact范围。
- [x] [原multi-return最后生命周期合同](../tests/unit/test_public_multi_return_runtime.py)
  producerPENDING→consumer先绑定lineage，再真实producer成功、
  consumer准备/pub/finish；3close顺序/真实storedpin-unpin后
  同DropGC皆保证lastsibling才release一次。并发保3真closer＋
  1真referenceconsumer转L1，非同步假mailbox或3GC并行。
- [x] [registry原并发](../tests/unit/test_function_registry.py)与
  [原duplicate spillback](../tests/unit/test_spillback_runtime.py)
  分别2thread L1有界验收，原RLock/LeaseID锁/localcopy/Hybrid
  policy/cache不变；不把snapshot复制冒逐Task GCS查询。
- [x] [原nested大参数](../tests/integration/test_nested_large_argument_path.py)
  保64KiBlift/1nestedhandle2alias/sourcePENDING/close-beforePush/
  targetrealAcquire/用户get后才放producer；原2Tasks/2hop/三object
  GC+hidden两physical读均在共有期限/失败cleanup下通过。
- [x] Core/Node/Worker首docstring纠正Driver-only、单Worker与only
  execution的陈旧说明，明确实际内嵌owner/pool/publication边界；
  不把文案更新当新算法/完整出口。
- [x] 七个原Core-owner case迁到纯composition并保IDs，ERROR晚回包测试
  以真实Cancel/Node拒LateStart建立合法边界，不造相互矛盾的Complete。
  四个local-reference case仍用真实线程，现有有限close/FIFOstop/启动失败
  teardown并分别经L1 runner通过；两项gc.collect仍heavy，不拿pure冒充。
- [x] 原public put、stored result、Worker nested task与escaping owner ref
  四个exact MP路径已收紧共享期限和failure清理并各自通过；结果版本在
  dispatcher修复前，见状态页，不能直接当其修后完整门禁。
- [x] 两local lineage与四nested-argument原case已迁到真实pure composition，
  继续验证原holds/lineage/retry/retain补偿/最终GC合同；余两live attempt-
  borrower/timing case仍heavy，不以pure代替真实时间保证。
- [x] 共享sealed删除和owner-death观察增加actual len/SHA-256防御性核对，
  [7项pure故障模型](../tests/unit/test_replica_integrity_cleanup.py)验证损坏/
  unreadable/real pin与显式repair；旧完成receipt不读新epoch、delete后forget
  重放保留。F5及跨authority实际retry/GC另已分别复验，非public损坏复现。
- [ ] 完成其余主线smoke的有界生命周期审查；部分旧测试listener/setup在try外
  或漏端口清理。不能把runner允许某ID或历史通过当作全部入口安全。
- [ ] 在安全分类、fixture 清理修复后重新执行完整单元 gate，而非把历史通过数当成当前通过。
- [ ] 完成已接线统一 selected-output journal/Node adapter、GCS recovery、owner batch
  CAS/per-slot GC 的当前 revision 验收，覆盖 mixed-tier contained multi-return、
  targeted、Node/owner loss、ACK ambiguity 与 drain 组合；不能以接线或历史单返回
  smoke 代替这一完整验收。
- [x] 将旧 INLINE/STORED 故障 gate 与私有双参数 API 移除，统一为
  OutputPublicationGate 的 INTENT、PROMOTIONS、ARM、Complete 四阶段；
  [STORED 原有阶段与新增 ARM-UNKNOWN](../tests/integration/test_stored_outer_node_loss_path.py)、
  [INLINE KEEP/DROP](../tests/integration/test_inline_node_loss_path.py) 已分别通过 bounded runner。
  ARM-UNKNOWN 只验收一项 OwnedChild STORED 场景，不勾选完整故障矩阵或 K0/K1 出口。
- [x] [存活borrowed child的ARM-UNKNOWN单槽切片](../tests/integration/test_borrowed_output_unknown_path.py)
  已独立有界通过：真实Release/resolution先于retry，Task hold/lineage跨retry，
  旧borrower自然退休，同一child使用新contained holds并最终回收；不是mixed/targeted验收。
- [x] [F1 mixed shared-borrowed ARM-UNKNOWN](../tests/integration/test_mixed_borrowed_output_unknown_path.py)
  单故障进程切片已验收两槽DROP、全部旧holds退休后retry、新sibling逐槽GC和
  最后sibling才释放lineage；不扩大为其余失效组合。
- [x] [F3本地Complete成功但未报告/交付](../tests/integration/test_unreported_complete_node_loss_path.py)
  已验收：只在test wrapper中隔离发送和交付，实际GCS冻结为UNKNOWN后清理再retry。
  观察者持有localComplete证据不变成owner custody，也不改变配置gate语义；
  UNKNOWN下的真实envelope到达choice前/后另有pure交错合同。
- [x] [F2 targeted mixed selected故障](../tests/integration/test_targeted_borrowed_output_unknown_path.py)
  与原single-target exact用例分别有界通过：旧target cleanup先于SYSTEMretry，
  健康sibling snapshot/hold不变，reconstruction Task hold跨retry，selected
  按原return index发布并逐槽GC。故意的payload tier变化与两次retry预算计数
  见测试说明；本项不替代其它 F4–F7 场景。
- [x] [F4/F5 pre-Complete owner-death](../tests/integration/test_precomplete_output_owner_death_path.py)
  两个 exact ID 已分别验收 INTENT／PROMOTIONS 时点：真实 outer-owner 死亡，
  全 Node fence 后打开原 gate，同一活 executor 返回 Finalize ACK，live child
  holds／graph／replica 清理；包含替换 Worker 的新端口和 PID 核验。
- [x] [F7 real-wire contained cycle](../tests/integration/test_contained_cycle_control_path.py)
  以真实注册的 metadata-only INTENT/PREPARE 验证原子拒环和 ABORT fencing；
  另一个 public Task 提供真实 owner GC 的 COMMIT/RELEASE 证据，不声称
  metadata 输入具有已建立的 child holds 或 public ObjectRef。
- [x] [F6 foreign-owner late replica](../tests/integration/test_foreign_late_output_replica_cleanup_path.py)
  已独立验收 real foreign Grant 后延迟 report、latched DROP、原 owner 的
  RETIRED/custody 与 consumer cancel/no Push，自治物理 GC 完成后才重放旧 Drop；
  public targeted reconstruction 后旧 report/Drop 不改变新 epoch/bytes、
  healthy sibling 和 child 生命周期。只覆盖该限定单故障＋显式重建组合。
- [x] [存活 Worker owner 的 Node-loss 重试](../tests/integration/test_worker_owner_node_loss_path.py)
  已验收 Driver-certified survivor 安装视图经 local Node 到 embedded Core；
  owner 自主 UNKNOWN 清理及同 owner/executor retry，Driver READY 前只读。
  同 owner 的 provisional token 使用独立命名空间，现有 custody 机制不变。
  此项补恢复前提；F6 由上述独立用例验收，两者不完成整个 Worker owner 语义矩阵。
- [ ] Worker 单返回 coordinator、Core 旧发布分支与 Node 两套旧 journal/handler/
  supervisor 组装及GCS旧registry/RPC已删除，相关测试已迁移或带映射归档。
  共享source/Node incarnation已抽离到publication_sources，owner旧publication/
  retirement/GC关联、独立旧模型、wire字段和InlineInstall facade也已移除；
  源码/旧测试完整归档，保留真正共享的child-hold原语及历史pickle同类型导出。
  继续逐项验收历史合同映射与真实故障组合，不把归档本身当完整等价覆盖。
- [ ] 完成统一重建准入/终态清理的更宽故障验收。当前已修复并有纯契约：
  whole graph先预检所有producer/renewal再START、targeted先renew再单槽退休、
  owner-routed deferred、OPEN失败锁定与ERROR前清理，以及all-LOST/parent
  admission不能绕过active targeted session；不等于全部重建组合完成。
- [ ] 补齐旧Node INLINE合同中的executor-owned child死亡后以精确GCS death proof
  替代不可达release ACK的完整验收。统一Node/adapter已接入typed注册/death/watermark
  证明，pure矩阵覆盖拒绝不完整证明及dead/live child混合；新增
  `test_dead_executor_child_cleanup_precedes_retry_on_same_live_node`已取得独立有界
  真实Worker-only death证据。更宽组合仍未完成，不能用无限重试或伪造ACK代替死亡证明。
- [ ] 对照 Ray 重新审查 ordinary success 的逐任务 GCS 发布依赖与教学主线。
  现有 INTENT/ARM/terminal/adopted 是 mini 自选协议，不能仅用文档标注或更多
  故障测试把这一架构差异当作还原度目标已满足；任何收敛必须保留已要求的
  ownership、未知结果与恢复语义，不通过删除能力来换取更短路径。
  当前K1还明确记录global fail-fast DAG与phase-specific恢复保证；直接采用Ray式
  owner-led完成并取消这些保证需要先确认设计取舍。把graph/phase权威搬出GCS
  只是角色迁移，不自动消除集中协调或提高还原度；未批准前不把这些保证悄悄降级。

### 当前实现快照

| 范围 | 已落盘内容 | 当前证据与缺口 |
|---|---|---|
| 身份、协议、资源 | 强类型 ID、稳定逻辑 ID、不可变消息、定点资源账本、Hybrid policy | Gate A 覆盖；自定义资源强制两节点 spillback 已通过 multiprocess smoke |
| 对象与依赖 | create/write/seal、pin、owner token/location、顶层依赖、nested ref、pull 合并和校验；Task hold 完整绑定 kind/submitter/TaskID/origin AttemptID | Gate A 覆盖 local/foreign fetch/death 与迟到 location-report 双向竞态、absolute deadline、final fence 与 owner-death report convergence；secondary route promotion/零 reconstruction 有独立 bounded smoke 记录 |
| 统一 output publication | ref-free/contained、mixed-tier multi-return/targeted 共用 selected manifest、Node journal、GCS metadata recovery、owner batch CAS/per-slot GC；INTENT/ARM 先于 local Complete；cleanup_pending 阻止补偿完成前 retry | 统一 gate 的 STORED 阶段、INLINE KEEP/DROP 与一项 OwnedChild ARM-UNKNOWN 已逐项验收；其余 UNKNOWN/shared-borrowed/owner-preComplete 与同版完整矩阵仍待完成 |
| Placement Group | 四种策略、bounded backtracking/STRICT_SPREAD matching、GCS 2PC、Node child ledger、bundle-bound Task、同步 create/remove、shutdown drain 与 participant Node-loss LOST | happy/remove/shutdown/Node-loss 均有真实 smoke；bundle rescheduling 与 Actor PG 省略 |
| 恢复 | application/system failure 分类、retry budget、local/nested/foreign-owner/foreign-input/multi-return all-lost/partial-loss lineage reconstruction、attempt/generation fencing | Worker/remote与Driver-local Node/foreign owner/input/multi-return均有真实证据；更宽 fault matrix仍未验收 |
| Actor | 公开 Actor API、GCS 创建、Node lifetime resources、专属 Worker、直达方法、FIFO、同 Node Worker restart 与跨 Node migration | K0、same-Node restart、Node-loss migration均有真实证据；不包含 method retry、named/detached、concurrency groups 或 PG Actor |
| `put` | Driver owner 直接发布 inline 或本地 stored 对象，不占 Worker | 当前 public put smoke 已通过；put 对象明确无 producer lineage、不可 reconstruction |
| 普通 Task 运行时 | `api/core/node/worker`、spawn Worker、lease 后直达 `PushTask` | 每节点可配置 1–2 个固定普通 Worker；当前双 Worker、两节点两 lane 与 Worker 内嵌 Core 子任务 smoke 已通过；nested trace 覆盖 `worker_core` 的 child submit/lease/grant/push/finish；PENDING 保持同一 LeaseID 做 $O(1)$ 瞬时重评 |
| Transport/trace/control | loopback TCP、结构化 trace、独立 GCS-lite 节点/资源注册与启动快照 | loopback、两节点与跨进程 smoke 有独立记录；golden trace 验证四条跨 PID RPC cause 边；最新选择集见状态页 |
| 集群 shutdown | 单一 epoch 的 BeginDrain/clean barrier/Core commit/Node Finalize/GCS stop | 纯单元契约验证 Core commit-before-Node-finalize；commit 失败不发送 Node finalize；真实 smokes 验证干净 teardown |
| Python 3.9 | 源码静态兼容性检查和包导入验证 | 已验证 import，不代表完整测试矩阵已运行 |

`output_publication.py` 的 batch identity/manifest、`output_publication_journal.py`、
`output_recovery.py`、`output_publication_node.py` 及 `ownership.py` 的 batch CAS/per-slot
GC 已组成当前普通成功运行后端。Worker 不再将 contained 结果投影到旧 INLINE/STORED
单返回协议，原 Worker coordinator 源文件已删除。旧故障 gate 及其双参数 API 也已
删除；Core旧分支、Node旧构造/handlers/扫描、GCS旧registry/RPC和owner旧关联也已退役。
独立旧模型与旧wire已移到非执行归档，共享child-custody原语不代表另一套发布后端。
multi-return contained 和 targeted publication 已接线，但完整真实
进程故障矩阵尚未验收。不得以新增模块或纯测试数量替代这项 runtime 验收。

Gate A 的目标是纯模型 L0，不能仅凭历史 `unit` marker 认定安全。项目默认 pytest
配置包含 `-m unit`，但已有分类缺口待修复；当前本机只运行逐项审查的纯集合。
独立 `loopback_smoke`/multiprocess 用例需要显式选择。环境专用
解释器路径属于维护者验证细节，不作为项目使用命令。

历史已验收的五个纵向子切片是（不替代统一后端的同版复验）：单节点 ordinary Task 的 spawn、lease、direct
`PushTask`、结果获取和 shutdown；以及单节点 stored result 的 Worker→Node seal、
descriptor-only reply、owner location publish 和 `get` fetch/checksum。后者也验证了
ObjectStore 只持有物理 bytes、CoreWorker owner table 持有逻辑状态的职责边界；
第三条是独立 GCS＋两个 Node/Worker 的注册与发现，由自定义资源确定性触发 Hybrid
spillback，CoreWorker 再向目标节点取得 lease、direct `PushTask` 并取回小结果。
lease 热路径使用启动期安装到 NodeManager 的不可变集群快照，不逐任务查询 GCS。
这不包括当前成功 publication 的 GCS 依赖：Worker 以 `StartLease` 确认执行权，
成功结果在统一 prepare 获得 INTENT/ARM ACK 后以 `CompleteLease` 取得 Node
envelope，再缓存成功 reply。Node 原子提交 local Complete/释放资源，不等待 GCS
terminal；Core adoption 仍同步确认 terminal/adopted。结果不明的 Core timeout
不释放资源，Worker loss 也必须先由 Node 确认进程退出。
Driver Core 的 coordinator 只把依赖 ready 的任务交给至多两条 dispatch lane；
`PENDING_CAPACITY` 释放当前 lane，经有界延迟后保持同一 TaskID/AttemptID/LeaseID，以
$O(1)$ 瞬时 Node 重评重排。并发 smoke 已证明两条 ready Task 可在两个节点重叠执行。普通
Worker 已按 job
懒创建并在线程内绑定 CoreWorker；child ID 使用 parent attempt 命名空间，子任务仍走
普通 lease/direct push。当前 nested smoke 只返回 plain result，parent 使用 0 CPU；
Worker-owned inline/stored ref borrower、foreign INLINE Task dependency 与 CPU yield 运行时
均已接通；cross-node foreign stored dependency 与 stored physical GC 也已接通。nested trace 已证明 `worker_core`
child submit/lease/grant/push/finish 进入统一 collector。
第四条是 store-backed `ObjectRef` 的跨节点依赖路径：消费者可在生产者 ready 前
提交；目标 Node 在 grant 前 pin 源副本，以 16 KiB chunks 拉取、校验并 seal，随后
Worker 从目标本地 ObjectStore 物化 `RefArg`。lease 与 push 消息只携带 descriptor，
不携带对象 bytes。第五条是 K0 Actor：公开 API 经 GCS 创建、Node 持有 lifetime
resources、专属 Worker 执行，方法调用绕过 GCS 并保持串行状态；shutdown 清理 6 个
受管 PID/端口。Actor restart 不在这项证据内。shutdown barrier 的纯单元契约固定所有
Node 先 BeginDrain、连续 clean 后 Driver Core 先 commit、再并行 Node Finalize，GCS 最后
退出；Core commit 失败时 Node cleanup endpoint 保持未 finalize。

## K0：真实纵向切片

### K0.1 身份、规格与可观测性

- [ ] 定义稳定的 TaskID、ObjectID、ActorID。
- [ ] 从第一天携带 AttemptID、Actor generation，即使初始值只有 0。
- [ ] 定义不可变 `TaskSpec`、资源向量和 typed protocol message。
- [ ] 所有状态转换通过可校验 transition helper。
- [ ] JSONL trace 记录消息、实体 ID、进程内序号和因果边。

验收：纯状态机单元测试证明非法转换被拒绝；同一 Task 的不同 attempt 保持相同
TaskID/ObjectID；trace 不依赖跨进程全局时间排序。

### K0.2 真实进程拓扑和生命周期

- [ ] 使用 macOS/Linux 都安全的 `spawn` 启动方式。
- [ ] 启动一个 GCS、两个逻辑节点的 NodeManager/ObjectStore 和有界 Worker pool。
- [ ] Driver 与 Worker 都具备完整 CoreWorker owner 语义；当前 Worker 已接通
  plain-result 子任务、Worker-owned inline/stored ObjectRef borrower 及 foreign INLINE/
  stored dependency；normal-owner stored outer adoption/GC 已接，完整 fault matrix 仍缺。
- [ ] `shutdown()` 精确终止自己创建的 PID、关闭端口并清理临时文件。
- [ ] 启动失败和部分启动有确定性回滚。

验收：有界 smoke test 启停后无子进程、端口或临时资源泄漏；不得用 `pkill` 等
宽泛清理。

### K0.3 普通 Task 与 direct submission

- [ ] `@remote`、`.remote()`、`ObjectRef`、`get`、`wait` 的最小 API。
- [ ] `.remote()` 完成同步序列化/引用与大参数准备后返回 ObjectRef，
  用户任务异步执行；未就绪顶层依赖由 dependency gate 等待其值。
  准备阶段的 seal/hold 可阻塞或抛错，不保证立即返回。
- [ ] 未就绪顶层 ObjectRef 在 dependency gate 等待且不占 CPU/Worker。
- [ ] NodeManager 签发幂等 Worker lease 和 allocation token。
- [ ] 支持确定性 spillback。
- [ ] lease 后由提交者 CoreWorker 直接 `PushTask` 到 Worker。
- [ ] 用户异常作为 application error 传播且默认不做系统重试。
- [ ] Worker 以 `StartLease` 取得执行权；所有成功结果先完成统一 batch prepare，
  再以 `CompleteLease` 取得 envelope、完成 custody drain 并缓存成功 reply。
  当前真实 Start ACK、Worker discovery、Node journal、GCS recovery 与 Core batch
  adoption 已接线；完整真实进程验收仍需完成。早期失败可先缓存错误选择，
  publication 后失败还须补偿 ACK；NodeManager 原子释放，Core 结果 timeout
  不释放运行中资源。本地 ObjectStoreError 明确拒绝；cleanup_pending 保留真实
  终态并阻止 Core 在 rollback/GCS ACK 前启动新 attempt。
- [ ] 所有终止路径恰好释放一次 Worker 和资源；Worker loss 仅在确认进程退出后回收。
- [ ] lease 回复持续不明时，以同一身份重放并通过 `CancelWorkerLease` fencing 后才终止。
  pre-Push lease、target hop、grant/location、cancel 与 Push 已共用统一
  OutputPublicationID candidate；candidate 不表示提前存在 GCS intent 或副作用。
- [ ] PushTask 只有首次连接建立失败才可释放 grant；RemoteCallError/传输不明后必须
  重放完全相同的 PushTask、Worker endpoint 和 LeaseID。
- [ ] Worker 关闭新准入后仍服务已接受 PushTask 的精确缓存重放，clean shutdown 等待
  reply cache 与 CompleteLease ACK 收敛。
- [ ] Core shutdown 不终结不明 Push/cancel：保持 PENDING、submitted tokens 和恢复线程，
  协议收敛后由第二次 shutdown 完成清理。

验收 trace：

```text
TaskSubmitted → DependencyReady → LeaseRequested → Spillback?
→ LeaseGranted → PushTask → TaskStarted → TaskFinished → ObjectReady
```

其中 `PushTask` 的发送者必须是提交者 CoreWorker；这段 trace 仅列 placement/direct
submission 主线，不涵盖当前成功 publication 的全部控制面步骤。GCS 不转发任务
或结果 bytes，但 INTENT/ARM 与 owner terminal/adopted 同步参与成功路径；不能把
Node local Complete 不等待 terminal 扩大为整个成功路径不访问 GCS。

### K0.4 资源与 Hybrid Scheduling

- [ ] 稀疏标量资源向量，至少覆盖 CPU、GPU、memory 和自定义资源。
- [ ] 节点本地资源账本区分 total 与 available。
- [ ] 明确区分 `INFEASIBLE` 与 `PENDING_CAPACITY`。
- [ ] 无 GPU 请求优先非 GPU 节点。
- [ ] 利用率阈值、locality 和 seeded top-k 选择可确定性测试。
- [ ] 目标 NodeManager 对过期摘要重新校验，永不超卖。

验收：每次 grant/completion 后资源守恒；重复 completion 是 no-op；固定 seed 产生
可复现决策。

### K0.5 ObjectStore、owner 与跨节点 pull

- [ ] ObjectStore 支持 `create → write → seal`，未 seal 不可见，seal 后不可覆盖。
- [ ] 小结果 inline 与大结果 by-reference 的阈值可为测试调低。
- [ ] owner 保存逻辑状态和 locations；ObjectStore 只保存物理 bytes。
- [ ] 跨节点 pull 在源副本 pin 后传输，目标完整校验并 seal 后才 ready。
- [ ] 重复 pull/location add/remove 幂等。
- [ ] GCS、Driver 和 TaskSpec 不转发大对象 bytes。

验收：一个 64 KiB 测试对象在低阈值下被强制跨节点 pull；trace 证明消费者只在
目标副本 seal 后开始。

当前证据：64 KiB 结果先在源 Node seal，消费任务在依赖 ready 前即可提交；目标 Node
在 grant 前完成 source pin、16 KiB 分块传输、checksum 校验与原子 seal，Worker 再从
本地物化 `RefArg`。消息断言证明 lease/push 不转发对象 bytes。完整引用回收、真实
并发 pull 压力和故障恢复尚未验收，因此 K0 仍不能标为完成。
加固范围还包括：相同 object 的并发 pull 串行化；失败 pull reset 并以新 epoch 重试；
pin release 有界重试且 shutdown sweep 遗留 pin；成功后 owner 同时记录 source/target
locations；`GetObject` 校验 attempt、owner、size、checksum；submitted ref 在 enqueue
时建立。规模/压力和故障恢复仍不在当前 smoke 覆盖内。

### K0.6 Actor

- [ ] `@remote` Actor class、handle 和 method ObjectRef。
- [ ] 创建请求经过 GCS，并绑定专属 Worker。
- [ ] 创建完成后，方法调用由 caller CoreWorker 直达 Actor Worker。
- [ ] 默认串行 mailbox 保证每个 caller sequence 的 FIFO。
- [ ] 协议从第一天携带 generation。

验收：Counter Actor 连续调用维持状态；trace 同时证明创建走控制面、方法调用绕过
GCS。

当前证据：公开 `@remote` class、ActorHandle 和方法 ObjectRef 已接通；创建走 GCS，
Node 预留 lifetime resources 并启动专属 Worker，后续调用由 caller 直达，mailbox
验证 FIFO/generation。Counter 的三次调用得到 1/2/3，且 6 个受管 PID/端口均清理。
shutdown 会先停止接收、drain 已接收调用并 fencing 新调用；Core sequence 与 Worker
缓存使相同 replay 幂等、冲突 replay 被拒绝。
同一存活 Node 内的 Actor Worker Phase B1 restart 已接通：ActorID 保持稳定，generation/
route epoch 递增，旧 route 的 in-flight call 失败且不在新实例透明重放，构造器状态重置。
Actor Node-loss migration 已接通；method retry、named/detached、concurrency groups 与 PG Actor 未实现。
Actor constructor/method 的 ObjectRef 参数同样明确不进入 K0＋K1；它是 Actor 与完整
ownership 的高成本交叉功能，不是创建走控制面、调用走 direct path、FIFO 与 generation
fencing 这条教学主线的必要条件。`actor_arguments.py` 仅保留 future pure design。

### K0 出口条件

- [ ] 上述各项的纯单元测试全部通过。
- [ ] 全部 allowlisted 有界多进程 smoke test 分别通过，且每次单独运行；当前
  早期基线、后续新增与复验是累计证据，未同版全量重跑；完整清单见状态页。
- [x] 成功和用户异常各保存一份可解释、可执行的黄金 trace 语义合约；合约符号化
  动态 ID 且不依赖 timestamp，并可渲染为参与者 sequence。
- [ ] 两逻辑节点的所有资源账本在测试 teardown 后恢复基线。
- [ ] README 中的最小 API 被实际验证后，移除“仅为目标体验”提示。

## K1：ownership、恢复与 gang scheduling

K1 只能建立在已验收的 K0 ID、状态机和真实进程路径上，不能另写一套模拟语义。

### K1.1 分布式引用生命周期

- [x] logical Task hold 使用完整
  `TaskReferenceHold(kind, submitting_worker_id, task_id, origin_attempt_id)`；所有 retained
  RPC 回显完整 hold，owner active/tombstone 以完整值为键，SYSTEM retry 保持 origin，
  lineage reconstruction 用新 reconstruction AttemptID 建立新 incarnation。
- [ ] local、submitted-task、borrower、contained-reference 使用唯一 token；inline borrower、
  outer contained edge、transfer-pin release obligation、stored physical GC 与 local-owner recursive lineage 已接通。
- [ ] borrower 在收到 owner ACK 前不能被视为安全持有；inline 路径已满足。
- [ ] 重复 borrow/release 幂等，并以 release tombstone 防止迟到 Acquire 复活。
- [x] INLINE outer result 中的 nested ObjectRef 建立 contained edge，并在 outer 生命周期结束后
  以 ACK 驱动释放 transfer pin。历史 normal-owner、single-return、non-targeted STORED outer
  也有专用 bounded smoke；当前统一后端已接入 multi-return/targeted，新增组合的
  完整验收不包含在这一历史勾选中。
- [x] contained ObjectID graph 采用 fail-fast DAG policy；pure authority 已实现批量
  `PREPARE/COMMIT/ABORT/RELEASE_CONTAINER`、prepared-edge DFS、self-loop/path error 与
  exact replay，且普通 Python 容器自环不进入这张图。STORED 与 INLINE publication、
  adoption 和 owner GC 均接入同一图权威；当前统一后端对全部 selected edges 批量
  预留，不把旧单返回验收自动扩大到全部 batch 故障组合。
- [ ] 最后一个 token 消失后才删除副本和 lineage。
- [ ] owner 死亡返回明确错误，不伪装 owner 接管。
  normal owner 不被替换；GCS owner-death runtime 只收敛 publication/holds/replicas，
  统一 Node journal 与 GCS recovery 记录精确 owner-cleaned 终态；存活 Worker 清理
  pending/cached custody 并 fence 后续 Push/drain publication。更宽的 owner API
  错误矩阵仍单独验收。
- [ ] 统一 publication 的故障矩阵闭环，完整保留 STORED/INLINE、contained/ref-free、
  mixed-tier multi-return 与 targeted 范围。publishing Node loss 必须按 INTENT、
  ARM/Complete 与 owner custody 证据清理或保留 selected slots；ARM 无 terminal
  不能被当成从未 Complete。intent-before-effect、GCS recovery、Core KEEP/DROP、
  Node/owner death 仲裁与 retirement 已接线，迟到旧结果不能复活 DEAD location。
  已知 Complete 的 LOST 保留成功事实，显式 `get` 才 reconstruction；UNKNOWN
  清理仍遵守系统重试预算与 finish barrier。原有 pre-Complete 与
  post-Complete/pre-TaskReply STORED、INLINE KEEP/DROP 已迁移统一 gate 并逐项通过；
  新增 OwnedChild与live borrowed-child STORED ARM-UNKNOWN各有窄验收。仍缺其它 UNKNOWN/terminal
  丢失、shared-borrowed、owner-preComplete 及其 mixed/targeted 组合；完整真实
  进程故障矩阵以 `current-status.md` 为准。

### K1.2 Lineage 与 attempt fencing

当前纵切面已覆盖 single-return stored output 与三层 local-owner DAG：同 TaskID/ObjectID、
新 AttemptID、START/JOIN 合并、旧 attempt fencing 和 `max_retries` 预算。下列完整范围仍
保持未勾选。
P0 加固还保证所有约束在状态突变前预校验，且 shutdown fence 在修改 owner/recovery/
accepted count 前生效。

- [ ] owner 保存 task 输出的 producer TaskSpec 及其依赖引用。
- [ ] 相同 producer 的并发 reconstruction 请求被合并。
- [ ] reconstruction 使用新 AttemptID、原 TaskID/ObjectID。
- [ ] 旧 attempt 的 result/location 消息不能修改当前状态。
- [ ] `put()` 对象、owner 死亡和重试预算耗尽返回不同的明确错误。
- [ ] 用户异常与系统失败采用不同重试决策。

### K1.3 Blocking get 的 CPU yield

已接通 Node 权威 blocked/unblocked handlers、Worker notifier 和 Core 真实等待点：完整
identity 与 episode sequence fencing、$O(1)$ 账本转换、unblock tombstone、signed debt 和
terminal finalizer。单 CPU、双 Worker nested-get multiprocess smoke 已验收；固定池支持每
Node 1–2 个 ordinary Worker。
原 exact smoke 已收紧全路径 deadline、引用与 socket teardown，并按当前 source
独立复验；pure notifier backoff也改为fake delay recorder。完整success/failure/
cancel矩阵与全部单元gate仍须单独核验。

- [ ] 单 CPU Worker 内阻塞 `get` 只临时让出 CPU。
- [ ] GPU、自定义资源和 Actor lifetime resource 保持占用。
- [ ] 依赖 ready 后先 reacquire CPU 再恢复用户代码。
- [ ] 成功、失败和取消路径都只释放/恢复一次。

### K1.4 Placement Group

当前最小两 bundle 纵切片已接通并有显式 remove 与 shutdown 自动清理的真实进程证据；
下列勾选表示机制已进入当前教学运行时，不表示 node-loss 恢复已经完成。

- [x] bundle planner 在 shadow resource view 上确定性规划。
- [x] `prepare/commit/abort` 消息和节点 reservation 状态机幂等。
- [x] 任一 prepare 失败会在所有已 prepare 节点回滚。
- [x] 全部 commit ACK 前 PG 不可见、PG Task 不可启动。
- [x] PG Task 只能从指定 bundle pool 分配。
- [x] 节点丢失时整个教学 PG 进入明确的 `LOST` 状态。

### K1.5 节点故障与 Actor restart

- [x] 精确检测受管普通 Worker crash-stop 并以 fresh WorkerID replacement；远端 managed
  Node crash Phase A 也已覆盖一个 Driver-owned ordinary Task 的 survivor retry。
- [ ] 删除死亡节点的 locations、lease 与 allocation。
- [x] 普通 Task 在 Worker outcome 确认可重试的失败且清理收敛后按预算产生新 attempt；
  成功Complete的原envelope可得时仍采用同attempt，不因Worker退出重执行。
  stored publication 的
  原有 pre-/post-Complete Node-loss smoke 已迁移统一后端，新增一项 OwnedChild
  ARM-UNKNOWN 证明清理后预算内 retry。cleanup_pending 仍要求 Core 等待完整补偿
  与 GCS ACK；这一窄勾选不覆盖新 batch 后端的完整 orphan fault matrix。
- [x] 同一存活 Node 内 Actor Worker restart 保持 ActorID，并增加 generation/route epoch。
- [x] 旧 generation 的 endpoint、call/reply/mailbox ACK 被 fencing；本切片不实现 heartbeat。
- [x] old in-flight Actor method 失败，不透明重放；fresh Worker 从构造器初态启动。
- [x] Actor 所在 Node 死亡后迁移到 survivor，保持 ActorID 并推进 generation/route epoch；
  旧调用失败且不透明重放。method retry、named/detached、concurrency groups 与 PG Actor
  明确省略。

### K1 出口条件

- [ ] borrower/nested ref、lineage、CPU yield、PG rollback、node loss、Actor restart
  都有纯状态机单测。
- [ ] 每类机制各有一个受限、单故障的真实多进程 smoke test。
- [ ] 所有故障测试由语义 event/failpoint 驱动，不依赖 `sleep()` 猜时序。
- [ ] 旧 attempt/generation/transaction 消息的迟到注入被稳定 fencing。
- [ ] 每个故障场景 teardown 后无资源、进程和未 seal 对象泄漏。

## 明确不进入 K0＋K1

- [ ] 不实现 GCS HA、共识、持久日志和 owner 接管。
- [ ] 不实现 Autoscaler、Dashboard、Jobs、runtime env 和多语言。
- [ ] 不实现对象 spilling、Plasma/RDMA 或生产级零拷贝。
- [ ] 不实现完整抢占、公平调度、复杂标签与 scheduler policy 集合。
- [ ] 不实现 Actor concurrency groups、异步 Actor 的完整语义。
- [ ] 不实现 Actor constructor/method 的 ObjectRef 参数。
- [ ] 不实现生产级安全、多租户和真实多机运维。
- [ ] 不实现 Ray Data、Train、Tune、Serve、RLlib；后续只能用 Core API 做小型组合
  示例。

这些条目故意保持未勾选，因为“不实现”不是交付工作；它们是范围护栏。
