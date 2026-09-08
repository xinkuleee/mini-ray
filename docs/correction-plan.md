# 纠偏方案：语义差异与冻结交付基线

状态：**待明确语义确认，尚未实施**。日期：2026-09-08。

本页整理已讨论的 S1–S4、需求级冻结交付表，以及最后一次局部修订。保存／推送
不构成实施授权。当前运行时代码仍保留既有 GCS 发布、global DAG 与 phase-specific
恢复；实际代码、历史证据及未验证草稿见 [handoff](handoff.md)。

## 1. 不变目标与需求来源

原目标仍是与 mini-SGLang 相似定位的教学 mini-ray：真实多进程，保留 Ray Core
的机制、算法与职责因果链；只改造当前唯一后端，不用兼容 API 或另一套演示替代。

- K0：Task／Actor／ObjectRef、动态子任务、真实进程、资源向量与 Hybrid、Worker lease／
  spillback／direct submission、owner、不可变对象、跨节点 pull、稳定 ID、基本重试与 trace。
- K1：分布式引用／nested refs、lineage reconstruction、阻塞 get 的 CPU yield、Actor
  generation/restart、PG 原子资源预留、受管节点故障及确定性故障验证。
- 继续保留已承诺的 multi-return、mixed-tier、targeted 健康兄弟隔离、逐槽与最后 sibling GC。

三种来源必须分开：用户确认的能力；为能力必需的原子性、身份、fencing、保活与清理；
实现中自选的 GCS 发布事务、全局无环保证及滚动验收门槛。后来写入 roadmap／测试／
状态文档不自动成为用户新增要求；已经承诺的可观察语义也不能静默删除。

## 2. S1–S4 语义差异

| 取舍 | 改变 | 不改变／责任去向 |
|---|---|---|
| S1：owner-led 普通结果发布 | 取消每个普通输出的 GCS INTENT／ARM／terminal／adopted 门禁；不是取消所有 GCS 访问或必要 ACK | GCS 保留成员／权威死亡、Actor、PG；Node 负责物理 Complete／lease，owner 负责 selected-set 可见性，child owner 负责引用真相，Node／Worker 保回复与源 custody 至真实交接 |
| S2：取消 global contained DAG 强保证 | 不再保证跨 owner 并发成环被全局 fail-fast 拒绝；不承诺引用计数回收 ObjectID 环 | 保普通 Python 容器环、nested refs、独立 borrower 保活与无环引用关系的 GC；全局防环职责被撤销而非搬服务，不新增 tracing GC；有环不能虚报 clean |
| S3：以存活权威收据区分成功与 UNKNOWN | 删除 GCS 全局发布阶段历史；仅 GCS 曾记录成功、owner 未收到而 Node 已死时，可从旧 POSTCOMPLETE 变为 UNKNOWN，影响重试次数／预算／惰性重建时机 | 存活权威的 exact Complete／commit 不可回退；UNKNOWN 不等于未执行或已失败；清理、预算、旧 epoch fencing、健康 sibling 不变、owner 死亡明确失败及非 exactly-once 外部副作用承诺保留 |
| S4：按需求冻结发布门禁 | 不因白名单增加自动扩发布范围，不要求全部历史 runtime 迁 pure，不因文案变化机械重复全子集 | 保成本审查、单 exact、真实协议断言、精确清理；已知违反冻结保证的缺陷须关闭；白名单仅执行许可，纯模型不能替代端到端 |

S3 是真实语义取舍，不只是少存诊断数据：若仍要求“GCS 曾知道成功就必须在 Node
死亡后继续可知”，必须保留额外存活收据副本，不能与无条件删除该历史同时承诺。

### S1：交接记录的完整生命周期

清单登记、Node Complete、owner 结果可见、回复托管退休是四个不同事实：

1. 清单登记只接管待发布效果的清理责任，不表示执行成功。
2. Node Complete 确认实际执行及所选输出条件成立，完成对应 lease 清账。
3. owner 对冻结 selected-set 原子提交结果、outgoing edges 及相应状态，随后才可见。
4. 真实接管 ACK 允许交付方退休回复托管，不代表对象副本或引用已 GC。

当前 GCS INTENT 的清理清单改由 outer owner 的待交接记录承接；ARM 的许可改为
Node 本地身份及发布条件校验；terminal 的成功事实由存活 Node／owner 的 exact
收据持有；adopted 改为 owner 接管 ACK。不是把同一中央事务搬到新服务，也不承诺
无握手或 RPC 必然更少。

| 对象 | 责任人与交接条件 |
|---|---|
| provisional hold | executor 为持有者，child owner 为引用权威，Node 推进发布；经 child owner 原子 promotion，或精确 Release／适用死亡清扫解除。发送方超时不代表不存在 |
| final hold | outer owner 负责最终释放，child owner 保存 incoming hold；生效前 outer owner 必须已确认完整清理清单；可见时责任原子转入正常 outgoing edges |
| Worker source custody | Worker 保留源引用、import session 及未完成本地释放；正常路径确认全部 selected final custody 后实际释放本地来源，撤销路径按远端补偿条件释放；promotion ACK 不证明本地 close 已成功 |

待交接记录只在两种情况下退休：

- 清理责任已原子转入正常 outgoing edges；尚未完成的交付／缓存退休等责任仍由
  原责任方持有，不能一起抹掉。
- 所有可能产生的效果已逐项被精确补偿、阻止再生效的 fencing，或适用的权威死亡
  事实解除。fencing 不能替代已经存在的资源释放。

撤销须关闭原 publication／attempt 的前进权限，覆盖已发送而结果未知的 prepare／
promotion。优先复用 child owner 的 Release tombstone；迟到请求不得重新建立引用，
历史成功回执不能授权继续发布。不另建平行清理权威，不用永久保留记录代替收敛。

尤其不能漏掉 publisher 死、outer owner 活却未知 final holds 的窗口。Node Complete 前
必须确认 selected outputs 和所需 final custody 已就绪。owner 死亡后，存活 Node 还须
处理 partial write、reply cache、Worker source custody 和 lease，不能只扫 sealed
replica。死亡、资源释放或线程停止只证明各自事实，不能冒充其它清理完成。

当前责任来源可查 [publication prepare](../src/miniray/output_publication_node.py)、
[child hold/tombstone](../src/miniray/ownership.py)、[Worker custody](../src/miniray/worker.py)、
[GCS 补偿](../src/miniray/control.py)与 [Node 收尾](../src/miniray/node.py)。

### B1：准入证明与 exact replay 记录分离

- 临时准入证明服务当前请求。START 来自 owner／recovery 真正准入及正常队列交接；
  JOIN 来自合法并入实际已提交目标，不再入队。preview、OPEN、当前状态不能产证明。
- 首次成功 ACK 发出前，固化完整请求绑定和原回复供后续 exact replay。继续保留
  “borrower 释放后仍可重放原回复”；历史重放不重新准入，新请求仍查当前凭证。
- 临时证明随请求处理结束释放。重放记录在 owner Core／job incarnation 内保留，
  到关闭协议准入、处理完已接受请求的明确终结事件退休；更早退休只能用显式协议，
  不能任意加 TTL。owner 死亡不提供接管或跨 incarnation 重放。
- 每个有效事务只存紧凑请求／回复身份与准入事实，不存结果载荷、完整 lineage，
  也不为查询、重传或后续 execution 无限复制历史。

停止条件：完成、失败、再次 LOST 或 epoch 推进不改写原事实；错误身份和 queued
sibling 不得借用证明；精确重放不重复入队或消费预算。不再靠增加终态分支修补。

## 3. D0 前置边界与 D1 职责

D0 开始前先确定状态归属、提交点和必要跨模块事务接口，再在这些边界内实施。

| 权威边界 | 拥有什么／不拥有什么 |
|---|---|
| Task 生命周期 | attempt、预算、队列交接、执行推进与收尾；不直接改 Node 资源 |
| Owner | 逻辑结果、引用、待交接清单、outgoing edges 与 GC；不制造远端 bytes |
| Node lease | execution、Worker slot、allocation、依赖 pins 的原子绑定与释放；policy 仅提议 |
| publication | 托管交接、效果重放及补偿推进；不复制 Task 成功或引用存活权威 |

必要的跨边界原子提交保留明确组合事务，不能拆成各自独立提交。外部 RPC 与本地
提交边界明确；shutdown 聚合各责任方的未完成义务，不另造平行 clean 标志。
D0 不继续把新状态堆入 Core／Node 后再承诺“以后拆”；D1 只在职责落实后整理
代码布局、教学材料和追踪入口。禁止通用工作流引擎或空壳 adapter。

## 4. 需求级冻结交付表

“已有证据”指历史记录和相应断言，不证明改造后的版本已通过。下表是需求级
基线，不是执行白名单；测试路径只是证据入口，不授权整文件或目录运行。

| 编号／原要求 | 当前实现与缺口 | 权威边界 | 指定有限证据／停止条件 |
|---|---|---|---|
| D0 架构与发布责任 | GCS 协议已实现；S1–S3 待确认，owner-led 承接未实现 | Owner／Node／child owner／GCS 按 §2–3 分工 | 普通成功真 trace 无逐任务 GCS 门禁；publisher 死且 owner 活未获结果、owner 死且 publisher 活的责任交接；无无人持清单、早删、假成功／clean；同后端涵盖各 storage/selected 形态 |
| K0.1 ID／规格／trace | 强类型 ID、不可变规格、因果事件及黄金合同已有；新 trace oracle 待验证 | ID 值与事实权威分开；trace 只观察 | `test_ids_resources.py` 的 stable-ID 合同；[success/application-error trace](../tests/integration/test_cross_process_trace.py) 的 exact 用例。按真实新职责显式替换 GCS 断言，不删后冒原语义不变 |
| K0.2 真实拓扑／生命周期 | spawn、两 Node、固定 Worker pool、Worker Core 已接通；固定版验证缺口 | 启动器／WorkerPool／各业务责任方 | [partial startup rollback](../tests/integration/test_startup_rollback_path.py)、[two-worker overlap](../tests/integration/test_two_worker_pool_path.py)；每项进程 gate 的 PID／端点／资源收尾。无宽泛清理、无线程停止冒 clean |
| K0.3 Task／依赖／dynamic child／direct Push | API、gate、lease/push 已实现，推进状态仍集中 Core | Task 生命周期／dependency gate／Node lease／Worker | [cross-node dependency](../tests/integration/test_cross_node_dependency_pull.py)、[Worker child](../tests/integration/test_worker_nested_task_path.py)、应用异常 trace；PENDING 依赖不占执行资源，真实提交者直达 Worker，用户异常不误系统 retry |
| K0.4 Hybrid／locality／ledger | [resources.py](../src/miniray/resources.py)、[lease_policy.py](../src/miniray/lease_policy.py)已实现 | policy 提议，目标 Node 最新 ledger 分配 | fixed-point、feasible/available、非GPU、阈值、seeded top-k 纯合同及 [locality wire](../tests/integration/test_lease_locality_path.py)；旧摘要不超卖、grant/释放守恒，不需压力矩阵 |
| K0.5 Store／pull／put | create/write/seal、pin、分块校验、INLINE/STORED 已实现 | owner 逻辑，Store bytes，ObjectManager transfer | K0.3 pull 真链、[public put](../tests/integration/test_public_put_path.py)、partial-write/重复 pull 定点合同；未seal不可见，lease/Push/GCS不转大bytes，put无lineage且不占Worker |
| K0.6 Actor direct／FIFO | registry、专属Worker、mailbox/client已实现 | GCS 创建，Node lifetime allocation，ActorWorker 状态 | [Counter](../tests/integration/test_actor_k0_path.py)、[Actor causal trace](../tests/integration/test_actor_cross_process_trace.py)、mailbox gap/dedup；每callerFIFO且不重复执行，不新增全局顺序 |
| K1.1 borrower／nested／GC | tokens、Release墓碑、逐槽GC已有；D0后接线待验 | outer outgoing／child incoming／独立borrower | [two borrowers outlive outer](../tests/integration/test_contained_ref_lifecycle_path.py)、[physical GC](../tests/integration/test_stored_physical_gc_path.py)；保活到真实Release，metadata/副本/lineage分别证明，close或shutdown不代证GC |
| K1.1 nested＋StoredArg＋foreign replay | local大nested与foreign TOP_LEVEL renewal已有；foreign nested STORED＋whole replay未找到指定真证据 | submitter hold／foreign retained／Worker import | [large nested](../tests/integration/test_nested_large_argument_path.py)、[foreign TOP_LEVEL renewal](../tests/integration/test_foreign_input_lineage_reconstruction_path.py)；只补或复用那个确实缺证的交界，不推全笛卡尔积 |
| K1.2 lineage／retry／fencing | [recovery](../src/miniray/recovery.py)、whole/targeted/foreign实现已接；B1待修 | Task/recovery预算，normal submit执行，owner fence | [recursive lineage](../tests/integration/test_recursive_lineage_reconstruction_path.py)、[system retry](../tests/integration/test_task_retry_path.py)与put/ownerdeath/exhaustion合同；root-only恢复、逻辑ID稳定、新attempt，旧结果/位置不改新状态 |
| multi-return／mixed／targeted交互 | unified selected-output已实现并有历史证据 | Task共享预算，owner冻结selected-set batchCAS | [whole multi-return](../tests/integration/test_multi_return_reconstruction_path.py)、[mixed contained target](../tests/integration/test_multi_contained_output_path.py)；健康sibling不变、一次准入、逐槽GC/最后lineage，非任意故障组合 |
| B1 首ACK根因 | paired state只修部分窗口；[新targeted回归](../tests/unit/test_targeted_reconstruction_first_ack.py)未验 | 真准入＋queuehandoff产证明，owner/job内exact replay | §2 B1生命周期；真实快完成、后续epoch、后台先提交、preview/错身份/queued slot及一次真并发交接；无重复queue/budget，不枚举终态 |
| B2 drop请求绑定 | 缓存早返回绕过完整request conflict，未修 | owner request binding/cache | 同ID同request重放，同ID变字段拒绝无第二NodeDrop；[真实drop ACK-loss/reconstruction](../tests/integration/test_foreign_wait_drop_path.py)保原断言，不扩debug矩阵 |
| K1.3 CPU yield | notifier/ledger运行时已接 | Worker通知，Node账本，Task最终清账 | [single-CPU nested get](../tests/integration/test_blocking_get_cpu_yield_path.py)、onlyCPU/episode/timeout/error/Complete/Worker-loss定点合同；其它资源不释放，逻辑debt明确且一次清账；不新增RUNNING cancel |
| K1.4 PG | planner、reservation、2PC、LOST已有 | GCS规划/可见性，Node bundle ledger | [normal/remove](../tests/integration/test_placement_group_path.py)、[prepare failure](../tests/integration/test_placement_group_prepare_failure_path.py)、[Node loss](../tests/integration/test_placement_group_node_loss_path.py)及真实ledger拒绝纯组合；全ACK前不可见、完整回滚、隔离、LOST，不自动迁bundle |
| K1.5 Worker/Node/owner death | managed proof、传播、恢复已接；D0/S3后待验 | detector观察，GCS提交death，各owner/Node执行本地清理 | [Worker postComplete](../tests/integration/test_worker_crash_recovery_path.py)、[Node survivor retry](../tests/integration/test_node_crash_recovery_path.py)、[owner death](../tests/integration/test_worker_owner_death_path.py)，含既有Driver-home/Worker-owner入口；timeout不判死，无owner接管，旧位置不复活，已知/UNKNOWN按S3 |
| K1.5 Actor restart/migration | generation/reset已有；`resource`字符串分类存在静态缺陷，未复现 | Node typed failure，GCS restart policy | [restart](../tests/integration/test_actor_restart_path.py)、[migration](../tests/integration/test_actor_node_loss_migration_path.py)，容量可等待/构造器含resource文本仍终止的有限正反；ActorID稳定、代际fence、不跨代透明重放method |
| D1 职责与教学 | authority类已有，Core/Node跨权威推进与shutdown白盒耦合仍在 | §3边界，不新增平行状态 | 正常/恢复/退出用同状态入口，不读对方私有字典拼clean；七条主线可解释且映射Ray；仅搬文件不算完成 |
| D2 固定版验收 | 历史证据分散，两草稿未验且分类失配 | 需求→证据→安全执行各自独立 | §5执行清单逐项证明冻结保证，已知真实缺陷关闭，文档与实现一致；之后停止，不heavy清零、不滚动扩allowlist |

证据层级不能混淆：PG failure MP 的第二拒绝由 GCS 语义 failpoint 产生，ABORT/ACK
是真实 wire；真实第二 Node 容量拒绝另有 reducer 组合证据。顺序调用 START/JOIN
不是实际并发 RPC，typed reply double 不是 E2E，历史 pass 不自动绑定新代码。

## 5. D2：需求冻结与执行清单冻结

本表现在作为需求级基线。进入固定版本验收前，一次性落实：

- 需求编号；
- 精确测试选择器、参数和关键断言；
- 成本与进程／线程、内存／对象、时间及清理边界；
- 绑定的源码版本；没有 Git 时用可核验内容快照；
- 旧证据保留、替代或不再适用的具体理由。

已有证据能证明同一不变量且仍适用时直接复用，不为填表新增测试；旧结果不能
认证改动后的路径。foreign nested STORED＋replay 只保留已指出的交界验证缺口，
不能推导完整笛卡尔积。白名单只决定执行许可，不能定义发布范围。新反例确实
违反冻结保证时必须记录并关闭；仅能再构造一个交错，不是新增阻塞项理由。

本机为 MacBook Air M4／16GB；未知成本先按重型，不运行重型或未经审查的全量／
目录测试。真实线程/进程用例逐项静态审查后单 exact 隔离执行，不并行 pytest；
保原真实协议断言，不造 ACK／手填终态／删断言／以pure模型冒端到端。

## 6. 一次性待确认范围与唯一顺序

除 S1–S4 外，一次性明确三个原建议边界：

1. 观察 API：本版保持 metadata-only `wait`，不加 `fetch_local`；提供可解释 trace
   与现有观察入口，不额外要求建议中的 `debug.explain` 拼写。
2. 存储优化：本版保现 bytes store，延后 shm/mmap/零拷贝，不冒称共享内存实现。
3. Actor 参数：本版不支持 constructor/method 的 ObjectRef 参数；创建/direct/FIFO/
   generation/restart等能力保留。不能把后写roadmap当成用户已确认省略的证据。

唯一顺序：**一次性确认可观察语义与范围 → 确定状态归属、提交点和事务接口 →
D0 在这些边界内完成责任接管及已列根因修复 → D1 整理布局／教材／追踪入口 →
D2 一次性冻结执行清单并验收。**

用户确认的是可观察语义；具体协议正确性由实施方负责证明，不把实现细节反复
变成用户审批。确认前不实施、不恢复旧循环，不把方案修订或源码归档标成目标完成。
