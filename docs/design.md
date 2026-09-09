# 基础版架构：从任务到引用回收

本页描述 teaching-base 的实际单输出运行时：用真实进程、消息和资源账本解释所选 Ray Core 机制。
它是缩小规模与范围的教学实现，不承诺生产 Ray 的完整功能、接口兼容或相同内部协议。
运行命令见[学习路径](learning-path.md)和[测试指南](testing.md)，具体版本与实测边界见[基础验收账本](acceptance-baseline.md)。

## 1. 进程、身份与权威

默认教学拓扑是单 job、单 GCS、单机 loopback、1–2 个逻辑 Node；每 Node 使用固定的 1–2 个普通 Worker。
进程由 spawn 启动；Actor 使用独立 Worker。Driver 和普通 Worker 都能持有 CoreWorker，子任务走同一提交后端。

| 参与方 | 实际负责的事实 | 不代替的事实 |
|---|---|---|
| Driver / Worker 内的 CoreWorker | 提交、依赖准备、owner 表、恢复准入及本地组合提交 | 远端 Node 的资源释放与物理 bytes |
| GCSLite | 成员与资源摘要、函数导出、权威死亡、owner-wide Node fence、Actor、PG 协调 | 普通 Task 转发、普通结果发布事务、全局 contained 图 |
| NodeServer | Worker pool、lease/allocation、依赖物化、Store、执行 Complete、回复托管 | owner 的逻辑可见性和 child owner 的引用真相 |
| ObjectOwnerTable | 对象状态、位置、存活理由、outgoing child edges、退休与 GC | 由 metadata 凭空生成 bytes |
| child owner | incoming hold 的完整身份、Acquire/Release 与墓碑 | outer Task 的成功或全局防环 |
| Worker | 执行用户函数，保留序列化源、引用和 import session 至交接或补偿 | 用函数返回冒充 owner 已接管 |

TaskID/ObjectID 表示稳定逻辑身份；AttemptID、Worker/Node incarnation 和 Actor generation 表示执行世代。
请求还绑定 lease 或 put operation、owner、manifest digest、hold/source 等字段。同一请求身份不能换字段重放。
当前内部仍有单元素 output/slot 容器；它们不表示公共 API 支持独立多返回槽。

入口：[api.py](../src/miniray/api.py)、[core.py](../src/miniray/core.py)、[control.py](../src/miniray/control.py)、
[node.py](../src/miniray/node.py)、[ids.py](../src/miniray/ids.py)、[task_outputs.py](../src/miniray/task_outputs.py)。

## 2. 提交、依赖与 lease

每次 Task 返回一个 ObjectRef；tuple/list/dict 是这个对象的值。
wait 的 num_returns 是等待几个引用，仍为 metadata-only，不等于 Task 的输出个数。

remote 返回句柄前会同步序列化参数并取得必要引用保活，因此准备阶段可以阻塞或报错；用户函数异步执行。
超限 by-value 参数明确拒绝，调用者用显式 put 复用大值；运行时不自动生成 StoredArg。
顶层 ObjectRef 是 readiness 依赖；容器里的 Ref 是引用数据，由用户代码显式 get。

Core 在提交接纳时登记 Task/lineage holds；sender 随后关闭自己的句柄不能使 accepted Task 的输入消失。
尚未 ready 的顶层依赖停在 dependency gate，不占 dispatch lane、Worker 或 CPU。
依赖可用后，Core 选择 lease 首跳，目标 Node 用自己的最新账本最终决定 grant、PENDING_CAPACITY 或 spillback。

locality 按存储依赖的有效位置建议首跳；Hybrid 的可行性/可用性判断和 Node 实际扣账是后续步骤。
资源摘要可能过期，不能使 Node 超卖。PENDING 保持原请求身份重评，不凭等待再建一个 attempt。
lease 通过后，提交者 Core 直接向指定 Worker 发送 PushTask；GCS 不转发任务或大对象 bytes。

普通成功与应用异常使用同一执行链；应用异常默认终态，明确系统失败才进入预算控制的重试。
返回超时只表示调用者没有结果，不证明 Worker 停止、Node 释放 allocation 或远端没有效果。

入口：CoreWorker._register_submission、_prepare_task_dependencies、_execute；
[dependency.py](../src/miniray/dependency.py)、[lease_policy.py](../src/miniray/lease_policy.py)、
[resources.py](../src/miniray/resources.py)与 Node 的 request/start/complete lease handlers。

## 3. 结果交接：Complete、READY 与退休

普通结果由 Node 与 owner 交接。OutputHandoffTable 保存 owner 本地的准确清单/Complete/adoption/abort 历史，
本身不拥有执行成功或对象可见性；Node journal 则记录自己实际发出的效果与收到的收据。
基础版没有 GCS 普通发布 INTENT/ARM/terminal/adopted 门禁，也没有全局 ObjectID 图服务。

1. Worker 执行并序列化一次，保留 payload、原 child handles 和 import session。
2. Node 验证完整 manifest/payload，先让 outer owner 登记清理清单并取得准确 ACK。
3. Node 推进 child prepare、物化 INLINE/STORED 结果及 final-hold promotion；未知回复重放原效果。
4. Node journal 记录准确 Complete，并使本地 lease/资源清账收敛；owner 是否已 READY 是另一个事实。
5. Core 在组合锁中检查当前 attempt，提交 owner 结果/outgoing、recovery 状态与唤醒，记录准确 adoption。
6. owner 的接管证明驱动 Node/Worker 退休回复与来源托管；对象的 bytes、引用和 lineage 仍由正常 GC 回收。

这些步骤不能压成一个“成功”：函数返回、Node Complete、owner READY、bytes 可用、回复退休、对象 GC 各有观察点。
owner 已 READY 后丢失退休 ACK，只继续精确重放和 finish/GC 屏障，不回滚 READY 或再次执行函数。
abort 关闭旧身份的前进权限；已经存在的 holds、partial write 或副本仍要准确释放，不能把 fence 当清理完成。

跨模块必要原子性由 Core 的现有组合锁维护；外部调用后重查当前身份/撤销状态，旧 ACK 不是永久前进许可。
临时scratch托管丢失时，先核已有owner完整receipt/result；owner仍持有的INLINE bytes不能误判LOST，已锁定UNKNOWN/DISCARD也不能因晚消息反转。
对象后来 LOST、重建或 GC，不改写旧交接事实；紧凑历史保留在 owner/job 生命周期内，不随 payload 一起删除。

入口：[output_handoff.py](../src/miniray/output_handoff.py)、[output_publication_journal.py](../src/miniray/output_publication_journal.py)、
[output_publication_node.py](../src/miniray/output_publication_node.py)，Core 的 register_output_handoff 与 _drive_output_publication_adoption。

## 4. bytes、pull 与 source pin

ObjectStore 只暴露完整 sealed bytes：create/write 期间不可读，seal 后不可覆盖。
owner 保存逻辑身份与位置；ObjectManager 管传输和本地副本，二者不能互相代证。
INLINE 携带小值，STORED 使用 descriptor；lease/Push 中的存储依赖不携带大对象 bytes。

跨 Node pull 先 pin 源副本，再分块读取；目标核对身份、长度和 checksum，完整 seal 后才交给 Worker 读取。
同对象的重复 pull 要与失效/GC 协调；不能只看旧位置摘要就宣布目标 bytes 已存在。

TransferPinOutbox 在发送 Pin 前保存准确 source request。active reader 尚未退出时，后台不能释放其 source pin。
读取结束后用原 transfer identity 关闭；丢 ACK 时保留义务并重放同一 Release，不换一个 token 猜结果。
源 Node 的 Release-before-late-Pin 先写关闭墓碑，因此迟到 Pin 不能重新保活旧 transfer session。
源/请求者死亡使用已安装的准确 Node 死亡事实；RPC timeout 不构成死亡证明。

入口：[object_store.py](../src/miniray/object_store.py)、[object_manager.py](../src/miniray/object_manager.py)、
[transfer_pins.py](../src/miniray/transfer_pins.py)及 Node 的 pin/chunk/release handlers。

## 5. 多 owner 与 pregrant 副本交接

执行许可与物理副本 custody 是两种责任。依赖拉取可能已 seal 一些 bytes，随后另一个源失败，最终却没有 grant。
Node 用 LeaseDependencyInventory 保存这类 pregrant 效果；Cancel 返回准确 inventory 后，Core 进入同一副本交接驱动，
不伪造 Worker、grant 或执行 Complete，也不因为“未获 lease”就遗忘已物化 bytes。

Core 的 _drive_location_handoff 对每个 owner 保存真实报告收据。首个确定失败锁定用户可见错误，并尽早取消执行权；
一个 owner 的 ACK 未知不能跳过其它 owner 已 seal 的副本。CUSTODY_ONLY 表示接管副本责任，不允许 consumer 执行。
全部 owner 收据或适用死亡授权收齐后，才向 Node 确认准确 inventory 已移交；ACK 丢失只重放这份 inventory。

提交者 Worker 死亡时，Node 仍按冻结 route/hold 推进已登记的 owner 交接，不伪造死 Worker 的 ACK。
正常交接、Grant 回复未知和 pregrant 取消复用实际责任路径，没有另一套演示或存储后端。

入口：[lease_dependencies.py](../src/miniray/lease_dependencies.py)、Core 的 _execute / _drive_location_handoff，
Node 的 cancel_worker_lease、ack_lease_dependency_custody 与 abandoned dependency 推进。

## 6. 引用、put、GC 与 quarantine

owner 固定不变。Worker 创建的对象逃逸给 Driver 后，owner 仍是原 Worker；副本不能接管其逻辑身份。
每次解码产生的 borrower 独立保活，取得 owner Acquire ACK 后才可用；两个 borrower 可以比 outer 对象活得更久。
Task hold、物理 attempt borrower、lineage hold、outer contained hold 和 transfer pin 分别退休。

含 Ref 的 put 使用独立 put operation，先发现完整引用清单、保留来源、取得 child holds，再原子安装 owner 对象。
它支持 INLINE/STORED，但没有 Task lease、用户函数执行或 producer lineage，因此丢失 put 的全部 bytes 不会触发重建。
Task 首次发布和 whole replay 的新结果仍使用实际 owner/child 交接；删除自动 lift 不删除 foreign stored/nested replay 责任。

最后真实存活理由消失后，owner 冻结 collection/retirement 计划，收齐 child Release、物理 Drop 与相应 lineage 释放。
准确已安装的 child-owner 死亡证明只解除该 child 的责任，不能替活 child 或别的 owner 清账。
close ACK、空队列、回复缓存退休或进程停止都不独立证明 metadata、bytes 和 lineage 已全部 GC。

ReplicaCleanupQueue 保留按 object/attempt/owner/Node/checksum 绑定的 Drop；PINNED 或 ACK 未知不能提前完成。
Node 的删除水位阻止旧写入；真实物理 absence 与 metadata/manager 收口后才记录完成，旧 receipt 不读写新 epoch 副本。
迟到 replica report 要由原 owner 的退休/collection 历史裁决；STALE_EPOCH 本身不是释放 ACK。

如果报告冲突且没有 custody/删除授权，Core 进入 quarantine：保留证据和未清责任，不盲删共享 bytes，不反复轮询同一拒绝。
准确死亡安装可唤醒相应处理；外来 descriptor 不能自行授权删除或替换 owner。
手造无历史 epoch 或损坏 metadata 的纯模型只说明 fail-closed，不证明公共 API 可达，也不承诺自动修复任意损坏。
基础版不保证 ObjectID contained 环全局拒绝或回收；普通 Python 容器自环是另一件事。

入口：[ownership.py](../src/miniray/ownership.py)、[put_handoff.py](../src/miniray/put_handoff.py)、
[ref_transfer.py](../src/miniray/ref_transfer.py)、[replica_cleanup.py](../src/miniray/replica_cleanup.py)与 Node 的物理 Drop 尾部。

## 7. 故障知识、whole 重建与首 ACK

| 当前可核查事实 | 基础版处理 |
|---|---|
| owner 已 READY | 保留成功；只收口托管，bytes 后续仍可能丢失 |
| 存活参与方持有准确 Complete | 继续原交接；若确无可用 bytes，记录已知成功但 LOST，不制造 READY |
| Node 已死，owner 未接管且无存活准确成功收据 | UNKNOWN；先 fence/清理旧责任，再按有限系统重试策略推进 |
| 已知成功对象无 bytes，owner 活且有 lineage | 旧责任退休后，由 get 等恢复需求触发 whole-function replay |
| put 无 lineage、预算耗尽或 owner 死亡 | 返回对应明确错误；不切换 owner 或恢复 GCS 内存 |

重建保持 TaskID/ObjectID，使用新的 AttemptID；递归依赖先就绪，foreign inputs 以真实 retained 换代完成交接。
重建前先收口旧 publication/holds/副本责任；只重建整次单输出函数，没有 targeted 或健康 sibling 隔离。
未知回复不是再次消费执行预算的理由；外部副作用不提供 exactly-once。

OwnedObjectReconstructionReducer 的 START/JOIN 来自真实 owner/recovery 准入及 queue handoff 的 immutable receipt。
首 ACK 使用该收据，不用 ACK 到达时的当前 READY/LOST 状态推测过去是否准入；preview 不产生执行权限。
完整请求及原回复在发送 ACK 前缓存，borrower 释放或 epoch 后移后仍能精确重放，但不会再次入队或扣预算。
相同事务身份换字段被拒绝；新请求仍检查当前凭证。历史重放与新的准入证明各有用途。

owner-routed Drop 同样先检查 operation_id 的完整 request 绑定，再取历史 reply；不能用缓存绕过冲突校验。
历史收据不包含可无限复制的结果 payload，也不把当前墓碑当全局恢复服务。

入口：[recovery.py](../src/miniray/recovery.py)、[reconstruction_runtime.py](../src/miniray/reconstruction_runtime.py)、
[owner_reconstruction.py](../src/miniray/owner_reconstruction.py)、[foreign_lineage_runtime.py](../src/miniray/foreign_lineage_runtime.py)。

## 8. 阻塞 get 的 CPU yield

Worker 在真实等待前用 BlockingNotifier 通知 Node；ready fast path 不通知，嵌套等待共用一个 episode。
Node 按完整 lease/task/attempt/worker identity 与单调 sequence 校验 Block/Unblock，墓碑拒绝迟到旧 Block。
只让出 CPU，GPU/自定义资源及 Actor lifetime allocation 保持占用；完成/失败/死亡按真实持有状态一次清账。

unblock 在恢复用户代码前立即恢复逻辑 CPU allocation，不等物理空闲。
若 child 正占着已让出的 CPU，账本内部形成 signed CPU debt；调度器看见的 available 非负，后续释放先偿还 debt。
这样 parent 可以继续，而调度器不会把负债当作新容量超卖。

notifier 锁入口或 Block 构造失败要恢复本地 depth，不消耗未发出的 sequence；一旦可能发送 Block，保留原 sequence，
退出时推进准确 Unblock。group 只保存实际进入成功的 scope，不能把失败对象当已通知状态。

Core 的 LOST 等待先在 condition 锁内读谓词，离锁进入 notifier，再持锁重查；Unblock 也不占该权威锁。
PENDING/foreign poll 在通知后重算原 deadline 的剩余量，不额外授予完整等待预算；到达的 READY/ERROR 仍走正常优先级。
get 超时不等于取消已经开始的 notifier/Unblock 控制 RPC，也不承诺任意 OS 中断/OOM 恢复。

入口：[blocking.py](../src/miniray/blocking.py)、ResourceLedger.yield_cpu / reacquire_cpu、
Node 的 notify_worker_blocked/unblocked handlers 与 Core 的 get 等待分支。

## 9. Actor：创建、直达与 generation

Actor 创建走 GCS，Node 保留 lifetime resources 并启动专属 ActorWorker；后续方法由 caller 直接发送。
串行 mailbox 保每 caller FIFO 和同代去重，不承诺不同 caller 的全局顺序。
方法返回普通值的 ObjectRef；constructor/method 的 Ref 参数和结果值内 Ref 明确不支持。

存活同 Node 内可有限重启：ActorID 不变，generation/route epoch 增长，构造器重跑、内存状态重置。
旧调用/回复被 fencing，不透明重放到新实例；Actor Node 死亡为终态，不跨 Node migration。
容量不足与构造失败使用 typed failure，构造器 traceback 中出现 resource 字样不改变分类。

入口：[actor_state.py](../src/miniray/actor_state.py)、[actor_client.py](../src/miniray/actor_client.py)、
[actor_worker.py](../src/miniray/actor_worker.py)及 GCS/Node 的 Actor handlers。

## 10. PG：预留、LOST 与已有续行

PG 支持 1–2 个 bundles 和 STRICT_PACK/STRICT_SPREAD。GCS 冻结小型计划并协调，Node 独立维护 bundle 资源账本。
prepare 失败回滚所有已 prepare 参与方；全部 commit ACK 前不能对外 CREATED，PG Task 只用指定 bundle pool。
participant Node 死亡使该 attempt 终态 LOST；活 Node 的预留仍要真实 abort，不能据死亡清掉别处资源。

PG LOST 判定与 owner/recovery 的 system-retry 提交共享 Core 组合锁。
死亡先提交就不推进 attempt/预算；retry 先提交也必须在后续 fresh 准入拒绝已 LOST 的 capability。

若 PG loss 已选为用户错误，未知 lease 仍须 Cancel/inventory/custody 收口；传输 ambiguity 不能覆盖原 typed cause。
其它更早选定的错误保持原义。错误已知与远端义务已解除不是同一事实。

_ReadyTask 的 DispatchKind 区分 fresh 准入和已有 lease/cancel/Push/custody/output 续行。
PG 的新准入拒绝不能截断旧 publication 退休、Node-loss 裁决或精确重放；已知 Complete 不因另一个 bundle 死亡变 ERROR。
这也不保证任意 PG 故障后任务成功：UNKNOWN 仍按原清理与 PG 终态边界处理。

入口：[placement.py](../src/miniray/placement.py)、[placement_group_runtime.py](../src/miniray/placement_group_runtime.py)、
[control.py](../src/miniray/control.py)，Core 的 dispatcher、_retry_system_failure 与 ambiguous lease cancellation。

## 11. 成员死亡与退出

GCS 提交成员/owner 死亡事实，Driver/Node 安装带身份与顺序的视图，Core 才消费本地已安装事实。
超时、断连或旧摘要不能宣布死亡；Node 死亡只解除其私有内存责任，存活 child owner 的 holds 仍需准确处置。
owner-wide fence 是关闭前进的许可边界，后续物理清理和已接受请求的退休还要各自完成。

shutdown 先关闭新准入，再收口已接纳任务、borrower、put、回复托管和 GC，保持所需 owner/Node 端点可服务。
Core 与 Node 的 drain/finalize 各看实际义务；未决 ACK 或 quarantine 不能被空 clean 标志盖住。
只有适用的清理事实齐备才报告 clean；强制终止只证明进程停止。启动失败回滚只处理自己已经启动的参与方。

入口：[api.py](../src/miniray/api.py)、[node_monitor.py](../src/miniray/node_monitor.py)、
[node_death_view.py](../src/miniray/node_death_view.py)、[owner_death_fence_registry.py](../src/miniray/owner_death_fence_registry.py)。

## 12. trace 与阅读边界

TraceRecord 记录实体/执行身份、process_sequence 和 cause_event_id；跨进程按因果边解释，不把 timestamp 排序当全局时钟。
原始 trace 保留本次运行身份，golden contract 符号化随机 ID 并检查角色、合法状态和真实因果链。
观察 sink 异常不改变业务提交；trace 的 ACK 事件表示观察到回复，不自动证明后续本地 commit 已完成。

output_owner_ready 观察实际 owner CAS/wake 后的事实；output_payload_retired 只表示回复托管退休，不代表物理 GC。
基础版示例解释 owner-led 路径，不能引入增强 GCS 阶段来“补齐”基础 trace，也不能把缺失 trace 当丢失业务事实。

第一遍沿例 01 的 API→Core→Node lease→Worker→owner 阅读；第二遍用例 03/06 与引用实验追踪 bytes、holds 和重建；
再用例 05/04/07 看 CPU、Actor、PG。源码入口见[学习路径](learning-path.md)，证据层级见[基础验收账本](acceptance-baseline.md)。
纯模型、真实 authority 组合和有界进程各证明自己的边界；历史 pass 不认证后续修改，本文不替代版本绑定的实测结果。

入口：[trace.py](../src/miniray/trace.py)、[trace_contract.py](../src/miniray/trace_contract.py)、
[golden_traces](../src/miniray/golden_traces/)。旧规格与清理前文本从[历史索引](history-index.md)取回。
