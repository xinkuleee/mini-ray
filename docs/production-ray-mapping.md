# mini-ray 与 Ray Core 的职责映射

日期：2026-09-09。本页对应已独立通过约定验收的基础版；**本地固定标记为`teaching-base-v0.1`，增强版`teaching-enhanced-v0.2`两项协议已同版验收**。
基础标记的同版318项纯合同、32个真实smoke及七个main产物见[基础账本](acceptance-baseline.md)；当前增强结果见[增强账本](acceptance-enhanced.md)。相对源码链接跟随检出版本。

Ray 源码对照固定为 **c3162dce8d064824293875c5d0bbfd76a54e04ce**；本轮已核对本地 [ray checkout](C:/Users/t-hdong/Desktop/gao/ray) 的 HEAD。
下表链接到该本地 checkout 的实际文件；[固定版本上游树](https://github.com/ray-project/ray/tree/c3162dce8d064824293875c5d0bbfd76a54e04ce) 用于以后检出相同布局。
“对应”指职责和问题相近，不表示类、线程、消息或错误语义逐行一致；mini-ray 不承诺替代 Ray API。

## 从 mini 带回 Ray 的机制

| 领域 | mini-ray 阅读入口 | 固定 Ray 源码入口 | 保留的因果关系与简化 |
|---|---|---|---|
| Python API 与身份 | [api.py](../src/miniray/api.py)、[task_outputs.py](../src/miniray/task_outputs.py)、[ids.py](../src/miniray/ids.py) | [remote_function.py](C:/Users/t-hdong/Desktop/gao/ray/python/ray/remote_function.py)、[task_spec.h](C:/Users/t-hdong/Desktop/gao/ray/src/ray/common/task/task_spec.h) | remote 提交异步执行；Task/Object 的逻辑身份与 attempt 分离。mini 每 Task 仅一个输出 |
| 提交与依赖 | [core.py](../src/miniray/core.py)、[dependency.py](../src/miniray/dependency.py) | [normal_task_submitter.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc)、[dependency_resolver.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/core_worker/task_submission/dependency_resolver.cc) | 顶层 Ref 是执行依赖；取得 lease 后由提交者直接向 Worker PushTask，不经中央任务转发队列 |
| Node 资源与 lease | [node.py](../src/miniray/node.py)、[resources.py](../src/miniray/resources.py)、[lease_policy.py](../src/miniray/lease_policy.py) | [node_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/raylet/node_manager.cc)、[cluster_lease_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc)、[local_lease_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/raylet/scheduling/local_lease_manager.cc)、[hybrid_scheduling_policy.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc) | snapshot/policy 提议，实时本地 ledger 最终扣账；feasible 与 available 不同。mini 固定小 Worker pool |
| 控制面与死亡事实 | [control.py](../src/miniray/control.py)、[owner_death_fence_registry.py](../src/miniray/owner_death_fence_registry.py)、[node_death_view.py](../src/miniray/node_death_view.py) | [gcs_node_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/gcs/gcs_node_manager.cc)、[node_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/raylet/node_manager.cc) | GCS 管成员和协调；受管进程死亡与超时不同。mini owner-wide fence/Driver 安装屏障是显式教学协议 |
| 字节与对象传输 | [object_store.py](../src/miniray/object_store.py)、[object_manager.py](../src/miniray/object_manager.py)、[transfer_pins.py](../src/miniray/transfer_pins.py) | [object_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/object_manager/object_manager.cc)、[pull_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/object_manager/pull_manager.cc) | owner/位置不等于 bytes；source pin、完整目标、immutable seal。mini Python bytes/TCP chunk 不等于 Plasma/共享内存 |
| ownership 与 nested Ref | [ownership.py](../src/miniray/ownership.py)、[ref_transfer.py](../src/miniray/ref_transfer.py)、[owner_service.py](../src/miniray/owner_service.py) | [reference_counter.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/core_worker/reference_counter.cc)、[core_worker.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/core_worker/core_worker.cc) | local、Task、borrower、contained、lineage 是不同存活理由；owner identity 不随 borrower 或副本迁移。mini 直接 owner/borrower，没有完整 borrower tree 优化 |
| retry 与 reconstruction | [recovery.py](../src/miniray/recovery.py)、[reconstruction_runtime.py](../src/miniray/reconstruction_runtime.py)、[owner_reconstruction.py](../src/miniray/owner_reconstruction.py) | [task_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/core_worker/task_manager.cc)、[object_recovery_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/core_worker/object_recovery_manager.cc) | 丢失结果按 lineage 重算、旧 attempt fenced；mini 单输出 whole replay、有限预算，不保证外部副作用 exactly-once |
| Actor | [actor_client.py](../src/miniray/actor_client.py)、[actor_state.py](../src/miniray/actor_state.py)、[actor_worker.py](../src/miniray/actor_worker.py) | [gcs_actor_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/gcs/actor/gcs_actor_manager.cc)、[actor_task_submitter.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc) | 创建走控制面，方法直达；mini 串行、每 caller FIFO/去重，仅同 Node restart，Node loss 终态 |
| Placement Group | [placement.py](../src/miniray/placement.py)、[placement_group_runtime.py](../src/miniray/placement_group_runtime.py)、[control.py](../src/miniray/control.py) | [gcs_placement_group_scheduler.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/gcs/gcs_placement_group_scheduler.cc)、[placement_group_resource_manager.cc](C:/Users/t-hdong/Desktop/gao/ray/src/ray/raylet/placement_group_resource_manager.cc) | bundle 全 commit 前不可见；Node reservation 才是物理资源。mini 至多两个 bundle、两种 STRICT 策略、Node loss→LOST |

[固定版本 Task lifecycle 文档](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/doc/source/ray-core/internals/task-lifecycle.rst) 可与七个 [教学示例](learning-path.md) 对照。
GCS 不逐个调度普通 Task，不等于 Ray 普通 Task 永不访问 GCS；基础标记退出了mini自创逐结果发布门禁；增强主线把它作为明确的mini研究协议加入，不能混称Ray原始流程。

## 基础版普通结果怎样发布

Worker 执行并序列化一次，保留结果 bytes 和源引用；owner 登记待交接清单；child owner 确认引用保活，Node 物化并记录准确 Complete。
owner 随后原子安装结果和 outgoing edges，真实接管 ACK 后 Node/Worker 才能退休托管；GC 另按引用存活理由回收。
[output_handoff.py](../src/miniray/output_handoff.py)、[output_publication_node.py](../src/miniray/output_publication_node.py) 与 Core 的组合代码展示这些责任。
这些具体 DTO、journal/收据及步骤是 mini 的显式组织，不是对 Ray 消息顺序的逐条复刻。

| 事实 | 不能据此推导的另一事实 |
|---|---|
| 用户函数已返回 | Node 已 Complete、owner 已接受 |
| Node Complete | owner READY 或所有 cleanup 已结束 |
| owner 已提交成功 | 当前所有 bytes 仍可读取 |
| 回复/source 托管已退休 | 逻辑对象、contained hold 或实体副本已经 GC |
| RPC timeout | 对端死亡、操作没发生或清理成功 |

基础版 GCS 不保存普通 publication manifest、INTENT/ARM/terminal/adopted 历史或 contained graph。
它仍提交成员/死亡事实并驱动 owner-wide Node fence；fence 未获真实完成 ACK 时节点安全可见性和 shutdown 仍受约束。
owner-led 并不意味着无握手、无清单、无收据；必要的未知效果补偿与 fencing 不能随中央协议一起删除。

## 对象和恢复语义的明确边界

- Task 每次只产一个 ObjectRef；tuple/list 可以是一个完整返回值。无独立多返回槽、targeted reconstruction、健康 sibling 隔离。
- Task 顶层 Ref 参数自动参与依赖；nested Ref 保持句柄；直接返回 child Ref 是引用数据，不自动 get。
- 显式 put 支持普通值和有效含 Ref 的值，借用/交接仍有完整身份；put 没有 Worker lease 或 producer lineage。
- 存活 Node 的准确 Complete 可支持原结果继续交付；已知成功且 bytes 全失仍为 LOST。
- Node 死而 owner 未提交、无存活准确成功收据时为 UNKNOWN；清理后按有限系统重试，不能说从未执行。
- owner 死亡明确失败，副本存在不授权 owner 接管；[Ray 对象故障语义](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/doc/source/ray-core/fault_tolerance/objects.rst) 是对照，不是兼容性认证。
- 普通 Python 容器环属于序列化；ObjectID 之间的强引用环不保证自动回收。基础版没有全局图防环或 tracing GC。

## 工程简化不是生产能力

| mini 的界限 | 不可从实验推导的结论 |
|---|---|
| 单机 loopback，1–2 逻辑 Node，受管进程 fail-stop | 多机部署、网络分区容错、完整 failure detector |
| 单 Python/job，固定 1–2 ordinary Worker slots/Node，每 attempt 一个 lease | 动态 Worker pool、runtime env、多语言、跨任务 lease pipeline |
| Python/cloudpickle、TCP request/reply、bytes store | gRPC/IPC 性能、共享内存/零拷贝、spilling/RDMA、生产 memory pressure |
| 确定性小型 Hybrid/locality；两种硬 PG 策略 | 完整公平性、抢占、标签策略、生产 PG 优化或重排 |
| 串行 Actor，同 Node restart；引用参数/结果内 Ref 不支持 | 并发组、named/detached、跨 Node migration、状态恢复或透明方法重放 |
| 单 GCS、内存收据、owner 不接管 | GCS HA、持久化共识、跨 incarnation 恢复 |

## 第二阶段：明确的 mini 协议研究

第二阶段已在固定基础版之后接入GCS普通结果发布事务和全局ObjectID图防环，两项均已在最终增强snapshot03按有限合同同版验收。
前者为特定故障窗口增加存活的发布事实副本，后者对受支持引用边增加全局 PREPARED/COMMITTED 准入和成环拒绝；二者也增加同步依赖、补偿与退休责任。
Ray 的 ReferenceCounter 不等于 mini 自创全局图；增强版的价值是比较协议保证和代价，不是“更接近生产 Ray”。

当前真实公共API已证明：put/Task无环发布与GC；B=put([A])之后重建A返回B形成两对象环候选；两Node四对象并发预约联合判环。证据分层与固定snapshot见增强账本，旧metadata-only请求不再承担公共可达性证明。
不能用可变 Python list 修改已 put 的 immutable 值、手造 ObjectRef 或伪 borrower 来补可达性。
固定基础版持续作为首次学习入口；增强版沿同一实现主线演进，不在产品中长期保留双后端。
具体窗口、未决项和两版交界见 [两阶段计划 §10](redesign-plan.md)，当前增强通过范围以[增强账本](acceptance-enhanced.md)为准。
