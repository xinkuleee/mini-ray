# 协议增强版与 Ray Core 的职责映射

本页解释当前 `teaching-enhanced`：Task、lease、owner、对象和资源机制继承基础版，GCS 普通结果发布事务与全局 ObjectID 引用图防环是本版额外的两项 mini 自定义保证。相对源码链接均指本分支；R2.2 已保存验收与 R2.3 修复进度见[状态页](current-status.md)。

首次学习仍推荐[固定基础版教材](https://github.com/xinkuleee/mini-ray/blob/ab4cfb317fd786a286359d8f4e971ff195739375/docs/learning-path.md)。B 的 R2.2 实测源码为 `0a340b792c89667e493f7e7313935e45e29071bf`；教材与[基础版验收包](https://github.com/xinkuleee/mini-ray/blob/ab4cfb317fd786a286359d8f4e971ff195739375/artifacts/cleanup-base/final/acceptance.json)固定于后续仅加文档/证据的 `ab4cfb317fd786a286359d8f4e971ff195739375`，后者不冒充直接实测提交。

E 的[R2.2 验收](../artifacts/cleanup-enhanced/final/acceptance.json)独立绑定 `3f5b725fb26390b86c78f085486fb73d897d3e42`；[B 历史账本](acceptance-baseline.md)与[E 历史账本](acceptance-enhanced.md)分别保留原 318/32、377/37。R2.3 修复已完成，本次以阶段性本地提交固定；旧结果不改标为后继 HEAD 已通过。

Ray 源码对照固定为 **c3162dce8d064824293875c5d0bbfd76a54e04ce**；对照原始审计固定版本。
下表链接到该固定版本的上游文件；[固定版本上游树](https://github.com/ray-project/ray/tree/c3162dce8d064824293875c5d0bbfd76a54e04ce) 用于以后检出相同布局。
“对应”指职责和问题相近，不表示类、线程、消息或错误语义逐行一致；mini-ray 不承诺替代 Ray API。

## 从共同机制带回 Ray

| 领域 | 当前 E 的共同机制入口 | 固定 Ray 源码入口 | 保留的因果关系与简化 |
|---|---|---|---|
| Python API 与身份 | [api.py](../src/miniray/api.py)、[task_outputs.py](../src/miniray/task_outputs.py)、[ids.py](../src/miniray/ids.py) | [remote_function.py](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/python/ray/remote_function.py)、[task_spec.h](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/common/task/task_spec.h) | remote 提交异步执行；Task/Object 的逻辑身份与 attempt 分离。mini 每 Task 仅一个输出 |
| 提交与依赖 | [core.py](../src/miniray/core.py)、[dependency.py](../src/miniray/dependency.py) | [normal_task_submitter.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/task_submission/normal_task_submitter.cc)、[dependency_resolver.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/task_submission/dependency_resolver.cc) | 顶层 Ref 是执行依赖；取得 lease 后由提交者直接向 Worker PushTask，不经中央任务转发队列 |
| Node 资源与 lease | [node.py](../src/miniray/node.py)、[resources.py](../src/miniray/resources.py)、[lease_policy.py](../src/miniray/lease_policy.py) | [node_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/raylet/node_manager.cc)、[cluster_lease_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/raylet/scheduling/cluster_lease_manager.cc)、[local_lease_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/raylet/scheduling/local_lease_manager.cc)、[hybrid_scheduling_policy.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc) | snapshot/policy 提议，实时本地 ledger 最终扣账；feasible 与 available 不同。mini 固定小 Worker pool |
| 控制面与死亡事实 | [control.py](../src/miniray/control.py)、[owner_death_fence_registry.py](../src/miniray/owner_death_fence_registry.py)、[node_death_view.py](../src/miniray/node_death_view.py) | [gcs_node_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/gcs/gcs_node_manager.cc)、[node_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/raylet/node_manager.cc) | GCS 管成员和协调；受管进程死亡与超时不同。mini owner-wide fence/Driver 安装屏障是显式教学协议 |
| 字节与对象传输 | [object_store.py](../src/miniray/object_store.py)、[object_manager.py](../src/miniray/object_manager.py)、[transfer_pins.py](../src/miniray/transfer_pins.py) | [object_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/object_manager/object_manager.cc)、[pull_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/object_manager/pull_manager.cc) | owner/位置不等于 bytes；source pin、完整目标、immutable seal。mini Python bytes/TCP chunk 不等于 Plasma/共享内存 |
| ownership 与 nested Ref | [ownership.py](../src/miniray/ownership.py)、[ref_transfer.py](../src/miniray/ref_transfer.py)、[owner_service.py](../src/miniray/owner_service.py) | [reference_counter.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/reference_counter.cc)、[core_worker.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/core_worker.cc) | local、Task、borrower、contained、lineage 是不同存活理由；owner identity 不随 borrower 或副本迁移。mini 直接 owner/borrower，没有完整 borrower tree 优化 |
| retry 与 reconstruction | [recovery.py](../src/miniray/recovery.py)、[reconstruction_runtime.py](../src/miniray/reconstruction_runtime.py)、[owner_reconstruction.py](../src/miniray/owner_reconstruction.py) | [task_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/task_manager.cc)、[object_recovery_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/object_recovery_manager.cc) | 丢失结果按 lineage 重算、旧 attempt fenced；mini 单输出 whole replay、有限预算，不保证外部副作用 exactly-once |
| Actor | [actor_client.py](../src/miniray/actor_client.py)、[actor_state.py](../src/miniray/actor_state.py)、[actor_worker.py](../src/miniray/actor_worker.py) | [gcs_actor_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/gcs/actor/gcs_actor_manager.cc)、[actor_task_submitter.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/task_submission/actor_task_submitter.cc) | 创建走控制面，方法直达；mini 串行、每 caller FIFO/去重，仅同 Node restart，Node loss 终态 |
| Placement Group | [placement.py](../src/miniray/placement.py)、[placement_group_runtime.py](../src/miniray/placement_group_runtime.py)、[control.py](../src/miniray/control.py) | [gcs_placement_group_scheduler.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/gcs/gcs_placement_group_scheduler.cc)、[placement_group_resource_manager.cc](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/raylet/placement_group_resource_manager.cc) | bundle 全 commit 前不可见；Node reservation 才是物理资源。mini 至多两个 bundle、两种 STRICT 策略、Node loss→LOST |

[固定版本 Task lifecycle 文档](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/doc/source/ray-core/internals/task-lifecycle.rst) 可与七个 [教学示例](learning-path.md) 对照。
GCS 不逐个调度普通 Task，不等于 Ray 普通 Task 永不访问 GCS。B 没有 mini 逐结果发布门禁；本增强版加入这一自定义门禁，同时保留 Core→Worker direct push。上表中的成员、死亡、Actor/PG 协调是两版共同职责，E 的 GCS 发布与图职责见下节。

## 本版普通结果：共同职责与增强步骤

Worker 执行并序列化一次，保留结果 bytes 和源引用；Node 核对 manifest/payload 并取得 outer owner 登记收据。C0 向 GCS 登记 INTENT，C1 预留图边；真实 child prepare、物化与 promotion 完成后，C2 以准确准备收据取得 ARM。

C3 由 Node 提交 Complete 并收口本地 lease/资源，C4 由 GCS 保存准确 terminal。C5 提交图边后，C6 仍由 owner/Core 原子提交 READY、outgoing 与 recovery，C7 再由 GCS 记录实际 adopted 收据。GCS 不替 owner 提交 READY，不替 Node 释放资源，也不保存结果 bytes。C7 ACK 未知只继续精确查询/重放与托管退休，不回滚已提交的 C6。详细边界见[当前设计](design.md)。

当前 E 的入口为[Node 发布](../src/miniray/output_publication_node.py)、[owner 交接](../src/miniray/output_handoff.py)、[发布与图状态](../src/miniray/enhanced_publication.py)、[控制推进](../src/miniray/enhanced_publication_control.py)及[owner 客户端](../src/miniray/enhanced_publication_client.py)。这些 DTO、journal、阶段和收据是 mini 的组织方式，不是 Ray 消息顺序的逐条复刻。

| 事实 | 不能据此推导的另一事实 |
|---|---|
| 用户函数已返回 | Node 已 Complete、owner 已接受 |
| GCS INTENT / ARM | 函数已成功或 owner 已 READY |
| Node Complete / GCS terminal | 当前 bytes 可用、owner READY 或所有 cleanup 已结束 |
| graph COMMITTED | owner 已接管结果 |
| owner 已提交成功 | 当前所有 bytes 仍可读取 |
| 回复/source 托管已退休 | 逻辑对象、contained hold 或实体副本已经 GC |
| RPC timeout | 对端死亡、操作没发生或清理成功 |

对照固定 B 的[Node 发布](https://github.com/xinkuleee/mini-ray/blob/0a340b792c89667e493f7e7313935e45e29071bf/src/miniray/output_publication_node.py)、[owner 交接](https://github.com/xinkuleee/mini-ray/blob/0a340b792c89667e493f7e7313935e45e29071bf/src/miniray/output_handoff.py)与[Core](https://github.com/xinkuleee/mini-ray/blob/0a340b792c89667e493f7e7313935e45e29071bf/src/miniray/core.py)：B 同样保留清单、child holds、Complete/adoption、未知效果补偿与退休，但没有普通结果 GCS 事务或全局图。两版 GCS 都保留成员/死亡事实、owner-wide Node fence、Actor 和 PG 职责；fence 不是实际清理完成收据。

以上固定 B 对象已可由本地 Git 取回；教学分支尚未发布，远端固定链接未在线核验。在同一仓库可直接查看：

```bash
git show 0a340b792c89667e493f7e7313935e45e29071bf:src/miniray/output_publication_node.py
```

## 对象和恢复语义的明确边界

- Task 每次只产一个 ObjectRef；tuple/list 可以是一个完整返回值。无独立多返回槽、targeted reconstruction、健康 sibling 隔离。
- Task 顶层 Ref 参数自动参与依赖；nested Ref 保持句柄；直接返回 child Ref 是引用数据，不自动 get。
- 显式 put 支持普通值和有效含 Ref 的值。含 Ref put 进入同一图准入/提交/释放子协议，绑定独立 put operation 与 owner 安装事实，没有 Task lease、ARM、Complete 或 producer lineage。
- 存活 Node/owner 的准确 Complete，以及 E 的准确 GCS terminal，可提供已发生成功的知识；仅 INTENT/ARM 不足以证明成功。已知成功而所有可用 bytes 确实丢失时仍为 LOST，不生成 READY。
- Node 死亡且 owner 尚未接管时，按完整身份查询适用的准确历史；没有存活准确成功事实才按 UNKNOWN、原清理与有限系统重试边界推进，不能说从未执行。旧成功记录也不能推翻已经锁定的撤销选择或授权新 attempt。
- owner 死亡明确失败，副本存在不授权 owner 接管；[Ray 对象故障语义](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/doc/source/ray-core/fault_tolerance/objects.rst) 是对照，不是兼容性认证。
- 普通 Python 容器环是序列化问题。E 对受支持 contained-edge 入口联合检查已提交边与并发预留，拒绝成环候选；这不是 tracing GC，不承诺回收任意手造引用图，也不增加不支持的 Ref 入口。B 没有这项全局门禁。

## 工程简化不是生产能力

| mini 的界限 | 不可从实验推导的结论 |
|---|---|
| 单机 loopback，1–2 逻辑 Node，受管进程 fail-stop | 多机部署、网络分区容错、完整 failure detector |
| 单 Python/job，固定 1–2 ordinary Worker slots/Node，每 attempt 一个 lease | 动态 Worker pool、runtime env、多语言、跨任务 lease pipeline |
| Python/cloudpickle、TCP request/reply、bytes store | gRPC/IPC 性能、共享内存/零拷贝、spilling/RDMA、生产 memory pressure |
| 确定性小型 Hybrid/locality；两种硬 PG 策略 | 完整公平性、抢占、标签策略、生产 PG 优化或重排 |
| 串行 Actor，同 Node restart；引用参数/结果内 Ref 不支持 | 并发组、named/detached、跨 Node migration、状态恢复或透明方法重放 |
| 单 GCS、内存收据、owner 不接管 | GCS HA、持久化共识、跨 incarnation 恢复 |

## 本版两项自定义保证的收益与代价

普通发布事务增加一个存活的准确发布事实来源；全局图对受支持引用边提供联合 PREPARED/COMMITTED 判环。代价是普通发布新增同步 GCS 依赖，以及预留、补偿、死亡清扫、fencing 和收据退休责任。单 GCS 内存权威不提供 HA、持久恢复或 owner 接管。Ray 的 ReferenceCounter 不等于这个 mini 自定义全局图，增强版也不表示更接近生产 Ray。

R2.2 保存了公共二对象 whole 重建成环候选的拒绝、真实并发四对象联合预留判环，以及 Task/含 Ref put/whole 引用生命周期的有限证据；具体选择器、快照和旧形状处置见[增强验收包](../artifacts/cleanup-enhanced/final/acceptance.json)及[七组合同映射](../artifacts/cleanup-enhanced/final/k9-enhanced-contract-map.md)。真实 GCS handler、纯 reducer、synthetic trace matcher 和有界进程各证明自己的范围，不互相冒充。后继修改的验证进度见[状态页](current-status.md)。

这些有限证据不外推所有引用图或故障组合；可达性仍以受支持公共操作为准，不通过修改已经 put 的 immutable bytes、手造 ObjectRef 或伪 borrower 补造。

[两阶段合同 §10](redesign-plan.md)保留能力与故障边界；R2.3 修复以[唯一主计划 §11](project-cleanup-plan.md)为准。每条分支只有本版运行时，B 持续作为首次学习入口。
