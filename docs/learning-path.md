# mini-ray 两阶段学习路径

日期：2026-09-09。**基础版已独立通过约定验收；本地固定标记为`teaching-base-v0.1`，增强标记`teaching-enhanced-v0.2`也已通过约定同版验收**。
固定同版证据及范围以 [验收账本](acceptance-baseline.md) 为准；本页是源码导读，不把旧 checkpoint 当作当前版本通过。
七个示例和引用实验使用同一个真实后端，没有另一套演示运行时。下面前三遍按基础标记阅读，末节再回到主线研究增强协议；相对源码链接跟随当前检出版本。

## 先读清楚四个职责

| 组件 | 权威与职责 |
|---|---|
| CoreWorker / object owner | Task/ObjectID、依赖与直接提交；结果可见性、引用理由、lineage、待交接清单和精确收据 |
| Node | 本地资源与 Worker lease、对象副本、pull、准确 Complete 和回复托管；不决定逻辑引用是否存活 |
| Worker / ActorWorker | 执行用户函数或串行方法；保留返回值与源句柄直到真实交接或补偿完成 |
| GCS-lite | 成员、死亡事实及 owner-wide Node fence、Actor 创建与 PG 协调；不保存普通结果发布清单或全局引用图 |

普通结果使用 owner-led 交接。函数返回、Node Complete、owner READY、bytes 仍可用、回复托管退休、对象 GC 是不同事实。
Node 释放 lease 不等于 owner 已接受结果；owner 已知成功也不能凭失效 descriptor 制造字节。

## 怎样运行

先按 [验收账本的环境和精确入口](acceptance-baseline.md) 准备 Linux/WSL 与 Python 3.12。
`scripts/run_baseline.py` 读取一份显式基础清单，复用已有 30 秒进程树边界。
清单已纳入的真实进程实验逐个执行；新增 exact 先落实资源和清理边界，不并行 pytest、不跑整个目录。
先`git switch --detach teaching-base-v0.1`；例如列出选择器、运行固定纯合同批次和第一条主线：


```bash
python scripts/run_baseline.py --list
python scripts/run_baseline.py --pure
python scripts/run_baseline.py --smoke 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

把完整 selector 的参数分别换成 example02 至 example07 可追踪其余主线。
runner 超时表示实验失败，强制终止不证明 clean shutdown；get/close 的 timeout 也不自动取消分布式操作。
当前历史 reviewed-pure manifest 含退役协议，不能作为基础版推荐集合。
清单中的候选、实际已通过证据和仍缺的保留行为分开记录在验收账本，列入清单不代表封版。

## 七条主线

### 1. Task、ObjectRef 与执行身份

入口：[01_task_path.py](../examples/01_task_path.py)。一次 remote 返回一个 ObjectRef；tuple/list 是其完整值，不拆成多个返回槽。
调用 remote 在返回前完成参数序列化及持有准备，但不等待用户函数执行；get 读取结果，wait 只观察状态。
源码顺序：[api.py](../src/miniray/api.py) → [core.py](../src/miniray/core.py) 的 submit → [ids.py](../src/miniray/ids.py) / [task_outputs.py](../src/miniray/task_outputs.py) → [worker.py](../src/miniray/worker.py)。
观察稳定 TaskID/ObjectID 与不同执行 AttemptID；成功 trace 使用 [当前黄金合同](../src/miniray/golden_traces/ordinary_task_success.json)，在基础标记中不经过GCS INTENT/ARM/terminal/adopted；增强版真实路径包含这些事件，不能归一化抹除。

### 2. Lease、spillback 与直接提交

入口：[02_spillback_direct_submission.py](../examples/02_spillback_direct_submission.py)。自定义资源要求任务前往另一 Node；lease 回复提供 Worker endpoint，提交 Core 直接 PushTask。
读 [resources.py](../src/miniray/resources.py) 的 HybridPolicy、[node.py](../src/miniray/node.py) 的 lease handler，再回 Core 的执行推进。
区分 total feasible 与 available；集群摘要只提供提示，Node 最新账本才准许分配。
可选读 [lease_policy.py](../src/miniray/lease_policy.py)：已有对象位置影响第一次向谁请求 lease，不取代 Node 的资源裁决。

### 3. Store、对象位置与跨 Node pull

入口：[03_cross_node_object_pull.py](../examples/03_cross_node_object_pull.py)。较大结果保存在 Node，依赖消费者通过描述符定位和拉取字节。
读 [object_store.py](../src/miniray/object_store.py)、[object_manager.py](../src/miniray/object_manager.py)、[transfer_pins.py](../src/miniray/transfer_pins.py)。
source pin 保护传输来源，完整校验并 seal 后目标才可见；lease/Push/GCS 控制消息不运送该大对象载荷。
这是 Python bytes store，不是共享内存、Plasma、零拷贝或 spilling。

### 4. Actor 控制路径与方法直达

入口：[04_actor_control_direct.py](../examples/04_actor_control_direct.py)。创建由 GCS 协调，方法调用直达专属 ActorWorker。
读 [control.py](../src/miniray/control.py) 的 ActorCoordinator、[actor_client.py](../src/miniray/actor_client.py)、[actor_state.py](../src/miniray/actor_state.py)、[actor_worker.py](../src/miniray/actor_worker.py)。
每 caller FIFO 与同 generation 去重保持串行对象语义；普通方法结果仍是异步 ObjectRef。
高级故障路径保同一存活 Node 内有限重启：ActorID 不变，generation/route 前进，构造器重跑，旧在途方法失败而不透明重放。
Node 丢失则 Actor 终态失败，不跨 Node migration；constructor/method 的参数及结果值内 ObjectRef 明确不支持。

### 5. 动态子任务与 blocking get 的 CPU yield

入口：[05_nested_get_cpu_yield.py](../examples/05_nested_get_cpu_yield.py)。Worker 内嵌 Core 提交子任务，阻塞 get 时临时让出 CPU，让子任务能够运行。
读 [blocking.py](../src/miniray/blocking.py)、Worker 通知与 Node [resources.py](../src/miniray/resources.py) 账本。
只让出 CPU，其他资源与 lease 仍保留；unblock/Complete/Worker death 必须准确、一次清账。
这与容器内的 nested ObjectRef 是两个问题；CPU reacquire 是逻辑账本恢复，不是等待物理 CPU 空闲的调度屏障。

### 6. 单输出 lineage reconstruction

入口：[06_lineage_reconstruction.py](../examples/06_lineage_reconstruction.py)。先取得结果，丢弃其副本，再通过 get 触发按 lineage 重算。
读 [recovery.py](../src/miniray/recovery.py)、[reconstruction_runtime.py](../src/miniray/reconstruction_runtime.py)、[owner_reconstruction.py](../src/miniray/owner_reconstruction.py)。
完整函数重执行，TaskID/ObjectID 保持、AttemptID 变化；应用异常默认终态，系统失败按预算处理。
首次 START/JOIN ACK 依赖真实准入事实，不能用后来 READY/LOST 状态猜历史；精确重放不重复排队或扣预算。
put 没有 producer lineage；owner 死亡不能切换副本接管其逻辑对象。

### 7. Placement Group 的原子预留

入口：[07_placement_group.py](../examples/07_placement_group.py)。两个 bundle 使用 STRICT_SPREAD，全部 commit 后才提供可用映射。
每 Node 两 CPU，因此 STRICT_PACK 本可把两个单 CPU bundle 放在同一 Node；当前分散是硬策略要求，不是容量偶然结果。
读 [placement.py](../src/miniray/placement.py) 的精确小规模 planner 与 child ledger，再读 [placement_group_runtime.py](../src/miniray/placement_group_runtime.py)、GCS/Node 2PC。
本版最多两个 bundle，只支持 STRICT_PACK / STRICT_SPREAD；prepare 失败要准确 abort，participant Node 丢失进入 LOST，不重排。
PG Task 使用已预留 child ledger，不再次扣 root；活跃零资源或 CPU-yielded lease 仍阻止提前释放 root reservation。

## 第二遍：引用是数据，存活理由彼此独立

[两 borrower 活过 outer](../tests/integration/test_contained_ref_lifecycle_path.py) 展示 Worker-owned child 返回 Driver 后 owner 不变。
两次 get(outer) 可获得同一 child 的独立 borrower；关闭 outer 只释放其 contained hold，不能让其他 borrower 一起失效。
[nested 参数保活](../tests/integration/test_nested_task_argument_path.py) 展示 sender 先 close 后已接受 Task 的 hold 仍保护引用。
Task 顶层 Ref 参数进入 readiness gate；容器中的 Ref 仍是句柄，由用户显式 get。直接返回 child Ref 也是引用数据，不自动 get。
读 [dependency.py](../src/miniray/dependency.py)、[ref_transfer.py](../src/miniray/ref_transfer.py)、[ownership.py](../src/miniray/ownership.py)、[owner_service.py](../src/miniray/owner_service.py)。

显式 put 支持普通值和含有效 owned/borrowed Ref 的值；累计过大的按值参数必须先 put，不再自动 lift 成 StoredArg。
读 [put_handoff.py](../src/miniray/put_handoff.py) 的单次 discovery 和 Core.put：put operation 负责完整清单、hold 获取、owner 安装与失败补偿，没有 Worker lease 或可重执行 Task lineage。
[含 Ref put 实验](../tests/integration/test_put_contained_ref_path.py) 追踪 source close、top-level 参数物化、consumer whole reconstruction 与最终释放；其最新验收状态仍查账本。
这个 put 由 Driver 拥有，consumer replay 保留 put 的原 attempt；它不证明 Task 产生 stored outer 后的
foreign retained hold 换代。后一个交界已由[Task nested replay实验](../tests/integration/test_task_contained_reconstruction_path.py)在snapshot03同版复验，包含真实retained换代、导入及释放；固定同版证据见账本，不能因都包含引用而混称。
[stored physical GC](../tests/integration/test_stored_physical_gc_path.py) 分别证明 metadata、source/target bytes 与 lineage 回收；shutdown clean 不替代它。

引用协议只接受完整 typed hold/source；token 字符串是身份字段，不是独立凭证。
普通 Python 容器自环与 ObjectID 间引用环不同；基础版没有全局防环或 tracing GC，不承诺强引用环自动回收。

## 第三遍：交接与故障知识

读 [output_handoff.py](../src/miniray/output_handoff.py)、[output_publication_node.py](../src/miniray/output_publication_node.py) 与 Core 的 adoption/退出入口。
owner 先登记清单；child owner 确认保活、Node 完成物化及 Complete；owner 再原子安装结果与 outgoing edges；真实接管后才退休回复和来源托管。

| 事实 | 能得出的结论 |
|---|---|
| 存活 Node 有准确 Complete | 继续交付原执行，不因回复丢失重跑用户函数 |
| 已知成功但可用 bytes 全失 | 对象为 LOST；有 lineage 且 owner 活时 get 可请求重建，不能伪 READY |
| Node 死、owner 未提交且无存活成功收据 | UNKNOWN；准确收口旧责任后按有限系统重试处理，不推断从未执行 |
| owner 死亡 | 明确失败；存活方清理各自责任，不接管 owner |
| RPC timeout | 保留未决效果或报 unavailable，不能直接推断死亡或 clean |

旧 Complete/adoption/abort/release 收据必须与当前状态分开；迟到消息不能修改新 attempt、重建引用或重复消耗预算。
Worker 在 Complete 后退出且 TaskReply 丢失的实验，证明从 Node 托管取得结果；它不证明 adoption ACK 丢失。
[adoption ACK-loss实验](../tests/integration/test_output_retirement_ack_path.py)在snapshot03复验了owner READY、真实回复退休、精确ACK重放及GC屏障；该一次丢包切片通过，固定同版证据见验收账本。
实际故障切片、纯组合证据与未证明交界只以 [验收账本](acceptance-baseline.md) 为准，不扩成全部交错矩阵。

## 第四遍：回到主线研究增强协议

基础标记`teaching-base-v0.1`已经独立交付。完成前三遍后，在干净工作树执行`git switch --detach teaching-enhanced-v0.2`阅读固定增强实现；
增强版的377项纯合同、37smoke及七个main产物见[增强账本](acceptance-enhanced.md)，基础版历史结果不代替增强版同版证据。

先读[enhanced_publication.py](../src/miniray/enhanced_publication.py)：INTENT/PREPARED/ARM只允许下一步；准确Node Complete形成terminal；
图COMMITTED不等于owner READY；fence与RETIRED分开，历史收据不能复活旧边。再读[control组合](../src/miniray/enhanced_publication_control.py)
和[owner client](../src/miniray/enhanced_publication_client.py)，最后回Node journal及Core实际CAS/GC边界。

普通Task采用强制GCS门禁，put仅复用图协议且无Task Complete/adoption；child holds、bytes、资源和owner可见性仍各归原权威。
W2观察Node退出后GCS是否真的接受terminal：有准确事实时已知成功但bytes缺失仍LOST，没有时UNKNOWN。W3/W4观察图提交、owner CAS、
托管退休和旧epoch清理，不把历史成功等同当前可读取。

[真实图实验](../tests/integration/test_enhanced_cycle_path.py)从公共put/Task/reconstruction到达成环候选：B真实持有A，A重建返回B时被拒绝；
两个Node的A→X与B→Y并发预留与已有X→B、Y→A合并时只能一方获准。导入模块仅保存真实Worker-owned put，
没有新增设置结果API、伪造borrower或用Python list环替代ObjectID环。可达性与清理已获有限真实证据，自环/首次发布互环不作已证明声明。

这是mini自定义保证及同步/补偿成本的教学增量，不称为更接近生产Ray。固定基础版持续作为首次学习入口；同一主线演进，
不在运行时保留长期双后端。完整职责与差异见[Ray映射](production-ray-mapping.md)和[两阶段计划§10](redesign-plan.md)。
