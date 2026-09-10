# 基础版学习路径

首次学习推荐从基础分支teaching-base开始：先跟一次真实Task，再看引用和故障。本文链接已完成K4–K6整理的B源码；K7最终同版验收仍在进行，复现实验前先看[状态页](current-status.md)，不要把分批结果当成最终HEAD证明。[增强版合同](redesign-plan.md)的§10属于后续阅读；E尚未创建，不能用于解释B的普通结果trace。

## 第一遍：提交、执行、交接

先运行[README的example01精确smoke](../README.md)，读[01_task_path.py](../examples/01_task_path.py)。记录哪个PID执行函数、谁持有ObjectRef，以及为什么TaskID/ObjectID不会随一次重执行变化。

| 阅读顺序 | 当前源码入口 | 要回答的问题 |
|---|---|---|
| API与同步准备 | [api.py](../src/miniray/api.py)的RemoteFunction.remote；[core.py](../src/miniray/core.py)的CoreWorker._register_submission | remote返回前准备哪些参数、身份与引用责任，哪些工作仍异步？ |
| 单输出身份与值 | [task_outputs.py](../src/miniray/task_outputs.py)的TaskExecution；[output_discovery.py](../src/miniray/output_discovery.py)的PreparedOutput | 为什么tuple/list是一个value，payload只序列化一次，成功Envelope只有一个result？ |
| 等待与投递 | CoreWorker._prepare_task_dependencies、_execute；[dependency.py](../src/miniray/dependency.py)、[lease_dependencies.py](../src/miniray/lease_dependencies.py) | 为什么依赖pending时不能占住执行Worker？ |
| 最终资源准入 | [node.py](../src/miniray/node.py)的_handle_request_lease、_handle_request_lease_serialized；[resources.py](../src/miniray/resources.py) | 调度建议与Node实际资源分配有什么区别？ |
| 执行 | [worker.py](../src/miniray/worker.py)的_handle_push_task、_begin_task | lease/attempt如何绑定同一次执行，歧义回复为何不能当失败重来？ |
| 结果交接 | [output_publication_node.py](../src/miniray/output_publication_node.py)的prepare/complete/report_terminal；[output_handoff.py](../src/miniray/output_handoff.py) | Node Complete、owner READY和回复托管退休为何是不同事实？ |
| owner可见性 | CoreWorker._drive_output_publication_adoption；[ownership.py](../src/miniray/ownership.py)的ObjectOwnerTable | 身份、checksum、当前attempt与已有收据如何决定是否可提交？ |

B仍有GCS：[control.py](../src/miniray/control.py)的GCSLite、NodeRegistry、WorkerRegistry负责注册/成员和死亡事实，Actor/PG有各自协调器。普通Task结果没有GCS发布阶段或图门禁。

## 七条示例主线

全部示例保留原始main。使用 testing.md 中的同一smoke命令，将参数 example01 替换为对应编号，逐个运行。

| 示例 | 观察重点 | 下一份源码 |
|---|---|---|
| [01 Task路径](../examples/01_task_path.py) | TaskID/ObjectID、真实Worker、普通结果因果trace | api/core/node/worker、output_handoff |
| [02 spillback](../examples/02_spillback_direct_submission.py) | 自定义资源使首选Node无法执行，投递仍Core→Worker | [lease_policy.py](../src/miniray/lease_policy.py)、Node lease处理 |
| [03 跨Node pull](../examples/03_cross_node_object_pull.py) | metadata位置、sealed bytes、pin和分块传输 | [object_manager.py](../src/miniray/object_manager.py)、[object_store.py](../src/miniray/object_store.py) |
| [04 Actor](../examples/04_actor_control_direct.py) | GCS创建、直接方法调用、串行执行 | [actor_client.py](../src/miniray/actor_client.py)、control中的Actor协调器 |
| [05 nested get](../examples/05_nested_get_cpu_yield.py) | 等待子任务时释放CPU与恢复责任 | [blocking.py](../src/miniray/blocking.py)的BlockingNotifier、[resources.py](../src/miniray/resources.py)的ResourceLedger |
| [06 lineage](../examples/06_lineage_reconstruction.py) | 删除物理副本后，同ObjectID的新attempt | [reconstruction_runtime.py](../src/miniray/reconstruction_runtime.py)、[owner_reconstruction.py](../src/miniray/owner_reconstruction.py) |
| [07 PG](../examples/07_placement_group.py) | 两bundle约束、全ACK前不可见、资源归还 | [placement_group_runtime.py](../src/miniray/placement_group_runtime.py)、control/Node PG入口 |

## 第二遍：引用、bytes与回收

读[设计中的引用与GC](design.md)后，沿[ref_transfer.py](../src/miniray/ref_transfer.py)、[publication_sources.py](../src/miniray/publication_sources.py)、[transfer_pins.py](../src/miniray/transfer_pins.py)追踪source保活到实际交接。child owner的incoming hold与outer owner的outgoing关系不是同一个计数；独立borrower可以活过outer。

接着读CoreWorker._put_value、[put_handoff.py](../src/miniray/put_handoff.py)与[put_work.py](../src/miniray/put_work.py)的具体待办记录。put不产生Worker lease或Task lineage；含Ref put仍需要真实child交接。比较顶层Ref参数和容器内Ref的含义，理解为什么删除自动大参数lift不等于删除nested ref。

最后对照[replica_cleanup.py](../src/miniray/replica_cleanup.py)、ObjectOwnerTable的collection/retirement和Node的owned drop入口。引用close、metadata删除、lineage释放和bytes回收分别观察；quarantine或诊断登记不能授权删除未知副本。

## 第三遍：历史事实与故障

从[基础账本B04–B07](acceptance-baseline.md)选择一个已有有限场景，先写出owner、Node、child各自掌握什么事实，再读CoreWorker._drive_output_node_loss_once与owner reconstruction handle。

对照[output_protocol.py](../src/miniray/output_protocol.py)中的窄Complete ACK与完整history query，再看Core的Node-loss/retirement work记录。重点区分准确Complete、UNKNOWN、已知成功但bytes丢失的LOST；第一次准入与旧收据重放；未知RPC与未发生效果；已安装死亡事实与单纯timeout。旧epoch不能覆盖新执行，完整死亡proof只能解除对应死亡参与方的责任。

需要实际测试时只用[测试指南](testing.md)中的有限入口。旧tests目录、[历史索引](history-index.md)和原计划里的旧函数名都不是当前整树执行许可；当前API不因旧夹具失败而恢复。
