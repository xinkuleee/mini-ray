# P4 Owner Node-loss work：具体record小切片

状态：**隔离试做，未采纳；需要后续hunk重基和本版有界验收。** 输入是 `audit/p1-evaluation`，对应root指定 `execution/base-p1trial-02`；02是执行快照名，不是目录后缀。[输入身份](C:/Users/t-hdong/Desktop/gao/audit/p4-owner-work-trial/inputs.json)与[完整机器结果](C:/Users/t-hdong/Desktop/gao/audit/p4-owner-work-trial/assessment.json)保留实际SHA。未修改base/P1、put候选、manifest或Git提交；未运行pytest/collection。

## 实际改变

只将Core `_output_node_cleanup[publication_id]` 的四字段自由字典改为 `_OutputNodeCleanupWork`：

```python
manifest: OutputPublicationManifest
complete: OutputPublicationCompleteWitness | None
keep: bool
acks: dict[ReleaseContainedReference, ReleaseContainedReferenceReply | WorkerDeathRecord]
```

acks按每个record独立default_factory初始化；complete/keep仍只在原第一次锁定决策时赋值。方法内字符串索引改属性读取，未增加新的重建、GC、owner成功权威；准确收据继续交原`NodeLostOutputResolution`验证。该record是本地内部状态，类型标注不冒充wire校验。

Core唯一业务改动位于`_drive_output_node_loss_once`，另加class/import；两个测试只改原私有字段观察方式。原方法116物理行不变，Core总物理行12756→12767（**+11**）；AST node数1154→1141只因属性语法较短，不代表少13个状态/分配。没有删除任何台账、减少RPC或移动整个Core。[候选hunk](C:/Users/t-hdong/Desktop/gao/audit/p4-owner-work-trial/candidate.patch)不能整文件覆盖后续P3/Core。

`_output_loss_choices`历史、`_output_loss_drivers`单驱动ticket、`_output_result_custody`bytes托管及owner/recovery原子提交均未改；不把它们机械合成另一个“领域权威”。complete为None的UNKNOWN也不因后来envelope到达改值。

## 有限实际方法试验

[exercise.py](C:/Users/t-hdong/Desktop/gao/audit/p4-owner-work-trial/exercise.py)分别在input/candidate的独立30秒脚本进程中执行，五场景两版均退出0。脚本运行真实Core注册、handoff、owner、member-death消费、retry与cleanup reducer；不导入pytest测试体。每场景一个accepted Task、零或一个child、最多一次retry、八次以内同步callback；runtime构造、真实线程/socket/process/timer/wait均被guard禁止。

1. ordinary准确Complete但无存活bytes：LOST，零retry/child释放。
2. contained UNKNOWN：child实际Release后丢ACK，继续保存同请求、complete=None和DISCARD；晚envelope不翻转为成功，只消费一次retry预算。
3. contained envelope在决策前已到：KEEP真实INLINE bytes与live child holds。
4. CF-001：经真实`_sync_worker_deaths`消费完整`WorkerDeathRecord`，不向已死child发RPC，准确义务收敛。
5. 无已安装死亡事实：Release超时不推断死亡，保持owner原状态和pending义务。

结果：[input](C:/Users/t-hdong/Desktop/gao/audit/p4-owner-work-trial/input-results-01.txt)、[candidate](C:/Users/t-hdong/Desktop/gao/audit/p4-owner-work-trial/candidate-results-01.txt)。Complete/death/envelope为显式边界输入，不称真实OS死亡、用户Task执行、物理GC或进程smoke。关闭仅按原纯fixture释放句柄，不手填终态或伪造cluster clean。

已有保留selector的意义与后续验证入口：

- `test_common_cleanup_progress.py`：CF-001完整死亡proof、冲突/别名、缺证明、UNKNOWN晚envelope预算，以及CF-002已死child退休。
- `test_core_output_node_loss.py`：known/unknown丢ACK、旧延迟work/输入hold、KEEP及latched DROP。
- `test_output_node_loss_control.py`：畸形child ACK不能退义务，真实Release墓碑后精确重放。
- `test_node_lost_output_resolution.py`：完整manifest/child收据、surviving replica、旧epoch不污染。
- `test_publisher_node_loss_handoff_path.py::test_completed_output_publisher_node_loss_keeps_success_receipt_but_result_lost` 及 `::test_promoted_output_publisher_node_loss_cleans_live_child_then_reports_unknown`：原真实publisher窗口，留root现有runner执行，不以脚本替代。

本次CF-002的退休算法没有变化、没有重新执行该selector；不能把CF-001与UNKNOWN脚本通过称全部CF闭合。CF-003 latched decision通过第二/第三场景实际观察。

## GC typed检查

`_ObjectGcObligation`已有六个明确类型字段：plan、pending_drops、pending_edges、output_plan、retry_scheduled、retry_round。其AST与输入**完全相同**；不存在本切片需要新增record的自由嵌套字典字段。pending_drops和pending_edges分别表达多个物理位置/child边，不因单输出而删成标量。

原GC唯一`_object_gc_obligations`仍由Core生命周期推进，成员死亡只解除对应Node Drop，child Release与最终owner/recovery忘记仍走现有边界。未造第二GC权威；其它Core marker/put字典属于另外已分配候选，不在本次扩范围。

## 初步收益判断

收益是具体字段类型可读性和去字符串索引，代价是+11源码行及两个私有观察测试适配；**没有可报告的状态数量、RPC或性能降低**。因此不能仅凭dataclass写成“P4已显著简化”。候选保留给root按P3后的实际hunk与原selector验收：若类型明确性不足以抵消接口维护成本，可记录本真实试做及局限后选择保留原表示。当前尚未作最终采用/回退决定。
