# P4 retirement work具体record试做

状态：隔离候选，未采纳。输入Core/ownership来自冻结 `audit/p3-integration-candidate/candidate`，其余不变依赖来自 `audit/p1-evaluation`。准确SHA见[inputs.json](C:/Users/t-hdong/Desktop/gao/audit/p4-retirement-work-trial/inputs.json)。未修改base、P1/P3、manifest或提交，未运行pytest/collection。

## 修改范围与责任

`Core._output_retirement_work[ObjectID]` 的自由字典仅改为本地 `_OutputRetirementWork`：

```python
plan: OutputOwnerPublicationRetirementPlan
child: dict[ReleaseContainedReference, ReleaseContainedReferenceReply | WorkerDeathRecord]
replica: dict[DropObjectReplica, DropObjectReplicaReply | NodeDeathRecord]
```

两个receipt map各自default_factory初始化，按原请求key准确保存。plan仍由owner `begin_output_publication_retirement`产生；Core只持待RPC完成进度，owner P3的单一plan/receipt map仍是退休权威。child/replica多值不标量化，不换成bool或预期效果，不新增retirement service。

实际生产消费者仍为原 `_retire_lost_output_memberships`；只改构造和属性访问。单驱动ticket、late replica barrier、锁外RPC、同锁owner提交、shutdown待办、完整死亡proof查验都不变。ownership.py逐字相同；没有改owner validate/commit重复问题，也未混入P5。put/node-loss两个另外候选没有合并进此Core。

Core总行12756→12767（+11），方法保持93行，状态表减少0、新权威0、RPC变化0；另一个原测试只把`work['plan']/['replica']`观察改属性。收益是具体类型/字段阅读，不是性能或状态数量下降。[候选patch](C:/Users/t-hdong/Desktop/gao/audit/p4-retirement-work-trial/candidate.patch)须按hunk合入，不能整文件覆盖其他Core进展。

## 实际有限方法试验

[exercise.py](C:/Users/t-hdong/Desktop/gao/audit/p4-retirement-work-trial/exercise.py)在input/candidate分别以30秒受界脚本执行，三场景全部退出0；没有调用pytest或导入测试体。每场景一个accepted Task、一个8KiB Node Store、零或一个child；真实Node Start/Prepare/Complete、Core owner publication/finish及debug Drop先建立实际published LOST对象，然后调用Core真正的retirement方法。

| 场景 | 真实观察 |
|---|---|
| ordinary STORED | bytes已由真实Node Drop删除，退休Drop第一次真实ALREADY_DROPPED回包被丢弃；同plan/current record保留，第二次同request完成，不改引用/lineage/预算 |
| contained STORED | child真正Release后保留精确reply，不因后续Drop ACK未知重复释放；第二轮复用child receipt和原Droprequest完成P3 owner退休 |
| contained + installed death | 经Core真实member consumer安装完整WorkerDeathRecord；child RPC禁止，记录完整death receipt；Drop丢ACK后仍以同proof/map完成，owner校验与P3终态receipt保留 |

三场景均核冻结retirement_id/member、缺失replica ACK时owner只持退休claim、错误不改lineage/当前attempt、ticket释放、成功后Core工作表移除、owner单map保存完整retirement receipt、重复调用无新增RPC。结果见[input-results](C:/Users/t-hdong/Desktop/gao/audit/p4-retirement-work-trial/input-results-01.txt)和[candidate-results](C:/Users/t-hdong/Desktop/gao/audit/p4-retirement-work-trial/candidate-results-01.txt)。

这不是OS死亡/进程smoke；member death是明确输入事实，transport为同步回调。物理bytes的seal/drop确实执行，关闭只释放原纯fixture句柄，不冒称完整GC或cluster clean。type annotation不代替untrusted receipt校验，原owner validator保持原样。

## 保留测试与P4整体处置

待root按当前本版runner顺序验收：`test_targeted_owner_defer.py::test_owner_retirement_drop_ack_loss_defers_then_exact_request_starts`（名称历史保留，实际是单输出）；`test_output_owner_retirement.py`原精确proof/错epoch/别名反例；`test_owner_retirement.py`完整child死亡proof；真实Core reconstruction concurrency仅运行已审exact。候选修改的两文件已AST/内存compile，未跑上述测试。

P4整体不能因本slice通过标完成：put、node-loss work、retirement work分别已有候选，GC已有typed `_ObjectGcObligation`无需造第二权威；root仍需按hunk串行集成、实际保留合同验收，并逐项记录采用或有据保留现状。本候选+11行可能值得字段可读性成本，但没有证据支持“显著精简运行时”结论。
