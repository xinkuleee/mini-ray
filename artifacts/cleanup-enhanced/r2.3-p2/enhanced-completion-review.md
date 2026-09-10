# R23-01 E 最终独立只读复核

结论：**未发现需返工的E适配问题；R23-01在E的计划有限范围内具备关闭证据。** 对照已验B after02与当前E，基础测试不变量保持，新增authority阶段/闭包断言没有把Node cleaned冒充全局图退休。本文不判定R23-02 ACK试验完成。未修改正式仓库，未运行pytest、collection或项目测试导入。

## B到E的实际差异

- 两版8个测试函数名与参数decorator AST完全相同，仍为15个参数实例；B的独立STORED/INLINE构造、真实MATERIALIZE观察、物理收据和manager未知、Worker ACK重放、单次资源归还及deepcopy-before-corruption修复全部继承。
- E只增加增强模块导入、一个观察helper及各检查点调用、closed_holds断言和准确的测试范围说明；没有改共享fixture、runtime或新增B/E开关。正式源码、示例、runner、pyproject及lock相对HEAD无diff。
- 当前gate的pure/smoke/known_non_unit列表与HEAD相同；目标文件作为R23-01、unit的reviewed migration登记，未提升gate或改历史405数字。

## 实际authority与Node闭包边界

`test_owner_finalize_replica_receipts.py:62` 的helper通过现有fixture.authority.query(GetPublication)读取准确PublicationSnapshot，不新建记录、不直接赋阶段、不调用RetireGraph。既有test_output_publication_node_server fixture中publication_rpc确实使用PublicationAuthority.apply，Node adapter实际写journal收据；helper逐条比较authority与journal的相同reference/stage receipt。

| 实际测试点 | E断言与职责 |
|---|---|
| STORED物化前中断 | INTENT/PREPARED已存在，prepared=None、无ARMED/Complete；Node只有物化intent，没有结果ACK；owner-wide Node fence本身不伪造GCS fence |
| INLINE实际Prepare成功 | INTENT/PREPARED/ARMED；TaskPreparedReceipt与ACTIVE journal的真实preparation相等，仍无Complete |
| Finalize开始与首Worker ACK未知 | 准确owner死亡触发唯一实际FencePublication；authority FENCED且forward关闭；Node仍ACTIVE，adapter闭包为None |
| 准确Worker ACK后Node退休 | :350只接受准确返回的ClosedContainedHolds(reference, (), ())与adapter一致；随后仍核authority graph_active=True、closed_holds=None、无Complete/adoption/RETIRED receipt |

上述行为与现有output_publication_node.finish_owner_death相符：先记录fence、释放具体child责任，cleanup回调完成才退休Node journal；owner_death_closed_holds在本地finished后返回证明，后续RetireGraph仍是调用方职责。helper仅在ACTIVE时读取journal preparation，退休后核历史authority，未通过重做Prepare复活前进权限。空child证明只证明此fixture的空集合，不冒充含child全局清理。

受控typed Worker ACK仍是纯组合的外部边界，未宣称真实Worker进程；真实owner死亡接线由下面独立进程selector承担。测试特意不把Node成功回包写回authority来制造“全局clean”。

## 同候选运行与身份复核

证据：audit/two-version-cleanup/execution/r23-enhanced-after-01。HEAD仍为fd40a407a46ada3d4941c034a73355f45355b270；identity明确11处dirty路径及working-tree candidate，archive SHA256为 `26a54d5549fd61ddbd0e4dc29816f38c18b8f17baf013335312262fcb42f2669`。

| 精确执行范围 | 原日志 | 保存结果 / 外层耗时 |
|---|---|---|
| tests/unit/test_owner_finalize_replica_receipts.py | 000-case.log | 15 passed，pytest1.89s；外层5.001s，exit0 |
| tests/unit/test_output_owner_death_node.py | 001-case.log | 10 passed，pytest1.38s；外层5.255s，exit0 |
| tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds | 002-case.log | 1 passed，pytest10.81s；外层14.879s，exit0 |

snapshot中的412项执行输入逐一raw SHA匹配当前E。目标测试raw SHA为 `ace08a31f85d5d5a33708e32142de9c106fb1daebab57d077f58b318d3332114`；manifest为 `48fba07fb74f7f91cdd619a0a7d88701e7d72ab1d481fe103114d480b13ba9d2`。E预修r23-enhanced-before-01的14失败日志仍保留，其原result_retained断言错误未被后续通过覆盖。

这些结果属于准确列明的E dirty候选，不属于B，也不改标成旧clean HEAD重新测试。有限15/10/1不能相加进历史405或声称全部当前gate重跑；当前范围不需要因本次只读审查再追加运行。
