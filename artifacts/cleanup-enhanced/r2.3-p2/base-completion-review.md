# R23-01 B 定向修复与 R23-03/04 正式文档独立复核

结论：**未发现需返工的合同回退；B 的 R23-01 在所定有限范围内具备关闭证据，E 的两项文档修复已按冻结候选实际落地。** 本报告仅复读源码差异、已保存日志和hash；没有运行pytest、collection或项目测试导入，没有修改正式仓库。E共同测试与GCS ACK评估仍归各自任务，本文不宣称全部R2.3完成。

## B：修复保留不变量

审查正式B HEAD仍为 ab4cfb317fd786a286359d8f4e971ff195739375，当前是明确登记dirty输入的修复候选。差异仅涉及目标测试、reviewed migration登记与进度/计划文档；src、共享helper、示例、runner、pyproject及lock未改。pure/smoke/known_non_unit gate列表与HEAD一致，新测试仅按R23-01、unit登记迁移项。

- 原7函数/14参数实例变为8函数/15实例：6个保留同名函数的decorator AST全部相同；唯一旧STORED/INLINE混合函数被两个独立storage tier函数替代，没有删任何原fault参数。
- `test_owner_finalize_replica_receipts.py:135`起分别观察真实MATERIALIZE intent、无ACK/结果/Complete，与claim、实际分配、partial/sealed bytes、metadata、资源及cleanup义务。没有把原True机械替换为False后省掉真实清理断言。
- `184`行的受控Worker边界核完整endpoint/request、锁外调用、已ABANDONED/归还CPU、仍ACTIVE、物理收据先于Worker确认。typed回调仍明确为纯组合外部边界，未冒充真实Worker进程。
- `343`行独立INLINE通过实际Prepare建立handoff和MATERIALIZE ACK；完整payload在首Worker ACK未知前后保留，准确重放后退休，lease.release委托实际ledger且严格只调用一次。全过程guard禁止实体写入/删除，允许Generic Drop必要只读检查；最终Drop按准确身份拒绝且无物理收据。
- `409/437/476/500/523`行分别保留abort/delete False或异常、manager效果前/后未知、物理收据完成后Worker ACK未知及禁止重复storage操作、实体效果后异常、损坏bytes/错误claim。before/after效果、结果托管、实体删除、Worker ACK和journal退休没有合并为一个clean布尔值。
- `528`行的after02修正先deepcopy有效预期，再在互相独立的预期与真实claim effect上置相同非法transfer_index，使Node实际看到原畸形claim。仍检查完整claim表相等、store快照不变、无Worker调用及无Drop收据；没有绕过运行时边界校验。

## B：已保存结果及身份

证据根：`audit/two-version-cleanup/execution/`。

| 输入 | 保存结果 | 判断 |
|---|---|---|
| r23-base-before-01 | 14 failed，首因真实snapshot.result_retained=False与旧True断言冲突 | 原源码/测试加新受审registry的预修候选，不改标原clean HEAD实测 |
| r23-base-after-01 | 14 passed / 1 failed；畸形claim deepcopy在Node调用前被wire重建校验拒绝 | 原日志保留；是观察准备错误，不称runtime清理失败，也未覆盖成最终通过 |
| r23-base-after-02 | 目标文件15 passed（1.32s）；邻接Node文件10 passed（0.77s）；实际owner-death进程1 passed（7.29s） | 三条同候选运行exit0，外层记录4.692/3.686/10.179s，未提高原界限 |

after02 archive SHA256为 `98fa2fbf0eb921b87b13b9a335d7247d2a6f7ee69dab8080d1e2f8e1690ba5f3`，identity明确working-tree candidate及6处dirty路径。snapshot中402项执行输入（src/tests/scripts/examples/.github及配置）逐一raw SHA与当前B匹配，未发现差异。目标测试raw SHA为 `d3051956feb52ea44d954cafc4f8e80560eb57b4d5b5d90237abf134917d948b`；manifest为 `6882d7dfe77aee95e92705329a71349700af65aed93b958af6344984b41d44db`。

after02采用Python3.12.13和原锁定依赖。15/10/1为已保存的有限定向证据，不是本代理新运行；不能加进历史343或称全部旧gate在新HEAD重跑。原after01候选目录及其source-review仍明确冻结于失败输入，没有将旧patch改标为after02。

## E：R23-03/04 已应用文档

正式E以下三个文件raw SHA均精确等于 `docs-candidate/manifest.json` 的candidate_sha256：

| 文件 | SHA256 |
|---|---|
| docs/production-ray-mapping.md | c79aa81fb08168bab0c392e2717d58f2ab2a26c482d24df94e444ebe8f04c27d |
| docs/testing.md | 599c896d7725f0141adbdde72a0eef42c28362e09e280775930cbdd8e3ba73ed |
| docs/design.md | d57f31511042cb6eb109a5c6581778cda3118293cf7a7fc16527021504bfae03 |

因此此前104相对链接目标和27固定Git对象的静态检查适用于这三份正式内容。mapping当前E机制/B固定0a340b7对照、教材与证据ab4cfb3，以及Ray固定c3162dc均未混版；testing明确R2.2的392候选与3f5b725的405/1、37/37，R2.3进行中；design只更正首段身份，未预写StageAck优化已采纳。当前status顶部已将R2.2通过结果与R2.3后继修复分开。

无新增测试或矩阵要求，未把文档静态检查称为运行时通过。root最终更新每项状态、独立证据归档和交付身份即可；本报告不要求额外重复这些已完成定向运行。
