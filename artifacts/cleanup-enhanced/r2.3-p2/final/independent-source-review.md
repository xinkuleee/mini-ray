# R23-02 StageAck 候选独立源码复核

结论：**在本次7文件有限静态审查中，未确认需要返工的丢字段、别名隔离、receipt/forward或首fence/迟到历史错误。** 这不是候选已通过运行或已有净收益的结论；采纳仍取决于root的原定合同运行和同输入成本对照。未运行测试、collection、项目模块或DTO构造，未修改候选、基线或正式仓库。

## 精确审查输入

基线为R23-01后E archive `26a54d5549fd61ddbd0e4dc29816f38c18b8f17baf013335312262fcb42f2669`。candidate-src.patch SHA256为 `1da561e41b36764f29bb6de23794f4525f199665bfa35c0c56f4ed29664c8574`，与candidate-src-identity.json一致。

逐一复算7个基线/候选文件hash均匹配身份记录；全src差异也恰为这7文件：control、enhanced_publication、enhanced_publication_client、enhanced_publication_control、node、output_publication_journal、output_publication_node。7文件均AST解析成功；没有Core、publication_gate或公共output_protocol源码变化。

## 已核关键路径

| 范围 | 本次具体检查结果 |
|---|---|
| StageAck表示与隔离 | enhanced_publication.py:613起仅新增成功mutation返回值；完整request echo、reference、owner、具体stage receipt、typed accepted_fact及closing facts均通过_set/_copy重建。新类自动纳入_WIRE_TYPES，replace/unpickle重入构造；未发现返回值直接别名指向authority保存对象。query/ABSENT和typed rejection继续使用原PublicationReply。 |
| 必要事实 | Begin/Prepare为准确reference；ARM为原TaskPreparedReceipt；terminal及Task commit为Complete；put commit为PutPreparedReceipt；adoption为原owner proof；fence为准确已接受first fence；retire为原ClosedContainedHolds。恢复所需全history仍由未缩减的GetPublication返回，没有迁入第二个Core状态表。 |
| authority提交顺序 | _apply_locked原状态校验、_validate_prepared、_validate_closed及完整updated snapshot构造不变；:1071先构造/校验StageAck，:1072才写_records，随后更新sequence。正常首次transition没有新增“已提交却因回包构造失败”的窗口。 |
| forward与旧历史 | :704保留Begin/Prepare/ARM关闭后拒绝，与原_require_forward一致；首次commit仍需open，已有commit可精确重放但其receipt须早于fence。terminal/adoption没有被新增open条件错误拦截，仍可在合法fence/retire后补记；旧回包forward不替代Core既有RPC后本地epoch/abort检查。 |
| 首fence例外 | 普通authority仍拒绝first fence换proof。controller:70仅在现有完整死亡登记验证后使用accepted_existing_fence=True；request echo仍是后到death，accepted_fact/fence/fence_receipt仍是实际first fence，不谎称后到death首次提交。先安排死亡cleanup再构造返回值是原controller已有顺序，未由该候选新增。 |
| Node/journal | journal:322强制mutation StageAck，比较实际manifest owner、reference、request、stage；ARM accepted_fact继续与真实journal preparation相等，ACTIVE/forward与收据重放校验保留。adapter只将准确receipt存入原publication_receipts，没有增加phase权威。 |
| terminal观察窗口 | node:1126起在既有C4接受/丢ACK观察点核实际StageAck的Complete、owner、reference及TERMINAL；没有事后query代替接收时点。 |
| owner client与Core | client:47校验原request、具体reply类型、reference和本地已知publication owner；:95/:106额外将commit fact与实际Complete/prepared相等。adopt仍返回原PublicationReceipt，Core既有Node adoption ACK消费者不需StageAck兼容；query仍给完整snapshot。 |
| trace与死亡推进 | control:2804从StageAck的已验证owner及reference取得原trace字段，无新网络query；观察异常不改变业务回复。controller成功mutation后的owner死亡检查使用同一owner上下文。 |

## 边界与下一步

未把StageAck构造器解释为独立重建全部authority历史：完整record不变量仍由原PublicationSnapshot和authority提交校验负责，消费者核自己实际持有的请求、manifest、prepared或Complete。删除snapshot投影不等于删除权威校验。

本审未增加新故障矩阵或要求额外重跑。root既定的四个增强纯文件、8个新边界case及原计划生命周期/计量负责行为和收益证据；DTO回包缩小、AST成功、此静态无发现均不能单独支持采纳。若后续源码变动，当前结论只对上述冻结hash有效。
