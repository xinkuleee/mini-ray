# teaching-base 当前状态

2026-09-15普通Task异常边界修复：基于已审核方案，一次覆盖函数/参数解码、callable、结果本地序列化、分类及纯文本诊断，并分开prepared发布恢复。当前两版各94＋9＋20单元用例及4项真实进程场景通过；B原源码红测80通过/14失败。未重跑整版gate，不声明全部P1/P2清零。本轮验收时为未提交工作树，受测源码与提交后的版本按文件哈希对应；证据见[本轮验收](../artifacts/p1-exception-boundary-2026-09-15/acceptance.json)。

当前仍有三个已知 P2：Start 被终态拒绝后 Worker obligation 未收口；BeginDrain 后新发生的 Worker 死亡缺少持续扫描处理；正常 adoption 未退休 Worker 缓存的 TaskReply/PushTask payload。它们不属于本轮 P1 修复，也不同于下文历史 R2.3 已完成的四项 P2。回复/缓存退休边界见[设计说明](design.md)。

以下R2.3数据、代码量及“未推送”叙述保留其历史时点；已推送检查点为dc86d5f，不能作为本轮源码的测试结果。本次恢复原基线后重新实施，已撤回的局部P1候选不充当当前验收。

R2.3中基础版共同测试修复已完成，仍为推荐首次学习入口；不含普通结果GCS发布事务或全局防环。

- R2.3提交前HEAD：ab4cfb317fd786a286359d8f4e971ff195739375；本次阶段性提交固定已验证的测试、文档及证据，当前提交以git rev-parse HEAD为准。
- 本轮准确输入：r23-base-after-02，archive98fa2fbf0eb921b87b13b9a335d7247d2a6f7ee69dab8080d1e2f8e1690ba5f3。
- 新目标15项、邻接Node10项、真实owner死亡1项全部通过；不是重新运行完整gate。
- 运行时源码、共享helper、依赖及原gate测试未改。原R2.2实测0a340b7的343/1deselected与32smoke仍归原固定输入，未移贴当前HEAD。
- 源码56个Python文件，57,995物理行、47,449代码行；不含测试/文档/证据。

完整身份、原失败及后续验证见[本轮B验收](../artifacts/cleanup-base/r2.3-p2/acceptance.json)；历史整版证据见[R2.2验收](../artifacts/cleanup-base/final/acceptance.json)。STORED未物化ACK时无journal result但仍负清理责任，独立INLINE证明payload在Worker ACK未知时继续保留。

E已独立完成对应测试和StageAck增强表示评估/验收；B未加入增强端点或功能开关。R2.3仅处理四项审查P2，其余P3无caller helper、别名/恒零参数及旧fixture兼容仍明确待后续，不称历史负担全部清零。详见[计划§11](project-cleanup-plan.md)、[执行记录](cleanup-progress.md)。

运行证据基于Linux/WSL Python3.12.13、uv0.11.26与CHECKED_HASH源码缓存，原5秒启动/30秒runner期限未放宽；原失败保留。本次仅阶段性本地提交，未推送或运行远端CI，原tag与历史产物不变。
