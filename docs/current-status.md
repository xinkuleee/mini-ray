# teaching-base 当前状态

R2.3中基础版共同测试修复已完成，仍为推荐首次学习入口；不含普通结果GCS发布事务或全局防环。

- R2.3提交前HEAD：ab4cfb317fd786a286359d8f4e971ff195739375；本次阶段性提交固定已验证的测试、文档及证据，当前提交以git rev-parse HEAD为准。
- 本轮准确输入：r23-base-after-02，archive98fa2fbf0eb921b87b13b9a335d7247d2a6f7ee69dab8080d1e2f8e1690ba5f3。
- 新目标15项、邻接Node10项、真实owner死亡1项全部通过；不是重新运行完整gate。
- 运行时源码、共享helper、依赖及原gate测试未改。原R2.2实测0a340b7的343/1deselected与32smoke仍归原固定输入，未移贴当前HEAD。
- 源码56个Python文件，57,995物理行、47,449代码行；不含测试/文档/证据。

完整身份、原失败及后续验证见[本轮B验收](../artifacts/cleanup-base/r2.3-p2/acceptance.json)；历史整版证据见[R2.2验收](../artifacts/cleanup-base/final/acceptance.json)。STORED未物化ACK时无journal result但仍负清理责任，独立INLINE证明payload在Worker ACK未知时继续保留。

E已独立完成对应测试和StageAck增强表示评估/验收；B未加入增强端点或功能开关。R2.3仅处理四项审查P2，其余P3无caller helper、别名/恒零参数及旧fixture兼容仍明确待后续，不称历史负担全部清零。详见[计划§11](project-cleanup-plan.md)、[执行记录](cleanup-progress.md)。

运行证据基于Linux/WSL Python3.12.13、uv0.11.26与CHECKED_HASH源码缓存，原5秒启动/30秒runner期限未放宽；原失败保留。本次仅阶段性本地提交，未推送或运行远端CI，原tag与历史产物不变。
