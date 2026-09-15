# teaching-enhanced 当前状态

2026-09-15普通Task异常边界修复：基于已审核方案，一次覆盖函数/参数解码、callable、结果本地序列化、分类及纯文本诊断，并分开prepared发布恢复。当前两版各94＋9＋20单元用例及4项真实进程场景通过；B原源码红测80通过/14失败。未重跑整版gate，不声明全部P1/P2清零。本轮验收时为未提交工作树，受测源码与提交后的版本按文件哈希对应；证据见[本轮验收](../artifacts/p1-exception-boundary-2026-09-15/acceptance.json)。

当前仍有三个已知 P2：Start 被终态拒绝后 Worker obligation 未收口；BeginDrain 后新发生的 Worker 死亡缺少持续扫描处理；正常 adoption 未退休 Worker 缓存的 TaskReply/PushTask payload。它们不属于本轮 P1 修复，也不同于下文历史 R2.3 已完成的四项 P2。回复/缓存退休边界见[设计说明](design.md)。

以下R2.3数据、代码量及“未推送”叙述保留其历史时点；已推送检查点为b3aca51，不能作为本轮源码的测试结果。本次恢复原基线后重新实施，已撤回的局部P1候选不充当当前验收。

R2.3四项审查P2已按计划修复并完成有限验收，本次阶段性提交固定已验证成果。首次学习仍推荐teaching-base，再研究本版两项mini自定义保证。

| 身份/证据 | 当前准确记录 |
|---|---|
| R2.3提交前HEAD | fd40a407a46ada3d4941c034a73355f45355b270；本次提交与下列实测快照绑定，当前提交以git rev-parse HEAD为准，不改标原始验收身份 |
| 原B派生点 | f9a9b35015f114afda9c87e653b6fedcda2eb0b2 |
| 有限gate快照 | r23-enhanced-final-01；archivea3bfb6588e639dd2655ed2b8a23c736f3fb4730dfbf0cec51af14f6ae0f3b1fb |
| gate结果 | 405 passed / 1 deselected，37/37smoke首次全部通过，包含七个原main |
| 最终受影响/安装快照 | r23-enhanced-final-02；archivee9b33771f1e0721d404f1a135a22fac1ac7958e0cd537d30110dc1439f96c17e |
| 快照差异 | 后者只增加非gate的journal测试迁移、文档和登记hash；逐gate导入/配置闭包验证未变，未把不同执行输入相加 |
| 安装 | 当前源码uv冻结安装、pip check/freeze及项目外import通过；Python3.12.13、uv0.11.26 |
| 源码统计 | 59个Python文件，60,223物理行、49,319代码行；不含测试、文档、证据 |

详细原日志、受影响exact集合、试验、失败与身份映射见[本轮验收](../artifacts/cleanup-enhanced/r2.3-p2/final/acceptance.json)。原R2.2的3f5b725验收保持原事实，不认证后续源码。

成功GCS mutation现返回PublicationStageAck，表达准确阶段fact、owner和关闭事实；完整query/ABSENT与typed失败继续用PublicationReply。Node/owner/child职责、GCS单内存权威、图算法和同步阶段均不变，无新RPC/后端。owner READY、Node Complete、bytes、回复退休与GC仍分开；metadata不能恢复bytes或接管owner。

单候选实际四条受控生命周期的GCS业务帧字节减少18.9–36.3%，指定本地copy/validation调用减少15.6–28.3%，query数不增。代价是+204物理/+181代码行和一种严格wire类型；不称源码更短、全系统更简单或网络加速。原mixed-r2顺序差异保留，固定seed0后另作三对原序校准，没有归一proof。

两版owner-finalize修复各15/10/live1通过，E另迁移旧journal夹具23例（原完整Reply及StageAck均曾失败，修后两者通过），原参数与错误反例保留。四项P2完成不包含其余P3，不声称全部注册测试或任意故障组合均验证。

运行使用Linux/WSL和CHECKED_HASH源码缓存，5秒启动/30秒runner界限未放宽，所有真实失败保留。本次仅阶段性本地提交，未推送或运行远端CI；两分支及历史tag/产物保留。
