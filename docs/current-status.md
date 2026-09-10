# teaching-base 当前状态

日期：2026-09-10。**K4正式构造与单输出整理已完成，K5结构整理进行中，K7独立封版尚未完成。** 分批通过只归对应候选，不把旧tag或不同批次的结果相加成新B整体验收。

| 身份 | 当前记录 |
|---|---|
| 实际分支 | refs/heads/teaching-base |
| 历史起点 | teaching-base-v0.1 / 69106772567a4131f5ec76e898a3c4bf3bb6dbe6 |
| 当前提交 | 用git rev-parse HEAD读取；可能另有未提交候选，以每批identity.json与snapshot.json共同识别 |
| tested_source_commit / head_at_acceptance | 新B最终验收映射尚未建立；K7完成时单独记录 |
| E派生点 | teaching-enhanced尚未创建；须从K7已验收B交付HEAD派生 |
| 远端状态 | 未运行远端Actions或发布清理分支 |

## 已完成与当前工作

K0已创建B并固定输入。K1统一有界gate/migration入口与CF-001至004公共修复已完成；registry只归一CRLF→LF，其它内容变化仍拒绝。原始snapshot和日志继续按原字节保存，见[K1记录](../artifacts/cleanup-base/k1/summary.json)。

K2/K3已分批迁移发布、owner、Node、Worker、引用与foreign lineage等合同；成功、失败、修正与受测输入逐批见[执行记录](cleanup-progress.md)和[cleanup-base证据](../artifacts/cleanup-base/)。近期[K3e](../artifacts/cleanup-base/k3e/results.json)修复owner仍持有INLINE bytes而scratch丢失被误判LOST的问题，增强后继须继承；[K3f](../artifacts/cleanup-base/k3f/results.json)保存当前工具与来源/Worker恢复验证。它们不替代新B最终gate与七个main同版验收。

本次已应用短README、设计、学习路径、测试指南及历史索引；4份过期计划和48份history工作副本按原blob可恢复。有效知识已进入[设计](design.md)与既定合同，live测试迁移完成状态仍按每项记录判断。文档行数减少不计作runtime精简。

P1已实现TaskExecution和单个value/payload/result，14个受影响源码文件净减183物理行；同一隔离候选纯gate343通过、1项按原标记排除，32个smoke首次全部通过含七main。应用版另有准确快照映射与回归，见[P1验收](../artifacts/cleanup-base/k4p1/acceptance.json)。
Node journal内部result/retirement已标量化；adapter八表合一在真实切片通过后因历史扫描和接口成本未采纳。effect、owner退休、typed义务与Core/ACK评估仍有后续工作，不能把候选类写成已实现。
本机多进程冷启动出现过多次5秒或30秒超时；原K3对照同样变慢。验收记录保留失败和相同解释器checked-hash源码缓存策略，未放宽时限，也不宣称已证明冷启动稳定或性能无回归。

## 后续边界

B保留GCS成员、死亡事实、Actor/PG协调，普通结果没有GCS发布事务或全局防环。E的两项自定义保证仍是确定交付项；B独立验收不等待E，最终两个heads须同时保留。

首次学习从[README](../README.md)和[学习路径](learning-path.md)进入。旧基础318/32及七main只属于[固定基础账本](acceptance-baseline.md)。[整理计划](project-cleanup-plan.md)是唯一执行计划，[历史索引](history-index.md)负责旧全文恢复；没有第二份滚动backlog。
