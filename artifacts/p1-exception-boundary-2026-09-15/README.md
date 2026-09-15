本目录记录按已审核方案完成的普通 Task 异常边界有限验收。approved-plan.md 是固定的实施合同；其“待实施”状态属于审核时点，当前结果以 acceptance.json 为准。

B红测80通过/14失败；两版各94＋9＋20单元回归和4项真实进程场景通过。不是整个历史测试树或任意故障组合验收。完整归档留在工作区 audit/two-version-cleanup/execution；本目录的snapshot/identity/原始日志固定实际输入与结果。

本目录是验收时点的固定记录。acceptance.json 的未提交/未推送、parent_commit 和 candidate 描述验收输入，不是发布后的实时 Git 状态；后续提交只在源码/测试/依赖与所列哈希匹配时继承这些结果。当前限制以 docs/current-status.md 和 docs/design.md 为准。

方案原文及独审原文保持字节不变，因此其中的原始路径没有改写：

| 原审核工作区名称 | 本目录归档名称 |
| --- | --- |
| solution-plan.md | [approved-plan.md](approved-plan.md) |
| independent-plan-review.md | [approved-plan-review.md](approved-plan-review.md) |

两份原文中的其他相对路径（original-*.json、plan-language-check、rollback-result.json 等）属于原工作区 audit/p1-reanalysis-2026-09-15，并非本目录已归档的文件。当前实现验收依据是本目录 acceptance.json、snapshot、identity 与原始测试日志；不把原方案的“待实施”描述当作当前完成状态。
