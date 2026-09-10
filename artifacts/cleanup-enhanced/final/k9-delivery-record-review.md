# K9 双分支交付记录独立检查

日期：2026-09-10。范围仅为 R2.2 交付记录、现有日志/身份、两分支进度页及七组合同映射；未改正式仓库、源码或测试，未执行测试，也不增加故障矩阵。

**结论：未发现七组当前可达协议合同仍需新增实现或测试的未决项；交付文档和最终身份归档尚需收尾。** 检查期间最后 controller 两种畸形 ACK 场景已完成：enhanced-contracts-05 完整 control 文件 15 passed，映射 remaining_bounded_runs 已清空。最终 E 当前提交的整版结果仍应按 root 正在完成的真实记录归档，不能把早期 393/37 直接标成新 HEAD 的 405/37。

## 已核对的实际身份与证据

| 对象 | 实际检查结果 | 可支持的声明 |
|---|---|---|
| B 清理分支 | refs/heads/teaching-base → f9a9b35015f114afda9c87e653b6fedcda2eb0b2 | 真分支持续存在；不是只留 tag |
| B 实测源码 | 0a340b792c89667e493f7e7313935e45e29071bf；final/runtime identity archive 382c12d12c6da48c8aaeda35a708ea1c999124bd499b486e23bcf2c871227838 | 343 passed / 1 deselected，32 个 smoke 完整通过、七 main 输出存在，最终 B 独立验收 |
| B 安装 | artifacts/cleanup-base/final/install/identity.json 同上源码与归档，source_inputs_unchanged=true | 冻结安装结果属于 B 的实际 0a340b7；不是旧 stage1 安装结果 |
| E 派生 | f9a9b35 是 teaching-enhanced 的祖先，merge-base --is-ancestor 返回 0 | 按 K7 后派生顺序交付，而非从旧 E 复制整分支 |
| E 早期正式源码 | dbc504c0cbc69dcdc4b64128b72b6f852bdc34fa，final-01 clean archive 8083cee2e467d2f33b15228ce89609358ab46213dac37cea0cf1128f9703d80a | 其 pure + 37 smoke 38 行全 exit0；不包含后加 K9 用例 |
| E 当前源码/测试提交 | 3f5b725fb26390b86c78f085486fb73d897d3e42，final-02 clean archive 4790883c8912f48afac88856825bb00dc3ce838dfaeec328eb79e7b6b82d1232 | 当前 pure 405 passed / 1 deselected 及已实际跑到的 main/smoke；最终汇总以该归档结果为准 |
| E 安装 | audit/two-version-cleanup/install/enhanced-final-01/identity.json 指向 dbc504c | 安装证明属于 dbc504c。root 已决定实际重跑 final-02 安装，完成后记录新身份，不把旧安装改标为 3f5b725；本报告不要求额外重复安装 |
| 历史 tag | teaching-base-v0.1 → 69106772567a4131f5ec76e898a3c4bf3bb6dbe6；teaching-enhanced-v0.2 → ce29981a547f83b53b0c1df9f91354dcf89d8e4f | 旧历史未被清理分支改标 |
| main | ce29981a547f83b53b0c1df9f91354dcf89d8e4f | 仍为旧增强源码，不是第三个清理运行时 |

七组映射文件 audit/k9-enhanced-contract-map.json 当前状态为 ALL_SEVEN_ORIGINAL_GROUPS_DISPOSITIONED_AND_CURRENT_REACHABLE_CONTRACTS_VALIDATED。所有列出的证据日志路径实际存在；该文件明确区分旧文件前缀通过、追加用例未被旧快照覆盖、完整新文件实测，以及已退休/不可达旧架构形状。没有把全部 338 个 reviewed selector 都声明已执行，也没有把 matcher 的 synthetic trace 当 runtime 证据。此表达合理。

## 交付前记录修正项

### 1. 两版 progress 顶部当前状态与末尾事实冲突

两版 docs/cleanup-progress.md 第 7–8 行仍写“P1 标量化尚在隔离候选”“K4 至 K7 未完成；teaching-enhanced 尚未创建”。末尾已记录 B 独立验收、E 正式派生与 K8 实施。顶部应改为当前摘要；保留逐阶段历史段落时标明为按时间追加的执行历史，不把旧段落逐条改写成新结果。

E progress 末尾仍只到“下一步固定正式 E 源码提交、运行最终30pure/37smoke与安装”。当前已有 dbc504c 和 3f5b725，以及 final-01 / final-02，应追加 K9 完成记录和准确证据路径。B progress 的“正式两提交映射仍待 K8”也应由当前交付摘要指到实际 B 0a340b7 → E 继承/派生身份，不删除原历史描述。

### 2. E 推荐入口仍只反映接入前候选

mini-ray-enhanced/README.md 第 7 行仍写“运行时增量正在应用，尚未验收”；docs/current-status.md 标题和首段仍称候选，tested_source_commit/head_at_acceptance 行仍为待填，正文只指向 prebranch enhanced-trial-01 的 392/37。

请在最终 E 运行结果收齐后改为其实际 tested_source_commit、最终交付 HEAD、exact archive/manifest 和单独安装身份。保留历史 trial392、formal393 与当前405 的区别。现有“E 是 mini 自定义两项保证，不更接近生产 Ray；首次学习推荐 B”的定位正确，应保留。

### 3. 当前历史账本的现状说明仍过期

两版 docs/acceptance-baseline.md 新增的历史说明还写“最终 K7 验收尚未完成”。这个句子描述本次整理，不属于不可改写的历史 318/32 原证据，应改为当前 B acceptance 的链接。原日期、tag、318/32 和“历史冻结时第二阶段尚未实施”事实保持不动。

docs/acceptance-enhanced.md 开头仍直接写“两阶段均已完成、固定学习标记 teaching-enhanced-v0.2”。这属于旧 ce29981/tag 的保留历史前缀，应保持原字节，只在其后追加当前清理版的身份/证据说明，避免读者误认旧页认证当前3f5b725。它链接的 redesign-plan.md 在两分支实际存在，并已明确保留为两阶段能力/正确性合同、把当前执行安排转交 project-cleanup-plan；这个链接正确，不应误删或改链接。不要重写原377/37、历史前缀或原stage2产物。

E 的 docs/history-index.md 当前标题仍是“基础版B历史资料索引”、历史输入仅列 B691，正文“B没有两项机制”虽是准确 B 事实，但易被当成本 E 页的当前描述。应明确这是从 B 继承的共同历史索引，并给 E ce29981 原文/新增61份artifact入口；无需复制同一批历史载荷或改写 B 原表。

### 4. E 当前最终证据尚未进入正式归档与双分支映射

检查时 artifacts/cleanup-enhanced 只有 enhanced-trial-01、enhanced-k8-01、enhanced-k8-02、integration；最终运行、contracts01–05、合同映射和安装仍主要位于工作区 audit。integration/summary.json 合理记录“final commit gate pending”，但不能用作最终验收摘要。

交付前按 R2.2 在新目录归档 current E acceptance / final runtime / contracts / installation 身份与原日志，并保存七组 map。将 map 内工作区相对路径或反斜杠路径转换为仓库可读证据链接时保留源 run/identity/hash；未转换前只能称“本工作区能定位”，不能称克隆后的仓库中证据闭合。

最终映射至少要列：两 branch_ref、B tested_source_commit=0a340b7、B交付/派生点=f9a9b35、E当前 tested_source_commit=3f5b725（若后续执行输入不再改）、各最终 HEAD 和证据追加提交规则。若 B 后续只改学习文档，列明其与 f9a9b35 的差异。不要追逐包含自身 SHA 的记录造成循环提交。

### 5. main 缺少计划要求的双清理分支导航

mini-ray/README.md 当前仍推荐 detach teaching-base-v0.1，并以旧318/377账本描述两个版本。旧事实正确，但 K9 要求 main 的清晰导航尚未落地：应追加说明当前 main 源码是 ce29981 旧增强检查点，推荐 teaching-base 学习 Ray Core、teaching-enhanced 研究两项额外保证，并链接各分支实际状态/验收。只修改导航，不改 main 运行时和旧历史。

### 6. K9 尺度校准是测量快照，不应改标为最终 HEAD

artifacts/cleanup-enhanced/integration/k9-size-calibration-README.md 明确记录测量时 E HEAD 尚为 B 派生点、运行时源码哈希固定，这作为过程证据是诚实的。现在最终引用该表时，应补一条与当前3f5b725 src 全字节相同的映射，或给出实际差值，不能把原标题中的“当前未提交工作树”直接当今天状态。

同理，测试数/文档数/artifact数已因 K9 追加变化；只要用户最终比较 runtime 规模，用明确的源代码分母即可。已记录 B 47,449、E 49,138 token-code 行及低置信预算限制，不把测试/历史文档减少算 runtime 精简，表述正确。

## 无需新增的门槛

本检查不要求执行全部 reviewed registry、不要求补不可达旧 GCS 双 saga 形状、不新增故障乘积，也不因为远端 Actions 未运行而推导本地验收失败。计划允许尚未发布时如实标明本地交付、远端未验证。若本次最终决定推送，才需核两个真实 refs/heads 对应本地最终 HEAD；不能只推 tag。

历史 tag 指向与日志来源本轮只读核对正确。两版 progress 当前已有的 Markdown 链接都能解析；其中普通文本 artifacts/cleanup-base/final 应在最终摘要改为可点击目录/acceptance入口，明确 runtime/ 与 install/ 子目录，而不是不存在的 final/identity.json。

七组协议闭合与最终运行接近完成，剩余问题是本报告列出的当前状态、可携带证据及身份映射。没有依据把这些记录修缮扩为新的源码或测试任务。
