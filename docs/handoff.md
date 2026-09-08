# 开发快照与交接状态

日期：2026-09-08。目标仓库：`xinkuleee/mini-ray`。

这是现有代码、历史证据和后续方案的可追溯归档，不是 v0.1 验收发布，也不表示
K0/K1 已完成。整理／推送只授权保存和发布当前工作；**不构成对 S1–S4 或三个范围
边界的实施批准**。后续仍等待明确语义确认，不自动恢复旧 checkpoint 循环。

## 阅读顺序

1. 本页：当前落盘内容、真实缺陷与未验证草稿。
2. [纠偏方案](correction-plan.md)：最后修订的语义取舍、生命周期、冻结交付表和停止条件。
3. [已有后端设计](design.md)与[Ray 对照](production-ray-mapping.md)：代码当前如何工作。
4. [学习路径](learning-path.md)：七个原示例沿同一个实际后端追踪机制。
5. [历史状态](current-status.md)与[测试策略](testing.md)：各 checkpoint 的证据及安全边界。

原 [roadmap](roadmap.md)、[acceptance-matrix](acceptance-matrix.md)和
`docs/history/` 保留历史来源；后来写入其中的增强项或白名单门槛不自动成为用户要求。

## 已有代码

| 部分 | 当前内容及证据边界 |
|---|---|
| K0 运行时 | Python API、真实 spawn 多进程、一至两个逻辑节点、Worker 内嵌 Core、lease／spillback／direct PushTask、依赖 gate、Hybrid／locality 和资源账本；有历史真实进程证据 |
| 对象／引用 | INLINE/STORED、不可变 Store、跨节点 pull、owner metadata、borrower／nested／lineage holds、逐对象与最后 sibling GC；有对应局部和进程记录，不是全部交错证明 |
| K1 | 系统重试、lineage、targeted／mixed／multi-return、CPU yield、PG prepare/commit/abort、managed Node/Worker death、Actor generation/restart/migration 已有运行时接线 |
| 当前发布架构 | 仍为唯一 selected-output 后端；普通成功仍使用 GCS INTENT/ARM/terminal/adopted、global contained DAG 与 phase-specific recovery。owner-led 取舍尚未实施 |
| 教学入口 | `examples/01` 至 `07`、结构化 trace、成功／应用异常黄金语义合约和上游职责映射；不另维护演示后端 |

## 真实缺陷、验证缺口与草稿

| 编号／类别 | 状态及源码依据 | 下一步边界 |
|---|---|---|
| B1：准入证明缺陷 | [owner_reconstruction.py](../src/miniray/owner_reconstruction.py) 已有 paired snapshot 与部分同 attempt 终态修复，但仍用当前状态推断历史准入；[Core](../src/miniray/core.py) targeted START 后重读 session，快完成可能丢掉已发生的 START | 按纠偏方案区分请求期准入证明与后续 exact replay 记录；不继续枚举终态 |
| B2：drop 请求绑定缺陷 | `CoreWorker._request_drop_owned_object_serialized` 在完整请求冲突校验前按 operation_id 命中缓存 | 同 ID 同请求重放；同 ID 变字段拒绝且无额外 Node Drop；尚未修复 |
| Actor 类型分类缺陷 | [control.py](../src/miniray/control.py) 的 migration 以错误文本含 `resource` 判断容量不足；constructor traceback 可进入同分支 | 静态调用链已确认，未执行复现；以 typed failure 区分容量与构造失败 |
| 验证缺口 | foreign nested STORED 参数＋whole replay 的指定真实交界证据未找到 | 仅验证 retained 换代、StoredArg manifest、Worker import／replay／最终释放，不推导完整笛卡尔积 |
| 未验证草稿一 | [test_targeted_reconstruction_first_ack.py](../tests/unit/test_targeted_reconstruction_first_ack.py)：两个首 ACK 窗口及 preview／queued-slot 控制 | 尚未运行、未进入 reviewed-pure manifest；文件存在不等于已红或已修 |
| 未验证草稿二 | [test_foreign_wait_drop.py](../tests/unit/test_foreign_wait_drop.py)：原合同的 threadless 改写，原样保留 | 五个函数现标为 unit，但 [reference classification](../tests/unit/test_reference_safety_classification.py) 仍要求原 heavy 分类；该守卫本身已被 manifest 选择，当前存在静态不一致 |

第二份草稿的 caller-only public-drop 用例使用 typed reply double，并明确断言 owner／
物理 bytes 未变；它不能冒充真正删除的端到端证据。原
[foreign wait/drop 进程路径](../tests/integration/test_foreign_wait_drop_path.py)独立保留。
本次不为取得绿色结果修改这些断言、分类或白名单，也不擅自丢弃未完成工作。

## 证据边界

最近记录的 `foreign-reconstruction-ack-contracts` checkpoint 早于上述两份草稿。
其命令、结果和限定范围保留在 [current-status](current-status.md)／[testing](testing.md)。
这些历史结果不是当前上传树的绿色证明；特别是分类守卫已与草稿不一致。

本次整理仅进行文档、文件清单、敏感内容模式和 Git 发布检查，**不运行 pytest、
compileall、示例或运行时**。MacBook Air M4／16GB 限制继续有效：未知成本先按重型，
不运行未经审查的全量／目录测试；真实线程／进程测试须先逐项审查再单独执行。

Git 提交本身绑定完整上传树；以下 SHA-256 额外锁定归档前的关键代码／草稿，
不代表正确性或安全认证：

| 文件 | SHA-256 |
|---|---|
| `src/miniray/core.py` | `65494bf4876ad4590b4af55074773a8d8b2839a47a65c387608193d7930116d9` |
| `src/miniray/node.py` | `f418d9613a907caa0ac2c8bf274bf57780a4863b27036e4ac0766176feebf4db` |
| `src/miniray/control.py` | `36faeb456849c23eaf9d64ffadae08e617f6a504f5475730c259e5ab209d2e13` |
| `src/miniray/ownership.py` | `fa1599a809b9ca31b3fc1ec50492ece8483902d3f0bd9631a9b7b31b5ecb8d5f` |
| `src/miniray/owner_reconstruction.py` | `3016d9d177d4f3d6e46e1676681e4b484bb830c4293a52c97b95d1e5c3255f77` |
| `tests/unit/test_foreign_wait_drop.py` | `ec5bdfa247f2b1e52f14aca02dc53898526915e641e3d65f4b64cb9c0220dacb` |
| `tests/unit/test_targeted_reconstruction_first_ack.py` | `9ab55536c81085568c8b7430ff63bf398a76333dce6362b16cdc1578f31a86b6` |
| `scripts/reviewed_pure_manifest.json` | `943bb7da6d225a1d27069a5cc07a549d0e1a6fe9b777e1496d66bb2421bbca0f` |

## 后续入口

仅在用户明确确认可观察语义后，按 [纠偏方案](correction-plan.md)的唯一顺序继续。
具体协议正确性由实施方证明；不反复把实现细节升级成用户审批，也不凭发布快照
宣称方案已确认、保证已取消或目标已完成。
