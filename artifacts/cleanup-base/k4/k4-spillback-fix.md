# K4 spillback 测试遗漏修复

日期：2026-09-10。工作树：`C:/Users/t-hdong/Desktop/gao/mini-ray-base`，分支 `teaching-base`。

## 范围与状态

仅修改 `tests/unit/test_spillback_runtime.py`；本说明位于仓库外审计目录。没有修改源码、共享 helper、manifest，也没有执行 pytest、提交或推送。原文件已有 K4 Node worker-slot 和 Core home-route 迁移 hunk，均原样保留。

本文件保留全部 12 个原测试函数及其标记：11 项 unit，1 项 `loopback_smoke`。此次发布协议修改集中于三个 `_SubmissionFixture` 场景，不删调度、资源、重复 Push、超时、拒绝或并发检查。

当前修改文件原字节 SHA256：`e4dcbae229a76aea8badbc0e31f0340d4fdd3aa617cb3bd8aae5e88a545c7b42`。
仅 CRLF→LF 规范化 SHA256：`3e379b9847ccaf61ba5f1907f6aed8ff7924e914c60cbf369aacd04388fa723a`。

## 为什么必须修

原测试从 `_pure_node_output` 导入 helper，并在 Core RPC fake 中驱动 `ReportOutputPublicationTerminal`、`ReportOutputPublicationAdopted`、`ReportOutputPublicationSlotCollected` 和 `OutputRecoveryReply`。它们属于旧中央发布恢复协议。清理后的 B 没有该普通结果 GCS 权威；沿用旧 helper 既无法导入，也不能证明 B 的实际 owner-led 结果发布。

仅改 import 不够：`_pure_node_output_current` 默认自带独立 `OutputHandoffTable`，适合只测 Node 的 fixture，而本文件要执行实际 Core 的 owner CAS。若 Core 没有相同注册记录，交接不成立；若复制第二份注册表，则测试仍存在重复权威。

依据为当前源码：`CoreWorker.register_output_handoff`、`report_output_handoff_complete`、`get_output_handoff`、`_drive_output_publication_adoption`、`_reference_released`，以及 `NodeServer._handle_complete_worker_lease`、`_handle_ack_output_publication_adopted`。

## 修复方式

1. 使用 `_pure_node_output_current.prepare_ref_free_output`。仅在该 helper 的一次构造期间，将其 `OutputHandoffTable` 构造绑定到本文件 `_CoreHandoffEndpoint`。该 endpoint 不存储第二份状态，注册、查询与 Complete 记录均调用真实 Core typed handler，校验实际回复的类型、完整 request、accepted、error 和 snapshot；未知或拒绝不会转成成功。离开上下文立即恢复 helper 构造符。
2. 删除 RPC fake 中整个过期 GCS 普通结果发布分支。GCS 路径只允许本来就存在的 Node 注册、资源更新和地址查询。
3. Node Complete 后仍核 CPU 已释放、精确 Complete 和 INLINE envelope；额外核 Complete 报告保留在 Node outbox、此时 owner 尚无 Complete。Core 之后从准确 envelope 接收执行事实并完成真实 owner CAS。
4. adoption RPC 先核真实 owner receipt 与 owner handoff 的 ADOPTED proof；调用真实 Node handler 后核准确 Node 回复与 journal retirement tombstone。节点回复托管退休前后 owner collection 仍为 ACTIVE，防止把退休解释成对象 GC。
5. 在真实 `complete_output_publication_collection` 上安装只观察的 wrapper。它检查 Core 锁内的实际 frozen obligation、已清空的 drop/edge 义务和 COLLECTING 状态，再执行原函数并保存实际返回收据。后续核集合完成、lineage 消失、Core 对象/descriptor/GC obligation 清空及 owner 对精确 GC plan 的收据重放。
6. Node 的迟到 Complete 报告在 GC 后通过实际 Core handler 重放。核历史 handoff 和 Node tombstone 均不变，不重建 bytes、ObjectRef、owner 对象状态或清理义务。

基础版不再有 `slot_collections` 中央记录。因此原“GCS 记了一条收集记录”由真实 owner collection plan/receipt 和空 GC obligation 替代，不能继续照抄旧增强协议作为 B 的通过条件。

## 原有合同保留

| 场景 | 仍保留的证据 |
|---|---|
| 两 Node spillback | 原 LeaseID/TaskID/AttemptID 不变；第二跳仅增加 target Node；home 不持有资源；只有授权 target Complete 释放资源 |
| ambiguous Push timeout | 真实 RUNNING lease 和 CPU 保持；无伪成功结果；原 Push pickle bytes、grant、lease request 和未决义务不变；单次人工精确重放 |
| Worker 明确 rejection | 首次保持 GRANTED 且不执行 Start；保留原 Push 和 allocation；精确重放后真实 Start/Complete |
| 重复 Complete | 精确历史 witness 不变，`released=False`，无第二次资源释放或资源版本增加 |
| ObjectRef/GC | 两次关闭只释放一次真实 local token；实际 task finish barrier 后通过有界 FIFO 驱动 GC；lineage 与收据均核对 |
| 其它调度与并发 | 其余测试函数和标记保留；原 L1 的两真实请求线程、Barrier/event、锁内状态和 bounded join 不变 |

三个 composition 场景仍只构造 threadless Core、一个/两个 Node reducer、NodeRegistry 和最多 1 KiB ObjectStore；不执行用户函数、不启动进程/线程/监听器/定时器、不等待。原 tripwire 与 FIFO/调用次数边界保留。

## 已做静态检查与待执行证据

- Python AST parse 通过；与 HEAD 比较，12 个原测试函数名及顺序均保留。此检查不冒称函数行为已通过。
- `git diff --check -- tests/unit/test_spillback_runtime.py` 通过，只有 Git 的 LF/CRLF 提示。
- 搜索确认此文件已无旧中央 `REPORT_OUTPUT_PUBLICATION` / `ReportOutputPublication*` / `OutputRecoveryReply` / `slot_collections` 使用。普通 Task 的 `_recovery` 是 B 保留的重建和 lineage 权威。
- **尚未运行测试**。root 需按当前内容刷新精确迁移 selector 的审查 hash，再冻结候选并通过本分支有界 runner 执行。不要整文件无筛选导入 pure gate。

优先运行三个 unit exact selector：

- `tests/unit/test_spillback_runtime.py::test_core_preserves_identity_and_does_not_release_from_submitter`
- `tests/unit/test_spillback_runtime.py::test_core_does_not_release_after_ambiguous_push_timeout`
- `tests/unit/test_spillback_runtime.py::test_explicit_worker_rejection_requeues_exact_push`

其余 8 项 unit 与 1 项 `loopback_smoke` 保持原分类，按 root 当前 K4 fixture 回归范围有限执行。此文件仍处于 K4 DTO 标量化之前；P1 候选适配须以本修复为最新输入，不能从旧文件再次带回 GCS helper。
