# P1 最小完整修复方案：独立审查

日期：2026-09-15。对象：同目录 `solution-plan.md` 最终文本。

**结论：方案通过独立审查；未发现需要继续修改方案的阻断问题。可以按该边界实施，但这不是代码已修复或验收已通过的结论。** 这里的“最小”指复用现有责任与状态、完整封住已确认的异常边界，不是证明补丁行数的数学最小值。

最终方案原始文件 SHA256：`03e15ee9558fc8c5d81605ea3b18556dcdbdcf7bdf3e5a3f715a891284f4bcf1`。方案如继续变更，本报告不自动覆盖变更后的文本。

## 独立核对范围

直接读取并比较恢复后的两版源码，没有把其他分析报告的结论当作源码事实：

| 分支 | HEAD | worker.py 原始文件 SHA256 | 工作树 |
| --- | --- | --- | --- |
| teaching-base | dc86d5f71cbb4fd7dc62a0b24347bb54b42e5602 | 5fb52ef06ac352b88ec27fed27447ecc04b14443c482929322d3b63bd243c1b8 | 干净 |
| teaching-enhanced | b3aca513e29a5da3c092fc607950e2e6e0385b19 | a8b5f68a4355219e0e13712013c253719af4826c7c49467bfbfdb32938dae6ea | 干净 |

核对文件包括两版 `worker.py`、`dependency.py`、`output_discovery.py`、`runtime_binding.py`，以及协议错误回复、Core 重放与 ObjectRef 导入/关闭、TCP 请求处理和现有 Worker 测试夹具的相关实现。两版 Worker 的目标异常路径相同；其文件差异是 enhanced 的 owner-publication 路由。前三个辅助模块在两版间没有差异。这个事实允许采用共同修复原则，不代替两版分别执行验收。

审阅了 `original-boundary-probe.py`、两份 `original-*.json` 和 `plan-language-check.py/json`。原始探针使用实际 cloudpickle/参数编码器和真实 Worker/Node 方法，每个场景两次精确 Push；其源码与记录支持所列十类有限复现。审查者没有重新运行探针、测试收集、集成进程或候选实现。本次只新增本审查文件。

## 为什么方案成立

1. **根因定位有依据。** 原 Worker 在 Start 后、reply/prepared cache 前允许 callable、解码、序列化及诊断异常逃出；请求线程异常不是 Worker 进程死亡。Core 对 RUNNING lease 保留并重放原 Push，原 Worker 在空缓存下再次进入用户执行阶段。修复 Worker 的阶段边界比把 RPC 错误伪造为 Core 任务终态更准确。
2. **分类方案避免重复访问不可信异常实例。** 原 callable 分支的 `isinstance` 可以访问用户的 `__class__`。先 `except BlockingNotificationError`、后 `except BaseException` 使用语言异常匹配，保留真实 BlockingNotificationError 及子类的 SYSTEM_ERROR，同时把其他 callable 异常终结为 APPLICATION_ERROR。解码与本地输出错误继续遵循现有 SYSTEM_ERROR，未顺便修改重试语义。
3. **诊断回退够小且完整。** 类型名、消息、traceback 同处一个 BaseException 防护块，任何失败或非精确 str 都退为固定普通字符串。该设计同时处理元类属性、异常格式化和 str 子类 wire reducer 风险；将协议对象构造留在防护块外，不会掩盖身份或状态错误。不需要新消息、通用 MRO 框架或部分诊断恢复策略。
4. **prepared 边界是真实责任切点。** `discover` 失败已经 ABORTED 并清空 source handles；成功后的 prepared 持有实际 bytes、manifest 和 imports。让 prepared 安装与 `_resume_discovered_outputs` 位于本地 discovery catch 外，可以区分用户 reducer 抛私有异常与真正发布待恢复状态，也不会把已发生远端效果改写成新的普通失败。缓存先于 Complete、prepared 保留和精确重放继续使用原协议。
5. **故障注入移动有明确必要性。** 原 nested-import crash 的测试用 SystemExit 子类替代真实 `os._exit`；扩大 decode catch 后，若不分离注入位置，就会误吞该测试代表的进程终止。方案保留受保护的 acquired 校验，把真实退出留在 commit 之后、binding 创建之前、外层引用 finally 之内，并让 binding 准备失败仍按 SYSTEM_ERROR 处理。该顺序与现有 borrower 存活观察点一致，不增加识别测试类的生产兼容分支。
6. **清理边界没有被掏空。** 普通失败仍在缓存/Complete 前 rollback/close；成功输出由 prepared 保有 cleanup 责任。实际导入路径创建具体 ObjectRef，关闭涉及原 Core release intent，不能仅凭测试夹具可注入任意 close callback 就扩大到 dependency 吞异常改造。方案明确不把本地 release 收据、Node Complete、owner 可见性和物理 GC 当作同一事实。

## 审查反馈已处理

- 原验收措辞“用户/decoder/reducer 最多一次”可能误读为首次诊断也只调用一次用户钩子。最终 §8 已限定为 callable、输入重建 hook 和结果 reducer 的相同 Push 重放计数，并说明 `str(exc)` 与 traceback 格式化在首次处理内可能重复读取诊断钩子；缓存重放不得重新分类或格式化。
- 最终 §3 已明确 Node Complete 只确定执行终态与 Node 账本事实，owner 可见性、回复托管退休、GC 各有责任，避免把执行收口描述成结果已被 owner 接管。

以上都是合同精度修缮，不要求新增运行时状态或故障矩阵。最终文本已完整重读，哈希对应修缮后的版本。

## 实施后仍必须取得的证据

这份方案尚未运行新实现。以下是验收要求，不应预先写成完成事实：

- 两版分别把所列实际序列化/异常场景接入 Worker/Node 回归，验证同 Push 不重执行、精确状态、一次 lease 释放及同 Worker 后续任务；诊断字段须实际随 TaskReply wire roundtrip。
- 分别验证应用失败和系统失败的 Complete 回复丢失重放，以及 prepared 安装后的控制流异常只恢复既有托管，不生成另一失败终态。
- 验证 nested-import crash 的原顺序和真实进程死亡行为，以及含 nested ObjectRef 的有界集成场景、借用释放和退出清理。基础版证据不能替代 enhanced 验收。

“有限异常边界可终结”不等于任务副作用全局 exactly-once，也不等于全项目 P1 清零。新 attempt 的系统重试、真实进程死亡、永久不返回 hook、运行时被篡改等不是本补丁新增保证。既有 Start 拒绝义务、drain 后死亡、Worker payload 退休 P2 仍在本包之外；不应为此扩大这次最小完整修复，也不能在交付时把它们宣称已解决。
