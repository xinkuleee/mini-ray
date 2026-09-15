# P1 完整异常边界修复：独立运行时审查

日期：2026-09-15。范围：两版实际工作树中的 `src/miniray/worker.py`，对应已通过审核的 `audit/p1-reanalysis-2026-09-15/solution-plan.md`。

**结论：未发现需要修正的运行时缺陷；两版实际补丁符合方案指定的异常分类、诊断回退、引用清理和 prepared 托管边界。** 这是当前补丁的静态审查结论，不是全部源码 P1 清零，也不代替两版测试验收。

## 已核对版本与范围

方案 SHA256：`03e15ee9558fc8c5d81605ea3b18556dcdbdcf7bdf3e5a3f715a891284f4bcf1`。候选最初审查时尚未应用；本报告是在两版实际应用之后重新读取 Git diff、核对原始文件哈希及 AST 后形成。

| 版本 | 未改变的 HEAD | 实际 worker.py SHA256 | 目标方法行号 |
| --- | --- | --- | --- |
| teaching-base | dc86d5f71cbb4fd7dc62a0b24347bb54b42e5602 | d6a4321cc7df62bd1d55f3377bf06c24fed00f7024489a1c97b355052ce9d89d | `_error_reply` 172；`_handle_admitted_push_task` 538 |
| teaching-enhanced | b3aca513e29a5da3c092fc607950e2e6e0385b19 | d4dc7d951a3aa8b20c1b23658dd9530cb64a7287c008596391aa45703b838e9d | `_error_reply` 172；`_handle_admitted_push_task` 539 |

- B 实际完整文件与已审候选 `worker-candidate.py` 的原始 SHA256 一致。
- 两版两个目标方法的 AST 均与已审候选完全一致。分别将两个目标方法的 body 遮蔽后，整个 Worker AST 与各自 HEAD 一致；Git diff 也仅显示这两个方法的局部修改。
- 两版 `git diff --name-only HEAD -- src` 均仅包含 `src/miniray/worker.py`；`src` 没有未跟踪文件。没有修改其他运行时模块。
- B/E Worker 完整文件仍仅保留 enhanced 的两处既有 `abort_owner_publication` 路由/不可用回复差异，没有把基础版覆盖到增强版。
- 另外核对 `dependency.py`、`output_discovery.py`、`runtime_binding.py`、`blocking.py`、`protocol.py` 和相关 Core ObjectRef 导入/关闭实现。前四个文件两版一致。审查使用原始源码，不以另一位审查者的结论代替源码核对。

## 实质核对

1. **诊断统一回退完整。** 类型名、消息和 traceback 在同一个 `BaseException` 防护块内生成，三项均须为精确 `str`。任一读取、格式化或类型检查失败后，三项一起替换为固定普通字符串；没有再次读取异常、保留异常对象或让字符串子类进入回复。`TaskReply` 和 `RemoteErrorInfo` 的构造位于防护块之外，因此身份/状态校验没有被吞掉。传入的 APPLICATION/SYSTEM status 未改变。
2. **callable 分类没有再访问异常实例的 `__class__`。** 先 `except BlockingNotificationError`、后 `except BaseException` 使用语言异常匹配；真实该类型及子类继续 SYSTEM_ERROR，其他 callable/binding 作用域异常按 APPLICATION_ERROR。与 callable 不同，binding 对象的准备失败独立按 SYSTEM_ERROR 处理。
3. **decode 和本地 discovery 的控制流异常可终结。** 两处不再特殊重抛 `SystemExit`/`KeyboardInterrupt`。函数/参数解码保持原 SYSTEM_ERROR，result reducer 所在 discovery 阶段也保持原 SYSTEM_ERROR；没有将系统错误改成应用错误，也没有改变新 attempt 的既有有限重试策略。
4. **prepared 是准确的责任切点。** 本地 discovery 的 catch 只覆盖创建 discovery 会话和 `discover(value)`。`_PreparedOutputReply` 构造、安装、nested imports 转移和 resume 都在成功分支中，处于该 catch 之外。用户 reducer 抛 `_OutputCompletionPending` 不再凭类名逃过本地终结；安装后的 publication/Complete 异常仍由 retained prepared 精确重放，不能返回 callable 或被改写为新的普通失败。构造 prepared 只是既有 dataclass 赋值，未增加用户执行钩子或远端效果。
5. **真实死亡注入未变为 Python 普通失败。** nested-import acquired 校验留在受保护准备阶段，commit 后真实 `os._exit` 位于 decode/callable catch 外、binding 创建前及既有引用 finally 内。真实进程退出不会栈展开；测试以 Python 异常代替退出时也不会误归类为任务失败。普通绑定准备失败仍会走 finally 的 close。
6. **清理和终态权威保持原样。** decode 失败仍先 rollback；commit 后 rollback 为 no-op，由外层 close 负责进口引用。普通失败的 close 仍在缓存和 Complete 前。discovery 自身对 BaseException 转 ABORTED 并清空本地 source handles；成功输出则把 imports 交给 prepared 的既有清理责任。`_execution_lock`、Start 身份、cache-before-Complete、Complete ACK 验证、owner fencing 与义务收口方法均未修改。

## 限制与验收关系

本审查没有执行 pytest、真实进程或故障探针；只使用 Python AST 解析、Git 读取和文件哈希检查。测试覆盖、红绿证据、manifest 闭包和进程清理由单独的验收记录及测试审查负责，本报告不预先认定增强版测试通过。审查者没有修改项目源码、测试或文档，仅新增本报告。

这个补丁处理方案所列的普通 Task 有限 Python 异常场景，不承诺跨新 AttemptID 的副作用 exactly-once，不将 Node Complete 等同于 owner 可见性或 GC。既有 Start 拒绝义务、drain 后死亡及正常 Worker payload 退休 P2 仍属独立范围。计划排除的运行时/私有状态篡改、永久不返回钩子、内存耗尽和真实进程死亡不因此变为已修复保证。
