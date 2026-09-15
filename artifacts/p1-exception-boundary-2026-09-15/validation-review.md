# P1 完整修复：有限验证范围静态复核

日期：2026-09-15。审查者：`/root/p1_verification_setup`。本记录只审查选择器、已有运行边界与登记要求，未执行测试、未改变项目代码，也不是通过声明。范围为 teaching-base 与 teaching-enhanced 各自的当前候选。

## 精确验证清单

每版独立冻结输入并串行执行以下七个选择器；不运行整个历史测试树，不改变既有 CI gate。

| 入口 | 精确选择器 | 作用 |
| --- | --- | --- |
| `--case` / unit | `tests/unit/test_worker_unified_output.py` | 当前单输出协议回归及新增用户 hook 失败闭环、缓存重放、错误诊断、Complete ACK 未知和 prepared 恢复 |
| `--case` / unit | `tests/unit/test_worker_completion_paths.py` | 既有 Worker completion 与精确终结责任 |
| `--case` / unit | `tests/unit/test_worker_nested_task_arguments.py` | 导入/rollback/close、attempt 隔离及 crash-before-binding 的既有顺序断言 |
| `--case` / 新 multiprocess_smoke migration | `tests/integration/test_task_retry_path.py::test_user_hook_failures_are_terminal_without_restarting_worker` | 新增真实进程用户异常与不可格式化诊断、引用释放、同 Worker 后续任务；最终成本和断言由测试审查者另行复核 |
| `--smoke` / 既有 multiprocess_smoke gate | `tests/integration/test_task_retry_path.py::test_explicit_worker_system_error_retries_once` | 原 SYSTEM_ERROR 创建新 attempt 的有限重试 |
| `--smoke` / 既有 multiprocess_smoke gate | `tests/integration/test_worker_crash_recovery_path.py::test_after_complete_worker_crash_recovers_output_without_reexecution` | 真实 Worker 在 Complete 后退出，保留成功托管并恢复原 attempt |
| `--case` / 新登记的既有 multiprocess_smoke | `tests/integration/test_worker_death_ownership_path.py::test_dead_attempt_borrower_is_swept_while_logical_hold_spans_retry` | 真实 nested-import 后 os._exit，死亡权威、借用清理与逻辑 hold 跨重试 |

前三个 unit 文件在两个原 manifest 中均已登记为整文件 migration；不得重复登记其内部单个用例。两个既有 smoke 已是 gate 精确选择器。新增用户异常用例与下面的既有 nested-death 用例只能各登记一个精确 migration，不自动晋升 gate。

## nested-import 死亡场景的独立静态成本复核

审查时 B/E 的 `tests/integration/test_worker_death_ownership_path.py` 完全一致，原始字节 SHA256 为：

`aa657bacc9222e3da66f051f9b4f9c04fea7ce96f6e096c371f00a92fd97f87a`

此文件此前未在当前 manifest 登记。Git 提交 `2e50c35fe90c41626c5d38627a87da52334d55cd` 对该文件只有旧 runner 到新 runner 的 docstring 说明修改；清理计划将其列为 `keep_reviewed_not_confirmed_obsolete`。K1 明确不自动吸收旧 108 个 extra 选择器。未发现因不安全或已废弃而拒绝该场景的记录；缺少登记不能解释成已通过或已废弃。

当前运行逻辑的有限边界：

- 1 个 GCS、1 个 Node、1 个普通 Worker 槽，原 Worker 退出后替换一次；受管子进程峰值 3 个，预期生命周期共 4 个 PID。
- 1 个 1 MiB object store，2 个微小对象，1 个逻辑 Task，`max_retries=1`，最多 2 个 attempt。
- 使用现有 1 个 Driver loopback listener 和 1 个被接受的 gate 连接；测试不新建线程。最多 6 个不同端点。
- 初始化后共用 15 秒工作 deadline；最终 gate/reference 清理共用 3 秒。各 connect/accept/recv/send 取剩余 deadline；被动 RPC 记录上限 64，Acquire 记录上限 4；轮询最多 1024 次且同时受 deadline 限制。
- shutdown 位于嵌套 finally，listener/connection 关闭、observer 恢复、引用关闭失败记录、实际 PID/受管 active_children/端点检查均保留。运行器仍是原 POSIX 30 秒测试进程树边界及原 2 秒 TERM 清理宽限，不放宽任何期限。

有效性方面，该用例观察真实 Acquire 后第一个 borrower 和 submitted hold 共存；原 Worker 以 `CRASH_AFTER_NESTED_IMPORT_EXIT_CODE` 退出且被 reap；精确 GCS `PROCESS_EXIT` 记录被安装为 owner 死亡事实；新 Worker 使用新 lease/attempt 1，复用同一逻辑 submitted hold；旧 borrower 被清理而新 borrower/hold 仍保活；放行既有 gate 后结果成功、引用终结及 source/result collection。它不以端点不可达代替死亡事实。

根代理最终选择将其作为本次既有真实死亡回归的一项：当前修复移动了该 failpoint 所在块，因此真实进程证据直接相关且成本明确。此选择不增加拓扑、故障组合或期限。只需在两版更新该文件过期的“当前未登记”docstring，并登记精确 selector/闭包；不修改测试业务断言。上述 SHA 标识修改 docstring 之前的受审逻辑，最终执行 SHA 必须重新记录。通过与否由后续实际日志决定。

## manifest 与运行证据约束

- 保留 `pure`、`smoke` 和 `known_non_unit_in_pure` 原清单；只增加上述两个必要 migration。不借本次登记吸收其他历史未选择场景。
- 只有已复核的实际变更输入可刷新：Worker、单输出 unit、nested-argument unit（若修改）、Task retry integration，以及 nested-death 文件的登记说明。不得盲目改所有行的 review provenance。源文件变更会传播到已有引用它的闭包；这不等于新增测试范围。
- 既有闭包保持原成员集合，除非最终新增 import 的路径经明确复核；保留显式额外 reviewed files。unit cost 说明应反映真实有限用户函数/编码器 hook，不能继续声称完全没有用户执行。
- review hash 采用 runner 的 `_review_input_hash`，仅 CRLF 转 LF；冻结源码、archive 与日志仍记录原始字节 SHA256。更新脚本应在本版候选停止修改后使用新进程读取，避免复用另一版模块或先前缓存。
- 使用现有 `freeze_cleanup_candidate.py`、`prepare_validation_bytecode.py` 和 `run_cleanup_candidate.py`，按版、按红/绿候选使用不同名称；源快照在执行前后逐项核对。既有 Python 3.12.13 和锁定依赖保留；checked-hash cache 是运行环境准备，不是源代码修改或期限扩张。
- 原始失败、通过退出码、准确命令、环境、受测 SHA 与日志分别保存。当前新测试输入不能套用旧 callable-only 候选的通过记录；日志即使被 Git 的 `*.log` 忽略也必须列入最终证据清单。

本静态审查未发现必须新增进程、增加 deadline 或扩大收集范围才能执行上述选择器的理由；实际功能正确性、耗时及完整清理由后续受限运行和独立代码/测试审查确认。
