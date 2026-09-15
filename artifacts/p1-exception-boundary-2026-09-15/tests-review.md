# P1 完整边界修复：新增测试独立审查

日期：2026-09-15。审查者：`/root/p1_full_tests_review`。依据：`audit/p1-reanalysis-2026-09-15/solution-plan.md` 的有限验收合同。

**结论：最终新增测试通过静态审查，未发现尚需修正的有效性、假通过、精确重放或清理问题。该结论覆盖下面的两版测试文件，不代替实际红绿执行，也不声称全项目无缺陷。** 审查者没有运行 pytest、测试收集、进程或探针；只读取代码、比较 AST/哈希，并写本文件。

## 受审输入

| 文件 | teaching-base 原始 SHA256 | teaching-enhanced 原始 SHA256 |
| --- | --- | --- |
| `tests/unit/test_worker_unified_output.py` | `cee0de9e86d7c3c5f87a14dfa21dc3dbc8efc58c6b535164c8523f490fdcac34` | `93069129f6f722be45ebecdc25a4e68e5b04da8e25ece9374d84491129909bb1` |
| `tests/integration/test_task_retry_path.py` | `3294ed016c42716ccbb11bcad4860bd9193ce60dd062b067e47f2c1806043f5c` | 与基础版相同 |
| `tests/unit/test_worker_nested_task_arguments.py` | `e6e3daea6c21c77fbb604681dff51c907527e06c43277139a0cd06aee2338f99` | 与基础版相同 |
| `tests/integration/test_worker_death_ownership_path.py` | `9996eb077e6f244344f9ff5b0384b33a5d4544303f460a4fb09e8175294f6882` | 与基础版相同 |

两版分别与各自 HEAD 比较：上述文件原有全部顶层函数和类的 AST 未改变、未删除。新增内容仅为两个 unit 测试函数，以及一个 remote 函数和一个 integration 测试函数；nested-death 只更新登记说明，nested-argument 无变化。两版新增 unit 函数 AST 完全相同。增强版原有 `_Fixture`、`_ActualNodePublication` 和关联 `_SingleOutputRPC` 未被基础版覆盖，仍使用自己的 PublicationAuthority、PublicationClient 和 owner/Node 回调。

## 单元测试为何能识别目标缺陷

`test_user_executable_boundaries_complete_once_and_reuse_worker` 有 16 个基本场景及两个选定 ACK 丢失场景。真实 cloudpickle 处理函数载荷、闭包重建、参数编码/解码和输出 reducer；没有把实际失败替换成 fake loads 或单独调用错误格式化 helper。`sys.setprofile` 按唯一钩子代码名计数，观察的是反序列化后实际执行的函数，不依赖被复制的闭包 list；finally 恢复原 profile。

- 首次执行后精确 Push wire roundtrip 再重放，要求返回同一缓存对象，用户执行、输入重建、输出 reducer 和诊断计数均不再增加。decode/binding 失败时 callable 为零；其余 callable 为一；首次诊断不强求一次调用，符合方案。
- 使用真实 Node 的 lease grant、Start、Complete handler 与 `_CountingLedger`。错误状态、task/attempt/worker 身份、Node COMPLETED 事实、缓存请求、一次物理 release、空执行义务和 import session 已关闭均有断言。随后同 Worker 执行另一真实序列化的成功 Task，并再次精确重放。
- 一个 APPLICATION_ERROR 和一个 SYSTEM_ERROR 在真实 Node Complete 之后抛 `ConnectionError`，模拟一个 RPC seam 的 ACK 丢失。缓存必须在该切点已存在，重放的 Complete 请求必须完全相同，第二次不再释放资源。这是有限 seam 证据，不是 live TCP 丢包测试，也不是所有故障的组合。
- 诊断类覆盖 message、notes/traceback、元类类型名、实例 `__class__` 和返回 str 子类。TaskReply 经真实 transport `_serialize` 及 pickle loads 后仍仅含精确 str；带危险 reducer 的文本子类不能进入 wire。普通错误、真实 BlockingNotificationError 及子类有独立预期，避免把错误类别统一成假等价。
- binding 场景是明确的 Worker 方法 seam；私有 `_OutputCompletionPending` 场景是额外协议隔离检查，不把它们表述成新公共 API 或常规用户场景。

`test_prepared_control_flow_interruption_replays_custody_without_new_failure` 在真实 adapter/journal prepare 已生效后注入一次普通 SystemExit。它断言 prepared/imports 仍托管、没有普通失败回复或 Complete/lease release，重放复用相同 manifest 和 bytes，最终只执行一次 callable/reducer、提交一次 Complete 并关闭 imports。该用例复用既有 `_Fixture` 的函数查找替身，实际结果序列化和 Node publication reducer 仍是真实实现；函数反序列化证据来自前述矩阵，不能混称每项都执行真实函数解码。增强版同一用例通过本版中央发布回调运行，未绕过该版实现。

## 真实进程测试与清理

新增 integration 测试保留 1 GCS、1 Node、1 Worker、1 MiB store，执行一个 nested-reference source put、两个应用失败和一个后续成功 Task。SystemExit 验证原 Worker PID 与 ObjectID 诊断；不可格式化异常验证固定纯文本回退；均检查 attempt 0、APPLICATION_FAILED、重试额度未使用和精确 Push 顺序/身份，后续任务返回同一个 Worker PID。

借用清理不只检查最终空表：每次得到一个新的真实 borrower token，要求其 WorkerID 正确且 submitted/borrowed holds 已退；再等待同一个 ObjectID/token 的真实 owner release 调用。允许实际返回 False，因为 submitted-hold cascade 可以先合法退休该 token；不会把 cascade 单独当作 Worker 已关闭引用的证明。

观察器先执行原方法，再被动保存原请求/结果，记录上限 16，不修改协议结果或在线程内制造断言异常。release 观察发生在 Core `_completion` 锁内且随后 notify；测试使用相同锁检查/等待，锁顺序与观察器一致，不存在所审路径的丢失唤醒。`_finished_tasks` 使用 TaskID 的断言与 `_PendingTask.task_key` 一致。Push 观察先于 Core 安装 owner 可见状态，任务完成屏障之后读取不会漏掉成功记录。

所有工作共享初始化后 10 秒，public close 共享 3 秒；没有新增测试线程、listener 或 sleep，仍须使用已有外部 30 秒进程树 runner。finally 逐项关闭引用、恢复观察器、shutdown，并检查所有受管 PID、active_children 与五个端点。工作异常仍为主异常，清理异常附注，不因次要 close 失败掩盖原始失败。这里验证 release intent/真实 owner 调用与正常关闭，不冒充所有对象的物理 GC 或进程级 exactly-once。

既有 nested-import 死亡测试的业务 AST 未改。其单次死亡/替换、有限拓扑、deadline 和观察成本另见同目录 `validation-review.md`；本报告不把它重新包装成新增故障矩阵。

## 审查中已修正的测试问题

最初矩阵在捕获恶意用户异常后直接 `pytest.fail(...)`，会保留恶意异常的隐式 context；baseline red 的 pytest 链式异常渲染可能再次触发故障诊断 hook。审查反馈后，最终代码改为固定文本 `AssertionError` 并 `from None`，且本次哈希包含该修正。修正发生在最终 red 输入冻结前，不改变运行时语义或将失败吞成通过。

## 证据范围与后续验收

静态设计共新增 19 个 unit 参数化实例，原 75 个案例保留，预计该文件收集 94 项；该数不是本审查运行结果。两版须分别保存实际红/绿或相应基线证据、当前输入哈希、依赖、命令、退出码和清理日志，不能用本报告、基础版结果或旧 callable-only 记录代替增强版执行。原 gate/selector 清单及最终 manifest 闭包哈希的完整复核由根任务的登记审查负责；新增 import 仅需真实闭包，不能借此扩大测试收集。
