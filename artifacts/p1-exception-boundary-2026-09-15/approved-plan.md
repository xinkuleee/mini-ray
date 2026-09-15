# P1 重新分析与最小完整修复方案

日期：2026-09-15。状态：**已撤回此前 P1 修改；本文件仅为待实施方案，不是已修复声明。** 两版运行时保持恢复后的源码。最终独立方案审查见本目录 independent-plan-review.md；分析证据与草稿不代替该审查。

## 1. 回退基线和交付范围

| 分支 | 恢复到的提交 | 验证 |
| --- | --- | --- |
| teaching-base | dc86d5f71cbb4fd7dc62a0b24347bb54b42e5602 | tracked/未跟踪工作树干净 |
| teaching-enhanced | b3aca513e29a5da3c092fc607950e2e6e0385b19 | tracked/未跟踪工作树干净 |

撤回两版各 8 个文件的 callable/formatter 改动与配套测试、manifest、完成声明；各 2 个 P1 artifacts 目录移到 audit/p1-reanalysis-2026-09-15/originals 对应分支。原始文件、忽略的 .log、Git diff 全部保留并逐项核对 SHA256。没有 reset 分支、重写提交或推送。main 的历史导航整理保持原样，README SHA 与所有 Git refs 均未改变。

回退到源码基线也恢复了文档在那个检查点的历史“未推送”文字；这不表示远端回退。现在的回退事实以 rollback-result.json 为准，旧 P1 acceptance 仅是被撤回候选的历史证据，不是当前验收。

本次目标是**普通 Task 在用户可执行钩子抛出 Python 异常后，不再以同一物理 attempt 无缓存地重新执行**。覆盖函数/参数解码、callable、输出本地序列化，以及异常分类/诊断；验证从 Start 到缓存/Complete 的责任交接。不是 Actor、所有历史 P1/P2 或任意故障模型的全面改造。

## 2. 独立复核的根因与实际证据

原流程：Node Start 已确认 RUNNING → 执行用户可执行代码 → 尚无 _replies / _prepared_output_replies → 异常逃出请求线程。Node 不会因此认为 Worker 进程死亡；Core 查询到 RUNNING 后重放原 Push；空缓存使同一代码再次执行。Core 不能把 RPC 错误当作任务终态，因此应修 Worker 的执行边界。

“用户可执行代码”不只 function(...)：cloudpickle.loads、参数重建、输出 __reduce__、异常 __str__/__notes__/元类属性访问均可执行用户逻辑。三次局部补丁遗漏的共同原因是没有按这些阶段和托管切点组织异常处理。

两版恢复后均执行了 original-boundary-probe.py，使用真实 cloudpickle/参数编码器、真实 Worker/Node handler 与资源账本，每个场景只发两次相同 Push。10 类有限场景全部确认无 Complete、RUNNING、未释放 allocation、无 reply/prepared cache：

| 场景 | 观察到的重复行为 | 可达性/证据界限 |
| --- | --- | --- |
| callable SystemExit / KeyboardInterrupt | callable 各执行两次 | 普通用户函数 |
| message、traceback、类型名、实例 __class__ 访问抛错 | callable 各执行两次 | 用户函数内定义普通自定义异常，真实函数反序列化 |
| 函数捕获对象反序列化失败 | 重建 hook 两次，callable 零次 | callable 闭包经 cloudpickle 编码/解码 |
| 参数重建失败 | 重建 hook 两次，callable 零次 | 现有 encode_task_argument(..., serializer='cloudpickle') |
| 结果 __reduce__ 抛 SystemExit | callable 两次 | 普通返回对象 |
| 结果 reducer 抛 _OutputCompletionPending | callable 两次 | 额外的内部协议异常隔离检查；需显式导入私有类，不作为推荐 API 或新增能力 |

原始结果：original-base.json / original-enhanced.json；Windows Python 3.12.14，现有禁止 socket、线程、子进程和等待的 tripwire。只验证真实处理器的有限状态变化，不声称完整 live cluster 已复现。Core/TCP 自动重放因果链另由原源码核对。

## 3. 目标行为：按阶段区分错误与可恢复托管

| 阶段 | 应保留的事实 | 失败后的动作 | 允许的重放 |
| --- | --- | --- | --- |
| 尚未取得有效 Start | Node 仍是 lease 权威 | 沿现有 Start 拒绝/未知处理，不伪造 Complete | 现有协议恢复；已知拒绝义务 P2 单独处理 |
| 函数/参数解码、引用导入及 binding 准备 | 还没有输出发布；导入可能已有保活义务 | 捕获该阶段 BaseException，SYSTEM_ERROR，rollback/close 后缓存并 Complete | 相同 Push 只重放缓存；系统策略可在新 AttemptID 内有限重试 |
| callable 和其 binding 作用域 | 用户函数已经开始 | 真正 BlockingNotificationError 为 SYSTEM_ERROR；其余 BaseException 为 APPLICATION_ERROR | 应用异常终态；相同 Push 不调用用户函数 |
| 本地 output discovery/serialize，prepared 尚未安装 | 无 Node 输出发布；源 handle 归本地 discovery | BaseException 按现有 SYSTEM_ERROR，清理后缓存/Complete | 相同 Push 不再执行 callable 或 reducer；不同 attempt 的有限系统重试仍允许 |
| prepared 已准确存入 _prepared_output_replies | 已保存 bytes、manifest、imports；下一步可能已有远端效果 | 只交给现有 publication/resume/补偿路径 | 重放准确 bytes/请求，不能回到 callable，也不能凭异常类型改成普通失败 |
| 错误回复已经缓存、Complete 回复未知 | Worker 终态选择已固定；Node 的 Complete 真相另存 | 保留精确缓存，重试现有 Complete/查询 | 不重新分类、格式化或执行用户 hook |
| Complete 已确认 | lease/资源已收口，应用/系统状态已固定 | 既有 owner 错误传播/有限恢复 | 相同 Push 返回相同缓存；不重复释放 |

**不承诺任务副作用全局 exactly-once。** SYSTEM_ERROR、真实 Worker 死亡和重建的既有策略可能创建新 attempt，函数可能重跑。这与 RUNNING 状态下同一 attempt 无预算限制地重执行不同。结果序列化错误继续保留 SYSTEM_ERROR；不趁修复改变既有分类。

Node Complete 只确认该执行的终态与 Node 资源账本事实；owner 将结果/错误变为可观察状态、回复托管退休及对象 GC 仍是不同责任。这里不新增 owner 可见性提交点，也不用 Complete ACK 冒充 owner 已接管或物理 GC 完成。

## 4. 推荐最小实现：只调整 Worker 中三个边界

### 4.1 用语言异常匹配完成 callable 分类

采用先 except BlockingNotificationError、后 except BaseException 的两个明确分支，分别调用相同 _error_reply 与现有终结链。它按真实异常类型匹配，避免对异常实例做 isinstance 而访问用户 __class__。不引入通用类型/MRO 框架，不要求异常一定继承 Exception；GeneratorExit 等 BaseException 也按本阶段处理。

函数/参数 decode 与本地 discovery 的现有 BaseException catch 保留原 SYSTEM_ERROR，取消这两个阶段对 SystemExit/KeyboardInterrupt 的特殊重抛。没有一个大 catch 包住全部 Push、Node Start 或 Complete。

### 4.2 把 _error_reply 的诊断转换作为一个小操作

在一个 try 内读取 type(exc).__name__、str(exc)、traceback.format_exc()，随后要求三者均满足 type(value) is str。任一步抛 BaseException 或返回非精确 str，则统一改为固定字段，例如：

- type_name = ExceptionDetailsUnavailable
- message = Exception diagnostics unavailable
- traceback = 空字符串

正常错误保留原类型和文本。异常自身的格式化坏掉时，允许丢失全部原诊断信息，明确告诉用户信息不可用；不改变传入 status、task/attempt/worker 身份。一个统一回退比三个独立“尽量挽救字段”的 guard 分支更少，当前教学目标不需要部分诊断恢复。

精确 str 检查防止把带自定义 reducer 的 str 子类带进 wire。不要再次 str/repr 原异常作为回退，不把原异常对象存入 TaskReply。诊断 try 外再构造 RemoteErrorInfo/TaskReply，使合法身份/状态校验仍正常执行，不把协议数据错误伪装成诊断失败。

plan-language-check.py 已有限验证异常匹配不会调用实例 __class__、故障诊断会变成纯 str、普通类型和 SYSTEM/APPLICATION 分类正常，pickle roundtrip 成功。这只是方案所依赖 Python 行为的检查，不是 miniray 实现或验收。

### 4.3 用代码块边界区分 discovery 与 publication

本地 discovery 的 try 只负责创建会话并完成 discover(value)。在该 catch 中，不再按 _OutputCompletionPending 之名放行用户异常。成功后，在 catch 之外创建/安装 _PreparedOutputReply，转移 nested_imports，再调用 _resume_discovered_outputs。

这样未知 prepare/promote/Complete、成功输出补偿及 source/import 交接异常，都留在已经存在的 prepared custody 协议中；本地失败只在没有远端输出发布前终结。无需改 Core、Node、GCS、图或 wire protocol。

既有 CRASH_AFTER_NESTED_IMPORT 是主动的真实进程终止注入，需与 decode 的异常归类代码分离：保留 acquired 引用检查在受保护准备阶段（无 nested ref 仍 SYSTEM_ERROR），在导入 commit 成功之后、binding 创建及 callable 之前执行 os._exit，且位于 decode/callable catch 外、现有引用 finally 内。它实际不会返回；不要增加识别测试 _NestedImportCrash 的生产分支。对移动后的 binding 准备失败仍单独使用 SYSTEM_ERROR。此局部块移动用于清晰区分真实死亡与 Python 应用异常，保持现有 failpoint 顺序、borrower 存活点和已有测试语义。

## 5. 必要清理与权威不变

- _execution_lock、accepted Push 精确身份、Start 校验、cache-before-Complete、Complete ACK 校验、owner 死亡 fencing 全保留。
- decode 失败 rollback；pre-publication finally close imports；两者在普通失败 Complete 前完成。不能为避免挂起直接忽略未知清理结果，也不能先缓存让 replay 跳过尚未交接的 imports。
- OutputDiscoverySession 自身已有 BaseException→ABORTED/清空源 handle。没有远端发布前不新增远端补偿协议。
- runtime imports 是 Core 创建的具体 ObjectRef，close 记录本地 release intent，远端 release ACK 由原 Core 义务推进。本次没有受支持调用链证据表明任意用户 .close callback 可注入该 session，因此不扩大为依赖模块的吞异常改造。
- prepared 成功路径保持原 source/import 退休责任，不能因为本地修复而删记录、回滚已成功事实或重新序列化。
- 真正 os._exit、OS kill、Worker 主线程中断继续是进程级行为；Driver 主线程 Ctrl-C 不进入这里。运行时全局/内置类型被篡改、ObjectRef 私有状态破坏、永久不返回 hook、内存耗尽等不属于这次有限 Python 异常保证；不以此排除普通自定义异常/序列化钩子。

## 6. 为什么不选更短或更大的替代方案

| 方案 | 不采用的具体原因 |
| --- | --- |
| 再补类型名或只删 callable 重抛 | 同根因的 decode、discovery、分类和 wire 文本入口仍会漏；前轮已证明局部补丁不足 |
| 机械删除三个重抛 | 仍有诊断/分类风险，且原 discovery catch 跨过 prepared/publication 切点，会误改协议恢复行为 |
| 全 Push 一个 catch，统一报 SYSTEM_ERROR | Start 未接受、已有 prepared、Complete 已提交、清理未知不能合并成同一个新结果 |
| 直接杀 Worker | 会扩大为 Worker owner/子任务丢失及系统重试，改变用户异常语义，并依赖其他死亡收口路径；不是等价最小修复 |
| 新增 EXECUTING/FAILED 状态表或中央事务 | 重复现有 reply/prepared/Node 权威，新增退出、退休、死亡清理成本；当前可达缺口无需新增长期状态 |

本地 Ray 参考提交 c3162dce8d064824293875c5d0bbfd76a54e04ce 的 _raylet.pyx 在更外层把 SystemExit/未处理 BaseException 转为 C++ Worker-exiting 状态。mini-Ray 的 socket 请求线程没有该外层机制，所以内层照搬 rethrow 没有对应终态责任。这里选择教学版的局部错误终结语义，不宣称完整复现 Ray 的退出/取消机制。

## 7. 精确修改范围与实施顺序

实施时两版分别修改 src/miniray/worker.py 的 _error_reply、_handle_admitted_push_task 局部块；不重写整个方法族。测试优先复用各版现有真实 Worker/Node 单输出夹具，并保留 enhanced 自身 adapter/authority。

必要配套：tests/unit/test_worker_unified_output.py 的有界失败闭环，tests/unit/test_worker_nested_task_arguments.py 的顺序与借用回归，tests/integration/test_task_retry_path.py 的一条有限集成场景，以及 scripts/baseline_manifest.json 的精确闭包/selector 登记。只有实际发现需要时才调整同文件现有断言，不降校验、不扩目录收集。状态/设计说明在两版验收后同步，旧 R2.3 历史不改标。main 不改。

一次完成全部三个边界后再做独审，不能把“先只改 callable、以后再补其他入口”当本方案完成。先 B 留红/绿证据，随后 E 独立映射与验收；不用 B 结果代替 E。

## 8. 有限验收合同

| 组 | 必须验证的有限证据 | 不扩成的故障矩阵 |
| --- | --- | --- |
| 用户入口 | 本目录10个原始场景分别转成真实 Worker/Node 回归；补一个普通异常对照和真正 BlockingNotificationError/子类对照 | 不与全部 death/GC/图窗口做笛卡尔组合 |
| 实际序列化 | 参数/function decode 和 result reducer 用实际编码器；诊断加入返回带 reducer 的 str 子类，TaskReply 实际序列化/反序列化后只有普通字符串 | 不仅调用诊断 helper 就宣称闭环 |
| 核心不变式 | 同 Push 重放不再次调用 callable、输入解码重建 hook 或结果 reducer；有限单钩子用例总计最多一次，decode失败时 callable零次；Complete terminal、精确状态和lease释放一次；清除执行义务；同 Worker 可执行下一 Task | 不宣称跨新 AttemptID 副作用 exactly-once；诊断 hook 不套此次数保证 |
| ACK 未知 | 一个应用失败和一个 SYSTEM_ERROR 分别在真实 Node Complete 后丢响应，再重放；缓存身份/状态不变、不重执行/重复释放 | seam 证据不写成 live TCP 丢包 |
| prepared责任 | 原成功/补偿/ACK未知/拒绝身份测试通过；加一项 prepared已安装后 resume 抛控制流异常再重放，要求只恢复托管，不生成另一普通失败或重执行 | 不重新设计增强事务或猜未证明的故障组合 |
| 引用与真实进程 | 每版一个有 nested ObjectRef 的真实三进程场景：应用异常及可格式化/不可格式化诊断到 Driver、借用释放有证据、同 Worker 后续任务、原有限shutdown/PID/端口清理 | close本地收据不冒充全部物理GC |
| 既有行为 | Worker completion、nested argument、原 SYSTEM_ERROR有限重试及真实Worker死亡测试按原有明确 selector/期限执行；保留 nested-import crash 的原观察顺序 | 不扩大到历史全树 |

测试命名/断言分别标记 APPLICATION_ERROR、SYSTEM_ERROR新attempt重试和prepared恢复；有差异的行为不得归一化成“都只执行一次”。两版执行输入 SHA、依赖、命令、退出码、日志、红绿区别分别保存。日志需要明确纳入后续提交，不能遗漏被 *.log 忽略的证据。

诊断转换首次处理会调用 str(exc)，traceback格式化也可能再次调用异常 __str__，因此不保证诊断 hook 在首次处理内只调用一次。要求是诊断失败不能阻止终态生成，且缓存后的 Push 重放不得再次执行分类、诊断或用户执行钩子。

## 9. 审核与退出条件

本方案形成后另请未参与编写的审查者从恢复源码审核：每个用户钩子是否落入正确边界、诊断是否纯文本、阶段语义是否保留、prepared是否可能被覆盖、真实死亡注入/引用清理是否仍成立、验收是否能识别同attempt重执行。发现问题先修本方案，再审核最终文本。

只有实现后的这些证据实际通过，才可声明“普通 Task 在本方案所列有限 Python 异常场景下完成终结，且同一 Push 不再重执行”；不声明项目全部 P1 清零。已知 Start拒绝义务、drain后死亡、正常Worker payload退休三个 P2 不属于本包。回退已撤下前轮集成新增测试，因此其中旧清理诊断P3也随测试移出，不可继续称它仍在当前测试。
