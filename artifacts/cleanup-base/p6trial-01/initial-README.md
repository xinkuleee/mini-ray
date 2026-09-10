# P6 TaskSubmissions 最小隔离试做

日期：2026-09-10。依据：R2.2 JSON 的 P6 与计划 §4.3。此目录是实际重构候选及静态评估，不是另一份实施计划。

**建议：该切片目前收益不足，保留现状。** 这是有代码试做依据的建议；P4 尚未给出处置，且本候选未运行，因此不冒称 P6 已验收，也不自动把建议登记为最终处置。

## 实际产物与范围

- input/ 固定当时 teaching-base 工作树源码、测试、runner 与依赖；input-identity.json 记录 42ed62d7231d17d3b7f703670a357c509183e853 HEAD 及每个文件实际脏工作树 SHA256，不能把 HEAD 单独当成受测版本。
- candidate/ 是可供 root 后续登记和冻结的完整副本。实际改写 CoreWorker._register_submission，新增 task_submissions.py，同步两个 encoder 注入点。
- candidate.patch 为相对于本目录 input 的准确四路径增量。
- metrics.json 为静态计数与状态归属，finite-selectors.json 为未运行的有限验收选择项。
- build_candidate.py 与 close_candidate.py 是仓库外生成/核验脚本；已运行。没有执行 pytest、修改 manifest、提交、推送或改变 base/P1/P4。

本次选“参数准备 → 现有 owner/recovery 本地提交 → 未提交时输入撤销”生命周期。未提取 LeaseAttempts、OwnerObjects、finish/drain，也未修改调度、重建、Actor、PG 算法。PG 测试的改动仅是 encoder 所在模块改变后的原 failpoint 接线。

## 候选到底改变了什么

TaskSubmissions 是每次提交建立、随该提交结束的对象，独占原闭包中的参数发现与输入 hold 工作账本。它接收 typed SubmissionInputCustody，只使用 owner 的 add/release submitted hold 和明确的 local waiter、foreign retain/release/orphan、GC enqueue、ObjectRef 类型判断能力。不接收 Core，也不从新模块 import Core，不使用 mixin、getattr 或动态方法字典。

Core 保留任务身份预留、inflight/accepted 计数、PG 最后准入判断、输出 ref 准备与绑定、owner/recovery/foreign-lineage 注册、waiter 与 queue。准备 foreign hold 仍在 Core 锁外；commit_local_inputs 仍在原 owner/recovery 组合锁内调用。queue put 仍是最后提交点；异常撤销顺序仍为已绑定结果与 authority 撤销、lineage release、input holds reverse release、orphan fallback。远程 ACK 校验与死亡 fencing 仍调用原 Core reducer。

_ForeignDependencyGuard 的唯一定义移到输入领域，为消除循环 import 以 TaskDependencyGuard 命名；Core 的同名 import alias 只绑定到同一个 class，不创建第二套凭据类型或旧签名后端。现有 finish/retry/foreign-lineage 消费者仍用同一 immutable guard。

## 前后量化

| 项目 | 试做前 | 试做后 | 解释 |
|---|---:|---:|---|
| Core 物理行 | 12,763 | 12,512 | 下降 251 |
| 新增输入模块 | 0 | 246 | 总源码仅净减 5 行 |
| _register_submission 方法物理行 | 507 | 276 | 原闭包与输入工作移入领域 |
| 受影响源码 AST 节点总量 | 67,016 | 67,757 | 增加 741；不能把排版压缩当简化 |
| 方法直接 Core 私有访问次数 | 40 | 35 | 是方法内 self._x，不是跨模块访问 |
| 方法私有依赖种类，含 getattr | 24 | 23 | 只少一个 guard-key 依赖，核心耦合基本仍在 |
| Core 读取新领域私有字段 | 不适用 | 0 | 只读属性/DTO，调用明确方法 |
| 新领域读取 Core 私有字段 | 不适用 | 0 | 没有 Core 引用或 import |
| 新 typed 能力字段 | 0 | 7 | owner + 6 callbacks，owner Protocol 只声明 2 方法 |
| 新 Protocol 类型 | 0 | 2 | input reference 与 input owner |
| 每提交新增对象 | 0 | 4 | domain、capability record、encoded call、submitted inputs |
| 新长期 Task 成功表 | 0 | 0 | success 仍在原 authority |
| payload 编码/deepcopy 次数增量 | 0 | 0 | tuple/凭据引用不深复制 bytes |
| Core 其它方法 AST 完全不变 | — | 234 | finish/shutdown/execute 等均未改 |

物理行比较包括空行、docstring 与注释，不是实测可执行代码预算。AST 节点计数同样不代表运行开销或性能预测，只用于识别“Core 看起来变短，但总结构不一定变小”。

## 状态唯一归属

| 状态/事实 | 原拥有者 | 候选拥有者 | 是否减少长期状态 |
|---|---|---|---|
| protected、foreign guards、nested local/foreign holds、nested sources/transfers、reference list、lineage ID 发现，共 8 组工作 collection | Core 方法局部变量 | per-call TaskSubmissions 私有字段 | 否；局部所有者明确化 |
| submitted/lineage hold 真值 | ObjectOwnerTable | 原表 | 否，不能移动为成功 bool |
| Task 成功/当前 attempt | owner + RecoveryManager | 原 authority | 否，无重复表 |
| foreign lineage admission | 原 ForeignLineageRegistry | 原 registry，经 Core 锁提交 | 否 |
| orphan foreign release、死亡 tombstone | Core 既有 reducer | 原 reducer | 否 |
| finish barrier、active/finished/finishing、shutdown drain | Core | Core | 否，0 项长期 drain 职责改善 |

新 DTO 只包裹原本传给 PendingTask 的四个 tuple，未深复制引用对象或另存 payload。额外创建 domain/capability/DTO 和若干 bound callable 的成本是真实存在的；本次没有做运行时间或内存测量。

## 为什么暂不推广

输入工作归属变清晰是实际收益，但切片仍有 23 种 Core 私有依赖和原全部 finish/drain 状态。读取一次 nested hold 的建立与失败收口，需要从 Core 接线进入 domain，再回到原 owner/RPC handler，然后返回 Core authority 提交。原来也要看 owner/RPC；现在额外有七字段适配记录与两份边界 DTO。

该试做没有缩小长期 pending 职责表，也没有减少 owner/recovery 组合状态；净源码减 5 行主要来自新模块较紧凑排版，AST 与接线成本实际增加。为了得到更大的职责收益，需要继续移动 finish/orphan/drain 生命周期，届时将跨越当前许可切片和 P4 未封的 pending work。不能用这种未来可能性把当前试做判作已获净收益，也不能在本次试做顺手扩大范围。

建议记录“此准备/提交切片暂不采纳，保留原 Core 方法”，同时保留候选与指标供 P4/P1 完成后重新判断。该建议不豁免 CF 正确性修复、fixture 兼容移除或单输出清理，也不作为 B/E 交付新门槛。

## 静态证据与尚未证明项

已确认：四个变化文件 AST parse 与 compile 通过；Core 除 _register_submission 外 234 方法 AST 完全一致；新模块没有 Core import/引用/getattr；Core 没有访问 domain/DTO 私有字段。完整 test 搜索找到两项 core_module.encode_task_argument 注入点并改到实际 encoder 新模块，原断言和 marker 保留。TaskDependencyGuard 字段与校验来自原唯一定义。

**未做运行验收。** 特别尚未证明：准备失败后确切 release/orphan 路径、queue 拒绝后的共同 rollback、remote retain ACK unknown、nested/top-level 共用 hold、GC 与系统重试在候选中的组合行为。finite-selectors.json 列出原 ordinary scalar、nested local/foreign Ref、精准 rollback 等 9 个 pure selector 和 2 个现有 smoke，可供 root 如要提升候选时按当前 reviewed runner 有界执行；历史通过不能算本候选通过。

原 PG failpoint 测试仍为 heavy，不能因只是接线变动就混入 pure 或称已评审有界；若采纳候选必须另核其现有 helper 闭包并保持原标记。P1 接受后还需重新对齐 TaskExecution/单输出 DTO。P4 未形成实施或有据保留决定前，此目录仅为隔离试做。
