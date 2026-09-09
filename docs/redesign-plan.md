# mini-ray 两阶段实施计划

日期：2026-09-09。原始分析基线：`ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e`。

后续维护说明：用户已要求同时整理并保留两条实际Git分支。新的执行安排见[双分支完整整理计划](project-cleanup-plan.md)：`teaching-base`参照Ray Core关键机制，`teaching-enhanced`基于前者增加两项mini自定义协议保证。
本页继续作为两阶段已确定的能力、正确性与验收合同；原tag和证据保持不变，后继两分支各自绑定新源码与证据，不能仅用两个tag交付。
本次整理先验收B，再从其交付HEAD派生E并保留B；main保留旧增强源码、追加双分支导航，不作为第三个整理版。不在一个运行时增加双后端开关。

**两阶段目标已确定：先交付不含 GCS 普通结果发布事务与全局引用图防环的基础版本，再基于该版本交付同时包含这两项的增强版本。第二阶段是计划内的必交付阶段，不再是“有需要再做”的候选项。**

第一阶段保持单输出、真实多进程、对象所有权与 nested refs，退出独立多返回槽与 targeted reconstruction 等范围；第二阶段只增加上述两项协议保证，不自动恢复其它退出或延期能力。原两阶段在同一仓库顺序演进；本次后继整理按上方计划交付两条实际分支。

这是按用户最新决定修订的实施计划，第一阶段已按本计划独立完成约定行为验收；当前证据与剩余项见 [验收账本](acceptance-baseline.md)。下文状态表保留设计合同，不能替代实际测试；第二阶段两项机制已在增强snapshot03同版通过约定验收，固定标记为`teaching-enhanced-v0.2`。此前“仅文档、不改源码/测试、不提交”的限制属于计划修订轮，已由后续实施指令推进到实现阶段。旧 [correction-plan.md](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/docs/correction-plan.md) 作为历史设计来源；其中等待确认的旧流程不覆盖本次已确定的两阶段顺序。下文 §1–9 说明第一阶段基础版本；§10 说明第二阶段增强版本及两阶段交付衔接。文中“本版”未另行限定时指第一阶段。

## 0. 两阶段总览

| 阶段 | 交付内容 | 发布边界 |
|---|---|---|
| 第一阶段：基础教学版 | 用单输出、owner-led 的真实执行链理解 Ray Core 关键机制；无 GCS 普通输出事务、无全局 ObjectID 图防环 | §6 P0–P5 与 §7 基础行为验收完成，保留独立可运行的源码版本、教材和测试证据 |
| 第二阶段：协议增强版 | 基于基础版，额外研究 mini 自定义的中央发布事务与全局防环保证 | 两项都进入真实运行时并通过新增保证及组合窗口验收，同时复验保留的基础行为；不能以仅实现其中一项或纯模型通过结束 |

基础版是参照Ray Core所选机制的mini教学实现，定位类似mini-sglang与SGLang、nano-vllm与vLLM；对应、简化和省略分别说明，不承诺生产Ray的完整实现、接口兼容或相同内部协议。基础版不缺少本项目所选 Ray Core 机制的必要正确性逻辑；增强版也不表示“更接近生产 Ray”。增强收益是多一个存活的发布事实来源、对受支持 ObjectID 边的全局成环拒绝；代价是普通成功路径增加同步 GCS 依赖，以及预留、失败补偿、死亡清扫和收据退休的额外状态与测试。两版要分别讲清实际路径，不能用增强版保证反向贬低基础版。

顺序为：**第一阶段实现与验收 → 固定第一阶段版本 → 第二阶段实现与验收 → 固定增强版本。**第二阶段不作为第一阶段验收前提；第一阶段通过后按本计划继续第二阶段，不再把这两项是否需要实现变成一次选择。

阶段交界保存源码提交/不可变版本标记、Python/OS、依赖锁定与解析结果、精确示例/测试选择器及参数、退出结果和规范化 trace。只有这些基础版证据完成且可据此检出运行，才进入第二阶段；实施时在基础版验收完成后建立对应提交与标记；基础版同版验收已通过，本地提交/标记为`teaching-base-v0.1`。第二阶段新增用例不倒灌为第一阶段门禁，发现共同基础中的真实缺陷则另行记录并按受影响行为修复，不改写已经执行的历史结果。

增强版沿基础版演进，不在运行时长期维护“基础/增强”两套后端或功能开关。第一阶段不提前保留旧事务空壳、全局图接线或通用扩展框架。即使增强版成为最新版本，README/学习路径仍持续推荐基础版分支及其固定验收提交作为第一次学习入口，提供对应依赖、示例和源码地图；增强版另设协议增量入口，基础版不只作为一个难以发现的历史提交。

### 0.1 两阶段共同保留的正确性基础

| 公共基础 | 第一阶段必须保留 | 第二阶段怎样复用 |
|---|---|---|
| 完整请求身份 | job、owner/执行者/Node incarnation、Task/单 ObjectID/attempt，Task lease 或 put operation，清单 digest、hold token 与原请求绑定；身份相同但字段改变要拒绝 | 用同一身份关联 GCS/图收据，不重新发明一套可与 owner/Node 身份独立推进的 publication ID |
| 职责边界 | owner 管逻辑可见性和 outgoing；Node 管 execution/资源/副本；child owner 管 incoming hold；各自状态有明确提交点 | GCS 只增加准入、准确发布事实和图协调，不复制 READY、引用真相或资源账本 |
| 结果交接 | 单次序列化、准确 bytes/descriptor、owner 待交接清单、Node Complete、owner CAS、真实接管 ACK | 在既有交接点增加特定门禁；无须重写普通执行、pull 或另造结果后端 |
| 引用保活 | source/import 保留到交接或精确补偿；Task/lineage/borrower/contained hold 各有生命周期 | 图记录覆盖对应 contained 边，不能替代真实 Acquire/Release 或把 source 临时 hold 当最终 outer 边 |
| 精确收据 | 已发生的准入、Complete、adoption、撤销/释放事实与当前对象状态分开；历史重放不重新产生效果 | GCS 保存相应事实的准确记录；查询历史与前进许可仍分开，不能由旧 ACK 复活已退休 epoch |
| 清理责任 | 未知效果、partial write、child holds、reply/source 托管和资源分别有负责人；有 fence 不等于已有资源已释放 | owner 活时继续由 owner 推进交接，Node/child owner 执行本地清理；owner 死后的 GCS 协调仅接续已登记清单，见 §10.3 |

“退出中央协调机制”只删除 GCS 普通发布阶段、全局图决策与专属接线；不能删除上表的必要正确性逻辑再在第二阶段重造。先逐项标明旧代码/断言属于公共不变量还是额外保证，提取现有职责的窄接口；不为未知扩展预建框架，也不把交接、补偿和图协调重新集中进 Core。旧协议族仅作为可追溯材料，第二阶段按单输出合同复用适用算法与断言，不能整体恢复。

## 1. 当前代码量是否合理

### 1.1 判断

**对一个覆盖大量故障交错的分布式协议实验原型，许多代码有技术意义；对 mini-SGLang 定位的 mini-ray，整体规模和复杂度分配不合理。**

不合理之处主要是选择了过宽的行为合同，再为这些合同增加多层事务、状态与测试。很多局部代码解决了真实问题，但问题本身不是这一教学版本必须承担的。纠偏应先减少承诺和独立状态，再简化实现；不能把必要的引用保活、身份校验或故障清理当成冗余删掉。

### 1.2 数字与统计口径

此前对上述分析基线使用 AST 识别 docstring，并按 Python token 划分物理行。下列“代码行”指含代码 token、排除纯注释/docstring/空行的物理行，包含声明、类型注解、多行表达式和字符串载荷，**不是可执行语句数，也不是可删除行数**。本节表格属于改造前原始分析；实施中的2026-09-09同口径校准见§8.1，不把历史表格冒充当前规模。

| 范围 | Python 文件 | 物理行 | 代码行 |
|---|---:|---:|---:|
| `src/` | 58 | 67,167 | 55,196 |
| `tests/`，含 helper | 322 | 126,049 | 106,251 |
| 两个测试 runner | 2 | 769 | 614 |

源码另有 3,832 行纯 docstring、2,137 行纯注释、6,002 行空行。代码行占 **82.2%**，说明膨胀不是教材注释太多。测试代码行约为源码的 **1.92 倍**；这个比例本身不异常，真正的问题是测试大量追随内部协议阶段和手工夹具，而当前仍没有可靠的完整交付基线。

| 集中位置 | 物理行 | 代码行 | 解释 |
|---|---:|---:|---|
| `core.py` | 13,953 | 11,841 | 提交、引用、恢复、发布、退出等责任交织 |
| `node.py` | 7,507 | 6,509 | 本地 lease/资源之外还推进多种发布与清理协议 |
| `protocol.py` | 7,290 | 6,058 | 207 个 class，含消息、枚举与值类型；不是 207 个 RPC |
| `ownership.py` | 5,367 | 4,365 | 必要引用模型与多槽、发布、退休状态并存 |
| `control.py` | 5,047 | 4,238 | GCS 控制职责与普通结果事务混合 |
| 发布/全局图相关独立模块组 | 5,445 | 4,261 | 11 个文件，不含上述大文件中的接线；并非都应删除 |

五个大文件合计 39,164 物理行，占源码约 58%。模块组为互斥文件归类，具体名单见 [统计数据](C:/Users/t-hdong/Desktop/gao/audit/mini-ray-complexity.json) 与 [统计脚本](C:/Users/t-hdong/Desktop/gao/audit/measure_mini_ray_complexity.py)。不能把整个组的行数当作预计节省量。

mini-SGLang README 自述约 5,000 行，nano-vLLM 自述约 1,200 行，两者和本次统计口径不同。分布式运行时要实现引用生命期、远端结果不确定性和进程收尾，本来也会比紧凑推理引擎复杂。它们值得借鉴的是**少量核心机制真正进入执行路径，代码规模足以整体阅读**，而非一个统一行数。

### 1.3 哪些膨胀有意义

| 成本 | 为什么值得保留 | 能缩小什么 |
|---|---|---|
| 真实进程、消息、启动回滚和退出 | 不存在这些边界，就无法观察分布式执行和进程失败 | 缩到单机 1–2 Node、固定少量 Worker，不做部署系统 |
| owner、borrower、task/lineage/contained holds | 对象不能因发送者先释放而提前消失；逻辑引用与物理副本分开是 Ray 的关键 | 一条显式引用协议、一种完整身份，退出历史 token 兼容形态 |
| lease/本地资源账本/依赖 gate | 解释谁决定资源、为何 pending 依赖不能占 Worker、为何可以 spillback | 简化策略和 Worker 管理，不删资源守恒 |
| attempt/generation、去重、重试预算 | 回复丢失和执行失败不是同一回事，旧结果不能覆盖新执行 | 缩小故障模型，不承诺所有故障组合或外部副作用 exactly-once |
| seal/pin/pull/副本回收 | 区分对象身份、位置、字节；否则大对象只是随任务重复拷贝 | bytes store 即可，零拷贝和 spilling 延期 |
| 清理义务与 tombstone | 超时不能证明远端效果没发生，迟到消息不能复活已释放引用 | 合并重复台账、按生命周期退休，不能任意 TTL 丢掉未知效果 |
| 因果 trace 与代表性故障测试 | 让读者看到跨进程机制发生了什么 | 聚焦稳定业务事件，不锁死每条内部调用 |

### 1.4 原始分析中哪些膨胀不符合基础版目的

本节描述原始版本`ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e`；链接固定到该版本，退出项不再描述当前活动源码。

1. **自创中央发布事务。** 普通输出同步走 GCS INTENT/ARM，再报告 terminal/adopted；加入全局成功历史及阶段恢复。它服务更强的 mini-specific 保证，却改变了读者应理解的 Ray 普通成功路径。[源码依据](https://github.com/xinkuleee/mini-ray/blob/ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e/src/miniray/output_publication_node.py#L165)。
2. **全局 contained DAG。** 每次发布需要全局图 prepare/commit/abort，以拒绝跨 owner 成环。引用计数本身不要求一套全局防环服务。[源码依据](https://github.com/xinkuleee/mini-ray/blob/ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e/src/miniray/contained_cycle.py#L288)。
3. **多返回槽的组合复杂度。** 成本不在 `num_returns` 参数，而在“一个函数执行＋多个对象不同存储层/生命周期＋部分丢失＋健康 sibling 保持不变＋共享预算＋最后 sibling 回收 lineage”。`TargetExecutionKey` 进入 Task/lease/Worker/owner 协议，影响普通路径。[身份定义](https://github.com/xinkuleee/mini-ray/blob/ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e/src/miniray/task_outputs.py#L123)、[消息校验](https://github.com/xinkuleee/mini-ray/blob/ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e/src/miniray/protocol.py#L98)。
4. **超出当前公开能力的预备实现与兼容适配。** 例如 Actor 引用参数 reducer 尚未接入公开运行时；token-only legacy hold 和旧 pickle 路径继续增加身份形态；placement helper 适配多种资源接口。先查活动调用者，再迁移或退出，不能仅凭名称删除。[Actor reducer](https://github.com/xinkuleee/mini-ray/blob/ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e/src/miniray/actor_arguments.py#L1)、[legacy hold](https://github.com/xinkuleee/mini-ray/blob/ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e/src/miniray/contained_edges.py#L66)。
5. **围绕历史实现而非行为建立门禁。** 多份分类守卫重复锁函数名/计数，纯夹具手工组装大批私有字段。原始 targeted 草稿就因为夹具先关闭 session、再查询必须存在的 session 而失败，尚未到达目标竞态。[夹具](https://github.com/xinkuleee/mini-ray/blob/ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e/tests/unit/test_targeted_reconstruction_first_ack.py#L337)。

把这些实现移到高级章节只能降低第一遍阅读成本，**不会减少运行时复杂度**。第一阶段须从活动代码、消息和测试义务中移除中央事务、全局图及已退出能力的专属路径，但保留 §0.1 的身份、交接、引用和清理不变量。历史保存在 Git。第二阶段复用这些公共基础实现两项额外保证，不把旧协议族整体搬回来；不留另一套长期后端或 feature flag 分叉。

## 2. 明确的产品目标与边界

读者完成本项目后，应能回答：

1. `.remote()` 如何变成一个 Worker 上的执行，为什么需要 lease 和 direct submission？
2. 普通任务、Actor、PG 分别由谁决定位置、状态和资源？
3. ObjectRef、owner、borrower、replica、bytes 是怎样不同的实体？
4. 依赖等待、对象 pull、嵌套 get 与 CPU 资源如何配合？
5. 对象丢失或 Worker 死亡后为什么可以重执行，哪些情况不能恢复？
6. 同一个逻辑 ID 如何对应多个物理 attempt/generation，如何屏蔽旧消息？

推荐 v0.1：Python-only、单 job、单 GCS、单机 loopback、1–2 逻辑 Node、每 Node 1–2 普通 Worker；串行 Actor 使用独立 Worker。完整进程验收以 Linux/WSL 为主，当前 Windows 只做受审纯合同等已支持层级。先选择 Python 3.12 为验收基线，广泛版本声明必须与证据相符。

故障模型为受管进程 fail-stop、有限 RPC 超时/回复丢失和明确注入窗口。真实运行中遇到未覆盖组合仍须失败明确、不可伪造成功或 clean；不承诺网络分区恢复、GCS 重启接管、任意多重故障下持续可用。

## 3. 保留什么、退出什么，以及原因

### 3.1 保留的基础机制

| 能力 | 本版保留形态 | 理由与验收重点 |
|---|---|---|
| Task / ObjectRef / remote / get / wait | 每次 Task 返回一个 ObjectRef；值可以是 tuple/list/dict；wait 保 metadata-only | 单输出足以说明异步数据流；`wait(..., num_returns=k)` 的 k 表示等待个数，与 Task 多返回无关，必须保留 |
| 逻辑 ID 与物理身份 | TaskID/ObjectID 稳定，AttemptID/Worker incarnation 可变 | 重试、消息重放和旧结果 fencing 的共同基础 |
| 真实进程拓扑 | GCS、Node、普通 Worker、Worker 内嵌 Core | 动态子任务必须走同一后端，不能模拟成 Driver 调用 |
| lease / spillback / direct PushTask | 提交方取得 Node lease 后直接发送 Worker | Ray 任务路径的核心职责链；不改成 GCS 转发队列 |
| 资源与调度 | 精确 CPU/自定义资源向量、feasible/available、Node 最终账本；小型 Hybrid/locality | 保调度问题本身。locality 只是首跳建议，旧摘要不能使 Node 超卖 |
| Worker pool | 固定 1–2 slots，预启动且复用进程，每 attempt 一个 lease | 已足够显示并发与 lease；动态扩缩及 leased-worker pipeline 属性能/运维扩展 |
| 对象存储 | 单对象 INLINE/STORED、create/write/seal、容量、pin、校验与跨 Node pull | inline/stored 区分是对象系统核心；取消的是同次任务多槽 mixed，不是两种存储层 |
| put | 显式创建单对象、无 producer lineage；含 Ref 值已接入单独的 put operation、交接和原子 owner 安装；证据见验收账本，基线原快照曾拒绝 | 教会大对象复用和“并非所有对象都能重建”；不能把尚未接通的引用编码说成已有能力 |
| owner / borrower / nested refs | 单 outer 对象可持 child refs；Task 调用的顶层 Ref 参数为执行依赖，nested Ref 为数据/生命期边 | 足以展示 ref-as-data、owner 与持有者分离、borrower 可比 outer 活得更久；这是值得承担的复杂度 |
| GC 与副本清理 | 最后真实存活理由消失才回收；引用、metadata、bytes、lineage 分别证明 | 避免用 close ACK、线程退出或回复缓存退休冒充物理 GC |
| 动态子任务与 CPU yield | Worker 调 remote/get；阻塞时仅 yield CPU，unblock/Complete 一次清账 | 解释单 CPU 嵌套任务如何避免资源死锁；动态嵌套与容器内 nested ref 是不同概念 |
| retry / lineage | 单输出、有限 whole-function replay、简单递归依赖 DAG、foreign owner 路由 | 保恢复机制，取消跨输出槽协调；应用异常默认终态，系统失败受预算控制 |
| 死亡事实 | 受管 Worker/Node 检测和有序传播；timeout 不判死 | owner 不变，先找存活副本，再决定重算；owner 死后明确失败 |
| trace / debug | 跨进程因果、任务/对象/资源状态、有限 drop 故障入口 | 观察不参与正确性；故障入口用于实验，不做通用管理平台 |

### 3.2 保留为高级实验，但仍是同一后端

| 能力 | 冻结范围 | 为什么保留，为什么止于此 |
|---|---|---|
| Actor | GCS 创建、固定 ActorWorker、串行方法、每 caller FIFO/去重；方法返回普通值的 ObjectRef | Actor 是不同于 Task 的核心抽象。值内含 ObjectRef 的 Actor 参数/结果本版不支持 |
| Actor restart | 存活同 Node 内有限重启、构造器重跑、generation 递增、旧调用 fencing；构造失败 typed 分类 | 展示逻辑 Actor 与物理 incarnation 的区别；不透明重放跨代方法，不承诺状态恢复 |
| Placement Group | 最多两个 bundles，`STRICT_PACK` / `STRICT_SPREAD`，真实 prepare/commit/abort、移除、participant loss→LOST | gang reservation 是独立机制，值得保；两种硬约束已经能说明协同预留与区别，不需要通用装箱优化 |

PG 必须保留真正的全 ACK 前不可见、prepare 失败回滚及 bundle 隔离。保 PG 的协调事务与删除普通 Task 的 GCS 发布事务不矛盾：前者是明确要求多个 Node 原子预留资源，后者给普通结果增加了一项额外中央权威。

### 3.3 明确从本版活动实现退出

| 退出项 | 理由 | 失去的行为 / 迁移方法 |
|---|---|---|
| `remote(num_returns>1)` 及独立返回槽 | 同一执行与多个独立对象生命周期耦合，是当前复杂度主要放大器 | 函数仍可 `return a, b`，`ray.get(ref)` 得到一个 tuple；不能独立等待/释放/重建其中某一槽。大于 1 在提交前显式拒绝，不暗改含义 |
| selected-set / targeted reconstruction | 单输出后不存在健康 sibling 与 LOST 槽的隔离问题 | 删除 target mask、OPEN/MERGE/QUEUED session、逐槽 producer epoch、last-sibling GC；每个丢失结果重算整个函数 |
| 多槽 mixed-tier 发布 | 由多返回槽衍生，缺少独立教学价值 | 一次返回只选择 INLINE 或 STORED；不同 Task 仍可分别使用两层 |
| GCS INTENT/ARM/terminal/adopted 普通结果门禁与全局阶段历史 | 第一阶段先恢复 Ray Core 基础主链，第二阶段再单独展示中央发布协议 | 第一阶段由 owner/Node 持有收据，部分窗口为 UNKNOWN；第二阶段增加 GCS 已记录的成功事实与事务门禁，见 §10 |
| global contained DAG / 全局 fail-fast 环拒绝 | 第一阶段不承担额外全局协调，第二阶段将防环作为明确增强保证 | 第一阶段不保证 ObjectID 环被全局拒绝/GC；第二阶段增加全局成环拒绝与图生命周期。两版均不混淆普通 Python 容器环 |
| 旧 token-only hold / 历史 pickle alias / 无活动调用者的预备 reducer | 当前不是跨版本兼容产品，额外身份形态降低可解释性 | 核查并迁移现有调用者后删除；只支持同 job/同版本协议；旧实验由原 commit 重放 |
| 重复分类计数锁、只验证退役阶段的测试 | 这些锁维护历史形状，不能证明新行为 | 保行为不变量，合并执行政策和 helper；每项删测给去向，不靠 xfail/删红测取得绿色 |

### 3.4 延期的能力

| 延期项 | 充分理由 | 本版替代/边界 |
|---|---|---|
| 自动大参数 lift / 隐式 StoredArg | 是便利优化，不是对象存储本身；引入另一种提交和持有路径 | 大值使用 `ray.put(value)` 后传顶层 Ref；超限普通参数清晰报错并回滚已取得引用，不能重复塞进控制消息 |
| Actor 跨 Node migration | 普通 Task Node-loss 恢复已教故障放置，Actor 同 Node restart 已教 generation；二者交叉增加生命周期与资源重绑定 | Actor Node 死亡为终态 ActorDiedError；不尝试迁移，取消该保证需写迁移说明 |
| Actor constructor/method 的 ObjectRef 参数，以及返回值内 ObjectRef | 跨 Actor lifetime 与每次 method lifetime 会新增另一套引用/重放边界 | 保普通参数和普通结果的异步 ObjectRef；不支持嵌套句柄，入口/结果编码明确拒绝 |
| PG soft PACK/SPREAD、复杂搜索和自动重排 | 两 Node 教学不需要 100,000-state 装箱搜索；复杂策略与故障重排独立成题 | 小范围硬约束规划，全部预留仍真实；请求不支持的策略明确拒绝，不能谎报 INFEASIBLE |
| shm/mmap、零拷贝、spilling、RDMA | 性能工程会增加平台/内存生命周期，当前重点是逻辑机制 | 保 bytes store 与节点间直传，明确与 Plasma 的差异 |
| GCS HA、owner 接管、持久恢复、网络分区容错 | 要求新的存活权威和共识/恢复协议，直接扩大故障模型 | GCS 失败终止 job；owner 死亡不可恢复其逻辑对象，即使有 replica |
| 动态 Worker 管理、lease pipeline、Autoscaler、Jobs/runtime env、多语言 | 对生产吞吐和运维重要，但非本版辨识性主链的前提 | 固定 Worker 池与每任务 lease；文档写明这个简化 |
| Dashboard/通用解释 API、Actor concurrency groups、named/detached、Serve/Data 等 | 属新的产品子系统；不能因 Ray 支持就进入 mini 的交付范围 | 结构化 trace、少量 snapshot 与七个示例足够 |
| 原生 Windows 完整进程运行时验收 | 现 runner 用 POSIX 进程组，PID 探针也存在平台差异 | 先 Linux/WSL；Windows 需 Job Object/句柄查询和真实清理测试后再声明支持 |

“延期”不等于保留一套半接通实现跟随本版发布。先盘点共享代码：有保留能力调用的部分提取复用，其余从活动导入和发布测试中退出。

## 4. 为什么保 nested refs，却删 multi-return

这两项不能按“都是高级特性”一并处理。

- nested ref 让 ObjectRef 本身成为可传递的数据，说明 owner、当前持有者、outer 对象与 child 对象是不同生命期。两次 `get(outer)` 得到两个 borrower，关闭 outer 后 borrower 仍可用，正是 Ray 对象系统有辨识度的问题。
- multi-return 则让一次函数执行产生多个独立生命周期，并要求它们与共享 attempt/预算协同。它主要扩展输出形态与部分恢复能力；单结果 tuple 已能承载一般多值计算结果。
- 删除 multi-return 后，单 outer 仍可包含多个 child refs；只有一个结果可见性提交和一次 outer GC，不再需要多个返回槽的 batch CAS、健康 sibling 隔离或最后 sibling 释放 lineage。

本版 nested 合同：

1. 保直接 ObjectRef 和现有显式引用序列化能支持的常用容器；复用单次序列化产生的 manifest，不另写通用对象图引擎。发布前明确支持形态，无法建立完整引用清单时拒绝，不能让隐藏 Ref 变成脱管句柄。
2. Task 调用的顶层 Ref 参数进入 readiness gate；容器内 Ref 保持句柄，交给用户显式 get。函数直接 `return child_ref` 仍是单 outer 中的引用数据，不自动 get child。重复引用按身份去重，但不同 Python borrower 生命周期独立。
3. 发送者在提交被接受后关闭原句柄，Task/lineage hold 仍须保活；物理 attempt 的 borrower 与逻辑 Task 的 hold 分开。
4. INLINE 与 STORED outer 共用 owner/child 生命周期。原归档快照的 Core.put 拒绝含 ObjectRef 的值；当前已实现 `put(ref-container)` 的完整清单、引用获取/回滚、独立 put 身份与 owner 原子安装。实际纯/组合与真实进程证据统一记入验收账本，不能把本段设计本身当证明。现有 Task 产生 STORED outer 可独立验证含引用的对象路径；不能以它冒充 put 含 Ref 已通过。
5. 保无环引用关系的释放；ObjectID 环不承诺全局拒绝/回收，不新增 tracing GC。停止进程与对象已全部 GC 必须分别报告。
6. owner identity 固定。Worker 创建的 ref 逃逸给 Driver，owner 仍是那个 Worker；其死亡不能通过切换 replica 伪造 owner 接管。

现有可复用行为入口：[两 borrower 活过 outer](../tests/integration/test_contained_ref_lifecycle_path.py)、[nested 参数保活](../tests/integration/test_nested_task_argument_path.py)、[实体副本 GC](../tests/integration/test_stored_physical_gc_path.py)、[单输出 nested 重建](../tests/integration/test_local_nested_reconstruction_path.py)。这些是行为来源，旧通过结果不认证新实现。

## 5. 新架构怎样简化，而不是把 GCS 事务搬家

### 5.1 状态归属

| 状态负责人 | 最少需要的信息 | 边界 |
|---|---|---|
| Task lifecycle | 函数/参数、TaskID、单 ObjectID、当前 attempt、预算、队列准入及收尾 | 一条普通执行/重试路径，不维护 whole 与 targeted 双轨 |
| Object owner | PENDING/READY/ERROR/LOST、producer epoch、位置、存活理由、outgoing child edges | 唯一逻辑结果与引用真相；不证明远端 bytes 存在 |
| Owner 待交接记录 | 绑定 task/attempt/lease 的完整单结果清单、child hold 身份、待补偿义务 | 接管后责任原子转入正常对象记录；不另复制 Task 成功历史 |
| Node execution/lease | Worker slot、allocation、依赖 pins、运行终态、准确 Complete 收据/回复托管 | Node 资源的唯一权威；Node Complete 不等于 owner READY |
| Worker 局部托管 | 序列化一次的 bytes、原 child handles/import session | 直到真实交接或补偿后才释放，未知 ACK 不重跑函数 |
| Child owner | incoming hold 完整绑定、活跃/释放事实、tombstone、责任 owner incarnation | 负责引用真相；不承担全局图或 outer Task 成功裁决 |
| GCS | 节点成员/权威死亡、Actor、PG | 不保存普通输出发布阶段和全局 contained 图 |

这些是职责，不要求每行新建服务或 framework。Task 与 Object 在同一 Core 内仍可使用显式组合事务维护原子性；拆类不能把一个必要提交拆成两个独立提交。

待交接责任转入对象记录后，仍须保留紧凑的 exact commit/abort 收据。对象后来 LOST、重建或 GC，不得改变原交接事实；迟到 Complete/接管 ACK 只能重放旧回复，不能重新发布、建 child hold 或准入执行。载荷托管可以退休，身份和终结收据保留到对应 owner/job incarnation 关闭并处理完已接受请求，或由明确的重放退休协议提前结束；不任意 TTL 删除，也不无限复制结果载荷。

### 5.2 普通结果的完整顺序

1. Worker 执行一次并序列化一次，保留 bytes、原引用和 import session，生成单对象清单。
2. outer owner 在任何归它最终释放的 child hold 生效前，登记完整清理清单。普通无 child 值也使用同一交付入口；可以不做无意义的 child RPC。
3. child owner 确认引用保活；Node 托管 INLINE 或完成 STORED seal。未知回复只重放原完整身份和效果。
4. Node 记录准确 Complete 并归还该 lease 的资源；回复托管仍在，尚不能宣称对象 READY。
5. owner 校验当前 attempt，原子发布结果与 outgoing edges，完成 Task 状态转换、唤醒等待者；待交接清理责任转入正常对象生命周期。
6. owner 接管 ACK 允许 Node/Worker 退休回复和来源托管。对象副本、contained holds 与 lineage 由正常 GC 处理。

必须分别观察：用户函数返回/执行成功、Node Complete、owner READY、bytes 是否仍可用、回复托管退休、对象 GC。Complete 或历史 READY 均不证明当前还有 bytes；**去 GCS 不能以混淆这些事实为代价。**

上面是普通 Task 的拟定交付顺序。put 的既有普通值路径，以及 §4 拟支持的含 Ref 路径，复用适用的 owner/child 交接、对象存储和 GC，用独立 put operation 身份；没有 Worker 执行、lease 或 producer lineage，不能为了共用代码伪造 Task 执行或 Node lease Complete。

初次迁移可复用当前 child prepare/promote 来降低变更风险，但必须移除全局阶段权威。后续可以独立评估将其合为 `AcquireContained`：Worker 原 source 已保活，outer owner 已登记清单，child owner 原子验证 source 并建立 final hold，ACK 后再释放 source。

这个操作合并不是封版必做项。只有证明 Release-before-late-Acquire、不匹配请求拒绝、源/outer owner 死亡、ACK 丢失和同 Worker source/final 两种 hold 的区分后才能采用；证明成本过高就保留局部两步交接，不另开一轮无限协议优化。

### 5.3 结果未知与故障规则

| 可用事实 | 新版行为 |
|---|---|
| owner 已提交 READY | 保持已知成功，不能因接管 ACK 丢失回滚；只推进托管退休 |
| 存活 Node 有完整准确 Complete | 对当前合法交接继续查询 bytes/descriptor 并推进交付，不能仅因 ACK 丢失重跑；若结果确实丢失，owner须对准确Complete/当前attempt作本地“已知执行成功但结果LOST”的原子转换，不伪造READY或adoption，旧责任收口后才按预算准入重建。该转换已由基础版C6-L纯组合与publisher Node-loss真实切片验证，边界见验收账本 |
| 已知成功，STORED 主副本丢失，存在正常协议确认的存活副本 | 更新位置并使用副本，不重建 producer |
| 已知成功，对象所有可用副本丢失 | 标 LOST；有 lineage 且 owner 活时由 get 触发预算内重建；put 不可重建 |
| Node 已死，owner 未提交，也无存活准确 Complete | UNKNOWN；先 fence/清理旧交接责任，再按有限系统重试策略重执行。UNKNOWN 不表示未执行过 |
| owner 死亡 | 明确 OwnerDiedError；存活 Node/child owner 按 incarnation 清理各自责任，无 owner 接管 |
| 请求超时但无权威死亡事实 | 保留未决请求/义务或报告 unavailable；不能仅凭 timeout 推断死亡、清理成功或再次执行 |

旧版只有 GCS 保有成功历史的部分 POSTCOMPLETE 情况会变成 UNKNOWN，重试次数/预算与惰性重建时机可能变化。这是本版明确接受的简化，不能宣称完全语义等价。外部副作用不提供 exactly-once。

### 5.4 模块整理原则

- Core 只组合 API、Task、Object owner、direct submit；Node 只组合 ledger/lease、Store/pull、execution 收据。正常、故障、shutdown 都调用同一责任入口。
- 每个权威提供自己的待办义务/终态视图，shutdown 聚合这些事实；不读多方私有字典再造一份 clean 布尔真相。
- wire 按 task/lease/object/actor/PG 领域拆分，保一个 canonical schema，复用统一结构校验；每个接收边界验证完整请求，每个产生效果的权威仍按当前 attempt/incarnation/tombstone 判断准入或识别纯历史重放。不能把第一次 ACK 当成此后产生效果的永久许可，也不在每层复制一套近似 schema。
- 只抽已出现的重复小构造，不建通用事务引擎、全局事件总线或空壳 adapter。
- 重构必须降低独立状态、消息变体和跨职责读写；搬文件、缩写变量、删除教学注释不算完成。

## 6. 第一阶段：基础版本的实施步骤

实施按“实现＋受影响测试＋教材”一起推进；snapshot03同版318项纯合同及32个smoke已通过，七个原main产物与文件hash保存至`artifacts/stage1-baseline/`。本地提交/标记`teaching-base-v0.1`固定§0的可检出版本交界。迁移中的代码只在唯一后端推进，不长期维护legacy/new双运行时。

| 阶段 | 工作与主要落点 | 退出条件 |
|---|---|---|
| P0 范围/证据冻结 | 固定本页能力/API 差异/故障合同；登记现有失败；选择主验收平台；建立行为→旧断言→处置的小账本 | 每项退出有理由与迁移；不把历史 pass 或未完成草稿当当前 oracle |
| P1 执行入口与保留缺陷 | 修受支持平台 runner、环境净化和夹具基础；先对保留行为做最小基线。B2 完整 request 绑定先于缓存；B1 修真实 START/JOIN 准入事实而非后置快照 | 精确重放不重复入队/扣预算/删除；当前保留集合的失败有清晰归因，未知成本不扩大执行 |
| P2 单输出与范围退出 | API 限制 remote num_returns；Task/Worker/owner/recovery 移除 selected/targeted/sibling。退出自动 lift、Actor migration、soft PG 策略、未接通 Actor refs 及无调用者兼容形态 | 单输出是唯一活动模型；tuple 返回、wait 数量、单对象两层、nested/whole replay 都正常；退出能力在入口明确拒绝 |
| P3 owner-led 与去全局图 | 按 §5 明确提交点；迁移普通结果/contained holds/收据与补偿；删除 GCS publication history 和 global DAG | 普通成功 trace 无逐任务 GCS 发布门禁；无并行成功权威；publisher 死与 owner 死两类责任均闭合 |
| P4 恢复/引用/高级实验收口 | 整理单输出 whole recovery、foreign hold renewal、CPU yield、Actor 同 Node restart、硬约束 PG 2PC；消除 Core/Node 重复状态 | 一条执行/recovery 路径；alive borrower、旧 attempt、预算、资源守恒、PG可见性/回滚均有主证据 |
| P5 教材与固定版验收 | 七个原 main 同步改 trace/API；加引用实验；README 安装、源码地图、Same/Simplified/Omitted；保存 §0 的版本、依赖与执行证据 | 基础版约定行为绑定同一源码版本验证，已知合同内缺陷关闭，平台声明与文档一致即封版；第二阶段测试不参与此门禁 |

第一阶段内部顺序是 P0→P1→P2→P3→P4→P5。P5 通过后固定基础版本，再进入 §10 的第二阶段。P2 的任务输出模型会影响大部分领域，不能让多个 agent 同时修改 Core/Node/owner 而各自发明新接口。可并行的是只读审查、独立资源 planner 纯逻辑和已冻结接口下的教材/测试适配。

**不再先修所有旧缺陷，再决定删什么。** 两个 targeted 草稿当前被自身夹具阻断，P2 会退出该能力；无需为退役的 targeted 会话先补齐全部竞态。B1 的单输出 whole admission 仍保留，必须修。B2 drop 仍服务对象实验，必须修。Actor migration 的旧字符串分类记录为退出路径缺陷；保留的构造/同 Node restart 仍需 typed failure 和正常回归，不能借退出 migration 忽略共享代码问题。

每一步的失败处理：若不变量无法满足，保留该提交之前的可工作检查点，修正设计/实现；不能通过关闭断言或永久把旧协议藏在兼容开关后面结束阶段。若必要协议增加代码，先保护已确定语义并解释成本，再更新估算；不自行取消两阶段目标或靠削减必要测试满足预算。

## 7. 测试怎样收缩而不失去证据

### 7.1 三个层级，一份行为账本

| 层级 | 内容 | 不能冒充什么 |
|---|---|---|
| 纯模型 | ID、资源账本、状态转换、请求冲突、Release tombstone、预算/准入 | 不证明线程竞争、网络与进程退出 |
| 小型协议组合 | 真实 Owner/Recovery/Store/lease reducer＋显式队列/typed transport callback | typed 成功 stub 不等于真实 Node/Worker 交接 |
| 有界真实进程 | 真实 spawn、lease/push/pull、Actor/PG、少量确定性故障、PID/端口/义务收尾 | 强制终止只证明实验被停住，不证明 clean shutdown |

每个保留行为指定一个主要 oracle；必要时加一个真实接线切片。按保活、准入、交付、资源、故障等责任边界选样本，不按 storage×owner×slot×fault 穷举。支持范围内出现真实反例仍必须修，固定集合不是忽略已知缺陷的理由。

迁移账本字段限定为：行为编号、旧测试/关键断言、保留/改写/合并/退役/未证明、新责任方、新证据、语义变化理由。同一测试文件中 GCS 阶段顺序可退役，但 bytes 正确、资源一次释放和 ACK 重放不重复的断言应迁移。不能按文件名整批删除。

### 7.2 固定行为组

| 行为组 | 最少应证明的事实 |
|---|---|
| 七条学习主线 | 原 main 实际运行；API→task、spillback、pull、Actor、CPU yield、lineage、PG |
| 启动/退出/并发 | 部分启动失败回滚；两个 Worker 实际重叠；每个 E2E 的真实资源/端点退出 |
| 依赖/调度 | pending 不占执行资源；Node 防超卖；首跳 locality 与最终放置区分；lease/push 身份一致 |
| 对象存储 | 未 seal 不可见；大 bytes 不经 GCS/Task 控制消息；source pin/完整 target；put 无执行/lineage |
| 引用 | 两 borrower 活过 outer；sender 先 close 后 accepted task 仍保活；最后引用使 metadata/bytes/lineage 各自收敛 |
| 单输出 lineage | 一个递归链、一次系统失败、一条 foreign owner 路由；逻辑 ID 稳定、attempt 递增、旧结果 fenced |
| nested stored 与 replay | 现有 Task 产生 stored outer→依赖物化→单输出 replay；真实 retained 换代/import/最终释放。含 Ref 的 put 入口按 §4 单独落实证据，不能与 Task 路径混称 |
| 准入与幂等 | 首 ACK 前快完成仍保留真实 START 证明；同请求精确重放；同 ID 改字段拒绝；preview 无准入效力 |
| owner-led 交接 | 接管 ACK 丢失；publisher 死而 owner 活；owner 死而 publisher/child owner 活；迟到消息不复活效果 |
| 结果知识 | 存活准确 Complete 与 UNKNOWN 分开；旧 GCS-only POSTCOMPLETE→UNKNOWN 是明确差异；预算与清理不丢失 |
| CPU | 单 CPU 嵌套 get 子任务实际能执行；仅 CPU yield；重复通知/Complete/death 不双重清账 |
| Actor | direct/FIFO/同代去重；同 Node restart；旧代调用 fenced；含 resource 文本的构造异常不误判容量 |
| PG | STRICT_PACK/STRICT_SPREAD 使用可区分的资源布局；全 commit 前不可见；实际容量拒绝/abort；Node-loss→LOST |
| API 缩范围 | remote num_returns>1、超限大 by-value 参数、不支持 Actor refs/PG策略明确拒绝且无新增副作用；wait num_returns 保持 |

延期自动 lift **不自动消除** foreign nested stored/replay 的交界；Task 的 stored outer 已可经过该引用边界，当前已接通的put入口也须覆盖同一不变量；原始快照曾拒绝Ref不构成删除证据的理由。Actor method 返回普通值的 ObjectRef 与返回值本身含 Ref 也应分别测试，不能误删所有 Actor 异步结果。

已有七个示例并不使用 Task `num_returns>1`，所以单输出范围不会破坏这七条主线的教学目的；PG 示例需补一种能区分硬约束的资源布局，避免容量本身强迫分散而看不出策略。

### 7.3 删除重复治理成本

- 七份 safety classification 与 runner/mode 的重复列表收成一个静态 policy checker、一份 selector/mode/cost 声明；计数自动展示，不再用多个测试硬锁 455/201/254 等历史数字。
- 迁移时一次对照新旧执行集合，保证没有无意扩大范围；仍保未知 case 不自动进入、混合文件不继承纯模式、无目录/glob/参数透传、插件环境净化和路径校验。
- helper 分值构造、权威组合、真实进程 harness 三层；通过正式的小型依赖注入构造，不把一个大 `object.__new__(CoreWorker)` 私有字段清单复制到几十个文件。
- 不把清零 accepted count、手填终态、typed fake ACK 或吞掉清理异常做成通用 clean 工具；不从另一个 test 模块导入大 Fixture。
- GCS/DAG 专属断言从基础版活动 gate 退出，保留历史来源供第二阶段逐项改写适用证据；多槽协议按已退出能力退役。公共不变量迁到新边界，不能连同中央协调代码一起删除；不先把全部历史 runtime 测试迁 pure。

### 7.4 CI 与完成定义

本节仅定义第一阶段门禁；第二阶段新增保证与组合证据见 §10.6，不倒灌到 P5。Linux/Python 3.12 为主 gate：固定纯/组合集合＋逐个有界真实进程集合，串行执行。每个 exact case 有统一工作/退出 deadline 和受管进程/内存上限。runner 本身须用小型真实进程验证超时、中断、清理，而不是只 mock Popen。

Windows 在修好平台清理之前不声明完整运行时可用；macOS/Python 其它版本只在实际有对应 gate 时写支持。压力、多故障、fuzz、性能与上游 differential 可作为独立实验，不自动扩为本版封版条件。

发布结束条件：冻结合同内的已知缺陷关闭；每个行为有对应证据；新 API/语义有迁移说明；同一版本七个示例和约定故障路径通过；没有活动代码依赖退役协议。pass 总数、覆盖率、heavy 清零和“还能想到一个交错”都不单独决定完成。

## 8. 第一阶段规模与可读性预算

不能从静态行数承诺精确可删比例。**1.4–2.2 万代码行是低置信度设计预算，不是实测预测、压缩承诺或硬上限。**下表只是责任领域的初始费用分配，不能替代实施测量。明确校准点为：第一阶段跑通“单输出普通值＋单输出含引用值”的完整执行、交接、读取及回收路径后（跨 P2/P3，含真实 child/owner/Node 责任，不以纯模型完成代替），依据实际代码量、状态/消息种类、同步交互和剩余清理义务重新估算；P5 固定基础版本时记录实数。

| 领域 | 代码行预算，千行 |
|---|---:|
| API / ID / wire / transport / bootstrap | 2–3.5 |
| Task / lease / 调度 / Worker | 3–4.5 |
| 对象、引用、pull、交接、GC | 4–6 |
| lineage / 故障恢复与清理 | 2.5–4 |
| Actor / PG | 1.5–3 |
| trace / 小型 debug | 0.5–1 |
| 合计设计区间 | 约 14–22 |

合计下界按取整表示。测试和文档另计，不设测试/源码固定比例。分组是拟定责任划分，不能与现有卫星文件组直接相减得出“已可删除多少行”。

如果保留合同确实需要超过预算，须解释对应机制、状态/消息、同步依赖和测试成本，更新低置信度估算并记录尚可简化之处。不得通过删除必要校验、清理逻辑或测试来凑行数，不因超预算自行取消既定保证，也不能靠拆成很多小文件掩盖总复杂度。更重要的验收指标是：

- 第一个示例能沿 API→Task→lease→Worker→owner 解释；普通值结果不必学习 GCS publication saga。
- 每项状态只有一个权威，跨权威原子提交点明确，shutdown 不拼私有字典。
- 单输出与恢复只有一条执行路径，协议中不残留 target mask、sibling、全局 graph/阶段事务。
- 每个主要模块可用一两句话描述责任；超过约 1,500 代码行触发职责审查，但不是机械拆文件的要求。
- 新机制须同时给出“教什么、与 Ray 对应何处、引入几种状态/消息、如何有限验证、删掉会失去什么”；便利选项不能自动获得核心范围资格。

### 8.1 2026-09-09 实施中校准（snapshot02之后）

单输出与含引用值已经进入真实执行、交接、读取/回收路径，因此按既定校准点重新测量。
使用同一AST/token行口径，枚举当前实际存在的`src/`、`tests/`、`scripts/` Python文件，纳入未跟踪新增文件并排除已删除文件；
`docs/history`中的归档材料不算活动源码。测量只解析文件，不导入或执行运行时。本次已纳入Seal校验修复、Worker/Core教学说明及新增真实切片；P5若继续修改源码仍需重新记录实数。
[校准数据](C:/Users/t-hdong/Desktop/gao/audit/mini-ray-stage1-current-complexity.json)与
[校准脚本](C:/Users/t-hdong/Desktop/gao/audit/measure_stage1_current.py)保留输入口径。

| 范围 | Python文件 | 物理行 | 代码行 |
|---|---:|---:|---:|
| 活动`src/` | 56 | 59,386 | 48,555 |
| `tests/`，包含尚未迁移的历史测试 | 340 | 130,975 | 110,374 |
| 三个runner | 3 | 977 | 784 |

源码比原始55,196代码行减少6,641行，即**12.03%**。这证明部分行为和协议退出，不能证明整体已达到紧凑教学项目的规模。
基础预算上界22,000与当前实数相差26,555行；增强版25,000–30,000总量预算也低于当前基础实现，已不能作为近期实现规模预测。
两组数字仍保留为低置信度初始设计预算和偏差记录，不变成硬上限，也不通过直接扩大一个数字宣布纠偏完成。
下一次估算以48,555当前实数为锚，分别列剩余保留合同修复、可验证的状态/消息简化、第二阶段新增协议成本；
在这些改动及校准完成前，不给新的虚假精确终值。P5记录基础版最终实数，E2再根据实际增量更新增强版预算。

| 当前集中责任 | 当前代码行 / 相对原始变化 | 成本解释与尚可收敛处 |
|---|---:|---|
| Core | 10,985 / −856 | 提交、lease歧义、依赖交接、引用/副本GC、whole恢复、死亡传播和退出仍交织；owner CAS必须保留，但不能继续吸收全部协议推进 |
| Node | 6,449 / −60 | 本地lease/资源、Store、Worker pool、PG参与者、依赖与owner-death清理成本仍在；退出GCS接线没有自动消除这些责任 |
| canonical protocol | 5,518 / −540 | 197个class含消息、枚举和值类型，不是197个RPC；完整身份、边界校验和pickle重验证占可见篇幅。可合并已出现的重复构造，不能删效果权威的当前状态检查 |
| ownership | 3,456 / −909 | 实际引用理由、tombstone、结果/边安装与retirement/GC合同保留；单输出仍有历史slot形态，须区分窄包装与真正独立状态再整理 |
| GCS control | 3,117 / −1,121 | 普通发布历史已退出；成员/死亡事实、owner-wide fence、Actor与PG仍有真实协调责任 |
| API / Worker | 2,576 / 0；1,897 / 待领域细分 | 启动回滚、实际进程/端点退出与执行边界尚未按mini学习链收敛；不以重命名/搬文件当简化 |
| 发布卫星历史分组 | 2,577 / −1,684 | 剩余主要是manifest、Node journal、source交接与精确收据；新owner/put模块在统计中属other source，不能将此分组等同中央事务 |

七个超过1,500代码行的模块合计33,998行，占活动源码70.02%，均触发职责审查。当前静态枚举另有66个Enum、291个声明值；
其中包含错误类别和普通值，不等于独立状态数或RPC数。独立生命周期、authority之间的同步调用与未决清理义务仍需按实际路径逐项说明，不能用这个计数替代设计分析。
当前可以确认没有普通逐结果GCS门禁、targeted执行或global graph运行时；Worker/Core的旧GCS/ARM说明已经清理。B04/B06有限证据已在snapshot03与全部基础清单同版复验通过；Core/Node的职责集中审查与可读性成本如上记录，不宣称总规模已经达标。
超预算不自动阻止已确定合同的交付，但须连同这些明确成本与未完成简化如实记录；不把全部余额称为必要，也不靠删除必要校验/测试凑预算。

## 9. 第一阶段交付物与迁移边界

第一阶段落地后应产出：单后端源码、精简公开 API、七个递进示例、独立引用生命期实验、可复现测试入口、一个当前状态页、一个 Ray 职责映射页、一份版本迁移说明，以及 §0 的源码/环境/依赖/执行证据包。历史 checkpoint 与退役研究材料保留可追溯入口，不继续作为 README 主阅读链。基础版固定学习入口持续保留；第二阶段另交付相对于此基础版本的协议、语义与教学增量。

需要明确写入迁移说明的变化：独立多返回槽退出；自动大参数 lift 退出；Actor Node-loss 不再迁移；PG 策略范围缩小；GCS-only 成功历史的部分场景变为 UNKNOWN；跨 ObjectID 环不保证全局拒绝/GC；旧序列化兼容形态退出。`wait` 等待个数、普通 tuple/list 返回、单对象双 storage tier、nested borrower 保活不能被误伤。

既有文档称某些高级语义“已承诺”，这属于历史需求主张；本线程用户明确要求按教学定位重新判断保留/退出。本计划把损失和替代方案写明，避免把沉没成本当作继续扩张的理由，也不把计划阶段的取舍冒充已改好的代码。

## 10. 第二阶段：加入两项协议增强

### 10.1 范围与职责

本阶段确定交付 GCS 普通结果发布事务和全局引用图防环，研究的是 **mini 自定义的中央协调保证**，不是补齐基础版缺少的 Ray 必要机制，也不称为“更接近生产 Ray”。单输出、拓扑、Actor/PG 范围、bytes store 和平台约束沿用基础版；不恢复 multi-return、targeted、Actor migration、自动 lift 或其它已延期能力。

| 新增机制 | 新的保证与代价 | 保持的职责边界 |
|---|---|---|
| GCS 普通结果发布事务 | 存活 GCS 保存准确清单、准入许可、执行成功/接管/撤销收据；承载发布的Node死亡后，部分窗口仍可知道执行成功。代价是普通成功等待 GCS ACK，GCS 不可达会阻止相关推进，还需处理未知回复、补偿和记录退休 | owner 唯一提交逻辑可见性与 outgoing；Node 唯一提交执行/资源/Store 事实；GCS 保存准确事实与准入/协调记录，不执行 Task、不保存结果 bytes |
| 全局引用图防环 | 对完整覆盖的受支持 contained 边，在同一图权威中检查并安装 PREPARED/COMMITTED 边，拒绝合并后成环。代价是同步图准入、并发预留、取消与边生命周期的额外协调 | child owner 唯一维护 incoming hold；图是防环所需的保守投影，不是引用计数真相；图释放不能代替真实 Release 或物理 GC |

单 GCS 是内存权威，不提供 HA、共识、持久恢复或 owner 接管。INTENT 表示清单已登记，ARM 表示发布条件获许可，均不证明执行成功。Node 准确 Complete 是执行事实来源；GCS 仅在接受并绑定该事实后拥有 terminal 记录。成功元数据不能恢复 bytes：若 owner 未收到值且所有有效载荷/副本已丢失，只能按已知成功但结果 LOST 处理，不能制造 READY。

术语统一：**owner** 指 outer ObjectRef 的逻辑所有者；**publisher** 指执行并持有序列化源的 Worker；**Node** 指承载该执行的 NodeManager/Store，两者死亡不能混称。执行成功、Node Complete、owner READY、当前 bytes 可用、回复托管退休、对象 GC 是不同事实。READY 后可能变 LOST/进入新 attempt/GC，历史成功与 adoption 收据仍不可改写。

以下§10.2–10.4仍是增强版合同；当前实现和分层证据见[增强验收账本](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/docs/acceptance-enhanced.md)。W1/W3/W4有限组合、W2准确知识差异、owner死亡与公共图实验均已纳入增强snapshot03同版验收：377项纯合同、37个smoke、七个原main产物。以下合同有对应分层证据，不外推所有故障组合。

### 10.2 增强版的交付顺序

复用 §0.1 和 §5 的单对象身份、交接及清理接口，增加具体 GCS/图门禁，不重建整套旧协议族。INTENT/ARM/terminal/adopted 可作为教学术语，不能为了保留旧名称复制平行 Task 成功状态。

1. Worker 执行并序列化一次，保留真实 source/import handles；owner 登记完整单结果交接清单。**C0：GCS 本地原子接受 INTENT**，绑定完整请求与清理目标。ACK 前不得发起依赖此 INTENT 的新图/final hold 效果。
2. **C1：GCS 在同一临界区检查并安装图 PREPARED**，candidate 加上所有仍有效的 PREPARED/COMMITTED 边共同检查。图已预约不等于 child hold 已存在；无 child 的值不产生图边。
3. 保持 source 保活，按既有引用协议建立/接管 child holds，完成 Node 物化。**C2：GCS 校验准确准备收据并登记 ARM**，只授予对应 incarnation/attempt 的许可。拒绝或撤销关闭前进权限，不能授权迟到新效果。
4. **C3：Node 将当前 lease 的终态与一次性资源释放提交为本地 Complete**，保留准确结果/托管记录；不等待 GCS terminal RPC 才释放资源。**C4：GCS 原子接受该准确 Complete 作为 terminal 事实**；ACK 未知按 W2 查询/重放。
5. **C5：GCS 将该准确图预留提交为 COMMITTED**；**C6：owner 校验当前 attempt 和可用结果，原子提交 READY、outgoing edges、Task 状态转换与 exact adoption receipt**。C5/C6 不是跨进程原子事务，间隙按 W3 处理；不能把 graph COMMITTED 当作 owner 已接管。
6. **C7：GCS 幂等接受源自 C6 的 adopted 收据**。本计划将正常回复托管退休许可绑定准确C6及C7 ACK；C7回复未知先查询/精确重放，不能回滚C6，也不能把READY写成尚未成功。死亡/撤销路径凭§10.3的准确清理证据退休，无需等待已死owner发adopted。后续 outer GC/旧 epoch 退休按 W4 释放对应 holds/图 membership/副本责任，再确认各自清理完成。

**C6-L：已知执行成功但载荷丢失的替代本地提交。**若owner尚未C6，已有准确C3/C4且所有可用bytes确实丢失，则owner在自身权威内校验完整身份、当前attempt与撤销选择，原子记录该执行已成功、结果LOST和恢复所需状态；不伪造READY/adoption，不仅因记录该事实再扣预算。旧交接由W2/W4收口，随后真实准入下一次重建才按既定预算记账。C6-L不替代已存在的C6历史，也不能重新打开已退休旧epoch；具体收据及与取消的有限顺序由U1对应证据验证。

C0–C7与C6-L是本计划的提交点标签，不要求按标签各建通用状态机或服务。每个效果权威仍依据当前 attempt/incarnation/tombstone 判定准入；查询历史只返回历史，绝不重新产生效果。owner端的C6/C6-L与本地撤销选择共享一个提交序列，Node端的取消与Complete也必须在本地序列化；GCS不替代这两个裁决。

Task 首次发布与重建必须使用同一 contained-edge 入口。若第一阶段按 §4 实现了含 Ref 的 put，它也进入同一图准入/提交/释放子协议，以 put operation 和 owner 安装事实绑定，不伪造 Worker lease 或 Task Complete；当前含 Ref put 的覆盖以验收账本为准，不能将历史归档快照的拒绝行为与当前实现混称。其它不支持的引用入口保持明确拒绝，不新增 API 制造成环示例。

### 10.3 共用的失败推进与退休责任

| 情况 | 负责推进的现有参与方 | 只能依据什么作裁决 |
|---|---|---|
| owner 存活 | owner 的交接/退休驱动维护其完整待办；Node 推进本地 Complete、partial write、资源与回复；child owner 执行准确 hold 操作 | owner 不能代填远端成功/清理；GCS 不能自行推断 owner 未 READY |
| publisher Worker 死、Node 存活 | Node supervisor 提交权威 Worker 死亡、处理 lease/source 本地责任；owner 查询 Node/GCS 收据继续交付或清理 | Worker 死不等于 Node 死，也不抹掉 Node 已持有的 Complete/bytes；未交接的 Worker 内存按死亡事实失去 |
| Node 死、owner 存活 | owner 依已安装 Node 死亡事实移除死副本，查询所有已知存活准确收据；child owner 处理旧 holds，GCS保留其已接受事实 | 仅存活 Node/owner/GCS 的准确 Complete/terminal 可证明已知成功；不存在此证据才是 UNKNOWN，不能凭 INTENT/ARM 升级 |
| owner 权威死亡、GCS 存活 | GCS 按 C0 已登记清单及同一死亡记录协调遗留效果；Node/child owner仍是各自资源/引用真相的提交者 | GCS 只接续协调，不取得 ObjectRef owner 身份、不发布 READY；不能等已死 owner 发 ACK 才清理。Driver owner 无法取得适用死亡证据时依 job 终止处理，不由 RPC 超时猜死 |
| GCS 暂不可达或自身退出 | 未满足 GCS 门禁的前进操作有限等待/报 unavailable并保留义务；Node 已发生 Complete 的资源释放与有据的本地清理继续。确认 GCS 退出后 launcher 终止 job并清理受管进程 | 不因 GCS 查询失败撤销已 READY 对象；不另选 GCS 或恢复丢失内存表；强制退出只证明进程停止，未证实的分布式 GC 不报 clean |

**撤销与边移除的实施顺序：**owner 活时先原子确定该交接未 adoption 且关闭迟到 C6，再通知 GCS 关闭该身份的新准入/图 commit；owner 死时由权威死亡事实关闭其前进权限。Node 取消与 Complete 必须裁决出一个真实结果；已 Complete 的执行不能回写为“未执行”。随后对可能已发出的 child 操作安装准确 Release/tombstone，释放该 publication 的有效 holds/outgoing 边。GCS 必须继续把仍可能有效的边计入防环，直到这些边解除且不能复活，才最终移除 PREPARED 预约或 RELEASE 已 COMMITTED membership。先 fence 不等于先抹图，fence 也不能代替已有资源释放。

PREPARED 的最终 ABORT 可留下墓碑；COMMITTED 只能记录 RELEASED/已退休并保留历史 commit，不能倒退成“从未提交”。提前关闭前进权限与最终图边移除是两个事实，不能复用一个即时清表操作冒充。实际wire以独立FENCED/RETIRED收据表达这个区分；GCS/owner死亡、提交与准确重放的所选顺序已由U1/U2有限证据验证。

**收据退休规则：**每项 effect 仅凭准确 ACK、适用的权威死亡事实，或可核查的“效果已解除且不能再生效”证明退休。Node 死亡可解除该 Node 私有内存/副本义务，不能解除存活 child owner 的 hold；child owner 死亡可按既定 owner-failure 合同处理其对象，不能拿它替其它参与方清账。普通 borrower 的独立生命周期不随 outer graph membership 一起清除。载荷缓存、活动义务与紧凑历史收据分别退休；exact commit/abort/release/adoption 收据保存到该 incarnation/job 关闭并处理完已接受请求，或明确的重放退休协议结束，不任意 TTL 丢弃。

### 10.4 四个关键窗口的状态与责任表

本表绑定 §10.2 提交点与 §10.3 推进方；W1–W4 是有限验收窗口，不是再展开 storage×owner×fault 的组合矩阵。下列窗口已按增强账本指定的纯/组合/真实进程层级验证；行中每种死亡分支不因此被扩写成全部真实故障组合。

| 窗口 | 权威事实与记录位置 | 允许的下一步 / 提交点 | ACK 丢失后的查询或准确重放 |
|---|---|---|---|
| **W1 图 PREPARED，child 交接失败** | GCS 有 C0 清单和 C1 预约；owner 有 pending；child owner 可能已安装部分 holds；Worker/Node 仍有源/局部效果。没有成功 Complete 的推断 | 可重试错误只重放同一 child 操作；确定失败则 owner 原子选择撤销并关闭 C6，GCS关闭前进准入；Node 裁决取消/Complete，随后精确 Release。不得继续 ARM/graph COMMIT/READY | 查询或重放每个完整 child request；超时不算失败或无效果。对已发送而未知的 Acquire/promote，Release-before-late-operation 必须产生拒绝或历史回复，不能复活 hold |
| **W2 Node Complete，terminal 回复未知** | Node 有 C3、资源已释放；GCS 可能已完成 C4，只是 ACK 未到。owner 尚无 adoption 事实 | 查询 GCS 同一 publication；已有 C4则继续 C5/C6，未记录但存活持有者有准确 C3则重放 terminal。合法 bytes 缺失时由owner提交C6-L并收口旧交接，再按既定规则准入重建；不能仅因 ACK 丢失重执行或双重释放资源 | 记录缺席不推翻存活准确 C3；查询不可达保留未决。迟到 terminal 可保存历史成功事实，但若撤销/新 attempt 已封闭旧前进权限，不能恢复 C5/C6；接纳规则由U1的准确历史/fence证据验证 |
| **W3 图 COMMITTED，owner 尚未 READY或其ACK未知** | GCS 有 C4/C5；owner 可能仍 pending，也可能 C6已提交但回复丢失；Node/Worker可能仍托管payload。图 commit不证明 owner adoption | 查询 owner 的 **exact adoption/撤销 receipt**，不只看当前 READY/LOST；已有C6则报告C7/重放ACK；仍pending且current/bytes/holds合法则幂等CAS；确定不能发布时原子选择撤销/退休，禁止迟到CAS | owner暂不可达不能当未接管，不能抢先释放child。GCS graph commit查询与owner receipt查询各验证自己的事实；重放不得再建图边或重复owner提交 |
| **W4 owner READY后重建/GC/迟到commit** | owner保留旧C6，GCS保留旧C5/C7及release历史；同ObjectID当前可能LOST、新attempt或已GC。现存bytes由当前Node/owner证明 | GC或重建先关闭旧epoch交接并收口其义务，再退旧图membership；最小方案在旧责任解除屏障后才准入新publication，不新增原子跨epoch图替换协议。等待旧清理不重复扣执行预算 | 旧commit/release/adopted查询返回绑定旧epoch的历史事实；旧commit不能插回旧边，旧Release不能删新边。准确历史ACK不证明当前值可取，也不重新准入Task |

| 窗口 | owner / publisher / Node 死亡后的责任 | 补偿、fencing与收据退休 | 对应有限证据 |
|---|---|---|---|
| **W1** | owner活时驱动撤销；publisher死由存活Node收其局部责任，owner继续；Node死则owner/child按死亡事实处理；owner死后存活GCS按INTENT清单协调Node/child，不新增清理服务 | 先锁定撤销、fence迟到前进，再解除精确publication holds；有效边未解除就保留图的保守占用，最后ABORT。partial write、bytes/source/reply另行清理；缺一项证据保留未决，不假clean | 一个outer含两个child：第一项建立后丢ACK，第二项拒绝；延迟第一项重放/前进消息。验证hold不复活、图预约最终退出、Node不双清账。无需把每种child来源再乘全部故障 |
| **W2** | publisher死但Node活保留C3/托管；Node死后GCS已有C4或其它存活准确C3仍为已知成功，否则UNKNOWN；owner死由GCS协调死亡清理而非等adoption | 精确重放terminal，不重做用户执行；bytes全失标LOST。Complete资源清账不依赖terminal ACK；回复托管须等真实接管或明确补偿；旧终结事实与新前进许可分开退休 | 一对窗口：GCS真实接受terminal后ACK丢失、Node退出；terminal未被接受且无其它存活成功收据时Node退出。观察知识分类、bytes状态、预算与一次资源释放 |
| **W3** | owner活保持唯一CAS/撤销裁决；publisher死优先查Node托管；Node死由owner查存活bytes/事实并继续或处理LOST；owner权威死后GCS以完整清单协调释放，绝不接管对象 | 未知adoption不得补偿；确定撤销后只释放该publication holds/outgoing及副本责任，再RELEASE已COMMITTED图，保留旧commit。先前C6已成功就只能正常退休，不能回滚可见对象 | 分别停在C5之后/C6之前和C6之后/ACK之前；准确查询区分pending/已接管。另复用既定owner-death切片证明GCS收尾，不要求所有参与方同时故障 |
| **W4** | owner活推进旧epoch GC/重建屏障；owner死后GCS按已登记旧清单协调。publisher退出不改变adopted对象身份；Node死只影响其当前副本及本地义务，独立borrower由各child owner保活 | 旧有效边与hold解除且不能复活后退旧membership，再建立新epoch。旧GC只删旧identity/bytes；紧凑commit/release/adoption收据不随当前对象GC消失。fence不证明实体已删 | 单对象重建到新READY后重放旧commit/Release，再GC并再次重放；新边不被删、旧边不复活、对象/回复/图收据各自状态正确，无需恢复targeted/sibling |

W1–W4 按 owner/publisher/Node 角色分别定义责任，不要求为表中每个死亡分支新增一套进程测试。E0 将现有有限 owner-death、publisher-death、ACK-loss 场景映射到最能观察相应边界的行；共用的不变量由纯/组合层验证。与另一行完全同义的场景复用，不生成新的交叉积门槛。

### 10.5 全局防环的可达场景与入口覆盖

**公共API可达成环候选与真实并发预留现已证明。**增强snapshot01
`b6da4a46b3877dac9252e2a50a56af58e05adaf3589ef3ec613dfe5f49ae9d7c`上的三个真实图切片通过；
实现入口是[真实图测试](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/tests/integration/test_enhanced_cycle_path.py)和[普通Worker函数](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/tests/integration/_cycle_runtime_state.py)。
旧metadata-only控制实验保留历史来源，但不再用它承担公共可达性证明。这些入口后来在最终增强snapshot03全体同版复验通过。

| 场景 | 最小构造与真实经过的边界 | 已有证据与边界 |
|---|---|---|
| R1无环公共发布/GC | child=put(普通值)，contained put和Task返回child；实际持有、get、close、child Release、bytes/图退休 | 1 passed / 5.62s；Task有准确Complete/adoption，put无Task执行事实；证明两入口接线/回收 |
| R2两对象成环候选 | 单普通Worker先产生stored A₀；后续Task合法借入A并保存真正Worker-owned B=put([A])，图B→A已提交；whole reconstruction A₁返回B，C1实际拒绝A→B | 1 passed / 8.41s；A₁无PREPARED/ARM/COMMITTED/Complete/adoption，实际空rollback scope证明无child/materialization效果；B释放后A可GC |
| R3并发预留 | 两Node各一Worker保留X=put([B])、Y=put([A])；ray.wait触发既有whole reconstruction，候选A→X与B→Y在C1竞争，联合为A→X→B→Y→A | 1 passed / 15.25s；两次发送前与两次原回复后有限屏障，观察恰一PREPARED/graph_active和一CYCLE，且双方均未ARM/COMMITTED/Complete/adoption；败方rollback、胜方READY，最终全图退休 |
| R4公共API自环/首次发布互环 | R2已证明利用稳定ObjectID、真实保活和whole替换的公共两对象环候选；没有变更已发布bytes或手造引用 | 自环、两对象首次发布互环尚未证明，亦非新增门禁；不扩API制造需求 |

R2/R3使用可导入模块中的普通函数，持久Worker局部变量只保留真正owner-local put句柄，不保留已结束Task的临时borrower。
被测GCS锁、判环、Node交接和原始回复不替换；屏障只允许观察真实阶段。成环候选被拒绝，不表示运行时先发布了一个实际引用环。
R3在胜方仍PREPARED时观察，证明全局判环包含有效预约，不能缩写为仅已COMMITTED边的竞争。

不能用`x=[]; a=put(x); x.append(b)`证明ObjectID环：put已经序列化immutable值，修改原Python容器不会修改a。
Python list自环、Task互等依赖和ObjectID contained环是三件不同的事；未使用detached ObjectRef、伪borrower、Actor邮箱或新增设置结果API。

| contained-edge入口/退出 | 两阶段范围及第二阶段覆盖要求 |
|---|---|
| Task首次结果，包括直接返回child Ref或容器内Ref，INLINE/STORED | 支持；由同一序列化manifest产生边，C1准入、C5提交、C6可见，不按storage分出后端；Task嵌套参数的临时Task hold本身不是新outer结果边 |
| put普通值 | 当前支持；没有contained边，不伪造graph边或Task lease |
| put含Ref值 | 增强版已以真实put operation/owner安装身份接入同一图协议，R1/R2/R3给出真实接线与GC证据；最终同版复验已通过，不能把基础引用测试冒充图证据 |
| 单输出whole reconstruction产生新结果/新child集合 | 已沿W4旧责任屏障接入，R2/R3真实到达新publication/epoch；最终同版复验已通过，不恢复targeted或绕开图替换outgoing |
| outer GC、交接撤销、owner death | 按W1/W3/W4释放准确旧图membership；记录退休不依赖独立borrower全部消失，且旧消息不能复活已退休边 |
| Actor参数/结果内Ref、任意脱管序列化等不支持入口 | 维持明确拒绝，不为成环实验新增能力或把它们排除后仍声称全API图覆盖 |

### 10.6 实施、验收与版本交界

| 步骤 | 交付 | 验收条件 |
|---|---|---|
| E0 接收已验收基础版，落实有限设计问题 | 检查§0证据包；将U1–U5分配到相应提交点/参与方/证据；锁定W1–W4和R1–R3所需的有限选择器与成本 | P5已经独立完成；未决问题不回写成基础版缺陷；每项新增保证有具体边界，未证可达的公共环例子不写成可用示例 |
| E1 GCS发布事务 | 复用单对象交接，加入C0/C2/C4/C7及准确查询/撤销/退休 | 无child普通成功确实经过门禁；W2事实/ACK/死亡分支及owner-death协调有有限证据；INTENT/ARM不能证明成功 |
| E2 全局图防环 | C1/C5与真实Task及所有最终支持的put/reconstruction入口接线；图保守占用/释放按W1/W3/W4 | R1正向接线、R2真实公共成环候选拒绝、R3真实并发预留均需在最终版复验；纯算法另报层级，R4未证自环不虚构或新增API |
| E3 两项组合窗口 | 只取W1–W4中尚未由E1/E2覆盖的交接窗口，复用既有owner/publisher死亡切片 | 预约失败与hold补偿、图commit/ownerCAS间隙、旧epoch晚消息各有责任闭合证据；不假成功/clean，不双清账，不扩全故障乘积 |
| E4 增强版验收与教材 | 在增强版同一提交复验保留基础行为＋新增保证＋组合窗口，保存对应环境/依赖/trace/结果 | 两项都真实启用，所选合同内已知缺陷关闭；如可达性/责任问题仍影响宣称的保证，保持第二阶段未完成，不用纯模型顶替；基础版入口持续可运行可发现 |

顺序E0→E1→E2→E3→E4；E0是在基础版已封版后接收证据，并在各相关机制编码前细化其未决设计，不要求提前证明整个增强版。E4才结束第二阶段。使用§7既有三个测试层级；下面是同一行为账本的证据分类，不另造测试框架或完整复制基础测试树。

| 验收分类 | 应验证的内容 | 与基础版的关系 |
|---|---|---|
| B 基础行为回归 | 同一public API、单输出、真实lease/pull、引用保活、CPU yield、Actor/PG、资源/身份不变量 | 在增强版自己的提交复验，不拿基础版旧pass背书；不要求“无GCS门禁”的基础trace原样通过 |
| G/D 新增保证 | G：准确GCS发布事实、许可/查询；D：含有效预约的图原子判环及全部支持入口 | 仅属于第二阶段；G由准确Node/owner收据支撑，D由纯算法＋真实handler/入口证据共同支撑 |
| X 两项组合 | W1/W3/W4的跨责任交接，以及W2终结报告与图/owner后续的关联 | 只补此前未覆盖责任窗口；一条证据可映射多条不变量，不自动增加shape×tier×owner×fault门禁 |
| Δ 明确语义差异 | W2/R2的两版预期及新增同步GCS依赖 | 允许规范化随机ID/时间，不能归一化UNKNOWN、已知成功、LOST、READY、预算或真实ACK成假等价 |

Δ的核心成对实验：Node退出、owner未提交且无其它存活准确成功收据，基础版判UNKNOWN；增强版只有GCS真实接受terminal时判已知成功。两版都必须另报bytes可用性；已知成功且无bytes仍为LOST，不能把“知道成功”归一成“get成功”。若GCS未接受terminal，增强版也不能凭INTENT/ARM升级；预算与旧attempt fencing按明确不同路径检查。基础版没有图端点，不为两版对照给它新增空端点；图拒绝实验与基础版不提供该全局保证的事实对照即可，不能虚构基础版public成环成功。

### 10.7 有限实施义务的关闭记录

| 编号 | 实施义务 | 增强snapshot03关闭依据 |
|---|---|---|
| U1 取消、Complete、迟到terminal | owner撤销与C6/C6-L、Node取消与C3、GCS事实查询/迟到历史报告的线性化；未adoption但已知成功无bytes的owner本地LOST提交；不能由晚收terminal重新开放已撤销epoch | 基础版已落实C6-L；增强snapshot02的真实W2两切片证明未收terminal→UNKNOWN、真实接受但回复丢失→已知成功LOST，bytes/预算/前进权限分开。最终snapshot03已同版复验，不外推任意网络故障 |
| U2 图commit/释放与ownerCAS | C5后owner未接管或ACK未知；前进fence与保守图占用分开，COMMITTED只能准确release；旧责任屏障怎样避免新epoch被旧Release误删 | 窄owner client、GCS reducer/control和Node journal保留职责；W1/W3/W4真实authority组合纳入最终纯批次，R1/R2/R3真实进程覆盖图/GC接线。fence不抹图，准确child清理后才退休 |
| U3 死亡后的协调记录与存活性 | owner活/死的驱动切换、Node保留的源/Complete、GCS按INTENT协调的记录与退休、Driver owner与GCS自身死亡边界 | GCS在既有死亡驱动中推进准确清单；最终owner死亡、publisher Node-loss真实切片与9个control/10个owner退休合同验证所选责任，完整child死亡proof匹配已安装fence；无owner接管或持久服务 |
| U4 含Ref的put与其它入口覆盖 | 基础版已实现含Ref的put、无lease身份、hold获取/失败回滚和owner安装；第二阶段必须复用这些责任，图覆盖不能漏入口 | 最终R1公共put/Task正向与GC、R2/R3借用put保活及whole替换同时通过，put无Task Complete/adoption；独立无lease身份和owner安装复用基础责任，没有新增API |
| U5 成环可达性 | R1/R2/R3公共put、Task、whole重建及并发预约已在snapshot01真实通过；R4自环/首次发布互环仍未证明 | 有限公共可达性责任已关闭，最终增强版已同版复验上述三切片；没有放松身份校验、伪borrower/ACK或新增API。未证场景如实限定，不追加门禁 |

U1–U5在本版选择的有限合同内均已满足；不存在必须实施后再补的未决发布窗口。R4自环、首次发布互环和更宽故障矩阵未证明，不作为新增门槛，也不授权新增能力。该结论是有限证据充分，不是形式化证明或任意故障保证；基础历史交付不被改写。

### 10.8 代码预算与两版教学入口

两组数字均为**低置信度设计预算**：基础版约1.4–2.2万代码行，增强版总量约2.5–3万代码行。它们不是实测预测、硬上限或第二阶段必然增量，测试/文档另计；不能用增强版预算放宽基础版范围。按§8在基础版单输出普通值及含引用值的完整发布/读取/回收路径跑通后首次重新估算，P5/E0绑定实数，E2按实际新增消息、状态、同步依赖与补偿成本复核。超预算解释成本并更新估算，不删必要校验、清理或测试凑数。

最终增强源码实测**50,277代码行、61,440物理行、59个Python文件**，较基础48,555代码行增加1,722行（3.55%）。
统计见[增强规模证据](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/artifacts/stage2-enhanced/complexity.json)。增量来自独立GCS/图metadata authority、owner client、死亡协调、
Node收据及Core的窄交接/退休接线；每次普通成功新增六种GCS阶段RPC，未知回复与cleanup亦有额外查询。
**紧凑规模目标没有达成。**两阶段有限协议合同和范围纠偏已交付，但Core/Node/wire集中及整体阅读成本仍是保留债务；
不把预算改大就宣布体量合理，也不把所有现存代码当必要，更不通过删校验/清理/测试凑行数。

增强版成为最新版本后，README仍明确推荐“先从基础版分支及其固定验收提交学习Ray Core”，附该版依赖/命令/示例/源码入口；增强版另附“mini中央事务与防环实验”入口及两版语义/trace差异。历史基础trace只能归属于其固定版本，不能伪装成增强版实时路径。通过版本检出进行比较，不在产品中常驻双后端，也不把基础版降格为缺必要机制的旧原型。

## 参考与证据

- [前一轮审查报告与实际测试限制](C:/Users/t-hdong/Desktop/gao/audit/mini-ray-review-2026-09-08.md)。该报告属于实施前静态审查；当时选定纯集合为17 passed / 3 failed / 2 teardown errors，并非全库结果。当前实施证据见验收账本，当前规模见§8.1。
- [mini-SGLang README](https://github.com/sgl-project/mini-sglang/blob/9a91cfafe754aa85daee49998176275667eb58f2/README.md)、[架构](https://github.com/sgl-project/mini-sglang/blob/9a91cfafe754aa85daee49998176275667eb58f2/docs/structures.md)。
- [nano-vLLM README](https://github.com/GeeeekExplorer/nano-vllm/blob/bb823b3e06983d71485a8e1f23715ebd87d98ef8/README.md)。
- [Ray Task Lifecycle](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/doc/source/ray-core/internals/task-lifecycle.rst)、[NormalTaskSubmitter](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/task_submission/normal_task_submitter.cc)。不承诺普通 Task 从不访问 GCS；这里退出的是逐结果发布门禁。
- [Ray 对象故障语义](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/doc/source/ray-core/fault_tolerance/objects.rst)、[ReferenceCounter](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/src/ray/core_worker/reference_counter.cc)、[递归对象序列化](https://github.com/ray-project/ray/blob/c3162dce8d064824293875c5d0bbfd76a54e04ce/doc/source/ray-core/objects/serialization.rst)。
