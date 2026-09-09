# 代码价值与精简审计

日期：2026-09-09。审计版本：`ce29981a547f83b53b0c1df9f91354dcf89d8e4f`。

**本轮继续定位后的确认清单：**[逐文件/逐函数清单](retirement-inventory.md)、[机器清单](retirement-inventory.json)。
已确认19项无用或退役实现（766物理行／590代码行）、24个未用import绑定；26份整套过期测试实现（10,792物理行）。
另在52个混合文件中标出147个函数当前形态及3组参数分支。下面早期521代码行是首批小范围统计，最新590是扩展复核后的合计，二者不相加。

90个直接import失效文件已全部列出，但失效不等于无价值；未列为整份过期的文件需保留有效合同并迁移。
尤其`test_multi_contained_output_path.py`被18个文件导入helper，不能直接连带删除。此次仍只找出和分类，没有修改运行时或测试。

本次先完成远端推送，再研究当前实现。`xinkuleee/mini-ray` 的 `main` 已指向上述提交；
`teaching-base-v0.1` 指向 `69106772567a4131f5ec76e898a3c4bf3bb6dbe6`，
`teaching-enhanced-v0.2` 指向 `ce29981`。使用普通 fast-forward、原子推送，没有 force、删除或改写标记。

本报告是现有[两阶段计划](redesign-plan.md)的实施后审查，不替换该计划，不改变既定能力。
本次未修改运行时或测试，也未重新运行测试；既有377项纯合同、37个真实进程用例的结果仍只属于其固定版本。

## 1. 判断

**不是所有代码都有当前运行价值；存在明确的无用实现、历史兼容负担和可以简化的表示。**
但也不能把50,277行的大部分直接判成冗余。当前最主要的成本来自真实执行、引用生命期、远端效果未知、重建和清理的组合。
它们有工程价值，当前组织方式与整体阅读成本仍不符合紧凑教学项目的理想形态。

前一轮完成的是两版限定合同和有限验收，**没有完成充分的代码精简与可读性纠偏**。
通过验收说明被验证的行为成立，不说明每个旧类、兼容分支、辅助模型和历史测试都值得保留。

本次能明确给出的结论分为三类：

1. **可保持现有行为的直接清理**：无活动调用的私有方法、未用数据类型、非运行时旧模型及少量未用 import。
2. **可保持现有行为的设计精简**：去掉单输出上的多槽表示，收敛同一参与方的多份进度记录，把夹具兼容移出运行时，收敛消息校验与构造。
3. **会改变行为的进一步缩范围**：取消某类引用/恢复/Actor/PG/退出保证。这不是普通去冗余，本报告不把它混入已授权的精简建议。

GCS普通结果事务和全局防环仍按既定目标保留；不恢复多返回槽、targeted reconstruction等退出能力。

## 2. 方法和实测规模

对全部59个源码Python文件进行了AST、token、定义和引用盘点；人工重点复核Core、Node、Worker、ownership、发布协议和测试入口。
对无引用候选继续检查静态handler表、动态`getattr`来源、`__all__`、测试和文档，避免把“没有直接函数调用”当作不可达证明。
没有使用覆盖率缺失、未进入当前清单或类数量来自动判无价值，也没有执行旧测试树做探测。

“代码行”是含代码token、排除纯注释/docstring/空行的物理行；“物理行”包含这些内容。它们都不是必要代码量或可删除量。

| 版本/范围 | Python文件 | 物理行 | 代码行 | 解读 |
|---|---:|---:|---:|---|
| 原始源码 `ef16ebc` | 58 | 67,167 | 55,196 | 改造前基线 |
| 固定基础版 `6910677` | 56 | 59,386 | 48,555 | 比原始代码行减少12.03% |
| 固定增强版 `ce29981` | 59 | 61,440 | 50,277 | 比基础版增加1,722行，即3.55%；比原始减少8.91% |

增强版三个新增模块合计1,150代码行；1,722是包括既有模块接线等变化的**净增量**。
所以两项增强机制不是50k规模的主要来源；取消它们既违背既定目标，也解决不了基础版已有48.6k的问题。

| 集中位置 | 代码行 | 主要成本 |
|---|---:|---|
| `core.py` | 11,139 | 提交、引用、依赖交接、lease歧义、恢复、发布和退出 |
| `node.py` | 6,522 | lease/资源、Worker pool、对象副本、Actor/PG参与者及清理 |
| `protocol.py` | 5,522 | 消息和值类型、完整身份、可选结果组合、校验与重建 |
| `ownership.py` | 3,481 | 当前对象状态、各种引用理由、历史收据和GC/退休 |
| `control.py` | 3,177 | 成员/死亡、Actor、PG及增强协议组合 |
| `api.py` | 2,577 | 大部分是启动、回滚、监控和退出，公开API包装约350行 |
| `worker.py` | 1,901 | 准入、执行、引用导入、回复托管、嵌套Core与退出 |

七个文件合计34,319代码行，占**68.26%**；Core单独占22.16%。
`Core._execute`约592代码行，`_register_submission`约460行；单纯拆文件不会消除这些职责之间的状态关联。

反过来，资源、Store、对象传输、ID、单输出身份、lease依赖/策略、runtime binding、CPU yield、ref transfer、contained edges、transfer pins
这12个基础模块合计约2,458代码行。机制本体可以很紧凑，膨胀主要发生在它们的组合层。

## 3. 第一批有充分依据的退出候选

以下是仓库自身的静态使用结论，不保证外部用户从未调用私有接口。删除时仍应做受影响验证。
表内候选互不重叠，不含顺带可删的import/空行，也不把测试删改计入源码收益。

| 位置（当前提交行号） | 物理/代码行 | 为什么可以退出 | 保留什么 |
|---|---:|---|---|
| [Core](../src/miniray/core.py) `:11637` `_known_output_completion_locked` | 15/14 | 无源码、测试、handler或导出引用；旧Complete查询入口 | 实际envelope、owner handoff和GCS准确历史查询 |
| [Node](../src/miniray/node.py) `:3427` `_start_worker_locked` | 5/3 | 无引用，只包装第一个Worker slot | 真正pool启动和启动回滚 |
| Node `:6840` `_drain_actor_workers_once` | 71/68 | 无引用的另一套Actor停止/清账实现 | 活动`_stop_all_actor_workers`和现有drain协议 |
| [Worker](../src/miniray/worker.py) `:1056` `_claim_system_error_failpoint` | 7/5 | 无引用，已由`_claim_failpoint`承担 | 实际失败注入及系统重试实验 |
| Worker `:1825` `_begin_drain` | 25/23 | 无引用；实际handler绑定`_handle_begin_drain` | 真正准入关闭、drain和退出ACK |
| Core `:1075` `_ActorInflightCall` | 13/6 | 无实例化、类型引用或导出 | 实际`ActorClientTable`中的call fence |
| [dependency.py](../src/miniray/dependency.py) `:608` `_deduplicate` | 9/9 | 无调用的旧helper | 当前参数编码与真正去重逻辑 |
| Core `:12322` `_resolve_task_dependencies` | 91/83 | 只有旧`test_core_owner_integration.py:196`调用，运行时无调用；旧路径还拒绝foreign refs | 把submitted hold断言迁到真实依赖准备/派发路径 |
| Node `:770` `_legacy_unregister_from_gcs_best_effort` | 29/20 | 旧测试仅monkeypatch或断言不调用，运行时不调用 | Driver sentinel/membership的准确死亡处理 |
| Node `:3516` `_stop_worker` | 5/3 | 旧测试只将其列为禁止调用项 | pool停止和受管进程清理 |
| [runtime_state.py](../src/miniray/runtime_state.py) 整体 | 401/287 | 明确标注deprecated、non-runtime facade；无源码入口，只有旧模型测试及历史清单使用 | 将仍有价值的CPU/资源断言对应到实际`ResourceLedger`和Node，不维持第二个教学模型 |

前七项是**145物理行/128代码行**；后三个旧私有方法是125/106；整个旧模型是401/287。
合计可作为第一轮清理对象的实现为**671物理行/521代码行**。这是候选定义跨度的实测总和，不是已经完成的删除diff。

这只相当于源码代码行的**1.04%**。即使全部退出，简单相减仍有49,756代码行；
不能据此承诺“删死代码即可回到两万行”。

另有明确未用的Core/Node import，例如`contextmanager`、`RecoveryDecision`、`PullState`等。
它们往往与仍使用的名字共处一条import，不把名称数当节省行数；公开re-export和类型注解不能误删。
`debug.py`虽然没有源码内部import，仍有外部诊断测试调用，不能按同样标准直接判成死模块。

## 4. 活动代码中的多余设计

### 4.1 单输出仍支付多输出表示成本

[task_outputs.py:32](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/src/miniray/task_outputs.py#L32)
要求唯一输出为`(ObjectID.for_task(task_id, 0),)`，但后续仍携带`slots`、`results`、`output_ids`、`slot_index`及按槽字典。
Node journal的结果和退休信息仍用只能含键0的dict；owner退休先组装membership元组，再拒绝长度不为1。

这不是新增正确性保证，而是已经退出的能力维度留下的表示。建议使用单个result、payload、membership和退休记录，
从Task/attempt派生唯一ObjectID；仅child index与真实replica集合继续保持多值。
保留`wait(num_returns=k)`、一个值中的多个ObjectRef和多个副本，它们与Task多输出没有关系。

另有无活动源码调用的`full_output_ids`别名、`ordered_edges`便利属性、`journal.retire_slot`和`OutputPublicationSlotCleanupProof`旧分支。
该cleanup proof不是必要的真实Drop收据；正常路径使用实际adoption或owner-death cleanup。应退出旧接口，
其中仍有价值的收据防篡改、精确重放断言迁到活动路径。

### 4.2 为旧测试夹具维持运行时兼容

这里的冗余不仅是行数，它让读者难以分辨正式协议。已有明确例子：

- [Core:1326](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/src/miniray/core.py#L1326)
  `_make_message`反射dataclass字段并静默丢掉未知参数，仍服务早期`spec/task_spec`命名过渡；当前可以直接调用规范构造器。
- Core `:1790`、`:1914`在实际路径容忍没有初始化route的`object.__new__`夹具；`:1583`、`:2942`维护旧`_inline_gc_obligations`别名。
- [Node:2191](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/src/miniray/node.py#L2191)
  把旧单Worker字段导入pool，随后再把pool写回旧字段。pool是唯一权威，但仍维护重复表示。
- Worker `:1024`捕获`TypeError`后重试旧shutdown签名；Core `:8061`按异常文本退回不带route的旧调用。
  这些是测试double适配，不是教学机制，还混淆签名错误与方法内部错误。

建议先让少量正式fixture factory显式构造必需状态和正确签名，再删运行时fallback。
不能一看到`getattr`就删：部分初始化失败、可选trace以及真实未决义务仍需处理。

### 4.3 同一参与方内的台账可合并，跨参与方的事实不能合并

[Node adapter:110](https://github.com/xinkuleee/mini-ray/blob/ce29981a547f83b53b0c1df9f91354dcf89d8e4f/src/miniray/output_publication_node.py#L110)
有8个字典和一个ticket set。其中terminal pending/reported、lease pending/converged重复用同一个publication键保存同一个Complete。

可以改为一个publication progress record，保存独立的“本地lease已收敛”“远端报告已确认”等字段；
Complete从本地journal取。减少跨字典的移动、复制、pop和一致性约束，但不能把两个完成事实合成一个`done`。

Owner同时保存退休plan表和嵌入同一plan的完成receipt表，也可收成`retirement_id -> pending plan | completed receipt`。
历史精确重放仍要验证原请求；收据不能随当前对象GC丢弃。

Core中GC、Node-loss交接、foreign hold、location handoff等责任也应各自持有状态，并提供明确的pending/drain接口。
目前shutdown要查看大量私有dict与counter，说明封装不足；把方法移动到别的文件、仍让Core任意读写其内部状态，不算真正简化。

### 4.4 重复校验和完整快照回包值得收敛

发布/ownership的12个相关文件中静态统计到144处具名`replace(...)`调用。一个GCS请求会在构造、authority入口、
新snapshot、reply构造、client接收和journal接受等多层重建。每个阶段ACK又携带完整request和snapshot，重复装载清单、准备收据和历史。
这些是重复工作线索，不是144处都可删。

建议区分“外部消息解码校验”和“权威已持有的值”；阶段ACK只返回完整身份、准确stage/receipt及该请求需要的事实，
恢复查询仍返回完整snapshot。owner锁内的validate＋commit可复用与本地状态绑定的validated transition，
避免重复构造同一大清单；不要引入通用schema框架或通用事务引擎。

必须保留接收端完整请求绑定、反序列化重验证、对外快照隔离，以及每次RPC后重新检查当前epoch/fence。
此前Seal校验修复和深层对象篡改反例说明这些边界有真实作用；不能用“trusted loopback”省略必要校验。

`GetWorkerLeaseOutcomeReply`将多个有意义的结果形态混在可选字段中，单个`__post_init__`约195代码行。
可以用明确结果变体或窄构造入口减少不可能组合；所有现有成功、失败、托管退休和cleanup-pending语义仍要保留。
不要用未校验的`kind/payload`字典换掉类型，只为减少class数量。

## 5. 哪些复杂度有价值，不能作为冗余砍掉

| 必须区分的事实/职责 | 删除或混合后的问题 |
|---|---|
| Node Complete、owner READY、GCS terminal/adopted | W2/W3丢失回复或Node死亡时会错误推断可见性/成功；增强保证被撤销 |
| 成功metadata与bytes可用 | 有成功历史但bytes全失时伪造可读取结果 |
| 图PREPARED/COMMITTED、前进fence、真实child hold | fence后立即抹图或以图替代引用保活，会漏环/提前回收/让晚到消息复活 |
| 回复托管退休与对象GC | 收到adoption就提前删仍被引用的对象，或永不退休回复缓存 |
| publication死亡清理与owner-wide副本清扫 | 没有publication记录的依赖副本/put残留无人清理 |
| put与Task的执行身份 | 为共享流程伪造lease、Complete或lineage，教学含义和故障语义都会出错 |
| 精确资源账本、真实进程回滚、clean与forced退出 | 无法解释或验证资源归还和实验重复运行 |

源码中明确启动/退出清理的方法保守统计已达2,137代码行，还不含多数消息和通用GC。
它们有成本，但“统一kill所有进程”会改变合同，不是同义精简。
同理，取消foreign retained reconstruction、Actor同Node restart或PG真实2PC能节省代码，却属于新的范围选择。

## 6. 外围代码比运行时更需要退出历史负担

| 静态盘点 | 数量（物理行口径） | 判断 |
|---|---:|---|
| `tests/`中Python文件 | 348文件，133,422行 | 包含大量旧实现合同 |
| 当前29 pure＋37 smoke涉及的test模块 | 55模块，18,058行 | 加实际helper import闭包为59文件、18,712行；不是应保留测试的硬上限 |
| 未选test模块 | 285模块，114,344行 | 未选不等于无价值，不能整批删除 |
| 确定失效的顶层import | 90文件，涉及47,366行 | 引用已删除模块或symbol；当前选择中为0；不是90个失败case，也不是全部失效数 |
| 历史reviewed manifest | 237文件，其中69文件有直接失效import | 不应继续承担当前运行门禁 |
| bounded硬编码清单 | 145个exact，比当前37项多108项 | 29个目标文件有直接失效import；被列入不是当前可运行证据 |
| `docs/history/` | 48文件，33,713行 | 其中14份源码文本副本13,966行，加重检索与学习负担 |
| 两版固定`artifacts/` | 126文件，约249KB | 体积小、具有独立版本证据价值，应保留 |

确切过时例子包括旧`test_multi_contained_output_path.py`的TargetExecutionKey、
`test_actor_arguments.py`依赖已删除Actor Ref模型、`test_output_recovery.py`依赖旧事务族。
还有能导入但语义已过时的`test_trace_contract.py`旧阶段断言，静态缺失import统计不会覆盖它。

建议按合同去向收口：退出能力的专属测试从活动树及执行清单一起退出；混合文件先提取仍有效的epoch、hold、ACK、Drop和GC反例。
例如未选的`test_ids_resources.py`、`test_object_ownership.py`、`test_drop_object_replica.py`包含高价值基础合同，
不能只因为未进入377项就删除。测试间目前有大量跨`test_*.py`导入helper，应抽出少量职责清楚的support模块，
避免一个公共close helper把整份旧协议测试拉进来。

当前baseline和bounded双清单要求一个新smoke登记两次；应收成一个显式selection数据源，执行器保留精确路径校验、模式、
环境净化、PID归属/重用防护、超时与中断清理。历史`run_reviewed_pure.py`及锁定455/201/254等旧数字的测试可以退休或迁移。
import前scope保护与进程树隔离处理不同风险，不能当重复治理一起删。

文档应只保留一套当前状态、学习路径和协议说明。旧roadmap/handoff/testing中的“待授权”“尚未实施”应转为短历史索引，
大段历史副本改为固定Git对象链接；先核对归档对象和hash可以取回。两版tag和artifact保持不变。
当前Core开头仍称GCS只管membership/Actor/PG，runner及CI仍称first-stage，属于已经发现的教学文字漂移。
修正这些措辞有阅读价值，但不应当作源码精简量。

## 7. 精简顺序与现实预期

| 顺序 | 做什么 | 为什么先做 | 验证与停止条件 |
|---|---|---|---|
| A：明确残留 | 清理第3节私有残留、旧runtime_state及未用import；迁移有用旧断言 | 证据最强、不会改变目标机制 | 受影响纯合同与已有Actor/重试/依赖/退出exact；确认handler/导出没有悬空 |
| B：让活动树可信 | 统一当前执行清单、迁移混合测试、退休旧能力文件和历史复制文档 | 避免后续重构被废弃夹具与旧名称锁牵引 | import闭包/manifest一致，保留两版证据及高价值负例，不为清理跑完整遗留树 |
| C：消除已退出维度 | 单输出标量化，删除逐槽退休旧接口 | 从数据模型上减少不可能组合 | 单值tuple/list、contained refs、whole重建、GC、W1–W4及真实防环保持 |
| D：收敛职责和台账 | fixture移出runtime；Node progress/owner retirement规范记录；Core领域状态归属清楚 | 减少跨字典一致性与私有字段知识 | 每项事实有唯一负责方；shutdown通过其义务接口查询；保持未知ACK和死亡边界 |
| E：减少重复边界工作 | 窄ACK、规范decoder、锁内validated transition | 有潜在运行与阅读收益，风险高于死代码清理 | 原畸形请求、篡改snapshot、精确重放和旧epoch反例仍过；实际profile后才报性能改善 |

A中当前明确候选只有521代码行。发布/ownership局部的单输出表示、旧接口和台账收敛，
人工估计合并约200–400**物理行**，属于低置信度重构预算，各项有重叠；不与521代码行直接相加，也不外推整个Core。
重复校验/消息表示/Core职责重设计可能带来更大收益，但尚无实际diff证明能降多少。

**目前没有依据承诺保留全部合同就能压到两万或三万行。**
合理做法是先完成A–C中的具体一轮，用实际剩余代码、状态字段、消息形态和阅读路径重新校准，再判断D/E的收益。
若仍远离教学体量，要讨论的是下一轮实现结构或明确的行为范围，而不是继续增加协议、机械拆文件或删必要验证来达标。

## 8. 本次产物与边界

完成了远端代码/两个tag推送以及本报告。运行时、测试和既有证据未修改；本轮没有执行pytest。
审计遍历了全部源码的静态结构，但没有对每一行做形式化用途证明，未声称列出了所有冗余。
本报告新增的发现和建议尚未实施，也没有用旧版本测试结果证明未来删除安全。

工作区外的复核材料：

- `audit/code_value_inventory.py`、`audit/mini-ray-code-value-inventory.json`：AST/token定义、引用与候选清单。
- `audit/section-deadcode-review.md`：私有残留的动态入口/测试使用复核。
- `audit/redundancy-publication-ownership-ce29981.md`：发布/ownership表示、台账与复制成本。
- `audit/ce29981-architecture-value-review.md`、`audit/ce29981-architecture-method-cost.json`：职责集中与方法成本。
- `audit/redundancy-tests-docs-section.md`、`audit/redundancy-test-doc-inventory.json`：测试、清单与文档静态盘点。
