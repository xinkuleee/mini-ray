# mini-ray 两条教学分支的完整整理计划

日期：2026-09-10。状态：**R2.2两版有限验收已保存；独立复审发现的四项P2已在R2.3按计划修复并完成有限验收；本次阶段性提交固定R2.3成果，未推送，P3仍明确保留**。实际进度见[执行记录](cleanup-progress.md)，本轮目标/边界/验收见§11；两条本地分支保留，未发布远端。§1–10保留R2.2原输入及执行合同，旧完成概括以§11审查纠正为准；本次未创建/未实施描述相应历史修订轮。
修订：R2.3追加§11，限定修复全部审查P2；原R2.2在R2.1的清理范围和修复依据上，补齐两条实际Git分支的定位、存续、同步、CI与交付检查；结构重构仍逐包验证收益。

本计划覆盖代码、测试、工具、配置、示例、文档、历史资料和验收产物。
原R2.2逐项输入及本轮p2_remediation见[机器清单](project-cleanup-plan.json)，逐文件导航见[整理范围索引](project-cleanup-index.md)。
此前[价值审计](code-value-audit.md)与[退休清单](retirement-inventory.md)是输入证据，不是另一套执行计划。

## 1. 两条实际分支分别整理，原版本保持不变

R2.2制定时实际只有`main`分支和两个固定tag；当前`teaching-base`已独立验收，`teaching-enhanced`已从其验收交付HEAD派生并完成自身验收。整理完成必须同时保留基础版B、增强版E两条实际`refs/heads/*`分支；两个tag或同一分支的两个提交不能代替此交付条件。以下固定源码是审计起点，不是已创建的清理分支：

| 版本 / 计划交付分支 | 固定源码 | 现有验收起点 | 教学定位与整理交付 |
|---|---|---|---|
| B / `teaching-base` | `teaching-base-v0.1` / `69106772567a4131f5ec76e898a3c4bf3bb6dbe6` | 24个纯文件、32个smoke；历史318 passed/1 deselected | 参照Ray Core关键机制的mini-ray基础教学实现；无普通GCS发布事务、无全局防环 |
| E / `teaching-enhanced` | `teaching-enhanced-v0.2` / `ce29981a547f83b53b0c1df9f91354dcf89d8e4f` | 29个纯文件、37个smoke；历史377 passed/1 deselected | 基于整理B增加两项mini自定义协议保证的enhanced教学实现；两项都是确定交付项 |

B与Ray的关系，定位上类似mini-sglang与SGLang、nano-vllm与vLLM：通过可读的小型实现理解选定机制，并逐项说明与Ray的对应、简化和省略；不承诺生产Ray的完整实现、接口兼容或相同内部协议。
E复用B的共同基础，额外研究**GCS普通结果发布事务、全局ObjectID引用图防环**；本轮自定义feat范围限于这两项，不自动扩展成任意新功能集合。
增强收益是增加一个存活的发布事实来源、全局拒绝受支持contained-edge入口的成环候选；成本是普通发布路径增加同步GCS依赖，以及预留、补偿、死亡清扫、fencing和收据退休的状态与测试。
单GCS内存权威不等于HA、持久恢复或owner接管；成功元数据也不能恢复bytes。

**实施顺序：拆分原B→E差异中的公共修复与增强协议 → 从B建立清理分支并先回迁公共修复 → 低风险整理检查点 → 经逐包评估的结构清理 → B独立封版 → 从清理B派生E并适配真正增强增量 → E独立验收。**
计划分支名为`teaching-base`和`teaching-enhanced`。实施时核验引用并记录创建结果；同名如已有其他工作，不覆盖，先核对来源再更新计划映射。本次没有创建分支。原tag与原artifact不重写；新结果用新提交、标记和证据目录。
不把原E的Core/Node整文件覆盖回来，不同时在两条未稳定分支重复重写全部代码。

用户最新要求更新了原[两阶段计划](redesign-plan.md)中“基础版固定后继续增强”的后续安排：基础版也保留可继续修复的实际分支。
本轮交付两条整理分支；每个版本内部仍只有一种运行时，不引入base/enhanced功能开关、假GCS、通用后端框架。
不额外承诺未来所有新功能都双线开发，但此次公共修复和清理必须在两条分支落实。

共同保留：单输出Task/ObjectRef、真实进程、lease/direct push、资源与依赖gate、INLINE/STORED、put、nested refs、
独立borrower、实际GC、CPU yield、有限whole reconstruction、串行Actor/同Node restart、两bundle硬约束PG和既定故障边界。
B不加入增强协议；E两项都保留。增强版不是“更接近生产Ray”，基础版也不缺所选机制的必要正确性逻辑。
不恢复多返回槽、targeted reconstruction、自动StoredArg、Actor Ref参数扩展、Actor跨Node迁移或soft PG策略。

### 1.1 分支存续、公共修复与main入口

- K0从固定B创建`refs/heads/teaching-base`，K7完成B独立验收；K8从B已验收的交付HEAD创建`refs/heads/teaching-enhanced`并记录派生点。E旧tag仅提供经过分流的增量输入，不以整文件覆盖新B。
- 两条分支在K9同时存在，并在此次交付后继续保留；E完成后不能删除B只留tag。tag只固定检查点，学习入口始终先指向B分支并附可复现实验的固定提交。
- 分叉后的共同缺陷先在B修复、验收，再以审查过的提交同步或适配至E，记录B源提交、E对应提交及差异原因。E中先发现的共同问题也按此顺序提取；增强专属提交不反向合入B。受影响分支分别重验，不把E新增保证变成B交付前提。
- `main`保留既有增强源码及历史，不映射为第三个整理版本；此次整理不在main维护另一套运行时。交付时仅向其README追加导航，说明main源码为旧增强检查点，推荐`teaching-base`学习Ray Core、`teaching-enhanced`研究自定义协议。导航按各分支实际通过状态更新，不提前宣称E完成，不重写历史或为此强推。
- 远端发布时同样核对`refs/heads/teaching-base`、`refs/heads/teaching-enhanced`分别匹配本地交付HEAD，不能只推tag。尚未发布时据实标注本地交付与远端未验证；本次计划修订不创建、提交或推送这些引用。

每版交付记录分开保存`branch_ref`、历史起点、`fork_point_commit`、`tested_source_commit`、`head_at_acceptance`及证据提交/路径；这些字段记录已解析的完整SHA，不填写将来才知道的虚构值。
若测试后只追加证据或说明文档，允许最终HEAD后移，但必须列明与实测提交之间的差异，确认源码、测试、示例、运行工具、manifest、依赖锁和构建配置未变；测试结果仍归原`tested_source_commit`，不能改标为新HEAD实测。任一执行输入改变，受影响行为必须按新源码重验。
K9重新解析两条分支HEAD，核对上述映射与证据；HEAD意外漂移或缺少映射时不能宣称双分支交付完成。验收摘要可在交付报告记录最终HEAD，无需把包含自身提交SHA的记录再次提交而循环追逐HEAD。

## 2. 全项目范围与判断依据

已从两个固定提交分别提取文件并检查：**B 541个tracked文件，E 614个；路径并集614个，509个相同、32个内容不同、73个增强独有。**
三份当前未跟踪审计文件另行登记。每个路径在JSON的`file_actions`均有两版状态、身份、保留/改写/移除策略和详细证据入口。

| 范围 | B | E | 口径 |
|---|---:|---:|---|
| 源码Python文件/代码行 | 56 / 48,555 | 59 / 50,277 | 排除纯注释、docstring、空行 |
| 源码Python物理行 | 59,386 | 61,440 | 不含两个golden JSON |
| 测试Python文件/物理行 | 340 / 130,975 | 348 / 133,422 | 含helper |
| docs文件 | 59 | 60 | 不含README，含history |
| docs/history文件/物理行 | 48 / 33,713 | 48 / 33,713 | 非执行历史材料 |
| 示例 | 7 | 7 | 仅例01文本不同 |
| 固定artifact文件 | 65 | 126 | E含B原65份＋61份增强证据 |

源码做AST/token、调用、handler、动态入口和export筛查，再人工复核候选；测试全树做合同/导入/函数筛查，受退役能力影响处深入阅读。
151个已分类旧测试文件在两版均存在且SHA相同，可以继承函数级审查；**迁移目标仍按B/E分别确定**。
11份现行测试/helper的版本差异与8份E独有文件另行核对，没有将B正常owner-led夹具误判过期。

| 判定 | 依据 | 策略 |
|---|---|---|
| 明确无用实现 | 无源码/handler/dynamic/export使用，或只剩已退出合同调用 | 删除；必要旧断言先迁移 |
| 明确弃用合同 | 内容实际要求multi-return、targeted、自动lift、跨Node Actor等退出能力 | 退出旧接口/测试实现，不恢复能力使其变绿 |
| 历史表示负担 | 正式构造已唯一，旧表示仅服务夹具/兼容；同参与方重复存同一事实 | 改表示与构造，保留正确性含义 |
| 绑定过期、目标有效 | import/字段/旧callback失效，但仍测试hold、epoch、ACK、资源、GC | 修或迁移，不能整份删除 |
| 历史文档 | 旧阶段复制、过期“当前/待授权”描述、源码文本副本 | 提取有效知识，核实归档后移除工作副本 |
| 当前机制或证据 | 实际API路径、权威事实、错误反例、独立日志 | 保留；仅受具体重构影响时适配 |

文件覆盖完整不等于对每一行做形式化必要性证明。清单的保留/待验证项不是删除许可；未进入当前测试清单也不是无价值证据。
Git原始blob与Windows落地文件因LF/CRLF可能SHA不同；JSON分开记录Git OID、raw SHA与checkout SHA。
归档用`git cat-file`按字节读取核验，不能用PowerShell文本重定向改变换行后称原文逐字相同。

### 2.1 先分流B→E增量，公共正确性修复不能等到K8

固定E相对固定B的差异并非全是中央事务/全局图。**文件名含enhanced、或首次出现于E，不是“B不需要”的判据。**
K0新增差异分流账本，以提交差异的具体方法/行为为单位，覆盖全部16个源码增量路径及相关测试，分类为公共修复、增强专属、观察/夹具适配，混合文件必须拆hunk处理。
已确认公共修复在K1有界入口可用后、K2大批删除之前落实到B；K7不得用“旧B通过过”跳过这些反例。

| 公共修复 | 已核实的问题与范围 | B执行要求；E处理 |
|---|---|---|
| 完整child死亡事实 | B `Core._drive_output_node_loss_once`将`dead_worker_record()`的简化`DeadWorkerReferenceRecord`放入cleanup；`NodeLostOutputResolution`只接受完整`WorkerDeathRecord`或精确Release，导致构造失败并重复重排 | 从真实成员死亡后缀保留完整proof，B用于Node-loss cleanup并供公共退休修复使用；E另保留put/GC收据支持。不把未证明的B put问题列为必修，不依赖GCS普通事务 |
| 死亡child的旧结果退休 | B旧membership退休只能向已死child请求Release；E已支持完整已安装死亡证明覆盖对应hold | B owner退休接收准确Release或已安装的完整child死亡证明；保持每hold覆盖、不可跨owner清账、不能伪造ACK |
| 固定Node-loss本地裁决 | B在已选择UNKNOWN/DISCARD、部分child释放失败后，晚到envelope可再次尝试向ABORTED handoff登记Complete，导致反复失败；E已优先使用latched事实并检查ABORTED | B只移植latched完整/未知事实与ABORTED守卫，保留本地裁决，不读取E的central.complete、不加入图门禁 |
| 成员死亡fence推进公平性 | B每次只推进pending首项，首项持续PINNED/超时会饿死后面的独立fence；E有轮转cursor | B在现有成员清理driver中轮转未完成fence，锁外RPC；不引入E新增的publication/sweep两类交替协议 |

`tests/unit/test_enhanced_owner_retirement.py`不依赖增强模块，它的10个精确死亡/退休合同应迁成两版共有测试，建议去掉enhanced命名。
新增基础反例必须通过B实际owner/Node-loss/成员事实路径，证明简化proof不会进入cleanup、死亡child不再等待不存在的Release；该组合覆盖此前漏接线，不能只移植表层dataclass测试。
每个分流项必须列原B/E位置、回迁hunk、B有限负例、E回归及“无增强依赖”检查。账本中未分类的差异阻止K7；不是把E全文件直接合回B。
KEEP→LOST后的C7 proof重放、图child闭包收据等差异依附E新增远端提交，不因代码看似一般就机械回迁B；逐hunk记录保留在哪个版本。
旧tag和旧318/377等结果保持历史事实，新B的正确性修复与新结果另记。

## 3. 源码整理：明确删什么、保留什么

### 3.1 十九项确认残留，两版都有

19项在B/E中都存在，定义大小相同但行号不同，合计**766物理行/590代码行**，跨度不重叠。两版准确位置见范围索引。

| 对象 | 动作与理由 | 保留/替代 |
|---|---|---|
| Core `_known_output_completion_locked` | 删除无调用旧查询 | B实际handoff/envelope；E另有准确GCS历史，不由当前READY构造过去事实 |
| Core `_resolve_task_dependencies` | 迁移旧测试后删；无runtime调用，旧同步路径还拒绝foreign refs | 真正`_prepare_task_dependencies`及queue/lease，保留submitted hold到dispatch |
| `_ActorInflightCall`、dependency `_deduplicate` | 删除未用类型/helper | 实际ActorClientTable fence和当前去重 |
| Node `_start_worker_locked`、`_stop_worker` | 删除旧first-slot包装，更新旧禁止调用列表 | 真正pool生命周期与公开first-worker诊断 |
| Node `_legacy_unregister_from_gcs_best_effort` | 删从不被runtime调用的旧unregister | Driver sentinel/成员死亡，旧spy改观察实际消息 |
| Node `_drain_actor_workers_once` | 删除无引用的另一套Actor退出实现 | 活动`_stop_all_actor_workers`和真实资源/退出ACK |
| Worker `_claim_system_error_failpoint`、`_begin_drain` | 删除未用包装/旧drain | `_claim_failpoint`、`_handle_begin_drain` |
| `runtime_state.py`整模块 | 迁移资源断言后退出安装包；deprecated且无runtime接线 | 实际ResourceLedger/Node/CPU yield，不再维护第二个教学状态模型 |
| Recovery `validate_terminal_reconstruction_failure`＋`FAIL_RECONSTRUCTION_TARGETS` | 删除targeted-only失败语义；旧测试仍用3returns/healthy sibling | 单输出ERROR/LOST、预算、epoch fencing |
| Node `_legacy_worker_compat`两次写入、monitor `_processes` | 删未读字段 | pool状态；`_sentinels`已有相同Process强引用 |
| journal `retire_slot`＋`OutputPublicationSlotCleanupProof` | 退出旧逐槽API/无人生成proof | 只收窄其union/分支，保留共享tombstone、`retire_completed`与真实Drop/Release |
| `StoredPublicationQueryDisposition` | 删除旧图query残余枚举 | B handoff/E Publication现行查询 |

另24个import绑定在两版分别核查未读；只删绑定，不按名称数累计行数。删除与旧引用修正同次提交，不增加永久alias。

### 3.2 二十一项候选接口：明确9删、7改、5保留

| 策略 | 具体接口 | 原因与改法 |
|---|---|---|
| 删除9项便利入口 | `derive_task_id`、`derive_object_id`、`forget_collected_object`、`record_terminal_system_failure`、`live_node_infos`、`lineage_inputs`、`rewrite_nested_holds`、`abort_task_lineage_edges`、`from_node_info` | 改用已有ID类方法、validate/commit或完整abort入口，底层校验/locked helper保留；这是新版本收窄未用便利面的决定，不是任意公开API无用 |
| 先迁移7项再退出旧入口 | `abandon_renewal`、`add_outgoing_contained_edge(s)`、`collect_if_unused`、`collect_unused_with_edges`、`publish_task_outputs`、`commit_publish_task_outputs` | fixture改测真实原子结果＋边、冻结collection plan、准确收据。未知效果反例先落实，Actor正在用的validator/validated commit不能删 |
| 保留5项 | `TaskRecord.mark_running`、`first_worker_forced`、`admission_is_open`、`has_registered_lineage`、`publish_inline` | 合法状态操作、公开诊断或当前有效纯fixture，无足够依据判弃用 |

21项不计入590行的确认跨度。新整理版允许内部wire/私有API不兼容，但不要求跨版本混合Worker互通；旧tag保留原接口。

### 3.3 退出生产代码中的旧fixture兼容

先在`tests/support/`建立少量明确的Core/Node/owner工厂：初始化真实ledger/table/queue与正确签名，只替换网络/进程外部边界。
随后逐项修改：

1. Node只存`_workers`；删singular镜像/importer，公开first-worker属性只读派生。
2. Core只存`_object_gc_obligations`；删inline旧别名/惰性导入。route只接受正式HomeRoute/InstallClusterSnapshot。
3. 删除TypeError文本识别旧参数、shutdown旧签名fallback；保留timeout、捕获route、owner端点保持逻辑。
4. 删除Worker缺`_replies`便准入、从旧cache补accepted状态的fixture分支；真实accepted/未决义务保留。
5. `_make_message`使用规范构造器，未知字段应暴露错误，不再反射过滤历史参数。
6. OwnerService/Worker owner地址各自保留；必需handler走明确接口，退出早期可选新方法迁移壳。

理由：正式运行时不再了解几十种旧夹具形状，必需状态和内部错误清楚。不能机械删所有`getattr`；可选trace、真实部分启动失败与已关闭参与方仍需处理。

## 4. 结构修改：具体目标形态

以下是待逐包落地或以证据保留现状的结构方案，不与低风险清理强制捆绑。逐方法范围、API、不变量与验证见JSON。
K3结束先形成低风险整理检查点；K4–K6每包先选一条普通值＋含Ref生命周期试做，记录删除的表示/状态、增加的接口、复制成本与阅读路径。
证明净收益且合同不变才推广；若只搬代码、引入第二权威或无法证明别名隔离，撤回试做并记录该包“保留现状”的证据与限制，不假称已简化。
全部计划条目仍需明确处置，但不把尚无收益证据的Core/ACK重设计设成两版交付的强制前提。公共正确性修复不受此收益豁免。

### 4.1 真正的单输出表示

目前禁止多返回，却层层携带`slots/results/output_ids`元组、恒零`slot_index`和逐槽dict；这是已退出维度的历史成本。
目标结构含义如下，代码尚未实施：

```python
TaskExecution(attempt_id)              # task_id/唯一object_id规范派生
PublicationID(lease_id, execution)
OutputManifest(header, value, digest)  # 一个value，多个transfers仍保留
PreparedOutput(manifest, payload)      # 一个bytes，source handles另有托管
OutputEnvelope(manifest, complete, result)
OwnerOutputMembership(manifest)
```

修改`task_outputs/output_publication/output_protocol/output_discovery/protocol/journal`及Core/Worker/Node/ownership调用端。
P1还必须同步适配`output_publication_node.py`、`output_handoff.py`、`publication_gate.py`和`reconstruction_runtime.py`的直接构造/slots读取；
E在K8同步适配`enhanced_publication.py`等新增消费者，B不创建这些模块。当前AST核对为B14个、E15个直接源码消费者，逐路径清单和检查依据在JSON。
这些仅为P1新类型/接口兼容的必要同提交修改，不提前实施P2/P3的进度表/退休表重构，也不改变reconstruction算法。
实施时重新检查源码和保留测试/fixture的完整导入及构造闭包，未适配消费者阻止P1提交，不能依赖未来K5包才恢复可运行。
以普通值＋含引用值完整执行/回收纵向迁移，在一个可运行提交闭合，不留新旧双模型。
保留多个child/borrower/replica、`wait(num_returns=k)`，tuple/list仍是单个Python返回值。
规范派生继续绑定Task/ObjectID/attempt，job/owner/Node/lease/hold/checksum校验不减少。

### 4.2 Node progress和owner退休记录规范化

Node八个dict收为一个publication progress记录；本地lease收敛、远端报告确认必须是独立字段，canonical Complete仍来自journal。
journal结果/退休slot字典改一个result/retirement；child效果仍保存sent intent和真实reply。E保留ARM/child cleanup证据，B不预埋GCS字段。
Owner plan表与包含相同plan的完成receipt表改成`retirement_id -> pending plan | completed receipt`，完整旧请求仍可精确重放。

收益是减少同参与方内重复键和值、跨dict移动和一致性要求。当前membership、历史adoption、GC tombstone、attempt fence不可混为一事。
Node Complete、owner READY、GCS terminal/graph/adopted是不同权威事实；图释放不是child Release，也不是bytes删除。

### 4.3 typed清理义务与限定Core职责提取

put自由work字典、output-loss/retirement嵌套dict改为`PutHandoff`、`MaterializationWork`、`ChildTransferProgress`、`OwnerOutputWork`、`ObjectCollectionWork`等少量具体记录。
记录实际请求、sent intent、回执、本地裁决和未决责任，不把真实receipt换成一组可能矛盾的成功bool。

| 领域 | 负责 | 不取得的权威 |
|---|---|---|
| TaskSubmissions | 参数与submitted/lineage/borrower hold准备、提交/撤销、finish | 不另造Task成功表；owner/recovery/enqueue仍在明确组合锁提交 |
| LeaseAttempts | 既有lease/cancel/push/outcome continuation和有界重放 | Node仍决定执行/资源，恢复准入仍归现有权威 |
| OwnerObjects | put、adoption尾部、GC/退休和结构化pending查询 | 不执行DFS，不取得GCS图或child引用计数 |

模块只拿需要的typed适配器、RPC、enqueue/wake，不拿整个Core后任意读写私有字段，不用mixin或泛型服务容器。
shutdown保留原时序，改为询问领域未决责任；不能提前关闭owner端点。
本包不重写调度算法、资源ledger、整个reconstruction算法或Actor/PG模型。仅搬函数而不改变状态归属不算完成。

### 4.4 解码校验与窄ACK

外部请求/回复严格解码成规范值，权威内部插入时校验；同一锁内且没有RPC时，validate＋commit复用绑定expected revision的本地transition。
不能把已验证token穿过网络或解锁后任意提交。外部快照仍deep-detached；无法证明内部别名隔离时保留现有copy。

阶段ACK只携带准确identity、stage receipt和必要事实；恢复query保留完整snapshot。ACK丢失、请求重绑定、frozen深篡改、旧ACK伪装新权限都必须有反例验证。
不引入通用反射schema、通用事务引擎或未经校验的kind/payload字典。
本次不合并C0/C1或C4/C5 RPC：会改变历史/准入关系与观察窗口，净收益未证明；不混入当前必做清理。

## 5. 测试：每份删除还是修改

151份旧分类文件B/E逐字相同：**26份整套实现过期、75份混合迁移、45份仅绑定失效、5份仅部分selector过期**。
52份文件中的147个函数当前形态及3组参数分支需拆迁；这不是147条业务不变量均可删除。
两版相同90文件存在确定顶层missing import，共涉及47,366物理行；它们与上述分类重叠，不能相加。

### 5.1 两版都退出的26份旧实现

完整文件列表在范围索引，分为九份integration退役能力实验、十份unit退役能力/旧模型测试、七份旧分类名册锁。
它们的主合同涉及多返回/targeted、Actor跨Node迁移或Ref参数、自动StoredArg、RuntimeState旧facade、generic export-pin旧协议，
以及固定旧函数名/参数数量的`*_safety_classification.py`。共10,792物理行、99个顶层测试函数。

删除的是**旧测试实现**。先核每份`retained_contract_groups`，把仍适用的精确身份、ACK、预算、hold和GC负例迁到B/E实际路径；
再删除旧harness/文件、相应执行入口和旧分类锁。不能用“整份过期”代替查看helper依赖与共同反例。

### 5.2 混合/绑定失效：两版的改法不同

| 类型 | B怎么改 | E怎么改 | 原因 |
|---|---|---|---|
| 75混合文件 | 抽取单输出/Complete/引用/GC共同反例，接owner-led；纯中央事务/图断言退出B | 同共同反例，适用增强保证迁到新PublicationAuthority/Node/owner边界 | 旧模块被删除，不等于它曾解决的所有问题消失 |
| 45绑定失效 | 修旧import/属性/helper，使用B实际收据 | 使用E真实GCS/图收据，不恢复旧整族 | 缺模块只证明绑定坏，业务目标未必弃用 |
| 5部分selector文件 | 只删/改精确列明函数，保留其余 | 同，保留E额外保证 | 避免整文件误删 |
| 列外文件 | 保留，受具体改动影响再检查 | 同 | 未选入gate或本轮未确认，不是删除证据 |

例：旧“Node死亡后一概拒绝迟到terminal”在E应改为准确晚C4可以保留历史、但不重新放行；B不因此凭空增加GCS事实。
`multi_owner`可能表示多个输入owner，targeted node可能是定向调度，`wait(num_returns=2)`只是等待两个引用，不能按关键词删。

### 5.3 helper、现行差异与迁移前提

`test_multi_contained_output_path.py`中的`_close_local/_pid_exists`有18个文件导入。
先提取纯hygiene到少量`tests/support/`模块，更新所有消费者；精确close收据、deadline、PID/端口校验保留，业务场景不并入万能fixture。
`_pure_core.py`导入记录B75/E77，当前make/close/mailbox继续保留，仅退出3-output/partial分支。
旧`_pure_node_output.py`有6个、`_pure_reference_output_runtime.py`有4个导入点，先把有效消费者重接本版权威组合，再退休旧helper。
`_pure_output_runtime.py`两版都是当前有效实现，不能按前缀一起删。

11份现行测试/helper内容不同，必须各自保留：B owner-led夹具不被E callback替代；E增加真实GCS检查，不能变成always-success stub。
E新增的8份测试/helper须按合同分类：4份纯测试、2份进程测试与cycle helper真正依赖增强协议，B不补假接口；
另1份`test_enhanced_owner_retirement.py`是公共owner退休合同，按§2.1回迁B。历史“B中不存在”只描述固定快照，不决定新B的计划动作。

每个迁移反例建立明确记录：**旧selector/断言 → 退出需求或保留不变量 → 本版新selector → 真实权威 → 证据层级/结果 → 未覆盖边界**。
相似测试不是完整替代；一个新case可覆盖多条不变量，不展开storage×owner×fault乘积。

以下映射缺口实施时先补最小准确迁移或保留原反例，相应前提未满足不能删除或宣称完成：

1. foreign secondary replica/重建epoch交错：正常双副本GC不等于迟到旧Drop故障。
2. surviving replica KEEP、INLINE已收/未收、adopted owner死亡、Worker-owner死亡：成功知识与bytes来源不同。
3. 同Core两caller真实并发：owner请求reducer并发、同步首ACKrace不等于整个Core并发。
4. 多输入owner、Grant未知、pregrant、transfer-pin ACK丢失：旧helper失效不能抹掉托管合同。
5. 两版GCS知识/图保证不同：B不加空端点，E不以DFS模型替代真实拒环。

这些是具体文件删除的前置条件，不是新添无穷故障门禁；未落实时条目标“未完成迁移”。

## 6. 工具、配置与CI

保留`scripts/run_baseline.py`与`scripts/baseline_manifest.json`路径；baseline表示当前checkout的验收集合，只修First-stage措辞。
`run_bounded_test.py`的PID/env/进程树实现提到私有`_test_process.py`；下述迁移测试入口已可用并验证后，才退出旧CLI及硬编码allowlists。
`run_reviewed_pure.py`和历史manifest从活动工具退出，有用JSON/path/env/timeout负例迁到当前入口；455项旧名册由固定tag保留。

manifest一次收敛到唯一当前schema：pure保留交付gate文件列表，smoke显式exact selector＋marker，另有同文件内的`reviewed_migrations`注册表、edition出处和既有非unit排除。
edition只显示出处，不选择运行时。拒绝未知/重复JSON字段、目录/glob/路径逃逸、冲突marker、任意pytest透传；list不import测试。
**受审可执行集合不等于固定交付gate。**公共命令保留`--pure`/`--smoke EXACT`，新增`--case EXACT`执行已登记的单个纯文件或精确case；同一私有内核提供环境净化、30秒和进程树清理。
`--case`只查本版gate项或`reviewed_migrations`，不接任意路径/glob/pytest参数。纯文件必须明确受审且以`-m unit`执行；混合/并发文件只能登记经过审查的精确case与marker。
迁移项记录selector、marker、所修合同/工作包、review_source_commit、transitive import闭包hash及计划的资源/超时/退出成本；实际测试修改后重新核查这些身份，未核查或hash变化先拒绝执行。
迁移登记不自动进入CI gate；迁移case的通过记录进入其工作包证据。若需晋升gate，明确记录原因、旧新selector差额和成本，移出迁移注册表，不能静默扩大。
例如foreign late replica原本仅在旧bounded清单，K3重写后按实际新文件登记为迁移case并执行，保留错epoch/托管反例，而非退回无进程树保护的裸pytest。
K1必须证明一个单纯文件和一个真实进程迁移case能经新入口执行、未登记项被拒绝、超时/中断完整清理，再移除旧runner。不自动吸收108个extra或455项历史集合。

保留30秒执行界限、PID/PGID正值与归属、PID复用防护、fresh快照、TERM→有界KILL/reap、detached孙进程清理、插件环境净化。
`conftest.py`的import前scope拦截与进程树隔离职责不同，均保留。timeout/强杀不报clean，不让CLI任意放宽预算。

`.gitignore`、LICENSE、已验证Python3.12/构建pin、`uv.lock`保留。无依赖变化不重解锁。
两版package/`__version__`同为`0.1.0.dev0`，文档用分支标明教学定位、用完整SHA/tag固定可复现身份；不为整理自动发明发行版本号或增加动态版本依赖。

CI保留workflow路径与手动运行入口、改为版别中立名称；push ref、PR目标或workflow_dispatch所选ref为`teaching-base`/`teaching-enhanced`时，分别作为B/E候选，main导航提交不默认归入其中一版。B/E独立job checkout候选完整commit、读本分支自己的manifest、保存独立结果。
记录事件类型、事件ref/SHA、PR来源/目标分支（适用时）、候选SHA和实际checkout SHA；PR合并预览通过必须明确其来源，不能冒充分支HEAD实测。
可选对照job注明是另一当前分支的已解析SHA还是历史教学checkpoint；历史tag通过不等于另一当前分支通过。K8前E清理分支尚未创建，不为等它而阻塞B；K9须有两条实际分支各自的验收身份映射。
B无`needs: enhanced`且不读E selectors；每job内纯批次和smoke串行。记录实际Python patch、uv0.11.26、OS、源码/锁/清单身份。
没有实际CI run不得宣称远端验收通过。

## 7. 文档、历史与示例的去向

### 7.1 当前文档沿原路径重写职责

| 文件 | 动作 | 改法与理由 |
|---|---|---|
| README | 重写/缩短 | 两条整理分支各自负责本版安装与B优先/E进阶导航；main只追加旧源码身份与双分支入口，不作为第三版；协议/结果分别链接 |
| `docs/design.md` | 重写，保留知识 | 本版实际Task/资源/对象/引用/恢复/Actor/PG；E增加两协议及W1–W4。旧全文归档，不再另建竞争architecture文件 |
| `docs/learning-path.md` | 重写地图 | B参照Ray Core关键机制、E研究两项mini自定义增量；链接指向对应分支及固定验收提交，不能B教材无说明跳到E源码 |
| `production-ray-mapping.md` | 修链接/差异 | 固定Ray `c3162dce8d064824293875c5d0bbfd76a54e04ce`链接替代本机绝对路径；保留对应/简化/省略的真实含义 |
| `docs/testing.md` | 重写 | 3311行checkpoint堆积改现行运行、层级、界限指南；selector归manifest，结果归账本 |
| `docs/current-status.md` | 重写为短状态页 | 原3391/3392行历史尾部转固定Git索引；只报告本版新源码与实测进度 |
| `docs/redesign-plan.md` | 保留合同，改定位 | 两阶段已交付合同与本次后继整理关系；不再作为清理的第二个backlog |
| `acceptance-baseline.md` | 保留历史事实，追加后继链接/记录 | 不改旧318/32/hash；E内“第二阶段未实施”注明是B冻结时状态 |
| `acceptance-enhanced.md` | E保留，B保持缺席 | 不改旧377/37；新整理结果单独绑定提交，不让B获得增强保证 |

### 7.2 四个过期计划入口

`correction-plan.md`仍待确认并保留多返回/targeted；`handoff.md`仍否认后续实施授权、称owner-led未实现；
`roadmap.md`仍说方案未实施；`acceptance-matrix.md`仍是旧未完成出口。
它们已被固定版本事实替代：**归档原blob → 提取独有有效合同至design/redesign/acceptance → 修好入链 → 删除工作副本**。
不长期保留四个重定向壳形成多条计划链，历史统一进下一节索引。

### 7.3 docs/history的48份文件逐项处理

两版48个raw Git OID相同。43份`.py.txt`旧源码/测试载荷从新工作副本移除，5份家族README的来源/职责映射合并为`docs/history-index.md`。
14份源码副本13,966行已经包含在33,713总行数中，不能相加；文档缩减不算runtime缩减。

每份清单都记录原路径、固定commit、blob OID、raw/checkout SHA、字节数、主题、现行合同目标、前置条件。
实际`git cat-file`恢复原字节已经验证；实施移除前复核索引并按字节导出比较raw SHA。
如果历史反例尚无准确去向，先保留迁移待办，不能靠删除文档掩盖合同缺口。
新索引只保元数据与短说明，不把33k载荷再次复制到新目录或JSON。

### 7.4 示例、artifact与未跟踪研究资料

七个原main均保留。例01的B owner-led与E六GCS阶段trace分别保存，不能规范化成假等价。
例06保whole重建的稳定ObjectID/新attempt；例07保每Node两CPU区别STRICT策略。只适配受改动导入/消息，不新造演示后端。

B的65份artifact和E的126份全部保持原字节，包含失败记录、旧临时路径、安装脚本、精确selector、环境与trace。
它们是历史事实，不“整理成最新结果”；新整理版用新目录，不覆盖旧目录。总量约250KB，不以去重破坏阶段独立性。

三份当前`code-value-audit.md`、`retirement-inventory.md/json`是用户要求的静态证据，保留原审计范围。
834行人读清单可合并为摘要＋机器索引，但它们尚未提交，不能声称旧tag能恢复；先提交归档或做已核SHA的版本化导出。
本轮新`project-cleanup-plan.md`是唯一执行计划，JSON和索引是附表，不是另一条backlog。

## 8. 实施阶段、依赖与交付

下表K0–K9是整理阶段，和增强协议C0–C7不同。JSON来源中的S/P标签只分组，**执行顺序以此表为准**。

| 阶段 | 工作 | 退出条件 |
|---|---|---|
| K0 固定输入/差异分流 | 核对B/E标签、文件/归档；从固定B创建teaching-base并登记引用；逐hunk分流增量 | 原tag/artifact不变；B引用/来源明确；全部源码差异有分类，已确认公共修复和B负例明确 |
| K1 有界验证/公共修复 | 先建立gate与迁移registry的同一有界入口，再回迁公共死亡/退休修复及负例；抽取helper、迁移安全负例后退旧CLI | 公共修复B实际通过且无增强依赖；清单外受审case可有界执行，未登记者拒绝，旧入口可安全退出 |
| K2 明确残留 | B先删19项/import，按9删7改5保留收窄API；同步旧调用/反例 | 编译/export/handler闭合，真实权威不被假fixture替代 |
| K3 历史测试/文档/检查点 | 逐条处理旧测试/文档，迁移项通过registry验证；保存低风险整理检查点 | 旧断言有去向、活动保留测试无坏绑定、文档不冲突；高风险包不阻碍该检查点 |
| K4 正式构造/单输出 | 先去fixture fallback，再纵向标量化ID/wire/执行/owner调用端 | 普通值、含Ref、put、whole重建/GC跑通；child/replica/wait多值不变 |
| K5 本地记录/职责 | 单结果journal、Node progress、owner退休、typed义务、限定三个Core领域 | 不重复拥有事实；RPC不持权威锁；shutdown按领域待办推进 |
| K6 校验/ACK | 规范decoder、局部validated commit、窄ACK；不合并协议阶段 | 畸形、篡改、错绑、晚消息反例保持；不能证明隔离就保留copy |
| K7 B独立验收 | 同一新源码B行为＋公共修复负例＋已纳入结构改动；七main/依赖/清单/日志；绑定实测提交与B HEAD | 公共修复全部落实；结构包已实施或有据保留现状；B分支保留且可运行，无两项中央机制，不等E才交付 |
| K8 E增量适配 | 从已验收B交付HEAD创建teaching-enhanced、记录派生点；只适配真正增强协议/观察/测试增量 | 公共修复从B继承，分叉后同步有提交映射；两协议真实启用，不合并旧大文件/恢复退出能力 |
| K9 E验收/双分支交付 | E共同回归＋新增保证＋W1–W4/R1–R3；保存两版证据与HEAD映射、学习入口；发布时核对远端引用 | 两个refs/heads同时存在并各自绑定通过证据；B继续保留；main导航清楚；旧证据可追溯；必做条目无含糊未决 |

每阶段内部一个可验证生命周期一个小提交，不等最后才第一次集成。文档随实际代码更新，不提前称已精简。
若试做只移动复杂度或增加第二份权威，撤回实现，记录改进收益不足及保留现状决定；不能以拆文件验收。
最后每包须为“实施并验证”或“有据保留现状”，不能把未做试验记成已评估；公共修复、明确残留与删除前提仍须完成。
逐文件总表合并源码条目、公共修复、结构包的全部阶段/验证。`mandatory_item_ids`与`assessment_package_ids`分别记录必做清理与可评估结构修改；
一个结构包决定保留现状，只豁免该包的表示重构，不能豁免同文件的CF公共修复、SRC/IMP残留删除或API调用迁移。

## 9. 验收矩阵与不可降低的边界

| 证据 | B | E |
|---|---|---|
| 实际分支与身份 | teaching-base分支存在；实测提交、最终HEAD及证据映射可核对；不等待E | teaching-enhanced从已验收B派生；自身验收映射完整；交付时B/E两条引用均保留 |
| 基础行为 | 原24纯文件/32smoke逐项映射；治理删除导致数量变化须说明 | 原29/37逐项映射，自己的版本重新执行 |
| 公共正确性修复 | 完整死亡proof、死亡child退休/重建与Node-loss实际接线负例必须通过，不加GCS发布/图 | 继承共同修复并回归；不凭enhanced文件名前缀遗漏B |
| 七main/trace | owner-led普通成功无GCS发布门禁 | 六GCS阶段准确因果链；owner READY/bytes/回复退休分开 |
| 引用/对象 | source到交接、独立borrower、真实bytes/metadata/lineage GC、put无lineage | 同，加Task/put/whole替换图准入/提交/退休 |
| 历史/未知回复 | 完整请求绑定；Complete不等于READY；不重复执行/扣预算 | 同，加GCS准确事实/前进许可分开，C7未知不回滚READY |
| 死亡/清理 | owner/Node/Worker按角色，缺存活成功事实可UNKNOWN | W2真实C4存活可已知成功但无bytes仍LOST；INTENT/ARM不能升级成功 |
| 组合窗口 | owner-led交接、GC/epoch屏障 | W1实际child效果补偿；W3图commit/ownerCAS间隙；W4旧消息不影响新epoch |
| 拒环可达性 | 不提供该保证，不加空端点/虚构基础版成环 | 原E已验证的公共二对象环候选拒绝和并发PREPARED联合四对象场景，在新整理版重新验证，不换成纯DFS |
| 资源/工具/退出 | spawn、启动回滚、CPU-only yield、Actor/PG、PID/端口、forced与clean | 同，加死亡增强待办；前置drain不等尚未由Core GC的活owner图 |

每次经同一有界内核运行已复核gate子项或登记迁移case；迁移后交付集合仍为明确有限选择，不用目录collection探旧树。
迁移未解决只阻止相应删除，不扩为任意故障乘积；不以skip/xfail或假ACK隐藏保留合同失败。
新测试针对实际风险，不为每个新增dataclass写字段镜像测试。

最终每版保存§1.1的分支/派生/实测/HEAD/证据映射，以及依赖/解析、OS/Python/uv、exact选择/marker、命令/界限、原日志、七main输出/trace、退出结果、前后统计。
旧318/377数目不是新版本完成标准。不能将UNKNOWN、已知成功、LOST、READY、CYCLE、预算差异归一化为假等价。

## 10. 规模预期、未决验证和完成定义

590代码行确认残留只占E约1.17%，删完不会从50k变20k。
发布/ownership局部标量化及台账旧人工预算约200–400物理行，互有重叠，不能与590代码行相加。
Core提取、typed义务、窄ACK可能短期增加类型行数，净收益须实际diff/profile；测试/文档缩减不算runtime缩减。
原1.4–2.2万/2.5–3万只是低置信度设计预算，不能改大数字就称体量合理，也不能删必要校验/测试凑数。

校准点：K2直接清理后、K4普通＋含引用全链跑通后、K7基础最终、K9增强最终。
同时观察职责、跨模块私有字段访问、独立台账、同步RPC、完整payload复制和学习路径，不只看文件数。

仍需实施验证：45绑定/75混合文件有效反例逐项迁移、五组非等价覆盖缺口、validated token不跨锁/RPC、canonical值不暴露可变别名、
增强图退出与真实child/bytes清理没有等待环。它们是具体工作包责任，不是重新讨论是否交付增强版。

完成定义：每路径处置明确、删除前提满足、保留合同有本版证据；teaching-base与teaching-enhanced两条实际分支同时保留且分别运行，各自HEAD与验收映射一致；B推荐学习入口、E增量入口及main旧源码说明清楚；原历史可取回。两个tag或两个提交不满足双分支交付。
有用内容保留是正确结果；没有证据的内容不能为了“全部整理”全部删除。

本次实际仅固定版本提取、静态/原Git字节检查与计划生成；没有删除代码/测试、创建清理分支、运行pytest、提交或推送本轮计划。

## 11. R2.3：独立复审后全部 P2 问题的修复计划

### 11.1 本次目标、输入和完成口径

本节是现有清理计划的后续修正，优先于前文历史检查点中的完成表述；不是另一个实现计划。这里的 **P2 是审查严重级别**，与前文名为“P2：Node journal/进度记录”的结构工作包不同。本节编号统一为 `R23-01` 至 `R23-04`。

本节保留R2.3实施合同；当前R23-01至04已完成，准确快照/原日志/处置见本版r2.3-p2证据与当前进度。下文试验/验收要求不改写成旧R2.2已完成的事实。原R2.2有限通过记录继续属于各自固定输入；不再将它解释为全部保留测试已迁移、所有结构子项均已处置或文档完全一致。

| 版别 | 本轮审查/修复输入HEAD | 已保存的R2.2实测提交 | 已有有限结果 |
|---|---|---|---|
| B / teaching-base | `ab4cfb317fd786a286359d8f4e971ff195739375` | `0a340b792c89667e493f7e7313935e45e29071bf` | 343 passed / 1 deselected；32/32 smoke及七main、冻结安装 |
| E / teaching-enhanced | `fd40a407a46ada3d4941c034a73355f45355b270` | `3f5b725fb26390b86c78f085486fb73d897d3e42` | 405 passed / 1 deselected；37/37 smoke及七main、冻结安装 |

E准确历史派生点仍为B已验收交付HEAD `f9a9b35015f114afda9c87e653b6fedcda2eb0b2`。本轮共同测试修复先B再E，记录来源/目标提交或候选hash；不重新派生E、不用旧E整文件覆盖B。

| 编号 / 审查P2 | 适用分支 | 必须达到的目标状态 | 为什么这样修 |
|---|---|---|---|
| R23-01：owner-finalize有效测试残留旧槽假设 | B、E分别修复验证 | 单输出STORED与INLINE各自可达；payload持有、清理义务、实体删除和Worker ACK独立断言；全部原有效反例有去向 | 原测试并非无价值，而是夹具变为单结果后还按两槽理解；删除测试会丢失真实收据合同，恢复双槽违反定位 |
| R23-02：增强P5 GCS ACK/copy未明确处置 | 只E | 一次有限实际候选试验、字段/成本/正确性对照、明确“实施并验证”或“有据保留当前设计” | 此P2是设计评估缺口，不是已证明的协议错误；只有试做/测量才能支持原计划允许的保留，不能补一句理由就销项 |
| R23-03：Ray对应页把B语义挂到E源码 | 只E | 当前E链接解释当前E；B对照固定版本；清楚区分Ray对应机制与mini新增两项保证 | 学习者须能从文字追到真实代码；只替换一句“本版”仍留下错误流程和链接 |
| R23-04：E测试指南仍称未验收候选 | 只E，B仅同步本轮进度 | 已保存R2.2结果、当前改动状态、R2.3新证据分别标识；不把405/37自动移贴给新HEAD | 结果具有版本边界；稳定描述历史验收可避免后续代码前移时再次误导 |

审查来源为工作区 `audit/review-after-cleanup/{review-summary,base-source,enhanced-source,dual-docs}.md`；具体问题和下述目标已完整写入本节，实施不依赖只能在该工作区访问的外部链接。

**范围边界：**本轮不处理其余P3的无caller三输出helper、full_output_ids、surviving_output_locations恒零参数、通用fixture补造、Actor旧别名或raw-string过期注释；保留为明确未完成清单，不称全部历史负担清零。R23-01文件内无效slot参数及“两槽”说明因直接造成P2测试错误，随该项退出。R23-04顺带改 `design.md` 首段“候选”，因为它属于同一当前身份描述。

不增加新API、新故障模型、GCS HA/持久恢复/owner接管，不恢复多返回、targeted等退出能力；不改调度/Actor/PG算法、依赖版本、原tag或旧artifact。main不实施运行时修复；原计划轮未授权提交、推送或CI发布；当前用户已明确要求阶段性提交，本次分别固定B/E本地成果，不推送或触发远端CI。

### 11.2 R23-01：修复两版保留的物理回收测试

**精确范围：**两版 `tests/unit/test_owner_finalize_replica_receipts.py`、各自 `scripts/baseline_manifest.json` 的迁移登记与受影响闭包hash，以及当前进度/新证据。优先只改该测试文件；已有 `test_output_publication_node_server._node` 支持 `stored=False`，无需先改共享工厂、运行时或owner/Node/Worker接口。共享helper只有出现真实签名/缺字段障碍且不能在本文件明确构造时才最小适配，必须列出受影响消费者。

**根因链：**`_case(..., "intent-only")` 调 `_node(refs=False)`，其默认 `stored=True`；`_drop(..., slot_index=0)` 已不读取index，始终指向唯一ObjectID。第265行却断言该值是INLINE。另所有原STORED中断均发生在MATERIALIZE ACK之前；当前journal只有在真实物化ACK后才设置 `record.result`，因此这些窗口的 `result_retained=True` 也不成立。

`result_retained` 只表示journal持有的结果描述/INLINE数据，不是“所有清理已完成”，也不是物理bytes是否存在。STORED可有partial/sealed bytes却没有该结果；INLINE可持有结果而没有Node实体副本。应修正观察点并增强责任断言，不能将所有True机械替换为False。

#### 11.2.1 目标状态与责任表

以下是测试应到达并验证的状态，不通过直接赋值伪造journal事实。所有phase都从实际Node Prepare和现有故障切入建立。

| 场景 / 精确中断点 | 中断后的权威事实 | 正确结果持有/物理状态 | Finalize责任及允许完成点 |
|---|---|---|---|
| STORED intent-only，Seal callback入口前异常 | journal ACTIVE，有MATERIALIZE intent、无ACK、无Complete | result_retained=False；无write claim、无bytes、无sealed metadata | 已安装准确owner死亡fence后，Node以原intent封住未来写入并完成本地清理收据；仍须完整匹配的typed Worker ACK才退休journal（本纯测试为受控外部边界） |
| STORED created / partial | 同上 | result_retained=False；准确write claim，未seal的完整分配/partial bytes | Node按同claim执行abort；失败/未知保claim和待办，不准越过Worker门槛 |
| STORED seal-before-metadata | 同上 | result_retained=False；准确claim和sealed bytes，但未发布sealed metadata | 校验size/hash/pin/claim后delete，再完成manager清理；两部分完成前不能发成功最终收据 |
| STORED corrupt-sealed或错误claim | intent存在，但清理证据不匹配 | result_retained=False；错误bytes/claim仍在 | 拒绝删除和最终确认，不调用Worker、不制造Drop收据；保留当前负例，不放宽校验 |
| 本地删除已发生，manager ACK未知 | 无Complete，lease已ABANDONED并归还资源 | bytes可已无；claim仍保留；dropped metadata不等完整Drop receipt | 精确重放manager步骤，保before/after-effect两种原断言；完整物理收据后才进入Worker确认 |
| 物理清理已完成，Worker ACK丢失 | journal仍ACTIVE；owner-cleanup未finished，ticket已释放 | 对这些STORED中断仍result_retained=False；已存在准确Drop receipt | 原请求重放Worker ACK，禁止再次触碰已完成storage工作；ACK确认后journal RETIRED，不能造Complete或rollback |
| 独立INLINE，Prepare成功但尚未Complete | 实际owner登记与MATERIALIZE ACK；journal ACTIVE，Complete=None | result_retained=True，持有准确INLINE payload；无Node bytes/claim/sealed metadata/Drop receipt | Node按owner fence终止lease；首Worker ACK未知时结果必须仍True且未finished；精确ACK后变False/RETIRED，无实体删除收据 |

这些case中Node未Complete，因此最终lease应为ABANDONED，不伪造执行成功；现有“Complete后死亡仍保Complete”的case在 `test_output_owner_death_node.py` 保留，不能把本表推广到那种窗口。资源释放、物理清理、Worker ACK、journal退休、GCS图退休是不同提交点。

#### 11.2.2 具体改法与保留合同

1. `_drop` 删除无用slot参数，始终从唯一manifest和准确attempt/owner/Node/checksum构造请求；更新本文件全部调用及“两槽”说明。
2. `_case`/`_assert_not_finalized` 对STORED中断明确断言MATERIALIZE intent存在、对应ACK不存在、materialized/result_retained=False、Complete=None；同时按phase检查claim、bytes、metadata、ticket、Worker调用次序。上述物理存在性不可用单个False替代。
3. 将旧复合case拆成独立STORED intent-only和INLINE no-physical-receipt case。INLINE用 `_node(refs=False, stored=False)` 实际完成Prepare，不调用Complete；同case有界丢一次Worker ACK，核真实journal结果True→True→False、lease只释放一次，随后Generic Drop拒绝且仍无该ObjectID的物理收据。完成断言分为公共journal/lease/Worker事实与STORED专属物理收据；INLINE不能调用要求Drop receipt的旧 `_assert_finalized` 分支。未完成断言按场景显式给结果持有预期，不统一True或False。INLINE guard只禁止storage写入/删除、物化callback和manager删除，允许Generic Drop做 `contains` 只读检查；不复用禁止所有读写的STORED重放guard。
4. 保留原created/seal中断、abort/delete返回False/抛异常、manager before/after-effect、Worker ACK未知、removal-effect-then-error、bytes/claim篡改各参数分支。预计从7个函数/14个参数case变为8个函数/15个case，实际数量由精确有界运行日志确认，不为凑数量删分支。
5. 继续复用真实Node/journal/store/ledger方法；Worker回调是现有纯组合的显式测试边界，要校验完整request、endpoint、次数及锁外调用，不能把手工typed ACK称为真实Worker进程清理。另以已登记的真实owner-death进程case验证接线，不另造通用Worker框架或更多故障组合。

**B/E差异：**B没有普通GCS发布/图，测试不能补空端点。E调用现有增强 `_node` 真实authority fixture：STORED各中断已经INTENT/PREPARED，尚无ARMED/Complete；INLINE成功Prepare后有ARMED但仍无Complete。现有Finalize路径会以真实FencePublication取得FENCED；Worker首ACK未知不退休Node，精确ACK后Node RETIRED并返回 `ClosedContainedHolds(exact_reference, (), ())`。authority仍FENCED、graph_active=True、closed_holds=None且无RETIRED receipt；Node返回的闭包不自动写入authority。不能把Node级cleaned写成GCS RETIRED，也不代调用者额外执行RetireGraph来隐藏职责。zero-child闭包只证明空集合，成员死亡/control/全局退休由已有增强controller和实际进程case证明，不为本节点纯case伪造全局成功或新建一套GCS fixture。

**运行范围：**先审本文件导入闭包和资源成本，以迁移项登记 `tests/unit/test_owner_finalize_replica_receipts.py`，`marker=unit`，工作包R23-01；不自动晋升CI gate。原HEAD尚未登记该文件，因此先固定“原测试/源码字节＋新受审registry”的预修候选，准确记录dirty状态和hash；通过现有 `run_baseline.py --case` 执行一次保存首个真实失败，不能将它改标为原clean HEAD实测。再执行修复快照整个文件。预修失败可能早于第265行，按真实日志记原因。两版分别执行，不合并计数。

每版另执行已登记 `tests/unit/test_output_owner_death_node.py`（邻接payload/Complete/Worker语义）和 `tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds`（真实进程）。若共享helper或runtime未改，其余原gate继续归旧实测SHA，以输入相同证明保留结果，不要求为此重跑全树；若共享helper改变，按实际消费者再选有限登记项，不盲目刷新hash。

**退出条件：**15个目标case的实际数量、全部原不变量去向、每版失败/通过、准确输入hash与邻接证据齐全；AST/导入成功不代替行为。若修正夹具后暴露真实runtime错误，本项状态保持未完成，先形成最小真实反例和责任分析；不以弱化断言“修绿”，不扩大成无界运行时改造。

### 11.3 R23-02：完成增强 GCS ACK/copy 的有限评估与处置

**建议路线：保留当前完整Reply作为对照，在隔离候选中只试缩“成功mutation回包”；用完整生命周期证据决定是否采纳。** 本轮目标是补齐原P5承诺的实际评估，尚不能预先承诺窄ACK更好，也不能仅凭“防御copy必要”就免做试验。

当前九类请求是八种mutation：BeginPublication、PrepareGraph、ArmTask、RecordTerminal、CommitGraph、RecordAdoption、FencePublication、RetireGraph，加一种GetPublication查询。put复用图子协议，不新增Task ARM/Complete。

#### 11.3.1 候选边界和不可改变的状态

- 对照版本保留现有PublicationReply；候选仅为成功mutation定义具体类型的StageAck（名称可随实现定，但只能这一种候选）。保留完整原request echo、PublicationRef、准确stage receipt、由权威已接受值生成的必要fact及当前forward信息。先去完整snapshot冗余，不同时压缩request echo或改历史绑定规则。
- GetPublication保持可表达ABSENT的完整detached snapshot；失败reply暂保原request、typed error和已有可选snapshot。查询仍承担unknown恢复、Node-loss成功知识、GC/death闭包历史，不能把恢复所需内容偷移到另一个Core缓存。
- 候选只改变传输/校验表示：现有单GCS记录和graph判环算法、锁顺序、receipt sequence、提交先后、fence/retirement语义不变。StageAck是从当前权威状态派生的返回值，不加第二份可变phase/forward表。
- 首次transition仍先构造并校验可返回的值，再提交 `_records` 和sequence；不能把原先提交前的fallible构造挪到提交后，造成报错却已提交的新窗口。输入边界和外部返回仍有深校验/隔离，禁止把frozen dataclass当不可篡改证明。
- 不改B、不合并C0+C1/C4+C5、不引入通用schema/插件/后端、不长期保留两种运行模式。对照和候选只存在于独立固定快照，最终正式E选一种。

#### 11.3.2 必要信息与消费者表

下表为候选必须保留的语义，不将尚未验证的字段布局冒充正确实现。`accepted_fact` 必须按具体请求/阶段有明确类型和exact校验，不能使用任意dict。身份、request与receipt引用/阶段一致是每一行的共同要求。

| 请求 | 成功ACK必须表达的事实 | 当前主要消费者 / 不能省掉的区别 |
|---|---|---|
| BeginPublication | 准确INTENT及对应publication/route绑定；当前是否仍可前进 | Node `_record_gcs`、owner client begin；有INTENT不等成功执行，不准据旧ACK重新准入 |
| PrepareGraph | 准确PREPARED及本次引用/阶段；当前forward信息 | Node graph gate、client begin；CYCLE走失败reply，不能伪造PREPARED或释放已有child |
| ArmTask | ARMED以及权威接受的完整TaskPreparedReceipt | journal必须比较真实child/materialization preparation，不能只看stage或拿期望manifest合成证明 |
| RecordTerminal | TERMINAL及准确Complete witness | Node terminal gate直接消费complete；允许有合法晚成功历史而forward已关闭，不能把accepted与forward绑成一个bool |
| CommitGraph | COMMITTED与当前forward信息；put须绑定真实PutPreparedReceipt | owner Task/put准入；已经commit的精确重放可无forward，RPC后还需本地epoch/abort检查 |
| RecordAdoption | ADOPTED及准确owner adoption proof | owner/client、Node退休；C7未知不能回滚C6 READY，不能把该收据解释为bytes仍在 |
| FencePublication | 已安装的准确fence与FENCED receipt，而非请求必然成为新决策 | controller有“后到owner death排清理但返回已有fence”的真实分支；ACK必须区分请求echo与实际accepted fence，不能把旧fence说成本次death首次提交 |
| RetireGraph | RETIRED及实际接受的ClosedContainedHolds/首次历史绑定 | client/death driver；图退休不代替实体bytes/child效果；精确重放不接受换proof顺序的新请求 |
| GetPublication（不窄化） | ABSENT或完整准确detached history | Core Node-loss读取complete、controller读取publication/closed_holds/receipts；INTENT/ARM不证明成功，metadata不能恢复bytes |

同时适配 `control.py` trace取publication、`enhanced_publication_control.py` mutation后owner死亡检查、Node result gate读取Complete、journal ARM核prepared，以及实际测试桥。PrepareGraph和Task CommitGraph请求只有reference，不能从中推导owner；由ACK携带已验证owner identity，或在原锁内取detached标量trace上下文。不得为trace新增网络query或另造成功表；其它消费者若必须追加查询，该成本和新的未知回复责任必须完整纳入比较。

**封闭修改范围：**E的 `enhanced_publication.py`、`enhanced_publication_client.py`、`enhanced_publication_control.py`、`output_publication_node.py`、`output_publication_journal.py`、`control.py`、`node.py`，及实际直接消费者中 `core.py`、`publication_gate.py` 的必要适配。现有精确测试/helper随调用变动适配；`output_protocol.py` 的B公共窄Complete ACK不因本试验重写。开始前列明全部成功/失败/query消费者，新增路径必须给出调用证据，不能全Core重构。

#### 11.3.3 试验、计量与采纳判据

1. **固定对照。**以已完成R23-01的E候选为基线，固定源码、测试、manifest、Python/uv/lock和换行口径；原完整Reply就是对照，不拿更早多槽原型的pickle结果作收益。
2. **只搭一个可运行候选。**覆盖普通Task零child和含现有owned/borrowed child的Task完整发布/owner接管/退休，再覆盖现有含Ref put与whole替换入口。不为每个child/storage/fault排列制造case；必须通过真实authority/Node/client消费，不能只对独立DTO做pickle长度实验便称端到端改善。
3. **按同样轨迹测成本。**分别列request与reply的实际序列化bytes、完整publication/prepared/Complete/closed-hold的重复携带和重建次数、正常与未知ACK恢复的RPC/query数量、所加/所删类型/字段/独立状态/校验分支、源码diff/阅读路径。使用同一序列化方式、相同输入和固定重复次数；先记录测量口径，保raw数据。若测耗时，固定预热/重复次数并报告分布及环境，仅作辅助；不设置拍脑袋加速百分比或LOC硬上限。
4. **比较语义而非假等价。**必须逐项比较exact接受/拒绝、receipt/历史、当前forward、owner READY、bytes、child hold、资源、finish/GC屏障；成功reply形状/bytes改变是预期差异，B UNKNOWN与E存活TERMINAL知识差异也继续保留。不得归一成一个success结果。
5. **采纳必须同时成立：**全部受影响合同通过；权威/同步点不增加；测得完整history在普通成功回包中减少，且未被额外query、同等大的fact重装、反复copy或明显更复杂的消费/错误分支抵消。不能只凭回包变小或拆出类型数量评价。
6. **否则有据保留当前设计。**保留实际候选diff、运行结果、成本对照和逐项理由；明确哪种必要fact/隔离/恢复消费抵消收益。普通实现错误/坏fixture应先修到可比较，未跑通纵向路径的半成品不能凭“复杂”销项。若有限真实试做已经以可复现反例证明本候选破坏必要隔离/历史语义，可作为失败试验证据撤回并保留原设计，无需为了完成而采用不安全实现；必须明确未完成的候选验证，不称候选通过或获得净收益。归档试验但正式E不保开关或死接口；只有静态猜测、没有有效对照/实际反例时保持未完成。一次候选完成后择一，不连续扩展通用框架寻找更好数字。

#### 11.3.4 有限验证范围及证据边界

首先执行现有四个增强纯文件：`test_enhanced_publication.py`、`test_enhanced_node_publication.py`、`test_enhanced_publication_control.py`、`test_enhanced_owner_client.py`，并按实际签名变化执行已登记的Node/journal/trace接线集合。若需要针对新ACK字段增加负例，只覆盖新边界的deep-tamper、request/stage/fact错绑、mutation回复篡改不反向污染权威、旧receipt不授权新forward、fence特例；每个新case必须对应上述新增风险。

已有实际组合继续承担W1–W4：图预留后child失败/补偿，C3完成而C4未知及publisher死亡，C5与C6间隙，READY后C7/重建/GC/晚消息；已有公共二对象环与并发联合四对象预留、Task/put/whole边入口必须按E实际路径验证。测试桥若以前从mutation snapshot读取一般历史，可用实际query观察并注明test-only查询；依赖本次mutation接收时点的ARM、terminal gate、fence等断言必须核新typed accepted_fact，不能以事后query替代时点或在测试里伪造snapshot让协议“通过”。

候选若进入正式E，固定同一新执行输入，运行当前有限pure gate与37个smoke（包含七main、三个真实图场景和两个terminal-loss场景），再执行受影响但不在gate的登记case，包括R23-01。计数以实际日志为准；不承诺仍为405。依赖pin不变，不重解锁；采纳StageAck会改变包内源码/导入，需在新固定输入做一次锁定安装、依赖一致性和项目外导入核查，旧安装不冒充新SHA。若只有测试/文档变化，则记录包源码、构建配置、lock和依赖未变，旧安装仍只认证其旧输入，不为无变化重复安装。若最终保留原运行时，试验结果归候选，不给正式E贴候选的通过标签，也不因试验要求B重验。

**仍待实施证明：**ARM/晚terminal/fence特例在窄ACK下的fact校验是否足够、全部直接消费者是否无需新增同步query、返回值构造失败是否仍在提交前、深层别名隔离是否保持、收益是否抵消类型/分支成本。它们正是本项试验要解决的问题；此计划不冒充已经证明。

### 11.4 R23-03/R23-04：E教材、测试指南与身份说明修复

**精确范围：**E `docs/production-ray-mapping.md`、`docs/testing.md`、`docs/design.md` 首段；按实际实施状态更新两版 `docs/current-status.md`、`docs/cleanup-progress.md` 和本计划状态。README只有相关链接/身份不一致时最小同步，不扩大为文案重写。B Ray映射的基础机制正确，不机械复制E内容覆盖它。

#### 11.4.1 Ray对应页目标

整页定位改为“当前E的共同Ray Core机制，以及两项mini自定义增量”。保留固定Ray对照commit `c3162dce8d064824293875c5d0bbfd76a54e04ce`，并明确职责类似不等于协议一致。

- 共同API/调度/bytes/owner/Actor/PG映射可继续链接当前E源码；涉及普通发布必须写当前E的GCS事实与图门禁，不能再称“本版去掉门禁”。
- 普通结果段说明C0/C1/ARM、Node Complete、GCS terminal、graph commit、owner READY、adopted/退休分别由谁提交；不把此顺序说成Ray原样实现。
- 更新Node-loss/成功知识/图边段：存活GCS准确TERMINAL可保成功知识；仅INTENT/ARM仍不够；bytes全失仍LOST。Task/put/whole入口与已保存真实拒环证据一致；不再把已验收场景写成尚未证明，也不把所有Python容器环都称ObjectID环。
- 保留B对照时，源码链接固定B实测 `0a340b7`；B教材和已归档验收可链接文档交付 `ab4cfb3`，注明该HEAD的源码与实测相同且后续只加文档/证据。不能用E相对链接伪装B代码。当前E状态与验收入口指向E自己的页面/记录。
- 收益写为额外存活发布事实来源与全局拒环，成本写为同步GCS、补偿/fencing/闭包收据/死亡清理；不得写“更接近生产Ray”。

#### 11.4.2 测试指南目标与版本前移规则

`testing.md:5` 改为“R2.2已保存验收”：正式E实测 `3f5b725` 的405 passed/1 deselected、37/37及安装来自 `artifacts/cleanup-enhanced/final/acceptance.json`。`enhanced-trial-01` 的392/37只作为明确历史候选或通过进度页链接，不占“当前待完成”位置。第42行用“本分支有限gate：30个pure文件/37个smoke，执行条目以当前manifest为准”，区分文件/selector数与pytest case数。

R23-01新增文件先作为迁移项，不静默扩大gate。R23-02若使测试数量变化，按实际日志更新新验收而不是沿用405；指南无需重复一个会随源码变化的“当前已全部通过”断言。`design.md:3` 改“本页描述teaching-enhanced运行时”，删除候选身份。

实施期间状态页固定区分：上一次已验收源码、当前R2.3待办/候选、已实际完成的新检查。旧 `final/acceptance.json`、runtime/install/log与阶段产物全部保持原字节；新结果进入各分支新的 `artifacts/cleanup-{base|enhanced}/r2.3-p2/`。若R23-02仅有据保留，记录正式运行时未变和候选试验身份，不虚构一份新运行时实测。

文档验收是逐段对照当前源码/固定B链接、当前manifest与准确acceptance，并检查相对链接和必要锚点。只修文档不新增runtime测试；本机未推送时如实标注固定远端链接尚未在线核验，不能为文档检查擅自发布。

### 11.5 实施顺序、证据与退出条件

| 顺序 | 工作 | 进入下一步的具体证据 |
|---|---|---|
| 0 | 核输入/范围、准备本轮处置表 | 两条HEAD、工作树、原tag/artifact身份；四项P2逐一登记，P3单独未处理 |
| 1 | R23-01先B修复，之后E适配 | 本文件原失败→修后完整参数集合、邻接pure与真实owner-death通过；B→E映射和分支差异明确 |
| 2 | 并行修R23-03/04文档 | E真实机制/B固定对照不混用，旧验收与当前候选分开；不依赖ACK优化是否采纳 |
| 3 | R23-02在步骤1的E上做单候选试验 | 九类消费者表、实际纵向路径、字段/bytes/copy/RPC/结构对照、准确故障合同证据 |
| 4 | 选择采纳或有据保留，完成所需受影响验收 | 采纳则正式E新输入有限gate/迁移通过；保留则正式运行时未变、试验成本及退出理由可查；B不等待此选择才记录自身修复结果 |
| 5 | 独立复审四P2与交付映射 | 四项逐项closed，后续HEAD只加文档/证据时有diff证明；剩余P3明确，不宣称“所有代码都必要” |

每项新记录共同包含问题、目标状态/不变量、改动路径/候选hash、B/E适用性、证据层级、未覆盖边界、最终处置。R23-01另列原/新selector、exact命令/marker/界限、环境及实际预修失败/修后日志；R23-02另列九类消费者、候选diff、成本原始数据、合同运行及实际失败（若有），不要求完整Reply基线必须失败；文档项列错误段落、前后差异、语义/链接静态核对，不制造pytest或失败日志。审查报告是依据，不作为第二套backlog；机器附录 `p2_remediation` 与本节使用相同四项编号。

继续只用既有有界runner及经审查登记的精确选择；不裸跑目录pytest/collection、不提高5秒启动/30秒测试界限、不用skip/xfail、不刷掉失败、也不为“全P2”把所有338项都设成新门槛。仅当新变化、真实失败或具体未决责任影响范围扩大时，才追加相应有限验证并说明原因。

最终目标是**四项审查P2全部有实际修复或符合原P5规则的实验证据处置**，两版职责/历史仍清楚。该目标不包含本轮未处理P3，不保证达到旧20k预算，不以代码行减少代替合同正确性或教学清晰度。

R2.3执行结果：B修复目标15＋邻接10＋实际owner死亡1通过；E共同修复同样15/10/1通过。增强StageAck单候选经真实四轨迹×3对照及mixed固定seed校准、源码/净收益独审后采纳，正式有限gate405/1deselected＋37/37smoke全部首次通过，受影响登记项全部通过并完成新源码锁定安装。另修E journal旧B夹具，原23失败在完整Reply与StageAck两边复现，同一修复23例分别通过，纳入最后迁移证据。测量、失败、不同快照及gate闭包映射完整保留；没有提交或推送，本轮不处理P3。详见本版artifacts/cleanup-*/r2.3-p2和当前状态页。
