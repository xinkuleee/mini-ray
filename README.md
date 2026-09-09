# mini-ray

mini-ray 是一个 Python-only 的 Ray Core 教学项目。它与 Ray 的关系，类似
mini-SGLang 与 SGLang、nano-vLLM 与 vLLM：保留可沿源码追踪的关键机制，缩小运行规模和行为范围。
读者可以从一次 `.remote()` 追到 Worker lease、直接提交、对象传输、引用保活和有限故障恢复。

**第一阶段基础版已独立通过约定验收：318项纯合同通过、32个真实smoke通过，七个原main产物已保存。**
基础版固定为`teaching-base-v0.1`（`69106772567a4131f5ec76e898a3c4bf3bb6dbe6`）。**增强版`teaching-enhanced-v0.2`也已通过约定验收：377项纯合同、37个真实smoke及七个原main。两阶段按顺序独立完成。**
范围见[两阶段计划](docs/redesign-plan.md)，结果分别见[基础账本](docs/acceptance-baseline.md)、[增强账本](docs/acceptance-enhanced.md)。[基础版证据包](artifacts/stage1-baseline/)保持固定。

首次学习推荐固定基础版。在干净工作树检出：

```bash
git switch --detach teaching-base-v0.1
uv sync --frozen --extra test --python 3.12
```

增强版成为最新版本后，这个标记仍用于学习本README所述的owner-led路径；不要用增强版实时trace替换基础版历史证据。

## 两个教学版本

| 版本 | 教学目的 | 交付顺序 |
|---|---|---|
| 第一阶段：基础教学版 | 理解 Ray Core 的执行、调度、对象、引用和恢复机制；普通结果由 owner、Node 与 child owner 分工交接 | 独立验收，固定源码版本、依赖、示例和测试证据，作为推荐的首次学习入口 |
| 第二阶段：协议增强版 | 在基础版上研究 mini 自定义的 GCS 普通结果发布事务与全局 ObjectID 引用图防环 | 两项均已实现并按有限合同验收；标记`teaching-enhanced-v0.2`保留保证与组合窗口证据 |

增强版增加中央发布事实和全局成环拒绝，也增加同步依赖、故障补偿与收据管理成本。
它不代表“更接近生产 Ray”，基础版保留所选机制必需的正确性逻辑。
不因进入第二阶段而恢复多返回槽、targeted reconstruction 或其他已退出能力，也不长期维护两套运行时后端。
增强版成为最新版本后，固定基础版仍保留清晰的学习入口。

## 两版共同范围

| 机制 | 保留的行为与边界 |
|---|---|
| 运行规模 | 单 job、单 GCS、单机 loopback、1–2 个逻辑 Node，每 Node 1–2 个普通 Worker；真实 spawn 进程 |
| 普通 Task | 一次调用只返回一个 ObjectRef，num_returns=1；tuple/list 是这个对象的完整值，不拆槽 |
| 调度 | Node 本地资源账本、dependency gate、Worker lease、spillback、Core 到 Worker 的直接 PushTask |
| 对象与传输 | INLINE 或不可变 sealed bytes；显式 put；owner 位置元数据；Node 间 pin、分块 pull 与副本回收 |
| 引用 | 本地引用、Task/lineage hold、独立 borrower、容器内 ObjectRef；完整 typed 身份、精确 ACK 和释放收据 |
| 普通结果交接 | owner 登记完整清单，Node 保留物化与 child handoff 责任；Node Complete 与 owner READY 分开 |
| 恢复 | 稳定 TaskID/ObjectID、变化的 AttemptID、预算内系统重试与单输出 whole reconstruction；旧消息 fencing |
| 嵌套执行 | Worker 可提交子任务；阻塞 get 临时让出 CPU，其他资源仍受原 lease 约束 |
| Actor | 独立 Worker 上串行调用、同一存活 Node 内按预算重启、generation fencing；Node 丢失进入终态，不迁移 |
| Placement Group | 至多两个 bundle，仅 STRICT_PACK / STRICT_SPREAD；prepare/commit/abort 与完整提交后可见 |
| 观察 | 因果 trace 与有限故障实验；trace 用于解释执行，不决定发布或死亡事实 |

Task 参数只有 InlineArg 和显式对象依赖 RefArg。累计按值参数超过 inline 预算会要求使用
`ray.put(value)`，不自动提升为 StoredArg。显式 put 支持普通值及含已拥有/借入引用的容器，
保留 source 到真实交接或补偿完成；put 不产生 Worker lease，也不伪造可重执行的 Task lineage。
将其 ObjectRef 作为顶层参数会读取对象值；放在普通容器中传递则保留为 handle，依赖等待与引用保活具有不同职责。

引用协议只接受完整 typed hold/source；字符串 borrower_token 或 transfer_token 是这些身份中的字段，
不能单独代替 container、owner 和原始借用来源。Actor 的引用参数扩展、历史 pickle alias 和裸 token 兼容已退出。

基础版没有 GCS 普通结果发布事务，也没有全局 ObjectID 引用图防环保证。
普通 Python 容器自身的环与多个 ObjectID 之间的引用环是不同问题；引用计数不承诺自动回收全局强引用环。
项目不提供生产部署、GCS 持久恢复/HA、已死 owner 接管或任意网络分区恢复。
受管进程 fail-stop 与有限回复丢失是本版故障范围；外部副作用不具有 exactly-once 保证。

## 基础版：从一次任务理解职责

1. Core 为 Task 和唯一输出分配稳定身份，登记依赖与引用生命期，再向 Node 申请 Worker lease。
2. Node 根据本地资源与依赖决定 grant 或 spillback。Core 取得 grant 后直接向目标 Worker 提交任务。
3. Worker 在 Start ACK 后执行并序列化一次。Node 先向结果 owner 登记完整清单，再准备 child holds、物化结果并完成引用交接。
4. Node 记录精确 Complete，并收敛本地 lease/资源释放；向 owner 报告 Complete 的回复丢失不会把执行变回未完成。
5. owner 校验结果与当前 attempt，通过本地 CAS 使结果 READY。实际 adoption ACK 允许退休 Node/Worker 的回复托管；最后引用释放另行驱动物理和元数据 GC。

执行成功、结果可见、bytes 可用、回复托管退休、对象 GC 是五件不同的事。
成功元数据无法恢复已丢失的 bytes，超时也不能证明远端没有发生效果；未知效果必须保留精确清理责任。
在基础标记中，GCS负责成员/死亡事实、Actor与PG控制，普通结果不逐项经过中央事务。以下源码表链接当前工作树；阅读上述基础路径须先检出基础标记。

| 阅读位置 | 关注的职责 |
|---|---|
| [api.py](src/miniray/api.py)、[core.py](src/miniray/core.py) | 用户入口、提交与 owner 侧组合；Core 仍在继续收敛职责和规模 |
| [ids.py](src/miniray/ids.py)、[protocol.py](src/miniray/protocol.py)、[task_outputs.py](src/miniray/task_outputs.py) | 逻辑/物理身份、消息与单输出合同 |
| [node.py](src/miniray/node.py)、[resources.py](src/miniray/resources.py)、[worker.py](src/miniray/worker.py) | lease、资源与执行边界 |
| [output_handoff.py](src/miniray/output_handoff.py)、[output_publication_node.py](src/miniray/output_publication_node.py)、[output_publication_journal.py](src/miniray/output_publication_journal.py) | owner 清单、Node 效果与精确收据 |
| [ownership.py](src/miniray/ownership.py)、[dependency.py](src/miniray/dependency.py)、[put_handoff.py](src/miniray/put_handoff.py) | 逻辑引用、nested 参数和显式 put 交接 |
| [object_store.py](src/miniray/object_store.py)、[object_manager.py](src/miniray/object_manager.py) | 字节、副本与跨 Node 传输 |
| [reconstruction_runtime.py](src/miniray/reconstruction_runtime.py)、[actor_worker.py](src/miniray/actor_worker.py)、[placement_group_runtime.py](src/miniray/placement_group_runtime.py) | whole replay、串行 Actor 和硬约束 PG |

## 增强版：在同一职责边界增加两项保证

当前主线的普通Task成功路径必须经过GCS INTENT、图PREPARED、ARM、准确terminal、图COMMITTED和adopted。
Node Complete仍在本地一次释放资源；owner在图提交后唯一执行READY/outgoing/recovery CAS；准确C7 ACK后才允许Node回复托管退休。
put使用同一图协议的独立put身份和真实child/Seal收据，不生成Task Complete或adoption。重建先收口旧epoch，再预留新图边。

新增源码入口是[enhanced_publication.py](src/miniray/enhanced_publication.py)的metadata/图authority、
[enhanced_publication_control.py](src/miniray/enhanced_publication_control.py)的成员和死亡清理组合、
[enhanced_publication_client.py](src/miniray/enhanced_publication_client.py)的owner同步调用；它们不接管owner、bytes或资源账本。

增强版增加存活的成功事实来源和全局成环拒绝；普通成功也因此增加同步GCS依赖、未知回复等待、精确补偿及收据退休。
INTENT/ARM不能证明执行成功；GCS知道成功但bytes全失仍为LOST，不能凭metadata返回值。没有HA、持久恢复或owner接管。

真实公共API成环候选已经到达生产图入口：Worker先保留`B = put([A])`，重建A时返回B，产生A→B→A候选而被C1拒绝；
两Node的四对象并发实验验证PREPARED预约也参与联合判环。它们没有修改已序列化Python容器、手造Ref或调用DFS，详见增强账本。
W2准确terminal知识差异、所选owner死亡切片、全部37个smoke和29文件纯合同已在增强snapshot03同版通过。研究增量时检出`teaching-enhanced-v0.2`；[增强证据包](artifacts/stage2-enhanced/)保存源码hash、结果、环境和七个main产物。

## 七个示例的阅读顺序

七个入口继续使用原示例main；固定基础结果查基础账本，当前增强结果及待办查增强账本。两版trace保留真实语义差异。

| 顺序 | 原示例 | 要回答的问题 |
|---|---|---|
| 1 | [Task 与 ObjectRef](examples/01_task_path.py) | 一次 remote 如何经过 lease、direct Push、owner 交接到 READY？ |
| 2 | [Spillback 与直接提交](examples/02_spillback_direct_submission.py) | 谁选择节点，谁分配资源，任务最终发给谁？ |
| 3 | [跨 Node 对象 pull](examples/03_cross_node_object_pull.py) | ObjectID、owner、位置与实际 bytes 怎样分离？ |
| 4 | [Actor 控制与直达调用](examples/04_actor_control_direct.py) | 创建与方法调用分别经过哪条路径，串行状态由谁持有？ |
| 5 | [嵌套 get 与 CPU yield](examples/05_nested_get_cpu_yield.py) | 父任务等待子任务时，CPU 如何交还并重新取得？ |
| 6 | [Lineage 重建](examples/06_lineage_reconstruction.py) | bytes 丢失后如何保持 ObjectID，并用新的 attempt 重执行？ |
| 7 | [Placement Group](examples/07_placement_group.py) | bundle 如何原子提交、绑定任务并显式释放？ |

例 7 使用每 Node 两个 CPU、一个普通 Worker；两个各需一个 CPU 的 bundle 本可由 STRICT_PACK
放在同一 Node，STRICT_SPREAD 则要求分开。示例观察真实提交与执行，两种约束及失败回滚的有限证据见账本。

## 安装与有界验证

完整进程验收使用 **Linux / WSL 的 Linux 环境**，以 Python 3.12 为基线。
原生 Windows 当前只承担已审查的纯合同与小型无后台组合验证，不能据此声称多进程运行时已在 Windows 验收。
包元数据限定Python3.12，依赖解析与安装证据已随版本固定，其他Python版本不在当前验证范围。

在 Linux 仓库根目录，使用 `uv 0.11.26` 和已安装的 Python 3.12，按仓库锁文件安装：

```bash
uv sync --frozen --extra test --python 3.12
source .venv/bin/activate
python scripts/run_baseline.py --list
```

锁文件安装、实际执行环境与结果见各版账本；远端CI配置已提供，本次没有推送或远端CI运行声明。
同一runner在各检出版本读取各自[显式清单](scripts/baseline_manifest.json)。基础学习须留在固定标记，协议研究再检出`teaching-enhanced-v0.2`。以下先运行固定纯合同批次，再运行原示例 1；
其余 smoke 使用清单中的完整 selector，每次运行一个：

```bash
python scripts/run_baseline.py --pure
python scripts/run_baseline.py --smoke 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

基础 runner 复用已有的 30 秒 POSIX 进程树边界，超时是失败，不是取消成功或 clean shutdown。
`ObjectRef.close(timeout=...)` 等待本地释放收据，远端释放及 GC 是否完成需独立观察。
不要用裸 pytest 或整个目录采集代替验收账本的有限集合；历史测试中仍有已退出能力的旧合同。
`--list` 只枚举当前显式候选，不代表已验收；旧 bounded/reviewed-pure 清单保留历史用途。

当前单输出纯合同可在安装好的环境中精确运行。例如 PowerShell：

```powershell
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = '1'
python -m pytest -m unit tests/unit/test_single_output_contract.py -q -p no:cacheprovider
```

基础版已在snapshot03同一固定源码复验约定纯合同、32个真实切片与七个示例；结果、平台、依赖及退出证据随版本保存。
测试输入SHA256为`42fa8b6406ae5672b434b1b479aa08ca3c36faab131429ea287970bf135cd5d0`；该结果仅属于基础标记；当前增强主线的源码、测试与runner已经继续演进。
固定基础版校准为 **48,555 代码行 / 59,386 物理行**，较原始 55,196 代码行减少 **12.03%**。
这是行为范围收缩后的实施中测量，仍明显超过 1.4–2.2 万行低置信度设计预算，不能宣称已达到紧凑教学代码目标；
成本归属、职责审查及后续估算见两阶段计划 §8。必要校验、清理与测试不因行数预算而删除。
增强版实测**50,277代码行 / 61,440物理行**，比基础版增加1,722代码行（3.55%）。**紧凑代码量目标尚未达成**：本次完成范围与协议纠偏，Core/Node/wire集中和整体阅读成本仍是保留债务；不把有限测试通过说成全历史测试通过或规模已合理。旧K0/K1历史不覆盖两阶段计划和各版账本。

## License

Apache License 2.0，见 [LICENSE](LICENSE)。
