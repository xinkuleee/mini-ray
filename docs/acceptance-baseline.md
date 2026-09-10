# 第一阶段验收账本

本页的“第二阶段尚未实施”及318/32等结果是固定`teaching-base-v0.1`冻结时的历史事实。后续原增强版已交付；本次`teaching-base`整理分支的分批结果见[执行记录](cleanup-progress.md)，最终K7验收尚未完成，不能用本页旧结果认证当前HEAD。

日期：2026-09-09。实施来源：[两阶段计划](redesign-plan.md)。**第一阶段基础版已独立通过约定验收；本地固定提交/标记为`teaching-base-v0.1`，第二阶段尚未实施。**
本页记录保留行为、旧证据迁移和有限执行集合；不是第二份实施计划。GCS普通结果发布事务、全局图与二者组合只属于第二阶段门禁。

证据分为纯模型、真实authority加显式队列/typed callback的小型组合、有界真实进程。callback返回成功不代替真实RPC，
强制终止不代替clean shutdown，close收据不代替对象GC。不同修改点通过的结果不相加，不认证后来修改的版本。

## 固定基础版证据

最终snapshot03在同一固定源码上执行24个纯合同文件、32个smoke精确选择器，**318 passed / 1 deselected，32个smoke全部通过**。
七个原始main均实际运行，stdout与规范化trace保存于证据包。B04/B06的两个有限证据缺口已纳入本次全量基础清单复验。
不将中间候选的结果累加，也不以此认证整个历史tests目录或第二阶段保证。

| 记录 | 固定事实 | 证明边界 |
|---|---|---|
| 原始分析版本 | `ef16ebc26a2621e9730a8fc6a85cab4cbcabd01e` | 改造前历史，不是基础版交付 |
| 最终测试源码 | snapshot03归档SHA256 `42fa8b6406ae5672b434b1b479aa08ca3c36faab131429ea287970bf135cd5d0`；[文件hash](../artifacts/stage1-baseline/snapshot.json) | 所有执行绑定这份源码输入；证据汇总文档和artifact归档在测试后写入，未改测试源码 |
| Linux纯批次 | **318 passed / 1 deselected，pytest 6.95s**；[原始日志](../artifacts/stage1-baseline/00-pure.txt) | 唯一deselection是owner reconstruction真实线程case，不计作纯证据 |
| Linux真实切片 | **32个exact全部通过**；[结果与日志索引](../artifacts/stage1-baseline/results.json) | 逐个有界串行运行，包含七个main、ACK-loss与Task nested replay；不外推未选故障组合 |
| 示例产物 | [七个原main输出与canonical trace](../artifacts/stage1-baseline/example-output/) | 各txt保存真实main输出及规范化trace，不是独立演示后端 |
| 执行环境 | [environment.json](../artifacts/stage1-baseline/environment.json) | Linux/WSL、CPython3.12.13 musl及精确依赖版本；不是Windows完整运行时认证 |
| 锁定安装 | [Linux](../artifacts/stage1-baseline/dependency-repro/linux-results.json)、[Windows](../artifacts/stage1-baseline/dependency-repro/results.json)均lock check、frozen editable install、import成功 | 相同pyproject/uv.lock hash；安装不等于运行时测试，远端CI未执行 |
| 学习版本 | `teaching-base-v0.1` | 本地提交/标记固定；通过该标记独立检出学习，增强版另沿主线实施 |

历史过程记录：第一次固定候选为312/1与30smoke；snapshot02为318/1及两个新增smoke窄验证。
这些记录解释迁移过程，最终交付依据仅为上表snapshot03的同版结果。

## 行为与迁移去向

下表保持九组。已保留断言迁到实际责任方，退役协议不因旧测试存在就恢复；约定保留合同的已知缺口已关闭，以下限定每项证据能证明的范围。

| 编号 / 保留行为 | 历史断言处置 | 当前主要证据 | 验收边界 / 不可外推 |
|---|---|---|---|
| B01 执行边界与生命周期 | runner环境净化、超时清理、真实资源/端点退出保留；旧名字和计数锁不作完成指标 | 三个旧runner合同及新baseline runner；原task path；startup rollback、双Worker实际重叠；新增runner真实超时清理exact | snapshot03逐项通过；Linux父进程和detached grandchild超时切片不等于所有信号/平台都已验证 |
| B02 单输出API与七条主线 | 多返回、targeted/sibling专属断言退出；tuple/list是单值，wait数量不变 | `test_single_output_contract.py`、`test_explicit_put_arguments.py`；七个原main的exact | 七个main在snapshot03通过；入口拒绝的无副作用断言保留。example07当前每Node两CPU、一Worker，可区分两种硬约束 |
| B03 调度、依赖与Store | lease身份、pending gate、immutable Store、pin/pull、Node最终资源账本保留；自动大参数lift退出 | example02/03；cross-node dependency、locality；`test_placement_child_ledgers.py` | snapshot03证明所选真实数据移动/首跳差异；不把policy建议当最终资源分配。物理GC归B05 |
| B04 owner-led交接与结果知识 | GCS普通事务/全局DAG门禁退役；Complete一次释放、已知成功不回滚、未知效果责任保留 | `test_output_handoff.py`、`test_owner_led_publication.py`、`test_node_lost_output_resolution.py`；Worker-after-Complete；publisher Node-loss的UNKNOWN与准确Complete→LOST两个新切片 | **snapshot03复验通过，有限ACK-loss证据缺口关闭**：`test_output_retirement_ack_path.py`丢一次真实Node退休回复，验证READY、精确重放、finish/GC屏障、无重新执行与最终GC；不外推多次丢包/进程死亡组合 |
| B05 nested refs、独立borrower、真实回收 | child owner holds、Release tombstone、borrower独立生命周期与无环GC保留；图保证退出 | 两borrower活过inline outer；stored physical GC观察owner metadata/lineage与source/target bytes；含Ref put观察child Release | stored physical GC已有sender先close、accepted Task保活切片，不新增重复门槛。两borrower和put实验本身不独立证明foreign child metadata最终GC |
| B06 显式put与nested stored replay | put不造Worker lease或Task lineage；移除自动lift不消除Task产生stored outer交界 | `test_put_handoff.py`、`test_put_owner_edges.py`、`test_put_reference_runtime.py`、`test_put_home_failover.py`；含Ref stored put两个真实exact | put合同覆盖owned/borrowed、INLINE/STORED、未知child/Seal/Drop ACK及真实absence fence。**snapshot03复验通过，有限Task nested replay缺口关闭**：`test_task_contained_reconstruction_path.py`观察foreign retained换代、再次导入、output旧/新hold替换与最终GC；Driver-owned put保留attempt0仍是另一条证据 |
| B07 whole recovery、完整请求与死亡 | B1真实START/JOIN、B2完整drop绑定保留；targeted首ACK草稿退出 | 三个owner reconstruction合同与drop请求合同；example06、foreign owner路由、递归lineage、一次系统失败；Node/Worker/owner死亡切片 | 精确重放不重复准入/扣预算。foreign owner重建结果与foreign input retained换代不同，后者由B06新增真实切片证明；INTENT类历史知识不能带回基础版 |
| B08 CPU yield与Actor | CPU只让出CPU、重复通知/Complete/death一次清账保留；Actor同Node restart/FIFO保留，Node migration退出 | example04/05；typed Actor failure及control/registry合同；真实Actor restart exact | 只按实际证据声明同Node重启；构造异常中resource文本不误判容量。Node-loss不偷偷迁移 |
| B09 硬约束PG 2PC | 至多两bundle、两种STRICT策略及prepare/commit/abort/LOST保留；soft优化退出 | placement/child ledger纯合同；example07、正常remove、第二participant PREPARE拒绝、participant Node-loss exact | 第二participant拒绝是明确测试注入，并非真实容量不足；不得混称。全ACK前不可见与实际容量账本由对应主要合同证明 |

B04新增exact是`tests/integration/test_output_retirement_ack_path.py::test_lost_actual_adoption_ack_replays_retirement_without_reexecution`。
单Node/Worker、一个2KiB stored Task，丢一次真实已接受退休回复；观察owner READY及finish/custody屏障，重放同一proof且不再次Push，
退休保留bytes直到正常GC。只有一次ACK丢失，没有进程故障，不宣称所有交错已覆盖。

B06新增exact是`tests/integration/test_task_contained_reconstruction_path.py::test_foreign_task_outer_renews_imports_replaces_edges_and_collects`。
一个parent提交Worker-owned Task，结果为child Ref和2KiB padding；Driver consumer读取真实foreign stored依赖，也返回含Ref的stored结果。
只drop/replay consumer一次，验证retained凭证换代、再次导入、旧/new contained hold及最终foreign lineage/bytes/child GC。
共三TaskID/四次执行、一个tiny put、单Node双Worker；不将put无lineage路径或完整多故障矩阵混入。

## 可复现执行入口

Linux/WSL中使用Python3.12和uv0.11.26，从仓库锁文件安装后执行：

```bash
uv sync --frozen --extra test --python 3.12
source .venv/bin/activate
python scripts/run_baseline.py --list
python scripts/run_baseline.py --pure
python scripts/run_baseline.py --smoke 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

当前工作树完整选择器只维护在[baseline_manifest.json](../scripts/baseline_manifest.json)，不复制第二份易漂移的列表。
七个原main共用上面参数化测试，完整参数为example01至example07；其他入口按清单原样一次一个执行。
`--pure`是一个30秒有界批次，`--smoke`仅接受基础清单与既有bounded清单交集中的一个exact；没有目录、glob、自由参数透传或`--all`。
新增保留合同case进入清单前落实成本/清理，历史清单不自动扩大基础门禁。

当前[baseline.yml](../.github/workflows/baseline.yml)固定Ubuntu/Python3.12/uv0.11.26，安装后串行运行这份清单并保存结果。
它还没有远端执行证据，仓库没有为此提交或推送。列表成功、静态YAML检查不能冒充安装或运行通过。

原生Windows只承担已受审纯合同/静态枚举，不宣称完整多进程支持。两个运行入口在Windows实际执行前拒绝，
`--list`仍可用；必要时可按已经审过的单文件纯合同命令运行，不把完整pytest目录作为替代。

## 环境与证据保存

当前真实进程环境是WSL Linux中的workspace独立CPython3.12.13 musl；依赖是pytest8.4.2、cloudpickle3.1.2、
iniconfig2.3.0、packaging26.3、pluggy1.6.0、pygments2.21.0。挂载盘冷导入曾超出既有启动窗口，
成功实验在同一次WSL调用内将Python、依赖与固定源码解包到`/dev/shm/mini-ray-validation`，预编译后立即运行。
没有放宽启动/测试预算，也没有修改系统Python或Docker配置。tmpfs不是持久交付物；输入归档、文件hash、结果和日志保存在上表workspace目录。

固定快照执行驱动先校验归档文件hash，再调用正式baseline入口；
每个内层runner拥有30秒测试树期限及退出清理，外层不会提前杀掉清理负责人。命令、环境、依赖解析与真实trace产物已归入[证据包](../artifacts/stage1-baseline/)。锁定安装已在Linux和Windows实际完成，记录绑定pyproject SHA256 `1d40acba410a38b46ee1c8385c0faed5089a2dba237b8388da5c76352484c6c6`、uv.lock SHA256 `642c93f87460dde26f47c06cf4d4908c2dc90834981957355ab2f8eff4b2e999`。Windows第一次freeze因缺workspace cache参数失败，随后补同一显式cache成功；没有把失败隐藏为通过。
snapshot03后仅汇总文档/证据；后续源码或测试修改必须另记其受影响验证，不能改写本次历史结果。

## 历史测试的边界

`scripts/reviewed_pure_manifest.json`保留历史201 whole files + 254 exact selectors / 237文件 / 2910 passed记录；这些数字不重写为基础版结果。
静态import审查已确认至少56个所选文件直接依赖已删除模块或TargetExecutionKey/TargetOutputManifest；精确函数selector也需先导入整个模块。
这是失效文件数下界，不是实际失败case数，没有通过整树pytest collection探测。

图/中央事务/多槽专属断言退役；完整身份、准确收据、保活、原子可见、旧attempt fence与GC迁到B01–B09。
第二阶段只提取仍适用的历史合同，不恢复旧文件整族，不把旧测试import失败本身变成基础版新增门槛。

## 阶段交界

- snapshot03的24文件纯批次、全部32smoke与七个原main产物已完成，B04/B06有限合同闭合。
- 原main、源码地图、黄金trace、API迁移、baseline入口及历史归档说明同步；包元数据限定Python3.12。
- 当前源码48,555代码行，仍超过初始低置信度预算；成本与职责集中问题按两阶段计划§8如实记录，未宣称紧凑代码量目标达成。
- 本地提交/标记`teaching-base-v0.1`固定本次基础版及证据，固定后才进入第二阶段；无推送或远端发布。
- 第二阶段两项增强是确定交付，尚未实现；后续保留本基础标记为推荐学习入口，不维护长期双后端。


## 整理E后继的引用说明

本页B历史318/32原文保留。E在共同B机制上仅增加两项mini自定义保证，本页结果不作为E候选或正式E HEAD的独立验收。E历史377/37见[增强账本](acceptance-enhanced.md)，本次候选与已建立的正式分支映射见[状态页](current-status.md)。
