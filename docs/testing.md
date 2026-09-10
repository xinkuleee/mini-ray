# 协议增强版测试指南

运行入口是[scripts/run_baseline.py](../scripts/run_baseline.py)，选择权威是本分支[scripts/baseline_manifest.json](../scripts/baseline_manifest.json)。测试结果必须绑定实际源码、测试、工具、manifest和依赖身份；当前完成状态见[current-status](current-status.md)，历史B318/32与E377/37分别见[基础账本](acceptance-baseline.md)、[增强账本](acceptance-enhanced.md)，不认证本次后继源码。

当前E隔离候选清单为30个pure文件和37个smoke，enhanced-trial-01取得392 passed / 1 deselected及37smoke通过；这只属于候选archive，不是已创建正式E分支的最终HEAD验收。正式E已从验收B派生，仍须在增量应用后冻结自身manifest、源码和依赖重新建立映射，见[状态页](current-status.md)。

## 安装与列出范围

在仓库根目录执行：

```bash
uv sync --frozen --extra test --python 3.12
uv run --frozen python scripts/run_baseline.py --list
```

Python范围为3.12，uv按0.11.26复现。list解析显式清单，不import或collect测试，不构成通过证据。运行使用POSIX进程组清理，Linux/WSL是主验证平台；原生Windows可list，当前执行模式在启动子进程前明确拒绝。安装/import成功不能当完整运行时认证。

## 三种执行选择

| 模式 | 选择范围 | 用途 |
|---|---|---|
| --pure | pure列表的显式unit文件，以unit marker执行 | 本版固定交付纯合同批次；同文件已知非unit case另列排除 |
| --smoke EXACT | smoke列表里的一个完整selector及其marker | 一次有限线程/socket或真实进程切片 |
| --case EXACT | 本版gate项或reviewed_migrations登记的一份纯文件/精确case | 有界分批调试或验证已审迁移，不自动扩大交付gate |

例如：

```bash
uv run --frozen python scripts/run_baseline.py --pure
uv run --frozen python scripts/run_baseline.py --case tests/unit/test_single_output_contract.py
uv run --frozen python scripts/run_baseline.py --smoke 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

smoke逐个串行执行。不要透传目录、glob、-k或任意pytest参数；无参pytest和目录collection由conftest在测试import前拒绝。仅有unit marker或collect-only并不证明被导入的文件安全。裸pytest的scope检查也不能代替runner的进程树边界。

## 迁移登记与重审

reviewed_migrations与gate分开：一项记录selector、marker、工作包、审查来源提交、导入闭包及配置输入hash、有限资源/超时/退出成本。清单解析不执行测试；--case执行迁移项前会重算其受审输入身份，变化则拒绝，必须核对变化后更新登记。文本登记仅将CRLF归一为LF；其它内容变化仍拒绝，原始证据hash不归一。

整文件只用于明确受审的unit文件；含进程/并发场景按精确case与marker登记。不能为了消除拒绝而盲目刷新hash，也不能把全部旧108/455项注册成新gate。确需提升为gate时，记录原因、旧新选择差额与成本，移出migration登记，保持单一选择权威。

本版额外覆盖发布事实、联合预留判环和有限C0–C7故障窗口；保持原30/37候选范围，不扩成任意storage×owner×fault矩阵。B结果不替代E自己的trace和实际GCS阶段。

项目的历史测试树包含旧协议与失效夹具。未进入gate不代表测试无价值；已退出multi-return、targeted或旧runtime facade也不应为旧测试恢复。保留反例迁到真实权威后，以“旧断言→现行合同→新selector→本版证据”记账。

## 时间、进程与退出

所有运行复用[_test_process.py](../scripts/_test_process.py)：30秒测试树期限、插件环境净化、PID/PGID正值及归属检查、fresh快照与PID复用防护、有界TERM→KILL/reap和detached后代清理。外层执行器必须给内部清理负责人完成退出的时间，不先杀掉它再声称无泄漏。

timeout返回失败，强制终止不是clean。测试自己的close/cleanup deadline、runner的整个测试树deadline与业务RPC timeout各有范围；ACK未知仍保留待清理责任。资源归还、PID/端口消失、owner metadata/lineage与bytes GC分别观察。

## 证据分层

| 层级 | 能证明什么 | 不能替代什么 |
|---|---|---|
| 纯合同 | 精确身份、状态转移、拒绝原子性、资源账本 | 真实RPC/进程退出与跨参与方接线 |
| 小型组合 | 真实authority配显式队列/typed callback的调用与续行 | 回调成功不能冒充真实远端ACK |
| 有界进程 | 选定故障窗口、真实消息、PID/端口/资源收尾 | 不外推所有storage×owner×fault组合 |

新测试针对实际风险，复用同义不变量，不给每个dataclass增加字段镜像测试。已知保留合同失败必须修复或明确未完成迁移；不能用skip/xfail、假ACK或假clean隐藏。

## CI与保存结果

[workflow](../.github/workflows/baseline.yml)及[_ci_baseline.py](../scripts/_ci_baseline.py)按push ref、PR目标或手动选定ref识别B/E。每job读自己的manifest，纯批次与smoke串行；B不等待E。PR合并预览SHA、分支HEAD和历史tag对照必须分开标记。

每次保留原日志、exact selector/marker、命令与界限、Python/uv/OS、依赖解析、源码与清单身份和退出结果。不同候选的通过不能相加成同版验收；失败日志不覆盖。无实际远端CI run不得写远端通过。

旧工具名称、checkpoint正文和协议载荷的来源见[历史索引](history-index.md)。它们用于追溯，不是第二份运行清单。
