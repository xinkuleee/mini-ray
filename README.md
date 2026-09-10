# mini-ray：协议增强教学版

mini-ray 是参照 Ray Core 关键机制的 Python 教学实现。它与 Ray 的关系类似 mini-SGLang 与 SGLang、nano-vLLM 与 vLLM：保留可以沿源码追踪的执行、调度、对象、引用和有限恢复机制，缩小运行规模与行为范围。它不承诺生产 Ray 的完整功能、接口兼容或相同内部协议。

本页面向从验收基础版派生的 **teaching-enhanced**。E在共同的Task、owner、Node和child owner机制上，仅增加两项mini自定义保证：GCS普通结果发布事务、全局ObjectID引用图防环。两项在本版同时启用，不是可关闭的替代后端；它不比B更接近生产Ray。首次学习仍推荐独立的teaching-base，理解基本路径后再读E的额外同步阶段与责任。

正式teaching-enhanced已从验收B创建并独立验收：405项纯合同通过、1项按原标记排除，37个精确smoke首次全部通过，七main与冻结安装证据已保存；准确派生点、候选与最终验收身份见[状态页](docs/current-status.md)。固定历史E teaching-enhanced-v0.2的[377/37账本](docs/acceptance-enhanced.md)与历史B的[318/32账本](docs/acceptance-baseline.md)分别保留，不能认证本次后继HEAD。

## 安装与第一次运行

使用 Python 3.12 和 uv 0.11.26，在仓库根目录安装锁定依赖。运行时和有界进程验收以 Linux/WSL 为主；原生 Windows 可以安装、读源码和列出选择器，当前 runner 不提供 Windows 进程树清理。

```bash
git switch teaching-enhanced
uv sync --frozen --extra test --python 3.12
uv run --frozen python scripts/run_baseline.py --list
```

正式E分支已创建并验收，可按上述命令检出；精确实测提交及其后文档/证据提交的区别见状态页。首次阅读可先切换teaching-base走同一示例入口。

在 Linux/WSL 运行第一条完整主线；该精确smoke会调用原始示例并检查退出：

```bash
uv run --frozen python scripts/run_baseline.py --smoke 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

观察 TaskID、ObjectID、executor PID 和 canonical trace，然后打开[01_task_path.py](examples/01_task_path.py)。函数返回的 tuple 是一个完整值，Task 只产生一个 ObjectRef。需要复现实验时，按状态页给出的固定验收提交检出，不能仅依赖会前移的分支名。

## 保留的范围

| 机制 | 本版形态 |
|---|---|
| Task与调度 | 单输出，真实spawn进程，dependency gate、Worker lease、spillback和Core→Worker direct push |
| 对象 | INLINE或不可变sealed bytes；owner元数据、Node副本、pin/pull；显式put不建立Task lineage |
| 引用 | 本地引用、Task/lineage hold、独立borrower、容器内ObjectRef与真实GC；完整身份和精确收据 |
| 两项E增量 | GCS保存准确发布事实；图已提交边与并发预留共同判环，覆盖受支持Task/put/whole replacement入口 |
| 恢复 | 稳定ObjectID、新attempt、有限系统重试及单输出whole reconstruction；旧消息fencing |
| 嵌套执行 | Worker内嵌Core；阻塞get仅让出CPU，恢复执行时归还资源责任 |
| Actor | 独立Worker、串行方法、同Node有限restart；Node丢失终态，不跨Node迁移 |
| Placement Group | 最多两个bundle，STRICT_PACK/STRICT_SPREAD，两阶段提交后可见 |

运行规模为单job、单GCS、单机loopback、1–2个逻辑Node和少量Worker。故障合同限受管进程fail-stop、有限RPC未知回复及明确窗口。没有GCS持久恢复/HA、已死owner接管、任意网络分区恢复或外部副作用exactly-once保证。

大参数使用显式 put；顶层 ObjectRef 依赖取值，普通容器内的 ObjectRef 保留handle。ObjectRef.close 表示释放自己的引用，不代表物理GC已完成。drop_object 是重建实验的副本故障注入入口，不是正常内存管理API。

不支持多返回槽、targeted reconstruction、自动StoredArg、Actor引用参数/结果扩展或soft PG策略。普通Python容器环与ObjectID引用环不同；E在受支持contained-edge入口拒绝全局成环候选，不承诺修复任意损坏或手工注入的旧环。B仍不提供该额外图保证。

## 阅读与证据

- [学习路径](docs/learning-path.md)：七个示例和每条机制的源码入口。
- [当前设计](docs/design.md)：身份、职责、交接和有限故障边界。
- [测试指南](docs/testing.md)：固定gate、受审迁移case与进程退出界限。
- [Ray对应关系](docs/production-ray-mapping.md)：对应、简化和省略。
- [历史索引](docs/history-index.md)：旧计划与退役协议的固定原文；不是待恢复的运行时。

增强版的中央发布事实与全局防环增加同步GCS依赖、预留/补偿、死亡清理和收据退休成本；不表示“更接近生产Ray”。两个分支共用基础机制，各自保存实际源码与证据。
