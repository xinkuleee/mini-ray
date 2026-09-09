# 基础版固定验收证据

源码候选归档 SHA256：`42fa8b6406ae5672b434b1b479aa08ca3c36faab131429ea287970bf135cd5d0`。
`snapshot.json` 保存测试前476个输入文件的逐文件SHA256；固定提交中的源码、测试、脚本、示例和依赖与其一致。
测试后新增本证据包、汇总说明和版本入口，因此不要用汇总文档与测试前文档的差异推断运行时变化。

- `results.json`：一个固定纯批次和32个串行精确进程用例；全部退出码为0。
- `00-pure.txt`：318 passed / 1 deselected。被排除的真实线程case不是纯模型证据。
- `example-output/`：七个原main的真实输出，含例1通过合同匹配的规范化trace。
- `environment.json`：实际Linux/WSL、Python与依赖版本。
- `dependency-repro/`：独立Windows/Linux的冻结安装和import证据；缓存缺失等前置失败保留在各记录中。安装成功不表示Windows完整进程支持。
- `complexity.json`：统一AST/token口径的源码规模，不把行数预算视为正确性证明。

日志文件扩展名由`.log`改成`.txt`以随版本保存，正文未改；结果索引的日志路径已对应更新。
部分命令记录包含当时的临时绝对路径，只是运行来源，不要求重新创建这些路径。
附带的`freeze_project.py`、`run_frozen_baseline.py`和`measure_stage1_current.py`是实际审计脚本的副本，
保留当时workspace布局；常规用户运行入口是仓库根目录的`python scripts/run_baseline.py --list`。

复验使用Python3.12与uv0.11.26，在固定基础版运行：

```bash
uv sync --frozen --extra test --python 3.12
source .venv/bin/activate
python scripts/run_baseline.py --pure
python scripts/run_baseline.py --smoke 'tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]'
```

其余用例按`--list`中的完整选择器逐个运行。每次真实用例受30秒进程树界限约束，不能以超时退出冒充clean shutdown。
本证据不认证全部历史测试、任意故障组合或第二阶段协议。
