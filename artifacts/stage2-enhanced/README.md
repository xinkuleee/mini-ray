# 协议增强版固定验收证据

测试输入归档SHA256：`f3782f572b96233e8f9bae3a7915b8a7f3534904c11f857621df367aa530344a`。
`snapshot.json`记录488个测试前输入文件；最终提交中的运行时、测试、示例、runner与锁定依赖与之相同。
测试完成后添加证据包和汇总文档，没有用历史结果认证新的运行时代码。

- `results.json`：固定29文件纯批次及37个串行真实进程选择器，全部退出码0。
- `00-pure.txt`：377 passed / 1 deselected；真实线程case仍不计为纯证据。
- `example-output/`：七个原main的实际输出；例1保存六个GCS阶段、owner READY和托管退休的规范化trace。
- `environment.json`：实际Linux/WSL、CPython3.12.13与精确依赖版本。
- `complexity.json`：同一AST/token口径，50,277代码行；这是实测规模，不是紧凑教学目标已完成的证明。
- `dependency-repro/`：相同快照的冻结安装和增强模块import证据。

日志正文保持原样，扩展名改为`.txt`以随Git保存，结果索引同步修改。记录中的临时绝对路径属于当时的执行环境。
根目录的`python scripts/run_baseline.py --list`提供本版本精确入口；安装命令为`uv sync --frozen --extra test --python 3.12`，uv版本0.11.26。
每次smoke独立使用30秒进程树界限；超时不能被解释为clean shutdown。

基础行为、两项新增保证与组合窗口均在本快照复验。W1/W3/W4主要为真实权威对象的有界协议组合，
W2和公共成环/并发预留为真实进程证据；不要混淆证据层级。未运行全部历史测试、压力或任意故障组合。
基础教学版本仍为`teaching-base-v0.1`，其证据位于相邻`stage1-baseline/`且未修改。
