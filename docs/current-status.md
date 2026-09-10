# teaching-base 当前状态

基础教学分支已独立验收。推荐从[学习路径](learning-path.md)开始理解所选Ray Core机制；增强分支在这一基础上增加两项mini自定义协议保证。

- 分支：`teaching-base`。
- 实测源码与验收时HEAD：`0a340b792c89667e493f7e7313935e45e29071bf`。
- 固定纯集合：343 passed，1项按原非unit标记排除。
- 同源码32个精确smoke首次全部通过，包含七个原示例main。
- Python3.12.13、uv0.11.26；当前源码冻结安装、依赖兼容与安装路径验证通过。
- 源码56个Python文件，57,995物理行、47,449代码行；不含测试、文档或验收材料。

详细身份、环境、命令、原日志与统计见[本版验收](../artifacts/cleanup-base/final/acceptance.json)。实测之后追加的证据/文档提交不冒充重新测试；最终交付映射会记录实际B分支HEAD。

P1–P5所采纳的单结果、owner退休、typed待办和窄Complete ACK已实现。P2进度表合并与P6 Core领域提取经过真实试做，因增加扫描/接口成本而保留现有设计。完整逐项结果见[执行记录](cleanup-progress.md)。

早期本机冷启动/运行曾发生5秒或30秒超时，全部失败保留；最终运行记录包含相同解释器checked-hash源码缓存策略，不放宽时限，不承诺冷启动性能等价。依赖离线缓存缺失的失败和随后联网冻结安装也分别保留。

B保留成员、资源、Actor和PG的GCS职责，不含普通结果GCS发布事务或全局ObjectID防环。`teaching-enhanced`已从验收B交付HEAD派生并独立验收这两项保证；其405/37结果与身份见增强分支自己的状态页，未发布远端或运行远端CI。原tag和历史artifact不变。
