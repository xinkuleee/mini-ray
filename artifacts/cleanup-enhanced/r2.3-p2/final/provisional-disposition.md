# 单StageAck候选：主审处置与剩余验收

当前选择：**采纳该已试验的单候选，正式应用和最终通过仍须完成计划的有限门禁。** 对照源码和候选源码均固定，7个源码文件之外不扩设计。

四种真实受控生命周期（零child Task、owned+borrowed Task、含Ref put、whole替换）各三次，两版均完成全部断言。普通路径、put、whole的原序请求/完整语义检查点相同；mixed原r2出现两child Release及首次Retire proof tuple顺序差异，原日志和false比较保留。经确认未改Core原set遍历影响，固定PYTHONHASHSEED=0分别补三次mixed，共六次全部原序相等，没有改生产排序或归一proof。

| 生命周期 | GCS完整业务交换帧字节：对照→候选 | 本地copy/validation调用：对照→候选 | GCS query数 |
|---|---|---|---|
| Task无child | 37131→25798 | 21495→15602 | 2→2 |
| Task含owned/borrowed child | 149225→100718 | 66733→50631 | 8→8 |
| contained put | 113114→91700 | 37000→31234 | 8→8 |
| whole替换 | 181290→115534 | 79056→56708 | 8→8 |

这些数字包含实际成功/丢失ACK、request echo、必要fact/closing复制和原业务query，不包括trace sidecar、真实网络接收反序列化或耗时性能。具体有序原始值保存在measurements.json关联captures；不同层指标不相加。

采纳理由：E的教学目标正是解释中央阶段事实与全局准入。成功mutation只返回本阶段准确事实，查询继续负责完整历史，前进条件来自关闭证据；这使读取边界与其责任直接对应。实际减少整份history被每阶段、每接收层重复携带/验证，没有新增权威或同步调用，也没有把历史挪成Core缓存。

代价同样明确：+204物理/+181代码行，新增一个8字段wire类型、62个if和严格factory/client分支；完整snapshot validator仍保留，是额外维护面。此候选不是源码缩短，也不保证总体教材更短。增加的校验主要集中在一种具体响应和已知owner检查，Node/journal接线很小，Core/调度未改；在现四条实际路径上，必要重复fact和额外owner复制已计入后仍有收益，未被新query或同量history重装抵消。因此按原计划的职责及净成本规则选择采纳，不用任意LOC/百分比阈值。

已过：候选四增强文件37/5/15/4、新ACK边界8、完整pure405/1deselected、R23-01目标15、Node-server13、trace6。尚需计划内真实process/37smoke、相关受影响迁移、正式输入安装和最终独审。

追加发现：test_output_publication_journal.py的23例在候选和未改E均因旧B-only夹具未建立PREPARED门禁失败。失败不是StageAck回归，但属于本轮journal接线验证必须处理的具体旧绑定。仅适配该文件的E真实authority/child/ARM等前置，保原23例，不删或绕门禁；修复后再验证。其余P3不借此批量清理。

当前未宣称R23-02正式验收完成。若后续出现实际协议问题，按原计划保留最小反例并限定修复或撤回，不以本选择掩盖失败。
