# R23-02 StageAck 独立净收益与采纳边界复核

**结论：支持采纳此单一冻结候选，随后完成原计划已有的正式E有限gate、受影响迁移和安装/导入检查。** 当前材料已证明一个有净收益的局部协议表示取舍，尚未证明正式新源码完成最终验收。源码变长和验证分支增加是实际代价，不应宣传为全运行时更短、更简单或网络加速。

本复核者未编写StageAck源码或计量harness；此前只独立阅读7文件源码。这里重新检查measurements、原capture、比较脚本/harness与结构成本，未执行测试、项目导入或计量工作负载，未改正式仓库。作者的disposition-review.md已公开兼任实现者身份；本结论基于独立复算，不把该作者建议视为额外测试证据。

## 固定材料与复算

- 源码补丁SHA256：1da561e41b36764f29bb6de23794f4525f199665bfa35c0c56f4ed29664c8574。与此前独立源码审查身份相同。
- measurements.json SHA256：a0b986e7148ceff0ecc04a8d0a7f2ce67c6f47e39b84e452fe24135a65a62a4d。共30份capture，即两版各4场景×3次，再各补mixed×3次seed0；所有引用capture SHA均核对匹配，原frame byte与copy计数重新汇总匹配。所有保存运行exit0，capture的COMPLETED_ASSERTIONS及failure_cleanup_error=None成立。
- 两版test_ack_lifecycle_measurement.py字节相同，SHA256为2068eba394db342dfe4ff284e4758011b61f3ddf110b420487c507613983c83d，且与两次冻结snapshot登记一致。使用真实本地Core/Node adapter/authority/client方法与受控callback，非孤立DTO拼装。
- structure-cost.json的同7文件统计为+204物理行、+181代码物理行、1个wire class/8字段、+62个if；不把AST计数等同执行耗时。

## 有收益，但收益范围明确

| 实际受控生命周期 | GCS业务frame字节：完整Reply→StageAck | 下降 | 计数内copy/validation调用：完整Reply→StageAck | 下降 |
|---|---:|---:|---:|---:|
| 普通零child Task | 37,131→25,798 | 30.5% | 21,495→15,602 | 27.4% |
| owned/borrowed含Ref Task（含一次commit回复未知） | 149,225→100,718 | 32.5% | 66,733→50,631 | 24.1% |
| 含Ref put | 113,114→91,700 | 18.9% | 37,000→31,234 | 15.6% |
| whole替换及旧历史重放/收口 | 181,290→115,534 | 36.3% | 79,056→56,708 | 28.3% |

四场景的完整业务frame总量也下降14.8–31.3%，不是只挑新ACK本身大小。callback、原请求和GCS query数没有增加，Task0为2个query，其余为8；混合路径每次保留一次实际已生成但丢弃的reply并计入成本。重复轮次成本相同，最小收益的put也未被fact重装或client新增owner检查抵消。

可采纳的原因是：operation的准确阶段收据/accepted_fact与完整历史query形成明确边界，现有ARM、Complete、forward、首fence消费者可沿同一参与方链条读取必要事实；消除了正常成功reply多层重建整份历史的可测重复工作。原records、graph、journal和Core职责不变，没有新RPC、等待点或恢复缓存。收益属于每次成功阶段都会遇到的本地处理与序列化负担，足以支持这次受限表示改动。

反面成本不能省略：新增StageAck构造器与projection factory又形成一份跨字段约束，未来协议修改必须与完整snapshot验证同步；client.call从13行增到36行，理解两种reply家族和8种fact含义也比原单snapshot更费阅读。request echo与accepted_fact仍可能重复，关闭历史还带fence/退休收据。其净收益不是消除全部copy，更不是降低总代码量；只是本实验未发现这些新增成本抵消实际纵向节省，且改动集中在具体wire边界，没有增加另一套状态机。

## 原mixed-r2不相等没有被掩盖

初始12对中task_mixed-r2的same_actual_requests、same_semantic_checkpoints仍为false。measurement-differences.json准确保留两child Release及首次RetireGraph闭包tuple顺序相反，不能排序后称原历史完全相等。未改Core的pending_edges迭代，初轮未固定Python hash seed。

追加PYTHONHASHSEED=0后3对mixed的原顺序请求、完整检查点及callback均相等，成本与原轮一致。因此可以据新增同环境对照评价候选；原r2仍是有效首proof次序不同的运行，不改成相等，也不构成候选改变既定生命周期的证据。

## 测量与最终选择边界

计量使用项目serializer及业务request/reply envelope＋4字节frame长度，排除trace sidecar。profile仅覆盖指定的_copy、__post_init__与snapshot/prepared/closed验证调用，排除测量自身序列化、显式测试观察；它不是全CPU指令或完整运行成本。harness不运行真实socket/OS Worker，Node completion回调也不证明真实资源ledger行为；receive反序列化、网络延迟、吞吐、端到端加速未测量。上述百分比只能标对应字节/指定调用次数。

因此不提出网络性能补测作为新门槛，也不新增全过期测试树或故障矩阵。root已有实际进程、邻接journal旧fixture迁移及正式37-smoke gate/安装计划继续完成即可。基线journal23旧fixture失败须如实保存与迁移；不能把基线已有错误归给StageAck，也不能用基线失败跳过保留合同的正式闭合。

本报告支持采用当前实现方向，不提前把R23-02写成最终已验收；若正式同输入验证发现真实协议问题，应按原计划修复或有据撤回，不能用本净收益建议覆盖失败。最终只应用7文件业务源码及必要测试/文档，不携带harness、profile或对照开关。
