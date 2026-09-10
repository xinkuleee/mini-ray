# teaching-enhanced 当前候选状态

日期：2026-09-10。**正式teaching-enhanced分支已从验收B交付HEAD创建，E运行时增量正在应用，尚未完成本分支验收。** 独立候选enhanced-trial-01已实际完成30个pure文件、392 passed / 1 deselected及37个smoke全部通过；结果只归该候选的identity/snapshot，不能移贴为正式E当前或未来HEAD的验收。

| 身份 | 当前记录 |
|---|---|
| 实际分支 | refs/heads/teaching-enhanced；工作区mini-ray-enhanced，已创建 |
| E准确派生点 / B交付HEAD | f9a9b35015f114afda9c87e653b6fedcda2eb0b2；B交付只在受测源码上增加文档/证据 |
| B tested_source_commit | 0a340b792c89667e493f7e7313935e45e29071bf；B纯343、32smoke首轮全过及uv frozen安装通过 |
| 本文档输入 | 从B当前入口适配E；B受测SHA和E派生SHA均不是E tested_source_commit |
| 已运行隔离候选 | enhanced-trial-01；archive SHA256 babbc024ebed972a8da816cd06a9b7b0634e4bd356f5142faf87d121ee8f2596；其identity明确branch/head为null |
| tested_source_commit / head_at_acceptance | 正式E复验和最终映射待root记录，不填未知SHA |
| 历史E | teaching-enhanced-v0.2 / ce29981a547f83b53b0c1df9f91354dcf89d8e4f；377/37只属该历史版本 |
| 远端状态 | 未运行远端Actions或发布本次清理分支 |

## 当前机制与证据边界

E仅增加GCS普通发布事务与全局ObjectID引用图防环。owner仍唯一提交READY/outgoing/recovery，Node仍唯一提交Complete及资源释放，child owner仍唯一持有incoming hold；GCS成功metadata不能制造bytes或接管owner。TaskExecution、单value/payload/result、typed待办、scalar journal snapshot与窄Complete ACK继承B；不恢复恒零结果维度或旧global graph模块。

普通Task依次经历C0 INTENT、C1图预留、实际child/materialization、C2 ARM、C3本地Complete、C4 terminal记录、C5图提交、C6 owner提交和C7 adopted收据。put只使用其独立身份与图子协议，没有Task lease/ARM/Complete。新增收益和同步GCS依赖、不可达等待、预留补偿及死亡清理成本见[设计](design.md)。

候选392/37的原始记录位于工作区audit/two-version-cleanup/execution/enhanced-trial-01/{identity.json,snapshot.json,results.json,000-pure.log}。本目录是过程证据，正式E归档位置和HEAD映射待root封版后记录。该候选底为base-k6试验，正式E已从含共同尾部正确性修复的B交付HEAD派生，E增量仍须在自身分支验证，不能由本次候选通过自动覆盖。

[B历史账本](acceptance-baseline.md)与[E历史账本](acceptance-enhanced.md)原事实不重写。首次学习推荐B；E用于比较两项mini自定义保证的阶段、故障知识与成本，不表示更完整的生产Ray实现。最终必须保留两条实际分支各自的源码与证据，B不等待E，E不借B结果冒充自身验收。
