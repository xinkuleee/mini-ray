# teaching-enhanced 当前状态

增强教学分支已完成计划中的独立有限验收；首次学习仍推荐teaching-base，再在本分支研究两项mini自定义保证。

| 身份 | 已核验记录 |
|---|---|
| 实际分支 | refs/heads/teaching-enhanced |
| 从B派生的交付HEAD | f9a9b35015f114afda9c87e653b6fedcda2eb0b2 |
| B实测源码 | 0a340b792c89667e493f7e7313935e45e29071bf；343/1deselected、32smoke与冻结安装通过 |
| E运行时实现提交 | dbc504c0cbc69dcdc4b64128b72b6f852bdc34fa |
| E实测源码与验收时HEAD | 3f5b725fb26390b86c78f085486fb73d897d3e42；在上述运行时上补齐迁移测试及审查身份 |
| E同版有限门禁 | 30个纯文件405 passed / 1 deselected，37/37精确smoke首次全部通过，含七个原main |
| 安装 | Python3.12.13、uv0.11.26；同版uv冻结安装、pip check/freeze和项目外导入通过 |
| 源码统计 | 59个Python文件，60,019物理行、49,138代码行；不含测试、文档或验收材料 |
| 远端 | 两分支本地保留；未推送或运行远端CI |

详细身份、环境、命令、原日志、七main与合同映射见[本版验收](../artifacts/cleanup-enhanced/final/acceptance.json)。追加文档/证据后的分支HEAD与实测提交分开记录，不声称后续HEAD直接运行过测试。原K3七组增强合同均已有处置和有限证据，不表示全部338个注册入口均已执行。

E只增加GCS普通结果发布事务与全局ObjectID图防环。owner唯一提交READY/outgoing/recovery，Node唯一提交Complete及资源释放，child owner持有incoming hold。GCS成功metadata不能恢复bytes或接管owner，INTENT/ARM也不能证明执行成功；单GCS内存权威不提供HA或持久恢复。

普通Task依次经历C0 INTENT、C1图预留、实际child/materialization、C2 ARM、C3本地Complete、C4 terminal记录、C5图提交、C6 owner提交和C7 adopted收据。put使用独立身份与图子协议，没有Task lease/ARM/Complete。新增收益、同步依赖、补偿及死亡清理成本见[设计](design.md)。两项同时启用，不恢复多返回、targeted或旧整套协议。

P1–P5所采纳的单结果、owner退休、typed待办和窄ACK继承B；P2进度表合并与P6 Core领域提取经过真实试做，因成本增加保留现状，见[执行记录](cleanup-progress.md)。源码代码行从固定E的50,277降为49,138；不能把历史文档和测试的缩减计入运行时节省，也没有达到旧20k预算。

运行基于Docker Desktop WSL Linux，预先准备相同解释器CHECKED_HASH源码缓存，5秒启动及原有限测试界限未放宽。早期运行超时、离线缺包、K8夹具失败和trace迁移失败分别保留；最终通过不抹去这些限制，也不承诺无缓存冷启动性能等价。

[B历史账本](acceptance-baseline.md)与[E历史账本](acceptance-enhanced.md)保持原版本事实。当前真实进程证据与纯reducer、synthetic trace matcher证据分开；旧双saga、死亡Core继续运行等退出形状有明确非等价处置。两条学习入口和各自验收均保留。
