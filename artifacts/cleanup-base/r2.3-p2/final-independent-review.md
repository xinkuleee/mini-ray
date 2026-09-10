# R2.3 四项P2最终独立完成复核

**结论：按现有主计划§11，R23-01至R23-04均有对应实际修复/处置和有限证据，未发现剩余实质未达项；当前可按“本地未提交工作树已完成四项P2”交付。** 不包括明确排除的P3，不表示全部历史或登记测试运行过。复核期间root已完成最后43项并归档，最终状态措辞的小残留也已核对修正。

本代理未编写运行时StageAck或计量harness，此前承担过独立源码/成本审查与受委派文档候选。本轮只读代码差异、AST、已有JSON/日志及Git对象，未运行pytest、collection、项目模块或运行时构造，未修改正式仓库。

## 四项关闭依据

| 条目 | 实际结果与判断 |
|---|---|
| R23-01 两版owner-finalize迁移 | B after02、E after01分别15目标/10邻接/1实际owner死亡通过；独立STORED和INLINE、真实ACK前无result但保清理义务、bytes/manager/Worker/journal分权、错误claim不删除均保留。E明确Node退休后authority仍FENCED/graph_active，未伪造全局退休。两版各自dirty快照hash独立保存，B结果未替E。 |
| R23-02 增强GCS ACK/copy | 一个7源码文件StageAck候选已按冻结hash正式应用；完整query/ABSENT和typed failure仍用PublicationReply，没有第二权威或新RPC。源码与成本独审支持采纳，后续实际final gate、43项受影响登记与新源码安装完成，条件已满足。 |
| R23-03 E Ray对应页 | 当前E相对源码链接解释两项增强门禁，B代码固定0a340b7、教材/证据固定ab4cfb3；Ray固定c3162dc，职责对应不冒充相同协议。GCS成功知识/bytes、put/Task、图/容器环分别说明。 |
| R23-04 现行测试/设计身份 | 392候选、R2.2正式405/37及R2.3新快照分开；design不再称正式E为候选，已说明StageAck及完整历史query边界。状态/README/计划明确完成但未提交，新工作树不冒充旧HEAD直接实测。 |

## 最终运行与身份，不混算快照

- B HEAD仍为 ab4cfb317fd786a286359d8f4e971ff195739375；R23-01实测dirty输入after02 archive为98fa2fbf0eb921b87b13b9a335d7247d2a6f7ee69dab8080d1e2f8e1690ba5f3。原B运行时、共享helper、gate/依赖未改，旧343/32仍归0a340b7，没有声称重跑整版。
- E HEAD仍为 fd40a407a46ada3d4941c034a73355f45355b270。final01 archive a3bfb6588e639dd2655ed2b8a23c736f3fb4730dfbf0cec51af14f6ae0f3b1fb 保存38条首次exit0：pure 405 passed/1 deselected、37/37 smoke，selector逐项等当前gate，七main包含在内。
- E final02 archive e9b33771f1e0721d404f1a135a22fac1ac7958e0cd537d30110dc1439f96c17e 保存 **43项** 受影响registered选择全exit0，等于affected-selections完整集合，无遗漏或重复；口头原42已由实际43替代。包含journal23、StageAck8及owner-finalize15等，不能把这些case加进405。
- final01→final02恰变化docs/design、learning、manifest、非gate journal测试。解析两归档manifest确认pure/smoke/exclusion列表相同；67个gate选择的已保存AST导入/配置闭包与差异集合无交叉，journal不在其中。故gate结果准确归final01，final02用闭包映射和实际受影响验证衔接，无须为纯文档/非gate迁移再重复整gate。
- final02新源码冻结安装、pip check/freeze和项目外import等7命令均exit0；安装identity的archive与final02一致，日志显示真实新目录build/install及import路径。没有拿旧安装移贴新StageAck。
- 最后复核B及E当前执行输入均匹配各自实测snapshot；E共413项执行输入raw hash无差异。之后当前说明/证据改动不改标为新运行结果。

正式E新final证据已存在。gate目录50、affected49、install9份文件与对应audit原文件逐字一致（不复制project.tar）；acceptance明确dirty输入、final01/final02区别、43选择、安装及测量限制。B新acceptance也准确记录自身15/10/1与历史门禁未重跑。

## 范围、反例与历史保留

E正式src差异恰为7个已审StageAck文件，各raw hash等候选身份；B无src变化。无helper/alias/恒零参数/P3清理夹带，没有改Actor/PG调度算法、公共B ACK、Core恢复权威或图算法。计量harness未进入正式活动tests，只有证据目录保原文。

所有本轮已改原测试的同名函数与参数decorator均保留；唯一删除的原函数是计划明确拆分的STORED/INLINE复合函数，转为两个独立函数。新增8个StageAck边界case对应新wire风险。后发现的E journal旧夹具保持原18函数/23参数，实际补齐child-owner replies与GCS阶段收据；owner/CAS/实体删除仍明确属于模型边界。旧完整Reply与StageAck均实际23失败，修后同一文件两边23通过，final02再23通过，未将基线问题归罪新ACK或据此删合同。

原owner-finalize14失败（各版）、B after01 deepcopy14过/1失败、E journal旧23失败及对应修正全部保留。ACK试验初24运行中的mixed-r2原请求/闭包顺序false仍在，追加seed0六次原序校准而非排序抹差异。测量是受控本地真实路径的serialized business frames与指定copy/validation调用，明确没有network/time/receive-decode性能结论；+204物理/+181代码行及新wire校验复杂度未隐去。

两实际分支继续存在；两tag仍指原6910677与ce29981。原B65/E126 artifact在HEAD的Git OID逐项与原tag一致，当前tracked artifact无diff；原R2.2/R1/R2历史没有被新结果改写。未提交、推送或运行远端CI，符合当前授权与完成口径。

## 最终文档一致性

两版README/current-status/cleanup-progress/plan/testing/design/learning/Ray mapping八页的相对链接目标均存在。已重读最终plan页首与§11当前说明、JSON四项status、E design首段：统一四P2完成、仍为未提交工作树，历史时点由页首限定；原“待修/正在推进”残留已由root修正。P3仍明确未处理，没有把§11变成第二套计划。

可直接将本报告纳入各版新证据；不要求额外矩阵、全历史树、全部340登记或再次运行已覆盖gate。若之后任何执行输入变化，当前结论只适用于上述冻结hash，按原计划对实际受影响行为更新证据。
