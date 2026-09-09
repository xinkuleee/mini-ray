# 双分支整理执行记录

本页记录[清理计划](project-cleanup-plan.md)的执行，不是另一份计划。

- K0完成：teaching-base从固定6910677派生，审计输入提交1a38228。原tag不变。
- K1完成：单schema2有界gate/迁移入口、CF-001至004公共修复；新入口验证后退休三个旧工具文件。详见[实际记录](../artifacts/cleanup-base/k1/summary.json)。首轮新fixture失败已保存，真实准入修正后12组合case通过。
- K2进行中：确认残留/API清理；只有删除前提已满足条目才应用。
- K3至K7未完成；teaching-enhanced尚未创建，须从K7已验收B派生后执行K8至K9。
- 未运行远端Actions或发布清理分支。

K1复现修正：registry文本身份只归一CRLF到LF，其它字节变化仍拒绝。新增工具回归31 passed；独立LF副本的两迁移闭包静态验证通过。原始测试artifact不改写。

K2a完成：43项确认无消费者的定义/字段/import/便利接口从16个源码文件移除，保留所有校验底层；341 passed/1 deselected，真实golden trace 1 passed。余下需迁移SRC/API项尚未删除；新替代反例在验证前不构成删除许可。

K2b迁移前置：5个替代纯文件18case通过，Actor普通restart真实smoke通过。修复原合同要求的Actor结果Ref拒绝（含hidden reducer），未增加ActorRef支持；18个旧helper导入已提取。尚未据此批删旧测试。

K2c迁移前置：foreign-lineage4、owner API10、owned-drop8、owner-service4、dead-worker3、foreign-finish14均通过。七个待迁API仍保留到旧消费者闭合；不以新case通过直接跳过原混合文件合同。

K2d/e切片：cluster19、worker locality重试1、lease inventory39/registry13、Node monitor1、Node lease4、recovery13通过。worker locality初次Node启动超时原日志保留。core-owner4个fixture未完成真实准入导致失败，待修；该file与5个SRC死亡smoke尚未验收，不删除关联SRC。

K2g：原adopted-owner与四个publisher死亡窗口在当前B handoff/Node/实际bytes路径全部通过；不以纯模型替代。finish11/lostlock4/deadline7/entryfailure7通过；foreignwait4通过1因fixture未消费put唤醒失败，已修待重验。

K2h：foreignwait修fixture后5通过；Task真实Seal/Drop/owner rollback未知组合1通过；explicit put含PENDING Ref跨Node导入真实smoke1通过。core-owner6通过1仅旧错误文案待同步，未记整体通过。

K2i：core-owner7、sourceRef/Taskseal2、NodeMember6、NodeWorkerDeath8通过。关联6个旧SRC接口删除并AST核验，Node5未执行因父fixture规范hash变化被正确拒绝，下一冻结重登后验收。

K2j发布族：discovery34/protocol37/journal23、Node-server13/owner-death10/dead-child28/replica22通过；publication50通过1异常类型断言、finalize6通过3旧progress预期失败均已精确修测试，待下一冻结复验。保留所有失败及hash拒绝记录，未降低runtime校验。

K2k：publication51/finalize9、stored-contained7、Worker completion9/unified75、owner33/same-owner12/cross-cleanup45通过；同Core双caller真实2线程重建1通过。object-ownership7通过1因未终态fixture导致collection为空已修待验。

K3a：objectownership8、Workerinline8/materialized7/stored25、foreignlineage6/service15通过；Worker side-Core14个pure函数与4个真实loopback selector全部通过。未扩大固定gate，迁移证据单独保存。

K3b：contained-GC5/core-task-retry6/lease-handshake6通过；local/foreign迟到副本原两个真实smoke均通过（13.43s/18.62s），保旧epoch准确Drop与新重建bytes/hold不被晚消息破坏。

K3central：6旧central/graph文件迁B26case全过，collection-policy11case通过，2纯全局图文件按B/E差异映射退休；E待迁清单保留而非忘记。single-owner-model15原函数转单输出25case通过。

K3c：新Core publication13/latecleanup5/NodePG11/surviving4/coretaskretry6/ownerdefer3/journal23通过，最后2个无用SRC退休保持精确tombstone。NodeDeath12过4坏夹具、递归5过1缺descriptor、foreign4过4rawhold、retirement5过3漏loss route、PG4仅已set Event tripwire均已定位为测试接线待复验；未降校验。

K3d：NodeDeath16/recursive6/foreign8/retirement8修后通过；PG4/boundary2/ownerdeath2/CoreNodeLoss9/fencing13/terminal6/lineage4通过。NodeBlocking全部7exact过、borrower所有pure与2L1过；CorePG单一空result夹具、containedruntime2个失败ACK门禁、supervisor单一两槽wire负例已精确修待复验。ownerINLINE scratch丢失确认为真实Core错误，修复保实际owner bytes，下一快照6+9+12回归通过。

K3e：确认并修复ownerINLINE真实数据仍在却因scratch丢失被误判LOST；先从既有owner完整receipt/result重组envelope，保已锁UNKNOWN不反转。receipt6+NodeLoss9+CF12、lease4/storedadopt26、owner存活34/replica20/retirement26全部过；增强后继须继承此公共修复。

K3f：单次manifest校验共用path缓存、单次verify复用已得hash，缓存不跨CLI；33工具case通过、入口约1.2s。publication-source21/worker-crash16通过，旧module alias/跨版本pickle回退不恢复。

K3g：inline恢复7/Worker discovery29/stored metadata2/PG key1/supervisor wire2通过；pregrant/GrantACK未知/PinACK未知/ReleaseACK未知/requester真实死亡5个原集成场景均通过，未加新故障组合。

K3h：cancel-inventory40/location-custody27/Actoroptions1通过；readiness trace3个旧Pending字段fixture失败已移除过期field等待复验。

K3j：ordinary reconstruction9、recursive planning11通过；保DAG依赖顺序/去重/正确拒环与nested lifetime-only区别，旧selected/sibling维度未恢复。

K3i/k：readiness trace4、public values修后24、typed-source7/contained identity6/nested manifest18通过；stored gate37pure+3原socket L1通过。contained retry2剩调度旧通知夹具已第二次修待重验，失败完整保留。

K3l：PGdispatch2/普通结果knowledge5/containedGC失败ACK2修后通过；INLINE/STORED Node server原44函数全部pure+原L1通过，保重放/锁/实际写入/Complete不重复清账。3个未登记selector拒绝不计测试失败，已在下一冻结补登记。

K3m：inlinegate33/NodeDeathGC2/foreignstored13/trace52/traceobservation6/Nodeadapter28通过，foreigninline12pure函数13case+2原race L1全部通过；preflight未知分支1处错要求successreceipt已定位修测试，25其它通过。

K3n：collection通用guard17通过并退出旧7个硬编码锁；14个原集成迁移全部通过，覆盖回收收据重放/PG peer loss两个窗口/route与multiowner/child Worker death/INLINE已收未收/存活副本KEEP/unreported/embedded owner证书/borrowedunknown。

K3o：foreignStoredTask原19全部通过，完整两个owner/location/custody/quarantine/GC未抹掉；owner预检修后26与Worker nested20通过。当前只剩原集成最后2case、owner-fence小组合及既定full32共同回归用于whole12退休，后续进入K4。
