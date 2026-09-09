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
