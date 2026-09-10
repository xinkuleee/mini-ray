# P5 after P4：完整窄Complete ACK候选

状态：**候选已冻结，未执行测试，未采纳到base。** P6必须先有正式处置记录，再由root进行本版P5有界验收；本包不通过修改计划跳过依赖。

冻结目录：[base-p5trial-01](C:/Users/t-hdong/Desktop/gao/audit/two-version-cleanup/execution/base-p5trial-01/identity.json)。

- archive SHA256：`a0516c862f64ea35f1fedf19f1322877b0b938ee625a7333982799649f830ee8`；421个输入。
- 准确底为P4trial02：`cfc3730b2d2b595e39e623e698275b707b0c89ee9e7881733a820d633d7c6fb5`。
- `p4-input/`为原底，`p4-overlay/`为可执行候选；[p4-to-p5.patch](C:/Users/t-hdong/Desktop/gao/audit/p5-after-p4-candidate/p4-to-p5.patch)只含代码/测试hunk，不含registry刷新。
- 最初P3准备稿留在input/candidate及p3-to-p5.patch，仅作过程证据，不作为最终应用源。

## 修改与闭包

生产仅output_protocol/core/node/worker四文件：Complete回复为`OutputHandoffCompleteAck(witness,accepted,error)`；Node真实callback核exact类型、深重建、同witness及accepted，Worker无existing owner也返回对应具体拒绝。Register/Rollback/GetHandoff维持完整snapshot，query深复制不改；不合并RPC，不增加validated token，不跨锁复用未校验对象。

27个现有Complete相关consumer已逐项核对：[closure.json](C:/Users/t-hdong/Desktop/gao/audit/p5-after-p4-candidate/closure.json)。18个需改窄ACK/观察，9个仅handler直通、accepted-only或请求failpoint，明确记录无需改的依据。原测试名称全部保持。新增`tests/unit/test_output_complete_ack.py`含8个pure case（2ordinary/contained丢ACK路径、4畸形/错误ACK、1历史权限/query别名、1Worker无owner拒绝）；只做AST/内存compile，未运行。

spillback测试桥实现的是local handoff table接口：窄ACK后若原测试要完整snapshot，明确经该test-only桥的query观察；没有从ACK制造snapshot，也没有给production Complete callback增加查询。记录该测试接口成本，不能把它隐去当“所有路径零query”。

## 与P3/P4的隔离

先在精确P3snapshot上生成有限patch，再`git apply --check`/apply到P4trial02完整展开树，未用旧Core覆盖新Core。AST复核Core只有`report_output_handoff_complete`改变；P4 `_put_value`、`_drive_put_handoff_cleanup`、`_retire_lost_output_memberships`、`_drive_output_node_loss_once`保持。P4trial02 storefull observer修复及ownership.py逐字保留。root另待处理的P3 preflight一行修复未混入。

原26 pure/32 smoke门禁不变。仅在隔离候选registry追加新pure文件并刷新所有review输入hash：311→312。`review_source_commit`沿用底的真实提交SHA，identity明确它是候选来源而非把候选冒充实际分支HEAD。

## root有界验证建议

新文件先经现有`run_baseline --case tests/unit/test_output_complete_ack.py`；随后固定pure gate和受影响原consumer按其已注册whole/exact执行，特别Core worker crash、Core concurrency、Node publication、spillback/finishbarrier与protocol深篡改合同。普通/含Ref真实接线沿既有32smoke及受审迁移项，串行不目录collection。

本包没有执行pytest、collection、test body或集群，只有静态AST/输入hash/精确展开与patch。此前旧singletonB试做的1820→643B等数字不转贴为当前P1/P4性能结果；当前scalar版本需要独立测量才可报告成本。
