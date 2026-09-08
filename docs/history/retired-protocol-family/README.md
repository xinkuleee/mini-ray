# 已退役 publication 协议族测试

本目录保存旧生命周期/线协议的逐字 .py.txt 文档副本。它们不是 skip/xfail 活动测试，不参与 pytest 收集、当前 gate 或通过计数。源文件归档前后的 SHA-256 一致；归档可恢复原文，但不能靠恢复旧运行时 alias、flag 或旧结果字段来冒充统一后端验收。

先归档下列十三个专用旧族测试，再在共享graph/source合同获得明确活动映射后归档 test_stored_publication.py；另一个实施任务保存了 test_inline_recovery.py 原文并将其活动文件迁为统一合同，见后文。测试名包含 stored/inline 不构成归档依据：当前统一 NodeServer、Worker、Core adoption、owner-death control、Node-death GC、shared-source/pin 和 integration 测试仍保持活动。十四份旧生命周期源码已逐字移至[source](source)，其中stored_publication.py的活动文件仅保留共享类型的历史pickle导出，不再实现旧journal/recovery。

## 原文与完整性

| 原活动文件（tests/unit/ 下） | 非执行副本 | SHA-256 |
|---|---|---|
| test_stored_contained_transaction.py | [test_stored_contained_transaction.py.txt](test_stored_contained_transaction.py.txt) | 61ed360d61da0752b98f19e9478adc5ce93da1526aa4ea8dd40b61bf4ab167f5 |
| test_stored_publication_node.py | [test_stored_publication_node.py.txt](test_stored_publication_node.py.txt) | 14584438d4e2ee9dabb53c2fb4f837a19f812465498ecb4abaaa11adf1d72359 |
| test_stored_publication_owner.py | [test_stored_publication_owner.py.txt](test_stored_publication_owner.py.txt) | ccdfdceabf245cbd9252817e7a07201c3b8cf4a283cbeca0f8a959bcc0bda0f5 |
| test_stored_node_loss.py | [test_stored_node_loss.py.txt](test_stored_node_loss.py.txt) | 28982f6b93c738a8e1646e8982d6dbec407ff0d54f6bca2387c836f0243e19a2 |
| test_inline_contained_publication.py | [test_inline_contained_publication.py.txt](test_inline_contained_publication.py.txt) | 6c9f39cf70ff278728483af4524b1820d4e5f2391dcedc6fdbecd1a56f1d32f9 |
| test_inline_contained_node.py | [test_inline_contained_node.py.txt](test_inline_contained_node.py.txt) | 7ec5b7990c433e211e252bf5d5ea0f7da2efcd87093eef5841c668340020fb0d |
| test_inline_contained_runtime.py | [test_inline_contained_runtime.py.txt](test_inline_contained_runtime.py.txt) | dacb58493fc6f62f3b0cf0b3ee7851fece556594345d13b2c9d18eef77be0a33 |
| test_inline_contained_protocol.py | [test_inline_contained_protocol.py.txt](test_inline_contained_protocol.py.txt) | 6e4b4f88a160a11acada7996fb7f64126e9cbb29eb118bdfe4a35582dea41d96 |
| test_inline_recovery_runtime.py | [test_inline_recovery_runtime.py.txt](test_inline_recovery_runtime.py.txt) | 253fb49f4a829bfc9199ff768e4ade5f4b19b1bb65fc7b3600a23448eb5dc35b |
| test_inline_recovery_protocol.py | [test_inline_recovery_protocol.py.txt](test_inline_recovery_protocol.py.txt) | 8dec4a21f78423fd7437c2d3b2e49d73161ca62088e8b0c82064ad7f750c5019 |
| test_publication_owner_death.py | [test_publication_owner_death.py.txt](test_publication_owner_death.py.txt) | 76677021064358a96b172a33c434ef61bdff3a19aee3728bca2dece04394da87 |
| test_publication_owner_death_runtime.py | [test_publication_owner_death_runtime.py.txt](test_publication_owner_death_runtime.py.txt) | f3d2ecbaa01b0b4b6974611ac8f91ecebd5cce53c991286416f1cec6e38ff2cf |
| test_publication_owner_death_arbitration.py | [test_publication_owner_death_arbitration.py.txt](test_publication_owner_death_arbitration.py.txt) | 2077522ea04f3c3a08af6a0e37a49f32e708bd44126fa056a3819dc0e32b4fc9 |
| test_stored_publication.py | [test_stored_publication.py.txt](test_stored_publication.py.txt) | 7bf2a01ad1269e2ea3587fa068f830e14e2785d0d2da5bf6513ed4c31d780a33 |

## 当前合同映射

“对应”表示已读到相关活动断言，不表示所有旧case、callback顺序或真实竞争逐项等价。分层覆盖与尚缺组合必须一起阅读。新用例落盘不是通过声明；实际执行记录见[当前状态](../../current-status.md)和[测试策略](../../testing.md)，本页不复制易漂移的通过数。

| 旧文件/核心合同 | 当前活动对应 | 迁移边界与仍需验证的部分 |
|---|---|---|
| test_stored_contained_transaction.py：owned/foreign prepare、seal前发现闭合、partial effects逆向补偿、ACK精确重放 | [test_output_discovery.py](../../../tests/unit/test_output_discovery.py)、[test_output_publication_journal.py](../../../tests/unit/test_output_publication_journal.py) 的 test_partial_materialization_rollback_covers_unacknowledged_effects_reverse / test_partial_promotion_rollback_releases_possible_final_before_provisional；[test_output_publication_node.py](../../../tests/unit/test_output_publication_node.py) 的 test_precomplete_rollback_compensates_intended_effects_without_ack | 单 execution 批发现、Node journal及统一 owner CAS替代旧三段事务；旧edge-commit ACK未知即可回滚不应套到成功Complete之后。 |
| test_stored_publication_node.py：intent-before-effect、prepare/promotion重放、local Complete、rollback、shutdown custody、坏ACK | [test_output_publication_node.py](../../../tests/unit/test_output_publication_node.py) 的 test_effect_then_lost_ack_replays_the_exact_frozen_publication / test_terminal_failure_does_not_hold_cpu_or_repeat_complete / test_rollback_ack_loss_retains_same_effect_and_resumes；[test_stored_publication_node_server.py](../../../tests/unit/test_stored_publication_node_server.py) 已是统一真实Node handlers | GCS副本报告或terminal本身不再提供payload；local Complete/witness与异步outbox、真实本地bytes/descriptor分开。旧claim与再次promotion不是独立必要phase。 |
| test_stored_publication_owner.py：graph/owner/wake顺序、单effect重放、错owner拒绝、reverse GC | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py) 的 test_adoption_orders_terminal_graph_atomic_owner_ready_and_metadata_acks / test_effect_then_lost_ack_replays_exact_batch_without_second_owner_cas / test_reverse_gc_ack_loss_retains_exact_obligation_and_skips_finished_effects | 现存同名stored文件是统一Core测试，不应归档。owner death与正在进行的Core adoption/GC竞争仍见G3，不以组件测试宣称全覆盖。 |
| test_stored_node_loss.py：pre/post-Complete、exact death/descriptor、child死亡证明、清理顺序与终态 | [test_output_recovery.py](../../../tests/unit/test_output_recovery.py)、[test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py) 的 test_typed_node_loss_routes_preserve_committed_graph_and_owner_witness_choice；[test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py)、[test_stored_publication_node_death_gc.py](../../../tests/unit/test_stored_publication_node_death_gc.py) | 新known/UNKNOWN与per-slot KEEP/DROP替代旧模式；fully adopted对象按引用寿命GC，不恢复旧立即owner-retirement顺序。child-owner死亡替代ACK的每个组合待G3。 |
| test_inline_contained_publication.py：identity/digest、pin bitmap、commit boundary、rollback、collection、并发first-application | [test_output_publication.py](../../../tests/unit/test_output_publication.py)、[test_output_publication_journal.py](../../../tests/unit/test_output_publication_journal.py) 的 test_intent_prepare_graph_materialize_promote_and_arm_are_ordered_gates / test_complete_fences_rollback_and_every_forward_effect / test_whole_owner_adoption_clears_all_slots_and_retains_only_exact_metadata | INLINE/STORED不再两个ID域；单槽是批的特例。旧八线程first-application测试不由pure replay等价替代，见G2。 |
| test_inline_contained_node.py：journal绑定、borrowed credentials、prepare歧义、Worker loss与Complete、lateACK | [test_output_publication_node_server.py](../../../tests/unit/test_output_publication_node_server.py)、[test_inline_publication_node_server.py](../../../tests/unit/test_inline_publication_node_server.py)、[test_output_publication_node.py](../../../tests/unit/test_output_publication_node.py) | 后两个历史名文件目前测试统一后端；不能再用Node intent metadata凭空恢复未接收INLINE bytes。 |
| test_inline_contained_runtime.py：graph/pin安装/owner adoption协调、rollback中断、错误回执与rebind | [test_output_publication_node.py](../../../tests/unit/test_output_publication_node.py) 的 test_wrong_child_echo_is_not_acknowledged_and_is_compensated；[test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py) 的 test_rebound_remote_ack_preserves_obligation_before_following_effect | 共享prepare/promote pin原语替代InlineInstall facade；新成功主链不需要旧独立INLINE协调器。 |
| test_inline_contained_protocol.py：旧OPEN/PREPARE/InlineInstall回显、envelope跨terminal wire、pickle深校验 | [test_output_protocol.py](../../../tests/unit/test_output_protocol.py) 的 test_mixed_unified_envelopes_work_on_all_terminal_boundaries / test_terminal_replies_reject_incompatible_authority_and_deep_envelope_mutation / test_terminal_envelopes_bind_executor_attempt_full_and_selected_manifest；[test_publication_sources.py](../../../tests/unit/test_publication_sources.py) 的shared pin/source wire | TaskReply/Complete/outcome旧inline_publication、stored_publication字段应不存在；共享PrepareStoredContainedPin/Promote/Reply仍是活跃原语，不能删除。 |
| test_inline_recovery_runtime.py：reverse cleanup、child/graph/authorityACK丢失、UNKNOWN选择、owner supersession、metadata-only | [test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py) 的 test_cleanup_revalidates_child_ack_before_retiring_an_obligation / test_typed_node_loss_routes_preserve_committed_graph_and_owner_witness_choice；[test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py) 的 test_no_envelope_cleanup_ack_loss_fences_local_state_until_exact_replay；[test_output_recovery.py](../../../tests/unit/test_output_recovery.py) | 已验证合同的存在不能覆盖owner death在childACK后/graph选择后接管的全部时序；旧runtime自有receipt账本不恢复，G3仍开放。 |
| test_inline_recovery_protocol.py：intent/work/decision/terminal深绑定、borrowed-source验证、错误reply不能授权 | [test_output_protocol.py](../../../tests/unit/test_output_protocol.py) 的 test_recovery_rejects_stage_digest_manifest_and_proof_rebinding / test_prepare_and_recovery_deep_revalidate_tampered_values_on_roundtrip / test_metadata_completion_rejects_changed_full_target_manifest_and_nested_tampering；[test_output_publication.py](../../../tests/unit/test_output_publication.py) 的source fingerprint/deep validation | 新wire以全batch与per-slot vector为身份；旧response shape/enums不作API兼容保证。没有声明所有Node-loss endpoint畸变组合穷尽。 |
| test_publication_owner_death.py：metadata-only request、Node fences、child/graph/replica次序、adopted历史不被改写、exact终态 | [test_output_recovery.py](../../../tests/unit/test_output_recovery.py) 的 test_owner_death_is_orthogonal_and_never_rewrites_node_or_owner_work；[test_publication_owner_death_control.py](../../../tests/unit/test_publication_owner_death_control.py)、[test_output_owner_death_node.py](../../../tests/unit/test_output_owner_death_node.py) | 统一registry加owner-wide fences替代旧双tier owner-death saga；预留/已采用/Node loss交叉仍不是全矩阵。 |
| test_publication_owner_death_runtime.py：一次一effect、PINNED重试、dead Node/child证据替代RPC、坏ACK/identity | [test_publication_owner_death_control.py](../../../tests/unit/test_publication_owner_death_control.py) 的 test_exact_child_ack_loss_and_invalid_finalize_ack_keep_cleanup_replayable / test_explicit_progress_filters_owner_and_global_drain_converges；[test_node_owner_death_fence.py](../../../tests/unit/test_node_owner_death_fence.py)；[test_output_owner_death_node.py](../../../tests/unit/test_output_owner_death_node.py) | 共享fence/physical inventory已有活动合同；跨多个replica节点、child-owner death与正常adoption竞争的组合必须单独验收。 |
| test_publication_owner_death_arbitration.py：freeze dominates、exact claim、导入旧Node-loss收据、收尾与closed admission | [test_output_recovery.py](../../../tests/unit/test_output_recovery.py) 的 test_owner_death_is_orthogonal_and_never_rewrites_node_or_owner_work / test_owner_death_after_mixed_decision_preserves_vector_and_fences_replay_authorization；[test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py) | 一个metadata authority取代独立跨saga arbitrator；不保留旧tier key/receipt-import API，但必须保留新接管顺序与幂等清理，见G3。 |
| test_stored_publication.py：旧journal/recovery状态机及共享source、graph manifest/receipt/admission合同 | 旧lifecycle映射至 output journal/recovery；[test_publication_sources.py](../../../tests/unit/test_publication_sources.py) 保留共享binding；[test_contained_graph_protocol.py](../../../tests/unit/test_contained_graph_protocol.py) 保留generic exact manifest/replay；[test_contained_graph_manifest_boundaries.py](../../../tests/unit/test_contained_graph_manifest_boundaries.py) 保留full-manifest防降级、unseen-ABORT墓碑、forged receipt与closed admission清理 | 共享合同已从旧模块fixture拆出，新测试尚以实际gate结果为准。旧并发case不可用顺序测试冒充，见G2。 |

## 两份特殊文件的分别处理

这两份经独立分析处理，未按旧文件名直接删除全部安全合同：

- [test_stored_publication.py 原文](test_stored_publication.py.txt)：现已逐字归档并移除活动源。共享BorrowedContainedSource绑定对应test_publication_sources；完整graph manifest exact replay对应generic graph测试；原581/682/630/652的防降级、unseen-ABORT、伪造receipt、closed-admission合同均迁入G1的新pure文件。旧lifecycle/真实竞争与共享合同分开映射，不宣称整个旧文件逐case覆盖相等。
- [test_inline_recovery.py](../../../tests/unit/test_inline_recovery.py)：已由独立任务迁为 OutputPublicationRecoveryAuthority。活动纯合同保留完整intent/exact replay，以及 owner-first/node-first 冻结 work 不变；test_owner_death_and_intent_admission_linearize_atomically 和 test_owner_keep_drop_race_has_one_winner_and_no_payload_in_authority 保留原 exact ID，分别标为L1，使用两个请求线程、有超时barrier和共享join/cleanup deadline，无socket/子进程。[旧原文](test_inline_recovery.py.txt) 的 SHA-256 已核对为 78ece984945f364eb24c4d58337c42667e11c50b49fb7e4f9ea2df0fd9417a65。此迁移不证明其它旧registry case逐项等价，也不由本次文档写入宣称测试通过。

不再保留依赖旧authority的特殊活动源；唯一保留历史文件名的 test_inline_recovery.py 现已使用统一authority并保留精确L1入口。活动import图与整个故障矩阵是否完成仍须由实际安全回归核实。

## 三项显式缺口及当前状态

### G1：共享graph/source合同的单独保留

已新增 [test_contained_graph_manifest_boundaries.py](../../../tests/unit/test_contained_graph_manifest_boundaries.py)，使用真实统一full/targeted manifest与ContainedReferenceGraphAuthority：

- test_full_manifest_cannot_be_downgraded_or_released_through_edge_only_apis：edge-only prepare/commit/abort/publish 必须抛身份冲突，不以False弱化；ObjectID-only release_container返回空tuple且不动完整manifest；精确per-container release及其重放保留另一槽，再最终收敛。
- test_unseen_full_manifest_abort_tombstones_exact_identity_and_fences_late_prepare：从未见过的完整manifest ABORT必须创建权威墓碑、精确重放ALREADY_ABORTED；迟到prepare与改digest的rebound都明确拒绝，终态不变。
- test_manifest_receipt_rejects_forged_release_state_and_foreign_edge：空RELEASE证明、PREPARED状态配ALREADY_COMMITTED、foreign edge证明必须拒绝；合法单container的有序receipt仍可构造。
- test_graph_admission_close_preserves_prepared_and_committed_cleanup_replays：关闭新准入后，已有prepared/committed exact replay有效，unseen prepare被拒绝，prepared不能提前release；精确abort/release/replay最终清理完所有存活边。

状态：以上共享合同、source binding和generic manifest均已纳入root实际reviewed选择并通过；两个保留L1也已分别有界运行。旧生命周期测试已从活动import图移除，但完整历史合同等价性仍未证明，不能凭归档完成声明覆盖相等。

### G2：真实竞争与测试安全分类

归档的 test_inline_contained_publication.py 包含原八线程、多个并发调用的first-application case；同样已归档的test_stored_publication.py还有并发intent和无超时Barrier的owner-death/admission case。它们的历史unit marker不等于安全pure。新顺序replay/冻结状态机测试不证明真实互斥与一次获胜。

状态：两个INLINE recovery L1已保留ID并迁为统一的真实双线程intent/owner-death与KEEP-DROP竞争，不能再写成完全没有此类活动测试。真实graph COMMIT/Node-death等其它竞争及旧多并发exact-intent/prepare“一次获胜”合同仍需分别核查到严格有界L1，不由顺序测试代替；不运行归档代码，也不恢复八线程或无超时等待。实际L1通过记录由root维护，完整taxonomy/同版回归不因本次归档完成。

### G3：owner-death接管Node-loss正在清理的工作

旧inline recovery runtime的test_owner_death_supersession_fences_new_or_retained_cleanup检查清理前、childACK后、graph选择后被owner death接管。旧Node-loss/owner-death runtime还检查child-owner死亡必须证明精确owner，EXPECTED exit不能替代ACK。新registry、owner-wide fence、Node/Worker finalize和typed ACK已有分层合同，但这些交叉不声明全部等价覆盖。

状态：仍需小型同步故障组合及审查后的L1补证。建议限定一个两槽batch、固定child holds、一次确定death切入、显式有限步推进，检查旧driver停止、新cleanup接管且不双释放/漏释放；错误或EXPECTED death不能让缺失ACK消失。不能用恢复旧arbitrator API来代替统一机制的正确性。

## 不变目标与替代边界

当前成功路径只有统一discovery/Prepare/Node journal/ARM/local Complete/Core batch CAS；storage tier只决定物化。GCS metadata不含结果bytes；Node local Complete不等待GCS terminal ACK；成功Complete之后不得回滚。旧“GCS回执制造成功envelope”、双tier轮询、独立claim/promotions replay和InlineInstall facade均不是必须保留的生产接口。

归档是保存历史证据，不是删除用户要求的安全语义，也不关闭K0/K1目标。最终仍须结合[路线图](../../roadmap.md)、当前活跃测试的实际结果及未闭合故障矩阵评审。
