# 已退役 Core publication 测试的历史映射

本目录保存四份原测试源码的非执行历史副本：

- [test_core_inline_publication.py.txt](test_core_inline_publication.py.txt)
- [test_inline_data_plane_fullflow.py.txt](test_inline_data_plane_fullflow.py.txt)
- [test_core_stored_node_loss_takeover.py.txt](test_core_stored_node_loss_takeover.py.txt)
- [test_core_stored_node_loss_fullflow.py.txt](test_core_stored_node_loss_fullflow.py.txt)

这些是文档资源，不是被 skip/xfail 掩盖的活动测试，不参与 pytest 收集、任何当前 gate 或通过数。它们引用已退役的 Core 私有类型，不能通过重新命名为 .py、恢复旧生产开关或增加兼容分支来充当当前验收。历史函数名、内部 phase 与返回值也不是 API 兼容承诺。

归档是职责/协议迁移，不是宣布旧安全合同全部完成。下面只描述已核查的当前测试断言，并把直接覆盖、分层证据和未组合窗口分开。此文档写入过程没有运行测试或 import；实际验证记录以[当前状态](../../current-status.md)和[测试策略](../../testing.md)为准。局部记录不构成同版全量回归，K0/K1 的[出口条件](../../roadmap.md)保持未完成。

## 阅读约定

- **直接**：当前活动测试明确检查同一项安全不变量；不表示所有旧内部断言或故障组合逐字相同。
- **分层/部分**：组件或相邻时序已有断言，但未在同一条 Core→Node/GCS→owner/GC 链上合成全部旧场景。
- **替代**：旧机制已不存在，保留的是下面列出的新语义，不能恢复旧协议以迎合历史测试。
- **待补组合**：当前可执行选择集中没有找到该精确交错；末节 R-A/R-B/R-C 给出范围受限的后续方向。

旧用例名省略共同的 test_ 前缀；行号指向冻结的 .py.txt 内容。A/C/N/R 等编号是本页的测试索引，不是新的测试 gate。表中“现有覆盖”和“待验/替代边界”必须一起阅读。

## 必须保留的语义，不必保留的旧机制

1. **一个候选身份替代 INLINE→STORED fallback。** 当前普通 Task 使用 OutputPublicationID(LeaseID, execution)，包括完整/targeted 返回集合。pre-Push candidate 只是身份，不提前创建 GCS intent；同一冻结域的精确 absence 或已确认清理才可授权预算内 retry。已知 Complete 不能被 ABSENT/pre-Complete 矛盾回复降级。旧双 candidate、owner-only driver 和跨 tier 轮询不再是验收目标。
2. **READY、成功 reply 缓存与 finish ACK 是不同边界。** Worker 成功 TaskReply 必须有真实 Node Complete witness；不能恢复旧“先缓存成功、再等 Complete ACK”的捷径。Core 在 graph ACK 和 owner batch CAS 后可唤醒读者，再等待 GCS adopted/Node payload-retirement ACK；在尾部 ACK 收敛前 finish barrier 仍阻止逻辑清账。旧 ACK-before-wake 断言要替换为“只 CAS 一次、保留义务/holds、不得提前 finish”，不是把 READY 可见性回退。
3. **fully adopted Node loss 使用逐槽生命期。** INLINE 已收到的数据可继续保留；丢失 STORED replica 标为 LOST。正常 GC/选定槽 retirement 依据各槽 refs、graph 和 canonical integrity metadata 清理；不为了复制旧 fullflow 去重建一个立即扫除所有 adopted pins 的 synthetic takeover driver。缺完整 descriptor 的普通 LOST source 返回明确错误，而非假装安全收集。
4. **旧工作完成返回值不是释放 successor 的许可。** 旧 _execute 可以表示“此物理工作已无需继续”，但实际 _finish_pending_task 必须校验当前 attempt/hold/barrier。whole-task SYSTEM retry 保留逻辑 hold，lineage reconstruction 建立新 hold；不能直接搬用旧布尔返回值断言绕过该校验。

## 当前可执行合同索引

以下链接均相对项目路径，可随仓库一起移动。部分文件保留 stored 等历史名字，但内容已改为统一 output 后端；名字不表示执行旧协议。

| 编号 | 当前活动用例 | 实际覆盖范围 |
|---|---|---|
| A1 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L131) · test_adoption_orders_terminal_graph_atomic_owner_ready_and_metadata_acks | terminal/graph → owner CAS → READY → adopted/Node retirement ACK |
| A2 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L177) · test_effect_then_lost_ack_replays_exact_batch_without_second_owner_cas | terminal、graph、owner、wake、adopted、Node retirement 的 effect-then-error |
| A3 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L237) · test_rebound_remote_ack_preserves_obligation_before_following_effect | 正常 adoption 各 RPC 的错配/畸变 ACK 不放行后续 effect |
| A4 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L273) · test_success_envelope_identity_is_checked_before_any_owner_or_gcs_effect | lease/owner/job/executor/Node 身份不符时零发布 |
| A5 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L304) · test_live_executor_does_not_mask_completed_output_outcome | Worker 仍存活不掩盖已完成的 outcome；不重新执行 |
| A6 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L325) · test_first_terminal_report_observes_continuous_push_and_finish_fences | Push 到 adoption 的统一 candidate/finish fence 连续性 |
| A7 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L349) · test_attempt_replaced_during_graph_ack_never_overwrites_successor | graph ACK 返回时 attempt 已前进，旧结果不 CAS/wake |
| A8 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L388) · test_publisher_death_after_graph_ack_keeps_exact_output_takeover | graph ACK 后 Node death 保留 envelope/义务，不走正常 CAS |
| A9 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L423) · test_reverse_gc_orders_children_graph_drop_report_and_metadata_without_touching_sibling | 逐槽 child → graph → replica → report → metadata，保留 sibling |
| A10 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L462) · test_reverse_gc_ack_loss_retains_exact_obligation_and_skips_finished_effects | child/graph-release/drop/collected/metadata 边界失败的精确重放 |
| A11 | [test_core_stored_publication_adoption.py](../../../tests/unit/test_core_stored_publication_adoption.py#L544) · test_release_graph_wrong_ack_cannot_admit_replica_drop | 错误 graph RELEASE ACK 不允许删副本 |
| C1 | [test_core_output_publication.py](../../../tests/unit/test_core_output_publication.py#L89) · test_core_adopts_all_slots_and_gc_releases_only_each_slots_children | 真实 mixed batch adoption/逐槽 GC 的正常闭环 |
| C2 | [test_core_output_publication.py](../../../tests/unit/test_core_output_publication.py#L177) · test_metadata_only_successful_outcome_never_falls_back_to_system_retry | 成功 witness 从 custody/owner slots 找 bytes；没有则保持 unresolved |
| N1 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L145) · test_no_envelope_cleanup_ack_loss_fences_local_state_until_exact_replay | known/UNKNOWN 最终 cleanup ACK 丢失，观察 ACK 后才 LOST 或 retry |
| N2 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L246) · test_old_delayed_completion_cannot_release_current_attempt_input_hold | 真实 input hold；known 重建换 incarnation，SYSTEM retry 保留 hold；旧 finalizer 无权释放 |
| N3 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L319) · test_envelope_arriving_during_query_is_seen_before_immutable_keep_drop | 查询中到达 bytes 必须先保留再选择 KEEP/DROP |
| N4 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L349) · test_late_envelope_cannot_reverse_a_latched_drop | DROP 锁定后到达 envelope 不得反转决定 |
| N5 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L376) · test_adoption_rpc_error_after_other_lane_finished_cannot_reinsert_old_work | 另一 lane 已完成后旧 RPC 异常不能重新插入工作 |
| N6 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L405) · test_task_attempt_fences_old_target_receipt_even_when_its_slots_are_unchanged | task attempt 而非仅 selected slot epoch 阻止旧 target receipt |
| N7 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L443) · test_new_sibling_attempt_does_not_abandon_committed_batch_adoption_tail | 新 sibling attempt 已开始时，旧已提交 batch 的 metadata 尾部仍收敛 |
| R1 | [test_core_output_receipt_loss.py](../../../tests/unit/test_core_output_receipt_loss.py#L30) · test_owner_cas_receipt_restores_inline_custody_for_node_loss_after_scratch_cache_disappears | scratch custody/marker 丢失后从真实 owner receipt/data 恢复 INLINE KEEP |
| R2 | [test_core_output_receipt_loss.py](../../../tests/unit/test_core_output_receipt_loss.py#L91) · test_known_complete_cannot_be_erased_by_absent_or_precomplete_recovery_reply | 已知 Complete 与 ABSENT/pre-Complete 矛盾时保留义务，禁止 retry |
| R3 | [test_core_output_receipt_loss.py](../../../tests/unit/test_core_output_receipt_loss.py#L124) · test_node_loss_owner_cas_effect_then_error_reuses_resolution_without_second_cleanup | Node-loss owner CAS 已生效后异常，重放同 resolution 不二次 cleanup |
| R4 | [test_core_output_receipt_loss.py](../../../tests/unit/test_core_output_receipt_loss.py#L153) · test_owner_retirement_is_an_independent_shutdown_fence_without_protocol_marker | 独立 owner retirement 在 protocol marker 缺失时仍阻止 shutdown |
| R5 | [test_core_output_receipt_loss.py](../../../tests/unit/test_core_output_receipt_loss.py#L175) · test_owner_routed_targeted_retirement_never_holds_core_lock_across_rpc | targeted retirement 不持 Core lock 跨 RPC，保留健康 sibling |
| L1 | [test_core_output_lease_domain.py](../../../tests/unit/test_core_output_lease_domain.py#L105) · test_pre_push_loss_queries_one_output_domain_before_budgeted_retry | lease/grant/location/cancel 使用一个 OutputPublicationID domain 查询 |
| L2 | [test_core_output_lease_domain.py](../../../tests/unit/test_core_output_lease_domain.py#L110) · test_targeted_pre_push_loss_preserves_exact_subset_and_healthy_siblings | 相同 pre-Push 合同保留 full manifest 与非连续 targets |
| G1 | [test_stored_publication_node_death_gc.py](../../../tests/unit/test_stored_publication_node_death_gc.py#L69) · test_adopted_stored_outer_collects_graph_after_node_death | 已 fully adopted 后 Node loss，正常逐槽 GC，graph/report ACK 丢失与 lineage 收敛 |
| G2 | [test_stored_publication_node_death_gc.py](../../../tests/unit/test_stored_publication_node_death_gc.py#L228) · test_plain_lost_object_without_descriptor_is_not_collected | 缺完整 integrity metadata 的普通 LOST source 明确拒绝不安全 collection |
| D1 | [test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py#L26) · test_frozen_output_work_cleans_exact_child_vector_and_preserves_kept_graph | intent/armed/complete 的真实 GCS child/graph cleanup vector |
| D2 | [test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py#L83) · test_owner_applies_resolved_loss_without_fabricating_payload_or_losing_incoming_refs | known/UNKNOWN owner reducer 保留 incoming holds/lineage，不凭 metadata 造 bytes |
| D3 | [test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py#L225) · test_cleanup_revalidates_child_ack_before_retiring_an_obligation | Node/owner death 的 child ACK subclass/flag/hold/rejection 校验 |
| D4 | [test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py#L144) · test_owner_death_cleanup_is_driven_after_owner_wide_fences | owner-wide fence 先于 publication owner-death cleanup |
| D5 | [test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py#L279) · test_owner_finalization_revalidates_cleaned_flag_and_replays_without_child_releases | 错误 cleaned flag 保留末端义务，不重复 child release |
| P1 | [test_output_publication_node_server.py](../../../tests/unit/test_output_publication_node_server.py#L81) · test_handlers_complete_mixed_selected_outputs_locally_without_terminal_rpc | 真实 Node handlers 本地 Complete/ledger 与 terminal outbox 分离 |
| P2 | [test_output_publication_node.py](../../../tests/unit/test_output_publication_node.py#L269) · test_terminal_failure_does_not_hold_cpu_or_repeat_complete | terminal report 失败不占 CPU、不二次 Complete |
| P3 | [test_output_publication_node.py](../../../tests/unit/test_output_publication_node.py#L287) · test_owner_adoption_can_precede_terminal_outbox_without_reopening_forward_work | owner adoption 可先于 Node outbox ACK，不重开 forward effects |
| T1 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L118) · test_retirement_gates_publish_advance_and_gc_but_not_reference_release | active retirement 屏障与正常引用 release 分开 |
| T2 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L272) · test_retired_slot_can_reconstruct_with_original_index_and_old_replays_are_fenced | 旧 membership retirement 后才替换原 return index |
| U1 | [test_output_recovery.py](../../../tests/unit/test_output_recovery.py#L421) · test_unknown_complete_owner_decision_is_per_slot_and_does_not_upgrade_frozen_history | UNKNOWN 有本地 witness 可逐槽决定，但不改冻结历史 |
| U2 | [test_output_recovery.py](../../../tests/unit/test_output_recovery.py#L501) · test_owner_death_is_orthogonal_and_never_rewrites_node_or_owner_work | 两种 death 的先后序及冻结 work 不重写 |
| U3 | [test_output_recovery.py](../../../tests/unit/test_output_recovery.py#L600) · test_payload_envelopes_and_descriptor_objects_never_enter_metadata_registry | recovery authority 拒绝 payload/envelope |
| O1 | [test_output_owner_death_node.py](../../../tests/unit/test_output_owner_death_node.py#L65) · test_owner_finalize_retires_exact_node_and_worker_custody_once | running/partial/complete Node/Worker owner cleanup |
| O2 | [test_output_owner_death_node.py](../../../tests/unit/test_output_owner_death_node.py#L103) · test_worker_cleanup_ack_loss_retains_node_payload_and_replays_exact_request | Worker cleanup ACK 丢失，Node 保留数据与 exact replay |

## test_core_inline_publication.py.txt

| 旧用例（冻结行号） | 保留的安全合同 | 当前可执行覆盖 | 待验或替代边界 |
|---|---|---|---|
| 231 · publish_reply_commits_graph_then_atomic_owner_then_wakes | graph 已确认、owner 原子发布后才可见 | 直接：[A1](../../../tests/unit/test_core_stored_publication_adoption.py#L131)、[C1](../../../tests/unit/test_core_output_publication.py#L89) | 旧 ACK-before-wake 顺序已被当前 READY/finish 分离替代，见下节。 |
| 277 · publish_reply_rejects_inline_envelope_from_another_lease | 错误 lease 不得有 owner/GCS effect | 直接：[A4](../../../tests/unit/test_core_stored_publication_adoption.py#L273) | 当前还检查 owner/job/executor/Node；不保留旧 envelope 类型。 |
| 297 · lost_graph_commit_ack_retains_exact_obligation_and_replays | graph COMMIT effect-then-ACK-loss 保留原义务且只 CAS 一次 | 直接：[A2](../../../tests/unit/test_core_stored_publication_adoption.py#L177) | lost_effect=graph；此缺口已经补入，不再列待实现。 |
| 353 · node_death_cannot_discard_completed_inline_adoption | Node death 切换恢复时不丢实际 Complete 数据 | 直接/分层：[A8](../../../tests/unit/test_core_stored_publication_adoption.py#L388)、[N3](../../../tests/unit/test_core_output_node_loss.py#L319) | 保留统一 takeover/custody，不要求旧 obligation 类。 |
| 382 · completed_outcome_envelope_recovers_without_push_reexecution | outcome 成功不重新 Push；死亡消息与交付相交仍保留 bytes | 分层：[A5](../../../tests/unit/test_core_stored_publication_adoption.py#L304)、[C2](../../../tests/unit/test_core_output_publication.py#L177)、[N3](../../../tests/unit/test_core_output_node_loss.py#L319) | outcome 返回瞬间 Node death 的完整交错仍属余项 R-C。 |
| 478 · inline_reverse_gc_orders_child_graph_metadata_without_replica_drop | 逐槽 GC 必须先释放 child/graph；INLINE 不需要 replica drop | 分层：[C1](../../../tests/unit/test_core_output_publication.py#L89)、[A9](../../../tests/unit/test_core_stored_publication_adoption.py#L423)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L69) | mixed 正常 GC 与 dead-Node GC 有覆盖，不宣称复制旧纯 INLINE 全时序。 |
| 530 · lost_graph_release_ack_retains_collection_until_exact_replay | graph RELEASE ACK 丢失保留 metadata，不重复已 ACK child release | 直接/分层：[A10](../../../tests/unit/test_core_stored_publication_adoption.py#L462)、[A11](../../../tests/unit/test_core_stored_publication_adoption.py#L544)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L69) | 当前直接故障覆盖 STORED 槽及 dead-Node GC；共用 collection 机制，不冒充全 tier 组合。 |
| 595 · normal_adoption_ack_loss_replays_only_ack_before_ready | 末端 ACK 未收敛不能 finish；不重复 owner CAS | 直接＋替代：[A1](../../../tests/unit/test_core_stored_publication_adoption.py#L131)、[A2](../../../tests/unit/test_core_stored_publication_adoption.py#L177) | 当前可 READY 后等待 adopted/Node ACK；不能恢复旧等待 wake 规则。 |
| 657 · duplicate_task_reply_during_owner_ready_adoption_ack_wait_preserves_pins | 重复交付不清理仍有效 pins，不丢 adoption 尾部 | 分层：[A2](../../../tests/unit/test_core_stored_publication_adoption.py#L177)、[N7](../../../tests/unit/test_core_output_node_loss.py#L443) | 保留原始 TaskReply 二次到达与真实 child pin 的交叉余项 R-C。 |
| 732 · inline_node_loss_pending_drives_gcs_rollback_then_system_retry | cleanup 明确完成前不 retry | 分层：[D1](../../../tests/unit/test_output_node_loss_control.py#L26)、[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[L1](../../../tests/unit/test_core_output_lease_domain.py#L105) | control 含 pre-Complete child 清理；Core 无 envelope 含 ARM/known，组合不等同旧后端。 |
| 777 · inline_node_loss_local_complete_envelope_keeps_before_adopt_and_wake | known/UNKNOWN 均不得丢已收到的真实 Complete envelope | 分层：[N3](../../../tests/unit/test_core_output_node_loss.py#L319)、[R1](../../../tests/unit/test_core_output_receipt_loss.py#L30)、[U1](../../../tests/unit/test_output_recovery.py#L421) | UNKNOWN＋真实 custody＋contained cleanup 的 Core 全链仍属 R-C。 |
| 851 · busy_inline_driver_retains_received_bytes_without_replacing_work | busy lane 不能丢交付 bytes；原义务不能被竞争 lane 随意覆盖 | 分层：[N3](../../../tests/unit/test_core_output_node_loss.py#L319)、[N5](../../../tests/unit/test_core_output_node_loss.py#L376) | 已有查询中交付；全部 marker/queue/ticket 组合未逐项等价，见 R-C。 |
| 928 · inline_driver_same_thread_normal_to_node_loss_handoff_is_reentrant | 正常 adoption→Node loss 切换不死锁、不重复 effect | 替代/分层：[N3](../../../tests/unit/test_core_output_node_loss.py#L319)、[A8](../../../tests/unit/test_core_stored_publication_adoption.py#L388) | 旧线程 ID/depth 实现已退休；不把其内部计数当作新 API 合同。 |
| 969 · inline_driver_reentrant_exception_releases_all_ticket_depths | 异常退出释放运行票据，仍保留实际 pending bytes/义务 | 待补组合：[N5](../../../tests/unit/test_core_output_node_loss.py#L376) | 当前已测旧 RPC 异常不重插工作；Core BaseException 中断票据后重新进入尚需 R-C。 |
| 1004 · inline_node_loss_without_local_bytes_cleans_before_lost_or_retry | known→LOST/显式重建；UNKNOWN→清理后预算 retry，绝不造 bytes | 直接/分层：[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[D2](../../../tests/unit/test_output_node_loss_control.py#L83) | N1 是真实 Core/recovery、无 child refs；child 效果另由 D1 覆盖。 |
| 1106 · unknown_cleanup_old_delayed_execute_preserves_new_attempt_and_holds | 旧 delayed work 不覆盖新 attempt 或释放其 submitted hold | 直接：[N2](../../../tests/unit/test_core_output_node_loss.py#L246) | 覆盖真实 SYSTEM retry hold 与原 accepted count；旧 _execute bool 不作 API。 |
| 1196 · known_lost_finalizer_barrier_and_old_finish_fence_after_get_reconstruction | known LOST 的 finalizer 先清旧 hold，再许可重建；旧 finalizer 不碰新 hold | 直接：[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[N2](../../../tests/unit/test_core_output_node_loss.py#L246) | 已补真实 input hold；不再列未覆盖。 |
| 1289 · drop_latch_before_lost_decision_ack_fences_late_task_reply | DROP 一经锁定，即使 decision ACK 丢失，迟到 bytes 不反转决定 | 部分：[N4](../../../tests/unit/test_core_output_node_loss.py#L349) | 缺 decision 本身 effect-then-ACK-loss＋迟到 envelope 的同场景，R-A。 |
| 1359 · owner_commit_effect_then_exception_receipt_wins_keep_over_stale_progress | owner receipt/data 胜过丢失 scratch 进度，Node loss 后保留真实 INLINE | 直接：[R1](../../../tests/unit/test_core_output_receipt_loss.py#L30)、[R3](../../../tests/unit/test_core_output_receipt_loss.py#L124)、[A2](../../../tests/unit/test_core_stored_publication_adoption.py#L177) | 当前 receipt-only 为 ref-free mixed batch；更宽 child 组合留 R-C。 |
| 1423 · old_work_after_other_lane_adopts_and_collects_never_reacquires_payload | 另一 lane 已收集后，旧 RPC 不再缓存 bytes/创建 metadata | 部分：[N5](../../../tests/unit/test_core_output_node_loss.py#L376)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L69) | finished 与 GC 分别测；已完成 GC 后旧 RPC 返回的组合留 R-C。 |
| 1521 · inline_absent_cascades_to_exact_stored_candidate | 未知 result tier 不得漏查可能已发生的发布 | 替代：[L1](../../../tests/unit/test_core_output_lease_domain.py#L105)、[L2](../../../tests/unit/test_core_output_lease_domain.py#L110)、[R2](../../../tests/unit/test_core_output_receipt_loss.py#L91) | 一个 OutputPublicationID domain 已替代 INLINE→STORED cascade。 |
| 1559 · resolved_aborted_cascades_to_exact_stored_candidate | 不能因一个旧 tier 已 abort 就忽略另一 tier 的效果 | 替代：[L1](../../../tests/unit/test_core_output_lease_domain.py#L105)、[L2](../../../tests/unit/test_core_output_lease_domain.py#L110)、[R2](../../../tests/unit/test_core_output_receipt_loss.py#L91) | 不恢复两个 publication candidate；统一强事实不能被弱/矛盾回复覆盖。 |
| 1604 · resolved_aborted_without_stored_candidate_enters_system_retry | 有无效果的精确结论之后才允许 retry | 替代/分层：[L1](../../../tests/unit/test_core_output_lease_domain.py#L105)、[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[D1](../../../tests/unit/test_output_node_loss_control.py#L26) | exact absence 或已完成清理可 retry；不把旧 disposition 枚举当 API。 |
| 1643 · normal_ack_applied_then_node_death_uses_resolved_takeover_without_recommit | 正常 adoption 已应用后 Node death 不重复本地提交 | 分层：[N7](../../../tests/unit/test_core_output_node_loss.py#L443)、[R3](../../../tests/unit/test_core_output_receipt_loss.py#L124) | N7 覆盖 retained targeted 尾部与 publisher death；不保证旧报文顺序。 |
| 1710 · resolved_adopted_takeover_recovers_owner_receipt_without_local_obligation | 本地 scratch marker 缺失也能服从真实 owner receipt | 直接/分层：[R1](../../../tests/unit/test_core_output_receipt_loss.py#L30)、[C2](../../../tests/unit/test_core_output_publication.py#L177)、[R3](../../../tests/unit/test_core_output_receipt_loss.py#L124) | receipt-only、已 resolved 重放分别覆盖，不宣称所有后端历史状态可兼容读取。 |
| 1748 · death_frozen_stale_inline_without_custody_drops_without_graph_commit | 旧 attempt 不覆盖新 owner、不启动错误 retry | 替代/分层：[A7](../../../tests/unit/test_core_stored_publication_adoption.py#L349)、[N6](../../../tests/unit/test_core_output_node_loss.py#L405)、[T2](../../../tests/unit/test_output_owner_retirement.py#L272) | 旧特殊 stale-cleanup 路由不恢复；已发生外部清理的所有交错仍属 R-C。 |
| 1807 · death_frozen_stale_retirement_ack_loss_replays_only_ack | stale retirement 尾部 ACK 丢失不重复 child/graph 效果 | 分层：[A10](../../../tests/unit/test_core_stored_publication_adoption.py#L462)、[N7](../../../tests/unit/test_core_output_node_loss.py#L443)、[T2](../../../tests/unit/test_output_owner_retirement.py#L272) | KEEP 后 successor 前进再 retirement ACK 丢失的完整交叉仍属 R-C。 |
| 1885 · normal_stale_cleanup_switches_to_frozen_retirement | normal cleanup 中获悉 Node death 必须服从冻结恢复事实 | 替代/分层：[A8](../../../tests/unit/test_core_stored_publication_adoption.py#L388)、[N7](../../../tests/unit/test_core_output_node_loss.py#L443) | 统一 Node-loss/adoption 路由替代旧 cleanup_only 类。 |

## test_inline_data_plane_fullflow.py.txt

| 旧用例（冻结行号） | 保留的安全合同 | 当前可执行覆盖 | 待验或替代边界 |
|---|---|---|---|
| 186 · local_complete_survives_gcs_outage_then_owner_ack_precedes_outbox | GCS outage 不阻 local Complete/resource release；outbox 与最终 GC 分离 | 分层：[P1](../../../tests/unit/test_output_publication_node_server.py#L81)、[P2](../../../tests/unit/test_output_publication_node.py#L269)、[P3](../../../tests/unit/test_output_publication_node.py#L287)、[A1](../../../tests/unit/test_core_stored_publication_adoption.py#L131)、[C1](../../../tests/unit/test_core_output_publication.py#L89) | 真实组件单独组合已覆盖；当前 Core terminal/adopted 同步顺序见下节，不承诺旧整段 fullflow 等价。 |
| 228 · death_keeps_only_an_owner_received_envelope | reported/unreported terminal 都不能用 GCS metadata 造 payload | 分层：[N3](../../../tests/unit/test_core_output_node_loss.py#L319)、[R1](../../../tests/unit/test_core_output_receipt_loss.py#L30)、[U1](../../../tests/unit/test_output_recovery.py#L421)、[U3](../../../tests/unit/test_output_recovery.py#L600) | unreported Complete＋真实 contained pins 的 Core KEEP 全链仍属 R-C。 |
| 261 · unreceived_bytes_cleanup_is_lost_not_gcs_payload_recovery | 无 envelope known/UNKNOWN × final cleanup ACK 丢失分流 | 直接/分层：[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[N2](../../../tests/unit/test_core_output_node_loss.py#L246)、[D1](../../../tests/unit/test_output_node_loss_control.py#L26)、[D2](../../../tests/unit/test_output_node_loss_control.py#L83)、[U3](../../../tests/unit/test_output_recovery.py#L600) | Core 状态/预算/holds 已直接补入；同一 Core fullflow 的 child cleanup 仍是分层证据。 |

## test_core_stored_node_loss_takeover.py.txt

| 旧用例（冻结行号） | 保留的安全合同 | 当前可执行覆盖 | 待验或替代边界 |
|---|---|---|---|
| 106 · absent_candidate_is_the_only_takeover_state_that_starts_generic_retry | 无 publication 的精确证据才可直接 retry，已知成功绝不能降级 | 替代：[L1](../../../tests/unit/test_core_output_lease_domain.py#L105)、[L2](../../../tests/unit/test_core_output_lease_domain.py#L110)、[R2](../../../tests/unit/test_core_output_receipt_loss.py#L91)、[N1](../../../tests/unit/test_core_output_node_loss.py#L145) | 旧“only ABSENT”命名不再描述全部路径：UNKNOWN 在完整清理后也可预算 retry。 |
| 133 · pending_takeover_preserves_exact_shutdown_obligation | 未决 takeover 保留 shutdown barrier | 直接/分层：[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[R4](../../../tests/unit/test_core_output_receipt_loss.py#L153) | N1 验证 unresolved/finish；R4 验证独立 owner retirement shutdown fence。 |
| 156 · committed_node_death_replaces_candidate_without_clearing_it | Node death 切换恢复不出现 candidate 丢失窗口 | 直接＋替代：[L1](../../../tests/unit/test_core_output_lease_domain.py#L105)、[L2](../../../tests/unit/test_core_output_lease_domain.py#L110)、[A6](../../../tests/unit/test_core_stored_publication_adoption.py#L325) | 统一候选身份替代旧 stored/inline 双字段。 |
| 173 · dead_adoption_converts_to_takeover_instead_of_replaying_dead_node | dead publisher 不能继续正常 adoption RPC | 分层：[A8](../../../tests/unit/test_core_stored_publication_adoption.py#L388)、[N7](../../../tests/unit/test_core_output_node_loss.py#L443) | 保留统一 takeover 和 metadata 尾部，不保留旧排队消息类型。 |
| 189 · active_owner_retirement_blocks_finalize_without_protocol_table | 丢 protocol marker 也不能绕过 active owner retirement | 直接：[R4](../../../tests/unit/test_core_output_receipt_loss.py#L153) | 当前测试建立真实 retirement；此缺口已补入。 |
| 216 · core_takeover_object_gate_blocks_gc_and_reconstruction_before_owner_cas | 活动恢复/retirement 时不得提前重建或 GC | 分层：[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[T1](../../../tests/unit/test_output_owner_retirement.py#L118)、[R4](../../../tests/unit/test_core_output_receipt_loss.py#L153)、[R5](../../../tests/unit/test_core_output_receipt_loss.py#L175) | owner reducer gate、Core reconstruction/finish、shutdown 分开验证；更宽竞态属 R-B/R-C。 |
| 241 · candidate_survives_explicit_phase_overwrite_after_prior_clear | 阶段改写不丢潜在 publication identity | 替代/分层：[L1](../../../tests/unit/test_core_output_lease_domain.py#L105)、[L2](../../../tests/unit/test_core_output_lease_domain.py#L110)、[A6](../../../tests/unit/test_core_stored_publication_adoption.py#L325) | 覆盖统一 pre-Push 阶段与连续 Push fence，不保留旧双 candidate。 |
| 272 · push_replay_rederives_candidate_after_protocol_table_was_cleared | exact Push 中的信息足以重新绑定同一候选身份 | 分层：[L1](../../../tests/unit/test_core_output_lease_domain.py#L105)、[A6](../../../tests/unit/test_core_stored_publication_adoption.py#L325) | 无 marker 的全部 ambiguous-Push 时序尚不声称逐项等价，R-C。 |
| 305 · owner_only_absent_or_pending_never_clears_driver_or_retries | 已 adopted 的 owner 工作不能被弱 absence 擦掉 | 替代/分层：[R2](../../../tests/unit/test_core_output_receipt_loss.py#L91)、[C2](../../../tests/unit/test_core_output_publication.py#L177)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L69) | fully adopted 后走正常 per-slot GC，不再建立旧 owner-only driver。 |
| 332 · known_completed_non_owner_absent_never_starts_generic_retry | known Complete 不能被 ABSENT 降级 | 直接：[R2](../../../tests/unit/test_core_output_receipt_loss.py#L91) | 覆盖真实统一 envelope 与 contradictory typed reply。 |
| 361 · known_completed_non_owner_rejects_precomplete_resolution_retry | known Complete 不能被 pre-Complete 结论降级 | 直接：[R2](../../../tests/unit/test_core_output_receipt_loss.py#L91) | 保留 pending custody/finalizer barrier，不 retry。 |
| 406 · owner_only_precomplete_resolution_never_retries_or_clears_driver | 已成功 owner 不能服从矛盾的 pre-Complete 回滚 | 替代/分层：[R2](../../../tests/unit/test_core_output_receipt_loss.py#L91)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L69) | 统一事实校验替代旧 owner-only 路径。 |
| 450 · synthetic_driver_scan_includes_lost_and_collecting_without_task_fence | 已结束 Task 的 LOST/collecting 对象仍有 GC 责任 | 替代/部分：[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L69)、[G2](../../../tests/unit/test_stored_publication_node_death_gc.py#L228) | 正常 owner per-slot GC 替代 synthetic driver；Node death 与已 collecting 同时发生仍属 R-B。 |
| 492 · fenced_newer_attempt_still_finishes_old_lane_once | 旧 lane 清账一次且绝不伤害 successor hold/count | 分层：[N2](../../../tests/unit/test_core_output_node_loss.py#L246)、[N7](../../../tests/unit/test_core_output_node_loss.py#L443) | 真实 whole-task hold fencing 与 disjoint-target 尾部分别覆盖；二者交叉仍属 R-C，不恢复旧成功返回值断言。 |
| 615 · owner_retirement_post_commit_exception_replays_exact_tombstone | Node-loss owner CAS 已生效后异常，必须按同一结果重放 | 直接：[R3](../../../tests/unit/test_core_output_receipt_loss.py#L124) | 不二次 GCS cleanup，修复 recovery，独立于 normal-adoption CAS 测试。 |

## test_core_stored_node_loss_fullflow.py.txt

| 旧用例（冻结行号） | 保留的安全合同 | 当前可执行覆盖 | 待验或替代边界 |
|---|---|---|---|
| 202 · core_drives_real_gcs_stored_takeover_to_exact_lost_terminal | pending/adopted STORED publisher loss 保留成功事实，最终清理 child/graph/owner 状态 | 分层＋替代：[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[R3](../../../tests/unit/test_core_output_receipt_loss.py#L124)、[D1](../../../tests/unit/test_output_node_loss_control.py#L26)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L69) | pending Core 分支与真实 child control 分层；fully adopted 分支现按引用生命期 GC，不复刻旧立即 owner-only retirement。 |

## 剩余三类风险与有界验证方向

### R-A：decision ACK 不明与迟到交付/畸变回复

N4 已检查 DROP 锁定后的迟到 envelope；N1 已检查最终 cleanup ACK 丢失。这不等于“GCS 已接受 DROP、其 decision ACK 丢失、然后真实 TaskReply 到达”的同一场景已经验证。最小补充是一个两槽 task、一次 Decide effect-then-timeout、一次迟到交付和一次 exact replay，断言 decision identity/vector 不变、不收回 DROP、不重新保留被拒绝的 bytes，也不提前 retry/finish。使用同步 RPC hook，不启动真实并发。

A3/A4/A11 已覆盖正常 adoption/graph 的错配回复，D3/D5 覆盖 child/finalize 回复，因此不能再笼统写“endpoint 校验无测试”。仍需将 Get/Decide/Progress Node-loss 回复的深层 mutation/错误 owner 或 frozen identity 注入真实 Core 消费边界，验证不把“某个别的 publication 已完成”当成本任务的 cleanup 证明。

### R-B：owner death 与 Core adoption/正常 GC 的竞争

D4、D5、O1、O2 与 U2 已覆盖 owner-wide fences、Node/Worker cleanup、ACK 丢失和两类 death 的组件次序。缺口是把这些与真实 Core pending/adoption、已进入 COLLECTING 的槽以及 surviving sibling 同步交错。最小组合使用一个 mixed batch、一个共享 borrowed child、各输出独立的 final hold（以及必要的 provisional 身份）、一次 owner-death/finalize ACK 丢失；每次手动推进一个 effect，断言正常 adoption 不能复活被 fence 的 owner，cleanup 不双重释放或丢掉 sibling 存活边，shutdown 不越过 active retirement。旧 synthetic-driver 的 LOST/collecting 扫描断言不能直接等价标为已完成。

### R-C：分层证据尚未组成完整故障矩阵，及安全分类

已经补入并直接映射的关键合同包括：graph COMMIT/RELEASE ACK 丢失（A2/A10）、receipt-only Node loss（R1）、known Complete 与弱历史矛盾（R2）、Node-loss owner CAS effect-then-error（R3）、独立 retirement shutdown fence（R4）、无 envelope known/UNKNOWN final cleanup ACK 丢失与真实 input holds（N1/N2），以及 fully adopted Node-loss GC（G1/G2）。不要继续把它们列为完全缺失。

但仍应逐项核查以下组合，而非以更多历史通过数或时间戳补证：UNKNOWN 有真实本地 Complete custody 且 contained pins 尚存；另一 lane 已完成 adoption **和 GC** 后旧 RPC 才返回；disjoint targeted successor 已前进但旧 metadata 尾部和真实旧 input hold 同时待 finalizer；Core 驱动器被 BaseException 中断后 ticket 释放与义务保留。每个场景优先复用现有 threadless fixtures，限定两槽、一个 input、一个故障点和显式有限步 replay；已有 OwnedChild STORED ARM-UNKNOWN 的真实进程切片不能外推这些全部组合。

历史 unit marker 本身不是安全证明。活动测试的分类、默认选择集以及同一 revision 的完整回归仍须按测试策略审计；归档文件不能恢复进 gate 充数，新增 pure 场景也不能替代已审查的逐项 bounded 真实进程验收。本页不宣称旧/新覆盖完全相等，也不据此关闭 K0/K1。
