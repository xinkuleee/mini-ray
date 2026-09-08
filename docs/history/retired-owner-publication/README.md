# 已退役 owner publication 合同映射

本目录是四份旧 INLINE/STORED owner 测试的逐字历史备份，不是 skip/xfail 活动测试，不进入 pytest 收集或任何 gate。原 .py 的安全意图必须映射到当前统一 OutputPublication/OutputOwnerPublicationPlan，而不能靠恢复旧 class、方法、flag 或兼容别名消除 import 错误。

归档不表示旧/新覆盖完全相等。本页“直接”只指当前活动测试明确断言相应不变量；“分层/部分”指相邻组件有证据但尚未组合；“替代”指旧协议限制/阶段顺序不再是目标。新增测试的存在不是通过声明；本页不记录用例数或耗时，当前验证结果见[状态页](../../current-status.md)与[测试策略](../../testing.md)，K0/K1 [目标](../../roadmap.md)不变。

## 原文完整性

以下 SHA-256 在归档前和 .py.txt 写入后均用只读哈希命令核对一致；不代表归档代码可以在当前运行时执行。

| 旧活动路径（相对仓库根） | 逐字副本 | SHA-256 |
|---|---|---|
| tests/unit/test_inline_owner_publication.py | [test_inline_owner_publication.py.txt](test_inline_owner_publication.py.txt) | f7080bbb8b63784a2fb03b464d36c55ffe165a2400252b4d7424161727312569 |
| tests/unit/test_stored_owner_atomic_publication.py | [test_stored_owner_atomic_publication.py.txt](test_stored_owner_atomic_publication.py.txt) | 4c5c981be08a7bbe25446d9a44c729bb2fb3a7691d395e643d5c5847cf1ee136 |
| tests/unit/test_stored_owner_retirement.py | [test_stored_owner_retirement.py.txt](test_stored_owner_retirement.py.txt) | 335b316117a34aee2b4a3acfd87fa7c1bc7ac13ff46292c0b86b10cae87e1619 |
| tests/unit/test_unreceived_inline_owner_retirement.py | [test_unreceived_inline_owner_retirement.py.txt](test_unreceived_inline_owner_retirement.py.txt) | 2f4d300261e091583ca09d65ef882dba5536404ded5a0cdf288be11f55e5258e |

## 当前合同索引

链接相对项目路径，函数名是稳定定位依据。部分活动文件保留历史文件名，但不表示执行旧owner协议。

| 编号 | 活动测试 | 已表达的不变量 |
|---|---|---|
| P1 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L172) · test_mixed_batch_publishes_once_without_retaining_sibling_payloads | bytes/descriptor、edges、membership 一次 CAS；exact replay；每槽不持有 sibling bytes |
| P2 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L219) · test_conflicting_same_publication_replay_never_changes_ready_slots | 同 publication 改结果/digest 必须零修改 |
| P3 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L294) · test_one_stale_selected_slot_fences_entire_batch | 后槽 stale 时整批 fenced |
| P4 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L309) · test_late_slot_conflict_cannot_publish_earlier_slots | 后槽已有结果，前槽不得部分发布 |
| P5 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L321) · test_plan_execution_and_nested_payload_revalidation_precede_owner_mutation | execution/嵌套 payload 预校验先于 owner mutation |
| P6 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L474) · test_each_slot_collects_only_its_own_edges_and_never_keeps_sibling_bytes | 有 live hold 不收集；必须逐槽 graph proof；保 sibling；精确 collection replay |
| P7 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L511) · test_collection_rejects_another_slots_graph_ack_and_altered_full_graph | 其它槽/其它事务的 graph RELEASE ACK 不能删除本槽 |
| P8 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L551) · test_shared_child_holds_and_task_lineage_survive_until_their_own_last_slot | child holds 分槽；最后 sibling 才释放 Task lineage |
| P9 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L585) · test_terminal_collection_replay_binds_metadata_hash_without_storing_taskspec | terminal replay 绑定原 TaskSpec metadata hash/collection identity，不保存 TaskSpec |
| P10 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L670) · test_published_slot_edges_cannot_be_extended_by_legacy_helpers | 已采用的 outgoing edges 不可从普通 helper 扩展 |
| P11 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L659) · test_legacy_objects_still_use_their_original_owner_and_gc_apis | 无统一 membership 的普通对象仍可使用 generic owner/GC |
| P12 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L456) · test_no_reference_batch_uses_the_same_publication_and_per_slot_collection | ref-free 正常成功仍使用统一批发布，不强制非空 graph |
| P13 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L273) · test_wrong_owner_or_job_has_no_partial_result_mutation | owner/job 不符拒绝整批 |
| P14 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L631) · test_public_collection_receipt_ids_cannot_poison_tombstones_or_siblings | 返回 receipt 的深层修改不能污染 owner tombstone/sibling |
| P15 | [test_output_owner_publication.py](../../../tests/unit/test_output_owner_publication.py#L443) · test_lost_stored_slot_replay_never_restores_a_dead_replica | 旧完整结果重放不复活已 LOST replica |
| T1 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L78) · test_retirement_preserves_stable_id_incoming_holds_and_task_lineage | retirement 保留 stable ID、incoming holds、task lineage 与健康 siblings |
| T2 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L118) · test_retirement_gates_publish_advance_and_gc_but_not_reference_release | active retirement 阻发布/advance/GC，不阻 release |
| T3 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L364) · test_normal_collection_and_reconstruction_retirement_cannot_overlap | collection 与 reconstruction retirement 互斥 |
| T4 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L179) · test_completion_requires_every_cleanup_proof_and_never_partially_clears | 完整 child/graph/replica 证明前不部分清 metadata |
| T5 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L166) · test_retirement_rejects_overlapping_claim_and_rebound_identity | 同 retirement ID 重绑定/重叠操作被拒绝 |
| T6 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L272) · test_retired_slot_can_reconstruct_with_original_index_and_old_replays_are_fenced | 旧 plan/publish_stored fenced；新 targeted attempt 可发布原 index |
| T7 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L342) · test_post_retirement_gc_collects_metadata_only_and_preserves_sibling_lineage | 已准确 retirement 的槽 GC 不重复释放 child/replica，保 sibling lineage |
| T8 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L375) · test_public_plans_proofs_and_terminal_getters_cannot_mutate_owner_history | retirement proof/getter 深拷贝与 metadata-only 历史 |
| T9 | [test_output_owner_retirement.py](../../../tests/unit/test_output_owner_retirement.py#L211) · test_cleanup_proofs_cannot_substitute_another_hold_slot_or_epoch | cleanup proof 不允许其它 child/hold/slot/epoch 替代 |
| D1 | [test_output_node_loss_control.py](../../../tests/unit/test_output_node_loss_control.py#L251) · test_owner_applies_resolved_loss_without_fabricating_payload_or_losing_incoming_refs | known/UNKNOWN 的 per-slot owner resolution，不从 metadata 造 bytes |
| N1 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L145) · test_no_envelope_cleanup_ack_loss_fences_local_state_until_exact_replay | final cleanup ACK 丢失不改变 Core/owner；known LOST 与 UNKNOWN retry 分开 |
| N2 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L246) · test_old_delayed_completion_cannot_release_current_attempt_input_hold | 真实 lineage/输入 hold；旧 delayed/finalizer 不动新 attempt |
| N3 | [test_core_output_node_loss.py](../../../tests/unit/test_core_output_node_loss.py#L349) · test_late_envelope_cannot_reverse_a_latched_drop | 已锁定 DROP 不被迟到 bytes 反转 |
| R1 | [test_core_output_receipt_loss.py](../../../tests/unit/test_core_output_receipt_loss.py#L30) · test_owner_cas_receipt_restores_inline_custody_for_node_loss_after_scratch_cache_disappears | owner receipt 的真实 bytes 可恢复 custody，不能把已接受数据当未交付 |
| R2 | [test_core_output_receipt_loss.py](../../../tests/unit/test_core_output_receipt_loss.py#L124) · test_node_loss_owner_cas_effect_then_error_reuses_resolution_without_second_cleanup | resolution CAS 后异常的 exact replay，不重做清理 |
| G1 | [test_stored_publication_node_death_gc.py](../../../tests/unit/test_stored_publication_node_death_gc.py#L228) · test_plain_lost_object_without_descriptor_is_not_collected | 普通 LOST 无 canonical descriptor 的不安全 GC 被拒绝 |
| F1 | [test_output_owner_resolution_preflight.py](../../../tests/unit/test_output_owner_resolution_preflight.py#L95) · test_pristine_unreceived_batch_preserves_incoming_holds_and_canonical_lineage | 本次新增：两槽 pristine known/UNKNOWN 成功基线；保所有 incoming/lineage 与精确历史 |
| F2 | [test_output_owner_resolution_preflight.py](../../../tests/unit/test_output_owner_resolution_preflight.py#L130) · test_bad_second_unreceived_slot_cannot_erase_metadata_or_partially_resolve_batch | 本次新增已验证：坏后槽 partial metadata/producer lineage/state 必须整批零修改 |

## test_inline_owner_publication.py

旧函数名省略 test_ 前缀。

| 旧case | 安全意图 | 当前覆盖 | 边界/待验 |
|---|---|---|---|
| inline_bytes_edges_and_graph_commit_atomically_and_replay | INLINE payload/edges/graph 元数据原子发布和 exact replay | 直接：[P1](../../../tests/unit/test_output_owner_publication.py#L172) | 从单槽计划替换为 selected-set CAS。 |
| plan_rejects_empty_or_foreign_container_graphs | wrong-container 不得混入 graph；原单返回非空图要求 | 替代/分层：[P5](../../../tests/unit/test_output_owner_publication.py#L321)、[P12](../../../tests/unit/test_output_owner_publication.py#L456) | 非空 graph/单返回限制已取消；refs=False 合法，foreign selected container 仍应拒绝。 |
| stale_and_conflicting_inline_commits_have_zero_mutation | stale/rebound/部分 publication 不得追加修改 | 直接/增量：[P2](../../../tests/unit/test_output_owner_publication.py#L219)、[P3](../../../tests/unit/test_output_owner_publication.py#L294)、[P4](../../../tests/unit/test_output_owner_publication.py#L309)、[F2](../../../tests/unit/test_output_owner_resolution_preflight.py#L130) | F2 已通过坏 PENDING 后槽预检的纯回归。 |
| inline_collection_requires_the_exact_graph_release_receipt | live ref 阻 GC；graph receipt 必须精确；collection/replay 幂等 | 直接：[P6](../../../tests/unit/test_output_owner_publication.py#L474)、[P7](../../../tests/unit/test_output_owner_publication.py#L511) | 按槽和整个 batch manifest 校验，不恢复旧 plan 类型。 |
| legacy_inline_publication_keeps_legacy_collection_path | 普通无 membership 对象可 generic GC | 直接：[P11](../../../tests/unit/test_output_owner_publication.py#L659) | 这不承诺旧 INLINE publication API 保留。 |
| terminal_inline_history_cannot_keep_result_or_lineage_payload_bytes | 全 owner 终史无 payload/TaskSpec，caller plan 精确重放仍校验全部内容 | 部分：[P8](../../../tests/unit/test_output_owner_publication.py#L551)、[P9](../../../tests/unit/test_output_owner_publication.py#L585)、[P14](../../../tests/unit/test_output_owner_publication.py#L631)、[T8](../../../tests/unit/test_output_owner_retirement.py#L375) | 完整 vars(owner) 与 function/result 篡改组合为 O-C；不能以扫描几个字典视为全部等价。 |

## test_stored_owner_atomic_publication.py

旧函数名省略 test_ 前缀。

| 旧case | 安全意图 | 当前覆盖 | 边界/待验 |
|---|---|---|---|
| descriptor_edges_and_adoption_commit_under_one_owner_transaction | descriptor/location/edges/adoption 原子发布 | 直接：[P1](../../../tests/unit/test_output_owner_publication.py#L172) | 移除旧 claim/adoption DTO，统一 manifest membership 权威。 |
| stale_attempt_is_fenced_and_conflicting_replay_changes_nothing | 旧 attempt 与冲突重放零修改 | 直接＋替代：[P2](../../../tests/unit/test_output_owner_publication.py#L219)、[P3](../../../tests/unit/test_output_owner_publication.py#L294) | 不保留旧 owner_commit_id 字段作为独立 owner CAS 权威。 |
| preexisting_partial_publication_is_rejected_without_more_mutation | 已有 partial edges 不能被后续提交悄悄覆盖 | 分层/增量：[P4](../../../tests/unit/test_output_owner_publication.py#L309)、[F2](../../../tests/unit/test_output_owner_resolution_preflight.py#L130) | 普通 CAS 后槽已有结果已有覆盖；无 membership loss cleanup 的 partial preflight 由本次 F2 补。 |
| adopted_edge_manifest_is_immutable | 同边 replay no-op，新增边被拒绝 | 直接：[P10](../../../tests/unit/test_output_owner_publication.py#L670) | 普通 helper 不可绕过已提交 batch graph。 |
| collection_reuses_generic_freeze_but_requires_exact_graph_release | generic freeze 可复用但不可跳过 graph RELEASE；wire order 与局部 sorted order各自保持 | 直接/分层：[P6](../../../tests/unit/test_output_owner_publication.py#L474)、[P7](../../../tests/unit/test_output_owner_publication.py#L511)、[P8](../../../tests/unit/test_output_owner_publication.py#L551) | 新每槽仅一个 edge 的主fixture不穷尽旧逆序双edge排序组合。 |
| wrong_graph_release_cannot_delete_frozen_owner_metadata | 错误 graph proof 不删除 COLLECTING owner metadata | 直接：[P7](../../../tests/unit/test_output_owner_publication.py#L511)、[T9](../../../tests/unit/test_output_owner_retirement.py#L211) | 新覆盖跨slot/改事务与hold/epoch错配。 |

## test_stored_owner_retirement.py

旧函数名省略 test_ 前缀。

| 旧case | 安全意图 | 当前覆盖 | 边界/待验 |
|---|---|---|---|
| pending_owner_is_marked_lost_without_exposing_a_location | 未交付但known Complete 经清理为LOST，不暴露location | 替代/直接：[D1](../../../tests/unit/test_output_node_loss_control.py#L251)、[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[F1](../../../tests/unit/test_output_owner_resolution_preflight.py#L95) | 统一resolution发生在GCS清理后，不复制旧先标LOST再做远端清理的阶段。 |
| matching_ready_owner_loses_dead_location_and_keeps_exact_plan | 已收到stored lost时保留足够membership支持精确后续清理 | 分层：[P15](../../../tests/unit/test_output_owner_publication.py#L443)、[T1](../../../tests/unit/test_output_owner_retirement.py#L78) | 新retirement完成可清canonical descriptor；不能要求永存旧envelope计划。 |
| newer_attempt_and_matching_normal_collection_are_fenced | new attempt/collection不能被旧retirement覆盖 | 直接/分层：[P3](../../../tests/unit/test_output_owner_publication.py#L294)、[T3](../../../tests/unit/test_output_owner_retirement.py#L364)、[T6](../../../tests/unit/test_output_owner_retirement.py#L272) | 返回 disposition/异常形态非旧API兼容目标。 |
| active_retirement_gates_results_attempts_and_collection_not_releases | active retirement 排斥publication/advance/GC，允许ref release | 直接：[T2](../../../tests/unit/test_output_owner_retirement.py#L118) | 使用统一 output retirement，不恢复stored专用fence。 |
| exact_terminal_resolution_clears_graph_metadata_and_opens_reconstruction | 缺任一cleanup proof不清metadata；完整后可重建并精确重放 | 直接：[T1](../../../tests/unit/test_output_owner_retirement.py#L78)、[T4](../../../tests/unit/test_output_owner_retirement.py#L179)、[T6](../../../tests/unit/test_output_owner_retirement.py#L272)、[T9](../../../tests/unit/test_output_owner_retirement.py#L211) | 旧descriptor保留策略由统一已退休槽语义替换。 |
| conflicting_effect_replay_preserves_first_retirement | 同一retirement身份重绑定必须拒绝 | 直接：[T5](../../../tests/unit/test_output_owner_retirement.py#L166)、[T8](../../../tests/unit/test_output_owner_retirement.py#L375) | 保留最初plan/proofs，不引入旧saga effect类型。 |

## test_unreceived_inline_owner_retirement.py

旧函数名省略 test_ 前缀。

| 旧case | 安全意图 | 当前覆盖 | 边界/待验 |
|---|---|---|---|
| retirement_is_metadata_only_atomic_and_preserves_lineage | 未交付结果只做metadata状态转换、保lineage/incoming refs | 直接/增量：[D1](../../../tests/unit/test_output_node_loss_control.py#L251)、[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[F1](../../../tests/unit/test_output_owner_resolution_preflight.py#L95) | F1 已通过本轮纯回归；old object identity is语义不作wire API保证。 |
| accepted_inline_bytes_win_without_installing_drop_tombstone | 已采用真实bytes不被误当未交付DROP | 分层/替代：[R1](../../../tests/unit/test_core_output_receipt_loss.py#L30)、[N3](../../../tests/unit/test_core_output_node_loss.py#L349) | KEEP/DROP在Core custody lock仲裁；owner reducer不自行逆转已确认DROP。 |
| retired_attempt_is_fenced_but_new_attempt_reconstructs | same retired attempt不可重发，新attempt保持logical ID可重建 | 直接/部分：[T6](../../../tests/unit/test_output_owner_retirement.py#L272)、[N1](../../../tests/unit/test_core_output_node_loss.py#L145) | 跨tier/多入口完整矩阵留O-B。 |
| unreceived_inline_uses_existing_lineage_reconstruction_coordinator | 复用真实canonical lineage reconstruction，不另写恢复语义 | 直接：[N1](../../../tests/unit/test_core_output_node_loss.py#L145)、[N2](../../../tests/unit/test_core_output_node_loss.py#L246) | known需旧finish barrier之后才允许显式请求。 |
| retirement_replay_binds_the_exact_graph | digest/edge顺序改写不可成为合法retirement replay | 分层：[P2](../../../tests/unit/test_output_owner_publication.py#L219)、[P7](../../../tests/unit/test_output_owner_publication.py#L511)、[T5](../../../tests/unit/test_output_owner_retirement.py#L166)、[R2](../../../tests/unit/test_core_output_receipt_loss.py#L124) | 新loss receipt精确manifest/digest/decision校验；全部wrong graph排列未宣称穷尽。 |
| retirement_rejects_foreign_identity_without_mutation | owner/task/attempt/transaction/container不得越权替换 | 分层/增量：[P5](../../../tests/unit/test_output_owner_publication.py#L321)、[P13](../../../tests/unit/test_output_owner_publication.py#L273)、[T9](../../../tests/unit/test_output_owner_retirement.py#L211)、[F2](../../../tests/unit/test_output_owner_resolution_preflight.py#L130) | 旧empty graph拒绝已替代为refs=False合法；F2补lineage归属不符。 |
| retirement_requires_one_return_typed_identity_and_lineage | typed identity/canonical producer lineage必须存在 | 替代/增量：[P5](../../../tests/unit/test_output_owner_publication.py#L321)、[F2](../../../tests/unit/test_output_owner_resolution_preflight.py#L130) | 单返回/InlinePublicationID要求已删除；F2补missing/不一致producer。 |
| partial_pending_metadata_cannot_be_erased_by_retirement | partial PENDING字段不能被cleanup当空槽覆盖 | 本次新增已验证：[F2](../../../tests/unit/test_output_owner_resolution_preflight.py#L130) | 坏第二槽须全批snapshot/receipts/retiredsets不变；不把测试落盘称通过。 |
| stale_or_missing_output_does_not_acquire_retirement | stale/missing对象不能获取旧retirement权威 | 分层/增量：[P3](../../../tests/unit/test_output_owner_publication.py#L294)、[T6](../../../tests/unit/test_output_owner_retirement.py#L272)、[F2](../../../tests/unit/test_output_owner_resolution_preflight.py#L130) | F2覆盖后槽nextattempt；missing whole ID的各入口未宣称全部覆盖。 |
| retired_inline_gc_is_metadata_and_lineage_only | exact cleanup后的LOST可metadata-only GC，不重复child/replica清理 | 直接/分层：[T7](../../../tests/unit/test_output_owner_retirement.py#L342)、[N2](../../../tests/unit/test_core_output_node_loss.py#L246) | last-sibling lineage规则替代旧单槽特例，Node-loss resolution后的全部GC组合仍应核查。 |
| tombstone_does_not_make_contaminated_lost_metadata_collectible | 已有retirement墓碑不能掩盖后来污染的LOST状态 | 部分：[T7](../../../tests/unit/test_output_owner_retirement.py#L342)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L228) | 需retired后再污染fields并GC的O-B增量；F2仅初次resolution预检。 |
| old_retirement_does_not_authorize_new_attempts_stored_gc | 旧墓碑不能授权新attempt LOST缺canonical数据GC | 部分：[T6](../../../tests/unit/test_output_owner_retirement.py#L272)、[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L228) | 分别有attempt fence和unknown LOST拒绝，组合属O-B。 |
| retired_inline_attempt_cannot_reappear_through_stored_publication | 同attempt不能从replica/task-output/newpublication路径跨tier复活 | 部分：[T6](../../../tests/unit/test_output_owner_retirement.py#L272)、[P15](../../../tests/unit/test_output_owner_publication.py#L443) | publish_stored+旧plan已有；其余入口和新身份重发由O-B补。 |
| other_lost_results_are_not_retirements_and_keep_stored_gc_rules | 普通LOST不是已完成cleanup，仍要求canonical metadata | 直接/分层：[G1](../../../tests/unit/test_stored_publication_node_death_gc.py#L228)、[T7](../../../tests/unit/test_output_owner_retirement.py#L342) | ordinary canonical有/无的所有分支与retired分支不可互相冒充。 |

## 三项审计关注与当前状态

### O-A：无 membership 的 PENDING 必须是 pristine 且有一致 producer lineage

已新增 [test_output_owner_resolution_preflight.py](../../../tests/unit/test_output_owner_resolution_preflight.py)。它复用统一 owner fixture，限制两个 selected slots、一个共享 child 与一条 task-lineage edge；已知 Complete 和 UNKNOWN 各有成功基线。后槽注入 inline bytes、error、canonical descriptor、location、outgoing contained edge、缺失/不一致 producer、非PENDING状态或新attempt，要求整个 batch、incoming/lineage、loss/publication receipts 及 retired identity sets 完全不变。

状态：生产预检已修复，F1/F2 已通过本轮 reviewed pure 选择；不是完整故障矩阵完成。

### O-B：retirement 墓碑不能跨 tier、入口或 attempt 越权

T6/P15 已覆盖旧 batch plan/publish_stored 的 fencing与新 targeted attempt，G1覆盖普通LOST缺canonical metadata拒绝。仍需逐项核对同一退休 attempt经 publish_task_outputs、另一统一publication身份、普通replica入口跨tier重发的完整矩阵；新attempt必须可正常发布，但旧墓碑不能让其缺descriptor的LOST状态被GC。已退休对象后来污染result/location/edge字段也必须拒绝metadata-only collection。

状态：[test_output_owner_retired_fencing.py](../../../tests/unit/test_output_owner_retired_fencing.py) 已实现并通过上述跨入口/跨tier/跨epoch与污染GC契约；统一publication还显式检查retired attempt，不仅靠旧publication ID。其它未列组合仍开放。

### O-C：终态 owner 的全可达状态不得留有结果/参数/函数 bytes

P8/P9/P14/T8已有指定receipt字典metadata-only、caller plan重放、参数hash与深拷贝合同。旧inline历史测试更强：最终整个owner除锁之外递归不可达result bytes、TaskSpec或FunctionDefinition，调用者自己保留的原plan仍可精确重放，改result、参数、函数内容或collection ID必须拒绝。

状态：[test_output_owner_terminal_metadata.py](../../../tests/unit/test_output_owner_terminal_metadata.py) 已通过全vars(owner)与child owner可达性扫描、caller原/重建plan精确重放及参数/kwargs/函数/结果/collection ID篡改拒绝。它使用两INLINE槽，不冒充物理stored GC的运行时验证。

## 不应复活的旧语义

- 单返回、非空 contained graph、InlinePublicationID/StoredPublicationID是旧后端限制；当前ref-free、mixed multi-return和targeted outputs都走统一manifest。
- 已知Complete缺bytes与UNKNOWN必须分流：前者精确cleanup后LOST并保lineage，后者cleanup后预算内系统retry；owner resolution不是外部效果执行器。
- 已收到bytes是否KEEP由Core真实custody/receipt与冻结decision共同决定，owner reducer不能为了旧测试自行逆转已锁定DROP。
- 已adopted对象的Node loss保留逐槽引用责任；新retirement完成后清canonical descriptor并不丢安全性，不要求永久保存旧payload-bearing owner plan。
- 验收return值、旧exception类名、私有字典布局不是API兼容承诺；必须保留的是原子性、精确身份、持有关系与可重放的正确终态。

只读源码映射不替代同版安全回归、真实进程故障矩阵或用户目标的完成。上述未验证项不得借非执行归档隐藏。
