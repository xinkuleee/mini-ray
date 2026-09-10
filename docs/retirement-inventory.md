# 已确认过期项：逐文件与逐函数清单

审计版本：`ce29981a547f83b53b0c1df9f91354dcf89d8e4f`。本次只定位和分类，**没有删除源码或测试**。
分析结论见[代码价值审计](code-value-audit.md)，完整证据、保留断言、替代路径、依赖者与hash见[机器清单](retirement-inventory.json)。
行号固定于当前提交；函数数未展开参数化case。“整份实现过期”不表示内部所有通用正确性断言都无价值。

## 1. 明确无用或退役的实现

| 编号 | 文件与行号 | 定义/字段 | 物理/代码行 | 删除前提 |
|---|---|---|---:|---|
| SRC-001 | [core.py](../src/miniray/core.py):11637 | `CoreWorker._known_output_completion_locked` | 15/14 | 未发现仓库使用，仍需受影响回归 |
| SRC-002 | [core.py](../src/miniray/core.py):12322 | `CoreWorker._resolve_task_dependencies` | 91/83 | 先迁移旧调用/断言；保留有效合同 |
| SRC-003 | [node.py](../src/miniray/node.py):770 | `NodeServer._legacy_unregister_from_gcs_best_effort` | 29/20 | 先迁移旧调用/断言；保留有效合同 |
| SRC-004 | [node.py](../src/miniray/node.py):3427 | `NodeServer._start_worker_locked` | 5/3 | 未发现仓库使用，仍需受影响回归 |
| SRC-005 | [node.py](../src/miniray/node.py):3516 | `NodeServer._stop_worker` | 5/3 | 先迁移旧调用/断言；保留有效合同 |
| SRC-006 | [node.py](../src/miniray/node.py):6840 | `NodeServer._drain_actor_workers_once` | 71/68 | 未发现仓库使用，仍需受影响回归 |
| SRC-007 | [worker.py](../src/miniray/worker.py):1056 | `WorkerServer._claim_system_error_failpoint` | 7/5 | 未发现仓库使用，仍需受影响回归 |
| SRC-008 | [worker.py](../src/miniray/worker.py):1825 | `WorkerServer._begin_drain` | 25/23 | 未发现仓库使用，仍需受影响回归 |
| SRC-009 | [core.py](../src/miniray/core.py):1075 | `_ActorInflightCall` | 13/6 | 未发现仓库使用，仍需受影响回归 |
| SRC-010 | [dependency.py](../src/miniray/dependency.py):608 | `_deduplicate` | 9/9 | 未发现仓库使用，仍需受影响回归 |
| SRC-011 | [runtime_state.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/src/miniray/runtime_state.py):1 | `runtime_state module` | 401/287 | 先迁移旧调用/断言；保留有效合同 |
| SRC-012 | [recovery.py](../src/miniray/recovery.py):787 | `RecoveryManager.validate_terminal_reconstruction_failure` | 46/35 | 先迁移旧调用/断言；保留有效合同 |
| SRC-013 | [recovery.py](../src/miniray/recovery.py):67 | `RecoveryAction.FAIL_RECONSTRUCTION_TARGETS` | 1/1 | 先迁移旧调用/断言；保留有效合同 |
| SRC-014 | [node.py](../src/miniray/node.py):384 | `NodeServer._legacy_worker_compat initialization` | 1/1 | 未发现仓库使用，仍需受影响回归 |
| SRC-015 | [node.py](../src/miniray/node.py):2215 | `NodeServer._legacy_worker_compat legacy assignment` | 1/1 | 未发现仓库使用，仍需受影响回归 |
| SRC-016 | [node_monitor.py](../src/miniray/node_monitor.py):49 | `ManagedNodeMonitor._processes` | 1/1 | 未发现仓库使用，仍需受影响回归 |
| SRC-017 | [output_publication_journal.py](../src/miniray/output_publication_journal.py):552 | `OutputPublicationJournal.retire_slot` | 14/7 | 先迁移旧调用/断言；保留有效合同 |
| SRC-018 | [output_publication_journal.py](../src/miniray/output_publication_journal.py):193 | `OutputPublicationSlotCleanupProof` | 27/19 | 先迁移旧调用/断言；保留有效合同 |
| SRC-019 | [protocol.py](../src/miniray/protocol.py):6342 | `StoredPublicationQueryDisposition` | 4/4 | 先迁移旧调用/断言；保留有效合同 |

合计**19项、766物理行／590代码行**，跨度互不重叠。另24个未用import绑定列于JSON，不按名字数量叠加行数。

## 2. 整份测试实现明确过期

共**26个文件、10,792物理行、99个测试函数定义**；均不在当前29文件／37smoke选择中。
可以退休旧实现，但须先抽取被其他文件使用的helper，迁移仍有效的身份、引用、回收反例。历史由固定tag取回。

| 文件 | 物理行 | 函数数 | 过期的主合同 | helper导入点 |
|---|---:|---:|---|---:|
| [test_actor_node_loss_migration_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_actor_node_loss_migration_path.py) | 573 | 1 | 已退出的Actor跨Node迁移 | 0 |
| [test_mixed_borrowed_output_unknown_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_mixed_borrowed_output_unknown_path.py) | 466 | 1 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_multi_contained_output_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_contained_output_path.py) | 381 | 1 | 已退出的multi-return／targeted／sibling生命周期 | 18 |
| [test_multi_output_node_loss_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_output_node_loss_path.py) | 400 | 1 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_multi_return_partial_reconstruction_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_return_partial_reconstruction_path.py) | 332 | 1 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_multi_return_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_return_path.py) | 407 | 1 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_multi_return_reconstruction_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_return_reconstruction_path.py) | 388 | 1 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_nested_large_argument_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_nested_large_argument_path.py) | 741 | 1 | 已退出的自动StoredArg提升 | 0 |
| [test_targeted_borrowed_output_unknown_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_targeted_borrowed_output_unknown_path.py) | 610 | 2 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_actor_arguments.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_actor_arguments.py) | 414 | 9 | 已退出的Actor Ref参数模型 | 0 |
| [test_actor_safety_classification.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_actor_safety_classification.py) | 126 | 1 | 只锁旧迁移轮名字、参数数量和marker名册；保留通用隔离规则 | 0 |
| [test_large_argument_lift.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_large_argument_lift.py) | 946 | 18 | 已退出的自动StoredArg提升 | 0 |
| [test_legacy_runtime_safety_classification.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_legacy_runtime_safety_classification.py) | 355 | 1 | 只锁旧迁移轮名字、参数数量和marker名册；保留通用隔离规则 | 0 |
| [test_multi_return_partial_seal_cleanup.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_multi_return_partial_seal_cleanup.py) | 472 | 1 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_multi_return_submission_transaction.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_multi_return_submission_transaction.py) | 323 | 6 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_node_actor_node_loss_migration.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_node_actor_node_loss_migration.py) | 130 | 3 | 已退出的Actor跨Node迁移 | 0 |
| [test_observability_safety_classification.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_observability_safety_classification.py) | 140 | 1 | 只锁旧迁移轮名字、参数数量和marker名册；保留通用隔离规则 | 0 |
| [test_placement_node_safety_classification.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_placement_node_safety_classification.py) | 379 | 1 | 只锁旧迁移轮名字、参数数量和marker名册；保留通用隔离规则 | 0 |
| [test_recovery_safety_classification.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_recovery_safety_classification.py) | 79 | 1 | 只锁旧迁移轮名字、参数数量和marker名册；保留通用隔离规则 | 0 |
| [test_reference_safety_classification.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_reference_safety_classification.py) | 379 | 1 | 只锁旧迁移轮名字、参数数量和marker名册；保留通用隔离规则 | 0 |
| [test_runtime_state.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_runtime_state.py) | 135 | 5 | 非运行时RuntimeState旧教学facade | 0 |
| [test_targeted_reconstruction.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_reconstruction.py) | 589 | 18 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_targeted_reconstruction_protocol.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_reconstruction_protocol.py) | 489 | 10 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_targeted_worker_execution.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_worker_execution.py) | 225 | 4 | 已退出的multi-return／targeted／sibling生命周期 | 0 |
| [test_worker_export_pin_rollback.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_worker_export_pin_rollback.py) | 1108 | 7 | 多槽/targeted及已删除generic export-pin协议 | 0 |
| [test_worker_safety_classification.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_worker_safety_classification.py) | 205 | 2 | 只锁旧迁移轮名字、参数数量和marker名册；保留通用隔离规则 | 0 |

特别注意：`test_multi_contained_output_path.py`的`_close_local`、`_pid_exists`被**18个文件**导入。
其中很多仍测试依赖托管、late replica和owner死亡，不能连带删除。准确导入位置与名字见JSON的`imported_by`。

## 3. 混合文件中明确过期的函数或参数

共**52个文件中的147个函数当前形态，以及3组参数分支**。这些条目需要退出或改写；不是147项业务不变量都可以删除。
参数分支没有运行collection，不虚构其展开后的node ID；未列出的函数也不因此自动判为当前可运行。

### tests/integration/test_contained_cycle_control_path.py

文件：[test_contained_cycle_control_path.py](../tests/integration/test_contained_cycle_control_path.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 183 | `test_registered_unified_graph_rejects_cycle_then_real_owner_gc_releases_container` | 旧graph端点和metadata-only A/B加裸ABORT实验失效，但文件后半真实公共contained输出与GC属于有效合同。 |

### tests/integration/test_core_reconstruction_concurrency.py

文件：[test_core_reconstruction_concurrency.py](../tests/integration/test_core_reconstruction_concurrency.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 523 | `test_concurrent_multi_return_sibling_requests_start_once_and_join` | 同文件有真实两个请求对同一单输出合并与3-return sibling合并；只能退休后者，前者是有价值并发合同。 |

### tests/integration/test_foreign_late_output_replica_cleanup_path.py

文件：[test_foreign_late_output_replica_cleanup_path.py](../tests/integration/test_foreign_late_output_replica_cleanup_path.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 169 | `test_foreign_late_replica_is_collected_and_old_messages_preserve_reconstructed_epoch` | late foreign secondary replica清理和旧消息不能伤害重建epoch有价值；当前场景硬绑定两输出healthy sibling和TargetExecutionKey。 |

### tests/integration/test_late_output_replica_cleanup_path.py

文件：[test_late_output_replica_cleanup_path.py](../tests/integration/test_late_output_replica_cleanup_path.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 92 | `test_late_sealed_secondary_is_rejected_then_cleaned_after_real_consumer_cancellation` | late sealed secondary被拒绝、消费者不执行且托管清理闭合是当前正确性；设置却使用两输出producer和旧GCSpublication family。 |

### tests/integration/test_output_surviving_replica_path.py

文件：[test_output_surviving_replica_path.py](../tests/integration/test_output_surviving_replica_path.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 108 | `test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss` | 存活STORED replica优先保留、不应凭primary死亡重执行有价值；当前文件用混合两输出加旧graph/owner-decision family。 |

### tests/unit/test_borrowed_object_refs.py

文件：[test_borrowed_object_refs.py](../tests/unit/test_borrowed_object_refs.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 183 | `test_export_session_is_lazy_transactional_and_keeps_committed_pin` | Removed ReferenceExportSession generic export API |
| 220 | `test_exporting_ref_without_bound_owner_endpoint_creates_no_pin` | Removed generic ReferenceExportSession API; endpoint identity invariant belongs supported discovery |

### tests/unit/test_bounded_test_modes.py

文件：[test_bounded_test_modes.py](../tests/unit/test_bounded_test_modes.py)。当前验收选择此文件，处置时同步修改清单。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 207 | `test_mixed_files_have_no_inherited_unit_marker_and_every_case_is_classified` | 固定七份旧协议文件的历史marker分类；通用隔离规则保留，旧文件名单退出。 |
| 240 | `test_parameterized_background_cases_are_exact_single_fault_selectors` | 锁定旧publication_owner_death_control的两个参数形态及历史allowlist；该协议族已替换。 |

### tests/unit/test_collection_safety.py

文件：[test_collection_safety.py](../tests/unit/test_collection_safety.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 127 | `test_mixed_classification_keeps_original_cases_without_inherited_unit` | Staticoriginalfunctionset/count/parameterdecoratorlockisobsoletegovernance; retainnoinheritedunitconceptinconsolidatedguard |
| 150 | `test_original_threaded_node_drain_cases_remain_nonunit_after_bounded_review` | Staticoriginalfunctionset/count/parameterdecoratorlockisobsoletegovernance; retainnoinheritedunitconceptinconsolidatedguard |
| 166 | `test_node_blocking_classification_preserves_six_unit_cases_and_one_bounded_race` | Staticoriginalfunctionset/count/parameterdecoratorlockisobsoletegovernance; retainnoinheritedunitconceptinconsolidatedguard |
| 188 | `test_core_blocking_original_six_functions_seven_cases_use_reviewed_pure_composition` | Staticoriginalfunctionset/count/parameterdecoratorlockisobsoletegovernance; retainnoinheritedunitconceptinconsolidatedguard |
| 221 | `test_contained_cycle_concurrency_cannot_inherit_the_pure_policy_marker` | Staticoriginalfunctionset/count/parameterdecoratorlockisobsoletegovernance; retainnoinheritedunitconceptinconsolidatedguard |
| 235 | `test_node_death_api_runtime_keeps_two_real_thread_cases_out_of_unit` | Staticoriginalfunctionset/count/parameterdecoratorlockisobsoletegovernance; retainnoinheritedunitconceptinconsolidatedguard |
| 250 | `test_trace_export_keeps_four_real_filesystem_contracts_separate_from_pure_rejections` | Staticoriginalfunctionset/count/parameterdecoratorlockisobsoletegovernance; retainnoinheritedunitconceptinconsolidatedguard |

### tests/unit/test_contained_cycle_policy.py

文件：[test_contained_cycle_policy.py](../tests/unit/test_contained_cycle_policy.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 167 | `test_abort_of_unseen_transaction_is_a_non_mutating_replay` | 旧unseen edge-only abort不留记录；当前完整preBegin Fence必须有精确tombstone。 |
| 236 | `test_multi_container_batch_reserves_one_manifest_atomically` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_contained_edge_gc.py

文件：[test_contained_edge_gc.py](../tests/unit/test_contained_edge_gc.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 163 | `test_legacy_commit_without_outer_id_retains_pin_but_has_no_edge_metadata` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 182 | `test_multi_return_rejection_rolls_back_every_discovered_export_pin` | 仅已删除ReferenceExportSession的人工caller-rejection pin回滚，非当前multi-return拒绝API。 |

### tests/unit/test_contained_graph_manifest_boundaries.py

文件：[test_contained_graph_manifest_boundaries.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_contained_graph_manifest_boundaries.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 48 | `test_full_manifest_cannot_be_downgraded_or_released_through_edge_only_apis` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_contained_graph_protocol.py

文件：[test_contained_graph_protocol.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_contained_graph_protocol.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 185 | `test_gcs_generic_routes_and_typed_dispatch_share_one_publication_authority` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_contained_pin_owner_identity.py

文件：[test_contained_pin_owner_identity.py](../tests/unit/test_contained_pin_owner_identity.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 36 | `test_typed_hold_snapshot_keeps_authority_and_legacy_projection` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 139 | `test_dead_fence_rejects_late_typed_hold_but_never_guesses_legacy_owner` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_core_actor_restart.py

文件：[test_core_actor_restart.py](../tests/unit/test_core_actor_restart.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 160 | `test_node_loss_migration_reuses_route_fence_and_sequence_reset` | Installs ActorNodeLossRecord then ALIVE on another Node, contradicts current DEAD |

### tests/unit/test_core_output_lease_domain.py

文件：[test_core_output_lease_domain.py](../tests/unit/test_core_output_lease_domain.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 110 | `test_targeted_pre_push_loss_preserves_exact_subset_and_healthy_siblings` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_core_output_node_loss.py

文件：[test_core_output_node_loss.py](../tests/unit/test_core_output_node_loss.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 436 | `test_task_attempt_fences_old_target_receipt_even_when_its_slots_are_unchanged` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 474 | `test_new_sibling_attempt_does_not_abandon_committed_batch_adoption_tail` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_core_output_receipt_loss.py

文件：[test_core_output_receipt_loss.py](../tests/unit/test_core_output_receipt_loss.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 175 | `test_owner_routed_targeted_retirement_never_holds_core_lock_across_rpc` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_core_reconstruction_runtime.py

文件：[test_core_reconstruction_runtime.py](../tests/unit/test_core_reconstruction_runtime.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 346 | `test_multi_return_all_lost_requeues_full_manifest_from_nonzero_sibling` | Targeted/sibling session path retired |
| 409 | `test_multi_return_partial_loss_opens_then_starts_only_lost_target` | Targeted/sibling session path retired |
| 472 | `test_partial_target_success_publishes_only_target_and_preserves_healthy` | Targeted/sibling session path retired |
| 523 | `test_late_loss_starts_second_session_with_distinct_lifecycle_key` | Targeted/sibling session path retired |
| 580 | `test_target_start_renews_foreign_lineage_before_owner_attempt_commit` | Targeted/sibling session path retired |
| 698 | `test_target_start_installs_renewed_foreign_guards_and_nested_hold` | Targeted/sibling session path retired |
| 761 | `test_target_open_waiting_backs_off_and_definitive_failure_wakes_targets` | Targeted/sibling session path retired |
| 809 | `test_targeted_explicit_system_error_queries_node_before_retry` | Targeted/sibling session path retired |
| 887 | `test_multi_return_reconstruction_error_wakes_every_sibling` | Targeted/sibling session path retired |

### tests/unit/test_core_worker_crash_recovery.py

文件：[test_core_worker_crash_recovery.py](../tests/unit/test_core_worker_crash_recovery.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 381 | `test_partial_drop_ack_replays_only_missing_replica_before_retry` | Two sibling result slots partial deletion retired; multi-replica cleanup is different and still retained |

### tests/unit/test_inline_publication_gate.py

文件：[test_inline_publication_gate.py](../tests/unit/test_inline_publication_gate.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 167 | `test_execution_scope_is_part_of_one_shot_publication_identity` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_multi_container_graph_protocol.py

文件：[test_multi_container_graph_protocol.py](../tests/unit/test_multi_container_graph_protocol.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 57 | `test_two_containers_release_independently_under_one_manifest_identity` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 133 | `test_release_request_keeps_existing_wire_aliases_and_pickle_validation` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_multi_return_owner_model.py

文件：[test_multi_return_owner_model.py](../tests/unit/test_multi_return_owner_model.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 97 | `test_manifest_rejects_missing_reordered_or_foreign_slots` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 117 | `test_batch_registration_validation_is_side_effect_free_and_commit_is_atomic` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 140 | `test_partial_registration_is_rejected_without_filling_missing_siblings` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 158 | `test_success_manifest_validation_and_commit_cover_mixed_storage_atomically` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 190 | `test_success_manifest_requires_exact_order_and_one_owner_before_mutation` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 233 | `test_stored_replay_binds_complete_descriptor_without_partial_mutation` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 279 | `test_stored_replay_attempt_drift_is_fenced_without_mutation` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 298 | `test_validated_success_plan_rechecks_and_refuses_partial_visibility` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 322 | `test_attempt_advance_validates_all_siblings_before_one_commit` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 351 | `test_attempt_advance_rejects_one_unready_sibling_without_advancing_any` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 367 | `test_task_error_validation_commit_and_replay_are_manifest_atomic` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 389 | `test_validated_error_plan_rechecks_and_never_partially_fails_siblings` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |
| 406 | `test_recovery_lineage_survives_until_the_last_sibling_is_collected` | Multi-output/sibling fixture or atomic mixed-slot assertion must retire/rewrite for single output |

### tests/unit/test_nested_argument_manifest.py

文件：[test_nested_argument_manifest.py](../tests/unit/test_nested_argument_manifest.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 188 | `test_stored_arg_is_a_readiness_edge_but_keeps_nested_refs_non_gating` | StoredArg form/materialize_stored retired, retain invariant via explicit RefArg materialization if needed |
| 225 | `test_stored_arg_malformed_manifest_rolls_back_acquired_handles` | StoredArg form/materialize_stored retired, retain invariant via explicit RefArg materialization if needed |
| 255 | `test_stored_arg_never_uses_the_already_decoded_ref_value_loader` | StoredArg form/materialize_stored retired, retain invariant via explicit RefArg materialization if needed |
| 421 | `test_task_spec_rejects_cross_storage_nested_owner_conflicts` | StoredArg form/materialize_stored retired, retain invariant via explicit RefArg materialization if needed |

### tests/unit/test_output_discovery.py

文件：[test_output_discovery.py](../tests/unit/test_output_discovery.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 159 | `test_same_python_ref_memoizes_per_slot_but_siblings_have_independent_holds` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 262 | `test_target_subset_uses_original_slot_indices_and_stable_tokens` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 305 | `test_failure_in_later_slot_clears_prior_source_custody_and_forbids_retry` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 386 | `test_late_slot_identity_conflict_aborts_the_complete_batch_before_effects` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_owner_publication.py

文件：[test_output_owner_publication.py](../tests/unit/test_output_owner_publication.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 172 | `test_mixed_batch_publishes_once_without_retaining_sibling_payloads` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 294 | `test_one_stale_selected_slot_fences_entire_batch` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 309 | `test_late_slot_conflict_cannot_publish_earlier_slots` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 349 | `test_targeted_batch_changes_only_selected_slots_with_original_indices` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 367 | `test_targeted_commit_does_not_block_on_healthy_sibling_collection` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 380 | `test_targeted_slot_collection_keeps_healthy_siblings_and_task_lineage` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 418 | `test_new_targeted_publication_cannot_replace_unretired_membership` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 474 | `test_each_slot_collects_only_its_own_edges_and_never_keeps_sibling_bytes` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 535 | `test_empty_edge_slot_needs_no_graph_ack_even_when_siblings_have_refs` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 551 | `test_shared_child_holds_and_task_lineage_survive_until_their_own_last_slot` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_owner_resolution_preflight.py

文件：[test_output_owner_resolution_preflight.py](../tests/unit/test_output_owner_resolution_preflight.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 130 | `test_bad_second_unreceived_slot_cannot_erase_metadata_or_partially_resolve_batch` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_owner_retirement.py

文件：[test_output_owner_retirement.py](../tests/unit/test_output_owner_retirement.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 305 | `test_one_retirement_vector_can_span_old_publications_and_attempts` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 342 | `test_post_retirement_gc_collects_metadata_only_and_preserves_sibling_lineage` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_owner_surviving_replica.py

文件：[test_output_owner_surviving_replica.py](../tests/unit/test_output_owner_surviving_replica.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 346 | `test_bad_last_stored_slot_cannot_partially_apply_a_keep_batch` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_protocol.py

文件：[test_output_protocol.py](../tests/unit/test_output_protocol.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 286 | `test_mixed_unified_envelopes_work_on_all_terminal_boundaries` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 368 | `test_retired_wire_layout_is_rejected_instead_of_shifting_positional_authorities` | 锁旧wire positional layout而非当前身份语义；按当前拒绝畸形wire重写。 |

### tests/unit/test_output_publication.py

文件：[test_output_publication.py](../tests/unit/test_output_publication.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 126 | `test_mixed_slots_form_one_metadata_graph_and_one_data_plane_envelope` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 147 | `test_cross_slot_shared_children_have_distinct_holds_and_independent_gc` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 176 | `test_single_slot_is_a_degenerate_batch_not_a_distinct_protocol` | single作为多output特例的设计断言已退出；现在single是唯一合同。 |
| 188 | `test_targeted_identity_keeps_original_indices_and_full_canonical_manifest` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_publication_control.py

文件：[test_output_publication_control.py](../tests/unit/test_output_publication_control.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 67 | `test_normal_gcs_constructs_only_unified_publication_authorities_and_routes` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 154 | `test_node_death_freezes_metadata_and_blocks_late_terminal_or_graph` | 旧Node-death把准确late terminal一律拒绝；当前允许历史C4，不允许前进。 |

### tests/unit/test_output_publication_journal.py

文件：[test_output_publication_journal.py](../tests/unit/test_output_publication_journal.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 127 | `test_one_journal_completes_mixed_slots_with_exact_data_plane_replay` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 150 | `test_no_refs_and_targeted_indices_reuse_same_lifecycle` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 404 | `test_slot_cleanup_retires_only_one_payload_and_cannot_rebuild_partial_envelope` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 433 | `test_all_inline_siblings_keep_independent_payload_custody` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 473 | `test_adoption_of_one_slot_never_discards_unretired_sibling_payload` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 516 | `test_conflicting_last_slot_prevents_partial_bulk_payload_retirement` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_publication_node.py

文件：[test_output_publication_node.py](../tests/unit/test_output_publication_node.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 400 | `test_later_slot_bad_bytes_has_zero_journal_or_external_effects` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 488 | `test_mixed_batch_owner_adoption_then_per_slot_gc_preserves_sibling_child_holds` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_output_recovery.py

文件：[test_output_recovery.py](../tests/unit/test_output_recovery.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 163 | `test_mixed_no_refs_and_targeted_slots_share_the_same_registry` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 204 | `test_node_death_freezes_exact_phase_and_late_forward_reports_never_upgrade_it` | 旧freeze_node_death禁止所有terminal_report；新历史C4与前进许可分开。 |
| 356 | `test_late_unreported_rollback_is_fenced_by_node_loss_not_installed_as_history` | 旧冻结workset禁止迟到rollback历史，须比较新firstFence/cleanup，不原样套用。 |
| 365 | `test_adoption_before_terminal_outbox_records_complete_without_reopening_effects` | 旧adoption可以补录Complete；当前C7先要求准确C4+C5。 |
| 396 | `test_slot_collection_is_independent_and_later_adoption_never_erases_its_proof` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 421 | `test_unknown_complete_owner_decision_is_per_slot_and_does_not_upgrade_frozen_history` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 443 | `test_owner_decision_requires_full_selected_vector_exact_owner_and_keep_witness` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 463 | `test_drop_vector_needs_no_invented_complete_and_collected_slot_cannot_be_kept` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 526 | `test_owner_death_after_mixed_decision_preserves_vector_and_fences_replay_authorization` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_public_multi_return_runtime.py

文件：[test_public_multi_return_runtime.py](../tests/unit/test_public_multi_return_runtime.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 148 | `test_public_remote_returns_single_ref_or_ordered_tuple` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 185 | `test_submission_registers_all_entries_waiters_refs_and_task_lineage` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 606 | `test_ordered_mixed_success_publishes_and_wakes_all_siblings_atomically` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 810 | `test_core_stored_replay_drift_cannot_overwrite_any_descriptor` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 878 | `test_invalid_success_manifest_publishes_no_sibling` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 920 | `test_application_error_publishes_same_terminal_error_to_all_siblings` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 940 | `test_system_retry_advances_all_siblings_once_and_preserves_task_key` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 982 | `test_retry_owner_preflight_failure_consumes_no_recovery_budget` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 1115 | `test_owner_terminal_preflight_failure_does_not_mutate_recovery_or_siblings` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 1219 | `test_task_lifecycle_tables_use_task_id_not_slot_zero` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 1502 | `test_dependency_lineage_is_task_scoped_and_releases_with_last_sibling` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 1710 | `test_concurrent_sibling_closes_claim_task_lineage_exactly_once` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 1740 | `test_static_multi_return_contained_refs_require_unified_publication` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |
| 1779 | `test_collecting_stored_sibling_does_not_own_or_move_task_lineage` | Multi-return/sibling-shaped behavior removed; preserve its general identity/atomicity assertion only after explicit single-output mapping |

### tests/unit/test_publication_control_boundary.py

文件：[test_publication_control_boundary.py](../tests/unit/test_publication_control_boundary.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 107 | `test_service_uses_only_public_adapter_methods_and_current_callbacks` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_publication_sources.py

文件：[test_publication_sources.py](../tests/unit/test_publication_sources.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 91 | `test_historical_reexports_preserve_one_shared_type_or_function` | 要求已删除stored_publication reexport兼容。 |
| 181 | `test_old_pickle_global_names_resolve_to_shared_values_without_changing_class` | 要求已删除模块的pickle GLOBAL可解码兼容。 |
| 124 | `test_source_fingerprint_preserves_v1_framing_golden；仅 kind == 'legacy-contained'` | 仅退出legacy/多slot/target参数；typed source或single参数保留并迁移。 |
| 161 | `test_shared_values_and_pin_messages_pickle_with_exact_identity；仅 kind == 'legacy-contained'` | 仅退出legacy/多slot/target参数；typed source或single参数保留并迁移。 |

### tests/unit/test_reconstruction_runtime.py

文件：[test_reconstruction_runtime.py](../tests/unit/test_reconstruction_runtime.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 193 | `test_stored_arg_reconstruction_rewrites_hold_and_keeps_storage_dependency` | Removed StoredArg input form |
| 245 | `test_multi_return_request_from_nonzero_sibling_advances_whole_manifest` | Nonzero sibling all-output reconstruction removed |
| 276 | `test_partial_multi_return_loss_is_rejected_before_any_mutation` | Partial sibling loss mode removed |

### tests/unit/test_recovery.py

文件：[test_recovery.py](../tests/unit/test_recovery.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 146 | `test_targeted_terminal_failure_preserves_task_lineage_and_remaining_budget` | FAIL_RECONSTRUCTION_TARGETS and later healthy sibling reconstruction explicitly retired |
| 102 | `test_reconstruction_keeps_logical_ids_and_merges_all_outputs_by_task` | Cross-sibling JOIN shape removed; keep single-output same-object JOIN invariant |

### tests/unit/test_reviewed_pure_runner.py

文件：[test_reviewed_pure_runner.py](../tests/unit/test_reviewed_pure_runner.py)。当前验收选择此文件，处置时同步修改清单。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 51 | `test_manifest_freezes_reviewed_scope_and_explicit_reviewed_extensions` | 冻结455/201/254等旧集合及已退出multi/targeted selector；不保护当前执行结果。 |

### tests/unit/test_same_owner_output_custody.py

文件：[test_same_owner_output_custody.py](../tests/unit/test_same_owner_output_custody.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 169 | `test_same_owner_discovery_promotes_and_collects_each_selected_shared_child_slot` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_stored_intent_gate.py

文件：[test_stored_intent_gate.py](../tests/unit/test_stored_intent_gate.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 97 | `test_targeted_arrival_frame_preserves_full_manifest_and_selected_indices` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 157 | `test_full_and_all_selected_targeted_executions_keep_distinct_scope` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 189 | `test_frame_rejects_noncanonical_scope_bitmap_and_phase` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 139 | `test_fixed_frame_round_trip_preserves_full_or_targeted_execution；仅 count != 1 or targets is not None` | 仅退出legacy/多slot/target参数；typed source或single参数保留并迁移。 |

### tests/unit/test_targeted_output_publication.py

文件：[test_targeted_output_publication.py](../tests/unit/test_targeted_output_publication.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 163 | `test_unified_targeted_success_preflight_and_commit_preserve_sibling` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 236 | `test_known_lost_complete_rewakes_open_successor_but_fences_started_successor` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_targeted_owner_defer.py

文件：[test_targeted_owner_defer.py](../tests/unit/test_targeted_owner_defer.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 359 | `test_new_lost_sibling_is_queued_next_not_joined_to_unrelated_started_target` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_targeted_reconstruction_first_ack.py

文件：[test_targeted_reconstruction_first_ack.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_reconstruction_first_ack.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 538 | `test_new_lost_slot_is_queued_not_joined_to_another_started_target` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_targeted_retirement_admission.py

文件：[test_targeted_retirement_admission.py](../tests/unit/test_targeted_retirement_admission.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 314 | `test_retiring_stored_target_does_not_touch_lost_inline_sibling_behind_finish_gate` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 347 | `test_target_merged_during_retirement_is_repreflighted_before_any_attempt_advance` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 641 | `test_all_lost_sibling_request_cannot_bypass_existing_targeted_failure_latch` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 726 | `test_parent_lineage_cannot_reconstruct_producer_with_active_targeted_session` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_task_finish_barrier.py

文件：[test_task_finish_barrier.py](../tests/unit/test_task_finish_barrier.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 789 | `test_system_retry_moves_every_barrier_but_preserves_logical_hold` | Multi-output barrier fanout retired; single-output hold preservation remains |

### tests/unit/test_trace_contract.py

文件：[test_trace_contract.py](../tests/unit/test_trace_contract.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 606 | `test_publication_stages_select_four_distinct_request_edges` | Exact four-stage count contradicts current six-stage GCS success path; replace with current distinct-stage rule, not remove causal uniqueness guarantee. |

### tests/unit/test_worker_completion_paths.py

文件：[test_worker_completion_paths.py](../tests/unit/test_worker_completion_paths.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 385 | `test_reconstructed_multi_return_executes_once_and_returns_full_manifest` | Task num_returns3 and three independently decoded output slots retired |

### tests/unit/test_worker_nested_task_arguments.py

文件：[test_worker_nested_task_arguments.py](../tests/unit/test_worker_nested_task_arguments.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 406 | `test_inline_and_stored_arguments_share_rollback_on_later_decode_failure` | Removed StoredArg branch; retain rollback ordering via current RefArg paths |
| 460 | `test_stored_stream_failure_rolls_back_previously_imported_inline_handle` | Removed StoredArg stream decoder; same corruption/cleanup invariant needs current materialization |
| 511 | `test_stored_argument_decodes_pulled_stream_with_nested_import_session` | Removed direct StoredArg decoder API |

### tests/unit/test_worker_output_discovery.py

文件：[test_worker_output_discovery.py](../tests/unit/test_worker_output_discovery.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 77 | `test_multi_contained_outputs_publish_once_after_all_selected_slots_discover` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 127 | `test_bad_later_selected_slot_releases_discovery_custody_without_publication_effect` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

### tests/unit/test_worker_stored_publication.py

文件：[test_worker_stored_publication.py](../tests/unit/test_worker_stored_publication.py)。当前验收未直接选择此文件。

| 行 | 函数/参数 | 当前形态过期的原因 |
|---:|---|---|
| 301 | `test_batch_digest_binds_later_slot_metadata_before_any_replay_effect` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |
| 322 | `test_later_serialization_failure_creates_no_node_publication_or_pin` | 已退出targeted/multi-slot/shared-batch/legacy接口或旧authority形状；其中通用不变量不得随旧接口一起丢失。 |

## 4. 不能整份删除的文件

75个混合迁移文件、45个仅绑定失效文件、5个只标出部分过期函数的文件分别如下。类别互斥；第三节是其函数级子集。
“明确导入/绑定过期”与“业务目标没有价值”是不同判断。必须保留仍适用的测试合同。

| 文件 | 分类 | 过期函数/参数条目数 |
|---|---|---:|
| [test_abandoned_dependency_custody_path.py](../tests/integration/test_abandoned_dependency_custody_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_ambiguous_grant_custody_path.py](../tests/integration/test_ambiguous_grant_custody_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_borrowed_output_unknown_path.py](../tests/integration/test_borrowed_output_unknown_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_contained_cycle_control_path.py](../tests/integration/test_contained_cycle_control_path.py) | 混合：先提取有效合同 | 1 |
| [test_core_reconstruction_concurrency.py](../tests/integration/test_core_reconstruction_concurrency.py) | 仅指定函数过期 | 1 |
| [test_cross_cleanup_receipt_path.py](../tests/integration/test_cross_cleanup_receipt_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_foreign_late_output_replica_cleanup_path.py](../tests/integration/test_foreign_late_output_replica_cleanup_path.py) | 混合：先提取有效合同 | 1 |
| [test_inline_node_loss_path.py](../tests/integration/test_inline_node_loss_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_late_output_replica_cleanup_path.py](../tests/integration/test_late_output_replica_cleanup_path.py) | 混合：先提取有效合同 | 1 |
| [test_local_replica_handoff_failure_path.py](../tests/integration/test_local_replica_handoff_failure_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_multi_owner_handoff_failure_path.py](../tests/integration/test_multi_owner_handoff_failure_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_output_child_owner_worker_loss_path.py](../tests/integration/test_output_child_owner_worker_loss_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_output_owner_death_path.py](../tests/integration/test_output_owner_death_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_output_surviving_replica_path.py](../tests/integration/test_output_surviving_replica_path.py) | 混合：先提取有效合同 | 1 |
| [test_pg_publication_peer_loss_path.py](../tests/integration/test_pg_publication_peer_loss_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_pregrant_custody_path.py](../tests/integration/test_pregrant_custody_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_stored_outer_node_loss_path.py](../tests/integration/test_stored_outer_node_loss_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_stored_outer_publication_path.py](../tests/integration/test_stored_outer_publication_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_transfer_pin_ack_loss_path.py](../tests/integration/test_transfer_pin_ack_loss_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_transfer_pin_requester_death_path.py](../tests/integration/test_transfer_pin_requester_death_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_unreported_complete_node_loss_path.py](../tests/integration/test_unreported_complete_node_loss_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_worker_lease_locality_path.py](../tests/integration/test_worker_lease_locality_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_worker_owner_node_loss_path.py](../tests/integration/test_worker_owner_node_loss_path.py) | 绑定失效：目标仍保留 | 0 |
| [test_blocking_notifier_entry_failure.py](../tests/unit/test_blocking_notifier_entry_failure.py) | 绑定失效：目标仍保留 | 0 |
| [test_borrowed_object_refs.py](../tests/unit/test_borrowed_object_refs.py) | 混合：先提取有效合同 | 2 |
| [test_bounded_test_modes.py](../tests/unit/test_bounded_test_modes.py) | 仅指定函数过期 | 2 |
| [test_cancelled_grant_inventory.py](../tests/unit/test_cancelled_grant_inventory.py) | 混合：先提取有效合同 | 0 |
| [test_cluster_shutdown_barrier.py](../tests/unit/test_cluster_shutdown_barrier.py) | 绑定失效：目标仍保留 | 0 |
| [test_collection_safety.py](../tests/unit/test_collection_safety.py) | 混合：先提取有效合同 | 7 |
| [test_contained_cycle_policy.py](../tests/unit/test_contained_cycle_policy.py) | 混合：先提取有效合同 | 2 |
| [test_contained_edge_gc.py](../tests/unit/test_contained_edge_gc.py) | 混合：先提取有效合同 | 2 |
| [test_contained_edge_runtime.py](../tests/unit/test_contained_edge_runtime.py) | 混合：先提取有效合同 | 0 |
| [test_contained_graph_manifest_boundaries.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_contained_graph_manifest_boundaries.py) | 混合：先提取有效合同 | 1 |
| [test_contained_graph_protocol.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_contained_graph_protocol.py) | 混合：先提取有效合同 | 1 |
| [test_contained_pin_owner_identity.py](../tests/unit/test_contained_pin_owner_identity.py) | 混合：先提取有效合同 | 2 |
| [test_core_actor_restart.py](../tests/unit/test_core_actor_restart.py) | 仅指定函数过期 | 1 |
| [test_core_lost_blocking_lock_order.py](../tests/unit/test_core_lost_blocking_lock_order.py) | 绑定失效：目标仍保留 | 0 |
| [test_core_node_death_recovery.py](../tests/unit/test_core_node_death_recovery.py) | 绑定失效：目标仍保留 | 0 |
| [test_core_output_lease_domain.py](../tests/unit/test_core_output_lease_domain.py) | 混合：先提取有效合同 | 1 |
| [test_core_output_node_loss.py](../tests/unit/test_core_output_node_loss.py) | 混合：先提取有效合同 | 2 |
| [test_core_output_publication.py](../tests/unit/test_core_output_publication.py) | 混合：先提取有效合同 | 0 |
| [test_core_output_receipt_loss.py](../tests/unit/test_core_output_receipt_loss.py) | 混合：先提取有效合同 | 1 |
| [test_core_output_surviving_replica.py](../tests/unit/test_core_output_surviving_replica.py) | 混合：先提取有效合同 | 0 |
| [test_core_placement_group_scheduling.py](../tests/unit/test_core_placement_group_scheduling.py) | 绑定失效：目标仍保留 | 0 |
| [test_core_reconstruction_runtime.py](../tests/unit/test_core_reconstruction_runtime.py) | 混合：先提取有效合同 | 9 |
| [test_core_stored_publication_adoption.py](../tests/unit/test_core_stored_publication_adoption.py) | 混合：先提取有效合同 | 0 |
| [test_core_worker_crash_recovery.py](../tests/unit/test_core_worker_crash_recovery.py) | 混合：先提取有效合同 | 1 |
| [test_dead_worker_reference_cleanup.py](../tests/unit/test_dead_worker_reference_cleanup.py) | 绑定失效：目标仍保留 | 0 |
| [test_foreign_inline_task_dependencies.py](../tests/unit/test_foreign_inline_task_dependencies.py) | 绑定失效：目标仍保留 | 0 |
| [test_foreign_reconstruction_runtime.py](../tests/unit/test_foreign_reconstruction_runtime.py) | 绑定失效：目标仍保留 | 0 |
| [test_foreign_stored_object_refs.py](../tests/unit/test_foreign_stored_object_refs.py) | 混合：先提取有效合同 | 0 |
| [test_foreign_stored_task_dependencies.py](../tests/unit/test_foreign_stored_task_dependencies.py) | 混合：先提取有效合同 | 0 |
| [test_foreign_task_finish_barrier.py](../tests/unit/test_foreign_task_finish_barrier.py) | 绑定失效：目标仍保留 | 0 |
| [test_foreign_wait_drop.py](../tests/unit/test_foreign_wait_drop.py) | 绑定失效：目标仍保留 | 0 |
| [test_get_notification_deadline.py](../tests/unit/test_get_notification_deadline.py) | 绑定失效：目标仍保留 | 0 |
| [test_inline_publication_gate.py](../tests/unit/test_inline_publication_gate.py) | 混合：先提取有效合同 | 1 |
| [test_inline_publication_node_server.py](../tests/unit/test_inline_publication_node_server.py) | 混合：先提取有效合同 | 0 |
| [test_inline_recovery.py](../tests/unit/test_inline_recovery.py) | 混合：先提取有效合同 | 0 |
| [test_late_cleanup_shutdown.py](../tests/unit/test_late_cleanup_shutdown.py) | 绑定失效：目标仍保留 | 0 |
| [test_lease_completion_handshake.py](../tests/unit/test_lease_completion_handshake.py) | 绑定失效：目标仍保留 | 0 |
| [test_lease_dependency_inventory.py](../tests/unit/test_lease_dependency_inventory.py) | 混合：先提取有效合同 | 0 |
| [test_lease_dependency_registry.py](../tests/unit/test_lease_dependency_registry.py) | 绑定失效：目标仍保留 | 0 |
| [test_location_report_custody.py](../tests/unit/test_location_report_custody.py) | 绑定失效：目标仍保留 | 0 |
| [test_multi_container_graph_protocol.py](../tests/unit/test_multi_container_graph_protocol.py) | 混合：先提取有效合同 | 2 |
| [test_multi_return_owner_model.py](../tests/unit/test_multi_return_owner_model.py) | 混合：先提取有效合同 | 13 |
| [test_nested_argument_manifest.py](../tests/unit/test_nested_argument_manifest.py) | 混合：先提取有效合同 | 4 |
| [test_node_blocking_get_authority.py](../tests/unit/test_node_blocking_get_authority.py) | 绑定失效：目标仍保留 | 0 |
| [test_node_lease_execution.py](../tests/unit/test_node_lease_execution.py) | 绑定失效：目标仍保留 | 0 |
| [test_node_publication_owner_death_finalize.py](../tests/unit/test_node_publication_owner_death_finalize.py) | 绑定失效：目标仍保留 | 0 |
| [test_object_ownership.py](../tests/unit/test_object_ownership.py) | 绑定失效：目标仍保留 | 0 |
| [test_output_control_shutdown.py](../tests/unit/test_output_control_shutdown.py) | 混合：先提取有效合同 | 0 |
| [test_output_dead_child_cleanup.py](../tests/unit/test_output_dead_child_cleanup.py) | 绑定失效：目标仍保留 | 0 |
| [test_output_discovery.py](../tests/unit/test_output_discovery.py) | 混合：先提取有效合同 | 4 |
| [test_output_node_loss_control.py](../tests/unit/test_output_node_loss_control.py) | 混合：先提取有效合同 | 0 |
| [test_output_owner_death_node.py](../tests/unit/test_output_owner_death_node.py) | 绑定失效：目标仍保留 | 0 |
| [test_output_owner_publication.py](../tests/unit/test_output_owner_publication.py) | 混合：先提取有效合同 | 10 |
| [test_output_owner_resolution_preflight.py](../tests/unit/test_output_owner_resolution_preflight.py) | 混合：先提取有效合同 | 1 |
| [test_output_owner_retired_fencing.py](../tests/unit/test_output_owner_retired_fencing.py) | 混合：先提取有效合同 | 0 |
| [test_output_owner_retirement.py](../tests/unit/test_output_owner_retirement.py) | 混合：先提取有效合同 | 2 |
| [test_output_owner_surviving_replica.py](../tests/unit/test_output_owner_surviving_replica.py) | 混合：先提取有效合同 | 1 |
| [test_output_owner_terminal_metadata.py](../tests/unit/test_output_owner_terminal_metadata.py) | 混合：先提取有效合同 | 0 |
| [test_output_protocol.py](../tests/unit/test_output_protocol.py) | 混合：先提取有效合同 | 2 |
| [test_output_publication.py](../tests/unit/test_output_publication.py) | 混合：先提取有效合同 | 4 |
| [test_output_publication_control.py](../tests/unit/test_output_publication_control.py) | 混合：先提取有效合同 | 2 |
| [test_output_publication_journal.py](../tests/unit/test_output_publication_journal.py) | 混合：先提取有效合同 | 6 |
| [test_output_publication_node.py](../tests/unit/test_output_publication_node.py) | 混合：先提取有效合同 | 2 |
| [test_output_publication_node_server.py](../tests/unit/test_output_publication_node_server.py) | 混合：先提取有效合同 | 0 |
| [test_output_recovery.py](../tests/unit/test_output_recovery.py) | 混合：先提取有效合同 | 9 |
| [test_output_replica_node.py](../tests/unit/test_output_replica_node.py) | 绑定失效：目标仍保留 | 0 |
| [test_owner_death_fence_control.py](../tests/unit/test_owner_death_fence_control.py) | 混合：先提取有效合同 | 0 |
| [test_owner_service.py](../tests/unit/test_owner_service.py) | 绑定失效：目标仍保留 | 0 |
| [test_public_multi_return_runtime.py](../tests/unit/test_public_multi_return_runtime.py) | 混合：先提取有效合同 | 14 |
| [test_publication_control_boundary.py](../tests/unit/test_publication_control_boundary.py) | 混合：先提取有效合同 | 1 |
| [test_publication_owner_death_control.py](../tests/unit/test_publication_owner_death_control.py) | 混合：先提取有效合同 | 0 |
| [test_publication_pg_dispatch.py](../tests/unit/test_publication_pg_dispatch.py) | 混合：先提取有效合同 | 0 |
| [test_publication_pg_loss_paths.py](../tests/unit/test_publication_pg_loss_paths.py) | 混合：先提取有效合同 | 0 |
| [test_publication_sources.py](../tests/unit/test_publication_sources.py) | 混合：先提取有效合同 | 4 |
| [test_publication_trace_observation.py](../tests/unit/test_publication_trace_observation.py) | 混合：先提取有效合同 | 0 |
| [test_reconstruction_runtime.py](../tests/unit/test_reconstruction_runtime.py) | 混合：先提取有效合同 | 3 |
| [test_recovery.py](../tests/unit/test_recovery.py) | 混合：先提取有效合同 | 2 |
| [test_recursive_reconstruction_planning.py](../tests/unit/test_recursive_reconstruction_planning.py) | 混合：先提取有效合同 | 0 |
| [test_reviewed_pure_runner.py](../tests/unit/test_reviewed_pure_runner.py) | 仅指定函数过期 | 1 |
| [test_same_owner_output_custody.py](../tests/unit/test_same_owner_output_custody.py) | 混合：先提取有效合同 | 1 |
| [test_stored_contained_owner_table.py](../tests/unit/test_stored_contained_owner_table.py) | 混合：先提取有效合同 | 0 |
| [test_stored_intent_gate.py](../tests/unit/test_stored_intent_gate.py) | 混合：先提取有效合同 | 4 |
| [test_stored_physical_gc.py](../tests/unit/test_stored_physical_gc.py) | 混合：先提取有效合同 | 0 |
| [test_stored_publication_node_death_gc.py](../tests/unit/test_stored_publication_node_death_gc.py) | 混合：先提取有效合同 | 0 |
| [test_stored_publication_node_server.py](../tests/unit/test_stored_publication_node_server.py) | 混合：先提取有效合同 | 0 |
| [test_targeted_output_publication.py](../tests/unit/test_targeted_output_publication.py) | 混合：先提取有效合同 | 2 |
| [test_targeted_owner_defer.py](../tests/unit/test_targeted_owner_defer.py) | 混合：先提取有效合同 | 1 |
| [test_targeted_reconstruction_first_ack.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_reconstruction_first_ack.py) | 混合：先提取有效合同 | 1 |
| [test_targeted_retirement_admission.py](../tests/unit/test_targeted_retirement_admission.py) | 混合：先提取有效合同 | 4 |
| [test_task_finish_barrier.py](../tests/unit/test_task_finish_barrier.py) | 混合：先提取有效合同 | 1 |
| [test_task_readiness_trace_contract.py](../tests/unit/test_task_readiness_trace_contract.py) | 混合：先提取有效合同 | 0 |
| [test_trace_contract.py](../tests/unit/test_trace_contract.py) | 混合：先提取有效合同 | 1 |
| [test_typed_borrow_sources.py](../tests/unit/test_typed_borrow_sources.py) | 混合：先提取有效合同 | 0 |
| [test_worker_completion_paths.py](../tests/unit/test_worker_completion_paths.py) | 仅指定函数过期 | 1 |
| [test_worker_crash_supervisor.py](../tests/unit/test_worker_crash_supervisor.py) | 绑定失效：目标仍保留 | 0 |
| [test_worker_inline_publication.py](../tests/unit/test_worker_inline_publication.py) | 绑定失效：目标仍保留 | 0 |
| [test_worker_materialized_contained_arguments.py](../tests/unit/test_worker_materialized_contained_arguments.py) | 绑定失效：目标仍保留 | 0 |
| [test_worker_nested_task_arguments.py](../tests/unit/test_worker_nested_task_arguments.py) | 混合：先提取有效合同 | 3 |
| [test_worker_output_discovery.py](../tests/unit/test_worker_output_discovery.py) | 混合：先提取有效合同 | 2 |
| [test_worker_side_core_contract.py](../tests/unit/test_worker_side_core_contract.py) | 绑定失效：目标仍保留 | 0 |
| [test_worker_stored_publication.py](../tests/unit/test_worker_stored_publication.py) | 混合：先提取有效合同 | 2 |
| [test_worker_unified_output.py](../tests/unit/test_worker_unified_output.py) | 混合：先提取有效合同 | 0 |

### 三个需单独迁移的helper

| helper | 问题与保留内容 | 导入点数 |
|---|---|---:|
| [_pure_core.py](../tests/unit/_pure_core.py) | Sharedhelperretainsoptional3output/partialtargetfixturebranch, but make_pure_coreanddefaultsingleoutputhelperarecurrentdependencies. No wholehelperdelete. | 77 |
| [_pure_node_output.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/_pure_node_output.py) | 引用已删除output_recovery，并构造旧adapter回调、target_execution及1–3输出；没有当前验收调用者。真实Node Complete/资源不重复归还的fixture意图仍有价值，先迁移调用者，不能凭helper失效删除其测试合同。 | 6 |
| [_pure_reference_output_runtime.py](../tests/unit/_pure_reference_output_runtime.py) | 子类仍使用push.target_execution、旧report_slot_collected/recovery对象和逐槽cleanup proof；Store与精确旧epoch Drop语义保留。父类_pure_output_runtime已迁移，不随之删除。 | 4 |

`_pure_output_runtime.py`已迁为当前单输出及增强协议，必须保留；不能按文件名前缀一起删除。

## 5. 90个直接失效的import文件：完整列表

模块文件缺席或源码顶层symbol不存在已逐项核对，共涉及47,366物理行。**这不是47,366行都无用，也不是90个测试case。**
与前述分类存在重叠，不能相加；当前验收选择中的直接失效文件为0。

| 文件 | 物理行 | 直接失效的模块或符号 |
|---|---:|---|
| [test_borrowed_output_unknown_path.py](../tests/integration/test_borrowed_output_unknown_path.py) | 432 | `miniray.output_recovery`（:42） |
| [test_contained_cycle_control_path.py](../tests/integration/test_contained_cycle_control_path.py) | 340 | `miniray.contained_cycle`（:33）；`miniray.output_recovery`（:44） |
| [test_core_reconstruction_concurrency.py](../tests/integration/test_core_reconstruction_concurrency.py) | 533 | `miniray.output_recovery`（:49） |
| [test_foreign_late_output_replica_cleanup_path.py](../tests/integration/test_foreign_late_output_replica_cleanup_path.py) | 541 | `miniray.output_recovery`（:50）；`miniray.task_outputs.TargetExecutionKey`（:55）；`miniray.task_outputs.TargetOutputManifest`（:55） |
| [test_inline_node_loss_path.py](../tests/integration/test_inline_node_loss_path.py) | 751 | `miniray.contained_cycle`（:40）；`miniray.control.COMMIT_CONTAINED_GRAPH_HANDLER`（:43）；`miniray.control.GET_CONTAINED_GRAPH_HANDLER`（:43）；`miniray.output_recovery`（:51）；`miniray.stored_publication`（:58） |
| [test_late_output_replica_cleanup_path.py](../tests/integration/test_late_output_replica_cleanup_path.py) | 379 | `miniray.output_recovery`（:40） |
| [test_mixed_borrowed_output_unknown_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_mixed_borrowed_output_unknown_path.py) | 466 | `miniray.output_recovery`（:41） |
| [test_multi_contained_output_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_contained_output_path.py) | 381 | `miniray.task_outputs.TargetExecutionKey`（:39） |
| [test_multi_output_node_loss_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_output_node_loss_path.py) | 400 | `miniray.output_recovery`（:40）；`miniray.task_outputs.TargetExecutionKey`（:44） |
| [test_multi_owner_handoff_failure_path.py](../tests/integration/test_multi_owner_handoff_failure_path.py) | 441 | `miniray.control.PROGRESS_PUBLICATION_OWNER_DEATH_HANDLER`（:32） |
| [test_multi_return_partial_reconstruction_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_multi_return_partial_reconstruction_path.py) | 332 | `miniray.task_outputs.TargetExecutionKey`（:40） |
| [test_output_child_owner_worker_loss_path.py](../tests/integration/test_output_child_owner_worker_loss_path.py) | 383 | `miniray.control.GET_CONTAINED_GRAPH_HANDLER`（:36） |
| [test_output_owner_death_path.py](../tests/integration/test_output_owner_death_path.py) | 421 | `miniray.control.GET_CONTAINED_GRAPH_HANDLER`（:40）；`miniray.stored_publication`（:47） |
| [test_output_surviving_replica_path.py](../tests/integration/test_output_surviving_replica_path.py) | 374 | `miniray.contained_cycle`（:31）；`miniray.control.GET_CONTAINED_GRAPH_HANDLER`（:34）；`miniray.control.RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER`（:34）；`miniray.output_recovery`（:39） |
| [test_stored_outer_node_loss_path.py](../tests/integration/test_stored_outer_node_loss_path.py) | 592 | `miniray.control.GET_CONTAINED_GRAPH_HANDLER`（:42）；`miniray.output_recovery`（:46）；`miniray.stored_publication`（:53） |
| [test_stored_outer_publication_path.py](../tests/integration/test_stored_outer_publication_path.py) | 536 | `miniray.contained_cycle`（:38）；`miniray.control.COMMIT_CONTAINED_GRAPH_HANDLER`（:42）；`miniray.control.RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER`（:42） |
| [test_targeted_borrowed_output_unknown_path.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/integration/test_targeted_borrowed_output_unknown_path.py) | 610 | `miniray.output_recovery`（:52）；`miniray.targeted_reconstruction`（:61）；`miniray.task_outputs.TargetExecutionKey`（:62） |
| [test_unreported_complete_node_loss_path.py](../tests/integration/test_unreported_complete_node_loss_path.py) | 477 | `miniray.output_recovery`（:48） |
| [test_worker_owner_node_loss_path.py](../tests/integration/test_worker_owner_node_loss_path.py) | 356 | `miniray.output_recovery`（:41） |
| [_pure_node_output.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/_pure_node_output.py) | 71 | `miniray.output_recovery`（:21） |
| [test_actor_arguments.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_actor_arguments.py) | 414 | `miniray.actor_arguments`（:8） |
| [test_borrowed_object_refs.py](../tests/unit/test_borrowed_object_refs.py) | 1360 | `miniray.contained_cycle`（:29）；`miniray.output_recovery`（:45）；`miniray.ref_transfer.ReferenceExportSession`（:55） |
| [test_cancelled_grant_inventory.py](../tests/unit/test_cancelled_grant_inventory.py) | 431 | `miniray.task_outputs.TargetExecutionKey`（:30）；`miniray.task_outputs.TargetOutputManifest`（:30） |
| [test_contained_cycle_policy.py](../tests/unit/test_contained_cycle_policy.py) | 248 | `miniray.contained_cycle`（:15） |
| [test_contained_edge_gc.py](../tests/unit/test_contained_edge_gc.py) | 241 | `miniray.ref_transfer.ReferenceExportSession`（:20） |
| [test_contained_edge_runtime.py](../tests/unit/test_contained_edge_runtime.py) | 1329 | `miniray.contained_cycle`（:48）；`miniray.output_recovery`（:60） |
| [test_contained_graph_manifest_boundaries.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_contained_graph_manifest_boundaries.py) | 192 | `miniray.contained_cycle`（:18） |
| [test_contained_graph_protocol.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_contained_graph_protocol.py) | 214 | `miniray.contained_cycle`（:21） |
| [test_contained_pin_owner_identity.py](../tests/unit/test_contained_pin_owner_identity.py) | 176 | `miniray.contained_edges.LegacyContainedReferenceHold`（:7） |
| [test_core_output_lease_domain.py](../tests/unit/test_core_output_lease_domain.py) | 280 | `miniray.targeted_reconstruction`（:25）；`miniray.task_outputs.TargetExecutionKey`（:26） |
| [test_core_output_node_loss.py](../tests/unit/test_core_output_node_loss.py) | 603 | `miniray.output_recovery`（:25） |
| [test_core_output_receipt_loss.py](../tests/unit/test_core_output_receipt_loss.py) | 197 | `miniray.output_recovery`（:14） |
| [test_core_output_surviving_replica.py](../tests/unit/test_core_output_surviving_replica.py) | 315 | `miniray.output_recovery`（:26） |
| [test_core_placement_group_scheduling.py](../tests/unit/test_core_placement_group_scheduling.py) | 1520 | `miniray.output_recovery`（:41） |
| [test_core_reconstruction_runtime.py](../tests/unit/test_core_reconstruction_runtime.py) | 913 | `miniray.core._DelayedTargetedReconstruction`（:21）；`miniray.core._StartTargetedReconstruction`（:21） |
| [test_core_stored_publication_adoption.py](../tests/unit/test_core_stored_publication_adoption.py) | 574 | `miniray.contained_cycle`（:24） |
| [test_core_worker_crash_recovery.py](../tests/unit/test_core_worker_crash_recovery.py) | 1145 | `miniray.output_recovery`（:45） |
| [test_foreign_stored_object_refs.py](../tests/unit/test_foreign_stored_object_refs.py) | 592 | `miniray.ref_transfer.ReferenceExportSession`（:29） |
| [test_foreign_stored_task_dependencies.py](../tests/unit/test_foreign_stored_task_dependencies.py) | 1870 | `miniray.ref_transfer.ReferenceExportSession`（:45） |
| [test_inline_publication_node_server.py](../tests/unit/test_inline_publication_node_server.py) | 588 | `miniray.contained_cycle`（:21） |
| [test_inline_recovery.py](../tests/unit/test_inline_recovery.py) | 304 | `miniray.output_recovery`（:35） |
| [test_late_cleanup_shutdown.py](../tests/unit/test_late_cleanup_shutdown.py) | 346 | `miniray.output_recovery`（:28） |
| [test_lease_completion_handshake.py](../tests/unit/test_lease_completion_handshake.py) | 492 | `miniray.output_recovery`（:24） |
| [test_lease_dependency_inventory.py](../tests/unit/test_lease_dependency_inventory.py) | 382 | `miniray.task_outputs.TargetExecutionKey`（:17）；`miniray.task_outputs.TargetOutputManifest`（:17） |
| [test_location_report_custody.py](../tests/unit/test_location_report_custody.py) | 408 | `miniray.output_recovery`（:30） |
| [test_multi_container_graph_protocol.py](../tests/unit/test_multi_container_graph_protocol.py) | 164 | `miniray.contained_cycle`（:12） |
| [test_multi_return_partial_seal_cleanup.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_multi_return_partial_seal_cleanup.py) | 472 | `miniray.output_recovery`（:34） |
| [test_nested_argument_manifest.py](../tests/unit/test_nested_argument_manifest.py) | 444 | `miniray.protocol.StoredArg`（:22） |
| [test_node_lease_execution.py](../tests/unit/test_node_lease_execution.py) | 371 | `miniray.output_recovery`（:25） |
| [test_output_control_shutdown.py](../tests/unit/test_output_control_shutdown.py) | 387 | `miniray.contained_cycle`（:20）；`miniray.output_recovery`（:28） |
| [test_output_discovery.py](../tests/unit/test_output_discovery.py) | 508 | `miniray.task_outputs.TargetExecutionKey`（:29）；`miniray.task_outputs.TargetOutputManifest`（:29） |
| [test_output_node_loss_control.py](../tests/unit/test_output_node_loss_control.py) | 498 | `miniray.contained_cycle`（:14）；`miniray.output_recovery`（:20） |
| [test_output_owner_publication.py](../tests/unit/test_output_owner_publication.py) | 684 | `miniray.contained_cycle`（:13）；`miniray.stored_publication`（:32）；`miniray.task_outputs.TargetExecutionKey`（:33）；`miniray.task_outputs.TargetOutputManifest`（:33） |
| [test_output_owner_resolution_preflight.py](../tests/unit/test_output_owner_resolution_preflight.py) | 171 | `miniray.output_recovery`（:26） |
| [test_output_owner_retired_fencing.py](../tests/unit/test_output_owner_retired_fencing.py) | 290 | `miniray.output_recovery`（:29） |
| [test_output_owner_retirement.py](../tests/unit/test_output_owner_retirement.py) | 397 | `miniray.ownership.TargetOutputAttemptAdvancePlan`（:17）；`miniray.task_outputs.TargetExecutionKey`（:24）；`miniray.task_outputs.TargetOutputManifest`（:24） |
| [test_output_owner_surviving_replica.py](../tests/unit/test_output_owner_surviving_replica.py) | 405 | `miniray.output_recovery`（:28） |
| [test_output_owner_terminal_metadata.py](../tests/unit/test_output_owner_terminal_metadata.py) | 289 | `miniray.contained_cycle`（:20） |
| [test_output_protocol.py](../tests/unit/test_output_protocol.py) | 476 | `miniray.output_recovery`（:22）；`miniray.task_outputs.TargetExecutionKey`（:27）；`miniray.task_outputs.TargetOutputManifest`（:27） |
| [test_output_publication.py](../tests/unit/test_output_publication.py) | 520 | `miniray.contained_cycle`（:13）；`miniray.task_outputs.TargetExecutionKey`（:29）；`miniray.task_outputs.TargetOutputManifest`（:29） |
| [test_output_publication_control.py](../tests/unit/test_output_publication_control.py) | 187 | `miniray.contained_cycle`（:11）；`miniray.output_recovery`（:16） |
| [test_output_publication_journal.py](../tests/unit/test_output_publication_journal.py) | 648 | `miniray.contained_cycle`（:11） |
| [test_output_publication_node.py](../tests/unit/test_output_publication_node.py) | 582 | `miniray.contained_cycle`（:18）；`miniray.output_recovery`（:31）；`miniray.stored_publication`（:34） |
| [test_output_recovery.py](../tests/unit/test_output_recovery.py) | 614 | `miniray.output_recovery`（:29） |
| [test_owner_service.py](../tests/unit/test_owner_service.py) | 213 | `miniray.stored_publication`（:13） |
| [test_public_multi_return_runtime.py](../tests/unit/test_public_multi_return_runtime.py) | 1829 | `miniray.output_recovery`（:67） |
| [test_publication_control_boundary.py](../tests/unit/test_publication_control_boundary.py) | 390 | `miniray.output_recovery`（:18） |
| [test_publication_owner_death_control.py](../tests/unit/test_publication_owner_death_control.py) | 393 | `miniray.output_recovery`（:35） |
| [test_publication_pg_loss_paths.py](../tests/unit/test_publication_pg_loss_paths.py) | 275 | `miniray.output_recovery`（:35） |
| [test_publication_sources.py](../tests/unit/test_publication_sources.py) | 269 | `miniray.contained_edges.LegacyContainedReferenceHold`（:22） |
| [test_publication_trace_observation.py](../tests/unit/test_publication_trace_observation.py) | 367 | `miniray.output_recovery`（:30） |
| [test_reconstruction_runtime.py](../tests/unit/test_reconstruction_runtime.py) | 338 | `miniray.protocol.StoredArg`（:7） |
| [test_same_owner_output_custody.py](../tests/unit/test_same_owner_output_custody.py) | 325 | `miniray.contained_cycle`（:27）；`miniray.output_recovery`（:37） |
| [test_stored_contained_owner_table.py](../tests/unit/test_stored_contained_owner_table.py) | 280 | `miniray.stored_publication`（:20） |
| [test_stored_intent_gate.py](../tests/unit/test_stored_intent_gate.py) | 307 | `miniray.task_outputs.TargetExecutionKey`（:25）；`miniray.task_outputs.TargetOutputManifest`（:25） |
| [test_stored_publication_node_server.py](../tests/unit/test_stored_publication_node_server.py) | 769 | `miniray.output_recovery`（:30） |
| [test_targeted_output_publication.py](../tests/unit/test_targeted_output_publication.py) | 374 | `miniray.output_recovery`（:28）；`miniray.targeted_reconstruction`（:34） |
| [test_targeted_owner_defer.py](../tests/unit/test_targeted_owner_defer.py) | 437 | `miniray.targeted_reconstruction`（:35） |
| [test_targeted_reconstruction.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_reconstruction.py) | 589 | `miniray.targeted_reconstruction`（:18）；`miniray.task_outputs.TargetExecutionKey`（:22） |
| [test_targeted_reconstruction_first_ack.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_reconstruction_first_ack.py) | 571 | `miniray.core._StartTargetedReconstruction`（:40）；`miniray.targeted_reconstruction`（:51） |
| [test_targeted_reconstruction_protocol.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_reconstruction_protocol.py) | 489 | `miniray.task_outputs.TargetExecutionKey`（:33） |
| [test_targeted_retirement_admission.py](../tests/unit/test_targeted_retirement_admission.py) | 780 | `miniray.core._DelayedTargetedReconstruction`（:24）；`miniray.core._StartTargetedReconstruction`（:24）；`miniray.targeted_reconstruction`（:35） |
| [test_targeted_worker_execution.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_targeted_worker_execution.py) | 225 | `miniray.task_outputs.TargetExecutionKey`（:14） |
| [test_task_finish_barrier.py](../tests/unit/test_task_finish_barrier.py) | 808 | `miniray.output_recovery`（:44） |
| [test_task_readiness_trace_contract.py](../tests/unit/test_task_readiness_trace_contract.py) | 158 | `miniray.task_outputs.TargetExecutionKey`（:14） |
| [test_worker_crash_supervisor.py](../tests/unit/test_worker_crash_supervisor.py) | 928 | `miniray.output_recovery`（:37） |
| [test_worker_export_pin_rollback.py](https://github.com/xinkuleee/mini-ray/blob/69106772567a4131f5ec76e898a3c4bf3bb6dbe6/tests/unit/test_worker_export_pin_rollback.py) | 1108 | `miniray.task_outputs.TargetExecutionKey`（:24） |
| [test_worker_side_core_contract.py](../tests/unit/test_worker_side_core_contract.py) | 1556 | `miniray.stored_publication`（:45） |
| [test_worker_stored_publication.py](../tests/unit/test_worker_stored_publication.py) | 397 | `miniray.stored_publication`（:28） |
| [test_worker_unified_output.py](../tests/unit/test_worker_unified_output.py) | 1134 | `miniray.task_outputs.TargetExecutionKey`（:28）；`miniray.task_outputs.TargetOutputManifest`（:28） |

## 6. 检查边界

全部59个源码文件做静态引用筛查并人工复核候选；348个测试Python文件做顶层合同/AST筛查，受退役能力影响处深入阅读。
列表外表示本轮未确认过时，不代表已证明没有冗余、可执行或覆盖全部故障。21个未确认源码候选及必要动态回调误报排除项另在JSON。
本轮未修改运行时或测试，未运行pytest，未提交或推送本轮文档。
