# P1 source候选独立语义审查

日期：2026-09-10。比较范围为 `audit/k4-p1-candidate/final/rebase-input` 与 `final/rebase-overlay`。只读审protocol、discovery、journal、Node adapter及相关DTO/owner CAS边界；未修改候选、base或测试，未执行pytest、collection或测试体。root已独立审DTO/Core/Worker，本审计不替代其结论。

**结果：在本次审读范围内，未找到scalar改造引入的确定语义缺陷。可进入有界验证；这不是运行时等价或可直接合入证明。** 对重点关注的Node `effect.slot_index`与metadata恢复bytes，现有完整校验链仍在，详见下文。

## 1. slot_index仍有真实入口校验

[OutputPublicationEffect](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/output_publication_journal.py:102) 对transfer/materialization/drop效果先`_uint`，再明确要求`slot_index == 0`；batch效果必须为None，非transfer效果不得带transfer_index。这个检查拒绝bool、负值、1和深篡改后的输入。

Node seal/drop虽然从`manifest.slots[effect.slot_index]`改为`manifest.value`，却并未直接跳过index验证：

1. seal/drop先`replace(effect)`；
2. [_validate_output_replica_effect](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/node.py:1636)再次深重建，核exact stage、Node incarnation、canonical effect、materialize intent与rollback轮次；
3. 校验通过后才读取scalar value并检查descriptor/request、owner、attempt、checksum、size和实际bytes；
4. [_OutputReplicaWriteClaim.matches_drop](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/node.py:205)也明确要求int零和同唯一ObjectID。

Adapter的[_value](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/output_publication_node.py:535)、journal的`_effect`、`_validate_descriptor`、`_prepare_retirement`各自保留零索引约束；child transfer_index仍按真实多child长度验证。因此未发现“scalar直接取value让slot1落到slot0”的通路，无需仅为重复写一遍同检查增加Node分支。

## 2. Complete元数据不能重新制造payload

[journal.complete](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/output_publication_journal.py:335)先核准确witness与prepared状态；已有retirement tombstone即抛`OutputPublicationPayloadRetired`。未退休时要求`set(record.results) == {0}`，再以实际`record.results[0]`构造scalar envelope，之后才写COMPLETED。比旧只检查字典长度更明确，没有从manifest构造bytes。

[Node outcome](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/node.py:1424)捕获PayloadRetired后保持`envelope=None`、`descriptors=()`，只输出准确`output_completion`。protocol继续禁止metadata completion同时携带envelope或replica descriptors，并要求found/SUCCEEDED/COMPLETED及lease/task/attempt/output身份一致。INLINE没有伪STORED投影；STORED projection由真实envelope.result派生。

因此本轮未发现“知道成功等于READY/仍有bytes”的混淆。journal内部`results`、`retired_slots`仍为key0字典属于后继记录形态，不应在P1内顺便重设计清理算法。

## 3. tuple/list是一个用户值，payload仍只有bytes

[discover(value)](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/output_discovery.py:148)只调用一次`cloudpickle.dumps(value)`，不遍历用户tuple/list以输出槽拆开；完整序列化成功后才返回`PreparedOutput(manifest,payload)`。异常仍清空临时source handles并将session置ABORTED，one-shot禁止隐式重序列化。Worker唯一生产caller是`discovery.discover(value)`。

`PreparedOutput`与[PrepareOutputPublication](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/output_protocol.py:90)都要求exact bytes并核长度/checksum；tuple/list/bytearray/generator不再是payload容器兼容输入。这是预定内部wire变化，不是用户值能力缩减。`TaskReply.results`以及lease return/object IDs仍明确以singleton tuple接原wire边界；发现的生产消费者没有把scalar bytes直接迭代成整型列表。

transfer token factory从双参数改为child ordinal单参数，默认token仍绑定完整publication与canonical outer0，child遍历/prepare/promote/reverse cleanup均保多值顺序，不把多个child错误标量化。

## 4. 身份、owner CAS与别名隔离

删除OutputValue内冗余ObjectID后，完整manifest在[_validate_manifest_inputs](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/output_publication.py:310)把final hold outer绑定到publication唯一ObjectID、provisional owner绑定executor、final owner绑定owner，并验证source executor及同child owner一致。`PreparedContainedTransfer`仍要求provisional/final outer相同，故该校验未消失，只移动到拥有完整上下文的manifest。

Envelope仍深重建manifest、witness、descriptor，并对唯一ObjectID/tier/size/hash/owner/Node完整比较。TaskReply的`results == (envelope.result,)`保持额外wire结果和envelope一致性；Complete/outcome仍重新进入严格执行身份校验。

[owner commit](C:/Users/t-hdong/Desktop/gao/audit/k4-p1-candidate/final/rebase-overlay/src/miniray/ownership.py:1817)先deep-revalidate plan并在锁下检查旧receipt、当前attempt、collection/retirement/dead-owner及lineage，预构造public receipt、membership、edges和locations后才写READY与outgoing edges。没有提前READY，也没有把多步锁分成不同权威。public receipt与内部manifest使用分离的重建路径；journal返回envelope/materialized result/snapshot的深重建也保留。

这些是静态路径判断，不等价于并发/深篡改测试已执行。候选原有负例已适配scalar叶子，不需要恢复旧slot DTO才能验证恶意输入。

## 5. 最小必要验证切片（只审，不运行）

以下15个既有函数精确定位已用AST确认存在；可按主执行者最终冻结registry选择whole/exact，不在本报告扩大执行入口。

| 责任 | 建议函数 |
|---|---|
| 用户tuple与wire envelope | `test_single_output_contract.py::test_success_envelope_requires_its_single_output_and_exact_complete`；`test_worker_executes_and_serializes_sequence_as_one_result[tuple/list]` |
| once serialization/payload | `test_output_discovery.py::test_one_output_serializes_each_reducer_once_before_becoming_observable`；`test_discovery_metadata_and_payload_are_separate_and_wire_roundtrip_exact`；`test_prepared_output_rejects_payload_containers_without_invoking_reducers` |
| journal exact/retirement/alias | `test_output_publication_journal.py::test_wrong_manifest_stage_indices_and_descriptor_have_zero_mutation`；`test_owner_adoption_retires_one_payload_and_preserves_exact_complete_metadata`；`test_snapshots_and_data_plane_returns_do_not_alias_journal_authority` |
| Node非零effect与物理保护 | `test_output_replica_node.py::test_malformed_materialization_identity_is_rejected_before_claim`（含ordinal/bytes/nested）；`test_drop_requires_exact_next_rollback_effect_and_deep_request_identity` |
| metadata不能恢复bytes | `test_output_protocol.py::test_retired_payload_completion_is_exact_metadata_only`；`test_metadata_completion_rejects_changed_execution_and_nested_tampering` |
| adapter真实交接/GC | `test_output_publication_node.py::test_bad_single_output_bytes_has_zero_journal_or_external_effects`；`test_effect_then_lost_ack_replays_the_exact_frozen_publication`；`test_owner_adoption_then_gc_releases_single_output_and_each_child_hold` |

Owner CAS/public receipt/query别名检查由root既定owner测试闭包执行，不能用上述Node切片替代。最终仍需ordinary、contained Task/put、whole replay/GC既定真实进程证据；本报告不增加故障笛卡尔积。

## 6. 静态验证和输入身份

独立AST比较确认source恰有14个AST变化文件，与final记录一致；对这14source及上述六个测试文件仅AST解析、内存compile，未import或执行测试体。其它测试“格式等价”不能推出这14source语义等价，本审查没有作这种声明。

本次重点读取overlay文件raw SHA256：

```text
protocol.py 928dc76eee6eeb5b93792e269f285ca57ec66efe930e2263ab3ff2ab8b7ddd09
output_protocol.py ab2096c25afbe3ee05c5a55d3e2296f6991142235f7789c4bb5b385bd60f14bc
output_discovery.py 54c489aa0909169ee5689f85dbe978d04a24426a3a4253b9e63b39a3828787e4
output_publication_journal.py 6eaafb67207df2fda2de188572e0107d3cee979764e7647aa9b0fb447a3550a8
output_publication_node.py 230034196b5300d8aed2ce6f69b16ecafcdd1473cf034b712efdd7bb05d0c97a
node.py 58e16482e51f907557096f2fd7765e9b0541c0893f2a6d11943a1c4587107b92
output_publication.py 8540d99b5a307b9c31d2c746cf9e049a351858c8ffdcb952b21f8f755dcd2095
ownership.py ea431f1d167138d42a624f2c96cf2693d026b13bcd3c6b163e47bdbbf41a2e34
```

输入变化后按新diff重新核对，不能将本只读结论改标到其它候选版本。
