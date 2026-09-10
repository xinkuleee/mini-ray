# K4 已应用兼容清理：有限验证闭包审计

日期：2026-09-10。只读工作树：`C:/Users/t-hdong/Desktop/gao/mini-ray-base`；HEAD为 `42ed62d7231d17d3b7f703670a357c509183e853`，另有未提交K4修改。本审计不修改源码、测试或manifest，不导入项目/测试，不运行collection、pytest或集群。机器清单及输入SHA见[k4-validation-audit.json](C:/Users/t-hdong/Desktop/gao/audit/k4-validation-audit.json)。

结论：REP-001/002/003/004/005/010/011的保留合同可由有限集合闭合；不能由K3历史绿色直接认证K4。隔离P1标量DTO候选尚不属于本工作树，本报告不把其scalar测试算成已应用证据。

## 最小验证集合

共34个主要pure/loopback目标合同，按已有exact或whole-file注册执行，不能把whole注册下的函数名直接交给`--case`。准确selector、行号、已有注册、成本和输入hash差异均在JSON，避免再建一份执行manifest。

| 边界 | 最小目标 | 成本/证据边界 |
|---|---|---|
| Worker权威：REP-004/010 | side-Core新2pure：`test_worker_shutdown_internal_typeerror_is_not_retried_as_an_old_signature`、`test_cached_push_does_not_reconstruct_missing_acceptance_or_reopen_admission`；另取closed exact replay、unclean Core drain、prepublication local abort3个原pure；Worker push的Start ACK未知、Complete ACK未知、closed冲突3pure | 不启动Worker/Core；fake transport或真实内存publication。新反例证明不从cache恢复准入、不因内部TypeError重试旧签名 |
| Worker生命周期 | side-Core原4L1：binding线程隔离、drain task before Core、timeout保留Core、只fence新retain | binding1thread；drain最多2ownedthread；无socket/进程；gate≤2s、shutdown1s或1ms、join≤2s；逐exact30s |
| Node pool/partial startup：REP-001 | pool原2pure；NodeWorkerDeath ready registration、rejected registration、published-prefix rollback3pure；membership generic-stop1pure | 正式`_WorkerSlot`，fake Process。survivor经真实1KiB本地publication完成；first-worker诊断只从pool派生 |
| Core route：REP-003/005 | 完整snapshot death正/冲突/旧proof3pure；new lease、migrated cancel、running push3pure；foreign stored原deadline1pure | 一份捕获HomeRoute，旧cancel/push身份不改绑；到期不发object RPC。主fixture仅内存锁/队列/typed Node custody |
| GC/OwnerService：REP-002/011 | put reference未知Seal/Drop与plain large2pure；put home failover3pure；OwnerService全4pure | 单一GC表保留义务；两threadless Core/16KiB Store以内；OwnerService fake server不监听，必需handler缺失在server创建前失败 |

root最后消息报告：k4b的Worker side-Core全部20exact已通过，含新2pure和原4L1；k4c的pool2、membership6、NodeWorkerDeath8通过，spillback12当时正在执行且多数已通过。此为执行者消息，审计未读取其原日志或重复运行。

另外保留5条真实接线建议，优先复用共同gate：partial startup rollback、同Node双Worker重叠、CPU yield、stored physical GC、Driver home Node failover。当前静态注册中前三类的startup/pool/physicalGC有原入口；CPU yield的独立exact与Driver home-failover exact未登记，不能直接绕过runner运行。CPU已有example05代表证据，可先判断是否已在当前K4版本验证，避免仅为新表格重复实验。

Core真实partial-constructor的3L1（lane-start前失败、start后抛错、重复unpublished abort）是条件保险：如构造/abort输入改变且无K4后证据才补；当前未登记，先逐项审读登记。其线程数/joins有明确界限，不能将“未进gate”解释为无用。

## 审查中已闭合的两处实际问题

1. [Node._sweep_exited_workers](C:/Users/t-hdong/Desktop/gao/mini-ray-base/src/miniray/node.py:2899)的missing-pool guard仅兼容旧membership fixture。正式构造在外部效果前已建立pool，真实start失败也保留pool；root已删除guard，并在[test_node_membership_snapshot.py](C:/Users/t-hdong/Desktop/gao/mini-ray-base/tests/unit/test_node_membership_snapshot.py:29)建立一个`process=None`正式slot。两份Actor-only fixture虽省略普通pool，但未找到进入该sweep的实际caller，不机械加入迁移工作。
2. [pool survivor Complete](C:/Users/t-hdong/Desktop/gao/mini-ray-base/tests/unit/test_worker_pool_node.py:243)不能断言owner handoff同步Complete。root已改为：Node本地释放完成、handoff仍None、准确witness进入pending outbox，显式report后才获得同witness。新断言保留真实异步边界，没有制造ACK。

## K3 helper闭包及删除前提

最终只读搜索发现旧`tests/unit/_pure_node_output.py`直接Python import caller为0；旧`_unified_worker_rpc.py`也无caller。当前`_pure_node_output_current.py`的五个实际caller为：

- `test_lease_completion_handshake.py:36`
- `test_node_lease_execution.py:27`
- `test_node_placement_group_runtime.py:24`
- `test_spillback_runtime.py:56`
- `test_worker_pool_node.py:24`

旧helper自身仍含已删除`output_recovery` import、`request.target_execution`、INTENT/ARM/graph callback；不是可用备用后端。**删除前必须等新pool/spillback验证闭合，并重绑registry。** 当前`test_worker_pool_node.py`的`reviewed_files`仍额外列旧helper；run_baseline既检查import闭包，也核验显式额外文件，删文件后会正确拒绝该注册。应移除这一过时输入并重算规范closure/hash，不能删掉真实新helper依赖来绕过校验。

新的`tests/support/_worker_protocol.py`只初始化未启动Worker的真实表/锁；`complete_boundary`把成功交给实际publication peer，非成功为明确typed transport替身。不得把它称完整Node集群。side-Core drain L1的admitted stub会以真实publication构造完成后显式登记completion ACK，适合证明drain次序；完整Worker ACK接线由上述3个Worker push纯合同补证。

当前`src/tests`精确文本筛查未命中旧`_inline_gc_obligations`、`_gc_obligations(`、`_ensure_worker_slots_locked`、`_sync_first_worker_compat_locked`、`_requester_route_or_legacy_id`、`_make_message(`、`snapshot_installed=`。这不许可机械删除所有`getattr`：可选trace、真实启动未完成与已关闭参与方依然需要各自语义处理。singleton `slots/results/output_ids`是隔离P1的表示范围，不能误当未清REP旧fixture字段。

## 当前注册状态与收尾

最终静态检查时，34个主要pure/L1目标都能解析到现有exact或whole-file注册，规范输入hash（仅CRLF→LF）一致。新2pure在审计过程中由root完成登记；此前“未登记”已转为已解决，不写成现存阻塞。注册匹配只证明执行输入被绑定，不证明测试已经通过。

root收尾建议：保存k4b/k4c实际日志→等spillback12完成→移除旧helper过时registry输入后再删除helper→复核这五caller闭包及受影响注册→记录本K4已应用REP和隔离P1分别的源码身份。需要扩大到真实smoke时沿原有界入口逐项串行；不因本报告建议项未在gate就删除其保留合同，也不因为full旧测试未绿恢复已退出的兼容类型。
