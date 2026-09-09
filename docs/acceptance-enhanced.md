# 协议增强版验收账本

日期：2026-09-09。实施依据是[两阶段计划§10](redesign-plan.md)，本页只记录证据，不另立实现计划。
**两个阶段均已完成约定范围的独立验收。增强版同时启用两项协议，固定学习标记为`teaching-enhanced-v0.2`。**
基础学习版本为`teaching-base-v0.1` / `69106772567a4131f5ec76e898a3c4bf3bb6dbe6`，其[证据包](../artifacts/stage1-baseline/)不改写。

## 当前实现与责任

普通Task成功路径必经GCS INTENT、图PREPARED、ARM、准确terminal、图COMMITTED及adopted；不是可关闭的第二后端。
owner仍唯一提交READY/outgoing/recovery；Node仍唯一提交Complete和一次资源释放；child owner仍唯一持有incoming hold。
`enhanced_publication.py`只管准确metadata历史、前进fence和保守图；`enhanced_publication_control.py`将其与成员/死亡事实及已有清理驱动组合；
`enhanced_publication_client.py`在owner侧持有精确metadata和同步调用，既不存bytes也不成为新READY权威。

put经过同一图子协议，使用独立put身份、真实child准备/晋升和INLINE本地值或STORED Seal收据。put没有Task Complete/ARM/adoption，
C5后owner尚未安装时若物化Node死亡，关闭该put并准确补偿；不把旧物化收据改绑新Node。whole reconstruction先解除旧epoch责任，再预留新图边。

收益是GCS存活时多一个准确执行事实来源、对所有受支持contained-edge入口的全局防环。代价是普通成功新增同步RPC和不可达等待，
以及每个publication的阶段/准备/补偿收据、死亡清扫与退休义务。INTENT/ARM不证明执行成功；成功metadata不恢复bytes；
单GCS内存权威没有HA、持久恢复或owner接管，也不提供外部副作用exactly-once。

## 最终固定版本证据

最终增强snapshot03归档SHA256为`f3782f572b96233e8f9bae3a7915b8a7f3534904c11f857621df367aa530344a`，
[文件hash](../artifacts/stage2-enhanced/snapshot.json)绑定实际测试输入。29个纯合同文件与37个精确smoke在同一固定源码上串行验收，
**377 passed / 1 deselected，37个smoke全部通过**；七个原main的stdout及canonical trace全部保存。

| 证据 | 结果 / 位置 | 边界 |
|---|---|---|
| 纯合同及小型组合 | 377 passed / 1 deselected，pytest11.30s；[日志](../artifacts/stage2-enhanced/00-pure.txt) | 不是整个历史tests树；唯一deselection为独立标注的真实线程case |
| 真实运行时 | 37个exact exit code均为0；[结果索引](../artifacts/stage2-enhanced/results.json) | 包含保留基础行为、R1/R2/R3、W2和所选死亡切片；逐个30秒POSIX边界，不外推任意故障矩阵 |
| 七个教学main | [输出与trace](../artifacts/stage2-enhanced/example-output/) | 使用原main和同一增强后端；保留真实GCS阶段增量，没有归一化成基础版路径 |
| 环境与依赖 | [环境](../artifacts/stage2-enhanced/environment.json)、[安装记录](../artifacts/stage2-enhanced/dependency-repro/linux-results.json) | Linux/WSL CPython3.12.13；最终488文件输入在Linux frozen安装/import前后hash一致；Windows不宣称完整进程支持 |
| 规模 | [同口径统计](../artifacts/stage2-enhanced/complexity.json) | 源码59文件、61,440物理行、50,277代码行；不把测试和历史材料算作活动源码 |
| 可检出版本 | `teaching-enhanced-v0.2` | 基础`teaching-base-v0.1`保持不变；通过版本检出比较，不长期维护双后端 |

snapshot01的四切片和snapshot02的376/1及W2/owner死亡属于中间过程；最终完成依据是本页snapshot03全体同版结果，不累加历史pass。
测试后仅汇总文档与证据包，运行时源码、测试和runner不变；远端CI及推送不是本次完成事实。

## 保证与窗口证据

纯模型只证明归约器/收据合同；小型组合使用真实Core/Node/owner authority和显式队列/回调，不冒充TCP；
真实进程使用既有30秒POSIX进程树runner，timeout不当clean，独立borrower不随outer死亡误释放。

| 分类 / 窗口 | 当前证据 | 证据层级 / 宣称边界 |
|---|---|---|
| B 基础回归 | 约定保留纯合同与32个基础smoke在增强版同版复验通过 | 七个main及增强版不同trace期望已验证；不复制基础版无GCS门禁断言 |
| G/D 准入与历史 | `test_enhanced_publication.py`：准确identity、收据、预留联合判环、terminal/commit/退休重放 | 纯模型；不以手造Complete或图请求认证runtime |
| W1 图已预约、child失败 | `test_enhanced_node_publication.py`五个有限case，含两child：第一次真实prepare后丢ACK，第二个已收集child拒绝；完整scope、owner fence及Release后图退休 | 真实Node journal/child owner/GCS reducer组合；无真实进程死亡，不扩成每种来源×故障矩阵 |
| W2 Complete、terminal ACK未知 | `test_enhanced_terminal_loss_path.py`一对已有exact：未接受terminal与已接受后ACK丢失，再让publisher Node退出 | snapshot03两个真实exact同版通过；实际已接受回复在Node adapter边界丢弃，不声称网络packet loss。观察UNKNOWN对照已知成功LOST，预算/bytes分别断言 |
| W3 图COMMITTED但owner未READY / C7 ACK未知 | `test_enhanced_owner_client.py`四case中的两个：真实图commit后丢ACK不提前READY；真实owner CAS与C7后丢ACK不回滚READY，保留finish/GC屏障 | 小型组合；四case已纳入最终377项同版通过，不冒充额外TCP切片 |
| W4 重建、GC、迟到消息 | 同上真实Core组合：旧epoch退休、actual whole重建后旧commit/release重放不复活或删新边；再GC保留历史 | 与W3同一最终通过集合；真实正向/GC由同版R1/R2/R3补接线，不能归一化成两版完全等价 |
| owner死亡清理 | `test_enhanced_publication_control.py`九个有限case，真实成员表/图authority/child table；死亡进度轮转、准确fence、final shutdown门禁 | 纯/组合；另snapshot03实际pre-Complete owner死亡切片同版通过，GCS/Node/child清理闭合；不外推所有死亡交错 |
| child-owner死亡退休 | `test_enhanced_owner_retirement.py`10项纳入最终同版通过；mixed实际Release与完整已安装死亡证明、旧proof冲突/coverage/深拷贝 | 纯owner合同；不伪造dead child的Release ACK。Node/Core死亡接线由同版所选死亡切片与组合边界验证 |
| U4/U5 入口与可达性 | snapshot03 R1/R2/R3：真实Task、含Ref put、whole replacement，公共成环候选及并发预留均到达生产handler | 可达性已证明；自环、首次发布互环、更多并发未证明也不新增门禁，最终版本已复验这三个有限入口 |

U1–U5的有限实施义务已由上述同版分层证据满足，没有把必要未决窗口留给发布后；未证明的自环、首次发布互环或更宽故障组合不是本版门槛。

## 真实成环场景怎样到达

顺序最小场景使用一个持续存在的普通Worker和可导入模块中的普通函数：首次Task产出stored A₀；下一Task获得A的真实borrower，
执行并保存Worker本地`B = ray.put([A])`，因而B→A成立且由真实hold保活。释放A₀ bytes后，同ObjectID A的whole reconstruction读取持有的B，
序列化结果提出A→B。旧A epoch已按屏障退休，B的incoming hold仍保护逻辑A；GCS在C1实际拒绝新候选，没有修改A的旧immutable bytes。
失败候选没有PREPARED/ARM/COMMITTED/Complete/adoption；真实空rollback scope证明未发生child/materialization效果，随后释放B让A最终GC。

并发场景在两个Node各一个Worker先保留`X = put([B])`、`Y = put([A])`，实际图含X→B和Y→A。
`ray.wait`使两个LOST结果走既有whole reconstruction，分别提出A→X与B→Y。若两者同时预留，合并后成A→X→B→Y→A。
两次发送前及两次原始回复后屏障只控制观察时机；原GCS锁、算法、请求、回复不替换。观察时正好一个候选PREPARED且graph_active，
另一个实际CYCLE，两方均未ARM/COMMITTED，所以证明预约联合，而不只是已提交边的竞争。败方准确rollback，胜方继续READY/adoption，最后释放puts并退休全部membership。

这些函数保留真实Worker-owned put句柄，不保留已结束Task的borrower。未增加设置结果/Actor引用邮箱等API，未手造ObjectRef/borrower/ACK，未直接调用DFS。
普通Python list自环和ObjectID引用环不同；对已put的Python容器再append不会改变stored对象。

## 执行与完成边界

初学者先检出`teaching-base-v0.1`运行[基础账本](acceptance-baseline.md)；研究本阶段再检出`teaching-enhanced-v0.2`。
增强版仍复用`scripts/run_baseline.py`和[同一显式清单](../scripts/baseline_manifest.json)，它在基础标记与当前主线分别读取各自版本的集合。
新增exact已审阅、纳入并同版通过；仍一次只运行一个，不跑整个历史目录。

```bash
python scripts/run_baseline.py --smoke 'tests/integration/test_enhanced_cycle_path.py::test_public_reconstruction_rejects_reachable_two_object_cycle'
```

W1–W4所选窗口、U1–U5有限义务及29文件/37smoke已经同版验收。结果不表示所有历史测试通过，也不承诺HA、owner接管或任意多重故障下可用。

**规模目标未达成。**基础版48,555、增强版50,277代码行，新增1,722行（3.55%），增强版另有61,440物理行。
1.4–2.2万与2.5–3万仍是已被实数校准的低置信度初始预算，不是硬上限。此次完成的是范围和协议职责纠偏及两阶段有限合同；
Core/Node/wire集中和整体阅读成本仍是保留债务，不能把所有现存代码称为必要或以测试通过宣布紧凑教学目标完成。
新增成本包括明确GCS/图收据、同步准入、死亡清理、精确退休及其接线；未删除必要校验、清理或测试来凑行数。
