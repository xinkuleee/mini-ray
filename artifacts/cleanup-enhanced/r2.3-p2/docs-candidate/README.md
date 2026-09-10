# R23-03 / R23-04 增强版文档候选

状态：三文档候选已准备，尚未由本代理应用到正式仓库。基于 E fd40a407a46ada3d4941c034a73355f45355b270 的实际文件；input 保留准备时原字节，candidate 保留待应用字节。

- docs/production-ray-mapping.md 解释当前 E 的共同机制与 C0–C7、put/图/Node-loss 增量；B 源码固定0a340b7、教材及验收固定ab4cfb3，Ray仍固定c3162dc。
- docs/testing.md 仅改原第5/42行：392候选与正式3f5b725的405/1、37/37有明确历史边界，R2.3修复进行中，不称新HEAD已验收；当前gate仍30 pure文件/37 smoke，迁移登记不自动扩gate。
- docs/design.md 仅改原第3行，删除正式E的候选称谓，区分架构说明、R2.2验收与R2.3进度。

补丁：changes.patch，SHA256 25dc89e25990860c7f3d08587821238329eae6ca27b4248376830911cfdda3df。manifest.json 保存逐文件原/新raw hash和候选LF hash；static-check.json 保存静态检查明细。补丁已对正式E执行 git apply --check，返回0。候选本身不再变动。

静态核对：104个相对链接目标存在，27个固定B/Ray Git对象均用本地cat-file核对存在；远端URL未在线核验。design除第3行外逐行相同。git无索引空白检查无输出；其退出1表示存在差异，不是空白错误。包装时保留原文件换行，以显式cr-at-eol规则检查CRLF。

语义复核已对照当前E output_publication_node.py、enhanced_publication_client.py、design结果/故障/图段和已保存七组合同映射。GCS terminal不等bytes/READY，INTENT/ARM不证明成功，put不伪造Task ARM/Complete，固定B无普通GCS门禁均已区分。未对隔离ACK试验预写StageAck已采纳。

没有修改source、test、P3、历史artifact、current-status、cleanup-progress、plan或README；没有运行pytest、collection或测试导入。root负责应用及阶段状态更新。

README建议（未改）：当前正式E第7行仍用未限定的“已独立验收405/37”，可最小改为“R2.2实测3f5b725已独立验收；R2.3后继修复进度见状态页”，避免后续执行输入变化时误指新HEAD。当前状态页和进度页请与本候选同时按root实际记录同步。
