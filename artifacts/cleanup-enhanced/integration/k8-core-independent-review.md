# K8 Core independent bounded review

Date: 2026-09-10. Scope: current audit/k8-runtime-candidate/files Core put, owner abort, Node-loss, adoption C5–C7 and GC/retirement increments, compared with fixed E and shared B. This is a source review; no runtime test was executed by this review task.

Read Core hashes: candidate 325ad0463f36f56481ae8c110a11bcdfa073307c9f5ae01ad3cc9a5bb05f9f25; shared B 0720f615a9cd250a46dfda95addbfcfcee165f5f0c8cd6351cd8ad84b03a27ae; fixed E 004d578d0a6d64283b70e126643bc0c1c539ec3361233672f14821fc5d24d8de. Later edits require rechecking exact affected methods.

## Finding: adoption ACK tail can retire another lane's cleanup marker

**Confirmed shared-source defect.** Root executed the identical two-lane test on the baseline and repaired snapshots: both baseline cases failed exactly because the old lane returned terminal and finished; the repaired snapshot passed both cases in 1.08 seconds. No fixture error accounted for the failures. Both original shared B and E ended _drive_output_publication_adoption by calling _clear_protocol_unresolved(pending), whose authority check was only the attempt identity. A Node-loss continuation for the same attempt can replace that marker while the old adoption ACK is in flight. The old tail then clears the newer responsibility and allows _finish_pending_task to retire its finish barrier, although _output_node_cleanup still contains unacknowledged child work.

The second lane is reachable through existing continuation scheduling, not an invented public API: an initial _publish_reply adoption ACK failure schedules DelayedReady(output_adoption). The original _execute caller still has its post-published-False _consume_node_death_at_lane check. A dispatcher may start that real delayed adoption replay before the original caller resumes; a newly installed death then causes the original caller to enqueue a NodeLoss takeover. There is no task-wide adoption/NodeLoss exclusion ticket. The loss-only ticket excludes only two loss drivers.

For STORED output with no survivor, actual owner CAS and Node reply-custody retirement can precede the death. A loss lane chooses DISCARD and releases a real child hold. While its first Release reply is unknown, the old adoption tail can report terminal and finish. With the current loss exception path, the finished task may regain an unresolved marker; a later replay exits as obsolete without settling work. Simply adding an obsolete catch-return is insufficient because it would leave the real child cleanup responsibility behind.

Prepared repair: audit/common-late-loss-fix/source.patch changes only the shared adoption tail. Under the Core lock it preserves an existing NodeLoss marker and returns nonterminal. If death was installed before marker transfer, it invokes the existing exact Node-loss continuation outside the lock. If the loss already completed, the existing obsolete predicate returns terminal without blocking normal completion. No B loss-catch bypass was added.

Regression: test_adoption_loss_lane_handoff.py uses one real test thread plus main, actual Node/owner/journal/1 KiB store/two child transfers, an initial actual lost adoption ACK, the resulting real delayed replay item, actual installed death and same-attempt lane transfer, then one actual child Release reply loss. The same window is checked before/after marker classification. It preserves pending work and verifies exact retry/finish rather than manually clearing state or faking ACKs. Both identical-test snapshots are ready for root red/green execution:

- base-adoption-race-before: archive 116d945070afee5b7c88fc104f6669aae4bd2be34bd413b6ac4ec4a6e379546f.
- base-adoption-race-fixed: archive 536434ddd52e0c178c36ad20d28179cb9696743f65d6c7d10dd32a715d879554.

The initially observed E query-before-work late-exception path is a consequence of the same premature completion. After the shared tail repair, its assumed old lane can no longer legitimately finish through an installed death/takeover. Do not automatically add an E-only catch suppression without first demonstrating a remaining reachable pre-work case; existing work must never be silently discarded.

## Requested protocol windows reviewed without another confirmed defect

Put after graph C5: current E preserves fixed E's materialization binding. A Node death after commit_put causes failure of the uninstalled put instead of resealing on a survivor. Its typed PutHandoff retains the exact publication/abort decision, every possible child hold, materialization request and cleanup receipt; graph reservation includes even unsent transfers. Before C5, survivor resealing remains possible only after installed death. This behavior differs from B but is deliberate, not a missing failover. Existing put-home tests cover pre-C5 death/unknown Seal; a post-C5-before-owner-CAS death check still needs a finite E-specific regression before that window is claimed covered.

Owner READY with unknown C7: owner/recovery commit and handoff adoption precede C7. The exception path retains the same adoption envelope and finish marker. Replay reads the actual owner receipt, replays exact C4/C5/C7 and does not roll back READY. Early close remains blocked by the finish barrier until the typed C7/Node tail converges. The four E owner-client tests cover this bounded path; they do not alone prove every concurrent death interleaving.

Node-loss metadata without bytes: the choice reads actual handoff/envelope first and central.complete last; an already latched complete=None is retained. KEEP requires surviving owner INLINE custody or validated owner STORED replica facts. Central INTENT/ARM cannot produce Complete, and central success alone leaves LOST without payload. The inherited K3 owner-INLINE repair reads actual committed membership/result when transient scratch is absent. No new metadata-to-bytes reconstruction was found.

GC/retirement: current code retains exact child receipts or installed WorkerDeathRecord and calls real fence/retire, while physical Drop remains a separate Node responsibility. Query-based fence/retire idempotence returns existing historical receipts rather than rewriting them. These checks do not establish that every replica-unknown or owner-death interaction has run; the existing bounded suite remains required.

## Finite evidence gaps

1. The shared two-lane red/green test is complete as reported above. The same method-tail fix has been inherited into the E audit candidate as an additional patch; B commit mapping and current-version regression remain root responsibilities. The patch and exact E before/after hashes are in audit/common-late-loss-fix/enhanced-inheritance/identity.json.
2. Keep the original E post-C5 put death window distinct from pre-C5 home failover; add one exact callback/death boundary only if not already covered by current E validation.
3. Existing W2 two process tests and W3 owner-client tests remain the right evidence for unknown/known Complete, bytes absence and C7; no broader fault product or new protocol gate is requested by this review.

No other concrete source defect was established within this bounded pass. This is not a proof of all distributed state combinations or a declaration of K8/K9 completion.
