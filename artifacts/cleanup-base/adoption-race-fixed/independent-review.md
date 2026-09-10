# Common late Node-loss versus adoption-tail review

Status: independent read-only source review; no runtime execution and no source edit.

## Finding

The existing checks do not exclude an old adoption tail clearing a same-attempt Node-loss takeover marker. Once that takeover is established, this is a concrete authority mismatch: the Node ACK proves payload retirement, not completion of later publisher-loss child cleanup.

Source: mini-ray-base/src/miniray/core.py; SHA256 0720f615a9cd250a46dfda95addbfcfcee165f5f0c8cd6351cd8ad84b03a27ae.

## Actual authority and finish checks

- `_drive_output_publication_adoption` (11417; tail 11556): after an actual typed adopted ACK it calls `_clear_protocol_unresolved(pending)` without rechecking the current obligation, then pops retained custody and returns terminal True.
- `_clear_protocol_unresolved` (9208) validates only TaskID/AttemptID, then deletes the current marker and the same-attempt death-classification hint. It does not require that the cleared marker is the adoption obligation that earned this ACK.
- `_consume_node_death_at_lane` (8683, takeover 8749) explicitly replaces an adoption marker with an `_OutputNodeLossObligation` for the same publication and queues a loss ReadyTask.
- `_drive_output_node_loss` (11209) has a ticket set, but only loss drivers acquire it; adoption has no shared ticket. The loss driver records `_OutputNodeCleanupWork`, latches DROP, then performs real child release RPC outside the Core lock.
- `_finish_pending_task` (8953) tests current attempt and `_protocol_unresolved`, not active `_output_loss_drivers` or `_output_node_cleanup`. If the old tail removes the marker, terminal finish can remove the barrier and set `_finished_tasks` while loss cleanup is still external.
- `_output_replay_is_obsolete_locked` (11358) considers a finished task obsolete. A resumed loss driver can therefore return before committing its owner resolution or settling remaining holds. The work record is not proof of cleanup, and a broad finish guard alone would only mask the ownership error.

## Reachability, including limitations

A plain `handle_node_death` call (1935) does not by itself launch a concurrent loss lane. It removes locations and enqueues `_NodeDeathObserved`; `_classify_node_death` (8615) ordinarily installs a hint. An experiment that manually invokes a loss method must identify its lane-boundary source.

The runtime has two dispatch lanes and no task-wide publication ticket/deduplication. A concrete source of two continuation chains exists: failed adoption queues its delayed adoption continuation (11552) before returning; the enclosing normal `_execute` success-reply path then calls `_consume_node_death_at_lane` when `_publish_reply` returned False (9794). The delayed continuation can be promoted and started by another lane while the original caller is suspended between those operations. If it enters adoption while the publisher is still live and reaches a successful Node ACK, a subsequent installed death lets the original caller enqueue a same-attempt loss takeover. That original lane is then free to drive the loss task. `_promote_delayed_ready` (8825) does not cancel/deduplicate the earlier adoption continuation, and `_dispatch_loop` (8876) expressly dispatches adoption/loss even when the owner result is no longer Pending.

Spiller's L1 currently isolates the final dangerous interleaving using actual Complete/adoption ACK, actual Node death/consume takeover, actual first child release followed by ACK loss, and terminal finish. The source chain above supplies a runtime continuation origin; source review is not a claim that this exact full scheduler prelude has already been executed.

## Minimal repair target

The adoption tail must finish only its own converged obligation. Under one Core lock it should inspect the current marker for the same execution/publication. If it is a Node-loss obligation, keep marker and custody and return nonterminal False so that the already queued loss lane owns convergence. If death is installed without a takeover marker, the tail must hand off to the existing Node-loss path rather than discard the successful ACK or reinterpret it as application failure. The final marker check and conditional clear need one critical section; checking and then calling a separately locking clear outside that section leaves another race.

Do not erase `_output_node_cleanup`, synthesize an owner loss receipt, turn an actual Complete into unknown execution, or block forever behind a generic busy flag. An obligation-specific conditional clear is the local fix. Similar success/error tails elsewhere should be audited separately rather than mechanically altering every clear call.

## Required bounded assertions

1. Same exact publication/attempt, actual Node adopted ACK already committed.
2. Installed death, owner STORED result becomes LOST and DROP choice is latched.
3. First actual child Release applies then its ACK is held/lost, remaining cleanup exists.
4. Old adoption resumes: must return False, preserve the Node-loss marker, and fail terminal finish.
5. Exact loss replay completes remaining cleanup and owner resolution; only then finish succeeds.
6. No duplicate release authority, second owner CAS, retry-budget consumption, or invented Drop/Complete.
