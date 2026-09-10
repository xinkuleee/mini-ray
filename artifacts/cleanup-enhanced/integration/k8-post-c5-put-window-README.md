# E planned-window review and one put test proposal

Status: audit-only patch, not installed in candidate. No runtime, source, manifest, base or frozen trial is modified. Static review and AST compilation only; no pytest, collection, target imports or runtime execution.

## Evidence and boundaries

- W1: `test_enhanced_node_publication.py` covers actual Begin/ARM ACK loss, real child prepare/promote/release transitions, and journal-derived rollback scope/closed holds. Before confirmed compensation, graph reservations remain active.
- W2: the four enhanced unit files separate C3 local Complete, C4 knowledge and late terminal history. Existing `test_owner_led_publication.py::test_owner_death_releases_both_holds_then_sources_without_reversing_complete` also consumes actual adapter `owner_death_closed_holds` through a real PublicationClient retirement, proving Node local retirement does not itself remove graph responsibility. `tests/integration/test_enhanced_terminal_loss_path.py` already has two planned actual-process cases: local Complete without surviving GCS terminal, and accepted terminal ACK discarded before publisher-tree death. Their source asserts exact owner knowledge, both hold tombstones, graph retirement, finish-barrier removal, and byte loss. This static audit does not re-execute those cases.
- W3/W4: `test_enhanced_owner_client.py` covers real C5 ACK loss while owner remains PENDING, C7 ACK loss while owner READY and Node reply retained, and real reconstruction retiring old graph membership before a successor. Its Node ACK callback is explicitly a local journal boundary after validating genuine owner CAS and C7. Actual Node C7 retirement is separately exercised by `test_output_publication_node_server.py::test_payload_retirement_is_independent_of_terminal_outbox_and_physical_replica` and the adapted finish-barrier backend. No additional matrix case is proposed here.
- `test_enhanced_publication.py` constructs metadata facts for authority contracts. Such inputs are not runtime observations. Registered worker deaths and rejected forged child deaths are covered by `test_enhanced_publication_control.py`; its empty physical sweep fixture is not evidence about arbitrary payload-bearing Node shutdown. No universal supervision claim follows from these finite cases.

## One missing existing-contract window

The current `test_put_home_failover.py` covers death after Seal but before C5, where exact reseal on the installed survivor is permitted, and unknown Seal without death proof. It does not cover death after real C5 fixed the materialization but before owner installation. The other enhanced put cases validate metadata or owner-death cleanup rather than this actual Core path.

`post-c5-put-window.patch` adds exactly one test using the existing homes fixture. It forwards the real CommitGraph request to the existing authority, queries that accepted record through the real PublicationClient, verifies owner PENDING and open put custody, installs real NodeRegistry death, and returns the actual C5 ACK. The expected existing Core contract is NodeDiedError for lost materialization; no survivor Seal, new CommitGraph, owner value or public ObjectRef may appear. Cleanup retains the original route/Seal evidence, uses the installed NodeDeathRecord as physical closure, fences with the actual Core abort receipt, retires the graph and preserves the original C5 preparation/receipt.

The case is intentionally a plain stored put with zero children: its empty ClosedContainedHolds is exact for that manifest, not a fabricated cleanup ACK. Child lifetime windows remain covered by their existing tests. The dead Node store is never read after death and is not deleted to simulate process exit.

The existing three test function ASTs are unchanged. See `source-review.json` for source-input and patch SHA256 values. Candidate installation and actual serial execution remain parent-owned.
