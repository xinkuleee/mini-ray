# R23-01 E candidate source review and B-to-E mapping

This candidate starts from the parent's B after-02 test, including the actual-run-driven deepcopy observation fix. It changes only this test file for E. No formal repository, shared fixture, runtime, manifest, gate, or P3 file was changed by this agent; no pytest, project import, or collection ran here.

## Frozen identities

| Artifact | SHA256 |
|---|---|
| Original E test / `source-original.txt` | `052874c16ff36f455e1827f1fffd60810fa3001f47eab60ca344b5ab77fb2b50` |
| Validated B source / `source-base-validated.txt` | `d3051956feb52ea44d954cafc4f8e80560eb57b4d5b5d90237abf134917d948b` |
| E candidate `tests/unit/test_owner_finalize_replica_receipts.py` | `ace08a31f85d5d5a33708e32142de9c106fb1daebab57d077f58b318d3332114` |
| E original → E candidate `test-repair.patch` | `509c9164b98fc0c2db44e48933313be43d8a1c782491a9340c9ed6566d9038a2` |
| Validated B → E candidate `base-to-enhanced.patch` | `a5a546110953ab1ca00cbf87ac28e50dfefb54478fad182ef1b52051a47609a6` |

Candidate size: 31,959 bytes, UTF-8 without BOM, CRLF. Candidate is frozen; the parent owns application and actual E validation. B passing evidence does not certify this E input.

## E-specific observations

One local assertion helper queries the existing fixture's real `PublicationAuthority` through `GetPublication`. It does not construct authority records, register membership, run a synthetic retirement, call RetireGraph, or modify the B test with edition branches.

| Test point | Required E observation | Current source basis |
|---|---|---|
| All interrupted STORED preparations | Actual INTENT/PREPARED receipts, no ARMED/prepared payload/Complete, graph active and forward open | `output_publication_node.py:178–194` reserves graph before materialization and arms only after ACKs |
| Independent successful INLINE Prepare | Actual INTENT/PREPARED/ARMED; authority preparation equals real journal preparation; no Complete | Same prepare path and `output_publication_journal.py:352–365` |
| Before either Worker ACK and after unknown cleanup | FENCED receipt matches the one actual FencePublication RPC; exact owner-death proof; local ACTIVE, no closed child proof yet; global graph active/closed_holds=None/no RETIRED | `output_publication_node.py:454–479`; `owner_death_closed_holds` at 490–501 |
| Successful Node cleanup | Local RETIRED plus exact returned `ClosedContainedHolds(reference, (), ())`; authority remains FENCED, graph active and no global closed_holds or RETIRED | `node.py:1528–1533`; adapter 490–501; `PublicationSnapshot.graph_active` 567–568 |

The helper checks exact receipt order/reference and equality with the journal's real stored receipts. It compares `preparation_receipt()` only while the local journal is ACTIVE because that API correctly rejects retired journals. It checks authority snapshots after local retirement without replaying preparation or changing authority state.

The fake Worker callback remains a controlled typed acknowledgement boundary; it does not supply a graph retirement receipt. Direct authority queries are observation-only and do not add to the fixture's mutation RPC call list. The existing actual-process evidence remains separate.

## Original contract preservation

All eight B function names, parameter decorators and bodies remain, with only E assertions inserted. Expected parameter totals remain `2 + 1 + 1 + 4 + 2 + 1 + 2 + 2 = 15`. The original seven-function/fourteen-instance to eight/fifteen split and invariant mapping remain documented in `../base-test-candidate/old-new-contract-mapping.md`.

The deepcopy-before-corruption fix from the validated B source is included unchanged: clone the valid expected claim map first, then separately corrupt expected and actual effect transfer_index, preserving complete map equality after the real Node call.

Independent static review found no remaining issue, verified the basis and candidate hashes, checked the E journal/authority lifecycle against source, and confirmed the exact eight-function/fifteen-instance inventory. These are static feasibility statements, not E passing test results.
