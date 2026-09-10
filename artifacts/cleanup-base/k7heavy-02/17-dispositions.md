# K7-REG-02: 17 original heavy functions

All 17 have concrete isolated migration patches. All contracts remain supported: **0 functions retired, 0 new fault products**. Marker disposition is **10 unit functions and 7 bounded loopback functions**. Original selector names and parameter expansions remain; these are not collected case counts. No base file or registry was changed here and no tests ran.

| Group | Original functions | Proposed marker | Patch |
|---|---:|---|---|
| Actor restart | 6 | 3 unit + 3 loopback | actor.patch |
| PG control/publication races | 3 | 3 loopback | pg/pg.patch |
| Stored physical GC | 6 | 6 unit | stored-physical-gc.patch |
| Supervisor retry and Complete/death race | 2 | 1 unit + 1 loopback | supervisor/supervisor-bounded.patch |

The JSON contains every exact selector, cost, patch SHA256, and disposition. Each component review records preserved assertions and remaining runtime-validation limits. Root must apply only the reviewed patches, preserve concurrent base changes, refresh the candidate closure hashes, and run each registered original.

GC uses genuine owner registration, Node Complete and Core adoption plus actual Node Drop handlers; no fake success reply or fake physical deletion ACK. Concurrency scenes retain actual threads with explicit gates, deadlines, finally cleanup and joins. Unit migrations replace unrelated infrastructure with existing threadless authorities and explicit event/retry progress, not replacement algorithms.
