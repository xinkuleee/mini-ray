# Integrated P3 candidate

Input: current audit/p1-evaluation 02 source snapshot. This candidate has not run tests and does not inherit P1 runtime acceptance.

- Scope: ownership.py, core.py and 14 direct test consumers.
- Source delta: +80 / -110 (net -30). Test delta: +86 / -34; 104 names and decorators preserved.
- One retirement lifecycle map; scalar owner membership and retirement plan; owner collection tombstone no redundant zero index.
- Public deep detachment, exact child death proof, GC and attempt/publication fences remain distinct.
- Core adoption/receipt queries and owner public validate/query bodies are unchanged.
- Child and replica sequences and Node journal effect index zero remain.

Checks: 16-file AST, no legacy owner scalar/map accesses, no trailing whitespace, input SHA stability and patch application check pass. No pytest/test import, manifest edits or commits.

Runtime: root should apply after P2 and run the exact owner/child-death functions plus bounded ordinary example01/example06, contained reconstruction, adoption-ACK and concurrency selectors listed in integration-report.json.

Cost: removes duplicate terminal plan storage and singleton structure. Completion now shallow-copies pending plus completed records; performance is unmeasured.
