# One concrete StageAck candidate (design only)

No runtime file implements this design. Its field sufficiency and net cost must be established in the one actual candidate after R23-01 E freeze. Keep `PublicationReply` for GetPublication and rejected mutations; remove accepted-mutation use of that full reply in the candidate. Do not keep a feature switch.

```python
AcceptedPublicationFact = (
    PublicationRef | TaskPreparedReceipt | PutPreparedReceipt
    | OutputPublicationCompleteWitness | OutputPublicationAdoptionProof
    | OwnerAbortReceipt | OwnerRetirementReceipt | WorkerDeathRecord
    | ClosedContainedHolds
)

@dataclass(frozen=True)
class PublicationStageAck(_Wire):
    request: PublicationMutation       # exactly eight concrete request classes
    reference: PublicationRef
    owner_worker_id: WorkerID          # derived from accepted authority record
    receipt: PublicationReceipt        # original stage and original sequence
    accepted_fact: AcceptedPublicationFact
    fence: FenceProof | None = None    # actual first fence, not requested echo
    fence_receipt: PublicationReceipt | None = None
    retired_receipt: PublicationReceipt | None = None

    @property
    def accepted(self):
        return True                   # success-only type, not another field

    @property
    def forward_open(self):
        return self.fence is None and self.retired_receipt is None
```

There is no copied publication/receipt-history tuple, no mutable phase flag and no stored forward boolean. Current forward derives from exact closing evidence. These extra closing fields cost almost nothing on normal open replies but can retain a larger first-death proof on closed historical replies; measurement must include that cost. Full request echo remains, so ARM/put preparation is deliberately present in both request and accepted fact. This is a cost to measure rather than hide.

`accepted_fact` is selected by an explicit concrete-type branch, not arbitrary kind/payload or a universal schema. Begin/Prepare use PublicationRef because Begin already echoes the whole publication and Prepare accepts only the exact reservation identity. ARM carries TaskPreparedReceipt; terminal carries Complete; Task commit carries Complete; put commit carries PutPreparedReceipt; adoption carries its proof; fence carries actual first fence; retirement carries actual first ClosedContainedHolds.

## Strict validation

1. Rebuild every incoming nested value through existing concrete `_copy`/constructors; reject subclasses and hidden payloads. Every field is deeply detached on return. `_Wire.__reduce__` makes unpickle call constructors again. The candidate does not relax full `PublicationSnapshot` validation or `_validate_prepared/_validate_closed` in authority.
2. Request must be one of the eight exact mutations, never GetPublication. Reference must equal `request_reference(request)`. Receipt must have the same reference, exact request-associated stage and a positive integer sequence (not bool). All new WorkerID/receipt/fact fields are exact types.
3. Validate the accepted fact against the request: Begin/Prepare reference equality; ARM preparation equality; terminal Complete equality; Task Commit Complete reference equality; put Commit preparation equality; adoption full proof equality and owner equality; Retire full ClosedContainedHolds equality including tuple order. Do not sort retirement proof sets during replay. Full prepared/closed history authority checks still run before ACK construction.
4. Owner context is generated from the accepted authority publication. Bind it to the request owner where the request has one (Begin/Fence publication, adoption proof, put key), to Task promotion final-hold owners when applicable, and to the caller known publication/journal manifest at actual Node/client decode boundaries. Task PrepareGraph/CommitGraph have no owner in their reference, so a type-correct owner alone is not an independent owner-binding proof. Their actual client calls must supply/check the already remembered publication owner; Node journal and terminal gate have their manifest owner. Trace merely observes this authority-derived context and obtains no mutation permission from it.
5. `fence` and `fence_receipt` must be both absent or both present. Present closing receipt must have exact reference and FENCED stage; first proof must name the same owner and, for owner abort/retirement, exact reference. Worker death must pass existing strict death-shape validation; registry authenticity remains the existing controller responsibility. `retired_receipt`, when present, is exact RETIRED for the reference, requires a fence and follows its sequence. For FENCED ACK its receipt equals fence_receipt; for RETIRED ACK its receipt equals retired_receipt.
6. Successful Begin/Prepare/ARM must have forward open because current authority rejects fenced replay of those operations. Terminal/adoption/previous commit may carry closed state. Existing closed commit must not be admitted as a new owner install. The reader must still recheck local ACTIVE/epoch/abort after RPC. Even an open ACK is a fact about one remote acceptance instant, never a timeless capability or a cache license for a later call.
7. A malformed success ACK or full success mutation reply is rejected by candidate consumers. Rejections retain `PublicationReply(request, False, optional_snapshot, typed_error)` and exact request/error checking. GetPublication retains `PublicationReply(request, True, snapshot_or_none)`; mutation StageAck cannot stand in for a full history query. Existing non-GCS output/abort protocols are unchanged.

The narrow constructor cannot independently repeat every full-snapshot graph/causal validation after transmission without carrying that snapshot again. The full authority validator still proves causal stages and child coverage. The new decoder proves exact returned request/stage/fact/closing-context bindings, and actual consumers compare their locally known preparation/owner/Complete. Whether this preserves every retained external boundary contract is a required trial result.

## One authority-side factory and first-fence exception

Use a private concrete `stage_ack_from_snapshot(request, snapshot, receipt, *, accepted_existing_fence=False)` factory. It reads only the current detached/authority-owned validated snapshot while the existing authority/composition locks are held. It must verify the full publication equality for Begin/Fence, fact equality for other requests, and exact stage receipt membership before projection. The argument is local construction context, not a new network field or saved state. Normal calls leave it false.

For the ordinary first transition, build/validate the updated full snapshot, then build/validate StageAck, then publish `_records` and sequence exactly where `enhanced_publication.py:898–902` now does so. For stage replay, project the retained snapshot and original receipt. Do not add a second success table or weaken authority error handling.

Only `EnhancedPublicationControl.handle` existing `:60–70` branch may set `accepted_existing_fence=True`: the incoming request is FencePublication with a fully registry-validated WorkerDeathRecord for the publication owner, the exact same publication already has a different first fence, and `commit_owner_death` has scheduled the existing cleanup obligation. ACK.request remains this later death request; ACK.accepted_fact/ACK.fence and receipt remain the first canonical fence. Constructor permits this mismatch only for the concrete death-request shape and matching owner; factory additionally requires that special local branch. This is not permission for other proof mismatches or a claim that new death first committed the fence.

The existing controller post-mutation death check reads the ACK checked owner, while its pre-mutation query remains unchanged. Full query/for_owner scans still supply cleanup history. `control.py` trace derives ObjectID and Task execution from ACK.reference, with ACK.owner_worker_id for the one missing trace identity. No extra query is introduced for observation.

## Bounded negative cases to map before candidate validation

- Changed request/reference/stage/receipt/fact, including a self-consistent wrong owner context on reference-only requests, must fail at the actual consumer that knows the owner.
- Mutation-return deep alias tamper must not alter retained authority state; deep malformed values must fail before first authority commit. Include preparation transfer hold and accepted closing death/hold content.
- ARM reply must equal actual journal preparation. Terminal gate must inspect accepted Complete before its intentionally lost reply.
- Replaying an old positive receipt cannot reopen locally fenced progress; historical closed commit/terminal/adoption remain historical.
- Later owner death preserves first fence and cleanup admission; arbitrary different abort/retirement proof is rejected.
- Exact retirement replay succeeds; a reordered different request after retirement remains rejected, even if it names the same proof set.

Reuse existing cases first; add only missing new-boundary assertions. No benchmark-only fake authority, snapshot reconstruction in test bridges, or additional failure matrix is proposed.
