"""Pure single-output owner preflight/commit contracts.

The old independent sibling dimension is retired. One logical output with two
storage choices retains registration, descriptor identity, stale-plan/attempt,
error replay and lineage collection assertions. No runtime, network or bytes
store is created; plain owner publication is the current Actor-value boundary.
"""
from dataclasses import replace
import hashlib
import pytest
from miniray import protocol
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ConflictingObjectResultError, InvalidObjectTransitionError, ObjectAlreadyRegisteredError, ObjectOwnerTable, ObjectState
from miniray.recovery import RecoveryManager, UnknownTaskError
from miniray.resources import ResourceVector
from miniray.task_outputs import MAX_TASK_RETURNS, TaskExecutionKey, TaskOutputManifest, validate_num_returns
pytestmark = pytest.mark.unit

def _spec(count=1):
    job=JobID(b'j'*16);task=TaskID.derive(job,TaskID.for_driver(job),7)
    return protocol.TaskSpec(job,task,AttemptID(task,0),protocol.FunctionKey(job,__name__,'producer','v1'),(),count,ResourceVector(),WorkerID(b'o'*16),max_retries=2)

def _result(spec,stored=False):
    data=b'one-result'
    return protocol.ResultDescriptor(spec.return_ids()[0],protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE,len(data),spec.owner_worker_id,NodeID(b'n'*16),hashlib.sha256(data).hexdigest(),None if stored else data)

def _publish(table,execution,results):
    # Do not carry a validated plan through an unlocked boundary.
    with table._lock:
        plan=table.validate_publish_task_outputs(execution,results)
        if plan is None:return False
        table.commit_validated_publish_task_outputs(plan)
        return True

def test_single_manifest_derives_stable_identity_across_attempts():
    spec=_spec();execution=TaskExecutionKey.from_task_spec(spec)
    assert MAX_TASK_RETURNS==validate_num_returns(1)==1
    assert execution.output_ids==(ObjectID.for_task(spec.task_id),)
    assert execution.for_attempt(spec.attempt_id.next()).output_ids==execution.output_ids

@pytest.mark.parametrize('invalid',(True,False,0,2,1.5,'1'))
def test_manifest_rejects_non_single_public_counts(invalid):
    with pytest.raises((TypeError,ValueError)):validate_num_returns(invalid)

def test_manifest_rejects_missing_duplicate_or_foreign_identity():
    spec=_spec();output=spec.return_ids()[0]
    for ids in ((),(output,output),(ObjectID.for_task(TaskID.random()),)):
        with pytest.raises(ValueError):TaskOutputManifest(spec.task_id,ids)
    with pytest.raises(ValueError):TaskExecutionKey(TaskOutputManifest.from_task_spec(spec),AttemptID(TaskID.random(),0))

def test_registration_preflight_is_side_effect_free_and_exact_replay_is_idempotent():
    spec=_spec();table=ObjectOwnerTable();output=spec.return_ids()[0]
    plan=table.validate_register_task_outputs(spec,local_tokens=('handle',))
    assert not table.contains(output)
    table.commit_register_task_outputs(plan);before=table.snapshot(output)
    assert before.state is ObjectState.PENDING and before.local_tokens==frozenset({'handle'})
    table.commit_register_task_outputs(plan)
    assert table.snapshot(output)==before

def test_incompatible_existing_registration_is_rejected_without_mutation():
    spec=_spec();table=ObjectOwnerTable();output=spec.return_ids()[0]
    table.register(output,current_attempt=spec.attempt_id,producer_task_spec='different-producer')
    before=table.snapshot(output)
    with pytest.raises((ObjectAlreadyRegisteredError,ValueError)):
        table.validate_register_task_outputs(spec)
    assert table.snapshot(output)==before

@pytest.mark.parametrize('stored',(False,True))
def test_single_success_preflights_then_publishes_one_exact_value(stored):
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);result=_result(spec,stored)
    execution=TaskExecutionKey.from_task_spec(spec)
    with table._lock:
        plan=table.validate_publish_task_outputs(execution,(result,))
        assert table.snapshot(result.object_id).state is ObjectState.PENDING
        table.commit_validated_publish_task_outputs(plan)
    before=table.snapshot(result.object_id)
    assert before.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
    assert _publish(table,execution,(result,))
    assert table.snapshot(result.object_id)==before

def test_success_requires_exact_object_and_registered_owner_before_mutation():
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);result=_result(spec)
    execution=TaskExecutionKey.from_task_spec(spec);before=table.snapshot(result.object_id)
    for values in ((),(result,result),(replace(result,object_id=ObjectID.for_task(TaskID.random())),),(replace(result,owner_worker_id=WorkerID.random()),)):
        with pytest.raises(ValueError):table.validate_publish_task_outputs(execution,values)
        assert table.snapshot(result.object_id)==before

@pytest.mark.parametrize('field',('object_id','owner_worker_id','node_id','size_bytes','checksum'))
def test_stored_replay_binds_complete_descriptor_without_mutation(field):
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);result=_result(spec,True);execution=TaskExecutionKey.from_task_spec(spec)
    assert _publish(table,execution,(result,));before=table.snapshot(result.object_id)
    values={'object_id':ObjectID.for_task(TaskID.random()),'owner_worker_id':WorkerID.random(),'node_id':NodeID.random(),'size_bytes':999,'checksum':'a'*64}
    with pytest.raises((ValueError,ConflictingObjectResultError)):_publish(table,execution,(replace(result,**{field:values[field]}),))
    assert table.snapshot(result.object_id)==before

def test_stored_replay_attempt_drift_is_fenced_without_mutation():
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);result=_result(spec,True);execution=TaskExecutionKey.from_task_spec(spec)
    assert _publish(table,execution,(result,));before=table.snapshot(result.object_id)
    assert not _publish(table,execution.for_attempt(spec.attempt_id.next()),(result,))
    assert table.snapshot(result.object_id)==before

def test_stale_success_preflight_is_revalidated_after_boundary():
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);result=_result(spec);execution=TaskExecutionKey.from_task_spec(spec)
    plan=table.validate_publish_task_outputs(execution,(result,));assert plan is not None
    table.publish_inline(result.object_id,spec.attempt_id,b'different')
    before=table.snapshot(result.object_id)
    with pytest.raises(ConflictingObjectResultError):table.validate_publish_task_outputs(plan.execution,plan.results)
    assert table.snapshot(result.object_id)==before

def test_attempt_advance_preflights_and_fences_stale_expected_attempt():
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);execution=TaskExecutionKey.from_task_spec(spec)
    plan=table.validate_advance_task_outputs(execution,spec.attempt_id.next())
    assert table.snapshot(spec.return_ids()[0]).current_attempt==spec.attempt_id
    assert table.commit_advance_task_outputs(plan)
    assert table.commit_advance_task_outputs(plan)
    stale=table.validate_advance_task_outputs(execution,spec.attempt_id.next().next())
    assert not table.commit_advance_task_outputs(stale)
    assert table.snapshot(spec.return_ids()[0]).current_attempt==spec.attempt_id.next()

def test_ready_output_cannot_advance_without_loss_transition():
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);output=spec.return_ids()[0]
    table.publish_inline(output,spec.attempt_id,b'value');before=table.snapshot(output)
    with pytest.raises(InvalidObjectTransitionError):table.validate_advance_task_outputs(TaskExecutionKey.from_task_spec(spec),spec.attempt_id.next())
    assert table.snapshot(output)==before

def test_error_preflight_commit_and_replay_preserve_exact_error():
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);execution=TaskExecutionKey.from_task_spec(spec);error=RuntimeError('failed')
    plan=table.validate_publish_task_error(execution,error);assert plan is not None
    assert table.snapshot(spec.return_ids()[0]).state is ObjectState.PENDING
    assert table.commit_publish_task_error(plan)
    assert table.snapshot(spec.return_ids()[0]).error is error
    assert table.publish_task_error(execution,error)

def test_stale_error_preflight_cannot_replace_another_terminal_error():
    spec=_spec();table=ObjectOwnerTable();table.register_task_outputs(spec);execution=TaskExecutionKey.from_task_spec(spec)
    plan=table.validate_publish_task_error(execution,RuntimeError('first'))
    table.publish_error(spec.return_ids()[0],spec.attempt_id,RuntimeError('second'));before=table.snapshot(spec.return_ids()[0])
    with pytest.raises((InvalidObjectTransitionError,ConflictingObjectResultError)):table.commit_publish_task_error(plan)
    assert table.snapshot(spec.return_ids()[0])==before

def test_single_output_collection_retires_producer_lineage_after_exact_claim():
    spec=_spec();recovery=RecoveryManager();recovery.register_task(spec,max_retries=2);recovery.record_task_success(spec.task_id,spec.attempt_id)
    output=spec.return_ids()[0];active=recovery.request_reconstruction(output).attempt_id
    assert recovery.lineage_for_object(output) is not None and recovery.active_recovery(spec.task_id)==active
    plan=recovery.validate_forget_collected_object(output,expected_task_spec=spec,expected_attempt=active)
    assert plan.remove_task and recovery.lineage_for_object(output) is not None
    assert recovery.commit_forget_collected_object(plan)
    assert recovery.lineage_for_object(output) is None and recovery.active_recovery(spec.task_id) is None
    with pytest.raises(UnknownTaskError):recovery.task_record(spec.task_id)
