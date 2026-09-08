"""Pure public API contracts for ``put`` and ordinary-task retry options."""

from __future__ import annotations

import inspect

import pytest

import miniray as ray
from miniray import protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, TaskID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _identity(value: object) -> object:
    return value


class _Actor:
    def ping(self) -> str:
        return "pong"


def test_put_is_a_public_single_value_api() -> None:
    assert ray.put is not None
    assert "put" in ray.__all__
    assert tuple(inspect.signature(ray.put).parameters) == ("value",)
    with pytest.raises(RuntimeError, match=r"init()"):
        ray.put(1)


@pytest.mark.parametrize("invalid", [True, -1, 1.5, "1"])
def test_max_retries_rejects_non_negative_integer_violations(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError), match="max_retries"):
        ray.remote(max_retries=invalid)(_identity)


def test_max_retries_is_supported_only_for_remote_functions_and_options_copy() -> None:
    remote_function = ray.remote(num_cpus=0, max_retries=2)(_identity)
    overridden = remote_function.options(max_retries=1)

    assert isinstance(remote_function, ray.RemoteFunction)
    assert isinstance(overridden, ray.RemoteFunction)
    assert overridden is not remote_function
    assert remote_function._max_retries == 2
    assert overridden._max_retries == 1

    with pytest.raises(TypeError, match="max_retries.*Actor|Actor.*max_retries"):
        ray.remote(max_retries=1)(_Actor)

    actor_class = ray.remote(num_cpus=0)(_Actor)
    with pytest.raises(TypeError, match="max_retries.*Actor|Actor.*max_retries"):
        actor_class.options(max_retries=1)


@pytest.mark.parametrize("invalid", [True, -1, 1.5, "1"])
def test_task_spec_rejects_invalid_max_retries(invalid: object) -> None:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    key = protocol.FunctionKey(job_id, __name__, "_identity", "v1")
    with pytest.raises(ProtocolError, match="max_retries"):
        protocol.TaskSpec(
            job_id=job_id,
            task_id=task_id,
            attempt_id=AttemptID(task_id, 0),
            function=key,
            args=(),
            num_returns=1,
            resources=ResourceVector(),
            owner_worker_id=WorkerID.random(),
            max_retries=invalid,  # type: ignore[arg-type]
        )
