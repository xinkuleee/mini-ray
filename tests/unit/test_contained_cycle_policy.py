"""Pure Python argument-cycle contract retained for the base edition.

A self-referential Python container is a serialized value, not an ObjectID
contained-reference edge. Global ObjectID cycle admission belongs to the
enhanced edition and is tracked by the K3 central-test migration ledger.
No runtime constructor, thread, socket, process, timer or wait is used.
"""

import pytest

from miniray.dependency import decode_inline_argument, encode_task_argument
from miniray.protocol import InlineArg


@pytest.mark.unit
def test_python_container_cycle_is_serialized_without_an_object_id_edge() -> None:
    value: list[object] = []
    value.append(value)

    encoded = encode_task_argument(value)

    assert isinstance(encoded, InlineArg)
    assert encoded.nested_refs == ()
    decoded = decode_inline_argument(encoded)
    assert isinstance(decoded, list)
    assert decoded[0] is decoded
