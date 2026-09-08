"""Finite metadata-only examples of locality-first lease routing.

No Core, Node, resource ledger, runtime fixture, transport, wait, or user task
is constructed. Each case has at most five locality records and three nodes.
Resource feasibility and live-route filtering belong to other components, not
to the bytes-first hint tested here.
"""

from dataclasses import FrozenInstanceError

import pytest

from miniray.ids import NodeID, ObjectID, TaskID
from miniray.lease_policy import ObjectLocality, preferred_lease_node


pytestmark = pytest.mark.unit


def _node(index: int) -> NodeID:
    return NodeID(bytes([index]) * 16)


def _object(index: int) -> ObjectID:
    return ObjectID(TaskID(bytes([17]) * 16), index)


def test_locality_counts_bytes_instead_of_object_count():
    home, data_node = _node(1), _node(2)
    objects = (
        ObjectLocality(_object(0), 40, (home,)),
        ObjectLocality(_object(1), 40, (home,)),
        ObjectLocality(_object(2), 100, (data_node,)),
    )

    assert preferred_lease_node(objects, fallback_node_id=home) == data_node


def test_locality_sums_distinct_objects_not_only_the_largest():
    home, data_node = _node(1), _node(2)
    objects = (
        ObjectLocality(_object(0), 100, (home,)),
        ObjectLocality(_object(1), 60, (data_node,)),
        ObjectLocality(_object(2), 50, (data_node,)),
    )

    assert preferred_lease_node(objects, fallback_node_id=home) == data_node


def test_shared_dependencies_and_duplicate_locations_count_once():
    home, remote = _node(1), _node(2)
    shared = ObjectLocality(_object(0), 40, (remote, remote))
    objects = (shared, shared, shared, ObjectLocality(_object(1), 60, (home,)))

    assert preferred_lease_node(objects, fallback_node_id=remote) == home


def test_repeated_records_union_replicas_and_count_each_nodes_local_bytes():
    first, second, fallback = _node(1), _node(2), _node(3)
    objects = (
        ObjectLocality(_object(0), 80, (first,)),
        ObjectLocality(_object(0), 80, (second,)),
        ObjectLocality(_object(1), 30, (first,)),
        ObjectLocality(_object(2), 20, (second,)),
        ObjectLocality(_object(3), 105, (fallback,)),
    )

    # Scores are 110, 100, 105. Retaining only the first or last record
    # for object 0 changes one of these two decisions incorrectly.
    assert preferred_lease_node(objects, fallback_node_id=fallback) == first
    assert preferred_lease_node(reversed(objects), fallback_node_id=fallback) == first


def test_positive_tie_prefers_home_even_when_its_id_sorts_last():
    remote, home = _node(1), _node(3)
    objects = (ObjectLocality(_object(0), 80, (remote, home)),)

    assert preferred_lease_node(objects, fallback_node_id=home) == home


def test_tie_without_home_uses_node_id_not_input_order():
    first, second, home = _node(1), _node(2), _node(3)
    objects = (
        ObjectLocality(_object(1), 80, (second,)),
        ObjectLocality(_object(0), 80, (first,)),
    )

    assert preferred_lease_node(objects, fallback_node_id=home) == first
    assert preferred_lease_node(reversed(objects), fallback_node_id=home) == first


def test_absent_or_zero_locality_leaves_the_callers_fallback_unchanged():
    home, remote = _node(1), _node(2)
    unknown = ObjectLocality(_object(0), 100, ())
    zero = ObjectLocality(_object(1), 0, (remote, home))

    for objects in ((), (unknown,), (zero,), (unknown, zero)):
        assert preferred_lease_node(objects, fallback_node_id=home) is None


def test_conflicting_sizes_are_rejected_even_without_locality_contribution():
    home, remote = _node(1), _node(2)
    for first_size, second_size, locations in ((40, 41, (remote,)), (0, 1, ())):
        objects = (
            ObjectLocality(_object(0), first_size, locations),
            ObjectLocality(_object(0), second_size, locations),
            ObjectLocality(_object(1), 100, (home,)),
        )
        for ordering in (objects, tuple(reversed(objects))):
            with pytest.raises(ValueError, match="conflicting size_bytes"):
                preferred_lease_node(ordering, fallback_node_id=home)


def test_locality_copies_and_freezes_metadata_without_mutating_inputs():
    first, second = _node(1), _node(2)
    locations = [second, first, second]
    item = ObjectLocality(_object(0), 80, locations)
    assert locations == [second, first, second]
    assert item.locations == (first, second)
    locations.clear()
    assert item.locations == (first, second)
    with pytest.raises(FrozenInstanceError):
        item.size_bytes = 0

    objects = [item, ObjectLocality(_object(1), 10, (second,))]
    before = tuple(objects)
    assert preferred_lease_node(objects, fallback_node_id=first) == second
    assert tuple(objects) == before
    assert item.locations == (first, second)


def test_locality_accepts_a_single_pass_metadata_iterator():
    home, remote = _node(1), _node(2)
    objects = (
        ObjectLocality(_object(0), 40, (home,)),
        ObjectLocality(_object(1), 80, (remote,)),
    )
    seen = []

    def records():
        for item in objects:
            seen.append(item.object_id)
            yield item

    assert preferred_lease_node(records(), fallback_node_id=home) == remote
    assert seen == [_object(0), _object(1)]


def test_locality_rejects_boolean_negative_and_non_integer_sizes():
    for size in (False, True, -1, 1.5, "2", None):
        with pytest.raises(ValueError, match="size_bytes"):
            ObjectLocality(_object(0), size, (_node(1),))


def test_locality_rejects_wrong_identity_and_record_types():
    home = _node(1)
    for object_id in (home, _object(0).task_id, "object", None):
        with pytest.raises(ValueError, match="object_id"):
            ObjectLocality(object_id, 1, (home,))
    for node_id in (_object(0).task_id, "node", None):
        with pytest.raises(ValueError, match="NodeIDs"):
            ObjectLocality(_object(0), 1, (node_id,))
        with pytest.raises(ValueError, match="fallback_node_id"):
            preferred_lease_node((), fallback_node_id=node_id)
    with pytest.raises(ValueError, match="locations"):
        ObjectLocality(_object(0), 1, None)
    with pytest.raises(ValueError, match="ObjectLocality"):
        preferred_lease_node((object(),), fallback_node_id=home)
