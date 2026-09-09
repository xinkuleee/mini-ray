"""Importable ordinary functions with one persistent state per real Worker.

Do not decorate these names: cloudpickle must find each original callable at
its module attribute. Tests apply ray.remote to these unchanged functions.
Only the genuine owner-local put handle survives a Task boundary; imported
argument borrowers are never saved after their attempt closes.
"""

import os
import time

import miniray as ray
from miniray.runtime_binding import current_core_worker


PADDING = b"G" * 2048
saved_box = None
produce_calls = 0


def produce():
    global produce_calls
    produce_calls += 1
    if saved_box is None:
        return {"pid": os.getpid(), "calls": produce_calls, "padding": PADDING}
    return {"pid": os.getpid(), "calls": produce_calls,
            "padding": PADDING, "box": saved_box}


def retain(items):
    global saved_box
    assert saved_box is None and len(items) == 1
    source = items[0]
    assert isinstance(source, ray.ObjectRef) and source.borrower_token is not None
    saved_box = ray.put([source])
    core = current_core_worker()
    assert core is not None and saved_box.owner_worker_id == core.worker_id
    assert saved_box.borrower_token is None and not saved_box.closed
    publication = core._publication_client().current(saved_box.object_id)
    assert publication is not None
    return os.getpid(), saved_box.object_id, saved_box.owner_worker_id, publication.reference


def clear(deadline):
    global saved_box
    if saved_box is None:
        return os.getpid(), produce_calls, False
    box, saved_box = saved_box, None
    done = box._release_done
    box.close(timeout=max(0.0, deadline - time.monotonic()))
    assert box.closed and done is not None and done.is_set()
    return os.getpid(), produce_calls, True


def echo_container(container):
    return {"child": container[0], "padding": PADDING}
