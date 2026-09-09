"""Historical close/PID helper extracted without behavioral changes.

Used only by retained legacy tests being migrated; not a public-close or
physical-GC proof. Extraction avoids importing retired multi-return decorators.
"""

import os
import time


def _close_local(reference, deadline):
    if reference is None or reference.closed:
        return
    assert reference.borrower_token is None
    reference._closed = True
    if reference._finalizer is not None:
        reference._finalizer()
    if reference._release_done is not None:
        assert reference._release_done.wait(max(0.0, deadline - time.monotonic()))


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
