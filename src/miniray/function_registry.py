"""Authoritative registry for immutable remote-function definitions.

The GCS owns this metadata table, while its RPC facade remains in
``control.py``. Registration decides idempotency and conflicts; the facade
only translates those decisions into protocol replies and trace events.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from threading import RLock
from typing import Hashable, Tuple

from .errors import FunctionNotRegisteredError, FunctionRegistryError
from .protocol import FunctionDefinition, FunctionKey


class UnknownFunctionError(FunctionNotRegisteredError):
    """Raised when a function payload has not been exported."""


class FunctionRegistrationConflictError(FunctionRegistryError):
    """Raised when one function ID is re-used for different bytes."""


@dataclass(frozen=True)
class FunctionSnapshot:
    """Payload-free metadata suitable for diagnostics."""

    function_id: Hashable
    size_bytes: int
    sha256: str


class FunctionRegistry:
    """Store serialized remote functions by logical function ID.

    A repeated export with identical bytes is an idempotent replay. Different
    bytes under the same ID are a protocol error and are never overwritten.
    """

    def __init__(self) -> None:
        self._payloads: dict[Hashable, bytes] = {}
        self._lock = RLock()

    def register(
        self, function_id: Hashable, payload: bytes | bytearray | memoryview
    ) -> bool:
        """Store ``payload`` and return whether a new entry was created."""

        _require_hashable(function_id, "function_id")
        try:
            immutable_payload = bytes(payload)
        except (TypeError, ValueError) as exc:
            raise TypeError("function payload must be bytes-like") from exc

        with self._lock:
            old = self._payloads.get(function_id)
            if old is None:
                self._payloads[function_id] = immutable_payload
                return True
            if old != immutable_payload:
                raise FunctionRegistrationConflictError(
                    "function {!r} is already registered with different "
                    "payload bytes".format(function_id)
                )
            return False

    register_function = register

    def register_definition(self, definition: FunctionDefinition) -> bool:
        if not isinstance(definition, FunctionDefinition):
            raise TypeError("definition must be a FunctionDefinition")
        return self.register(definition.key, definition.payload)

    def get(self, function_id: Hashable) -> bytes:
        with self._lock:
            try:
                return self._payloads[function_id]
            except KeyError:
                raise UnknownFunctionError(
                    "unknown function: {!r}".format(function_id)
                ) from None

    get_function = get

    def get_definition(self, key: FunctionKey) -> FunctionDefinition:
        if not isinstance(key, FunctionKey):
            raise TypeError("key must be a FunctionKey")
        return FunctionDefinition.from_payload(key, self.get(key))

    def contains(self, function_id: Hashable) -> bool:
        with self._lock:
            return function_id in self._payloads

    def snapshot(self) -> Tuple[FunctionSnapshot, ...]:
        with self._lock:
            return tuple(
                FunctionSnapshot(
                    function_id=function_id,
                    size_bytes=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
                for function_id, payload in sorted(
                    self._payloads.items(), key=lambda item: repr(item[0])
                )
            )


def _require_hashable(value: object, name: str) -> None:
    try:
        hash(value)
    except (TypeError, ValueError) as exc:
        raise TypeError("{} must be hashable".format(name)) from exc


__all__ = [
    "FunctionRegistrationConflictError",
    "FunctionRegistry",
    "FunctionSnapshot",
    "UnknownFunctionError",
]
