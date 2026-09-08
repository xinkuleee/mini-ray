"""Stable, executable contracts over volatile runtime trace records.

Raw :class:`~miniray.protocol.TraceRecord` values are diagnostic evidence: they
contain process IDs, event IDs, RPC IDs, and timestamps from one particular
run.  A golden teaching trace should instead describe the architecture that
must remain true across runs.  This module provides that second layer.

Contracts are small JSON resources.  ``$name`` field values bind dynamic
identities, RPC rules verify concrete send-to-receive causal edges, and order
rules use only per-process sequence numbers plus ``cause_event_id``.  Wall-clock
timestamps and the lexical values of generated IDs are never compared.

RPC reply/server anchors associate a business fact with one concrete round
trip through explicit cause links. A phase name alone is not an ACK. The
packaged success contract describes one isolated, reference-free Task; it is
not a general concurrent-trace model checker or a runtime progress authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import resources
from typing import (
    Dict,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from . import protocol


TRACE_CONTRACT_SCHEMA = "miniray.trace-contract/v1"
SUCCESS_TRACE_CONTRACT = "ordinary_task_success"
APPLICATION_ERROR_TRACE_CONTRACT = "ordinary_task_application_error"
BUILTIN_TRACE_CONTRACTS = (
    SUCCESS_TRACE_CONTRACT,
    APPLICATION_ERROR_TRACE_CONTRACT,
)


class TraceContractError(ValueError):
    """A saved contract is malformed or names an unknown built-in."""


class TraceContractMismatch(AssertionError):
    """Observed records do not satisfy an executable trace contract."""


@dataclass(frozen=True)
class EventRule:
    key: str
    component: str
    event: str
    fields: Tuple[Tuple[str, str], ...] = ()
    min_count: int = 1
    max_count: Optional[int] = None
    show_fields: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RPCRule:
    """One physical RPC, optionally tied to a scoped business observation.

    ``reply_event`` must directly follow the client's actual received reply.
    ``server_event`` must lie on the handler's explicit cause chain. Neither
    is inferred from a handler name or another thread's observation order.
    """
    key: str
    client: str
    server: str
    handler: str
    client_component: Optional[str] = None
    server_component: Optional[str] = None
    round_trip: bool = False
    reply_ok: Optional[str] = None
    after_event: Optional[str] = None
    before_event: Optional[str] = None
    reply_event: Optional[str] = None
    server_event: Optional[str] = None


@dataclass(frozen=True)
class ForbiddenRule:
    component: str
    event: Optional[str] = None
    event_prefixes: Tuple[str, ...] = ()
    fields: Tuple[Tuple[str, str], ...] = ()


@dataclass(frozen=True)
class OrderRule:
    before: str
    after: str


@dataclass(frozen=True)
class TraceContract:
    """A stable semantic pattern loaded from a repository JSON resource."""

    name: str
    title: str
    participants: Tuple[Tuple[str, str], ...]
    input_bindings: Tuple[str, ...]
    events: Tuple[EventRule, ...]
    rpcs: Tuple[RPCRule, ...]
    orders: Tuple[OrderRule, ...]
    forbidden: Tuple[ForbiddenRule, ...]
    render_order: Tuple[str, ...]

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "TraceContract":
        """Validate and construct the deliberately small v1 JSON schema."""

        if value.get("schema") != TRACE_CONTRACT_SCHEMA:
            raise TraceContractError(
                "trace contract schema must be {!r}".format(
                    TRACE_CONTRACT_SCHEMA
                )
            )
        name = _required_text(value, "name")
        title = _required_text(value, "title")
        participants_value = _required_mapping(value, "participants")
        participants = tuple(
            (
                _nonempty_text(key, "participant key"),
                _nonempty_text(label, "participant label"),
            )
            for key, label in participants_value.items()
        )
        input_bindings = _text_tuple(value.get("input_bindings", ()), "input_bindings")
        events = tuple(
            _event_rule(item)
            for item in _mapping_sequence(value.get("events", ()), "events")
        )
        rpcs = tuple(
            _rpc_rule(item)
            for item in _mapping_sequence(value.get("rpcs", ()), "rpcs")
        )
        orders = tuple(
            _order_rule(item)
            for item in _mapping_sequence(value.get("orders", ()), "orders")
        )
        forbidden = tuple(
            _forbidden_rule(item)
            for item in _mapping_sequence(
                value.get("forbidden", ()), "forbidden"
            )
        )
        render_order = _text_tuple(
            value.get("render_order", ()), "render_order"
        )

        keys = tuple(rule.key for rule in events) + tuple(
            rule.key for rule in rpcs
        )
        if not keys or len(keys) != len(set(keys)):
            raise TraceContractError(
                "event and RPC rule keys must be non-empty and unique"
            )
        known_points = set(keys)
        event_keys = {rule.key for rule in events}
        for rpc in rpcs:
            if any(
                anchor is not None and anchor not in event_keys
                for anchor in (
                    rpc.after_event, rpc.before_event,
                    rpc.reply_event, rpc.server_event,
                )
            ):
                raise TraceContractError(
                    "RPC event anchors must name event rules"
                )
            if (rpc.reply_event is not None or rpc.server_event is not None) and not rpc.round_trip:
                raise TraceContractError(
                    "RPC reply_event/server_event requires round_trip=true"
                )
            suffixes = ["request_sent", "request_received"]
            if rpc.round_trip:
                suffixes.extend(("reply_sent", "reply_received"))
            known_points.update(
                "{}.{}".format(rpc.key, suffix)
                for suffix in suffixes
            )
        for order in orders:
            if order.before not in known_points or order.after not in known_points:
                raise TraceContractError(
                    "order rule references an unknown point: {} -> {}".format(
                        order.before, order.after
                    )
                )
        if len(render_order) != len(set(render_order)) or not set(
            render_order
        ).issubset(known_points):
            raise TraceContractError(
                "render_order must contain unique event, RPC or RPC endpoint keys"
            )
        return cls(
            name=name,
            title=title,
            participants=participants,
            input_bindings=input_bindings,
            events=events,
            rpcs=rpcs,
            orders=orders,
            forbidden=forbidden,
            render_order=render_order,
        )

    def match(
        self,
        records: Iterable[protocol.TraceRecord],
        **bindings: object
    ) -> "TraceContractMatch":
        """Match a snapshot without consulting timestamps or raw ID values."""

        return _match_contract(self, records, bindings)


@dataclass(frozen=True)
class TraceContractMatch:
    """One contract evaluation and its deterministic symbolic bindings."""

    contract: TraceContract
    records: Tuple[protocol.TraceRecord, ...]
    bindings: Mapping[str, str]
    event_matches: Mapping[str, Tuple[protocol.TraceRecord, ...]]
    rpc_matches: Mapping[str, Tuple[protocol.TraceRecord, ...]]
    violations: Tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.violations

    def explain(self) -> str:
        if self.ok:
            return "trace satisfies contract {!r}".format(self.contract.name)
        return "trace contract {!r} failed:\n{}".format(
            self.contract.name,
            "\n".join("- {}".format(item) for item in self.violations),
        )

    def require(self) -> "TraceContractMatch":
        if not self.ok:
            raise TraceContractMismatch(self.explain())
        return self

    def render_sequence(self) -> str:
        """Render the matched canonical path without volatile identities."""

        self.require()
        return _render_match(self)


def load_trace_contract(name: str) -> TraceContract:
    """Load one of the two packaged, reviewable golden contracts."""

    if name not in BUILTIN_TRACE_CONTRACTS:
        raise TraceContractError(
            "unknown built-in trace contract {!r}; choose {}".format(
                name, ", ".join(BUILTIN_TRACE_CONTRACTS)
            )
        )
    package = resources.files("miniray.golden_traces")
    text = package.joinpath("{}.json".format(name)).read_text(encoding="utf-8")
    try:
        value = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise TraceContractError(
            "could not decode trace contract {!r}".format(name)
        ) from exc
    if not isinstance(value, dict):
        raise TraceContractError("trace contract root must be a JSON object")
    contract = TraceContract.from_dict(value)
    if contract.name != name:
        raise TraceContractError(
            "trace contract resource name disagrees with its contents"
        )
    return contract


def render_trace_sequence(
    records: Iterable[protocol.TraceRecord],
    contract: TraceContract,
    **bindings: object
) -> str:
    """Convenience API used by examples: match, validate, then render."""

    return contract.match(records, **bindings).render_sequence()


def _match_contract(
    contract: TraceContract,
    records: Iterable[protocol.TraceRecord],
    supplied_bindings: Mapping[str, object],
) -> TraceContractMatch:
    snapshot = tuple(records)
    if not all(isinstance(record, protocol.TraceRecord) for record in snapshot):
        raise TypeError("trace contract records must all be TraceRecord values")
    ordered = tuple(sorted(snapshot, key=_record_key))
    bound = {}  # type: Dict[str, str]
    violations = []  # type: list[str]
    if len({record.event_id for record in snapshot}) != len(snapshot):
        violations.append("trace event IDs must be unique")
    for name, raw in supplied_bindings.items():
        if isinstance(raw, str):
            value = raw
        else:
            value = str(raw)
        if not name or not value:
            raise ValueError("trace bindings must have non-empty names and values")
        bound[name] = value
    for name in contract.input_bindings:
        if name not in bound:
            violations.append(
                "missing input binding ${}".format(name)
            )

    event_matches = {}  # type: Dict[str, Tuple[protocol.TraceRecord, ...]]
    for rule in contract.events:
        first = None  # type: Optional[protocol.TraceRecord]
        first_bindings = None  # type: Optional[Dict[str, str]]
        for record in ordered:
            candidate = dict(bound)
            if _event_matches(record, rule, candidate, allow_capture=True):
                first = record
                first_bindings = candidate
                break
        if first_bindings is not None:
            bound.update(first_bindings)
        matches = tuple(
            record
            for record in ordered
            if _event_matches(
                record, rule, dict(bound), allow_capture=False
            )
        )
        event_matches[rule.key] = matches
        if len(matches) < rule.min_count:
            violations.append(
                "event {} requires at least {} occurrence(s), observed {}".format(
                    _event_rule_name(rule), rule.min_count, len(matches)
                )
            )
        if rule.max_count is not None and len(matches) > rule.max_count:
            violations.append(
                "event {} allows at most {} occurrence(s), observed {}".format(
                    _event_rule_name(rule), rule.max_count, len(matches)
                )
            )

    rpc_matches = {}  # type: Dict[str, Tuple[protocol.TraceRecord, ...]]
    used_requests = set()  # type: set[str]
    selected_anchors = {}  # type: Dict[str, protocol.TraceRecord]
    by_id = {record.event_id: record for record in ordered}
    for rule in contract.rpcs:
        # One semantic fact can anchor an enclosing handler and its nested
        # RPC. Once selected, both rules must keep the same observation even
        # if another physical retry later acknowledges the same publication.
        candidate_events = dict(event_matches)
        candidate_events.update((key, (event,)) for key, event in selected_anchors.items())
        match = _find_rpc(ordered, rule, candidate_events, used_requests)
        rpc_matches[rule.key] = match
        if not match:
            suffix = " round trip" if rule.round_trip else " request edge"
            violations.append(
                "missing cross-process RPC{} {} -> {}: {}".format(
                    suffix, rule.client, rule.server, rule.handler
                )
            )
        else:
            used_requests.add(match[0].event_id)
            # A phase may be acknowledged again by a new physical RPC. Keep
            # its observed count, but bind orders/rendering to the particular
            # acknowledgement belonging to this complete round trip.
            for key, selected in (
                (rule.reply_event, _reply_anchor(rule, match, candidate_events)),
                (rule.server_event, _server_anchor(rule, match, candidate_events, by_id)),
            ):
                if key is not None and selected is not None:
                    selected_anchors[key] = selected
                    event_matches[key] = (selected,) + tuple(
                        record for record in event_matches[key]
                        if record.event_id != selected.event_id
                    )
    adjacency = _causal_graph(ordered)
    for order in contract.orders:
        before = _point_record(order.before, event_matches, rpc_matches)
        after = _point_record(order.after, event_matches, rpc_matches)
        if (
            before is not None
            and after is not None
            and not _reachable(before.event_id, after.event_id, adjacency)
        ):
            violations.append(
                "causal order is not established: {} -> {}".format(
                    order.before, order.after
                )
            )

    for rule in contract.forbidden:
        forbidden = tuple(
            record
            for record in ordered
            if _forbidden_matches(record, rule, bound)
        )
        if forbidden:
            names = ", ".join(
                "{}.{}".format(record.component, record.event)
                for record in forbidden[:3]
            )
            violations.append(
                "forbidden event(s) observed: {}".format(names)
            )

    return TraceContractMatch(
        contract=contract,
        records=snapshot,
        bindings=dict(bound),
        event_matches=event_matches,
        rpc_matches=rpc_matches,
        violations=tuple(violations),
    )


def _event_matches(
    record: protocol.TraceRecord,
    rule: EventRule,
    bindings: Dict[str, str],
    *,
    allow_capture: bool
) -> bool:
    return (
        record.component == rule.component
        and record.event == rule.event
        and _fields_match(
            dict(record.fields), rule.fields, bindings,
            allow_capture=allow_capture,
        )
    )


def _fields_match(
    observed: Mapping[str, str],
    expected: Sequence[Tuple[str, str]],
    bindings: Dict[str, str],
    *,
    allow_capture: bool
) -> bool:
    for field, pattern in expected:
        actual = observed.get(field)
        if actual is None:
            return False
        if pattern.startswith("$"):
            name = pattern[1:]
            previous = bindings.get(name)
            if previous is None:
                if not allow_capture:
                    return False
                bindings[name] = actual
            elif previous != actual:
                return False
        elif pattern != actual:
            return False
    return True


def _find_rpc(
    records: Sequence[protocol.TraceRecord],
    rule: RPCRule,
    event_matches: Mapping[str, Tuple[protocol.TraceRecord, ...]],
    used_requests: set[str],
) -> Tuple[protocol.TraceRecord, ...]:
    client_component = rule.client_component or rule.client
    server_component = rule.server_component or rule.server
    by_id = {record.event_id: record for record in records}
    after = (
        event_matches.get(rule.after_event, ())[0]
        if rule.after_event is not None
        and event_matches.get(rule.after_event, ())
        else None
    )
    before = (
        event_matches.get(rule.before_event, ())[0]
        if rule.before_event is not None
        and event_matches.get(rule.before_event, ())
        else None
    )
    sends = tuple(
        record
        for record in records
        if record.component == client_component
        and record.event_id not in used_requests
        and record.event == "rpc_request_sent"
        and dict(record.fields).get("handler") == rule.handler
        and (
            after is None
            or (
                record.process_id == after.process_id
                and record.process_sequence > after.process_sequence
            )
        )
        and (
            before is None
            or (
                record.process_id == before.process_id
                and record.process_sequence < before.process_sequence
            )
        )
    )
    for sent in sends:
        sent_fields = dict(sent.fields)
        rpc_id = sent_fields.get("rpc_id")
        if not rpc_id:
            continue
        received = next(
            (
                record
                for record in records
                if record.component == server_component
                and record.event == "rpc_request_received"
                and record.cause_event_id == sent.event_id
                and record.process_id != sent.process_id
                and dict(record.fields).get("handler") == rule.handler
                and dict(record.fields).get("rpc_id") == rpc_id
            ),
            None,
        )
        if received is None:
            continue
        if not rule.round_trip:
            return sent, received
        for reply_sent in records:
            if not (
                reply_sent.component == server_component
                and reply_sent.event == "rpc_reply_sent"
                and reply_sent.process_id == received.process_id
                and reply_sent.process_sequence > received.process_sequence
                and dict(reply_sent.fields).get("handler") == rule.handler
                and dict(reply_sent.fields).get("rpc_id") == rpc_id
                and (rule.reply_ok is None or dict(reply_sent.fields).get("ok") == rule.reply_ok)
                and _explicitly_follows(received.event_id, reply_sent, by_id)
            ):
                continue
            for reply_received in records:
                if not (
                    reply_received.component == client_component
                    and reply_received.event == "rpc_reply_received"
                    and reply_received.cause_event_id == reply_sent.event_id
                    and reply_received.process_id == sent.process_id
                    and reply_received.process_sequence > sent.process_sequence
                    and dict(reply_received.fields).get("handler") == rule.handler
                    and dict(reply_received.fields).get("rpc_id") == rpc_id
                    and (rule.reply_ok is None or dict(reply_received.fields).get("ok") == rule.reply_ok)
                ):
                    continue
                match = sent, received, reply_sent, reply_received
                if rule.reply_event is not None and _reply_anchor(rule, match, event_matches) is None:
                    continue
                if rule.server_event is not None and _server_anchor(rule, match, event_matches, by_id) is None:
                    continue
                return match
    return ()


def _explicitly_follows(
    predecessor_id: str, record: protocol.TraceRecord,
    by_id: Mapping[str, protocol.TraceRecord],
) -> bool:
    """Follow only emitted cause links, not another thread's process order.

    Nested handler/RPC scopes form a chain through their real send and receive
    events. A matching name, timestamp or later process sequence cannot prove
    that a business fact belongs to this particular request handler.
    """
    seen = {record.event_id}
    cause = record.cause_event_id
    while cause is not None and cause not in seen:
        if cause == predecessor_id:
            return True
        seen.add(cause)
        parent = by_id.get(cause)
        if parent is None:
            return False
        cause = parent.cause_event_id
    return False


def _reply_anchor(rule, match, events) -> Optional[protocol.TraceRecord]:
    if rule.reply_event is None or len(match) < 4:
        return None
    sent, _received, _reply_sent, reply_received = match
    return next((
        event for event in events.get(rule.reply_event, ())
        if event.process_id == sent.process_id
        and event.process_sequence > reply_received.process_sequence
        and event.cause_event_id == reply_received.event_id
    ), None)


def _server_anchor(rule, match, events, by_id) -> Optional[protocol.TraceRecord]:
    if rule.server_event is None or len(match) < 4:
        return None
    _sent, received, reply_sent, _reply_received = match
    return next((
        event for event in events.get(rule.server_event, ())
        if event.process_id == received.process_id
        and received.process_sequence < event.process_sequence < reply_sent.process_sequence
        and _explicitly_follows(received.event_id, event, by_id)
        and _explicitly_follows(event.event_id, reply_sent, by_id)
    ), None)


def _causal_graph(
    records: Sequence[protocol.TraceRecord],
) -> Mapping[str, Tuple[str, ...]]:
    by_id = {record.event_id: record for record in records}
    outgoing = {event_id: set() for event_id in by_id}
    by_process = {}  # type: Dict[str, list[protocol.TraceRecord]]
    for record in records:
        by_process.setdefault(record.process_id, []).append(record)
        if record.cause_event_id in by_id:
            outgoing[record.cause_event_id].add(record.event_id)  # type: ignore[index]
    for process_records in by_process.values():
        process_records.sort(key=lambda record: (record.process_sequence, record.event_id))
        for before, after in zip(process_records, process_records[1:]):
            if before.process_sequence < after.process_sequence:
                outgoing[before.event_id].add(after.event_id)
    return {key: tuple(sorted(values)) for key, values in outgoing.items()}


def _reachable(
    start: str, target: str, adjacency: Mapping[str, Tuple[str, ...]]
) -> bool:
    if start == target:
        return False
    pending = [start]
    seen = {start}
    while pending:
        current = pending.pop()
        for successor in adjacency.get(current, ()):
            if successor == target:
                return True
            if successor not in seen:
                seen.add(successor)
                pending.append(successor)
    return False


def _point_record(
    point: str,
    events: Mapping[str, Tuple[protocol.TraceRecord, ...]],
    rpcs: Mapping[str, Tuple[protocol.TraceRecord, ...]],
) -> Optional[protocol.TraceRecord]:
    event = events.get(point, ())
    if event:
        return event[0]
    if "." not in point:
        rpc = rpcs.get(point, ())
        return rpc[0] if rpc else None
    key, suffix = point.rsplit(".", 1)
    positions = {
        "request_sent": 0,
        "request_received": 1,
        "reply_sent": 2,
        "reply_received": 3,
    }
    rpc = rpcs.get(key, ())
    index = positions.get(suffix)
    if index is None or len(rpc) <= index:
        return None
    return rpc[index]


def _forbidden_matches(
    record: protocol.TraceRecord,
    rule: ForbiddenRule,
    bindings: Mapping[str, str],
) -> bool:
    if record.component != rule.component:
        return False
    if rule.event is not None and record.event != rule.event:
        return False
    if rule.event_prefixes and not record.event.startswith(rule.event_prefixes):
        return False
    return _fields_match(
        dict(record.fields), rule.fields, dict(bindings), allow_capture=False
    )


def _render_match(match: TraceContractMatch) -> str:
    contract = match.contract
    participants = dict(contract.participants)
    event_rules = {rule.key: rule for rule in contract.events}
    rpc_rules = {rule.key: rule for rule in contract.rpcs}
    lines = ["{}: {}".format(contract.name, contract.title)]
    for index, key in enumerate(contract.render_order, 1):
        if key in event_rules:
            rule = event_rules[key]
            record = match.event_matches[key][0]
            fields = dict(record.fields)
            expected = dict(rule.fields)
            details = []
            for field in rule.show_fields:
                pattern = expected.get(field)
                actual = fields.get(field, "")
                details.append(
                    "{}={}".format(
                        field, _stable_field_value(field, pattern, actual)
                    )
                )
            suffix = " [{}]".format(", ".join(details)) if details else ""
            lines.append(
                "{:02d}. {} : {}{}".format(
                    index,
                    participants.get(rule.component, rule.component),
                    rule.event,
                    suffix,
                )
            )
            continue
        rpc_key, separator, endpoint = key.rpartition(".")
        if key in rpc_rules:
            rpc_key, endpoint = key, ""
        rpc = rpc_rules[rpc_key]
        details = []
        is_reply = endpoint in ("reply_sent", "reply_received")
        if endpoint:
            details.append(endpoint)
        else:
            # A bare RPC is positioned at request initiation, never at its
            # completion. Explicit endpoints expose nested calls in the
            # teaching success path without pretending they happen after Push.
            details.append("request; round trip verified" if rpc.round_trip else "request edge")
        if is_reply and rpc.reply_ok is not None:
            details.append("transport_ok={}".format(rpc.reply_ok))
        if endpoint == "reply_received" and rpc.reply_event is not None:
            rule = event_rules[rpc.reply_event]
            record = match.event_matches[rule.key][0]
            fields, expected = dict(record.fields), dict(rule.fields)
            details.extend(
                "{}={}".format(field, _stable_field_value(field, expected.get(field), fields.get(field, "")))
                for field in rule.show_fields
            )
        suffix = " [{}]".format(", ".join(details)) if details else ""
        source, target = (rpc.server, rpc.client) if is_reply else (rpc.client, rpc.server)
        lines.append(
            "{:02d}. {} -> {} : {}{}".format(
                index,
                participants.get(source, source),
                participants.get(target, target),
                rpc.handler,
                suffix,
            )
        )
    return "\n".join(lines)


def _stable_field_value(
    field: str, pattern: Optional[str], actual: str
) -> str:
    if pattern is not None and pattern.startswith("$"):
        symbolic = pattern[1:]
        if symbolic.endswith("_id"):
            symbolic = symbolic[:-3]
        return "<{}>".format(symbolic)
    if field.endswith("_id") or field.endswith("_ids"):
        return "<{}>".format(field.rstrip("s")[:-3])
    return pattern if pattern is not None else actual


def _event_rule(value: Mapping[str, object]) -> EventRule:
    fields = _string_mapping(value.get("fields", {}), "event fields")
    minimum = _count(value.get("min_count", 1), "min_count")
    maximum_value = value.get("max_count")
    maximum = (
        None
        if maximum_value is None
        else _count(maximum_value, "max_count")
    )
    if maximum is not None and maximum < minimum:
        raise TraceContractError("max_count cannot be smaller than min_count")
    show_fields = _text_tuple(value.get("show_fields", ()), "show_fields")
    if not set(show_fields).issubset(dict(fields)):
        raise TraceContractError("show_fields must also be constrained fields")
    return EventRule(
        key=_required_text(value, "key"),
        component=_required_text(value, "component"),
        event=_required_text(value, "event"),
        fields=fields,
        min_count=minimum,
        max_count=maximum,
        show_fields=show_fields,
    )


def _rpc_rule(value: Mapping[str, object]) -> RPCRule:
    round_trip = value.get("round_trip", False)
    if not isinstance(round_trip, bool):
        raise TraceContractError("RPC round_trip must be a boolean")
    reply_ok_value = value.get("reply_ok")
    reply_ok = (
        None
        if reply_ok_value is None
        else _nonempty_text(reply_ok_value, "RPC reply_ok")
    )
    if reply_ok is not None and not round_trip:
        raise TraceContractError("RPC reply_ok requires round_trip=true")
    return RPCRule(
        key=_required_text(value, "key"),
        client=_required_text(value, "client"),
        server=_required_text(value, "server"),
        handler=_required_text(value, "handler"),
        client_component=_optional_text(
            value.get("client_component"), "client_component"
        ),
        server_component=_optional_text(
            value.get("server_component"), "server_component"
        ),
        round_trip=round_trip,
        reply_ok=reply_ok,
        after_event=_optional_text(value.get("after_event"), "after_event"),
        before_event=_optional_text(
            value.get("before_event"), "before_event"
        ),
        reply_event=_optional_text(value.get("reply_event"), "reply_event"),
        server_event=_optional_text(value.get("server_event"), "server_event"),
    )


def _order_rule(value: Mapping[str, object]) -> OrderRule:
    return OrderRule(
        before=_required_text(value, "before"),
        after=_required_text(value, "after"),
    )


def _forbidden_rule(value: Mapping[str, object]) -> ForbiddenRule:
    event_value = value.get("event")
    event = (
        None
        if event_value is None
        else _nonempty_text(event_value, "forbidden event")
    )
    prefixes = _text_tuple(
        value.get("event_prefixes", ()), "event_prefixes"
    )
    if event is None and not prefixes:
        raise TraceContractError(
            "forbidden rule requires event or event_prefixes"
        )
    return ForbiddenRule(
        component=_required_text(value, "component"),
        event=event,
        event_prefixes=prefixes,
        fields=_string_mapping(value.get("fields", {}), "forbidden fields"),
    )


def _required_text(value: Mapping[str, object], key: str) -> str:
    return _nonempty_text(value.get(key), key)


def _required_mapping(
    value: Mapping[str, object], key: str
) -> Mapping[str, object]:
    candidate = value.get(key)
    if not isinstance(candidate, dict) or not candidate:
        raise TraceContractError("{} must be a non-empty object".format(key))
    return candidate


def _mapping_sequence(
    value: object, label: str
) -> Tuple[Mapping[str, object], ...]:
    if not isinstance(value, (tuple, list)):
        raise TraceContractError("{} must be an array".format(label))
    if not all(isinstance(item, dict) for item in value):
        raise TraceContractError("{} entries must be objects".format(label))
    return tuple(value)  # type: ignore[return-value]


def _string_mapping(
    value: object, label: str
) -> Tuple[Tuple[str, str], ...]:
    if not isinstance(value, dict):
        raise TraceContractError("{} must be an object".format(label))
    result = []
    for key, item in value.items():
        key_text = _nonempty_text(key, "{} key".format(label))
        item_text = _nonempty_text(item, "{} value".format(label))
        if item_text.startswith("$") and len(item_text) == 1:
            raise TraceContractError("trace binding name cannot be empty")
        result.append((key_text, item_text))
    return tuple(sorted(result))


def _text_tuple(value: object, label: str) -> Tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        raise TraceContractError("{} must be an array".format(label))
    return tuple(_nonempty_text(item, label) for item in value)


def _nonempty_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise TraceContractError("{} must be a non-empty string".format(label))
    return value


def _optional_text(value: object, label: str) -> Optional[str]:
    if value is None:
        return None
    return _nonempty_text(value, label)


def _count(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TraceContractError("{} must be a non-negative integer".format(label))
    return value


def _event_rule_name(rule: EventRule) -> str:
    return "{} ({})".format(rule.key, _event_name(rule.component, rule.event))


def _event_name(component: str, event: str) -> str:
    return "{}.{}".format(component, event)


def _record_key(record: protocol.TraceRecord) -> Tuple[str, int, str]:
    return record.process_id, record.process_sequence, record.event_id


__all__ = [
    "APPLICATION_ERROR_TRACE_CONTRACT",
    "BUILTIN_TRACE_CONTRACTS",
    "SUCCESS_TRACE_CONTRACT",
    "TRACE_CONTRACT_SCHEMA",
    "EventRule",
    "ForbiddenRule",
    "OrderRule",
    "RPCRule",
    "TraceContract",
    "TraceContractError",
    "TraceContractMatch",
    "TraceContractMismatch",
    "load_trace_contract",
    "render_trace_sequence",
]
