"""Pure fault-injection checks. Times are monotonic; no network or file I/O.

Sequence numbers are workload order, not vector clocks. The checker assumes
one sequential writer per key. Only indeterminate attempts excuse an older
survivor: they may apply after their client has stopped waiting.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Attempt:
    key: str
    seq: int
    value: str
    start: float
    end: float
    outcome: str  # acked, rejected, indeterminate
    status: int | None = None

    def __post_init__(self):
        if self.outcome not in {"acked", "rejected", "indeterminate"}:
            raise ValueError("invalid attempt outcome")
        if self.end < self.start or self.seq < 1:
            raise ValueError("invalid attempt times or sequence")
        if not self.value.startswith(f"{self.key}|{self.seq}|"):
            raise ValueError("value does not match key and sequence")
        if self.outcome == "acked" and self.status != 200:
            raise ValueError("acknowledged attempt requires HTTP 200")
        if self.outcome == "rejected" and (self.status is None or self.status == 200):
            raise ValueError("rejected attempt requires a non-200 status")
        if self.outcome == "indeterminate" and self.status is not None:
            raise ValueError("indeterminate attempt must not have a response status")


def floor(attempts, key, at):
    """Highest sequence acknowledged strictly before the read began."""
    return max((a.seq for a in attempts if a.key == key and
                a.outcome == "acked" and a.end < at), default=0)


def check_put(attempt):
    return ["PUT returned 500"] if attempt.status == 500 else []


def _check_values(attempts, key, values, minimum, at=float("inf")):
    # Match the entire value, never just its sequence prefix. Ignore attempts
    # that had not even begun at the observation time.
    known = {a.value: a for a in attempts if a.key == key and a.start <= at}
    errors = []
    acceptable = minimum == 0
    for value in values:
        attempt = known.get(value)
        if attempt is None:
            errors.append(f"invented value for {key}: {value!r}")
        elif attempt.seq >= minimum or attempt.outcome == "indeterminate":
            acceptable = True
    if not acceptable:
        errors.append(f"lost acknowledged write for {key}: required seq >= {minimum}")
    return errors


def check_read(attempts, key, started, status, values=(), *, ended=None, minimum=None):
    """Check a decoded response; transport errors use status=None.

    The caller may pass the floor captured before sending. History supplied
    afterward must include attempts whose values were observed in the response.
    """
    errors = []
    if status in {500, 502}:
        return [f"GET returned {status}"]
    required = floor(attempts, key, started) if minimum is None else minimum
    if status == 404:
        return [f"404 after acknowledged write for {key}"] if required else []
    if status not in {200, 300}:
        return errors  # 503 and transport failure are unavailability, not stale reads.
    if not values:
        errors.append("successful read contained no versions")
    errors.extend(_check_values(attempts, key, values, required,
                                float("inf") if ended is None else ended))
    return errors


def canonical(siblings):
    """Normalize order and zero clock entries; timestamps are informational."""
    return sorted((v["value"], tuple(sorted((node, counter)
                  for node, counter in v["vc"].items() if counter != 0)))
                  for v in siblings)


def check_final(attempts, states):
    """states maps node IDs to {key: [{value, vc}, ...]} snapshots."""
    errors = []
    keys = {a.key for a in attempts}
    for state in states.values():
        keys.update(state)
    if not states:
        return ["no node snapshots"] if attempts else []
    for key in sorted(keys):
        views = [canonical(state.get(key, [])) for state in states.values()]
        if any(view != views[0] for view in views[1:]):
            errors.append(f"nodes disagree for {key}")
        required = floor(attempts, key, float("inf"))
        for node, state in states.items():
            errors.extend(f"{node}: {e}" for e in _check_values(
                attempts, key, [v["value"] for v in state.get(key, [])], required))
    return errors
