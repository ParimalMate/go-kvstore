"""Seeded fault schedule for the chaos harness. Pure logic: no I/O, no clocks.

A seed fixes the whole schedule, including every recovery, so the same seed
always injects the same faults at the same offsets. Real-process timing can
still change how the cluster reacts, so a seed replays the fault schedule and
not the exact interleaving.
"""
import hashlib
import random
from dataclasses import dataclass

NODES = ("n1", "n2", "n3")
MAX_NODES_FAULTED = 2

# (name, weight): relative frequency of each injected fault.
ACTIONS = (("kill", 20), ("pause", 20), ("cut", 25), ("oneway", 15),
           ("bounce", 10), ("slow", 10))

# Recovery events paired with each fault.
RECOVERY = {"kill": "revive", "pause": "resume", "cut": "uncut",
            "oneway": "unoneway", "slow": "unslow"}

BOUNCE_BUSY = 1.0  # a graceful restart makes its node unavailable for about this long


@dataclass(frozen=True)
class Event:
    at: float
    action: str
    args: tuple = ()

    def line(self):
        return f"{self.at:.3f} {self.action} {' '.join(str(a) for a in self.args)}".rstrip()


def _pick_action(rng):
    total = sum(weight for _, weight in ACTIONS)
    point = rng.uniform(0, total)
    for name, weight in ACTIONS:
        point -= weight
        if point <= 0:
            return name
    return ACTIONS[-1][0]


def build_schedule(seed, duration, nodes=NODES):
    """Return events sorted by time. Recoveries that would land after the
    duration are dropped: the harness heals everything when chaos ends."""
    rng = random.Random(f"{seed}:schedule")
    events = []
    node_free_at = {n: 0.0 for n in nodes}
    link_free_at = {}
    t = 0.0
    while True:
        t += rng.uniform(0.5, 2.5)
        if t >= duration:
            break
        # Draw every random value first so skipped faults do not shift the stream.
        action = _pick_action(rng)
        target = rng.choice(nodes)
        a, b = rng.sample(nodes, 2)
        outage = {"kill": rng.uniform(0.5, 4.0), "pause": rng.uniform(0.5, 3.0)}
        link_time = rng.uniform(1.0, 5.0)
        delay_ms = rng.randrange(100, 801, 50)

        faulted = sum(1 for free in node_free_at.values() if free > t)
        if action in ("kill", "pause", "bounce"):
            if node_free_at[target] > t or faulted >= MAX_NODES_FAULTED:
                continue
            length = BOUNCE_BUSY if action == "bounce" else round(outage[action], 3)
            node_free_at[target] = t + length
            if action == "bounce":
                events.append(Event(round(t, 3), "bounce", (target,)))
            else:
                events.append(Event(round(t, 3), action, (target,)))
                if t + length < duration:
                    events.append(Event(round(t + length, 3), RECOVERY[action], (target,)))
            continue
        length = round(link_time, 3)
        if action == "cut":
            busy = link_free_at.get((a, b), 0.0) > t or link_free_at.get((b, a), 0.0) > t
            if busy:
                continue
            link_free_at[(a, b)] = link_free_at[(b, a)] = t + length
            events.append(Event(round(t, 3), "cut", (a, b)))
        else:  # oneway or slow act on the directed link a -> b
            if link_free_at.get((a, b), 0.0) > t:
                continue
            link_free_at[(a, b)] = t + length
            args = (a, b, delay_ms) if action == "slow" else (a, b)
            events.append(Event(round(t, 3), action, args))
        if t + length < duration:
            recovery_args = (a, b)
            events.append(Event(round(t + length, 3), RECOVERY[action], recovery_args))
    events.sort(key=lambda e: e.at)
    return events


def schedule_hash(events):
    digest = hashlib.sha256("\n".join(e.line() for e in events).encode()).hexdigest()
    return digest[:16]
