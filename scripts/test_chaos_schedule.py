import unittest

import chaos_schedule as cs

FAULTS = {"kill", "pause", "bounce"}
LINK_FAULTS = {"cut", "oneway", "slow"}


def faulted_nodes_over_time(events):
    """Replay a schedule and return the most nodes down or paused at once."""
    down, worst = set(), 0
    busy_until = {}
    for e in events:
        busy_until = {n: t for n, t in busy_until.items() if t > e.at}
        if e.action in ("kill", "pause"):
            down.add(e.args[0])
        elif e.action in ("revive", "resume"):
            down.discard(e.args[0])
        elif e.action == "bounce":
            busy_until[e.args[0]] = e.at + cs.BOUNCE_BUSY
        worst = max(worst, len(down | set(busy_until)))
    return worst


class ScheduleTests(unittest.TestCase):
    def test_same_seed_same_schedule(self):
        a = cs.build_schedule(7, 60)
        b = cs.build_schedule(7, 60)
        self.assertEqual(a, b)
        self.assertEqual(cs.schedule_hash(a), cs.schedule_hash(b))

    def test_different_seeds_differ(self):
        hashes = {cs.schedule_hash(cs.build_schedule(s, 60)) for s in range(1, 21)}
        self.assertEqual(len(hashes), 20)

    def test_golden_hash_locks_the_format(self):
        # Changing the generator changes every published seed; do it on purpose.
        self.assertEqual(cs.schedule_hash(cs.build_schedule(1, 60)), "c4b96251baf1d646")

    def test_never_more_than_two_nodes_faulted(self):
        for seed in range(1, 200):
            events = cs.build_schedule(seed, 120)
            self.assertLessEqual(faulted_nodes_over_time(events), cs.MAX_NODES_FAULTED, f"seed {seed}")

    def test_every_recovery_follows_its_fault_and_pairs_up(self):
        pairs = {"revive": "kill", "resume": "pause", "uncut": "cut",
                 "unoneway": "oneway", "unslow": "slow"}
        for seed in range(1, 100):
            open_faults = {}
            for e in cs.build_schedule(seed, 90):
                if e.action in ("kill", "pause"):
                    self.assertNotIn(e.args, open_faults, f"seed {seed}: overlapping fault on {e.args}")
                    open_faults[e.args] = e.action
                elif e.action in ("cut", "oneway", "slow"):
                    key = ("link",) + e.args[:2]
                    self.assertNotIn(key, open_faults, f"seed {seed}: overlapping link fault")
                    open_faults[key] = e.action
                elif e.action in pairs:
                    key = e.args if e.action in ("revive", "resume") else ("link",) + e.args[:2]
                    self.assertEqual(open_faults.pop(key, None), pairs[e.action], f"seed {seed}: orphan {e.line()}")

    def test_events_stay_inside_the_duration_and_are_sorted(self):
        for seed in range(1, 50):
            events = cs.build_schedule(seed, 30)
            self.assertTrue(all(0 < e.at < 30 for e in events))
            self.assertEqual([e.at for e in events], sorted(e.at for e in events))

    def test_a_cut_never_overlaps_another_fault_on_the_same_link(self):
        for seed in range(1, 100):
            busy = {}
            for e in cs.build_schedule(seed, 90):
                if e.action in ("cut", "oneway", "slow"):
                    links = [(e.args[0], e.args[1])] + ([(e.args[1], e.args[0])] if e.action == "cut" else [])
                    for link in links:
                        self.assertNotIn(link, busy, f"seed {seed}: {e.line()}")
                        busy[link] = e.action
                elif e.action in ("uncut", "unoneway", "unslow"):
                    for link in [(e.args[0], e.args[1])] + ([(e.args[1], e.args[0])] if e.action == "uncut" else []):
                        busy.pop(link, None)

    def test_all_fault_kinds_occur_over_many_seeds(self):
        seen = set()
        for seed in range(1, 40):
            seen.update(e.action for e in cs.build_schedule(seed, 60))
        self.assertTrue({"kill", "pause", "cut", "oneway", "bounce", "slow"} <= seen)

    def test_pause_never_exceeds_three_seconds(self):
        for seed in range(1, 100):
            started = {}
            for e in cs.build_schedule(seed, 120):
                if e.action == "pause":
                    started[e.args[0]] = e.at
                elif e.action == "resume":
                    self.assertLessEqual(e.at - started.pop(e.args[0]), 3.0 + 1e-6)

    def test_short_duration_still_valid(self):
        self.assertEqual(cs.build_schedule(1, 0.4), [])


if __name__ == "__main__":
    unittest.main()
