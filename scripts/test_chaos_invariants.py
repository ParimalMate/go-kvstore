import unittest
from chaos_invariants import Attempt, canonical, check_final, check_put, check_read, floor


def attempt(seq=1, outcome="acked", end=2, value=None, status=None):
    if status is None:
        status = {"acked": 200, "rejected": 503, "indeterminate": None}[outcome]
    return Attempt("k", seq, value or f"k|{seq}|token|payload", 0, end, outcome, status)


def states(*values):
    versions = [{"value": v, "vc": {"n1": i + 1}} for i, v in enumerate(values)]
    return {node: {"k": versions} for node in ["n1", "n2", "n3"]}


class InvariantTests(unittest.TestCase):
    def test_ack_visible(self):
        a = attempt()
        self.assertEqual(check_read([a], "k", 3, 200, [a.value]), [])

    def test_old_after_ack(self):
        old, new = attempt(), attempt(2)
        self.assertTrue(check_read([old, new], "k", 3, 200, [old.value]))

    def test_404_after_ack(self):
        self.assertTrue(check_read([attempt()], "k", 3, 404))

    def test_old_indeterminate_allowed(self):
        old, new = attempt(outcome="indeterminate"), attempt(2)
        self.assertEqual(check_read([old, new], "k", 3, 200, [old.value]), [])
        self.assertEqual(check_final([old, new], states(old.value)), [])

    def test_old_rejected_not_allowed(self):
        old, new = attempt(outcome="rejected"), attempt(2)
        self.assertTrue(check_read([old, new], "k", 3, 200, [old.value]))
        self.assertTrue(check_final([old, new], states(old.value)))

    def test_invented_value(self):
        self.assertTrue(check_read([], "k", 3, 200, ["unknown"]))

    def test_mangled_payload(self):
        a = attempt()
        self.assertTrue(check_read([a], "k", 3, 200, [a.value + "changed"]))

    def test_read_before_ack(self):
        self.assertEqual(check_read([attempt()], "k", 1, 404), [])

    def test_any_sibling_above_floor(self):
        old, new = attempt(), attempt(2)
        self.assertEqual(check_read([old, new], "k", 3, 300, [old.value, new.value]), [])

    def test_nodes_disagree(self):
        a = attempt()
        snapshot = states(a.value)
        snapshot["n3"] = {}
        self.assertTrue(any("disagree" in e for e in check_final([a], snapshot)))

    def test_identical_but_lost_write(self):
        old, new = attempt(), attempt(2)
        self.assertTrue(any("lost" in e for e in check_final([old, new], states(old.value))))

    def test_empty_history(self):
        self.assertEqual(check_final([], {"n1": {}, "n2": {}, "n3": {}}), [])
        self.assertEqual(check_read([], "k", 0, 404), [])

    def test_put_500(self):
        self.assertTrue(check_put(attempt(outcome="rejected", status=500)))

    def test_get_502_and_500(self):
        for status in [500, 502]:
            self.assertTrue(check_read([], "k", 0, status))

    def test_unavailable_not_violation(self):
        for status in [None, 503]:
            self.assertEqual(check_read([attempt()], "k", 3, status), [])

    def test_every_value_checked(self):
        a = attempt()
        self.assertTrue(check_read([a], "k", 3, 300, [a.value, "invented"]))
        self.assertTrue(check_final([a], states(a.value, "invented")))

    def test_floor_uses_ack_time(self):
        self.assertEqual(floor([attempt(1, end=2), attempt(2, end=4)], "k", 3), 1)
        self.assertEqual(floor([attempt(end=2)], "k", 2), 0)

    def test_informational_timestamp_and_clock_order(self):
        a = {"value": "v", "vc": {"n1": 1}, "ts": 1}
        b = {"value": "v", "vc": {"n2": 0, "n1": 1}, "ts": 999}
        self.assertEqual(canonical([a]), canonical([b]))

    def test_future_attempt_cannot_explain_value(self):
        a = Attempt("k", 1, "k|1|token|v", 10, 11, "acked", 200)
        self.assertTrue(check_read([a], "k", 1, 200, [a.value], ended=2))

    def test_empty_success_is_invalid(self):
        self.assertTrue(check_read([], "k", 0, 200, []))

    def test_invalid_attempt_metadata(self):
        with self.assertRaises(ValueError):
            Attempt("k", 1, "k|1|t|v", 2, 1, "acked", 200)
        with self.assertRaises(ValueError):
            Attempt("k", 1, "k|1|t|v", 0, 1, "acked", 503)


if __name__ == "__main__":
    unittest.main()
