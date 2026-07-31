import unittest

from farm_runner import CYCLE_TIMEOUT_S, discover_cdp, failure_backoff, should_escalate


class RecoveryPolicyTests(unittest.TestCase):
    def test_backoff_is_short_and_bounded(self):
        self.assertEqual(failure_backoff(1, 60), 60)
        self.assertEqual(failure_backoff(2, 60), 120)
        self.assertEqual(failure_backoff(5, 60), 120)
        self.assertEqual(failure_backoff(99, 10, cap=30), 30)

    def test_backoff_respects_minimum(self):
        self.assertEqual(failure_backoff(0, 15), 15)
        self.assertEqual(failure_backoff(3, 15), 45)

    def test_escalation_threshold(self):
        self.assertFalse(should_escalate(4))
        self.assertTrue(should_escalate(5))
        self.assertTrue(should_escalate(5, threshold=3))

    def test_cycle_timeout_is_finite(self):
        self.assertEqual(CYCLE_TIMEOUT_S, 120.0)

    def test_non_farm_endpoint_is_rejected(self):
        import os
        old = os.environ.get("FARM_CDP")
        try:
            os.environ["FARM_CDP"] = "http://127.0.0.1:9223"
            self.assertEqual(discover_cdp(), (None, None, None))
        finally:
            if old is None:
                os.environ.pop("FARM_CDP", None)
            else:
                os.environ["FARM_CDP"] = old


if __name__ == "__main__":
    unittest.main()