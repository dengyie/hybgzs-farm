import unittest

from farm_runner import failure_backoff, should_escalate


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


if __name__ == "__main__":
    unittest.main()