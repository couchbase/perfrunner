import math
from unittest import TestCase

from perfrunner.helpers.misc import parse_duration_to_secs


class MiscTest(TestCase):
    def test_parse_duration_to_secs(self):
        self.assertAlmostEqual(parse_duration_to_secs("1.5s"), 1.5)
        self.assertAlmostEqual(parse_duration_to_secs("1m30s"), 90.0)
        self.assertAlmostEqual(parse_duration_to_secs("1m40.0s"), 100.0)
        self.assertAlmostEqual(parse_duration_to_secs("2h3m4.005s"), 7384.005)
        self.assertAlmostEqual(parse_duration_to_secs("500ms"), 0.5)
        self.assertAlmostEqual(parse_duration_to_secs("500µs"), 5e-4)
        self.assertAlmostEqual(parse_duration_to_secs("500μs"), 5e-4)
        self.assertAlmostEqual(parse_duration_to_secs("500us"), 5e-4)
        self.assertAlmostEqual(parse_duration_to_secs("500ns"), 5e-7)
        # A bare number is read as seconds, and surrounding whitespace is tolerated
        self.assertAlmostEqual(parse_duration_to_secs("3"), 3.0)
        self.assertAlmostEqual(parse_duration_to_secs(" 1m 30s "), 90.0)
        # Anything not fully consumed is NaN, never silently read as seconds
        for bad in ("", "   ", "garbage", "1.5d", "1.5s trailing", "leading 1.5s", "-1.5s"):
            self.assertTrue(math.isnan(parse_duration_to_secs(bad)), f"expected NaN for {bad!r}")
