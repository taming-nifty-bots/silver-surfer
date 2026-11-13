"""Tests for Silver signal freshness, independent charts and session gaps."""

import unittest
from datetime import datetime

import pandas as pd
from unittest.mock import patch

import signals


class SignalTests(unittest.TestCase):
    def test_calendar_aware_gap_detection(self):
        bars = pd.DataFrame({"datetime": pd.to_datetime(["2026-11-24 17:00", "2026-11-24 17:02"])})
        gaps = signals.session_gaps(bars, "2026-11-24", "2026-11-24 17:02", False, True)
        self.assertEqual(gaps, ["2026-11-24 17:01:00"])
        self.assertEqual(signals.session_gaps(bars, "2026-11-24", "2026-11-24 17:02",
                                              False, False), [])

    def test_build_uses_fresh_candle_and_independent_evening_box(self):
        bars = pd.DataFrame([dict(datetime=pd.Timestamp("2026-11-13 17:00"),
                                  open=100000, high=100001, low=99999,
                                  close=100000, volume=1)])
        brick = dict(datetime=pd.Timestamp("2026-11-13 17:00"),
                     brick_time=pd.Timestamp("2026-11-13 17:00"), close=100001,
                     upper40=100000, lower40=90000, upper10=100000, lower10=90000,
                     upper7=100000, lower7=90000)
        with patch.object(signals.strategy, "chart_history", return_value=bars), \
                patch.object(signals.strategy, "chart", return_value=pd.DataFrame([brick])) as chart:
            result = signals.build_signal(bars, "2026-11-13", datetime(2026, 11, 13, 17, 1),
                                          "evening")
            self.assertEqual(result["side"], "BUY")
            self.assertEqual(chart.call_args.args[1], 0.15)
            with self.assertRaises(ValueError):
                signals.build_signal(bars, "2026-11-13", datetime(2026, 11, 13, 17, 2),
                                     "evening")

    def test_gap_detection_covers_after_23_until_last_managed_candle(self):
        bars = pd.DataFrame({"datetime": pd.date_range("2026-11-13 17:00", "2026-11-13 23:09",
                                                       freq="min").delete(365)})
        gaps = signals.session_gaps(bars, "2026-11-13", "2026-11-13 23:09", False, True)
        self.assertEqual(gaps, ["2026-11-13 23:05:00"])


if __name__ == "__main__":
    unittest.main()
