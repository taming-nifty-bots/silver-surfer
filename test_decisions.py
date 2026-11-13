"""Offline tests for fresh entries, activation and forced exits."""

import unittest
from copy import deepcopy
from unittest.mock import patch

import pandas as pd

import decisions


def signal():
    return dict(session="morning", security_id="101", data_asof="2026-11-13 10:00:00",
                brick_time="2026-11-13 10:00:00", candle_volume=1,
                candle_close=101000.0, close=100000.0, side="BUY",
                upper7=103000.0, lower7=100001.0,
                upper10=103000.0, lower10=99000.0)


def trade():
    return dict(session="morning", security_id="101", entry_timestamp="2026-11-13 09:59:03",
                side="BUY", entry_price=100000.0, profit_active=False)


class DecisionTests(unittest.TestCase):
    def test_fresh_entry_and_stale_signal(self):
        self.assertEqual(decisions.entry_decision(signal(), "2026-11-13 10:01:10", True), "BUY")
        self.assertIsNone(decisions.entry_decision(signal(), "2026-11-13 10:01:10", False))
        with self.assertRaises(ValueError):
            decisions.entry_decision(signal(), "2026-11-13 10:02:00", True)

    def test_no_opening_brick_and_session_boundary(self):
        doc = signal()
        doc.update(data_asof="2026-11-13 09:00", brick_time="2026-11-13 09:00")
        self.assertIsNone(decisions.entry_decision(doc, "2026-11-13 09:01", True))
        doc.update(data_asof="2026-11-13 16:59", brick_time="2026-11-13 16:59")
        self.assertIsNone(decisions.entry_decision(doc, "2026-11-13 17:00", True))
        doc["session"] = "evening"
        self.assertEqual(decisions.entry_decision(doc, "2026-11-13 17:00", True), "BUY")
        doc["session"] = "morning"
        doc.update(data_asof="2026-11-13 16:53", brick_time="2026-11-13 16:53")
        self.assertEqual(decisions.entry_decision(doc, "2026-11-13 16:54", True), "BUY")
        doc.update(data_asof="2026-11-13 16:54", brick_time="2026-11-13 16:54")
        self.assertIsNone(decisions.entry_decision(doc, "2026-11-13 16:55", True))

    def test_exit_activation_and_channel(self):
        result = decisions.exit_decision(trade(), signal(), "2026-11-13 10:01")
        self.assertEqual(result, dict(reason="channel7", profit_active=True))
        doc = signal()
        doc["candle_close"] = 100999
        self.assertEqual(decisions.exit_decision(trade(), doc, "2026-11-13 10:01"),
                         dict(reason=None, profit_active=False))
        doc["candle_close"] = 98000
        position = dict(trade(), side="SELL")
        doc["close"] = 104000
        self.assertEqual(decisions.exit_decision(position, doc, "2026-11-13 10:01"),
                         dict(reason="channel7", profit_active=True))

    def test_force_close_without_signal(self):
        self.assertEqual(decisions.exit_decision(trade(), None, "2026-11-13 16:55")["reason"],
                         "session cutoff")
        self.assertEqual(decisions.exit_decision(trade(), None, "2026-11-14 08:00")["reason"],
                         "session cutoff")
        with self.assertRaises(ValueError):
            decisions.exit_decision(trade(), None, "2026-11-13 16:54")

    def test_contract_mismatch_and_old_signal(self):
        doc = deepcopy(signal())
        doc["security_id"] = "102"
        with self.assertRaises(ValueError):
            decisions.exit_decision(trade(), doc, "2026-11-13 10:01")
        with self.assertRaises(ValueError):
            decisions.exit_decision(trade(), signal(), "2026-11-13 10:02")

    def test_recover_activation_from_missed_minute(self):
        candles = pd.DataFrame([
            dict(datetime="2026-11-13 09:58", close=103000),
            dict(datetime="2026-11-13 09:59", close=100000),
            dict(datetime="2026-11-13 10:00", close=101000),
            dict(datetime="2026-11-13 10:01", close=100000),
        ])
        self.assertTrue(decisions.recover_activation(trade(), candles, "2026-11-13 10:02"))
        candles.loc[2, "close"] = 100000
        self.assertFalse(decisions.recover_activation(trade(), candles, "2026-11-13 10:02"))
        self.assertTrue(decisions.recover_activation(dict(trade(), profit_active=True),
                                                     candles, "2026-11-13 10:02"))

    def test_recovery_excludes_unfinished_bar_and_rejects_duplicates(self):
        candles = pd.DataFrame([
            dict(datetime="2026-11-13 10:00", close=100000),
            dict(datetime="2026-11-13 10:01", close=101000),
        ])
        self.assertFalse(decisions.recover_activation(trade(), candles, "2026-11-13 10:01:30"))
        candles.loc[1, "datetime"] = "2026-11-13 10:00"
        with self.assertRaises(ValueError):
            decisions.recover_activation(trade(), candles, "2026-11-13 10:02")

    def test_short_activation_recovery(self):
        candles = pd.DataFrame([dict(datetime="2026-11-13 10:00", close=99000)])
        self.assertTrue(decisions.recover_activation(dict(trade(), side="SELL"),
                                                    candles, "2026-11-13 10:01"))

    def test_recover_missed_exit_in_chronological_order(self):
        position = dict(trade(), last_checked_asof="2026-11-13 09:59:00")
        candles = pd.DataFrame([dict(datetime="2026-11-13 10:01", close=99000),
                                dict(datetime="2026-11-13 10:00", close=101000)])
        first = dict(signal(), data_asof="2026-11-13 10:00:00", candle_close=101000,
                     close=100500, lower7=100000)
        second = dict(signal(), data_asof="2026-11-13 10:01:00", candle_close=99000,
                      close=99000, lower7=100000)
        with patch("decisions.signals.build_signal", side_effect=[first, second]):
            result = decisions.recover_missed_exit(position, candles, "2026-11-13 10:01")
        self.assertEqual(result, dict(reason="channel7", profit_active=True,
                                      data_asof="2026-11-13 10:01:00"))


if __name__ == "__main__":
    unittest.main()
