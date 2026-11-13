"""Tests for daily contract locks and monotonic signal publication."""

import unittest
from unittest.mock import patch

import pandas as pd

import publisher
import strategy
from test_paper_state import Collection, contract


def document(candle="2026-11-13 10:00:00"):
    return dict(_id="silver:2026-11-13:morning", strategy_version=strategy.VERSION,
                security_id="101", expiry="2026-11-30", data_asof=candle)


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.collection = Collection()

    def test_contract_lock_persists_across_restarts(self):
        first = publisher.lock_contract(self.collection, "2026-11-13", contract())
        second = publisher.lock_contract(self.collection, "2026-11-13",
                                         dict(contract(), security_id="102"))
        self.assertEqual(first, second)
        self.assertEqual(second["security_id"], "101")

    def test_publish_is_monotonic_and_contract_is_immutable(self):
        self.assertTrue(publisher.publish(self.collection, document()))
        self.assertFalse(publisher.publish(self.collection, document()))
        self.assertFalse(publisher.publish(self.collection, document("2026-11-13 09:59:00")))
        newer = document("2026-11-13 10:01:00")
        self.assertTrue(publisher.publish(self.collection, newer))
        self.assertEqual(self.collection.find_one({"_id": newer["_id"]}), newer)
        with self.assertRaises(ValueError):
            publisher.publish(self.collection, dict(newer, security_id="102"))

    def test_position_contract_and_closed_session_are_rejected(self):
        bars = pd.DataFrame()
        status = dict(morning=True, evening=False)
        with self.assertRaises(ValueError):
            publisher.prepare_signal(bars, "2026-11-13", "2026-11-13 17:01",
                                     "evening", status, contract(), None)
        with self.assertRaises(ValueError):
            publisher.prepare_signal(bars, "2026-11-13", "2026-11-13 10:01",
                                     "morning", status, contract(),
                                     dict(security_id="102", expiry="2026-11-30"))

    def test_missing_candles_are_published_without_blocking(self):
        with patch("publisher.signals.build_signal", return_value={"data_asof": "2026-11-13 10:00"}):
            with patch("publisher.signals.session_gaps", return_value=["2026-11-13 09:10:00"]):
                doc = publisher.prepare_signal(pd.DataFrame(), "2026-11-13", "2026-11-13 10:01",
                                               "morning", dict(morning=True, evening=True),
                                               contract(), None)
        self.assertEqual(doc["missing_minutes"], ["2026-11-13 09:10:00"])
        self.assertTrue(publisher.publish(self.collection, doc))


if __name__ == "__main__":
    unittest.main()
