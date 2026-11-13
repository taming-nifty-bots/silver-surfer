"""Paper executor integration tests using in-memory Mongo stand-ins."""

import unittest
from datetime import datetime
from unittest.mock import patch

import pandas as pd

import executor
import paper_state
import strategy
from test_paper_state import Collection, contract, quote, signal


class ExecutorTests(unittest.TestCase):
    def setUp(self):
        self.states, self.trades, self.signals = Collection(), Collection(), Collection()
        self.alerts = []
        self.now = datetime(2026, 11, 13, 10, 1, 2)
        self.doc = dict(signal(), _id="silver:2026-11-13:morning",
                        strategy_version=strategy.VERSION, contract=contract(),
                        expiry=contract()["expiry"], candle_close=99000,
                        close=99000, lower7=98000, lower10=97000,
                        upper7=101000, upper10=102000)
        self.signals.insert_one(self.doc)

    def clock(self):
        return self.now

    def get_quote(self, security_id):
        self.assertEqual(security_id, "101")
        return quote(), self.now, self.now

    def get_candles(self, position):
        return pd.DataFrame([dict(datetime="2026-11-13 10:01", close=100000)])

    def cycle(self):
        executor.run_cycle(self.states, self.trades, self.signals, None,
                           self.get_quote, self.get_candles, self.clock, self.alerts.append)

    def test_paper_entry_activation_exit_and_archive(self):
        with patch("executor.market.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        self.assertEqual(state["position"]["entry_price"], 99000)
        self.now = datetime(2026, 11, 13, 10, 2, 2)
        self.doc.update(data_asof="2026-11-13 10:01:00", brick_time="2026-11-13 10:01:00",
                        candle_close=100000, close=97000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        self.cycle()
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        self.assertIsNone(state["position"])
        self.assertEqual(state["pending_trade"]["exit_reason"], "channel7")
        self.assertEqual(state["pending_trade"]["pnl"], -2000)
        with patch("executor.market.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.assertIsNone(self.states.find_one({"_id": paper_state.STATE_ID})["pending_trade"])
        self.assertEqual(len(self.trades.docs), 1)

    def test_cutoff_exit_without_calendar_or_signal(self):
        with patch("executor.market.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.signals.docs.clear()
        self.now = datetime(2026, 11, 13, 17, 0, 0)
        with patch("executor.market.session_status", side_effect=RuntimeError("calendar unavailable")):
            self.cycle()
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        self.assertIsNone(state["position"])
        self.assertEqual(state["pending_trade"]["exit_reason"], "session cutoff")

    def test_stale_signal_never_enters(self):
        self.now = datetime(2026, 11, 13, 10, 2, 0)
        with patch("executor.market.session_status", return_value=dict(morning=True, evening=True)):
            with self.assertRaises(ValueError):
                self.cycle()
        self.assertIsNone(self.states.find_one({"_id": paper_state.STATE_ID})["position"])

    def test_restart_warns_when_checks_and_candles_were_missed(self):
        with patch("executor.market.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 10, 4, 0)
        self.doc.update(data_asof="2026-11-13 10:03:00", brick_time="2026-11-13 10:03:00",
                        candle_close=99000, close=99000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch("executor.decisions.recover_missed_exit",
                   return_value=dict(reason=None, profit_active=False, data_asof="2026-11-13 10:03:00")):
            self.cycle()
        self.assertTrue(any("missed signal checks" in text for text in self.alerts))
        self.assertTrue(any("missing completed candles" in text for text in self.alerts))

    def test_recovered_exit_uses_current_quote(self):
        with patch("executor.market.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 10, 4, 0)
        self.doc.update(data_asof="2026-11-13 10:03:00", brick_time="2026-11-13 10:03:00")
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch("executor.decisions.recover_missed_exit",
                   return_value=dict(reason="channel7", profit_active=True,
                                     data_asof="2026-11-13 10:01:00")):
            self.cycle()
        trade = self.states.find_one({"_id": paper_state.STATE_ID})["pending_trade"]
        self.assertEqual(trade["exit_timestamp"], "2026-11-13 10:04:00")
        self.assertEqual(trade["exit_price"], 97000)
        self.assertTrue(any("Recovered channel7" in text for text in self.alerts))

    def test_failed_exit_quote_does_not_checkpoint_the_exit_signal(self):
        with patch("executor.market.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 10, 2, 2)
        self.doc.update(data_asof="2026-11-13 10:01:00", brick_time="2026-11-13 10:01:00",
                        candle_close=100000, close=97000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch.object(self, "get_quote", side_effect=ValueError("No quote")):
            with self.assertRaises(ValueError):
                self.cycle()
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        self.assertTrue(state["position"]["profit_active"])
        self.assertEqual(state["position"]["last_checked_asof"], "2026-11-13 10:00:00")
        self.assertEqual(state["position"]["exit_requested"], "channel7")
        self.signals.docs.clear()
        self.now = datetime(2026, 11, 13, 10, 4, 2)
        self.cycle()
        self.assertEqual(self.states.find_one({"_id": paper_state.STATE_ID})["pending_trade"]["exit_reason"],
                         "channel7")


if __name__ == "__main__":
    unittest.main()
