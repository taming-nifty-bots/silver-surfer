"""Offline tests for paper state, concurrency and archive recovery."""

import unittest
from copy import deepcopy
from types import SimpleNamespace

from pymongo.errors import DuplicateKeyError
from tamingnifty.charges import silver_micro_estimated_order_charges, sum_charges

import paper_state


class Collection:
    def __init__(self):
        self.docs = {}

    def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate")
        self.docs[doc["_id"]] = deepcopy(doc)

    def find_one(self, query):
        for doc in self.docs.values():
            if self.matches(doc, query):
                return deepcopy(doc)
        return None

    def matches(self, doc, query):
        for key, value in query.items():
            current = doc
            for part in key.split("."):
                if not isinstance(current, dict) or part not in current:
                    return False
                current = current[part]
            if current != value:
                return False
        return True

    def update_one(self, query, changes):
        for doc in self.docs.values():
            if self.matches(doc, query):
                for key, value in changes.get("$set", {}).items():
                    parts = key.split(".")
                    target = doc
                    for part in parts[:-1]:
                        target = target[part]
                    target[parts[-1]] = deepcopy(value)
                for key, value in changes.get("$inc", {}).items():
                    doc[key] += value
                return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)

    def replace_one(self, query, replacement):
        for key, doc in self.docs.items():
            if self.matches(doc, query):
                self.docs[key] = deepcopy(replacement)
                return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)


def signal(session="morning", candle="2026-11-13 10:00:00", side="BUY"):
    return dict(session=session, security_id="101", data_asof=candle,
                brick_time=candle, candle_volume=1, side=side)


def quote():
    return dict(depth={"buy": [{"price": 97000, "quantity": 1}],
                       "sell": [{"price": 99000, "quantity": 1}]})


def contract():
    return dict(security_id="101", symbol="SILVERMIC", expiry="2026-11-30", lot_size=1)


class PaperStateTests(unittest.TestCase):
    def setUp(self):
        self.states, self.trades = Collection(), Collection()
        self.state = paper_state.create_state(self.states)

    def open(self, state=None, doc=None, start="2026-11-13 10:01:01",
             end="2026-11-13 10:01:02", quantity=1):
        return paper_state.open_position(self.states, state or self.state,
                                         doc or signal(), contract(), quantity, quote(), start, end)

    def test_competing_entries_and_restart_preserve_one_position(self):
        position = self.open()
        self.assertEqual(position["entry_price"], 99000)
        with self.assertRaises(RuntimeError):
            self.open()
        recovered = paper_state.create_state(self.states)
        self.assertEqual(recovered["position"], position)
        self.assertTrue(recovered["morning_used"])

    def test_close_archive_and_replay_after_crash(self):
        self.open()
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        trade = paper_state.close_position(self.states, state, "channel10", quote(),
                                           "2026-11-13 10:02:01", "2026-11-13 10:02:02")
        self.assertEqual(trade["exit_price"], 97000)
        self.assertEqual(trade["pnl"], -2000)
        self.assertAlmostEqual(trade["net_pnl"], trade["pnl"] - trade["charges"]["total"], places=2)
        pending = self.states.find_one({"_id": paper_state.STATE_ID})
        self.trades.insert_one(trade.copy())
        paper_state.archive_trade(self.states, self.trades, pending)
        self.assertIsNone(self.states.find_one({"_id": paper_state.STATE_ID})["pending_trade"])
        self.assertEqual(self.trades.find_one({"_id": trade["_id"]}), trade)
        with self.assertRaises(RuntimeError):
            paper_state.close_position(self.states, state, "channel10", quote(),
                                       "2026-11-13 10:02:01", "2026-11-13 10:02:02")

    def test_state_and_trades_share_collection(self):
        self.open(quantity=3)
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        trade = paper_state.close_position(self.states, state, "cutoff", quote(),
                                           "2026-11-13 16:55:00", "2026-11-13 16:55:01")
        pending = self.states.find_one({"_id": paper_state.STATE_ID})
        self.states.insert_one(trade.copy())
        paper_state.archive_trade(self.states, self.states, pending)
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        self.assertIsNone(state["pending_trade"])
        self.assertEqual(self.states.find_one({"_id": trade["_id"]}), trade)
        self.assertEqual(len(self.states.docs), 2)

    def test_conflicting_archived_record_is_not_cleared(self):
        self.open()
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        trade = paper_state.close_position(self.states, state, "cutoff", quote(),
                                           "2026-11-13 17:00:00", "2026-11-13 17:00:01")
        self.trades.insert_one(dict(trade, pnl=0))
        with self.assertRaises(RuntimeError):
            paper_state.archive_trade(self.states, self.trades,
                                      self.states.find_one({"_id": paper_state.STATE_ID}))
        self.assertIsNotNone(self.states.find_one({"_id": paper_state.STATE_ID})["pending_trade"])

    def test_one_trade_per_session_and_no_same_minute_handoff(self):
        self.open()
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        paper_state.close_position(self.states, state, "cutoff", quote(),
                                   "2026-11-13 17:00:00", "2026-11-13 17:00:01")
        pending = self.states.find_one({"_id": paper_state.STATE_ID})
        paper_state.archive_trade(self.states, self.trades, pending)
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        with self.assertRaises(ValueError):
            self.open(state, signal("evening", "2026-11-13 16:59:00"),
                      "2026-11-13 17:00:02", "2026-11-13 17:00:03")
        self.open(state, signal("evening", "2026-11-13 17:00:00", "SELL"),
                  "2026-11-13 17:01:01", "2026-11-13 17:01:02")

    def test_activation_cannot_be_reset_or_updated_by_stale_worker(self):
        self.open()
        old = self.states.find_one({"_id": paper_state.STATE_ID})
        new = paper_state.update_activation(self.states, old, True)
        self.assertTrue(new["position"]["profit_active"])
        with self.assertRaises(ValueError):
            paper_state.update_activation(self.states, new, False)
        with self.assertRaises(RuntimeError):
            paper_state.update_activation(self.states, old, True)

    def test_quote_timing_and_lot_size_validation(self):
        with self.assertRaises(ValueError):
            self.open(start="2026-11-13 10:00:59", end="2026-11-13 10:01:01")
        with self.assertRaises(ValueError):
            self.open(start="2026-11-13 10:01:01", end="2026-11-13 10:01:05")
        bad = dict(contract(), lot_size=2)
        with self.assertRaises(ValueError):
            paper_state.open_position(self.states, self.state, signal(), bad, 1, quote(),
                                      "2026-11-13 10:01:01", "2026-11-13 10:01:02")

    def test_quantity_validation(self):
        self.assertEqual(paper_state.validate_quantity("6", 3), 6)
        for value in (None, "", "0", "-1", "1.5", "nan", True, "abc"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                paper_state.validate_quantity(value, 1)
        with self.assertRaises(ValueError):
            paper_state.validate_quantity(5, 3)

    def test_multiple_lots_use_one_order_charge_per_side(self):
        position = self.open(quantity=3)
        self.assertEqual(position["quantity"], 3)
        self.assertTrue(position["entry_quote"]["insufficient_best_size"])
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        trade = paper_state.close_position(self.states, state, "cutoff", quote(),
                                           "2026-11-13 16:55:00", "2026-11-13 16:55:01")
        charges = sum_charges(silver_micro_estimated_order_charges("BUY", 99000, 3),
                              silver_micro_estimated_order_charges("SELL", 97000, 3))
        self.assertEqual(trade["pnl"], -6000)
        self.assertEqual(trade["charges"], charges)
        self.assertEqual(charges["brokerage"], 40)
        self.assertAlmostEqual(trade["net_pnl"], -6000 - charges["total"])
        self.assertTrue(trade["exit_quote"]["insufficient_best_size"])

    def test_evening_quote_received_after_deadline_does_not_fill(self):
        evening = signal("evening", "2026-11-13 22:49:00")
        with self.assertRaises(ValueError):
            self.open(doc=evening, start="2026-11-13 22:50:00",
                      end="2026-11-13 22:50:00.001")
        state = self.states.find_one({"_id": paper_state.STATE_ID})
        self.assertIsNone(state["position"])
        self.assertFalse(state["evening_used"])


if __name__ == "__main__":
    unittest.main()
