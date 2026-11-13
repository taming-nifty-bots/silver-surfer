"""Offline Silver Surfer regression tests and optional historical replay."""

import os
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from pathlib import Path
from threading import RLock
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
from pymongo.errors import DuplicateKeyError, OperationFailure
from tamingnifty.charges import silver_micro_estimated_order_charges, sum_charges

import executor
import executor as paper
import executor as paper_state
import signal_job as publisher
import strategy
import strategy as decisions
import strategy as market
import strategy as signals



class Collection:
    def __init__(self):
        self.docs = {}
        self.indexes = {}
        self.lock = RLock()

    def create_index(self, keys, **options):
        with self.lock:
            name = options["name"]
            specification = dict(key=keys, **options)
            if name in self.indexes and self.indexes[name] != specification:
                raise OperationFailure("Index options conflict")
            if options.get("unique"):
                seen = set()
                for doc in self.docs.values():
                    for value in self.index_values(doc, name):
                        if value in seen:
                            raise DuplicateKeyError("duplicate index key")
                        seen.add(value)
            self.indexes[name] = specification
            return name

    def index_values(self, doc, name):
        if name == "silver_one_active_trade":
            return {doc["strategy_state"]} if doc.get("strategy_state") == "active" else set()
        if name == "silver_occupied_minutes":
            return set(doc.get("occupied_minutes", []))
        return set()

    def check_indexes(self, doc, exclude=None):
        for name, spec in self.indexes.items():
            if not spec.get("unique"):
                continue
            values = self.index_values(doc, name)
            for key, other in self.docs.items():
                if key != exclude and values & self.index_values(other, name):
                    raise DuplicateKeyError("duplicate index key")

    def insert_one(self, doc):
        with self.lock:
            if doc["_id"] in self.docs:
                raise DuplicateKeyError("duplicate session")
            self.check_indexes(doc)
            self.docs[doc["_id"]] = deepcopy(doc)

    def find(self, query):
        with self.lock:
            return [deepcopy(doc) for doc in self.docs.values() if self.matches(doc, query)]

    def find_one(self, query, sort=None):
        docs = self.find(query)
        if sort:
            docs.sort(key=lambda doc: doc[sort[0][0]], reverse=sort[0][1] == -1)
        return docs[0] if docs else None

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
        with self.lock:
            for doc in self.docs.values():
                if self.matches(doc, query):
                    updated = deepcopy(doc)
                    for key, value in changes.get("$set", {}).items():
                        updated[key] = deepcopy(value)
                    for key, value in changes.get("$inc", {}).items():
                        updated[key] += value
                    for key, value in changes.get("$max", {}).items():
                        if key not in updated or value > updated[key]:
                            updated[key] = value
                    for key, value in changes.get("$min", {}).items():
                        if key not in updated or value < updated[key]:
                            updated[key] = value
                    for key in changes.get("$unset", {}):
                        updated.pop(key, None)
                    self.check_indexes(updated, exclude=doc["_id"])
                    self.docs[doc["_id"]] = updated
                    return SimpleNamespace(modified_count=1)
            return SimpleNamespace(modified_count=0)

    def replace_one(self, query, replacement):
        with self.lock:
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
        self.trades = Collection()
        paper_state.initialize_store(self.trades)
        self.state = paper_state.create_state(self.trades)

    def open(self, state=None, doc=None, start="2026-11-13 10:01:01",
             end="2026-11-13 10:01:02", quantity=1):
        return paper_state.open_position(self.trades, state or self.state,
                                        doc or signal(), contract(), quantity, quote(), start, end)

    def close(self, reason="channel10", start="2026-11-13 10:02:01", end="2026-11-13 10:02:02"):
        return paper_state.close_position(self.trades, paper_state.create_state(self.trades),
                                          reason, quote(), start, end)

    def test_competing_entries_and_restart_preserve_one_position(self):
        position = self.open()
        self.assertEqual(position["entry_price"], 99000)
        self.assertNotIn("id", position)
        for field in ("profit_active", "occupied_minutes", "exit_requested"):
            self.assertNotIn(field, position)
        self.assertIsNone(position["net_pnl"])
        self.assertEqual(position["strategy_version"], strategy.VERSION)
        self.assertEqual(position["instrument_name"], "SILVERMIC")
        self.assertEqual(position["broker"], "DHAN")
        self.assertTrue(position["simulated"])
        self.assertEqual(position["entry_date"], "2026-11-13")
        self.assertEqual(position["entry_time"], "10:01:02")
        self.assertEqual(position["entry_signal"], "2026-11-13 10:00:00")
        self.assertEqual(position["signal_doc_id"], "silver:morning")
        self.assertEqual(position["lot_size"], 1)
        self.assertEqual(position["requested_quantity"], 1)
        with self.assertRaises(RuntimeError):
            self.open()
        recovered = paper_state.create_state(self.trades)
        self.assertEqual(recovered["position"], position)
        self.assertTrue(recovered["morning_used"])
        self.assertEqual(self.trades.find_one({"_id": position["_id"]}), position)

    def test_close_updates_same_trade_without_archive_or_singleton(self):
        position = self.open()
        trade = self.close()
        self.assertEqual(trade["exit_price"], 97000)
        self.assertEqual(trade["exit_date"], "2026-11-13")
        self.assertEqual(trade["exit_time"], "10:02:02")
        self.assertEqual(trade["pnl"], -2000)
        self.assertAlmostEqual(trade["net_pnl"], trade["pnl"] - trade["charges"]["total"], places=2)
        self.assertIsNone(paper_state.create_state(self.trades)["position"])
        self.assertEqual(self.trades.find_one({"_id": position["_id"]}), trade)
        self.assertEqual(len(self.trades.docs), 1)
        paper_state.initialize_store(self.trades)

    def test_existing_trade_with_duplicate_id_recovers_and_closes(self):
        position = self.open()
        self.trades.docs[position["_id"]]["id"] = position["_id"]
        paper_state.initialize_store(self.trades)
        self.assertEqual(paper_state.create_state(self.trades)["position"]["id"], position["_id"])
        trade = self.close()
        self.assertNotIn("id", trade)
        self.assertEqual(self.trades.find_one({"_id": position["_id"]}), trade)
        paper_state.initialize_store(self.trades)

    def test_startup_rejects_mismatched_duplicate_id(self):
        position = self.open()
        self.trades.docs[position["_id"]]["id"] = "different"
        with self.assertRaises(RuntimeError):
            paper_state.initialize_store(self.trades)

    def test_startup_removes_duplicate_id_from_closed_trade(self):
        position = self.open()
        trade = self.close()
        self.trades.docs[position["_id"]]["id"] = position["_id"]
        paper_state.initialize_store(self.trades)
        self.assertEqual(self.trades.find_one({"_id": position["_id"]}), trade)

    def test_session_id_survives_close_and_blocks_stale_entry(self):
        self.open()
        self.close()
        with self.assertRaises(RuntimeError):
            self.open()
        with self.assertRaises(ValueError):
            self.open(paper_state.create_state(self.trades))

    def test_unique_partial_index_blocks_other_session_with_stale_snapshot(self):
        self.open()
        with self.assertRaises(RuntimeError):
            self.open(self.state, signal("evening", "2026-11-13 17:00:00"),
                      "2026-11-13 17:01:01", "2026-11-13 17:01:02")
        self.assertEqual(len(self.trades.docs), 1)

    def test_stale_snapshot_can_enter_in_exit_minute_without_reservation(self):
        stale = self.state
        self.open()
        self.close("cutoff", "2026-11-13 17:00:00", "2026-11-13 17:00:01")
        self.open(stale, signal("evening", "2026-11-13 16:59:00"),
                  "2026-11-13 17:00:02", "2026-11-13 17:00:03")
        self.assertEqual(len(self.trades.docs), 2)

    def test_short_pnl_and_charges(self):
        self.open(doc=signal(side="SELL"), quantity=3)
        trade = self.close()
        charges = sum_charges(silver_micro_estimated_order_charges("SELL", 97000, 3),
                              silver_micro_estimated_order_charges("BUY", 99000, 3))
        self.assertEqual(trade["pnl"], -6000)
        self.assertEqual(trade["charges"], charges)
        self.assertAlmostEqual(trade["net_pnl"], -6000 - charges["total"])

    def test_running_pnl_marks_long_and_short_and_closes(self):
        position = self.open(quantity=3)
        paper_state.mark_position(self.trades, position, 100000, "2026-11-13 10:01:10")
        position = paper_state.create_state(self.trades)["position"]
        self.assertEqual(position["running_pnl"], 3000)
        self.assertIsNone(position["net_pnl"])
        self.assertEqual(position["max_pnl_reached"], 3000)
        paper_state.mark_position(self.trades, position, 98000, "2026-11-13 10:01:20")
        position = paper_state.create_state(self.trades)["position"]
        self.assertEqual((position["running_pnl"], position["min_pnl_reached"]), (-3000, -3000))
        trade = self.close()
        self.assertEqual(trade["running_pnl"], trade["pnl"])
        self.assertEqual(trade["min_pnl_reached"], -6000)

        other = Collection()
        paper_state.initialize_store(other)
        short = paper_state.open_position(other, paper_state.create_state(other),
                                          signal(side="SELL"), contract(), 3, quote(),
                                          "2026-11-13 10:01:01", "2026-11-13 10:01:02")
        paper_state.mark_position(other, short, 96000, "2026-11-13 10:01:10")
        marked = paper_state.create_state(other)["position"]
        self.assertEqual((marked["running_pnl"], marked["max_pnl_reached"]), (3000, 3000))
        with self.assertRaises(RuntimeError):
            paper_state.mark_position(other, short, 96000, "2026-11-13 10:01:11")

    def test_legacy_trade_starts_tracking_only_when_first_marked(self):
        position = self.open()
        for key in ("running_pnl", "max_pnl_reached", "min_pnl_reached", "pnl_tracking_since"):
            del self.trades.docs[position["_id"]][key]
        position = paper_state.create_state(self.trades)["position"]
        paper_state.mark_position(self.trades, position, 100000, "2026-11-13 10:01:10")
        position = paper_state.create_state(self.trades)["position"]
        self.assertEqual(position["pnl_tracking_since"], "2026-11-13 10:01:10")
        self.assertEqual(position["max_pnl_reached"], 1000)
        self.assertEqual(position["min_pnl_reached"], 1000)

    def test_threads_competing_for_different_sessions_have_one_winner(self):
        def enter(session):
            try:
                if session == "morning":
                    self.open()
                else:
                    self.open(self.state, signal("evening", "2026-11-13 17:00:00"),
                              "2026-11-13 17:01:01", "2026-11-13 17:01:02")
                return True
            except RuntimeError:
                return False
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(enter, ["morning", "evening"] * 4))
        self.assertEqual(sum(results), 1)
        self.assertEqual(len(self.trades.find({"strategy_state": "active"})), 1)

    def test_one_trade_per_session_and_no_same_minute_handoff(self):
        self.open()
        self.close("cutoff", "2026-11-13 17:00:00", "2026-11-13 17:00:01")
        state = paper_state.create_state(self.trades)
        with self.assertRaises(ValueError):
            self.open(state, signal("evening", "2026-11-13 16:59:00"),
                      "2026-11-13 17:00:02", "2026-11-13 17:00:03")
        self.open(state, signal("evening", "2026-11-13 17:00:00", "SELL"),
                  "2026-11-13 17:01:01", "2026-11-13 17:01:02")
        self.assertEqual(len(self.trades.docs), 2)

    def test_checkpoint_rejects_stale_worker(self):
        self.open()
        old = paper_state.create_state(self.trades)
        paper_state.checkpoint_signal(self.trades, old, "2026-11-13 10:01:00")
        with self.assertRaises(RuntimeError):
            paper_state.checkpoint_signal(self.trades, old, "2026-11-13 10:01:00")

    def test_stale_close_fails_after_another_worker_closes(self):
        self.open()
        old = paper_state.create_state(self.trades)
        restarted = paper_state.create_state(self.trades)
        trade = paper_state.close_position(self.trades, restarted, "channel10", quote(),
                                          "2026-11-13 10:02:01", "2026-11-13 10:02:02")
        with self.assertRaises(RuntimeError):
            paper_state.close_position(self.trades, old, "channel10", quote(),
                                       "2026-11-13 10:02:01", "2026-11-13 10:02:02")
        self.assertEqual(self.trades.find_one({"_id": trade["_id"]}), trade)

    def test_exit_quote_cannot_start_before_entry(self):
        self.open()
        state = paper_state.create_state(self.trades)
        with self.assertRaises(ValueError):
            paper_state.close_position(self.trades, state, "channel10", quote(),
                                       "2026-11-13 10:01:01", "2026-11-13 10:01:03")
        self.assertIsNotNone(paper_state.create_state(self.trades)["position"])

    def test_startup_creates_required_index_and_fails_on_incompatible_index(self):
        self.assertEqual(self.trades.indexes["silver_one_active_trade"],
                         dict(key=[("strategy_state", 1)], name="silver_one_active_trade",
                              unique=True, partialFilterExpression={"strategy_state": "active"}))
        self.assertNotIn("silver_occupied_minutes", self.trades.indexes)
        self.trades.indexes["silver_one_active_trade"]["unique"] = False
        with self.assertRaises(OperationFailure):
            paper_state.initialize_store(self.trades)

    def test_startup_rejects_legacy_singleton_without_mutating_it(self):
        legacy = dict(_id="silver-surfer:paper", position={"id": "unreconciled"})
        self.trades.insert_one(legacy)
        with self.assertRaises(RuntimeError):
            paper_state.initialize_store(self.trades)
        self.assertEqual(self.trades.find_one({"_id": legacy["_id"]}), legacy)

    def test_startup_accepts_trade_without_minute_reservation(self):
        position = self.open()
        paper_state.initialize_store(self.trades)
        self.assertEqual(self.trades.find_one({"_id": position["_id"]}), position)

    def test_startup_rejects_multiple_active_trades(self):
        position = self.open()
        del self.trades.indexes["silver_one_active_trade"]
        other = dict(position, _id="silver:2026-11-13:evening", session="evening")
        self.trades.insert_one(other)
        with self.assertRaises((RuntimeError, DuplicateKeyError)):
            paper_state.initialize_store(self.trades)

    def test_quote_timing_and_lot_size_validation(self):
        with self.assertRaises(ValueError):
            self.open(start="2026-11-13 10:00:59", end="2026-11-13 10:01:01")
        with self.assertRaises(ValueError):
            self.open(start="2026-11-13 10:01:01", end="2026-11-13 10:01:05")
        with self.assertRaises(ValueError):
            paper_state.open_position(self.trades, self.state, signal(), dict(contract(), lot_size=2),
                                      1, quote(), "2026-11-13 10:01:01", "2026-11-13 10:01:02")

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
        trade = self.close()
        charges = sum_charges(silver_micro_estimated_order_charges("BUY", 99000, 3),
                              silver_micro_estimated_order_charges("SELL", 97000, 3))
        self.assertEqual(trade["pnl"], -6000)
        self.assertEqual(trade["charges"], charges)
        self.assertEqual(charges["brokerage"], 40)
        self.assertAlmostEqual(trade["net_pnl"], -6000 - charges["total"])
        self.assertTrue(trade["exit_quote"]["insufficient_best_size"])

    def test_evening_quote_received_after_deadline_does_not_fill(self):
        with self.assertRaises(ValueError):
            self.open(doc=signal("evening", "2026-11-13 22:49:00"),
                      start="2026-11-13 22:50:00", end="2026-11-13 22:50:00.001")
        self.assertEqual(self.trades.docs, {})


def decision_signal():
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
        self.assertEqual(decisions.entry_decision(decision_signal(), "2026-11-13 10:01:10", True), "BUY")
        self.assertIsNone(decisions.entry_decision(decision_signal(), "2026-11-13 10:01:10", False))
        with self.assertRaises(ValueError):
            decisions.entry_decision(decision_signal(), "2026-11-13 10:02:00", True)

    def test_no_opening_brick_and_session_boundary(self):
        doc = decision_signal()
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
        result = decisions.exit_decision(trade(), decision_signal(), "2026-11-13 10:01")
        self.assertEqual(result, dict(reason="channel7", profit_active=True))
        doc = decision_signal()
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

    def test_evening_entry_deadline_is_exact(self):
        doc = dict(decision_signal(), session="evening", data_asof="2026-11-13 22:49:00",
                   brick_time="2026-11-13 22:49:00")
        self.assertEqual(decisions.entry_decision(doc, "2026-11-13 22:50:00", True), "BUY")
        self.assertIsNone(decisions.entry_decision(doc, "2026-11-13 22:50:00.001", True))
        doc.update(data_asof="2026-11-13 22:50:00", brick_time="2026-11-13 22:50:00")
        self.assertIsNone(decisions.entry_decision(doc, "2026-11-13 22:51:00", True))

    def test_evening_exit_cutoff(self):
        position = dict(trade(), session="evening", entry_timestamp="2026-11-13 22:00:00")
        with self.assertRaises(ValueError):
            decisions.exit_decision(position, None, "2026-11-13 23:09:59")
        self.assertEqual(decisions.exit_decision(position, None, "2026-11-13 23:10:00")["reason"],
                         "session cutoff")

    def test_contract_mismatch_and_old_signal(self):
        doc = deepcopy(decision_signal())
        doc["security_id"] = "102"
        with self.assertRaises(ValueError):
            decisions.exit_decision(trade(), doc, "2026-11-13 10:01")
        with self.assertRaises(ValueError):
            decisions.exit_decision(trade(), decision_signal(), "2026-11-13 10:02")

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
        self.assertFalse(decisions.recover_activation(dict(trade(), profit_active=True),
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
        first = dict(decision_signal(), data_asof="2026-11-13 10:00:00", candle_close=101000,
                     close=100500, lower7=100000)
        second = dict(decision_signal(), data_asof="2026-11-13 10:01:00", candle_close=99000,
                      close=99000, lower7=100000)
        with patch("strategy.build_signal", side_effect=[first, second]):
            result = decisions.recover_missed_exit(position, candles, "2026-11-13 10:01")
        self.assertEqual(result, dict(reason="channel7", profit_active=True,
                                      data_asof="2026-11-13 10:01:00"))

    def test_recovery_reconstructs_activation_before_checkpoint(self):
        position = dict(trade(), last_checked_asof="2026-11-13 10:00:00")
        del position["profit_active"]
        candles = pd.DataFrame([dict(datetime="2026-11-13 09:59", close=101000),
                                dict(datetime="2026-11-13 10:00", close=100000),
                                dict(datetime="2026-11-13 10:01", close=99000)])
        current = dict(decision_signal(), data_asof="2026-11-13 10:01:00",
                       candle_close=99000, close=99000, lower7=100000)
        with patch("strategy.build_signal", return_value=current):
            result = decisions.recover_missed_exit(position, candles, "2026-11-13 10:01")
        self.assertEqual(result["reason"], "channel7")


def failed_alert(message):
    raise RuntimeError("offline alert")


class ExecutorTests(unittest.TestCase):
    def setUp(self):
        self.trades, self.signals = Collection(), Collection()
        paper_state.initialize_store(self.trades)
        self.alerts = []
        self.now = datetime(2026, 11, 13, 10, 1, 2)
        self.doc = dict(signal(), _id="silver:morning",
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
        executor.run_cycle(self.trades, self.signals, None,
                           self.get_quote, self.get_candles, self.clock, self.alerts.append, 1)

    def test_paper_entry_activation_and_exit(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        state = paper_state.create_state(self.trades)
        self.assertEqual(state["position"]["entry_price"], 99000)
        self.now = datetime(2026, 11, 13, 10, 2, 2)
        self.doc.update(data_asof="2026-11-13 10:01:00", brick_time="2026-11-13 10:01:00",
                        candle_close=100000, close=97000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        self.cycle()
        state = paper_state.create_state(self.trades)
        self.assertIsNone(state["position"])
        trade = self.trades.find_one({"_id": "silver:2026-11-13:morning"})
        self.assertEqual(trade["exit_reason"], "channel7")
        self.assertEqual(trade["pnl"], -2000)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.assertEqual(len(self.trades.docs), 1)

    def test_short_entry_exit_and_charges(self):
        self.doc["side"] = "SELL"
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                               self.get_candles, self.clock, self.alerts.append, 3)
        self.now = datetime(2026, 11, 13, 10, 2, 2)
        self.doc.update(data_asof="2026-11-13 10:01:00", brick_time="2026-11-13 10:01:00",
                        candle_close=97000, close=103000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch.object(self, "get_candles", return_value=pd.DataFrame(
                [dict(datetime="2026-11-13 10:01", close=97000)])):
            self.cycle()
        trade = self.trades.find_one({"_id": "silver:2026-11-13:morning"})
        self.assertEqual((trade["side"], trade["entry_price"], trade["exit_price"], trade["quantity"]),
                         ("SELL", 97000, 99000, 3))
        self.assertEqual(trade["pnl"], -6000)
        self.assertAlmostEqual(trade["net_pnl"], trade["pnl"] - trade["charges"]["total"], places=2)

    def test_configured_quantity_and_existing_position_recovery(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                               self.get_candles, self.clock, self.alerts.append, 3)
        state = paper_state.create_state(self.trades)
        self.assertEqual(state["position"]["quantity"], 3)
        self.assertTrue(any("Qty 3" in text for text in self.alerts))
        self.now = datetime(2026, 11, 13, 16, 55)
        self.cycle()
        trade = self.trades.find_one({"_id": "silver:2026-11-13:morning"})
        self.assertEqual(trade["quantity"], 3)
        self.assertEqual(trade["pnl"], -6000)

    def test_missing_and_invalid_quantity_rejected_at_startup(self):
        environment = dict(CONNECTION_STRING="offline", user_name="test", slack_token="offline",
                           DHAN_CLIENT_ID="offline", DHAN_PIN="offline", DHAN_TOTP="offline",
                           SILVER_SESSION="morning", live_trading="false")
        for quantity in (None, "0", "1.5"):
            values = dict(environment)
            if quantity is not None:
                values["quantity"] = quantity
            with self.subTest(quantity=quantity), patch.dict(os.environ, values, clear=True), \
                    patch("executor.load_dotenv"), patch("executor.find_dotenv", return_value=""), \
                    self.assertRaises(ValueError):
                executor.main()

    def test_live_and_invalid_modes_fail_before_connecting(self):
        environment = dict(CONNECTION_STRING="offline", user_name="test", slack_token="offline",
                           DHAN_CLIENT_ID="offline", DHAN_PIN="offline", DHAN_TOTP="offline",
                           SILVER_SESSION="morning", quantity="1")
        for mode in ("true", "1", "live", ""):
            with self.subTest(mode=mode), \
                    patch.dict(os.environ, dict(environment, live_trading=mode), clear=True), \
                    patch("executor.load_dotenv"), patch("executor.find_dotenv", return_value=""), \
                    patch("executor.MongoClient") as mongo, \
                    patch.object(executor.dhan, "login_to_dhan") as login:
                with self.assertRaisesRegex(ValueError, "live orders are disabled"):
                    executor.main()
                mongo.assert_not_called()
                login.assert_not_called()

    def test_paper_main_recovers_same_trade_and_closes_at_cutoff(self):
        environment = dict(CONNECTION_STRING="offline", user_name="test", slack_token="offline",
                           DHAN_CLIENT_ID="offline", DHAN_PIN="offline", DHAN_TOTP="offline",
                           SILVER_SESSION="morning", live_trading="false", quantity="1")
        trade_collection = MagicMock()
        trade_collection.with_options.return_value = self.trades
        collections = dict(supertrend=self.signals, silver_surfer_test=trade_collection,
                           exchange_calendars=Collection())
        database = MagicMock()
        database.__getitem__.side_effect = collections.__getitem__
        client = MagicMock()
        client.__getitem__.return_value = database

        def reach_cutoff(seconds):
            self.now = datetime(2026, 11, 13, 16, 55)

        with patch.dict(os.environ, environment, clear=True), \
                patch.object(executor, "load_dotenv"), \
                patch.object(executor, "find_dotenv", return_value=""), \
                patch.object(executor, "MongoClient", return_value=client), \
                patch.object(executor, "WebClient"), \
                patch.object(executor, "notify"), \
                patch.object(executor, "now_ist", side_effect=self.clock), \
                patch.object(executor.time, "sleep", side_effect=reach_cutoff), \
                patch.object(executor.dhan, "login_to_dhan"), \
                patch.object(executor.dhan, "get_commodity_ltp", return_value=99000), \
                patch.object(executor.dhan, "get_depth", side_effect=[quote(), quote()]) as depth, \
                patch.object(strategy, "session_status", return_value=dict(morning=True, evening=True)):
            executor.main()
        self.assertEqual(len(self.trades.docs), 1)
        trade = self.trades.find_one({"_id": "silver:2026-11-13:morning"})
        self.assertEqual(trade["strategy_state"], "closed")
        self.assertEqual(trade["exit_reason"], "session cutoff")
        self.assertEqual(trade["exit_price"], 97000)
        self.assertEqual(depth.call_count, 2)
        concerns = trade_collection.with_options.call_args.kwargs
        self.assertEqual(concerns["write_concern"].document, {"w": "majority"})
        self.assertEqual(concerns["read_concern"].document, {"level": "majority"})
        client.close.assert_called_once_with()

    def test_entry_persisted_before_alert_failure_is_not_repeated(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)), \
                self.assertRaisesRegex(RuntimeError, "offline alert"):
            executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                               self.get_candles, self.clock, failed_alert, 1)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)), \
                patch.object(self, "get_quote") as get_quote:
            self.cycle()
            get_quote.assert_not_called()
        self.assertEqual(len(self.trades.docs), 1)
        self.assertIsNotNone(paper_state.create_state(self.trades)["position"])

    def test_exit_persisted_before_alert_failure_is_not_repeated(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 16, 55)
        with self.assertRaisesRegex(RuntimeError, "offline alert"):
            executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                               self.get_candles, self.clock, failed_alert, 1)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)), \
                patch.object(self, "get_quote") as get_quote:
            self.cycle()
            get_quote.assert_not_called()
        self.assertEqual(len(self.trades.docs), 1)
        self.assertIsNone(paper_state.create_state(self.trades)["position"])

    def test_cutoff_exit_without_calendar_or_signal(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.signals.docs.clear()
        self.now = datetime(2026, 11, 13, 16, 55, 0)
        with patch("strategy.session_status", side_effect=RuntimeError("calendar unavailable")):
            self.cycle()
        state = paper_state.create_state(self.trades)
        self.assertIsNone(state["position"])
        self.assertEqual(self.trades.find_one({"_id": "silver:2026-11-13:morning"})["exit_reason"],
                         "session cutoff")

    def test_evening_position_is_managed_after_23_and_forced_at_2310(self):
        self.now = datetime(2026, 11, 13, 22, 1)
        self.doc.update(_id="silver:evening", session="evening",
                        data_asof="2026-11-13 22:00:00", brick_time="2026-11-13 22:00:00")
        self.signals.insert_one(self.doc)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 23, 9, 20)
        state = paper_state.create_state(self.trades)
        self.assertFalse(executor.session_finished(state, "evening", self.now))
        self.signals.docs.clear()
        with self.assertRaises(ValueError):
            self.cycle()
        self.assertIsNotNone(paper_state.create_state(self.trades)["position"])
        self.now = datetime(2026, 11, 13, 23, 10, 0)
        with patch("strategy.session_status", side_effect=RuntimeError("calendar unavailable")):
            self.cycle()
        trade = self.trades.find_one({"_id": "silver:2026-11-13:evening"})
        self.assertEqual(trade["exit_reason"], "session cutoff")
        self.assertEqual(trade["exit_timestamp"], "2026-11-13 23:10:00")

    def test_evening_no_position_stops_after_entry_deadline(self):
        state = paper_state.create_state(self.trades)
        self.assertFalse(executor.session_finished(state, "evening", datetime(2026, 11, 13, 22, 50)))
        self.assertTrue(executor.session_finished(state, "evening", datetime(2026, 11, 13, 22, 50, 1)))

    def test_evening_fresh_signal_manages_position_after_23(self):
        self.now = datetime(2026, 11, 13, 22, 1)
        self.doc.update(_id="silver:evening", session="evening",
                        data_asof="2026-11-13 22:00:00", brick_time="2026-11-13 22:00:00")
        self.signals.insert_one(self.doc)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        state = paper_state.create_state(self.trades)
        paper_state.checkpoint_signal(self.trades, state, "2026-11-13 23:07:00")
        self.doc.update(data_asof="2026-11-13 23:08:00", candle_close=99000, close=96000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        self.now = datetime(2026, 11, 13, 23, 9, 20)
        with patch.object(self, "get_candles", return_value=pd.DataFrame(
                [dict(datetime="2026-11-13 23:08", close=99000)])):
            self.cycle()
        trade = self.trades.find_one({"_id": "silver:2026-11-13:evening"})
        self.assertEqual(trade["exit_reason"], "channel10")

    def test_evening_cutoff_during_candle_fetch_and_failed_quote_retry(self):
        self.now = datetime(2026, 11, 13, 22, 1)
        self.doc.update(_id="silver:evening", session="evening",
                        data_asof="2026-11-13 22:00:00", brick_time="2026-11-13 22:00:00")
        self.signals.insert_one(self.doc)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        state = paper_state.create_state(self.trades)
        paper_state.checkpoint_signal(self.trades, state, "2026-11-13 23:07:00")
        self.doc.update(data_asof="2026-11-13 23:08:00", candle_close=99000, close=99000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        self.now = datetime(2026, 11, 13, 23, 9, 59)

        def late_candles(position):
            self.now = datetime(2026, 11, 13, 23, 10)
            return pd.DataFrame([dict(datetime="2026-11-13 23:08", close=99000)])

        with patch.object(self, "get_candles", side_effect=late_candles), \
                patch.object(self, "get_quote", side_effect=ValueError("No quote")):
            with self.assertRaises(ValueError):
                self.cycle()
        state = paper_state.create_state(self.trades)
        self.assertNotIn("exit_requested", state["position"])
        self.cycle()
        trade = self.trades.find_one({"_id": "silver:2026-11-13:evening"})
        self.assertEqual(trade["exit_reason"], "session cutoff")

    def test_evening_waits_one_minute_then_recovers_old_morning_position(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        start = datetime(2026, 11, 13, 16, 58)
        self.now = datetime(2026, 11, 13, 17, 0, 59)
        executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                           self.get_candles, self.clock, self.alerts.append, 1,
                           session="evening", started_at=start)
        self.assertIsNotNone(paper_state.create_state(self.trades)["position"])
        self.now = datetime(2026, 11, 13, 17, 1)
        executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                           self.get_candles, self.clock, self.alerts.append, 1,
                           session="evening", started_at=start)
        state = paper_state.create_state(self.trades)
        self.assertIsNone(state["position"])
        self.assertEqual(self.trades.find_one({"_id": "silver:2026-11-13:morning"})["exit_reason"],
                         "session cutoff")

    def test_evening_waits_one_minute_after_late_start(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        start = datetime(2026, 11, 13, 18, 30)
        self.now = datetime(2026, 11, 13, 18, 30, 59)
        executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                           self.get_candles, self.clock, self.alerts.append, 1,
                           session="evening", started_at=start)
        self.assertIsNotNone(paper_state.create_state(self.trades)["position"])
        self.now = datetime(2026, 11, 13, 18, 31)
        executor.run_cycle(self.trades, self.signals, None, self.get_quote,
                           self.get_candles, self.clock, self.alerts.append, 1,
                           session="evening", started_at=start)
        self.assertIsNone(paper_state.create_state(self.trades)["position"])

    def test_failed_forced_exit_keeps_position_for_retry(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 16, 55)
        with patch.object(self, "get_quote", side_effect=ValueError("No quote")):
            with self.assertRaises(ValueError):
                self.cycle()
        self.assertIsNotNone(paper_state.create_state(self.trades)["position"])
        self.cycle()
        self.assertIsNone(paper_state.create_state(self.trades)["position"])

    def test_session_stops_after_close_and_does_not_stop_for_open_position(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        state = paper_state.create_state(self.trades)
        self.assertFalse(executor.session_finished(state, "morning", datetime(2026, 11, 13, 16, 56)))
        self.now = datetime(2026, 11, 13, 16, 55)
        self.cycle()
        state = paper_state.create_state(self.trades)
        self.assertTrue(executor.session_finished(state, "morning", self.now))

    def test_stale_signal_never_enters(self):
        self.now = datetime(2026, 11, 13, 10, 2, 20)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            with self.assertRaises(ValueError):
                self.cycle()
        self.assertIsNone(paper_state.create_state(self.trades)["position"])

    def test_wait_briefly_for_publisher_without_alerting_on_normal_minute_rollover(self):
        self.now = datetime(2026, 11, 13, 10, 2, 5)
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.assertIsNone(paper_state.create_state(self.trades)["position"])
        self.assertEqual(self.alerts, [])

    def test_restart_warns_when_checks_and_candles_were_missed(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 10, 4, 0)
        self.doc.update(data_asof="2026-11-13 10:03:00", brick_time="2026-11-13 10:03:00",
                        candle_close=99000, close=99000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch("strategy.recover_missed_exit",
                   return_value=dict(reason=None, profit_active=False, data_asof="2026-11-13 10:03:00")):
            self.cycle()
        self.assertTrue(any("missed signal checks" in text for text in self.alerts))
        self.assertTrue(any("missing completed candles" in text for text in self.alerts))

    def test_recovered_exit_uses_current_quote(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 10, 4, 0)
        self.doc.update(data_asof="2026-11-13 10:03:00", brick_time="2026-11-13 10:03:00")
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch("strategy.recover_missed_exit",
                   return_value=dict(reason="channel7", profit_active=True,
                                     data_asof="2026-11-13 10:01:00")):
            self.cycle()
        trade = self.trades.find_one({"_id": "silver:2026-11-13:morning"})
        self.assertEqual(trade["exit_timestamp"], "2026-11-13 10:04:00")
        self.assertEqual(trade["exit_price"], 97000)
        self.assertTrue(any("Recovered channel7" in text for text in self.alerts))

    def test_failed_exit_quote_does_not_checkpoint_the_exit_signal(self):
        with patch("strategy.session_status", return_value=dict(morning=True, evening=True)):
            self.cycle()
        self.now = datetime(2026, 11, 13, 10, 2, 2)
        self.doc.update(data_asof="2026-11-13 10:01:00", brick_time="2026-11-13 10:01:00",
                        candle_close=100000, close=97000)
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch.object(self, "get_quote", side_effect=ValueError("No quote")):
            with self.assertRaises(ValueError):
                self.cycle()
        state = paper_state.create_state(self.trades)
        self.assertNotIn("profit_active", state["position"])
        self.assertEqual(state["position"]["last_checked_asof"], "2026-11-13 10:00:00")
        self.assertNotIn("exit_requested", state["position"])
        self.now = datetime(2026, 11, 13, 10, 4, 2)
        self.doc.update(data_asof="2026-11-13 10:03:00", brick_time="2026-11-13 10:03:00")
        self.signals.replace_one({"_id": self.doc["_id"]}, self.doc)
        with patch("strategy.recover_missed_exit",
                   return_value=dict(reason="channel7", profit_active=True,
                                     data_asof="2026-11-13 10:01:00")):
            self.cycle()
        self.assertEqual(self.trades.find_one({"_id": "silver:2026-11-13:morning"})["exit_reason"],
                         "channel7")


def calendar_checks():
    document = dict(_id="MCX:2026", exchange="MCX", year=2026, status="verified",
                    valid_from="2026-01-01", valid_to="2026-12-31", timezone="Asia/Kolkata",
                    holidays=[
                        dict(date="2026-11-24", morning="closed", evening="open"),
                        dict(date="2026-12-25", morning="closed", evening="closed"),
                    ],
                    special_sessions=[dict(date="2026-11-08")])
    calendars = SimpleNamespace(find_one=lambda query: document if query["_id"] == "MCX:2026" else None)
    assert market.session_status(calendars, date(2026, 11, 24)) == dict(morning=False, evening=True, special=False)
    assert market.session_status(calendars, date(2026, 12, 25)) == dict(morning=False, evening=False, special=False)
    assert market.session_status(calendars, date(2026, 11, 8)) == dict(morning=False, evening=False, special=True)
    assert market.session_status(calendars, date(2026, 11, 7)) == dict(morning=False, evening=False, special=False)
    assert market.trading_days_before_expiry(calendars, date(2026, 11, 19), date(2026, 11, 30)) == 7
    assert market.trading_days_before_expiry(calendars, date(2026, 11, 18), date(2026, 11, 30)) == 8
    master = pd.DataFrame([
        dict(EXCH_ID="MCX", SEGMENT="M", INSTRUMENT="FUTCOM", UNDERLYING_SYMBOL="SILVERMIC",
             SM_EXPIRY_DATE="2026-11-30", SECURITY_ID=1, DISPLAY_NAME="NOV", LOT_SIZE=1, TICK_SIZE=100),
        dict(EXCH_ID="MCX", SEGMENT="M", INSTRUMENT="FUTCOM", UNDERLYING_SYMBOL="SILVERMIC",
             SM_EXPIRY_DATE="2027-02-26", SECURITY_ID=2, DISPLAY_NAME="FEB", LOT_SIZE=1, TICK_SIZE=100),
    ])
    assert market.silver_contract(master, calendars, date(2026, 11, 18))["security_id"] == "1"
    assert market.silver_contract(master, calendars, date(2026, 11, 19))["security_id"] == "2"
    assert market.silver_contract(master, calendars, date(2026, 11, 19))["tick_size"] == 1
    try:
        market.session_status(calendars, date(2027, 1, 1))
    except ValueError:
        pass
    else:
        raise AssertionError("Missing calendar must not be treated as open")
    print("MCX session and seven-trading-day rollover checks passed")


class PaperTests(unittest.TestCase):
    def setUp(self):
        self.quote = {"depth": {
            "buy": [{"price": 97000, "quantity": 1}, {"price": 96995, "quantity": 4}],
            "sell": [{"price": 99000, "quantity": 1}, {"price": 99010, "quantity": 4}],
        }}

    def test_both_sides_and_wide_spread(self):
        buy = paper.quote_fill(self.quote, "BUY", 1)
        sell = paper.quote_fill(self.quote, "SELL", 1)
        self.assertEqual(buy["price"], 99000)
        self.assertEqual(sell["price"], 97000)
        self.assertEqual(buy["spread"], 2000)
        self.assertFalse(buy["insufficient_best_size"])

    def test_insufficient_top_size_is_recorded_not_a_filter(self):
        buy = paper.quote_fill(self.quote, "BUY", 2)
        self.assertEqual(buy["price"], 99000)
        self.assertTrue(buy["insufficient_best_size"])

    def test_invalid_or_missing_books_rejected(self):
        self.quote["depth"]["buy"] = []
        with self.assertRaises(ValueError):
            paper.quote_fill(self.quote, "BUY", 1)
        self.quote["depth"]["buy"] = [{"price": 100000, "quantity": 1}]
        with self.assertRaises(ValueError):
            paper.quote_fill(self.quote, "SELL", 1)
        self.quote["depth"]["buy"] = [{"price": float("nan"), "quantity": 1}]
        with self.assertRaises(ValueError):
            paper.quote_fill(self.quote, "BUY", 1)


def document(candle="2026-11-13 10:00:00"):
    return dict(_id="silver:morning", strategy_version=strategy.VERSION,
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

    def test_silver_lock_and_signals_coexist_with_interceptor(self):
        interceptor = {"_id": "nifty_options_renko", "marker": "unchanged"}
        self.collection.insert_one(interceptor)
        publisher.lock_contract(self.collection, "2026-11-13", contract())
        publisher.publish(self.collection, document())
        evening = dict(document("2026-11-13 17:00:00"), _id="silver:evening")
        publisher.publish(self.collection, evening)
        self.assertEqual(self.collection.find_one({"_id": interceptor["_id"]}), interceptor)
        self.assertEqual(len(self.collection.docs), 4)

    def test_contract_lock_rolls_forward_but_never_back(self):
        publisher.lock_contract(self.collection, "2026-11-13", contract())
        newer = dict(contract(), security_id="102")
        self.assertEqual(publisher.lock_contract(self.collection, "2026-11-14", newer), newer)
        with self.assertRaises(ValueError):
            publisher.lock_contract(self.collection, "2026-11-13", contract())
        self.assertEqual(len(self.collection.docs), 1)

    def test_signal_document_is_reused_and_cannot_move_backward(self):
        publisher.publish(self.collection, document())
        newer = dict(document("2026-11-14 10:00:00"), security_id="102")
        self.assertTrue(publisher.publish(self.collection, newer))
        self.assertFalse(publisher.publish(self.collection, document("2026-11-13 10:01:00")))
        self.assertEqual(self.collection.find_one({"_id": "silver:morning"}), newer)
        self.assertEqual(len(self.collection.docs), 1)

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
        with patch("strategy.build_signal", return_value={"data_asof": "2026-11-13 10:00"}):
            with patch("strategy.session_gaps", return_value=["2026-11-13 09:10:00"]):
                doc = publisher.prepare_signal(pd.DataFrame(), "2026-11-13", "2026-11-13 10:01",
                                               "morning", dict(morning=True, evening=True),
                                               contract(), None)
        self.assertEqual(doc["missing_minutes"], ["2026-11-13 09:10:00"])
        self.assertTrue(publisher.publish(self.collection, doc))

    def test_signal_job_writes_only_fixed_signal_ids_and_closes_client(self):
        environment = dict(CONNECTION_STRING="offline", user_name="test", slack_token="offline",
                           DHAN_CLIENT_ID="offline", DHAN_PIN="offline", DHAN_TOTP="offline",
                           SILVER_SESSION="morning")
        trades = Collection()
        calendars = Collection()
        database = MagicMock()
        collections = dict(supertrend=self.collection, silver_surfer_test=trades,
                           exchange_calendars=calendars)
        database.__getitem__.side_effect = collections.__getitem__
        client = MagicMock()
        client.__getitem__.return_value = database
        now = datetime(2026, 11, 13, 10, 1, 2)
        times = [now, now, now, datetime(2026, 11, 13, 16, 55)]
        signal = dict(data_asof="2026-11-13 10:00:00", session="morning")
        with patch.dict(os.environ, environment, clear=True), \
                patch.object(publisher, "load_dotenv"), \
                patch.object(publisher, "find_dotenv", return_value=""), \
                patch.object(publisher, "MongoClient", return_value=client), \
                patch.object(publisher, "WebClient"), \
                patch.object(publisher, "notify"), \
                patch.object(publisher, "now_ist", side_effect=times), \
                patch.object(publisher.time, "sleep"), \
                patch.object(publisher.dhan, "load_instruments"), \
                patch.object(publisher.dhan, "login_to_dhan"), \
                patch.object(publisher.dhan, "fetch_candles", return_value=pd.DataFrame()), \
                patch.object(strategy, "silver_contract", return_value=contract()), \
                patch.object(strategy, "session_status", return_value=dict(morning=True, evening=True)), \
                patch.object(strategy, "build_signal", return_value=signal), \
                patch.object(strategy, "session_gaps", return_value=[]):
            publisher.main()
        self.assertEqual(set(self.collection.docs), {"silver:contract", "silver:morning"})
        self.assertEqual(trades.docs, {})
        self.assertEqual(calendars.docs, {})
        self.assertEqual({call.args[0] for call in database.__getitem__.call_args_list},
                         set(collections))
        client.close.assert_called_once_with()


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
        with patch.object(signals, "chart_history", return_value=bars), \
                patch.object(signals, "chart", return_value=pd.DataFrame([brick])) as chart:
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


def basic_checks():
    brick = dict(brick_time="2026-11-13 10:00", close=101, upper40=100, lower40=90,
                 upper10=102, lower10=95, upper7=101.5, lower7=99)
    assert strategy.entry_side("2026-11-13 10:00", brick) == "BUY"
    assert strategy.entry_side("2026-11-13 10:01", brick) is None
    assert strategy.entry_side("2026-11-13 09:00", dict(brick, brick_time="2026-11-13 09:00")) is None
    assert strategy.entry_side("2026-11-13 10:00", dict(brick, close=100)) is None
    assert strategy.entry_side("2026-11-13 10:00", dict(brick, close=89)) == "SELL"
    assert strategy.profit_active("BUY", 100, 101, False)
    assert not strategy.profit_active("BUY", 100, 100.99, False)
    assert strategy.profit_active("SELL", 100, 99, False)
    assert strategy.profit_active("SELL", 100, 100, True)
    assert not strategy.channel_exit("BUY", dict(brick, close=99), True)
    assert strategy.channel_exit("BUY", dict(brick, close=98.99), True)
    assert strategy.channel_exit("SELL", dict(brick, close=101.51), True)
    assert not strategy.channel_exit("SELL", dict(brick, close=101.51), False)
    print("Strategy boundary checks passed")


def replay_checks(morning_cutoff=time(17), evening_last_entry=time(22, 59),
                  evening_cutoff=time(23)):
    raw = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(raw))
    import silver_micro_fresh_final_20261113 as fresh

    data = pd.read_csv(fresh.SOURCE, parse_dates=["datetime"])
    data = data.loc[data.expiry_rank == 1].copy()
    data["day"] = data["datetime"].dt.date
    sessions = pd.read_csv(raw / "silver_micro_holiday_aware_20261113_sessions.csv")
    expected = pd.read_csv(raw / "silver_micro_holiday_aware_20261113_trades.csv")
    expected = expected.loc[(expected.scenario == "existing_rule") & (expected.slippage == 5)]
    results = []
    for session in sessions.to_dict("records"):
        if session["status"] != "scored":
            continue
        day = pd.Timestamp(session["date"]).date()
        start = time(17) if session["holiday"] else time(9)
        current = data.loc[(data.day == day) & (data.datetime.dt.time >= start)
                           & (data.datetime.dt.time <= evening_cutoff)].reset_index(drop=True)
        history = strategy.chart_history(data, day, datetime.combine(day, evening_cutoff) + timedelta(minutes=1))
        features = {}
        for setup, box in (("morning", 0.1), ("evening", 0.15)):
            end = (datetime.combine(day, time(17)) if setup == "morning"
                   else datetime.combine(day, evening_cutoff) + timedelta(minutes=1))
            candles = history.loc[history.datetime < end]
            bricks = strategy.chart(candles, box)
            features[setup] = pd.merge_asof(current[["datetime"]], bricks, on="datetime", direction="backward")
        rows = current.to_dict("records")
        charts = {key: value.to_dict("records") for key, value in features.items()}
        taken = set()
        position = None
        for i in range(len(rows) - 1):
            row, nxt = rows[i], rows[i + 1]
            assert nxt["datetime"] == row["datetime"] + timedelta(minutes=1)
            clock = nxt["datetime"].time()
            if position:
                brick = charts[position["setup"]][i]
                position["active"] = strategy.profit_active(position["side"], position["price"],
                                                             row["close"], position["active"])
                cutoff = morning_cutoff if position["setup"] == "morning" else evening_cutoff
                if clock >= cutoff or strategy.channel_exit(position["side"], brick, position["active"]):
                    direction = 1 if position["side"] == "BUY" else -1
                    results.append((str(day), position["setup"], position["side"],
                                    str(position["signal"]), str(position["entry"]),
                                    str(nxt["datetime"]), position["price"],
                                    float(nxt["open"]) - direction * 5))
                    position = None
                    continue
            if position or clock >= evening_cutoff:
                continue
            if morning_cutoff <= clock < time(17):
                continue
            if clock >= time(17) and clock > evening_last_entry:
                continue
            setup = "morning" if clock < time(17) else "evening"
            if setup in taken:
                continue
            side = strategy.entry_side(row["datetime"], charts[setup][i])
            if side:
                direction = 1 if side == "BUY" else -1
                position = dict(setup=setup, side=side, signal=row["datetime"], entry=nxt["datetime"],
                                price=float(nxt["open"]) + direction * 5, active=False)
                taken.add(setup)
        assert position is None
    actual = pd.DataFrame(results, columns=["date", "setup", "side", "signal_time", "entry_time",
                                           "exit_time", "entry_price", "exit_price"])
    columns = list(actual.columns)
    wanted = expected[columns].reset_index(drop=True)
    if morning_cutoff == time(17) and evening_cutoff == time(23) and evening_last_entry == time(22, 59):
        pd.testing.assert_frame_equal(actual, wanted, check_dtype=False)
        print(f"Holiday-aware research parity passed: {len(actual)} trades, entries, exits and prices")
    return actual


def compare_cutoffs():
    from tamingnifty.charges import money, silver_micro_estimated_order_charges, sum_charges

    baseline = replay_checks()
    changed = replay_checks(time(16, 55))
    for label, frame in (("17:00", baseline), ("16:55", changed)):
        values = []
        for trade in frame.to_dict("records"):
            direction = 1 if trade["side"] == "BUY" else -1
            exit_side = "SELL" if trade["side"] == "BUY" else "BUY"
            fees = sum_charges(silver_micro_estimated_order_charges(trade["side"], trade["entry_price"], 1),
                               silver_micro_estimated_order_charges(exit_side, trade["exit_price"], 1))
            values.append(money(direction * (trade["exit_price"] - trade["entry_price"]) - fees["total"]))
        wins = [value for value in values if value > 0]
        losses = [value for value in values if value < 0]
        print(f"{label}: trades={len(values)}, estimated_net={money(sum(values))}, "
              f"wins={len(wins)}, losses={len(losses)}, "
              f"profit_factor={sum(wins) / -sum(losses):.3f}")
    compared = baseline.merge(changed, on=["date", "setup"], how="outer", suffixes=("_old", "_new"),
                              indicator=True)
    for row in compared.to_dict("records"):
        if (row["_merge"] != "both" or row["entry_time_old"] != row["entry_time_new"]
                or row["exit_time_old"] != row["exit_time_new"]
                or row["exit_price_old"] != row["exit_price_new"]):
            print("Changed:", row)


def compare_evening():
    from tamingnifty.charges import money, silver_micro_estimated_order_charges, sum_charges

    old = replay_checks(time(16, 55))
    new = replay_checks(time(16, 55), time(22, 50), time(23, 10))
    for label, frame in (("old evening", old), ("new evening", new)):
        values = []
        for trade in frame.to_dict("records"):
            direction = 1 if trade["side"] == "BUY" else -1
            exit_side = "SELL" if trade["side"] == "BUY" else "BUY"
            fees = sum_charges(silver_micro_estimated_order_charges(trade["side"], trade["entry_price"], 1),
                               silver_micro_estimated_order_charges(exit_side, trade["exit_price"], 1))
            values.append(money(direction * (trade["exit_price"] - trade["entry_price"]) - fees["total"]))
        wins = [value for value in values if value > 0]
        losses = [value for value in values if value < 0]
        print(f"{label}: trades={len(values)}, estimated_net={money(sum(values))}, wins={len(wins)}, "
              f"losses={len(losses)}, profit_factor={sum(wins) / -sum(losses):.3f}")
    compared = old.merge(new, on=["date", "setup"], how="outer", suffixes=("_old", "_new"),
                         indicator=True)
    for row in compared.to_dict("records"):
        if (row["_merge"] != "both" or row["entry_time_old"] != row["entry_time_new"]
                or row["exit_time_old"] != row["exit_time_new"]
                or row["exit_price_old"] != row["exit_price_new"]):
            print("Changed:", row)


class CalendarAndStrategyTests(unittest.TestCase):
    def test_calendar_and_contract_selection(self):
        calendar_checks()

    def test_strategy_boundaries(self):
        basic_checks()



if __name__ == "__main__":
    result = unittest.main(argv=[sys.argv[0]], exit=False).result
    if not result.wasSuccessful():
        sys.exit(1)
    if "--replay" in sys.argv:
        replay_checks()
    if "--compare-cutoff" in sys.argv:
        compare_cutoffs()
    if "--compare-evening" in sys.argv:
        compare_evening()
