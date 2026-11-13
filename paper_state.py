"""Atomic, paper-only position state and durable closed-trade handoff."""

from datetime import timedelta

import pandas as pd
from pymongo.errors import DuplicateKeyError
from tamingnifty.charges import money, silver_micro_estimated_order_charges, sum_charges

import decisions
import paper


STATE_ID = "silver-surfer:paper"


def create_state(states):
    try:
        states.insert_one(dict(_id=STATE_ID, version=0, position=None,
                               pending_trade=None, slot_day=None,
                               morning_used=False, evening_used=False,
                               last_exit_minute=None))
    except DuplicateKeyError:
        pass
    state = states.find_one({"_id": STATE_ID})
    if state is None:
        raise RuntimeError("Paper state could not be read")
    return state


def valid_time(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tz is not None:
        raise ValueError("Paper timestamps must be naive India-local timestamps")
    return stamp


def priced_quote(quote, side, quantity, started_at, received_at):
    start = valid_time(started_at)
    received = valid_time(received_at)
    elapsed = (received - start).total_seconds()
    if not 0 <= elapsed <= 3:
        raise ValueError("Paper quote request exceeded three seconds")
    fill = paper.quote_fill(quote, side, quantity)
    fill["requested_at"] = str(start)
    fill["received_at"] = str(received)
    fill["request_seconds"] = elapsed
    return fill


def open_position(states, state, signal, contract, quote, started_at, received_at):
    received = valid_time(received_at)
    asof = valid_time(signal["data_asof"])
    if received.floor("min") != asof + timedelta(minutes=1):
        raise ValueError("Entry quote arrived outside the signal minute")
    if valid_time(started_at) < asof + timedelta(minutes=1):
        raise ValueError("Entry quote request preceded the completed candle")
    if state["position"] is not None or state["pending_trade"] is not None:
        raise ValueError("A position or unarchived closed trade already exists")
    if signal["session"] not in ("morning", "evening") or signal["side"] not in ("BUY", "SELL"):
        raise ValueError("Invalid paper entry")
    if decisions.entry_decision(signal, received, True) != signal["side"]:
        raise ValueError("Paper entry does not meet the signal rules")
    if str(signal["security_id"]) != str(contract["security_id"]):
        raise ValueError("Signal and paper contract differ")
    quantity = contract["lot_size"]
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity != 1:
        raise ValueError("Paper accounting currently requires a one-unit Silver Micro lot")
    day = str(received.date())
    if state["slot_day"] is not None and state["slot_day"] > day:
        raise ValueError("Paper state is dated after the entry")
    if state["slot_day"] == day and state[signal["session"] + "_used"]:
        raise ValueError("This session already used its paper trade")
    if state["last_exit_minute"] == str(received.floor("min")):
        raise ValueError("A position closed during this minute")
    fill = priced_quote(quote, signal["side"], quantity, started_at, received_at)
    position = dict(id=f"silver:{day}:{signal['session']}", session=signal["session"],
                    side=signal["side"], security_id=str(contract["security_id"]),
                    symbol=contract["symbol"], expiry=contract["expiry"],
                    quantity=quantity, entry_price=fill["price"],
                    entry_timestamp=str(received), entry_quote=fill,
                    entry_signal_asof=str(asof), last_checked_asof=str(asof),
                    profit_active=False, exit_requested=None)
    values = dict(position=position, slot_day=day)
    if state["slot_day"] != day:
        values.update(morning_used=False, evening_used=False)
    values[signal["session"] + "_used"] = True
    result = states.update_one({"_id": STATE_ID, "version": state["version"],
                                "position": None, "pending_trade": None},
                               {"$set": values, "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper entry lost a concurrent state update; no fill recorded")
    return position


def update_activation(states, state, active):
    if state["position"] is None or not isinstance(active, bool):
        raise ValueError("A paper position and boolean activation are required")
    if state["position"]["profit_active"] and not active:
        raise ValueError("Paper profit activation cannot be reset")
    if state["position"]["profit_active"] == active:
        return state
    result = states.update_one({"_id": STATE_ID, "version": state["version"],
                                "position.id": state["position"]["id"]},
                               {"$set": {"position.profit_active": active},
                                "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper activation lost a concurrent state update")
    return states.find_one({"_id": STATE_ID})


def checkpoint_signal(states, state, active, asof):
    position = state["position"]
    if position is None or not isinstance(active, bool):
        raise ValueError("A paper position and boolean activation are required")
    if position["profit_active"] and not active:
        raise ValueError("Paper profit activation cannot be reset")
    current = valid_time(asof)
    if current < valid_time(position["last_checked_asof"]):
        raise ValueError("Cannot move the position checkpoint backwards")
    result = states.update_one({"_id": STATE_ID, "version": state["version"],
                                "position.id": position["id"]},
                               {"$set": {"position.profit_active": active,
                                         "position.last_checked_asof": str(current)},
                                "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper checkpoint lost a concurrent state update")
    return states.find_one({"_id": STATE_ID})


def request_exit(states, state, reason, active):
    position = state["position"]
    if position is None or not isinstance(reason, str) or not reason.strip() or not isinstance(active, bool):
        raise ValueError("A paper position, exit reason and boolean activation are required")
    if position["profit_active"] and not active:
        raise ValueError("Paper profit activation cannot be reset")
    result = states.update_one({"_id": STATE_ID, "version": state["version"],
                                "position.id": position["id"], "position.exit_requested": None},
                               {"$set": {"position.profit_active": active,
                                         "position.exit_requested": reason},
                                "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper exit request lost a concurrent state update")
    return states.find_one({"_id": STATE_ID})


def close_position(states, state, reason, quote, started_at, received_at):
    position = state["position"]
    if position is None or state["pending_trade"] is not None:
        raise ValueError("No paper position available to close")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("Paper exit reason is required")
    received = valid_time(received_at)
    if received < valid_time(position["entry_timestamp"]):
        raise ValueError("Exit quote precedes entry")
    side = "SELL" if position["side"] == "BUY" else "BUY"
    fill = priced_quote(quote, side, position["quantity"], started_at, received_at)
    direction = 1 if position["side"] == "BUY" else -1
    pnl = money(direction * (fill["price"] - position["entry_price"]) * position["quantity"])
    charges = sum_charges(
        silver_micro_estimated_order_charges(position["side"], position["entry_price"], 1),
        silver_micro_estimated_order_charges(side, fill["price"], 1))
    trade = dict(position, _id=position["id"], exit_price=fill["price"],
                 exit_timestamp=str(received), exit_quote=fill, exit_reason=reason,
                 pnl=pnl, charges=charges, net_pnl=money(pnl - charges["total"]))
    result = states.update_one({"_id": STATE_ID, "version": state["version"],
                                "position.id": position["id"], "pending_trade": None},
                               {"$set": {"position": None, "pending_trade": trade,
                                         "last_exit_minute": str(received.floor("min"))},
                                "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper exit lost a concurrent state update; no fill recorded")
    return trade


def archive_trade(states, trades, state):
    trade = state["pending_trade"]
    if trade is None:
        return
    try:
        trades.insert_one(trade.copy())
    except DuplicateKeyError:
        if trades.find_one({"_id": trade["_id"]}) != trade:
            raise RuntimeError("Archived paper trade differs from pending trade")
    result = states.update_one({"_id": STATE_ID, "version": state["version"],
                                "pending_trade._id": trade["_id"]},
                               {"$set": {"pending_trade": None}, "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper archive handoff lost a concurrent state update")
