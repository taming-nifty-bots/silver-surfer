"""Silver Surfer executor. Paper mode fills synchronously from depth; live mode
places real Dhan MCX orders through the same pending/active/closing state
machine used by Nifty Interceptor, once live_trading=true."""

import os
import time
import math
import hashlib
from datetime import datetime, time as clock_time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from slack_sdk import WebClient
from tamingnifty import connect_dhan as dhan
from tamingnifty import utils
from tamingnifty.charges import money, silver_micro_estimated_order_charges, sum_charges

import strategy

# A live trade occupies the single-position slot from the moment its order is
# submitted (pending) until its exit is confirmed (closing finishes to closed).
ACTIVE_STATES = ("pending", "active", "closing")


def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)


def notify(slack, message):
    try:
        utils.notify(message="Silver Surfer paper | " + message,
                     slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"),
                     slack_client=slack)
    except Exception as error:
        print("Slack notification failed: " + utils.exception_detail(error), flush=True)


def quote_fill(quote, side, quantity):
    if side not in ("BUY", "SELL"):
        raise ValueError("Paper order side must be BUY or SELL")
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        raise ValueError("Paper order quantity must be a positive whole number")
    book = {}
    for kind in ("buy", "sell"):
        levels = []
        for row in quote["depth"][kind]:
            price = float(row["price"])
            size = float(row["quantity"])
            if not math.isfinite(price) or price <= 0:
                raise ValueError("Invalid market depth price")
            if not math.isfinite(size) or size < 0 or int(size) != size:
                raise ValueError("Invalid market depth quantity")
            if size > 0:
                levels.append((price, int(size)))
        if not levels:
            raise ValueError("Missing executable bid or ask")
        levels.sort(reverse=kind == "buy")
        book[kind] = levels[0]
    bid, bid_size = book["buy"]
    ask, ask_size = book["sell"]
    if bid > ask:
        raise ValueError("Crossed market depth")
    price = ask if side == "BUY" else bid
    available = ask_size if side == "BUY" else bid_size
    return dict(price=price, bid=bid, ask=ask, bid_size=bid_size,
                ask_size=ask_size, spread=round(ask - bid, 2),
                insufficient_best_size=available < quantity)


def initialize_store(trades):
    # Do not silently abandon a legacy singleton's open position or pending close.
    for trade in trades.find({}):
        state = trade.get("strategy_state")
        if (state not in ("pending", "active", "closing", "aborted", "closed")
                or not all(key in trade for key in ("_id", "session", "entry_date", "version"))
                or ("id" in trade and trade["_id"] != trade["id"])
                or trade["session"] not in ("morning", "evening")):
            raise RuntimeError("Legacy or invalid paper data; reconcile and migrate before startup")
        if trade["_id"] != f"silver:{trade['entry_date']}:{trade['session']}":
            raise RuntimeError("Invalid paper trade ID; reconcile before startup")
        if state in ("active", "closing", "closed") and not all(
                key in trade for key in ("entry_timestamp", "entry_price", "quantity")):
            raise RuntimeError("Legacy or invalid paper data; reconcile and migrate before startup")
        if state == "closed":
            if valid_time(trade["exit_timestamp"]) < valid_time(trade["entry_timestamp"]):
                raise RuntimeError("Invalid paper exit time; reconcile before startup")
        if state == "closed" and "id" in trade:
            result = trades.update_one({"_id": trade["_id"], "id": trade["_id"],
                                        "version": trade["version"], "strategy_state": "closed"},
                                       {"$unset": {"id": ""}})
            if result.modified_count != 1:
                raise RuntimeError("Could not remove duplicate ID from closed paper trade")
    trades.create_index([("strategy_state", 1)], name="silver_one_active_trade",
                        unique=True, partialFilterExpression={"strategy_state": {"$in": list(ACTIVE_STATES)}})


def create_state(trades):
    """Build a transient executor view; never persist a singleton or lock."""
    state = dict(position=None, slot_day=None, morning_used=False,
                 evening_used=False, last_exit_minute=None)
    state["position"] = trades.find_one({"strategy_state": {"$in": list(ACTIVE_STATES)}})
    latest = trades.find_one({}, sort=[("_id", -1)])
    if latest is not None:
        day = latest["entry_date"]
        state["slot_day"] = day
        for session in ("morning", "evening"):
            trade = trades.find_one({"_id": f"silver:{day}:{session}"})
            if trade is None:
                continue
            state[session + "_used"] = True
            if trade["strategy_state"] == "closed":
                minute = str(valid_time(trade["exit_timestamp"]).floor("min"))
                if state["last_exit_minute"] is None or minute > state["last_exit_minute"]:
                    state["last_exit_minute"] = minute
    return state


def valid_time(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tz is not None:
        raise ValueError("Paper timestamps must be naive India-local timestamps")
    return stamp


def valid_price(price):
    value = float(price)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("Expected a finite positive market price")
    return value


def priced_quote(quote, side, quantity, started_at, received_at):
    start = valid_time(started_at)
    received = valid_time(received_at)
    elapsed = (received - start).total_seconds()
    if not 0 <= elapsed <= 3:
        raise ValueError("Paper quote request exceeded three seconds")
    fill = quote_fill(quote, side, quantity)
    fill["requested_at"] = str(start)
    fill["received_at"] = str(received)
    fill["request_seconds"] = elapsed
    return fill


def validate_quantity(quantity, lot_size):
    if isinstance(quantity, bool) or isinstance(lot_size, bool):
        raise ValueError("quantity and lot size must be positive whole numbers")
    try:
        qty = int(quantity)
        lot = int(lot_size)
        if qty != float(quantity) or lot != float(lot_size):
            raise ValueError("quantity and lot size must be positive whole numbers")
    except (TypeError, ValueError, OverflowError):
        raise ValueError("quantity and lot size must be positive whole numbers")
    if qty <= 0 or lot <= 0 or qty % lot:
        raise ValueError(f"quantity must be a positive multiple of contract lot size {lot}")
    return qty


def open_position(trades, state, signal, contract, quantity, quote, started_at, received_at):
    received = valid_time(received_at)
    asof = valid_time(signal["data_asof"])
    if received.floor("min") != asof + timedelta(minutes=1):
        raise ValueError("Entry quote arrived outside the signal minute")
    if valid_time(started_at) < asof + timedelta(minutes=1):
        raise ValueError("Entry quote request preceded the completed candle")
    if state["position"] is not None:
        raise ValueError("A position already exists")
    if signal["session"] not in ("morning", "evening") or signal["side"] not in ("BUY", "SELL"):
        raise ValueError("Invalid paper entry")
    if strategy.entry_decision(signal, received, True) != signal["side"]:
        raise ValueError("Paper entry does not meet the signal rules")
    if str(signal["security_id"]) != str(contract["security_id"]):
        raise ValueError("Signal and paper contract differ")
    if contract["lot_size"] != 1:
        raise ValueError("Silver Micro paper accounting requires a one-unit contract lot size")
    quantity = validate_quantity(quantity, contract["lot_size"])
    day = str(received.date())
    if state["slot_day"] is not None and state["slot_day"] > day:
        raise ValueError("Paper state is dated after the entry")
    if state["slot_day"] == day and state[signal["session"] + "_used"]:
        raise ValueError("This session already used its paper trade")
    if state["last_exit_minute"] == str(received.floor("min")):
        raise ValueError("A position closed during this minute")
    fill = priced_quote(quote, signal["side"], quantity, started_at, received_at)
    position = dict(_id=f"silver:{day}:{signal['session']}",
                    strategy_version=strategy.VERSION, instrument_name="SILVERMIC",
                    broker="DHAN", simulated=True, session=signal["session"],
                    side=signal["side"], security_id=str(contract["security_id"]),
                    symbol=contract["symbol"], expiry=contract["expiry"],
                    lot_size=contract["lot_size"], quantity=quantity, requested_quantity=quantity,
                    entry_price=fill["price"], entry_date=day,
                    entry_time=received.strftime("%H:%M:%S"),
                    entry_timestamp=str(received), entry_quote=fill,
                    entry_signal=str(asof), signal_doc_id=f"silver:{signal['session']}",
                    last_checked_asof=str(asof),
                    strategy_state="active", version=0,
                    pnl=0.0, net_pnl=None, running_pnl=0.0,
                    max_pnl_reached=0.0, min_pnl_reached=0.0,
                    last_mark=str(received), pnl_tracking_since=str(received),
                    charges=sum_charges(silver_micro_estimated_order_charges(
                        signal["side"], fill["price"], quantity)))
    try:
        trades.insert_one(position.copy())
    except DuplicateKeyError as error:
        raise RuntimeError("Paper entry conflicts with an active trade or used session; no fill recorded") from error
    return position


def mark_position(trades, position, price, marked_at):
    price = float(price)
    if not math.isfinite(price) or price <= 0:
        raise ValueError("Invalid commodity LTP for running P&L")
    marked = valid_time(marked_at)
    if marked < valid_time(position["entry_timestamp"]):
        raise ValueError("P&L mark precedes entry")
    direction = 1 if position["side"] == "BUY" else -1
    gross = money(direction * (price - position["entry_price"]) * position["quantity"])
    values = {"running_pnl": gross, "last_mark": str(marked)}
    if "pnl_tracking_since" not in position:
        values["pnl_tracking_since"] = str(marked)
    result = trades.update_one(
        {"_id": position["_id"], "version": position["version"], "strategy_state": "active"},
        {"$set": values, "$max": {"max_pnl_reached": gross},
         "$min": {"min_pnl_reached": gross}, "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper P&L mark lost a concurrent trade update")


def checkpoint_signal(trades, state, asof):
    position = state["position"]
    if position is None:
        raise ValueError("A paper position is required")
    current = valid_time(asof)
    if current < valid_time(position["last_checked_asof"]):
        raise ValueError("Cannot move the position checkpoint backwards")
    result = trades.update_one({"_id": position["_id"], "version": position["version"],
                                "strategy_state": "active"},
                               {"$set": {"last_checked_asof": str(current)},
                                "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Paper checkpoint lost a concurrent state update")
    return dict(state, position=dict(position, last_checked_asof=str(current),
                                     version=position["version"] + 1))


def close_position(trades, state, reason, quote, started_at, received_at):
    position = state["position"]
    if position is None:
        raise ValueError("No paper position available to close")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("Paper exit reason is required")
    received = valid_time(received_at)
    if valid_time(started_at) < valid_time(position["entry_timestamp"]):
        raise ValueError("Exit quote precedes entry")
    side = "SELL" if position["side"] == "BUY" else "BUY"
    fill = priced_quote(quote, side, position["quantity"], started_at, received_at)
    direction = 1 if position["side"] == "BUY" else -1
    pnl = money(direction * (fill["price"] - position["entry_price"]) * position["quantity"])
    charges = sum_charges(
        silver_micro_estimated_order_charges(position["side"], position["entry_price"], position["quantity"]),
        silver_micro_estimated_order_charges(side, fill["price"], position["quantity"]))
    trade = dict(position, strategy_state="closed", version=position["version"] + 1,
                 exit_price=fill["price"],
                 exit_date=str(received.date()), exit_time=received.strftime("%H:%M:%S"),
                 exit_timestamp=str(received), exit_quote=fill, exit_reason=reason,
                 pnl=pnl, running_pnl=pnl, last_mark=str(received),
                 max_pnl_reached=max(position.get("max_pnl_reached", pnl), pnl),
                 min_pnl_reached=min(position.get("min_pnl_reached", pnl), pnl),
                 charges=charges, net_pnl=money(pnl - charges["total"]))
    trade.pop("id", None)
    values = dict(trade)
    del values["_id"]
    result = trades.update_one({"_id": position["_id"], "version": position["version"],
                                "strategy_state": "active"},
                               {"$set": values, "$unset": {"id": ""}})
    if result.modified_count != 1:
        raise RuntimeError("Paper exit lost a concurrent state update; no fill recorded")
    return trade


def live_order_key(trade_id_value, leg):
    return f"{trade_id_value}:{leg}"


def execute_live_order(conn, orders, key, security_id, symbol, side, quantity, role, now):
    """Submit (or recover) one real Dhan MCX order and chase it to a final state.

    Mirrors Nifty Interceptor's correlation-ID pattern: an order intent is saved
    before submission so a crash between sending and recording the response is
    recoverable, and a filled/partially-filled order is never resubmitted.
    Interceptor only ever BUYs an entry, so it decides "any final fill is an
    acceptable outcome, even zero" by side. Silver trades both directions, so
    that same leniency is keyed on role ("ENTRY" vs "EXIT") instead: an EXIT
    always requires a full confirmed fill, an ENTRY accepts whatever the
    broker finally reports (0 is aborted, partial is flattened by the caller).
    tamingnifty's BUY-only entry_deadline/price-cap escalation is skipped
    entirely here for the same reason it can't be used symmetrically for a
    SELL entry; both legs use best_price_first depth repricing instead, the
    same policy Interceptor already relies on for its own SELL-side exits.
    """
    existing = orders.find_one({"_id": key})
    if existing is None:
        correlation = hashlib.sha256(key.encode("utf-8")).hexdigest()[:30]
        prepared = dhan.prepare_order(conn, security_id, side, quantity, "MCX_COMM", "MARGIN",
                                      correlation_id=correlation, best_price_first=True,
                                      check_spread=False)
        intent = dict(_id=key, orderStatus="SUBMITTING", transactionType=side,
                      securityId=str(security_id), tradingSymbol=symbol, quantity=quantity,
                      simulated=False, strategy_version=strategy.VERSION,
                      prepared=prepared, correlationId=correlation)
        try:
            result = orders.insert_one(intent)
        except DuplicateKeyError:
            existing = orders.find_one({"_id": key})
        else:
            if not result.acknowledged:
                raise RuntimeError("Order intent was not acknowledged; no order submitted")
            # Never resubmit after an ambiguous response or a crash after sending.
            placed = dhan.submit_prepared_order(conn, prepared)
            if not placed.get("orderId"):
                raise ValueError(f"Order response has no ID; reconcile intent {key} with Dhan")
            orders.update_one({"_id": key}, {"$set": placed})
            existing = orders.find_one({"_id": key})
    if not existing or existing.get("simulated") is not False:
        raise ValueError(f"Unconfirmed order submission {key}; reconcile with Dhan before retrying")
    if not existing.get("orderId"):
        if not existing.get("correlationId"):
            raise ValueError(f"Legacy unconfirmed order {key}; reconcile with Dhan")
        recovered = dhan.order_by_correlation(conn, existing["correlationId"])
        orders.update_one({"_id": key}, {"$set": recovered})
        existing = orders.find_one({"_id": key})
    if "prepared" not in existing:
        raise ValueError(f"Legacy order {key} has no saved execution policy; reconcile with Dhan")
    if existing.get("orderStatus") == "TRADED" and existing.get("fill_timestamp"):
        order = existing
    else:
        order = dhan.wait_for_fill(
            conn, existing["orderId"], slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"),
            prepared=existing["prepared"],
            audit_collection=orders.database[orders.name.replace("orders_", "order_quotes_", 1)],
            audit_id=key)
        if str(order.get("orderId")) != str(existing["orderId"]):
            raise ValueError(f"Broker returned a different order for {existing['orderId']}; reconcile with Dhan")
        if order.get("filledQty", 0) < existing.get("filledQty", 0):
            raise ValueError(f"Order {existing['orderId']} filled quantity decreased; reconcile with Dhan")
        if order.get("filledQty", 0) > 0:
            order["fill_timestamp"] = str(now)
        orders.update_one({"_id": key}, {"$set": order})
        order = orders.find_one({"_id": key})
    if (str(order.get("securityId")) != str(security_id)
            or order.get("transactionType") != side or order.get("quantity") != quantity):
        raise ValueError(f"Order {order['orderId']} contract/side/quantity mismatch; reconcile with Dhan")
    filled = order.get("filledQty")
    if (not isinstance(filled, int) or not 0 <= filled <= quantity
            or (order.get("orderStatus") == "TRADED" and filled != quantity)):
        raise ValueError(f"Order {order['orderId']} has inconsistent fills; reconcile with Dhan")
    if role == "ENTRY" and order.get("orderStatus") in dhan.FINAL_ORDER_STATUSES:
        if order.get("filledQty", 0) > 0:
            valid_price(order["averageTradedPrice"])
        return order
    if order.get("orderStatus") != "TRADED" or order.get("filledQty") != quantity:
        raise ValueError(
            f"Order {order['orderId']} {order.get('orderStatus')}: "
            f"filled {order.get('filledQty', 0)}/{quantity}; "
            f"{order.get('omsErrorDescription', '')}. Reconcile with Dhan; no duplicate order will be sent")
    valid_price(order["averageTradedPrice"])
    return order


def claim_live_entry(trades, state, signal, contract, quantity, now):
    """Reserve the session trade slot before any broker order is sent."""
    current = valid_time(now)
    asof = valid_time(signal["data_asof"])
    if current.floor("min") != asof + timedelta(minutes=1):
        raise ValueError("Live entry claim arrived outside the signal minute")
    if state["position"] is not None:
        raise ValueError("A position already exists")
    if signal["session"] not in ("morning", "evening") or signal["side"] not in ("BUY", "SELL"):
        raise ValueError("Invalid live entry")
    if strategy.entry_decision(signal, current, True) != signal["side"]:
        raise ValueError("Live entry does not meet the signal rules")
    if str(signal["security_id"]) != str(contract["security_id"]):
        raise ValueError("Signal and live contract differ")
    if contract["lot_size"] != 1:
        raise ValueError("Silver Micro live accounting requires a one-unit contract lot size")
    quantity = validate_quantity(quantity, contract["lot_size"])
    day = str(current.date())
    if state["slot_day"] is not None and state["slot_day"] > day:
        raise ValueError("Live state is dated after the entry")
    if state["slot_day"] == day and state[signal["session"] + "_used"]:
        raise ValueError("This session already used its trade")
    if state["last_exit_minute"] == str(current.floor("min")):
        raise ValueError("A position closed during this minute")
    trade = dict(_id=f"silver:{day}:{signal['session']}", strategy_version=strategy.VERSION,
                instrument_name="SILVERMIC", broker="DHAN", simulated=False,
                session=signal["session"], side=signal["side"],
                security_id=str(contract["security_id"]), symbol=contract["symbol"],
                expiry=contract["expiry"], lot_size=contract["lot_size"],
                quantity=quantity, requested_quantity=quantity,
                entry_date=day, entry_signal=str(asof), signal_doc_id=f"silver:{signal['session']}",
                strategy_state="pending", version=0)
    try:
        trades.insert_one(trade.copy())
    except DuplicateKeyError as error:
        raise RuntimeError("Live entry conflicts with an active trade or used session; no order sent") from error
    return trade


def confirm_live_entry(conn, trades, orders, trade, now):
    key = live_order_key(trade["_id"], "ENTRY")
    order = execute_live_order(conn, orders, key, trade["security_id"], trade["symbol"],
                               trade["side"], trade["quantity"], "ENTRY", now)
    filled = order["filledQty"]
    if filled == 0:
        trades.update_one(
            {"_id": trade["_id"], "version": trade["version"], "strategy_state": "pending"},
            {"$set": dict(strategy_state="aborted",
                          exit_reason=f"Entry {order['orderStatus']} without fills: "
                          f"{order.get('omsErrorDescription', '')}; session slot consumed"),
             "$inc": {"version": 1}})
        return None
    price = valid_price(order["averageTradedPrice"])
    filled_at = order.get("fill_timestamp", order.get("createTime"))
    if filled_at is None:
        raise ValueError("Filled entry order is missing its confirmation timestamp")
    received = valid_time(filled_at)
    charges = sum_charges(silver_micro_estimated_order_charges(trade["side"], price, filled))
    updates = dict(strategy_state="active", quantity=filled, requested_quantity=trade["quantity"],
                   entry_price=price, entry_timestamp=str(received),
                   entry_time=received.strftime("%H:%M:%S"), entry_order_id=key,
                   last_checked_asof=trade["entry_signal"],
                   pnl=0.0, net_pnl=None, running_pnl=0.0,
                   max_pnl_reached=0.0, min_pnl_reached=0.0,
                   last_mark=str(received), pnl_tracking_since=str(received),
                   charges=charges)
    if filled != trade["quantity"]:
        updates.update(strategy_state="closing",
                       exit_reason="Cancelled partial entry; flatten actual filled quantity")
    result = trades.update_one(
        {"_id": trade["_id"], "version": trade["version"], "strategy_state": "pending"},
        {"$set": updates, "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Live entry confirmation lost a concurrent state update")
    return trades.find_one({"_id": trade["_id"]})


def begin_live_exit(trades, state, reason, now):
    position = state["position"]
    if position is None:
        raise ValueError("No live position available to close")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("Live exit reason is required")
    if position["strategy_state"] != "active":
        return position
    result = trades.update_one(
        {"_id": position["_id"], "version": position["version"], "strategy_state": "active"},
        {"$set": dict(exit_reason=reason, strategy_state="closing"), "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Live exit request lost a concurrent state update")
    return trades.find_one({"_id": position["_id"]})


def confirm_live_exit(conn, trades, orders, trade, now):
    side = "SELL" if trade["side"] == "BUY" else "BUY"
    key = live_order_key(trade["_id"], "EXIT")
    order = execute_live_order(conn, orders, key, trade["security_id"], trade["symbol"],
                               side, trade["quantity"], "EXIT", now)
    price = valid_price(order["averageTradedPrice"])
    filled_at = order.get("fill_timestamp", order.get("createTime"))
    if filled_at is None:
        raise ValueError("Filled exit order is missing its confirmation timestamp")
    received = valid_time(filled_at)
    direction = 1 if trade["side"] == "BUY" else -1
    pnl = money(direction * (price - trade["entry_price"]) * trade["quantity"])
    charges = sum_charges(
        silver_micro_estimated_order_charges(trade["side"], trade["entry_price"], trade["quantity"]),
        silver_micro_estimated_order_charges(side, price, trade["quantity"]))
    updates = dict(strategy_state="closed", exit_price=price, exit_date=str(received.date()),
                   exit_time=received.strftime("%H:%M:%S"), exit_timestamp=str(received),
                   exit_order_id=key, pnl=pnl, running_pnl=pnl, last_mark=str(received),
                   max_pnl_reached=max(trade.get("max_pnl_reached", pnl), pnl),
                   min_pnl_reached=min(trade.get("min_pnl_reached", pnl), pnl),
                   charges=charges, net_pnl=money(pnl - charges["total"]))
    result = trades.update_one(
        {"_id": trade["_id"], "version": trade["version"], "strategy_state": "closing"},
        {"$set": updates, "$inc": {"version": 1}})
    if result.modified_count != 1:
        raise RuntimeError("Live exit confirmation lost a concurrent state update")
    return trades.find_one({"_id": trade["_id"]})


def checked_signal(signals, day, session, now):
    doc = signals.find_one({"_id": f"silver:{session}"})
    if doc is None or doc.get("strategy_version") != strategy.VERSION:
        if now.second < 15:
            return None
        raise ValueError("Missing or incompatible Silver signal")
    candle = pd.Timestamp(doc["data_asof"])
    if candle + pd.Timedelta(minutes=1) != pd.Timestamp(now).floor("min"):
        if now.second < 15:
            return None
        raise ValueError("Silver signal is stale or future-dated")
    return doc


def session_finished(state, session, now):
    if state is None:
        return False
    position = state["position"]
    if position is not None:
        if position["strategy_state"] != "active":
            return False
        return (session == "morning" and position["session"] == "evening"
                and now.date() == pd.Timestamp(position["entry_timestamp"]).date()
                and now.time() >= clock_time(17))
    if state["slot_day"] == str(now.date()) and state[session + "_used"]:
        return True
    if session == "evening":
        return now.time() > strategy.EVENING_LAST_ENTRY
    return now.time() >= clock_time(16, 55) and now.time() < clock_time(23)


def run_cycle(trades, signals, calendars, quote_source, candles_source, clock, alert, quantity,
              session=None, started_at=None, simulated=True, orders=None, conn_source=None):
    if not simulated and (orders is None or conn_source is None):
        raise ValueError("Live execution requires an orders collection and a connection source")
    state = create_state(trades)
    now = clock()
    position = state["position"]
    if position is not None:
        if session is not None and position["session"] != session:
            if (position["session"] == "morning" and session == "evening" and started_at is not None
                    and now < max(started_at, now.replace(hour=17, minute=0, second=0, microsecond=0))
                    + timedelta(minutes=1)):
                return
            if (position["session"] == "evening" and session == "morning"
                    and now.date() == pd.Timestamp(position["entry_date"]).date()):
                return
        if position["strategy_state"] in ("pending", "closing"):
            if simulated:
                raise RuntimeError("Paper trading must not leave a pending/closing position")
            conn = conn_source()
            if position["strategy_state"] == "pending":
                confirmed = confirm_live_entry(conn, trades, orders, position, now)
                if confirmed is None:
                    alert(f"Entry {position['_id']} did not fill; session slot consumed")
                    return
                alert(f"Entry {confirmed['_id']} {confirmed['side']} Qty {confirmed['quantity']} "
                      f"at {confirmed['entry_price']} (live)")
                if confirmed["strategy_state"] != "closing":
                    return
                position = confirmed
                alert(f"Partial live fill; flattening {position['_id']} to quantity {position['quantity']}")
            closed = confirm_live_exit(conn, trades, orders, position, now)
            alert(f"Exit {closed['_id']} at {closed['exit_price']}; estimated net {closed['net_pnl']}")
            return
        cutoff = strategy.exit_decision(dict(position, profit_active=False), None, now) if (
            now.date() > pd.Timestamp(position["entry_timestamp"]).date()
            or now.time() >= (datetime.strptime("16:55", "%H:%M").time()
                              if position["session"] == "morning"
                              else strategy.EVENING_EXIT)) else None
        if cutoff is not None:
            if simulated:
                quote, started, received = quote_source(position["security_id"])
                trade = close_position(trades, state, cutoff["reason"], quote, started, received)
                alert(f"Forced exit {trade['_id']} at {trade['exit_price']}; "
                      f"estimated net {trade['net_pnl']}")
                if trade["exit_quote"]["insufficient_best_size"]:
                    alert("Best-level size was insufficient; full paper exit is optimistic")
            else:
                begin_live_exit(trades, state, cutoff["reason"], now)
                alert(f"Forced exit requested for {position['_id']}; submitting live order")
            return
        candles = None
        try:
            doc = checked_signal(signals, str(now.date()), position["session"], now)
        except ValueError as error:
            alert(f"Signal unavailable ({error}); reconstructing it from observed candles.")
            candles = candles_source(position)
            doc = strategy.build_signal(candles, now.date(), now, position["session"])
            doc["security_id"] = position["security_id"]
        if doc is None:
            return
        checked = pd.Timestamp(position["last_checked_asof"])
        asof = pd.Timestamp(doc["data_asof"])
        if asof <= checked:
            return
        if candles is None:
            candles = candles_source(position)
        current = clock()
        if asof > checked + pd.Timedelta(minutes=1):
            alert(f"Position {position['_id']} missed signal checks after {checked}; "
                  "reconstructing observed candles; any missed exit uses a current quote.")
        expected = pd.date_range(pd.Timestamp(position["entry_timestamp"]).floor("min"),
                                 asof, freq="min")
        observed = set(pd.to_datetime(candles["datetime"]))
        missing = [str(stamp) for stamp in expected if stamp not in observed]
        if missing:
            alert(f"Position {position['_id']} has {len(missing)} missing completed candles; "
                  "activation and missed exits cannot be fully recovered.")
        if asof > checked + pd.Timedelta(minutes=1):
            result = strategy.recover_missed_exit(position, candles, asof)
            if result["reason"] is not None:
                alert(f"Recovered {result['reason']} exit signal from {result['data_asof']}; "
                      "using the current quote rather than a historical fill.")
        else:
            active = strategy.recover_activation(position, candles, current)
            result = strategy.exit_decision(dict(position, profit_active=active), doc, current)
        current = clock()
        if current.date() > pd.Timestamp(position["entry_timestamp"]).date() or (
                position["session"] == "morning" and current.time() >= clock_time(16, 55)) or (
                position["session"] == "evening" and current.time() >= strategy.EVENING_EXIT):
            result["reason"] = result["reason"] or "session cutoff"
        if result["reason"] is not None:
            if simulated:
                quote, started, received = quote_source(position["security_id"])
                trade = close_position(trades, state, result["reason"], quote, started, received)
                alert(f"Exit {trade['_id']} at {trade['exit_price']}; estimated net {trade['net_pnl']}")
                if trade["exit_quote"]["insufficient_best_size"]:
                    alert("Best-level size was insufficient; full paper exit is optimistic")
            else:
                begin_live_exit(trades, state, result["reason"], now)
                alert(f"Exit requested for {position['_id']}; submitting live order")
        else:
            checkpoint_signal(trades, state, asof)
        return

    status = strategy.session_status(calendars, now.date())
    session = session or ("morning" if now.hour < 17 else "evening")
    if not status[session] or not 9 <= now.hour < 23:
        return
    if session == "morning" and now.time() >= clock_time(16, 55):
        return
    if session == "evening" and now.time() < clock_time(17):
        return
    if session == "evening" and now.time() > strategy.EVENING_LAST_ENTRY:
        return
    if state["slot_day"] == str(now.date()) and state[session + "_used"]:
        return
    doc = checked_signal(signals, str(now.date()), session, now)
    if doc is None:
        return
    side = strategy.entry_decision(doc, now, status[session])
    if side is None:
        return
    validate_quantity(quantity, doc["contract"]["lot_size"])
    if simulated:
        quote, started, received = quote_source(doc["security_id"])
        position = open_position(trades, state, doc, doc["contract"], quantity,
                                             quote, started, received)
        alert(f"Entry {position['_id']} {side} Qty {position['quantity']} at {position['entry_price']}; "
              f"spread {position['entry_quote']['spread']}")
        if position["entry_quote"]["insufficient_best_size"]:
            alert("Best-level size was insufficient; full paper fill is optimistic")
    else:
        trade = claim_live_entry(trades, state, doc, doc["contract"], quantity, now)
        alert(f"Entry claimed {trade['_id']} {side} Qty {trade['quantity']}; submitting live order")


def main():
    load_dotenv(find_dotenv())
    for key in ("CONNECTION_STRING", "user_name", "slack_token", "DHAN_CLIENT_ID",
                "DHAN_PIN", "DHAN_TOTP"):
        if not os.environ.get(key, "").strip():
            raise ValueError("Missing environment variable: " + key)
    mode = os.environ.get("live_trading", "false").strip().lower()
    if mode not in ("true", "false"):
        raise ValueError("live_trading must be true or false")
    simulated = mode == "false"
    if not simulated and not hasattr(dhan, "prepare_order"):
        raise RuntimeError("Live execution requires tamingnifty's depth-order API")
    session = os.environ.get("SILVER_SESSION", "").strip().lower()
    if session not in ("morning", "evening"):
        raise ValueError("SILVER_SESSION must be morning or evening")
    quantity = os.environ.get("quantity")
    if quantity is None:
        raise ValueError("Missing quantity; configure the executor job explicitly")
    quantity = validate_quantity(quantity, 1)
    slack = WebClient(token=os.environ["slack_token"])
    client = MongoClient(os.environ["CONNECTION_STRING"])
    db = client["Bots"]
    user = os.environ["user_name"]
    trades = db[f"silver_surfer_{user}"].with_options(
        write_concern=WriteConcern("majority"), read_concern=ReadConcern("majority"))
    orders = db[f"orders_silver_surfer_{user}"]
    signals = db["supertrend"]
    calendars = db["exchange_calendars"]

    def quote_source(security_id):
        conn = dhan.login_to_dhan(slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"))
        started = now_ist()
        quote = dhan.get_depth(conn, security_id, "MCX_COMM")
        return quote, started, now_ist()

    def candles_source(position):
        conn = dhan.login_to_dhan(slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"))
        now = now_ist()
        start = pd.Timestamp(position["entry_timestamp"]).to_pydatetime().replace(
            hour=0, minute=0, second=0, microsecond=0) - timedelta(days=21)
        return dhan.fetch_candles(conn, position["security_id"], "MCX_COMM", "FUTCOM", start, now)

    def mark_source(security_id):
        conn = dhan.login_to_dhan(slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"))
        price = dhan.get_commodity_ltp(conn, security_id)
        return price, now_ist()

    def conn_source():
        return dhan.login_to_dhan(slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"))

    def alert(message):
        notify(slack, message)

    started_at = now_ist()
    waiting_alert = False
    last_error = None
    last_alert = now_ist()
    try:
        initialize_store(trades)
        mode_text = "paper" if simulated else "live"
        notify(slack, f"Started {session} in {mode_text} mode; Qty {quantity}"
               + ("; live orders disabled" if simulated else "; LIVE ORDERS ENABLED"))
        while True:
            now = now_ist()
            try:
                state = create_state(trades)
                if (session == "evening" and now.time() >= clock_time(17) and state is not None
                        and state["position"] is not None
                        and state["position"]["session"] == "morning" and not waiting_alert):
                    alert("Earlier morning position remains open; waiting one minute before recovery.")
                    waiting_alert = True
                run_cycle(trades, signals, calendars, quote_source, candles_source, now_ist, alert, quantity,
                          session=session, started_at=started_at, simulated=simulated,
                          orders=orders, conn_source=conn_source)
                if last_error is not None:
                    alert("Executor recovered")
                    last_error = None
                state = create_state(trades)
                if (state["position"] is not None
                        and state["position"]["strategy_state"] == "active"):
                    price, marked_at = mark_source(state["position"]["security_id"])
                    mark_position(trades, state["position"], price, marked_at)
                if session_finished(state, session, now_ist()):
                    break
                if (state["position"] is None
                        and not strategy.session_status(calendars, now.date())[session]):
                    break
            except Exception as error:
                message = utils.exception_detail(error)
                print(message, flush=True)
                if message != last_error or now - last_alert >= timedelta(minutes=15):
                    alert("ERROR: " + message)
                    last_error, last_alert = message, now
            delay = 5
            if (not simulated and state is not None and state["position"] is not None
                    and state["position"]["strategy_state"] in ("pending", "closing")):
                delay = 2
            time.sleep(delay)
    except Exception as error:
        alert("STARTUP/FATAL ERROR: " + utils.exception_detail(error))
        raise
    finally:
        client.close()


if __name__ == "__main__":
    main()
