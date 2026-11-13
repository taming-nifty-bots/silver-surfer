"""Silver Surfer paper executor. Live broker orders are deliberately disabled."""

import os
import time
import math
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
        if (trade.get("strategy_state") not in ("active", "closed")
                or not all(key in trade for key in
                           ("_id", "session", "entry_timestamp", "entry_price", "quantity",
                            "version"))
                or ("id" in trade and trade["_id"] != trade["id"])
                or trade["session"] not in ("morning", "evening")):
            raise RuntimeError("Legacy or invalid paper data; reconcile and migrate before startup")
        day = str(valid_time(trade["entry_timestamp"]).date())
        if trade["_id"] != f"silver:{day}:{trade['session']}":
            raise RuntimeError("Invalid paper trade ID; reconcile before startup")
        if trade["strategy_state"] == "closed":
            if valid_time(trade["exit_timestamp"]) < valid_time(trade["entry_timestamp"]):
                raise RuntimeError("Invalid paper exit time; reconcile before startup")
        if trade["strategy_state"] == "closed" and "id" in trade:
            result = trades.update_one({"_id": trade["_id"], "id": trade["_id"],
                                        "version": trade["version"], "strategy_state": "closed"},
                                       {"$unset": {"id": ""}})
            if result.modified_count != 1:
                raise RuntimeError("Could not remove duplicate ID from closed paper trade")
    trades.create_index([("strategy_state", 1)], name="silver_one_active_trade",
                        unique=True, partialFilterExpression={"strategy_state": "active"})


def create_state(trades):
    """Build a transient executor view; never persist a singleton or lock."""
    state = dict(position=None, slot_day=None, morning_used=False,
                 evening_used=False, last_exit_minute=None)
    state["position"] = trades.find_one({"strategy_state": "active"})
    latest = trades.find_one({}, sort=[("_id", -1)])
    if latest is not None:
        day = str(valid_time(latest["entry_timestamp"]).date())
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
        return (session == "morning" and position["session"] == "evening"
                and now.date() == pd.Timestamp(position["entry_timestamp"]).date()
                and now.time() >= clock_time(17))
    if state["slot_day"] == str(now.date()) and state[session + "_used"]:
        return True
    if session == "evening":
        return now.time() > strategy.EVENING_LAST_ENTRY
    return now.time() >= clock_time(16, 55) and now.time() < clock_time(23)


def run_cycle(trades, signals, calendars, quote_source, candles_source, clock, alert, quantity,
              session=None, started_at=None):
    state = create_state(trades)
    now = clock()
    position = state["position"]
    if position is not None:
        if session is not None and position["session"] != session:
            if (position["session"] == "morning" and session == "evening" and started_at is not None
                    and now < max(started_at, now.replace(hour=17, minute=0, second=0, microsecond=0))
                    + timedelta(minutes=1)):
                return
            if position["session"] == "evening" and session == "morning" and now.date() == pd.Timestamp(position["entry_timestamp"]).date():
                return
        cutoff = strategy.exit_decision(dict(position, profit_active=False), None, now) if (
            now.date() > pd.Timestamp(position["entry_timestamp"]).date()
            or now.time() >= (datetime.strptime("16:55", "%H:%M").time()
                              if position["session"] == "morning"
                              else strategy.EVENING_EXIT)) else None
        if cutoff is not None:
            quote, started, received = quote_source(position["security_id"])
            trade = close_position(trades, state, cutoff["reason"], quote, started, received)
            alert(f"Forced exit {trade['_id']} at {trade['exit_price']}; "
                  f"estimated net {trade['net_pnl']}")
            if trade["exit_quote"]["insufficient_best_size"]:
                alert("Best-level size was insufficient; full paper exit is optimistic")
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
            quote, started, received = quote_source(position["security_id"])
            trade = close_position(trades, state, result["reason"], quote, started, received)
            alert(f"Exit {trade['_id']} at {trade['exit_price']}; estimated net {trade['net_pnl']}")
            if trade["exit_quote"]["insufficient_best_size"]:
                alert("Best-level size was insufficient; full paper exit is optimistic")
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
    quote, started, received = quote_source(doc["security_id"])
    position = open_position(trades, state, doc, doc["contract"], quantity,
                                         quote, started, received)
    alert(f"Entry {position['_id']} {side} Qty {position['quantity']} at {position['entry_price']}; "
          f"spread {position['entry_quote']['spread']}")
    if position["entry_quote"]["insufficient_best_size"]:
        alert("Best-level size was insufficient; full paper fill is optimistic")


def main():
    load_dotenv(find_dotenv())
    for key in ("CONNECTION_STRING", "user_name", "slack_token", "DHAN_CLIENT_ID",
                "DHAN_PIN", "DHAN_TOTP"):
        if not os.environ.get(key, "").strip():
            raise ValueError("Missing environment variable: " + key)
    mode = os.environ.get("live_trading", "false").strip().lower()
    if mode != "false":
        raise ValueError("Silver Surfer only supports paper execution; live orders are disabled")
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

    def alert(message):
        notify(slack, message)

    started_at = now_ist()
    waiting_alert = False
    last_error = None
    last_alert = now_ist()
    try:
        initialize_store(trades)
        notify(slack, f"Started {session} in paper mode; Qty {quantity}; live orders disabled")
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
                          session=session, started_at=started_at)
                if last_error is not None:
                    alert("Executor recovered")
                    last_error = None
                state = create_state(trades)
                if state["position"] is not None:
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
            time.sleep(5)
    except Exception as error:
        alert("STARTUP/FATAL ERROR: " + utils.exception_detail(error))
        raise
    finally:
        client.close()


if __name__ == "__main__":
    main()
