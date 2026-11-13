"""Silver Surfer paper executor. Live broker orders are deliberately disabled."""

import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient
from slack_sdk import WebClient
from tamingnifty import connect_dhan as dhan
from tamingnifty import utils

import decisions
import market
import paper_state
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


def checked_signal(signals, day, session, now):
    doc = signals.find_one({"_id": f"silver:{day}:{session}"})
    if doc is None or doc.get("strategy_version") != strategy.VERSION:
        raise ValueError("Missing or incompatible Silver signal")
    candle = pd.Timestamp(doc["data_asof"])
    if candle + pd.Timedelta(minutes=1) != pd.Timestamp(now).floor("min"):
        raise ValueError("Silver signal is stale or future-dated")
    return doc


def run_cycle(states, trades, signals, calendars, quote_source, candles_source, clock, alert):
    state = paper_state.create_state(states)
    if state["pending_trade"] is not None:
        paper_state.archive_trade(states, trades, state)
        state = states.find_one({"_id": paper_state.STATE_ID})
    now = clock()
    position = state["position"]
    if position is not None:
        if position["exit_requested"] is not None:
            quote, started, received = quote_source(position["security_id"])
            trade = paper_state.close_position(states, state, position["exit_requested"],
                                               quote, started, received)
            alert(f"Exit {trade['id']} at {trade['exit_price']}; estimated net {trade['net_pnl']}")
            if trade["exit_quote"]["insufficient_best_size"]:
                alert("Best-level size was insufficient; full paper exit is optimistic")
            return
        cutoff = decisions.exit_decision(position, None, now) if (
            now.date() > pd.Timestamp(position["entry_timestamp"]).date()
            or now.time() >= (datetime.strptime("17:00", "%H:%M").time()
                              if position["session"] == "morning"
                              else datetime.strptime("23:00", "%H:%M").time())) else None
        if cutoff is not None:
            quote, started, received = quote_source(position["security_id"])
            trade = paper_state.close_position(states, state, cutoff["reason"], quote, started, received)
            alert(f"Forced exit {trade['id']} at {trade['exit_price']}; "
                  f"estimated net {trade['net_pnl']}")
            if trade["exit_quote"]["insufficient_best_size"]:
                alert("Best-level size was insufficient; full paper exit is optimistic")
            return
        doc = checked_signal(signals, str(now.date()), position["session"], now)
        checked = pd.Timestamp(position["last_checked_asof"])
        asof = pd.Timestamp(doc["data_asof"])
        if asof <= checked:
            return
        candles = candles_source(position)
        current = clock()
        if asof > checked + pd.Timedelta(minutes=1):
            alert(f"Position {position['id']} missed signal checks after {checked}; "
                  "reconstructing observed candles; any missed exit uses a current quote.")
        expected = pd.date_range(max(checked + pd.Timedelta(minutes=1),
                                     pd.Timestamp(position["entry_timestamp"]).floor("min")),
                                 asof, freq="min")
        observed = set(pd.to_datetime(candles["datetime"]))
        missing = [str(stamp) for stamp in expected if stamp not in observed]
        if missing:
            alert(f"Position {position['id']} has {len(missing)} missing completed candles; "
                  "activation and missed exits cannot be fully recovered.")
        if asof > checked + pd.Timedelta(minutes=1):
            result = decisions.recover_missed_exit(position, candles, asof)
            if result["reason"] is not None:
                alert(f"Recovered {result['reason']} exit signal from {result['data_asof']}; "
                      "using the current quote rather than a historical fill.")
        else:
            active = decisions.recover_activation(position, candles, current)
            result = decisions.exit_decision(dict(position, profit_active=active), doc, current)
        current = clock()
        if current.date() > pd.Timestamp(position["entry_timestamp"]).date() or (
                position["session"] == "morning" and current.hour >= 17) or (
                position["session"] == "evening" and current.hour >= 23):
            result["reason"] = result["reason"] or "session cutoff"
        if result["reason"] is not None:
            state = paper_state.request_exit(states, state, result["reason"], result["profit_active"])
            quote, started, received = quote_source(position["security_id"])
            trade = paper_state.close_position(states, state, result["reason"], quote, started, received)
            alert(f"Exit {trade['id']} at {trade['exit_price']}; estimated net {trade['net_pnl']}")
            if trade["exit_quote"]["insufficient_best_size"]:
                alert("Best-level size was insufficient; full paper exit is optimistic")
        else:
            paper_state.checkpoint_signal(states, state, result["profit_active"], asof)
        return

    status = market.session_status(calendars, now.date())
    session = "morning" if now.hour < 17 else "evening"
    if not status[session] or not 9 <= now.hour < 23:
        return
    if state["slot_day"] == str(now.date()) and state[session + "_used"]:
        return
    doc = checked_signal(signals, str(now.date()), session, now)
    side = decisions.entry_decision(doc, now, status[session])
    if side is None:
        return
    quote, started, received = quote_source(doc["security_id"])
    position = paper_state.open_position(states, state, doc, doc["contract"], quote, started, received)
    alert(f"Entry {position['id']} {side} at {position['entry_price']}; "
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
    slack = WebClient(token=os.environ["slack_token"])
    client = MongoClient(os.environ["CONNECTION_STRING"])
    db = client["Bots"]
    user = os.environ["user_name"]
    states = db[f"silver_surfer_state_{user}"]
    trades = db[f"silver_surfer_trades_{user}"]
    signals = db[f"silver_surfer_signals_{user}"]
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

    def alert(message):
        notify(slack, message)

    notify(slack, "Started in one-lot paper mode; live orders disabled")
    last_error = None
    last_alert = now_ist()
    try:
        while True:
            now = now_ist()
            try:
                run_cycle(states, trades, signals, calendars, quote_source, candles_source, now_ist, alert)
                if last_error is not None:
                    alert("Executor recovered")
                    last_error = None
                state = states.find_one({"_id": paper_state.STATE_ID})
                if now.hour == 23 and now.minute >= 5 and state["position"] is None and state["pending_trade"] is None:
                    break
            except Exception as error:
                message = utils.exception_detail(error)
                print(message, flush=True)
                if message != last_error or now - last_alert >= timedelta(minutes=15):
                    alert("ERROR: " + message)
                    last_error, last_alert = message, now
            time.sleep(5)
    finally:
        client.close()


if __name__ == "__main__":
    main()
