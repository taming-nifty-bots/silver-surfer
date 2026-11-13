"""Publish completed-candle Silver Surfer levels during MCX sessions."""

import os
import time
from datetime import datetime, time as clock_time, timedelta
from zoneinfo import ZoneInfo

from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient
from slack_sdk import WebClient
from tamingnifty import connect_dhan as dhan
from tamingnifty import utils

import market
import executor
import paper_state
import publisher
import strategy


def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)


def notify(slack, message):
    try:
        utils.notify(message="Silver Surfer signal | " + message,
                     slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"),
                     slack_client=slack)
    except Exception as error:
        print("Slack notification failed: " + utils.exception_detail(error), flush=True)


def main():
    load_dotenv(find_dotenv())
    for key in ("CONNECTION_STRING", "user_name", "slack_token", "DHAN_CLIENT_ID",
                "DHAN_PIN", "DHAN_TOTP"):
        if not os.environ.get(key, "").strip():
            raise ValueError("Missing environment variable: " + key)
    session = os.environ.get("SILVER_SESSION", "").strip().lower()
    if session not in ("morning", "evening"):
        raise ValueError("SILVER_SESSION must be morning or evening")
    slack = WebClient(token=os.environ["slack_token"])
    client = MongoClient(os.environ["CONNECTION_STRING"])
    db = client["Bots"]
    user = os.environ["user_name"]
    signals = db["supertrend"]
    locks = signals
    states = db[f"silver_surfer_{user}"]
    calendars = db["exchange_calendars"]
    last_minute = None
    last_error = None
    last_alert = now_ist()
    previous_gaps = None
    notify(slack, f"Started {session}; publishing paper-forward-test signals")
    try:
        while True:
            now = now_ist()
            try:
                state = states.find_one({"_id": paper_state.STATE_ID})
                if executor.session_finished(state, session, now):
                    break
                if session == "morning" and now.time() >= clock_time(16, 55):
                    break
                if session == "evening" and now.time() >= strategy.EVENING_EXIT:
                    break
                status = market.session_status(calendars, now.date())
                if not status[session]:
                    break
                minute = now.replace(second=0, microsecond=0)
                active = (clock_time(9) <= now.time() < clock_time(16, 55)
                          if session == "morning" else clock_time(17) <= now.time() < strategy.EVENING_EXIT)
                if active and status[session] and minute != last_minute:
                    lock = locks.find_one({"_id": "silver:" + str(now.date())})
                    if lock is None:
                        candidate = market.silver_contract(dhan.load_instruments(), calendars, now.date())
                    else:
                        candidate = lock["contract"]
                    contract = publisher.lock_contract(locks, now.date(), candidate)
                    state = states.find_one({"_id": paper_state.STATE_ID})
                    position = None if state is None else state["position"]
                    if position is not None and (str(position["security_id"]) != contract["security_id"]
                                                 or position["expiry"] != contract["expiry"]):
                        raise ValueError("An open paper position has a different contract")
                    conn = dhan.login_to_dhan(slack_channel=os.environ.get("SLACK_CHANNEL", "silver-surfer"))
                    start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=21)
                    candles = dhan.fetch_candles(conn, contract["security_id"], "MCX_COMM", "FUTCOM",
                                                 start, now)
                    published_at = now_ist()
                    if published_at.date() != now.date():
                        raise ValueError("Date changed while fetching Silver candles")
                    doc = publisher.prepare_signal(candles, now.date(), published_at, session,
                                                   status, contract, position)
                    if publisher.publish(signals, doc):
                        gaps = doc["missing_minutes"]
                        if gaps and gaps != previous_gaps:
                            notify(slack, f"Missing {len(gaps)} expected candles; latest {gaps[-1]}. "
                                          "Continuing with observed candles; signals and exits may differ.")
                        previous_gaps = gaps
                        print(f"Published {session} signal through {doc['data_asof']}", flush=True)
                    last_minute = minute
                if last_error is not None:
                    notify(slack, "Signal recovered")
                    last_error = None
            except Exception as error:
                message = utils.exception_detail(error)
                print(message, flush=True)
                if message != last_error or now - last_alert >= timedelta(minutes=15):
                    notify(slack, "ERROR: " + message)
                    last_error, last_alert = message, now
            time.sleep(5)
    finally:
        client.close()


if __name__ == "__main__":
    main()
