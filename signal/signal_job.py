"""Publish completed-candle Silver Surfer levels during MCX sessions."""

import os
import time
from datetime import datetime, time as clock_time, timedelta
from zoneinfo import ZoneInfo

from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pandas as pd
from slack_sdk import WebClient
from tamingnifty import connect_dhan as dhan
from tamingnifty import utils

import executor
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


def lock_contract(signals, day, candidate):
    key = "silver:contract"
    day = str(day)
    document = signals.find_one({"_id": key})
    if document is None:
        document = dict(_id=key, day=day, strategy_version=strategy.VERSION, contract=candidate)
        try:
            signals.insert_one(document)
        except DuplicateKeyError:
            document = signals.find_one({"_id": key})
    if document is not None and document["day"] < day:
        updated = dict(_id=key, day=day, strategy_version=strategy.VERSION, contract=candidate)
        result = signals.replace_one({"_id": key, "day": document["day"]}, updated)
        if result.modified_count != 1:
            raise RuntimeError("Concurrent Silver contract lock update; retry")
        document = updated
    if document is not None and document["day"] != day:
        raise ValueError("Silver contract lock is from a later day")
    if document is None or document.get("strategy_version") != strategy.VERSION:
        raise ValueError("Missing or incompatible Silver contract lock")
    if not isinstance(document.get("contract"), dict):
        raise ValueError("Invalid Silver contract lock")
    return document["contract"]


def prepare_signal(candles, day, now, session, status, contract, position):
    if not status[session]:
        raise ValueError("MCX session is closed")
    if position is not None and (str(position["security_id"]) != str(contract["security_id"])
                                 or position["expiry"] != contract["expiry"]):
        raise ValueError("Open paper position does not match the locked contract")
    doc = strategy.build_signal(candles, day, now, session)
    doc.update(_id=f"silver:{session}", strategy_version=strategy.VERSION,
               security_id=str(contract["security_id"]), expiry=contract["expiry"],
               contract=contract, updated_at=str(pd.Timestamp(now)))
    doc["missing_minutes"] = strategy.session_gaps(
        candles, day, doc["data_asof"], status["morning"], status["evening"])
    return doc


def publish(collection, doc):
    existing = collection.find_one({"_id": doc["_id"]})
    if existing is None:
        try:
            collection.insert_one(doc.copy())
            return True
        except DuplicateKeyError:
            raise RuntimeError("Concurrent Silver signal publication; retry")
    old = pd.Timestamp(existing["data_asof"])
    new = pd.Timestamp(doc["data_asof"])
    if old.date() == new.date() and (
            existing.get("strategy_version") != doc["strategy_version"]
            or existing.get("security_id") != doc["security_id"]
            or existing.get("expiry") != doc["expiry"]):
        raise ValueError("Existing signal conflicts with the daily contract lock")
    if new <= old:
        return False
    result = collection.replace_one({"_id": doc["_id"], "data_asof": existing["data_asof"]}, doc)
    if result.modified_count != 1:
        raise RuntimeError("Concurrent Silver signal update; retry")
    return True


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
                state = executor.create_state(states)
                if executor.session_finished(state, session, now):
                    break
                if session == "morning" and now.time() >= clock_time(16, 55):
                    break
                if session == "evening" and now.time() >= strategy.EVENING_EXIT:
                    break
                status = strategy.session_status(calendars, now.date())
                if not status[session]:
                    break
                minute = now.replace(second=0, microsecond=0)
                active = (clock_time(9) <= now.time() < clock_time(16, 55)
                          if session == "morning" else clock_time(17) <= now.time() < strategy.EVENING_EXIT)
                if active and status[session] and minute != last_minute:
                    lock = signals.find_one({"_id": "silver:contract"})
                    if lock is None or lock.get("day") != str(now.date()):
                        candidate = strategy.silver_contract(dhan.load_instruments(), calendars, now.date())
                    else:
                        candidate = lock["contract"]
                    contract = lock_contract(signals, now.date(), candidate)
                    state = executor.create_state(states)
                    position = state["position"]
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
                    doc = prepare_signal(candles, now.date(), published_at, session,
                                                   status, contract, position)
                    if publish(signals, doc):
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
                # Every session's first minute has no completed candle yet (the
                # first candle needs a full minute to close) -- this always
                # resolves itself by the next minute, so don't alert on it.
                if (not message.endswith("No completed candles for the current trading day")
                        and (message != last_error or now - last_alert >= timedelta(minutes=15))):
                    notify(slack, "ERROR: " + message)
                    last_error, last_alert = message, now
            time.sleep(5)
    finally:
        client.close()


if __name__ == "__main__":
    main()
