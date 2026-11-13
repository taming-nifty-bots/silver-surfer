"""Contract locking and monotonic publication for Silver Surfer signals."""

import pandas as pd
from pymongo.errors import DuplicateKeyError

import signals
import strategy


def lock_contract(locks, day, candidate):
    key = "silver:" + str(day)
    document = dict(_id=key, strategy_version=strategy.VERSION, contract=candidate)
    try:
        locks.insert_one(document)
    except DuplicateKeyError:
        document = locks.find_one({"_id": key})
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
    doc = signals.build_signal(candles, day, now, session)
    doc.update(_id=f"silver:{day}:{session}", strategy_version=strategy.VERSION,
               security_id=str(contract["security_id"]), expiry=contract["expiry"],
               contract=contract, updated_at=str(pd.Timestamp(now)))
    gaps = signals.session_gaps(candles, day, doc["data_asof"],
                                status["morning"], status["evening"])
    doc["missing_minutes"] = gaps
    return doc


def publish(collection, doc):
    existing = collection.find_one({"_id": doc["_id"]})
    if existing is None:
        try:
            collection.insert_one(doc.copy())
            return True
        except DuplicateKeyError:
            raise RuntimeError("Concurrent Silver signal publication; retry")
    if (existing.get("strategy_version") != doc["strategy_version"]
            or existing.get("security_id") != doc["security_id"]
            or existing.get("expiry") != doc["expiry"]):
        raise ValueError("Existing signal conflicts with the daily contract lock")
    old = pd.Timestamp(existing["data_asof"])
    new = pd.Timestamp(doc["data_asof"])
    if new <= old:
        return False
    result = collection.replace_one({"_id": doc["_id"], "data_asof": existing["data_asof"]}, doc)
    if result.modified_count != 1:
        raise RuntimeError("Concurrent Silver signal update; retry")
    return True
