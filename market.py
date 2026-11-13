"""MCX session calendar and Silver Micro contract selection."""

import math
from datetime import datetime, timedelta

import pandas as pd


def calendar_for_day(calendars, day):
    document = calendars.find_one({"_id": "MCX:" + str(day.year)})
    if not isinstance(document, dict):
        raise ValueError("Missing MCX calendar for " + str(day.year))
    if (document.get("exchange") != "MCX" or document.get("year") != day.year
            or document.get("status") != "verified"
            or document.get("valid_from") != f"{day.year}-01-01"
            or document.get("valid_to") != f"{day.year}-12-31"
            or document.get("timezone") != "Asia/Kolkata"):
        raise ValueError("Invalid or unverified MCX calendar for " + str(day.year))
    if not isinstance(document.get("holidays"), list) or not isinstance(document.get("special_sessions"), list):
        raise ValueError("MCX session exceptions are missing")
    seen = set()
    for holiday in document["holidays"]:
        date = datetime.strptime(holiday["date"], "%Y-%m-%d").date()
        if date.year != day.year or date in seen:
            raise ValueError("Invalid or duplicate MCX holiday")
        seen.add(date)
        if holiday.get("morning") not in ("open", "closed") or holiday.get("evening") not in ("open", "closed"):
            raise ValueError("Invalid MCX holiday session status")
    seen = set()
    for special in document["special_sessions"]:
        date = datetime.strptime(special["date"], "%Y-%m-%d").date()
        if date.year != day.year or date in seen:
            raise ValueError("Invalid or duplicate MCX special session")
        seen.add(date)
    return document


def session_status(calendars, day):
    document = calendar_for_day(calendars, day)
    for special in document["special_sessions"]:
        if special["date"] == day.isoformat():
            return dict(morning=False, evening=False, special=True)
    if day.weekday() >= 5:
        return dict(morning=False, evening=False, special=False)
    for holiday in document["holidays"]:
        if holiday["date"] == day.isoformat():
            return dict(morning=holiday["morning"] == "open",
                        evening=holiday["evening"] == "open", special=False)
    return dict(morning=True, evening=True, special=False)


def trading_days_before_expiry(calendars, day, expiry):
    if day > expiry:
        raise ValueError("Contract has expired")
    count = 0
    current = day
    while current < expiry:
        status = session_status(calendars, current)
        if status["morning"] or status["evening"] or status["special"]:
            count += 1
        current += timedelta(days=1)
    return count


def silver_contract(master, calendars, day):
    columns = ("EXCH_ID", "SEGMENT", "INSTRUMENT", "UNDERLYING_SYMBOL",
               "SM_EXPIRY_DATE", "SECURITY_ID", "DISPLAY_NAME", "LOT_SIZE", "TICK_SIZE")
    if not set(columns).issubset(master.columns):
        raise ValueError("Dhan instrument master is missing required fields")
    matches = master.loc[(master["EXCH_ID"] == "MCX") & (master["SEGMENT"] == "M")
                         & (master["INSTRUMENT"] == "FUTCOM")
                         & (master["UNDERLYING_SYMBOL"] == "SILVERMIC")].copy()
    matches["expiry"] = pd.to_datetime(matches["SM_EXPIRY_DATE"], errors="coerce").dt.date
    if matches["expiry"].isna().any():
        raise ValueError("Invalid Silver Micro expiry in instrument master")
    matches = matches.loc[matches["expiry"] >= day].sort_values("expiry")
    if matches.empty or matches["expiry"].duplicated().any():
        raise ValueError("Missing or duplicate active Silver Micro contracts")
    nearest = matches.iloc[0]
    days = trading_days_before_expiry(calendars, day, nearest["expiry"])
    if days <= 7:
        if len(matches) < 2:
            raise ValueError("Next Silver Micro expiry is not listed")
        row = matches.iloc[1]
    else:
        row = nearest
    security_id = float(row["SECURITY_ID"])
    lot = float(row["LOT_SIZE"])
    tick = float(row["TICK_SIZE"]) / 100
    if (not math.isfinite(security_id) or security_id <= 0 or int(security_id) != security_id
            or not math.isfinite(lot) or lot <= 0 or int(lot) != lot
            or not math.isfinite(tick) or tick <= 0 or not str(row["DISPLAY_NAME"]).strip()):
        raise ValueError("Invalid Silver Micro contract details")
    return dict(security_id=str(int(security_id)), symbol=str(row["DISPLAY_NAME"]),
                expiry=str(row["expiry"]), lot_size=int(lot), tick_size=tick)
