"""Silver Surfer rules, completed-candle signals, and MCX contract selection."""

import math
from datetime import datetime, time, timedelta

import pandas as pd
from tamingnifty import ta


VERSION = "silver-renko-40-10-to-7-2026-11-13"
EVENING_LAST_ENTRY = time(22, 50)
EVENING_EXIT = time(23, 10)


def chart_history(candles, day, as_of):
    bars = candles.copy()
    required = ("datetime", "open", "high", "low", "close", "volume")
    if not set(required).issubset(bars.columns):
        raise ValueError("Missing commodity OHLCV columns")
    bars["datetime"] = pd.to_datetime(bars["datetime"])
    bars = bars.loc[bars["datetime"] < pd.Timestamp(as_of).floor("min")]
    bars = bars.sort_values("datetime").reset_index(drop=True)
    if bars.empty or bars["datetime"].duplicated().any():
        raise ValueError("Empty or duplicate commodity candles")
    if bars[list(required)].isna().any().any():
        raise ValueError("Missing commodity candle values")
    for field in required[1:]:
        if not bars[field].map(math.isfinite).all():
            raise ValueError("Non-finite commodity candle " + field)
    if (bars[["open", "high", "low", "close"]] <= 0).any().any() or (bars["volume"] < 0).any():
        raise ValueError("Invalid commodity candle prices or volume")
    if ((bars["high"] < bars[["open", "close", "low"]].max(axis=1)).any()
            or (bars["low"] > bars[["open", "close", "high"]].min(axis=1)).any()):
        raise ValueError("Invalid commodity OHLC ordering")
    day = pd.Timestamp(day).date()
    dates = sorted(set(bars.loc[bars["datetime"].dt.date < day, "datetime"].dt.date))
    if len(dates) < 5:
        raise ValueError("Need five previous observed dates for the same contract")
    anchor = dates[-5]
    return bars.loc[(bars["datetime"].dt.date >= anchor)
                    & (bars["datetime"].dt.date <= day)].reset_index(drop=True)


def chart(candles, box_size):
    if box_size not in (0.1, 0.15):
        raise ValueError("Unsupported Silver Renko box size")
    if candles.empty:
        raise ValueError("Cannot build a Renko chart without candles")
    if (candles[["open", "close"]] < 10).any().any():
        raise ValueError("Commodity prices too small for safe percentage Renko steps")
    bricks = ta.convert_to_renko(brick_size=box_size, df=candles[["datetime", "open", "close"]].copy())
    for n in (7, 10, 40):
        bricks[f"upper{n}"] = bricks["high"].shift(1).rolling(n).max()
        bricks[f"lower{n}"] = bricks["low"].shift(1).rolling(n).min()
    bricks["brick_time"] = bricks["datetime"]
    return bricks.drop_duplicates("datetime", keep="last").reset_index(drop=True)


def entry_side(candle_time, brick):
    candle_time = pd.Timestamp(candle_time)
    if candle_time.time() <= time(9) or pd.Timestamp(brick["brick_time"]) != candle_time:
        return None
    if pd.notna(brick["upper40"]) and brick["close"] > brick["upper40"]:
        return "BUY"
    if pd.notna(brick["lower40"]) and brick["close"] < brick["lower40"]:
        return "SELL"
    return None


def profit_active(side, entry_price, candle_close, active):
    if side not in ("BUY", "SELL"):
        raise ValueError("Invalid entry side")
    if not math.isfinite(float(entry_price)) or float(entry_price) <= 0:
        raise ValueError("Invalid entry price")
    if not math.isfinite(float(candle_close)) or float(candle_close) <= 0:
        raise ValueError("Invalid candle close")
    direction = 1 if side == "BUY" else -1
    return bool(active or direction * (candle_close - entry_price) >= 0.01 * entry_price)


def channel_exit(side, brick, active):
    n = 7 if active else 10
    if side == "BUY":
        level = brick[f"lower{n}"]
        return bool(pd.notna(level) and brick["close"] < level)
    if side == "SELL":
        level = brick[f"upper{n}"]
        return bool(pd.notna(level) and brick["close"] > level)
    raise ValueError("Invalid entry side")


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


def build_signal(candles, day, now, session):
    if session not in ("morning", "evening"):
        raise ValueError("Invalid Silver session")
    now = pd.Timestamp(now)
    day = pd.Timestamp(day).date()
    if now.date() != day:
        raise ValueError("Signal date differs from current date")
    bars = chart_history(candles, day, now)
    today = bars.loc[bars["datetime"].dt.date == day]
    if today.empty:
        raise ValueError("No completed candles for the current trading day")
    latest = today.iloc[-1]
    as_of = pd.Timestamp(latest["datetime"])
    if as_of != now.floor("min") - pd.Timedelta(minutes=1):
        raise ValueError("Latest completed Silver candle is stale")
    box = 0.1 if session == "morning" else 0.15
    bricks = chart(bars, box)
    last = bricks.iloc[-1]
    if pd.isna(last["upper40"]) or pd.isna(last["lower40"]):
        raise ValueError("Insufficient preceding bricks for the entry channel")
    values = dict(session=session, box_size=box, data_asof=str(as_of),
                  candle_close=float(latest["close"]), candle_volume=float(latest["volume"]),
                  brick_time=str(last["brick_time"]), close=float(last["close"]),
                  side=entry_side(as_of, last))
    for n in (7, 10, 40):
        values[f"upper{n}"] = float(last[f"upper{n}"]) if pd.notna(last[f"upper{n}"]) else None
        values[f"lower{n}"] = float(last[f"lower{n}"]) if pd.notna(last[f"lower{n}"]) else None
    return values


def session_gaps(candles, day, as_of, morning_open, evening_open):
    day = pd.Timestamp(day).date()
    end = pd.Timestamp(as_of)
    if end.date() != day:
        raise ValueError("Gap check date differs from signal date")
    observed = set(pd.to_datetime(candles["datetime"]))
    expected = []
    if morning_open:
        first = datetime.combine(day, time(9))
        last = min(end, pd.Timestamp(datetime.combine(day, time(16, 59))))
        if last >= first:
            expected.extend(pd.date_range(first, last, freq="min"))
    if evening_open:
        first = datetime.combine(day, time(17))
        last = min(end, pd.Timestamp(datetime.combine(day, time(23, 9))))
        if last >= first:
            expected.extend(pd.date_range(first, last, freq="min"))
    return [str(stamp) for stamp in expected if stamp not in observed]


def recover_activation(trade, candles, now):
    """Recheck completed closes since entry, including those missed on restart."""
    now = pd.Timestamp(now)
    entry = pd.Timestamp(trade["entry_timestamp"])
    if now < entry:
        raise ValueError("Position entry is in the future")
    if not {"datetime", "close"}.issubset(candles.columns):
        raise ValueError("Activation recovery needs candle timestamps and closes")
    bars = candles.copy()
    bars["datetime"] = pd.to_datetime(bars["datetime"])
    bars = bars.loc[(bars["datetime"] >= entry.floor("min"))
                    & (bars["datetime"] < now.floor("min"))]
    bars = bars.sort_values("datetime").reset_index(drop=True)
    if bars["datetime"].duplicated().any():
        raise ValueError("Duplicate candles during activation recovery")
    active = False
    for row in bars.to_dict("records"):
        active = profit_active(trade["side"], trade["entry_price"], row["close"], active)
    return active


def entry_decision(signal, now, session_open):
    now = pd.Timestamp(now)
    if signal["session"] not in ("morning", "evening"):
        raise ValueError("Invalid signal session")
    if not session_open:
        return None
    candle = pd.Timestamp(signal["data_asof"])
    if candle.date() != now.date() or candle + pd.Timedelta(minutes=1) != now.floor("min"):
        raise ValueError("Entry signal is not from the immediately preceding minute")
    if pd.Timestamp(signal["brick_time"]) != candle or signal["side"] is None:
        return None
    if candle.time() <= time(9) or signal["candle_volume"] <= 0:
        return None
    if signal["side"] not in ("BUY", "SELL"):
        raise ValueError("Invalid signal side")
    if signal["session"] == "morning":
        if not time(9) < now.time() < time(16, 55):
            return None
    elif not time(17) <= now.time() <= EVENING_LAST_ENTRY:
        return None
    return signal["side"]


def exit_decision(trade, signal, now):
    now = pd.Timestamp(now)
    entry = pd.Timestamp(trade["entry_timestamp"])
    if trade["session"] not in ("morning", "evening"):
        raise ValueError("Invalid position session")
    if now < entry:
        raise ValueError("Position entry is in the future")
    cutoff = time(16, 55) if trade["session"] == "morning" else EVENING_EXIT
    if now.date() > entry.date() or now.time() >= cutoff:
        return dict(reason="session cutoff", profit_active=trade["profit_active"])
    if signal is None:
        raise ValueError("Fresh completed candle required for position management")
    if (str(signal["security_id"]) != str(trade["security_id"])
            or signal["session"] != trade["session"]):
        raise ValueError("Signal contract or session does not match open position")
    candle = pd.Timestamp(signal["data_asof"])
    if candle.date() != now.date() or candle + pd.Timedelta(minutes=1) != now.floor("min"):
        raise ValueError("Exit signal is not from the immediately preceding minute")
    if candle < entry.floor("min"):
        return dict(reason=None, profit_active=trade["profit_active"])
    active = profit_active(trade["side"], trade["entry_price"],
                           signal["candle_close"], trade["profit_active"])
    reason = "channel7" if active else "channel10"
    if not channel_exit(trade["side"], signal, active):
        reason = None
    return dict(reason=reason, profit_active=active)


def recover_missed_exit(trade, candles, asof):
    """Reconstruct observed missed minutes; any exit must fill at a current quote."""
    frame = candles.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    if frame["datetime"].duplicated().any():
        raise ValueError("Duplicate candles during exit recovery")
    start = max(pd.Timestamp(trade["last_checked_asof"]) + pd.Timedelta(minutes=1),
                pd.Timestamp(trade["entry_timestamp"]).floor("min"))
    end = pd.Timestamp(asof)
    if end < start:
        raise ValueError("Invalid exit recovery interval")
    frame = frame.loc[(frame["datetime"] >= start) & (frame["datetime"] <= end)]
    frame = frame.sort_values("datetime")
    active = recover_activation(trade, candles, max(start, pd.Timestamp(trade["entry_timestamp"])))
    for stamp in frame["datetime"]:
        signal = build_signal(candles, stamp.date(), stamp + pd.Timedelta(minutes=1),
                              trade["session"])
        signal["security_id"] = trade["security_id"]
        result = exit_decision(dict(trade, profit_active=active), signal,
                               stamp + pd.Timedelta(minutes=1))
        active = result["profit_active"]
        if result["reason"] is not None:
            return dict(reason=result["reason"], profit_active=active, data_asof=str(stamp))
    return dict(reason=None, profit_active=active, data_asof=str(end))
