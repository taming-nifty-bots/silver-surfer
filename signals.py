"""Build a Silver Surfer signal from completed, exact-contract candles."""

from datetime import datetime, time

import pandas as pd

import strategy


def build_signal(candles, day, now, session):
    if session not in ("morning", "evening"):
        raise ValueError("Invalid Silver session")
    now = pd.Timestamp(now)
    day = pd.Timestamp(day).date()
    if now.date() != day:
        raise ValueError("Signal date differs from current date")
    bars = strategy.chart_history(candles, day, now)
    today = bars.loc[bars["datetime"].dt.date == day]
    if today.empty:
        raise ValueError("No completed candles for the current trading day")
    latest = today.iloc[-1]
    as_of = pd.Timestamp(latest["datetime"])
    if as_of != now.floor("min") - pd.Timedelta(minutes=1):
        raise ValueError("Latest completed Silver candle is stale")
    box = 0.1 if session == "morning" else 0.15
    bricks = strategy.chart(bars, box)
    last = bricks.iloc[-1]
    if pd.isna(last["upper40"]) or pd.isna(last["lower40"]):
        raise ValueError("Insufficient preceding bricks for the entry channel")
    values = dict(session=session, box_size=box, data_asof=str(as_of),
                  candle_close=float(latest["close"]), candle_volume=float(latest["volume"]),
                  brick_time=str(last["brick_time"]), close=float(last["close"]),
                  side=strategy.entry_side(as_of, last))
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
