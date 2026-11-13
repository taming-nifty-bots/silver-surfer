"""Entry and exit decisions for Silver Surfer's paper executor."""

from datetime import time

import pandas as pd

import signals
import strategy


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
    active = trade["profit_active"]
    for row in bars.to_dict("records"):
        active = strategy.profit_active(trade["side"], trade["entry_price"], row["close"], active)
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
    elif not time(17) <= now.time() < time(23):
        return None
    return signal["side"]


def exit_decision(trade, signal, now):
    now = pd.Timestamp(now)
    entry = pd.Timestamp(trade["entry_timestamp"])
    if trade["session"] not in ("morning", "evening"):
        raise ValueError("Invalid position session")
    if now < entry:
        raise ValueError("Position entry is in the future")
    cutoff = time(16, 55) if trade["session"] == "morning" else time(23)
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
    active = strategy.profit_active(trade["side"], trade["entry_price"],
                                    signal["candle_close"], trade["profit_active"])
    reason = "channel7" if active else "channel10"
    if not strategy.channel_exit(trade["side"], signal, active):
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
    active = trade["profit_active"]
    for stamp in frame["datetime"]:
        signal = signals.build_signal(candles, stamp.date(), stamp + pd.Timedelta(minutes=1),
                                      trade["session"])
        signal["security_id"] = trade["security_id"]
        result = exit_decision(dict(trade, profit_active=active), signal,
                               stamp + pd.Timedelta(minutes=1))
        active = result["profit_active"]
        if result["reason"] is not None:
            return dict(reason=result["reason"], profit_active=active, data_asof=str(stamp))
    return dict(reason=None, profit_active=active, data_asof=str(end))
