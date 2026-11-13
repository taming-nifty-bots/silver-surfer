"""Silver Surfer's completed-candle Renko rules."""

import math
from datetime import time

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
