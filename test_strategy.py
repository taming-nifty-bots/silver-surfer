"""Offline tests; --replay also checks the preserved vault research data."""

import sys
from datetime import datetime, time, timedelta
from pathlib import Path

import pandas as pd

import strategy


def basic_checks():
    brick = dict(brick_time="2026-11-13 10:00", close=101, upper40=100, lower40=90,
                 upper10=102, lower10=95, upper7=101.5, lower7=99)
    assert strategy.entry_side("2026-11-13 10:00", brick) == "BUY"
    assert strategy.entry_side("2026-11-13 10:01", brick) is None
    assert strategy.entry_side("2026-11-13 09:00", dict(brick, brick_time="2026-11-13 09:00")) is None
    assert strategy.entry_side("2026-11-13 10:00", dict(brick, close=100)) is None
    assert strategy.entry_side("2026-11-13 10:00", dict(brick, close=89)) == "SELL"
    assert strategy.profit_active("BUY", 100, 101, False)
    assert not strategy.profit_active("BUY", 100, 100.99, False)
    assert strategy.profit_active("SELL", 100, 99, False)
    assert strategy.profit_active("SELL", 100, 100, True)
    assert not strategy.channel_exit("BUY", dict(brick, close=99), True)
    assert strategy.channel_exit("BUY", dict(brick, close=98.99), True)
    assert strategy.channel_exit("SELL", dict(brick, close=101.51), True)
    assert not strategy.channel_exit("SELL", dict(brick, close=101.51), False)
    print("Strategy boundary checks passed")


def replay_checks():
    raw = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(raw))
    import silver_micro_fresh_final_20261113 as fresh

    data = pd.read_csv(fresh.SOURCE, parse_dates=["datetime"])
    data = data.loc[data.expiry_rank == 1].copy()
    data["day"] = data["datetime"].dt.date
    sessions = pd.read_csv(raw / "silver_micro_holiday_aware_20261113_sessions.csv")
    expected = pd.read_csv(raw / "silver_micro_holiday_aware_20261113_trades.csv")
    expected = expected.loc[(expected.scenario == "existing_rule") & (expected.slippage == 5)]
    results = []
    for session in sessions.to_dict("records"):
        if session["status"] != "scored":
            continue
        day = pd.Timestamp(session["date"]).date()
        start = time(17) if session["holiday"] else time(9)
        current = data.loc[(data.day == day) & (data.datetime.dt.time >= start)
                           & (data.datetime.dt.time <= time(23))].reset_index(drop=True)
        history = strategy.chart_history(data, day, datetime.combine(day, time(23, 1)))
        features = {}
        for setup, box in (("morning", 0.1), ("evening", 0.15)):
            end = datetime.combine(day, time(17)) if setup == "morning" else datetime.combine(day, time(23, 1))
            candles = history.loc[history.datetime < end]
            bricks = strategy.chart(candles, box)
            features[setup] = pd.merge_asof(current[["datetime"]], bricks, on="datetime", direction="backward")
        rows = current.to_dict("records")
        charts = {key: value.to_dict("records") for key, value in features.items()}
        taken = set()
        position = None
        for i in range(len(rows) - 1):
            row, nxt = rows[i], rows[i + 1]
            assert nxt["datetime"] == row["datetime"] + timedelta(minutes=1)
            clock = nxt["datetime"].time()
            if position:
                brick = charts[position["setup"]][i]
                position["active"] = strategy.profit_active(position["side"], position["price"],
                                                             row["close"], position["active"])
                cutoff = time(17) if position["setup"] == "morning" else time(23)
                if clock >= cutoff or strategy.channel_exit(position["side"], brick, position["active"]):
                    direction = 1 if position["side"] == "BUY" else -1
                    results.append((str(day), position["setup"], position["side"],
                                    str(position["signal"]), str(position["entry"]),
                                    str(nxt["datetime"]), position["price"],
                                    float(nxt["open"]) - direction * 5))
                    position = None
                    continue
            if position or clock >= time(23):
                continue
            setup = "morning" if clock < time(17) else "evening"
            if setup in taken:
                continue
            side = strategy.entry_side(row["datetime"], charts[setup][i])
            if side:
                direction = 1 if side == "BUY" else -1
                position = dict(setup=setup, side=side, signal=row["datetime"], entry=nxt["datetime"],
                                price=float(nxt["open"]) + direction * 5, active=False)
                taken.add(setup)
        assert position is None
    actual = pd.DataFrame(results, columns=["date", "setup", "side", "signal_time", "entry_time",
                                           "exit_time", "entry_price", "exit_price"])
    columns = list(actual.columns)
    wanted = expected[columns].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, wanted, check_dtype=False)
    print(f"Holiday-aware research parity passed: {len(actual)} trades, entries, exits and prices")


if __name__ == "__main__":
    basic_checks()
    if "--replay" in sys.argv:
        replay_checks()
