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


def replay_checks(morning_cutoff=time(17), evening_last_entry=time(22, 59),
                  evening_cutoff=time(23)):
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
                           & (data.datetime.dt.time <= evening_cutoff)].reset_index(drop=True)
        history = strategy.chart_history(data, day, datetime.combine(day, evening_cutoff) + timedelta(minutes=1))
        features = {}
        for setup, box in (("morning", 0.1), ("evening", 0.15)):
            end = (datetime.combine(day, time(17)) if setup == "morning"
                   else datetime.combine(day, evening_cutoff) + timedelta(minutes=1))
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
                cutoff = morning_cutoff if position["setup"] == "morning" else evening_cutoff
                if clock >= cutoff or strategy.channel_exit(position["side"], brick, position["active"]):
                    direction = 1 if position["side"] == "BUY" else -1
                    results.append((str(day), position["setup"], position["side"],
                                    str(position["signal"]), str(position["entry"]),
                                    str(nxt["datetime"]), position["price"],
                                    float(nxt["open"]) - direction * 5))
                    position = None
                    continue
            if position or clock >= evening_cutoff:
                continue
            if morning_cutoff <= clock < time(17):
                continue
            if clock >= time(17) and clock > evening_last_entry:
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
    if morning_cutoff == time(17) and evening_cutoff == time(23) and evening_last_entry == time(22, 59):
        pd.testing.assert_frame_equal(actual, wanted, check_dtype=False)
        print(f"Holiday-aware research parity passed: {len(actual)} trades, entries, exits and prices")
    return actual


def compare_cutoffs():
    from tamingnifty.charges import money, silver_micro_estimated_order_charges, sum_charges

    baseline = replay_checks()
    changed = replay_checks(time(16, 55))
    for label, frame in (("17:00", baseline), ("16:55", changed)):
        values = []
        for trade in frame.to_dict("records"):
            direction = 1 if trade["side"] == "BUY" else -1
            exit_side = "SELL" if trade["side"] == "BUY" else "BUY"
            fees = sum_charges(silver_micro_estimated_order_charges(trade["side"], trade["entry_price"], 1),
                               silver_micro_estimated_order_charges(exit_side, trade["exit_price"], 1))
            values.append(money(direction * (trade["exit_price"] - trade["entry_price"]) - fees["total"]))
        wins = [value for value in values if value > 0]
        losses = [value for value in values if value < 0]
        print(f"{label}: trades={len(values)}, estimated_net={money(sum(values))}, "
              f"wins={len(wins)}, losses={len(losses)}, "
              f"profit_factor={sum(wins) / -sum(losses):.3f}")
    compared = baseline.merge(changed, on=["date", "setup"], how="outer", suffixes=("_old", "_new"),
                              indicator=True)
    for row in compared.to_dict("records"):
        if (row["_merge"] != "both" or row["entry_time_old"] != row["entry_time_new"]
                or row["exit_time_old"] != row["exit_time_new"]
                or row["exit_price_old"] != row["exit_price_new"]):
            print("Changed:", row)


def compare_evening():
    from tamingnifty.charges import money, silver_micro_estimated_order_charges, sum_charges

    old = replay_checks(time(16, 55))
    new = replay_checks(time(16, 55), time(22, 50), time(23, 10))
    for label, frame in (("old evening", old), ("new evening", new)):
        values = []
        for trade in frame.to_dict("records"):
            direction = 1 if trade["side"] == "BUY" else -1
            exit_side = "SELL" if trade["side"] == "BUY" else "BUY"
            fees = sum_charges(silver_micro_estimated_order_charges(trade["side"], trade["entry_price"], 1),
                               silver_micro_estimated_order_charges(exit_side, trade["exit_price"], 1))
            values.append(money(direction * (trade["exit_price"] - trade["entry_price"]) - fees["total"]))
        wins = [value for value in values if value > 0]
        losses = [value for value in values if value < 0]
        print(f"{label}: trades={len(values)}, estimated_net={money(sum(values))}, wins={len(wins)}, "
              f"losses={len(losses)}, profit_factor={sum(wins) / -sum(losses):.3f}")
    compared = old.merge(new, on=["date", "setup"], how="outer", suffixes=("_old", "_new"),
                         indicator=True)
    for row in compared.to_dict("records"):
        if (row["_merge"] != "both" or row["entry_time_old"] != row["entry_time_new"]
                or row["exit_time_old"] != row["exit_time_new"]
                or row["exit_price_old"] != row["exit_price_new"]):
            print("Changed:", row)


if __name__ == "__main__":
    basic_checks()
    if "--replay" in sys.argv:
        replay_checks()
    if "--compare-cutoff" in sys.argv:
        compare_cutoffs()
    if "--compare-evening" in sys.argv:
        compare_evening()
