"""Offline MCX calendar and contract-selection checks."""

from datetime import date
from types import SimpleNamespace

import pandas as pd

import market


def main():
    document = dict(_id="MCX:2026", exchange="MCX", year=2026, status="verified",
                    valid_from="2026-01-01", valid_to="2026-12-31", timezone="Asia/Kolkata",
                    holidays=[
                        dict(date="2026-11-24", morning="closed", evening="open"),
                        dict(date="2026-12-25", morning="closed", evening="closed"),
                    ],
                    special_sessions=[dict(date="2026-11-08")])
    calendars = SimpleNamespace(find_one=lambda query: document if query["_id"] == "MCX:2026" else None)
    assert market.session_status(calendars, date(2026, 11, 24)) == dict(morning=False, evening=True, special=False)
    assert market.session_status(calendars, date(2026, 12, 25)) == dict(morning=False, evening=False, special=False)
    assert market.session_status(calendars, date(2026, 11, 8)) == dict(morning=False, evening=False, special=True)
    assert market.session_status(calendars, date(2026, 11, 7)) == dict(morning=False, evening=False, special=False)
    assert market.trading_days_before_expiry(calendars, date(2026, 11, 19), date(2026, 11, 30)) == 7
    assert market.trading_days_before_expiry(calendars, date(2026, 11, 18), date(2026, 11, 30)) == 8
    master = pd.DataFrame([
        dict(EXCH_ID="MCX", SEGMENT="M", INSTRUMENT="FUTCOM", UNDERLYING_SYMBOL="SILVERMIC",
             SM_EXPIRY_DATE="2026-11-30", SECURITY_ID=1, DISPLAY_NAME="NOV", LOT_SIZE=1, TICK_SIZE=100),
        dict(EXCH_ID="MCX", SEGMENT="M", INSTRUMENT="FUTCOM", UNDERLYING_SYMBOL="SILVERMIC",
             SM_EXPIRY_DATE="2027-02-26", SECURITY_ID=2, DISPLAY_NAME="FEB", LOT_SIZE=1, TICK_SIZE=100),
    ])
    assert market.silver_contract(master, calendars, date(2026, 11, 18))["security_id"] == "1"
    assert market.silver_contract(master, calendars, date(2026, 11, 19))["security_id"] == "2"
    assert market.silver_contract(master, calendars, date(2026, 11, 19))["tick_size"] == 1
    try:
        market.session_status(calendars, date(2027, 1, 1))
    except ValueError:
        pass
    else:
        raise AssertionError("Missing calendar must not be treated as open")
    print("MCX session and seven-trading-day rollover checks passed")


if __name__ == "__main__":
    main()
