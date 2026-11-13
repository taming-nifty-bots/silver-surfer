# Silver Surfer

The paper implementation uses separate morning and evening signal and executor
jobs, with the same image and Mongo collections. Morning jobs start at 08:58 IST;
evening jobs start at 16:58 IST, subject to the MCX calendar. Morning entries
end at 16:55, when any open morning position is force-exited. After a session's
trade is closed, its jobs stop. If a morning position remains open
at 17:00, the evening executor waits one minute and then retries its exit.
Evening entries are permitted no later than 22:50:00 IST; any open evening
position is force-exited starting at 23:10 IST. Quote or service failures
can delay the simulated fill, so the executor continues retrying and alerts.
The prior 23:00-cutoff replay does not validate these changed evening rules.
Signal publication and executor startup were verified for the earlier
single-day deployment; a completed paper trade, scheduled restart and
real-world recovery remain unverified. Live execution
is deliberately disabled; setting `live_trading` to anything other than
`false` stops the executor.
Each executor requires the `quantity` environment variable, a positive whole
number of contract units and a multiple of the Dhan lot size. Set it to `1`
for the current one-lot forward test. A position retains its entry quantity
for exit and accounting even if the configured quantity later changes.
Paper accounting currently requires the Silver Micro contract lot size to be
one unit and rejects an unexpected contract lot size.
At each minute boundary, the executor allows up to 15 seconds for a fresh
signal before raising an error; fresh signals and forced exits are not delayed.
Daily contract and session signals reuse fixed document IDs in
`Bots.supertrend`, alongside Interceptor's signals. Open and closed paper
trades use `Bots.silver_surfer_<user>`; this collection contains only trades.
Trade documents follow Interceptor's common identifiers, strategy, broker,
simulation, quantity, signal and date/time fields. Silver keeps `side`,
`entry_price` and `exit_price` for futures and retains its paper quote and
recovery details; option-specific fields do not apply.
The MCX calendar remains shared in `Bots.exchange_calendars`.

`strategy.py` contains the calendar, contract selection, Renko rules, signal
construction and recovery helpers. `signal/signal_job.py` publishes the contract
lock and independently rebuilt session signals. `executor/executor.py` handles quotes, paper
trades, recovery and configured quantity. Entry and exit are recorded in the
same trade document, with an exit intent saved before requesting a quote.
Paper BUYs use the best ask and SELLs the best bid, with no spread filter.
Insufficient best-level size is recorded and alerted, but the full paper fill
is optimistic. Request duration is checked; the REST response does not establish
an independent exchange order-book timestamp.
The `MCX:YYYY` calendar is used to choose the Silver Micro contract. The
rollover implementation counts trading dates from the current
date through the day before expiry, including a date with only an evening or
special session. It selects the next contract at seven or fewer such dates.
Daily contract locks and open-position contract checks are implemented.

The jobs alert on missing observed candles and continue, as requested. On
restart, missed observed candles are reconstructed for activation and channel
exits; missed exits fill only at a current quote. Unobserved candles cannot
be reconstructed. Alert channel defaults to `silver-surfer`. Paper trade
charges are estimates, not reconciled Dhan contract-note rates.

For offline tests, make the `tamingnifty` source or installed package available:

```powershell
$env:PYTHONPATH = "$PWD;$PWD\executor;$PWD\signal"
python tests\test_silver.py
python tests\test_silver.py --replay
python tests\test_silver.py --compare-cutoff
```

The optional replay reads the historical research data in the surrounding vault;
it compares 66 recorded trades' sides, times and simulated prices. It does not
test job execution, fees, live quotes, broker fills or forward profitability.