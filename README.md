# Silver Surfer

Two Azure jobs were started for a one-lot paper forward test on 2026-11-13.
They run on weekdays from 08:58 IST, subject to the MCX calendar. Signal
publication and executor startup were verified; a completed paper trade,
scheduled restart and real-world recovery remain unverified. Live execution
is deliberately disabled; setting `live_trading` to anything other than
`false` stops the executor.

`strategy.py` contains the selected Renko breakout and channel rules.
`signals.py` builds levels from completed candles and identifies missing
regular-session minutes. `signal_job.py` publishes independently rebuilt
session signals, and `executor.py` consumes them for one-lot paper execution.
`paper_state.py` uses atomic state updates, records an exit intent before
requesting a quote, and recovers a closed trade if interrupted during archive.
Paper BUYs use the best ask and SELLs the best bid, with no spread filter.
Insufficient best-level size is recorded and alerted, but the full paper fill
is optimistic. Request duration is checked; the REST response does not establish
an independent exchange order-book timestamp.
`market.py` reads the `MCX:YYYY` calendar and chooses the Silver Micro
contract. The rollover implementation counts trading dates from the current
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
python test_market.py
python -m unittest test_executor test_publisher test_paper_state test_decisions test_paper test_signals
python test_strategy.py
python test_strategy.py --replay
```

The optional replay reads the historical research data in the surrounding vault;
it compares 66 recorded trades' sides, times and simulated prices. It does not
test job execution, fees, live quotes, broker fills or forward profitability.