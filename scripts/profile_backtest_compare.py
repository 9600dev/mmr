"""Run ORB through a given backtester module on a parquet bar dump; print time + fingerprint.

    uv run python scripts/profile_backtest_compare.py PATH/TO/backtester.py BARS.parquet [TZ]

Used to check an optimised backtester against the previous one on real
data: ``git show REV:trader/simulation/backtester.py > /tmp/old.py`` and run
both — the printed fingerprints must be identical. TZ re-localises the
(UTC) parquet index to what TickData.read returns (e.g. America/New_York).
"""
import sys, time, importlib.util, datetime as dt, os
sys.path.insert(0, "tests")
import pandas as pd
from backtest_synth import FrameStorage, result_fingerprint
from trader.objects import BarSize
path = sys.argv[1]
spec = importlib.util.spec_from_file_location("bt_under_test", path)
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
df = pd.read_parquet(sys.argv[2])
if len(sys.argv) > 3:
    df.index = df.index.tz_convert(sys.argv[3])
cfg = m.BacktestConfig(start_date=dt.datetime(2020,1,1), end_date=dt.datetime(2030,1,1), bar_size=BarSize.Mins1)
bt = m.Backtester(FrameStorage({208813719: df}), cfg)
t = time.perf_counter()
res = bt.run_from_module("strategies/opening_range_breakout.py", "OpeningRangeBreakout", [208813719])
print("RESULT", path.split("/")[-1], "bars", len(df), "elapsed", round(time.perf_counter() - t, 3), "trades", len(res.trades))
print("FP", result_fingerprint(res))
