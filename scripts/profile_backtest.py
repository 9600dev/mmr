"""Time / cProfile the bar-by-bar backtester on the seeded synthetic 1-min fixture.

    uv run python scripts/profile_backtest.py [N_DAYS=62] [SORT_KEY]

62 days x 570 bars ~= 35k bars (a 90-day 1-min run). Pass a pstats sort key
(e.g. ``tottime``/``cumulative``) to print a profile. Prints the exact
result fingerprint so runs before/after a change can be compared.
"""
import sys, time, cProfile, pstats, io, datetime as dt, logging
sys.path.insert(0, "tests")
from backtest_synth import make_minute_bars, FrameStorage, result_fingerprint
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.objects import BarSize
n_days = int(sys.argv[1]) if len(sys.argv) > 1 else 62
df = make_minute_bars(n_days=n_days)
print("bars", len(df))
cfg = BacktestConfig(start_date=dt.datetime(2023,1,1), end_date=dt.datetime(2030,1,1), bar_size=BarSize.Mins1)
bt = Backtester(FrameStorage({208813719: df}), cfg)
t = time.perf_counter()
do_prof = len(sys.argv) > 2
if do_prof:
    pr = cProfile.Profile(); pr.enable()
res = bt.run_from_module("strategies/opening_range_breakout.py", "OpeningRangeBreakout", [208813719])
if do_prof:
    pr.disable()
print("elapsed", round(time.perf_counter() - t, 3), "trades", len(res.trades))
print(result_fingerprint(res))
if do_prof:
    s = io.StringIO(); pstats.Stats(pr, stream=s).sort_stats(sys.argv[2]).print_stats(30); print(s.getvalue())
