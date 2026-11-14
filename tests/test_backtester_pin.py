"""Exact result pins for the bar-by-bar backtester (``Backtester.run``).

These pin the COMPLETE output — every trade, every signal, every equity
point, every metric, bit for bit — of deterministic synthetic runs. They were
recorded against the pre-optimisation loop (2026-11-05, commit 5fd7948) before
the per-bar pandas work was replaced with precomputed numpy arrays, so any
behavioural drift in the fast loop fails here.

Scenarios cover every path through the loop that the optimisation touched:
next_open and same_close fills, accumulate and live execution modes,
time-based exits (``close_by_time`` and ``max_hold_bars``), multi-conid
timelines with mismatched timestamps, duplicate-timestamp rows, gappy data,
bar-row-dependent slippage models (the exact ``bar`` Series each fill sees is
pinned too), the legacy ``on_prices`` fallback, and the real DuckDB read path.

If a pin changes because results were *meant* to change, re-record it — and
say why in the commit message. A pin that changes during a pure performance
refactor is a bug in the refactor.

Re-recorded 2026-11-14 (audit B4.2 + B4.4, docs/AUDIT_2026-11-14.md). Two
intended changes moved these pins, and nothing else did:

* **B4.4 — Sharpe and Sortino, every scenario, by exactly sqrt(bars per
  day / 390).** These fixtures have a 08:00-17:30 session, 570 one-minute bars
  a day, and the old factor assumed 390 (98,280 a year). Annualisation now
  counts the curve's own bars, so both ratios rise by sqrt(570/390) = 1.208941
  (``orb_multi_live``: 1.206818, its gappy two-instrument timeline has a
  median of 568 bars a day). No trade, signal, equity point or other metric
  moved in ``orb_single``, ``orb_fixed_bps``, ``orb_pyramid``,
  ``legacy_fallback`` and ``duckdb_path``.
* **B4.2 — the trades of the three scenarios that use ``close_by_time`` on
  UTC-indexed frames** (``orb_multi_live`` and ``orb_same_close`` via
  ``EOD_FLATTEN``; ``dupes_every_bar`` via ``close_by_time=20:00``).
  ``FrameStorage`` hands the backtester the raw UTC index, so their time exits
  used to fire on UTC wall time — ORB's 15:45 flatten at 10:45 New York. The
  exit is now compared on the strategy's session clock (default New York),
  as the live runtime does, so positions are held to 15:45 New York and the
  20:00 rule never fires inside a session that ends at 17:30. With that one
  conversion switched off these three scenarios reproduce their old trades,
  signals and equity exactly, differing only by the B4.4 factor above.
"""
import datetime as dt
import hashlib
import logging

import pytest

from backtest_synth import FrameStorage, make_minute_bars, result_fingerprint
from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.simulation.slippage import SquareRootImpact, VolatilitySlippage
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState

ORB = 'strategies/opening_range_breakout.py'
START = dt.datetime(2023, 1, 1)
END = dt.datetime(2030, 1, 1)


class RecordingSlippage:
    """Wraps a slippage model and records the exact bar Series of every fill."""

    def __init__(self, inner):
        self.inner = inner
        self.seen: list[str] = []

    def calculate(self, price, quantity, action, bar):
        self.seen.append('|'.join([
            str(bar.name), str(bar.dtype), ','.join(map(str, bar.index)),
            ','.join(repr(v) for v in bar.tolist()), repr(float(price)), repr(float(quantity)),
            str(action),
        ]))
        return self.inner.calculate(price, quantity, action, bar)

    def digest(self) -> str:
        return hashlib.sha256('\n'.join(self.seen).encode()).hexdigest()


class SmaCrossLegacy(Strategy):
    """on_prices-only strategy: exercises the default on_bar fallback."""

    def on_prices(self, prices):
        if len(prices) < 21:
            return None
        c = prices['close']
        fast_now, slow_now = c.iloc[-5:].mean(), c.iloc[-20:].mean()
        fast_prev, slow_prev = c.iloc[-6:-1].mean(), c.iloc[-21:-1].mean()
        if fast_now > slow_now and fast_prev <= slow_prev:
            return Signal(source_name='sma_legacy', action=Action.BUY, probability=0.7,
                          risk=0.3, max_hold_bars=30)
        if fast_now < slow_now and fast_prev >= slow_prev:
            return Signal(source_name='sma_legacy', action=Action.SELL, probability=0.7,
                          risk=0.3)
        return None


class AlternatingOnBar(Strategy):
    """Fast-path (on_bar) strategy that trades constantly.

    Run on frames full of duplicate timestamps so fills land on multi-row
    bars, pinning which row supplies the open (first) / close and bar Series
    (last) and that the per-conid bar index advances once per timestamp.
    """

    def on_bar(self, prices, state, index):
        if index % 7 == 0:
            return Signal(source_name='alt', action=Action.BUY, probability=0.5, risk=0.5,
                          quantity=5, close_by_time=dt.time(20, 0) if index % 2 else None,
                          max_hold_bars=4 if index % 3 == 0 else None)
        if index % 7 == 3:
            return Signal(source_name='alt', action=Action.SELL, probability=0.5, risk=0.5)
        return None


def _install(strategy, conids):
    ua = UniverseAccessor.__new__(UniverseAccessor)
    ctx = StrategyContext(
        name=strategy.__class__.__name__, bar_size=BarSize.Mins1, conids=conids,
        universe=None, historical_days_prior=0, paper_only=False,
        storage=None, universe_accessor=ua, logger=logging.getLogger('pin'),
    )
    strategy.install(ctx)
    strategy.state = StrategyState.RUNNING
    return strategy


def _cfg(**kw):
    return BacktestConfig(start_date=START, end_date=END, bar_size=BarSize.Mins1, **kw)


def _run_orb(frames, params=None, **cfg_kw):
    slip = RecordingSlippage(cfg_kw.pop('slippage_inner', SquareRootImpact()))
    bt = Backtester(FrameStorage(frames), _cfg(slippage_model=slip, **cfg_kw))
    res = bt.run_from_module(ORB, 'OpeningRangeBreakout', list(frames), params=params)
    fp = result_fingerprint(res)
    fp['bars_sha'] = slip.digest()
    return fp


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------

def scenario_orb_single(tmp_path):
    frames = {208813719: make_minute_bars(n_days=12, seed=7)}
    return _run_orb(frames)


def scenario_orb_fixed_bps(tmp_path):
    """The CLI default path: FixedBPS slippage, accumulate, next_open."""
    df = make_minute_bars(n_days=12, seed=3)
    bt = Backtester(FrameStorage({1: df}), _cfg())
    res = bt.run_from_module(ORB, 'OpeningRangeBreakout', [1])
    return result_fingerprint(res)


def scenario_orb_multi_live(tmp_path):
    frames = {
        1001: make_minute_bars(n_days=10, seed=11, drop_frac=0.05, duplicate_every=97),
        2002: make_minute_bars(n_days=10, seed=23, base_price=50.0, drop_frac=0.10),
    }
    return _run_orb(frames, params={'EOD_FLATTEN': True, 'MAX_HOLD_BARS': 45},
                    execution_mode='live')


def scenario_orb_same_close(tmp_path):
    frames = {
        1001: make_minute_bars(n_days=8, seed=5, duplicate_every=53),
        2002: make_minute_bars(n_days=8, seed=9, base_price=40.0, drop_frac=0.2),
    }
    return _run_orb(frames, params={'EOD_FLATTEN': True, 'MAX_HOLD_BARS': 30},
                    fill_policy='same_close', slippage_inner=VolatilitySlippage())


def scenario_orb_pyramid(tmp_path):
    frames = {1001: make_minute_bars(n_days=10, seed=31)}
    return _run_orb(frames, execution_mode='pyramid_fixed', pyramid_max_adds=2)


def scenario_legacy_fallback(tmp_path):
    out = {}
    for policy in ('next_open', 'same_close'):
        frames = {
            7: make_minute_bars(n_days=2, seed=17, duplicate_every=41),
            8: make_minute_bars(n_days=2, seed=19, base_price=30.0, drop_frac=0.15),
        }
        slip = RecordingSlippage(SquareRootImpact())
        bt = Backtester(FrameStorage(frames), _cfg(slippage_model=slip, fill_policy=policy))
        res = bt.run(_install(SmaCrossLegacy(), [7, 8]), [7, 8])
        fp = result_fingerprint(res)
        fp['bars_sha'] = slip.digest()
        out[policy] = fp
    return out


def scenario_dupes_every_bar(tmp_path):
    out = {}
    for policy in ('next_open', 'same_close'):
        frames = {
            11: make_minute_bars(n_days=1, seed=41, duplicate_every=3),
            12: make_minute_bars(n_days=1, seed=43, base_price=20.0, drop_frac=0.3,
                                 duplicate_every=5),
        }
        slip = RecordingSlippage(SquareRootImpact())
        bt = Backtester(FrameStorage(frames), _cfg(slippage_model=slip, fill_policy=policy))
        res = bt.run(_install(AlternatingOnBar(), [11, 12]), [11, 12])
        fp = result_fingerprint(res)
        fp['bars_sha'] = slip.digest()
        out[policy] = fp
    return out


def scenario_duckdb_path(tmp_path):
    db = str(tmp_path / 'pin_history.duckdb')
    df = make_minute_bars(n_days=5, seed=13)
    DuckDBDataStore(db).write(
        '4391', df[['open', 'high', 'low', 'close', 'volume', 'average', 'bar_count']])
    slip = RecordingSlippage(SquareRootImpact())
    bt = Backtester(TickStorage(db), _cfg(slippage_model=slip))
    res = bt.run_from_module(ORB, 'OpeningRangeBreakout', [4391],
                             params={'MAX_HOLD_BARS': 20})
    fp = result_fingerprint(res)
    fp['bars_sha'] = slip.digest()
    return fp


SCENARIOS = {
    'orb_single': scenario_orb_single,
    'orb_fixed_bps': scenario_orb_fixed_bps,
    'orb_multi_live': scenario_orb_multi_live,
    'orb_same_close': scenario_orb_same_close,
    'orb_pyramid': scenario_orb_pyramid,
    'legacy_fallback': scenario_legacy_fallback,
    'dupes_every_bar': scenario_dupes_every_bar,
    'duckdb_path': scenario_duckdb_path,
}

# --------------------------------------------------------------------------
# Pins (recorded against the pre-optimisation loop at 5fd7948; the values the
# 2026-11-14 audit fixes moved were re-recorded — see the module docstring)
# --------------------------------------------------------------------------

PINS: dict = {'duckdb_path': {'bars_sha': '8fb0c3a6750558b57d9805ffd7294f9108c53433da8f957843a5cd1fcac505c9',
                 'equity_sha': 'f48e6ad503c0a4340482d92dc27992227cd5bc6f2dca78170250aeeefbfb68e5',
                 'final_equity': '100064.40145029772',
                 'metrics': '0.0006440145029771305|16.69678754826522|-0.00012117556011932124|1.0|68.45726578379427|5.314722724144796|inf|64.44314422579795|0.007017543859649123',
                 'n_equity': 2850,
                 'n_signals': 2,
                 'n_trades': 2,
                 'signals_sha': '23290a9b4d21737ff64b0acb2dbd600a08ce958873510e5fe48d974f2b1ef4f8',
                 'trades_sha': '995aca4b600fc756f863a19bdfef1e82cab9036af134eb6e8fa9430ee4768c86'},
 'dupes_every_bar': {'next_open': {'bars_sha': '2d6bab3c59399506c666cc0af8cbfe47d484d6ae6beffbbdf7e212a48880c221',
                                   'equity_sha': '8fbe9ddace362b9b7220c8b599c3d9391861936f3540242ebc3e403a069ae9a5',
                                   'final_equity': '99987.50171909451',
                                   'metrics': '-0.00012498280905492987|-21.230393979242926|-0.00016614228809032258|0.5072463768115942|-28.207436179902835|-0.7522636800751382|0.7591529979500097|-4.679609873725168|0.6824561403508772',
                                   'n_equity': 570,
                                   'n_signals': 277,
                                   'n_trades': 277,
                                   'signals_sha': 'b07b12724741ff27ca4b4e78e869153d106b9aa5f6f1da0c9774dad53b7b13b8',
                                   'trades_sha': 'c43169301c8b49cb35cbc8790fddfc46744a19cf56cc7338b50ebee7190cebd8'},
                     'same_close': {'bars_sha': '433303a309c55edccaa5ffd4800c73d83b82948382e6a026449b482909f138df',
                                    'equity_sha': '0f486711f21376ea97315f95fd7dc4580e285b9512060cb6a90245016c7d2f8d',
                                    'final_equity': '99990.05317462294',
                                    'metrics': '-9.946825377060708e-05|-16.97892114017041|-0.00014113627638374958|0.4927536231884058|-23.316769725934435|-0.7047674511417098|0.7930721071915465|-3.3306205142581207|0.6789473684210526',
                                    'n_equity': 570,
                                    'n_signals': 278,
                                    'n_trades': 278,
                                    'signals_sha': '0ffbdf68bfbcf2be9d23f7a63f77b3b625bf9d7d2a9a585c28419e32af3e6389',
                                    'trades_sha': 'f19090070f8bf65c5d7e5455bd7169a37e33f33393cf47043d8faa69f4ecb15b'}},
 'legacy_fallback': {'next_open': {'bars_sha': '15f6c1f7ec3ea943a3d0879060dcfac2487b7907cab28fdb63e947cb29033c69',
                                   'equity_sha': '8a7194e4f54891414558ffddc3a878b1ce18f61dab8fdef469dba294fad55ff7',
                                   'final_equity': '99102.65634107003',
                                   'metrics': '-0.008973436589299721|-16.584984241390455|-0.013356276576079254|0.35294117647058826|-22.315548270218194|-0.6718516600181007|0.5666194336601684|-18.310637986802696|0.5692982456140351',
                                   'n_equity': 1140,
                                   'n_signals': 112,
                                   'n_trades': 102,
                                   'signals_sha': 'efedef2d645b9b562ac27c6895ca767241e4323e2e69d8b051ad7b3ab2304243',
                                   'trades_sha': '0e97c6a10630c8434d2e3b510706a51af4d7f3378100ca59cba6b3f9261f71e5'},
                     'same_close': {'bars_sha': '19a6d0b9187dd782c9e6c7d7e448df4e63a3e5c9c42a1775c51d3e3a5263d695',
                                    'equity_sha': 'cc8b5c64e82b7c84a8dd56d1b966c20d94142e130abeddd6b10f4e4ef792a07b',
                                    'final_equity': '99205.53496574677',
                                    'metrics': '-0.007944650342532222|-14.616671033450451|-0.01219462671535558|0.39215686274509803|-19.892666305517658|-0.6514877845771407|0.5993732005059028|-16.19940824552909|0.5684210526315789',
                                    'n_equity': 1140,
                                    'n_signals': 113,
                                    'n_trades': 103,
                                    'signals_sha': 'dc555eebb7eccb660d8ef51485bda0dbf4372916d44437dc8ca8ce0019c77ddc',
                                    'trades_sha': '6e015310dc2064359c090a53e1538810558852b08c78889d90d70d2f64d4d55b'}},
 'orb_fixed_bps': {'equity_sha': 'f3a0b01c0b3dc619149c868a2b7dcaa6a3a962d40a14847e17f33a48f0d69fde',
                   'final_equity': '99573.56206171992',
                   'metrics': '-0.004264379382800887|-10.811986744556183|-0.004411390357206914|0.0|-14.259368968753973|-0.966674684736105|0.0|-214.54313194195365|0.07090643274853801',
                   'n_equity': 6840,
                   'n_signals': 6,
                   'n_trades': 4,
                   'signals_sha': 'f7da72439d5dad76594780269a1c576870f98c56034507c90d6b73d52bcba8f7',
                   'trades_sha': '1ae2d083cc2b538f31ecc952462348879e7bf83d7c7ac375819fdf4670d5672d'},
 'orb_multi_live': {'bars_sha': '159e1afc61e865c871a6f8315cb573228a7519b88f1680ba96317fcd70994630',
                    'equity_sha': 'b67846c3f06de463bfd76a624c04ac416f7f8d5211e843276f11a4a3f52e44a2',
                    'final_equity': '99957.86212261002',
                    'metrics': '-0.0004213787738998054|-4.6582051180018516|-0.0007671941952461388|0.4|-6.151168833128382|-0.5492465617060808|0.4200681084639955|-42.934043155269144|0.03400281888653982',
                    'n_equity': 5676,
                    'n_signals': 14,
                    'n_trades': 10,
                    'signals_sha': '358cfdc9068820a120d9695ce45173866f40598fcdde81b74c60a5afa2cd98ce',
                    'trades_sha': '5057b677d244628d9dfefbe72bb601de8d8cbe8aba0acd733381135cf33f5d5d'},
 'orb_pyramid': {'bars_sha': '987a6ad87221030aee6f5e08f8f61b704a096a11a9c0d449b6c8bf9600833339',
                 'equity_sha': '9d1134f96644bc7390d3ffef297dcfb3db3cda48ba165fed5414e5dc9e8b39bb',
                 'final_equity': '100031.9721883236',
                 'metrics': '0.0003197218832360349|2.2841372096646992|-0.0006672768301040254|1.0|3.342936168699425|0.47914429036325407|inf|80.97490335154592|0.08105263157894736',
                 'n_equity': 5700,
                 'n_signals': 5,
                 'n_trades': 3,
                 'signals_sha': 'd299a38cf75ed40c6860a9d8842ec01d0fca793bd288021cb3e89e3927e91bfb',
                 'trades_sha': '01870408a0b4f1929b8e65f04afcf84791e8a5e4d01d735b2fa055b9c0af4e2c'},
 'orb_same_close': {'bars_sha': 'c7bcb38171fe506f99fa67368fc6096c494454be1ea50f9ba4d40fcfab1a24b0',
                    'equity_sha': '34e697b35453346815cc2558a3cfed8ee7dc6991ec861e613ab5bf75f18a73e7',
                    'final_equity': '99872.86060506744',
                    'metrics': '-0.0012713939493256277|-5.094289344832152|-0.001938078219197092|0.3333333333333333|-6.267190093756423|-0.6560075526014328|0.33958282990087485|-23.380075449799502|0.03859649122807018',
                    'n_equity': 4560,
                    'n_signals': 12,
                    'n_trades': 12,
                    'signals_sha': '010acbe29afdfaf13d54925c9e9989b2076039bdfd6a5a6c08e08411a77c2659',
                    'trades_sha': 'bbc8fb054126adb3528c230074b7babc0e3fbe85555d99aad868275fb6166daf'},
 'orb_single': {'bars_sha': '4640a6f67d494b9448d451c6e28d3be10d370b20d4dafb052a02489ce3a45d44',
                'equity_sha': 'ec46117ba50a81199f289f66d2ada426c86a00bf30f2478617d7076d878ba4c9',
                'final_equity': '104815.9287632487',
                'metrics': '0.048159287632487|13.801023027812967|-0.07218377096973869|0.5|20.117474012994546|0.6671761115483511|57.186935873658534|556.7527688767885|0.7599415204678363',
                'n_equity': 6840,
                'n_signals': 8,
                'n_trades': 8,
                'signals_sha': 'd72eec29c8de6a2e759776b996f8834fdf885a388485eaab2cce63f20a618215',
                'trades_sha': '6de3424bcf6a039e09d1ddccff39fe8b84e5d3678d5e354364239c5c92f72c32'}}


@pytest.mark.parametrize('name', list(SCENARIOS))
def test_backtest_result_pinned(name, tmp_path):
    assert SCENARIOS[name](tmp_path) == PINS[name]


def test_pins_exercise_the_paths_they_claim():
    """Guard against a pin silently degenerating into 'no trades'."""
    for name, fp in PINS.items():
        fps = fp.values() if name in ('legacy_fallback', 'dupes_every_bar') else [fp]
        for f in fps:
            assert f['n_trades'] > 0, name
            assert f['n_equity'] > 0, name
