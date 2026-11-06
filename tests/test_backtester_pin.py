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
# Pins (recorded against the pre-optimisation loop at 5fd7948)
# --------------------------------------------------------------------------

PINS: dict = {'duckdb_path': {'bars_sha': '8fb0c3a6750558b57d9805ffd7294f9108c53433da8f957843a5cd1fcac505c9',
                 'equity_sha': 'f48e6ad503c0a4340482d92dc27992227cd5bc6f2dca78170250aeeefbfb68e5',
                 'final_equity': '100064.40145029772',
                 'metrics': '0.0006440145029771305|13.81108495988631|-0.00012117556011932124|1.0|56.62580967317477|5.314722724144796|inf|64.44314422579795|0.007017543859649123',
                 'n_equity': 2850,
                 'n_signals': 2,
                 'n_trades': 2,
                 'signals_sha': '23290a9b4d21737ff64b0acb2dbd600a08ce958873510e5fe48d974f2b1ef4f8',
                 'trades_sha': '995aca4b600fc756f863a19bdfef1e82cab9036af134eb6e8fa9430ee4768c86'},
 'dupes_every_bar': {'next_open': {'bars_sha': '29a5846b4e1198294985ffc3443f954d995277c8a34d83d5dea432612cdd4e4b',
                                   'equity_sha': 'a74e4e33de7688609aedd5389dce1d00bb4856e649c811fa26fe7262a4ec074d',
                                   'final_equity': '99984.27685064577',
                                   'metrics': '-0.00015723149354229804|-22.702633804148878|-0.00017905268963191027|0.4748201438848921|-29.744807863378416|-0.8781297497710232|0.6889698450378908|-5.728629411956583|0.643859649122807',
                                   'n_equity': 570,
                                   'n_signals': 295,
                                   'n_trades': 278,
                                   'signals_sha': '6e52f0a8653797c0af3b8bcae258f4c0fb9eac75289e4707250a6d9348ce8309',
                                   'trades_sha': '3a44d82e051beabd239331566760112a9d8b7724fb984ce677283b1240127b43'},
                     'same_close': {'bars_sha': '704b0b3923f6dd0f5f40793447453c70fe39288059f23bfd21209171e5839080',
                                    'equity_sha': '2b1b40d73390e3d06be14bd229b31c30299d1968f77e2619b1a911dad38b1644',
                                    'final_equity': '99988.24410972808',
                                    'metrics': '-0.00011755890271913927|-16.952471189786372|-0.00014303759651877924|0.4748201438848921|-23.071204615690302|-0.8218741476385553|0.746359568202549|-3.300720519337728|0.643859649122807',
                                    'n_equity': 570,
                                    'n_signals': 296,
                                    'n_trades': 279,
                                    'signals_sha': 'bf463b34feef534a7902798f00c2183e4883c6ea60c6da7b103921ea530a4f19',
                                    'trades_sha': '241a75a9798d6a3438e82e0b6c24aeb4ec9a27d57461426511219b52a1e3ccba'}},
 'legacy_fallback': {'next_open': {'bars_sha': '15f6c1f7ec3ea943a3d0879060dcfac2487b7907cab28fdb63e947cb29033c69',
                                   'equity_sha': '8a7194e4f54891414558ffddc3a878b1ce18f61dab8fdef469dba294fad55ff7',
                                   'final_equity': '99102.65634107003',
                                   'metrics': '-0.008973436589299721|-13.718604597087177|-0.013356276576079254|0.35294117647058826|-18.45875634432741|-0.6718516600181007|0.5666194336601684|-18.310637986802696|0.5692982456140351',
                                   'n_equity': 1140,
                                   'n_signals': 112,
                                   'n_trades': 102,
                                   'signals_sha': 'efedef2d645b9b562ac27c6895ca767241e4323e2e69d8b051ad7b3ab2304243',
                                   'trades_sha': '0e97c6a10630c8434d2e3b510706a51af4d7f3378100ca59cba6b3f9261f71e5'},
                     'same_close': {'bars_sha': '19a6d0b9187dd782c9e6c7d7e448df4e63a3e5c9c42a1775c51d3e3a5263d695',
                                    'equity_sha': 'cc8b5c64e82b7c84a8dd56d1b966c20d94142e130abeddd6b10f4e4ef792a07b',
                                    'final_equity': '99205.53496574677',
                                    'metrics': '-0.007944650342532222|-12.09047458321812|-0.01219462671535558|0.39215686274509803|-16.45462060471131|-0.6514877845771407|0.5993732005059028|-16.19940824552909|0.5684210526315789',
                                    'n_equity': 1140,
                                    'n_signals': 113,
                                    'n_trades': 103,
                                    'signals_sha': 'dc555eebb7eccb660d8ef51485bda0dbf4372916d44437dc8ca8ce0019c77ddc',
                                    'trades_sha': '6e015310dc2064359c090a53e1538810558852b08c78889d90d70d2f64d4d55b'}},
 'orb_fixed_bps': {'equity_sha': 'f3a0b01c0b3dc619149c868a2b7dcaa6a3a962d40a14847e17f33a48f0d69fde',
                   'final_equity': '99573.56206171992',
                   'metrics': '-0.004264379382800887|-8.943353149974337|-0.004411390357206914|0.0|-11.79492496580812|-0.966674684736105|0.0|-214.54313194195365|0.07090643274853801',
                   'n_equity': 6840,
                   'n_signals': 6,
                   'n_trades': 4,
                   'signals_sha': 'f7da72439d5dad76594780269a1c576870f98c56034507c90d6b73d52bcba8f7',
                   'trades_sha': '1ae2d083cc2b538f31ecc952462348879e7bf83d7c7ac375819fdf4670d5672d'},
 'orb_multi_live': {'bars_sha': '96b7454d37a908645d7eddd9b5ce798673edb619b863303825029fee0a39087a',
                    'equity_sha': '3353acd385faedc4c3f7fb22980d8804136f3ad36dc1658913446fb64a7186f3',
                    'final_equity': '99999.23189834361',
                    'metrics': '-7.681016563898524e-06|-0.9790839829821683|-2.472804194704761e-05|0.4|-1.3265452953979426|-0.31061968352959685|0.7536853130232212|-0.8283157591427323|0.0012332628611698379',
                    'n_equity': 5676,
                    'n_signals': 14,
                    'n_trades': 10,
                    'signals_sha': 'a14420c19ca57694a2a4734dba469fe74d4d561b3d3fd485050ed06a258156b5',
                    'trades_sha': '098e59e6d9508e93b33f06087a7c2fb60003cac9c514d918ee80a66b9c2c60e9'},
 'orb_pyramid': {'bars_sha': '987a6ad87221030aee6f5e08f8f61b704a096a11a9c0d449b6c8bf9600833339',
                 'equity_sha': '9d1134f96644bc7390d3ffef297dcfb3db3cda48ba165fed5414e5dc9e8b39bb',
                 'final_equity': '100031.9721883236',
                 'metrics': '0.0003197218832360349|1.8893702139723552|-0.0006672768301040254|1.0|2.7651771520672894|0.47914429036325407|inf|80.97490335154592|0.08105263157894736',
                 'n_equity': 5700,
                 'n_signals': 5,
                 'n_trades': 3,
                 'signals_sha': 'd299a38cf75ed40c6860a9d8842ec01d0fca793bd288021cb3e89e3927e91bfb',
                 'trades_sha': '01870408a0b4f1929b8e65f04afcf84791e8a5e4d01d735b2fa055b9c0af4e2c'},
 'orb_same_close': {'bars_sha': '8c13f9746fb0053e665a1ceb3673a007c7dc079658c4ecc92d6597024a497f74',
                    'equity_sha': '228b90961bf4a7e64fef7628922830897d6beb84b816e88c6c6442ff3c393887',
                    'final_equity': '99893.57437985961',
                    'metrics': '-0.0010642562014039347|-6.132130101087781|-0.001406508200361968|0.0|-7.146900142572641|-0.7566654791845835|0.0|-18.103643876928505|0.007236842105263158',
                    'n_equity': 4560,
                    'n_signals': 12,
                    'n_trades': 12,
                    'signals_sha': 'cba614c8dd0ab614f70236651a5b8c8c175b0cf3f85ea3741a82f63342743e59',
                    'trades_sha': '03f4002dafa3ea38dbdbd4b1142cc76370015ce1c2d026d19e4fc8ef368b67ac'},
 'orb_single': {'bars_sha': '4640a6f67d494b9448d451c6e28d3be10d370b20d4dafb052a02489ce3a45d44',
                'equity_sha': 'ec46117ba50a81199f289f66d2ada426c86a00bf30f2478617d7076d878ba4c9',
                'final_equity': '104815.9287632487',
                'metrics': '0.048159287632487|11.415794865897793|-0.07218377096973869|0.5|16.640574839238486|0.6671761115483511|57.186935873658534|556.7527688767885|0.7599415204678363',
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
