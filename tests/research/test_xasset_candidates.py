"""C02 cross-asset family: every registered candidate is long-only / flat,
gross <= 1.0, weights no ticker before its first bar, executes through
``Backtester.run_panel`` without overdrawing cash, and passes the harness's
own lookahead check on the REAL dev panel (skipped only when the research
config is absent). Parametrised over ``research/lowfreq/xasset/candidates.yaml``
so a newly registered candidate is covered automatically.
"""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trader.research.lowfreq import config as _cfg
from trader.research.lowfreq import engine
from trader.research.lowfreq.costs import base_costs
from trader.research.lowfreq.loaders import PricePanel
from trader.research.lowfreq.registry import default_cell, expand_grid, load_family

REPO = Path(__file__).resolve().parents[2]
FAMILY = load_family('xasset', REPO / 'research' / 'lowfreq')
CANDIDATES = [e for e in FAMILY.entries.values() if e['role'] == 'candidate']
IDS = [e['id'] for e in CANDIDATES]
LATE = 'XLRE'          # listed 300 sessions after the others
LATE_START = 300


def synthetic_panel(n_days: int = 900, seed: int = 7) -> PricePanel:
    """SPY/IEF/GLD/BIL/XLRE total-return panel with realistic scale, XLRE late,
    a price close that differs from the total-return close (dividends) and
    volume/dollar_volume fields so the carry proxy is computable."""
    rng = np.random.default_rng(seed)
    days = pd.bdate_range('2015-01-02', periods=n_days)
    spec = {'SPY': (0.08, 0.16), 'IEF': (0.02, 0.06), 'GLD': (0.03, 0.15), 'BIL': (0.01, 0.002), LATE: (0.05, 0.18)}
    close, opn, vol, dv, high, low = {}, {}, {}, {}, {}, {}
    for k, (t, (mu, sig)) in enumerate(spec.items()):
        rets = rng.normal(mu / 252, sig / np.sqrt(252), n_days)
        tr = 100.0 * (k + 1) * np.cumprod(1 + rets)
        px = tr / np.linspace(1.0, 1.0 + 0.02 * (k + 1), n_days)     # price lags total return: a yield
        s = pd.Series(tr, index=days)
        if t == LATE:
            s.iloc[:LATE_START] = np.nan
        close[t] = s
        opn[t] = s.shift(1).fillna(s)
        high[t] = s * 1.002
        low[t] = s * 0.998
        v = pd.Series(1e6 * (k + 1), index=days).where(s.notna())
        vol[t] = v
        dv[t] = v * pd.Series(px, index=days).where(s.notna())
    fields = {k: pd.DataFrame(v) for k, v in (('high', high), ('low', low), ('volume', vol), ('dollar_volume', dv))}
    return PricePanel(open=pd.DataFrame(opn), close=pd.DataFrame(close), fields=fields, universe='etf')


@pytest.fixture(scope='module')
def panel() -> PricePanel:
    return synthetic_panel()


@pytest.fixture(scope='module')
def wide(panel):
    return engine.wide_panel(panel.frames())


def _cells(entry):
    return expand_grid(entry)


@pytest.mark.parametrize('entry', CANDIDATES, ids=IDS)
def test_registry_entry_is_pre_registered(entry):
    for k in ('hypothesis', 'rationale', 'registered_at'):
        assert entry.get(k), f'{entry["id"]}: {k} missing'
    assert set(entry['benchmarks']) == {'etf_ew', 'etf_6040'}, 'D04: must beat BOTH benchmarks'
    assert (REPO / 'research' / 'lowfreq' / 'xasset' / 'PREREG.md').read_text().count(entry['id']) >= 1


@pytest.mark.parametrize('entry', CANDIDATES, ids=IDS)
def test_weights_long_only_gross_le_one_every_cell(entry, panel, wide):
    for cell in _cells(entry):
        strat = engine.make_strategy(entry, cell, panel)
        state = strat.precompute_panel(wide)
        w = state['weights']
        assert list(w.columns) == list(wide['close'].columns)
        assert (w >= 0).all().all(), f'{entry["id"]} {cell}: negative weight'
        assert (w.sum(axis=1) <= 1.0 + 1e-9).all(), f'{entry["id"]} {cell}: gross > 1'
        # every rebalance instruction the executor would see is explicit, long-only, gross <= 1
        n_due = 0
        for i in range(len(wide) - 1):
            instr = strat.on_panel(wide, state, i)
            if instr is None:
                continue
            n_due += 1
            vals = np.array(list(instr.values()), dtype=float)
            assert set(instr) == set(wide['close'].columns)          # every column explicit
            assert vals.min() >= 0 and vals.sum() <= 1.0 + 1e-9
        assert n_due >= 12, f'{entry["id"]} {cell}: only {n_due} rebalances in {len(wide)} bars'


@pytest.mark.parametrize('entry', CANDIDATES, ids=IDS)
def test_no_ticker_weighted_before_its_first_bar(entry, panel, wide):
    close = wide['close']
    assert close[LATE].iloc[:LATE_START].isna().all()
    for cell in _cells(entry):
        strat = engine.make_strategy(entry, cell, panel)
        w = strat.precompute_panel(wide)['weights']
        assert float(w[LATE].iloc[:LATE_START].abs().sum()) == 0.0, f'{entry["id"]} {cell}: {LATE} weighted before listing'
        assert int(((w > 0) & close.isna()).sum().sum()) == 0, f'{entry["id"]} {cell}: weight on a name with no close'


@pytest.mark.parametrize('entry', CANDIDATES, ids=IDS)
def test_executes_without_overdrawing_cash(entry, panel):
    cell = default_cell(entry)
    start = panel.close.index[0]
    res = engine.run_window(entry, cell, panel, start, start + pd.Timedelta(days=400),
                            panel.close.index[-1] + pd.Timedelta(days=1), base_costs('etf'))
    assert res.n_trades > 0
    assert res.min_cash_fraction is not None and res.min_cash_fraction >= -1e-9     # D06 / long-only
    assert len(res.returns) > 200 and np.isfinite(res.returns).all()


def test_holds_bil_when_no_risk_asset_is_eligible(panel, wide):
    """C0: unallocated weight is BIL, not cash, whenever BIL prints."""
    entry = FAMILY.get('xa_tsmom_rp')
    strat = engine.make_strategy(entry, default_cell(entry), panel)
    w = strat.precompute_panel(wide)['weights']
    early = w.iloc[100]                       # before any 252-day signal exists
    assert early['BIL'] == pytest.approx(1.0) and early.drop('BIL').abs().sum() == 0.0


def test_late_starter_joins_once_it_has_history(panel, wide):
    entry = FAMILY.get('xa_faber_sma')
    strat = engine.make_strategy(entry, {'SMA_DAYS': 126}, panel)
    w = strat.precompute_panel(wide)['weights']
    assert float(w[LATE].iloc[LATE_START + 126 - 2]) == 0.0
    assert (w[LATE].iloc[LATE_START + 126:] > 0).any()


def test_carry_yield_proxy_recovers_distribution_yield(panel, wide):
    """The TR/price growth ratio minus one is the trailing distribution yield;
    the synthetic panel builds a known 2%*(k+1)/(n-1)-per-day price lag."""
    from strategies.lowfreq_xasset_carry import Carry
    entry = FAMILY.get('xa_carry')
    strat = engine.make_strategy(entry, {'TOP_N': 5}, panel)
    assert isinstance(strat, Carry)
    risk = wide['close'].drop(columns=['BIL'])
    price = strat.price_close(risk)
    yld = (risk / risk.shift(252)) / (price / price.shift(252)) - 1
    row = yld.iloc[-1].dropna()
    assert (row > 0).all() and row.idxmax() == LATE       # the largest constructed lag


# ---------------------------------------------------------------- real dev panel
def _research_config_present() -> bool:
    try:
        paths = _cfg.research_db_paths()
    except Exception:                                   # noqa: BLE001
        return False
    return all(Path(p).exists() for p in paths.values())


REAL = pytest.mark.skipif(not _research_config_present() or bool(os.environ.get('LOWFREQ_RESEARCH_ROOT')),
                          reason='research_dev config/databases not present on this machine')


@pytest.fixture(scope='module')
def real_panel():
    from trader.research.lowfreq import load_etf_panel
    return load_etf_panel(None, dt.date(2006, 2, 1), dt.date(2024, 6, 28))


@REAL
@pytest.mark.parametrize('entry', CANDIDATES, ids=IDS)
def test_no_lookahead_on_real_dev_panel(entry, real_panel):
    """The harness's own check (assert_no_panel_lookahead + fresh-state prefix
    replay) on the real 49-ETF dev panel, for the pre-registered default cell."""
    state = engine.lookahead_check(entry, default_cell(entry), real_panel)
    w = state['weights']
    close = real_panel.close.reindex(index=w.index, columns=w.columns)
    assert int(((w > 0) & close.isna()).sum().sum()) == 0
    assert (w.sum(axis=1) <= 1.0 + 1e-9).all() and (w >= 0).all().all()
