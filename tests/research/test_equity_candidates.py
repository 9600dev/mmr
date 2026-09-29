"""Equity-family candidates (C03, D05): long-only, gross <= 1, >= 30 names when
invested, weight only inside the published coverage, fundamentals read only
as-of (no dependence on later rows), and the harness's own lookahead check on
every registered candidate. Runs on a synthetic panel - no database."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trader.research.lowfreq import engine
from trader.research.lowfreq.costs import base_costs
from trader.research.lowfreq.loaders import PricePanel
from trader.research.lowfreq.registry import load_family
from strategies.lowfreq_equity_base import CoveredTilt, first_observed, latest_change

REPO = Path(__file__).resolve().parents[2]
FAMILY_ROOT = REPO / 'research' / 'lowfreq'
N_NAMES, N_DAYS, FILING_EVERY = 60, 420, 63


def _entry(module: str, cls: str) -> dict:
    return {'id': 'synthetic', 'role': 'candidate', 'hypothesis': 'h', 'rationale': 'r',
            'strategy': {'module': module, 'class': cls}, 'params': {}, 'benchmarks': [],
            'registered_at': '2026-09-29'}


def candidate_entries():
    fam = load_family('equity', FAMILY_ROOT)
    return [(cid, e) for cid, e in fam.entries.items() if e['role'] == 'candidate']


def make_panel(seed: int = 7, delay: dict | None = None) -> PricePanel:
    """60 names x 420 sessions from 2016-08-01. Quarterly filings (every 63
    sessions, offset per name) become known as step changes in the as-of
    frames; ``delay`` = {ticker: sessions} postpones that name's filings."""
    rng = np.random.default_rng(seed)
    days = pd.bdate_range('2016-08-01', periods=N_DAYS)
    tickers = [f'T{i:03d}' for i in range(N_NAMES)]
    rets = rng.normal(0.0003, 0.015, (N_DAYS, N_NAMES))
    close = pd.DataFrame(50.0 * (1 + np.arange(N_NAMES) / 10.0) * np.cumprod(1 + rets, axis=0),
                         index=days, columns=tickers)
    opn = close.shift(1).fillna(close.iloc[0])
    names = ('shares_diluted', 'revenue_ttm', 'net_income_ttm', 'gross_profit_ttm', 'total_assets',
             'cash_from_operations_ttm', 'stockholders_equity')
    fields = {k: pd.DataFrame(np.nan, index=days, columns=tickers) for k in names}
    for i, t in enumerate(tickers):
        first = (i * 7) % FILING_EVERY + (delay or {}).get(t, 0)
        q = 0
        for row in range(first, N_DAYS, FILING_EVERY):
            rev = 1e9 * (1 + i) * (1 + 0.02 * q) * (1 + 0.1 * rng.standard_normal())
            vals = {'shares_diluted': 1e8 * (1 + i / 5.0), 'revenue_ttm': rev, 'net_income_ttm': 0.1 * rev,
                    'gross_profit_ttm': 0.4 * rev, 'total_assets': 3.0 * rev,
                    'cash_from_operations_ttm': 0.12 * rev * (1 + 0.5 * rng.standard_normal()),
                    'stockholders_equity': 1.5 * rev}
            for k, v in vals.items():
                fields[k].iloc[row:, i] = v
            q += 1
    member = pd.DataFrame(True, index=days, columns=tickers)
    member.iloc[300:, :3] = False                    # three names leave the universe
    fields['member'] = member
    fields['dollar_volume'] = close * 1e6
    return PricePanel(open=opn, close=close, fields=fields, signals={}, universe='equity')


def replay(entry: dict, panel: PricePanel):
    """The executor's exact sequence: one instance, one state, on_panel in order."""
    wide = engine.wide_panel(panel.frames())
    strat = engine.make_strategy(entry, {}, panel)
    state = strat.precompute_panel(wide)
    return wide, state, [strat.on_panel(wide, state, i) for i in range(len(wide) - 1)]


@pytest.fixture(scope='module')
def panel():
    return make_panel()


@pytest.mark.parametrize('cid,entry', candidate_entries(), ids=lambda x: x if isinstance(x, str) else '')
def test_candidate_passes_harness_lookahead_check(cid, entry, panel):
    state = engine.lookahead_check(entry, {}, panel)        # raises AssertionError('LOOKAHEAD...') on a leak
    assert isinstance(state.get('coverage'), pd.DataFrame)


@pytest.mark.parametrize('cid,entry', candidate_entries(), ids=lambda x: x if isinstance(x, str) else '')
def test_candidate_is_long_only_unlevered_diversified_and_inside_coverage(cid, entry, panel):
    wide, state, instructions = replay(entry, panel)
    cov = state['coverage']
    invested = 0
    for i, w in enumerate(instructions):
        if w is None:
            continue
        vals = np.asarray(list(w.values()), dtype=float)
        assert np.all(vals >= 0.0), f'{cid}: short weight at index {i}'          # long-only
        assert vals.sum() <= 1.0 + 1e-9, f'{cid}: gross {vals.sum()} > 1 at {i}'    # D01 no leverage
        held = [k for k, v in w.items() if v > 0]
        if held:
            invested += 1
            assert len(held) >= CoveredTilt.MIN_NAMES, f'{cid}: {len(held)} names at {i}'   # D05
            assert abs(vals.sum() - 1.0) < 1e-9
            assert all(bool(cov.iloc[i][k]) for k in held), f'{cid}: held outside coverage at {i}'
            assert all(bool(panel.fields['member'].iloc[i][k]) for k in held), f'{cid}: non-member held at {i}'
    assert invested >= 3, f'{cid}: never invested on the synthetic panel'


@pytest.mark.parametrize('cid,entry', candidate_entries(), ids=lambda x: x if isinstance(x, str) else '')
def test_instruction_does_not_depend_on_later_fundamentals(cid, entry, panel):
    """As-of discipline: the instruction at index i is identical when every
    fundamentals row AFTER i is destroyed - so a filing can only act from the
    row the loader says it was known (its acceptance instant)."""
    wide, state, ref = replay(entry, panel)
    due = [i for i, w in enumerate(ref) if w is not None]
    for i in due[2:6]:
        wrecked = PricePanel(open=panel.open, close=panel.close, universe='equity',
                             fields={k: v.copy() for k, v in panel.fields.items()})
        for k, v in wrecked.fields.items():
            if k not in ('member', 'dollar_volume'):
                v.iloc[i + 1:] = 1e12                       # absurd future filings, rows > i only
        _, st2, alt = replay(entry, wrecked)
        assert engine._weights_equal(alt[i], ref[i]), f'{cid}: instruction at {i} read fundamentals after {i}'


def test_filing_acts_only_from_its_arrival_row():
    """Delay one name's filings by 5 sessions: every instruction BEFORE the
    original arrival is unchanged, and the name is not held before it has any
    filing (it is uncovered)."""
    base, delayed = make_panel(), make_panel(delay={'T010': 5})
    entry = _entry('strategies.lowfreq_equity_s2p', 'SalesToPriceQuintile')
    _, st_a, a = replay(entry, base)
    _, st_b, b = replay(entry, delayed)
    first_row = int(np.flatnonzero(base.fields['revenue_ttm']['T010'].notna())[0])
    for i in range(first_row):
        assert engine._weights_equal(a[i], b[i])
        assert not (a[i] or {}).get('T010') and not (b[i] or {}).get('T010')
    assert not st_a['coverage']['T010'].iloc[:first_row].any()
    assert st_a['coverage']['T010'].iloc[first_row:].any()


def test_latest_change_and_first_observed_helpers():
    idx = pd.bdate_range('2020-01-01', periods=10)
    f = pd.DataFrame({'A': [np.nan, 1.0, 1.0, 1.0, 3.0, 3.0, 2.0, 2.0, 2.0, 2.0]}, index=idx)
    new, prev = latest_change(f)
    assert new['A'].iloc[:4].isna().all() and prev['A'].iloc[:4].isna().all()   # no change yet
    assert (new['A'].iloc[4], prev['A'].iloc[4]) == (3.0, 1.0)
    assert (new['A'].iloc[6], prev['A'].iloc[6]) == (2.0, 3.0)
    assert (new['A'].iloc[9], prev['A'].iloc[9]) == (2.0, 3.0)                  # carried forward
    frozen = first_observed(f)
    assert np.isnan(frozen['A'].iloc[0]) and (frozen['A'].iloc[1:] == 1.0).all()


def test_registry_matches_prereg_single_cell_and_benchmark():
    fam = load_family('equity', FAMILY_ROOT)
    cands = candidate_entries()
    assert len(cands) >= 4 and len(cands) <= 40                                  # D05 budget
    for cid, e in cands:
        assert e['params'] == {}, f'{cid}: PREREG declares no grids'
        assert e['benchmarks'] == ['eq_ew_covered'], cid
        cls = engine.load_strategy_class(e['strategy']['module'], e['strategy']['class'])
        assert issubclass(cls, CoveredTilt)
    assert fam.entries['eq_ew_covered']['role'] == 'benchmark'
    assert (FAMILY_ROOT / 'equity' / 'PREREG.md').exists()


def test_run_window_executes_without_violations_and_never_overdraws(panel):
    entry = _entry('strategies.lowfreq_equity_pead_earnings', 'EarningsSurpriseQuintile')
    idx = panel.close.index
    res = engine.run_window(entry, {}, panel, idx[0].date(), idx[200].date(),
                            idx[-1].date() + dt.timedelta(days=1), base_costs('equity'))
    assert len(res.returns) > 150 and res.n_trades > 0
    assert res.min_cash_fraction is not None and res.min_cash_fraction >= -1e-9      # D06
    assert res.median_names_held is not None and res.median_names_held >= CoveredTilt.MIN_NAMES
    assert res.coverage is not None
