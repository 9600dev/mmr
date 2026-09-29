"""D07 terminal-loss stress tooling (C03, PREREG addendum R3): on a synthetic
panel where two held names stop printing, the forced exits reconstructed from
the fill log reconcile EXACTLY with the backtester's own diagnostics, a
regular open-fill on the terminal date is not mistaken for one, and marking
the exits to zero lowers the stressed equity by exactly their proceeds."""
from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trader.research.lowfreq import engine
from trader.research.lowfreq.costs import base_costs
from trader.research.lowfreq.loaders import PricePanel
from research.test_equity_candidates import _entry, make_panel

REPO = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location('stress_d07', REPO / 'research' / 'lowfreq' / 'equity' / 'stress_d07.py')
stress = importlib.util.module_from_spec(_spec)
sys.modules['stress_d07'] = stress
_spec.loader.exec_module(stress)


def stopped_panel() -> PricePanel:
    p = make_panel()
    close, opn = p.close.copy(), p.open.copy()
    for t, stop in (('T005', 300), ('T012', 350)):       # two names stop printing mid-window
        close.iloc[stop:, close.columns.get_loc(t)] = np.nan
        opn.iloc[stop:, opn.columns.get_loc(t)] = np.nan
    fields = dict(p.fields)
    fields['member'] = p.fields['member'] & close.notna()
    return PricePanel(open=opn, close=close, fields=fields, signals={}, universe='equity')


def test_forced_exits_reconcile_with_backtester_and_stress_removes_their_proceeds():
    panel = stopped_panel()
    entry = _entry('strategies.lowfreq_equity_size', 'SmallCapQuintile')   # a broad quintile: holds the stopped names
    costs = base_costs('equity')
    terminal = engine.terminal_prints(panel)
    assert set(terminal) == {'T005', 'T012'}
    idx = panel.close.index
    res, cov, window = stress._run(entry, {}, panel, idx[0].date(), idx[-1].date() + dt.timedelta(days=1), costs, terminal)
    diag = res.diagnostics
    events = stress._forced(res, terminal, window, costs.slippage_bps / 10_000.0)
    assert diag['panel_forced_exits'] >= 1, 'the synthetic book must have held a stopped name'
    assert len(events) == diag['panel_forced_exits']
    assert sum(e['quantity'] * e['last_print_close'] for e in events) == pytest.approx(diag['panel_forced_exit_value'])
    assert {e['ticker'] for e in events} <= {'T005', 'T012'}
    # every event is priced at the terminal print's close, never the open
    for e in events:
        ts = pd.Timestamp(e['date'])
        assert e['price'] == pytest.approx(float(window.close.at[ts, e['ticker']]) * (1 - costs.slippage_bps / 10_000.0))
    raw, stressed = stress._stressed_returns(res, events, idx[0].date())
    eq = res.equity_curve.astype(float)
    lost = sum(e['proceeds'] for e in events)
    assert (1 + stressed).prod() < (1 + raw).prod()
    # final stressed equity == final equity minus all proceeds (marked to zero from the exit day on)
    assert float(eq.iloc[-1]) - lost == pytest.approx(float(eq.iloc[0]) * float((1 + stressed).prod()), rel=1e-9)


def test_reconciliation_refuses_a_tampered_diagnostic():
    panel = stopped_panel()
    entry = _entry('strategies.lowfreq_equity_size', 'SmallCapQuintile')
    costs = base_costs('equity')
    terminal = engine.terminal_prints(panel)
    idx = panel.close.index
    res, _, window = stress._run(entry, {}, panel, idx[0].date(), idx[-1].date() + dt.timedelta(days=1), costs, terminal)
    res.diagnostics['panel_forced_exits'] += 1
    with pytest.raises(RuntimeError, match='reconciliation failed'):
        stress._forced(res, terminal, window, costs.slippage_bps / 10_000.0)
