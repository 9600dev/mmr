"""Parity on the RESEARCH DEV DB (C02 criterion 3): for three historical
rebalance sessions the weights ``targets`` produces for
``equity/s2p_top_quintile`` equal the weights ``engine.run_window`` (the real
``Backtester.run_panel`` path) issued on those sessions. Skipped when the dev
DB is not on this machine."""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

_REPO = str(Path(__file__).resolve().parents[1])
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trader.research.lowfreq import config as _cfg  # noqa: E402

_DEV = Path(os.path.expanduser(_cfg.DEFAULT_CONFIG))
pytestmark = pytest.mark.skipif(not _DEV.exists(), reason=f'research dev DB config {_DEV} not present')


@pytest.fixture
def dev_env(monkeypatch):
    monkeypatch.delenv('LOWFREQ_RESEARCH_CONFIG', raising=False)
    monkeypatch.delenv('LOWFREQ_RESEARCH_ROOT', raising=False)


def test_targets_match_run_panel_on_three_rebalance_dates(dev_env, monkeypatch):
    import importlib.util
    spec = importlib.util.spec_from_file_location('lowfreq_live_targets_tests', Path(__file__).with_name('test_lowfreq_live_targets.py'))
    tmod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tmod)
    record_on_panel = tmod.record_on_panel
    from trader.lowfreq_live import targets as T
    from trader.research.lowfreq import engine, loaders, registry
    from trader.research.lowfreq.costs import base_costs

    fam = registry.load_family('equity')
    entry = fam.get('s2p_top_quintile')
    data_start, meas_start, end_excl = dt.date(2023, 6, 1), dt.date(2023, 9, 15), dt.date(2024, 1, 10)
    panel = loaders.load_equity_panel(start=data_start, end=end_excl)
    recorded: dict = {}
    record_on_panel(monkeypatch, 's2p_top_quintile', recorded)
    captured: dict = {}
    orig_run = engine.Backtester.run_panel

    def run_panel(self, *a, **k):
        r = orig_run(self, *a, **k)
        captured['res'] = r
        return r
    monkeypatch.setattr(engine.Backtester, 'run_panel', run_panel)
    engine.run_window(entry, {}, panel, data_start, meas_start, end_excl, base_costs('equity'))
    res = captured['res']
    dates = [d for d in sorted(recorded) if d >= meas_start]
    assert dates[:3] == [dt.date(2023, 10, 2), dt.date(2023, 11, 1), dt.date(2023, 12, 1)]
    fills = {}
    for t in res.trades:
        fills.setdefault(pd.Timestamp(t.timestamp).date(), set()).add((t.conid, str(t.action)))
    for d in dates[:3]:
        sub = panel.sliced(pd.Timestamp(data_start), pd.Timestamp(d) + pd.Timedelta(days=1))
        out = T.compute_targets('equity', 's2p_top_quintile', 'paper_s2p', 25_000.0, d, panel=sub)
        T.validate_targets_document(out.document)
        assert out.document['strategy']['module'] == 'strategies.lowfreq_equity_s2p'
        assert out.document['weights'] == pytest.approx(recorded[d]), f'weights differ on {d}'
        assert len(out.document['weights']) >= 30                      # D05
        assert sum(out.document['weights'].values()) == pytest.approx(1.0)
        # the backtest's fills on the next session are in the names this instruction named
        nxt = min(x for x in fills if x > d)
        bought = {c for c, a in fills[nxt] if a.endswith('BUY')}
        assert bought <= set(out.document['weights']), f'{d}: BUY fills outside the targets'
        # the benchmark the ledger will track is the registry's decisive benchmark, on the same coverage
        assert set(out.benchmarks) == {'eq_ew_covered'} and len(out.benchmarks['eq_ew_covered']) >= 30
