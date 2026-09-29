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
    recording = tmod.recording
    from trader.lowfreq_live import targets as T
    from trader.research.lowfreq import engine, loaders, registry
    from trader.research.lowfreq.costs import base_costs

    fam = registry.load_family('equity')
    entry = fam.get('s2p_top_quintile')
    data_start, meas_start, end_excl = dt.date(2023, 6, 1), dt.date(2023, 9, 15), dt.date(2024, 1, 10)
    panel = loaders.load_equity_panel(start=data_start, end=end_excl)
    captured: dict = {}
    orig_run = engine.Backtester.run_panel

    def run_panel(self, *a, **k):
        r = orig_run(self, *a, **k)
        captured['res'] = r
        return r
    monkeypatch.setattr(engine.Backtester, 'run_panel', run_panel)
    real_make = engine.make_strategy
    with recording('s2p_top_quintile') as rec:
        engine.run_window(entry, {}, panel, data_start, meas_start, end_excl, base_costs('equity'))
    issued = {d: dict(w) for d, w in rec.items()}          # what run_panel consumed, frozen
    assert engine.make_strategy is real_make               # targets below runs unpatched
    monkeypatch.undo()
    res = captured['res']
    dates = [d for d in sorted(issued) if d >= meas_start]
    assert dates[:3] == [dt.date(2023, 10, 2), dt.date(2023, 11, 1), dt.date(2023, 12, 1)]
    # an independent oracle: the book the executor actually held after each fill session
    by_day: dict = {}
    for t in res.trades:
        by_day.setdefault(pd.Timestamp(t.timestamp).date(), []).append(t)
    fill_days = sorted(by_day)
    for d in dates[:3]:
        sub = panel.sliced(pd.Timestamp(data_start), pd.Timestamp(d) + pd.Timedelta(days=1))
        out = T.compute_targets('equity', 's2p_top_quintile', 'paper_s2p', 25_000.0, d, panel=sub)
        T.validate_targets_document(out.document)
        assert out.document['strategy']['module'] == 'strategies.lowfreq_equity_s2p'
        assert out.document['weights'] == pytest.approx(issued[d]), f'weights differ on {d}'
        assert len(out.document['weights']) >= 30                      # D05
        assert sum(out.document['weights'].values()) == pytest.approx(1.0)
        nxt = min(x for x in fill_days if x > d)
        positions: dict = {}
        for fd in fill_days:
            if fd > nxt:
                break
            for t in by_day[fd]:
                sgn = 1.0 if str(t.action).endswith('BUY') else -1.0
                positions[t.conid] = positions.get(t.conid, 0.0) + sgn * t.quantity
        held = {c for c, q in positions.items() if q > 1e-9}
        open_row = panel.open.loc[pd.Timestamp(nxt)]

        def unpriced(names):          # run_panel cannot fill what has no open print that session
            return {c for c in names if not (open_row.get(c, float('nan')) > 0)}
        targeted = set(out.document['weights'])
        missing = targeted - held
        assert missing == unpriced(missing), \
            f'{d}: targeted names not held after the fill although priced: ' \
            f'{ {c: open_row.get(c) for c in missing - unpriced(missing)} }'
        assert held - targeted == unpriced(held - targeted), f'{d}: held names outside the targets'
        # pit_daily_bars stores the top 800 by dollar volume per SESSION, so a targeted
        # name can have no bar on the fill day (2023-11-02: CTLT, RGEN, SFM, TECH, X had
        # none) and run_panel cannot fill it; a handful, never a material share of the book
        assert len(missing) <= 0.1 * len(targeted), f'{d}: {sorted(missing)} unfilled'
        # the benchmark the ledger will track is the registry's decisive benchmark, on the same coverage
        assert set(out.benchmarks) == {'eq_ew_covered'} and len(out.benchmarks['eq_ew_covered']) >= 30
    assert {d: dict(w) for d, w in rec.items()} == issued
