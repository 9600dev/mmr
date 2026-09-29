"""``targets`` on a temporary research DB (C02 criteria 2 + 3, fake data):
D08 document validity, staleness refusals, hold-day refusal, and parity with
what ``engine.run_window`` (the real ``Backtester.run_panel`` path) issued."""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

import importlib.util  # noqa: E402

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _load_research_conftest():
    # An installed top-level `tests` package shadows the namespace import, so load by path.
    spec = importlib.util.spec_from_file_location('lowfreq_research_conftest', _REPO / 'tests' / 'research' / 'conftest.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules['lowfreq_research_conftest'] = mod
    spec.loader.exec_module(mod)
    return mod


_rc = _load_research_conftest()
research_env = _rc.research_env          # fixture re-export
write_family = _rc.write_family
from trader.lowfreq_live import targets as T
from trader.research.lowfreq import engine, loaders, registry
from trader.research.lowfreq.costs import base_costs

CANDIDATE = {'id': 'tiny_s2p', 'role': 'candidate', 'hypothesis': 'h', 'rationale': 'r',
             'strategy': {'module': 'tests/lowfreq_live_synthetic.py', 'class': 'TinySalesToPrice'},
             'params': {}, 'benchmarks': ['eq_ew_covered'], 'registered_at': '2026-09-29'}
BENCH = {'id': 'eq_ew_covered', 'role': 'benchmark', 'hypothesis': 'h', 'rationale': 'r',
         'strategy': {'module': 'trader.research.lowfreq.reference_strategies', 'class': 'EqEwCovered'},
         'params': {}, 'benchmarks': [], 'registered_at': '2026-09-29'}
AS_OF = dt.date(2017, 6, 1)      # first NYSE session of June 2017: a rebalance day; inside the fixture's bars


@pytest.fixture
def equity_family(research_env):
    write_family(research_env['root'], 'equity', [BENCH, CANDIDATE], universe='equity',
                 dev_period={'start': '2016-10-01', 'end': '2018-01-01'},
                 walk_forward={'train_days': 200, 'test_days': 100}, warmup_days=60, top_n=500)
    return research_env


def test_targets_document_is_valid_d08_and_written(equity_family, tmp_path):
    res = T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, AS_OF)
    doc = res.document
    T.validate_targets_document(doc)
    assert doc['schema'] == 'mmr.targets.v1' and doc['as_of'] == '2017-06-01' == doc['prices_as_of']
    assert doc['strategy']['module'] == 'tests/lowfreq_live_synthetic.py'
    assert len(doc['strategy']['source_sha256']) == 64
    assert 0 < sum(doc['weights'].values()) <= 1.0 + 1e-9
    assert set(doc['weights']) <= {'AAA', 'BBB', 'CCC'} and len(doc['weights']) == 2   # QUANTILE 0.67 of 3
    assert res.benchmarks['eq_ew_covered'] is None            # D05 benchmark holds below 30 covered names
    out = tmp_path / 't' / 'paper_s2p' / '2017-06-01.json'
    T.write_targets(res, path=out)
    assert json.loads(out.read_text()) == doc
    side = json.loads(T.benchmark_sidecar_path(out).read_text())
    assert side['schema'] == T.BENCHMARK_SCHEMA and side['benchmarks'] == {'eq_ew_covered': None}
    with pytest.raises(T.TargetsRefused):
        T.write_targets(res, path=out)                 # never silently overwrite a record
    assert T.list_sleeve_targets('paper_s2p', str(tmp_path / 't')) == [out]


def test_targets_refuses_hold_day_and_stale_inputs(equity_family):
    with pytest.raises(T.TargetsRefused, match='no instruction'):
        T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, dt.date(2017, 6, 2))
    # bars stored through ~2018-03-13; a decision session after that has no bar
    panel = loaders.load_equity_panel(start='2017-12-01', end='2018-03-13')
    with pytest.raises(T.TargetsRefused, match='STALE BARS'):
        T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, dt.date(2018, 4, 2), panel=panel)
    # the fixture's newest filing is accepted 2018-02-04: 2018-03-01 is 25 days later
    with pytest.raises(T.TargetsRefused, match='STALE FUNDAMENTALS'):
        T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, dt.date(2018, 3, 1))
    assert T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, dt.date(2018, 3, 1),
                             max_fundamentals_lag_days=60).document['weights']
    with pytest.raises(T.TargetsRefused, match='sleeve'):
        T.compute_targets('equity', 'tiny_s2p', 'Paper-S2P', 25_000.0, AS_OF)
    with pytest.raises(registry.RegistryError):
        T.compute_targets('equity', 'nope', 'paper_s2p', 25_000.0, AS_OF)


def test_validate_targets_document_rules():
    good = {'schema': 'mmr.targets.v1', 'sleeve': 'paper_s2p', 'as_of': '2026-09-01',
            'generated_at': '2026-09-01T21:00:00+00:00',
            'strategy': {'family': 'equity', 'id': 'x', 'module': 'm', 'class': 'C', 'params': {}, 'source_sha256': 'a' * 64},
            'capital_usd': 1000.0, 'prices_as_of': '2026-09-01', 'weights': {'AAA': 0.5, 'BBB': 0.5}, 'notes': ''}
    T.validate_targets_document(good)
    for bad in ({'weights': {'AAA': 0.6, 'BBB': 0.5}}, {'weights': {'AAA': 0.0}}, {'weights': {'aaa': 0.5}},
                {'sleeve': 'Bad Sleeve'}, {'capital_usd': 0}, {'schema': 'v0'}, {'generated_at': '2026-09-01T21:00:00'},
                {'strategy': dict(good['strategy'], source_sha256='xyz')}):
        with pytest.raises(ValueError):
            T.validate_targets_document(dict(good, **bad))


@contextlib.contextmanager
def recording(cid: str):
    """Wrap engine.make_strategy so the EXECUTING instance's on_panel
    instructions are recorded per decision session (positive weights only),
    and RESTORE the real function on exit - the patch must not be active when
    `targets` runs, or the comparison would be targets against targets."""
    real = engine.make_strategy
    recorded: dict = {}

    def wrapped(entry, cell, panel, coverage=None):
        inst = real(entry, cell, panel, coverage)
        if entry['id'] == cid:
            orig = inst.on_panel

            def on_panel(panel_, state, i):
                w = orig(panel_, state, i)
                if w is not None:
                    recorded[pd.Timestamp(panel_.index[i]).date()] = {k: float(v) for k, v in w.items() if v > 0}
                return w
            inst.on_panel = on_panel
        return inst
    engine.make_strategy = wrapped
    try:
        yield recorded
    finally:
        engine.make_strategy = real


def test_parity_with_run_panel_on_fake_db(equity_family):
    fam = registry.load_family('equity')
    entry = fam.get('tiny_s2p')
    panel = loaders.load_equity_panel(start='2017-02-01', end='2017-09-15')
    real = engine.make_strategy
    with recording('tiny_s2p') as rec:
        engine.run_window(entry, {}, panel, dt.date(2017, 2, 1), dt.date(2017, 4, 1), dt.date(2017, 9, 15),
                          base_costs('equity'))
    issued = {d: dict(w) for d, w in rec.items()}          # snapshot of what run_panel consumed
    assert engine.make_strategy is real                     # targets runs UNPATCHED
    dates = [d for d in sorted(issued) if d >= dt.date(2017, 4, 1)]
    assert len(dates) >= 3
    for d in dates[:3]:
        sub = panel.sliced(pd.Timestamp('2017-02-01'), pd.Timestamp(d) + pd.Timedelta(days=1))
        res = T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 10_000.0, d, panel=sub)
        assert res.document['weights'] == pytest.approx(issued[d]), d
    assert {d: dict(w) for d, w in rec.items()} == issued    # nothing was recorded during targets
