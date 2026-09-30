"""Targeted tests for behavioural mutants that survived the 2026-09-29 mutation pass over
trader/lowfreq_live/targets.py (the D08 writer). Each test names the mutant(s) it kills; the ledger
is docs/MUTATION_REBALANCE.md. The research-DB fixture is the one tests/test_lowfreq_live_targets.py uses."""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _load_research_conftest():
    spec = importlib.util.spec_from_file_location('lowfreq_research_conftest_kills', _REPO / 'tests' / 'research' / 'conftest.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules['lowfreq_research_conftest_kills'] = mod
    spec.loader.exec_module(mod)
    return mod


_rc = _load_research_conftest()
research_env = _rc.research_env          # fixture re-export
write_family = _rc.write_family
from trader.lowfreq_live import targets as T
from trader.research.lowfreq import loaders, registry

CANDIDATE = {'id': 'tiny_s2p', 'role': 'candidate', 'hypothesis': 'h', 'rationale': 'r',
             'strategy': {'module': 'tests/lowfreq_live_synthetic.py', 'class': 'TinySalesToPrice'},
             'params': {}, 'benchmarks': ['eq_ew_covered'], 'registered_at': '2026-09-29'}
BENCH = {'id': 'eq_ew_covered', 'role': 'benchmark', 'hypothesis': 'h', 'rationale': 'r',
         'strategy': {'module': 'trader.research.lowfreq.reference_strategies', 'class': 'EqEwCovered'},
         'params': {}, 'benchmarks': [], 'registered_at': '2026-09-29'}
AS_OF = dt.date(2017, 6, 1)


@pytest.fixture
def equity_family(research_env):
    write_family(research_env['root'], 'equity', [BENCH, CANDIDATE], universe='equity',
                 dev_period={'start': '2016-10-01', 'end': '2018-01-01'},
                 walk_forward={'train_days': 200, 'test_days': 100}, warmup_days=60, top_n=500)
    return research_env


GOOD = {'schema': 'mmr.targets.v1', 'sleeve': 'paper_s2p', 'as_of': '2026-09-01',
        'generated_at': '2026-09-01T21:00:00+00:00',
        'strategy': {'family': 'equity', 'id': 'x', 'module': 'm', 'class': 'C', 'params': {}, 'source_sha256': 'a' * 64},
        'capital_usd': 1000.0, 'prices_as_of': '2026-09-01', 'weights': {'AAA': 0.5, 'BBB': 0.5}, 'notes': ''}


# ============================================================ pure: validate_targets_document

@pytest.mark.parametrize('bad, why', [
    ({'generated_at': pd.Timestamp('2026-09-01T21:00:00Z')}, 'generated_at'),       # _36: a non-string, even tz-aware, is refused
    ({'strategy': dict(GOOD['strategy'], family='')}, 'strategy.family'),           # _61: empty string
    ({'capital_usd': True}, 'capital_usd'),                                         # _87: bool is not a number
    ({'weights': {}}, 'weights'),                                                   # _98: empty mapping
    ({'weights': {'AAA': True}}, 'weight for AAA'),                                 # _115: bool weight
    ({'weights': {'AAA': 1.5}}, r'weight for AAA must be in \(0, 1\]'),             # _122: refused per-weight, not by the sum
])
def test_document_rules_refuse_the_edge_cases(bad, why):
    with pytest.raises(ValueError, match=why):
        T.validate_targets_document(dict(GOOD, **bad))


def test_fractional_capital_is_a_valid_document():                                  # _91
    T.validate_targets_document(dict(GOOD, capital_usd=0.5))


def test_positive_drops_zero_weights():                                             # _positive_5
    assert T._positive({'A': 0.0, 'B': 0.5, 'C': -0.1}) == {'B': 0.5}


# ============================================================ pure: write_targets / list_sleeve_targets

def _result(**over) -> T.TargetsResult:
    doc = dict(GOOD, **over)
    return T.TargetsResult(document=doc, benchmarks={'eq_ew': {'ZZZ': 0.5, 'AAA': 0.5}, 'holds': None})


def test_default_path_is_targets_dir_sleeve_as_of_and_existing_dirs_are_fine(tmp_path):   # write_targets_7..15, 22, 24, 26
    res = _result(sleeve='mutkill_zz')
    expected = tmp_path / 'mutkill_zz' / '2026-09-01.json'
    expected.parent.mkdir(parents=True)                       # the sleeve folder already exists
    out = T.write_targets(res, targets_dir=str(tmp_path))
    assert out.path == expected and expected.is_file()
    assert out.benchmark_path == tmp_path / 'mutkill_zz' / '2026-09-01.benchmark.json'


def test_sidecar_sorts_benchmark_weights_and_the_d08_file_keeps_schema_first(tmp_path):    # write_targets_52,53,66..80
    out = T.write_targets(_result(), path=tmp_path / 'p' / '2026-09-01.json')
    side = json.loads(out.benchmark_path.read_text())
    assert side['benchmarks'] == {'eq_ew': {'AAA': 0.5, 'ZZZ': 0.5}, 'holds': None}
    assert list(side['benchmarks']['eq_ew']) == ['AAA', 'ZZZ']
    text = out.path.read_text()
    assert text.startswith('{\n  "schema": "mmr.targets.v1",\n  "sleeve": "paper_s2p",\n') and text.endswith('}\n')
    assert out.benchmark_path.read_text().startswith('{\n  "schema": "mmr.lowfreq_benchmark.v1",\n')


def test_list_sleeve_targets_only_lists_dated_d08_files(tmp_path):                 # list_sleeve_targets_11
    root = tmp_path / 'paper_s2p'
    root.mkdir()
    for name in ('2026-09-01.json', '2026-09-01.benchmark.json', 'README.json', 'notes.json'):
        (root / name).write_text('{}')
    assert T.list_sleeve_targets('paper_s2p', str(tmp_path)) == [root / '2026-09-01.json']


# ============================================================ research-DB fixture: compute_targets / freshness / loader

def test_targets_diagnostics_and_provenance_fields(equity_family):                 # compute_targets_183,197,217,231,234,237; check_freshness_27,30
    res = T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, AS_OF)
    doc, diag = res.document, res.diagnostics
    assert doc['strategy']['class'] == 'TinySalesToPrice' and doc['strategy']['module'] == 'tests/lowfreq_live_synthetic.py'
    assert '.' not in doc['generated_at'] and doc['generated_at'].endswith('+00:00')
    assert doc['notes'].startswith('2 names, gross ') and 'registry entry sha256' in doc['notes']
    assert diag['universe'] == 'equity' and diag['n_names'] == 2 and diag['gross'] == pytest.approx(sum(doc['weights'].values()))
    assert diag['registry_entry_sha256'] == registry.entry_sha256(registry.load_family('equity').get('tiny_s2p'))
    assert isinstance(diag['latest_fundamentals_acceptance'], str) and isinstance(diag['fundamentals_lag_days'], int)
    assert diag['last_bar'] == '2017-06-01' and diag['n_bars'] > 0
    assert T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, AS_OF, notes='mine').document['notes'] == 'mine'


def test_fundamentals_lag_limit_is_inclusive(equity_family):                       # check_freshness_34
    # the fixture's newest filing is accepted 2018-02-04: 2018-03-01 is exactly 25 days later
    assert T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, dt.date(2018, 3, 1),
                             max_fundamentals_lag_days=25).diagnostics['fundamentals_lag_days'] == 25
    with pytest.raises(T.TargetsRefused, match='STALE FUNDAMENTALS'):
        T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, dt.date(2018, 3, 1), max_fundamentals_lag_days=24)


def test_capital_and_params_rules(equity_family):                                  # compute_targets_9,10,17,20,21,22,23
    with pytest.raises(T.TargetsRefused, match='capital'):
        T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 0.0, AS_OF)
    assert T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 0.5, AS_OF).document['capital_usd'] == 0.5
    explicit_default = T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, AS_OF, params={})
    assert explicit_default.document['strategy']['params'] == {}
    with pytest.raises(T.TargetsRefused, match='not a registered grid cell'):
        T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, AS_OF, params={'bogus': 1})


def test_unchecked_panel_must_still_end_on_the_decision_session(equity_family):    # compute_targets_57..63
    ends_on = loaders.load_equity_panel(start='2017-02-01', end='2017-06-01')
    res = T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, AS_OF, panel=ends_on, check_fresh=False)
    assert res.document['prices_as_of'] == '2017-06-01' and 'last_bar' not in res.diagnostics
    ends_before = loaders.load_equity_panel(start='2017-02-01', end='2017-05-25')
    with pytest.raises(T.TargetsRefused, match='must end at'):
        T.compute_targets('equity', 'tiny_s2p', 'paper_s2p', 25_000.0, AS_OF, panel=ends_before, check_fresh=False)


def test_load_panel_for_uses_the_entry_warmup_and_refuses_an_unknown_universe(equity_family):   # load_panel_for_3,9,23,26,36
    fam = registry.load_family('equity')
    entry = fam.get('tiny_s2p')
    panel = T.load_panel_for(fam, entry, AS_OF)
    first = pd.Timestamp(panel.close.index[0]).date()
    assert AS_OF - dt.timedelta(days=60) <= first <= AS_OF                    # family warmup_days=60, not the whole history
    short = T.load_panel_for(fam, dict(entry, warmup_days=10), AS_OF)
    assert pd.Timestamp(short.close.index[0]).date() >= AS_OF - dt.timedelta(days=10)
    assert len(short.close.index) < len(panel.close.index)
    with pytest.raises(T.TargetsRefused, match='unknown universe'):
        T.load_panel_for(fam, dict(entry, universe='crypto'), AS_OF)
