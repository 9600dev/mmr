"""Insider-family candidates (C04; D11, D14): registry shape, long-only,
gross <= 1, >= 30 names, weight only inside the published coverage, no
event used before its ``usable_from`` session (D14), and the harness's own
fresh-prefix lookahead check on every registered candidate. Synthetic panel
and synthetic events - no database."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trader.research.lowfreq import engine
from trader.research.lowfreq import insider_signals as ins
from trader.research.lowfreq.engine import _weights_equal
from trader.research.lowfreq.registry import load_family
from strategies.lowfreq_insider_base import InsiderOverlay, adv_rank_matches, overlay_weights

_spec = importlib.util.spec_from_file_location('insider_fixtures', Path(__file__).with_name('insider_fixtures.py'))
assert _spec is not None and _spec.loader is not None
FIX = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(FIX)

REPO = Path(__file__).resolve().parents[2]
FAMILY_ROOT = REPO / 'research' / 'lowfreq'
CONTROLS = {'ins_size_matched_any'}
MARKER_RULES = ('ins_any_purchase', 'ins_officer', 'ins_conviction', 'ins_opportunistic', 'ins_net_buy_ratio')


def candidate_entries():
    fam = load_family('insider', FAMILY_ROOT)
    return [(cid, e) for cid, e in fam.entries.items() if e['role'] == 'candidate']


def entry_of(cid: str) -> dict:
    return dict(candidate_entries())[cid]


@pytest.fixture(scope='module')
def panel():
    return FIX.make_panel()


@pytest.fixture(autouse=True)
def _events():
    ins.set_events_override(FIX.make_events())
    yield
    ins.set_events_override(None)


def replay(entry: dict, panel):
    """The executor's exact sequence: one instance, one state, on_panel in order."""
    wide = engine.wide_panel(panel.frames())
    strat = engine.make_strategy(entry, {}, panel)
    state = strat.precompute_panel(wide)
    return wide, state, [strat.on_panel(wide, state, i) for i in range(len(wide) - 1)]


def test_registry_has_four_distinct_hypotheses_the_d11_benchmark_and_a_budget_under_thirty():
    fam = load_family('insider', FAMILY_ROOT)
    cands = candidate_entries()
    assert len([c for c, _ in cands if c not in CONTROLS]) >= 4
    assert len(cands) <= 30                                              # D11 trial budget
    bench = fam.entries['eq_ew_pit']
    assert bench['role'] == 'benchmark' and bench['strategy']['class'] == 'EqEwCovered'
    for cid, e in cands:
        assert e['benchmarks'] == ['eq_ew_pit'], cid
        assert e['params'] == {}, f'{cid}: PREREG says single pre-registered cells'
        cls = engine.load_strategy_class(e['strategy']['module'], e['strategy']['class'])
        assert issubclass(cls, InsiderOverlay), cid
        assert Path(REPO / (e['strategy']['module'].replace('.', '/') + '.py')).name.startswith('lowfreq_insider_')


@pytest.mark.parametrize('cid,entry', candidate_entries(), ids=lambda x: x if isinstance(x, str) else '')
def test_candidate_passes_harness_lookahead_check(cid, entry, panel):
    state = engine.lookahead_check(entry, {}, panel)        # raises AssertionError('LOOKAHEAD...') on a leak
    assert isinstance(state.get('coverage'), pd.DataFrame)


@pytest.mark.parametrize('cid,entry', candidate_entries(), ids=lambda x: x if isinstance(x, str) else '')
def test_candidate_is_long_only_unlevered_diversified_and_inside_coverage(cid, entry, panel):
    wide, state, instructions = replay(entry, panel)
    cov = state['coverage']
    invested = overlaid = 0
    for i, w in enumerate(instructions):
        if w is None:
            continue
        vals = np.asarray(list(w.values()), dtype=float)
        assert np.all(vals >= 0.0), f'{cid}: short weight at index {i}'          # long-only
        assert vals.sum() <= 1.0 + 1e-9, f'{cid}: gross {vals.sum()} > 1 at {i}'    # D01 no leverage
        held = [k for k, v in w.items() if v > 0]
        if held:
            invested += 1
            assert len(held) >= InsiderOverlay.MIN_COVERED, f'{cid}: {len(held)} names at {i}'   # D11
            assert abs(vals.sum() - 1.0) < 1e-9
            assert all(bool(cov.iloc[i][k]) for k in held), f'{cid}: held outside coverage at {i}'
            assert all(bool(panel.fields['member'].iloc[i][k]) for k in held), f'{cid}: non-member held at {i}'
            base = min(v for v in vals if v > 0)
            above = [k for k, v in w.items() if v > base + 1e-12]
            overlaid += bool(above)
            assert len(above) * 0 + sum(w[k] - base for k in above) <= InsiderOverlay.SLEEVE_MAX + 1e-9
    assert invested >= 3, f'{cid}: never invested on the synthetic panel'
    assert overlaid >= 1, f'{cid}: the overlay never activated on the synthetic panel'


@pytest.mark.parametrize('cid,entry', candidate_entries(), ids=lambda x: x if isinstance(x, str) else '')
def test_without_events_the_book_is_the_equal_weight_benchmark(cid, entry, panel):
    ins.set_events_override(FIX.make_events().iloc[0:0])
    _, _, instructions = replay(entry, panel)
    seen = 0
    for w in instructions:
        if w is None:
            continue
        pos = np.asarray([v for v in w.values() if v > 0])
        assert pos.max() - pos.min() < 1e-12
        seen += 1
    assert seen >= 3


@pytest.mark.parametrize('cid,entry', candidate_entries(), ids=lambda x: x if isinstance(x, str) else '')
def test_no_event_is_used_before_its_usable_from_session(cid, entry, panel):
    """D14: instructions before the marker's usable_from are identical with
    and without the marker event; a rule the marker satisfies acts on it at
    the rebalance ON usable_from (mark at that close, fill next open), and
    not one session earlier when usable_from moves one session later."""
    u = FIX.marker_usable_from()
    pos = int(FIX.SESSIONS.get_loc(u))
    _, _, ref = replay(entry, panel)
    ins.set_events_override(FIX.make_events(with_marker=False))
    _, _, without = replay(entry, panel)
    for i in range(pos):
        assert _weights_equal(ref[i], without[i]), f'{cid}: instruction at {FIX.SESSIONS[i].date()} depends on a later event'
    ins.set_events_override(FIX.make_events(marker_shift=1))
    _, _, shifted = replay(entry, panel)
    assert _weights_equal(shifted[pos], without[pos]), f'{cid}: acted on an event before its usable_from'
    if cid in MARKER_RULES:
        assert ref[pos] is not None and without[pos] is not None
        base = min(v for v in without[pos].values() if v > 0)
        assert ref[pos][FIX.MARKER] > base + 1e-12, f'{cid}: the marker purchase did not qualify on usable_from'
        assert without[pos][FIX.MARKER] == pytest.approx(base)


def test_overlay_weights_arithmetic():
    covered = [f'N{i:03d}' for i in range(100)]
    ew = overlay_weights(covered, covered[:3], 0.02, 0.6, 5)                 # K < MIN_SIGNAL_NAMES: plain basket
    assert all(abs(v - 0.01) < 1e-12 for v in ew.values())
    w = overlay_weights(covered, covered[:10], 0.02, 0.6, 5)                 # S = 0.2
    assert abs(sum(w.values()) - 1.0) < 1e-12
    assert w['N000'] == pytest.approx(0.8 / 100 + 0.02) and w['N050'] == pytest.approx(0.8 / 100)
    w = overlay_weights(covered, covered[:40], 0.02, 0.6, 5)                 # capped: S = 0.6, 1.5 % each
    assert abs(sum(w.values()) - 1.0) < 1e-12 and w['N000'] == pytest.approx(0.4 / 100 + 0.015)
    assert overlay_weights(covered, ['ZZZ'], 0.02, 0.6, 1)['N000'] == pytest.approx(0.01)   # unknown name ignored


def test_size_matched_control_overlays_matches_instead_of_the_signal_names(panel):
    adv = pd.Series({f'N{i}': float(i) for i in range(10)})
    assert adv_rank_matches(['N3'], adv.index.tolist(), adv) == ['N2']       # nearest rank, tie -> ticker order
    assert adv_rank_matches(['N3', 'N4'], adv.index.tolist(), adv) == ['N2', 'N5']
    _, _, any_w = replay(entry_of('ins_any_purchase'), panel)
    _, _, ctl_w = replay(entry_of('ins_size_matched_any'), panel)
    checked = 0
    for a, c in zip(any_w, ctl_w):
        if a is None or c is None:
            continue
        base_a = min(v for v in a.values() if v > 0)
        sig = {k for k, v in a.items() if v > base_a + 1e-12}
        if not sig:
            continue
        base_c = min(v for v in c.values() if v > 0)
        over = {k for k, v in c.items() if v > base_c + 1e-12}
        assert not (sig & over), 'the control overlaid a signal name'
        assert len(over) == len(sig)
        checked += 1
    assert checked >= 1
