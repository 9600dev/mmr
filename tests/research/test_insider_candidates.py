"""Insider-family candidates (C04; D11, D14): registry shape (every rule has
its pre-registered size control), long-only, gross <= 1, >= 30 names, weight
only inside the published coverage, no event used before its ``usable_from``
session (D14), and the harness's own fresh-prefix lookahead check on every
registered candidate. Synthetic panel and synthetic events - no database."""
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
from strategies.lowfreq_insider_base import (CountRuleOverlay, InsiderOverlay, SizeMatchedOverlay,
                                             market_cap, overlay_weights, rank_matches)

_spec = importlib.util.spec_from_file_location('insider_fixtures', Path(__file__).with_name('insider_fixtures.py'))
assert _spec is not None and _spec.loader is not None
FIX = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(FIX)

REPO = Path(__file__).resolve().parents[2]
FAMILY_ROOT = REPO / 'research' / 'lowfreq'
CONTROL_PREFIX = 'ins_size_matched_'
MARKER_RULES = ('ins_purchase_count', 'ins_officer', 'ins_conviction', 'ins_opportunistic', 'ins_net_buy_ratio')


def candidate_entries():
    fam = load_family('insider', FAMILY_ROOT)
    return [(cid, e) for cid, e in fam.entries.items() if e['role'] == 'candidate']


def entry_of(cid: str) -> dict:
    return dict(candidate_entries())[cid]


def load_class(e: dict) -> type:
    return engine.load_strategy_class(e['strategy']['module'], e['strategy']['class'])


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


def test_registry_hypotheses_controls_benchmark_and_budget():
    fam = load_family('insider', FAMILY_ROOT)
    cands = candidate_entries()
    rules = [(c, e) for c, e in cands if not c.startswith(CONTROL_PREFIX)]
    controls = [(c, e) for c, e in cands if c.startswith(CONTROL_PREFIX)]
    assert len(rules) >= 4 and len(cands) <= 30                                # >= 4 hypotheses, D11 budget
    bench = fam.entries['eq_ew_pit']
    assert bench['role'] == 'benchmark' and bench['strategy']['class'] == 'EqEwCovered'
    rule_classes = {}
    for cid, e in cands:
        assert e['benchmarks'] == ['eq_ew_pit'], cid
        assert e['params'] == {}, f'{cid}: PREREG says single pre-registered cells'
        assert Path(e['strategy']['module'].replace('.', '/')).name.startswith('lowfreq_insider_'), cid
        cls = load_class(e)
        assert issubclass(cls, InsiderOverlay), cid
        if cid.startswith(CONTROL_PREFIX):
            assert issubclass(cls, SizeMatchedOverlay), cid
        else:
            assert not issubclass(cls, SizeMatchedOverlay), cid
            rule_classes[cid] = cls
    # deterministic eligibility: every rule has exactly one pre-registered size control mirroring it
    mirrored = [load_class(e).RULE for _, e in controls]
    assert sorted(c.__name__ for c in mirrored) == sorted(c.__name__ for c in rule_classes.values())
    assert 'ins_purchase_count' in rule_classes and issubclass(rule_classes['ins_purchase_count'], CountRuleOverlay)
    assert rule_classes['ins_purchase_count'].MEASURE == 'p_filings'       # count alone, no dollar floor


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
            assert sum(w[k] - base for k in above) <= InsiderOverlay.SLEEVE_MAX + 1e-9
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


def test_late_filed_prior_does_not_change_the_opportunistic_book_before_it_is_usable(panel):
    """The round-1 leak, at strategy level: with the March-2016 prior filed in
    May 2017, every instruction before May 2017 equals the book without it."""
    entry = entry_of('ins_opportunistic')
    _, _, ref = replay(entry, panel)
    ins.set_events_override(FIX.make_events(with_late_prior=False))
    _, _, without = replay(entry, panel)
    cutoff = int(FIX.SESSIONS.searchsorted(FIX.LATE_FILING))
    for i in range(cutoff):
        assert _weights_equal(ref[i], without[i]), f'instruction at {FIX.SESSIONS[i].date()} leaked a later filing'


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


def _overlaid(w: dict) -> set:
    base = min(v for v in w.values() if v > 0)
    return {k for k, v in w.items() if v > base + 1e-12}


def test_rule_and_its_size_control_hold_the_same_treated_sample_and_sleeve():
    """PREREG addendum 2026-09-29: with market cap unknown for a third of the
    names, a rule's treated sample is its qualifiers WITH a market cap, so at
    every rebalance the rule and its control overlay the same NUMBER of names
    with the same total sleeve, and the rule never overlays an unmatchable name."""
    p = FIX.make_panel()
    shares = p.fields['shares_diluted'].copy()
    shares.iloc[:, ::3] = np.nan                       # every third name has no market cap
    p.fields['shares_diluted'] = shares
    wide = engine.wide_panel(p.frames())
    mcap = market_cap(wide['close'].astype(float), {'shares_diluted': shares})
    cands = dict(candidate_entries())
    pairs = [(c, e) for c, e in cands.items() if c.startswith(CONTROL_PREFIX)]
    assert pairs
    activated = 0
    for ctl_id, ctl_entry in pairs:
        rule_cls = load_class(ctl_entry).RULE
        rule_id = next(c for c, e in cands.items() if load_class(e) is rule_cls)
        _, _, rule_w = replay(cands[rule_id], p)
        _, _, ctl_w = replay(ctl_entry, p)
        active = 0
        for i, (a, c) in enumerate(zip(rule_w, ctl_w)):
            assert (a is None) == (c is None), f'{rule_id}/{ctl_id}: rebalance schedules differ at {i}'
            if a is None or c is None:
                continue
            sig, over = _overlaid(a), _overlaid(c)
            assert len(sig) == len(over), f'{rule_id}/{ctl_id}: K differs at index {i} ({len(sig)} vs {len(over)})'
            assert all(pd.notna(mcap.iloc[i][k]) for k in sig), f'{rule_id}: overlaid a name without a market cap at {i}'
            assert not (sig & over)
            base_a = min(v for v in a.values() if v > 0)
            base_c = min(v for v in c.values() if v > 0)
            assert sum(a[k] - base_a for k in sig) == pytest.approx(sum(c[k] - base_c for k in over))
            active += bool(sig)
        activated += active                    # the cluster rule rarely reaches 5 names on this thin panel
    assert activated >= 1, 'no rule/control pair ever overlaid on the synthetic panel'


def test_size_control_overlays_market_cap_neighbours_instead_of_the_signal_names(panel):
    size = pd.Series({f'N{i}': float(i) for i in range(10)})
    assert rank_matches(['N3'], size.index.tolist(), size) == ['N2']        # nearest rank, tie -> ticker order
    assert rank_matches(['N3', 'N4'], size.index.tolist(), size) == ['N2', 'N5']
    assert rank_matches(['N3'], size.index.tolist(), size.drop('N3')) == []  # no size for the signal name: skipped
    wide = engine.wide_panel(panel.frames())
    close = wide['close'].astype(float)
    mcap = market_cap(close, {'shares_diluted': panel.fields['shares_diluted']})
    _, _, rule_w = replay(entry_of('ins_purchase_count'), panel)
    _, _, ctl_w = replay(entry_of('ins_size_matched_count'), panel)
    checked = 0
    for i, (a, c) in enumerate(zip(rule_w, ctl_w)):
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
        covered = [k for k, v in c.items() if v > 0]
        expected = set(rank_matches(sorted(sig), covered, mcap.iloc[i]))
        assert over == expected, 'the control did not pick the nearest market-cap neighbours'
        checked += 1
    assert checked >= 1
