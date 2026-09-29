"""The equity family's champion-selection code applies PREREG.md rules 1-4
exactly as committed at the round-1 checkpoint 92653bf (addendum R4): the
control is never eligible, rule 2's guards are literal, rule 3 ranks by
verdict tier then t(NW) then lower turnover, and the later (R2 item 3 / R3)
guards live in a separate post-hoc sensitivity function that never selects."""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location('eq_make_report', REPO / 'research' / 'lowfreq' / 'equity' / 'make_report.py')
mr = importlib.util.module_from_spec(_spec)
sys.modules['eq_make_report'] = mr
_spec.loader.exec_module(mr)


def _r(active, t_nw, verdict, turnover=8.0):
    return {'active': {'ann_return': active, 't_nw': t_nw}, 'verdict': verdict,
            'full_period': {'report_cell': {'turnover_per_year': turnover}}}


def _results(**over):
    base = {'s2p_top_quintile': _r(0.12, 2.0, 'PASS'), 'size_smallcap': _r(0.09, 1.4, 'PROMISING'),
            's2p_frozen_revenue': _r(0.119, 1.9, 'PROMISING'), 'pead_earnings': _r(0.07, 1.3, 'PROMISING'),
            'pead_revenue': _r(0.08, 1.1, 'PROMISING'), 'quality_value_blend': _r(0.08, 1.1, 'FAIL'),
            'value_momentum_filter': _r(0.06, 1.3, 'PROMISING'), 'low_volatility': _r(-0.05, -1.0, 'FAIL'),
            'low_accruals': _r(0.10, 1.8, 'PROMISING'),
            # later controls (R2/R3): present, count in N, never eligible
            'size_small_assets': _r(-0.01, -0.1, 'FAIL'), 'size_small_revenue': _r(-0.01, -0.2, 'FAIL'),
            'quality_value_frozen': _r(0.096, 1.2, 'PROMISING'), 'accruals_frozen': _r(0.073, 1.5, 'PROMISING'),
            'pead_earnings_raw': _r(0.069, 1.3, 'PROMISING'), 'pead_revenue_raw': _r(0.025, 0.7, 'FAIL'),
            'accruals_raw': _r(0.044, 1.05, 'PROMISING'), 'gross_profitability': _r(0.012, 0.2, 'FAIL'),
            'gross_profit_raw': _r(0.003, 0.05, 'FAIL'), 'momentum_12_1': _r(0.027, 0.4, 'FAIL'), 'revenue_raw': _r(-0.005, -0.1, 'FAIL')}
    base.update(over)
    return base


IDS = list(_results().keys())


def test_original_entries_are_exactly_the_92653bf_registry():
    try:
        text = subprocess.run(['git', 'show', '92653bf:research/lowfreq/equity/candidates.yaml'], cwd=str(REPO),
                              capture_output=True, text=True, timeout=20, check=True).stdout
    except (subprocess.SubprocessError, FileNotFoundError):
        pytest.skip('git history not available')
    reg = yaml.safe_load(text)
    assert [e['id'] for e in reg['candidates'] if e['role'] == 'candidate'] == list(mr.ORIGINAL)
    assert mr.RULE1_CONTROL in mr.ORIGINAL


def test_rule1_control_and_later_controls_are_never_eligible():
    champ, table = mr.choose_champion(IDS, _results())
    rows = dict((c, e) for c, e, _ in table)
    assert rows['s2p_frozen_revenue'] == 'ineligible'
    assert set(rows) == set(mr.ORIGINAL), 'only entries registered at 92653bf are ever considered'
    for later in ('quality_value_frozen', 'pead_earnings_raw', 'gross_profitability', 'momentum_12_1', 'revenue_raw'):
        assert later not in rows


def test_rule2_guards_are_literal_and_size_smallcap_stays_eligible():
    res = _results()
    champ, table = mr.choose_champion(IDS, res)
    rows = dict((c, e) for c, e, _ in table)
    assert champ == 's2p_top_quintile' and rows['s2p_top_quintile'] == 'eligible'
    assert rows['quality_value_blend'] == 'ineligible' and rows['value_momentum_filter'] == 'ineligible'
    assert rows['size_smallcap'] == 'eligible' and rows['low_volatility'] == 'eligible'
    # H1 not exceeding its frozen-numerator control by a hair -> ineligible, size_smallcap stands in
    res2 = _results(s2p_frozen_revenue=_r(0.12, 1.9, 'PROMISING'))
    champ2, table2 = mr.choose_champion(IDS, res2)
    rows2 = dict((c, e) for c, e, _ in table2)
    assert rows2['s2p_top_quintile'] == 'ineligible' and champ2 != 's2p_top_quintile'
    # with H1 out, the blend/filter guards compare against H1's return regardless of H1's eligibility
    assert rows2['quality_value_blend'] == 'ineligible'


def test_rule3_ranks_tier_then_t_then_lower_turnover():
    res = _results(s2p_top_quintile=_r(0.12, 2.0, 'PROMISING'), low_accruals=_r(0.10, 2.5, 'PROMISING'))
    champ, _ = mr.choose_champion(IDS, res)
    assert champ == 'low_accruals'                                   # same tier, higher t(NW)
    res = _results(s2p_top_quintile=_r(0.12, 2.0, 'PROMISING'), low_accruals=_r(0.10, 2.0, 'PROMISING', turnover=3.0))
    champ, _ = mr.choose_champion(IDS, res)
    assert champ == 'low_accruals'                                   # tie on tier and t -> lower turnover
    res = _results(s2p_top_quintile=_r(0.12, 1.0, 'PASS'), low_accruals=_r(0.10, 2.5, 'PROMISING'))
    champ, _ = mr.choose_champion(IDS, res)
    assert champ == 's2p_top_quintile'                               # tier beats t


def test_no_eligible_candidate_returns_none():
    res = _results()
    champ, table = mr.choose_champion(['s2p_frozen_revenue'], {'s2p_frozen_revenue': res['s2p_frozen_revenue']})
    assert champ is None and table == [('s2p_frozen_revenue', 'ineligible', 'rule 1: the numerator control, never a champion')]


def test_sensitivity_uses_the_later_guards_and_does_not_feed_selection():
    res = _results(revenue_raw=_r(0.13, 2.2, 'PASS'))                # H1 would fail the R3 guard, not the 92653bf rule
    champ, _ = mr.choose_champion(IDS, res)
    sens, table = mr.sensitivity_champion(IDS, res)
    assert champ == 's2p_top_quintile'
    assert dict((c, e) for c, e, _ in table)['s2p_top_quintile'] == 'ineligible'
    assert sens != 's2p_top_quintile'
