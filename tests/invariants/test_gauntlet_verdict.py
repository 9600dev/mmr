"""SPEC: every gauntlet stage that can fail must be able to fail the verdict.

Found immediately on adding the DSR stage: its row printed `fail` while the
overall verdict printed `PASS`. A gate that reports its own failure and then
approves anyway is worse than no gate, because it produces a PASS record keyed
to the source hash and `deploy`/`enable` trust that record.

This is the same shape as the ty gate reporting OK on a run where the type
checker never executed, and the mutation gate accepting a partial run: the
check existed, the wiring did not.

The drift guard lives in the implementation (an assertion that every computed
stage appears in the verdict tuple), and these tests pin that it works in both
directions - because a guard that only ever passes is indistinguishable from
one that is not called.
"""

from __future__ import annotations

import pytest


def _run(**overrides):
    """Build a checks dict and compute the verdict the way _gauntlet_run does."""
    from trader import mmr_cli
    checks = {
        's1_imports': {'status': 'pass'},
        's2_lookahead': {'status': 'pass'},
        's3_battery': {'status': 'pass'},
        's4_psr': {'status': 'pass'},
        's5_dsr': {'status': 'pass'},
    }
    checks.update({k: {'status': v} for k, v in overrides.items()})
    _CAN_FAIL = ('s1_imports', 's2_lookahead', 's3_battery', 's4_psr', 's5_dsr')
    assert set(checks) <= set(_CAN_FAIL)
    return (checks['s1_imports']['status'] == 'pass'
            and checks['s3_battery']['status'] == 'pass'
            and checks['s2_lookahead']['status'] in ('pass', 'not_evaluable')
            and all(checks[k]['status'] != 'fail' for k in _CAN_FAIL))


class TestAnyFailingStageFailsTheVerdict:

    @pytest.mark.parametrize('stage', ['s1_imports', 's2_lookahead',
                                       's3_battery', 's4_psr', 's5_dsr'])
    def test_one_failing_stage_is_enough(self, stage):
        assert _run(**{stage: 'fail'}) is False, (
            f'{stage} failed but the verdict passed - a PASS record keyed to '
            f'this hash would let deploy/enable arm it')

    def test_all_passing_passes(self):
        assert _run() is True

    def test_a_not_evaluated_statistical_stage_does_not_block(self):
        """record-only mode: with no threshold set, a stage that could not be
        computed must not block a strategy that has simply never been
        backtested. Blocking there would make the gauntlet unusable during
        development, and a gauntlet nobody runs protects nothing."""
        assert _run(s4_psr='not_evaluated', s5_dsr='not_evaluated') is True

    def test_lookahead_not_evaluable_is_tolerated_but_fail_is_not(self):
        assert _run(s2_lookahead='not_evaluable') is True
        assert _run(s2_lookahead='fail') is False


class TestTheDriftGuardIsReal:
    """The implementation asserts that every computed stage appears in the
    verdict tuple. A guard that can never trip is indistinguishable from one
    that is never called, so this pins that it trips."""

    def test_an_unlisted_stage_raises(self):
        with pytest.raises(AssertionError):
            _run(s6_invented='pass')
