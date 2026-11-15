"""scripts/sleeve_upkeep.py: the sleeve's daily and monthly routine as one job.

None of it had been run for six weeks when the job was written: bars stopped at
the first rebalance, no fills journal, an empty ledger, a missed decision
session. What matters here is which steps run on which day, that a failure
stops what depends on it, and that nothing in it can create an order.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
from pathlib import Path

import exchange_calendars
import pytest

_SPEC = importlib.util.spec_from_file_location(
    'sleeve_upkeep', Path(__file__).resolve().parent.parent / 'scripts' / 'sleeve_upkeep.py')
upkeep = importlib.util.module_from_spec(_SPEC)
import sys as _sys                                           # noqa: E402
_sys.modules['sleeve_upkeep'] = upkeep                       # dataclasses resolve their module by name
_SPEC.loader.exec_module(upkeep)

XNYS = exchange_calendars.get_calendar('XNYS')
UTC = dt.timezone.utc
PY = 'python3'


def _targets(tmp_path, sleeve='paper_s2p', as_of='2026-10-01', **over):
    document = {'schema': 'mmr.targets.v1', 'sleeve': sleeve, 'as_of': as_of, 'capital_usd': 50_000.0,
                'strategy': {'family': 'equity', 'id': 's2p_top_quintile', 'params': {'quantile': 0.2}},
                'weights': {'AAPL': 0.5}}
    document.update(over)
    folder = tmp_path / sleeve
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f'{as_of}.json').write_text(json.dumps(document))
    (folder / f'{as_of}.benchmark.json').write_text('{}')
    return folder


class TestLastCompletedSession:
    @pytest.mark.parametrize('now, expected', [
        ('2026-11-13 21:00', '2026-11-13'),          # Friday, the instant of the 16:00 ET close
        ('2026-11-13 20:59', '2026-11-12'),          # a minute before it: Friday is not complete
        ('2026-11-14 12:00', '2026-11-13'),          # Saturday
        ('2026-11-16 03:00', '2026-11-13'),          # Sunday night in New York
        ('2026-11-16 14:00', '2026-11-13'),          # Monday before the open
        ('2026-11-27 18:00', '2026-11-27'),          # the half day: complete at 13:00 ET
        ('2026-11-27 17:59', '2026-11-25'),          # ...and not a minute earlier (Thursday was Thanksgiving)
        ('2026-11-26 23:00', '2026-11-25'),          # Thanksgiving itself
    ])
    def test_the_session_is_the_last_one_that_has_closed(self, now, expected):
        moment = dt.datetime.fromisoformat(now).replace(tzinfo=UTC)
        assert upkeep.last_completed_session(moment, XNYS) == dt.date.fromisoformat(expected)

    def test_a_naive_instant_is_read_as_utc(self):
        assert upkeep.last_completed_session(dt.datetime(2026, 11, 13, 21, 0), XNYS) == dt.date(2026, 11, 13)


class TestFirstSessionOfMonth:
    @pytest.mark.parametrize('day, first', [
        ('2026-12-01', True), ('2026-12-02', False), ('2026-11-02', True),       # Nov 1 was a Sunday
        ('2026-11-30', False), ('2027-01-04', True),                              # Jan 1 a holiday, then a weekend
        ('2026-10-01', True),
    ])
    def test_it_is_the_first_trading_day_not_the_first_calendar_day(self, day, first):
        assert upkeep.is_first_session_of_month(dt.date.fromisoformat(day), XNYS) is first


class TestDiscovery:
    def test_a_sleeve_is_a_directory_with_a_dated_targets_file(self, tmp_path):
        _targets(tmp_path, 'paper_s2p')
        _targets(tmp_path, 'another')
        (tmp_path / 'only_sidecar').mkdir()
        (tmp_path / 'only_sidecar' / '2026-10-01.benchmark.json').write_text('{}')
        (tmp_path / 'empty').mkdir()
        (tmp_path / 'stray.json').write_text('{}')
        assert upkeep.discover_sleeves(tmp_path) == ['another', 'paper_s2p']

    def test_a_missing_targets_directory_has_no_sleeves(self, tmp_path):
        assert upkeep.discover_sleeves(tmp_path / 'absent') == []

    def test_the_spec_comes_from_the_newest_targets_file(self, tmp_path):
        _targets(tmp_path, as_of='2026-10-01', capital_usd=50_000.0)
        _targets(tmp_path, as_of='2026-11-02', capital_usd=60_000.0,
                 strategy={'family': 'equity', 'id': 's2p_top_decile', 'params': {}})
        spec = upkeep.sleeve_spec(tmp_path, 'paper_s2p')
        assert (spec.candidate, spec.capital, spec.params, spec.newest_as_of) == (
            'equity/s2p_top_decile', 60_000.0, {}, dt.date(2026, 11, 2))

    @pytest.mark.parametrize('over, needle', [
        ({'strategy': {'family': 'equity'}}, 'names no strategy family/id'),
        ({'strategy': {}}, 'names no strategy family/id'),
        ({'capital_usd': 0}, 'no positive capital_usd'),
        ({'capital_usd': None}, 'no positive capital_usd'),
        ({'capital_usd': True}, 'no positive capital_usd'),
        ({'capital_usd': '50000'}, 'no positive capital_usd'),
    ])
    def test_a_targets_file_that_does_not_say_is_refused_not_guessed(self, tmp_path, over, needle):
        _targets(tmp_path, **over)
        with pytest.raises(ValueError, match=needle):
            upkeep.sleeve_spec(tmp_path, 'paper_s2p')

    def test_a_sleeve_with_no_targets_file_is_refused(self, tmp_path):
        (tmp_path / 'paper_s2p').mkdir()
        with pytest.raises(ValueError, match='has no <as_of>.json'):
            upkeep.sleeve_spec(tmp_path, 'paper_s2p')


def _names(steps):
    return [(step.name, step.sleeve) for step in steps]


class TestPlanSteps:
    def test_an_ordinary_session_refreshes_reconciles_and_tracks(self, tmp_path):
        _targets(tmp_path)
        steps = upkeep.plan_steps(dt.date(2026, 11, 13), ['paper_s2p'], tmp_path, first_of_month=False, python=PY)
        assert _names(steps) == [('bars', ''), ('splits', ''), ('adjust', ''), ('reconcile', 'paper_s2p'), ('track', 'paper_s2p')]
        assert steps[0].argv == (PY, 'scripts/build_pit_universe.py', '--incremental', '--end', '2026-11-13')
        assert steps[1].argv == (PY, 'scripts/fetch_splits.py')
        assert steps[2].argv == (PY, 'scripts/pit_split_adjust.py', '--apply', '--accept-refusals')
        assert steps[3].argv == (PY, '-m', 'trader.rebalance', 'reconcile', '--sleeve', 'paper_s2p')
        assert steps[4].argv == (PY, '-m', 'trader.lowfreq_live', 'track', '--sleeve', 'paper_s2p')

    def test_prices_are_put_on_one_split_basis_before_anything_reads_them(self, tmp_path):
        """Bars are adjusted as of the day they were fetched: a split executed
        since leaves the days before it on the old basis until `adjust` runs."""
        _targets(tmp_path)
        steps = upkeep.plan_steps(dt.date(2026, 12, 1), ['paper_s2p'], tmp_path, first_of_month=True, python=PY)
        names = [step.name for step in steps]
        assert names.index('bars') < names.index('splits') < names.index('adjust')
        for reader in ('track', 'targets', 'plan'):
            assert names.index('adjust') < names.index(reader)

    def test_the_months_first_session_also_writes_the_decision_and_a_dry_run(self, tmp_path):
        _targets(tmp_path)
        steps = upkeep.plan_steps(dt.date(2026, 12, 1), ['paper_s2p'], tmp_path, first_of_month=True,
                                  python=PY, reports_dir=tmp_path / 'reports')
        assert _names(steps) == [('bars', ''), ('splits', ''), ('adjust', ''), ('reconcile', 'paper_s2p'), ('track', 'paper_s2p'),
                                 ('fundamentals', ''), ('targets', 'paper_s2p'), ('plan', 'paper_s2p')]
        assert steps[5].argv == (PY, 'scripts/fetch_fundamentals.py', '--incremental')
        assert steps[6].argv == (PY, '-m', 'trader.lowfreq_live', 'targets', 'equity/s2p_top_quintile',
                                 '--sleeve', 'paper_s2p', '--capital', '50000', '--as-of', '2026-12-01',
                                 '--params', '{"quantile": 0.2}')
        assert steps[7].argv == (PY, '-m', 'trader.rebalance', 'plan', '--sleeve', 'paper_s2p')
        assert steps[7].save_output_to == tmp_path / 'reports' / 'sleeve_paper_s2p_2026-12-01_plan.txt'

    def test_a_decision_already_written_for_the_session_is_not_written_again(self, tmp_path):
        _targets(tmp_path)
        _targets(tmp_path, as_of='2026-12-01')
        steps = upkeep.plan_steps(dt.date(2026, 12, 1), ['paper_s2p'], tmp_path, first_of_month=True, python=PY)
        assert _names(steps) == [('bars', ''), ('splits', ''), ('adjust', ''), ('reconcile', 'paper_s2p'), ('track', 'paper_s2p')]

    def test_params_are_passed_only_when_the_sleeve_has_them(self, tmp_path):
        _targets(tmp_path, strategy={'family': 'equity', 'id': 's2p_top_quintile', 'params': {}})
        steps = upkeep.plan_steps(dt.date(2026, 12, 1), ['paper_s2p'], tmp_path, first_of_month=True, python=PY)
        targets = next(step for step in steps if step.name == 'targets')
        assert '--params' not in targets.argv

    def test_two_sleeves_share_the_data_steps_and_keep_their_own_spec(self, tmp_path):
        _targets(tmp_path, 'alpha', capital_usd=10_000.0)
        _targets(tmp_path, 'beta', capital_usd=25_000.5)
        _targets(tmp_path, 'beta', as_of='2026-12-01')                      # beta already decided
        steps = upkeep.plan_steps(dt.date(2026, 12, 1), ['alpha', 'beta'], tmp_path, first_of_month=True, python=PY)
        assert _names(steps) == [('bars', ''), ('splits', ''), ('adjust', ''), ('reconcile', 'alpha'), ('track', 'alpha'),
                                 ('reconcile', 'beta'), ('track', 'beta'),
                                 ('fundamentals', ''), ('targets', 'alpha'), ('plan', 'alpha')]
        assert '10000' in next(step for step in steps if step.name == 'targets').argv

    def test_the_price_steps_can_be_skipped_together(self, tmp_path):
        _targets(tmp_path)
        steps = upkeep.plan_steps(dt.date(2026, 11, 13), ['paper_s2p'], tmp_path, first_of_month=False,
                                  python=PY, skip_bars=True)
        assert _names(steps) == [('reconcile', 'paper_s2p'), ('track', 'paper_s2p')]

    def test_no_step_can_create_an_order(self, tmp_path):
        """The job stops at the dry run: proposing and approving are the operator's (D15)."""
        _targets(tmp_path)
        steps = upkeep.plan_steps(dt.date(2026, 12, 1), ['paper_s2p'], tmp_path, first_of_month=True, python=PY)
        words = {word for step in steps for word in step.argv}
        assert not words & {'propose', 'approve', 'buy', 'sell', 'close', 'cancel', 'rebuild', '--overwrite'}


class _Recorder:
    def __init__(self, failing=()):
        self.failing = dict(failing)
        self.ran = []

    def __call__(self, step):
        self.ran.append((step.name, step.sleeve))
        return self.failing.get((step.name, step.sleeve), 0), f'{step.name} output\nlast line of {step.name}'


class TestRunning:
    def _steps(self, tmp_path):
        _targets(tmp_path, 'alpha')
        _targets(tmp_path, 'beta')
        return upkeep.plan_steps(dt.date(2026, 12, 1), ['alpha', 'beta'], tmp_path, first_of_month=True,
                                 python=PY, reports_dir=tmp_path / 'reports')

    def test_everything_runs_in_order_and_the_plan_is_saved(self, tmp_path):
        runner = _Recorder()
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert all(outcome.returncode == 0 for outcome in outcomes) and len(runner.ran) == len(outcomes) == 12
        saved = tmp_path / 'reports' / 'sleeve_alpha_2026-12-01_plan.txt'
        assert saved.read_text() == 'plan output\nlast line of plan'
        ok, lines = upkeep.summarise(outcomes)
        assert ok is True
        assert any('plan saved to' in line and 'rebalance propose --sleeve alpha' in line for line in lines)

    def test_a_failed_sleeve_step_stops_that_sleeve_only(self, tmp_path):
        runner = _Recorder({('reconcile', 'alpha'): 1})
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert ('track', 'alpha') not in runner.ran and ('targets', 'alpha') not in runner.ran
        assert ('plan', 'alpha') not in runner.ran
        assert {('track', 'beta'), ('targets', 'beta'), ('plan', 'beta')} <= set(runner.ran)
        skipped = [o for o in outcomes if o.returncode is None]
        assert [(o.step.name, o.step.sleeve) for o in skipped] == [('track', 'alpha'), ('targets', 'alpha'),
                                                                   ('plan', 'alpha')]
        assert not (tmp_path / 'reports' / 'sleeve_alpha_2026-12-01_plan.txt').exists()
        ok, lines = upkeep.summarise(outcomes)
        assert ok is False
        assert any(line.startswith('FAILED  reconcile [alpha] (exit 1):') for line in lines)
        assert any(line == 'SKIPPED track [alpha]: an earlier step for alpha failed' for line in lines)

    def test_a_failed_shared_step_stops_everything_after_it(self, tmp_path):
        runner = _Recorder({('bars', ''): 3})
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert runner.ran == [('bars', '')]
        assert [o.skipped_because for o in outcomes[1:]] == ['bars failed'] * 11
        assert upkeep.summarise(outcomes)[0] is False

    def test_a_split_history_that_cannot_be_read_stops_everything_that_reads_prices(self, tmp_path):
        runner = _Recorder({('splits', ''): 2})
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert runner.ran == [('bars', ''), ('splits', '')]
        assert [o.skipped_because for o in outcomes[2:]] == ['splits failed'] * 10

    def test_an_outage_at_the_fundamentals_vendor_keeps_the_daily_work_and_stops_the_decision(self, tmp_path):
        """Exit 2: the ingest aborted. Nothing to decide on."""
        runner = _Recorder({('fundamentals', ''): 2})
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert {('track', 'alpha'), ('track', 'beta')} <= set(runner.ran)
        assert not any(name in ('targets', 'plan') for name, _ in runner.ran)
        assert upkeep.summarise(outcomes)[0] is False

    def test_a_few_unreadable_names_are_reported_and_the_decision_is_still_attempted(self, tmp_path):
        """Exit 1: some names failed. `targets` has its own staleness gate, and a
        month's decision is not retried the next night - one bad ticker in six
        hundred must not cost it. The job still fails, so pycron reports it."""
        runner = _Recorder({('fundamentals', ''): upkeep.FUNDAMENTALS_SOME_NAMES_FAILED})
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert {('targets', 'alpha'), ('plan', 'alpha'), ('targets', 'beta'), ('plan', 'beta')} <= set(runner.ran)
        assert not any(o.returncode is None for o in outcomes)
        ok, lines = upkeep.summarise(outcomes)
        assert ok is False and any(line.startswith('FAILED  fundamentals (exit 1):') for line in lines)

    def test_an_adjustment_that_refused_a_series_is_reported_and_the_rest_runs(self, tmp_path):
        runner = _Recorder({('adjust', ''): upkeep.ADJUST_SOMETHING_REFUSED})
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert len(runner.ran) == 12 and not any(o.returncode is None for o in outcomes)
        ok, lines = upkeep.summarise(outcomes)
        assert ok is False and any(line.startswith('FAILED  adjust (exit 1):') for line in lines)

    def test_an_adjustment_that_could_not_run_stops_what_reads_prices(self, tmp_path):
        runner = _Recorder({('adjust', ''): 2})
        outcomes = upkeep.run_steps(self._steps(tmp_path), runner)
        assert runner.ran == [('bars', ''), ('splits', ''), ('adjust', '')]
        assert [o.skipped_because for o in outcomes[3:]] == ['adjust failed'] * 9

    def test_only_the_named_statuses_continue(self, tmp_path):
        steps = [upkeep.Step('first', ('x',), continue_on=(1,)), upkeep.Step('second', ('y',))]
        for code, ran_second in ((1, True), (2, False), (3, False)):
            runner = _Recorder({('first', ''): code})
            upkeep.run_steps(steps, runner)
            assert (('second', '') in runner.ran) is ran_second, code

    def test_a_sleeve_step_can_continue_too(self, tmp_path):
        steps = [upkeep.Step('a', ('x',), 'alpha', continue_on=(1,)), upkeep.Step('b', ('y',), 'alpha')]
        runner = _Recorder({('a', 'alpha'): 1})
        outcomes = upkeep.run_steps(steps, runner)
        assert runner.ran == [('a', 'alpha'), ('b', 'alpha')] and upkeep.summarise(outcomes)[0] is False

    def test_a_failed_plan_is_not_saved_as_a_plan(self, tmp_path):
        runner = _Recorder({('plan', 'alpha'): 1})
        upkeep.run_steps(self._steps(tmp_path), runner)
        assert not (tmp_path / 'reports' / 'sleeve_alpha_2026-12-01_plan.txt').exists()
        assert (tmp_path / 'reports' / 'sleeve_beta_2026-12-01_plan.txt').exists()

    def test_a_failure_shows_the_end_of_its_output(self, tmp_path):
        outcomes = upkeep.run_steps(self._steps(tmp_path)[:1], _Recorder({('bars', ''): 2}))
        _, lines = upkeep.summarise(outcomes)
        assert lines == ['FAILED  bars (exit 2):', '    bars output', '    last line of bars']


class TestCommandLine:
    def test_a_dry_run_prints_the_commands_and_runs_nothing(self, tmp_path, capsys, monkeypatch):
        _targets(tmp_path)
        monkeypatch.setattr(upkeep.subprocess, 'run', lambda *a, **k: pytest.fail('a dry run ran a command'))
        code = upkeep.main(['--targets-dir', str(tmp_path), '--session', '2026-12-01', '--dry-run'])
        out = capsys.readouterr().out
        assert code == 0
        assert 'session 2026-12-01 (first of its month), sleeves paper_s2p' in out
        assert 'trader.lowfreq_live targets equity/s2p_top_quintile --sleeve paper_s2p --capital 50000' in out
        assert 'propose' not in out

    def test_no_sleeves_is_nothing_to_do(self, tmp_path, capsys):
        assert upkeep.main(['--targets-dir', str(tmp_path)]) == 0
        assert 'nothing to do' in capsys.readouterr().out

    def test_a_sleeve_whose_spec_cannot_be_read_refuses_before_running_anything(self, tmp_path, capsys, monkeypatch):
        _targets(tmp_path, capital_usd=None)
        monkeypatch.setattr(upkeep.subprocess, 'run', lambda *a, **k: pytest.fail('ran a command'))
        code = upkeep.main(['--targets-dir', str(tmp_path), '--session', '2026-12-01'])
        assert code == 2 and 'REFUSED' in capsys.readouterr().err

    def test_the_exit_code_follows_the_steps(self, tmp_path, capsys, monkeypatch):
        _targets(tmp_path)
        calls = []

        class Done:
            def __init__(self, code):
                self.returncode, self.stdout = code, 'output'

        def fake_run(argv, **kwargs):
            calls.append((argv, kwargs))
            return Done(1 if 'reconcile' in argv else 0)

        monkeypatch.setattr(upkeep.subprocess, 'run', fake_run)
        monkeypatch.delenv('LOWFREQ_RESEARCH_CONFIG', raising=False)
        code = upkeep.main(['--targets-dir', str(tmp_path), '--session', '2026-11-13'])
        assert code == 1 and 'sleeve upkeep FAILED' in capsys.readouterr().out
        assert [argv[1:3] for argv, _ in calls] == [['scripts/build_pit_universe.py', '--incremental'],
                                                    ['scripts/fetch_splits.py'],
                                                    ['scripts/pit_split_adjust.py', '--apply'],
                                                    ['-m', 'trader.rebalance']]
        environment = calls[0][1]['env']
        assert environment['LOWFREQ_RESEARCH_CONFIG'].endswith('trader.yaml')    # the harness reads it (D03)
