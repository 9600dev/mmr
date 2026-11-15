"""The gauntlet can be run with a deployment's ``params:`` (audit 2026-11-14, B7).

S2 (lookahead) and S3 (battery) run on session-stamped frames. The session
came from the strategy CLASS (``SESSION_TZ`` / ``RTH_OPEN_MIN`` /
``RTH_CLOSE_MIN``), and the instance under test was built with no params. An
ASX deployment sets those three in ``strategy_runtime.yaml`` ``params:``, so
the gauntlet exercised ``OpeningRangeBreakout`` on 09:30-16:00 New York for a
strategy that trades 10:00-16:00 Sydney — a session that straddles midnight
UTC, which is exactly where per-day code goes wrong.

``mmr strategies gauntlet ... --param KEY=VALUE / --params JSON`` (the
backtest's own override parsing and coercion) now builds both the frames and
the strategy instance from those params and records them with the verdict. A
run without params is what it was.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pytest

import trader.mmr_cli as cli
from strategies.opening_range_breakout import OpeningRangeBreakout
from strategies.vwap_reclaim import VwapReclaim
from trader.data.backtest_store import compute_strategy_hash
from trader.data.gauntlet_store import GauntletStore
from trader.mmr_cli import (
    _gauntlet_check_lookahead,
    _gauntlet_new_instance,
    _gauntlet_run,
    _gauntlet_run_battery,
    _gauntlet_session_spec,
    _handle_strategies_gauntlet,
)

REPO = Path(__file__).resolve().parent.parent
ORB = REPO / 'strategies' / 'opening_range_breakout.py'
NEW_YORK = {'tz': 'America/New_York', 'open_minute': 570, 'close_minute': 960}
SYDNEY = {'tz': 'Australia/Sydney', 'open_minute': 600, 'close_minute': 960}
ASX_PARAMS = {'SESSION_TZ': 'Australia/Sydney', 'RTH_OPEN_MIN': 600, 'RTH_CLOSE_MIN': 960}


def _namespace(**kwargs):
    base = dict(module_path=str(ORB), class_name='OpeningRangeBreakout', min_psr=None, name=None)
    base.update(kwargs)
    return argparse.Namespace(**base)


@pytest.fixture
def gauntlet_db(tmp_duckdb_path, monkeypatch):
    monkeypatch.setattr(cli, '_gauntlet_db_path', lambda: tmp_duckdb_path)
    return tmp_duckdb_path


class TestSessionComesFromTheDeploymentParams:
    def test_without_params_the_class_session_is_what_is_tested(self, tmp_duckdb_path):
        """The defect as it stood, and the unchanged default."""
        result = _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path, bar_size='1 min')
        assert result['verdict'] == 'PASS'
        assert result['checks']['s2_lookahead']['session'] == NEW_YORK
        assert result['checks']['s3_battery']['session'] == NEW_YORK
        assert 'params' not in result['checks']['s2_lookahead']
        assert 'params' not in result['checks']['s3_battery']

    def test_asx_params_put_both_stages_on_the_sydney_session(self, tmp_duckdb_path):
        result = _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path, bar_size='1 min',
                               params=ASX_PARAMS)
        s2, s3 = result['checks']['s2_lookahead'], result['checks']['s3_battery']
        assert s2['session'] == SYDNEY and s3['session'] == SYDNEY
        assert s2['params'] == ASX_PARAMS and s3['params'] == ASX_PARAMS
        assert result['params'] == ASX_PARAMS

    def test_the_instance_under_test_uses_the_params_too(self, tmp_duckdb_path):
        """Sydney frames are 18:00-00:00 New York: an instance still on the
        class's New York session would see no session bar, compute nothing and
        fail both stages as vacuous. It passes, with signals, so the frames
        AND the strategy are on Sydney hours."""
        result = _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path, bar_size='1 min',
                               params=ASX_PARAMS)
        assert result['verdict'] == 'PASS', result['checks']
        s2, s3 = result['checks']['s2_lookahead'], result['checks']['s3_battery']
        assert {'orb_high', 'orb_low', 'exit_low'} <= set(s2['exercised_keys'])
        assert s3['signals'] > 0
        session_frames = [name for name, frame in s3['frames'].items()
                          if name.startswith('session_') and frame.get('signals')]
        assert session_frames, 'no session frame signalled'

    def test_sydney_frames_without_the_params_on_the_instance_would_be_vacuous(self):
        """The control for the test above: frames on one session, strategy on
        another, is the vacuous case — not a pass."""
        frames = cli._gauntlet_frames(SYDNEY, '1 min', battery=False)
        state = _gauntlet_new_instance(OpeningRangeBreakout, frames['session_trending']).precompute(
            frames['session_trending'])
        assert np.isnan(state['orb_high']).all()
        tuned = _gauntlet_new_instance(OpeningRangeBreakout, frames['session_trending'], ASX_PARAMS)
        assert not np.isnan(tuned.precompute(frames['session_trending'])['orb_high']).all()

    def test_session_spec_reads_params_over_class_attributes(self):
        assert _gauntlet_session_spec(OpeningRangeBreakout) == NEW_YORK
        assert _gauntlet_session_spec(OpeningRangeBreakout, ASX_PARAMS) == SYDNEY
        assert _gauntlet_session_spec(OpeningRangeBreakout, {'RTH_OPEN_MIN': 600}) == {
            **NEW_YORK, 'open_minute': 600}
        assert _gauntlet_session_spec(OpeningRangeBreakout, {'RANGE_MINUTES': 15}) == NEW_YORK
        assert _gauntlet_session_spec(OpeningRangeBreakout, {}) == NEW_YORK
        assert _gauntlet_session_spec(OpeningRangeBreakout, None) == NEW_YORK

    def test_stage_functions_take_the_params(self):
        s2 = _gauntlet_check_lookahead(OpeningRangeBreakout, '1 min', ASX_PARAMS)
        s3 = _gauntlet_run_battery(OpeningRangeBreakout, '1 min', ASX_PARAMS)
        assert s2['status'] == 'pass' and s2['session'] == SYDNEY
        assert s3['status'] == 'pass' and s3['session'] == SYDNEY and s3['signals'] > 0


class TestOtherParamsReachTheInstance:
    def test_class_attribute_and_params_dict_idioms(self):
        instance = _gauntlet_new_instance(OpeningRangeBreakout, None,
                                          {**ASX_PARAMS, 'RANGE_MINUTES': 15, 'my_knob': 3})
        assert instance.RANGE_MINUTES == 15 and instance.RTH_OPEN_MIN == 600
        assert instance.params['SESSION_TZ'] == 'Australia/Sydney'
        assert instance.params['my_knob'] == 3
        assert OpeningRangeBreakout.RANGE_MINUTES == 30, 'the class is not mutated'

    def test_cli_strings_are_coerced_like_a_backtest_override(self, tmp_duckdb_path):
        result = _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path, bar_size='1 min',
                               params={'SESSION_TZ': 'Australia/Sydney', 'RTH_OPEN_MIN': '600',
                                       'RTH_CLOSE_MIN': '960', 'EOD_FLATTEN': 'true'})
        assert result['params'] == {**ASX_PARAMS, 'EOD_FLATTEN': True}
        assert result['checks']['s3_battery']['session'] == SYDNEY

    def test_a_non_session_param_changes_what_is_tested(self, tmp_duckdb_path):
        default = _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path, bar_size='1 min')
        tight = _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path, bar_size='1 min',
                              params={'VOLUME_MULT': 50.0})
        assert default['checks']['s3_battery']['signals'] > 0
        assert tight['checks']['s3_battery']['signals'] < default['checks']['s3_battery']['signals']


class TestBadParamsAreRefusedNotGuessed:
    def test_a_typo_is_refused_by_name(self, tmp_duckdb_path):
        with pytest.raises(ValueError, match='RTH_OPEN_MINS'):
            _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path,
                          params={'RTH_OPEN_MINS': 600})

    def test_an_unknown_timezone_is_refused(self, tmp_duckdb_path):
        with pytest.raises(ValueError, match='SESSION_TZ'):
            _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path,
                          params={'SESSION_TZ': 'Mars/Olympus_Mons'})

    @pytest.mark.parametrize('params', [
        {'RTH_OPEN_MIN': 960, 'RTH_CLOSE_MIN': 600},
        {'RTH_OPEN_MIN': 1000},                      # after the class's 16:00 close
        {'RTH_CLOSE_MIN': 2000},
        {'RTH_OPEN_MIN': -5},
    ])
    def test_a_session_that_cannot_exist_is_refused(self, tmp_duckdb_path, params):
        """A class that DECLARES a broken session falls back to the default
        frames (unchanged); a session ASKED FOR on the command line is never
        silently replaced by another one."""
        with pytest.raises(ValueError, match='session'):
            _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path, params=params)

    def test_a_fractional_minute_is_refused(self, tmp_duckdb_path):
        with pytest.raises(ValueError, match='RTH_OPEN_MIN'):
            _gauntlet_run(ORB, 'OpeningRangeBreakout', tmp_duckdb_path,
                          params={'RTH_OPEN_MIN': 600.5})

    def test_a_refused_run_records_nothing(self, gauntlet_db):
        _handle_strategies_gauntlet(_namespace(param=['RTH_OPEN_MINS=600'], params=None))
        assert GauntletStore(gauntlet_db).latest_for_hash(
            compute_strategy_hash(str(ORB)), 'OpeningRangeBreakout') is None

    def test_session_tz_on_a_class_that_ignores_it_is_vacuous_not_passed(self, tmp_duckdb_path):
        """VwapReclaim is hard-coded to New York. Asking for Sydney frames is
        accepted (SESSION_TZ is a runtime-owned param for every class) and the
        answer is the truth: it computes nothing there."""
        assert _gauntlet_session_spec(VwapReclaim, {'SESSION_TZ': 'Australia/Sydney'}) == {
            'tz': 'Australia/Sydney', 'open_minute': 570, 'close_minute': 960}
        result = _gauntlet_run(REPO / 'strategies' / 'vwap_reclaim.py', 'VwapReclaim', tmp_duckdb_path,
                               bar_size='1 min', params={'SESSION_TZ': 'Australia/Sydney'})
        assert result['verdict'] == 'FAIL'
        assert result['checks']['s3_battery'].get('vacuous') is True


class TestCommandLine:
    def test_parser_accepts_param_and_params(self):
        parser = cli.build_parser() if hasattr(cli, 'build_parser') else None
        if parser is None:
            pytest.skip('no build_parser entry point')
        args = parser.parse_args(['strategies', 'gauntlet', str(ORB), '--class', 'OpeningRangeBreakout',
                                  '--param', 'SESSION_TZ=Australia/Sydney', '--param', 'RTH_OPEN_MIN=600',
                                  '--params', '{"RTH_CLOSE_MIN": 960}'])
        assert args.param == ['SESSION_TZ=Australia/Sydney', 'RTH_OPEN_MIN=600']
        assert args.params == '{"RTH_CLOSE_MIN": 960}'

    def test_param_flags_are_recorded_with_the_verdict(self, gauntlet_db, capsys, monkeypatch):
        monkeypatch.setattr(cli, '_json_mode', True)
        _handle_strategies_gauntlet(_namespace(
            bar_size='1 min', name='orb_wds',
            param=['SESSION_TZ=Australia/Sydney', 'RTH_OPEN_MIN=600'],
            params='{"RTH_CLOSE_MIN": 960}'))
        payload = json.loads(capsys.readouterr().out)['data']
        assert payload['verdict'] == 'PASS'
        assert payload['params'] == ASX_PARAMS

        record = GauntletStore(gauntlet_db).latest_for_hash(
            compute_strategy_hash(str(ORB)), 'OpeningRangeBreakout')
        assert record.verdict == 'PASS' and record.strategy_name == 'orb_wds'
        assert record.checks['s2_lookahead']['session'] == SYDNEY
        assert record.checks['s3_battery']['session'] == SYDNEY
        assert record.checks['s3_battery']['params'] == ASX_PARAMS
        assert 'Australia/Sydney' in record.notes and 'RTH_OPEN_MIN' in record.notes

    def test_run_without_params_records_what_it_always_did(self, gauntlet_db, capsys, monkeypatch):
        monkeypatch.setattr(cli, '_json_mode', True)
        _handle_strategies_gauntlet(_namespace(bar_size='1 min'))
        payload = json.loads(capsys.readouterr().out)['data']
        assert payload['verdict'] == 'PASS' and payload['params'] == {}
        record = GauntletStore(gauntlet_db).latest_for_hash(
            compute_strategy_hash(str(ORB)), 'OpeningRangeBreakout')
        assert record.checks['s3_battery']['session'] == NEW_YORK
        assert 'params' not in record.checks['s2_lookahead'] and 'params' not in record.checks['s3_battery']
        assert set(record.checks) == {'s1_imports', 's2_lookahead', 's3_battery', 's4_psr', 's5_dsr'}
        assert 'params' not in record.notes

    def test_table_output_names_the_params_and_the_session(self, gauntlet_db, capsys, monkeypatch):
        monkeypatch.setattr(cli, '_json_mode', False)
        _handle_strategies_gauntlet(_namespace(bar_size='1 min', param=[], params=json.dumps(ASX_PARAMS)))
        printed = ' '.join(capsys.readouterr().out.split())
        assert 'run with params' in printed and 'Australia/Sydney 10:00-16:00' in printed

        _handle_strategies_gauntlet(_namespace(bar_size='1 min'))
        assert 'run with params' not in capsys.readouterr().out

    def test_malformed_flag_is_reported_and_nothing_runs(self, gauntlet_db, capsys):
        _handle_strategies_gauntlet(_namespace(param=['RTH_OPEN_MIN'], params=None))
        assert GauntletStore(gauntlet_db).latest_for_hash(
            compute_strategy_hash(str(ORB)), 'OpeningRangeBreakout') is None

    def test_pass_record_is_still_keyed_by_source_hash_and_class(self, gauntlet_db):
        """Unchanged on purpose (see the report on B7): a PASS run under the
        Sydney session satisfies the same has_pass() a default run does."""
        _handle_strategies_gauntlet(_namespace(bar_size='1 min', param=[], params=json.dumps(ASX_PARAMS)))
        assert GauntletStore(gauntlet_db).has_pass(compute_strategy_hash(str(ORB)), 'OpeningRangeBreakout')
