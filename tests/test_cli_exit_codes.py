"""`mmr` one-shot invocations report failure through the exit status.

Field finding 2026-11-05: every failure — a refused order, an unknown
proposal, an argparse usage error — exited 0, so the LLM loop / cron could
only detect failure by parsing prose, and three different JSON failure shapes
were in use. Failures now set a non-zero exit code and every JSON failure
carries ``success: false`` and ``message``.
"""
import json
import sys

import pytest

import trader.mmr_cli as cli


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(cli, '_exit_code', 0)
    monkeypatch.setattr(cli, '_json_mode', True)
    yield


def test_success_payload_leaves_exit_code_zero(capsys):
    cli.print_dict({'a': 1}, title='t')
    assert cli._exit_code == 0
    assert json.loads(capsys.readouterr().out)['data'] == {'a': 1}


@pytest.mark.parametrize('payload', [
    {'success': False, 'message': 'boom'},
    {'success': False, 'action': 'BUY', 'symbol': 'F', 'error': 'boom'},
    {'error': True, 'message': 'boom', 'role': 'proposer'},
])
def test_every_failure_shape_normalizes_and_sets_exit_code(payload):
    out = json.loads(cli._json_dumps(payload))
    assert out['success'] is False and out['message'] == 'boom'
    assert cli._exit_code == 1
    for k in payload:            # original keys are preserved
        assert k in out


def test_print_status_failure_sets_exit_code(capsys):
    cli.print_status('nope', success=False)
    assert cli._exit_code == 1
    assert json.loads(capsys.readouterr().out) == {'success': False, 'message': 'nope'}


def test_red_console_line_sets_exit_code(monkeypatch):
    monkeypatch.setattr(cli, '_json_mode', False)
    cli.console.print('[red]Failed to do the thing[/red]')
    assert cli._exit_code == 1


def test_non_error_console_line_does_not(monkeypatch):
    monkeypatch.setattr(cli, '_json_mode', False)
    cli.console.print('[green]ok[/green]')
    cli.console.print('[yellow]warn[/yellow]')
    assert cli._exit_code == 0


def test_usage_error_exits_2(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['mmr', 'financials', 'AAPL'])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2
