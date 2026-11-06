"""`mmr close` targets a position by symbol; row numbers are REPL-only.

Field finding 2026-11-05: `close <row>` could never work as a one-shot command
(the row map only exists inside a REPL session), and rows are ordered by daily
P&L, so even where they "worked" they could point at a different position
between display and close. Non-interactive closes also crashed on input().
"""
import argparse
import json
from types import SimpleNamespace

import pandas as pd
import pytest

import trader.mmr_cli as cli
from trader.common.reactivex import SuccessFail


class _FakeMMR:
    def __init__(self):
        self.closed = []
        self._position_map = {}

    def portfolio(self):
        df = pd.DataFrame([{'symbol': 'F', 'conId': 9599491, 'position': 1.0},
                           {'symbol': 'T', 'conId': 37018770, 'position': 34.0}])
        self._position_map = {1: 'F', 2: 'T'}
        return df

    def positions(self):
        return self.portfolio()

    def close_position(self, symbol, quantity=None, con_id=None, _pos_size=None, **kw):
        self.closed.append((symbol, quantity, con_id))
        trade = SimpleNamespace(order=SimpleNamespace(orderId=1082),
                                orderStatus=SimpleNamespace(status='PreSubmitted'))
        return SuccessFail.success(obj=trade)


def _args(**kw):
    base = dict(target=None, conid=None, quantity=None, yes=False)
    base.update(kw)
    return argparse.Namespace(**base)


@pytest.fixture(autouse=True)
def _state(monkeypatch):
    monkeypatch.setattr(cli, '_exit_code', 0)
    monkeypatch.setattr(cli, '_json_mode', True)
    monkeypatch.setattr(cli, '_interactive', False)


def test_close_by_symbol_with_yes(capsys):
    mmr = _FakeMMR()
    cli._handle_close(mmr, _args(target='f', yes=True))
    assert mmr.closed == [('F', 1.0, 9599491)]
    out = json.loads(capsys.readouterr().out)
    assert out['success'] and out['order_id'] == 1082 and out['broker_status'] == 'PreSubmitted'
    assert cli._exit_code == 0


def test_close_by_conid(capsys):
    mmr = _FakeMMR()
    cli._handle_close(mmr, _args(conid=37018770, yes=True, quantity=10))
    assert mmr.closed == [('T', 10.0, 37018770)]


def test_row_number_refused_in_one_shot_mode(capsys):
    mmr = _FakeMMR()
    cli._handle_close(mmr, _args(target='1', yes=True))
    assert mmr.closed == []
    out = json.loads(capsys.readouterr().out)
    assert out['success'] is False and 'REPL' in out['message']
    assert cli._exit_code == 1


def test_row_number_works_in_repl(monkeypatch):
    monkeypatch.setattr(cli, '_interactive', True)
    mmr = _FakeMMR()
    mmr.portfolio()                       # the table the operator just looked at
    cli._handle_close(mmr, _args(target='2', yes=True))
    assert mmr.closed == [('T', 34.0, 37018770)]


def test_non_interactive_without_yes_is_refused_not_crashed(capsys):
    mmr = _FakeMMR()
    cli._handle_close(mmr, _args(target='F'))
    assert mmr.closed == []
    assert '--yes' in json.loads(capsys.readouterr().out)['message']
    assert cli._exit_code == 1


def test_oversized_close_is_clamped():
    mmr = _FakeMMR()
    cli._handle_close(mmr, _args(target='F', quantity=5, yes=True))
    assert mmr.closed == [('F', 1.0, 9599491)]


def test_unknown_symbol_fails():
    mmr = _FakeMMR()
    cli._handle_close(mmr, _args(target='ZZZZ', yes=True))
    assert mmr.closed == [] and cli._exit_code == 1


def test_close_all_requires_yes_and_supports_dry_run(capsys, monkeypatch):
    mmr = _FakeMMR()
    mmr.cancel_all = lambda: SuccessFail.success()
    cli._handle_close_all(mmr, argparse.Namespace(yes=False, dry_run=True))
    assert mmr.closed == []
    cli._handle_close_all(mmr, argparse.Namespace(yes=False, dry_run=False))
    assert mmr.closed == [] and cli._exit_code == 1


def test_parser_accepts_new_close_forms():
    p = cli.build_parser()
    a = p.parse_args(['close', 'F', '--yes', '--quantity', '2'])
    assert (a.target, a.yes, a.quantity) == ('F', True, 2.0)
    a = p.parse_args(['close', '--conid', '123', '-y'])
    assert a.conid == 123 and a.yes
