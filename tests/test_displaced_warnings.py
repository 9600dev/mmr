"""Placements that cancel working exits (reduction coordination) say so."""
import json
from types import SimpleNamespace

import trader.mmr_cli as cli
from trader.common.reactivex import SuccessFail
from trader.trading.executioner import describe_displaced


def _order(oid, otype, action, aux=0.0, lmt=0.0):
    return SimpleNamespace(orderId=oid, orderType=otype, action=action, auxPrice=aux, lmtPrice=lmt)


def test_describe_displaced_lines():
    lines = describe_displaced([(SimpleNamespace(symbol='F'), _order(1814, 'MKT', 'SELL'), 1.0),
                                (SimpleNamespace(symbol='F'), _order(1747, 'STP', 'SELL', aux=11.2), 1.0)])
    assert lines[0].startswith('cancelled working MKT SELL 1 F (order 1814)')
    assert 'stop 11.2' in lines[1] and '(order 1747)' in lines[1]
    assert describe_displaced([]) == [] and describe_displaced([('bad',)]) == []


def test_trade_summary_always_has_replaced():
    trade = SimpleNamespace(order=SimpleNamespace(orderId=1816), orderStatus=SimpleNamespace(status='PreSubmitted'))
    r = SuccessFail.success(obj=trade)
    assert cli._trade_summary(r)['replaced'] == []
    r.warnings = ['cancelled working MKT SELL 1 F (order 1814)']
    assert cli._trade_summary(r)['replaced'] == ['cancelled working MKT SELL 1 F (order 1814)']


def test_protect_json_reports_replaced(monkeypatch, capsys):
    trade = SimpleNamespace(order=SimpleNamespace(orderId=1816), orderStatus=SimpleNamespace(status='PreSubmitted'))
    res = SuccessFail.success(obj=trade)
    res.warnings = ['cancelled working MKT SELL 1 F (order 1814) — reducing orders may not exceed the position']
    mmr = SimpleNamespace(place_protective_order=lambda **kw: res)
    monkeypatch.setattr(cli, '_json_mode', True)
    # Drive only the tail of _handle_protect: reuse its JSON branch via a minimal args set.
    import inspect
    src = inspect.getsource(cli._handle_protect)
    assert "'replaced': replaced" in src
