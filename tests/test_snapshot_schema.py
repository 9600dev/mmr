"""Snapshot fixes (2026-11-05 smoke test).

- IB '-1' no-data sentinels became real numbers (closed ASX: bid=-1/ask=-1)
- `close` meant previous close on IB but last price on twelvedata
- `snapshot BHP --exchange ASX` silently returned the NYSE ADR via twelvedata
- snapshot-batch over IB was hard-coded to delayed data
"""
import argparse
import math
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import trader.mmr_cli as cli
from trader.sdk import MMR, normalize_ib_quote


def test_ib_sentinels_become_nan():
    q = normalize_ib_quote({'bid': -1, 'bidSize': 0, 'ask': -1, 'askSize': 0, 'last': None,
                            'close': 59.0, 'volume': -1})
    assert math.isnan(q['bid']) and math.isnan(q['bidSize'])
    assert math.isnan(q['ask']) and math.isnan(q['last']) and math.isnan(q['volume'])
    assert q['close'] == 59.0 and q['previous_close'] == 59.0
    assert math.isnan(q['change']) and q['source'] == 'ib'


def test_ib_change_derived_from_previous_close():
    q = normalize_ib_quote({'bid': 344.22, 'ask': 344.32, 'last': 344.29, 'close': 349.51})
    assert q['change'] == pytest.approx(-5.22)
    assert q['change_pct'] == pytest.approx(-1.4935, abs=1e-3)


@pytest.fixture
def _td_mmr(monkeypatch):
    def make(payload):
        mmr = MMR.__new__(MMR)
        client = MagicMock()
        client.quote.return_value.as_json.return_value = payload
        monkeypatch.setattr(MMR, '_twelvedata_client', property(lambda self: client), raising=False)
        return mmr
    return make


def test_twelvedata_close_means_previous_close(_td_mmr):
    mmr = _td_mmr({'symbol': 'AAPL', 'close': '343.77', 'previous_close': '349.51',
                   'open': '353.9', 'high': '355.38', 'low': '342.1', 'volume': '1',
                   'change': '-5.7', 'percent_change': '-1.6', 'datetime': '2026-11-05'})
    q = mmr.snapshot('AAPL', source='twelvedata')
    assert q['last'] == 343.77
    assert q['close'] == q['previous_close'] == 349.51
    assert q['source'] == 'twelvedata'


def test_twelvedata_refuses_exchange_hint(_td_mmr):
    mmr = _td_mmr({})
    with pytest.raises(ValueError, match='--source ib'):
        mmr.snapshot('BHP', exchange='ASX', currency='AUD', source='twelvedata')


@pytest.mark.parametrize('kw, expected', [
    (dict(source='twelvedata', exchange='', currency=''), 'twelvedata'),
    (dict(source='ib', exchange='', currency=''), 'ib'),
    (dict(source=None, exchange='ASX', currency=''), 'ib'),
    (dict(source=None, exchange='', currency='AUD'), 'ib'),
])
def test_effective_snapshot_source(kw, expected):
    assert cli._effective_snapshot_source(argparse.Namespace(**kw)) == expected


def test_batch_ib_path_is_live_by_default():
    mmr = MMR.__new__(MMR)
    calls = []
    client = MagicMock()
    client.is_setup = True
    def _batch(contracts, delayed):
        calls.append(delayed)
        return [{'symbol': 'AAPL', 'bid': 1.0, 'ask': 1.1, 'last': 1.05, 'close': 1.0}]
    client.rpc.return_value.get_snapshots_batch.side_effect = _batch
    mmr._client = client
    mmr._resolve_contract = lambda sym, **kw: SimpleNamespace(symbol=sym)
    rows = mmr.snapshot_batch(['AAPL'])
    assert calls == [False]
    assert rows[0]['source'] == 'ib' and rows[0]['previous_close'] == 1.0
