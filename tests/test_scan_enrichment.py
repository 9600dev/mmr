"""`mmr scan` enrichment + non-common exclusion (2026-11-05 smoke test).

The IB scanner path returned only rank/symbol/conId — no price, change or
volume — and listed rights such as 'NMZ RT' alongside common shares. Pinned:

- classification uses IB ContractDetails.stockType (authoritative; verified
  live: NMZ RT -> RIGHT, AIIA U / ALFUU -> UNIT, BRK B -> COMMON), with a
  conservative space-suffix fallback only when stockType is missing;
- prices come from ONE get_snapshots_batch call, matched by conId, and an
  unavailable value is null (NaN), never 0;
- `--include-all` keeps everything; JSON and Rich both show what was excluded.
"""

import argparse
import asyncio
import io
import json
import math
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from rich.console import Console

import trader.mmr_cli as cli
from trader.sdk import MMR
from trader.tools.instrument_filters import is_unclassified, scan_exclusion_reason


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('row,excluded', [
    # stockType present: authoritative in both directions
    ({'symbol': 'NMZ RT', 'secType': 'STK', 'stockType': 'RIGHT'}, True),
    ({'symbol': 'ARCLR', 'secType': 'STK', 'stockType': 'RIGHT'}, True),
    ({'symbol': 'AIIA U', 'secType': 'STK', 'stockType': 'UNIT'}, True),
    ({'symbol': 'ALFUU', 'secType': 'STK', 'stockType': 'UNIT'}, True),
    ({'symbol': 'XYZ WS', 'secType': 'STK', 'stockType': 'WARRANT'}, True),
    ({'symbol': 'BRK B', 'secType': 'STK', 'stockType': 'COMMON'}, False),
    ({'symbol': 'JPM PRC', 'secType': 'STK', 'stockType': 'PREFERRED'}, False),
    ({'symbol': 'IREX', 'secType': 'STK', 'stockType': 'ETF'}, False),
    ({'symbol': 'VSA', 'secType': 'STK', 'stockType': 'ADR'}, False),
    ({'symbol': 'NMZ', 'secType': 'STK', 'stockType': 'CLOSED-END FUND'}, False),
    # stockType says COMMON even though the symbol has an IB-style suffix
    ({'symbol': 'ABC U', 'secType': 'STK', 'stockType': 'COMMON'}, False),
    # warrant secTypes regardless of stockType
    ({'symbol': 'XYZ', 'secType': 'WAR', 'stockType': ''}, True),
    ({'symbol': 'XYZ', 'secType': 'IOPT'}, True),
    # fallback (no stockType): IB space-suffix convention only
    ({'symbol': 'NMZ RT', 'secType': 'STK'}, True),
    ({'symbol': 'NMZ', 'localSymbol': 'NMZ RT', 'secType': 'STK'}, True),
    ({'symbol': 'XYZ WS', 'secType': 'STK', 'stockType': None}, True),
    ({'symbol': 'XYZ WS A', 'secType': 'STK'}, True),
    ({'symbol': 'AIIA U', 'secType': 'STK'}, True),
    ({'symbol': 'ABC UN', 'secType': 'STK'}, True),
    ({'symbol': 'BRK B', 'secType': 'STK'}, False),
    ({'symbol': 'JPM PRC', 'secType': 'STK'}, False),
    ({'symbol': 'DBM.U', 'secType': 'STK'}, False),      # TSX USD line, not a unit
    ({'symbol': 'ALFUU', 'secType': 'STK'}, False),      # 5th letter alone is a guess
    ({'symbol': 'GOOGL', 'secType': 'STK'}, False),
    ({'symbol': 'ARCLR', 'secType': 'STK'}, False),
    ({'symbol': 'AAPL', 'secType': 'STK'}, False),
    ({'symbol': 'TRT', 'secType': 'STK'}, False),        # ends in RT, no space
    ({'symbol': 'NEWS', 'secType': 'STK'}, False),       # ends in WS, no space
])
def test_scan_exclusion_reason(row, excluded):
    assert (scan_exclusion_reason(row) is not None) is excluded


def test_reason_names_the_field():
    assert scan_exclusion_reason({'symbol': 'NMZ RT', 'stockType': 'RIGHT'}) == 'stockType RIGHT'
    assert 'stockType unavailable' in scan_exclusion_reason({'symbol': 'NMZ RT'})


def test_unclassified():
    assert is_unclassified({'symbol': 'A'}) and is_unclassified({'stockType': ' '})
    assert not is_unclassified({'stockType': 'COMMON'})


# ---------------------------------------------------------------------------
# SDK enrichment
# ---------------------------------------------------------------------------

ROWS = [
    {'rank': 1, 'symbol': 'CIIT', 'secType': 'STK', 'exchange': 'SMART', 'currency': 'USD',
     'conId': 902535787, 'stockType': 'COMMON'},
    {'rank': 2, 'symbol': 'NMZ RT', 'secType': 'STK', 'exchange': 'SMART', 'currency': 'USD',
     'conId': 939138231, 'stockType': 'RIGHT'},
    {'rank': 3, 'symbol': 'IREX', 'secType': 'STK', 'exchange': 'SMART', 'currency': 'USD',
     'conId': 859589334, 'stockType': 'ETF'},
    {'rank': 4, 'symbol': 'QUIET', 'secType': 'STK', 'exchange': 'NYSE', 'currency': 'USD',
     'conId': 111, 'stockType': None},
]

SNAPS = [  # deliberately out of order; QUIET missing; IREX has no last trade
    {'symbol': 'IREX', 'conId': 859589334, 'bid': 9.9, 'ask': 10.1, 'last': float('nan'),
     'open': 9.0, 'high': 10.5, 'low': 8.8, 'close': 9.5, 'volume': 0.0},
    {'symbol': 'CIIT', 'conId': 902535787, 'bid': 2.73, 'ask': 2.76, 'last': 2.74,
     'open': 1.65, 'high': float('nan'), 'low': float('nan'), 'close': 1.55,
     'volume': 547424.0},
]


@pytest.fixture
def mmr(monkeypatch):
    client = MagicMock()
    rpc = client.rpc.return_value
    rpc.scanner_data.return_value = [dict(r) for r in ROWS]
    rpc.get_snapshots_batch.return_value = [dict(s) for s in SNAPS]
    monkeypatch.setattr(MMR, '_rpc', property(lambda self: client), raising=False)
    from trader.trading.trading_filter import TradingFilter
    monkeypatch.setattr(TradingFilter, 'load', classmethod(lambda cls, *a, **k: TradingFilter()))
    m = MMR.__new__(MMR)
    m._test_rpc = rpc
    return m


class TestScanEnrichment:
    def test_excludes_right_and_enriches_in_one_batch(self, mmr):
        df = mmr.scan()
        assert list(df['symbol']) == ['CIIT', 'IREX', 'QUIET']
        assert mmr._test_rpc.get_snapshots_batch.call_count == 1
        contracts, delayed = mmr._test_rpc.get_snapshots_batch.call_args[0]
        assert [c.conId for c in contracts] == [902535787, 859589334, 111]
        assert delayed is False
        assert contracts[2].primaryExchange == 'NYSE' and contracts[0].primaryExchange == ''
        ciit = df.set_index('symbol').loc['CIIT']
        assert ciit['last'] == 2.74
        assert ciit['change_pct'] == pytest.approx((2.74 - 1.55) / 1.55 * 100, abs=1e-4)
        assert ciit['volume'] == 547424.0
        assert df.attrs['excluded'] == [{'symbol': 'NMZ RT', 'conId': 939138231,
                                         'stockType': 'RIGHT', 'reason': 'stockType RIGHT'}]
        assert df.attrs['unclassified'] == 1           # QUIET has no stockType

    def test_unavailable_is_null_never_zero(self, mmr):
        df = mmr.scan().set_index('symbol')
        irex, quiet = df.loc['IREX'], df.loc['QUIET']
        assert math.isnan(irex['last']) and math.isnan(irex['change_pct'])
        assert irex['volume'] == 0.0                    # a real reported zero stays 0
        for col in ('last', 'change_pct', 'volume'):
            assert math.isnan(quiet[col])               # no snapshot at all -> null
        assert any('QUIET' in w for w in mmr.scan().attrs['warnings'])
        js = json.loads(mmr.scan().to_json(orient='records'))
        q = next(r for r in js if r['symbol'] == 'QUIET')
        assert q['last'] is None and q['volume'] is None and q['change_pct'] is None

    def test_include_all_keeps_rights(self, mmr):
        df = mmr.scan(include_all=True)
        assert 'NMZ RT' in list(df['symbol'])
        assert df.attrs['excluded'] == []

    def test_snapshot_failure_is_loud_not_fatal(self, mmr):
        mmr._test_rpc.get_snapshots_batch.side_effect = TimeoutError('rpc timeout')
        df = mmr.scan()
        assert len(df) == 3
        assert df['last'].isna().all() and df['volume'].isna().all()
        assert any('price enrichment failed' in w for w in df.attrs['warnings'])

    def test_all_excluded_skips_snapshot(self, mmr):
        mmr._test_rpc.scanner_data.return_value = [dict(ROWS[1])]
        df = mmr.scan()
        assert df.empty and len(df.attrs['excluded']) == 1
        mmr._test_rpc.get_snapshots_batch.assert_not_called()

    def test_empty_scanner_result(self, mmr):
        mmr._test_rpc.scanner_data.return_value = []
        assert mmr.scan().empty


# ---------------------------------------------------------------------------
# CLI output
# ---------------------------------------------------------------------------

def _scan_ns(**kw):
    d = dict(list_locations=False, scan_code=None, preset=None, instrument='STK',
             location='STK.US.MAJOR', num=20, above_price=0.0, above_volume=0,
             market_cap_above=0.0, include_all=False)
    d.update(kw)
    return argparse.Namespace(**d)


@pytest.fixture
def rich_out(monkeypatch):
    buf = io.StringIO()
    monkeypatch.setattr(cli, 'console', Console(file=buf, width=200, force_terminal=False))
    return buf


class TestScanCli:
    def test_json(self, mmr, monkeypatch, capsys, rich_out):
        monkeypatch.setattr(cli, '_json_mode', True)
        cli._handle_scan(mmr, _scan_ns())
        payload = json.loads(capsys.readouterr().out)
        assert [r['symbol'] for r in payload['data']] == ['CIIT', 'IREX', 'QUIET']
        row = payload['data'][0]
        assert {'last', 'change_pct', 'volume', 'stockType'} <= set(row)
        assert row['last'] == 2.74
        assert payload['excluded'][0]['symbol'] == 'NMZ RT'
        assert payload['unclassified'] == 1
        assert isinstance(payload['warnings'], list)

    def test_json_include_all(self, mmr, monkeypatch, capsys, rich_out):
        monkeypatch.setattr(cli, '_json_mode', True)
        cli._handle_scan(mmr, _scan_ns(include_all=True))
        payload = json.loads(capsys.readouterr().out)
        assert 'NMZ RT' in [r['symbol'] for r in payload['data']]
        assert payload['excluded'] == []

    def test_rich(self, mmr, monkeypatch, rich_out):
        monkeypatch.setattr(cli, '_json_mode', False)
        cli._handle_scan(mmr, _scan_ns())
        out = rich_out.getvalue()
        assert 'CIIT' in out and '2.74' in out and '547,424' in out
        assert 'Excluded 1 non-common instrument(s): NMZ RT (stockType RIGHT)' in out
        assert '--include-all' in out

    def test_parser_flag(self):
        assert cli.build_parser().parse_args(['scan', '--include-all']).include_all is True
        assert cli.build_parser().parse_args(['scan']).include_all is False


# ---------------------------------------------------------------------------
# trader_service side: scanner_data attaches stockType
# ---------------------------------------------------------------------------

class TestScannerDataStockType:
    @pytest.mark.asyncio
    async def test_attaches_stock_type_and_tolerates_failure(self):
        from ib_async import Contract, ContractDetails
        from trader.listeners.ibreactive import IBAIORx

        @dataclass
        class _Scan:
            rank: int
            contractDetails: ContractDetails

        def _cd(sym, cid):
            cd = ContractDetails()
            cd.contract = Contract(conId=cid, symbol=sym, secType='STK', exchange='SMART',
                                   currency='USD', localSymbol=sym, tradingClass=sym)
            return cd

        ibrx = IBAIORx.__new__(IBAIORx)
        ibrx.ib = MagicMock()
        ibrx.ib.reqScannerDataAsync = AsyncMock(return_value=[
            _Scan(0, _cd('NMZ RT', 1)), _Scan(1, _cd('AAPL', 2)), _Scan(2, _cd('ERR', 3))])

        async def _details(contract):
            await asyncio.sleep(0)
            if contract.conId == 3:
                raise RuntimeError('boom')
            d = ContractDetails()
            d.stockType = {1: 'RIGHT', 2: 'COMMON'}[contract.conId]
            d.longName = f'name{contract.conId}'
            return [d]

        ibrx.ib.reqContractDetailsAsync = AsyncMock(side_effect=_details)
        rows = await ibrx.scanner_data()
        assert [r['stockType'] for r in rows] == ['RIGHT', 'COMMON', None]
        assert rows[0]['localSymbol'] == 'NMZ RT' and rows[0]['tradingClass'] == 'NMZ RT'
        assert rows[1]['longName'] == 'name2'
        assert [r['rank'] for r in rows] == [1, 2, 3]
