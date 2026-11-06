"""`mmr ideas` ETP filter (2026-11-05 smoke test).

On a crash day the momentum BUY list led with UVXY and UVIX — 1.5x/2x long
VIX-futures products. Pinned here:

- ETP-ness comes from the data source's instrument metadata (TwelveData's US
  ETF list, Massive ticker type, IB stockType); leverage/inversion from the
  ETP's name, applied ONLY to known ETPs (Ultra Clean Holdings is a stock);
- the curated ticker list is a secondary net that also works when the
  metadata call fails (and that failure is reported, not swallowed);
- default drops leveraged/inverse ETPs, --exclude-all-etp drops every ETP,
  --include-etp keeps them; JSON and Rich both say what was excluded and why.
"""

import argparse
import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from rich.console import Console

import trader.mmr_cli as cli
import trader.tools.idea_scanner as ids
from trader.tools.idea_scanner import IBIdeaScanner, IdeaScanner, TwelveDataIdeaScanner
from trader.tools.instrument_filters import (KNOWN_LEVERAGED_INVERSE, apply_etp_filter,
                                             classify_etp, leveraged_inverse_marker)
from test_idea_scanner import _make_ib_snapshot, _make_snapshot


# ---------------------------------------------------------------------------
# Name rules (only ever applied to instruments the metadata says are ETPs)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('name', [
    'ProShares Ultra VIX Short-Term Futures ETF',        # UVXY
    'PROSHARES ULTRA VIX ST FUTU',                       # UVXY, IB longName
    '2x Long VIX Futures ETF',                           # UVIX
    '-1x Short VIX Futures ETF',                         # SVIX
    'ProShares UltraPro QQQ',                            # TQQQ
    'ProShares UltraPro Short QQQ',                      # SQQQ
    'ProShares UltraShort 20+ Year Treasury',            # TBT
    'ProShares Short S&P 500',                           # SH
    'ProShares Short VIX Short-Term Futures ETF',        # SVXY
    'Direxion Daily Semiconductor Bull 3X Shares',       # SOXL
    'Direxion Daily S&P 500 Bear 1X Shares',             # SPDN (inverse 1x)
    'Direxion Shares ETF Trust Direxion Daily TSLA Bull 2X ETF',   # Massive name
    'T-REX 2XL CIFR DT ETF', 'TRADR 2X LONG IREN DAILY ETF', 'LEVERAGE SHARES 2X IREN',
    'Defiance Daily Target 1.75X Long MSTR ETF', 'Tradr 1.5X Short NVDA Daily ETF',
    'MicroSectors FANG+ Index 3X Leveraged ETN', 'Inverse VIX Short-Term Futures ETNs',
])
def test_leveraged_or_inverse_names(name):
    assert leveraged_inverse_marker(name)


@pytest.mark.parametrize('name', [
    'SPDR S&P 500 ETF Trust', 'iShares Bitcoin Trust ETF', 'SPDR Gold Trust',
    'ProShares VIX Short-Term Futures ETF',              # VIXY: 1x long, not levered
    'iPath Series B S&P 500 VIX Short-Term Futures ETN', # VXX: 1x
    'JPMorgan Ultra-Short Income ETF', 'PGIM Ultra Short Bond ETF',
    'DoubleLine Ultrashort Income ETF', 'Innovator U.S. Equity Ultra Buffer ETF - March',
    'YieldMax Ultra Option Income Strategy ETF', 'iShares Short Treasury Bond ETF',
    'Vanguard Short-Term Bond ETF', 'ProShares Long Online/Short Stores ETF',
    'GraniteShares 1x Long Something ETF', 'Invesco BulletShares 2027 Corporate Bond ETF',
    'iShares 1-3 Year Treasury Bond ETF', '', None,
])
def test_plain_names(name):
    assert leveraged_inverse_marker(name) is None


class TestClassify:
    def test_ultra_clean_holdings_is_a_stock(self):
        # Name contains 'Ultra' but metadata says common stock -> untouched.
        cls = classify_etp('UCTT', {'is_etp': False, 'type': 'CS',
                                    'name': 'Ultra Clean Holdings, Inc.', 'source': 'massive'})
        assert cls == {'is_etp': False, 'leveraged_inverse': False, 'reason': ''}

    def test_uvxy_from_metadata(self):
        cls = classify_etp('UVXY', {'is_etp': True, 'type': 'ETF', 'source': 'massive',
                                    'name': 'ProShares Ultra VIX Short-Term Futures ETF'})
        assert cls['leveraged_inverse'] and 'massive ETF' in cls['reason']

    def test_curated_net_without_metadata(self):
        cls = classify_etp('UVIX', None)
        assert cls['is_etp'] and cls['leveraged_inverse']
        assert 'curated list' in cls['reason'] and 'no source metadata' in cls['reason']

    def test_curated_net_when_name_is_wrong(self):
        # TwelveData lists SPXS under a UCITS fund's name (seen 2026-11-05).
        cls = classify_etp('SPXS', {'is_etp': True, 'type': 'ETF list', 'source': 'twelvedata',
                                    'name': 'Invesco S&P 500 UCITS ETF Acc'})
        assert cls['leveraged_inverse'] and 'curated list' in cls['reason']

    def test_plain_etp(self):
        cls = classify_etp('SPY', {'is_etp': True, 'type': 'ETF', 'source': 'massive',
                                   'name': 'SPDR S&P 500 ETF Trust'})
        assert cls['is_etp'] and not cls['leveraged_inverse']

    def test_unknown_stays_unknown(self):
        assert classify_etp('AAPL', None)['is_etp'] is None

    def test_curated_list_has_the_incident_tickers(self):
        assert {'UVXY', 'UVIX', 'SVIX', 'SQQQ', 'TQQQ'} <= KNOWN_LEVERAGED_INVERSE


def _meta(name, etp=True):
    return {'is_etp': etp, 'type': 'ETF' if etp else 'CS', 'name': name, 'source': 'test'}


CANDS = [{'ticker': 'UVXY'}, {'ticker': 'SPY'}, {'ticker': 'AAPL'}, {'ticker': 'MYST'}]
META = {'UVXY': _meta('ProShares Ultra VIX Short-Term Futures ETF'),
        'SPY': _meta('SPDR S&P 500 ETF Trust'), 'AAPL': _meta('Apple Inc.', etp=False)}


class TestApplyEtpFilter:
    def test_default_drops_leveraged_only(self):
        kept, s = apply_etp_filter(CANDS, META, 'leveraged', source='test')
        assert [c['ticker'] for c in kept] == ['SPY', 'AAPL', 'MYST']
        assert [e['ticker'] for e in s['excluded']] == ['UVXY']
        assert s['unknown'] == 1 and s['mode'] == 'leveraged'

    def test_all_drops_every_etp(self):
        kept, s = apply_etp_filter(CANDS, META, 'all')
        assert [c['ticker'] for c in kept] == ['AAPL', 'MYST']
        assert {e['ticker'] for e in s['excluded']} == {'UVXY', 'SPY'}

    def test_none_keeps_everything(self):
        kept, s = apply_etp_filter(CANDS, META, 'none')
        assert kept == CANDS and s['excluded'] == [] and s['mode'] == 'none'

    def test_bad_mode(self):
        with pytest.raises(ValueError):
            apply_etp_filter(CANDS, META, 'some')


# ---------------------------------------------------------------------------
# TwelveData backend (the default `mmr ideas` source)
# ---------------------------------------------------------------------------

TD_MOVERS = [
    {'symbol': 'UVXY', 'name': 'ProShares Ultra VIX Short-Term Futures ETF', 'last': 23.61,
     'high': 23.67, 'low': 16.51, 'volume': 49957100, 'change': 7.37, 'percent_change': 45.38},
    {'symbol': 'UVIX', 'name': '2x Long VIX Futures ETF', 'last': 12.0, 'high': 12.5,
     'low': 8.0, 'volume': 30000000, 'change': 4.0, 'percent_change': 50.0},
    {'symbol': 'GLD', 'name': 'SPDR Gold Trust', 'last': 300.0, 'high': 301.0, 'low': 290.0,
     'volume': 9000000, 'change': 20.0, 'percent_change': 7.1},
    {'symbol': 'ARDT', 'name': 'Ardent Health Partners', 'last': 15.0, 'high': 15.5,
     'low': 13.0, 'volume': 5000000, 'change': 2.0, 'percent_change': 15.0},
]
TD_ETFS = [
    {'symbol': 'UVXY', 'name': 'ProShares Ultra VIX Short-Term Futures ETF', 'country': 'United States'},
    {'symbol': 'UVIX', 'name': '2x Long VIX Futures ETF', 'country': 'United States'},
    {'symbol': 'GLD', 'name': 'SPDR Gold Trust, SPDR Gold Shares', 'country': 'United States'},
]


@pytest.fixture(autouse=True)
def _fresh_td_cache():
    ids._td_etp_cache.update({'at': 0.0, 'index': None})
    yield
    ids._td_etp_cache.update({'at': 0.0, 'index': None})


@pytest.fixture(autouse=True)
def _no_trading_filter(monkeypatch):
    from trader.trading.trading_filter import TradingFilter
    monkeypatch.setattr(TradingFilter, 'load', classmethod(lambda cls, *a, **k: TradingFilter()))


@pytest.fixture
def td_client():
    c = MagicMock()

    def movers(market, direction):
        return SimpleNamespace(as_json=lambda: TD_MOVERS if direction == 'gainers' else [])
    c.get_market_movers.side_effect = movers
    c.get_etf_list.return_value.as_json.return_value = TD_ETFS
    c.time_series.return_value.as_pandas.return_value = pd.DataFrame()
    return c


def _td_scan(client, **kw):
    return TwelveDataIdeaScanner(client).scan(preset='momentum', top_n=10, **kw)


class TestTwelveDataEtp:
    def test_default_excludes_vix_etps_keeps_plain_etp(self, td_client):
        df = _td_scan(td_client)
        assert 'UVXY' not in set(df['ticker']) and 'UVIX' not in set(df['ticker'])
        assert {'GLD', 'ARDT'} <= set(df['ticker'])
        s = df.attrs['etp_filter']
        assert {e['ticker'] for e in s['excluded']} == {'UVXY', 'UVIX'}
        assert all('leveraged/inverse ETP' in e['reason'] for e in s['excluded'])
        assert s['metadata_error'] is None
        td_client.get_etf_list.assert_called_once_with(country='United States')

    def test_index_is_cached(self, td_client):
        _td_scan(td_client)
        _td_scan(td_client)
        assert td_client.get_etf_list.call_count == 1

    def test_exclude_all(self, td_client):
        df = _td_scan(td_client, etp_mode='all')
        assert list(df['ticker']) == ['ARDT']

    def test_include_etp_skips_lookup(self, td_client):
        df = _td_scan(td_client, etp_mode='none')
        assert {'UVXY', 'UVIX'} <= set(df['ticker'])
        td_client.get_etf_list.assert_not_called()
        assert df.attrs['etp_filter']['mode'] == 'none'

    def test_metadata_failure_is_reported_and_curated_net_applies(self, td_client):
        td_client.get_etf_list.side_effect = RuntimeError('api credits exhausted')
        df = _td_scan(td_client)
        s = df.attrs['etp_filter']
        assert 'api credits exhausted' in s['metadata_error']
        assert {e['ticker'] for e in s['excluded']} == {'UVXY', 'UVIX'}   # curated
        assert all('curated list' in e['reason'] for e in s['excluded'])
        assert 'GLD' in set(df['ticker'])          # unknown, kept, counted
        assert s['unknown'] >= 2


# ---------------------------------------------------------------------------
# Massive backend
# ---------------------------------------------------------------------------

class TestMassiveEtp:
    def _client(self, details):
        c = MagicMock()
        empty = SimpleNamespace(values=[])
        c.get_rsi.return_value = c.get_ema.return_value = c.get_sma.return_value = empty
        snaps = [_make_snapshot('UVXY', price=23.6, change_pct=45.0, volume=50_000_000),
                 _make_snapshot('SPY', price=600.0, change_pct=4.0, volume=90_000_000),
                 _make_snapshot('UCTT', price=30.0, change_pct=9.0, volume=3_000_000)]
        c.get_snapshot_direction.side_effect = lambda **kw: snaps if kw['direction'] == 'gainers' else []

        def get_details(t):
            if isinstance(details.get(t), Exception):
                raise details[t]
            typ, name = details[t]
            return SimpleNamespace(type=typ, name=name)
        c.get_ticker_details.side_effect = get_details
        return c

    DETAILS = {'UVXY': ('ETF', 'ProShares Ultra VIX Short-Term Futures ETF'),
               'SPY': ('ETF', 'SPDR S&P 500 ETF Trust'),
               'UCTT': ('CS', 'Ultra Clean Holdings, Inc.')}

    def test_default(self):
        df = IdeaScanner(self._client(self.DETAILS)).scan(preset='momentum', top_n=10)
        assert set(df['ticker']) == {'SPY', 'UCTT'}
        ex = df.attrs['etp_filter']['excluded']
        assert [e['ticker'] for e in ex] == ['UVXY'] and 'massive ETF' in ex[0]['reason']

    def test_all(self):
        df = IdeaScanner(self._client(self.DETAILS)).scan(preset='momentum', top_n=10,
                                                         etp_mode='all')
        assert list(df['ticker']) == ['UCTT']

    def test_all_lookups_fail(self):
        details = {t: RuntimeError('503') for t in self.DETAILS}
        df = IdeaScanner(self._client(details)).scan(preset='momentum', top_n=10)
        s = df.attrs['etp_filter']
        assert 'Massive ticker details unavailable' in s['metadata_error']
        assert [e['ticker'] for e in s['excluded']] == ['UVXY']       # curated net


# ---------------------------------------------------------------------------
# IB backend (international, stockType from scanner rows / resolution)
# ---------------------------------------------------------------------------

def _flat_history(contract, *a, **k):
    # prev close well below the snapshot's last so momentum's +2% filter passes
    base = {'BBUS': 4.0, 'BHP': 40.0}[contract.symbol]
    return [{'date': f'2026-02-{10 + i}', 'open': base, 'high': base, 'low': base,
             'close': base, 'volume': 600_000} for i in range(20)]


class TestIbEtp:
    def test_scanner_rows_stock_type(self):
        rows = [
            {'rank': 1, 'symbol': 'BBUS', 'secType': 'STK', 'exchange': 'ASX', 'currency': 'AUD',
             'conId': 1, 'stockType': 'ETF', 'longName': 'BETASHARES US EQTY STRONG BEAR'},
            {'rank': 2, 'symbol': 'BHP', 'secType': 'STK', 'exchange': 'ASX', 'currency': 'AUD',
             'conId': 2, 'stockType': 'COMMON', 'longName': 'BHP GROUP LTD'},
        ]
        rpc = MagicMock()
        inner = rpc.rpc.return_value
        inner.scanner_data.return_value = rows
        inner.get_snapshots_batch.return_value = [
            _make_ib_snapshot('BBUS', conId=1, last=5.0, open_=4.5, high=5.1, low=4.4),
            _make_ib_snapshot('BHP', conId=2, last=45.0, open_=43.0, high=46.0, low=42.5)]
        inner.get_history_bars.side_effect = _flat_history
        df = IBIdeaScanner(rpc).scan(preset='momentum', location='STK.AU.ASX', top_n=10)
        assert 'BBUS' not in set(df['ticker']) and 'BHP' in set(df['ticker'])
        s = df.attrs['etp_filter']
        assert s['excluded'][0]['ticker'] == 'BBUS' and 'ib ETF' in s['excluded'][0]['reason']
        assert s['metadata_error'] is None

    def test_old_server_rows_report_missing_metadata(self):
        rows = [{'rank': 1, 'symbol': 'BHP', 'secType': 'STK', 'exchange': 'ASX',
                 'currency': 'AUD', 'conId': 2}]
        rpc = MagicMock()
        inner = rpc.rpc.return_value
        inner.scanner_data.return_value = rows
        inner.get_snapshots_batch.return_value = [
            _make_ib_snapshot('BHP', conId=2, last=45.0, open_=43.0, high=46.0, low=42.5)]
        inner.get_history_bars.side_effect = _flat_history
        df = IBIdeaScanner(rpc).scan(preset='momentum', location='STK.AU.ASX', top_n=10)
        assert 'IB stockType unavailable' in df.attrs['etp_filter']['metadata_error']


    def test_resolved_tickers_record_stock_type(self):
        rpc = MagicMock()
        rpc.rpc.return_value.resolve_contract.side_effect = lambda c: [SimpleNamespace(
            conId={'BBUS': 1, 'BHP': 2}[c.symbol], symbol=c.symbol, currency='AUD',
            primaryExchange='ASX', stockType={'BBUS': 'ETF', 'BHP': 'COMMON'}[c.symbol],
            longName={'BBUS': 'BETASHARES US EQTY STRONG BEAR', 'BHP': 'BHP GROUP'}[c.symbol])]
        sc = IBIdeaScanner(rpc)
        contracts, _ = sc._resolve_symbols(['BBUS', 'BHP'], 'STK.AU.ASX', lambda x: x)
        assert len(contracts) == 2
        meta, err = sc._etp_metadata(['BBUS', 'BHP'])
        assert err is None
        assert meta['BBUS']['is_etp'] is True and meta['BHP']['is_etp'] is False
        assert classify_etp('BBUS', meta['BBUS'])['leveraged_inverse'] is True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _ideas_ns(**kw):
    d = dict(presets=False, preset='momentum', tickers=None, universe=None, num=15,
             min_price=None, max_price=None, min_volume=None, min_change=None,
             max_change=None, fundamentals=False, news=False, detail=False,
             news_bodies=False, news_bodies_limit=3, location=None, source='twelvedata',
             etp_mode='leveraged')
    d.update(kw)
    return argparse.Namespace(**d)


@pytest.fixture
def rich_out(monkeypatch):
    buf = io.StringIO()
    monkeypatch.setattr(cli, 'console', Console(file=buf, width=200, force_terminal=False))
    return buf


def _fake_mmr():
    df = pd.DataFrame([{'ticker': 'ARDT', 'price': 15.0, 'change_pct': 15.0, 'volume': 5e6,
                        'score': 80.0, 'signal': 'BUY'}])
    df.attrs['etp_filter'] = {
        'mode': 'leveraged', 'source': 'twelvedata /etfs', 'unknown': 0,
        'metadata_error': None,
        'excluded': [{'ticker': 'UVXY', 'reason': "leveraged/inverse ETP (twelvedata ETF list; "
                                                  "name 'ProShares Ultra VIX' has 'Ultra')"}]}
    mmr = MagicMock()
    mmr.scan_ideas.return_value = df
    return mmr


class TestIdeasCli:
    def test_flags(self):
        p = cli.build_parser()
        assert p.parse_args(['ideas']).etp_mode == 'leveraged'
        assert p.parse_args(['ideas', '--include-etp']).etp_mode == 'none'
        assert p.parse_args(['ideas', '--exclude-all-etp']).etp_mode == 'all'
        with pytest.raises(SystemExit):
            p.parse_args(['ideas', '--include-etp', '--exclude-all-etp'])

    def test_mode_passed_through(self, rich_out):
        mmr = _fake_mmr()
        cli._handle_ideas(mmr, _ideas_ns(etp_mode='all'))
        assert mmr.scan_ideas.call_args.kwargs['etp_mode'] == 'all'

    def test_json(self, monkeypatch, capsys, rich_out):
        monkeypatch.setattr(cli, '_json_mode', True)
        cli._handle_ideas(_fake_mmr(), _ideas_ns())
        payload = json.loads(capsys.readouterr().out)
        assert payload['data'][0]['ticker'] == 'ARDT'
        assert payload['etp_filter']['excluded'][0]['ticker'] == 'UVXY'

    def test_rich(self, monkeypatch, rich_out):
        monkeypatch.setattr(cli, '_json_mode', False)
        cli._handle_ideas(_fake_mmr(), _ideas_ns())
        out = rich_out.getvalue()
        assert 'ETP filter excluded 1 (leveraged/inverse ETPs' in out
        assert 'UVXY: leveraged/inverse ETP' in out

    def test_sdk_rejects_bad_mode(self):
        from trader.sdk import MMR
        with pytest.raises(ValueError, match='etp_mode'):
            MMR.__new__(MMR).scan_ideas(etp_mode='bogus')
