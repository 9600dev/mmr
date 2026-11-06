"""Web dashboard (web/app.py): page content, payload compression, caching.

Runs the FastAPI app in-process against a fake SDK (no trader_service, no
DuckDB). Covers the 2026-11-05 speed fix: the risk section shares a
CorrelationHistoryCache across requests, reasoning markdown is memoised,
responses are gzip-compressed (losslessly), and per-section Server-Timing is
reported — without dropping any information from the page.
"""
import gzip
import time

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import web.app as webapp
from trader.trading.portfolio_risk import CorrelationHistoryCache

SYMBOLS = [f'SYM{i:02d}' for i in range(15)]


class FakeMMR:
    def __init__(self):
        self.risk_caches = []

    def account_cash(self):
        return {'account': 'U1234567', 'base_currency': 'CAD',
                'currencies': {'CAD': {'cash': 1000.0, 'exchange_rate': 1.0, 'base_value': 1000.0},
                               'USD': {'cash': 500.0, 'exchange_rate': 1.37, 'base_value': 685.0}},
                'total_base_value': 1685.0}

    def portfolio_snapshot(self):
        return {'currency': 'CAD', 'total_value': 150000.0, 'net_liquidation': 210000.0,
                'daily_pnl': -321.5, 'position_count': len(SYMBOLS), 'exposure_pct': 0.71,
                'movers': [], 'timestamp': '2026-11-05 10:00:00'}

    def status(self):
        return {'connected': True, 'account': 'U1234567', 'leverage': 0.71, 'cushion': 0.4,
                'account_values': {'BuyingPower': {'value': 400000.0, 'currency': 'CAD'}}}

    def risk_report(self, history_cache=None):
        self.risk_caches.append(history_cache)
        return {'total_positions': len(SYMBOLS), 'net_liquidation': 210000.0,
                'gross_exposure_pct': 0.71, 'net_exposure_pct': 0.71,
                'long_exposure_pct': 0.71, 'short_exposure_pct': 0.0, 'hhi': 0.07,
                'warnings': [{'level': 'warning', 'message': 'SYM00 is 11.0% of portfolio (>10%)',
                              'symbols': ['SYM00']}],
                'summary': 'Portfolio has 15 positions with 71% gross exposure.'}

    def get_risk_limits(self):
        return {'max_leverage': 2.0, 'max_position_size_pct': 0.1, 'max_daily_loss': 5000.0,
                'max_open_orders': 20, 'max_signals_per_hour': 30}

    def portfolio(self):
        return pd.DataFrame([{
            'account': 'U1234567', 'conId': 1000 + i, 'symbol': s, 'position': 10 + i,
            'mktPrice': 100.0 + i, 'avgCost': 95.0, 'marketValue': (10 + i) * (100.0 + i),
            'currency': 'USD', 'unrealizedPNL': 50.0 * i, 'realizedPNL': 0.0, 'dailyPNL': -5.0 * i,
        } for i, s in enumerate(SYMBOLS)])

    def strategies(self):
        return pd.DataFrame([{'name': f'strat{i}', 'state': 'RUNNING' if i % 2 else 'STOPPED',
                              'state_code': 3, 'bar_size': '1 min', 'conids': [1000 + i],
                              'hist_days_prior': 5, 'auto_execute': False} for i in range(5)])

    def proposals(self, status=None, limit=50):
        return pd.DataFrame([{
            'id': i, 'created': '11/05 09:3{0}'.format(i % 10), 'symbol': SYMBOLS[i % 15],
            'action': 'BUY' if i % 2 else 'SELL', 'size': f'{i + 1} sh', 'conf': 0.6,
            'status': 'PENDING' if i < 3 else 'EXECUTED',
            'reasoning': f'## Thesis {i}\n\n' + ('- **momentum** breakout with volume ' * 20)
                         + f'\n\n[ref](https://example.com/{i})',
        } for i in range(limit)])


@pytest.fixture
def client(monkeypatch):
    fake = FakeMMR()
    monkeypatch.setattr(webapp, '_mmr', fake)
    monkeypatch.setattr(webapp, '_get_mmr', lambda: fake)
    monkeypatch.setattr(webapp, '_ACCESS_TOKEN', '')
    webapp._render_md_cached.cache_clear()
    return TestClient(webapp.app), fake


def test_page_renders_every_section(client):
    c, _ = client
    r = c.get('/')
    assert r.status_code == 200
    body = r.text
    for needle in ['U1234567', 'Cash by currency', 'strat1', 'Gross exposure', 'SYM14',
                   'Thesis 99', 'href="https://example.com/99"', 'Approve',
                   'SYM00 is 11.0% of portfolio']:
        assert needle in body, needle
    assert body.count('class="modal"') == 100     # every proposal keeps its popup


def test_gzip_is_lossless_and_much_smaller(client):
    c, _ = client
    plain = c.get('/', headers={'Accept-Encoding': 'identity'})
    zipped = c.get('/', headers={'Accept-Encoding': 'gzip'})
    assert 'content-encoding' not in plain.headers
    assert zipped.headers.get('content-encoding') == 'gzip'
    # httpx transparently decodes; compare the decoded page to the plain one
    # (modulo the per-request Server-Timing header, which is not in the body).
    assert zipped.text == plain.text
    wire = len(gzip.compress(plain.content))
    assert wire < len(plain.content) / 4


def test_risk_section_uses_shared_history_cache(client):
    c, fake = client
    c.get('/')
    c.get('/')
    assert len(fake.risk_caches) == 2
    assert all(isinstance(x, CorrelationHistoryCache) for x in fake.risk_caches)
    assert fake.risk_caches[0] is fake.risk_caches[1] is webapp._RISK_HISTORY_CACHE


def test_server_timing_reports_every_section(client):
    c, _ = client
    r = c.get('/')
    st = r.headers['server-timing']
    for key in ['cash', 'snapshot', 'status', 'risk', 'risk_limits', 'positions',
                'strategies', 'proposals', 'render']:
        assert f'{key};dur=' in st


def test_markdown_render_memoised_and_still_sanitized(client, monkeypatch):
    calls = {'n': 0}
    real_convert = webapp._MD.convert

    def counting(text):
        calls['n'] += 1
        return real_convert(text)

    monkeypatch.setattr(webapp._MD, 'convert', counting)
    payload = '<img src=x onerror=alert(1)> [x](javascript:alert(1)) **ok**'
    first = webapp._render_md(payload)
    second = webapp._render_md(payload)
    assert first == second
    assert calls['n'] == 1
    assert 'onerror' not in first and 'javascript:' not in first and '<strong>ok</strong>' in first
    assert webapp._render_md('') == ''
    assert webapp._render_md(None) == ''


def test_section_failure_still_degrades_to_banner(client, monkeypatch):
    c, fake = client

    def boom(history_cache=None):
        raise ConnectionError('trader_service down')

    monkeypatch.setattr(fake, 'risk_report', boom)
    monkeypatch.setattr(fake, 'status', lambda: (_ for _ in ()).throw(ConnectionError('down')))
    r = c.get('/')
    assert r.status_code == 200
    assert 'trader_service: ConnectionError: trader_service down' in r.text
    assert 'SYM14' in r.text                       # other sections unaffected


def test_risk_history_ttl_env_validation(monkeypatch):
    monkeypatch.setenv('MMR_WEB_RISK_HISTORY_TTL', 'soon')
    with pytest.raises(ValueError, match='MMR_WEB_RISK_HISTORY_TTL'):
        webapp._risk_history_ttl()
    monkeypatch.setenv('MMR_WEB_RISK_HISTORY_TTL', '-5')
    with pytest.raises(ValueError):
        webapp._make_risk_history_cache()
    monkeypatch.setenv('MMR_WEB_RISK_HISTORY_TTL', '0')
    assert webapp._make_risk_history_cache().ttl_seconds == 0.0


def test_warm_page_overhead_is_small(client):
    """App-side cost of a warm page (fake SDK = zero RPC/DB time): template
    render + markdown + gzip. Generous bound; measured ~10-20ms."""
    c, _ = client
    c.get('/', headers={'Accept-Encoding': 'gzip'})
    t0 = time.perf_counter()
    for _ in range(5):
        c.get('/', headers={'Accept-Encoding': 'gzip'})
    per_page = (time.perf_counter() - t0) / 5
    assert per_page < 0.25, per_page
