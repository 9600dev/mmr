import os
import pytest
import yaml

from trader.trading.trading_filter import TradingFilter


class TestTradingFilter:
    def test_default_allows_everything(self):
        tf = TradingFilter.default()
        allowed, reason = tf.is_allowed('AAPL', exchange='NASDAQ', sec_type='STK', price=150.0)
        assert allowed is True
        assert reason == ''

    def test_denylist_blocks_symbol(self):
        tf = TradingFilter(denylist=['NKLA', 'RIDE'])
        allowed, reason = tf.is_allowed('NKLA')
        assert allowed is False
        assert 'denylist' in reason

    def test_denylist_allows_other_symbols(self):
        tf = TradingFilter(denylist=['NKLA'])
        allowed, reason = tf.is_allowed('AAPL')
        assert allowed is True

    def test_allowlist_exclusive(self):
        tf = TradingFilter(allowlist=['AAPL', 'MSFT'])
        allowed, reason = tf.is_allowed('AAPL')
        assert allowed is True

        allowed, reason = tf.is_allowed('NVDA')
        assert allowed is False
        assert 'not in allowlist' in reason

    def test_allowlist_overrides_denylist(self):
        # If symbol is in allowlist, denylist should not matter
        # (allowlist check passes, denylist check still applies — but design says
        #  if you're in the allowlist, you're allowed; denylist is checked after)
        tf = TradingFilter(allowlist=['AAPL', 'NKLA'], denylist=['NKLA'])
        # NKLA is in allowlist (passes first check) but also in denylist (fails second check)
        allowed, reason = tf.is_allowed('NKLA')
        assert allowed is False
        assert 'denylist' in reason

    def test_deny_exchange(self):
        tf = TradingFilter(deny_exchanges=['OTC', 'PINK'])
        allowed, reason = tf.is_allowed('PENNY', exchange='OTC')
        assert allowed is False
        assert 'exchange OTC is denied' in reason

        allowed, reason = tf.is_allowed('AAPL', exchange='NASDAQ')
        assert allowed is True

    def test_allowed_exchanges_exclusive(self):
        tf = TradingFilter(exchanges=['NASDAQ', 'NYSE'])
        allowed, reason = tf.is_allowed('AAPL', exchange='NASDAQ')
        assert allowed is True

        allowed, reason = tf.is_allowed('PENNY', exchange='OTC')
        assert allowed is False
        assert 'not in allowed exchanges' in reason

    def test_deny_sec_type(self):
        tf = TradingFilter(deny_sec_types=['WAR', 'BOND'])
        allowed, reason = tf.is_allowed('XYZ', sec_type='WAR')
        assert allowed is False
        assert 'sec_type WAR is denied' in reason

    def test_allowed_sec_types_exclusive(self):
        tf = TradingFilter(sec_types=['STK'])
        allowed, reason = tf.is_allowed('AAPL', sec_type='STK')
        assert allowed is True

        allowed, reason = tf.is_allowed('AAPL', sec_type='OPT')
        assert allowed is False
        assert 'not in allowed types' in reason

    def test_deny_location(self):
        tf = TradingFilter(deny_locations=['STK.CA', 'STK.HK.SEHK'])
        allowed, reason = tf.is_allowed('BHP', location='STK.CA')
        assert allowed is False
        assert 'location STK.CA is denied' in reason

        allowed, reason = tf.is_allowed('AAPL', location='STK.US.MAJOR')
        assert allowed is True

    def test_allowed_locations_exclusive(self):
        tf = TradingFilter(locations=['STK.US.MAJOR', 'STK.AU.ASX'])
        allowed, reason = tf.is_allowed('BHP', location='STK.AU.ASX')
        assert allowed is True

        allowed, reason = tf.is_allowed('RY', location='STK.CA')
        assert allowed is False
        assert 'not in allowed locations' in reason

    def test_location_case_insensitive(self):
        tf = TradingFilter(deny_locations=['stk.ca'])
        allowed, _ = tf.is_allowed('RY', location='STK.CA')
        assert allowed is False

    def test_no_location_skips_check(self):
        """When no location is passed, location filters don't apply."""
        tf = TradingFilter(deny_locations=['STK.CA'])
        allowed, _ = tf.is_allowed('RY')
        assert allowed is True

    def test_min_price_filter(self):
        tf = TradingFilter(min_price=5.0)
        allowed, reason = tf.is_allowed('PENNY', price=2.50)
        assert allowed is False
        assert 'below minimum' in reason

        allowed, reason = tf.is_allowed('AAPL', price=150.0)
        assert allowed is True

    def test_min_price_skips_zero_price(self):
        """Price=0 means no price data — should not be filtered."""
        tf = TradingFilter(min_price=5.0)
        allowed, _ = tf.is_allowed('AAPL', price=0.0)
        assert allowed is True

    def test_case_insensitive(self):
        tf = TradingFilter(denylist=['nkla'], exchanges=['nasdaq'])
        allowed, _ = tf.is_allowed('NKLA', exchange='NASDAQ')
        assert allowed is False  # denylist is case-insensitive

        tf2 = TradingFilter(allowlist=['aapl'], exchanges=['NASDAQ'])
        allowed, _ = tf2.is_allowed('AAPL', exchange='nasdaq')
        assert allowed is True

    def test_is_empty(self):
        assert TradingFilter.default().is_empty() is True
        assert TradingFilter(denylist=['NKLA']).is_empty() is False
        assert TradingFilter(min_price=5.0).is_empty() is False
        assert TradingFilter(allowlist=['AAPL']).is_empty() is False
        assert TradingFilter(deny_locations=['STK.CA']).is_empty() is False

    def test_load_save_roundtrip(self, tmp_path):
        filepath = str(tmp_path / 'filters.yaml')
        tf = TradingFilter(
            denylist=['NKLA', 'RIDE'],
            allowlist=['AAPL', 'MSFT'],
            exchanges=['NASDAQ'],
            deny_exchanges=['OTC'],
            sec_types=['STK'],
            deny_sec_types=['WAR'],
            locations=['STK.US.MAJOR'],
            deny_locations=['STK.CA', 'STK.HK.SEHK'],
            min_price=5.0,
        )
        tf.save(filepath)

        loaded = TradingFilter.load(filepath)
        assert loaded.denylist == ['NKLA', 'RIDE']
        assert loaded.allowlist == ['AAPL', 'MSFT']
        assert loaded.exchanges == ['NASDAQ']
        assert loaded.deny_exchanges == ['OTC']
        assert loaded.sec_types == ['STK']
        assert loaded.deny_sec_types == ['WAR']
        assert loaded.locations == ['STK.US.MAJOR']
        assert loaded.deny_locations == ['STK.CA', 'STK.HK.SEHK']
        assert loaded.min_price == 5.0

    def test_load_missing_file_returns_default(self, tmp_path):
        filepath = str(tmp_path / 'nonexistent.yaml')
        tf = TradingFilter.load(filepath)
        assert tf.is_empty() is True

    def test_to_dict(self):
        tf = TradingFilter(denylist=['NKLA'], min_price=3.0)
        d = tf.to_dict()
        assert d['denylist'] == ['NKLA']
        assert d['min_price'] == 3.0
        assert d['allowlist'] == []
