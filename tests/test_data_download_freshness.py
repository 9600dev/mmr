"""Tests for `_try_get_exchange_calendar_for` — the helper the freshness
guard relies on to locate an exchange calendar for a SecurityDefinition.

The freshness loop itself is driven by `TickData.missing()`, which already
has coverage in the DuckDB/TickData test suites. The new surface area
introduced by the freshness refactor is just this helper.
"""

from types import SimpleNamespace


class TestExchangeCalendarLookup:

    def _load_helper(self):
        from trader.mmr_cli import _try_get_exchange_calendar_for
        return _try_get_exchange_calendar_for

    def test_primary_exchange_preferred(self):
        helper = self._load_helper()
        sec = SimpleNamespace(primaryExchange='XNAS', exchange='SMART')
        cal = helper(sec)
        assert cal is not None  # XNAS is a valid ISO MIC (aliased under XNYS)

    def test_falls_back_to_exchange_when_primary_invalid(self):
        helper = self._load_helper()
        sec = SimpleNamespace(primaryExchange='not_a_real_calendar', exchange='XNYS')
        cal = helper(sec)
        assert cal is not None

    def test_returns_none_when_neither_resolves(self):
        helper = self._load_helper()
        sec = SimpleNamespace(primaryExchange='bogus1', exchange='bogus2')
        assert helper(sec) is None

    def test_handles_missing_attributes(self):
        """SecurityDefinition dataclasses can have empty string or None for
        exchange fields; neither should crash the helper."""
        helper = self._load_helper()
        assert helper(SimpleNamespace()) is None
        assert helper(SimpleNamespace(primaryExchange='', exchange='')) is None
        assert helper(SimpleNamespace(primaryExchange=None, exchange=None)) is None
