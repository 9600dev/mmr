"""TwelveData idea candidates report unavailable fields as null, not 0.0 (2026-11-05)."""
from trader.tools.idea_scanner import TwelveDataIdeaScanner


def _scanner():
    return TwelveDataIdeaScanner.__new__(TwelveDataIdeaScanner)


def test_missing_inputs_are_none():
    c, = _scanner()._build_candidates([{'symbol': 'APP', 'close': '353.2', 'volume': '786161',
                                        'percent_change': '29.5', 'high': '360', 'low': '330'}])
    assert c['gap_pct'] is None and c['rel_vol'] is None
    assert c['spread_pct'] is None and c['vwap'] is None
    assert c['range_pct'] > 0


def test_computable_inputs_present():
    c, = _scanner()._build_candidates([{'symbol': 'X', 'close': '10', 'open': '11', 'previous_close': '10',
                                        'volume': '200', 'average_volume': '100', 'percent_change': '0'}])
    assert c['gap_pct'] == 10.0 and c['rel_vol'] == 2.0
