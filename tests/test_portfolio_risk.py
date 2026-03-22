"""Unit tests for trader.trading.portfolio_risk."""

import pytest

from trader.trading.portfolio_risk import PortfolioRiskAnalyzer, RiskReport
from trader.data.position_groups import PositionGroupStore


def _make_positions(items):
    """Build position dicts from (symbol, marketValue) tuples."""
    return [{'symbol': s, 'marketValue': v} for s, v in items]


class TestRiskReportBasic:
    def test_empty_portfolio(self):
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze([], 100_000.0)
        assert report.total_positions == 0
        assert report.summary == 'No positions or account data available.'

    def test_zero_net_liq(self):
        positions = _make_positions([('AAPL', 10_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 0.0)
        assert report.summary == 'No positions or account data available.'

    def test_basic_metrics(self):
        positions = _make_positions([
            ('AAPL', 25_000),
            ('MSFT', 25_000),
            ('AMD', 25_000),
            ('NVDA', 25_000),
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        assert report.total_positions == 4
        assert report.net_liquidation == 200_000.0
        assert abs(report.gross_exposure_pct - 0.5) < 0.01
        assert len(report.top_positions) == 4

    def test_to_dict(self):
        report = RiskReport(total_positions=5, net_liquidation=100_000)
        d = report.to_dict()
        assert d['total_positions'] == 5
        assert d['net_liquidation'] == 100_000
        assert isinstance(d['warnings'], list)


class TestConcentration:
    def test_hhi_equal_weights(self):
        """4 equal positions → HHI = 4 * (0.25)^2 = 0.25."""
        positions = _make_positions([
            ('A', 25_000), ('B', 25_000), ('C', 25_000), ('D', 25_000),
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        assert abs(report.hhi - 0.25) < 0.01

    def test_hhi_single_position(self):
        """1 position → HHI = 1.0."""
        positions = _make_positions([('AAPL', 100_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        assert abs(report.hhi - 1.0) < 0.01

    def test_hhi_diversified(self):
        """10 equal positions → HHI = 0.10."""
        positions = _make_positions([(f'SYM{i}', 10_000) for i in range(10)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        assert abs(report.hhi - 0.10) < 0.01

    def test_critical_concentration_warning(self):
        """Position >15% of net liq → critical warning."""
        positions = _make_positions([
            ('AAPL', 20_000),
            ('MSFT', 5_000),
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        critical = [w for w in report.warnings if w['level'] == 'critical']
        assert len(critical) == 1
        assert 'AAPL' in critical[0]['message']

    def test_warning_concentration(self):
        """Position >10% but <15% of net liq → warning."""
        positions = _make_positions([
            ('AAPL', 12_000),
            ('MSFT', 5_000),
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        warnings = [w for w in report.warnings if w['level'] == 'warning' and 'AAPL' in w.get('message', '')]
        assert len(warnings) == 1

    def test_no_concentration_warning(self):
        """Position <10% → no warning."""
        positions = _make_positions([
            ('AAPL', 8_000),
            ('MSFT', 8_000),
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        conc_warnings = [w for w in report.warnings if 'portfolio' in w.get('message', '').lower()
                         and ('AAPL' in w.get('symbols', []) or 'MSFT' in w.get('symbols', []))]
        assert len(conc_warnings) == 0

    def test_hhi_warning(self):
        """HHI > 0.15 → warning."""
        # 2 positions, 60/40 split: HHI = 0.36 + 0.16 = 0.52
        positions = _make_positions([('AAPL', 60_000), ('MSFT', 40_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        hhi_warnings = [w for w in report.warnings if 'HHI' in w.get('message', '')]
        assert len(hhi_warnings) == 1


class TestGroupAllocations:
    def test_group_allocation_computed(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('tech', max_allocation_pct=0.30)
        store.add_member('tech', 'AAPL')
        store.add_member('tech', 'MSFT')

        positions = _make_positions([
            ('AAPL', 15_000),
            ('MSFT', 10_000),
            ('AMD', 5_000),
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0, group_store=store)
        assert len(report.group_allocations) == 1
        tech = report.group_allocations[0]
        assert tech['name'] == 'tech'
        assert abs(tech['value'] - 25_000) < 1
        assert abs(tech['pct'] - 0.25) < 0.01
        assert tech['over_budget'] is False

    def test_group_over_budget_warning(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('tech', max_allocation_pct=0.10)
        store.add_member('tech', 'AAPL')

        positions = _make_positions([('AAPL', 15_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0, group_store=store)
        tech = report.group_allocations[0]
        assert tech['over_budget'] is True
        budget_warnings = [w for w in report.warnings if 'budget' in w.get('message', '').lower()]
        assert len(budget_warnings) == 1

    def test_ungrouped_symbols(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('tech')
        store.add_member('tech', 'AAPL')

        positions = _make_positions([('AAPL', 10_000), ('AMD', 5_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0, group_store=store)
        assert 'AMD' in report.ungrouped_symbols
        assert 'AAPL' not in report.ungrouped_symbols

    def test_no_group_store(self):
        """Without group_store, no group analysis."""
        positions = _make_positions([('AAPL', 10_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        assert report.group_allocations == []
        assert report.ungrouped_symbols == []


class TestSummary:
    def test_summary_generated(self):
        positions = _make_positions([
            ('AAPL', 10_000),
            ('MSFT', 10_000),
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        assert report.summary != ''
        assert '2 positions' in report.summary

    def test_no_warnings_summary(self):
        # Need >=7 equal positions so HHI < 0.15 (7 * (1/7)^2 ≈ 0.143)
        positions = _make_positions([
            (f'SYM{i}', 5_000) for i in range(8)
        ])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        assert 'No risk issues' in report.summary

    def test_critical_in_summary(self):
        positions = _make_positions([('AAPL', 20_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 100_000.0)
        assert 'critical' in report.summary.lower()


class TestCorrelation:
    def test_no_duckdb_path_skips_correlation(self):
        positions = _make_positions([('AAPL', 10_000), ('MSFT', 10_000)])
        analyzer = PortfolioRiskAnalyzer()  # no duckdb_path
        report = analyzer.analyze(positions, 100_000.0)
        assert report.correlation_clusters == []
        assert report.data_coverage == 0

    def test_single_position_skips_correlation(self):
        positions = _make_positions([('AAPL', 10_000)])
        analyzer = PortfolioRiskAnalyzer('/tmp/nonexistent.duckdb')
        report = analyzer.analyze(positions, 100_000.0)
        assert report.correlation_clusters == []
