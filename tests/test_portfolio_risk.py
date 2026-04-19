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


class TestSignedExposure:
    """Verify long/short/hedged books report correctly."""

    def test_long_only_net_equals_gross(self):
        positions = _make_positions([('AAPL', 50_000), ('MSFT', 50_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        assert report.gross_exposure_pct == pytest.approx(0.5)
        assert report.net_exposure_pct == pytest.approx(0.5)
        assert report.long_exposure_pct == pytest.approx(0.5)
        assert report.short_exposure_pct == pytest.approx(0.0)

    def test_fully_hedged_book_has_zero_net(self):
        positions = _make_positions([('AAPL', 100_000), ('MSFT', -100_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        # Gross ~$200k, net ~$0 — classic hedged book.
        assert report.gross_exposure_pct == pytest.approx(1.0)
        assert report.net_exposure_pct == pytest.approx(0.0)
        assert report.long_exposure_pct == pytest.approx(0.5)
        assert report.short_exposure_pct == pytest.approx(0.5)
        # Summary should mention "hedged"
        assert 'hedged' in report.summary.lower()

    def test_net_short_book(self):
        positions = _make_positions([('AAPL', 20_000), ('TSLA', -100_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        assert report.net_exposure_pct < 0  # net short
        assert report.net_exposure_pct == pytest.approx(-0.40)

    def test_top_positions_expose_direction(self):
        positions = _make_positions([('AAPL', 50_000), ('TSLA', -50_000)])
        analyzer = PortfolioRiskAnalyzer()
        report = analyzer.analyze(positions, 200_000.0)
        by_symbol = {p['symbol']: p for p in report.top_positions}
        assert by_symbol['AAPL']['is_short'] is False
        assert by_symbol['TSLA']['is_short'] is True
        assert by_symbol['AAPL']['signed_pct'] == pytest.approx(0.25)
        assert by_symbol['TSLA']['signed_pct'] == pytest.approx(-0.25)

    def test_hedged_pair_does_not_warn_on_correlation_cluster(self):
        """If two correlated symbols are held in opposite directions, net
        exposure is ~0 and the cluster warning must NOT fire."""
        analyzer = PortfolioRiskAnalyzer()
        # Manually inject a mock cluster via the internal helper so we don't
        # need real correlation data.
        positions = _make_positions([
            ('AAPL', 100_000),
            ('MSFT', -100_000),
        ])
        # Patch _compute_correlations to return a correlated cluster so we can
        # test the downstream warning logic in isolation.
        def fake_corr(symbols, weights, signed_weights, net_liq):
            return ([{
                'symbols': ['AAPL', 'MSFT'],
                'avg_corr': 0.9,
                'combined_weight_pct': 1.0,      # gross: 100%
                'net_weight_pct': signed_weights.get('AAPL', 0) + signed_weights.get('MSFT', 0),
            }], 2)
        analyzer._compute_correlations = fake_corr  # type: ignore
        report = analyzer.analyze(positions, 200_000.0)
        # Net exposure of the cluster is 0, so no correlation warning.
        cluster_warnings = [w for w in report.warnings if 'cluster' in w['message'].lower()]
        assert cluster_warnings == []

    def test_directionally_stacked_cluster_does_warn(self):
        """If correlated symbols are all in the same direction, the warning
        fires on the net exposure."""
        analyzer = PortfolioRiskAnalyzer()
        positions = _make_positions([
            ('XLE', 80_000),
            ('XOM', 80_000),
        ])
        def fake_corr(symbols, weights, signed_weights, net_liq):
            return ([{
                'symbols': ['XLE', 'XOM'],
                'avg_corr': 0.9,
                'combined_weight_pct': 0.8,
                'net_weight_pct': 0.8,
            }], 2)
        analyzer._compute_correlations = fake_corr  # type: ignore
        report = analyzer.analyze(positions, 200_000.0)
        cluster_warnings = [w for w in report.warnings if 'cluster' in w['message'].lower()]
        assert len(cluster_warnings) == 1


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
