"""Portfolio risk analysis — concentration, correlation, group budgets, and warnings."""

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional  # noqa: F401

import math


@dataclass
class RiskReport:
    total_positions: int = 0
    net_liquidation: float = 0.0
    gross_exposure_pct: float = 0.0
    # Signed net exposure as a fraction of net liquidation. Negative means the
    # book is net short. For a fully hedged book |net_exposure_pct| ≈ 0 while
    # gross_exposure_pct may be large.
    net_exposure_pct: float = 0.0
    long_exposure_pct: float = 0.0
    short_exposure_pct: float = 0.0
    top_positions: List[dict] = field(default_factory=list)
    hhi: float = 0.0
    group_allocations: List[dict] = field(default_factory=list)
    ungrouped_symbols: List[str] = field(default_factory=list)
    correlation_clusters: List[dict] = field(default_factory=list)
    data_coverage: int = 0
    warnings: List[dict] = field(default_factory=list)
    summary: str = ''

    def to_dict(self) -> dict:
        return asdict(self)


class PortfolioRiskAnalyzer:
    """Analyzes portfolio risk: concentration, correlation, group budgets."""

    def __init__(self, duckdb_path: str = ''):
        self._duckdb_path = duckdb_path

    def analyze(
        self,
        positions: List[dict],
        net_liquidation: float,
        group_store=None,
    ) -> RiskReport:
        """Build a risk report from live portfolio positions.

        Parameters
        ----------
        positions : list of dict
            Each dict has at least 'symbol', 'marketValue' (or 'mktValue').
        net_liquidation : float
            Total account net liquidation value.
        group_store : PositionGroupStore, optional
            For group allocation analysis.
        """
        report = RiskReport(
            total_positions=len(positions),
            net_liquidation=net_liquidation,
        )

        if not positions or net_liquidation <= 0:
            report.summary = 'No positions or account data available.'
            return report

        # Normalize position data. We preserve the *signed* market value (shorts
        # are negative) so hedged books are correctly identified, and track the
        # absolute value separately for concentration/HHI (which are gross
        # exposure measures).
        pos_data = []
        for p in positions:
            symbol = p.get('symbol', '')
            signed = float(p.get('marketValue', p.get('mktValue', 0)) or 0)
            pos_data.append({
                'symbol': symbol,
                'value': abs(signed),
                'signed_value': signed,
                'is_short': signed < 0,
            })

        total_value = sum(p['value'] for p in pos_data)
        long_value = sum(p['value'] for p in pos_data if not p['is_short'])
        short_value = sum(p['value'] for p in pos_data if p['is_short'])
        net_signed = sum(p['signed_value'] for p in pos_data)

        if net_liquidation > 0:
            report.gross_exposure_pct = round(total_value / net_liquidation, 4)
            report.net_exposure_pct = round(net_signed / net_liquidation, 4)
            report.long_exposure_pct = round(long_value / net_liquidation, 4)
            report.short_exposure_pct = round(short_value / net_liquidation, 4)

        # Per-symbol weights. We report signed weights in top_positions so the
        # direction (long/short) is visible to the LLM, but drive concentration
        # warnings off absolute gross weight.
        weights: Dict[str, float] = {}
        signed_weights: Dict[str, float] = {}
        for p in pos_data:
            abs_pct = p['value'] / net_liquidation if net_liquidation > 0 else 0.0
            signed_pct = p['signed_value'] / net_liquidation if net_liquidation > 0 else 0.0
            weights[p['symbol']] = abs_pct
            signed_weights[p['symbol']] = signed_pct

        sorted_positions = sorted(pos_data, key=lambda x: x['value'], reverse=True)
        report.top_positions = [
            {
                'symbol': p['symbol'],
                'pct': round(weights[p['symbol']], 4),
                'signed_pct': round(signed_weights[p['symbol']], 4),
                'value': round(p['value'], 2),
                'is_short': p['is_short'],
            }
            for p in sorted_positions[:10]
        ]

        # HHI (Herfindahl-Hirschman Index) — a gross-exposure concentration
        # metric. HHI on signed weights would understate concentration for
        # hedged pairs, which is the wrong risk framing.
        if total_value > 0:
            portfolio_weights = [p['value'] / total_value for p in pos_data]
            report.hhi = round(sum(w ** 2 for w in portfolio_weights), 4)

        # Concentration warnings
        warnings = []
        for sym, pct in weights.items():
            if pct > 0.15:
                warnings.append({
                    'level': 'critical',
                    'message': f'{sym} is {pct:.1%} of portfolio (>15%)',
                    'symbols': [sym],
                })
            elif pct > 0.10:
                warnings.append({
                    'level': 'warning',
                    'message': f'{sym} is {pct:.1%} of portfolio (>10%)',
                    'symbols': [sym],
                })

        # HHI warning
        if report.hhi > 0.15:
            warnings.append({
                'level': 'warning',
                'message': f'Portfolio concentration (HHI={report.hhi:.3f}) is high — consider diversifying',
                'symbols': [],
            })

        # Group allocations
        if group_store:
            try:
                groups = group_store.list_groups()
                grouped_symbols = set()
                for group in groups:
                    group_value = sum(
                        p['value'] for p in pos_data if p['symbol'] in group.members
                    )
                    group_pct = group_value / net_liquidation if net_liquidation > 0 else 0.0
                    over_budget = (
                        group.max_allocation_pct > 0 and group_pct > group.max_allocation_pct
                    )
                    report.group_allocations.append({
                        'name': group.name,
                        'symbols': group.members,
                        'value': round(group_value, 2),
                        'pct': round(group_pct, 4),
                        'budget_pct': group.max_allocation_pct,
                        'over_budget': over_budget,
                    })
                    if over_budget:
                        warnings.append({
                            'level': 'warning',
                            'message': (
                                f'Group "{group.name}" at {group_pct:.1%} '
                                f'exceeds budget {group.max_allocation_pct:.1%}'
                            ),
                            'symbols': group.members,
                        })
                    grouped_symbols.update(group.members)

                all_symbols = {p['symbol'] for p in pos_data}
                report.ungrouped_symbols = sorted(all_symbols - grouped_symbols)
            except Exception:
                pass

        # Correlation analysis.
        # We pass signed_weights through so clusters can report combined NET
        # exposure. A long AAPL + short MSFT pair may be highly correlated, but
        # it hedges risk rather than concentrating it — warn only on net.
        correlation_clusters, data_coverage = self._compute_correlations(
            [p['symbol'] for p in pos_data], weights, signed_weights, net_liquidation
        )
        report.correlation_clusters = correlation_clusters
        report.data_coverage = data_coverage

        for cluster in correlation_clusters:
            # Warn on absolute net exposure: direction-correlated and large.
            net_pct = cluster.get('net_weight_pct', cluster.get('combined_weight_pct', 0))
            if abs(net_pct) > 0.30:
                warnings.append({
                    'level': 'warning',
                    'message': (
                        f'Correlated cluster ({", ".join(cluster["symbols"])}) '
                        f'has {net_pct:+.1%} net exposure'
                    ),
                    'symbols': cluster['symbols'],
                })

        report.warnings = warnings
        report.summary = self._generate_summary(report)
        return report

    def _compute_correlations(
        self,
        symbols: List[str],
        weights: Dict[str, float],
        signed_weights: Optional[Dict[str, float]] = None,
        net_liquidation: float = 0.0,
    ) -> tuple:
        """Compute correlation clusters from local DuckDB history data.

        Returns (clusters, data_coverage_count). Each cluster dict carries
        ``combined_weight_pct`` (absolute, concentration view) and
        ``net_weight_pct`` (signed, true exposure after hedging).
        """
        signed_weights = signed_weights or weights
        if not self._duckdb_path or len(symbols) < 2:
            return [], 0

        try:
            import pandas as pd
            from trader.data.duckdb_store import DuckDBDataStore, DuckDBConnection
            from trader.data.universe import UniverseAccessor

            ds = DuckDBDataStore(self._duckdb_path)

            # Look up conIds for each symbol from universe
            accessor = UniverseAccessor(self._duckdb_path)
            all_defs = accessor.all_definitions()
            symbol_to_conid = {}
            for d in all_defs:
                if hasattr(d, 'symbol') and hasattr(d, 'conId'):
                    symbol_to_conid[d.symbol] = d.conId

            # Read 60 days of daily closes for each symbol
            close_series = {}
            for sym in symbols:
                con_id = symbol_to_conid.get(sym)
                if con_id is None:
                    continue
                hist = ds.read(con_id, '1 day', count=60)
                if hist is not None and not hist.empty and 'close' in hist.columns:
                    close_series[sym] = hist['close']

            data_coverage = len(close_series)
            if data_coverage < 2:
                return [], data_coverage

            # Build DataFrame and compute correlation
            df = pd.DataFrame(close_series).dropna()
            if len(df) < 10:
                return [], data_coverage

            corr = df.corr()

            # Greedy agglomeration: cluster symbols with pairwise corr > 0.7
            clustered = set()
            clusters = []
            syms = list(close_series.keys())
            for i, s1 in enumerate(syms):
                if s1 in clustered:
                    continue
                cluster = [s1]
                for s2 in syms[i + 1:]:
                    if s2 in clustered:
                        continue
                    if s1 in corr.columns and s2 in corr.columns:
                        c = corr.loc[s1, s2]
                        if not (isinstance(c, float) and math.isnan(c)) and c > 0.7:
                            cluster.append(s2)

                if len(cluster) > 1:
                    # Compute average correlation within cluster
                    corr_vals = []
                    for ci in range(len(cluster)):
                        for cj in range(ci + 1, len(cluster)):
                            if cluster[ci] in corr.columns and cluster[cj] in corr.columns:
                                v = corr.loc[cluster[ci], cluster[cj]]
                                if isinstance(v, float) and not math.isnan(v):
                                    corr_vals.append(v)

                    avg_corr = sum(corr_vals) / len(corr_vals) if corr_vals else 0.0
                    combined_weight = sum(weights.get(s, 0) for s in cluster)
                    net_weight = sum(signed_weights.get(s, weights.get(s, 0)) for s in cluster)

                    clusters.append({
                        'symbols': cluster,
                        'avg_corr': round(avg_corr, 3),
                        'combined_weight_pct': round(combined_weight, 4),
                        'net_weight_pct': round(net_weight, 4),
                    })
                    clustered.update(cluster)

            return clusters, data_coverage

        except Exception:
            return [], 0

    def _generate_summary(self, report: RiskReport) -> str:
        """Generate a plain-English summary paragraph."""
        parts = []
        # Call out a hedged book explicitly: when gross is meaningfully above
        # net, the portfolio has offsetting positions and concentration numbers
        # should be read accordingly.
        if (
            report.gross_exposure_pct > 0
            and abs(report.gross_exposure_pct - abs(report.net_exposure_pct)) > 0.10
        ):
            parts.append(
                f'Portfolio has {report.total_positions} positions '
                f'with {report.gross_exposure_pct:.0%} gross / '
                f'{report.net_exposure_pct:+.0%} net exposure (hedged).'
            )
        else:
            parts.append(
                f'Portfolio has {report.total_positions} positions '
                f'with {report.gross_exposure_pct:.0%} gross exposure.'
            )

        if report.hhi > 0.15:
            parts.append(f'Concentration is high (HHI={report.hhi:.3f}).')
        elif report.hhi > 0.10:
            parts.append(f'Concentration is moderate (HHI={report.hhi:.3f}).')
        else:
            parts.append(f'Diversification is good (HHI={report.hhi:.3f}).')

        critical = [w for w in report.warnings if w['level'] == 'critical']
        warning_count = len([w for w in report.warnings if w['level'] == 'warning'])

        if critical:
            parts.append(f'{len(critical)} critical issue(s).')
        if warning_count:
            parts.append(f'{warning_count} warning(s).')

        if report.correlation_clusters:
            parts.append(
                f'{len(report.correlation_clusters)} correlated cluster(s) detected '
                f'({report.data_coverage}/{report.total_positions} symbols with data).'
            )

        if report.group_allocations:
            over_budget = [g for g in report.group_allocations if g['over_budget']]
            if over_budget:
                names = ', '.join(g['name'] for g in over_budget)
                parts.append(f'Groups over budget: {names}.')

        if not report.warnings:
            parts.append('No risk issues detected.')

        return ' '.join(parts)
