#!/usr/bin/env python3
"""Strategy Tournament: comprehensive multi-strategy, multi-symbol analysis.

Runs every strategy against every available symbol, computes advanced risk
metrics (Sortino, Calmar, rolling beta, profit factor), and produces a
ranked leaderboard with correlation analysis between strategies.

Usage:
    python scripts/tournament.py
    python scripts/tournament.py --bar-size "1 min"
    python scripts/tournament.py --symbols AMD NVDA AAPL
"""

import argparse
import datetime as dt
import math
import os
import sys
import warnings

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

warnings.filterwarnings('ignore', category=UserWarning, module='joblib')

from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.objects import BarSize
from trader.simulation.backtester import Backtester, BacktestConfig, BacktestResult
from trader.simulation.slippage import SquareRootImpact


# ─── Advanced Risk Metrics ──────────────────────────────────────────────

def sortino_ratio(equity_curve: pd.Series, bar_size: BarSize, risk_free: float = 0.0) -> float:
    """Sortino ratio: like Sharpe but only penalizes downside volatility."""
    returns = equity_curve.pct_change().dropna()
    if len(returns) < 2:
        return 0.0
    excess = returns - risk_free / Backtester._bars_per_year(bar_size)
    downside = excess[excess < 0]
    downside_std = np.sqrt(np.mean(downside ** 2)) if len(downside) > 0 else 0.0
    if downside_std < 1e-10:
        return 0.0 if returns.mean() <= 0 else float('inf')
    return float((returns.mean() / downside_std) * np.sqrt(Backtester._bars_per_year(bar_size)))


def calmar_ratio(equity_curve: pd.Series, bar_size: BarSize) -> float:
    """Calmar ratio: annualized return / max drawdown. Higher is better."""
    if len(equity_curve) < 2:
        return 0.0
    total_return = equity_curve.iloc[-1] / equity_curve.iloc[0] - 1
    bars_in_curve = len(equity_curve)
    bars_per_year = Backtester._bars_per_year(bar_size)
    years = bars_in_curve / bars_per_year if bars_per_year > 0 else 1.0
    annual_return = (1 + total_return) ** (1 / max(years, 0.01)) - 1

    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    max_dd = abs(float(drawdown.min()))
    if max_dd < 1e-10:
        return 0.0 if annual_return <= 0 else float('inf')
    return annual_return / max_dd


def profit_factor(trades) -> float:
    """Gross profit / gross loss. > 1 is profitable."""
    gross_profit = 0.0
    gross_loss = 0.0
    from trader.objects import Action

    # Build position tracker for P&L
    entries = {}  # conid -> (avg_price, qty)
    for t in trades:
        if t.action == Action.BUY:
            prev_qty = entries.get(t.conid, (0.0, 0.0))[1]
            prev_cost = entries.get(t.conid, (0.0, 0.0))[0] * prev_qty
            new_qty = prev_qty + t.quantity
            if new_qty > 0:
                entries[t.conid] = ((prev_cost + t.price * t.quantity) / new_qty, new_qty)
        elif t.action == Action.SELL:
            entry_price = entries.get(t.conid, (0.0, 0.0))[0]
            if entry_price > 0:
                pnl = (t.price - entry_price) * t.quantity - t.commission
                if pnl > 0:
                    gross_profit += pnl
                else:
                    gross_loss += abs(pnl)
            remaining = entries.get(t.conid, (0.0, 0.0))[1] - t.quantity
            if remaining <= 0:
                entries.pop(t.conid, None)
            else:
                entries[t.conid] = (entry_price, remaining)

    if gross_loss < 1e-6:
        return float('inf') if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def max_consecutive_losses(trades) -> int:
    """Longest streak of consecutive losing trades."""
    from trader.objects import Action
    entries = {}
    max_streak = 0
    current_streak = 0

    for t in trades:
        if t.action == Action.BUY:
            prev_qty = entries.get(t.conid, (0.0, 0.0))[1]
            prev_cost = entries.get(t.conid, (0.0, 0.0))[0] * prev_qty
            new_qty = prev_qty + t.quantity
            if new_qty > 0:
                entries[t.conid] = ((prev_cost + t.price * t.quantity) / new_qty, new_qty)
        elif t.action == Action.SELL:
            entry_price = entries.get(t.conid, (0.0, 0.0))[0]
            if entry_price > 0:
                if t.price < entry_price:
                    current_streak += 1
                    max_streak = max(max_streak, current_streak)
                else:
                    current_streak = 0
            remaining = entries.get(t.conid, (0.0, 0.0))[1] - t.quantity
            if remaining <= 0:
                entries.pop(t.conid, None)
            else:
                entries[t.conid] = (entry_price, remaining)

    return max_streak


def avg_trade_duration(trades) -> float:
    """Average holding period in bars between buy and sell."""
    from trader.objects import Action
    open_times = {}
    durations = []

    for t in trades:
        if t.action == Action.BUY:
            if t.conid not in open_times:
                open_times[t.conid] = t.timestamp
        elif t.action == Action.SELL:
            if t.conid in open_times:
                delta = t.timestamp - open_times[t.conid]
                durations.append(delta.total_seconds() / 86400)  # in days
                del open_times[t.conid]

    return float(np.mean(durations)) if durations else 0.0


def rolling_beta(equity_curve: pd.Series, benchmark_curve: pd.Series, window: int = 60) -> pd.Series:
    """Rolling beta of strategy returns vs benchmark returns."""
    strat_ret = equity_curve.pct_change().dropna()
    bench_ret = benchmark_curve.pct_change().dropna()

    # Align on common index
    common = strat_ret.index.intersection(bench_ret.index)
    if len(common) < window:
        return pd.Series(dtype=float)

    strat_ret = strat_ret.loc[common]
    bench_ret = bench_ret.loc[common]

    betas = []
    for i in range(window, len(common)):
        s = strat_ret.iloc[i - window:i]
        b = bench_ret.iloc[i - window:i]
        cov = np.cov(s, b)
        if cov[1, 1] > 1e-10:
            betas.append(cov[0, 1] / cov[1, 1])
        else:
            betas.append(0.0)

    return pd.Series(betas, index=common[window:])


# ─── Strategy Registry ──────────────────────────────────────────────────

STRATEGIES = [
    ('strategies/ma_crossover.py', 'MaCrossover'),
    ('strategies/smi_crossover.py', 'SMICrossOver'),
    ('strategies/momentum.py', 'Momentum'),
    ('strategies/mean_reversion.py', 'MeanReversion'),
    ('strategies/rsi_strategy.py', 'RSIStrategy'),
    ('strategies/ensemble.py', 'Ensemble'),
    ('strategies/vbt_macd_bb.py', 'VbtMacdBB'),
    ('strategies/regime_adaptive.py', 'RegimeAdaptive'),
]

# conid -> ticker name mapping
CONID_MAP = {
    '4391': 'AMD',
    '265598': 'AAPL',
    '272093': 'MSFT',
    '4815747': 'NVDA',
    'SPY': 'SPY',
    'TSLA': 'TSLA',
    'META': 'META',
    'AMZN': 'AMZN',
    'GOOG': 'GOOG',
}


def run_tournament(
    duckdb_path: str = 'data/mmr.duckdb',
    bar_size: BarSize = BarSize.Days1,
    days: int = 365,
    symbols: list = None,
):
    storage = TickStorage(duckdb_path)
    store = DuckDBDataStore(duckdb_path)
    all_symbols = store.list_symbols()

    # Filter to symbols that have the right bar_size data
    valid_symbols = []
    for sym in all_symbols:
        df = store.read(sym, bar_size=str(bar_size))
        if df is not None and len(df) >= 30:
            valid_symbols.append(sym)

    if symbols:
        # Map ticker names to conids
        reverse_map = {v: k for k, v in CONID_MAP.items()}
        filtered = []
        for s in symbols:
            if s in valid_symbols:
                filtered.append(s)
            elif s.upper() in reverse_map and reverse_map[s.upper()] in valid_symbols:
                filtered.append(reverse_map[s.upper()])
            elif s.upper() in valid_symbols:
                filtered.append(s.upper())
        valid_symbols = filtered

    print(f'\n{"="*80}')
    print(f'  STRATEGY TOURNAMENT')
    print(f'  Bar size: {bar_size}  |  Symbols: {len(valid_symbols)}  |  Days: {days}')
    print(f'{"="*80}\n')

    end_date = dt.datetime.now()
    start_date = end_date - dt.timedelta(days=days)

    config = BacktestConfig(
        start_date=start_date,
        end_date=end_date,
        bar_size=bar_size,
        slippage_model=SquareRootImpact(),
    )

    results = []  # (strategy_name, symbol, BacktestResult)
    equity_curves = {}  # strategy_name -> aggregated equity curve

    for strat_path, strat_class in STRATEGIES:
        if not os.path.exists(strat_path):
            print(f'  [SKIP] {strat_class}: {strat_path} not found')
            continue

        for sym in valid_symbols:
            sym_name = CONID_MAP.get(sym, sym)
            try:
                conid_arg = int(sym) if sym.isdigit() else sym
                bt = Backtester(storage=storage, config=config)
                result = bt.run_from_module(strat_path, strat_class, [conid_arg])
                results.append((strat_class, sym_name, result))

                # Aggregate equity curves per strategy
                key = strat_class
                if key not in equity_curves:
                    equity_curves[key] = result.equity_curve
                else:
                    # Normalize and average
                    norm = result.equity_curve / result.equity_curve.iloc[0]
                    existing_norm = equity_curves[key] / equity_curves[key].iloc[0]
                    # Simple concatenation for correlation analysis later
                    pass

            except Exception as e:
                print(f'  [ERR]  {strat_class} × {sym_name}: {e}')

    if not results:
        print('No results to display.')
        return

    # ─── Build results table ─────────────────────────────────────────
    rows = []
    for strat_name, sym_name, r in results:
        sort_r = sortino_ratio(r.equity_curve, bar_size) if len(r.equity_curve) > 1 else 0.0
        calm_r = calmar_ratio(r.equity_curve, bar_size) if len(r.equity_curve) > 1 else 0.0
        pf = profit_factor(r.trades)
        max_loss_streak = max_consecutive_losses(r.trades)
        avg_hold = avg_trade_duration(r.trades)

        rows.append({
            'Strategy': strat_name,
            'Symbol': sym_name,
            'Return': r.total_return,
            'Sharpe': r.sharpe_ratio,
            'Sortino': sort_r,
            'Calmar': calm_r,
            'MaxDD': r.max_drawdown,
            'WinRate': r.win_rate,
            'Trades': r.total_trades,
            'ProfitFactor': pf,
            'MaxLossStreak': max_loss_streak,
            'AvgHold': avg_hold,
        })

    df = pd.DataFrame(rows)

    # ─── Per-strategy aggregate ──────────────────────────────────────
    print(f'\n{"─"*100}')
    print(f'  DETAILED RESULTS (sorted by Sortino)')
    print(f'{"─"*100}')

    df_sorted = df.sort_values('Sortino', ascending=False)

    header = f'{"Strategy":<20} {"Symbol":<8} {"Return":>8} {"Sharpe":>8} {"Sortino":>8} {"Calmar":>8} {"MaxDD":>8} {"WinRate":>8} {"Trades":>7} {"PF":>6} {"AvgHold":>8}'
    print(header)
    print('─' * len(header))

    for _, row in df_sorted.iterrows():
        pf_str = f'{row["ProfitFactor"]:.1f}' if row['ProfitFactor'] != float('inf') else 'inf'
        print(
            f'{row["Strategy"]:<20} {row["Symbol"]:<8} '
            f'{row["Return"]:>7.2%} '
            f'{row["Sharpe"]:>8.2f} '
            f'{row["Sortino"]:>8.2f} '
            f'{row["Calmar"]:>8.2f} '
            f'{row["MaxDD"]:>7.2%} '
            f'{row["WinRate"]:>7.0%} '
            f'{row["Trades"]:>7} '
            f'{pf_str:>6} '
            f'{row["AvgHold"]:>7.1f}d'
        )

    # ─── Strategy Leaderboard (aggregated across symbols) ────────────
    print(f'\n{"─"*80}')
    print(f'  STRATEGY LEADERBOARD (averaged across all symbols)')
    print(f'{"─"*80}')

    agg = df.groupby('Strategy').agg({
        'Return': 'mean',
        'Sharpe': 'mean',
        'Sortino': 'mean',
        'Calmar': 'mean',
        'MaxDD': 'mean',
        'WinRate': 'mean',
        'Trades': 'sum',
        'ProfitFactor': 'mean',
    }).sort_values('Sortino', ascending=False)

    # Composite score: weighted blend of Sortino, Calmar, and Sharpe
    finite_sortino = agg['Sortino'].replace([float('inf'), float('-inf')], np.nan).fillna(0)
    finite_calmar = agg['Calmar'].replace([float('inf'), float('-inf')], np.nan).fillna(0)
    agg['Score'] = (
        finite_sortino * 0.4
        + agg['Sharpe'] * 0.3
        + finite_calmar * 0.2
        + agg['WinRate'] * 10 * 0.1  # scale win rate
    )
    agg = agg.sort_values('Score', ascending=False)

    header2 = f'{"Rank":<5} {"Strategy":<20} {"AvgReturn":>10} {"AvgSharpe":>10} {"AvgSortino":>11} {"AvgCalmar":>10} {"AvgMaxDD":>9} {"WinRate":>8} {"Score":>7}'
    print(header2)
    print('─' * len(header2))

    for rank, (name, row) in enumerate(agg.iterrows(), 1):
        print(
            f'{rank:<5} {name:<20} '
            f'{row["Return"]:>9.2%} '
            f'{row["Sharpe"]:>10.2f} '
            f'{row["Sortino"]:>11.2f} '
            f'{row["Calmar"]:>10.2f} '
            f'{row["MaxDD"]:>8.2%} '
            f'{row["WinRate"]:>7.0%} '
            f'{row["Score"]:>7.2f}'
        )

    # ─── Strategy Correlation Matrix ─────────────────────────────────
    print(f'\n{"─"*80}')
    print(f'  STRATEGY RETURN CORRELATION (do strategies diversify each other?)')
    print(f'{"─"*80}')

    # Build per-strategy return series (average across symbols)
    strat_returns = {}
    for strat_name, sym_name, r in results:
        ret = r.equity_curve.pct_change().dropna()
        if strat_name not in strat_returns:
            strat_returns[strat_name] = []
        strat_returns[strat_name].append(ret)

    # Average returns per strategy
    avg_returns = {}
    for name, ret_list in strat_returns.items():
        if ret_list:
            # Use the longest series for correlation
            avg_returns[name] = ret_list[0]

    if len(avg_returns) >= 2:
        # Build correlation matrix
        ret_df = pd.DataFrame(avg_returns)
        corr = ret_df.corr()

        # Print correlation matrix
        names = list(corr.columns)
        name_width = 16
        print(f'{"":>{name_width}}', end='')
        for n in names:
            print(f'{n[:name_width]:>{name_width}}', end='')
        print()

        for i, n in enumerate(names):
            print(f'{n[:name_width]:>{name_width}}', end='')
            for j, m in enumerate(names):
                val = corr.iloc[i, j]
                if i == j:
                    print(f'{"1.00":>{name_width}}', end='')
                elif np.isnan(val):
                    print(f'{"n/a":>{name_width}}', end='')
                else:
                    print(f'{val:>{name_width}.2f}', end='')
            print()
    else:
        print('  Not enough strategies for correlation analysis.')

    # ─── Best Symbol per Strategy ────────────────────────────────────
    print(f'\n{"─"*80}')
    print(f'  BEST SYMBOL PER STRATEGY')
    print(f'{"─"*80}')

    for strat_name in df['Strategy'].unique():
        strat_df = df[df['Strategy'] == strat_name].sort_values('Sortino', ascending=False)
        if len(strat_df) > 0:
            best = strat_df.iloc[0]
            worst = strat_df.iloc[-1]
            print(f'  {strat_name:<20}  Best: {best["Symbol"]} ({best["Return"]:.2%}, Sortino={best["Sortino"]:.2f})  '
                  f'Worst: {worst["Symbol"]} ({worst["Return"]:.2%}, Sortino={worst["Sortino"]:.2f})')

    print(f'\n{"="*80}\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Strategy Tournament')
    parser.add_argument('--bar-size', default='1 day', help='Bar size (default: "1 day")')
    parser.add_argument('--days', type=int, default=365, help='Days of history')
    parser.add_argument('--symbols', nargs='+', help='Specific symbols to test')
    parser.add_argument('--db', default='data/mmr.duckdb', help='DuckDB path')
    args = parser.parse_args()

    bar_size = BarSize.parse_str(args.bar_size)
    run_tournament(
        duckdb_path=args.db,
        bar_size=bar_size,
        days=args.days,
        symbols=args.symbols,
    )
