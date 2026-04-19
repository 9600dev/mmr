"""MMR CLI + interactive REPL.

Usage::

    # One-shot commands
    python -m trader.mmr_cli portfolio
    python -m trader.mmr_cli buy AMD --market --quantity 10

    # Interactive REPL (no args)
    python -m trader.mmr_cli
"""

from pathlib import Path
from rich.console import Console
from rich.columns import Columns
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from trader.sdk import MMR
from typing import Any, Dict, List, Optional

import argparse
import json
import shlex
import sys
import time

console = Console()

# Module-level flag set per dispatch() call
_json_mode = False


# ------------------------------------------------------------------
# Output helpers
# ------------------------------------------------------------------

def print_df(df, title=None):
    """Render a pandas DataFrame as a rich table (or JSON if _json_mode)."""
    if _json_mode:
        if df is None or df.empty:
            print(json.dumps({"data": [], "title": title}, default=str))
        else:
            records = json.loads(df.to_json(orient='records', date_format='iso'))
            print(json.dumps({"data": records, "title": title}, default=str))
        return

    if df is None or df.empty:
        console.print("[dim]No data[/dim]")
        return

    # Columns that benefit from wrapping (long text)
    wrap_cols = {'reasoning', 'thesis', 'rejection_reason'}

    table = Table(title=title, show_lines=False, expand=True)
    for col in df.columns:
        justify = 'right' if df[col].dtype in ('float64', 'int64', 'float32', 'int32') else 'left'
        if col in wrap_cols:
            table.add_column(str(col), justify=justify, no_wrap=False, ratio=2, min_width=20)
        else:
            table.add_column(str(col), justify=justify, no_wrap=True)

    # Columns that show signed, colored values
    _signed_cols = {'unrealizedPNL', 'realizedPNL', 'dailyPNL', 'change', 'pnl'}
    _pct_cols = {'%', 'change_pct'}

    for _, row in df.iterrows():
        cells = []
        for col in df.columns:
            val = row[col]
            if isinstance(val, float):
                if val != val:  # NaN
                    cells.append('[dim]-[/dim]')
                elif col in _signed_cols:
                    color = 'green' if val >= 0 else 'red'
                    sign = '+' if val > 0 else ''
                    cells.append(f'[{color}]{sign}{val:,.2f}[/{color}]')
                elif col in _pct_cols:
                    color = 'green' if val >= 0 else 'red'
                    sign = '+' if val > 0 else ''
                    cells.append(f'[{color}]{sign}{val:.2f}%[/{color}]')
                elif col == 'volume':
                    cells.append(f'{val:,.0f}')
                else:
                    cells.append(f'{val:,.2f}')
            else:
                cells.append(str(val))
        table.add_row(*cells)

    # Use pager for tall output (more rows than terminal height)
    import shutil
    term_height = shutil.get_terminal_size().lines
    if len(df) > term_height - 6:
        with console.pager(styles=True):
            console.print(table)
    else:
        console.print(table)


def print_dict(d, title=None):
    """Render a dict as a two-column rich table (or JSON if _json_mode)."""
    if _json_mode:
        print(json.dumps({"data": d if d else {}, "title": title}, default=str))
        return

    if not d:
        console.print("[dim]No data[/dim]")
        return

    table = Table(title=title, show_header=False)
    table.add_column("Key", style="bold")
    table.add_column("Value")
    for k, v in d.items():
        table.add_row(str(k), str(v))
    console.print(table)


def _build_portfolio_cards(df, title='Portfolio', account_summary=None):
    """Build a renderable Group of portfolio cards from a DataFrame."""
    from rich.console import Group
    import math

    if df is None or df.empty:
        return Text('No positions', style='dim')

    def _fmt(v, decimals=2):
        if isinstance(v, float) and (math.isnan(v) or v != v):
            return '-'
        return f'{v:,.{decimals}f}'

    panels = []
    total_daily = 0.0
    total_unrealized = 0.0
    total_value = 0.0

    # Sort by symbol for stable ordering
    if 'symbol' in df.columns:
        df = df.sort_values('symbol', key=lambda s: s.str.upper()).reset_index(drop=True)

    for _, row in df.iterrows():
        symbol = row.get('symbol', '?')
        position = row.get('position', 0)
        mkt_price = row.get('mktPrice', 0)
        avg_cost = row.get('avgCost', 0)
        mkt_value = row.get('marketValue', 0)
        unrealized = row.get('unrealizedPNL', 0)
        daily = row.get('dailyPNL', 0)
        pct = row.get('%', 0)
        con_id = row.get('conId', '')

        total_daily += daily if isinstance(daily, (int, float)) and daily == daily else 0
        total_unrealized += unrealized if isinstance(unrealized, (int, float)) and unrealized == unrealized else 0
        total_value += mkt_value if isinstance(mkt_value, (int, float)) and mkt_value == mkt_value else 0

        pnl_color = 'green' if unrealized >= 0 else 'red'
        daily_color = 'green' if daily >= 0 else 'red'
        pct_sign = '+' if pct > 0 else ''

        lines = Text()
        lines.append(f'  Qty: {_fmt(position, 0):>10}', style='bold')
        lines.append(f'  Price: {_fmt(mkt_price):>9}\n')
        lines.append(f'  Avg: {_fmt(avg_cost):>10}')
        lines.append(f'  Value: {_fmt(mkt_value):>9}\n')
        lines.append(f'  P&L: ')
        lines.append(f'{_fmt(unrealized):>10}', style=pnl_color)
        lines.append(f'  Daily: ')
        lines.append(f'{_fmt(daily):>9}', style=daily_color)
        lines.append(f'\n  Return: ')
        lines.append(f'{pct_sign}{_fmt(pct)}%', style=pnl_color)
        lines.append(f'    cId: {con_id}', style='dim')

        border_color = 'green' if unrealized >= 0 else 'red'
        panel = Panel(lines, title=f'[bold]{symbol}[/bold]', border_style=border_color, width=40)
        panels.append(panel)

    # Summary header (shown above cards so it's always visible)
    header = Text()
    daily_color = 'green' if total_daily >= 0 else 'red'
    daily_sign = '+' if total_daily > 0 else ''
    unr_color = 'green' if total_unrealized >= 0 else 'red'
    unr_sign = '+' if total_unrealized > 0 else ''

    if account_summary:
        cash = account_summary.get('cash', '')
        available = account_summary.get('available', '')
        net_liq = account_summary.get('net_liq', '')
        if cash or available or net_liq:
            header.append('  Cash:       ')
            header.append(f'{cash:>12}', style='bold')
            header.append(f'   Available:  ')
            header.append(f'{available:>12}')
            header.append(f'   Net Liq:   {net_liq:>12}')
            header.append('\n')

    header.append('  Daily P&L:  ')
    header.append(f'{daily_sign}{_fmt(total_daily):>12}', style=f'bold {daily_color}')
    header.append(f'   Unrealized: ')
    header.append(f'{unr_sign}{_fmt(total_unrealized):>12}', style=unr_color)
    header.append(f'   Positions: ${_fmt(total_value):>12}')

    return Group(
        Text(f'\n{title}', style='bold'),
        header,
        Text(''),
        Columns(panels, padding=(0, 1)),
        Text(''),
    )


def print_portfolio_cards(df, title='Portfolio'):
    """Render portfolio as cards — one panel per position, readable in narrow terminals."""
    if _json_mode:
        print_df(df, title=title)
        return
    console.print(_build_portfolio_cards(df, title=title))


def print_list(items, title=None):
    """Render a list as a single-column rich table (or JSON if _json_mode)."""
    if _json_mode:
        print(json.dumps({"data": [str(i) for i in items] if items else [], "title": title}, default=str))
        return

    if not items:
        console.print("[dim]No data[/dim]")
        return

    table = Table(title=title)
    table.add_column("Value")
    for item in items:
        table.add_row(str(item))
    console.print(table)


def print_json_result(data, title=None):
    """Print structured data as JSON (for handlers that build custom Rich tables)."""
    if _json_mode:
        print(json.dumps({"data": data, "title": title}, default=str))
        return
    # Fallback: try print_dict or print_df
    if isinstance(data, dict):
        print_dict(data, title)
    elif isinstance(data, list):
        import pandas as pd
        if data and isinstance(data[0], dict):
            print_df(pd.DataFrame(data), title)
        else:
            print_list(data, title)
    else:
        console.print(str(data))


def print_status(message, success=True):
    """Print a status message (or JSON if _json_mode)."""
    if _json_mode:
        print(json.dumps({"success": success, "message": message}, default=str))
        return
    if success:
        console.print(f'[green]{message}[/green]')
    else:
        console.print(f'[red]{message}[/red]')


# ------------------------------------------------------------------
# Argparse parser
# ------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    fmt = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(prog='mmr', description='MMR Trading CLI')
    parser.add_argument('--json', action='store_true', default=False,
                        help='Output JSON instead of Rich tables')
    parser.add_argument('--debug', '-d', action='store_true', default=False,
                        help='Show debug/info log output')
    sub = parser.add_subparsers(dest='command')

    # portfolio
    portfolio_p = sub.add_parser('portfolio', aliases=['p'], help='Portfolio with P&L',
                   epilog='Examples:\n'
                          '  portfolio\n'
                          '  portfolio --table\n'
                          '  p',
                   formatter_class=fmt)
    portfolio_p.add_argument('--table', action='store_true', help='Use table format instead of cards')

    # positions
    sub.add_parser('positions', aliases=['pos'], help='Raw positions',
                   epilog='Examples:\n'
                          '  positions\n'
                          '  pos',
                   formatter_class=fmt)

    # orders
    sub.add_parser('orders', help='Open orders',
                   epilog='Examples:\n'
                          '  orders',
                   formatter_class=fmt)

    # trades
    sub.add_parser('trades', help='Active trades',
                   epilog='Examples:\n'
                          '  trades',
                   formatter_class=fmt)

    # snapshot
    snap_p = sub.add_parser('snapshot', aliases=['snap'], help='Price snapshot',
                            epilog='Examples:\n'
                                   '  snapshot AAPL\n'
                                   '  snap AMD --delayed',
                            formatter_class=fmt)
    snap_p.add_argument('symbol', help='Symbol to snapshot')
    snap_p.add_argument('--delayed', action='store_true', default=False, help='Use delayed market data')
    snap_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    snap_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')

    # snapshot-batch
    snap_batch_p = sub.add_parser('snapshot-batch', help='Batch price snapshots (JSON)',
                                   epilog='Examples:\n'
                                          '  snapshot-batch AAPL MSFT AMD\n'
                                          '  snapshot-batch BHP CBA --exchange ASX --currency AUD',
                                   formatter_class=fmt)
    snap_batch_p.add_argument('symbols', nargs='+', help='Symbols to snapshot')
    snap_batch_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    snap_batch_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')

    # depth
    depth_p = sub.add_parser('depth', help='Market depth (Level 2 order book)',
                              epilog='Examples:\n'
                                     '  depth AAPL\n'
                                     '  depth BHP --exchange ASX --rows 10\n'
                                     '  depth AAPL --no-chart\n'
                                     '  depth AAPL --no-open',
                              formatter_class=fmt)
    depth_p.add_argument('symbol', help='Symbol to query')
    depth_p.add_argument('--rows', type=int, default=5, help='Number of price levels (default: 5)')
    depth_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    depth_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')
    depth_p.add_argument('--smart', action='store_true', default=False, help='Use SMART depth aggregation')
    depth_p.add_argument('--no-open', action='store_true', default=False, help='Do not open PNG in Preview')
    depth_p.add_argument('--no-chart', action='store_true', default=False, help='Skip chart rendering')

    # resolve
    res_p = sub.add_parser('resolve', help='Resolve symbol to contract details',
                           epilog='Examples:\n'
                                  '  resolve AMD\n'
                                  '  resolve 4391',
                           formatter_class=fmt)
    res_p.add_argument('symbol', help='Symbol or conId')
    res_p.add_argument('--sectype', default='STK', help='Security type (STK, CASH, OPT, FUT, etc.)')
    res_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    res_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')

    # buy
    buy_p = sub.add_parser('buy', help='Place a buy order',
                           epilog='Examples:\n'
                                  '  buy AMD --market --quantity 10\n'
                                  '  buy AAPL --market --amount 5000\n'
                                  '  buy MSFT --limit 420.50 --quantity 25',
                           formatter_class=fmt)
    buy_p.add_argument('symbol', help='Symbol to buy')
    buy_p.add_argument('--market', action='store_true', default=False, help='Market order')
    buy_p.add_argument('--limit', type=float, default=None, help='Limit price')
    buy_p.add_argument('--quantity', type=float, default=None, help='Number of shares')
    buy_p.add_argument('--amount', type=float, default=None, help='Dollar amount (auto-calculates quantity)')
    buy_p.add_argument('--sectype', default='STK', help='Security type (STK, CASH, OPT, FUT, etc.)')
    buy_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    buy_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')

    # sell
    sell_p = sub.add_parser('sell', help='Place a sell order',
                            epilog='Examples:\n'
                                   '  sell AMD --market --quantity 10\n'
                                   '  sell AAPL --limit 250.00 --quantity 50\n'
                                   '  sell MSFT --market --amount 5000',
                            formatter_class=fmt)
    sell_p.add_argument('symbol', help='Symbol to sell')
    sell_p.add_argument('--market', action='store_true', default=False, help='Market order')
    sell_p.add_argument('--limit', type=float, default=None, help='Limit price')
    sell_p.add_argument('--quantity', type=float, default=None, help='Number of shares')
    sell_p.add_argument('--amount', type=float, default=None, help='Dollar amount (auto-calculates quantity)')
    sell_p.add_argument('--sectype', default='STK', help='Security type (STK, CASH, OPT, FUT, etc.)')
    sell_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    sell_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')

    # cancel
    cancel_p = sub.add_parser('cancel', help='Cancel an order',
                              epilog='Examples:\n'
                                     '  cancel 123',
                              formatter_class=fmt)
    cancel_p.add_argument('order_id', type=int, help='Order ID to cancel')

    # cancel-all
    sub.add_parser('cancel-all', help='Cancel all open orders',
                   epilog='Examples:\n'
                          '  cancel-all',
                   formatter_class=fmt)

    # to-market
    to_market_p = sub.add_parser('to-market', help='Convert an open order to market',
                                 description='Cancel limit order and re-place as market. Preserves stop-loss children.',
                                 epilog='Examples:\n'
                                        '  to-market 36',
                                 formatter_class=fmt)
    to_market_p.add_argument('order_id', type=int, help='Order ID to convert')

    # close
    close_p = sub.add_parser('close', aliases=['c'], help='Close a position by row number',
                             epilog='Examples:\n'
                                    '  close 1              # close entire position at row 1\n'
                                    '  close 3 --quantity 50 # partial close\n'
                                    '  c 2',
                             formatter_class=fmt)
    close_p.add_argument('row', type=int, help='Row number from portfolio/watch output')
    close_p.add_argument('--quantity', type=float, default=None, help='Partial quantity (default: entire position)')

    # close-all-positions
    sub.add_parser('close-all-positions', help='Close all positions at market',
                   epilog='Examples:\n'
                          '  close-all-positions',
                   formatter_class=fmt)

    # resize-positions
    resize_p = sub.add_parser('resize-positions', aliases=['resize'], help='Proportionally resize all positions',
                              epilog='Examples:\n'
                                     '  resize-positions --max-bound 500000   # trim to $500k\n'
                                     '  resize-positions --min-bound 300000   # grow to $300k\n'
                                     '  resize --max-bound 500000 --dry-run   # preview only',
                              formatter_class=fmt)
    resize_p.add_argument('--max-bound', type=float, default=None, help='Maximum portfolio value (trim positions)')
    resize_p.add_argument('--min-bound', type=float, default=None, help='Minimum portfolio value (grow positions)')
    resize_p.add_argument('--dry-run', action='store_true', help='Show plan without executing')

    # strategies
    strat_p = sub.add_parser('strategies', aliases=['strat'], help='List or manage strategies',
                             epilog='Examples:\n'
                                    '  strategies             # list all strategies\n'
                                    '  strat enable smi_crossover\n'
                                    '  strat disable smi_crossover\n'
                                    '  strat create my_strategy\n'
                                    '  strat deploy my_strategy --conids 4391\n'
                                    '  strat undeploy my_strategy\n'
                                    '  strat signals my_strategy\n'
                                    '  strat backtest my_strategy --days 365',
                             formatter_class=fmt)
    strat_sub = strat_p.add_subparsers(dest='strat_action')
    enable_p = strat_sub.add_parser('enable', help='Enable a strategy')
    enable_p.add_argument('name', help='Strategy name')
    enable_p.add_argument('--paper', action='store_true', default=True)
    disable_p = strat_sub.add_parser('disable', help='Disable a strategy')
    disable_p.add_argument('name', help='Strategy name')
    strat_sub.add_parser('reload', help='Reload strategies from YAML and re-subscribe')

    # strategies available — scan the strategies directory for Strategy
    # subclasses. Shows what's on disk vs what's deployed.
    strat_avail_p = strat_sub.add_parser('available', aliases=['avail', 'list-files'],
                                          help='Scan strategies/ folder for Strategy subclasses')
    strat_avail_p.add_argument('--directory', default=None,
                                help='Override strategies directory (default: strategies_directory from config)')

    # strategies inspect — AST-based discovery of class attrs and dispatch mode.
    # Designed for LLMs planning a sweep: returns tunables, dispatch mode
    # (precompute vs on_prices-only, which is the O(N²) trap), and docstring
    # without executing the strategy file.
    strat_inspect_p = strat_sub.add_parser('inspect',
                                             help='Discover tunable params + dispatch mode for every strategy (AST scan, no exec)')
    strat_inspect_p.add_argument('--directory', default=None,
                                   help='Override strategies directory')
    strat_inspect_p.add_argument('--strategy', default=None,
                                   help='Inspect a single strategy file (absolute or relative)')

    # strategies create
    strat_create_p = strat_sub.add_parser('create', help='Create a strategy template file')
    strat_create_p.add_argument('name', help='Strategy name (e.g. my_strategy)')
    strat_create_p.add_argument('--directory', default='strategies', help='Directory for strategy file')

    # strategies deploy
    strat_deploy_p = strat_sub.add_parser('deploy', help='Deploy strategy to config')
    strat_deploy_p.add_argument('name', help='Strategy name')
    strat_deploy_p.add_argument('--conids', type=int, nargs='+', default=None, help='Contract IDs')
    strat_deploy_p.add_argument('--universe', default=None, help='Universe name')
    strat_deploy_p.add_argument('--bar-size', default='1 min', help='Bar size (default: "1 min")')
    strat_deploy_p.add_argument('--days', type=int, default=90, help='Historical days prior (default: 90)')
    strat_deploy_p.add_argument('--paper', action='store_true', default=True, help='Paper trading mode')

    # strategies undeploy
    strat_undeploy_p = strat_sub.add_parser('undeploy', help='Remove strategy from config')
    strat_undeploy_p.add_argument('name', help='Strategy name')

    # strategies signals
    strat_signals_p = strat_sub.add_parser('signals', help='View recent strategy signals')
    strat_signals_p.add_argument('name', help='Strategy name')
    strat_signals_p.add_argument('--limit', type=int, default=20, help='Number of signals (default: 20)')

    # strategies backtest
    strat_bt_p = strat_sub.add_parser('backtest', help='Backtest a deployed strategy')
    strat_bt_p.add_argument('name', help='Strategy name')
    strat_bt_p.add_argument('--days', type=int, default=365, help='Days of history (default: 365)')
    strat_bt_p.add_argument('--capital', type=float, default=100000, help='Initial capital (default: 100000)')
    strat_bt_p.add_argument('--bar-size', default=None, help='Override bar size from config')

    # listen
    listen_p = sub.add_parser('listen', help='Stream live ticks via ZMQ PubSub',
                              epilog='Examples:\n'
                                     '  listen AAPL\n'
                                     '  listen AMD --topic ticker',
                              formatter_class=fmt)
    listen_p.add_argument('symbol', help='Symbol to stream')
    listen_p.add_argument('--topic', default='ticker', help='ZMQ topic filter (default: ticker)')
    listen_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    listen_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')

    # watch
    sub.add_parser('watch', aliases=['w'], help='Live portfolio monitor (Ctrl+C to stop)',
                   epilog='Examples:\n'
                          '  watch\n'
                          '  w',
                   formatter_class=fmt)

    # account
    sub.add_parser('account', help='Show IB account ID',
                   epilog='Examples:\n'
                          '  account',
                   formatter_class=fmt)

    # status
    sub.add_parser('status', help='Service health check',
                   epilog='Examples:\n'
                          '  status',
                   formatter_class=fmt)

    # market-hours
    sub.add_parser('market-hours', aliases=['mh'], help='Show market open/close status',
                   epilog='Examples:\n'
                          '  market-hours\n'
                          '  mh',
                   formatter_class=fmt)

    # logs
    logs_p = sub.add_parser('logs', help='View service logs (errors/warnings by default)',
                            epilog='Examples:\n'
                                   '  logs                        # last 50 errors/warnings\n'
                                   '  logs --tail 100             # last 100 lines\n'
                                   '  logs --level DEBUG          # all log levels\n'
                                   '  logs --service trader       # trader service only\n'
                                   '  logs --today                # only today\'s logs',
                            formatter_class=fmt)
    logs_p.add_argument('--tail', '-n', type=int, default=50, help='Number of lines to show (default: 50)')
    logs_p.add_argument('--level', '-l', default='WARNING',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Minimum log level (default: WARNING)')
    logs_p.add_argument('--service', '-s', default=None,
                        choices=['trader', 'strategy', 'data'],
                        help='Filter to a specific service')
    logs_p.add_argument('--today', action='store_true', default=False,
                        help='Only show logs from today')

    # risk
    risk_p = sub.add_parser('risk', help='View/adjust risk gate limits',
                            epilog='Examples:\n'
                                   '  risk                              # show current limits\n'
                                   '  risk --max-open-orders 20         # raise open order limit\n'
                                   '  risk --max-leverage 2.0           # allow 2x margin\n'
                                   '  risk --max-daily-loss 5000        # raise daily loss limit\n',
                            formatter_class=fmt)
    risk_p.add_argument('--max-open-orders', type=int, default=None, help='Max open orders')
    risk_p.add_argument('--max-daily-loss', type=float, default=None, help='Max daily loss ($)')
    risk_p.add_argument('--max-position-size-pct', type=float, default=None, help='Max position concentration (0.10 = 10%%)')
    risk_p.add_argument('--max-signals-per-hour', type=int, default=None, help='Max signals per hour')
    risk_p.add_argument('--max-leverage', type=float, default=None, help='Max leverage (1.0 = cash only)')
    risk_p.add_argument('--min-margin-cushion', type=float, default=None, help='Min margin cushion (0.10 = 10%%)')

    # filter
    filter_p = sub.add_parser('filter', help='View/manage trading filters (denylist, allowlist, exchanges)',
                              epilog='Examples:\n'
                                     '  filter                              # show current filters\n'
                                     '  filter deny NKLA RIDE               # add to denylist\n'
                                     '  filter allow AAPL MSFT NVDA         # set exclusive allowlist\n'
                                     '  filter remove NKLA                  # remove from deny+allowlist\n'
                                     '  filter set --min-price 5.0          # set price floor\n'
                                     '  filter set --deny-exchanges OTC     # block an exchange\n'
                                     '  filter set --deny-locations STK.CA  # block Canadian instruments\n'
                                     '  filter reset                        # clear all filters\n',
                              formatter_class=fmt)
    filter_sub = filter_p.add_subparsers(dest='filter_action')
    filter_sub.add_parser('show', help='Show current filters')
    filter_deny_p = filter_sub.add_parser('deny', help='Add symbols to denylist')
    filter_deny_p.add_argument('symbols', nargs='+', help='Symbols to deny')
    filter_allow_p = filter_sub.add_parser('allow', help='Add symbols to allowlist (exclusive)')
    filter_allow_p.add_argument('symbols', nargs='+', help='Symbols to allow')
    filter_remove_p = filter_sub.add_parser('remove', help='Remove symbols from denylist/allowlist')
    filter_remove_p.add_argument('symbols', nargs='+', help='Symbols to remove')
    filter_sub.add_parser('reset', help='Clear all filters')
    filter_set_p = filter_sub.add_parser('set', help='Set filter parameters')
    filter_set_p.add_argument('--min-price', type=float, default=None, help='Minimum price for discovery')
    filter_set_p.add_argument('--deny-exchanges', nargs='+', default=None, help='Denied exchanges')
    filter_set_p.add_argument('--exchanges', nargs='+', default=None, help='Allowed exchanges (exclusive)')
    filter_set_p.add_argument('--sec-types', nargs='+', default=None, help='Allowed sec types (exclusive)')
    filter_set_p.add_argument('--deny-sec-types', nargs='+', default=None, help='Denied sec types')
    filter_set_p.add_argument('--locations', nargs='+', default=None, help='Allowed IB locations (exclusive, e.g. STK.US.MAJOR)')
    filter_set_p.add_argument('--deny-locations', nargs='+', default=None, help='Denied IB locations (e.g. STK.CA)')

    # session (position sizing)
    session_p = sub.add_parser('session', help='Position sizing & session status',
                                epilog='Examples:\n'
                                       '  session                          # show config + portfolio + capacity\n'
                                       '  session set risk aggressive      # set risk level\n'
                                       '  session set base 3000            # set base position size\n'
                                       '  session set base_pct 0.03        # set base to 3% of net liq\n'
                                       '  session limits                   # show hard limits only\n'
                                       '  session reset                    # reset to config defaults',
                                formatter_class=fmt)
    session_sub = session_p.add_subparsers(dest='session_action')
    session_sub.add_parser('limits', help='Show hard limits')
    session_sub.add_parser('reset', help='Reset to config defaults')
    session_set_p = session_sub.add_parser('set', help='Set a session parameter')
    session_set_p.add_argument('key', choices=['risk', 'base', 'base_pct', 'daily_loss_limit'],
                                help='Parameter to set')
    session_set_p.add_argument('value', help='New value')

    # history
    bar_sizes = ('1 secs, 5 secs, 10 secs, 15 secs, 30 secs, '
                 '1 min, 2 mins, 3 mins, 5 mins, 10 mins, 15 mins, 20 mins, 30 mins, '
                 '1 hour, 2 hours, 3 hours, 4 hours, 8 hours, '
                 '1 day, 1 week, 1 month')
    hist_p = sub.add_parser('history', help='Download historical data',
                            epilog='Examples:\n'
                                   '  history list\n'
                                   '  history list --symbol AAPL\n'
                                   '  history list --bar_size "1 day"\n'
                                   '  history massive --symbol AAPL\n'
                                   '  history massive --universe nasdaq_top25 --prev_days 365\n'
                                   '  history massive --symbol AAPL --bar_size "1 hour" --prev_days 60\n'
                                   '  history ib --symbol AAPL --bar_size "1 min" --prev_days 5\n'
                                   '  history ib --universe portfolio --prev_days 10\n'
                                   '\n'
                                   f'Bar sizes: {bar_sizes}',
                            formatter_class=fmt)
    hist_sub = hist_p.add_subparsers(dest='hist_action')

    hist_list_p = hist_sub.add_parser('list', aliases=['ls'], help='List downloaded history in local store')
    hist_list_p.add_argument('--symbol', '-s', default=None, help='Filter by symbol or conId')
    hist_list_p.add_argument('--bar_size', '-b', default=None, help='Filter by bar size (e.g. "1 day")')

    hist_massive_p = hist_sub.add_parser('massive', help='Download history from Massive.com')
    hist_massive_p.add_argument('--symbol', default=None, help='Single symbol to download')
    hist_massive_p.add_argument('--universe', default=None, help='Universe name to download')
    hist_massive_p.add_argument('--bar_size', default='1 day', help='Bar size (default: "1 day")')
    hist_massive_p.add_argument('--prev_days', type=int, default=30, help='Days of history (default: 30)')

    hist_td_p = hist_sub.add_parser('twelvedata', aliases=['td'], help='Download history from TwelveData')
    hist_td_p.add_argument('--symbol', default=None, help='Single symbol to download')
    hist_td_p.add_argument('--universe', default=None, help='Universe name to download')
    hist_td_p.add_argument('--bar_size', default='1 day', help='Bar size (default: "1 day")')
    hist_td_p.add_argument('--prev_days', type=int, default=30, help='Days of history (default: 30)')

    hist_ib_p = hist_sub.add_parser('ib', help='Download history from Interactive Brokers')
    hist_ib_p.add_argument('--symbol', default=None, help='Single symbol to download')
    hist_ib_p.add_argument('--universe', default=None, help='Universe name to download')
    hist_ib_p.add_argument('--bar_size', default='1 min', help='Bar size (default: "1 min")')
    hist_ib_p.add_argument('--prev_days', type=int, default=5, help='Days of history (default: 5)')
    hist_ib_p.add_argument('--ib_client_id', type=int, default=10, help='IB client ID (default: 10)')

    # financials
    fin_p = sub.add_parser('financials', aliases=['fin'], help='Financial statements from Massive.com',
                           epilog='Examples:\n'
                                  '  financials balance AAPL\n'
                                  '  financials income NVDA --timeframe annual --limit 8\n'
                                  '  financials cashflow MSFT\n'
                                  '  financials ratios AAPL\n'
                                  '  financials filing AAPL --section risk_factors\n'
                                  '  fin balance AAPL --limit 8 --timeframe annual',
                           formatter_class=fmt)
    fin_sub = fin_p.add_subparsers(dest='fin_action')

    fin_balance_p = fin_sub.add_parser('balance', help='Balance sheet')
    fin_balance_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')
    fin_balance_p.add_argument('--limit', type=int, default=4, help='Number of periods (default: 4)')
    fin_balance_p.add_argument('--timeframe', default='quarterly', choices=['quarterly', 'annual'],
                               help='Timeframe (default: quarterly)')
    fin_balance_p.add_argument('--source', choices=['massive', 'twelvedata'], default='massive',
                               help='Data source (default: massive)')

    fin_income_p = fin_sub.add_parser('income', help='Income statement')
    fin_income_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')
    fin_income_p.add_argument('--limit', type=int, default=4, help='Number of periods (default: 4)')
    fin_income_p.add_argument('--timeframe', default='quarterly', choices=['quarterly', 'annual'],
                              help='Timeframe (default: quarterly)')
    fin_income_p.add_argument('--source', choices=['massive', 'twelvedata'], default='massive',
                              help='Data source (default: massive)')

    fin_cashflow_p = fin_sub.add_parser('cashflow', help='Cash flow statement')
    fin_cashflow_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')
    fin_cashflow_p.add_argument('--limit', type=int, default=4, help='Number of periods (default: 4)')
    fin_cashflow_p.add_argument('--timeframe', default='quarterly', choices=['quarterly', 'annual'],
                                help='Timeframe (default: quarterly)')
    fin_cashflow_p.add_argument('--source', choices=['massive', 'twelvedata'], default='massive',
                                help='Data source (default: massive)')

    fin_ratios_p = fin_sub.add_parser('ratios', help='Financial ratios (TTM)')
    fin_ratios_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')
    fin_ratios_p.add_argument('--source', choices=['massive', 'twelvedata'], default='massive',
                              help='Data source (default: massive)')

    fin_filing_p = fin_sub.add_parser('filing', help='10-K filing sections (business, risk_factors)')
    fin_filing_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')
    fin_filing_p.add_argument('--section', default='business', choices=['business', 'risk_factors'],
                              help='Section (default: business)')
    fin_filing_p.add_argument('--limit', type=int, default=1, help='Number of filings (default: 1, most recent)')

    # news
    news_p = sub.add_parser('news', help='Market news from Massive.com',
                             epilog='Examples:\n'
                                    '  news                          # General market news\n'
                                    '  news AAPL                     # News for AAPL\n'
                                    '  news AAPL --limit 20          # More articles\n'
                                    '  news AAPL --source benzinga   # Use Benzinga source\n'
                                    '  news AAPL --detail            # Full article details + sentiment',
                             formatter_class=fmt)
    news_p.add_argument('ticker', nargs='?', default=None, help='Ticker to filter (optional)')
    news_p.add_argument('--limit', type=int, default=10, help='Number of articles (default: 10)')
    news_p.add_argument('--source', default='polygon', choices=['polygon', 'benzinga'],
                         help='News source (default: polygon)')
    news_p.add_argument('--detail', action='store_true', default=False,
                         help='Show full article details with descriptions/sentiment')

    # options
    opt_p = sub.add_parser('options', aliases=['opt'], help='Options data and trading',
                           epilog='Examples:\n'
                                  '  options expirations AAPL\n'
                                  '  options chain AAPL\n'
                                  '  options chain AAPL -e 3m --type call\n'
                                  '  options chain AAPL -e 2026-03-20 --strike-min 200 --strike-max 250\n'
                                  '  options snapshot O:AAPL260320C00250000\n'
                                  '  options implied AAPL                    # defaults to ~3 months\n'
                                  '  options implied AAPL -e 90d\n'
                                  '  options implied AAPL -e 6m\n'
                                  '  options buy AAPL -e 3m -s 250 -r C -q 5 --market\n'
                                  '  options sell AAPL -e 2026-03-20 -s 250 -r C -q 5 --limit 3.50',
                           formatter_class=fmt)
    opt_sub = opt_p.add_subparsers(dest='opt_action')

    opt_exp_p = opt_sub.add_parser('expirations', aliases=['exp'], help='List expiration dates')
    opt_exp_p.add_argument('symbol', help='Underlying symbol')

    opt_chain_p = opt_sub.add_parser('chain', help='Options chain snapshot')
    opt_chain_p.add_argument('symbol', help='Underlying symbol')
    opt_chain_p.add_argument('-e', '--expiration', default=None,
                             help='Expiration: YYYY-MM-DD, or relative like "90d", "3m" (default: nearest)')
    opt_chain_p.add_argument('--type', dest='contract_type', default=None, choices=['call', 'put'],
                             help='Filter by call or put')
    opt_chain_p.add_argument('--strike-min', type=float, default=None, help='Minimum strike price')
    opt_chain_p.add_argument('--strike-max', type=float, default=None, help='Maximum strike price')

    opt_snap_p = opt_sub.add_parser('snapshot', aliases=['snap'], help='Single option contract detail')
    opt_snap_p.add_argument('ticker', help='Option ticker (e.g. O:AAPL260320C00250000)')

    opt_impl_p = opt_sub.add_parser('implied', help='Implied probability distribution')
    opt_impl_p.add_argument('symbol', help='Underlying symbol')
    opt_impl_p.add_argument('-e', '--expiration', default=None,
                             help='Expiration: YYYY-MM-DD, or relative like "90d", "3m" (default: ~3 months)')
    opt_impl_p.add_argument('--risk-free-rate', type=float, default=0.05, help='Risk-free rate (default: 0.05)')

    opt_buy_p = opt_sub.add_parser('buy', help='Buy option contracts')
    opt_buy_p.add_argument('symbol', help='Underlying symbol')
    opt_buy_p.add_argument('-e', '--expiration', required=True,
                            help='Expiration: YYYY-MM-DD, or relative like "90d", "3m"')
    opt_buy_p.add_argument('-s', '--strike', type=float, required=True, help='Strike price')
    opt_buy_p.add_argument('-r', '--right', required=True, choices=['C', 'P'], help='C for call, P for put')
    opt_buy_p.add_argument('-q', '--quantity', type=float, required=True, help='Number of contracts')
    opt_buy_p.add_argument('--market', action='store_true', default=False, help='Market order')
    opt_buy_p.add_argument('--limit', type=float, default=None, help='Limit price per contract')

    opt_sell_p = opt_sub.add_parser('sell', help='Sell option contracts')
    opt_sell_p.add_argument('symbol', help='Underlying symbol')
    opt_sell_p.add_argument('-e', '--expiration', required=True,
                            help='Expiration: YYYY-MM-DD, or relative like "90d", "3m"')
    opt_sell_p.add_argument('-s', '--strike', type=float, required=True, help='Strike price')
    opt_sell_p.add_argument('-r', '--right', required=True, choices=['C', 'P'], help='C for call, P for put')
    opt_sell_p.add_argument('-q', '--quantity', type=float, required=True, help='Number of contracts')
    opt_sell_p.add_argument('--market', action='store_true', default=False, help='Market order')
    opt_sell_p.add_argument('--limit', type=float, default=None, help='Limit price per contract')

    # forex
    fx_p = sub.add_parser('forex', aliases=['fx'], help='Forex data (IB or Massive.com)',
                           epilog='Examples:\n'
                                  '  forex snapshot EURUSD              # via IB (default)\n'
                                  '  forex snapshot EURUSD --source massive\n'
                                  '  forex quote EUR USD                # via IB (default)\n'
                                  '  forex quote EUR USD --source massive\n'
                                  '  forex snapshot-all                 # Massive only\n'
                                  '  forex movers                       # Massive only\n'
                                  '  forex movers --losers\n'
                                  '  forex convert EUR USD 1000         # Massive only',
                           formatter_class=fmt)
    fx_sub = fx_p.add_subparsers(dest='fx_action')

    fx_snap_p = fx_sub.add_parser('snapshot', aliases=['snap'], help='Forex pair snapshot')
    fx_snap_p.add_argument('pair', help='Currency pair (e.g. EURUSD)')
    fx_snap_p.add_argument('--source', default='ib', choices=['ib', 'massive'],
                            help='Data source (default: ib)')

    fx_sub.add_parser('snapshot-all', help='All forex pair snapshots (Massive only)')

    fx_movers_p = fx_sub.add_parser('movers', help='Top forex movers (Massive only)')
    fx_movers_p.add_argument('--losers', action='store_true', default=False, help='Show losers instead of gainers')

    fx_quote_p = fx_sub.add_parser('quote', help='Last forex quote')
    fx_quote_p.add_argument('from_currency', help='From currency (e.g. EUR)')
    fx_quote_p.add_argument('to_currency', help='To currency (e.g. USD)')
    fx_quote_p.add_argument('--source', default='ib', choices=['ib', 'massive'],
                             help='Data source (default: ib)')

    fx_convert_p = fx_sub.add_parser('convert', help='Currency conversion (Massive only)')
    fx_convert_p.add_argument('from_currency', help='From currency (e.g. EUR)')
    fx_convert_p.add_argument('to_currency', help='To currency (e.g. USD)')
    fx_convert_p.add_argument('amount', type=float, help='Amount to convert')

    # movers
    movers_p = sub.add_parser('movers', help='Top market movers (Massive.com or TwelveData)')
    movers_p.add_argument('--market', '-m', default='stocks',
                           choices=['stocks', 'crypto', 'indices', 'options', 'futures'],
                           help='Market type (default: stocks)')
    movers_p.add_argument('--losers', action='store_true', default=False,
                           help='Show losers instead of gainers')
    movers_p.add_argument('--detail', action='store_true', default=False,
                           help='Enrich with company name, ratios, and news (card view; Massive only)')
    movers_p.add_argument('--num', '-n', type=int, default=20,
                           help='Number of results (default: 20)')
    movers_p.add_argument('--source', choices=['massive', 'twelvedata'], default='massive',
                           help='Data source (default: massive)')

    # scan
    scan_p = sub.add_parser('scan', help='IB market scanner',
                             epilog='Examples:\n'
                                    '  scan                            # Top gainers (default)\n'
                                    '  scan losers                     # Top losers\n'
                                    '  scan active                     # Most active by volume\n'
                                    '  scan hot-volume                 # Hot by volume change\n'
                                    '  scan --scan-code HIGH_OPT_VOLUME\n'
                                    '  scan gainers --above-price 10 --num 30\n'
                                    '  scan --instrument ETF --location STK.US',
                             formatter_class=fmt)
    scan_p.add_argument('preset', nargs='?', default=None,
                         help='Preset: gainers, losers, active, hot-volume, hot-price')
    scan_p.add_argument('--scan-code', default=None,
                         help='Raw IB scan code (overrides preset)')
    scan_p.add_argument('--instrument', default='STK',
                         help='Instrument type (default: STK)')
    scan_p.add_argument('--location', default='STK.US.MAJOR',
                         help='Location code (default: STK.US.MAJOR)')
    scan_p.add_argument('--num', '-n', type=int, default=20,
                         help='Number of results (default: 20)')
    scan_p.add_argument('--above-price', type=float, default=0.0,
                         help='Min price filter')
    scan_p.add_argument('--above-volume', type=int, default=0,
                         help='Min volume filter')
    scan_p.add_argument('--market-cap-above', type=float, default=0.0,
                         help='Min market cap filter')

    # ideas
    ideas_p = sub.add_parser('ideas', aliases=['scan-ideas'], help='Scan for trading ideas',
                              epilog='Examples:\n'
                                     '  ideas                             # Momentum scan (default, US/Massive)\n'
                                     '  ideas gap-up                      # Gap-up scan\n'
                                     '  ideas momentum --tickers AAPL MSFT AMD NVDA\n'
                                     '  ideas mean-reversion --universe sp500\n'
                                     '  ideas gap-up --min-price 10\n'
                                     '  ideas volatile --num 25\n'
                                     '  ideas --presets                   # List all presets\n'
                                     '  ideas momentum --detail           # With fundamentals + news\n'
                                     '  ideas momentum --location STK.AU.ASX  # ASX (IB-backed)\n'
                                     '  ideas gap-up --location STK.CA        # Canada (IB-backed)',
                              formatter_class=fmt)
    ideas_p.add_argument('preset', nargs='?', default='momentum',
                          help='Preset: momentum, gap-up, gap-down, mean-reversion, breakout, volatile')
    ideas_p.add_argument('--tickers', '-t', nargs='+', default=None,
                          help='Explicit ticker list to scan')
    ideas_p.add_argument('--universe', '-u', default=None,
                          help='Universe name to scan')
    ideas_p.add_argument('--num', '-n', type=int, default=15,
                          help='Top N results (default: 15)')
    ideas_p.add_argument('--min-price', type=float, default=None,
                          help='Override min price filter')
    ideas_p.add_argument('--max-price', type=float, default=None,
                          help='Override max price filter')
    ideas_p.add_argument('--min-volume', type=int, default=None,
                          help='Override min volume filter')
    ideas_p.add_argument('--min-change', type=float, default=None,
                          help='Override min change%% filter')
    ideas_p.add_argument('--max-change', type=float, default=None,
                          help='Override max change%% filter')
    ideas_p.add_argument('--presets', action='store_true', default=False,
                          help='List all available presets and exit')
    ideas_p.add_argument('--detail', action='store_true', default=False,
                          help='Enrich with fundamentals + news (shortcut for --fundamentals --news)')
    ideas_p.add_argument('--fundamentals', '-f', action='store_true', default=False,
                          help='Enrich results with financial ratios (PE, D/E, ROE, etc.)')
    ideas_p.add_argument('--news', action='store_true', default=False,
                          help='Enrich results with latest news headline and sentiment')
    ideas_p.add_argument('--location', '-l', default=None,
                          help='IB market location (e.g. STK.AU.ASX, STK.CA, STK.HK.SEHK)')
    ideas_p.add_argument('--source', choices=['massive', 'twelvedata'], default='massive',
                          help='Data source for US equities (default: massive). '
                               'Ignored when --location is set (IB path).')

    # propose
    propose_p = sub.add_parser('propose', help='Create a trade proposal',
                                epilog='Examples:\n'
                                       '  propose AMD BUY --market --quantity 100 --bracket 180 150\n'
                                       '  propose AMD BUY --limit 165 --amount 5000 --trailing-stop-pct 2.0 --tif GTC\n'
                                       '  propose AAPL SELL --market --quantity 50 --stop-loss 140\n'
                                       '  propose AMD BUY --market --quantity 10 --reasoning "Breakout above resistance"',
                                formatter_class=fmt)
    propose_p.add_argument('symbol', help='Symbol to trade')
    propose_p.add_argument('action', choices=['BUY', 'SELL'], help='BUY or SELL')
    propose_p.add_argument('--quantity', '-q', type=float, default=None, help='Number of shares')
    propose_p.add_argument('--amount', '-a', type=float, default=None, help='Dollar amount (auto-calculates quantity)')
    propose_p.add_argument('--market', action='store_true', default=False, help='Market order (default)')
    propose_p.add_argument('--limit', type=float, default=None, help='Limit price')
    propose_p.add_argument('--bracket', nargs=2, type=float, metavar=('TP', 'SL'),
                            default=None, help='Bracket order: take-profit and stop-loss prices')
    propose_p.add_argument('--trailing-stop-pct', type=float, default=None,
                            help='Trailing stop by percent (e.g. 2.0 = 2%%)')
    propose_p.add_argument('--trailing-stop-amt', type=float, default=None,
                            help='Trailing stop by fixed dollar amount')
    propose_p.add_argument('--stop-loss', type=float, default=None, help='Stop-loss price only')
    propose_p.add_argument('--tif', default='DAY', choices=['DAY', 'GTC', 'GTD', 'IOC', 'OPG'],
                            help='Time in force (default: DAY)')
    propose_p.add_argument('--no-outside-rth', action='store_true', default=False,
                            help='Disable extended hours (default: outside RTH enabled)')
    propose_p.add_argument('--reasoning', default='', help='Why this trade')
    propose_p.add_argument('--confidence', type=float, default=0.0, help='Confidence 0-1')
    propose_p.add_argument('--thesis', default='', help='Short thesis label')
    propose_p.add_argument('--source', default='manual', help='Source label (manual, llm, scanner)')
    propose_p.add_argument('--sectype', default='STK', help='Security type (STK, CASH, OPT, etc.)')
    propose_p.add_argument('--exchange', default='', help='Exchange hint (e.g. ASX, TSE, SEHK)')
    propose_p.add_argument('--currency', default='', help='Currency hint (e.g. AUD, JPY, HKD)')
    propose_p.add_argument('--group', default='', help='Position group name (e.g. mining, tech)')

    # proposals
    proposals_p = sub.add_parser('proposals', help='List or view trade proposals',
                                  epilog='Examples:\n'
                                         '  proposals                    # List pending proposals\n'
                                         '  proposals --all              # All statuses\n'
                                         '  proposals --status EXECUTED  # Filter by status\n'
                                         '  proposals show 3             # Full detail for proposal #3',
                                  formatter_class=fmt)
    proposals_sub = proposals_p.add_subparsers(dest='proposals_action')
    proposals_show_p = proposals_sub.add_parser('show', help='Show full proposal detail')
    proposals_show_p.add_argument('proposal_id', type=int, help='Proposal ID')
    proposals_p.add_argument('--status', default=None, help='Filter by status (PENDING, APPROVED, EXECUTED, etc.)')
    proposals_p.add_argument('--all', action='store_true', default=False, help='Show all statuses')
    proposals_p.add_argument('--limit', type=int, default=50, help='Max results (default: 50)')

    # approve
    approve_p = sub.add_parser('approve', help='Approve and execute a trade proposal',
                                epilog='Examples:\n'
                                       '  approve 3                    # Execute proposal #3\n'
                                       '  approve --all                # Execute all pending proposals',
                                formatter_class=fmt)
    approve_p.add_argument('proposal_id', type=int, nargs='?', default=None, help='Proposal ID to approve')
    approve_p.add_argument('--all', action='store_true', default=False, help='Approve all pending proposals')

    # reject
    reject_p = sub.add_parser('reject', help='Reject a trade proposal',
                               epilog='Examples:\n'
                                      '  reject 3\n'
                                      '  reject 3 --reason "Changed thesis"\n'
                                      '  reject --all                 # Reject all pending proposals',
                               formatter_class=fmt)
    reject_p.add_argument('proposal_id', type=int, nargs='?', default=None, help='Proposal ID to reject')
    reject_p.add_argument('--all', action='store_true', default=False, help='Reject all pending proposals')
    reject_p.add_argument('--reason', default='', help='Rejection reason')

    # group (position groups)
    group_p = sub.add_parser('group', help='Manage position groups',
                              epilog='Examples:\n'
                                     '  group list                              # groups with member counts + allocation\n'
                                     '  group create mining --budget 20         # 20% max allocation\n'
                                     '  group delete mining\n'
                                     '  group show mining                       # members + allocation\n'
                                     '  group add mining BHP RIO FMG\n'
                                     '  group remove mining BHP\n'
                                     '  group set mining --budget 25',
                              formatter_class=fmt)
    group_sub = group_p.add_subparsers(dest='group_action')
    group_sub.add_parser('list', help='List all groups with member counts')
    group_create_p = group_sub.add_parser('create', help='Create a new group')
    group_create_p.add_argument('name', help='Group name')
    group_create_p.add_argument('--budget', type=float, default=0.0, help='Max allocation %% (e.g. 20 = 20%%)')
    group_create_p.add_argument('--description', default='', help='Group description')
    group_delete_p = group_sub.add_parser('delete', help='Delete a group')
    group_delete_p.add_argument('name', help='Group name')
    group_show_p = group_sub.add_parser('show', help='Show group details')
    group_show_p.add_argument('name', help='Group name')
    group_add_p = group_sub.add_parser('add', help='Add symbols to a group')
    group_add_p.add_argument('name', help='Group name')
    group_add_p.add_argument('symbols', nargs='+', help='Symbols to add')
    group_remove_p = group_sub.add_parser('remove', help='Remove a symbol from a group')
    group_remove_p.add_argument('name', help='Group name')
    group_remove_p.add_argument('symbol', help='Symbol to remove')
    group_set_p = group_sub.add_parser('set', help='Update group settings')
    group_set_p.add_argument('name', help='Group name')
    group_set_p.add_argument('--budget', type=float, default=None, help='Max allocation %% (e.g. 25 = 25%%)')
    group_set_p.add_argument('--description', default=None, help='Group description')

    # portfolio-risk
    prisk_p = sub.add_parser('portfolio-risk', aliases=['prisk'], help='Portfolio risk analysis',
                              epilog='Examples:\n'
                                     '  portfolio-risk                  # full risk report\n'
                                     '  portfolio-risk --json           # JSON for LLM consumption',
                              formatter_class=fmt)

    # portfolio-snapshot (compact JSON for LLM loop)
    sub.add_parser('portfolio-snapshot', aliases=['psnap'], help='Compact portfolio snapshot (JSON)')

    # portfolio-diff (delta since last snapshot)
    sub.add_parser('portfolio-diff', aliases=['pdiff'], help='Portfolio changes since last snapshot (JSON)')

    # stream
    stream_p = sub.add_parser('stream', help='Stream live ticks from Massive.com',
                              epilog='Examples:\n'
                                     '  stream AAPL MSFT AMD\n'
                                     '  stream AAPL --trades\n'
                                     '  stream NVDA --delayed\n'
                                     '  stream --universe sp500\n'
                                     '  stream EURUSD GBPUSD --feed forex\n'
                                     '  stream EURUSD --feed forex --quotes',
                              formatter_class=fmt)
    stream_p.add_argument('symbols', nargs='*', help='Symbols to stream')
    stream_p.add_argument('--universe', '-u', default=None, help='Universe name to stream')
    stream_p.add_argument('--trades', action='store_true', default=False, help='Stream trades instead of aggs')
    stream_p.add_argument('--delayed', action='store_true', default=False, help='Use delayed feed')
    stream_p.add_argument('--feed', default=None,
                           choices=['stocks', 'forex', 'crypto', 'options', 'indices', 'futures'],
                           help='Market feed (default: from config)')
    stream_p.add_argument('--quotes', action='store_true', default=False, help='Stream bid/ask quotes (forex)')

    # universe
    uni_p = sub.add_parser('universe', aliases=['u'], help='Manage universes',
                           epilog='Examples:\n'
                                  '  universe list\n'
                                  '  universe show nasdaq_top25\n'
                                  '  universe create my_watchlist\n'
                                  '  universe add my_watchlist AAPL MSFT AMD NVDA\n'
                                  '  universe remove my_watchlist MSFT\n'
                                  '  universe import my_watchlist symbols.csv\n'
                                  '  universe delete my_watchlist',
                           formatter_class=fmt)
    uni_sub = uni_p.add_subparsers(dest='uni_action')
    uni_sub.add_parser('list', help='List all universes with symbol counts')
    uni_show_p = uni_sub.add_parser('show', help='Show symbols in a universe')
    uni_show_p.add_argument('name', help='Universe name')
    uni_create_p = uni_sub.add_parser('create', help='Create an empty universe')
    uni_create_p.add_argument('name', help='Universe name')
    uni_delete_p = uni_sub.add_parser('delete', help='Delete a universe')
    uni_delete_p.add_argument('name', help='Universe name')
    uni_add_p = uni_sub.add_parser('add', help='Resolve via IB and add symbols')
    uni_add_p.add_argument('name', help='Universe name')
    uni_add_p.add_argument('symbols', nargs='+', help='Symbols to add')
    uni_remove_p = uni_sub.add_parser('remove', help='Remove a symbol from a universe')
    uni_remove_p.add_argument('name', help='Universe name')
    uni_remove_p.add_argument('symbol', help='Symbol to remove')
    uni_import_p = uni_sub.add_parser('import', help='Bulk import from CSV file')
    uni_import_p.add_argument('name', help='Universe name')
    uni_import_p.add_argument('csv_file', help='Path to CSV file')

    # backtest
    bt_p = sub.add_parser('backtest', aliases=['bt'], help='Backtest a strategy (local, no service needed)',
                          epilog='Examples:\n'
                                 '  backtest -s strategies/my_strategy.py --class MyStrategy --conids 4391\n'
                                 '  bt -s strategies/smi.py --class SMI --conids 4391 265598 --days 180\n'
                                 '  bt -s strategies/smi.py --class SMI --universe portfolio',
                          formatter_class=fmt)
    bt_p.add_argument('-s', '--strategy', required=True, help='Path to strategy .py file')
    bt_p.add_argument('--class', dest='class_name', required=True, help='Strategy class name')
    bt_p.add_argument('--conids', type=int, nargs='+', default=None, help='Contract IDs to backtest')
    bt_p.add_argument('--universe', default=None, help='Universe name (resolve conids from universe)')
    bt_p.add_argument('--days', type=int, default=365, help='Days of history (default: 365)')
    bt_p.add_argument('--capital', type=float, default=100000, help='Initial capital (default: 100000)')
    bt_p.add_argument('--bar-size', default='1 min', help='Bar size (default: "1 min")')
    bt_p.add_argument('--slippage-model', default='fixed',
                      choices=['zero', 'fixed', 'sqrt', 'volatility'],
                      help='Slippage model (default: fixed)')
    bt_p.add_argument('--slippage-bps', type=float, default=1.0,
                      help='BPS for fixed model (default: 1.0)')
    # Saved by default so `backtests show` can compute statistical-
    # confidence tests (PSR, t-stat, bootstrap CIs, distribution stats,
    # losing-streak MC). Opt out with --no-save-trades for disk-tight runs.
    bt_p.add_argument('--no-save-trades', dest='save_trades',
                      action='store_false', default=True,
                      help='Skip persisting trade list + equity curve '
                           '(smaller DB; disables statistical-confidence '
                           'tests in `backtests show`)')
    bt_p.add_argument('--note', default='',
                      help='Free-text note attached to the saved run (for later query/compare)')
    bt_p.add_argument('--no-save', action='store_true', default=False,
                      help='Do not persist this run to the backtest history store')
    # --param KEY=VALUE overrides a strategy class attribute before the run.
    # Use --params '{"K":V,...}' for JSON input (easier for sweeps).
    bt_p.add_argument('--param', action='append', default=[], metavar='KEY=VALUE',
                      help='Override a strategy parameter (class attribute). '
                           'Repeatable: --param EMA_PERIOD=15 --param BAND_MULT=1.5')
    bt_p.add_argument('--params', default=None, metavar='JSON',
                      help='JSON object of param overrides '
                           '(e.g. \'{"EMA_PERIOD":15,"BAND_MULT":1.5}\'). '
                           'Merged with --param entries; --param wins on conflict.')
    bt_p.add_argument('--summary-only', action='store_true', default=False,
                      help='JSON output: skip the trades and equity-curve arrays '
                           '(keeps summary + run_id). Trades are still persisted '
                           'to the DB for `backtests show` unless --no-save-trades.')
    bt_p.add_argument('--sweep-id', type=int, default=None,
                      help='Internal: parent sweep id when invoked by '
                           '`mmr sweep run`. Persisted on the backtest row so '
                           '`backtests list --sweep <id>` can reconstruct the '
                           'leaderboard.')

    # sweep — declarative, cron-able nightly sweep from a YAML manifest.
    # Unlike bt-sweep (single strategy, one-shot CLI) this handles many
    # strategies + symbol/param grids, persists a parent `sweeps` row,
    # runs with real subprocess concurrency, and drops a morning digest.
    swp_p = sub.add_parser('sweep', help='Run / list / show declarative sweeps (cron-ready)',
                            epilog='Examples:\n'
                                   '  sweep run nightly.yaml          # execute a manifest\n'
                                   '  sweep run nightly.yaml --dry-run  # expand + estimate, no execute\n'
                                   '  sweep list                      # history of past sweeps\n'
                                   '  sweep show 7                    # leaderboard of sweep #7\n',
                            formatter_class=fmt)
    swp_sub = swp_p.add_subparsers(dest='sweep_action')

    swp_run_p = swp_sub.add_parser('run', help='Execute a sweep manifest')
    swp_run_p.add_argument('manifest', help='Path to sweep manifest YAML')
    swp_run_p.add_argument('--dry-run', action='store_true', default=False,
                            help='Expand grid + estimate wall time, do not execute')
    swp_run_p.add_argument('--concurrency', type=int, default=None,
                            help='Override concurrency across all sweeps in the manifest. '
                                 'Default: each sweep uses its own or auto-tune to cpu_count-1.')
    swp_run_p.add_argument('--skip-freshness', action='store_true', default=False,
                            help='Bypass the stale-data guard (default refuses if any '
                                 'conid lacks a bar from the last 3 trading days)')

    swp_list_p = swp_sub.add_parser('list', help='List past sweeps')
    swp_list_p.add_argument('--limit', type=int, default=25)

    swp_show_p = swp_sub.add_parser('show', help='Show sweep detail + leaderboard')
    swp_show_p.add_argument('sweep_id', type=int)
    swp_show_p.add_argument('--top', type=int, default=10)

    # bt-sweep — cartesian-product parameter sweep. One process, N backtests.
    sw_p = sub.add_parser('bt-sweep',
                           help='Single-strategy parameter sweep (sequential, in-process). For cron-ready multi-strategy runs use `mmr sweep run`.',
                           epilog='Examples:\n'
                                  '  bt-sweep -s strategies/keltner_breakout.py --class KeltnerBreakout\\\n'
                                  '      --conids 756733 --days 180\\\n'
                                  "      --grid '{\"EMA_PERIOD\":[10,20,30],\"BAND_MULT\":[1.5,2.0,2.5]}'\n"
                                  '  bt-sweep -s strategies/vwap_reversion.py --class VwapReversion\\\n'
                                  "      --conids 756733 --days 90 --grid '{\"ENTRY_STD\":[1.0,1.5,2.0]}'",
                           formatter_class=fmt)
    sw_p.add_argument('-s', '--strategy', required=True, help='Path to strategy .py file')
    sw_p.add_argument('--class', dest='class_name', required=True, help='Strategy class name')
    sw_p.add_argument('--conids', type=int, nargs='+', default=None, help='Contract IDs')
    sw_p.add_argument('--universe', default=None, help='Universe name (alternative to --conids)')
    sw_p.add_argument('--days', type=int, default=365, help='Days of history (default: 365)')
    sw_p.add_argument('--capital', type=float, default=100000, help='Initial capital')
    sw_p.add_argument('--bar-size', default='1 min', help='Bar size (default: "1 min")')
    sw_p.add_argument('--grid', required=True, metavar='JSON',
                       help='JSON object mapping param name → list of values. '
                            'Cartesian product is swept.')
    sw_p.add_argument('--top', type=int, default=10,
                       help='Show only the top-N results in the leaderboard (default: 10)')
    sw_p.add_argument('--note', default='', help='Note stamped on every sweep run')
    sw_p.add_argument('--slippage-bps', type=float, default=1.0)
    sw_p.add_argument('--no-save-trades', dest='save_trades',
                       action='store_false', default=True,
                       help='Skip persisting per-run trade + equity detail')

    # backtests (plural) — history of past runs
    bts_p = sub.add_parser('backtests', aliases=['bts'],
                            help='List / show / delete past backtest runs',
                            epilog='Examples:\n'
                                   '  backtests                           # ranked by composite quality score (active only)\n'
                                   '  backtests --all                     # include archived\n'
                                   '  backtests --archived                # only archived runs\n'
                                   '  backtests --sort-by time            # most recent first\n'
                                   '  backtests --sort-by sharpe          # best sharpe first\n'
                                   '  backtests --sort-by return --asc    # worst return first\n'
                                   '  backtests --strategy RSIStrategy\n'
                                   '  backtests show 42                   # full detail for run 42\n'
                                   '  backtests compare 40 41 42          # side-by-side table\n'
                                   '  backtests archive 40 41 42          # hide from default list\n'
                                   '  backtests unarchive 40              # restore a hidden run\n'
                                   '  backtests delete 42                 # permanently remove\n'
                                   '  backtests help                      # metric reference',
                            formatter_class=fmt)
    bts_sub = bts_p.add_subparsers(dest='bts_action')
    _sort_choices = sorted(_BTS_SORT_ALIASES.keys())

    def _add_list_args(p):
        p.add_argument('--strategy', default=None, help='Filter by strategy class name')
        p.add_argument('--sweep', type=int, default=None, dest='sweep_id',
                       help='Filter to runs from a specific sweep (see `mmr sweep list`)')
        p.add_argument('--limit', type=int, default=25, help='Max rows (default 25)')
        p.add_argument('--sort-by', default='score', choices=_sort_choices,
                       help='Sort column (default: score — a composite quality '
                            'ranking; use "time" for most-recent-first)')
        p.add_argument('--asc', action='store_true', default=False,
                       help='Ascending order (default: descending — best first)')
        p.add_argument('--card', action='store_true', default=False,
                       help='Card view — one panel per run with colored metrics')
        # Archived runs are soft-deleted and hidden by default. --all lifts
        # the filter; --archived flips it to archived-only (for review /
        # bulk-unarchive workflows).
        arch_grp = p.add_mutually_exclusive_group()
        arch_grp.add_argument('--all', dest='include_archived',
                               action='store_true', default=False,
                               help='Include archived runs (hidden by default)')
        arch_grp.add_argument('--archived', dest='archived_only',
                               action='store_true', default=False,
                               help='Show only archived runs')

    _add_list_args(bts_p)  # top-level `backtests` shortcut
    bts_list_p = bts_sub.add_parser('list', help='List past runs (default)')
    _add_list_args(bts_list_p)
    bts_show_p = bts_sub.add_parser('show', help='Show full detail for a run')
    bts_show_p.add_argument('run_id', type=int)
    # The raw trades + equity-curve arrays are multi-megabyte on a 1-min
    # 1-year run — they're already on disk in the DB, there's no reason
    # to ship them back on every `show`. Off by default; opt in with
    # --include-raw when you actually need the full series.
    bts_show_p.add_argument('--include-raw', action='store_true', default=False,
                             help='Include trades_json + equity_curve_json in --json output '
                                  '(multi-MB on 1-min × 365d runs; off by default)')

    # backtests confidence — batch read of just the stat-confidence block for N runs.
    # Designed for LLMs post-sweep: one call returns the small decision-
    # quality payload for every run without the trade/equity blobs.
    bts_conf_p = bts_sub.add_parser('confidence',
                                      help='Show statistical-confidence block for one or more runs (compact)')
    bts_conf_p.add_argument('run_ids', type=int, nargs='+',
                             help='One or more run ids')
    bts_cmp_p = bts_sub.add_parser('compare', help='Compare two or more runs side-by-side')
    bts_cmp_p.add_argument('run_ids', type=int, nargs='+')
    bts_arc_p = bts_sub.add_parser('archive',
                                     help='Archive runs — hides from default list, keeps the data')
    bts_arc_p.add_argument('run_ids', type=int, nargs='+')
    bts_unarc_p = bts_sub.add_parser('unarchive',
                                       help='Restore archived runs to the default list')
    bts_unarc_p.add_argument('run_ids', type=int, nargs='+')
    bts_del_p = bts_sub.add_parser('delete', help='Delete a saved run')
    bts_del_p.add_argument('run_id', type=int)
    bts_sub.add_parser('help', aliases=['metrics'],
                        help='Explain every metric in the backtest report')

    # data
    data_p = sub.add_parser('data', help='Local data exploration (no service needed)',
                            epilog='Examples:\n'
                                   '  data summary\n'
                                   '  data query AAPL --bar-size "1 day" --days 30\n'
                                   '  data download AAPL MSFT --bar-size "1 day" --days 365\n'
                                   '  data download AAPL --source twelvedata --days 30',
                            formatter_class=fmt)
    data_sub = data_p.add_subparsers(dest='data_action')

    data_sub.add_parser('summary', help='Show summary of all local historical data')

    data_query_p = data_sub.add_parser('query', help='Query local OHLCV data')
    data_query_p.add_argument('symbol', help='Symbol or conId')
    data_query_p.add_argument('--bar-size', default='1 day', help='Bar size (default: "1 day")')
    data_query_p.add_argument('--days', type=int, default=30, help='Days of history (default: 30)')
    data_query_p.add_argument('--tail', type=int, default=None, help='Show only last N rows')

    data_dl_p = data_sub.add_parser(
        'download',
        help='Download data from Massive.com or TwelveData to local DuckDB'
    )
    data_dl_p.add_argument('symbols', nargs='+', help='Symbols to download')
    data_dl_p.add_argument('--bar-size', default='1 day', help='Bar size (default: "1 day")')
    data_dl_p.add_argument('--days', type=int, default=365, help='Days of history (default: 365)')
    data_dl_p.add_argument(
        '--source',
        choices=['massive', 'twelvedata'],
        default='massive',
        help='Data source (default: massive)',
    )
    data_dl_p.add_argument(
        '--force', '-f',
        action='store_true',
        help=('Bypass the freshness guard and fetch the full requested window. '
              'Useful when toggling TwelveData extended-hours coverage (prepost) — '
              'without --force, stored regular-session bars make the full day look covered.'),
    )

    data_sub.add_parser(
        'migrate-symbols',
        help='Rewrite string-keyed tick_data rows to conId-keyed '
             '(requires trader_service)',
    )

    # help
    sub.add_parser('help', aliases=['h', '?'], help='Show help')

    # exit (REPL only)
    sub.add_parser('exit', aliases=['quit', 'q'], help='Exit REPL')

    return parser


# ------------------------------------------------------------------
# Command dispatch
# ------------------------------------------------------------------

def dispatch(mmr: MMR, args: argparse.Namespace) -> bool:
    """Execute a parsed command.  Returns True to continue the REPL, False to exit."""
    global _json_mode
    _json_mode = getattr(args, 'json', False)
    cmd = args.command

    if cmd in ('exit', 'quit', 'q'):
        return False

    if cmd in ('help', 'h', '?') or cmd is None:
        build_parser().print_help()
        return True

    # Commands that require a live IB upstream connection
    _ib_commands = {
        'portfolio', 'p', 'positions', 'pos', 'orders', 'trades', 'account',
        'buy', 'sell', 'cancel', 'cancel-all', 'close',
        'snapshot', 'snap', 'snapshot-batch', 'depth', 'resolve',
        'listen', 'watch', 'scan',
        'approve',
        'resize-positions',
        'portfolio-risk', 'prisk',
        'portfolio-snapshot', 'psnap',
        'portfolio-diff', 'pdiff',
    }

    if cmd in _ib_commands:
        upstream_err = mmr.check_ib_upstream()
        if upstream_err:
            if _json_mode:
                print(json.dumps({
                    'error': True,
                    'message': f'IB Gateway is not connected to IBKR servers: {upstream_err}',
                    'hint': 'Check IB Gateway via VNC (vnc://localhost:5901) or restart it with: docker compose restart ib-gateway',
                }))
            else:
                console.print(
                    f'[bold red]IB Gateway is not connected to IBKR servers[/bold red]\n'
                    f'[dim]{upstream_err}[/dim]\n\n'
                    f'Check IB Gateway via VNC: [cyan]vnc://localhost:5901[/cyan]\n'
                    f'Restart IB Gateway:       [cyan]docker compose restart ib-gateway[/cyan]'
                )
            return True

    try:
        if cmd in ('portfolio', 'p'):
            df = mmr.portfolio()
            if not df.empty:
                if 'position' in df.columns:
                    df = df[df['position'] != 0].reset_index(drop=True)
                if 'avgCost' in df.columns and 'mktPrice' in df.columns:
                    df['%'] = ((df['mktPrice'] - df['avgCost']) / df['avgCost'] * 100).round(2)
                df.insert(0, '#', range(1, len(df) + 1))
            if getattr(args, 'table', False):
                print_df(df, title='Portfolio')
            else:
                print_portfolio_cards(df, title='Portfolio')

        elif cmd in ('positions', 'pos'):
            df = mmr.positions()
            if not df.empty:
                df.insert(0, '#', range(1, len(df) + 1))
            print_df(df, title='Positions')

        elif cmd == 'orders':
            df = mmr.orders()
            if _json_mode:
                print_df(df, title='Orders')
            elif df.empty:
                console.print('[dim]No open orders[/dim]')
            else:
                import math

                def _safe_int(v, default=0):
                    try:
                        return int(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else default
                    except (ValueError, TypeError):
                        return default

                def _safe_float(v):
                    try:
                        if v is not None and not (isinstance(v, float) and math.isnan(v)):
                            return float(v)
                    except (ValueError, TypeError):
                        pass
                    return None

                for _, row in df.iterrows():
                    oid = _safe_int(row.get('orderId'))
                    sym = row.get('symbol', '?')
                    name = row.get('name', '')
                    action = row.get('action', '?')
                    otype = row.get('orderType', '?')
                    qty = _safe_float(row.get('quantity')) or 0
                    status = row.get('status', '?')

                    # Build price string based on order type
                    price_parts = []
                    lmt = _safe_float(row.get('lmtPrice'))
                    aux = _safe_float(row.get('auxPrice'))
                    if lmt:
                        price_parts.append(f'limit ${lmt:.2f}')
                    if aux:
                        price_parts.append(f'stop ${aux:.2f}')
                    price_str = ', '.join(price_parts) if price_parts else 'MARKET'

                    # Order value and account %
                    order_val = _safe_float(row.get('orderValue'))
                    acct_pct = _safe_float(row.get('acctPct'))
                    value_str = ''
                    if order_val:
                        value_str = f'  [dim]${order_val:,.0f}'
                        if acct_pct:
                            value_str += f' ({acct_pct:.1f}% of account)'
                        value_str += '[/dim]'

                    # Market context — show bid/ask and distance from order price
                    bid = _safe_float(row.get('bid'))
                    ask = _safe_float(row.get('ask'))
                    last = _safe_float(row.get('last'))
                    market_str = ''
                    if bid or ask:
                        parts = []
                        if bid:
                            parts.append(f'bid ${bid:.2f}')
                        if ask:
                            parts.append(f'ask ${ask:.2f}')
                        market_str = f'  [dim]mkt: {" / ".join(parts)}[/dim]'

                        # Show why a limit order isn't filling
                        order_price = lmt or aux
                        if order_price and status in ('Submitted', 'PreSubmitted'):
                            if action == 'BUY' and ask:
                                gap = ask - order_price
                                if gap > 0:
                                    market_str += f'  [yellow]${gap:.2f} below ask[/yellow]'
                                else:
                                    market_str += f'  [green]at/above ask[/green]'
                            elif action == 'SELL' and bid:
                                if otype == 'STP':
                                    gap = bid - order_price
                                    if gap > 0:
                                        market_str += f'  [dim]stop ${gap:.2f} below bid[/dim]'
                                elif lmt:
                                    gap = order_price - bid
                                    if gap > 0:
                                        market_str += f'  [yellow]${gap:.2f} above bid[/yellow]'
                                    else:
                                        market_str += f'  [green]at/below bid[/green]'

                    # Color the action
                    action_color = 'green' if action == 'BUY' else 'red'

                    # Status color
                    if status in ('Submitted', 'PreSubmitted'):
                        status_style = 'yellow'
                    elif status == 'Filled':
                        status_style = 'green'
                    elif status in ('Cancelled', 'Inactive'):
                        status_style = 'red'
                    else:
                        status_style = 'dim'

                    # Parent/child relationship
                    parent_str = ''
                    parent_id = _safe_int(row.get('parentId'))
                    if parent_id:
                        parent_str = f'  [dim]parent order #{parent_id}[/dim]'

                    # Fill info
                    fill_str = ''
                    filled = _safe_float(row.get('filled')) or 0
                    if filled > 0:
                        avg = _safe_float(row.get('avgFillPrice'))
                        fill_str = f'  [dim]filled {filled:.0f}/{qty:.0f}' + (f' @ ${avg:.2f}' if avg else '') + '[/dim]'

                    # Company name line
                    name_str = f'  [dim]{name}[/dim]' if name else ''

                    console.print(
                        f'  [bold]#{oid}[/bold] [{action_color}]{action}[/{action_color}] '
                        f'[bold]{sym}[/bold] x{qty:.0f} {otype} ({price_str}) '
                        f'[{status_style}]{status}[/{status_style}] '
                        f'[dim]{row.get("tif", "")}[/dim]'
                        f'{value_str}{market_str}{parent_str}{fill_str}{name_str}'
                    )

        elif cmd == 'trades':
            print_df(mmr.trades(), title='Trades')

        elif cmd in ('snapshot', 'snap'):
            result = mmr.snapshot(args.symbol, delayed=args.delayed,
                                  exchange=args.exchange, currency=args.currency)
            print_dict(result, title=f'Snapshot: {args.symbol}')

        elif cmd == 'snapshot-batch':
            results = mmr.snapshot_batch(args.symbols, exchange=args.exchange,
                                          currency=args.currency)
            print(json.dumps({"data": results, "title": "Snapshots"}, default=str))

        elif cmd == 'depth':
            _handle_depth(mmr, args)

        elif cmd == 'resolve':
            defs = mmr.resolve(args.symbol, sec_type=args.sectype,
                               exchange=args.exchange, currency=args.currency)
            if defs:
                import pandas as pd
                rows = [{
                    'conId': d.conId,
                    'symbol': d.symbol,
                    'secType': d.secType,
                    'exchange': d.exchange,
                    'primaryExchange': d.primaryExchange,
                    'currency': d.currency,
                    'longName': d.longName,
                } for d in defs]
                print_df(pd.DataFrame(rows), title=f'Resolve: {args.symbol}')
            else:
                console.print(f'[yellow]Could not resolve: {args.symbol}[/yellow]')

        elif cmd == 'buy':
            result = mmr.buy(
                args.symbol,
                amount=args.amount,
                quantity=args.quantity,
                limit_price=args.limit,
                market=args.market,
                sec_type=args.sectype,
                exchange=args.exchange,
                currency=args.currency,
            )
            _print_trade_result(result, 'BUY', args.symbol)

        elif cmd == 'sell':
            result = mmr.sell(
                args.symbol,
                amount=args.amount,
                quantity=args.quantity,
                limit_price=args.limit,
                market=args.market,
                sec_type=args.sectype,
                exchange=args.exchange,
                currency=args.currency,
            )
            _print_trade_result(result, 'SELL', args.symbol)

        elif cmd == 'cancel':
            result = mmr.cancel(args.order_id)
            if result.is_success():
                print_status(f'Cancelled order {args.order_id}')
            else:
                print_status(f'Cancel failed: {result.error}', success=False)

        elif cmd == 'cancel-all':
            result = mmr.cancel_all()
            if result.is_success():
                print_status('All orders cancelled')
            else:
                print_status(f'Cancel-all failed: {result.error}', success=False)

        elif cmd == 'to-market':
            result = mmr.to_market(args.order_id)
            if result.is_success():
                print_status(f'Order #{args.order_id} converted to market')
            else:
                print_status(f'to-market failed: {result.error}', success=False)

        elif cmd in ('close', 'c'):
            _handle_close(mmr, args)

        elif cmd == 'close-all-positions':
            _handle_close_all(mmr)

        elif cmd in ('resize-positions', 'resize'):
            _handle_resize_positions(mmr, args)

        elif cmd in ('strategies', 'strat'):
            _handle_strategies(mmr, args)

        elif cmd == 'listen':
            _handle_listen(mmr, args)

        elif cmd in ('watch', 'w'):
            _handle_watch(mmr)

        elif cmd == 'account':
            console.print(f'Account: {mmr.account()}')

        elif cmd == 'status':
            if _json_mode:
                import logging as _logging
                _logging.disable(_logging.CRITICAL)
            result = mmr.status()
            if _json_mode:
                import logging as _logging
                _logging.disable(_logging.NOTSET)
            print_dict(result, title='Status')
            if not _json_mode and not result.get('ib_upstream_connected', True):
                console.print(
                    f'\n[bold red]IB Gateway is not connected to IBKR servers[/bold red]\n'
                    f'[dim]{result.get("ib_upstream_error", "")}[/dim]\n'
                    f'Check VNC: [cyan]vnc://localhost:5901[/cyan]  |  '
                    f'Restart: [cyan]docker compose restart ib-gateway[/cyan]'
                )

        elif cmd in ('market-hours', 'mh'):
            rows = mmr.market_hours()
            if _json_mode:
                print_json_result(rows, title='Market Hours')
            else:
                table = Table(title='Market Hours')
                table.add_column('Exchange', no_wrap=True)
                table.add_column('Region', no_wrap=True)
                table.add_column('Status', no_wrap=True)
                table.add_column('Next Event', no_wrap=True)
                table.add_column('Time (UTC)', no_wrap=True)
                table.add_column('Relative', no_wrap=True)
                for r in rows:
                    status_str = (f'[green]{r["status"]}[/green]'
                                  if r['status'] == 'OPEN'
                                  else f'[red]{r["status"]}[/red]')
                    table.add_row(
                        r['exchange'], r['region'], status_str,
                        r['next_event'], r['next_event_time'], r['relative'],
                    )
                console.print(table)

        elif cmd == 'logs':
            _handle_logs(args)

        elif cmd == 'risk':
            _handle_risk(mmr, args)

        elif cmd == 'filter':
            _handle_filter(mmr, args)

        elif cmd == 'session':
            _handle_session(mmr, args)

        elif cmd == 'history':
            _handle_history(mmr, args)

        elif cmd in ('financials', 'fin'):
            _handle_financials(mmr, args)

        elif cmd == 'news':
            _handle_news(mmr, args)

        elif cmd in ('options', 'opt'):
            _handle_options(mmr, args)

        elif cmd in ('forex', 'fx'):
            _handle_forex(mmr, args)

        elif cmd == 'movers':
            _handle_movers(mmr, args)

        elif cmd == 'scan':
            _handle_scan(mmr, args)

        elif cmd in ('ideas', 'scan-ideas'):
            _handle_ideas(mmr, args)

        elif cmd == 'propose':
            _handle_propose(mmr, args)

        elif cmd == 'proposals':
            _handle_proposals(mmr, args)

        elif cmd == 'approve':
            _handle_approve(mmr, args)

        elif cmd == 'reject':
            _handle_reject(mmr, args)

        elif cmd == 'group':
            _handle_group(mmr, args)

        elif cmd in ('portfolio-risk', 'prisk'):
            _handle_portfolio_risk(mmr, args)

        elif cmd in ('portfolio-snapshot', 'psnap'):
            print_dict(mmr.portfolio_snapshot(), title='Portfolio Snapshot')

        elif cmd in ('portfolio-diff', 'pdiff'):
            print_dict(mmr.portfolio_diff(), title='Portfolio Diff')

        elif cmd == 'stream':
            _handle_stream(args)

        elif cmd in ('universe', 'u'):
            _handle_universe(mmr, args)

        elif cmd in ('backtest', 'bt'):
            _handle_backtest(args)

        elif cmd == 'bt-sweep':
            _handle_backtest_sweep(args)

        elif cmd == 'sweep':
            _handle_sweep(args)

        elif cmd in ('backtests', 'bts'):
            _handle_backtests(args)

        elif cmd == 'data':
            _handle_data(args)

        else:
            console.print(f'[yellow]Unknown command: {cmd}[/yellow]')

    except ConnectionError as e:
        print_status(f'Connection error: {e}', success=False)
    except ValueError as e:
        print_status(f'Error: {e}', success=False)
    except Exception as e:
        print_status(f'{type(e).__name__}: {e}', success=False)

    return True


def _print_trade_result(result, action: str, symbol: str):
    if _json_mode:
        if result.is_success():
            data = {
                'success': True,
                'action': action,
                'symbol': symbol,
                'orderId': getattr(result.obj, 'orderId', getattr(getattr(result.obj, 'order', None), 'orderId', '')) if result.obj else '',
                'status': getattr(getattr(result.obj, 'orderStatus', None), 'status', '') if result.obj else '',
            }
        else:
            data = {
                'success': False,
                'action': action,
                'symbol': symbol,
                'error': str(result.error) if result.error else '',
            }
        print(json.dumps(data, default=str))
        return

    if result.is_success():
        console.print(f'[green]{action} {symbol}: success[/green]')
        if result.obj:
            print_dict({
                'orderId': getattr(result.obj, 'orderId', getattr(getattr(result.obj, 'order', None), 'orderId', '')),
                'status': getattr(getattr(result.obj, 'orderStatus', None), 'status', ''),
            })
    else:
        console.print(f'[red]{action} {symbol}: failed[/red]')
        if result.error:
            console.print(f'[red]  {result.error}[/red]')
        if result.exception:
            console.print(f'[red]  {result.exception}[/red]')


def _handle_propose(mmr: MMR, args: argparse.Namespace):
    from trader.trading.proposal import ExecutionSpec

    # Determine order type
    order_type = 'LIMIT' if args.limit is not None else 'MARKET'

    # Determine exit type
    exit_type = 'NONE'
    take_profit_price = None
    stop_loss_price = None
    trailing_stop_percent = None
    trailing_stop_amount = None

    if args.bracket:
        exit_type = 'BRACKET'
        take_profit_price = args.bracket[0]
        stop_loss_price = args.bracket[1]
    elif args.trailing_stop_pct is not None:
        exit_type = 'TRAILING_STOP'
        trailing_stop_percent = args.trailing_stop_pct
    elif args.trailing_stop_amt is not None:
        exit_type = 'TRAILING_STOP'
        trailing_stop_amount = args.trailing_stop_amt
    elif args.stop_loss is not None:
        exit_type = 'STOP_LOSS'
        stop_loss_price = args.stop_loss

    spec = ExecutionSpec(
        order_type=order_type,
        limit_price=args.limit,
        exit_type=exit_type,
        take_profit_price=take_profit_price,
        stop_loss_price=stop_loss_price,
        trailing_stop_percent=trailing_stop_percent,
        trailing_stop_amount=trailing_stop_amount,
        tif=args.tif,
        outside_rth=not args.no_outside_rth,
    )

    errors = spec.validate()
    if errors:
        for e in errors:
            print_status(e, success=False)
        return

    proposal_id, leverage_info, snapshot_info = mmr.propose(
        symbol=args.symbol,
        action=args.action,
        quantity=args.quantity,
        amount=args.amount,
        execution=spec,
        reasoning=args.reasoning,
        confidence=args.confidence,
        thesis=args.thesis,
        source=args.source,
        sec_type=args.sectype,
        exchange=args.exchange,
        currency=args.currency,
        group=getattr(args, 'group', ''),
    )

    if _json_mode:
        # Fetch the stored proposal to get sizing_result from metadata
        detail = mmr.proposal_detail(proposal_id)
        sizing = (detail.get('metadata') or {}).get('sizing_result') if detail else None
        result = {
            'proposal_id': proposal_id,
            'symbol': args.symbol,
            'action': args.action,
            'amount': detail.get('amount') if detail else args.amount,
            'quantity': detail.get('quantity') if detail else args.quantity,
            'confidence': args.confidence,
            'group': getattr(args, 'group', ''),
            'order_type': order_type,
            'exit_type': exit_type,
            'status': 'PENDING',
            'sizing_result': sizing,
            'snapshot': snapshot_info,
            'leverage': leverage_info,
        }
        print(json.dumps({"data": result, "title": f"Proposal #{proposal_id}"}, default=str))
        return

    summary = f'{args.action} {args.symbol}'
    if args.quantity:
        summary += f' x{args.quantity}'
    elif args.amount:
        summary += f' ${args.amount}'
    summary += f' ({order_type}'
    if args.limit:
        summary += f' @{args.limit}'
    summary += ')'
    if exit_type != 'NONE':
        summary += f' [{exit_type}]'

    print_status(f'Proposal #{proposal_id} created (PENDING): {summary}')

    if snapshot_info:
        bid = snapshot_info.get('bid')
        ask = snapshot_info.get('ask')
        last = snapshot_info.get('last')
        parts = []
        if bid is not None and bid == bid:
            parts.append(f'bid {bid:g}')
        if ask is not None and ask == ask:
            parts.append(f'ask {ask:g}')
        if last is not None and last == last:
            parts.append(f'last {last:g}')
        if parts:
            console.print(f'[dim]  Snapshot: {" / ".join(parts)}[/dim]')

    if leverage_info:
        current = leverage_info.get('current_leverage', 0)
        estimated = leverage_info.get('estimated_leverage', 0)
        buying_power = leverage_info.get('buying_power', 0)
        if leverage_info.get('uses_margin'):
            console.print(
                f'[yellow]  MARGIN WARNING: Estimated leverage {estimated:.2f}x '
                f'(current: {current:.2f}x)[/yellow]'
            )
        else:
            console.print(f'[dim]  Leverage: {estimated:.2f}x (current: {current:.2f}x)[/dim]')
        console.print(f'[dim]  Buying power: ${buying_power:,.2f}[/dim]')


def _handle_proposals(mmr: MMR, args: argparse.Namespace):
    action = getattr(args, 'proposals_action', None)

    if action == 'show':
        detail = mmr.proposal_detail(args.proposal_id)
        if detail:
            print_dict(detail, title=f'Proposal #{args.proposal_id}')
            # Show leverage estimate if present (skip in JSON mode — already in metadata)
            if not _json_mode:
                leverage = (detail.get('metadata') or {}).get('leverage_estimate')
                if leverage:
                    current = leverage.get('current_leverage', 0)
                    estimated = leverage.get('estimated_leverage', 0)
                    net_liq = leverage.get('net_liquidation', 0)
                    buying_power = leverage.get('buying_power', 0)
                    uses_margin = leverage.get('uses_margin', False)
                    margin_label = ' [yellow]USES MARGIN[/yellow]' if uses_margin else ''
                    console.print(f'\n[bold]Leverage Estimate:[/bold]')
                    console.print(f'  Current:     {current:.2f}x')
                    console.print(f'  After trade: {estimated:.2f}x{margin_label}')
                    console.print(f'  Net Liq:     ${net_liq:,.2f}')
                    console.print(f'  Buying Power: ${buying_power:,.2f}')
        else:
            print_status(f'Proposal #{args.proposal_id} not found', success=False)
        return

    # List proposals
    status_filter = None
    if not args.all:
        status_filter = args.status or 'PENDING'

    from trader.data.proposal_store import ProposalStore
    store = mmr._proposal_store()
    props = store.query(status=status_filter, limit=args.limit)

    if _json_mode:
        df = mmr.proposals(status=status_filter, limit=args.limit)
        title = 'Proposals'
        if status_filter:
            title += f' ({status_filter})'
        print_df(df, title=title)
        return

    if not props:
        console.print('[dim]No proposals[/dim]')
        return

    title = 'Proposals'
    if status_filter:
        title += f' ({status_filter})'
    console.print(f'[bold]{title}[/bold]\n')

    for p in props:
        # Size
        if p.quantity is not None and p.quantity == p.quantity:
            size = f'{p.quantity:g} shares'
        elif p.amount is not None and p.amount == p.amount:
            size = f'${p.amount:,.0f}'
        else:
            size = '?'

        # Order type
        order_desc = 'MARKET' if p.execution.order_type == 'MARKET' else f'LIMIT @{p.execution.limit_price:g}'

        # Exit
        exit_abbrev = {'NONE': '', 'BRACKET': 'bracket', 'TRAILING_STOP': 'trailing stop', 'STOP_LOSS': 'stop loss'}
        exit_desc = exit_abbrev.get(p.execution.exit_type, p.execution.exit_type)

        # Status color
        status_colors = {'PENDING': 'yellow', 'EXECUTED': 'green', 'APPROVED': 'blue', 'REJECTED': 'red', 'FAILED': 'red'}
        sc = status_colors.get(p.status, 'white')

        # Header line
        console.print(f'  [bold]#{p.id}[/bold]  [bold]{p.action} {p.symbol}[/bold]  {size}  {order_desc}', end='')
        if exit_desc:
            console.print(f'  +{exit_desc}', end='')
        if status_filter is None:
            console.print(f'  [{sc}]{p.status}[/{sc}]', end='')
        console.print()

        # Detail lines
        details = []
        if p.reasoning:
            details.append(f'[dim]{p.reasoning}[/dim]')
        if p.confidence:
            details.append(f'[dim]conf: {p.confidence:.0%}[/dim]')

        # Bid/ask snapshot at proposal time
        snapshot = p.metadata.get('snapshot')
        if snapshot:
            bid = snapshot.get('bid')
            ask = snapshot.get('ask')
            last = snapshot.get('last')
            parts = []
            if bid is not None and bid == bid:  # not NaN
                parts.append(f'bid {bid:g}')
            if ask is not None and ask == ask:
                parts.append(f'ask {ask:g}')
            if last is not None and last == last:
                parts.append(f'last {last:g}')
            if parts:
                details.append(f'[dim]{" / ".join(parts)}[/dim]')

        leverage = p.metadata.get('leverage_estimate')
        if leverage and leverage.get('uses_margin'):
            details.append(f"[yellow]margin: {leverage['estimated_leverage']:.1f}x[/yellow]")
        created = p.created_at.strftime('%m/%d %H:%M') if p.created_at else ''
        if created:
            details.append(f'[dim]{created}[/dim]')
        if details:
            console.print(f'      {" | ".join(details)}')
        console.print()


def _handle_approve(mmr: MMR, args: argparse.Namespace):
    if args.all:
        pending = mmr._proposal_store().query(status='PENDING')
        if not pending:
            print_status('No pending proposals to approve')
            return
        console.print(f'Approving {len(pending)} pending proposal(s)...\n')
        for p in pending:
            _approve_one(mmr, p.id)
        return

    if args.proposal_id is None:
        print_status('Specify a proposal ID or use --all', success=False)
        return

    _approve_one(mmr, args.proposal_id)


def _approve_one(mmr: MMR, proposal_id: int):
    result = mmr.approve(proposal_id)
    if result.is_success():
        order_ids = result.obj if result.obj else []
        print_status(f'Proposal #{proposal_id} approved and executed. Order IDs: {order_ids}')
    else:
        error = str(result.error or result.exception or 'Unknown error')
        if 'leverage' in error.lower() or 'margin' in error.lower() or 'risk gate' in error.lower():
            print_status(f'Proposal #{proposal_id} rejected by risk gate: {error}', success=False)
        else:
            print_status(f'Approve #{proposal_id} failed: {error}', success=False)


def _handle_reject(mmr: MMR, args: argparse.Namespace):
    if args.all:
        pending = mmr._proposal_store().query(status='PENDING')
        if not pending:
            print_status('No pending proposals to reject')
            return
        for p in pending:
            mmr.reject(p.id, reason=args.reason)
            print_status(f'Proposal #{p.id} rejected')
        return

    if args.proposal_id is None:
        print_status('Specify a proposal ID or use --all', success=False)
        return

    success = mmr.reject(args.proposal_id, reason=args.reason)
    if success:
        print_status(f'Proposal #{args.proposal_id} rejected')
    else:
        print_status(f'Reject failed: proposal #{args.proposal_id} not found or not PENDING', success=False)


def _handle_group(mmr: MMR, args: argparse.Namespace):
    action = getattr(args, 'group_action', None)

    try:
        gs = mmr._group_store()
    except Exception as e:
        print_status(f'Could not initialize group store: {e}', success=False)
        return

    if action == 'create':
        budget = args.budget / 100.0 if args.budget > 1 else args.budget
        try:
            gs.create_group(args.name, description=args.description, max_allocation_pct=budget)
            if budget >= 0.01:
                label = f' (budget {budget:.0%})'
            elif budget > 0:
                label = f' (budget {budget:.1%})'
            else:
                label = ''
            print_status(f'Group "{args.name}" created{label}')
        except ValueError as e:
            print_status(str(e), success=False)

    elif action == 'delete':
        if gs.delete_group(args.name):
            print_status(f'Group "{args.name}" deleted')
        else:
            print_status(f'Group "{args.name}" not found', success=False)

    elif action == 'show':
        group = gs.get_group(args.name)
        if not group:
            print_status(f'Group "{args.name}" not found', success=False)
            return
        if _json_mode:
            from dataclasses import asdict
            print_dict(asdict(group), title=f'Group: {args.name}')
            return
        console.print(f'[bold]Group: {group.name}[/bold]')
        if group.description:
            console.print(f'  Description: {group.description}')
        if group.max_allocation_pct > 0:
            console.print(f'  Budget: {group.max_allocation_pct:.0%}')
        if group.members:
            console.print(f'  Members ({len(group.members)}): {", ".join(group.members)}')
        else:
            console.print('  [dim]No members[/dim]')

    elif action == 'add':
        added = []
        for sym in args.symbols:
            if gs.add_member(args.name, sym.upper()):
                added.append(sym.upper())
        if added:
            print_status(f'Added {", ".join(added)} to "{args.name}"')
        else:
            print_status('No symbols added (group not found or already members)', success=False)

    elif action == 'remove':
        if gs.remove_member(args.name, args.symbol.upper()):
            print_status(f'Removed {args.symbol.upper()} from "{args.name}"')
        else:
            print_status(f'{args.symbol.upper()} not in group "{args.name}"', success=False)

    elif action == 'set':
        budget = None
        if args.budget is not None:
            budget = args.budget / 100.0 if args.budget > 1 else args.budget
        if gs.update_group(args.name, description=args.description, max_allocation_pct=budget):
            print_status(f'Group "{args.name}" updated')
        else:
            print_status(f'Group "{args.name}" not found', success=False)

    else:
        # Default: list groups
        groups = gs.list_groups()
        if _json_mode:
            from dataclasses import asdict
            data = [asdict(g) for g in groups]
            print_dict({'groups': data}, title='Position Groups')
            return
        if not groups:
            console.print('[dim]No position groups[/dim]')
            return
        console.print('[bold]Position Groups[/bold]\n')
        for g in groups:
            budget_label = f' (budget {g.max_allocation_pct:.0%})' if g.max_allocation_pct > 0 else ''
            console.print(
                f'  [bold]{g.name}[/bold]{budget_label}  '
                f'{len(g.members)} members: {", ".join(g.members) if g.members else "-"}'
            )


def _handle_portfolio_risk(mmr: MMR, args: argparse.Namespace):
    try:
        report = mmr.risk_report()
    except Exception as e:
        print_status(f'Risk report failed: {e}', success=False)
        return

    if _json_mode:
        print_dict(report, title='Portfolio Risk Report')
        return

    console.print('[bold]Portfolio Risk Report[/bold]\n')

    # Summary
    console.print(f'  {report.get("summary", "")}')
    console.print()

    # Top positions
    top = report.get('top_positions', [])
    if top:
        console.print('[bold]Top Positions:[/bold]')
        for p in top[:5]:
            console.print(f'  {p["symbol"]:8s}  {p["pct"]:.1%}  ${p["value"]:,.0f}')
        console.print()

    # Concentration
    hhi = report.get('hhi', 0)
    console.print(f'[bold]Concentration:[/bold] HHI = {hhi:.3f}')
    exposure = report.get('gross_exposure_pct', 0)
    console.print(f'[bold]Gross Exposure:[/bold] {exposure:.1%}')
    console.print()

    # Group allocations
    groups = report.get('group_allocations', [])
    if groups:
        console.print('[bold]Group Allocations:[/bold]')
        for g in groups:
            over = ' [red]OVER BUDGET[/red]' if g['over_budget'] else ''
            budget = f' / {g["budget_pct"]:.0%} budget' if g['budget_pct'] > 0 else ''
            console.print(
                f'  {g["name"]:15s}  {g["pct"]:.1%}  ${g["value"]:,.0f}{budget}{over}'
            )
        console.print()

    # Correlation clusters
    clusters = report.get('correlation_clusters', [])
    if clusters:
        console.print('[bold]Correlated Clusters:[/bold]')
        for c in clusters:
            console.print(
                f'  {", ".join(c["symbols"])}  '
                f'avg corr={c["avg_corr"]:.2f}  weight={c["combined_weight_pct"]:.1%}'
            )
        console.print()

    # Warnings
    warnings = report.get('warnings', [])
    if warnings:
        console.print('[bold]Warnings:[/bold]')
        for w in warnings:
            color = 'red' if w['level'] == 'critical' else 'yellow'
            console.print(f'  [{color}]{w["level"].upper()}: {w["message"]}[/{color}]')


def _handle_close(mmr: MMR, args: argparse.Namespace):
    row = args.row
    if row not in mmr._position_map:
        console.print(f'[yellow]Row #{row} not found. Run "portfolio" or "positions" first.[/yellow]')
        return

    symbol = mmr._position_map[row]
    # Get current position size for confirmation — try portfolio first, fall back to positions
    pos_size = None
    df = mmr.portfolio()
    if not df.empty and 'symbol' in df.columns:
        match = df[df['symbol'] == symbol]
        if not match.empty:
            pos_size = float(match.iloc[0]['position'])

    if pos_size is None:
        df = mmr.positions()
        if not df.empty and 'symbol' in df.columns:
            match = df[df['symbol'] == symbol]
            if not match.empty:
                pos_size = float(match.iloc[0]['position'])

    if pos_size is None or pos_size == 0.0:
        console.print(f'[yellow]{symbol}: position is already flat (0 shares). Nothing to close.[/yellow]')
        return

    con_id = None
    if not df.empty and 'conId' in df.columns:
        match = df[df['symbol'] == symbol]
        if not match.empty:
            con_id = int(match.iloc[0]['conId'])

    close_qty = args.quantity if args.quantity else abs(pos_size)

    confirm = input(f'Close #{row}: {symbol} ({close_qty} shares) at market? [y/N] ')
    if confirm.lower() != 'y':
        console.print('[dim]Cancelled[/dim]')
        return

    result = mmr.close_position(symbol, quantity=args.quantity, con_id=con_id,
                                _pos_size=pos_size)
    if result.is_success():
        console.print(f'[green]Close {symbol}: submitted[/green]')
    else:
        console.print(f'[red]Close {symbol}: failed — {result.error}[/red]')


def _handle_close_all(mmr: MMR):
    df = mmr.portfolio()
    if df.empty:
        console.print('[dim]No positions to close.[/dim]')
        return

    # Filter to non-zero positions
    df = df[df['position'] != 0].reset_index(drop=True)
    if df.empty:
        console.print('[dim]All positions are already flat.[/dim]')
        return

    console.print(f'[bold]Positions to close ({len(df)}):[/bold]')
    for _, row in df.iterrows():
        pos = row['position']
        direction = 'LONG' if pos > 0 else 'SHORT'
        console.print(f'  {row["symbol"]}: {abs(pos):.0f} shares ({direction})')

    confirm = input(f'\nClose all {len(df)} positions at market? (will cancel all open orders first) [y/N] ')
    if confirm.lower() != 'y':
        console.print('[dim]Cancelled[/dim]')
        return

    # Cancel all open orders first (stops, trailing stops, etc.)
    result = mmr.cancel_all()
    if result.is_success():
        console.print('[green]All open orders cancelled[/green]')
    else:
        console.print(f'[yellow]Warning: cancel-all failed: {result.error}[/yellow]')

    # Close each position — skip_risk_gate because this is a liquidation command.
    # Pass _pos_size to avoid re-fetching portfolio for each position.
    for _, row in df.iterrows():
        symbol = row['symbol']
        con_id = int(row['conId']) if row.get('conId') else None
        pos_size = float(row['position'])
        result = mmr.close_position(
            symbol, con_id=con_id, skip_risk_gate=True, _pos_size=pos_size
        )
        if result.is_success():
            console.print(f'[green]Close {symbol}: submitted[/green]')
        else:
            console.print(f'[red]Close {symbol}: failed — {result.error}[/red]')


def _handle_resize_positions(mmr: MMR, args: argparse.Namespace):
    max_bound = args.max_bound
    min_bound = args.min_bound
    dry_run = args.dry_run

    if max_bound is None and min_bound is None:
        console.print('[yellow]Specify --max-bound and/or --min-bound[/yellow]')
        return

    if max_bound is not None and min_bound is not None and min_bound > max_bound:
        console.print('[yellow]--min-bound cannot be greater than --max-bound[/yellow]')
        return

    plan = mmr.compute_resize_plan(max_bound=max_bound, min_bound=min_bound)

    if not plan['adjustments']:
        console.print('[dim]Portfolio already within bounds. No adjustments needed.[/dim]')
        return

    # Display plan
    current = plan['current_total']
    target = plan['target_total']
    factor = plan['scale_factor']
    console.print(
        f'\n[bold]Resize Plan:[/bold] ${current:,.0f} → ${target:,.0f} (scale: {factor:.2f}x)\n'
    )

    from rich.table import Table
    table = Table(show_header=True, header_style='bold')
    table.add_column('Symbol')
    table.add_column('Curr Qty', justify='right')
    table.add_column('Target Qty', justify='right')
    table.add_column('Delta', justify='right')
    table.add_column('Action')
    table.add_column('Curr Value', justify='right')
    table.add_column('Target Value', justify='right')

    for adj in plan['adjustments']:
        delta_str = f'{adj["delta_qty"]:+d}'
        action_style = 'green' if adj['action'] == 'BUY' else 'red'
        table.add_row(
            adj['symbol'],
            str(int(adj['current_qty'])),
            str(int(adj['target_qty'])),
            delta_str,
            f'[{action_style}]{adj["action"]}[/{action_style}]',
            f'${adj["current_value"]:,.0f}',
            f'${adj["target_value"]:,.0f}',
        )
    console.print(table)

    # Show associated orders that will be adjusted
    has_orders = any(adj.get('associated_orders') for adj in plan['adjustments'])
    if has_orders:
        console.print('\n[bold]Associated orders to adjust:[/bold]')
        for adj in plan['adjustments']:
            for o in adj.get('associated_orders', []):
                old_qty = int(o['quantity'])
                new_qty = abs(int(adj['target_qty']))
                if o['orderType'] == 'TRAIL' and o['trailingPercent']:
                    price_str = f'{o["trailingPercent"]}%'
                elif o['orderType'] in ('STP', 'STP LMT'):
                    price_str = f'@ ${o["auxPrice"]:,.2f}'
                elif o['orderType'] == 'LMT':
                    price_str = f'@ ${o["lmtPrice"]:,.2f}'
                else:
                    price_str = ''
                console.print(
                    f'  {adj["symbol"]}: {o["orderType"]} {o["action"]} {old_qty} {price_str}'
                    f' → {o["orderType"]} {o["action"]} {new_qty} {price_str}'
                )

    if dry_run:
        console.print('\n[dim]Dry run — no orders placed.[/dim]')
        return

    confirm = input('\nProceed? [y/N] ')
    if confirm.lower() != 'y':
        console.print('[dim]Cancelled[/dim]')
        return

    results = mmr.execute_resize_plan(plan)

    for msg in results['successes']:
        console.print(f'[green]{msg}[/green]')
    for msg in results['failures']:
        console.print(f'[red]{msg}[/red]')
    for msg in results['warnings']:
        console.print(f'[yellow]{msg}[/yellow]')

    total_ok = len(results['successes'])
    total_fail = len(results['failures'])
    console.print(f'\n[bold]Done:[/bold] {total_ok} succeeded, {total_fail} failed')


def _handle_logs(args: argparse.Namespace):
    """Read and display service logs from disk. No service connection needed."""
    import datetime as dt
    import glob
    import os
    import re

    log_dir = os.path.expanduser('~/.local/share/mmr/logs')
    if not os.path.isdir(log_dir):
        console.print('[yellow]No log directory found[/yellow]')
        return

    level_priority = {'DEBUG': 10, 'INFO': 20, 'WARNING': 30, 'ERROR': 40, 'CRITICAL': 50}
    min_level = level_priority.get(args.level, 30)

    # Map service filter to log file prefix
    service_prefixes = {
        'trader': 'trader_',
        'strategy': 'strategy_',
        'data': 'data_service_',
    }

    # Find log files
    pattern = os.path.join(log_dir, '*.log')
    all_files = glob.glob(pattern)

    if args.service:
        prefix = service_prefixes.get(args.service, args.service)
        all_files = [f for f in all_files if os.path.basename(f).startswith(prefix)]

    if args.today:
        today_str = dt.date.today().strftime('%Y-%m-%d')
        all_files = [f for f in all_files if today_str in os.path.basename(f)]

    if not all_files:
        console.print('[yellow]No log files found matching filters[/yellow]')
        return

    # Skip empty files, sort by modification time (newest first)
    all_files = [f for f in all_files if os.path.getsize(f) > 0]
    all_files.sort(key=os.path.getmtime, reverse=True)
    # Read from the most recent non-empty files (limit to avoid reading thousands)
    files_to_read = all_files[:20]

    # Parse log lines: "2026-03-12 14:43:46,123::module::LEVEL::message"
    log_line_re = re.compile(r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),?\d*::(.+?)::(\w+)::(.*)$')
    entries = []

    for filepath in files_to_read:
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                current_entry = None
                for line in f:
                    m = log_line_re.match(line)
                    if m:
                        # Flush previous multiline entry
                        if current_entry and level_priority.get(current_entry['level'], 0) >= min_level:
                            entries.append(current_entry)
                        current_entry = {
                            'timestamp': m.group(1),
                            'module': m.group(2),
                            'level': m.group(3),
                            'message': m.group(4).strip(),
                            'source': os.path.basename(filepath),
                        }
                    elif current_entry:
                        # Continuation of multiline message
                        current_entry['message'] += '\n' + line.rstrip()
                # Flush last entry
                if current_entry and level_priority.get(current_entry['level'], 0) >= min_level:
                    entries.append(current_entry)
        except Exception:
            continue

    if not entries:
        console.print(f'[dim]No log entries at level {args.level} or above[/dim]')
        return

    # Sort chronologically, take last N
    entries.sort(key=lambda e: e['timestamp'])
    entries = entries[-args.tail:]

    if _json_mode:
        print_json_result(entries, title=f'Logs ({len(entries)} entries)')
    else:
        for entry in entries:
            level = entry['level']
            if level == 'ERROR' or level == 'CRITICAL':
                style = 'red'
            elif level == 'WARNING':
                style = 'yellow'
            elif level == 'INFO':
                style = 'green'
            else:
                style = 'dim'
            ts = entry['timestamp']
            mod = entry['module']
            msg = entry['message']
            # Truncate very long messages for display
            if len(msg) > 300:
                msg = msg[:300] + '...'
            console.print(f'[{style}]{ts} [{level:>8}] [{mod}] {msg}[/{style}]')


def _handle_risk(mmr: MMR, args: argparse.Namespace):
    # Collect any updates
    updates = {}
    field_map = {
        'max_open_orders': args.max_open_orders,
        'max_daily_loss': args.max_daily_loss,
        'max_position_size_pct': args.max_position_size_pct,
        'max_signals_per_hour': args.max_signals_per_hour,
        'max_leverage': args.max_leverage,
        'min_margin_cushion': args.min_margin_cushion,
    }
    for key, val in field_map.items():
        if val is not None:
            updates[key] = val

    _risk_descriptions = {
        'max_position_size_pct': 'Max single position as % of portfolio (0.1 = 10%)',
        'max_daily_loss': 'Max allowed daily loss in $ before blocking new orders',
        'max_open_orders': 'Max number of simultaneous open orders',
        'max_signals_per_hour': 'Max strategy signals allowed per hour (rate limit)',
        'max_leverage': 'Max leverage ratio (1.0 = cash only, 2.0 = 2x margin)',
        'min_margin_cushion': 'Min margin cushion (excess liquidity / net liquidity)',
    }

    if updates:
        result = mmr.set_risk_limits(**updates)
        print_status('Risk limits updated')
    else:
        result = mmr.get_risk_limits()

    import pandas as pd
    rows = [{'limit': k, 'value': v, 'description': _risk_descriptions.get(k, '')}
            for k, v in result.items()]
    print_df(pd.DataFrame(rows), title='Risk Limits')


def _handle_filter(mmr: MMR, args: argparse.Namespace):
    import pandas as pd

    action = getattr(args, 'filter_action', None)

    if action == 'deny':
        result = mmr.add_to_filter_list('denylist', args.symbols)
        print_status(f'Added to denylist: {", ".join(s.upper() for s in args.symbols)}')
    elif action == 'allow':
        result = mmr.add_to_filter_list('allowlist', args.symbols)
        print_status(f'Added to allowlist: {", ".join(s.upper() for s in args.symbols)}')
    elif action == 'remove':
        # Remove from both denylist and allowlist
        mmr.remove_from_filter_list('denylist', args.symbols)
        result = mmr.remove_from_filter_list('allowlist', args.symbols)
        print_status(f'Removed from filters: {", ".join(s.upper() for s in args.symbols)}')
    elif action == 'reset':
        result = mmr.reset_filters()
        print_status('All filters reset to defaults')
    elif action == 'set':
        updates = {}
        if args.min_price is not None:
            updates['min_price'] = args.min_price
        if args.deny_exchanges is not None:
            updates['deny_exchanges'] = [e.upper() for e in args.deny_exchanges]
        if args.exchanges is not None:
            updates['exchanges'] = [e.upper() for e in args.exchanges]
        if args.sec_types is not None:
            updates['sec_types'] = [t.upper() for t in args.sec_types]
        if args.deny_sec_types is not None:
            updates['deny_sec_types'] = [t.upper() for t in args.deny_sec_types]
        if args.locations is not None:
            updates['locations'] = [l.upper() for l in args.locations]
        if args.deny_locations is not None:
            updates['deny_locations'] = [l.upper() for l in args.deny_locations]
        if updates:
            result = mmr.set_filters(**updates)
            print_status('Filters updated')
        else:
            result = mmr.get_filters()
    else:
        # Default: show
        result = mmr.get_filters()

    _filter_descriptions = {
        'denylist': 'Symbols blocked from discovery and trading',
        'allowlist': 'If non-empty, ONLY these symbols are allowed (exclusive)',
        'exchanges': 'Allowed exchanges (empty = all)',
        'deny_exchanges': 'Blocked exchanges',
        'sec_types': 'Allowed security types (empty = all)',
        'deny_sec_types': 'Blocked security types',
        'locations': 'Allowed IB locations (empty = all)',
        'deny_locations': 'Denied IB locations (e.g. STK.CA)',
        'min_price': 'Price floor for discovery filtering',
    }
    rows = []
    for k, v in result.items():
        display = ', '.join(v) if isinstance(v, list) else str(v)
        rows.append({'filter': k, 'value': display or '(none)',
                     'description': _filter_descriptions.get(k, '')})
    print_df(pd.DataFrame(rows), title='Trading Filters')


def _handle_session(mmr: MMR, args: argparse.Namespace):
    from trader.trading.position_sizing import PositionSizingConfig

    action = getattr(args, 'session_action', None)

    if action == 'set':
        cfg = PositionSizingConfig.load()
        key = args.key
        value = args.value
        if key == 'risk':
            if value not in ('conservative', 'moderate', 'aggressive'):
                print_status(f'Invalid risk level: {value} (choose conservative/moderate/aggressive)', success=False)
                return
            cfg.risk_level = value
        elif key == 'base':
            cfg.base_position_usd = float(value)
        elif key == 'base_pct':
            cfg.base_position_pct = float(value)
        elif key == 'daily_loss_limit':
            cfg.daily_loss_limit_usd = float(value)
        cfg.save()
        print_status(f'Set {key} = {value}')
        return

    if action == 'reset':
        cfg = PositionSizingConfig()
        cfg.save()
        print_status('Session config reset to defaults')
        return

    if action == 'limits':
        cfg = PositionSizingConfig.load()
        limits = {
            'min_position_usd': f'${cfg.min_position_usd:,.0f}',
            'max_position_usd': f'${cfg.max_position_usd:,.0f}',
            'max_position_pct': f'{cfg.max_position_pct:.0%}',
            'max_total_exposure_pct': f'{cfg.max_total_exposure_pct:.0%}',
            'max_positions': str(cfg.max_positions),
        }
        print_dict(limits, title='Hard Limits')
        return

    # Default: full session status
    summary = mmr.session_status()

    if _json_mode:
        print_json_result(summary, title='Session Status')
        return

    # Config section
    cfg = summary['config']
    config_table = Table(title='Position Sizing Config', show_header=False)
    config_table.add_column('Key', style='bold')
    config_table.add_column('Value')
    config_table.add_row('Risk Level', f'[bold]{cfg["risk_level"]}[/bold]')
    if cfg.get('base_position_pct', 0) > 0 and cfg.get('effective_base_usd', 0) != cfg['base_position_usd']:
        config_table.add_row('Base Position', f'${cfg["effective_base_usd"]:,.0f} ({cfg["base_position_pct"]:.1%} of net liq)')
    else:
        config_table.add_row('Base Position', f'${cfg["base_position_usd"]:,.0f} (fixed)')
    config_table.add_row('Daily Loss Limit', f'${cfg["daily_loss_limit_usd"]:,.0f}')
    config_table.add_row('Min Position', f'${cfg["min_position_usd"]:,.0f}')
    config_table.add_row('Max Position', f'${cfg["max_position_usd"]:,.0f}')
    config_table.add_row('Max Position %', f'{cfg["max_position_pct"]:.0%} of net liq')
    config_table.add_row('Max Exposure', f'{cfg["max_total_exposure_pct"]:.0%} of net liq')
    config_table.add_row('Max Positions', str(cfg['max_positions']))
    config_table.add_row('Max ADV %', f'{cfg["max_adv_pct"]:.1%} of daily volume')
    config_table.add_row('Spread Penalty', f'above {cfg["spread_penalty_threshold"]:.2%} spread, up to {cfg["spread_penalty_factor"]:.0%} reduction')
    console.print(config_table)

    # Portfolio section
    port = summary['portfolio']
    if port['net_liquidation'] > 0:
        port_table = Table(title='Portfolio State', show_header=False)
        port_table.add_column('Key', style='bold')
        port_table.add_column('Value')
        port_table.add_row('Net Liquidation', f'${port["net_liquidation"]:,.0f}')
        port_table.add_row('Position Value', f'${port["gross_position_value"]:,.0f}')
        port_table.add_row('Available Funds', f'${port["available_funds"]:,.0f}')
        pnl_color = 'green' if port['daily_pnl'] >= 0 else 'red'
        pnl_sign = '+' if port['daily_pnl'] > 0 else ''
        port_table.add_row('Daily P&L', f'[{pnl_color}]{pnl_sign}${port["daily_pnl"]:,.0f}[/{pnl_color}]')
        port_table.add_row('Exposure', f'{port["exposure_pct"]:.0%}')
        port_table.add_row('Positions', str(port['position_count']))
        if port['pending_proposal_value'] > 0:
            port_table.add_row('Pending Proposals', f'${port["pending_proposal_value"]:,.0f}')
        console.print(port_table)
    else:
        console.print('[dim]No portfolio data (trader_service not connected)[/dim]')

    # Capacity section
    cap = summary['capacity']
    cap_table = Table(title='Remaining Capacity', show_header=False)
    cap_table.add_column('Key', style='bold')
    cap_table.add_column('Value')
    if port['net_liquidation'] > 0:
        cap_table.add_row('Remaining Budget', f'${cap["remaining_usd"]:,.0f}')
    cap_table.add_row('Remaining Positions', f'{cap["remaining_positions"]}')
    console.print(cap_table)

    # Recommended sizes
    sizes = summary['recommended_sizes']
    size_table = Table(title='Recommended Position Sizes')
    size_table.add_column('Confidence', style='bold')
    size_table.add_column('Amount', justify='right')
    size_table.add_column('Note', style='dim')
    for label, key in [('High (0.9)', 'high_confidence'),
                        ('Medium (0.5)', 'medium_confidence'),
                        ('Low (0.2)', 'low_confidence')]:
        s = sizes[key]
        note = s['capped_by'] if s['capped_by'] else ''
        size_table.add_row(label, f'${s["amount_usd"]:,.0f}', note)
    console.print(size_table)

    # Warnings
    if summary['warnings']:
        console.print()
        for w in summary['warnings']:
            console.print(f'[yellow]  Warning: {w}[/yellow]')


def _handle_strategies(mmr: MMR, args: argparse.Namespace):
    action = getattr(args, 'strat_action', None)
    if action == 'enable':
        result = mmr.enable_strategy(args.name, paper=args.paper)
        if result.is_success():
            print_status(f'Enabled: {args.name}')
        else:
            print_status(f'Enable failed: {result.error}', success=False)
    elif action == 'disable':
        result = mmr.disable_strategy(args.name)
        if result.is_success():
            print_status(f'Disabled: {args.name}')
        else:
            print_status(f'Disable failed: {result.error}', success=False)
    elif action == 'reload':
        result = mmr.reload_strategies()
        if result.is_success():
            print_status('Strategies reloaded')
        else:
            print_status(f'Reload failed: {result.error}', success=False)
    elif action == 'create':
        _handle_strategy_create(args)
    elif action == 'deploy':
        _handle_strategy_deploy(args)
    elif action == 'undeploy':
        _handle_strategy_undeploy(args)
    elif action == 'signals':
        _handle_strategy_signals(args)
    elif action == 'backtest':
        _handle_strategy_backtest(args)
    elif action in ('available', 'avail', 'list-files'):
        _handle_strategies_available(args)
    elif action == 'inspect':
        _handle_strategies_inspect(args)
    else:
        # Default: list strategies. Fall back to config file if service is down.
        _handle_strategies_from_config()


def _handle_strategies_inspect(args: argparse.Namespace):
    """Report tunable parameters + dispatch mode for every Strategy subclass.

    AST-based — does NOT execute the strategy file, so a broken import
    won't crash the scan. Surfaces:

      * ``class`` and ``file`` — identifiers needed for ``backtest --class``.
      * ``mode`` — ``precompute`` (fast path, O(N) total), ``on_prices``
        (legacy path, O(N²) on a backtest). The LLM should always sweep
        ``precompute`` strategies first; ``on_prices`` ones hang on 1-min
        1-year data.
      * ``tunables`` — upper-case class attributes with literal values.
        These are the ``--param KEY=VALUE`` knobs for ``bt-sweep``.

    Designed to save 5+ tool calls for an LLM planning a sweep: instead
    of reading every file, it gets one JSON payload with everything it
    needs to construct the grid.
    """
    import ast
    import pandas as pd

    # 1. Resolve strategies directory or a single file target.
    from trader.container import Container
    try:
        container = Container.instance()
        cfg = container.config()
        default_dir = cfg.get('strategies_directory', 'strategies')
    except Exception:
        default_dir = 'strategies'

    targets: List[Path] = []
    if args.strategy:
        p = Path(args.strategy).expanduser()
        if not p.is_absolute():
            cur = Path(__file__).resolve().parent
            while cur != cur.parent:
                candidate = cur / p
                if candidate.exists():
                    p = candidate
                    break
                cur = cur.parent
        if not p.exists():
            print_status(f'File not found: {args.strategy}', success=False)
            return
        targets = [p]
    else:
        strategies_dir = Path(args.directory or default_dir).expanduser()
        if not strategies_dir.is_absolute():
            cur = Path(__file__).resolve().parent
            while cur != cur.parent:
                candidate = cur / strategies_dir
                if candidate.exists():
                    strategies_dir = candidate
                    break
                cur = cur.parent
        if not strategies_dir.exists():
            print_status(
                f'Strategies directory not found: {strategies_dir}',
                success=False,
            )
            return
        targets = sorted(
            p for p in strategies_dir.glob('*.py') if not p.name.startswith('_')
        )

    def _literal(node) -> Any:
        """Return a literal value for an AST node or the sentinel
        ``_NON_LITERAL`` — we skip tunables whose defaults are expressions
        (they're typically computed at class scope, not user-tunable)."""
        try:
            return ast.literal_eval(node)
        except (ValueError, SyntaxError, TypeError):
            return _NON_LITERAL

    _NON_LITERAL = object()

    rows: List[Dict[str, Any]] = []
    for py in targets:
        try:
            tree = ast.parse(py.read_text())
        except SyntaxError as ex:
            rows.append({
                'file': py.name,
                'class': '',
                'mode': 'parse_error',
                'tunables': {},
                'docstring': f'syntax error: {ex.msg}',
            })
            continue

        for cls in [n for n in tree.body if isinstance(n, ast.ClassDef)]:
            # Must extend Strategy (direct or indirect — we match by name,
            # same heuristic as _handle_strategies_available).
            extends_strategy = any(
                (isinstance(b, ast.Name) and b.id == 'Strategy')
                or (isinstance(b, ast.Attribute) and b.attr == 'Strategy')
                for b in cls.bases
            )
            if not extends_strategy:
                continue

            tunables: Dict[str, Any] = {}
            methods: set = set()
            for item in cls.body:
                if isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name) and target.id.isupper():
                            val = _literal(item.value)
                            if val is _NON_LITERAL:
                                continue
                            tunables[target.id] = val
                elif isinstance(item, ast.AnnAssign):
                    # e.g. ``EMA_PERIOD: int = 20``
                    if (isinstance(item.target, ast.Name)
                            and item.target.id.isupper()
                            and item.value is not None):
                        val = _literal(item.value)
                        if val is not _NON_LITERAL:
                            tunables[item.target.id] = val
                elif isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods.add(item.name)
                    # Older strategies keep tunables in ``self.params`` and
                    # read them via ``self.params.get('key', default)``.
                    # Scan the method body for those calls so the LLM gets
                    # the knob list without reading the source.
                    for node in ast.walk(item):
                        if not isinstance(node, ast.Call):
                            continue
                        func = node.func
                        # Match self.params.get('key', default)
                        if (isinstance(func, ast.Attribute)
                                and func.attr == 'get'
                                and isinstance(func.value, ast.Attribute)
                                and func.value.attr == 'params'
                                and isinstance(func.value.value, ast.Name)
                                and func.value.value.id == 'self'
                                and node.args):
                            key_node = node.args[0]
                            if isinstance(key_node, ast.Constant) and isinstance(key_node.value, str):
                                key = key_node.value
                                default = (
                                    _literal(node.args[1])
                                    if len(node.args) > 1 else None
                                )
                                if default is _NON_LITERAL:
                                    default = None
                                # Preserve first-seen default if the key
                                # appears in multiple methods.
                                tunables.setdefault(key, default)

            has_precompute = 'precompute' in methods
            has_on_bar = 'on_bar' in methods
            has_on_prices = 'on_prices' in methods
            if has_precompute and has_on_bar:
                mode = 'precompute'
            elif has_precompute:
                # precompute without on_bar — strategy likely falls back to
                # the default on_bar that calls on_prices. Rare, but valid.
                mode = 'precompute+on_prices'
            elif has_on_prices:
                mode = 'on_prices'
            else:
                mode = 'inherited'

            docstring = ast.get_docstring(cls) or ''
            first_line = docstring.split('\n', 1)[0].strip()

            rows.append({
                'file': str(py.relative_to(py.parents[1]) if len(py.parents) > 1 else py.name),
                'class': cls.name,
                'mode': mode,
                'tunables': tunables,
                'docstring': first_line,
            })

    if _json_mode:
        print(json.dumps({'data': rows, 'title': 'Strategy Inspect'}, default=str))
        return

    from rich.table import Table
    from rich import box as _box
    tbl = Table(
        title=f'Strategy Inspect — {len(rows)} strategies',
        box=_box.ROUNDED, pad_edge=False, show_lines=True,
    )
    tbl.add_column('class', style='bold cyan', no_wrap=True)
    tbl.add_column('file', style='dim')
    tbl.add_column('mode', no_wrap=True)
    tbl.add_column('tunables', overflow='fold')
    tbl.add_column('summary', overflow='fold')

    for r in rows:
        if r['mode'] == 'precompute':
            mode_cell = '[green]precompute[/green] (fast)'
        elif r['mode'] == 'on_prices':
            mode_cell = '[yellow]on_prices[/yellow] (slow on 1-min)'
        elif r['mode'] == 'parse_error':
            mode_cell = '[red]parse error[/red]'
        else:
            mode_cell = r['mode']
        tuns = ', '.join(f'{k}={v}' for k, v in r['tunables'].items())
        tbl.add_row(r['class'], r['file'], mode_cell, tuns or '—',
                     (r['docstring'] or '')[:80])
    console.print(tbl)


def _handle_strategies_available(args: argparse.Namespace):
    """Scan the strategies directory for Strategy subclasses and report each
    one with its deployment status (deployed name, or '' if on-disk only).

    Parses each .py file with the ``ast`` module to find ``class X(Strategy)``
    definitions without executing the code — so a broken strategy file won't
    crash the scan and we don't need to resolve imports.
    """
    import ast
    import pandas as pd
    import yaml as _yaml

    # 1. Find strategies directory. Priority: --directory flag, then config,
    # then project-root default.
    from trader.container import Container
    try:
        container = Container.instance()
        cfg = container.config()
        default_dir = cfg.get('strategies_directory', 'strategies')
    except Exception:
        default_dir = 'strategies'

    strategies_dir = Path(args.directory or default_dir).expanduser()
    if not strategies_dir.is_absolute():
        # Resolve relative to project root (walk up from this file)
        cur = Path(__file__).resolve().parent
        while cur != cur.parent:
            if (cur / 'configs' / 'trader.yaml').exists():
                strategies_dir = cur / strategies_dir
                break
            cur = cur.parent

    if not strategies_dir.is_dir():
        print_status(f'Strategies directory not found: {strategies_dir}', success=False)
        return

    # 2. Load deployed strategies from strategy_runtime.yaml for cross-reference
    config_path = Path('~/.config/mmr/strategy_runtime.yaml').expanduser()
    deployed_by_file: Dict[str, list[str]] = {}
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                cfg_yaml = _yaml.safe_load(f) or {}
            for s in cfg_yaml.get('strategies', []) or []:
                mod = s.get('module', '')
                name = s.get('name', '')
                deployed_by_file.setdefault(mod, []).append(name)
        except Exception as ex:
            console.print(f'[yellow]could not read {config_path}: {ex}[/yellow]')

    # 3. Walk the strategies directory, AST-parse each .py, find Strategy
    # subclasses, and build a report.
    rows = []
    for py in sorted(strategies_dir.glob('*.py')):
        if py.name.startswith('_'):
            continue
        try:
            tree = ast.parse(py.read_text())
        except SyntaxError as ex:
            rows.append({
                'file': py.name,
                'class': f'<parse error: {ex.msg}>',
                'deployed_as': '',
                'docstring': '',
            })
            continue

        classes_in_file = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            # Accept any class that inherits from something named Strategy —
            # covers both `Strategy` and `trader.trading.strategy.Strategy`.
            base_names = []
            for b in node.bases:
                if isinstance(b, ast.Name):
                    base_names.append(b.id)
                elif isinstance(b, ast.Attribute):
                    base_names.append(b.attr)
            if 'Strategy' in base_names:
                classes_in_file.append(node)

        # Match deploy key either as "strategies/foo.py" or absolute path
        deploy_keys = [f'strategies/{py.name}', str(py), py.name]
        deployed = []
        for k in deploy_keys:
            if k in deployed_by_file:
                deployed.extend(deployed_by_file[k])

        if not classes_in_file:
            rows.append({
                'file': py.name,
                'class': '<no Strategy subclass>',
                'deployed_as': ', '.join(deployed),
                'docstring': '',
            })
            continue

        for cls in classes_in_file:
            doc = ast.get_docstring(cls) or ''
            doc_line = doc.splitlines()[0] if doc else ''
            rows.append({
                'file': py.name,
                'class': cls.name,
                'deployed_as': ', '.join(deployed) if len(classes_in_file) == 1 else '',
                'docstring': doc_line[:80] + ('…' if len(doc_line) > 80 else ''),
            })

    if _json_mode:
        print(json.dumps({'data': rows, 'title': 'Available Strategies'}))
        return

    title = f'Available Strategies ({strategies_dir})'
    if not rows:
        print_df(pd.DataFrame(), title=title)
        return
    print_df(pd.DataFrame(rows), title=title)


# ------------------------------------------------------------------
# Strategy lifecycle handlers (local-only, no service needed)
# ------------------------------------------------------------------

def _handle_strategies_from_config():
    """List strategies from config file when service is not running."""
    import pandas as pd
    import yaml

    config_path = Path('~/.config/mmr/strategy_runtime.yaml').expanduser()
    if not config_path.exists():
        print_status('No strategy_runtime.yaml found', success=False)
        return

    with open(config_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader) or {}

    strategies = config.get('strategies', [])
    if not strategies:
        print_df(pd.DataFrame(), title='Strategies (from config)')
        return

    rows = []
    for s in strategies:
        row = {
            'name': s.get('name', ''),
            'module': s.get('module', ''),
            'class_name': s.get('class_name', ''),
            'bar_size': s.get('bar_size', ''),
            'conids': str(s.get('conids', s.get('universe', ''))),
            'paper': s.get('paper', True),
            'days': s.get('historical_days_prior', ''),
        }
        params = s.get('params', {})
        if params:
            row['params'] = ', '.join(f'{k}={v}' for k, v in params.items())
        rows.append(row)
    print_df(pd.DataFrame(rows), title='Strategies (from config)')


def _handle_strategy_create(args: argparse.Namespace):
    """Create a strategy template file."""
    name = args.name
    directory = getattr(args, 'directory', 'strategies')

    # Convert snake_case name to CamelCase class name
    class_name = ''.join(word.capitalize() for word in name.split('_'))

    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / f'{name}.py'

    if file_path.exists():
        print_status(f'File already exists: {file_path}', success=False)
        return

    template = f'''from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Dict, Optional, Union

import pandas as pd


class {class_name}(Strategy):
    """Strategy: {name}

    Receives accumulated OHLCV DataFrames via on_prices().
    Return a Signal to generate a trade, or None to do nothing.

    DataFrame columns: date (index), open, high, low, close, volume, average, barCount

    Configure via params in strategy_runtime.yaml:
        params:
          fast_period: 10
          slow_period: 50
    Access in code: self.params.get('fast_period', 10)
    """

    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        fast_period = self.params.get('fast_period', 10)
        slow_period = self.params.get('slow_period', 50)

        if len(prices) < slow_period:
            return None

        # TODO: implement your strategy logic here
        # Example: simple moving average crossover
        # close = prices["close"]
        # fast_ma = close.rolling(fast_period).mean()
        # slow_ma = close.rolling(slow_period).mean()
        # if fast_ma.iloc[-1] > slow_ma.iloc[-1] and fast_ma.iloc[-2] <= slow_ma.iloc[-2]:
        #     return Signal(source_name=self.name, action=Action.BUY, probability=0.7, risk=0.3)

        return None
'''

    file_path.write_text(template)
    print_status(f'Created strategy: {file_path} (class: {class_name})')


def _handle_strategy_deploy(args: argparse.Namespace):
    """Deploy a strategy to strategy_runtime.yaml."""
    import yaml

    name = args.name
    config_path = Path('~/.config/mmr/strategy_runtime.yaml').expanduser()

    if not config_path.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config = {'strategies': []}
    else:
        with open(config_path, 'r') as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {'strategies': []}

    strategies = config.get('strategies', [])

    # Check for duplicates
    for s in strategies:
        if s.get('name') == name:
            print_status(f'Strategy "{name}" already deployed. Undeploy first.', success=False)
            return

    # Infer module path and class name from name
    module_path = f'strategies/{name}.py'

    # Try to find the actual Strategy subclass in the file
    class_name = None
    strategy_file = Path(module_path)
    if strategy_file.exists():
        import ast
        try:
            tree = ast.parse(strategy_file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    for base in node.bases:
                        base_name = base.id if isinstance(base, ast.Name) else (
                            base.attr if isinstance(base, ast.Attribute) else ''
                        )
                        if base_name == 'Strategy':
                            class_name = node.name
                            break
                    if class_name:
                        break
        except Exception:
            pass

    # Fallback: infer from name
    if not class_name:
        class_name = ''.join(word.capitalize() for word in name.split('_'))

    entry = {
        'name': name,
        'module': module_path,
        'class_name': class_name,
        'bar_size': getattr(args, 'bar_size', '1 min'),
        'historical_days_prior': args.days,
        'paper': args.paper,
    }

    if args.conids:
        entry['conids'] = args.conids
    if args.universe:
        entry['universe'] = args.universe

    strategies.append(entry)
    config['strategies'] = strategies

    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    print_status(f'Deployed strategy: {name}')


def _handle_strategy_undeploy(args: argparse.Namespace):
    """Remove a strategy from strategy_runtime.yaml."""
    import yaml

    name = args.name
    config_path = Path('~/.config/mmr/strategy_runtime.yaml').expanduser()

    if not config_path.exists():
        print_status(f'No strategy_runtime.yaml found', success=False)
        return

    with open(config_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader) or {'strategies': []}

    strategies = config.get('strategies', [])
    original_count = len(strategies)
    strategies = [s for s in strategies if s.get('name') != name]

    if len(strategies) == original_count:
        print_status(f'Strategy "{name}" not found in config', success=False)
        return

    config['strategies'] = strategies

    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    print_status(f'Undeployed strategy: {name}')


def _handle_strategy_signals(args: argparse.Namespace):
    """View recent signals from the event store."""
    import pandas as pd
    from trader.container import Container
    from trader.data.event_store import EventStore

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')

    if not duckdb_path:
        print_status('duckdb_path not configured', success=False)
        return

    store = EventStore(duckdb_path)
    events = store.query_by_strategy(args.name, limit=args.limit)

    if not events:
        print_df(pd.DataFrame(), title=f'Signals: {args.name}')
        return

    rows = []
    for e in events:
        rows.append({
            'time': str(e.timestamp)[:19],
            'type': e.event_type.value,
            'conid': e.conid,
            'symbol': e.symbol,
            'action': e.action,
            'quantity': e.quantity,
            'price': e.price,
            'probability': e.signal_probability,
            'risk': e.signal_risk,
        })
    print_df(pd.DataFrame(rows), title=f'Signals: {args.name}')


def _handle_strategy_backtest(args: argparse.Namespace):
    """Backtest a deployed strategy by looking up its config."""
    import yaml

    name = args.name
    config_path = Path('~/.config/mmr/strategy_runtime.yaml').expanduser()

    if not config_path.exists():
        print_status(f'No strategy_runtime.yaml found', success=False)
        return

    with open(config_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader) or {}

    strategies = config.get('strategies', [])
    found = None
    for s in strategies:
        if s.get('name') == name:
            found = s
            break

    if not found:
        print_status(f'Strategy "{name}" not found in config', success=False)
        return

    # Build synthetic args for _handle_backtest
    bt_args = argparse.Namespace(
        strategy=found.get('module', f'strategies/{name}.py'),
        class_name=found.get('class_name', ''.join(w.capitalize() for w in name.split('_'))),
        conids=found.get('conids'),
        universe=found.get('universe'),
        days=args.days,
        capital=args.capital,
        bar_size=getattr(args, 'bar_size', None) or found.get('bar_size', '1 min'),
    )
    _handle_backtest(bt_args)


# ------------------------------------------------------------------
# Backtest handler (local-only, no service needed)
# ------------------------------------------------------------------

def _handle_backtests(args: argparse.Namespace):
    """List / show / compare / delete past backtest runs from the store."""
    import pandas as pd
    from trader.container import Container
    from trader.data.backtest_store import BacktestStore

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')
    if not duckdb_path:
        print_status('duckdb_path not configured', success=False)
        return
    store = BacktestStore(duckdb_path)

    action = getattr(args, 'bts_action', None)

    if action in (None, 'list'):
        # For top-level `backtests` the filter args hang off args directly;
        # for `backtests list <flags>` they hang off the subparser. Prefer
        # whichever is set.
        strategy = getattr(args, 'strategy', None)
        limit = getattr(args, 'limit', 25)
        # argparse stores --sort-by as sort_by on the namespace
        sort_alias = getattr(args, 'sort_by', 'score') or 'score'
        descending = not getattr(args, 'asc', False)
        sort_column = _BTS_SORT_ALIASES.get(sort_alias, sort_alias)
        include_archived = getattr(args, 'include_archived', False)
        archived_only = getattr(args, 'archived_only', False)
        sweep_id = getattr(args, 'sweep_id', None)

        try:
            if sort_column == _BTS_SCORE_SENTINEL:
                # Composite score is computed in Python (not a DB column).
                # Pull a bigger pool than `limit` so the top-`limit` by
                # quality isn't skewed by the arbitrary created_at cutoff,
                # then truncate after ranking. Cap the pool to avoid
                # scanning the entire history when someone asks for --limit 5.
                pool_size = max(limit * 4, 100)
                records = store.list(
                    strategy_class=strategy,
                    limit=pool_size,
                    sort_by='created_at',
                    descending=True,
                    include_archived=include_archived,
                    archived_only=archived_only,
                    sweep_id=sweep_id,
                )
                records.sort(
                    key=_bt_composite_score,
                    reverse=descending,
                )
                records = records[:limit]
            else:
                records = store.list(
                    strategy_class=strategy,
                    limit=limit,
                    sort_by=sort_column,
                    descending=descending,
                    include_archived=include_archived,
                    archived_only=archived_only,
                    sweep_id=sweep_id,
                )
        except ValueError as ex:
            print_status(str(ex), success=False)
            return

        if _json_mode:
            print(json.dumps({
                'data': [
                    {
                        'id': r.id,
                        'class_name': r.class_name,
                        'bar_size': r.bar_size,
                        'start_date': str(r.start_date),
                        'end_date': str(r.end_date),
                        'total_trades': r.total_trades,
                        'total_return': r.total_return,
                        'sharpe_ratio': r.sharpe_ratio,
                        'max_drawdown': r.max_drawdown,
                        'win_rate': r.win_rate,
                        'final_equity': r.final_equity,
                        'note': r.note,
                        'archived': r.archived,
                        'sweep_id': r.sweep_id,
                    }
                    for r in records
                ],
                'title': 'Backtest History',
            }, default=str))
            return

        if not records:
            print_status('No backtest runs recorded yet — run `backtest ...` first.', success=False)
            return

        # Card view: one Panel per run with colored metric lines.
        if getattr(args, 'card', False):
            _print_backtest_cards(
                records, sort_column=sort_column, descending=descending,
                include_archived=include_archived, archived_only=archived_only,
            )
            return

        # Rich-rendered table with per-cell coloring. Green = strong signal
        # by industry heuristic; yellow = marginal; red = bad; default =
        # neutral / not applicable.
        from rich.table import Table
        from rich.text import Text

        title = _bt_history_title(
            sort_column, descending,
            include_archived=include_archived,
            archived_only=archived_only,
        )
        # Compact view — drop period (shown in `backtests show`) and
        # win_rate (ambiguous on its own; shown in `show` paired with pf).
        # Explicit narrow widths so nothing gets aggressively truncated.
        table = Table(title=title, show_lines=False, pad_edge=False)
        table.add_column('id',      justify='right', no_wrap=True, width=4)
        table.add_column('class',   no_wrap=True, width=16)
        table.add_column('trades',  justify='right', no_wrap=True, width=6)
        table.add_column('return',  justify='right', no_wrap=True, width=7)
        table.add_column('sharpe',  justify='right', no_wrap=True, width=6)
        table.add_column('sortino', justify='right', no_wrap=True, width=7)
        table.add_column('calmar',  justify='right', no_wrap=True, width=6)
        table.add_column('max_dd',  justify='right', no_wrap=True, width=7)
        table.add_column('pf',      justify='right', no_wrap=True, width=5)
        table.add_column('exp_bps', justify='right', no_wrap=True, width=8)
        table.add_column('t_in_mkt', justify='right', no_wrap=True, width=5)
        table.add_column('note',    overflow='ellipsis', width=22)

        def _cell(value, classifier, formatter):
            return Text(formatter(value), style=_bt_style(classifier, value))

        for r in records:
            pf = r.profit_factor
            pf_display = '∞' if pf > 1e15 else f'{pf:.2f}'
            strong = _bt_row_is_strong(r)
            archived = getattr(r, 'archived', False)
            id_text = Text(str(r.id), style=('bold green' if strong else ''))
            class_display = r.class_name
            if strong:
                class_display = f'✓ {class_display}'
            if archived:
                # Archived takes visual priority over the strong checkmark —
                # a row the user has explicitly hidden should read as
                # suppressed, not as a candidate.
                class_display = f'⊘ {class_display}'

            class_style = 'dim' if archived else ('bold green' if strong else '')
            table.add_row(
                id_text,
                Text(class_display, style=class_style),
                _cell(r.total_trades, _bt_class_trades, lambda v: f'{v}'),
                _cell(r.total_return, _bt_class_return, lambda v: f'{v:+.2%}'),
                _cell(r.sharpe_ratio, _bt_class_sharpe, lambda v: f'{v:+.2f}'),
                _cell(r.sortino_ratio, _bt_class_sortino, lambda v: f'{v:+.2f}'),
                _cell(r.calmar_ratio, _bt_class_calmar, lambda v: f'{v:.2f}'),
                _cell(r.max_drawdown, _bt_class_max_dd, lambda v: f'{v:.2%}'),
                Text(pf_display, style=_bt_style(_bt_class_pf, pf)),
                _cell(r.expectancy_bps, _bt_class_exp_bps, lambda v: f'{v:+.1f}'),
                Text(f'{r.time_in_market_pct:.0%}',
                     style=_bt_style(_bt_class_tim, r.time_in_market_pct)),
                Text((r.note or '')[:20]),
            )
        console.print(table)
        console.print(
            '[dim]Colors: [bold green]green[/] = strong by practitioner '
            'threshold, [yellow]yellow[/] = marginal, [red]red[/] = weak. '
            'Trade count [red]red[/] when < 20 (other metrics unreliable at '
            'that sample size). [bold green]✓ bold-green row[/] = 4+ quality '
            'metrics strong AND ≥ 30 trades — candidate worth investigating.[/]'
        )
        return

    if action == 'show':
        r = store.get(args.run_id)
        if not r:
            print_status(f'Backtest run #{args.run_id} not found', success=False)
            return

        pf = r.profit_factor
        pf_display = '∞' if pf > 1e15 else f'{pf:.2f}'
        wr_display = f'{r.win_rate:.2%}' if r.total_trades > 0 else 'n/a (no trades)'

        if _json_mode:
            detail = {
                'id':                   r.id,
                'created_at':           str(r.created_at)[:19],
                'class_name':           r.class_name,
                'strategy_path':        r.strategy_path,
                'code_hash':            (r.code_hash or '')[:16],
                'conids':               r.conids,
                'symbols':              _format_conids_with_symbols(r.conids),
                'universe':             r.universe or '',
                'bar_size':             r.bar_size,
                'period':               f'{str(r.start_date)[:10]} → {str(r.end_date)[:10]}',
                'initial_capital':      r.initial_capital,
                'fill_policy':          r.fill_policy,
                'slippage_bps':         r.slippage_bps,
                'commission_per_share': r.commission_per_share,
                'params':               r.params or {},
                'total_trades':         r.total_trades,
                'total_return':         r.total_return,
                'sharpe_ratio':         r.sharpe_ratio,
                'sortino_ratio':        r.sortino_ratio,
                'calmar_ratio':         r.calmar_ratio,
                'max_drawdown':         r.max_drawdown,
                'profit_factor':        'inf' if pf > 1e15 else r.profit_factor,
                'expectancy_bps':       r.expectancy_bps,
                'win_rate':             r.win_rate if r.total_trades > 0 else None,
                'time_in_market_pct':   r.time_in_market_pct,
                'final_equity':         r.final_equity,
                'trades_stored':        bool(r.trades_json),
                'note':                 r.note or '',
                'archived':             bool(getattr(r, 'archived', False)),
                'sweep_id':             getattr(r, 'sweep_id', None),
            }
            stats, stats_reason = _compute_bt_stats(r)
            stats_data = {
                'n_trades':              stats.n_trades,
                'n_bar_returns':         stats.n_bar_returns,
                'probabilistic_sharpe':  stats.psr,
                't_stat':                stats.t_stat,
                'p_value':               stats.p_value,
                'return_ci_lo':          stats.return_ci_lo,
                'return_ci_hi':          stats.return_ci_hi,
                'sharpe_ci_lo':          stats.sharpe_ci_lo,
                'sharpe_ci_hi':          stats.sharpe_ci_hi,
                'pnl_skew':              stats.pnl_skew,
                'pnl_excess_kurtosis':   stats.pnl_excess_kurtosis,
                'losing_streak_actual':  stats.losing_streak_actual,
                'losing_streak_mc_95':   stats.losing_streak_mc_95,
                'unavailable_reason':    stats_reason,
            }
            payload = {
                **detail,
                'statistical_confidence': stats_data,
            }
            # Raw arrays can be MB-scale on 1-min × 365d runs. Most
            # callers never need them (the persisted DB row is the
            # source of truth); include only on explicit --include-raw.
            if getattr(args, 'include_raw', False):
                payload['trades_json'] = r.trades_json
                payload['equity_curve_json'] = r.equity_curve_json
            print(json.dumps({'data': payload}, default=str))
            return

        # Rich detail — colored metric values matching the list view's
        # classifiers, plus a static "inputs" section and a "results"
        # section with the heuristic-colored numbers.
        from rich.table import Table
        from rich.text import Text as _Text

        strong = _bt_row_is_strong(r)
        header_style = 'bold green' if strong else 'bold'
        from rich import box as _box
        tbl = Table(
            title=_Text(f'Backtest Run #{r.id}{"  ✓" if strong else ""}',
                        style=header_style),
            show_header=False, box=_box.ROUNDED, pad_edge=False,
        )
        tbl.add_column('field', style='dim', no_wrap=True, width=22)
        tbl.add_column('value')

        def add(label, value, style=None):
            tbl.add_row(label, _Text(str(value), style=style or ''))

        def add_metric(label, value, classifier, formatter):
            try:
                style = _STYLE_FOR_CLASS.get(classifier(float(value)), '')
            except (TypeError, ValueError):
                style = ''
            tbl.add_row(label, _Text(formatter(value), style=style))

        # --- Inputs ---
        add('id',                   r.id)
        add('created_at',           str(r.created_at)[:19])
        add('class_name',           r.class_name, 'bold')
        add('strategy_path',        r.strategy_path)
        add('code_hash',            (r.code_hash or '')[:16])
        add('conids',               _format_conids_with_symbols(r.conids))
        add('universe',             r.universe or '—')
        add('bar_size',             r.bar_size)
        add('period',               f'{str(r.start_date)[:10]} → {str(r.end_date)[:10]}')
        add('initial_capital',      f'{r.initial_capital:,.2f}')
        add('fill_policy',          r.fill_policy)
        add('slippage_bps',         r.slippage_bps)
        add('commission_per_share', r.commission_per_share)
        # Render param overrides as "K=V, K=V" on a single line when there
        # are any — the full dict form is visually noisy and most runs
        # have only 2-3 overrides. Empty params stay as "—" for scanning.
        if r.params:
            params_line = ', '.join(f'{k}={v}' for k, v in sorted(r.params.items()))
            add('params', params_line)
        else:
            add('params', '—')

        # --- Separator ---
        tbl.add_row(_Text('── results ──', style='bold cyan'), '')

        # --- Results with classifier-driven color ---
        add_metric('total_trades',    r.total_trades,     _bt_class_trades,   lambda v: f'{int(v)}')
        add_metric('total_return',    r.total_return,     _bt_class_return,   lambda v: f'{v:+.2%}')
        add_metric('sharpe_ratio',    r.sharpe_ratio,     _bt_class_sharpe,   lambda v: f'{v:+.2f}')
        add_metric('sortino_ratio',   r.sortino_ratio,    _bt_class_sortino,  lambda v: f'{v:+.2f}')
        add_metric('calmar_ratio',    r.calmar_ratio,     _bt_class_calmar,   lambda v: f'{v:.2f}')
        add_metric('max_drawdown',    r.max_drawdown,     _bt_class_max_dd,   lambda v: f'{v:.2%}')
        # profit_factor special-case for ∞
        pf_style = _bt_style(_bt_class_pf, pf)
        tbl.add_row('profit_factor', _Text(pf_display, style=pf_style))
        add_metric('expectancy_bps', r.expectancy_bps,   _bt_class_exp_bps,  lambda v: f'{v:+.1f} bps')
        add('win_rate',              wr_display)  # ambiguous alone — no color
        add_metric('time_in_market', r.time_in_market_pct, _bt_class_tim,    lambda v: f'{v:.1%}')
        add('final_equity',          f'{r.final_equity:,.2f}')
        add('trades_stored',         bool(r.trades_json))
        add('note',                  r.note or '—')
        if getattr(r, 'archived', False):
            add('status', '⊘ archived (hidden from default list)', style='dim')

        # --- Statistical confidence section ---
        _append_stat_confidence_rows(tbl, r, add)

        console.print(tbl)
        if strong:
            console.print(
                '[dim]✓ This run has 4+ quality metrics in the green threshold '
                'and ≥ 30 trades — a real candidate.[/]'
            )
        return

    if action == 'confidence':
        # Compact bulk read of the stat-confidence block across N runs.
        # Designed for LLMs post-sweep: full `show` payloads include the
        # multi-MB trades_json blob per run — a 10-run batch of `show`
        # is ~170 MB of text that the caller only uses the 500-byte
        # confidence block from. This endpoint delivers just that block
        # (plus minimal identifying fields) for bulk decision-making.
        rows: List[Dict[str, Any]] = []
        for rid in args.run_ids:
            rec = store.get(rid)
            if rec is None:
                rows.append({'run_id': rid, 'error': 'not found'})
                continue
            stats, reason = _compute_bt_stats(rec)
            pf = rec.profit_factor
            rows.append({
                'run_id': rec.id,
                'sweep_id': rec.sweep_id,
                'class_name': rec.class_name,
                'symbols': _format_conids_with_symbols(rec.conids),
                'params': rec.params or {},
                'period': f'{str(rec.start_date)[:10]} → {str(rec.end_date)[:10]}',
                'summary': {
                    'total_trades': rec.total_trades,
                    'total_return': rec.total_return,
                    'sharpe_ratio': rec.sharpe_ratio,
                    'sortino_ratio': rec.sortino_ratio,
                    'profit_factor': (
                        'inf' if pf > 1e15 else rec.profit_factor
                    ),
                    'expectancy_bps': rec.expectancy_bps,
                    'max_drawdown': rec.max_drawdown,
                },
                'statistical_confidence': {
                    'n_trades': stats.n_trades,
                    'n_bar_returns': stats.n_bar_returns,
                    'probabilistic_sharpe': stats.psr,
                    't_stat': stats.t_stat,
                    'p_value': stats.p_value,
                    'return_ci_lo': stats.return_ci_lo,
                    'return_ci_hi': stats.return_ci_hi,
                    'sharpe_ci_lo': stats.sharpe_ci_lo,
                    'sharpe_ci_hi': stats.sharpe_ci_hi,
                    'pnl_skew': stats.pnl_skew,
                    'pnl_excess_kurtosis': stats.pnl_excess_kurtosis,
                    'losing_streak_actual': stats.losing_streak_actual,
                    'losing_streak_mc_95': stats.losing_streak_mc_95,
                    'unavailable_reason': reason,
                },
            })
        if _json_mode:
            print(json.dumps(
                {'data': rows, 'title': 'Backtest Confidence Batch'},
                default=str,
            ))
            return
        # Rich output for humans: one row per run, colored by PSR band.
        from rich.table import Table
        from rich.text import Text as _Text
        from rich import box as _box
        tbl = Table(
            title=f'Confidence — {len(rows)} run(s)',
            box=_box.ROUNDED, pad_edge=False, show_lines=False,
        )
        tbl.add_column('id', justify='right', width=5)
        tbl.add_column('class', no_wrap=True, width=20)
        tbl.add_column('params', overflow='ellipsis', width=28)
        tbl.add_column('PSR', justify='right', width=7)
        tbl.add_column('p_val', justify='right', width=7)
        tbl.add_column('return CI', justify='right', width=18)
        tbl.add_column('skew', justify='right', width=6)
        tbl.add_column('kurt', justify='right', width=6)
        for row in rows:
            if 'error' in row:
                tbl.add_row(str(row['run_id']), _Text('not found', style='red'),
                             '', '', '', '', '', '')
                continue
            sc = row['statistical_confidence']
            p_str = ', '.join(f'{k}={v}' for k, v in sorted((row['params'] or {}).items()))
            psr = sc['probabilistic_sharpe']
            if psr is None:
                psr_cell = _Text('—', style='dim')
            else:
                style = (
                    'bold green' if psr >= 0.95
                    else 'green' if psr >= 0.80
                    else 'yellow' if psr >= 0.50
                    else 'red'
                )
                psr_cell = _Text(f'{psr:.1%}', style=style)
            p_val = sc['p_value']
            p_cell = _Text('—' if p_val is None else f'{p_val:.4f}',
                            style=('green' if p_val is not None and p_val < 0.05 else 'yellow' if p_val is not None and p_val < 0.10 else 'red') if p_val is not None else 'dim')
            ci_lo, ci_hi = sc['return_ci_lo'], sc['return_ci_hi']
            if ci_lo is None:
                ci_cell = _Text('—', style='dim')
            else:
                ci_style = (
                    'green' if ci_lo > 0
                    else 'red' if ci_hi < 0
                    else 'yellow'
                )
                ci_cell = _Text(f'[{ci_lo:+.2f}, {ci_hi:+.2f}]', style=ci_style)
            skew = sc['pnl_skew']
            kurt = sc['pnl_excess_kurtosis']
            tbl.add_row(
                str(row['run_id']), row['class_name'], p_str,
                psr_cell, p_cell, ci_cell,
                f'{skew:+.2f}' if skew is not None else '—',
                f'{kurt:+.2f}' if kurt is not None else '—',
            )
        console.print(tbl)
        return

    if action == 'compare':
        records = [store.get(rid) for rid in args.run_ids]
        missing = [rid for rid, rec in zip(args.run_ids, records) if rec is None]
        if missing:
            print_status(f'Run(s) not found: {missing}', success=False)
            return
        rows = []
        for r in records:
            pf = r.profit_factor
            pf_display = '∞' if pf > 1e15 else f'{pf:.2f}'
            wr_display = f'{r.win_rate:.2%}' if r.total_trades > 0 else 'n/a'
            rows.append({
                'id': r.id,
                'class': r.class_name,
                'symbols': _format_conids_with_symbols(r.conids),
                'period': f'{str(r.start_date)[:10]}→{str(r.end_date)[:10]}',
                'trades': r.total_trades,
                'return': f'{r.total_return:+.2%}',
                'sharpe': f'{r.sharpe_ratio:.2f}',
                'sortino': f'{r.sortino_ratio:.2f}',
                'calmar': f'{r.calmar_ratio:.2f}',
                'max_dd': f'{r.max_drawdown:.2%}',
                'pf': pf_display,
                'exp_bps': f'{r.expectancy_bps:+.1f}',
                'win': wr_display,
                't_in_mkt': f'{r.time_in_market_pct:.0%}',
                'code': (r.code_hash or '')[:8],
                'note': (r.note or '')[:20],
            })
        if _json_mode:
            print(json.dumps({'data': rows, 'title': 'Backtest Comparison'}, default=str))
            return
        print_df(pd.DataFrame(rows), title='Backtest Comparison')
        return

    if action == 'delete':
        ok = store.delete(args.run_id)
        if ok:
            print_status(f'Deleted backtest run #{args.run_id}')
        else:
            print_status(f'Run #{args.run_id} not found', success=False)
        return

    if action in ('archive', 'unarchive'):
        archive = action == 'archive'
        ids = args.run_ids
        affected = store.set_archived(ids, archived=archive)
        verb = 'Archived' if archive else 'Unarchived'
        if affected == len(ids):
            print_status(f'{verb} {affected} run(s): {ids}')
        elif affected == 0:
            print_status(f'No runs updated — ids {ids} either not found '
                          f'or already in target state', success=False)
        else:
            # Partial success — surface which ids were already in target state.
            print_status(
                f'{verb} {affected} of {len(ids)} run(s); the rest were '
                f'already {"archived" if archive else "active"}',
            )
        return

    if action in ('help', 'metrics'):
        _print_backtest_metrics_help()
        return

    print_status(f'Unknown backtests action: {action}', success=False)


# --- backtests list: per-metric quality classification -----------------
# Each classifier returns 'good' (green), 'ok' (yellow), 'bad' (red), or
# 'neutral' (default). Thresholds come from practitioner heuristics —
# documented in _BACKTEST_METRIC_HELP — and are conservative (a green
# label means the number is a real positive signal, not "the best I've
# seen today").

def _bt_class_return(v):    return 'good' if v > 0.05 else ('ok' if v > 0 else 'bad')
def _bt_class_sharpe(v):    return 'good' if v > 2.0 else ('ok' if v > 1.0 else ('bad' if v < 0 else 'neutral'))
def _bt_class_sortino(v):   return 'good' if v > 2.5 else ('ok' if v > 1.5 else ('bad' if v < 0 else 'neutral'))
def _bt_class_calmar(v):
    # Calmar is 0 on zero-drawdown runs — that's neutral, not bad.
    if v == 0: return 'neutral'
    return 'good' if v > 1.5 else ('ok' if v > 0.5 else 'bad')
def _bt_class_max_dd(v):
    # More negative = worse. 0 is neutral (could mean "no drawdown" or "no trades").
    if v == 0: return 'neutral'
    return 'good' if v > -0.03 else ('ok' if v > -0.10 else 'bad')
def _bt_class_pf(v):
    # ∞ is stored as 1e18 sentinel — all-winners, but a small sample gets
    # here too, so only label "good" if paired with other signals.
    if v > 1e15: return 'ok'
    if v == 0:  return 'neutral'
    return 'good' if v > 2.0 else ('ok' if v > 1.2 else 'bad')
def _bt_class_exp_bps(v):   return 'good' if v > 5 else ('bad' if v < -5 else 'ok')
def _bt_class_trades(n):
    # Statistical reliability heuristic — below ~30 round-trips most other
    # metrics are noise. Color the trade count so users see immediately
    # when a run is too small to trust.
    if n >= 50: return 'good'
    if n >= 20: return 'ok'
    return 'bad'
def _bt_class_tim(v):
    # Selective 20–70% in-market is typical for good systematic strategies.
    if 0.20 <= v <= 0.70: return 'good'
    if 0.10 <= v <= 0.90: return 'ok'
    return 'neutral'

_STYLE_FOR_CLASS = {
    'good':    'bold green',
    'ok':      'yellow',
    'bad':     'red',
    'neutral': '',
}


def _bt_style(classifier, value):
    """Return a Rich style string for the given value + classifier."""
    try:
        cls = classifier(float(value))
    except (TypeError, ValueError):
        return ''
    return _STYLE_FOR_CLASS.get(cls, '')


def _bt_row_is_strong(r):
    """A backtest is 'strong' if 4+ quality metrics are green AND the
    trade count is statistically reliable. Used to bold the row ID."""
    trades_ok = r.total_trades >= 30
    if not trades_ok:
        return False
    quality_classes = [
        _bt_class_sharpe(r.sharpe_ratio),
        _bt_class_sortino(r.sortino_ratio),
        _bt_class_calmar(r.calmar_ratio),
        _bt_class_pf(r.profit_factor),
        _bt_class_exp_bps(r.expectancy_bps),
        _bt_class_return(r.total_return),
    ]
    return sum(1 for c in quality_classes if c == 'good') >= 4


def _bt_composite_score(r) -> float:
    """Single-number quality ranking for a backtest, used only to order the
    history list so the most promising runs float to the top.

    Weighted blend of sortino, profit_factor, expectancy_bps, total_return,
    and max_drawdown, each clipped to a sensible band so no single metric
    dominates. The whole thing is then multiplied by a *reliability* factor
    that penalises low trade counts — a 5-trade run with sharpe 10 is luck,
    not edge, and shouldn't outrank a 300-trade run with sharpe 3.

    Not a decision metric. Use the individual numbers (and `backtests show`)
    when judging whether to paper-trade a strategy.
    """
    trades = r.total_trades or 0
    if trades < 10:
        reliability = 0.2
    elif trades < 30:
        reliability = 0.6
    elif trades < 100:
        reliability = 0.9
    else:
        reliability = 1.0

    def _clip(x, lo, hi):
        return max(lo, min(hi, x))

    sortino = r.sortino_ratio or 0.0
    pf = r.profit_factor or 0.0
    # Treat infinity (no losers) as "just good, not exceptional" — often a
    # tiny sample. Cap at 3.0 so one lucky outlier can't dominate.
    pf_eff = 2.0 if pf > 1e15 else _clip(pf, 0.0, 3.0)
    exp_bps = r.expectancy_bps or 0.0
    ret = r.total_return or 0.0
    dd = r.max_drawdown or 0.0

    score = (
        0.30 * _clip(sortino / 3.0, -2.0, 2.0)
        + 0.20 * (pf_eff - 1.0)
        + 0.20 * _clip(exp_bps / 30.0, -2.0, 2.0)
        + 0.15 * _clip(ret / 0.10, -2.0, 2.0)
        + 0.15 * _clip(1.0 - abs(dd) / 0.15, -1.0, 1.0)
    )
    return score * reliability


# Sentinel returned by _BTS_SORT_ALIASES for the composite-score sort. The
# score isn't a column in the DB — we fetch rows ordered by created_at and
# re-sort in Python.
_BTS_SCORE_SENTINEL = '__score__'


def _parse_backtest_param_args(
    params_json: Optional[str],
    param_kv: List[str],
) -> Dict[str, Any]:
    """Merge ``--params '{...}'`` JSON and repeated ``--param KEY=VALUE``
    flags into a single dict. ``--param`` entries take precedence so users
    can override a single key without rewriting the whole JSON.

    Values from KEY=VALUE are returned as strings — the backtester's
    ``_coerce_param`` converts them to the class-attr type at apply time.
    Raises ``ValueError`` with a clear message on malformed input so the
    failure happens before a 3-minute backtest runs.
    """
    merged: Dict[str, Any] = {}
    if params_json:
        try:
            decoded = json.loads(params_json)
        except json.JSONDecodeError as ex:
            raise ValueError(f"--params is not valid JSON: {ex}")
        if not isinstance(decoded, dict):
            raise ValueError(
                f"--params must be a JSON object, got {type(decoded).__name__}"
            )
        merged.update(decoded)
    for entry in param_kv:
        if '=' not in entry:
            raise ValueError(
                f"--param expects KEY=VALUE, got {entry!r}"
            )
        key, _, value = entry.partition('=')
        key = key.strip()
        if not key:
            raise ValueError(f"--param has empty key: {entry!r}")
        merged[key] = value
    return merged


def _compute_bt_stats(record):
    """Compute the statistical-confidence bundle for a stored backtest
    run. Returns ``(stats, reason)`` where ``stats`` is the
    :class:`BacktestStats` dataclass (may contain Nones), and ``reason``
    is either ``None`` or a short human string explaining why key fields
    are missing (e.g. "run without --save-trades").
    """
    import numpy as np
    from trader.simulation.backtest_stats import compute_all
    from trader.simulation.backtester import Backtester

    # Parse persisted JSON blobs, tolerating empty strings.
    trades = None
    if record.trades_json:
        try:
            trades = json.loads(record.trades_json)
        except (ValueError, TypeError):
            trades = None

    bar_returns = None
    if record.equity_curve_json:
        try:
            curve = json.loads(record.equity_curve_json)
            values = np.array([float(p['value']) for p in curve], dtype=float)
            if len(values) >= 2:
                bar_returns = np.diff(values) / values[:-1]
        except (ValueError, TypeError, KeyError):
            bar_returns = None

    # Annualisation factor — mirrors Backtester._bars_per_year so the
    # Sharpe bootstrap CI is on the same scale as the stored sharpe_ratio.
    try:
        from trader.objects import BarSize
        bar_size_enum = BarSize(record.bar_size) if record.bar_size else BarSize.Days1
        bars_per_year = Backtester._bars_per_year(bar_size_enum)
    except Exception:
        bars_per_year = 252.0

    stats = compute_all(trades, bar_returns, bars_per_year=bars_per_year)

    reason = None
    if not record.trades_json and not record.equity_curve_json:
        reason = 'run without --save-trades — re-run to enable statistical tests'
    elif stats.n_trades == 0 and stats.n_bar_returns == 0:
        reason = 'no usable trade or equity data in saved run'

    return stats, reason


def _append_stat_confidence_rows(tbl, record, add) -> None:
    """Append the "── statistical confidence ──" section to the rich
    detail table for ``backtests show``. Pulls stats via
    :func:`_compute_bt_stats` and colors each row by its practitioner
    threshold so readers can scan for green/yellow/red at a glance.
    """
    from rich.text import Text as _Text

    stats, reason = _compute_bt_stats(record)
    tbl.add_row(_Text('── confidence ──', style='bold cyan'), '')

    if reason:
        tbl.add_row('', _Text(reason, style='dim italic'))
        return

    # PSR — single highest-signal "is it real?" metric.
    if stats.psr is not None:
        if stats.psr >= 0.95:
            style = 'bold green'
        elif stats.psr >= 0.80:
            style = 'green'
        elif stats.psr >= 0.50:
            style = 'yellow'
        else:
            style = 'red'
        tbl.add_row(
            'prob_sharpe > 0',
            _Text(
                f'{stats.psr:.1%}  (n={stats.n_bar_returns} bar returns)',
                style=style,
            ),
        )
    else:
        tbl.add_row('prob_sharpe > 0', _Text('insufficient bar data', style='dim'))

    # t-test
    if stats.p_value is not None:
        if stats.p_value < 0.01:
            style = 'bold green'
        elif stats.p_value < 0.05:
            style = 'green'
        elif stats.p_value < 0.10:
            style = 'yellow'
        else:
            style = 'red'
        t_display = f't={stats.t_stat:+.2f}   p={stats.p_value:.4f}'
        tbl.add_row('t-test (mean pnl)', _Text(t_display, style=style))
    else:
        tbl.add_row('t-test (mean pnl)', _Text('insufficient trades', style='dim'))

    # Bootstrap CI on mean per-trade P&L (dollar units)
    if stats.return_ci_lo is not None:
        if stats.return_ci_lo > 0:
            style = 'green'
        elif stats.return_ci_hi < 0:
            style = 'red'
        else:
            style = 'yellow'  # CI straddles zero → edge not statistically distinguishable
        tbl.add_row(
            'return 95% CI',
            _Text(
                f'[${stats.return_ci_lo:+,.2f}, ${stats.return_ci_hi:+,.2f}] per trade',
                style=style,
            ),
        )
    else:
        tbl.add_row('return 95% CI', _Text('need ≥ 10 trades', style='dim'))

    # Bootstrap CI on annualised Sharpe
    if stats.sharpe_ci_lo is not None:
        if stats.sharpe_ci_lo > 1.0:
            style = 'green'
        elif stats.sharpe_ci_lo > 0.0:
            style = 'yellow'
        else:
            style = 'red'
        tbl.add_row(
            'sharpe 95% CI',
            _Text(
                f'[{stats.sharpe_ci_lo:+.2f}, {stats.sharpe_ci_hi:+.2f}]',
                style=style,
            ),
        )
    else:
        tbl.add_row('sharpe 95% CI', _Text('need ≥ 30 bar returns', style='dim'))

    # Distribution shape (skew + excess kurt)
    if stats.pnl_skew is not None:
        # Negative skew + high kurt is the blow-up signature.
        skew_bad = stats.pnl_skew < -0.5
        kurt_bad = (stats.pnl_excess_kurtosis or 0) > 5.0
        if skew_bad and kurt_bad:
            style = 'bold red'
            tag = '⚠ negative skew + fat tails: blow-up risk'
        elif skew_bad:
            style = 'red'
            tag = 'negative skew: occasional large losers'
        elif kurt_bad:
            style = 'yellow'
            tag = 'fat tails: outsized single-trade moves'
        elif stats.pnl_skew > 0.2:
            style = 'green'
            tag = 'positive skew: winners run, losers cut'
        else:
            style = ''
            tag = 'roughly symmetric'
        tbl.add_row(
            'pnl distribution',
            _Text(
                f'skew={stats.pnl_skew:+.2f}  excess_kurt={stats.pnl_excess_kurtosis:+.2f}  — {tag}',
                style=style,
            ),
        )
    else:
        tbl.add_row('pnl distribution', _Text('need ≥ 3 trades', style='dim'))

    # Losing streak vs MC
    if stats.losing_streak_actual is not None:
        actual = stats.losing_streak_actual
        mc95 = stats.losing_streak_mc_95 or 0
        if actual > mc95:
            style = 'red'
            tag = f'worse than random (MC 95th pct = {mc95:.0f})'
        elif actual > 0.75 * mc95:
            style = 'yellow'
            tag = f'near the MC 95th pct ({mc95:.0f}) — losses cluster a bit'
        else:
            style = 'green'
            tag = f'within random expectation (MC 95th pct = {mc95:.0f})'
        tbl.add_row(
            'losing streak',
            _Text(f'{actual} trades — {tag}', style=style),
        )
    else:
        tbl.add_row('losing streak', _Text('need ≥ 5 trades', style='dim'))


def _format_conids_with_symbols(conids) -> str:
    """Render a list of conIds as ``SYMBOL (conId)`` using the local universe
    DB, falling back to a bare conId when the symbol isn't resolvable locally.

    Used by ``backtests show`` / ``compare`` so the user sees *which
    instruments* a run was executed on, not just opaque integers. If the
    universe DB isn't available at all (fresh install, no data_service run
    yet) this degrades gracefully to the raw list.
    """
    if not conids:
        return '—'
    try:
        from trader.container import Container
        from trader.data.universe import UniverseAccessor
        cfg = Container.instance().config()
        accessor = UniverseAccessor(
            cfg.get('duckdb_path', ''),
            cfg.get('universe_library', 'Universes'),
        )
    except Exception:
        return str(list(conids))

    parts = []
    for cid in conids:
        try:
            matches = accessor.resolve_universe(int(cid), first_only=True)
            if matches:
                _, sd = matches[0]
                parts.append(f'{sd.symbol} ({cid})')
                continue
        except Exception:
            pass
        parts.append(f'? ({cid})')
    return ', '.join(parts)


def _bt_history_title(
    sort_column: str,
    descending: bool,
    include_archived: bool = False,
    archived_only: bool = False,
) -> str:
    """Build the `backtests` list/card header line — wording matches the
    sort mode so we don't claim "best first" when it's actually
    chronological, and signals the archive filter so the user isn't
    confused about missing rows."""
    if sort_column == _BTS_SCORE_SENTINEL:
        base = (
            'Backtest History — ranked by quality score '
            f'({"best first" if descending else "worst first"})'
        )
    elif sort_column == 'created_at':
        base = (
            f'Backtest History — sorted by {sort_column} '
            f'({"newest first" if descending else "oldest first"})'
        )
    else:
        base = (
            f'Backtest History — sorted by {sort_column} '
            f'({"high→low" if descending else "low→high"})'
        )
    if archived_only:
        return f'{base}  [archived only]'
    if include_archived:
        return f'{base}  [incl. archived]'
    return base

# --- backtests list: sort-column aliases --------------------------------
# User-friendly alias → actual backtest_runs column name (or the score
# sentinel). Accepted as ``--sort-by`` choices on ``backtests list``.
_BTS_SORT_ALIASES = {
    'score': _BTS_SCORE_SENTINEL, 'quality': _BTS_SCORE_SENTINEL,
    'best': _BTS_SCORE_SENTINEL,
    'time': 'created_at', 'created': 'created_at', 'created_at': 'created_at',
    'return': 'total_return', 'total_return': 'total_return',
    'sharpe': 'sharpe_ratio', 'sharpe_ratio': 'sharpe_ratio',
    'sortino': 'sortino_ratio', 'sortino_ratio': 'sortino_ratio',
    'calmar': 'calmar_ratio', 'calmar_ratio': 'calmar_ratio',
    'pf': 'profit_factor', 'profit_factor': 'profit_factor',
    'expectancy': 'expectancy_bps', 'expectancy_bps': 'expectancy_bps', 'exp': 'expectancy_bps',
    'win': 'win_rate', 'win_rate': 'win_rate',
    'max_dd': 'max_drawdown', 'max_drawdown': 'max_drawdown', 'drawdown': 'max_drawdown',
    'trades': 'total_trades', 'total_trades': 'total_trades',
    'time_in_market': 'time_in_market_pct', 'tim': 'time_in_market_pct',
}


# --- Metric reference ---------------------------------------------------

_BACKTEST_METRIC_HELP = [
    # (name, what it measures, good threshold, when it lies)
    (
        'total_return',
        'Cumulative return over the backtest period: (final_equity / initial_capital) - 1.',
        'Positive is a necessary but nowhere-near-sufficient signal. Compare to buy-and-hold.',
        'Dominated by one big winning day on short windows. Annualization would distort it.',
    ),
    (
        'sharpe_ratio',
        'Annualized return divided by annualized standard deviation of returns. Classic risk-adjusted metric.',
        '> 1.0 decent, > 2.0 good, > 3.0 suspicious or exceptional.',
        'Treats upside volatility as bad (dumb for asymmetric strategies). Meaningless on < ~30 bars. Ignores tail risk.',
    ),
    (
        'sortino_ratio',
        'Like Sharpe but the denominator is downside-only std — only losses count as "bad volatility". Better for asymmetric P&L.',
        '> 1.5 decent, > 2.5 good. Roughly Sharpe × 1.4 on symmetric returns.',
        'Same short-window noise problem as Sharpe. Can look great when there are few losing days even if wins are tiny.',
    ),
    (
        'calmar_ratio',
        'total_return / |max_drawdown|. "Dollars of edge per dollar of worst drawdown." Raw form — NOT annualized, so comparable across window lengths.',
        '> 1.0 means you earned more than your worst loss. > 3.0 is notably good.',
        'A backtest with no drawdown reports 0 or ∞. Short windows often have no meaningful drawdown yet.',
    ),
    (
        'max_drawdown',
        'Worst peak-to-trough percentage decline in equity. Always ≤ 0.',
        'Better than −10% for long-horizon equity strategies. > −20% means you\'ll be tempted to turn it off at the wrong moment.',
        'A short backtest may not have seen a real drawdown yet. Doesn\'t reflect recovery time.',
    ),
    (
        'profit_factor',
        'Gross wins / |gross losses|, summed across round-trip P&Ls. The single number most practitioners read first.',
        '> 1 profitable, > 1.5 decent, > 2 robust edge, > 3 rare.',
        'A few huge winners inflate it. Watch trade count — 10-trade samples aren\'t reliable.',
    ),
    (
        'expectancy_bps',
        'Average round-trip P&L as basis points of entry notional. What the strategy has to survive against real-world frictions.',
        'Needs to clear your round-trip slippage + commissions with margin. +3 bps is OK, +10 bps is good, +30 bps+ is great.',
        'Very high expectancy on low-turnover is fine; very high expectancy on high-turnover means you probably optimized against the backtester\'s fill model.',
    ),
    (
        'win_rate',
        'Fraction of closed round-trips that finished positive. Rendered "n/a" when there are no trades.',
        'Meaningless alone. A 40% win rate is great if payoff is 3:1; a 70% win rate is terrible if payoff is 1:3.',
        'Looks good on strategies that cut winners early and let losers run. Always pair with profit_factor.',
    ),
    (
        'time_in_market_pct',
        'Fraction of bars during which at least one position was open.',
        'Tells you whether edge comes from being always-on (boring, probably index-like) or selective (more interesting). 20–60% is typical for systematic strategies.',
        'Not a quality metric by itself — context only. A 100% time-in-market strategy with Sharpe 2 is basically long-only.',
    ),
    (
        'total_trades',
        'Count of BUY and SELL fills — so a full round-trip is 2 trades.',
        'Low trade counts (< 30) mean most other metrics are statistical noise.',
        'High turnover amplifies slippage/commission exposure — always cross-check with expectancy_bps.',
    ),
    (
        'final_equity',
        'Cash + mark-to-market positions at the end of the backtest.',
        'Just your starting capital × (1 + total_return).',
        'Nothing hidden here; listed for at-a-glance readability in `backtests show`.',
    ),
]


def _print_backtest_cards(
    records,
    sort_column: str,
    descending: bool,
    include_archived: bool = False,
    archived_only: bool = False,
) -> None:
    """Render a list of BacktestRecords as card panels — one panel per run
    with colored metric lines, border color reflecting overall strength.
    Mirrors the portfolio card view in spirit: dense, scannable, readable
    in narrow terminals.
    """
    from rich.columns import Columns
    from rich.console import Group
    from rich.panel import Panel
    from rich.text import Text

    title = Text(
        _bt_history_title(
            sort_column, descending,
            include_archived=include_archived,
            archived_only=archived_only,
        ),
        style='bold',
    )

    def _fmt(value, classifier, fmt):
        """Styled Text for a metric value."""
        try:
            style = _STYLE_FOR_CLASS.get(classifier(float(value)), '')
        except (TypeError, ValueError):
            style = ''
        return Text(fmt(value), style=style)

    panels = []
    for r in records:
        pf = r.profit_factor
        pf_display = '∞' if pf > 1e15 else f'{pf:.2f}'
        strong = _bt_row_is_strong(r)
        archived = getattr(r, 'archived', False)

        # Border color: archived dominates — a soft-deleted run should read
        # as suppressed regardless of whether the metrics look good. Otherwise:
        # green when strong, red if clearly bad, yellow for marginal.
        if archived:
            border = 'grey50'
        elif strong:
            border = 'green'
        elif r.total_return < 0 or r.sharpe_ratio < 0:
            border = 'red'
        else:
            border = 'yellow'

        body = Text()

        # Fixed column widths so right-hand label always starts at the same
        # column regardless of left-hand value length. Without this, shorter
        # values (e.g. `1.75`) pull the right label leftward while longer
        # ones (`+1.98%`) push it right, producing a jagged card.
        LEFT_VAL_W = 8

        def _pad_to(text_obj: Text, width: int) -> Text:
            gap = width - len(text_obj.plain)
            if gap > 0:
                text_obj.append(' ' * gap)
            return text_obj

        def row(label, value_text, extra_label=None, extra_value=None):
            body.append(f'  {label:<9} ', style='dim')
            vt = value_text if isinstance(value_text, Text) else Text(str(value_text))
            if extra_label is not None:
                body.append_text(_pad_to(vt.copy(), LEFT_VAL_W))
                body.append(f'  {extra_label:<9} ', style='dim')
                body.append_text(
                    extra_value if isinstance(extra_value, Text)
                    else Text(str(extra_value)))
            else:
                body.append_text(vt)
            body.append('\n')

        # Row 1: period (alone — 21 chars won't fit alongside a second column
        # inside the 44-wide panel, so it gets its own line).
        period = f'{str(r.start_date)[:10]}→{str(r.end_date)[:10]}'
        row('period', period)

        # Row 2: trades (alone — sample-size deserves prominence; a small
        # trade count red-flags the whole row).
        row('trades', _fmt(r.total_trades, _bt_class_trades, lambda v: f'{v}'))

        # Row 3: return + time_in_market
        row('return',
            _fmt(r.total_return, _bt_class_return, lambda v: f'{v:+.2%}'),
            't_in_mkt',
            _fmt(r.time_in_market_pct, _bt_class_tim, lambda v: f'{v:.0%}'))

        # Row 3: sharpe + sortino
        row('sharpe',
            _fmt(r.sharpe_ratio, _bt_class_sharpe, lambda v: f'{v:+.2f}'),
            'sortino',
            _fmt(r.sortino_ratio, _bt_class_sortino, lambda v: f'{v:+.2f}'))

        # Row 4: calmar + max_dd
        row('calmar',
            _fmt(r.calmar_ratio, _bt_class_calmar, lambda v: f'{v:.2f}'),
            'max_dd',
            _fmt(r.max_drawdown, _bt_class_max_dd, lambda v: f'{v:.2%}'))

        # Row 5: profit_factor + expectancy_bps
        pf_text = Text(pf_display, style=_bt_style(_bt_class_pf, pf))
        row('pf', pf_text,
            'exp_bps',
            _fmt(r.expectancy_bps, _bt_class_exp_bps, lambda v: f'{v:+.1f}'))

        # Row 6: note + code_hash (dim)
        if r.note:
            body.append(f'  note      ', style='dim')
            body.append(r.note[:30], style='italic')
            body.append('\n')
        if r.code_hash:
            body.append(f'  code      ', style='dim')
            body.append(r.code_hash[:10], style='dim')

        if archived:
            # Leading ⊘ means "hidden from default list" — reads consistently
            # with the table view's marker.
            panel_title = (
                f'[dim]#{r.id} ⊘ {r.class_name}  {r.bar_size}[/]'
            )
        else:
            strong_mark = '✓ ' if strong else ''
            panel_title = (
                f'[bold]#{r.id}[/] {strong_mark}[bold]{r.class_name}[/]  '
                f'[dim]{r.bar_size}[/]'
            )
        panels.append(Panel(body, title=panel_title, border_style=border, width=44))

    console.print(Group(
        Text(''),
        title,
        Text(''),
        Columns(panels, padding=(0, 1)),
    ))


def _print_backtest_metrics_help():
    """Render the backtest metric reference as a Rich table."""
    from rich.table import Table

    if _json_mode:
        print(json.dumps({
            'data': [
                {'metric': name, 'what': what, 'good': good, 'watch_out': watch}
                for name, what, good, watch in _BACKTEST_METRIC_HELP
            ],
            'title': 'Backtest Metrics Reference',
        }))
        return

    table = Table(title='Backtest Metrics Reference — what each number tells you', show_lines=True)
    table.add_column('metric', style='bold cyan', no_wrap=True)
    table.add_column('what it measures', style='white')
    table.add_column('good', style='green')
    table.add_column('where it lies', style='yellow')
    for name, what, good, watch in _BACKTEST_METRIC_HELP:
        table.add_row(name, what, good, watch)
    console.print(table)
    console.print(
        "\n[dim]Heuristic: a strategy worth paper-trading usually has "
        "profit_factor > 1.5, sortino > 1.5, and expectancy_bps that "
        "comfortably clears your real-world round-trip costs. "
        "Sharpe alone is the worst single metric to optimize — too easy to game.[/dim]"
    )


def _handle_sweep(args: argparse.Namespace):
    """Top-level dispatcher for `mmr sweep {run,list,show}`."""
    action = getattr(args, 'sweep_action', None)
    if action == 'run':
        _handle_sweep_run(args)
    elif action == 'list':
        _handle_sweep_list(args)
    elif action == 'show':
        _handle_sweep_show(args)
    else:
        print_status(
            'Usage: sweep run <manifest.yaml> | sweep list | sweep show <id>',
            success=False,
        )


def _resolve_sym_to_conid(accessor, symbol: str) -> Optional[int]:
    """Resolve a ticker to its local-DB conId via UniverseAccessor.
    Returns None if unresolvable — caller decides whether that's fatal."""
    try:
        matches = accessor.resolve_universe(symbol, first_only=True)
    except Exception:
        return None
    if not matches:
        return None
    _universe, sd = matches[0]
    return int(sd.conId)


def _freshness_check(
    conids: List[int], max_staleness_days: int = 3,
) -> List[Dict[str, Any]]:
    """Return a list of ``{conid, last_bar}`` entries for conids whose
    most-recent bar in the local DuckDB is older than ``max_staleness_days``.

    Empty list means "everything is fresh enough". We check the `1 day`
    bar series only — even for 1-min sweeps, having yesterday's daily
    bar means the 1-min feed is probably also caught up.
    """
    import datetime as dt
    from trader.container import Container
    from trader.data.data_access import TickStorage
    from trader.objects import BarSize

    cfg = Container.instance().config()
    storage = TickStorage(cfg.get('duckdb_path', ''))
    tick = storage.get_tickdata(BarSize.Days1)

    stale = []
    cutoff = dt.datetime.now() - dt.timedelta(days=max_staleness_days)
    for cid in conids:
        try:
            df = tick.read(int(cid))
        except Exception as ex:
            stale.append({'conid': int(cid), 'last_bar': None,
                           'reason': f'read error: {ex}'})
            continue
        if df is None or len(df) == 0:
            stale.append({'conid': int(cid), 'last_bar': None,
                           'reason': 'no data'})
            continue
        last_ts = df.index.max()
        # DataFrame index may be tz-aware; compare as naive.
        last_naive = last_ts.to_pydatetime().replace(tzinfo=None) if hasattr(
            last_ts, 'to_pydatetime'
        ) else last_ts
        if last_naive < cutoff:
            stale.append({
                'conid': int(cid),
                'last_bar': str(last_naive)[:10],
                'reason': f'stale ({last_naive} < cutoff {cutoff})',
            })
    return stale


def _sweep_manifest_validate(manifest: Any) -> List[Dict[str, Any]]:
    """Validate + canonicalise a parsed sweep-manifest YAML.

    Expected shape::

        sweeps:
          - name: <str>
            strategy: <path>
            class: <ClassName>
            # exactly one of symbols/conids/universe:
            symbols: [SPY, QQQ, ...]   |  conids: [1, 2, ...]  |  universe: portfolio
            param_grid: {KEY: [v1, v2, ...], ...}   # optional; empty = single run w/ defaults
            days: 365                                 # default 365
            bar_size: "1 min"                         # default "1 min"
            concurrency: 8                            # default auto (cpu-1)
            note: "..."                               # optional

    Returns the list of *validated* sweep dicts. Raises ValueError with
    the offending sweep + field on any problem."""
    if not isinstance(manifest, dict) or 'sweeps' not in manifest:
        raise ValueError("manifest must be a YAML dict with top-level 'sweeps' list")
    sweeps = manifest['sweeps']
    if not isinstance(sweeps, list) or not sweeps:
        raise ValueError("'sweeps' must be a non-empty list")

    cleaned: List[Dict[str, Any]] = []
    for i, raw in enumerate(sweeps):
        if not isinstance(raw, dict):
            raise ValueError(f"sweeps[{i}] must be a dict, got {type(raw).__name__}")
        for required in ('name', 'strategy', 'class'):
            if not raw.get(required):
                raise ValueError(f"sweeps[{i}] missing required field {required!r}")
        # Exactly one symbol source.
        sources = sum(1 for k in ('symbols', 'conids', 'universe') if raw.get(k))
        if sources != 1:
            raise ValueError(
                f"sweeps[{i}] must have exactly one of "
                f"'symbols', 'conids', or 'universe' "
                f"(got {sources})"
            )
        pg = raw.get('param_grid') or {}
        if pg and not isinstance(pg, dict):
            raise ValueError(f"sweeps[{i}].param_grid must be a dict")
        for k, v in pg.items():
            if not isinstance(v, list) or not v:
                raise ValueError(
                    f"sweeps[{i}].param_grid[{k}] must be a non-empty list"
                )
        cleaned.append({
            'name': raw['name'],
            'strategy': raw['strategy'],
            'class': raw['class'],
            'symbols': raw.get('symbols'),
            'conids': raw.get('conids'),
            'universe': raw.get('universe'),
            'param_grid': pg,
            'days': int(raw.get('days', 365)),
            'bar_size': raw.get('bar_size', '1 min'),
            'concurrency': raw.get('concurrency'),  # None = auto-tune
            'note': raw.get('note', ''),
        })
    return cleaned


def _expand_sweep_jobs(
    sweep: Dict[str, Any], accessor,
) -> List[Dict[str, Any]]:
    """Expand a single validated sweep into concrete per-(symbol, param)
    job dicts. Resolves symbols → conids via the local universe DB; raises
    ValueError on unresolvable symbols."""
    import itertools

    if sweep.get('universe'):
        uni = accessor.get(sweep['universe'])
        if not uni:
            raise ValueError(f"universe {sweep['universe']!r} not found")
        symbol_conids = [(sd.symbol, int(sd.conId)) for sd in uni.security_definitions]
    elif sweep.get('conids'):
        symbol_conids = [(str(c), int(c)) for c in sweep['conids']]
    else:  # symbols
        resolved = []
        unresolved = []
        for sym in sweep['symbols']:
            cid = _resolve_sym_to_conid(accessor, sym)
            if cid is None:
                unresolved.append(sym)
            else:
                resolved.append((sym, cid))
        if unresolved:
            raise ValueError(
                f"sweep {sweep['name']!r}: could not resolve symbols "
                f"{unresolved} — add to a universe or pass conids directly"
            )
        symbol_conids = resolved

    grid = sweep.get('param_grid') or {}
    if grid:
        keys = list(grid.keys())
        value_lists = [grid[k] for k in keys]
        param_combos = [dict(zip(keys, combo))
                         for combo in itertools.product(*value_lists)]
    else:
        param_combos = [{}]  # single "run with defaults" job

    jobs = []
    for symbol, cid in symbol_conids:
        for params in param_combos:
            jobs.append({
                'sweep_name': sweep['name'],
                'symbol': symbol,
                'strategy': sweep['strategy'],
                'class_name': sweep['class'],
                'conids': [cid],
                'days': sweep['days'],
                'bar_size': sweep['bar_size'],
                'params': params,
                'note': sweep['note'],
            })
    return jobs


def _handle_sweep_run(args: argparse.Namespace):
    import datetime as dt
    import hashlib
    import os
    import pathlib
    from trader.container import Container
    from trader.data.backtest_store import BacktestStore, SweepRecord
    from trader.data.universe import UniverseAccessor

    manifest_path = pathlib.Path(args.manifest).expanduser()
    if not manifest_path.exists():
        print_status(f'manifest not found: {manifest_path}', success=False)
        return
    try:
        import yaml as _yaml
        manifest_yaml = manifest_path.read_text()
        manifest = _yaml.safe_load(manifest_yaml)
        sweep_specs = _sweep_manifest_validate(manifest)
    except ValueError as ex:
        print_status(f'manifest invalid: {ex}', success=False)
        return
    except Exception as ex:
        print_status(f'manifest load failed: {ex}', success=False)
        return

    # Share a single universe accessor across all sweeps in the manifest.
    cfg = Container.instance().config()
    duckdb_path = cfg.get('duckdb_path', '')
    accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))
    bstore = BacktestStore(duckdb_path)

    # Expand everything up front so `--dry-run` has the full plan.
    per_sweep_plans: List[Dict[str, Any]] = []
    try:
        for spec in sweep_specs:
            jobs = _expand_sweep_jobs(spec, accessor)
            per_sweep_plans.append({'spec': spec, 'jobs': jobs})
    except ValueError as ex:
        print_status(str(ex), success=False)
        return

    # Auto-tune concurrency: cpu_count - 1 to leave room for other processes
    # (e.g. a running trader_service). Cap at 16 to avoid DuckDB write thrash.
    def _auto_concurrency(requested) -> int:
        if args.concurrency is not None:
            return max(1, int(args.concurrency))
        if requested is not None:
            return max(1, int(requested))
        cpu = os.cpu_count() or 4
        return max(1, min(16, cpu - 1))

    # Dry-run: expand + estimate, exit.
    if args.dry_run:
        total_jobs = sum(len(p['jobs']) for p in per_sweep_plans)
        # Rough estimate: 90s/job per core at 1-min/365d on a precompute
        # strategy. This is deliberately conservative for the UI.
        est_per_job_seconds = 90
        total_cpu_seconds = total_jobs * est_per_job_seconds
        # Effective wall time if you ran each sweep at its own concurrency.
        wall_seconds = sum(
            max(1, len(p['jobs'])) * est_per_job_seconds
            / _auto_concurrency(p['spec']['concurrency'])
            for p in per_sweep_plans
        )
        plan: Dict[str, Any] = {
            'manifest': str(manifest_path),
            'total_jobs': total_jobs,
            'est_cpu_seconds': total_cpu_seconds,
            'est_wall_seconds': int(wall_seconds),
            'sweeps': [
                {
                    'name': p['spec']['name'],
                    'jobs': len(p['jobs']),
                    'concurrency': _auto_concurrency(p['spec']['concurrency']),
                }
                for p in per_sweep_plans
            ],
        }
        if _json_mode:
            print(json.dumps({'data': plan, 'title': 'Sweep Dry-run'}))
            return
        console.print(f'[bold]Dry-run: {manifest_path.name}[/]')
        console.print(f'  total jobs: {total_jobs}')
        console.print(f'  est CPU-time: {total_cpu_seconds / 60:.1f} min '
                       f'(~{est_per_job_seconds}s/job ×{total_jobs})')
        console.print(f'  est wall time: {wall_seconds / 60:.1f} min')
        for s in plan['sweeps']:
            console.print(
                f'  • {s["name"]:<30} {s["jobs"]:>4} jobs  '
                f'concurrency={s["concurrency"]}'
            )
        return

    # Freshness guard — check every conid touched by every sweep.
    if not args.skip_freshness:
        all_conids = sorted({
            c for p in per_sweep_plans for j in p['jobs'] for c in j['conids']
        })
        stale = _freshness_check(all_conids)
        if stale:
            summary = ', '.join(
                f'conid={s["conid"]} last={s["last_bar"]}'
                for s in stale[:5]
            )
            more = '' if len(stale) <= 5 else f' (+{len(stale)-5} more)'
            print_status(
                f'{len(stale)} conid(s) have stale data: {summary}{more}. '
                f'Refusing to run — rerun with --skip-freshness to override '
                f'or pull fresh data first.', success=False,
            )
            return

    # Execute each sweep sequentially (sweeps themselves are typically
    # small N; jobs within a sweep are what parallelize).
    import asyncio
    asyncio.run(_run_sweeps_async(
        manifest_yaml, per_sweep_plans,
        bstore=bstore, auto_concurrency=_auto_concurrency,
    ))


async def _run_sweeps_async(
    manifest_yaml: str,
    plans: List[Dict[str, Any]],
    *,
    bstore,
    auto_concurrency,
) -> None:
    """Run every sweep in the manifest, writing a digest when done."""
    import asyncio
    import hashlib
    import os
    import signal
    import datetime as dt
    from trader.data.backtest_store import SweepRecord

    # SIGINT: let in-flight subprocess jobs finish and write what they can.
    cancel_requested = {'flag': False}

    def _on_sigint(*_a):
        cancel_requested['flag'] = True
        if not _json_mode:
            console.print(
                '\n[yellow]SIGINT received — waiting for in-flight jobs '
                'to finish, then exiting gracefully...[/]'
            )

    try:
        signal.signal(signal.SIGINT, _on_sigint)
    except (ValueError, AttributeError):
        # Not in the main thread — skip handler installation.
        pass

    for plan in plans:
        spec = plan['spec']
        jobs = plan['jobs']
        concurrency = auto_concurrency(spec['concurrency'])
        config_hash = hashlib.sha256(
            json.dumps(jobs, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]

        # Record the sweep up-front so partial runs are still discoverable.
        sweep_id = bstore.create_sweep(SweepRecord(
            name=spec['name'],
            manifest_yaml=manifest_yaml,
            config_hash=config_hash,
            n_runs_planned=len(jobs),
            concurrency=concurrency,
            note=spec.get('note', ''),
        ))

        if not _json_mode:
            console.print(
                f'[bold]Sweep #{sweep_id}: {spec["name"]}[/] '
                f'— {len(jobs)} jobs at concurrency={concurrency}'
            )

        t0 = dt.datetime.now()
        results = await _execute_jobs_parallel(
            jobs, concurrency=concurrency, sweep_id=sweep_id,
            cancel_flag=cancel_requested,
        )
        elapsed = (dt.datetime.now() - t0).total_seconds()

        ok = sum(1 for r in results if r.get('status') == 'ok')
        fail = len(results) - ok
        status = (
            'cancelled' if cancel_requested['flag']
            else 'completed' if fail == 0
            else 'failed' if ok == 0
            else 'completed'  # partial success still counts as completed
        )
        digest_path = _write_sweep_digest(
            sweep_id=sweep_id, spec=spec, results=results,
            elapsed_s=elapsed, status=status,
        )
        bstore.finalize_sweep(
            sweep_id, status=status,
            n_runs_successful=ok, n_runs_failed=fail,
            digest_path=digest_path,
        )
        if not _json_mode:
            console.print(
                f'  → sweep #{sweep_id} {status}: {ok}/{len(jobs)} ok '
                f'in {elapsed/60:.1f}m  |  digest: {digest_path}'
            )

        if cancel_requested['flag']:
            break

    if _json_mode:
        # Emit a compact summary list of the sweep ids we just ran.
        ran = []
        for plan in plans:
            ran.append({'name': plan['spec']['name'],
                         'jobs': len(plan['jobs'])})
        print(json.dumps(
            {'data': {'sweeps_run': ran, 'cancelled': cancel_requested['flag']},
             'title': 'Sweep Batch'}, default=str,
        ))


async def _execute_jobs_parallel(
    jobs: List[Dict[str, Any]],
    *,
    concurrency: int,
    sweep_id: int,
    cancel_flag: Dict[str, bool],
) -> List[Dict[str, Any]]:
    """Run ``jobs`` as parallel ``mmr backtest`` subprocesses, capped at
    ``concurrency``. Each child writes its own BacktestRecord with the
    parent ``sweep_id`` stamped on it.

    Skips any pending launches after ``cancel_flag['flag']`` is set, but
    does NOT kill subprocesses already running — a sweep that's 90% done
    and gets interrupted still persists the 90%.
    """
    import asyncio
    import sys

    sem = asyncio.Semaphore(concurrency)
    mmr_py = sys.executable

    async def _one(job: Dict[str, Any]) -> Dict[str, Any]:
        if cancel_flag.get('flag'):
            return {'status': 'cancelled', 'job': job}
        async with sem:
            if cancel_flag.get('flag'):
                return {'status': 'cancelled', 'job': job}
            cmd = [
                mmr_py, '-m', 'trader.mmr_cli', '--json', 'backtest',
                '-s', job['strategy'],
                '--class', job['class_name'],
                '--conids', *[str(c) for c in job['conids']],
                '--days', str(job['days']),
                '--bar-size', job['bar_size'],
                '--summary-only',
                '--sweep-id', str(sweep_id),
            ]
            if job.get('params'):
                cmd.extend(['--params', json.dumps(job['params'])])
            if job.get('note'):
                cmd.extend(['--note', job['note']])
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=900,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return {'status': 'timeout', 'job': job}
            if proc.returncode != 0:
                return {
                    'status': 'error', 'job': job,
                    'error': (stderr.decode() or stdout.decode())[:500],
                }
            try:
                parsed = json.loads(stdout.decode().strip())
                summary = parsed.get('data', {}).get('summary', {})
                return {'status': 'ok', 'job': job, 'summary': summary}
            except json.JSONDecodeError:
                return {
                    'status': 'error', 'job': job,
                    'error': f'non-json stdout: {stdout.decode()[:200]}',
                }

    return await asyncio.gather(*(_one(j) for j in jobs))


def _write_sweep_digest(
    *, sweep_id: int, spec: Dict[str, Any], results: List[Dict[str, Any]],
    elapsed_s: float, status: str,
) -> str:
    """Write a markdown summary of a sweep to ``~/.local/share/mmr/reports/``.
    Returns the absolute path written (empty string on failure — digest
    writing never takes down a sweep)."""
    import datetime as dt
    import pathlib
    try:
        reports_dir = pathlib.Path.home() / '.local' / 'share' / 'mmr' / 'reports'
        reports_dir.mkdir(parents=True, exist_ok=True)
        ts = dt.datetime.now().strftime('%Y-%m-%d_%H%M%S')
        safe_name = ''.join(
            c if c.isalnum() or c in ('-', '_') else '_'
            for c in spec['name']
        )[:40]
        path = reports_dir / f'sweep_{sweep_id:04d}_{safe_name}_{ts}.md'

        ok = [r for r in results if r['status'] == 'ok']
        # Rank by sortino + pf − 10×|dd|, matching the rough ranking used
        # in the parallel sweep_helper. Full composite score would require
        # reloading each record; this is close enough for the digest.
        def _rough_score(r):
            s = r.get('summary', {})
            sortino = s.get('sortino_ratio') or 0.0
            pf = s.get('profit_factor')
            if pf == 'inf' or (isinstance(pf, (int, float)) and pf > 1e15):
                pf = 3.0
            elif not isinstance(pf, (int, float)):
                pf = 0.0
            dd = abs(s.get('max_drawdown') or 0.0)
            return sortino + min(pf, 3.0) - 10 * dd

        ok_sorted = sorted(ok, key=_rough_score, reverse=True)
        strong = [
            r for r in ok_sorted
            if (r['summary'].get('sharpe_ratio') or 0) > 2.0
            and (r['summary'].get('profit_factor') in ('inf',)
                  or (isinstance(r['summary'].get('profit_factor'), (int, float))
                       and r['summary'].get('profit_factor', 0) > 1.5))
        ]
        fails = [r for r in results if r['status'] != 'ok']

        lines: List[str] = []
        lines.append(f'# Sweep #{sweep_id}: {spec["name"]}')
        lines.append('')
        lines.append(f'- **Status**: {status}')
        lines.append(f'- **Duration**: {elapsed_s/60:.1f} min')
        lines.append(f'- **Results**: {len(ok)}/{len(results)} ok, {len(fails)} failed')
        lines.append(f'- **Strategy**: `{spec["class"]}` ({spec["strategy"]})')
        lines.append(f'- **Bar size**: `{spec["bar_size"]}` × {spec["days"]} days')
        if spec.get('param_grid'):
            lines.append(f'- **Param grid**: `{json.dumps(spec["param_grid"])}`')
        lines.append('')

        def _leader_table(rows: List[Dict[str, Any]], header: str) -> None:
            if not rows:
                return
            lines.append(f'## {header} ({len(rows)})')
            lines.append('')
            lines.append(
                '| rank | run_id | symbol | params | return | sharpe | sortino | pf | trades | max_dd |'
            )
            lines.append(
                '|------|--------|--------|--------|--------|--------|---------|----|---|--------|'
            )
            for rank, r in enumerate(rows[:20], 1):
                s = r['summary']
                p = json.dumps(r['job'].get('params', {}), separators=(',', '='))
                pf = s.get('profit_factor', 0)
                pf_d = '∞' if pf == 'inf' or (
                    isinstance(pf, (int, float)) and pf > 1e15
                ) else (f'{pf:.2f}' if isinstance(pf, (int, float)) else str(pf))
                lines.append(
                    f'| {rank} | {s.get("run_id", "")} | {r["job"]["symbol"]} | '
                    f'`{p}` | {(s.get("total_return") or 0):+.2%} | '
                    f'{(s.get("sharpe_ratio") or 0):+.2f} | '
                    f'{(s.get("sortino_ratio") or 0):+.2f} | {pf_d} | '
                    f'{s.get("total_trades", 0)} | '
                    f'{(s.get("max_drawdown") or 0):+.2%} |'
                )
            lines.append('')

        _leader_table(strong, 'Strong candidates')
        _leader_table(ok_sorted, 'All successful runs (ranked)')

        if fails:
            lines.append(f'## Failures ({len(fails)})')
            lines.append('')
            for r in fails[:20]:
                err = r.get('error') or r['status']
                sym = r['job'].get('symbol', '?')
                lines.append(
                    f'- {sym} params={r["job"].get("params", {})} — '
                    f'`{r["status"]}`: {err[:200]}'
                )
            lines.append('')

        lines.append('---')
        lines.append(f'Full run details: `mmr backtests list --sweep {sweep_id}`')
        lines.append(
            f'Per-run confidence: `mmr backtests confidence <run_ids>`  '
            f'(see `mmr sweep show {sweep_id}` for top run ids)'
        )

        path.write_text('\n'.join(lines))
        return str(path)
    except Exception as ex:
        if not _json_mode:
            console.print(f'[yellow]digest write failed: {ex}[/yellow]')
        return ''


def _handle_sweep_list(args: argparse.Namespace):
    from trader.container import Container
    from trader.data.backtest_store import BacktestStore
    cfg = Container.instance().config()
    bstore = BacktestStore(cfg.get('duckdb_path', ''))
    sweeps = bstore.list_sweeps(limit=args.limit)

    if _json_mode:
        data = []
        for s in sweeps:
            data.append({
                'id': s.id, 'name': s.name, 'status': s.status,
                'started_at': str(s.started_at)[:19] if s.started_at else None,
                'finished_at': str(s.finished_at)[:19] if s.finished_at else None,
                'n_runs_planned': s.n_runs_planned,
                'n_runs_successful': s.n_runs_successful,
                'n_runs_failed': s.n_runs_failed,
                'concurrency': s.concurrency,
                'digest_path': s.digest_path,
                'note': s.note,
            })
        print(json.dumps({'data': data, 'title': 'Sweep History'}, default=str))
        return

    if not sweeps:
        print_status('No sweeps recorded yet — run `mmr sweep run <manifest.yaml>`', success=False)
        return
    from rich.table import Table
    from rich import box as _box
    tbl = Table(title=f'Sweep History — {len(sweeps)} sweeps',
                  box=_box.ROUNDED, pad_edge=False)
    tbl.add_column('id', justify='right', width=4)
    tbl.add_column('name', no_wrap=True, width=30)
    tbl.add_column('status', width=10)
    tbl.add_column('runs', justify='right', width=10)
    tbl.add_column('started', width=16)
    tbl.add_column('duration', justify='right', width=10)
    tbl.add_column('digest', overflow='ellipsis', width=40)
    for s in sweeps:
        dur = (
            f'{(s.finished_at - s.started_at).total_seconds()/60:.0f}m'
            if s.finished_at and s.started_at else '—'
        )
        status_style = {
            'completed': 'green', 'failed': 'red',
            'cancelled': 'yellow', 'running': 'cyan',
        }.get(s.status, '')
        runs = f'{s.n_runs_successful}/{s.n_runs_planned}'
        if s.n_runs_failed:
            runs += f' (-{s.n_runs_failed})'
        tbl.add_row(
            str(s.id), s.name,
            f'[{status_style}]{s.status}[/]' if status_style else s.status,
            runs,
            str(s.started_at)[:16] if s.started_at else '—',
            dur,
            s.digest_path or '—',
        )
    console.print(tbl)


def _handle_sweep_show(args: argparse.Namespace):
    from trader.container import Container
    from trader.data.backtest_store import BacktestStore
    cfg = Container.instance().config()
    bstore = BacktestStore(cfg.get('duckdb_path', ''))
    swp = bstore.get_sweep(args.sweep_id)
    if not swp:
        print_status(f'sweep #{args.sweep_id} not found', success=False)
        return
    # Pull the child runs, ranked by composite score.
    records = bstore.list(
        sweep_id=args.sweep_id, limit=10_000,
        sort_by='created_at', descending=True, include_archived=True,
    )
    records.sort(key=_bt_composite_score, reverse=True)

    if _json_mode:
        print(json.dumps({
            'data': {
                'sweep': {
                    'id': swp.id, 'name': swp.name, 'status': swp.status,
                    'n_runs_planned': swp.n_runs_planned,
                    'n_runs_successful': swp.n_runs_successful,
                    'n_runs_failed': swp.n_runs_failed,
                    'digest_path': swp.digest_path,
                    'started_at': str(swp.started_at)[:19] if swp.started_at else None,
                    'finished_at': str(swp.finished_at)[:19] if swp.finished_at else None,
                },
                'leaderboard': [
                    {
                        'rank': rank,
                        'run_id': r.id,
                        'class_name': r.class_name,
                        'symbols': _format_conids_with_symbols(r.conids),
                        'params': r.params,
                        'score': _bt_composite_score(r),
                        'total_return': r.total_return,
                        'sharpe_ratio': r.sharpe_ratio,
                        'sortino_ratio': r.sortino_ratio,
                        'profit_factor': (
                            'inf' if r.profit_factor > 1e15 else r.profit_factor
                        ),
                        'total_trades': r.total_trades,
                        'max_drawdown': r.max_drawdown,
                    }
                    for rank, r in enumerate(records[:args.top], 1)
                ],
            },
            'title': f'Sweep #{swp.id}: {swp.name}',
        }, default=str))
        return

    from rich.table import Table
    from rich.text import Text as _Text
    from rich import box as _box

    console.print(
        f'[bold]Sweep #{swp.id}: {swp.name}[/]  '
        f'[dim]({swp.status}, {swp.n_runs_successful}/{swp.n_runs_planned} ok)[/]'
    )
    if swp.digest_path:
        console.print(f'[dim]digest: {swp.digest_path}[/]')
    console.print('')

    tbl = Table(title=f'Top {min(args.top, len(records))} of {len(records)} runs',
                  box=_box.ROUNDED, pad_edge=False)
    tbl.add_column('rank', justify='right', width=4)
    tbl.add_column('id', justify='right', width=5)
    tbl.add_column('symbols', no_wrap=True, width=20)
    tbl.add_column('params', overflow='ellipsis', width=30)
    tbl.add_column('score', justify='right', width=7)
    tbl.add_column('return', justify='right', width=8)
    tbl.add_column('sharpe', justify='right', width=7)
    tbl.add_column('pf', justify='right', width=6)
    tbl.add_column('trades', justify='right', width=6)
    for rank, r in enumerate(records[:args.top], 1):
        pf = r.profit_factor
        pf_d = '∞' if pf > 1e15 else f'{pf:.2f}'
        p_str = ', '.join(f'{k}={v}' for k, v in sorted((r.params or {}).items()))
        tbl.add_row(
            str(rank), str(r.id),
            _format_conids_with_symbols(r.conids),
            p_str,
            f'{_bt_composite_score(r):+.3f}',
            _Text(f'{r.total_return:+.2%}',
                   style=_bt_style(_bt_class_return, r.total_return)),
            _Text(f'{r.sharpe_ratio:+.2f}',
                   style=_bt_style(_bt_class_sharpe, r.sharpe_ratio)),
            _Text(pf_d, style=_bt_style(_bt_class_pf, pf)),
            str(r.total_trades),
        )
    console.print(tbl)


def _handle_backtest_sweep(args: argparse.Namespace):
    """Cartesian-product parameter sweep — run one backtest per grid point,
    persist each to the history store, and print a composite-score
    leaderboard at the end.

    Runs sequentially in-process so all combos share the same loaded data
    and the BacktestStore write path. For true parallelism across sweeps,
    use ``MMRHelpers.backtest_batch`` which spawns subprocess-level
    concurrency.
    """
    import datetime as dt
    import itertools
    from trader.container import Container
    from trader.data.backtest_store import (
        BacktestStore, BacktestRecord, compute_strategy_hash,
    )
    from trader.data.data_access import TickStorage
    from trader.data.universe import UniverseAccessor
    from trader.simulation.backtester import Backtester, BacktestConfig
    from trader.simulation.slippage import get_slippage_model
    from trader.objects import BarSize

    # 1. Parse the grid first so bad input fails fast.
    try:
        grid = json.loads(args.grid)
    except json.JSONDecodeError as ex:
        print_status(f'--grid is not valid JSON: {ex}', success=False)
        return
    if not isinstance(grid, dict) or not grid:
        print_status('--grid must be a non-empty JSON object', success=False)
        return
    keys = list(grid.keys())
    value_lists = []
    for k in keys:
        v = grid[k]
        if not isinstance(v, list) or not v:
            print_status(
                f'grid key {k!r} must map to a non-empty list', success=False,
            )
            return
        value_lists.append(v)
    combos = list(itertools.product(*value_lists))

    # 2. Shared infrastructure — load once, reuse across combos.
    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')
    storage = TickStorage(duckdb_path)
    accessor = UniverseAccessor(
        duckdb_path, cfg.get('universe_library', 'Universes'),
    )
    bstore = BacktestStore(duckdb_path)

    conids = args.conids
    if args.universe and not conids:
        uni = accessor.get(args.universe)
        if not uni:
            print_status(f'Universe not found: {args.universe}', success=False)
            return
        conids = [sd.conId for sd in uni.security_definitions]
    if not conids:
        print_status('Provide --conids or --universe', success=False)
        return

    bar_size = BarSize.parse_str(args.bar_size)
    config = BacktestConfig(
        start_date=dt.datetime.now() - dt.timedelta(days=args.days),
        end_date=dt.datetime.now(),
        initial_capital=args.capital,
        bar_size=bar_size,
        slippage_model=get_slippage_model('fixed', bps=args.slippage_bps),
    )
    backtester = Backtester(storage, config)
    code_hash = compute_strategy_hash(args.strategy)

    # 3. Run each combo.
    results: List[Dict[str, Any]] = []
    for i, combo in enumerate(combos, 1):
        params = {k: v for k, v in zip(keys, combo)}
        if not _json_mode:
            console.print(
                f'[dim]({i}/{len(combos)})[/dim] {params} ...',
                end='',
            )
        try:
            result = backtester.run_from_module(
                args.strategy, args.class_name, conids,
                universe_accessor=accessor, params=params,
            )
        except Exception as ex:
            err = f'{type(ex).__name__}: {ex}'
            results.append({'params': params, 'error': err})
            if not _json_mode:
                console.print(f' [red]ERROR[/red]: {err}')
            continue

        final_equity = (
            float(result.equity_curve.iloc[-1])
            if len(result.equity_curve) > 0 else float(config.initial_capital)
        )
        trades_json = ''
        equity_curve_json = ''
        if getattr(args, 'save_trades', True):
            trades_json = json.dumps([{
                'timestamp': str(t.timestamp), 'conid': int(t.conid),
                'action': str(t.action), 'quantity': float(t.quantity),
                'price': float(t.price), 'commission': float(t.commission),
                'signal_probability': float(t.signal_probability),
                'signal_risk': float(t.signal_risk),
            } for t in result.trades])
            equity_curve_json = json.dumps([
                {'timestamp': str(ts), 'value': float(v)}
                for ts, v in result.equity_curve.items()
            ])

        record = BacktestRecord(
            strategy_path=args.strategy,
            class_name=args.class_name,
            conids=list(conids),
            universe=args.universe or '',
            start_date=config.start_date,
            end_date=config.end_date,
            bar_size=str(bar_size),
            initial_capital=config.initial_capital,
            fill_policy=config.fill_policy,
            slippage_bps=args.slippage_bps,
            commission_per_share=config.commission_per_share,
            params=dict(getattr(result, 'applied_params', {}) or {}),
            code_hash=code_hash,
            total_trades=result.total_trades,
            total_return=result.total_return,
            sharpe_ratio=result.sharpe_ratio,
            max_drawdown=result.max_drawdown,
            win_rate=result.win_rate,
            final_equity=final_equity,
            sortino_ratio=result.sortino_ratio,
            calmar_ratio=result.calmar_ratio,
            profit_factor=result.profit_factor,
            expectancy_bps=result.expectancy_bps,
            time_in_market_pct=result.time_in_market_pct,
            trades_json=trades_json,
            equity_curve_json=equity_curve_json,
            note=args.note or f'sweep[{args.class_name}]',
        )
        rid = bstore.add(record)
        record.id = rid
        score = _bt_composite_score(record)
        results.append({
            'run_id': rid,
            'params': params,
            'score': score,
            'record': record,
        })
        if not _json_mode:
            console.print(f' run_id=[bold]#{rid}[/] score={score:+.3f}')

    successful = [r for r in results if 'error' not in r]
    successful.sort(key=lambda r: r['score'], reverse=True)
    errors = [r for r in results if 'error' in r]

    # 4. JSON / Rich leaderboard.
    if _json_mode:
        print(json.dumps({
            'data': {
                'total_combinations': len(combos),
                'successful': len(successful),
                'failed': len(errors),
                'leaderboard': [
                    {
                        'rank': rank,
                        'run_id': r['run_id'],
                        'params': r['params'],
                        'score': r['score'],
                        'total_return': r['record'].total_return,
                        'sharpe_ratio': r['record'].sharpe_ratio,
                        'sortino_ratio': r['record'].sortino_ratio,
                        'profit_factor': (
                            'inf' if r['record'].profit_factor > 1e15
                            else r['record'].profit_factor
                        ),
                        'expectancy_bps': r['record'].expectancy_bps,
                        'total_trades': r['record'].total_trades,
                        'max_drawdown': r['record'].max_drawdown,
                    }
                    for rank, r in enumerate(successful[:args.top], 1)
                ],
                'errors': [
                    {'params': r['params'], 'error': r['error']}
                    for r in errors
                ],
            },
            'title': f'Sweep: {args.class_name}',
        }, default=str))
        return

    from rich.table import Table
    from rich.text import Text
    from rich import box as _box

    top_n = min(args.top, len(successful))
    table = Table(
        title=f'{args.class_name} sweep — top {top_n} of '
              f'{len(combos)} combinations (ranked by composite score)',
        box=_box.ROUNDED, pad_edge=False,
    )
    table.add_column('rank', justify='right', width=4)
    table.add_column('id', justify='right', width=5)
    table.add_column('params', overflow='ellipsis', width=36)
    table.add_column('score', justify='right', width=7)
    table.add_column('trades', justify='right', width=6)
    table.add_column('return', justify='right', width=7)
    table.add_column('sharpe', justify='right', width=7)
    table.add_column('sortino', justify='right', width=7)
    table.add_column('pf', justify='right', width=6)
    table.add_column('exp_bps', justify='right', width=8)

    for rank, entry in enumerate(successful[:top_n], 1):
        r = entry['record']
        pdisplay = ', '.join(f'{k}={v}' for k, v in sorted(entry['params'].items()))
        pf_d = '∞' if r.profit_factor > 1e15 else f'{r.profit_factor:.2f}'
        table.add_row(
            str(rank),
            str(entry['run_id']),
            pdisplay,
            f'{entry["score"]:+.3f}',
            Text(str(r.total_trades),
                 style=_bt_style(_bt_class_trades, r.total_trades)),
            Text(f'{r.total_return:+.2%}',
                 style=_bt_style(_bt_class_return, r.total_return)),
            Text(f'{r.sharpe_ratio:+.2f}',
                 style=_bt_style(_bt_class_sharpe, r.sharpe_ratio)),
            Text(f'{r.sortino_ratio:+.2f}',
                 style=_bt_style(_bt_class_sortino, r.sortino_ratio)),
            Text(pf_d, style=_bt_style(_bt_class_pf, r.profit_factor)),
            Text(f'{r.expectancy_bps:+.1f}',
                 style=_bt_style(_bt_class_exp_bps, r.expectancy_bps)),
        )
    console.print(table)

    if errors:
        console.print(
            f'[yellow]{len(errors)}/{len(combos)} combination(s) failed — '
            f'first error: {errors[0]["error"]}[/yellow]'
        )


def _handle_backtest(args: argparse.Namespace):
    """Run a backtest using local data."""
    import datetime as dt
    import pandas as pd
    from trader.container import Container
    from trader.data.data_access import TickStorage
    from trader.data.universe import UniverseAccessor
    from trader.objects import BarSize
    from trader.simulation.backtester import Backtester, BacktestConfig

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')

    if not duckdb_path:
        print_status('duckdb_path not configured', success=False)
        return

    history_path = cfg.get('history_duckdb_path', '') or duckdb_path
    storage = TickStorage(history_path)

    # Resolve conids
    conids = args.conids
    if not conids and args.universe:
        accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))
        universe = accessor.get(args.universe)
        if not universe.security_definitions:
            print_status(f'Universe "{args.universe}" is empty', success=False)
            return
        conids = [d.conId for d in universe.security_definitions]

    if not conids:
        print_status('Specify --conids or --universe', success=False)
        return

    bar_size = BarSize.parse_str(getattr(args, 'bar_size', '1 min'))

    from trader.simulation.slippage import get_slippage_model
    slippage_name = getattr(args, 'slippage_model', 'fixed')
    slippage_bps = getattr(args, 'slippage_bps', 1.0)
    model_kwargs = {'bps': slippage_bps} if slippage_name == 'fixed' else {}
    slippage_model = get_slippage_model(slippage_name, **model_kwargs)

    config = BacktestConfig(
        start_date=dt.datetime.now() - dt.timedelta(days=args.days),
        end_date=dt.datetime.now(),
        initial_capital=args.capital,
        bar_size=bar_size,
        slippage_model=slippage_model,
    )

    backtester = Backtester(storage, config)

    # Merge --params JSON + --param KEY=VALUE into a single dict. Any
    # malformed entry is a user error — surface the exact failure without
    # running a 180s backtest that would be discarded anyway.
    try:
        param_overrides = _parse_backtest_param_args(
            getattr(args, 'params', None),
            getattr(args, 'param', []) or [],
        )
    except ValueError as e:
        print_status(str(e), success=False)
        return

    try:
        accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))
        result = backtester.run_from_module(
            args.strategy, args.class_name, conids,
            universe_accessor=accessor,
            params=param_overrides or None,
        )
    except FileNotFoundError as e:
        print_status(str(e), success=False)
        return
    except ValueError as e:
        print_status(str(e), success=False)
        return
    except Exception as e:
        print_status(f'{type(e).__name__}: {e}', success=False)
        return

    # Build summary. Distinguish "strategy emitted no trades" from "strategy
    # lost everything" — both used to render as `win_rate: 0.00%` which is
    # misleading when scanning `backtests list`.
    if result.total_trades == 0:
        win_rate_display = 'n/a (no trades)'
    else:
        win_rate_display = f'{result.win_rate:.2%}'

    # Profit factor is ∞ when a run has wins but no losses — render it as
    # the glyph rather than a raw 1e18 so humans can tell the difference
    # from "merely huge" values.
    pf = result.profit_factor
    pf_display = '∞' if pf > 1e15 else f'{pf:.2f}'

    summary = {
        'total_return':       f'{result.total_return:.2%}',
        'sharpe_ratio':       f'{result.sharpe_ratio:.2f}',
        'sortino_ratio':      f'{result.sortino_ratio:.2f}',
        'calmar_ratio':       f'{result.calmar_ratio:.2f}',
        'max_drawdown':       f'{result.max_drawdown:.2%}',
        'profit_factor':      pf_display,
        'expectancy_bps':     f'{result.expectancy_bps:+.1f} bps',
        'win_rate':           win_rate_display,
        'time_in_market':     f'{result.time_in_market_pct:.1%}',
        'total_trades':       result.total_trades,
        'start_date':         str(result.start_date)[:10],
        'end_date':           str(result.end_date)[:10],
        'final_equity':       f'{result.equity_curve.iloc[-1]:,.2f}' if len(result.equity_curve) > 0 else str(config.initial_capital),
    }

    # Sanity warning: sharpe on very short windows is statistically noise.
    # Surface it to stderr rather than burying it — real strategies rarely
    # have sharpe > 5.
    span_days = (config.end_date - config.start_date).days
    if (
        result.total_trades > 0
        and span_days < 14
        and abs(result.sharpe_ratio) > 5.0
        and not _json_mode
    ):
        console.print(
            f'[yellow]⚠ Sharpe {result.sharpe_ratio:.2f} over only {span_days} days '
            f'is almost certainly noise — extend the window before trusting this signal.[/yellow]'
        )

    # Persist the run to the backtest history store so it survives the
    # process. `--no-save` opts out; `--save-trades` opts in to persisting
    # the full trade + equity-curve detail alongside the summary metrics.
    run_id: Optional[int] = None
    if not getattr(args, 'no_save', False):
        try:
            from trader.data.backtest_store import (
                BacktestStore, BacktestRecord, compute_strategy_hash,
            )
            bstore = BacktestStore(duckdb_path)

            final_equity = (
                float(result.equity_curve.iloc[-1])
                if len(result.equity_curve) > 0 else float(config.initial_capital)
            )

            trades_json = ''
            equity_curve_json = ''
            if getattr(args, 'save_trades', False):
                # Coerce everything to native Python types — numpy scalars
                # (np.int64 from DataFrame groupby, np.float64 from arithmetic)
                # break json.dumps without a default= hook.
                trades_json = json.dumps([{
                    'timestamp': str(t.timestamp),
                    'conid': int(t.conid),
                    'action': str(t.action),
                    'quantity': float(t.quantity),
                    'price': float(t.price),
                    'commission': float(t.commission),
                    'signal_probability': float(t.signal_probability),
                    'signal_risk': float(t.signal_risk),
                } for t in result.trades])
                equity_curve_json = json.dumps([
                    {'timestamp': str(ts), 'value': float(v)}
                    for ts, v in result.equity_curve.items()
                ])

            # Capture effective --param overrides so `backtests show` can
            # display what was varied. ``applied_params`` is populated by
            # ``run_from_module`` after ``apply_param_overrides`` coerces
            # each entry to its class-attr type.
            params: Dict[str, Any] = dict(getattr(result, 'applied_params', {}) or {})

            record = BacktestRecord(
                strategy_path=args.strategy,
                class_name=args.class_name,
                conids=list(conids),
                universe=getattr(args, 'universe', '') or '',
                start_date=config.start_date,
                end_date=config.end_date,
                sortino_ratio=result.sortino_ratio,
                calmar_ratio=result.calmar_ratio,
                profit_factor=result.profit_factor,
                expectancy_bps=result.expectancy_bps,
                time_in_market_pct=result.time_in_market_pct,
                bar_size=str(bar_size),
                initial_capital=config.initial_capital,
                fill_policy=config.fill_policy,
                slippage_bps=slippage_bps if slippage_name == 'fixed' else 0.0,
                commission_per_share=config.commission_per_share,
                params=params,
                code_hash=compute_strategy_hash(args.strategy),
                total_trades=result.total_trades,
                total_return=result.total_return,
                sharpe_ratio=result.sharpe_ratio,
                max_drawdown=result.max_drawdown,
                win_rate=result.win_rate,
                final_equity=final_equity,
                trades_json=trades_json,
                equity_curve_json=equity_curve_json,
                note=getattr(args, 'note', '') or '',
                sweep_id=getattr(args, 'sweep_id', None),
            )
            run_id = bstore.add(record)
            summary['run_id'] = run_id
        except Exception as ex:
            if not _json_mode:
                console.print(f'[yellow]Could not persist backtest run: {ex}[/yellow]')

    if _json_mode:
        summary_only = getattr(args, 'summary_only', False)
        trades_data: List[Dict[str, Any]] = []
        if not summary_only:
            for t in result.trades:
                trades_data.append({
                    'timestamp': str(t.timestamp),
                    'conid': t.conid,
                    'action': str(t.action),
                    'quantity': t.quantity,
                    'price': t.price,
                    'commission': t.commission,
                    'signal_probability': t.signal_probability,
                    'signal_risk': t.signal_risk,
                })
        # JSON ∞ is non-standard; serialize it as the string "inf" so a
        # parser can branch on it without silently turning ∞ into null.
        def _jsonable_float(x: float):
            if x == float('inf'): return 'inf'
            if x == float('-inf'): return '-inf'
            if x != x: return None  # NaN
            return x

        data = {
            'summary': {
                'total_return':       result.total_return,
                'sharpe_ratio':       result.sharpe_ratio,
                'sortino_ratio':      result.sortino_ratio,
                'calmar_ratio':       result.calmar_ratio,
                'max_drawdown':       result.max_drawdown,
                'profit_factor':      _jsonable_float(result.profit_factor),
                'expectancy_bps':     result.expectancy_bps,
                # Mirror the Rich path: None when no trades, numeric otherwise.
                'win_rate':           (result.win_rate if result.total_trades > 0 else None),
                'time_in_market_pct': result.time_in_market_pct,
                'total_trades':       result.total_trades,
                'start_date':         str(result.start_date)[:10],
                'end_date':           str(result.end_date)[:10],
                'final_equity':       float(result.equity_curve.iloc[-1]) if len(result.equity_curve) > 0 else config.initial_capital,
                # run_id is populated above if persistence succeeded; None
                # if the user passed --no-save or persistence failed.
                'run_id':             run_id,
                'applied_params':     dict(getattr(result, 'applied_params', {}) or {}),
            },
        }
        if not summary_only:
            data['trades'] = trades_data
        print(json.dumps({
            'data': data,
            'title': f'Backtest: {args.class_name}',
        }, default=str))
        return

    # Rich output
    print_dict(summary, title=f'Backtest: {args.class_name}')

    if result.trades:
        rows = []
        for t in result.trades:
            rows.append({
                'time': str(t.timestamp)[:19],
                'conid': t.conid,
                'action': str(t.action),
                'qty': t.quantity,
                'price': f'{t.price:.2f}',
                'commission': f'{t.commission:.3f}',
                'prob': f'{t.signal_probability:.2f}',
            })
        print_df(pd.DataFrame(rows), title='Trades')


# ------------------------------------------------------------------
# Data handler (local-only, no service needed)
# ------------------------------------------------------------------

def _handle_data(args: argparse.Namespace):
    """Handle data subcommands."""
    action = getattr(args, 'data_action', None)
    if not action:
        console.print('[yellow]Usage: data summary|query|download|migrate-symbols[/yellow]')
        return

    if action == 'summary':
        _handle_data_summary()
    elif action == 'query':
        _handle_data_query(args)
    elif action == 'download':
        _handle_data_download(args)
    elif action == 'migrate-symbols':
        _handle_data_migrate_symbols(args)
    else:
        console.print(f'[yellow]Unknown data action: {action}[/yellow]')


def _handle_data_summary():
    """Show summary of all local historical data."""
    import pandas as pd
    from trader.container import Container
    from trader.data.data_access import TickStorage
    from trader.data.universe import UniverseAccessor

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')

    if not duckdb_path:
        print_status('duckdb_path not configured', success=False)
        return

    history_path = cfg.get('history_duckdb_path', '') or duckdb_path
    storage = TickStorage(history_path)
    accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))

    bar_sizes = storage.list_libraries()
    if not bar_sizes:
        print_df(pd.DataFrame(), title='Data Summary')
        return

    rows = []
    for bs_str in bar_sizes:
        from trader.objects import BarSize
        bs = BarSize.parse_str(bs_str)
        tickdata = storage.get_tickdata(bs)
        symbols = tickdata.list_symbols()
        for sym in symbols:
            # Storage keys can be either conIds (ints stored as strings) or
            # raw ticker strings — the latter happens when `data download`
            # runs for a symbol that isn't in the local universe yet. Try
            # conId-first and fall back to passing the key through as-is,
            # so tickers-only downloads still show up in the summary
            # instead of silently disappearing.
            name = sym
            con_id = None
            try:
                con_id = int(sym)
                storage_key = con_id
            except (TypeError, ValueError):
                storage_key = sym

            try:
                min_date, max_date = tickdata.date_summary(storage_key)
            except Exception:
                continue

            if con_id is not None:
                # Try to resolve conId back to a human symbol name.
                try:
                    results = accessor.resolve_symbol(con_id, first_only=True)
                    if results:
                        name = results[0].symbol
                except Exception:
                    pass

            rows.append({
                'conId': sym if con_id is not None else '',
                'symbol': name,
                'bar_size': bs_str,
                'start': str(min_date)[:19],
                'end': str(max_date)[:19],
            })

    print_df(pd.DataFrame(rows), title='Data Summary')


def _handle_data_query(args: argparse.Namespace):
    """Query local OHLCV data from DuckDB."""
    import datetime as dt
    import pandas as pd
    from trader.container import Container
    from trader.data.data_access import TickStorage
    from trader.data.store import DateRange
    from trader.data.universe import UniverseAccessor
    from trader.objects import BarSize

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')

    if not duckdb_path:
        print_status('duckdb_path not configured', success=False)
        return

    history_path = cfg.get('history_duckdb_path', '') or duckdb_path
    storage = TickStorage(history_path)
    accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))

    # Resolve symbol to the storage key used by `data download`:
    # conId when the ticker is registered in the universe, otherwise the raw
    # symbol string (which `data download` falls back to when it can't
    # resolve). Try conId-first so normal lookups still work for registered
    # symbols.
    symbol = args.symbol
    storage_key: Union[int, str]
    if symbol.isnumeric():
        storage_key = int(symbol)
    else:
        results = accessor.resolve_symbol(symbol, first_only=True)
        if results:
            storage_key = results[0].conId
        else:
            # Fall through to raw-symbol key so that data downloaded before
            # the symbol was registered is still readable.
            storage_key = symbol

    bar_size = BarSize.parse_str(getattr(args, 'bar_size', '1 day'))
    tickdata = storage.get_tickdata(bar_size)

    date_range = DateRange(
        start=dt.datetime.now() - dt.timedelta(days=args.days),
        end=dt.datetime.now()
    )

    df = tickdata.read(storage_key, date_range=date_range)
    if df.empty:
        print_df(pd.DataFrame(), title=f'Data: {symbol} ({bar_size})')
        return

    if args.tail:
        df = df.tail(args.tail)

    print_df(df, title=f'Data: {symbol} ({bar_size})')


def _try_get_exchange_calendar_for(security):
    """Best-effort exchange calendar lookup for a SecurityDefinition — mirrors
    DataService._try_get_exchange_calendar. Returns None if neither
    primaryExchange nor exchange resolves."""
    import exchange_calendars
    for attr in ('primaryExchange', 'exchange'):
        name = getattr(security, attr, None)
        if not name:
            continue
        try:
            return exchange_calendars.get_calendar(name)
        except Exception:
            continue
    return None


def _handle_data_download(args: argparse.Namespace):
    """Download data from the selected source directly to local DuckDB.

    Incremental by default: uses TickData.missing() to skip date ranges
    already present in storage. If the symbol can't be resolved to a
    SecurityDefinition (no local universe entry and trader_service down),
    falls back to a full-range download keyed by the ticker string.
    """
    import datetime as dt
    from trader.container import Container
    from trader.data.data_access import TickStorage
    from trader.data.store import DateRange
    from trader.data.universe import UniverseAccessor
    from trader.objects import BarSize

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')
    source = getattr(args, 'source', 'massive')

    if source == 'twelvedata':
        api_key = cfg.get('twelvedata_api_key', '')
        if not api_key:
            print_status('twelvedata_api_key not configured (set TWELVEDATA_API_KEY env var)', success=False)
            return
    else:
        api_key = cfg.get('massive_api_key', '')
        if not api_key:
            print_status('massive_api_key not configured in trader.yaml', success=False)
            return

    if not duckdb_path:
        print_status('duckdb_path not configured', success=False)
        return

    bar_size = BarSize.parse_str(getattr(args, 'bar_size', '1 day'))
    history_path = cfg.get('history_duckdb_path', '') or duckdb_path
    storage = TickStorage(history_path)
    accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))

    if source == 'twelvedata':
        from trader.listeners.twelvedata_history import TwelveDataHistoryWorker
        worker = TwelveDataHistoryWorker(twelvedata_api_key=api_key)
    else:
        from trader.listeners.massive_history import MassiveHistoryWorker
        worker = MassiveHistoryWorker(massive_api_key=api_key)

    # If trader_service is reachable, fall back to it for symbols that aren't
    # in the local universe so rows end up conId-keyed (instead of strings
    # like "JPM"). Without this, tickers the user hasn't explicitly added to
    # a universe get persisted under their symbol string and show up with
    # a blank conId in `data summary`. Timeout is kept tight (1s) so a
    # misconfigured or down trader_service doesn't make the CLI look hung.
    rpc_mmr = None
    try:
        from trader.sdk import MMR  # local import — avoids SDK cost when unused
        candidate = MMR(
            rpc_address=cfg.get('zmq_rpc_server_address'),
            rpc_port=cfg.get('zmq_rpc_server_port'),
            timeout=1,
        )
        candidate.connect()
        # Soft probe — if trader_service isn't up, skip silently
        candidate._rpc.rpc().get_status()  # type: ignore[attr-defined]
        rpc_mmr = candidate
    except Exception:
        if rpc_mmr is not None:
            try: rpc_mmr.close()
            except Exception: pass
            rpc_mmr = None

    end_date = dt.datetime.now()
    start_date = end_date - dt.timedelta(days=args.days)
    completed = 0
    failed = 0
    skipped_up_to_date = 0
    total_rows_written = 0
    tickdata = storage.get_tickdata(bar_size)

    for symbol in args.symbols:
        # Resolve to conId for storage key: local universe first, then
        # trader_service RPC if available. Registering a new resolution in
        # the universe means subsequent downloads (and anything else that
        # calls resolve_symbol locally) won't need to hit IB again.
        conid = None
        sec_def = None
        results = accessor.resolve_symbol(symbol, first_only=True)
        if results:
            sec_def = results[0]
            conid = sec_def.conId
        elif rpc_mmr is not None:
            try:
                resolved = rpc_mmr.resolve(symbol)  # List[SecurityDefinition]
                if resolved:
                    sec_def = resolved[0]
                    conid = sec_def.conId
                    # Persist into a "downloads" universe so the resolution
                    # sticks for future runs — matches the auto-register
                    # behaviour of `propose --group`.
                    universe_name = 'downloads'
                    try:
                        u = accessor.get(universe_name)
                    except Exception:
                        u = None
                    if u is None:
                        from trader.data.universe import Universe
                        u = Universe(name=universe_name, security_definitions=[sec_def])
                    else:
                        if not any(sd.conId == sec_def.conId for sd in u.security_definitions):
                            u.security_definitions.append(sec_def)
                    accessor.update(u)
                    if not _json_mode:
                        console.print(f'[dim]  registered {symbol} → conId {conid} in universe "downloads"[/dim]')
            except Exception as ex:
                if not _json_mode:
                    console.print(f'[dim]  resolve via trader_service failed for {symbol}: {ex}[/dim]')

        # Freshness guard: use tick_data.missing() to skip date ranges already
        # stored, so repeat runs are cheap (no API calls when fully covered).
        # Only possible when we have a SecurityDefinition for the exchange
        # calendar lookup — otherwise fall back to full-range download.
        # --force bypasses the guard (needed when re-fetching existing days to
        # add coverage, e.g. toggling TwelveData extended-hours bars on).
        missing_ranges = None
        if not getattr(args, 'force', False) and sec_def is not None:
            calendar = _try_get_exchange_calendar_for(sec_def)
            if calendar is not None:
                try:
                    missing_ranges = tickdata.missing(
                        conid,
                        calendar,
                        date_range=DateRange(start=start_date, end=end_date),
                    )
                except Exception as ex:
                    if not _json_mode:
                        console.print(f'[dim]  missing() failed for {symbol}: {ex} — full-range fetch[/dim]')
                    missing_ranges = None

        if missing_ranges is not None and len(missing_ranges) == 0:
            skipped_up_to_date += 1
            if not _json_mode:
                console.print(f'[dim]{symbol}: already up to date[/dim]')
            continue

        fetch_ranges = missing_ranges if missing_ranges else [DateRange(start=start_date, end=end_date)]

        symbol_rows = 0
        symbol_failed = False
        for dr in fetch_ranges:
            try:
                if not _json_mode:
                    console.print(
                        f'[dim]Downloading {symbol} ({bar_size}) '
                        f'{dr.start.strftime("%Y-%m-%d")} → {dr.end.strftime("%Y-%m-%d")}...[/dim]'
                    )
                df = worker.get_history(
                    ticker=symbol,
                    bar_size=bar_size,
                    start_date=dr.start,
                    end_date=dr.end,
                )
                if not df.empty:
                    storage_key = conid if conid else symbol
                    tickdata.write_resolve_overlap(storage_key, df)
                    symbol_rows += len(df)
            except Exception as e:
                symbol_failed = True
                if not _json_mode:
                    console.print(f'[red]  Error downloading {symbol} {dr.start.strftime("%Y-%m-%d")}-{dr.end.strftime("%Y-%m-%d")}: {e}[/red]')

        if symbol_rows > 0:
            completed += 1
            total_rows_written += symbol_rows
            if not _json_mode:
                console.print(f'[dim]  Wrote {symbol_rows:,} rows for {symbol}[/dim]')
        elif symbol_failed:
            failed += 1
        else:
            # No rows and no failure: upstream returned empty for every range.
            # That's legitimate (e.g. weekend-only window) — don't mark failed.
            if not _json_mode:
                console.print(f'[yellow]  No data returned for {symbol}[/yellow]')

    if rpc_mmr is not None:
        try: rpc_mmr.close()
        except Exception: pass

    if _json_mode:
        print(json.dumps({
            'success': failed == 0,
            'message': (f'{completed} downloaded, {skipped_up_to_date} already current, '
                        f'{failed} failed, {total_rows_written} rows written'),
            'completed': completed,
            'skipped_up_to_date': skipped_up_to_date,
            'failed': failed,
            'rows_written': total_rows_written,
        }))
    else:
        print_status(
            f'Download complete: {completed} downloaded, '
            f'{skipped_up_to_date} already current, {failed} failed, '
            f'{total_rows_written:,} rows written'
        )


def _handle_data_migrate_symbols(args: argparse.Namespace):
    """Rewrite tick_data rows whose 'symbol' column is a raw ticker string
    into conId-keyed rows, resolving via trader_service (and registering in
    the ``downloads`` universe). Idempotent; skips symbols that can't be
    resolved and leaves their rows untouched."""
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import UniverseAccessor, Universe

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')
    history_path = cfg.get('history_duckdb_path', '') or duckdb_path
    accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))

    db = DuckDBConnection.get_instance(history_path)
    # Find all distinct non-numeric storage keys
    rows = db.execute(
        "SELECT DISTINCT symbol FROM tick_data WHERE NOT regexp_matches(symbol, '^[0-9]+$')",
        fetch='all',
    ) or []
    string_symbols = [r[0] for r in rows]

    if not string_symbols:
        print_status('Nothing to migrate — all rows already conId-keyed.')
        return

    if not _json_mode:
        console.print(f'Found [bold]{len(string_symbols)}[/bold] symbol(s) with string keys: {", ".join(string_symbols)}')

    # Spin up a short-lived RPC client
    from trader.sdk import MMR as _MMR
    mmr = _MMR(
        rpc_address=cfg.get('zmq_rpc_server_address'),
        rpc_port=cfg.get('zmq_rpc_server_port'),
        timeout=5,
    )
    try:
        mmr.connect()
    except Exception as ex:
        print_status(f'trader_service not reachable — cannot resolve: {ex}', success=False)
        return

    migrated = 0
    skipped = 0
    try:
        for symbol in string_symbols:
            try:
                resolved = mmr.resolve(symbol)  # List[SecurityDefinition]
                if not resolved:
                    if not _json_mode:
                        console.print(f'[yellow]  {symbol}: could not resolve via IB — skipping[/yellow]')
                    skipped += 1
                    continue
                sec_def = resolved[0]
                conid = str(sec_def.conId)

                # Register in universe
                try:
                    u = accessor.get('downloads')
                except Exception:
                    u = None
                if u is None:
                    u = Universe(name='downloads', security_definitions=[sec_def])
                else:
                    if not any(sd.conId == sec_def.conId for sd in u.security_definitions):
                        u.security_definitions.append(sec_def)
                accessor.update(u)

                # Atomically re-key the tick_data rows
                def _rekey(conn, _sym=symbol, _cid=conid):
                    conn.execute(
                        "UPDATE tick_data SET symbol = ? WHERE symbol = ?",
                        [_cid, _sym],
                    )
                db.execute_atomic(_rekey)

                if not _json_mode:
                    console.print(f'[green]  ✓ {symbol} → conId {conid}[/green]')
                migrated += 1
            except Exception as ex:
                if not _json_mode:
                    console.print(f'[red]  {symbol}: migration failed: {ex}[/red]')
                skipped += 1
    finally:
        try: mmr.close()
        except Exception: pass

    if _json_mode:
        print(json.dumps({
            'success': skipped == 0,
            'migrated': migrated,
            'skipped': skipped,
            'total': len(string_symbols),
        }))
    else:
        print_status(f'Migration complete: {migrated} migrated, {skipped} skipped/failed')


def _handle_depth(mmr: MMR, args: argparse.Namespace):
    """Fetch and display market depth (Level 2 order book)."""
    from trader.tools.depth_chart import render_depth_chart, render_depth_table
    from datetime import datetime
    import subprocess

    try:
        data = mmr.depth(
            args.symbol,
            num_rows=args.rows,
            exchange=args.exchange,
            currency=args.currency,
            is_smart_depth=args.smart,
        )
    except ValueError as e:
        if _json_mode:
            print(json.dumps({'success': False, 'message': str(e)}))
        else:
            console.print(f'[red]Error: {e}[/red]')
            if args.exchange:
                console.print(
                    f'[yellow]Hint: verify the exchange code is correct '
                    f'(e.g. ASX, not Ax). Common codes: ASX, TSE, SEHK, NYSE, NASDAQ.[/yellow]'
                )
            else:
                console.print(
                    '[yellow]Hint: for international stocks, use --exchange and --currency '
                    '(e.g. depth STO --exchange ASX --currency AUD)[/yellow]'
                )
        return
    except Exception as e:
        if _json_mode:
            print(json.dumps({'success': False, 'message': str(e)}))
        else:
            console.print(f'[red]Error: {e}[/red]')
        return

    # Rich table to terminal
    if not _json_mode:
        table = render_depth_table(data)
        console.print(table)

    # Render chart PNG
    chart_path = None
    if not args.no_chart:
        depth_dir = Path.home() / '.local' / 'share' / 'mmr' / 'depth'
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        png_path = depth_dir / f'{args.symbol.upper()}_{ts}.png'
        chart_path = render_depth_chart(data, str(png_path))

        if not _json_mode:
            console.print(f'[dim]Chart saved: {chart_path}[/dim]')

        # Open in macOS Preview
        if not args.no_open:
            try:
                subprocess.run(['open', chart_path], check=False)
            except FileNotFoundError:
                pass  # not on macOS

    if _json_mode:
        output = {'data': data, 'chart_path': chart_path}
        print(json.dumps(output, default=str))


def _handle_listen(mmr: MMR, args: argparse.Namespace):
    """Stream ticks until Ctrl+C."""
    console.print(f'[dim]Listening to {args.symbol} (Ctrl+C to stop)...[/dim]')

    def on_tick(tick):
        if hasattr(tick, 'last') and hasattr(tick, 'bid') and hasattr(tick, 'ask'):
            symbol_name = tick.contract.symbol if hasattr(tick, 'contract') and tick.contract else args.symbol
            console.print(
                f'  {symbol_name}  '
                f'bid={getattr(tick, "bid", "")}'
                f'  ask={getattr(tick, "ask", "")}'
                f'  last={getattr(tick, "last", "")}'
                f'  vol={getattr(tick, "volume", "")}'
            )
        else:
            console.print(f'  {tick}')

    sub = mmr.subscribe_ticks(args.symbol, on_tick, topic=args.topic,
                              exchange=args.exchange, currency=args.currency)
    try:
        while sub.is_active():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        sub.stop()
        console.print('[dim]Stopped listening.[/dim]')


def _handle_history(mmr: MMR, args: argparse.Namespace):
    """Download or list historical data."""
    action = getattr(args, 'hist_action', None)
    if not action:
        console.print('[yellow]Usage: history list|massive|twelvedata|ib [options][/yellow]')
        return

    if action in ('list', 'ls'):
        import pandas as pd
        symbol = getattr(args, 'symbol', None)
        bar_size = getattr(args, 'bar_size', None)
        df = mmr.history_list(symbol=symbol, bar_size=bar_size)
        if df.empty:
            print_df(pd.DataFrame(), title='Downloaded History')
            return

        if _json_mode:
            print_df(df, title='Downloaded History')
            return

        from rich.table import Table as RichTable
        table = RichTable(title='Downloaded History')
        table.add_column('conId', style='dim')
        table.add_column('Symbol', style='cyan bold')
        table.add_column('Bar Size', style='green')
        table.add_column('Start', style='white')
        table.add_column('End', style='white')
        table.add_column('Rows', justify='right', style='yellow')

        for _, row in df.iterrows():
            start_str = str(row['start'])[:19] if pd.notna(row['start']) else ''
            end_str = str(row['end'])[:19] if pd.notna(row['end']) else ''
            table.add_row(
                str(row['symbol']),
                row['name'] or '-',
                row['bar_size'] or '-',
                start_str,
                end_str,
                f"{int(row['rows']):,}",
            )

        console.print(table)
        total_rows = int(df['rows'].sum())
        console.print(f'[dim]{len(df)} entries, {total_rows:,} total rows[/dim]')
        return

    if not args.symbol and not args.universe:
        console.print('[red]Specify --symbol or --universe[/red]')
        return

    symbols = [args.symbol] if args.symbol else None
    universe = args.universe

    if action == 'massive':
        console.print('[dim]Pulling Massive history via data_service...[/dim]')
        result = mmr.pull_massive(
            symbols=symbols,
            universe=universe,
            bar_size=args.bar_size,
            prev_days=args.prev_days,
        )
    elif action in ('twelvedata', 'td'):
        console.print('[dim]Pulling TwelveData history via data_service...[/dim]')
        result = mmr.pull_twelvedata(
            symbols=symbols,
            universe=universe,
            bar_size=args.bar_size,
            prev_days=args.prev_days,
        )
    elif action == 'ib':
        console.print('[dim]Pulling IB history via data_service...[/dim]')
        result = mmr.pull_ib(
            symbols=symbols,
            universe=universe,
            bar_size=args.bar_size,
            prev_days=args.prev_days,
            ib_client_id=args.ib_client_id,
        )
    else:
        console.print(f'[yellow]Unknown history action: {action}[/yellow]')
        return

    if result.get('errors'):
        for err in result['errors']:
            console.print(f'[red]  {err}[/red]')

    console.print(
        f'[green]Done: {result["enqueued"]} enqueued, '
        f'{result["completed"]} completed, '
        f'{result["failed"]} failed[/green]'
    )


def _fmt_financial_value(val) -> str:
    """Format a financial value for display (human-readable with M/B suffixes)."""
    if val is None or (isinstance(val, float) and (val != val)):  # NaN check
        return '-'
    if isinstance(val, (int, float)):
        abs_val = abs(val)
        if abs_val >= 1_000_000_000:
            return f'{val / 1_000_000_000:,.2f}B'
        elif abs_val >= 1_000_000:
            return f'{val / 1_000_000:,.1f}M'
        elif abs_val >= 1_000:
            return f'{val:,.0f}'
        else:
            return f'{val:,.2f}'
    return str(val)


def _print_financial_df(df, title=None):
    """Render a financial DataFrame transposed: line items as rows, periods as columns."""
    if df is None or df.empty:
        console.print("[dim]No data[/dim]")
        return

    # Build period labels from period_end (+ fiscal quarter if available)
    period_labels = []
    for _, row in df.iterrows():
        label = str(row.get('period_end', ''))[:10]
        fq = row.get('fiscal_quarter')
        fy = row.get('fiscal_year')
        if fq is not None and fy is not None:
            label = f"Q{int(fq)} {int(fy)}"
        elif fy is not None:
            label = f"FY {int(fy)}"
        period_labels.append(label)

    # Columns to skip in the transposed view (metadata, not line items)
    skip = {'cik', 'tickers', 'timeframe', 'filing_date', 'period_end', 'fiscal_year', 'fiscal_quarter'}
    line_items = [c for c in df.columns if c not in skip]

    table = Table(title=title, show_lines=False)
    table.add_column('', style='bold')  # line item name
    for label in period_labels:
        table.add_column(label, justify='right')

    for item in line_items:
        label = item.replace('_', ' ').title()
        cells = [_fmt_financial_value(df.iloc[i][item]) for i in range(len(df))]
        table.add_row(label, *cells)

    console.print(table)


def _handle_financials(mmr: MMR, args: argparse.Namespace):
    """Fetch financial statements from Massive.com."""
    import logging
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    action = getattr(args, 'fin_action', None)
    if not action:
        console.print('[yellow]Usage: financials balance|income|cashflow|ratios|filing SYMBOL [options][/yellow]')
        return

    symbol = args.symbol.upper()
    source = getattr(args, 'source', 'massive')

    try:
        if action == 'ratios':
            df = mmr.ratios(symbol, source=source)
            if df is None or df.empty:
                console.print('[dim]No data[/dim]')
                return
            # Display as key-value pairs (single row of ratios)
            row = df.iloc[0]
            skip = {'cik', 'ticker', 'symbol', 'name', 'exchange', 'currency'}
            data = {
                k.replace('_', ' ').title(): _fmt_financial_value(row[k])
                for k in df.columns if k not in skip
            }
            title_suffix = ' (TTM)' if source == 'massive' else ' — TwelveData'
            print_dict(data, title=f'Financial Ratios: {symbol}{title_suffix}')
        elif action == 'filing':
            if source == 'twelvedata':
                console.print('[red]`financials filing` is Massive.com-only — '
                              'TwelveData does not expose filing sections.[/red]')
                return
            section = args.section
            limit = args.limit
            results = mmr.filing_sections(symbol, section=section, limit=limit)
            if not results:
                console.print('[dim]No data[/dim]')
                return
            for filing in results:
                section_label = section.replace('_', ' ').title()
                console.print(f'\n[bold]{symbol} — {section_label}[/bold]')
                console.print(f'[dim]Period: {filing.get("period_end", "?")}  '
                              f'Filed: {filing.get("filing_date", "?")}  '
                              f'URL: {filing.get("filing_url", "")}[/dim]')
                console.print()
                text = filing.get('text', '')
                if text:
                    console.print(text)
                else:
                    console.print('[dim]No text content[/dim]')
                console.print()
        else:
            limit = args.limit
            timeframe = args.timeframe
            title_src = ' — TwelveData' if source == 'twelvedata' else ''
            if action == 'balance':
                df = mmr.balance_sheet(symbol, limit=limit, timeframe=timeframe, source=source)
                _print_financial_df(df, title=f'Balance Sheet: {symbol} ({timeframe}){title_src}')
            elif action == 'income':
                df = mmr.income_statement(symbol, limit=limit, timeframe=timeframe, source=source)
                _print_financial_df(df, title=f'Income Statement: {symbol} ({timeframe}){title_src}')
            elif action == 'cashflow':
                df = mmr.cash_flow(symbol, limit=limit, timeframe=timeframe, source=source)
                _print_financial_df(df, title=f'Cash Flow: {symbol} ({timeframe}){title_src}')
            else:
                console.print(f'[yellow]Unknown financials action: {action}[/yellow]')
    except ValueError as e:
        console.print(f'[red]{e}[/red]')


def _parse_relative_expiration(expr: str) -> int:
    """Parse a relative expiration like '90d', '3m', '90 days', '3 months' into target days from today.

    Returns the number of calendar days the expression represents.
    """
    import re
    expr = expr.strip().lower()

    # Try YYYY-MM-DD first — not relative
    if re.match(r'^\d{4}-\d{2}-\d{2}$', expr):
        return -1  # signal: it's an exact date

    m = re.match(r'^(\d+)\s*(d|day|days|w|week|weeks|m|mo|month|months|y|year|years)$', expr)
    if not m:
        return -1  # can't parse, treat as exact date string

    n = int(m.group(1))
    unit = m.group(2)

    if unit in ('d', 'day', 'days'):
        return n
    elif unit in ('w', 'week', 'weeks'):
        return n * 7
    elif unit in ('m', 'mo', 'month', 'months'):
        return n * 30
    elif unit in ('y', 'year', 'years'):
        return n * 365
    return -1


def _resolve_expiration(mmr: MMR, symbol: str, expiration_arg: str | None, default_days: int = 90) -> str | None:
    """Resolve an expiration argument to a concrete YYYY-MM-DD date.

    Handles:
    - None → default (~default_days out, find closest available)
    - '2026-03-20' → exact date (returned as-is)
    - '90d', '3m', '6 months' → relative, find closest available expiration

    Returns the resolved expiration string, or None if no expirations found.
    """
    import datetime as dt_mod

    if expiration_arg is None:
        target_days = default_days
    else:
        target_days = _parse_relative_expiration(expiration_arg)
        if target_days == -1:
            # It's an exact date string, return as-is
            return expiration_arg

    # Fetch available expirations and find the closest to target
    dates = mmr.options_expirations(symbol)
    if not dates:
        return None

    today = dt_mod.date.today()
    target_date = today + dt_mod.timedelta(days=target_days)

    # Find closest expiration to target
    best = None
    best_diff = float('inf')
    for d in dates:
        exp_date = dt_mod.datetime.strptime(d, '%Y-%m-%d').date()
        diff = abs((exp_date - target_date).days)
        if diff < best_diff:
            best_diff = diff
            best = d

    return best


def _handle_news(mmr: MMR, args: argparse.Namespace):
    """Fetch news from Massive.com."""
    import logging as _logging
    _logging.getLogger('urllib3').setLevel(_logging.WARNING)

    ticker = args.ticker.upper() if args.ticker else None
    title_label = f'News: {ticker}' if ticker else 'Market News'

    try:
        if args.detail:
            articles = mmr.news_detail(
                ticker=ticker, limit=args.limit, source=args.source,
            )
            if not articles:
                console.print('[dim]No articles found[/dim]')
                return
            for a in articles:
                tickers_str = ', '.join(a.get('tickers', []))
                console.print(f'\n[bold]{a["title"]}[/bold]')
                console.print(
                    f'[dim]{a["published"]}  |  {a["author"]}'
                    f'{"  |  " + tickers_str if tickers_str else ""}[/dim]'
                )
                desc = a.get('description') or a.get('teaser', '')
                if desc:
                    console.print(desc)
                insights = a.get('insights', [])
                for ins in insights:
                    sentiment = ins.get('sentiment', '')
                    color = 'green' if sentiment == 'positive' else 'red' if sentiment == 'negative' else 'yellow'
                    reasoning = ins.get('reasoning', '')
                    console.print(
                        f'  [{color}]{ins.get("ticker", "")}: {sentiment}[/{color}]'
                        f'{"  — " + reasoning if reasoning else ""}'
                    )
                url = a.get('url', '')
                if url:
                    console.print(f'[dim]{url}[/dim]')
        else:
            df = mmr.news(ticker=ticker, limit=args.limit, source=args.source)
            if df.empty:
                console.print('[dim]No articles found[/dim]')
                return
            # Truncate title for table display
            if 'title' in df.columns:
                df['title'] = df['title'].str[:80]
            if 'teaser' in df.columns:
                df['teaser'] = df['teaser'].str[:60]
            print_df(df, title=title_label)
    except ValueError as e:
        console.print(f'[red]{e}[/red]')


def _handle_options(mmr: MMR, args: argparse.Namespace):
    """Options data and trading commands."""
    import datetime as dt_mod

    action = getattr(args, 'opt_action', None)
    if not action:
        console.print('[yellow]Usage: options expirations|chain|snapshot|implied|buy|sell[/yellow]')
        return

    import logging as _logging
    _logging.getLogger('urllib3').setLevel(_logging.WARNING)

    if action in ('expirations', 'exp'):
        symbol = args.symbol.upper()
        dates = mmr.options_expirations(symbol)
        if not dates:
            console.print(f'[dim]No expiration dates found for {symbol}[/dim]')
            return
        import pandas as pd
        rows = []
        today = dt_mod.date.today()
        for d in dates:
            exp_date = dt_mod.datetime.strptime(d, '%Y-%m-%d').date()
            dte = (exp_date - today).days
            rows.append({'expiration': d, 'DTE': dte})
        print_df(pd.DataFrame(rows), title=f'Expirations: {symbol}')

    elif action == 'chain':
        symbol = args.symbol.upper()
        expiration = _resolve_expiration(mmr, symbol, args.expiration) if args.expiration else None
        if args.expiration and expiration is None:
            console.print(f'[dim]No expiration dates found for {symbol}[/dim]')
            return
        if expiration and expiration != args.expiration:
            console.print(f'[dim]Using expiration: {expiration}[/dim]')
        df = mmr.options_chain(
            symbol,
            expiration=expiration,
            contract_type=args.contract_type,
            strike_min=args.strike_min,
            strike_max=args.strike_max,
        )
        if df.empty:
            console.print(f'[dim]No chain data for {symbol}[/dim]')
            return

        # Format for display
        table = Table(title=f'Options Chain: {symbol} ({df.iloc[0]["expiration"]})')
        table.add_column('type', style='bold')
        for col in ['strike', 'bid', 'ask', 'mid', 'last', 'volume', 'OI', 'iv%',
                     'delta', 'gamma', 'theta', 'vega', 'break_even']:
            table.add_column(col, justify='right')

        for _, row in df.iterrows():
            ct = row['type']
            type_style = '[cyan]call[/cyan]' if ct == 'call' else '[magenta]put[/magenta]'
            table.add_row(
                type_style,
                f'{row["strike"]:.2f}',
                f'{row["bid"]:.2f}',
                f'{row["ask"]:.2f}',
                f'{row["mid"]:.2f}',
                f'{row["last"]:.2f}',
                f'{row["volume"]:.0f}',
                f'{row["open_interest"]:.0f}',
                f'{row["iv"]:.1f}%',
                f'{row["delta"]:.4f}',
                f'{row["gamma"]:.4f}',
                f'{row["theta"]:.4f}',
                f'{row["vega"]:.4f}',
                f'{row["break_even"]:.2f}',
            )
        console.print(table)

    elif action in ('snapshot', 'snap'):
        result = mmr.options_snapshot(args.ticker)
        print_dict(result, title=f'Option Snapshot: {args.ticker}')

    elif action == 'implied':
        symbol = args.symbol.upper()
        expiration = _resolve_expiration(mmr, symbol, args.expiration)
        if not expiration:
            console.print(f'[dim]No expiration dates found for {symbol}[/dim]')
            return
        if expiration != args.expiration:
            console.print(f'[dim]Using expiration: {expiration}[/dim]')
        data = mmr.options_implied(symbol, expiration, args.risk_free_rate)
        from trader.tools.chain import plot_market_implied_vs_constant_console
        plot_market_implied_vs_constant_console(
            data['x'], data['market_implied'], data['constant'],
            f'{symbol} for {expiration}, constant vs market implied'
        )

    elif action == 'buy':
        symbol = args.symbol.upper()
        if not args.market and args.limit is None:
            console.print('[red]Specify --market or --limit[/red]')
            return
        expiration = _resolve_expiration(mmr, symbol, args.expiration)
        if not expiration:
            console.print(f'[dim]No expiration dates found for {symbol}[/dim]')
            return
        if expiration != args.expiration:
            console.print(f'[dim]Using expiration: {expiration}[/dim]')
        result = mmr.buy_option(
            symbol, expiration, args.strike, args.right,
            args.quantity, limit_price=args.limit, market=args.market,
        )
        _print_trade_result(result, 'BUY', f'{symbol} {expiration} {args.strike}{args.right}')

    elif action == 'sell':
        symbol = args.symbol.upper()
        if not args.market and args.limit is None:
            console.print('[red]Specify --market or --limit[/red]')
            return
        expiration = _resolve_expiration(mmr, symbol, args.expiration)
        if not expiration:
            console.print(f'[dim]No expiration dates found for {symbol}[/dim]')
            return
        if expiration != args.expiration:
            console.print(f'[dim]Using expiration: {expiration}[/dim]')
        result = mmr.sell_option(
            symbol, expiration, args.strike, args.right,
            args.quantity, limit_price=args.limit, market=args.market,
        )
        _print_trade_result(result, 'SELL', f'{symbol} {expiration} {args.strike}{args.right}')

    else:
        console.print(f'[yellow]Unknown options action: {action}[/yellow]')


def _handle_forex(mmr: MMR, args: argparse.Namespace):
    """Forex data commands."""
    import logging as _logging
    _logging.getLogger('urllib3').setLevel(_logging.WARNING)

    action = getattr(args, 'fx_action', None)
    if not action:
        console.print('[yellow]Usage: forex snapshot|snapshot-all|movers|quote|convert[/yellow]')
        return

    try:
        if action in ('snapshot', 'snap'):
            source = getattr(args, 'source', 'ib')
            result = mmr.forex_snapshot(args.pair.upper(), source=source)
            print_dict(result, title=f'Forex Snapshot: {args.pair.upper()} ({source})')

        elif action == 'snapshot-all':
            df = mmr.forex_snapshot_all()
            print_df(df, title='Forex Snapshots')

        elif action == 'movers':
            direction = 'losers' if args.losers else 'gainers'
            df = mmr.forex_movers(direction=direction)
            print_df(df, title=f'Forex Movers ({direction})')

        elif action == 'quote':
            source = getattr(args, 'source', 'ib')
            result = mmr.forex_quote(args.from_currency.upper(), args.to_currency.upper(), source=source)
            print_dict(result, title=f'Forex Quote: {args.from_currency.upper()}/{args.to_currency.upper()} ({source})')

        elif action == 'convert':
            result = mmr.forex_convert(args.from_currency.upper(), args.to_currency.upper(), args.amount)
            print_dict(result, title=f'Convert: {args.amount} {args.from_currency.upper()} → {args.to_currency.upper()}')

        else:
            console.print(f'[yellow]Unknown forex action: {action}[/yellow]')
    except ValueError as e:
        console.print(f'[red]{e}[/red]')


SCAN_PRESETS = {
    'gainers': 'TOP_PERC_GAIN',
    'losers': 'TOP_PERC_LOSE',
    'active': 'MOST_ACTIVE',
    'hot-volume': 'HOT_BY_VOLUME',
    'hot-price': 'HOT_BY_PRICE',
}


def _handle_movers(mmr: MMR, args: argparse.Namespace):
    import logging as _logging
    _logging.getLogger('urllib3').setLevel(_logging.WARNING)

    direction = 'losers' if args.losers else 'gainers'
    market = args.market
    source = getattr(args, 'source', 'massive')

    if not args.detail:
        df = mmr.movers(market=market, direction=direction, source=source)
        if args.num and len(df) > args.num:
            df = df.head(args.num)
        title_suffix = ' — TwelveData' if source == 'twelvedata' else ''
        print_df(df, title=f'{market.title()} Movers ({direction}){title_suffix}')
        return

    # Detail card view — Massive-only (needs ratios + news enrichment that
    # TwelveData's Python client doesn't provide for this view).
    if source == 'twelvedata':
        console.print('[yellow]--detail is Massive-only. Dropping to table view for TwelveData.[/yellow]')
        df = mmr.movers(market=market, direction=direction, source='twelvedata')
        if args.num and len(df) > args.num:
            df = df.head(args.num)
        print_df(df, title=f'{market.title()} Movers ({direction}) — TwelveData')
        return
    movers = mmr.movers_detail(market=market, direction=direction, num=args.num)
    if not movers:
        console.print('[dim]No data[/dim]')
        return

    console.print(f'[bold]{market.title()} Movers ({direction}) — {len(movers)} results[/bold]\n')

    for m in movers:
        ticker = m['ticker']
        details = m.get('details', {})
        ratios = m.get('ratios', {})
        news = m.get('news', {})

        name = details.get('name', '')
        change = m.get('change', 0) or 0
        change_pct = m.get('change_pct', 0) or 0
        close = m.get('close', 0) or 0
        volume = m.get('volume', 0) or 0
        mkt_cap = details.get('market_cap')

        color = 'green' if change >= 0 else 'red'
        sign = '+' if change > 0 else ''

        # Header: ticker, price, change from open
        open_price = m.get('open', 0) or 0
        open_str = f' from ${open_price:,.2f}' if open_price else ''
        console.print(
            f'[bold]{ticker}[/bold]  '
            f'${close:,.2f}  '
            f'[{color}]{sign}{change:,.2f} ({sign}{change_pct:.2f}%){open_str}[/{color}]'
        )

        # Company name
        if name:
            console.print(f'[dim]{name}[/dim]')

        # Stats line: volume, market cap, ratios
        stats = [f'vol {volume:,.0f}']
        if mkt_cap:
            if mkt_cap >= 1e12:
                stats.append(f'cap ${mkt_cap/1e12:.1f}T')
            elif mkt_cap >= 1e9:
                stats.append(f'cap ${mkt_cap/1e9:.1f}B')
            elif mkt_cap >= 1e6:
                stats.append(f'cap ${mkt_cap/1e6:.0f}M')
        for label in ('pe', 'de', 'roe', 'eps'):
            val = ratios.get(label)
            if val is not None:
                stats.append(f'{label.upper()} {val:g}')
        console.print(f'[dim]{" | ".join(stats)}[/dim]')

        # News line
        headline = news.get('headline')
        if headline:
            sentiment = news.get('sentiment', '')
            sent_str = ''
            if sentiment:
                sent_color = 'green' if 'positive' in sentiment.lower() else 'red' if 'negative' in sentiment.lower() else 'yellow'
                sent_str = f'  [{sent_color}][{sentiment}][/{sent_color}]'
            console.print(f'[dim italic]{headline}[/dim italic]{sent_str}')

        console.print()


def _handle_scan(mmr: MMR, args: argparse.Namespace):
    scan_code = args.scan_code
    if not scan_code:
        preset = args.preset or 'gainers'
        scan_code = SCAN_PRESETS.get(preset)
        if not scan_code:
            console.print(f'[yellow]Unknown preset: {preset}. '
                          f'Available: {", ".join(SCAN_PRESETS.keys())}[/yellow]')
            return

    df = mmr.scan(
        scan_code=scan_code,
        instrument=args.instrument,
        location_code=args.location,
        num_rows=args.num,
        above_price=args.above_price,
        above_volume=args.above_volume,
        market_cap_above=args.market_cap_above,
    )
    print_df(df, title=f'IB Scanner: {scan_code}')


def _handle_ideas(mmr: MMR, args: argparse.Namespace):
    """Scan for trading ideas using preset-based scoring."""
    import logging as _logging
    _logging.getLogger('urllib3').setLevel(_logging.WARNING)

    # --presets flag: list presets and exit
    if args.presets:
        from trader.tools.idea_scanner import list_presets
        if _json_mode:
            print_df(list_presets(), title='Idea Scanner Presets')
        else:
            df = list_presets()
            table = Table(title='Idea Scanner Presets', show_lines=True)
            table.add_column('Preset', style='bold cyan', no_wrap=True)
            table.add_column('Filters', no_wrap=True)
            table.add_column('Indicators', no_wrap=True)
            for _, row in df.iterrows():
                table.add_row(row['preset'], row['filters'], row['indicators'])
            console.print(table)
        return

    # Determine source
    if args.tickers:
        source = 'tickers'
    elif args.universe:
        source = 'universe'
    else:
        source = 'movers'

    # --detail is shortcut for --fundamentals --news + company names
    use_fundamentals = args.fundamentals or args.detail
    use_news = args.news or args.detail
    use_names = args.detail

    data_source = getattr(args, 'source', 'massive')
    # News isn't available on the TwelveData path — tell the user once up-front
    # so an empty `headline` column doesn't look like a silent bug.
    if data_source == 'twelvedata' and use_news and not args.location:
        console.print('[yellow]Note: TwelveData has no news endpoint — news columns will be empty.[/yellow]')

    df = mmr.scan_ideas(
        preset=args.preset,
        source=source,
        tickers=args.tickers,
        universe=args.universe,
        top_n=args.num,
        min_price=args.min_price,
        max_price=args.max_price,
        min_volume=args.min_volume,
        min_change_pct=args.min_change,
        max_change_pct=args.max_change,
        fundamentals=use_fundamentals,
        news=use_news,
        names=use_names,
        location=args.location,
        data_source=data_source,
    )

    location_label = f' [{args.location}]' if args.location else ''
    source_label = ' — TwelveData' if data_source == 'twelvedata' and not args.location else ''
    title = f'Ideas: {args.preset}{location_label}{source_label}'

    if _json_mode:
        print_df(df, title=title)
        return

    if df.empty:
        if args.location:
            console.print(
                '[dim]No ideas found.[/dim]\n'
                '[dim]The IB scanner may not support this location, or your account '
                'may lack market data subscriptions for it.[/dim]\n'
                f'[dim]Try: ideas {args.preset} --location {args.location} '
                '--tickers SYM1 SYM2 SYM3[/dim]'
            )
        else:
            console.print('[dim]No ideas found[/dim]')
        return

    console.print(f'[bold]{title}[/bold]  ({len(df)} results)\n')

    for _, row in df.iterrows():
        ticker = row.get('ticker', '')
        price = row.get('price', 0) or 0
        change_pct = row.get('change_pct', 0) or 0
        volume = row.get('volume', 0) or 0
        score = row.get('score', 0) or 0
        signal = row.get('signal', '')

        color = 'green' if change_pct >= 0 else 'red'
        sign = '+' if change_pct > 0 else ''
        sig_color = 'green' if signal == 'BUY' else 'red' if signal == 'SELL' else 'yellow'

        # Header: ticker, price, change, signal, score
        name = row.get('name', '')
        name_str = f'  [dim]{name}[/dim]' if name and name == name else ''
        console.print(
            f'[bold]{ticker}[/bold]{name_str}  '
            f'${price:,.2f}  '
            f'[{color}]{sign}{change_pct:.2f}%[/{color}]  '
            f'[{sig_color}]{signal}[/{sig_color}]  '
            f'[dim]score {score}[/dim]'
        )

        # Indicators line
        indicators = []
        gap_pct = row.get('gap_pct')
        if gap_pct is not None and gap_pct == gap_pct:
            indicators.append(f'gap {gap_pct:+.1f}%')
        rel_vol = row.get('rel_vol')
        if rel_vol is not None and rel_vol == rel_vol:
            indicators.append(f'rvol {rel_vol:.1f}x')
        range_pct = row.get('range_pct')
        if range_pct is not None and range_pct == range_pct:
            indicators.append(f'range {range_pct:.1f}%')
        rsi = row.get('rsi')
        if rsi is not None and rsi == rsi:
            indicators.append(f'RSI {rsi:.0f}')
        for ema_col in ('ema_9', 'sma_20', 'sma_50'):
            val = row.get(ema_col)
            if val is not None and val == val:
                indicators.append(f'{ema_col.upper()} {val:.2f}')
        if indicators:
            console.print(f'[dim]vol {volume:,.0f} | {" | ".join(indicators)}[/dim]')
        else:
            console.print(f'[dim]vol {volume:,.0f}[/dim]')

        # Fundamentals line (if present)
        fund_parts = []
        for col, label in [('pe_ratio', 'PE'), ('debt_equity', 'D/E'), ('roe', 'ROE'),
                           ('eps', 'EPS'), ('mkt_cap', 'cap')]:
            val = row.get(col)
            if val is not None and val == val:
                if col == 'mkt_cap':
                    if val >= 1e12:
                        fund_parts.append(f'{label} ${val/1e12:.1f}T')
                    elif val >= 1e9:
                        fund_parts.append(f'{label} ${val/1e9:.1f}B')
                    elif val >= 1e6:
                        fund_parts.append(f'{label} ${val/1e6:.0f}M')
                else:
                    fund_parts.append(f'{label} {val:g}')
        if fund_parts:
            console.print(f'[dim]{" | ".join(fund_parts)}[/dim]')

        # News line (if present)
        headline = row.get('headline')
        if headline and headline == headline:  # not NaN
            sentiment = row.get('sentiment', '')
            sent_str = ''
            if sentiment and sentiment == sentiment:
                sent_color = 'green' if 'positive' in str(sentiment).lower() else 'red' if 'negative' in str(sentiment).lower() else 'yellow'
                sent_str = f'  [{sent_color}][{sentiment}][/{sent_color}]'
            console.print(f'[dim italic]{headline}[/dim italic]{sent_str}')

        console.print()


def _handle_stream(args: argparse.Namespace):
    """Stream live ticks from Massive.com until Ctrl+C."""
    from trader.container import Container
    from trader.listeners.massive_reactive import MassiveReactive

    container = Container.instance()
    config = container.config()

    massive_api_key = config.get('massive_api_key', '')
    if not massive_api_key:
        console.print('[red]massive_api_key not configured in trader.yaml[/red]')
        return

    massive_feed = args.feed or config.get('massive_feed', 'stocks')
    massive_delayed = args.delayed or config.get('massive_delayed', False)
    duckdb_path = config.get('duckdb_path', '')
    universe_library = config.get('universe_library', 'Universes')

    reactive = MassiveReactive(
        massive_api_key=massive_api_key,
        massive_feed=massive_feed,
        massive_delayed=massive_delayed,
        duckdb_path=duckdb_path,
        universe_library=universe_library,
    )

    # Determine data_type prefix based on feed and flags
    is_forex = massive_feed == 'forex'
    is_crypto = massive_feed == 'crypto'
    use_quotes = getattr(args, 'quotes', False)

    if use_quotes and (is_forex or is_crypto):
        data_type = 'C'  # ForexQuote
    elif args.trades and (is_forex or is_crypto):
        data_type = 'CAS'  # per-second currency aggs
    elif is_forex or is_crypto:
        data_type = 'CA'  # CurrencyAgg (1-min)
    elif args.trades:
        data_type = 'T'
    else:
        data_type = 'A'

    # resolve symbols from universe if provided
    if args.universe:
        from trader.data.universe import UniverseAccessor
        accessor = UniverseAccessor(duckdb_path, universe_library)
        universe = accessor.get(args.universe)
        if not universe.security_definitions:
            console.print(f'[red]Universe "{args.universe}" is empty or does not exist[/red]')
            return
        symbols = [d.symbol for d in universe.security_definitions]
    elif args.symbols:
        symbols = args.symbols
    else:
        console.print('[red]Specify symbols or --universe[/red]')
        return

    mode_label = 'quotes' if use_quotes else ('trades' if args.trades else 'aggs')
    console.print(f'[dim]Streaming {", ".join(symbols)} ({mode_label}, feed={massive_feed}, Ctrl+C to stop)...[/dim]')

    # Price format: 5 decimal places for forex, 2 for everything else
    pfmt = '.5f' if is_forex else '.2f'

    def on_agg(ticker):
        sym = ticker.contract.symbol if ticker.contract else '?'
        ccy = ticker.contract.currency if ticker.contract else ''
        label = f'{sym}/{ccy}' if is_forex and ccy else sym
        t = ticker.time.strftime('%H:%M:%S') if ticker.time else ''
        console.print(
            f'  {t}  {label:8s}'
            f'  O={ticker.open:{pfmt}}  H={ticker.high:{pfmt}}  L={ticker.low:{pfmt}}  C={ticker.close:{pfmt}}'
            f'  V={ticker.volume}'
        )

    def on_trade(ticker):
        sym = ticker.contract.symbol if ticker.contract else '?'
        t = ticker.time.strftime('%H:%M:%S.%f')[:12] if ticker.time else ''
        console.print(f'  {t}  {sym:6s}  last={ticker.last:{pfmt}}  size={ticker.lastSize}')

    def on_quote(ticker):
        sym = ticker.contract.symbol if ticker.contract else '?'
        ccy = ticker.contract.currency if ticker.contract else ''
        label = f'{sym}/{ccy}' if is_forex and ccy else sym
        t = ticker.time.strftime('%H:%M:%S.%f')[:12] if ticker.time else ''
        console.print(
            f'  {t}  {label:8s}'
            f'  bid={ticker.bid:{pfmt}}  ask={ticker.ask:{pfmt}}  mid={ticker.last:{pfmt}}'
        )

    if use_quotes:
        reactive.quote_subject.subscribe(on_next=on_quote)
    elif args.trades:
        reactive.trade_subject.subscribe(on_next=on_trade)
    else:
        reactive.agg_subject.subscribe(on_next=on_agg)

    reactive.start(symbols=symbols, data_type=data_type)

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        reactive.stop()
        console.print('[dim]Stream stopped.[/dim]')


def _handle_universe(mmr: MMR, args: argparse.Namespace):
    """Manage universes (CRUD operations)."""
    import pandas as pd
    from trader.container import Container
    from trader.data.universe import Universe, UniverseAccessor

    action = getattr(args, 'uni_action', None)
    if not action:
        console.print('[yellow]Usage: universe list|show|create|delete|add|remove|import[/yellow]')
        return

    container = Container.instance()
    cfg = container.config()
    accessor = UniverseAccessor(cfg['duckdb_path'], cfg['universe_library'])

    if action == 'list':
        counts = accessor.list_universes_count()
        print_dict(counts, title='Universes')

    elif action == 'show':
        universe = accessor.get(args.name)
        if not universe.security_definitions:
            console.print(f'[dim]Universe "{args.name}" is empty[/dim]')
            return
        rows = [{
            'conId': d.conId,
            'symbol': d.symbol,
            'secType': d.secType,
            'exchange': d.exchange,
            'primaryExchange': d.primaryExchange,
            'currency': d.currency,
            'longName': d.longName,
        } for d in universe.security_definitions]
        print_df(pd.DataFrame(rows), title=f'Universe: {args.name}')

    elif action == 'create':
        accessor.update(Universe(args.name, []))
        console.print(f'[green]Created universe: {args.name}[/green]')

    elif action == 'delete':
        confirm = input(f'Delete universe "{args.name}"? [y/N] ')
        if confirm.lower() != 'y':
            console.print('[dim]Cancelled[/dim]')
            return
        accessor.delete(args.name)
        console.print(f'[green]Deleted universe: {args.name}[/green]')

    elif action == 'add':
        for symbol in args.symbols:
            try:
                defs = mmr.resolve(symbol)
                if not defs:
                    console.print(f'[yellow]Could not resolve: {symbol}[/yellow]')
                    continue
                accessor.insert(args.name, defs[0])
                console.print(f'[green]Added {defs[0].symbol} (conId={defs[0].conId}) to {args.name}[/green]')
            except ConnectionError:
                console.print('[red]Cannot resolve symbols: trader_service not connected[/red]')
                return
            except Exception as e:
                console.print(f'[red]Error resolving {symbol}: {e}[/red]')

    elif action == 'remove':
        universe = accessor.get(args.name)
        if not universe.security_definitions:
            console.print(f'[yellow]Universe "{args.name}" is empty[/yellow]')
            return
        match = universe.find_symbol(args.symbol)
        if not match:
            console.print(f'[yellow]Symbol "{args.symbol}" not found in {args.name}[/yellow]')
            return
        universe.security_definitions = [
            d for d in universe.security_definitions if d.conId != match.conId
        ]
        accessor.update(universe)
        console.print(f'[green]Removed {match.symbol} from {args.name}[/green]')

    elif action == 'import':
        csv_path = Path(args.csv_file).expanduser()
        if not csv_path.exists():
            console.print(f'[red]File not found: {csv_path}[/red]')
            return
        csv_content = csv_path.read_text()
        count = accessor.update_from_csv_str(args.name, csv_content)
        console.print(f'[green]Imported {count} symbols into {args.name}[/green]')

    else:
        console.print(f'[yellow]Unknown universe action: {action}[/yellow]')


def _handle_watch(mmr: MMR):
    """Live-updating portfolio cards until Ctrl+C."""
    console.print('[dim]Watching portfolio (Ctrl+C to stop)...[/dim]')

    try:
        with Live(console=console, refresh_per_second=2) as live:
            while True:
                try:
                    df = mmr.portfolio()
                except Exception:
                    time.sleep(1)
                    continue

                if not df.empty:
                    # Filter out closed positions (qty=0) that IB still reports in-session
                    if 'position' in df.columns:
                        df = df[df['position'] != 0].reset_index(drop=True)
                    if 'avgCost' in df.columns and 'mktPrice' in df.columns:
                        df['%'] = ((df['mktPrice'] - df['avgCost']) / df['avgCost'] * 100).round(2)
                    df.insert(0, '#', range(1, len(df) + 1))

                # Fetch account summary for footer
                account_summary = None
                try:
                    from trader.messaging.clientserver import consume
                    acct_vals = consume(mmr._rpc.rpc(return_type=dict).get_account_values())
                    if acct_vals:
                        account_summary = {}
                        for key, display in (('TotalCashValue', 'cash'), ('AvailableFunds', 'available'), ('NetLiquidation', 'net_liq')):
                            entry = acct_vals.get(key)
                            if entry:
                                account_summary[display] = f'${float(entry["value"]):,.2f}'
                except Exception:
                    pass

                live.update(_build_portfolio_cards(df, title='Portfolio (live)', account_summary=account_summary))
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    console.print('[dim]Stopped watching.[/dim]')


# ------------------------------------------------------------------
# REPL
# ------------------------------------------------------------------

def _build_completer(parser: argparse.ArgumentParser):
    """Build a prompt_toolkit completer from the argparse parser."""
    from prompt_toolkit.completion import Completer, Completion

    # Extract top-level commands and their subcommands + flags
    commands = {}  # command -> {subcommands: [...], flags: [...]}
    for action in parser._subparsers._group_actions:
        for name, subparser in action.choices.items():
            entry = {'flags': [], 'subcommands': []}
            for sub_action in subparser._actions:
                if isinstance(sub_action, argparse._SubParsersAction):
                    entry['subcommands'] = list(sub_action.choices.keys())
                elif sub_action.option_strings:
                    entry['flags'].extend(sub_action.option_strings)
            commands[name] = entry

    # Global flags
    global_flags = []
    for action in parser._actions:
        if action.option_strings:
            global_flags.extend(action.option_strings)

    class MMRCompleter(Completer):
        def get_completions(self, document, complete_event):
            text = document.text_before_cursor
            words = text.split()
            word = document.get_word_before_cursor(WORD=True)

            if not words or (len(words) == 1 and not text.endswith(' ')):
                # Completing the command itself
                for cmd in sorted(commands.keys()):
                    if cmd.startswith(word):
                        yield Completion(cmd, start_position=-len(word))
                for flag in global_flags:
                    if flag.startswith(word):
                        yield Completion(flag, start_position=-len(word))
            elif len(words) >= 1:
                cmd = words[0]
                entry = commands.get(cmd, {})
                subs = entry.get('subcommands', [])
                flags = entry.get('flags', [])

                if len(words) == 1 and text.endswith(' '):
                    # Just typed command + space — show subcommands first, then flags
                    for sub in sorted(subs):
                        yield Completion(sub)
                    for flag in sorted(flags):
                        yield Completion(flag)
                elif len(words) == 2 and not text.endswith(' ') and subs:
                    # Completing subcommand
                    for sub in sorted(subs):
                        if sub.startswith(word):
                            yield Completion(sub, start_position=-len(word))
                    for flag in sorted(flags):
                        if flag.startswith(word):
                            yield Completion(flag, start_position=-len(word))
                else:
                    # Completing flags
                    if word.startswith('-'):
                        for flag in sorted(flags):
                            if flag.startswith(word):
                                yield Completion(flag, start_position=-len(word))

    return MMRCompleter()


def repl(mmr: MMR):
    """Interactive REPL with prompt_toolkit."""
    from prompt_toolkit import PromptSession
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.key_binding import KeyBindings

    history_dir = Path('~/.local/share/mmr').expanduser()
    history_dir.mkdir(parents=True, exist_ok=True)
    history_file = history_dir / '.mmr_repl_history'

    parser = build_parser()

    vi_mode = [True]

    bindings = KeyBindings()

    @bindings.add('escape', 'v')  # Alt+V toggles vi mode
    def _toggle_vi(event):
        vi_mode[0] = not vi_mode[0]
        event.app.vi_mode = vi_mode[0]
        mode_str = 'vi' if vi_mode[0] else 'emacs'
        event.app.output.write(f'\nInput mode: {mode_str}\n')
        event.app.output.flush()

    def _get_prompt():
        if not vi_mode[0]:
            return [('', 'mmr> ')]
        from prompt_toolkit.application import get_app
        try:
            app = get_app()
            mode = app.vi_state.input_mode
            mode_name = mode.value if hasattr(mode, 'value') else str(mode)
            if 'INSERT' in mode_name.upper():
                tag, style = 'I', 'fg:ansigreen'
            elif 'REPLACE' in mode_name.upper():
                tag, style = 'R', 'fg:ansired'
            else:
                tag, style = 'N', 'fg:ansiyellow'
        except Exception:
            tag, style = 'I', 'fg:ansigreen'
        return [(style, f'[{tag}]'), ('', ' mmr> ')]

    session = PromptSession(
        history=FileHistory(str(history_file)),
        auto_suggest=AutoSuggestFromHistory(),
        completer=_build_completer(parser),
        vi_mode=vi_mode[0],
        key_bindings=bindings,
    )

    console.print('[bold]MMR Trading REPL[/bold]  (type "help" for commands, Ctrl+C twice to exit)')

    _last_interrupt = [0.0]

    while True:
        try:
            text = session.prompt(_get_prompt).strip()
            _last_interrupt[0] = 0.0
        except KeyboardInterrupt:
            now = time.monotonic()
            if now - _last_interrupt[0] < 1.0:
                break
            _last_interrupt[0] = now
            console.print('[dim]Press Ctrl+C again to exit[/dim]')
            continue
        except EOFError:
            break

        if not text:
            continue

        # Toggle input mode
        if text in ('set vi', 'set vim'):
            vi_mode[0] = True
            session.vi_mode = True
            console.print('[dim]Input mode: vi[/dim]')
            continue
        elif text in ('set emacs', 'set default'):
            vi_mode[0] = False
            session.vi_mode = False
            console.print('[dim]Input mode: emacs[/dim]')
            continue

        try:
            tokens = shlex.split(text)
        except ValueError:
            console.print('[red]Invalid input[/red]')
            continue

        try:
            args = parser.parse_args(tokens)
        except SystemExit:
            # argparse calls sys.exit on parse errors; catch it in REPL
            continue

        if not dispatch(mmr, args):
            break

    console.print('[dim]Goodbye.[/dim]')


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

_LOCAL_ONLY_COMMANDS = {'backtest', 'bt', 'data', 'propose', 'proposals', 'reject', 'market-hours', 'mh', 'session', 'group'}
_LOCAL_ONLY_STRAT_ACTIONS = {'create', 'deploy', 'undeploy', 'signals', 'backtest'}


def main():
    # Pre-parse to check if this is a local-only command (no service needed)
    parser = build_parser()

    if len(sys.argv) > 1:
        try:
            args = parser.parse_args(sys.argv[1:])
        except SystemExit:
            return

        cmd = getattr(args, 'command', None)
        is_local = cmd in _LOCAL_ONLY_COMMANDS
        if cmd in ('strategies', 'strat'):
            strat_action = getattr(args, 'strat_action', None)
            if strat_action in _LOCAL_ONLY_STRAT_ACTIONS:
                is_local = True

        # Suppress logs by default; only show with --debug
        if not getattr(args, 'debug', False):
            import logging as _logging
            _logging.disable(_logging.CRITICAL)

        mmr = MMR()
        if not is_local:
            try:
                mmr.connect()
            except Exception as e:
                if not getattr(args, 'json', False):
                    console.print(f'[yellow]Warning: could not connect to trader_service: {e}[/yellow]')
                    console.print('[yellow]Some commands may fail.[/yellow]')

        try:
            dispatch(mmr, args)
        finally:
            mmr.close()
    else:
        # Interactive REPL — suppress logs by default
        import logging as _logging
        _logging.disable(_logging.CRITICAL)

        mmr = MMR()
        try:
            mmr.connect()
        except Exception as e:
            console.print(f'[yellow]Warning: could not connect to trader_service: {e}[/yellow]')
            console.print('[yellow]Some commands may fail.[/yellow]')
        try:
            repl(mmr)
        finally:
            mmr.close()


if __name__ == '__main__':
    main()
