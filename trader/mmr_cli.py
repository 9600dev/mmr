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
from rich.live import Live
from rich.table import Table
from trader.sdk import MMR

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
    sub.add_parser('portfolio', aliases=['p'], help='Portfolio with P&L',
                   epilog='Examples:\n'
                          '  portfolio\n'
                          '  p',
                   formatter_class=fmt)

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

    # resolve
    res_p = sub.add_parser('resolve', help='Resolve symbol to contract details',
                           epilog='Examples:\n'
                                  '  resolve AMD\n'
                                  '  resolve 4391',
                           formatter_class=fmt)
    res_p.add_argument('symbol', help='Symbol or conId')
    res_p.add_argument('--sectype', default='STK', help='Security type (STK, CASH, OPT, FUT, etc.)')

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

    # close
    close_p = sub.add_parser('close', aliases=['c'], help='Close a position by row number',
                             epilog='Examples:\n'
                                    '  close 1              # close entire position at row 1\n'
                                    '  close 3 --quantity 50 # partial close\n'
                                    '  c 2',
                             formatter_class=fmt)
    close_p.add_argument('row', type=int, help='Row number from portfolio/watch output')
    close_p.add_argument('--quantity', type=float, default=None, help='Partial quantity (default: entire position)')

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

    fin_income_p = fin_sub.add_parser('income', help='Income statement')
    fin_income_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')
    fin_income_p.add_argument('--limit', type=int, default=4, help='Number of periods (default: 4)')
    fin_income_p.add_argument('--timeframe', default='quarterly', choices=['quarterly', 'annual'],
                              help='Timeframe (default: quarterly)')

    fin_cashflow_p = fin_sub.add_parser('cashflow', help='Cash flow statement')
    fin_cashflow_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')
    fin_cashflow_p.add_argument('--limit', type=int, default=4, help='Number of periods (default: 4)')
    fin_cashflow_p.add_argument('--timeframe', default='quarterly', choices=['quarterly', 'annual'],
                                help='Timeframe (default: quarterly)')

    fin_ratios_p = fin_sub.add_parser('ratios', help='Financial ratios (TTM)')
    fin_ratios_p.add_argument('symbol', help='Stock ticker (e.g. AAPL)')

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
    movers_p = sub.add_parser('movers', help='Top market movers (Massive.com)')
    movers_p.add_argument('--market', '-m', default='stocks',
                           choices=['stocks', 'crypto', 'indices', 'options', 'futures'],
                           help='Market type (default: stocks)')
    movers_p.add_argument('--losers', action='store_true', default=False,
                           help='Show losers instead of gainers')
    movers_p.add_argument('--detail', action='store_true', default=False,
                           help='Enrich with company name, ratios, and news (card view)')
    movers_p.add_argument('--num', '-n', type=int, default=20,
                           help='Number of results (default: 20)')

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

    # data
    data_p = sub.add_parser('data', help='Local data exploration (no service needed)',
                            epilog='Examples:\n'
                                   '  data summary\n'
                                   '  data query AAPL --bar-size "1 day" --days 30\n'
                                   '  data download AAPL MSFT --bar-size "1 day" --days 365',
                            formatter_class=fmt)
    data_sub = data_p.add_subparsers(dest='data_action')

    data_sub.add_parser('summary', help='Show summary of all local historical data')

    data_query_p = data_sub.add_parser('query', help='Query local OHLCV data')
    data_query_p.add_argument('symbol', help='Symbol or conId')
    data_query_p.add_argument('--bar-size', default='1 day', help='Bar size (default: "1 day")')
    data_query_p.add_argument('--days', type=int, default=30, help='Days of history (default: 30)')
    data_query_p.add_argument('--tail', type=int, default=None, help='Show only last N rows')

    data_dl_p = data_sub.add_parser('download', help='Download data from Massive.com to local DuckDB')
    data_dl_p.add_argument('symbols', nargs='+', help='Symbols to download')
    data_dl_p.add_argument('--bar-size', default='1 day', help='Bar size (default: "1 day")')
    data_dl_p.add_argument('--days', type=int, default=365, help='Days of history (default: 365)')

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

    try:
        if cmd in ('portfolio', 'p'):
            df = mmr.portfolio()
            if not df.empty and 'avgCost' in df.columns and 'mktPrice' in df.columns:
                df['%'] = ((df['mktPrice'] - df['avgCost']) / df['avgCost'] * 100).round(2)
                df.insert(0, '#', range(1, len(df) + 1))
            print_df(df, title='Portfolio')

        elif cmd in ('positions', 'pos'):
            df = mmr.positions()
            if not df.empty:
                df.insert(0, '#', range(1, len(df) + 1))
            print_df(df, title='Positions')

        elif cmd == 'orders':
            print_df(mmr.orders(), title='Orders')

        elif cmd == 'trades':
            print_df(mmr.trades(), title='Trades')

        elif cmd in ('snapshot', 'snap'):
            result = mmr.snapshot(args.symbol, delayed=args.delayed)
            print_dict(result, title=f'Snapshot: {args.symbol}')

        elif cmd == 'resolve':
            defs = mmr.resolve(args.symbol, sec_type=args.sectype)
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

        elif cmd in ('close', 'c'):
            _handle_close(mmr, args)

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

        elif cmd == 'risk':
            _handle_risk(mmr, args)

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

        elif cmd == 'stream':
            _handle_stream(args)

        elif cmd in ('universe', 'u'):
            _handle_universe(mmr, args)

        elif cmd in ('backtest', 'bt'):
            _handle_backtest(args)

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
    )

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
            # Show leverage estimate if present
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

    close_qty = args.quantity if args.quantity else abs(pos_size)

    confirm = input(f'Close #{row}: {symbol} ({close_qty} shares) at market? [y/N] ')
    if confirm.lower() != 'y':
        console.print('[dim]Cancelled[/dim]')
        return

    result = mmr.close_position(symbol, quantity=args.quantity)
    if result.is_success():
        console.print(f'[green]Close {symbol}: submitted[/green]')
    else:
        console.print(f'[red]Close {symbol}: failed — {result.error}[/red]')


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

    if updates:
        result = mmr.set_risk_limits(**updates)
        print_status('Risk limits updated')
        print_dict(result, title='Risk Limits')
    else:
        result = mmr.get_risk_limits()
        print_dict(result, title='Risk Limits')


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
    else:
        # Default: list strategies. Fall back to config file if service is down.
        _handle_strategies_from_config()


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
        rows.append({
            'name': s.get('name', ''),
            'module': s.get('module', ''),
            'class_name': s.get('class_name', ''),
            'bar_size': s.get('bar_size', ''),
            'conids': str(s.get('conids', s.get('universe', ''))),
            'paper': s.get('paper', True),
            'days': s.get('historical_days_prior', ''),
        })
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
from typing import Optional

import pandas as pd


class {class_name}(Strategy):
    """Strategy: {name}

    Receives accumulated OHLCV DataFrames via on_prices().
    Return a Signal to generate a trade, or None to do nothing.

    DataFrame columns: date (index), open, high, low, close, volume, average, barCount
    """

    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 20:
            return None

        # TODO: implement your strategy logic here
        # Example: simple moving average crossover
        # fast_ma = prices["close"].rolling(10).mean()
        # slow_ma = prices["close"].rolling(20).mean()
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

    storage = TickStorage(duckdb_path)

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

    try:
        accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))
        result = backtester.run_from_module(args.strategy, args.class_name, conids, universe_accessor=accessor)
    except FileNotFoundError as e:
        print_status(str(e), success=False)
        return
    except ValueError as e:
        print_status(str(e), success=False)
        return
    except Exception as e:
        print_status(f'{type(e).__name__}: {e}', success=False)
        return

    # Build summary
    summary = {
        'total_return': f'{result.total_return:.2%}',
        'sharpe_ratio': f'{result.sharpe_ratio:.2f}',
        'max_drawdown': f'{result.max_drawdown:.2%}',
        'win_rate': f'{result.win_rate:.2%}',
        'total_trades': result.total_trades,
        'start_date': str(result.start_date)[:10],
        'end_date': str(result.end_date)[:10],
        'final_equity': f'{result.equity_curve.iloc[-1]:,.2f}' if len(result.equity_curve) > 0 else str(config.initial_capital),
    }

    if _json_mode:
        trades_data = []
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
        print(json.dumps({
            'data': {
                'summary': {
                    'total_return': result.total_return,
                    'sharpe_ratio': result.sharpe_ratio,
                    'max_drawdown': result.max_drawdown,
                    'win_rate': result.win_rate,
                    'total_trades': result.total_trades,
                    'start_date': str(result.start_date)[:10],
                    'end_date': str(result.end_date)[:10],
                    'final_equity': float(result.equity_curve.iloc[-1]) if len(result.equity_curve) > 0 else config.initial_capital,
                },
                'trades': trades_data,
            },
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
        console.print('[yellow]Usage: data summary|query|download[/yellow]')
        return

    if action == 'summary':
        _handle_data_summary()
    elif action == 'query':
        _handle_data_query(args)
    elif action == 'download':
        _handle_data_download(args)
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

    storage = TickStorage(duckdb_path)
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
            try:
                min_date, max_date = tickdata.date_summary(int(sym))
            except (ValueError, Exception):
                continue

            # Try to resolve conId to symbol name
            name = sym
            try:
                results = accessor.resolve_symbol(sym, first_only=True)
                if results:
                    name = results[0].symbol
            except Exception:
                pass

            rows.append({
                'conId': sym,
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

    storage = TickStorage(duckdb_path)
    accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))

    # Resolve symbol to conId
    symbol = args.symbol
    conid = None
    if symbol.isnumeric():
        conid = int(symbol)
    else:
        results = accessor.resolve_symbol(symbol, first_only=True)
        if results:
            conid = results[0].conId
        else:
            print_status(f'Cannot resolve symbol: {symbol}. Use a conId directly.', success=False)
            return

    bar_size = BarSize.parse_str(getattr(args, 'bar_size', '1 day'))
    tickdata = storage.get_tickdata(bar_size)

    date_range = DateRange(
        start=dt.datetime.now() - dt.timedelta(days=args.days),
        end=dt.datetime.now()
    )

    df = tickdata.read(conid, date_range=date_range)
    if df.empty:
        print_df(pd.DataFrame(), title=f'Data: {symbol} ({bar_size})')
        return

    if args.tail:
        df = df.tail(args.tail)

    print_df(df, title=f'Data: {symbol} ({bar_size})')


def _handle_data_download(args: argparse.Namespace):
    """Download data from Massive.com directly to local DuckDB."""
    import datetime as dt
    from trader.container import Container
    from trader.data.data_access import TickStorage
    from trader.data.universe import UniverseAccessor
    from trader.objects import BarSize

    container = Container.instance()
    cfg = container.config()
    duckdb_path = cfg.get('duckdb_path', '')
    massive_api_key = cfg.get('massive_api_key', '')

    if not massive_api_key:
        print_status('massive_api_key not configured in trader.yaml', success=False)
        return
    if not duckdb_path:
        print_status('duckdb_path not configured', success=False)
        return

    bar_size = BarSize.parse_str(getattr(args, 'bar_size', '1 day'))
    storage = TickStorage(duckdb_path)
    accessor = UniverseAccessor(duckdb_path, cfg.get('universe_library', 'Universes'))

    from trader.listeners.massive_history import MassiveHistoryWorker

    worker = MassiveHistoryWorker(massive_api_key=massive_api_key)

    end_date = dt.datetime.now()
    start_date = end_date - dt.timedelta(days=args.days)
    completed = 0
    failed = 0

    for symbol in args.symbols:
        # Resolve to conId for storage key
        conid = None
        results = accessor.resolve_symbol(symbol, first_only=True)
        if results:
            conid = results[0].conId

        try:
            if not _json_mode:
                console.print(f'[dim]Downloading {symbol} ({bar_size})...[/dim]')
            df = worker.get_history(
                ticker=symbol,
                bar_size=bar_size,
                start_date=start_date,
                end_date=end_date,
            )
            if not df.empty:
                tickdata = storage.get_tickdata(bar_size)
                storage_key = conid if conid else symbol
                tickdata.write_resolve_overlap(storage_key, df)
                completed += 1
                if not _json_mode:
                    console.print(f'[dim]  Wrote {len(df)} rows for {symbol}[/dim]')
            else:
                if not _json_mode:
                    console.print(f'[yellow]  No data returned for {symbol}[/yellow]')
                failed += 1
        except Exception as e:
            failed += 1
            if not _json_mode:
                console.print(f'[red]  Error downloading {symbol}: {e}[/red]')

    if _json_mode:
        print(json.dumps({
            'success': failed == 0,
            'message': f'{completed} downloaded, {failed} failed',
            'completed': completed,
            'failed': failed,
        }))
    else:
        print_status(f'Download complete: {completed} succeeded, {failed} failed')


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

    sub = mmr.subscribe_ticks(args.symbol, on_tick, topic=args.topic)
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
        console.print('[yellow]Usage: history list|massive|ib [options][/yellow]')
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

    try:
        if action == 'ratios':
            df = mmr.ratios(symbol)
            if df is None or df.empty:
                console.print('[dim]No data[/dim]')
                return
            # Display as key-value pairs (single row of ratios)
            row = df.iloc[0]
            skip = {'cik', 'ticker'}
            data = {
                k.replace('_', ' ').title(): _fmt_financial_value(row[k])
                for k in df.columns if k not in skip
            }
            print_dict(data, title=f'Financial Ratios: {symbol} (TTM)')
        elif action == 'filing':
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
            if action == 'balance':
                df = mmr.balance_sheet(symbol, limit=limit, timeframe=timeframe)
                _print_financial_df(df, title=f'Balance Sheet: {symbol} ({timeframe})')
            elif action == 'income':
                df = mmr.income_statement(symbol, limit=limit, timeframe=timeframe)
                _print_financial_df(df, title=f'Income Statement: {symbol} ({timeframe})')
            elif action == 'cashflow':
                df = mmr.cash_flow(symbol, limit=limit, timeframe=timeframe)
                _print_financial_df(df, title=f'Cash Flow: {symbol} ({timeframe})')
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

    if not args.detail:
        df = mmr.movers(market=market, direction=direction)
        if args.num and len(df) > args.num:
            df = df.head(args.num)
        print_df(df, title=f'{market.title()} Movers ({direction})')
        return

    # Detail card view
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
        print_df(list_presets(), title='Idea Scanner Presets')
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
    )

    location_label = f' [{args.location}]' if args.location else ''
    title = f'Ideas: {args.preset}{location_label}'

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
    """Live-updating portfolio table until Ctrl+C."""
    console.print('[dim]Watching portfolio (Ctrl+C to stop)...[/dim]')

    try:
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                try:
                    df = mmr.portfolio()
                except Exception:
                    time.sleep(3)
                    continue

                table = Table(title='Portfolio (live)')
                if df.empty:
                    table.add_column('info')
                    table.add_row('No positions')
                else:
                    if 'avgCost' in df.columns and 'mktPrice' in df.columns:
                        df['%'] = ((df['mktPrice'] - df['avgCost']) / df['avgCost'] * 100).round(2)
                    df.insert(0, '#', range(1, len(df) + 1))

                    for col in df.columns:
                        justify = 'right' if col not in ('#', 'account', 'symbol') else 'left'
                        table.add_column(str(col), justify=justify)

                    for _, row in df.iterrows():
                        cells = []
                        for col in df.columns:
                            val = row[col]
                            if isinstance(val, float) and col in ('unrealizedPNL', 'realizedPNL', 'dailyPNL'):
                                color = 'green' if val >= 0 else 'red'
                                cells.append(f'[{color}]{val:,.2f}[/{color}]')
                            elif isinstance(val, float) and col == '%':
                                color = 'green' if val >= 0 else 'red'
                                cells.append(f'[{color}]{val:+.2f}%[/{color}]')
                            elif isinstance(val, float):
                                cells.append(f'{val:,.2f}')
                            else:
                                cells.append(str(val))
                        table.add_row(*cells)

                live.update(table)
                time.sleep(3)
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

_LOCAL_ONLY_COMMANDS = {'backtest', 'bt', 'data', 'propose', 'proposals', 'reject'}
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
