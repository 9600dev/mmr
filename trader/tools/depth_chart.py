"""Market depth (Level 2) visualization.

Renders order book data as a diverging horizontal bar chart (PNG)
and a Rich table for terminal display.
"""

import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from rich.table import Table
from rich.text import Text


def render_depth_chart(depth_data: dict, output_path: str) -> str:
    """Render a depth chart PNG from order book data.

    *depth_data* must contain ``bids`` and ``asks`` lists (each entry has
    ``price`` and ``size`` keys), plus ``last``, ``bid``, ``ask``, and
    ``symbol``.

    Returns the absolute path to the saved PNG.
    """
    bids = depth_data.get('bids', [])
    asks = depth_data.get('asks', [])
    symbol = depth_data.get('symbol', '?')
    last = depth_data.get('last', float('nan'))
    bid_top = depth_data.get('bid', float('nan'))
    ask_top = depth_data.get('ask', float('nan'))

    # Fall back to best bid/ask from depth levels when top-of-book is NaN
    if math.isnan(bid_top) and bids:
        bid_top = max(b['price'] for b in bids)
    if math.isnan(ask_top) and asks:
        ask_top = min(a['price'] for a in asks)
    if math.isnan(last) and not math.isnan(bid_top) and not math.isnan(ask_top):
        last = (bid_top + ask_top) / 2

    # Sort bids descending (best bid first), asks ascending (best ask first)
    bids = sorted(bids, key=lambda x: x['price'], reverse=True)
    asks = sorted(asks, key=lambda x: x['price'])

    # Build parallel arrays for the chart — price levels ascending
    bid_prices = [b['price'] for b in reversed(bids)]
    bid_sizes = [b['size'] for b in reversed(bids)]
    ask_prices = [a['price'] for a in asks]
    ask_sizes = [a['size'] for a in asks]

    all_prices = bid_prices + ask_prices
    # Bid volumes extend LEFT (negative), ask volumes extend RIGHT (positive)
    all_volumes = [-s for s in bid_sizes] + ask_sizes
    all_colors = ['#00c853'] * len(bid_prices) + ['#ff1744'] * len(ask_prices)

    if not all_prices:
        # Edge case: no data — produce a minimal chart
        fig, ax = plt.subplots(figsize=(12, 4))
        ax.set_facecolor('#1a1a2e')
        fig.patch.set_facecolor('#1a1a2e')
        ax.text(0.5, 0.5, f'{symbol} — No depth data', color='white',
                ha='center', va='center', fontsize=16, transform=ax.transAxes)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=100, bbox_inches='tight', facecolor=fig.get_facecolor())
        plt.close(fig)
        return str(Path(output_path).resolve())

    price_labels = [f'{p:.2f}' for p in all_prices]

    fig, ax = plt.subplots(figsize=(12, max(4, len(all_prices) * 0.5 + 2)))
    fig.patch.set_facecolor('#1a1a2e')
    ax.set_facecolor('#1a1a2e')

    y_pos = range(len(all_prices))
    bars = ax.barh(y_pos, all_volumes, color=all_colors, edgecolor='none', height=0.7)

    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(price_labels, fontsize=10, color='#e0e0e0')
    ax.tick_params(axis='x', colors='#e0e0e0')

    # X-axis: show absolute volume values
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{abs(x):,.0f}'))
    ax.set_xlabel('Volume', color='#e0e0e0', fontsize=11)

    # Reference line for last price
    if not math.isnan(last):
        # Find y position closest to last price
        diffs = [abs(p - last) for p in all_prices]
        closest_idx = diffs.index(min(diffs))
        ax.axhline(y=closest_idx, color='white', linestyle='--', linewidth=1, alpha=0.7,
                   label=f'Last: {last:.2f}')

    # Spread shading between best bid and best ask
    if bid_prices and ask_prices:
        best_bid_idx = len(bid_prices) - 1  # top of bid section
        best_ask_idx = len(bid_prices)       # bottom of ask section
        ax.axhspan(best_bid_idx + 0.5, best_ask_idx - 0.5, color='#ffffff', alpha=0.05)

    # Volume labels — always on the outside end of each bar
    max_vol = max(abs(v) for v in all_volumes) if all_volumes else 1
    for bar, vol in zip(bars, all_volumes):
        if abs(vol) > max_vol * 0.3:
            x_pos = bar.get_width()
            # Place label outside the bar tip
            if vol < 0:
                # Bid bar extends left — label goes further left
                ax.text(x_pos - max_vol * 0.02, bar.get_y() + bar.get_height() / 2,
                        f'{abs(vol):,.0f}', va='center', ha='right',
                        color='white', fontsize=9, fontweight='bold')
            else:
                # Ask bar extends right — label goes further right
                ax.text(x_pos + max_vol * 0.02, bar.get_y() + bar.get_height() / 2,
                        f'{abs(vol):,.0f}', va='center', ha='left',
                        color='white', fontsize=9, fontweight='bold')

    # Title
    spread = ask_top - bid_top if not (math.isnan(ask_top) or math.isnan(bid_top)) else float('nan')
    title_parts = [f'{symbol} Market Depth']
    if not math.isnan(last):
        title_parts.append(f'Last: ${last:.2f}')
    if not math.isnan(spread):
        title_parts.append(f'Spread: ${spread:.4g}')
    ax.set_title(' | '.join(title_parts), color='white', fontsize=14, fontweight='bold', pad=15)

    # Grid
    ax.grid(axis='x', color='#333355', linewidth=0.5, alpha=0.5)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('#333355')
    ax.spines['left'].set_color('#333355')

    # Legend
    if not math.isnan(last):
        ax.legend(loc='upper right', facecolor='#1a1a2e', edgecolor='#333355',
                  labelcolor='#e0e0e0', fontsize=9)

    # Axis labels
    ax.set_ylabel('Price', color='#e0e0e0', fontsize=11)

    # Bid/Ask labels
    if bid_sizes:
        ax.text(0.02, 0.02, 'BIDS', transform=ax.transAxes, color='#00c853',
                fontsize=12, fontweight='bold', alpha=0.6)
    if ask_sizes:
        ax.text(0.95, 0.98, 'ASKS', transform=ax.transAxes, color='#ff1744',
                fontsize=12, fontweight='bold', alpha=0.6, ha='right', va='top')

    # Ensure bid volume labels don't overlap the y-axis price labels
    # by adding left margin proportional to the max bid volume
    if bid_sizes:
        ax.margins(x=0.15)

    plt.tight_layout()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=100, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)

    return str(Path(output_path).resolve())


def _make_bar(size: float, max_size: float, width: int, char: str = '█') -> str:
    """Build a fixed-width block-character bar scaled to *max_size*."""
    if max_size <= 0:
        return ''
    filled = round(size / max_size * width)
    return char * filled


def render_depth_table(depth_data: dict) -> Table:
    """Render order book data as a Rich table for terminal display.

    Returns a Rich Table with inline volume bars, bids on the left,
    asks on the right, best bid/ask at the center row.
    """
    bids = depth_data.get('bids', [])
    asks = depth_data.get('asks', [])
    symbol = depth_data.get('symbol', '?')
    last = depth_data.get('last', float('nan'))
    bid_top = depth_data.get('bid', float('nan'))
    ask_top = depth_data.get('ask', float('nan'))

    # Fall back to best bid/ask from depth levels when top-of-book is NaN
    if math.isnan(bid_top) and bids:
        bid_top = max(b['price'] for b in bids)
    if math.isnan(ask_top) and asks:
        ask_top = min(a['price'] for a in asks)

    # Sort: bids descending (best first), asks ascending (best first)
    bids = sorted(bids, key=lambda x: x['price'], reverse=True)
    asks = sorted(asks, key=lambda x: x['price'])

    spread = ask_top - bid_top if not (math.isnan(ask_top) or math.isnan(bid_top)) else float('nan')
    mid = (ask_top + bid_top) / 2 if not (math.isnan(ask_top) or math.isnan(bid_top)) else float('nan')
    bid_vol = sum(b['size'] for b in bids)
    ask_vol = sum(a['size'] for a in asks)
    ratio = bid_vol / ask_vol if ask_vol > 0 else float('inf')

    all_sizes = [b['size'] for b in bids] + [a['size'] for a in asks]
    max_size = max(all_sizes) if all_sizes else 1
    bar_width = 20

    # Build title
    last_str = f' | Last: ${last:.2f}' if not math.isnan(last) else ''

    # Build caption
    footer_parts = []
    if not math.isnan(spread):
        footer_parts.append(f'Spread: ${spread:.4g}')
    if not math.isnan(mid):
        footer_parts.append(f'Mid: ${mid:.2f}')
    if bid_vol or ask_vol:
        footer_parts.append(f'Bid Vol: {bid_vol:,.0f}')
        footer_parts.append(f'Ask Vol: {ask_vol:,.0f}')
        if not math.isinf(ratio):
            color = 'green' if ratio >= 1 else 'red'
            footer_parts.append(f'B/A Ratio: [{color}]{ratio:.2f}[/{color}]')

    table = Table(
        title=f'[bold]{symbol}[/bold] Order Book{last_str}',
        caption=' | '.join(footer_parts) if footer_parts else None,
        show_lines=False,
        pad_edge=True,
        padding=(0, 1),
    )
    table.add_column('Bid Vol', justify='right', style='green', no_wrap=True, min_width=bar_width)
    table.add_column('Size', justify='right', style='green', no_wrap=True, min_width=7)
    table.add_column('Bid', justify='right', style='bold green', no_wrap=True, min_width=8)
    table.add_column('', justify='center', width=3, no_wrap=True)
    table.add_column('Ask', justify='left', style='bold red', no_wrap=True, min_width=8)
    table.add_column('Size', justify='left', style='red', no_wrap=True, min_width=7)
    table.add_column('Ask Vol', justify='left', style='red', no_wrap=True, min_width=bar_width)

    # Asks first (highest to lowest, reversed so best ask is at bottom)
    for i in reversed(range(len(asks))):
        a = asks[i]
        bar = _make_bar(a['size'], max_size, bar_width)
        table.add_row(
            '', '', '',
            '',
            f'{a["price"]:.2f}',
            f'{a["size"]:,.0f}',
            bar,
        )

    # Spread separator row
    if bids and asks:
        spread_text = f'${spread:.4g}' if not math.isnan(spread) else ''
        table.add_row(
            '', '', '',
            Text(f'  {spread_text}  ', style='dim'),
            '', '', '',
        )

    # Bids (highest/best first)
    for i in range(len(bids)):
        b = bids[i]
        bar = _make_bar(b['size'], max_size, bar_width)
        table.add_row(
            bar,
            f'{b["size"]:,.0f}',
            f'{b["price"]:.2f}',
            '',
            '', '', '',
        )

    return table
