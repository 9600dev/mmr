#!/usr/bin/env python3
"""Build a liquidity-ranked US equity universe for cross-sectional research.

WHY LIQUIDITY RATHER THAN INDEX MEMBERSHIP
    The obvious universe is "today's S&P 500", and it is a trap: you would be
    studying the companies that survived to be in the index today, which is
    the definition of survivorship bias. Ranking by dollar volume has the same
    problem — it is still a selection made with today's information — but it
    is at least the universe you could actually TRADE, and it does not
    additionally condition on a committee having judged the company
    successful.

    SO THE BIAS IS NOT FIXED, ONLY REDUCED. Names that delisted, were acquired
    or went to zero over the study window are absent from any universe built
    this way, and every backtest run on it is optimistic by however much those
    names would have lost. For a LONG/SHORT cross-sectional study the effect
    is smaller than for long-only — the missing losers would have been shorts
    as often as longs — but it is not zero, and it must be stated in any
    result rather than discovered later.

    The honest fix is point-in-time index membership, which we do not have.
    Record the limitation; do not pretend it away.

WHAT IT DOES
    One grouped-daily call gives every US ticker's bar for a single session —
    one request instead of thousands. Rank by median dollar volume across
    several sampled sessions (not one, so a single halted or news-driven day
    cannot promote a name), keep common stocks on major exchanges, and write
    the top N into an MMR universe.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_MAJOR = {'XNYS', 'XNAS', 'ARCX', 'BATS'}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--top', type=int, default=500)
    ap.add_argument('--samples', type=int, default=5,
                    help='how many past sessions to median over (default 5)')
    ap.add_argument('--name', default='us_liquid500')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    import yaml
    from massive import RESTClient
    from trader.container import default_config_path

    cfg = yaml.safe_load(open(default_config_path()))
    client = RESTClient(os.environ.get('MASSIVE_API_KEY')
                        or cfg.get('massive_api_key'))

    # Which tickers are common stock on a major exchange. Done once; the
    # grouped-daily bars carry no security type.
    print('listing US common stocks...', flush=True)
    eligible = {}
    for t in client.list_tickers(market='stocks', active=True, limit=1000):
        if getattr(t, 'type', None) == 'CS' and \
                getattr(t, 'primary_exchange', '') in _MAJOR:
            eligible[t.ticker] = getattr(t, 'name', '') or ''
    print(f'  {len(eligible):,} eligible common stocks on major exchanges')

    # Sample several recent sessions. Weekends/holidays simply return nothing,
    # so walk back until we have enough non-empty ones.
    dollar_volumes: dict = {}
    day = dt.date.today() - dt.timedelta(days=1)
    got = 0
    while got < args.samples and day > dt.date.today() - dt.timedelta(days=40):
        try:
            bars = client.get_grouped_daily_aggs(day.isoformat(),
                                                 adjusted=True)
        except Exception as exc:                          # noqa: BLE001
            print(f'  {day}: {exc}', flush=True)
            day -= dt.timedelta(days=1)
            continue
        if not bars:
            day -= dt.timedelta(days=1)
            continue
        for b in bars:
            sym = getattr(b, 'ticker', None)
            if sym not in eligible:
                continue
            close = getattr(b, 'close', None)
            vol = getattr(b, 'volume', None)
            if close and vol and close > 0 and vol > 0:
                dollar_volumes.setdefault(sym, []).append(close * vol)
        got += 1
        print(f'  sampled {day}: {len(bars):,} bars', flush=True)
        day -= dt.timedelta(days=1)

    if not dollar_volumes:
        print('no sessions sampled', file=sys.stderr)
        return 1

    # Median, not mean: one halt or one earnings blowout must not promote a
    # name into a universe it does not belong in.
    ranked = sorted(
        ((sym, statistics.median(v)) for sym, v in dollar_volumes.items()
         if len(v) >= max(2, got // 2)),
        key=lambda x: -x[1])
    top = ranked[:args.top]

    print(f'\nranked {len(ranked):,} names over {got} sessions; '
          f'keeping top {len(top)}')
    print(f'  #1        {top[0][0]:<6} ${top[0][1]/1e9:.2f}B/day')
    print(f'  #{len(top)//2:<8} {top[len(top)//2][0]:<6} '
          f'${top[len(top)//2][1]/1e6:.0f}M/day')
    print(f'  #{len(top):<8} {top[-1][0]:<6} ${top[-1][1]/1e6:.0f}M/day')

    symbols = [s for s, _ in top]
    if args.dry_run:
        print('\n(dry run) would write universe '
              f'{args.name!r} with {len(symbols)} symbols')
        print('  ' + ' '.join(symbols[:40]) + ' ...')
        return 0

    out = os.path.join(os.path.expanduser('~'), f'{args.name}.txt')
    with open(out, 'w') as fh:
        fh.write('\n'.join(symbols) + '\n')
    print(f'\nwrote {len(symbols)} symbols to {out}')
    print(f'  next: mmr universe create {args.name} && '
          f'mmr universe add {args.name} $(cat {out} | tr "\\n" " ")')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
