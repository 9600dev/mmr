"""Synthetic panel + synthetic Form 4 events for the insider-family tests (C04).

No database: events are injected with ``insider_signals.set_events_override``.
Reserved names carry one deterministic pattern each (routine buyer, cluster,
joint filing, ten-percent-only buyer, mis-keyed price, heavy seller, and the
D14 timing MARKER whose ``usable_from`` is a rebalance session); every other
name receives random officer/director purchases and sales.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from trader.research.lowfreq import insider_signals as ins
from trader.research.lowfreq.loaders import PricePanel
from trader.research.lowfreq.reference_strategies import schedule_mask

N_NAMES, N_DAYS = 80, 440
SESSIONS = pd.bdate_range('2016-08-01', periods=N_DAYS)
TICKERS = [f'T{i:03d}' for i in range(N_NAMES)]
ROUTINE, CLUSTER, JOINT, TENPCT, MISKEYED, SELLER, MARKER = 'T010', 'T020', 'T030', 'T040', 'T050', 'T060', 'T070'
RESERVED = {ROUTINE, CLUSTER, JOINT, TENPCT, MISKEYED, SELLER, MARKER}
MARKER_VALUE = 80_000.0


def make_panel(seed: int = 7) -> PricePanel:
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0003, 0.015, (N_DAYS, N_NAMES))
    close = pd.DataFrame(50.0 * (1 + np.arange(N_NAMES) / 10.0) * np.cumprod(1 + rets, axis=0),
                         index=SESSIONS, columns=TICKERS)
    opn = close.shift(1).fillna(close.iloc[0])
    member = pd.DataFrame(True, index=SESSIONS, columns=TICKERS)
    member.iloc[300:, :3] = False                    # three names leave the universe
    fields = {'member': member,
              'dollar_volume': close * 1e5 * (1.0 + np.arange(N_NAMES))[None, :]}
    return PricePanel(open=opn, close=close, fields=fields, signals={}, universe='equity')


def marker_usable_from() -> pd.Timestamp:
    """A rebalance session (first session of a month) well inside the panel."""
    due = SESSIONS[schedule_mask(SESSIONS)]
    return pd.Timestamp(due[8])


def _row(ticker: str, filing_date: Any, usable_from: Any, shares: float, price: float, owner: int, *,
         officer: bool = True, director: bool = False, ten: bool = False, code: str = 'P',
         n_owners: int = 1, accession: Optional[str] = None, shares_after: Optional[float] = None,
         trans_date: Any = None, acc_id: List[int] = [0]) -> Dict[str, Any]:
    acc_id[0] += 1
    fd = pd.Timestamp(filing_date)
    return {'ticker': ticker, 'cik': 1000 + int(ticker[1:]), 'filing_date': fd, 'usable_from': pd.Timestamp(usable_from),
            'trans_date': pd.Timestamp(trans_date) if trans_date is not None else fd - pd.Timedelta(days=1),
            'code': code, 'shares': float(shares), 'price': float(price), 'value_usd': float(shares) * float(price),
            'acquired': code == 'P', 'owner_cik': owner, 'is_director': director, 'is_officer': officer,
            'officer_title': 'CEO' if officer else None, 'is_ten_pct': ten,
            'shares_after': float(shares_after) if shares_after is not None else float(shares) * 5.0,
            'accession': accession or f'acc-{acc_id[0]}', 'trans_sk': acc_id[0], 'n_owners': n_owners}


def make_events(seed: int = 11, with_marker: bool = True, marker_shift: int = 0) -> pd.DataFrame:
    """Events frame in the ``insider_events`` interface shape."""
    rng = np.random.default_rng(seed)
    rows: List[Dict[str, Any]] = []
    free = [t for t in TICKERS if t not in RESERVED]
    for _ in range(160):                                   # random officer/director purchases
        t = free[int(rng.integers(len(free)))]
        i = int(rng.integers(5, N_DAYS - 40))
        sh, px = float(rng.integers(500, 20000)), float(rng.uniform(20, 80))
        rows.append(_row(t, SESSIONS[i], SESSIONS[i + 1], sh, px, int(rng.integers(100, 400)),
                         officer=bool(rng.random() < 0.5), director=True,
                         shares_after=sh * float(rng.uniform(1.5, 40))))
    for _ in range(40):                                    # random sales
        t = free[int(rng.integers(len(free)))]
        i = int(rng.integers(5, N_DAYS - 40))
        rows.append(_row(t, SESSIONS[i], SESSIONS[i + 1], float(rng.integers(500, 20000)),
                         float(rng.uniform(20, 80)), int(rng.integers(100, 400)), code='S'))
    for year in range(2013, 2020):                         # routine buyer: every March
        fd = pd.Timestamp(f'{year}-03-12')
        rows.append(_row(ROUTINE, fd, fd + pd.offsets.BDay(1), 1500, 40.0, 900, trans_date=f'{year}-03-10'))
    for k, i in enumerate((100, 104, 108)):                # cluster: three distinct buyers
        rows.append(_row(CLUSTER, SESSIONS[i], SESSIONS[i + 1], 3000, 30.0, 501 + k))
    for owner in (601, 602):                               # one joint filing, two reporting owners
        rows.append(_row(JOINT, SESSIONS[120], SESSIONS[121], 2000, 100.0, owner, n_owners=2, accession='joint-1'))
    rows.append(_row(TENPCT, SESSIONS[130], SESSIONS[131], 100000, 50.0, 700, officer=False, director=False, ten=True))
    rows.append(_row(MISKEYED, SESSIONS[140], SESSIONS[141], 100, 1e6, 800))
    rows.append(_row(SELLER, SESSIONS[150], SESSIONS[151], 1000, 50.0, 850))
    rows.append(_row(SELLER, SESSIONS[152], SESSIONS[153], 20000, 50.0, 850, code='S'))
    if with_marker:
        u = marker_usable_from()
        pos = SESSIONS.get_loc(u) + marker_shift
        rows.append(_row(MARKER, SESSIONS[pos - 1], SESSIONS[pos], 2000, MARKER_VALUE / 2000.0, 950,
                         shares_after=6000))
    return ins.to_events_frame(rows)
