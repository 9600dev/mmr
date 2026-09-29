"""H1 — the program's survivor: top sales-to-price quintile of the covered PIT
universe (C03, PREREG.md H1; decision D05).

Signal: as-of trailing-twelve-month revenue / (close x as-of diluted shares).
The 2026-08-17 figure for this book (Sharpe 1.26 vs 1.04 covered-EW) is
PRE-D06/D07 (frozen leavers, implicit leverage) and is not a baseline.
"""
from __future__ import annotations

from typing import Dict

import pandas as pd

from strategies.lowfreq_equity_base import CoveredTilt, aligned, market_cap


def sales_to_price(close: pd.DataFrame, fields: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    return aligned(fields, close, 'revenue_ttm') / market_cap(close, fields)


class SalesToPriceQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        return sales_to_price(close, fields)
