"""H5 - net purchase ratio, Lakonishok & Lee (2001) (C04 PREREG.md; D11, D14).

Qualify: at least one officer/director purchase filing usable within the
trailing 126 sessions AND, over those sessions, NPR = (P$ - S$) / (P$ + S$)
>= 0.5 with dollar values 1/n_owners-weighted. Sales are dominated by
scheduled and option-related selling, so a name only qualifies when purchases
clearly outweigh them. The one dollar-based rule, because netting two flows
needs a common unit.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_insider_base import InsiderOverlay
from trader.research.lowfreq import insider_signals as ins


class NetBuyRatioOverlay(InsiderOverlay):
    LOOKBACK = 126
    NPR_MIN = 0.5

    def qualifies(self, frames, close, fields) -> pd.DataFrame:
        p = ins.trailing_sum(frames['p_value'], self.LOOKBACK)
        s = ins.trailing_sum(frames['s_value'], self.LOOKBACK)
        n = ins.trailing_sum(frames['p_filings'], self.LOOKBACK)
        total = p + s
        npr = (p - s) / total.where(total > 0)
        return (npr >= self.NPR_MIN) & (n >= 1)
