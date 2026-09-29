"""A CoveredTilt small enough for the three-name test panel (tests only)."""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt
from strategies.lowfreq_equity_s2p import sales_to_price


class TinySalesToPrice(CoveredTilt):
    QUANTILE = 0.67
    MIN_NAMES = 1
    MIN_COVERED = 1

    def signal(self, close, fields, signals):
        return sales_to_price(close, fields)
