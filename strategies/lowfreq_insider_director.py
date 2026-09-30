"""H4b - director (non-officer) purchases, the contrast to H4a (C04 PREREG.md; D11, D14).

Qualify: at least one open-market purchase filing by a director who is NOT
an officer usable within the trailing 63 sessions. Directors buy with less
day-to-day information; if H4b matches H4a the officer/director split
carries nothing.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import CountRuleOverlay


class DirectorPurchaseOverlay(CountRuleOverlay):
    MEASURE = 'p_filings_director'
