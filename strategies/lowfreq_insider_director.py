"""H4b - director (non-officer) purchases, the contrast to H4a (C04 PREREG.md; D11, D14).

Qualify: trailing-63-session open-market purchase value by directors who are
NOT officers of at least $25,000. Directors buy less often and with less
day-to-day information; if H4b matches H4a the officer/director split carries
nothing.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import ValueRuleOverlay


class DirectorPurchaseOverlay(ValueRuleOverlay):
    MEASURE = 'p_value_director'
