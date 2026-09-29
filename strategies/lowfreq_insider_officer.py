"""H4a - officer purchases (C04 PREREG.md; D11, D14).

Qualify: at least one open-market purchase filing by an OFFICER (any officer
title; a director who is also an officer counts here) usable within the
trailing 63 sessions. Officers run the business day to day and hold the
most private information; H4b is the director contrast.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import CountRuleOverlay


class OfficerPurchaseOverlay(CountRuleOverlay):
    MEASURE = 'p_filings_officer'
