"""H4a - officer purchases (C04 PREREG.md; D11, D14).

Qualify: trailing-63-session open-market purchase value by OFFICERS (any
officer title; a director who is also an officer counts here) of at least
$25,000. Officers run the business day to day and hold the most private
information; H4b is the director contrast.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import ValueRuleOverlay


class OfficerPurchaseOverlay(ValueRuleOverlay):
    MEASURE = 'p_value_officer'
