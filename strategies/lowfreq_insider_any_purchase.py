"""C0 - the purchase-count baseline / confounder (C04 PREREG.md; D11, D14).

Qualify: any open-market purchase (code P) by an officer or director whose
1/n_owners-weighted dollar value, summed over the trailing 63 sessions of
usable events, reaches $25,000. Every refined hypothesis must beat this.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import ValueRuleOverlay


class AnyPurchaseOverlay(ValueRuleOverlay):
    MEASURE = 'p_value'
