"""H3 - conviction purchases: large relative to the insider's own holding
(C04 PREREG.md; D11, D14).

Qualify: trailing-63-session officer/director purchase value of at least
$25,000 counting only purchases that were at least 10% of the insider's
POST-trade holding (``shares / shares_after >= 0.10``; a purchase that
raised the stake by >= 11%). Holding-relative, so it needs no market cap and
applies to every name.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import ValueRuleOverlay


class ConvictionPurchaseOverlay(ValueRuleOverlay):
    MEASURE = 'p_value_conv'
