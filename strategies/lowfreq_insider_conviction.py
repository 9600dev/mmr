"""H3 - conviction purchases: large relative to the insider's own holding
(C04 PREREG.md; D11, D14).

Qualify: at least one officer/director purchase filing usable within the
trailing 63 sessions whose purchase was at least 10% of the insider's
POST-trade holding (``shares / shares_after >= 0.10``, a stake increase of
>= 11%). Holding-relative, so it needs no market cap and applies to every name.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import CountRuleOverlay


class ConvictionPurchaseOverlay(CountRuleOverlay):
    MEASURE = 'p_filings_conv'
