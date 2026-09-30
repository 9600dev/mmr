"""C0 - the purchase-count baseline, the confounder every refinement must beat
(C04 PREREG.md; D11, D14).

Qualify: at least one open-market purchase FILING (code P) by an officer or
director became usable within the trailing 63 sessions. No dollar floor, no
other filter: the fact of buying alone.
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import CountRuleOverlay


class PurchaseCountOverlay(CountRuleOverlay):
    MEASURE = 'p_filings'
