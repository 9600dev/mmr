"""H1 - opportunistic (non-routine) insider purchases, Cohen, Malloy & Pomorski
(2012) (C04 PREREG.md; D11, D14).

Qualify: at least one officer/director purchase filing usable within the
trailing 63 sessions by an insider who is NOT routine for that issuer -
routine = the same owner bought the same issuer in the same calendar month
in each of the three prior years, counting only prior purchases already
usable at the classified purchase's own usable_from
(``insider_signals.routine_flags``).
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import CountRuleOverlay


class OpportunisticPurchaseOverlay(CountRuleOverlay):
    MEASURE = 'p_filings_opp'
