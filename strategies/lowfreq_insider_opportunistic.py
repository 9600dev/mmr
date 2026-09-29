"""H1 - opportunistic (non-routine) insider purchases, Cohen, Malloy & Pomorski
(2012) (C04 PREREG.md; D11, D14).

Qualify: trailing-63-session officer/director open-market purchase value of
at least $25,000 counting ONLY purchases by insiders who are NOT routine for
that issuer - routine = the same owner bought the same issuer in the same
calendar month in each of the three prior years (``insider_signals.routine_flags``,
prior years only).
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import ValueRuleOverlay


class OpportunisticPurchaseOverlay(ValueRuleOverlay):
    MEASURE = 'p_value_opp'
