"""D1..D8 - size-matched controls, one per rule (C04 PREREG.md; D11).

Each control mirrors one rule's qualifiers exactly, but on every rebalance
overlays, for each qualifier, the covered NON-qualifying name nearest in
MARKET CAP (close x as-of diluted shares; ties by ticker; each match used
once) instead of the qualifier itself. What a control earns is the part of
its rule's active return that comes from the SIZE of the names insiders
bought, not from the buying. Controls are never champions; a rule is
credited only if it beats its own control (PREREG.md, champion rule).
"""
from __future__ import annotations

from strategies.lowfreq_insider_base import SizeMatchedOverlay
from strategies.lowfreq_insider_cluster import ClusterPurchaseOverlay
from strategies.lowfreq_insider_conviction import ConvictionPurchaseOverlay
from strategies.lowfreq_insider_director import DirectorPurchaseOverlay
from strategies.lowfreq_insider_net_buy_ratio import NetBuyRatioOverlay
from strategies.lowfreq_insider_officer import OfficerPurchaseOverlay
from strategies.lowfreq_insider_opportunistic import OpportunisticPurchaseOverlay
from strategies.lowfreq_insider_purchase_count import PurchaseCountOverlay
from strategies.lowfreq_insider_value_to_mcap import ValueToMcapOverlay


class SizeMatchedPurchaseCount(SizeMatchedOverlay):
    RULE = PurchaseCountOverlay


class SizeMatchedOpportunistic(SizeMatchedOverlay):
    RULE = OpportunisticPurchaseOverlay


class SizeMatchedCluster(SizeMatchedOverlay):
    RULE = ClusterPurchaseOverlay


class SizeMatchedConviction(SizeMatchedOverlay):
    RULE = ConvictionPurchaseOverlay


class SizeMatchedOfficer(SizeMatchedOverlay):
    RULE = OfficerPurchaseOverlay


class SizeMatchedDirector(SizeMatchedOverlay):
    RULE = DirectorPurchaseOverlay


class SizeMatchedNetBuyRatio(SizeMatchedOverlay):
    RULE = NetBuyRatioOverlay


class SizeMatchedValueToMcap(SizeMatchedOverlay):
    RULE = ValueToMcapOverlay
