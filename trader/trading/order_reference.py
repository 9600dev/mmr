"""Recover strategy attribution and execution identity from broker orderRef."""
from __future__ import annotations


def split_order_reference(value) -> tuple[str, str]:
    reference = str(value or '')
    strategy, separator, intent_id = reference.rpartition('|mmr:')
    return (strategy, intent_id) if separator else (reference, '')
