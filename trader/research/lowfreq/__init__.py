"""Low-frequency research harness (design record D01-D05).

Public API: ``load_etf_panel``, ``load_equity_panel`` (loaders) and
``evaluate`` (the protocol). CLI: ``python -m trader.research.lowfreq``.
"""
from trader.research.lowfreq.loaders import PricePanel, load_equity_panel, load_etf_panel  # noqa: F401
from trader.research.lowfreq.evaluate import evaluate  # noqa: F401

__all__ = ['PricePanel', 'load_etf_panel', 'load_equity_panel', 'evaluate']
