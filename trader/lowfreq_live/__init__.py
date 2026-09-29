"""Live low-frequency signal and paper ledger (hillclimb C02).

``targets`` computes today's target weights for a registered harness candidate
with EXACTLY the research code path (``trader.research.lowfreq``: the same
loader, the same strategy class, the same ``precompute_panel`` -> ``on_panel``
-> ``normalise_weights`` sequence ``Backtester.run_panel`` runs) and writes a
D08 ``mmr.targets.v1`` file. ``track`` / ``report`` keep a paper ledger of a
sleeve's daily value against its virtual decisive benchmark.
"""
