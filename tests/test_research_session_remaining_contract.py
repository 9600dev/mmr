"""Boundary cases for invalid inputs and warning policy.

These assertions describe refusal and first-warning behavior; they do not
require a particular diagnostic sentence or a particular statistical score.
"""
import logging

import numpy as np

from trader.data import market_session
from trader.simulation.selection_bias import deflated_sharpe


def test_zero_frequency_is_unevaluable_even_with_only_one_trial():
    returns = np.array([-1.0, 0.0, 1.0])

    assert deflated_sharpe(returns, [1.0], bars_per_year=0.0) is None


def test_unknown_venue_warns_on_its_first_observation_not_its_repeat(monkeypatch, caplog):
    logger = logging.getLogger("mmr.contract.first_unknown_venue")
    monkeypatch.setattr(market_session, "logging", logger)
    monkeypatch.setattr(market_session, "_warned_unknown", set())
    caplog.set_level(logging.WARNING, logger=logger.name)

    assert market_session.in_session("2026-09-09T10:00:00Z", "UNMAPPED_A", "UNMAPPED_B") is True
    first = [record for record in caplog.records if record.name == logger.name]
    assert len(first) == 1
    assert first[0].levelno >= logging.WARNING
    assert "UNMAPPED_A" in first[0].getMessage()
    assert "UNMAPPED_B" in first[0].getMessage()

    assert market_session.in_session("2026-09-09T10:01:00Z", "UNMAPPED_A", "UNMAPPED_B") is True
    repeated = [record for record in caplog.records if record.name == logger.name]
    assert repeated == first
