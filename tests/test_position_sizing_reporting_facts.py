"""Essential sizing explanation facts, without whole-sentence snapshots."""

import re

import pytest

from trader.trading.position_sizing import (
    PortfolioState,
    PositionSizer,
    PositionSizingConfig,
    VolatilityInfo,
)


def _config(**changes):
    values = {
        "base_position_usd": 5000.0,
        "min_position_usd": 0.01,
        "max_position_usd": 100_000.0,
        "max_position_pct": 1.0,
        "max_total_exposure_pct": 1.0,
        "volatility_adjustment": False,
    }
    values.update(changes)
    return PositionSizingConfig(**values)


@pytest.mark.parametrize("confidence,reported,amount", [(2.0, 1.0, 5000.0), (-1.0, 0.0, 1500.0)])
def test_explanation_reports_effective_clamped_confidence(confidence, reported, amount):
    result = PositionSizer(_config()).compute(confidence=confidence)
    match = re.search(r"confidence\s+([-+]?\d+(?:\.\d+)?)", result.reasoning, re.IGNORECASE)
    assert match is not None, "the applied confidence must be explained"
    assert float(match.group(1)) == reported
    assert result.amount_usd == amount


@pytest.mark.parametrize(
    "base_pct,account,percentage,reported_account,amount",
    [
        (0.0, 100_000.0, None, None, 5000.0),
        (0.05, 0.0, None, None, 5000.0),
        (0.05, 10_000.0, 5.0, 10_000.0, 500.0),
        # Explanations currently round dollars to whole units; $1 is exact.
        (0.5, 1.0, 50.0, 1.0, 0.5),
    ],
    ids=["fixed-account", "fixed-offline", "percentage", "one-dollar-account"],
)
def test_base_explanation_identifies_the_actually_applied_percentage(
    base_pct, account, percentage, reported_account, amount,
):
    result = PositionSizer(_config(base_position_pct=base_pct)).compute(
        confidence=1.0, portfolio_state=PortfolioState(net_liquidation=account),
    )
    match = re.search(
        r"(\d+(?:\.\d+)?)\s*%\s+of\s+\$([\d,]+(?:\.\d+)?)",
        result.reasoning,
        re.IGNORECASE,
    )
    assert result.amount_usd == amount
    if percentage is None:
        # A fixed $5,000 base is not 0% of an account or 5% of a zero account.
        assert match is None, "fixed-dollar sizing must not claim an unapplied percentage"
    else:
        assert match is not None, "percentage-derived sizing must disclose that basis"
        assert float(match.group(1)) == percentage
        assert float(match.group(2).replace(",", "")) == reported_account


@pytest.mark.parametrize("risk_level,multiplier,amount", [("conservative", 0.5, 2500.0), ("aggressive", 1.5, 7500.0)])
def test_non_unit_risk_multiplier_is_disclosed_as_an_applied_fact(risk_level, multiplier, amount):
    result = PositionSizer(_config(risk_level=risk_level)).compute(confidence=1.0)
    multipliers = [
        float(match) for match in re.findall(r"(\d+(?:\.\d+)?)\s*[x×]", result.reasoning, re.IGNORECASE)
    ]
    assert result.amount_usd == amount
    assert multiplier in multipliers, "the applied non-unit risk multiplier must be explained"


@pytest.mark.parametrize("volatility", [None, VolatilityInfo(atr=0.0, price=100.0)])
def test_enabled_adjustment_discloses_unavailable_volatility(volatility):
    result = PositionSizer(_config(volatility_adjustment=True)).compute(
        confidence=1.0, volatility=volatility,
    )
    text = result.reasoning.casefold()
    assert result.amount_usd == 5000.0
    assert "volatility" in text or "atr" in text
    assert any(marker in text for marker in ("no ", "missing", "unavailable", "unknown", "not available"))
