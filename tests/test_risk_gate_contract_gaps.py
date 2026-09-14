"""Direct risk API defaults and truthful legacy-turnover accounting."""

import datetime as dt
import re
import sys

from trader.data.event_store import EventType, TradingEvent
from trader.objects import Action
from trader.trading import risk_gate as risk_module
from trader.trading.risk_gate import RiskGate, RiskLimits
from trader.trading.strategy import Signal


def _signal():
    return Signal(source_name="alpha", action=Action.BUY, probability=0.8, risk=0.2, conid=4391)


def _submission(store, order_id, strategy="alpha", **metadata):
    store.append(TradingEvent(
        event_type=EventType.ORDER_SUBMITTED,
        timestamp=dt.datetime.now(),
        strategy_name=strategy,
        conid=4391,
        order_id=order_id,
        quantity=1.0,
        price=sys.float_info.max,
        metadata=metadata,
    ))


def test_omitted_open_count_leaves_the_first_available_order_slot(event_store):
    gate = RiskGate(RiskLimits(max_open_orders=1), event_store)
    result = gate.evaluate(_signal(), portfolio_value=10_000.0, position_value=100.0)
    assert result.approved is True
    assert result.checks["max_open_orders"] == "pass"


def test_omitted_portfolio_value_never_invents_capacity_for_a_penny_order(event_store):
    gate = RiskGate(RiskLimits(), event_store)
    result = gate.evaluate(_signal(), position_value=0.05, sec_type="STK")
    assert result.approved is False
    assert result.checks["concentration"] == "fail"


def test_turnover_reports_every_legacy_row_separately_from_current_unknown_rows(event_store):
    _submission(event_store, 1)
    _submission(event_store, 2, strategy="beta")
    _submission(event_store, 3, strategy="gamma", notional_evaluable=False)
    _submission(event_store, 4, notional_evaluable=True, notional=1234.0)
    _submission(event_store, 5, exit_class=True)

    result = RiskGate(RiskLimits(), event_store)._daily_open_notional()
    assert result == (1234.0, {"alpha": 1234.0}, 1, 2)


def test_legacy_turnover_warning_preserves_the_known_numeric_lower_bound(event_store, monkeypatch):
    _submission(event_store, 1)
    _submission(event_store, 2, notional_evaluable=True, notional=1234.0)
    messages = []

    def record_warning(message, *args, **kwargs):
        messages.append(message % args if args else message)

    monkeypatch.setattr(risk_module.logging, "warning", record_warning)
    gate = RiskGate(RiskLimits(max_daily_open_notional=10_000.0), event_store)
    result = gate.evaluate(_signal(), portfolio_value=100_000.0, position_value=100.0)

    assert result.approved is True
    assert result.checks["daily_turnover"] == "pass"
    amounts = [
        float(value.replace(",", ""))
        for message in messages
        for value in re.findall(r"\$\s*([\d,]+(?:\.\d+)?)", message)
    ]
    assert 1234.0 in amounts, "legacy history must retain the known-dollar lower bound in its warning"
