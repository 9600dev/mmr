"""Sizing enforces the stricter of its own daily-loss limit and the risk gate's."""
from trader.trading.position_sizing import PortfolioState, PositionSizer, PositionSizingConfig


def _state(pnl):
    return PortfolioState(net_liquidation=100_000.0, daily_pnl=pnl)


def test_gate_stricter_blocks_sizing():
    sizer = PositionSizer(PositionSizingConfig(daily_loss_limit_usd=2000.0), gate_daily_loss_limit=1000.0)
    assert sizer.effective_daily_loss_limit == 1000.0
    r = sizer.compute(confidence=0.5, portfolio_state=_state(-1200.0))
    assert r.capped_by == 'daily_loss_limit'
    assert any('$1,000' in w for w in r.warnings)


def test_no_gate_keeps_sizing_limit():
    sizer = PositionSizer(PositionSizingConfig(daily_loss_limit_usd=2000.0))
    assert sizer.effective_daily_loss_limit == 2000.0
    r = sizer.compute(confidence=0.5, portfolio_state=_state(-1200.0))
    assert r.capped_by != 'daily_loss_limit'


def test_gate_looser_does_not_relax_sizing():
    sizer = PositionSizer(PositionSizingConfig(daily_loss_limit_usd=500.0), gate_daily_loss_limit=1000.0)
    assert sizer.effective_daily_loss_limit == 500.0
    assert sizer.daily_loss_limit_source() == 'position_sizing.daily_loss_limit_usd'


def test_session_summary_reports_and_warns_on_mismatch():
    sizer = PositionSizer(PositionSizingConfig(daily_loss_limit_usd=2000.0), gate_daily_loss_limit=1000.0)
    s = sizer.session_summary(_state(0.0))
    assert s['config']['effective_daily_loss_limit_usd'] == 1000.0
    assert s['config']['daily_loss_limit_source'] == 'risk_gate.max_daily_loss'
    assert s['config']['daily_loss_limit_usd'] == 2000.0  # config value untouched
    assert any('disagree' in w for w in s['warnings'])


def test_save_does_not_persist_gate_value(tmp_path):
    cfg = PositionSizingConfig(daily_loss_limit_usd=2000.0)
    PositionSizer(cfg, gate_daily_loss_limit=1000.0).compute(confidence=0.5, portfolio_state=_state(0.0))
    p = tmp_path / 'ps.yaml'
    cfg.save(str(p))
    assert PositionSizingConfig.load(str(p)).daily_loss_limit_usd == 2000.0
