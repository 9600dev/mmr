"""Operator sizing settings must round-trip through the selected config directory."""
from pathlib import Path

import yaml

from trader.trading.position_sizing import PositionSizingConfig


def test_default_save_and_load_use_selected_operator_config_directory(tmp_path, monkeypatch):
    from trader import container

    selected_config_dir = tmp_path / 'operator-config'
    selected_config_dir.mkdir()
    working_directory = Path.cwd()
    assert selected_config_dir != working_directory
    fallback_path = working_directory / 'position_sizing.yaml'
    fallback_before = fallback_path.read_bytes() if fallback_path.exists() else None
    # Neither public method receives an explicit path. The user's real config
    # discovery/copy routine is never called by this test.
    monkeypatch.setattr(container, 'ensure_config_dir', lambda: selected_config_dir)
    config = PositionSizingConfig(
        base_position_usd=875.0,
        max_position_usd=2500.0,
        max_position_pct=0.02,
        max_positions=3,
        risk_level='conservative',
        volatility_adjustment=False,
    )

    try:
        config.save()

        saved_path = selected_config_dir / 'position_sizing.yaml'
        assert saved_path.is_file()
        saved = yaml.safe_load(saved_path.read_text())
        assert saved['base_position_usd'] == 875.0
        assert saved['risk_level'] == 'conservative'
        assert saved['volatility_adjustment'] is False

        # The loaded settings must come from disk, rather than the object that
        # was just saved or the built-in defaults.
        config.base_position_usd = 9999.0
        loaded = PositionSizingConfig.load()
        assert loaded.base_position_usd == 875.0
        assert loaded.max_position_usd == 2500.0
        assert loaded.max_position_pct == 0.02
        assert loaded.max_positions == 3
        assert loaded.risk_level == 'conservative'
        assert loaded.volatility_adjustment is False
        assert Path.cwd() == working_directory
        assert (fallback_path.read_bytes() if fallback_path.exists() else None) == fallback_before
    finally:
        # A failing implementation/mutant must not leave an operator file
        # changed. Keeping cwd stable also lets mutmut locate its source tree.
        fallback_after = fallback_path.read_bytes() if fallback_path.exists() else None
        if fallback_after != fallback_before:
            if fallback_before is None:
                fallback_path.unlink()
            else:
                fallback_path.write_bytes(fallback_before)
