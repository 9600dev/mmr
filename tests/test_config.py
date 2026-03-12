import os
import pytest
from trader.config import (
    IB_GATEWAY_LIVE_PORT, IB_GATEWAY_PAPER_PORT,
    IB_LIVE_PORT, IB_PAPER_PORT, MMRConfig,
)


class TestMMRConfig:
    def test_from_yaml_parses_values(self, test_config_file):
        config = MMRConfig.from_yaml(test_config_file)
        assert config.ib.server_address == '127.0.0.1'
        # Fixture has trading_mode: paper → auto-resolves paper account/port
        assert config.ib.server_port == IB_PAPER_PORT
        assert config.ib.account == 'TESTPAPER'
        assert config.ib.paper_account == 'TESTPAPER'
        assert config.ib.live_account == 'TESTLIVE'
        assert config.zmq.rpc_server_port == 42001
        assert config.storage.universe_library == 'Universes'

    def test_defaults_when_keys_missing(self, tmp_path):
        minimal = tmp_path / "minimal.yaml"
        minimal.write_text("ib_server_address: 10.0.0.1\n")
        config = MMRConfig.from_yaml(str(minimal))
        assert config.ib.server_address == '10.0.0.1'
        # Default trading_mode is 'live' → live port
        assert config.ib.server_port == IB_LIVE_PORT
        # duckdb_path is resolved to absolute against project root
        assert config.storage.duckdb_path.endswith('data/mmr.duckdb')
        assert os.path.isabs(config.storage.duckdb_path)
        assert config.zmq.rpc_server_port == 42001

    def test_env_var_override_port(self, test_config_file, monkeypatch):
        monkeypatch.setenv('IB_SERVER_PORT', '9999')
        config = MMRConfig.from_yaml(test_config_file)
        assert config.ib.server_port == 9999

    def test_env_var_override_account(self, test_config_file, monkeypatch):
        monkeypatch.setenv('IB_ACCOUNT', 'ENVACCT')
        config = MMRConfig.from_yaml(test_config_file)
        assert config.ib.account == 'ENVACCT'

    def test_to_flat_dict_roundtrip(self, test_config_file):
        config = MMRConfig.from_yaml(test_config_file)
        flat = config.to_flat_dict()
        assert flat['ib_server_address'] == '127.0.0.1'
        assert flat['ib_server_port'] == IB_PAPER_PORT
        assert flat['ib_account'] == 'TESTPAPER'
        assert flat['ib_paper_account'] == 'TESTPAPER'
        assert flat['ib_live_account'] == 'TESTLIVE'
        assert flat['zmq_rpc_server_port'] == 42001
        assert 'duckdb_path' in flat

    # ── Account auto-resolution ──────────────────────────────────────────

    def test_paper_mode_selects_paper_account(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("trading_mode: paper\nib_paper_account: DU111\nib_live_account: U222\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.ib.account == 'DU111'
        assert config.paper_trading is True

    def test_live_mode_selects_live_account(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("trading_mode: live\nib_paper_account: DU111\nib_live_account: U222\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.ib.account == 'U222'
        assert config.paper_trading is False

    def test_ib_account_env_overrides_auto_selection(self, tmp_path, monkeypatch):
        monkeypatch.setenv('IB_ACCOUNT', 'OVERRIDE')
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("trading_mode: paper\nib_paper_account: DU111\nib_live_account: U222\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.ib.account == 'OVERRIDE'

    # ── Port auto-resolution ─────────────────────────────────────────────

    def test_paper_mode_selects_paper_port(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("trading_mode: paper\nib_paper_port: 7497\nib_live_port: 7496\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.ib.server_port == 7497

    def test_live_mode_selects_live_port(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("trading_mode: live\nib_paper_port: 7497\nib_live_port: 7496\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.ib.server_port == 7496

    def test_custom_ports_respected(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("trading_mode: paper\nib_paper_port: 4004\nib_live_port: 4003\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.ib.server_port == 4004

    def test_ib_server_port_env_overrides_auto_selection(self, tmp_path, monkeypatch):
        monkeypatch.setenv('IB_SERVER_PORT', '5555')
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("trading_mode: paper\nib_paper_port: 7497\nib_live_port: 7496\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.ib.server_port == 5555
