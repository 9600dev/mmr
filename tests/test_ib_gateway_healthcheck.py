"""The forwarding socket must not make a stopped Java API look healthy."""

import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "scripts/ib_gateway_healthcheck.sh"


@pytest.fixture
def probe(tmp_path):
    # Emulate TCP outcomes at the subprocess boundary, retaining the actual
    # shell probe and its deadline/port arguments. Docker smoke also exercises
    # the native socket check with real listeners in an isolated container.
    timeout = tmp_path / "timeout"
    timeout.write_text(f"#!{sys.executable}\n" + """import os, pathlib, sys
assert sys.argv[1:5] == ['-k', '1', '3', 'bash']
assert sys.argv[-2] == '--'
port = sys.argv[-1]
with pathlib.Path(os.environ['PROBE_CALLS']).open('a') as out:
    out.write(port + '\\n')
raise SystemExit(0 if port in os.environ['OPEN_PORTS'].split(',') else 1)
""")
    timeout.chmod(0o755)
    calls = tmp_path / "calls"

    def run(mode, open_ports, dual="no"):
        env = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
               "TRADING_MODE": mode, "DUAL_MODE": dual,
               "OPEN_PORTS": open_ports, "PROBE_CALLS": str(calls)}
        result = subprocess.run(["bash", str(PROBE)], env=env, capture_output=True,
                                text=True, timeout=10)
        return result, calls.read_text().splitlines() if calls.exists() else []

    return run


@pytest.mark.parametrize("mode,forwarder,api", [("paper", "4004", "4002"),
                                                ("live", "4003", "4001")])
def test_open_forwarder_cannot_mask_closed_java_api(probe, mode, forwarder, api):
    result, calls = probe(mode, forwarder)
    assert result.returncode != 0
    assert calls == [api]
    assert f"IB API port {api} unavailable" in result.stderr


@pytest.mark.parametrize("mode,api", [("paper", "4002"), ("live", "4001")])
def test_native_api_listener_passes(probe, mode, api):
    result, calls = probe(mode, api)
    assert result.returncode == 0, result.stderr
    assert calls == [api]


@pytest.mark.parametrize("mode,dual", [("both", "no"), ("paper", "yes")])
@pytest.mark.parametrize("ports,passes", [("4001", False), ("4002", False),
                                          ("4001,4002", True)])
def test_dual_mode_requires_both_api_listeners(probe, mode, dual, ports, passes):
    result, calls = probe(mode, ports, dual)
    assert (result.returncode == 0) == passes
    assert calls[0] == "4001"
    if "4001" in ports:
        assert calls == ["4001", "4002"]


@pytest.mark.parametrize("mode", ["", "invalid"])
def test_unknown_mode_fails_without_guessing_a_port(probe, mode):
    result, calls = probe(mode, "4001,4002,4003,4004")
    assert result.returncode != 0
    assert calls == []
    assert result.returncode == 2
    assert "unknown TRADING_MODE" in result.stderr


def test_timeout_tool_failure_is_unassessable_not_a_closed_api(tmp_path):
    timeout = tmp_path / "timeout"
    attempted = tmp_path / "timeout attempted"
    timeout.write_text(f"#!{sys.executable}\nfrom pathlib import Path\n"
                       f"Path({str(attempted)!r}).write_text('attempted')\n"
                       "raise SystemExit(125)\n")
    timeout.chmod(0o755)
    env = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
           "TRADING_MODE": "paper", "DUAL_MODE": "no"}

    result = subprocess.run(["bash", str(PROBE)], env=env, capture_output=True,
                            text=True, timeout=10)

    assert attempted.read_text() == "attempted"
    assert result.returncode == 2
    assert "unassessable: API probe failed (exit 125)" in result.stderr
    assert "IB API port 4002 unavailable" not in result.stderr


def test_compose_uses_the_readonly_native_api_probe():
    gateway = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]["ib-gateway"]
    assert gateway["healthcheck"]["test"] == [
        "CMD", "bash", "/home/ibgateway/scripts/mmr-healthcheck.sh",
    ]
    assert "./scripts/ib_gateway_healthcheck.sh:/home/ibgateway/scripts/mmr-healthcheck.sh:ro" in gateway["volumes"]
