"""Log topology + retention (2026-11-05 smoke-test findings).

* errors_<ts>.log received every ERROR twice: service loggers listed
  ``error_file_handler`` AND propagated to root, which lists it too.
* strategy_service created ``data_service_<its ts>.log`` because the shared
  ``ib_history`` logger is routed to ``data_service_handler``.
* nothing ever pruned session-stamped logs: 4,038 files / 1.6GB.
* every portfolio mark logged at DEBUG (now TRACE).
"""
import logging
import logging.config
import os
import pathlib
import time

import pytest
import yaml

from trader.common import logging_helper
from trader.common.logging_helper import (
    TRACE, ServiceRoleFilter, _attach_service_role_filters, _dedupe_propagated_handlers,
    log_retention_days, prune_old_logs,
)

REPO = pathlib.Path(__file__).resolve().parents[1]
DAY = 86400.0


@pytest.fixture
def restore_logging():
    yield
    # Put the session's real config back for the rest of the suite.
    logging_helper.global_loggers.clear()
    logging_helper.setup_logging(module_name='root')


# --------------------------------------------------------------------------
# Duplicate handlers via propagation
# --------------------------------------------------------------------------

def test_dedupe_removes_root_handler_from_propagating_logger():
    config = {
        'root': {'handlers': ['console', 'errors']},
        'loggers': {
            'svc': {'handlers': ['svc_file', 'errors'], 'propagate': True},
            'quiet': {'handlers': ['console', 'errors'], 'propagate': False},
        },
    }
    removed = _dedupe_propagated_handlers(config)
    assert removed == {'svc': ['errors']}
    assert config['loggers']['svc']['handlers'] == ['svc_file']
    # a non-propagating logger needs its own handlers — untouched
    assert config['loggers']['quiet']['handlers'] == ['console', 'errors']


def test_dedupe_considers_intermediate_ancestors_and_default_propagate():
    config = {
        'root': {'handlers': ['r']},
        'loggers': {
            'a': {'handlers': ['x'], 'propagate': False},
            'a.b': {'handlers': ['x', 'r', 'y']},  # propagate defaults to True
        },
    }
    _dedupe_propagated_handlers(config)
    # reaches 'a' (has x) and stops there: r is NOT inherited
    assert config['loggers']['a.b']['handlers'] == ['r', 'y']


def test_shipped_template_has_no_propagation_duplicates():
    config = yaml.safe_load((REPO / 'config_defaults' / 'logging.yaml').read_text())
    assert _dedupe_propagated_handlers(config) == {}


def test_error_written_once_to_errors_file(tmp_path, restore_logging):
    """End-to-end with the OLD (duplicated) shape, as in existing user configs."""
    errors = tmp_path / 'errors.log'
    svc = tmp_path / 'svc.log'
    fh = lambda p, lvl: {'class': 'logging.FileHandler', 'filename': str(p), 'level': lvl,
                         'formatter': 'f'}
    config = {
        'version': 1, 'disable_existing_loggers': False,
        'formatters': {'f': {'format': '%(name)s %(message)s'}},
        'handlers': {'errors': fh(errors, 'ERROR'), 'svc': fh(svc, 'DEBUG')},
        'root': {'level': 'NOTSET', 'handlers': ['errors']},
        'loggers': {'ibreactivex_t': {'level': 'DEBUG', 'handlers': ['svc', 'errors'],
                                      'propagate': True}},
    }
    _dedupe_propagated_handlers(config)
    logging.config.dictConfig(config)
    logging.getLogger('ibreactivex_t').error('ibrx reqId: 1 errorCode: 200')
    for h in logging.getLogger().handlers + logging.getLogger('ibreactivex_t').handlers:
        h.flush()
    assert errors.read_text().count('ibrx reqId: 1 errorCode: 200') == 1
    assert svc.read_text().count('ibrx reqId: 1 errorCode: 200') == 1


# --------------------------------------------------------------------------
# Service-file routing by process role
# --------------------------------------------------------------------------

def _record():
    return logging.LogRecord('ib_history', logging.INFO, __file__, 1, 'msg', None, None)


def test_role_filter_blocks_only_a_positively_different_service():
    assert ServiceRoleFilter('data_service', process_role='strategy_service').filter(_record()) is False
    assert ServiceRoleFilter('data_service', process_role='data_service').filter(_record()) is True
    # unknown role (CLI, pytest): old behaviour, never lose a log
    assert ServiceRoleFilter('data_service', process_role='').filter(_record()) is True


def test_process_role_detection(monkeypatch):
    import sys
    import types
    main = types.ModuleType('__main__')
    main.__spec__ = types.SimpleNamespace(name='trader.strategy_service')
    monkeypatch.setitem(sys.modules, '__main__', main)
    monkeypatch.setattr(sys, 'argv', ['x'])
    assert logging_helper._process_role() == 'strategy_service'
    main.__spec__ = None
    monkeypatch.setattr(sys, 'argv', ['/app/trader/data_service.py'])
    assert logging_helper._process_role() == 'data_service'
    monkeypatch.setattr(sys, 'argv', ['/usr/bin/mmr'])
    assert logging_helper._process_role() == ''


def test_strategy_process_does_not_create_data_service_file(tmp_path, restore_logging):
    data_file = tmp_path / 'data_service.log'
    strat_file = tmp_path / 'strategy_service.log'
    fh = lambda p: {'class': 'logging.handlers.RotatingFileHandler', 'filename': str(p),
                    'formatter': 'f', 'delay': True}
    config = {
        'version': 1, 'disable_existing_loggers': False,
        'formatters': {'f': {'format': '%(message)s'}},
        'handlers': {'data_service_handler': fh(data_file), 'strategy_service_handler': fh(strat_file)},
        'root': {'level': 'NOTSET', 'handlers': []},
        'loggers': {
            'ib_history_t': {'level': 'DEBUG', 'handlers': ['data_service_handler']},
            'strategy_runtime_t': {'level': 'DEBUG', 'handlers': ['strategy_service_handler']},
        },
    }
    _attach_service_role_filters(config, process_role='strategy_service')
    logging.config.dictConfig(config)
    logging.getLogger('ib_history_t').info('history line')
    logging.getLogger('strategy_runtime_t').info('strategy line')
    assert not data_file.exists()
    assert 'strategy line' in strat_file.read_text()


# --------------------------------------------------------------------------
# TRACE
# --------------------------------------------------------------------------

def test_trace_is_below_debug_and_not_emitted_at_debug():
    assert TRACE < logging.DEBUG
    assert logging.getLevelName(TRACE) == 'TRACE'
    lg = logging.getLogger('trace_probe')
    lg.setLevel(logging.DEBUG)
    assert not lg.isEnabledFor(TRACE)


def test_portfolio_mark_lines_are_trace():
    src = (REPO / 'trader' / 'trading' / 'trading_runtime.py').read_text()
    assert "logging.debug('__update_portfolio')" not in src
    assert "logging.debug('updating portfolio universe" not in src
    assert "logging.log(TRACE, '__update_portfolio')" in src


# --------------------------------------------------------------------------
# Retention
# --------------------------------------------------------------------------

def _touch(path: pathlib.Path, age_days: float, now: float, content: str = 'x'):
    path.write_text(content)
    t = now - age_days * DAY
    os.utime(path, (t, t))
    return path


@pytest.fixture
def now():
    return time.time()


def test_prunes_old_sessions_including_rotations(tmp_path, now):
    old = [
        _touch(tmp_path / 'trader_service_2026-09-01_10-00-00.log', 30, now),
        _touch(tmp_path / 'trader_service_2026-09-01_10-00-00.log.1', 31, now),
        _touch(tmp_path / 'errors_2026-09-01_10-00-00.log', 30, now),
        _touch(tmp_path / 'trader_2026-09-01_10-00-00.log.12', 40, now),
    ]
    fresh = _touch(tmp_path / 'trader_service_2026-11-01_10-00-00.log', 4, now)
    deleted = prune_old_logs(str(tmp_path), max_age_days=14, now=now)
    assert sorted(deleted) == sorted(str(p) for p in old)
    assert fresh.exists()


def test_session_with_any_recent_write_is_kept_whole(tmp_path, now):
    """A live long-running service: errors file idle for weeks, service file
    still getting its pulse. Deleting the idle file would orphan its handle."""
    idle = _touch(tmp_path / 'errors_2026-09-01_10-00-00.log', 60, now)
    rotated = _touch(tmp_path / 'trader_service_2026-09-01_10-00-00.log.3', 50, now)
    live = _touch(tmp_path / 'trader_service_2026-09-01_10-00-00.log', 0.01, now)
    assert prune_old_logs(str(tmp_path), max_age_days=14, now=now) == []
    assert idle.exists() and rotated.exists() and live.exists()


def test_never_deletes_current_session_or_unstamped_files(tmp_path, now, monkeypatch):
    stamp = '2026-08-01_09-00-00'
    monkeypatch.setattr(logging_helper, '_session_timestamp', stamp)
    mine = _touch(tmp_path / f'strategy_service_{stamp}.log', 90, now)
    mine_rot = _touch(tmp_path / f'strategy_service_{stamp}.log.2', 90, now)
    unstamped = [
        _touch(tmp_path / 'debug.log', 90, now),
        _touch(tmp_path / 'debug.log.3', 90, now),
        _touch(tmp_path / 'pycron_cron.log', 90, now),
        _touch(tmp_path / 'ib_gateway_watchdog.log', 90, now),
        _touch(tmp_path / 'notes.txt', 90, now),
    ]
    assert prune_old_logs(str(tmp_path), max_age_days=14, now=now) == []
    assert all(p.exists() for p in [mine, mine_rot, *unstamped])


def test_never_deletes_a_file_held_by_a_handler_here(tmp_path, now):
    held = _touch(tmp_path / 'data_service_2026-07-01_08-00-00.log', 90, now)
    sibling = _touch(tmp_path / 'errors_2026-07-01_08-00-00.log', 90, now)
    handler = logging.FileHandler(str(held), delay=True)
    lg = logging.getLogger('retention_probe_holder')
    lg.addHandler(handler)
    try:
        assert prune_old_logs(str(tmp_path), max_age_days=14, now=now) == []
    finally:
        lg.removeHandler(handler)
        handler.close()
    assert held.exists() and sibling.exists()


def test_zero_disables_and_missing_dir_is_noop(tmp_path, now):
    f = _touch(tmp_path / 'trader_2026-01-01_00-00-00.log', 300, now)
    assert prune_old_logs(str(tmp_path), max_age_days=0, now=now) == []
    assert f.exists()
    assert prune_old_logs(str(tmp_path / 'nope'), max_age_days=14, now=now) == []


def test_undeletable_file_is_reported_not_raised(tmp_path, now, monkeypatch, caplog):
    f = _touch(tmp_path / 'trader_2026-01-01_00-00-00.log', 300, now)
    real_remove = os.remove

    def deny(path):
        if str(path) == str(f):
            raise PermissionError(13, 'Permission denied', str(path))
        real_remove(path)

    monkeypatch.setattr(logging_helper.os, 'remove', deny)
    lg = logging.getLogger('retention_probe')
    with caplog.at_level(logging.WARNING, logger='retention_probe'):
        assert prune_old_logs(str(tmp_path), max_age_days=14, now=now, logger=lg) == []
    assert f.exists()
    assert any('could not delete' in r.getMessage() for r in caplog.records)


def test_retention_env(monkeypatch):
    monkeypatch.delenv('MMR_LOG_RETENTION_DAYS', raising=False)
    assert log_retention_days() == 14
    monkeypatch.setenv('MMR_LOG_RETENTION_DAYS', '3')
    assert log_retention_days() == 3
    monkeypatch.setenv('MMR_LOG_RETENTION_DAYS', '0')
    assert log_retention_days() == 0


@pytest.mark.parametrize('bad', ['two', '-1', '1.5'])
def test_retention_env_bad_value_fails_loudly(monkeypatch, bad):
    monkeypatch.setenv('MMR_LOG_RETENTION_DAYS', bad)
    with pytest.raises(ValueError, match='MMR_LOG_RETENTION_DAYS'):
        log_retention_days()


def test_default_uses_env_retention(tmp_path, now, monkeypatch):
    monkeypatch.setenv('MMR_LOG_RETENTION_DAYS', '30')
    f = _touch(tmp_path / 'trader_2026-10-01_00-00-00.log', 20, now)
    assert prune_old_logs(str(tmp_path), now=now) == []
    monkeypatch.setenv('MMR_LOG_RETENTION_DAYS', '10')
    assert prune_old_logs(str(tmp_path), now=now) == [str(f)]


@pytest.mark.parametrize('service', [
    'trader/trader_service.py', 'trader/strategy_service.py', 'trader/data_service.py'])
def test_services_prune_at_startup(service):
    assert 'prune_old_logs(' in (REPO / service).read_text()
