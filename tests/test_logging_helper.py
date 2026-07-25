"""Logging config resilience + the debug.log stamping exemption.

Regression cover for the 2026-07-24 uid-split bug: the container ran its
services as root while `docker.sh -e` execs in as trader, so the root-owned
/tmp/debug.log could not be opened by CLI runs. dictConfig raises on the
FIRST unopenable handler, and setup_logging's only fallback was
basicConfig() — so one unwritable path silently cost the CLI its console
handler and every service log with it.
"""

import logging
import logging.config
import os

import pytest
import yaml

from trader.common.logging_helper import (
    MMR_LOG_DIR,
    _drop_unwritable_handlers,
    _stamp_log_filenames,
)


# chmod-based denial is meaningless as root — which is exactly the uid this
# whole bug came from, so guard rather than emit a confusing failure.
requires_non_root = pytest.mark.skipif(
    hasattr(os, 'geteuid') and os.geteuid() == 0,
    reason='root bypasses file permissions',
)


def _rotating(filename: str) -> dict:
    return {
        'class': 'logging.handlers.RotatingFileHandler',
        'formatter': 'standard',
        'filename': filename,
    }


def _config(handlers: dict, root_handlers: list, loggers: dict | None = None) -> dict:
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {'standard': {'format': '%(message)s'}},
        'handlers': handlers,
        'root': {'level': 'NOTSET', 'handlers': root_handlers},
        'loggers': loggers or {},
    }


class TestStampLogFilenames:
    def test_debug_log_is_not_session_stamped(self):
        """debug.log is the single cross-session triage file — stamping it
        would fragment it into one partial file per process, which is the
        exact problem it exists to solve."""
        config = {'handlers': {'dbg': {'filename': '~/.local/share/mmr/logs/debug.log'}}}
        _stamp_log_filenames(config)
        assert config['handlers']['dbg']['filename'] == os.path.join(MMR_LOG_DIR, 'debug.log')

    def test_sibling_logs_are_session_stamped(self):
        config = {'handlers': {'svc': {'filename': '~/.local/share/mmr/logs/trader_service.log'}}}
        _stamp_log_filenames(config)
        filename = config['handlers']['svc']['filename']
        assert filename.startswith(os.path.join(MMR_LOG_DIR, 'trader_service_'))
        assert filename.endswith('.log')

    def test_tilde_is_expanded(self):
        config = {'handlers': {'svc': {'filename': '~/.local/share/mmr/logs/trader_service.log'}}}
        _stamp_log_filenames(config)
        assert '~' not in config['handlers']['svc']['filename']

    def test_handler_without_filename_is_untouched(self):
        config = {'handlers': {'console': {'class': 'logging.StreamHandler'}}}
        _stamp_log_filenames(config)
        assert 'filename' not in config['handlers']['console']


class TestDropUnwritableHandlers:
    @requires_non_root
    def test_unwritable_handler_is_dropped(self, tmp_path):
        blocked = tmp_path / 'blocked.log'
        blocked.touch()
        blocked.chmod(0o000)
        config = _config({'blocked': _rotating(str(blocked))}, ['blocked'])

        assert _drop_unwritable_handlers(config) == ['blocked']
        assert 'blocked' not in config['handlers']

    @requires_non_root
    def test_writable_handlers_survive(self, tmp_path):
        blocked = tmp_path / 'blocked.log'
        blocked.touch()
        blocked.chmod(0o000)
        good = tmp_path / 'good.log'
        config = _config(
            {
                'console': {'class': 'logging.StreamHandler', 'formatter': 'standard'},
                'good': _rotating(str(good)),
                'blocked': _rotating(str(blocked)),
            },
            ['console', 'good', 'blocked'],
        )

        _drop_unwritable_handlers(config)

        assert set(config['handlers']) == {'console', 'good'}

    @requires_non_root
    def test_dropped_handler_is_purged_from_every_handler_list(self, tmp_path):
        """A dangling reference in root/loggers makes dictConfig fail again —
        defeating the whole point of dropping the handler."""
        blocked = tmp_path / 'blocked.log'
        blocked.touch()
        blocked.chmod(0o000)
        good = tmp_path / 'good.log'
        config = _config(
            {'good': _rotating(str(good)), 'blocked': _rotating(str(blocked))},
            ['good', 'blocked'],
            loggers={'svc': {'level': 'DEBUG', 'handlers': ['good', 'blocked'], 'propagate': True}},
        )

        _drop_unwritable_handlers(config)

        assert config['root']['handlers'] == ['good']
        assert config['loggers']['svc']['handlers'] == ['good']

    @requires_non_root
    def test_surviving_config_applies_and_logs(self, tmp_path):
        """End-to-end: the old code raised here and fell back to basicConfig."""
        blocked = tmp_path / 'blocked.log'
        blocked.touch()
        blocked.chmod(0o000)
        good = tmp_path / 'good.log'
        config = _config(
            {'good': _rotating(str(good)), 'blocked': _rotating(str(blocked))},
            ['good', 'blocked'],
        )

        with pytest.raises(ValueError, match="Unable to configure handler 'blocked'"):
            logging.config.dictConfig(dict(config, handlers=dict(config['handlers'])))

        _drop_unwritable_handlers(config)
        logging.config.dictConfig(config)
        try:
            logging.getLogger('test_surviving_config').warning('still logging')
            assert 'still logging' in good.read_text()
        finally:
            logging.config.dictConfig({'version': 1, 'disable_existing_loggers': False})

    def test_probe_does_not_create_missing_files(self, tmp_path):
        """The probe must not defeat `delay: true` — that is what keeps every
        short-lived process from leaving an empty log file behind."""
        missing = tmp_path / 'not_yet.log'
        config = _config({'h': _rotating(str(missing))}, ['h'])

        assert _drop_unwritable_handlers(config) == []
        assert not missing.exists()

    @requires_non_root
    def test_unwritable_parent_directory_is_dropped(self, tmp_path):
        blocked_dir = tmp_path / 'nowrite'
        blocked_dir.mkdir()
        blocked_dir.chmod(0o500)
        config = _config({'h': _rotating(str(blocked_dir / 'x.log'))}, ['h'])
        try:
            assert _drop_unwritable_handlers(config) == ['h']
        finally:
            blocked_dir.chmod(0o700)

    def test_no_handlers_dropped_leaves_config_untouched(self, tmp_path):
        config = _config({'h': _rotating(str(tmp_path / 'a.log'))}, ['h'])
        assert _drop_unwritable_handlers(config) == []
        assert config['root']['handlers'] == ['h']


class TestShippedConfig:
    """The shipped template must keep the properties the code relies on."""

    @pytest.fixture
    def shipped(self):
        path = os.path.join(os.path.dirname(__file__), '..', 'config_defaults', 'logging.yaml')
        with open(path) as f:
            return yaml.safe_load(f)

    def test_every_rotating_handler_delays_file_creation(self, shipped):
        for name, handler in shipped['handlers'].items():
            if 'filename' in handler:
                assert handler.get('delay') is True, f'{name} would create an empty file at config time'

    def test_debug_log_lives_in_the_bind_mounted_log_dir(self, shipped):
        """Not /tmp — that was container-local and root-owned."""
        assert shipped['handlers']['debug_file_handler']['filename'].endswith(
            '.local/share/mmr/logs/debug.log'
        )

    def test_shipped_config_is_loadable(self, shipped):
        _stamp_log_filenames(shipped)
        assert _drop_unwritable_handlers(shipped) == []
