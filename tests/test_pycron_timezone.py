"""pycron schedules can be pinned to an IANA timezone independent of the host TZ."""
import datetime as dt
from zoneinfo import ZoneInfo

import pytest
import yaml

from pycron.pycron import Job, ZonedCronTab, resolve_timezone

NY = ZoneInfo('America/New_York')


def test_no_timezone_is_legacy_local():
    job = Job({'name': 'j', 'command': 'true', 'start': '0 9 * * 1-5'})
    assert job.timezone is None
    assert job.crontab_start.tz is None


def test_job_timezone_measures_from_zone_now(monkeypatch):
    # 08:00 NY on a Thursday -> 09:00 NY start is exactly one hour away,
    # regardless of what the process's local zone is.
    fixed = dt.datetime(2026, 11, 5, 8, 0, tzinfo=NY)

    class _FakeDT(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)

    import pycron.pycron as mod
    monkeypatch.setattr(mod.dt, 'datetime', _FakeDT)
    job = Job({'name': 'j', 'command': 'true', 'start': '0 9 * * 1-5',
               'timezone': 'America/New_York'})
    assert job.crontab_start.next(default_utc=False) == pytest.approx(3600.0)


def test_same_instant_different_zone_differs():
    instant = dt.datetime(2026, 11, 5, 13, 0, tzinfo=dt.timezone.utc)  # 08:00 NY, 05:00 LA
    ny = ZonedCronTab('0 9 * * 1-5', 'America/New_York')
    la = ZonedCronTab('0 9 * * 1-5', 'America/Los_Angeles')
    assert ny.next(instant.astimezone(NY), default_utc=False) == pytest.approx(3600.0)
    assert la.next(instant.astimezone(ZoneInfo('America/Los_Angeles')),
                   default_utc=False) == pytest.approx(4 * 3600.0)


def test_default_timezone_and_override():
    job = Job({'name': 'j', 'command': 'true', 'start': '0 9 * * 1-5', 'stop': '0 17 * * 1-5'},
              default_timezone='America/New_York')
    assert job.timezone == 'America/New_York'
    assert job.crontab_stop is not None and job.crontab_stop.tz_name == 'America/New_York'
    job2 = Job({'name': 'j', 'command': 'true', 'start': '0 9 * * 1-5',
                'timezone': 'Australia/Sydney'}, default_timezone='America/New_York')
    assert job2.crontab_start.tz_name == 'Australia/Sydney'
    assert job2.__dict__()['timezone'] == 'Australia/Sydney'


def test_unknown_timezone_raises():
    with pytest.raises(ValueError):
        resolve_timezone('Mars/Olympus_Mons')
    with pytest.raises(ValueError):
        Job({'name': 'j', 'command': 'true', 'start': '0 9 * * *', 'timezone': 'Nope/Zone'})


def test_shipped_defaults_declare_new_york():
    with open('config_defaults/pycron.yaml') as fh:
        cfg = yaml.safe_load(fh)
    assert cfg.get('timezone') == 'America/New_York'
    resolve_timezone(cfg['timezone'])
    for j in cfg['jobs']:
        Job(j, default_timezone=cfg['timezone'])  # every job parses in-zone
