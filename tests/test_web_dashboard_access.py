"""Web dashboard: where it listens, and the MMR_WEB_TOKEN gate on approve/reject.

Two defects from the 2026-11-14 audit (B5.5):

* the dashboard bound 0.0.0.0 unconditionally. Inside the container that is
  hidden behind compose's 127.0.0.1 port mapping; in hybrid mode it runs on the
  HOST, so the account, the positions and the approve button were on the LAN
  with no authentication;
* with ``MMR_WEB_TOKEN`` set, the approve/reject forms did not carry the token,
  so the one mutating action the dashboard has answered 401 — and the redirect
  back to the page dropped the token too.
"""
import html as html_lib
import re
import sys
import types
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import web.app as webapp
from test_web_dashboard import FakeMMR as _PageFakeMMR

TOKEN = 's3cret/token+with=odd&chars'
ROOT = Path(__file__).resolve().parents[1]


class FakeResult:
    def __init__(self, ok=True, error=None):
        self._ok = ok
        self.error = error

    def is_success(self):
        return self._ok


class FakeMMR(_PageFakeMMR):
    """The page fake from test_web_dashboard, plus one pending proposal to act on."""

    def __init__(self):
        super().__init__()
        self.approved = []
        self.rejected = []

    def proposals(self, status=None, limit=50):
        return pd.DataFrame([{'id': 7, 'created': '11/14 09:30', 'symbol': 'AMD', 'action': 'BUY',
                              'size': '10 sh', 'conf': 0.6, 'status': 'PENDING', 'reasoning': 'thesis'}])

    def approve(self, pid):
        self.approved.append(pid)
        return FakeResult(True)

    def reject(self, pid, reason):
        self.rejected.append((pid, reason))
        return True


@pytest.fixture
def fake(monkeypatch):
    mmr = FakeMMR()
    monkeypatch.setattr(webapp, '_mmr', mmr)
    monkeypatch.setattr(webapp, '_get_mmr', lambda: mmr)
    webapp._render_md_cached.cache_clear()
    return mmr


@pytest.fixture
def gated(fake, monkeypatch):
    monkeypatch.setattr(webapp, '_ACCESS_TOKEN', TOKEN)
    return TestClient(webapp.app), fake


@pytest.fixture
def ungated(fake, monkeypatch):
    monkeypatch.setattr(webapp, '_ACCESS_TOKEN', '')
    return TestClient(webapp.app), fake


def _form_fields(html, action):
    """Hidden fields of the form posting to ``action``, as a browser would send them."""
    form = re.search(r'<form[^>]*action="%s".*?</form>' % re.escape(action), html, re.DOTALL)
    assert form, f'no form posting to {action}'
    fields = {}
    for tag in re.findall(r'<input[^>]*>', form.group(0)):
        name = re.search(r'name="([^"]*)"', tag)
        value = re.search(r'value="([^"]*)"', tag)
        if name and 'type="hidden"' in tag:
            fields[name.group(1)] = html_lib.unescape(value.group(1)) if value else ''
    return fields


# ── bind address ─────────────────────────────────────────────────────────────

def test_default_bind_is_loopback(monkeypatch):
    monkeypatch.delenv('WEB_HOST', raising=False)
    assert webapp.bind_host() == '127.0.0.1'
    monkeypatch.setenv('WEB_HOST', '   ')
    assert webapp.bind_host() == '127.0.0.1'


def test_web_host_is_honoured(monkeypatch):
    monkeypatch.setenv('WEB_HOST', '0.0.0.0')
    assert webapp.bind_host() == '0.0.0.0'


@pytest.mark.parametrize('web_host,expected', [(None, '127.0.0.1'), ('0.0.0.0', '0.0.0.0')])
def test_main_passes_the_bind_host_to_uvicorn(monkeypatch, web_host, expected):
    calls = []
    monkeypatch.setitem(sys.modules, 'uvicorn',
                        types.SimpleNamespace(run=lambda app, **kwargs: calls.append(kwargs)))
    if web_host is None:
        monkeypatch.delenv('WEB_HOST', raising=False)
    else:
        monkeypatch.setenv('WEB_HOST', web_host)
    monkeypatch.setenv('WEB_PORT', '17424')
    webapp.main()
    assert calls == [{'host': expected, 'port': 17424, 'log_level': 'info'}]


def test_start_script_binds_all_interfaces_only_in_the_container():
    """The container needs 0.0.0.0 to be reachable through the port mapping
    (which compose pins to the host's loopback); the host must not get it."""
    script = (ROOT / 'start_mmr.sh').read_text()
    block = re.search(r'if \[ "\$IN_DOCKER" = true \]; then\n\s+WEB_BIND_HOST="([^"]+)"\n'
                      r'else\n\s+WEB_BIND_HOST="([^"]+)"\nfi', script)
    assert block, 'start_mmr.sh no longer chooses the dashboard bind address by IN_DOCKER'
    assert block.group(1) == '${WEB_HOST:-0.0.0.0}'
    assert block.group(2) == '${WEB_HOST:-127.0.0.1}'
    assert 'WEB_HOST="$WEB_BIND_HOST"' in script
    compose = (ROOT / 'docker-compose.yml').read_text()
    assert '"127.0.0.1:7424:7424"' in compose


# ── token gate ───────────────────────────────────────────────────────────────

def test_page_requires_the_token(gated):
    client, _ = gated
    assert client.get('/').status_code == 401
    assert client.get('/', params={'token': 'wrong'}).status_code == 401
    assert client.get('/', params={'token': TOKEN}).status_code == 200
    assert client.get('/', headers={'X-MMR-Token': TOKEN}).status_code == 200


def test_non_ascii_token_is_401_not_500(gated):
    client, _ = gated
    assert client.get('/', params={'token': 'pässwörd'}).status_code == 401


def test_forms_carry_the_token_and_approve_works(gated):
    client, fake = gated
    page = client.get('/', params={'token': TOKEN})
    fields = _form_fields(page.text, '/proposals/7/approve')
    assert fields['token'] == TOKEN and fields['csrf_token']

    # Exactly what the browser posts: the form's own fields, no query string.
    response = client.post('/proposals/7/approve', data=fields, follow_redirects=False)
    assert response.status_code == 303
    assert fake.approved == [7]

    # ...and the redirect lands back on a page that lets us in.
    landed = client.get(response.headers['location'])
    assert landed.status_code == 200
    assert '#7 approved &amp; submitted' in landed.text
    assert _form_fields(landed.text, '/proposals/7/approve')['token'] == TOKEN


def test_reject_form_carries_the_token_too(gated):
    client, fake = gated
    page = client.get('/', headers={'X-MMR-Token': TOKEN})
    fields = _form_fields(page.text, '/proposals/7/reject')
    assert fields['token'] == TOKEN
    response = client.post('/proposals/7/reject', data={**fields, 'reason': 'changed thesis'},
                           follow_redirects=False)
    assert response.status_code == 303
    assert fake.rejected == [(7, 'changed thesis')]
    assert client.get(response.headers['location']).status_code == 200


@pytest.mark.parametrize('action', ['approve', 'reject'])
def test_post_without_the_token_is_still_refused(gated, action):
    client, fake = gated
    page = client.get('/', params={'token': TOKEN})
    fields = _form_fields(page.text, f'/proposals/7/{action}')
    fields.pop('token')
    assert client.post(f'/proposals/7/{action}', data=fields).status_code == 401
    assert client.post(f'/proposals/7/{action}', data={**fields, 'token': 'wrong'}).status_code == 401
    assert fake.approved == [] and fake.rejected == []


def test_token_does_not_replace_the_csrf_check(gated):
    client, fake = gated
    response = client.post('/proposals/7/approve', data={'token': TOKEN, 'csrf_token': 'forged'})
    assert response.status_code == 403
    assert fake.approved == []


def test_ungated_dashboard_is_unchanged(ungated):
    """No MMR_WEB_TOKEN: no token field in the page, none in the redirect."""
    client, fake = ungated
    page = client.get('/')
    assert page.status_code == 200
    fields = _form_fields(page.text, '/proposals/7/approve')
    assert set(fields) == {'csrf_token'}
    assert 'name="token"' not in page.text
    response = client.post('/proposals/7/approve', data=fields, follow_redirects=False)
    assert response.status_code == 303
    assert response.headers['location'].startswith('/?flash=')
    assert 'token=' not in response.headers['location']
    assert fake.approved == [7]
