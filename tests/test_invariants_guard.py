"""The spec-protection guard must actually refuse the things it claims to.

The guard had no tests, which is the failure this repo keeps rediscovering: a
control that has never been observed refusing anything is indistinguishable
from one that cannot. It has two jobs.

1. Spec and implementation may not move in one commit, so weakening a property
   cannot hide inside an implementation diff.
2. A commit that REMOVES spec lines is a REVISION, and must carry the
   four-point protocol from tests/invariants/README.md in its message.

Job 2 exists because job 1 is only mechanical. It stops an agent editing both
in one commit; it does not stop an agent talking a human into a bad revision
across two. The protocol makes the human's part reviewable in seconds by
forcing the agent to state, in one sentence, what is now allowed that was not.

These tests drive the guard against real git repositories rather than mocks,
because what the guard reads is git's own staged diff, and a mock of that would
be a mock of the thing under test.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
GUARD = REPO / 'scripts' / 'invariants_guard.py'


@pytest.fixture
def repo(tmp_path):
    """A throwaway git repo shaped like MMR, with one spec file committed.

    The guard runs as a SUBPROCESS with cwd set here, never imported into this
    interpreter. That is how pre-commit invokes it, and it keeps the fake
    ``trader/`` directory below out of this process's import path: with the
    working directory changed, ``import trader.anything`` would resolve to the
    stub as a namespace package and poison every later test in the session.
    """
    def git(*args):
        return subprocess.run(['git', *args], cwd=tmp_path, capture_output=True,
                              text=True, check=True)

    git('init', '-q')
    git('config', 'user.email', 't@example.com')
    git('config', 'user.name', 'test')
    spec_dir = tmp_path / 'tests' / 'invariants'
    spec_dir.mkdir(parents=True)
    (spec_dir / 'test_thing.py').write_text(
        '# a rationale comment\n'
        'def test_a_property():\n'
        '    assert 1 + 1 == 2\n'
        '    assert 2 + 2 == 4\n'
    )
    impl_dir = tmp_path / 'trader' / 'trading'
    impl_dir.mkdir(parents=True)
    (impl_dir / 'thing.py').write_text('VALUE = 1\n')
    git('add', '-A')
    git('commit', '-qm', 'initial')
    return tmp_path, git


def run_guard(root: pathlib.Path, message: str | None = None,
              **env_overrides) -> int:
    """Run the guard the way git does.

    No message => the pre-commit stage, which checks staged files only. A
    message => the commit-msg stage, where git hands the hook the real message
    file as argv[1]. That split is not cosmetic: git writes COMMIT_EDITMSG
    AFTER pre-commit hooks run, so a message check at pre-commit reads a stale
    message from the previous commit. The guard did exactly that on its first
    real use and rejected a compliant one.
    """
    env = {k: v for k, v in os.environ.items()
           if k not in ('ALLOW_INVARIANTS_IMPL', 'ALLOW_SPEC_REVISION')}
    env.update(env_overrides)
    argv = [sys.executable, str(GUARD)]
    if message is not None:
        msg_file = root / '.git' / 'COMMIT_EDITMSG'
        msg_file.write_text(message)
        argv.append(str(msg_file))
    return subprocess.run(argv, cwd=root, capture_output=True, text=True,
                          env=env).returncode


def _set_message(root: pathlib.Path, text: str) -> None:
    (root / '.git' / 'COMMIT_EDITMSG').write_text(text)


def _spec(root: pathlib.Path) -> pathlib.Path:
    return root / 'tests' / 'invariants' / 'test_thing.py'


class TestSpecAndImplementationCannotMoveTogether:
    def test_both_staged_is_refused(self, repo):
        root, git = repo
        _spec(root).write_text(_spec(root).read_text() + '\n# touched\n')
        (root / 'trader' / 'trading' / 'thing.py').write_text('VALUE = 2\n')
        git('add', '-A')
        _set_message(root, 'change both')
        assert run_guard(root) == 1

    def test_implementation_alone_is_fine(self, repo):
        root, git = repo
        (root / 'trader' / 'trading' / 'thing.py').write_text('VALUE = 2\n')
        git('add', '-A')
        _set_message(root, 'impl only')
        assert run_guard(root) == 0

    def test_deleting_a_spec_file_is_seen(self, repo):
        """The most direct way to weaken the spec is to remove it. A name-only
        filter that omits D made that the one move the guard could not see."""
        root, git = repo
        git('rm', '-q', 'tests/invariants/test_thing.py')
        (root / 'trader' / 'trading' / 'thing.py').write_text('VALUE = 2\n')
        git('add', '-A')
        _set_message(root, 'remove the spec and change the code')
        assert run_guard(root) == 1


class TestRemovingAPropertyRequiresEvidence:
    """Job 2. A revision must argue for itself, in the commit message, where
    the argument survives beside the change it justifies."""

    def _weaken(self, root, git):
        _spec(root).write_text(
            '# a rationale comment\n'
            'def test_a_property():\n'
            '    assert 1 + 1 == 2\n'          # second assertion deleted
        )
        git('add', '-A')

    _FULL = ('spec: revise the thing\n\n'
             'Counterexample: held=1, qty=2\n'
             'Falsification: run scripts/x.py; it prints False\n'
             'Still caught: everything the old form caught\n'
             'Newly allowed: one ULP of rounding, nothing material\n')

    def test_a_removal_without_the_protocol_is_refused(self, repo):
        root, git = repo
        self._weaken(root, git)
        assert run_guard(root, message='made it pass') == 1

    def test_a_removal_with_all_four_points_is_allowed(self, repo):
        root, git = repo
        self._weaken(root, git)
        assert run_guard(root, message=self._FULL) == 0

    @pytest.mark.parametrize('missing', [
        'Counterexample:', 'Falsification:', 'Still caught:', 'Newly allowed:'])
    def test_every_point_is_load_bearing(self, repo, missing):
        """Three out of four is not the protocol. Each point is checked, so a
        revision cannot pass by supplying the easy ones."""
        root, git = repo
        self._weaken(root, git)
        assert run_guard(root, message=self._FULL.replace(missing, 'Removed:')) == 1

    def test_adding_properties_is_never_blocked(self, repo):
        """Additions must stay frictionless. A guard that taxes adding a
        property discourages exactly the behaviour it exists to encourage."""
        root, git = repo
        _spec(root).write_text(
            _spec(root).read_text()
            + '\n\ndef test_another_property():\n    assert True\n')
        git('add', '-A')
        assert run_guard(root, message='spec: add a property') == 0

    def test_rewording_a_comment_is_not_a_revision(self, repo):
        """Rationale prose is not a property. Counting comment edits as
        revisions would train people to paste the protocol without meaning
        it."""
        root, git = repo
        _spec(root).write_text(
            '# a clearer rationale comment\n'
            'def test_a_property():\n'
            '    assert 1 + 1 == 2\n'
            '    assert 2 + 2 == 4\n'
        )
        git('add', '-A')
        assert run_guard(root, message='docs: reword a comment') == 0

    def test_narrowing_a_generator_counts_as_a_revision(self, repo):
        """Weakening is not only deleting an assert. Shrinking the input range
        a property runs over removes the region where it would have failed."""
        root, git = repo
        _spec(root).write_text(
            'from hypothesis import given, strategies as st\n\n'
            '@given(x=st.integers(min_value=-100, max_value=100))\n'
            'def test_a_property(x):\n'
            '    assert isinstance(x, int)\n'
        )
        git('add', '-A')
        git('commit', '-qm', 'spec: property with a wide generator')
        _spec(root).write_text(
            'from hypothesis import given, strategies as st\n\n'
            '@given(x=st.integers(min_value=0, max_value=1))\n'
            'def test_a_property(x):\n'
            '    assert isinstance(x, int)\n'
        )
        git('add', '-A')
        assert run_guard(root, message='spec: narrow the range') == 1

    def test_the_override_still_works(self, repo):
        """Documented escape hatch. It is review hygiene, not a security
        control, and pretending otherwise would be the dishonest kind of
        gate."""
        root, git = repo
        self._weaken(root, git)
        assert run_guard(root, message='no protocol here',
                         ALLOW_SPEC_REVISION='1') == 0


class TestTheStageSplitIsLoadBearing:
    """The protocol check MUST run at commit-msg, not pre-commit.

    git writes COMMIT_EDITMSG after pre-commit hooks run, so a pre-commit check
    of the message reads whatever the PREVIOUS commit left there. The guard
    shipped that way and rejected the first compliant message it ever saw,
    because it was reading a stale file from an earlier test run. A guard that
    refuses correct input is worse than no guard: it teaches people to reach
    for the override.
    """

    def test_the_pre_commit_stage_ignores_the_message_entirely(self, repo):
        """No argument means pre-commit. It must judge staged files only, and
        must not consult any message file, however tempting one on disk is."""
        root, git = repo
        _spec(root).write_text(
            '# a rationale comment\n'
            'def test_a_property():\n'
            '    assert 1 + 1 == 2\n'
        )
        git('add', '-A')
        # A stale message with no protocol markers sits on disk.
        (root / '.git' / 'COMMIT_EDITMSG').write_text('something unrelated')
        assert run_guard(root) == 0, (
            'the pre-commit stage read a message it cannot legitimately see')

    def test_the_commit_msg_stage_reads_the_file_git_hands_it(self, repo):
        """Not a fixed path. git passes the message file as argv[1], and during
        a merge or a rebase that is not COMMIT_EDITMSG."""
        root, git = repo
        self_weaken = (
            '# a rationale comment\n'
            'def test_a_property():\n'
            '    assert 1 + 1 == 2\n'
        )
        _spec(root).write_text(self_weaken)
        git('add', '-A')
        elsewhere = root / '.git' / 'MERGE_MSG'
        elsewhere.write_text(
            'spec: revise\n\nCounterexample: x\nFalsification: y\n'
            'Still caught: z\nNewly allowed: w\n')
        (root / '.git' / 'COMMIT_EDITMSG').write_text('no markers here at all')
        env = {k: v for k, v in os.environ.items()
               if k not in ('ALLOW_INVARIANTS_IMPL', 'ALLOW_SPEC_REVISION')}
        rc = subprocess.run(
            [sys.executable, str(GUARD), str(elsewhere)],
            cwd=root, capture_output=True, text=True, env=env).returncode
        assert rc == 0, 'the guard ignored the message file git gave it'
