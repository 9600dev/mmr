"""Exercise credential hooks with synthetic data and local-only Git remotes."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC_CONTENT = "SYNTHETIC_VALUE=not-a-real-credential-for-hook-tests\n"


class LocalRepo:
    def __init__(self, path: Path, remote: Path, env: dict[str, str]):
        self.path = path
        self.remote = remote
        self.env = env

    def run(self, *args: str, input: str | None = None, check: bool = True,
            cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            args, cwd=cwd or self.path, env=self.env, input=input,
            capture_output=True, text=True, timeout=30, check=False,
        )
        if check:
            assert result.returncode == 0, result.stdout + result.stderr
        return result

    def git(self, *args: str, **kwargs) -> subprocess.CompletedProcess[str]:
        return self.run("git", *args, **kwargs)

    def write(self, name: str, contents: str = SYNTHETIC_CONTENT) -> Path:
        path = self.path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def commit(self, message: str = "synthetic fixture") -> str:
        self.git("commit", "--quiet", "-m", message)
        return self.git("rev-parse", "HEAD").stdout.strip()

    def install(self, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.run(sys.executable, "scripts/install_git_hooks.py", check=check)

    def guard(self, mode: str, *, input: str | None = None):
        args = [sys.executable, "scripts/env_guard.py", mode]
        if mode == "pre-push":
            args.extend(["origin", str(self.remote)])
        return self.run(*args, input=input, check=False)

    def remote_ref(self, ref: str) -> str | None:
        result = self.git("--git-dir", str(self.remote), "rev-parse", "--verify", ref,
                          check=False)
        return result.stdout.strip() if result.returncode == 0 else None


@pytest.fixture
def repo(tmp_path: Path) -> LocalRepo:
    # Prevent ambient Git configuration, hook skips or repository selectors from
    # affecting the fixture. The only permitted transport is a local file path.
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("GIT_", "PRE_COMMIT_")) and key != "SKIP"
    }
    env.update({
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "file",
        "PRE_COMMIT_HOME": str(tmp_path / "hook-cache"),
        "PATH": str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", ""),
    })
    path, remote = tmp_path / "work", tmp_path / "remote.git"
    path.mkdir()
    fixture = LocalRepo(path, remote, env)
    fixture.git("init", "--quiet", "--initial-branch=main")
    fixture.git("config", "user.name", "Hook Test")
    fixture.git("config", "user.email", "hook-test@example.invalid")
    fixture.git("config", "commit.gpgsign", "false")
    fixture.git("config", "tag.gpgsign", "false")
    fixture.git("init", "--bare", "--quiet", "--initial-branch=main", str(remote))
    fixture.git("remote", "add", "origin", str(remote))
    for name in ("env_guard.py", "install_git_hooks.py"):
        destination = path / "scripts" / name
        destination.parent.mkdir(exist_ok=True)
        shutil.copyfile(ROOT / "scripts" / name, destination)

    # Exercise the actual repository entry and stage, without running unrelated
    # type/invariant gates or fetching any third-party pre-commit environment.
    config = yaml.safe_load((ROOT / ".pre-commit-config.yaml").read_text())
    guard_hooks = [hook for group in config["repos"] if group["repo"] == "local"
                   for hook in group["hooks"] if hook["id"] == "env-credentials-guard"]
    assert len(guard_hooks) == 1
    fixture.write("scripts/message_guard.py", """import pathlib
import sys
message = pathlib.Path(sys.argv[1]).read_text()
raise SystemExit('reject-fixture-message' in message)
""")
    fixture.write(".pre-commit-config.yaml", yaml.safe_dump({"repos": [{
        "repo": "local",
        "hooks": [guard_hooks[0], {
            "id": "fixture-message-check", "name": "fixture message check",
            "entry": "python3 scripts/message_guard.py", "language": "system",
            "stages": ["commit-msg"], "always_run": True,
        }],
    }]}))
    fixture.write(".gitignore", ".env\n.env.*\n!.env.example\n")
    fixture.write("README.md", "Synthetic repository for local hook tests.\n")
    fixture.git("add", ".")
    fixture.commit("initial safe fixture")
    return fixture


def assert_blocked(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode != 0
    assert "credential" in (result.stdout + result.stderr).lower()
    assert SYNTHETIC_CONTENT.strip() not in result.stdout + result.stderr


def hidden_history(repo: LocalRepo, cleanup: str = "delete") -> str:
    """Create an unsafe history before hooks exist, then return to clean main."""
    repo.git("checkout", "--quiet", "-b", "legacy")
    repo.write("nested/.env.production")
    repo.git("add", "--force", "--", "nested/.env.production")
    repo.commit("synthetic historical credential pathname")
    if cleanup == "delete":
        repo.git("rm", "--quiet", "--", "nested/.env.production")
    else:
        repo.git("mv", "--", "nested/.env.production", "nested/settings.txt")
    tip = repo.commit("remove credential pathname from tip")
    repo.git("checkout", "--quiet", "main")
    return tip


@pytest.mark.parametrize("name", [
    ".env", ".env.production", "nested/.env.local", "nested/.env.example.backup",
    "with spaces/.env", "line\nbreak/.env.test\tcopy", ".env.directory/plain.txt",
    "--option/.env.local",
])
def test_staged_guard_rejects_forced_dotenv_paths_without_printing_values(repo, name):
    repo.write(name)
    repo.git("add", "--force", "--", name)
    assert_blocked(repo.guard("staged"))


def test_installed_hooks_allow_examples_and_preserve_commit_message_stage(repo):
    repo.install()
    first_push_hook = (repo.path / ".git/hooks/pre-push").read_bytes()
    repo.install()
    assert (repo.path / ".git/hooks/pre-push").read_bytes() == first_push_hook
    assert all(os.access(repo.path / ".git/hooks" / name, os.X_OK)
               for name in ("pre-commit", "commit-msg", "pre-push"))
    for name in (".env.example", "nested/.env.example", "templates/.env.example/readme.txt",
                 ".environment", "service.env"):
        repo.write(name, "PLACEHOLDER=replace-locally\n")
        repo.git("add", "--", name)
    before = repo.git("rev-parse", "HEAD").stdout
    rejected = repo.git("commit", "--quiet", "-m", "reject-fixture-message", check=False)
    assert rejected.returncode != 0
    assert "fixture message check" in rejected.stdout + rejected.stderr
    assert repo.git("rev-parse", "HEAD").stdout == before
    tip = repo.commit("safe example files")
    repo.git("push", "origin", "main")
    assert repo.remote_ref("refs/heads/main") == tip


def test_installed_commit_hook_rejects_credentials_and_keeps_local_file(repo):
    repo.install()
    local = repo.write(".env")
    repo.git("add", "--force", "--", ".env")
    before = repo.git("rev-parse", "HEAD").stdout
    assert_blocked(repo.git("commit", "--quiet", "-m", "must be refused", check=False))
    assert repo.git("rev-parse", "HEAD").stdout == before
    assert local.read_text() == SYNTHETIC_CONTENT


def test_staged_removal_is_allowed_but_does_not_clean_push_history(repo):
    repo.write(".env")
    repo.git("add", "--force", "--", ".env")
    repo.commit("historical synthetic credential pathname")
    repo.install()
    repo.git("rm", "--cached", "--", ".env")
    repo.commit("stop tracking credential file")
    assert_blocked(repo.git("push", "origin", "main", check=False))
    assert repo.remote_ref("refs/heads/main") is None


@pytest.mark.parametrize("cleanup", ["delete", "rename"])
def test_push_checks_non_head_branch_history_after_path_disappears(repo, cleanup):
    tip = hidden_history(repo, cleanup)
    assert ".env" not in repo.git("ls-tree", "-r", "--name-only", tip).stdout
    assert repo.git("branch", "--show-current").stdout.strip() == "main"
    repo.install()
    assert_blocked(repo.git("push", "origin", "legacy", check=False))
    assert repo.remote_ref("refs/heads/legacy") is None


def test_every_push_ref_is_checked_when_first_ref_is_clean(repo):
    bad = hidden_history(repo)
    good = repo.git("rev-parse", "main").stdout.strip()
    repo.git("branch", "a-clean", good)
    repo.git("branch", "z-unsafe", bad)
    zero = "0" * len(good)
    refs = (f"refs/heads/a-clean {good} refs/heads/a-clean {zero}\n"
            f"refs/heads/z-unsafe {bad} refs/heads/z-unsafe {zero}\n")
    assert_blocked(repo.guard("pre-push", input=refs))
    repo.install()
    assert_blocked(repo.git("push", "origin", "a-clean:a-clean", "z-unsafe:z-unsafe",
                            check=False))
    assert repo.remote_ref("refs/heads/a-clean") is None
    assert repo.remote_ref("refs/heads/z-unsafe") is None


@pytest.mark.parametrize("unsafe", [False, True], ids=["clean", "hidden-credential"])
def test_annotated_commit_tags_are_checked_through_their_history(repo, unsafe):
    tip = hidden_history(repo) if unsafe else repo.git("rev-parse", "HEAD").stdout.strip()
    repo.git("tag", "--annotate", "release", "--message", "synthetic release", tip)
    repo.install()
    pushed = repo.git("push", "origin", "refs/tags/release", check=False)
    if unsafe:
        assert_blocked(pushed)
        assert repo.remote_ref("refs/tags/release") is None
    else:
        assert pushed.returncode == 0, pushed.stdout + pushed.stderr
        assert repo.remote_ref("refs/tags/release") == repo.git(
            "rev-parse", "refs/tags/release").stdout.strip()


def test_remote_ref_deletion_is_allowed_even_for_unsafe_old_history(repo):
    hidden_history(repo)
    repo.git("push", "origin", "legacy")
    assert repo.remote_ref("refs/heads/legacy") is not None
    repo.install()
    repo.git("push", "origin", "--delete", "legacy")
    assert repo.remote_ref("refs/heads/legacy") is None


@pytest.mark.parametrize("update", [
    "not a ref update\n",
    "refs/heads/main invalid refs/heads/main " + "0" * 40 + "\n",
    "refs/heads/main " + "f" * 40 + " refs/heads/main " + "0" * 40 + "\n",
])
def test_invalid_push_metadata_fails_closed(repo, update):
    assert_blocked(repo.guard("pre-push", input=update))


def test_noncommit_tag_push_fails_closed(repo):
    blob = repo.git("hash-object", "-w", "--stdin", input=SYNTHETIC_CONTENT).stdout.strip()
    repo.git("update-ref", "refs/tags/blob", blob)
    repo.install()
    assert_blocked(repo.git("push", "origin", "refs/tags/blob", check=False))
    assert repo.remote_ref("refs/tags/blob") is None


def test_shallow_history_is_not_treated_as_verified(repo, tmp_path):
    clone = tmp_path / "shallow"
    repo.git("clone", "--quiet", "--depth=1", repo.path.as_uri(), str(clone))
    shallow = LocalRepo(clone, repo.remote, repo.env)
    assert shallow.git("rev-parse", "--is-shallow-repository").stdout.strip() == "true"
    shallow.git("remote", "set-url", "origin", str(repo.remote))
    shallow.install()
    rejected = shallow.git("push", "origin", "main", check=False)
    assert_blocked(rejected)
    assert "unshallow" in rejected.stdout + rejected.stderr
    assert repo.remote_ref("refs/heads/main") is None


def test_installer_preserves_unrelated_pre_push_hook(repo):
    existing = repo.path / ".git/hooks/pre-push"
    contents = b"#!/bin/sh\n# Unrelated existing local policy.\nexit 0\n"
    existing.write_bytes(contents)
    existing.chmod(0o751)
    result = repo.install(check=False)
    assert result.returncode != 0
    assert "unrelated pre-push" in result.stderr
    assert existing.read_bytes() == contents
    assert stat.S_IMODE(existing.stat().st_mode) == 0o751
    assert not (repo.path / ".git/hooks/pre-commit").exists()
    assert not (repo.path / ".git/hooks/commit-msg").exists()


def test_installer_refuses_custom_hooks_path_without_overwriting_it(repo):
    custom = repo.path / "custom-hooks"
    custom.mkdir()
    existing = custom / "pre-push"
    contents = b"#!/bin/sh\nexit 0\n"
    existing.write_bytes(contents)
    repo.git("config", "core.hooksPath", str(custom))
    result = repo.install(check=False)
    assert result.returncode != 0
    assert "core.hooksPath" in result.stderr
    assert existing.read_bytes() == contents
    assert not (repo.path / ".git/hooks/pre-push").exists()
