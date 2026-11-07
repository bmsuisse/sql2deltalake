"""The release workflow, checked as text (no database, no YAML parser): supported runners,
publishing only after the tests and the version check, and the version check itself."""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

WORKFLOWS = Path(__file__).parent.parent / ".github" / "workflows"


def _text(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def _jobs(text: str) -> str:
    return text[text.index("\njobs:\n") :]


def _job(text: str, name: str) -> str:
    """The lines of one job of a workflow, up to the next job at the same indentation."""
    text = _jobs(text)
    m = re.search(rf"^  {name}:\n(.*?)(?=^  \S|\Z)", text, re.M | re.S)
    assert m, f"no job {name}"
    return m.group(1)


def test_no_retired_macos_runners():
    for name in ("workflow.yml", "python-test.yml"):
        text = _text(name)
        assert not re.search(r"macos-1[34]\b", text), name
    release = _text("workflow.yml")
    assert "runner: macos-15," in release and "runner: macos-15-intel," in release


def test_publish_needs_the_tests_and_the_version_check():
    release = _text("workflow.yml")
    assert "uses: ./.github/workflows/python-test.yml" in _job(release, "tests")
    deploy = _job(release, "deploy")
    needs = re.search(r"needs: \[(.*)\]", deploy)
    assert needs and {"tests", "version", "wheels", "sdist", "smoke"} <= {
        n.strip() for n in needs.group(1).split(",")
    }
    assert "id-token: write" in deploy and "name: pypi" in deploy
    assert release.count("id-token: write") == 1
    assert re.search(r"^on:\n  release:\n    types: \[published\]", release, re.M)
    assert "workflow_call:" in _text("python-test.yml")


def test_test_workflow_is_pinned_and_read_only():
    text = _text("python-test.yml")
    assert re.search(r"^permissions:\n  contents: read", text, re.M)
    assert "curl" not in text
    for uses in re.findall(r"uses: (\S+)", text):
        assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", uses), uses
    assert text.count("toolchain: stable") == text.count("dtolnay/rust-toolchain@")
    for name in ("workflow.yml", "python-test.yml"):
        for job in re.findall(r"^  (\w[\w-]*):\n", _jobs(_text(name)), re.M):
            body = _job(_text(name), job)
            assert "timeout-minutes:" in body or "uses: ./" in body, (name, job)


def _version_check_script() -> str:
    body = _job(_text("workflow.yml"), "version")
    m = re.search(r"python - <<'PY'\n(.*?)\n\s*PY\n", body, re.S)
    assert m
    return "\n".join(line[10:] for line in m.group(1).splitlines())


@pytest.mark.parametrize(
    "py_version, cargo_version, tag, prerelease, ok",
    [
        ("0.1.0rc1", "0.1.0-rc.1", "v0.1.0rc1", "true", True),
        ("0.1.0rc1", "0.1.0-rc.1", "v0.1.0rc1", "false", False),
        ("0.1.0rc1", "0.1.0-rc.1", "v0.1.0", "true", False),
        ("0.1.0", "0.1.0", "v0.1.0", "false", True),
        ("0.1.0", "0.1.0", "0.1.0", "true", False),
        ("0.14.3", "0.15.0", "v0.14.3", "false", False),
        ("0.2.0b2", "0.2.0-beta.2", "v0.2.0b2", "true", True),
    ],
)
def test_version_check(tmp_path, py_version, cargo_version, tag, prerelease, ok):
    (tmp_path / "pyproject.toml").write_text(f'[project]\nversion = "{py_version}"\n')
    (tmp_path / "Cargo.toml").write_text(
        f'[workspace.package]\nversion = "{cargo_version}"\n'
    )
    script = tmp_path / "check.py"
    script.write_text(_version_check_script())
    out = tmp_path / "out"
    env = {**os.environ, "TAG": tag, "PRERELEASE": prerelease, "GITHUB_OUTPUT": str(out)}
    if sys.version_info < (3, 11):
        pytest.skip("the check uses tomllib")
    r = subprocess.run(
        [sys.executable, "-I", str(script)], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert (r.returncode == 0) == ok, r.stderr
    if ok:
        assert out.read_text() == f"version={py_version}\n"

