"""Tests for the build/config consistency checkers in tools/.

The checkers exist to catch failures that are silent by construction — a build
script that exits 0 while writing the wrong file, a config that has quietly forked
into two copies. A checker nobody has watched fail is a checker that does not
work, so each one gets a case that must fail and a case that must pass.

The config cases copy the repo into tmp_path first: the checkers read a checkout
from disk, so a fixture that mutated the repo would be testing the wrong thing
(and would leave a stale config.yaml behind for the next run).

Run:  python -m pytest gas-station/generator/tests/test_build_checks.py -v
"""
import importlib.util
import pathlib
import re
import shutil
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parents[3]
TOOLS = REPO / "tools"

# The checkers read: each industry's generator + config, plus base/api/main.py.
INDUSTRIES = ("grocery", "gas-station", "support")


def _load(name):
    """Import a tools/ module by path (tools/ is not a package)."""
    sys.path.insert(0, str(TOOLS))
    try:
        spec = importlib.util.spec_from_file_location(f"_t_{name}", TOOLS / f"{name}.py")
        assert spec is not None and spec.loader is not None, f"cannot load tools/{name}.py"
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(TOOLS))


def _mini_tree(tmp_path: pathlib.Path, name: str) -> pathlib.Path:
    """A checkout copy holding everything check_configs.py reads."""
    tree = tmp_path / name
    shutil.copytree(REPO, tree, ignore=shutil.ignore_patterns(
        "__pycache__", "*.pyc", ".git",
    ))
    assert all((tree / i / "generator" / "schema.sql").exists() for i in INDUSTRIES)
    return tree


# ── check_configs ────────────────────────────────────────────────────────────

def test_config_check_passes_on_the_repo():
    """The real tree has exactly one config per industry and they all load."""
    check_configs = _load("check_configs")
    assert check_configs.run(REPO) == 0, "check_configs failed on a clean checkout"


def test_config_check_fails_when_a_second_config_appears(tmp_path):
    """RED: re-introduce the orphan copy and the gate must fail.

    This is the regression that motivated the check — grocery's
    standalone/config.yaml drifted 117 lines behind config.yaml while the
    published image kept shipping it, and nothing noticed.
    """
    tree = _mini_tree(tmp_path, "repo")
    check_configs = _load("check_configs")

    # The clean tree must pass, or the failing assertion below proves nothing.
    assert check_configs.run(tree) == 0

    stale = tree / "grocery" / "standalone" / "config.yaml"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text((tree / "grocery" / "config.yaml").read_text())

    assert check_configs.run(tree) > 0, "a second config.yaml must fail the check"


def test_config_check_fails_when_the_shipped_copy_is_stale(tmp_path):
    """RED: the exact historical failure — a *stale* second copy, not a clone.

    A byte-identical copy is benign only by accident. The real regression is a
    copy that has fallen behind, so this plants one missing keys the
    authoritative config has.
    """
    tree = _mini_tree(tmp_path, "repo_stale")
    check_configs = _load("check_configs")
    assert check_configs.run(tree) == 0

    stale = tree / "grocery" / "standalone" / "config.yaml"
    stale.parent.mkdir(parents=True, exist_ok=True)
    authoritative = (tree / "grocery" / "config.yaml").read_text()
    # Chop the tail: lose the pricing/inventory/weather blocks.
    stale.write_text("\n".join(authoritative.splitlines()[:120]) + "\n")

    assert check_configs.run(tree) > 0, "a stale second config.yaml must fail the check"


def test_config_check_fails_on_an_unknown_config_key(tmp_path):
    """A config the generator will reject must fail here too.

    The generator rejects unknown keys (t_6081478a), so a typo'd config kills the
    generator on its first tick. Catching it here means the failure names the file
    before an image is built.
    """
    tree = _mini_tree(tmp_path, "repo_typo")
    check_configs = _load("check_configs")

    cfg_path = tree / "gas-station" / "config.yaml"
    cfg_path.write_text(cfg_path.read_text() + "\nvolumes:\n  not_a_real_key: 3\n")

    assert check_configs.run(tree) > 0, "an unknown config key must fail the check"


def test_config_check_fails_when_a_config_is_missing(tmp_path):
    """An industry with no config at all is a failure, not a skip."""
    tree = _mini_tree(tmp_path, "repo_missing")
    check_configs = _load("check_configs")
    (tree / "gas-station" / "config.yaml").unlink()
    assert check_configs.run(tree) > 0, "a missing config.yaml must fail the check"


# ── the other checkers, on the real tree ─────────────────────────────────────

def test_strip_scripts_keep_only_their_own_industry():
    """Each strip script keeps its routes, drops the others', and parses.

    The import half of the check needs fastapi, which the generator test job
    does not install — so the checker skips that half when the dependencies are
    absent, and this asserts the parts that always run: that the scripts exist,
    that their output parses, and that the kept route set is exactly right. A
    wrong route set fails here regardless of what is installed.
    """
    check_strip = _load("check_strip_scripts")
    assert check_strip.main() == 0


def test_api_schema_agreement_holds():
    """No route queries a table that no schema.sql creates, and no shared route
    projects a column an industry's schema.sql does not declare."""
    check = _load("check_api_schema_agreement")
    assert check.main() == 0


def test_api_schema_agreement_audits_the_shared_routes_it_was_written_for():
    """The column half must actually examine the routes of t_04e1e8be.

    That defect was invisible to the table half: `inv.receipts` and `inv.products`
    exist in every `schema.sql`, but `inv.receipts.load_id` and
    `inv.products.supplier_id` exist only in grocery's — so the two shared routes
    500'd `42703` on a gas-station data dir while the union table check passed.
    The audit skips a route whose SQL branches per industry or gates on a relation
    probe, and a skip rule that widened to cover these two would quietly restore
    exactly that blindness. So the routes are named, and gas-station must be one
    of the industries they were audited against.
    """
    check = _load("check_api_schema_agreement")
    src = (REPO / "base" / "api" / "main.py").read_text()
    schemas = {name: check.columns_in_schema(path)
               for name, path in check.industries(REPO).items()}
    findings, checked, skipped = check.column_findings(src, schemas)

    skipped_routes = {route for route, _ in skipped}
    for route in ("/{industry}/inventory/receipts", "/{industry}/inventory/products"):
        assert route not in skipped_routes, (
            f"{route} is no longer audited for columns ({dict(skipped)[route]}), so "
            "an unguarded column there would pass again (t_04e1e8be)")
    assert checked["gas-station"] > 0, (
        "no shared route body was audited against the gas-station schema")
    assert not findings, (
        "a shared route projects a column the gas-station schema lacks: "
        f"{findings}")


def test_shared_route_column_guard_has_teeth(tmp_path, capsys):
    """RED: drop the guard and the check must name the column and the industry.

    `inv.products.supplier_id` and `inv.receipts.load_id` are grocery's alone
    (t_04e1e8be), so projecting either one unconditionally is a parse-time
    `42703` — HTTP 500 — on every gas-station data dir. Both projections are
    guarded with `_has_column(industry, ...)` now; this removes each guard in a
    copy of the repo and requires the checker to fail *and* to say what broke.
    """
    check = _load("check_api_schema_agreement")
    tree = _mini_tree(tmp_path, "repo_unguarded_column")
    api = tree / "base" / "api" / "main.py"

    # The clean copy must pass first, or the failing assertions below prove
    # nothing about the mutation.
    assert check.main(tree) == 0, "the unmutated copy must pass"
    capsys.readouterr()

    source = api.read_text()
    unguarded = re.sub(
        r'load_id\s*=\s*\(\s*"r\.load_id, "\s*if\s*_has_column\([^)]*\)\s*else\s*""\s*\)',
        'load_id = "r.load_id, "', source, flags=re.S)
    assert unguarded != source, (
        "the receipts route no longer guards `r.load_id` with `_has_column`; if the "
        "guard moved, move this mutation with it (t_04e1e8be)")
    api.write_text(unguarded)
    assert check.main(tree) == 1, "an unguarded `r.load_id` must fail the check"
    out = capsys.readouterr().out
    assert "r.load_id" in out and "gas-station" in out, out

    unguarded_vendor = re.sub(
        r'supplier_id\s*=\s*\(\s*"ip\.supplier_id, "\s*if\s*_has_column\([^)]*\)\s*else\s*""\s*\)',
        'supplier_id = "ip.supplier_id, "', unguarded, flags=re.S)
    assert unguarded_vendor != unguarded, (
        "the products route no longer guards `ip.supplier_id` with `_has_column`; "
        "if the guard moved, move this mutation with it (t_04e1e8be)")
    api.write_text(unguarded_vendor)
    assert check.main(tree) == 1, (
        "an unguarded `ip.supplier_id` must fail the check — that was the other "
        "half of t_04e1e8be")
    out = capsys.readouterr().out
    assert "ip.supplier_id" in out and "gas-station" in out, out


def test_switch_status_sees_every_container_it_creates():
    """`switch.sh status` must not report 'none' for a container it can start.

    The gas-station branch used to look for `^verisim-gas-station$`, a name no
    mode creates, so a running dev stack printed "none" (t_a6ecb731).
    """
    # A shell script, so run it rather than importing it — same check, no copy.
    result = subprocess.run(
        ["bash", str(TOOLS / "check_switch_status.sh"), str(REPO)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_workflows_gate_publish_on_both_test_jobs():
    """Every workflow's publish job waits for test + integration, and keeps the
    credential guard ungated (the t_44f5663e regression)."""
    check = _load("check_workflows")
    assert check.main() == 0


# ── the standalone-image job's PR gate and API address (t_19eb986e) ─────────

_CLEAN_WORKFLOW = """\
name: synthetic
on:
  pull_request:
jobs:
  test:
    steps:
      - run: "true"
  integration:
    if: github.event_name != 'pull_request'
    steps:
      - name: Build image
        uses: docker/build-push-action@v5
        with:
          load: true
      - name: Smoke test
        run: |
          CANDIDATE_URLS="http://127.0.0.1:8000"
          echo "VERISIM_API_BASE_URL=${CANDIDATE_URLS}" >> "$GITHUB_ENV"
  publish:
    needs: [test, integration]
    steps:
      - name: Require publish credentials
        run: |
          if [ -z "${DOCKER_USERNAME}" ]; then exit 1; fi
"""

# Everything the three new checks are supposed to catch, in one file: the image
# job runs on PRs, the smoke step curls a loopback that is the job container on
# the act runner, the address is not exported for later steps, and the build
# round-trips a ~1 GB tar through the job container's writable layer.
_REGRESSED_WORKFLOW = """\
name: synthetic
on:
  pull_request:
jobs:
  test:
    steps:
      - run: "true"
  integration:
    steps:
      - name: Build image
        uses: docker/build-push-action@v5
        with:
          outputs: type=docker,dest=/tmp/image.tar
      - name: Load image
        run: docker load --input /tmp/image.tar
      - name: Smoke test
        run: |
          curl -s http://localhost:8010/health
  publish:
    needs: [test, integration]
    steps:
      - name: Require publish credentials
        run: |
          if [ -z "${DOCKER_USERNAME}" ]; then exit 1; fi
"""


def test_workflows_checker_catches_the_pr_build_and_localhost_regressions(tmp_path, capsys):
    """RED: plant the t_19eb986e defects and the checker must fail by name.

    The runner has one 40 G volume shared with a monitoring stack, and the
    blocking `test` job needs it: a ~1 GB image build on every PR push is disk
    churn against the gate, not coverage. And on the self-hosted act runner the
    job runs INSIDE a container with the host's docker socket, so a `-p` publish
    lands on the runner host and `localhost` from the job is the job itself —
    measured "connection refused" every time. Both were live regressions, so
    both get a case that must fail.
    """
    check = _load("check_workflows")
    wf_dir = tmp_path / ".github" / "workflows"
    wf_dir.mkdir(parents=True)
    (wf_dir / "clean.yml").write_text(_CLEAN_WORKFLOW)

    # The clean copy must pass first, or the failing assertion below proves
    # nothing about the mutation.
    assert check.main(tmp_path) == 0, "the unmutated workflow must pass"
    assert "gated off pull requests" in capsys.readouterr().out

    (wf_dir / "regressed.yml").write_text(_REGRESSED_WORKFLOW)
    assert check.main(tmp_path) == 1, "the regressed workflow must fail"
    out = capsys.readouterr().out
    assert "pull requests" in out, out
    assert "localhost" in out, out
    assert "load: true" in out, out


@pytest.mark.parametrize(
    "name", ["check_strip_scripts", "check_api_schema_agreement", "check_workflows"]
)
def test_checkers_do_not_read_sys_argv_at_import(name):
    """A checker must find the repo from its own location, not from argv.

    This is a real regression, caught in CI on the first run of this branch:
    all three bound `CHECKOUT` from `sys.argv[1]`, so under `python -m pytest`
    — where argv belongs to pytest — they resolved paths *inside the tests
    directory*. `check_workflows` then reported "no workflows under
    .../tests/.github/workflows" and exited 2.

    The local runner used during development chdir'd and rewrote argv itself,
    so it reproduced the intended behaviour and never the real one. The check
    is on the source text rather than a rerun, and it looks for argv being
    *used* rather than merely mentioned — these checkers explain the rule in
    their own comments, and a bare substring match would fail on that.
    """
    src = (TOOLS / f"{name}.py").read_text(encoding="utf-8")

    # Strip comments and docstrings, then look for a real attribute access.
    code = []
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        code.append(line.split("  # ", 1)[0])
    body = "\n".join(code)

    assert "argv[" not in body, (
        f"tools/{name}.py indexes argv — under `python -m pytest` that is the "
        f"path pytest was given, not the repo root. Derive CHECKOUT from "
        f"pathlib.Path(__file__).resolve().parents[1] instead."
    )


# ── the publish-tag convention ──────────────────────────────────────────────

def test_published_tags_are_versions_not_commit_shas():
    """A registry tag must be a tracked version, never a bare commit SHA.

    The measured failure (t_4b05829c): both workflows' `Compute tags` step
    assigned `:${GITHUB_SHA}` on a main push, so 18 of the 28 tags on
    smiti/verisim-grocery were 40-character SHAs and the last versioned release
    was v1.3.3 on 2026-09-21. The run was green throughout — a green CI run is
    not a published artifact, and it was not a readable one either.

    The convention now lives in `.github/scripts/compute-push-tags.sh`, keyed to
    the VERSION file at the repo root. This asserts the whole chain: VERSION is a
    version, it agrees with pyproject, both workflows route through the script
    rather than inlining their own, and the script as executed emits the version
    for both a main push and a v* tag push.
    """
    check = _load("check_publish_tags")
    assert check.main() == 0, "check_publish_tags failed — see its output above"


def test_publish_tag_gate_has_teeth(tmp_path):
    """RED: re-introduce the SHA-only tag computation and the gate must fail.

    A gate nobody has watched fail is a gate that does not work. The claim in
    the test above is that `check_publish_tags` catches the exact defect it was
    written for, so this plants that defect in a copy of the repo and requires a
    failure. `main(root)` exists for this: the checker resolves the real checkout
    from its own location (t_a6ecb731), so a mutated copy has to be handed to it
    explicitly.
    """
    check = _load("check_publish_tags")
    tree = _mini_tree(tmp_path, "repo_sha_tags")
    # The clean copy must pass first, or the failing assertion below proves
    # nothing about the mutation.
    assert check.main(tree) == 0, "the unmutated copy must pass"

    workflow = tree / ".github" / "workflows" / "verisim-grocery.yml"
    workflow.write_text(workflow.read_text().replace(
        'bash .github/scripts/compute-push-tags.sh >> "$GITHUB_OUTPUT"',
        'echo "tags=${IMAGE_NAME}:latest,${IMAGE_NAME}:${GITHUB_SHA}" >> "$GITHUB_OUTPUT"',
    ))
    assert check.main(tree) != 0, (
        "an inlined :${GITHUB_SHA} tag computation must fail the gate — that is "
        "the t_4b05829c bug (18 of 28 registry tags were bare SHAs)"
    )


def test_publish_tag_gate_catches_a_missing_version_file(tmp_path):
    """RED: delete VERSION and the gate must fail rather than fall back to a SHA.

    The card's real complaint is that a publish can succeed while tagging the
    image with an opaque identifier. If losing VERSION silently degraded to the
    SHA, the gate would have to be rewritten to catch it — so losing it is a
    failure, asserted here.
    """
    check = _load("check_publish_tags")
    tree = _mini_tree(tmp_path, "repo_no_version")
    assert check.main(tree) == 0

    (tree / "VERSION").unlink()
    assert check.main(tree) != 0, "a missing VERSION must fail the gate"


def test_publish_tag_gate_catches_a_pyproject_drift(tmp_path):
    """Two files naming the same version is still two files — so a drift is loud.

    pyproject's version was 0.1.0 and had never been bumped, which is part of
    why nothing tracked a release number. Bumping it to match VERSION means the
    two must now agree, or the release name is ambiguous.
    """
    check = _load("check_publish_tags")
    tree = _mini_tree(tmp_path, "repo_version_drift")
    assert check.main(tree) == 0

    pyproject = tree / "pyproject.toml"
    pyproject.write_text(pyproject.read_text().replace(
        'version = "1.3.4"', 'version = "0.1.0"', 1))

    assert check.main(tree) != 0, "a VERSION/pyproject drift must fail the gate"
