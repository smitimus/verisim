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
import shutil
import sys

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
    """Each strip script keeps its routes, drops the others', and imports."""
    check_strip = _load("check_strip_scripts")
    assert check_strip.main() == 0


def test_api_schema_agreement_holds():
    """No route queries a table that no schema.sql creates."""
    check = _load("check_api_schema_agreement")
    assert check.main() == 0
