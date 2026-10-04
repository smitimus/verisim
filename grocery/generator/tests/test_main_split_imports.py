"""
The split generator modules must import under BOTH entry paths (t_c2eca5dd).

`grocery/generator/main.py` is not only an importable module — it is the
container's entry point (`grocery/generator/Dockerfile` runs
`CMD ["python", "main.py"]`, and the standalone image's supervisord does the
same). That means:

  * as a package  — `grocery.generator.main` (pytest) and flat `main`
                     (a caller that put `grocery/generator` on sys.path)
                     => package-relative imports (`from .volume import ...`) work
  * as a script   — `python main.py`, where `__package__` is empty and every
                     sibling is a top-level module
                     => package-relative imports raise ImportError

So each split module branches on `__package__`. These tests pin that both paths
resolve, because a regression here does not fail the unit suite: it fails in the
container, at startup, with a traceback that names the import rather than the
generator.

The subprocess checks are the honest ones — an in-process import cannot be both
the package and the script in the same interpreter.
"""
import os
import pathlib
import subprocess
import sys
import textwrap

import pytest

GENERATOR = pathlib.Path(__file__).resolve().parents[1]
SPLIT_MODULES = ["bootstrap", "volume", "seed", "tick", "backfill"]


def _run(code, cwd):
    """Run `code` in a fresh interpreter that inherits THIS process's sys.path.

    Inheriting is the point: the generator's real dependencies (psycopg2,
    faker, yaml) are importable here because the test suite itself imported
    them. Stubbing them instead would test my stubs, not the import machinery.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=cwd, capture_output=True, text=True, timeout=120, env=env,
    )


@pytest.mark.parametrize("module", SPLIT_MODULES)
def test_module_imports_as_script(module):
    """`import <module>` with the generator dir on sys.path — the flat name."""
    r = _run(f"""
        import sys
        sys.path.insert(0, {str(GENERATOR)!r})
        import {module}
        print("OK")
    """, GENERATOR)
    assert "OK" in r.stdout, (
        f"`import {module}` failed on the flat path:\n{r.stderr[-1500:]}"
    )


@pytest.mark.parametrize("module", SPLIT_MODULES)
def test_module_imports_as_package(module):
    """`grocery.generator.<module>` — the dotted path pytest and CI use."""
    repo = GENERATOR.parents[1]
    r = _run(f"""
        import sys
        sys.path.insert(0, {str(repo)!r})
        import grocery.generator.{module}
        print("OK")
    """, repo)
    assert "OK" in r.stdout, (
        f"`import grocery.generator.{module}` failed:\n{r.stderr[-1500:]}"
    )


def test_main_runs_as_a_script_the_way_the_container_does():
    """`python main.py` must get past its imports.

    This is the container's actual command. It is expected to fail LATER, at the
    database connection, because there is no Postgres here — what matters is
    that it never fails with ImportError on a sibling module.
    """
    r = _run(f"""
        import runpy, sys
        sys.path.insert(0, {str(GENERATOR)!r})
        try:
            runpy.run_path({str(GENERATOR / "main.py")!r}, run_name="not_main")
            print("OK")
        except ImportError as e:
            print("IMPORT_ERROR:", e)
        except Exception:
            print("OK")
    """, GENERATOR)
    assert "IMPORT_ERROR" not in r.stdout, (
        f"running main.py as a script hit an ImportError:\n{r.stdout[-800:]}\n{r.stderr[-800:]}"
    )


@pytest.mark.parametrize("module", SPLIT_MODULES + ["main"])
def test_split_modules_guard_relative_imports(module):
    """Any package-relative sibling import must sit behind a `__package__` guard.

    Without the guard the module imports fine under pytest and explodes in the
    container, where the same file is loaded as `__main__`.
    """
    src = (GENERATOR / f"{module}.py").read_text()
    has_relative = any(
        ln.strip().startswith(("from .", "import .")) and " import " in ln
        for ln in src.splitlines()
    )
    if not has_relative:
        pytest.skip(f"{module}.py has no package-relative sibling import")
    assert "__package__" in src, (
        f"{module}.py uses package-relative sibling imports with no "
        f"`__package__` guard, so it raises ImportError when run as a script "
        f"(the container's `python main.py`)."
    )


def test_main_is_the_only_place_the_loop_lives():
    """The `while True` loop belongs to the entry point, not a sibling."""
    for name in SPLIT_MODULES:
        src = (GENERATOR / f"{name}.py").read_text()
        assert "while True:" not in src, (
            f"{name}.py contains the generation loop; that belongs in main.py"
        )
    assert "while True:" in (GENERATOR / "main.py").read_text()
