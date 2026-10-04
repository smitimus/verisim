"""The publish-tag convention, asserted from the repo the CI job reads.

`compute-push-tags.sh` decides what a release is called in the registry, and it
is the only place that decision is made — both workflows call it. This module
is the guard for the two ways that decision can rot:

* a workflow stops calling the script and inlines its own tag computation
  (which is exactly what the old code was, and how the two drifted apart); and
* the script itself is edited to drop the version, or the version stops being a
  version (a missing / malformed / pyproject-drifted VERSION file).

Both are silent by construction: a publish that emits `:${GITHUB_SHA}` succeeds,
the run is green, and the registry gains another unreadable tag. So this asserts
the shape rather than trusting the run.

Called by `gas-station/generator/tests/test_build_checks.py`; runnable directly:

    python tools/check_publish_tags.py [checkout]
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

# The repo root, derived from this file's location — NOT from sys.argv. These
# checkers are imported by the test suite, where argv belongs to pytest
# (t_a6ecb731 caught the argv version failing its own CI run).
CHECKOUT = pathlib.Path(__file__).resolve().parents[1]
COMPUTE = CHECKOUT / ".github" / "scripts" / "compute-push-tags.sh"
HARNESS = CHECKOUT / ".github" / "scripts" / "publish-tag-tests.sh"

# MAJOR.MINOR.PATCH with an optional -suffix / +build. Anchored, so a tag that
# merely *contains* a version does not pass.
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+([.-][0-9A-Za-z.-]+)?$")
SEMVER_IN_TAGLIST_RE = re.compile(
    r"^[a-z0-9.-]+(?:/[a-z0-9._-]+)*:\d+\.\d+\.\d+([.-][0-9A-Za-z.-]+)?$"
)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def read_version(root: pathlib.Path = CHECKOUT) -> str:
    """The tracked version, or '' when the file is absent."""
    path = root / "VERSION"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def pyproject_version(root: pathlib.Path = CHECKOUT) -> str:
    """The version pyproject declares, without importing tomllib-heavy paths.

    A regex rather than tomllib so the checker runs on a bare interpreter the
    way the rest of tools/ does; pyproject's version line is one fixed line.
    """
    path = root / "pyproject.toml"
    if not path.exists():
        return ""
    match = re.search(r'^version\s*=\s*"([^"]+)"', path.read_text(encoding="utf-8"), re.M)
    return match.group(1) if match else ""


def main(root: pathlib.Path | None = None) -> int:
    # The repo under test. Defaults to the real checkout; the tests pass a
    # mutated copy so the gate can be watched failing (a gate nobody has seen
    # fail is a gate that does not work).
    root = root or CHECKOUT
    rc = 0

    # ── The script exists and is the single source of the decision ───────────
    compute = root / ".github" / "scripts" / "compute-push-tags.sh"
    harness = root / ".github" / "scripts" / "publish-tag-tests.sh"
    if not compute.exists():
        print(f"FAIL — no {compute.relative_to(root)}: nothing computes the "
              f"published tags, so a workflow is inlining its own")
        return 1
    if not harness.exists():
        print(f"FAIL — no {harness.relative_to(root)}: the tag convention has "
              f"no test harness")
        return 1

    # ── VERSION is a version, and agrees with pyproject ──────────────────────
    version = read_version(root)
    if not version:
        print("FAIL — no VERSION file at the repo root. It is the tracked source "
              "of truth for the published tag (t_4b05829c); without it a publish "
              "falls back to a 40-char SHA.")
        rc = 1
    elif not VERSION_RE.match(version):
        print(f"FAIL — VERSION reads '{version}', not MAJOR.MINOR.PATCH")
        rc = 1
    else:
        declared = pyproject_version(root)
        if declared and declared != version:
            print(f"FAIL — VERSION is '{version}' but pyproject.toml declares "
                  f"'{declared}'. They name the same release; a publish is "
                  f"refused on a mismatch.")
            rc = 1
        else:
            print(f"OK — VERSION {version} is a version and agrees with pyproject")

    # ── The workflows call the script, and inline no tag computation ─────────
    workflows = sorted((root / ".github" / "workflows").glob("*.y*ml"))
    if not workflows:
        print("FAIL — no workflows to check")
        return 1

    for path in workflows:
        name = path.name
        text = path.read_text(encoding="utf-8")
        before = rc

        if "compute-push-tags.sh" not in text:
            print(f"{name}: FAIL — no workflow computes tags through "
                  f".github/scripts/compute-push-tags.sh; it must not inline its "
                  f"own (that duplication is how the SHA tag survived, t_4b05829c)")
            rc = 1

        # The old defect, spelled out: a `tags=` assignment built from
        # GITHUB_SHA with no version in it.
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or "tags=" not in stripped:
                continue
            if "compute-push-tags" in stripped:
                continue
            if "GITHUB_SHA" in stripped or "GITHUB_REF_NAME" in stripped:
                print(f"{name}: FAIL — an inlined tag computation assigns a tag "
                      f"from the ref/sha: {stripped!r}. A published tag must be a "
                      f"tracked version.")
                rc = 1

        if rc == before:
            print(f"{name}: OK — tags come from the shared script")

    # ── The script, executed, emits a version and no opaque tag ─────────────
    # Asserted against the real checkout for both event kinds. A tag the
    # registry can show a human is the whole point of the card.
    for ref, ref_name, expect_release in (
        ("refs/heads/main", "main", "false"),
        ("refs/tags/v" + (version or "0.0.0"), "v" + (version or "0.0.0"), "true"),
    ):
        env = {
            "REPO_ROOT": str(root),
            "IMAGE_NAME": "smiti/verisim-grocery",
            "GITHUB_REF": ref,
            "GITHUB_REF_NAME": ref_name,
            "GITHUB_SHA": "0123456789abcdef0123456789abcdef01234567",
            "PATH": "/usr/bin:/bin:/usr/local/bin",
        }
        proc = subprocess.run(
            ["bash", str(compute)], capture_output=True, text=True, env=env,
        )
        if proc.returncode != 0:
            print(f"compute-push-tags.sh {ref}: FAIL — exited {proc.returncode}: "
                  f"{proc.stderr.strip()}")
            rc = 1
            continue

        fields = dict(
            line.split("=", 1) for line in proc.stdout.strip().splitlines() if "=" in line
        )
        tags = [t.rsplit(":", 1)[-1] for t in fields.get("tags", "").split(",") if t]

        if fields.get("version") != version:
            print(f"compute-push-tags.sh {ref}: FAIL — reports version "
                  f"'{fields.get('version')}', VERSION says '{version}'")
            rc = 1

        if fields.get("release") != expect_release:
            print(f"compute-push-tags.sh {ref}: FAIL — release="
                  f"{fields.get('release')}, expected {expect_release}")
            rc = 1

        # Every tag that is not `latest` and not the commit sha must be a
        # version. This is the card's complaint, turned into an assertion.
        for tag in tags:
            if tag == "latest" or SHA_RE.match(tag):
                continue
            if SEMVER_IN_TAGLIST_RE.match(f"img:{tag}") or tag == f"v{version}":
                continue
            print(f"compute-push-tags.sh {ref}: FAIL — '{tag}' is neither a "
                  f"version nor the commit sha: an unreadable registry tag")
            rc = 1

        if version and version not in tags:
            print(f"compute-push-tags.sh {ref}: FAIL — the tracked version "
                  f"{version} is not in the push ({tags})")
            rc = 1

    # ── The harness passes, so the gate above has teeth ─────────────────────
    proc = subprocess.run(
        ["bash", str(harness), str(root)], capture_output=True, text=True,
    )
    if proc.returncode != 0:
        tail = proc.stdout.strip().splitlines()[-1:]
        print(f"publish-tag-tests.sh: FAIL — {tail}")
        rc = 1
    else:
        print(f"OK — {proc.stdout.strip().splitlines()[-1]}")

    # ── Both halves of the dual-registry/version merge are still intact ─────
    # This checker guards the tag NAMES (t_4b05829c). It cannot by itself see the
    # dual-registry publish (t_ff5a70ec), because that lives in the workflow and
    # the credential gate — and those two rewrote the same workflow hunk, so one
    # of them could have been merged away without this file noticing (t_e1b1de67).
    # reconciliation-tests.sh asserts both behaviours against the shipped
    # workflow and scripts, so run it here rather than leaving it as a script
    # nobody ever executes.
    reconciliation = root / ".github" / "scripts" / "reconciliation-tests.sh"
    if not reconciliation.exists():
        print(f"FAIL — no {reconciliation.relative_to(root)}: the merge of the "
              f"dual-registry publish and the tracked version tag has no gate, so "
              f"one of them can be silently dropped again (t_e1b1de67)")
        rc = 1
    else:
        proc = subprocess.run(
            ["bash", str(reconciliation), str(root)],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            for line in proc.stdout.strip().splitlines():
                if line.startswith("  FAIL"):
                    print(f"reconciliation-tests.sh: {line.strip()}")
            tail = proc.stdout.strip().splitlines()[-1:]
            print(f"reconciliation-tests.sh: FAIL — {tail}")
            rc = 1
        else:
            print(f"OK — {proc.stdout.strip().splitlines()[-1]}")

    return rc


if __name__ == "__main__":
    raise SystemExit(main())