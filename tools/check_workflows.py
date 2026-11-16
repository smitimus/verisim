"""Structural check of the GitHub Actions workflows.

A workflow is only syntax-checked when GitHub runs it, so a typo in a job name or
a malformed `needs:` shows up as a red run rather than as a review comment. This
parses every workflow and asserts the shape the release contract depends on:

* the jobs exist and `publish` gates on both test jobs;
* every `run:` step is a mapping (not a bare string), which is what silently
  disables a step;
* the credential guard is present and is NOT gated on the secret being non-empty —
  the t_44f5663e regression, where a publish reported success while publishing
  nothing for three weeks.

Usage:  python tools/check_workflows.py <checkout>
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _offline_deps  # noqa: E402  (needs the sys.path insert above)

# The repo root, derived from this file's location — NOT from sys.argv. This
# module is imported by the test suite, where sys.argv belongs to pytest and
# argv[1] is whatever path pytest was pointed at (t_a6ecb731, caught in CI).
CHECKOUT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = CHECKOUT / ".github" / "workflows"

# Every workflow must build-and-gate before it publishes.
REQUIRED_JOBS = ("test", "integration", "publish")


def main(root: pathlib.Path | None = None) -> int:
    # `root` exists so a test can point the checker at a mutated copy (same shape
    # as tools/check_publish_tags.py::main); production callers pass nothing and
    # get the checkout this file lives in.
    workflows_dir = (root / ".github" / "workflows") if root is not None else WORKFLOWS
    # yaml is a declared dependency of this project (pyproject/requirements), so
    # CI has it; the offline helper only matters on hosts that cannot pip install.
    for archive in _offline_deps.archive_paths():
        if archive not in sys.path:
            sys.path.append(archive)
    import yaml  # noqa: PLC0415

    rc = 0
    files = sorted(workflows_dir.glob("*.yml")) + sorted(workflows_dir.glob("*.yaml"))
    if not files:
        print(f"no workflows under {workflows_dir}")
        return 2

    for path in files:
        name = path.name
        try:
            doc = yaml.safe_load(path.read_text())
        except Exception as exc:  # noqa: BLE001
            print(f"{name}: FAIL — does not parse: {exc}")
            rc = 1
            continue

        jobs = doc.get("jobs") or {}
        missing = [j for j in REQUIRED_JOBS if j not in jobs]
        if missing:
            print(f"{name}: FAIL — missing job(s) {missing}; has {sorted(jobs)}")
            rc = 1
            continue

        # publish must wait for both gates, or a red test can still ship.
        needs = jobs["publish"].get("needs")
        needs = [needs] if isinstance(needs, str) else list(needs or [])
        if not {"test", "integration"} <= set(needs):
            print(f"{name}: FAIL — publish.needs={needs}, must include test + integration")
            rc = 1
        else:
            print(f"{name}: OK — publish gated on {sorted(set(needs))}")

        # Steps must be mappings; a bare string step is ignored by Actions.
        for job_name, job in jobs.items():
            for step in job.get("steps") or []:
                if not isinstance(step, dict):
                    print(f"{name}: FAIL — {job_name} has a non-mapping step: {step!r}")
                    rc = 1

        # The credential guard: present, and NOT conditioned on the secret's value.
        publish_steps = jobs["publish"].get("steps") or []
        guard = next(
            (s for s in publish_steps
             if isinstance(s, dict) and "DOCKER_USERNAME" in str(s.get("run", ""))
             and "exit 1" in str(s.get("run", ""))),
            None,
        )
        if guard is None:
            print(f"{name}: FAIL — no credential guard in publish "
                  f"(a missing credential must fail the run)")
            rc = 1
        elif "DOCKER_USERNAME" in str(guard.get("if", "")):
            print(f"{name}: FAIL — the credential guard is gated on the secret's "
                  f"value: if: {guard.get('if')!r} — that is the t_44f5663e "
                  f"regression (green publish, nothing published)")
            rc = 1
        else:
            print(f"{name}: OK — credential guard present and not value-gated")

        # ── the standalone-image job must not run on pull requests ──────────
        # t_19eb986e: a ~1 GB image plus a multi-GB buildx builder state, per run,
        # on the ONE 40 G act-runner volume that the blocking `test` job shares
        # with a monitoring stack. A card branch pushes several times per PR, and
        # `publish` builds its own image anyway — so a pull-request build is disk
        # churn against the gate, not coverage.
        integration_if = str(jobs["integration"].get("if") or "")
        if "pull_request" not in integration_if:
            print(f"{name}: FAIL — jobs.integration has no `if:` excluding pull "
                  f"requests (if: {integration_if!r}); every PR push then pays "
                  f"~1 GB image + builder state on the gate's runner (t_19eb986e)")
            rc = 1
        else:
            print(f"{name}: OK — the image job is gated off pull requests")

        # ── no step may hard-code the loopback address of the smoke container ──
        # On the self-hosted act runner the job runs INSIDE a container with the
        # host's docker socket, so `-p 8010:8000` publishes on the runner host and
        # `localhost:8010` from the job is the job itself: measured "connection
        # refused" every time, and the step burned its full 180 s while the image
        # was healthy (t_19eb986e). The smoke step resolves an address and exports
        # it; every later step must read that variable.
        #
        # Only step BODIES and inputs are inspected — the prose above these steps
        # has to be free to name the address it is warning about.
        run_bodies = [str(s.get("run") or "") for s in jobs["integration"].get("steps") or []]
        with_values = " ".join(str(v) for s in jobs["integration"].get("steps") or []
                               for v in (s.get("with") or {}).values())
        if any("://localhost:" in body for body in run_bodies):
            print(f"{name}: FAIL — an integration step hard-codes a `localhost` URL. "
                  f"On the act runner that address is the job container, not the "
                  f"published port (t_19eb986e). Export it from the smoke step.")
            rc = 1
        elif not any("API_BASE_URL=" in body for body in run_bodies):
            print(f"{name}: FAIL — no integration step exports an *_API_BASE_URL "
                  f"for the container's API; the smoke step owns the address "
                  f"(t_19eb986e)")
            rc = 1
        else:
            print(f"{name}: OK — the API address is resolved once and exported")

        # A tar round-trip is a second full copy of the image on the runner root.
        if "type=docker,dest=" in with_values:
            print(f"{name}: FAIL — the image build writes a tar via "
                  f"`outputs: type=docker,dest=…` and reloads it; use `load: true` "
                  f"(t_19eb986e)")
            rc = 1

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
