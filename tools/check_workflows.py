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

# The repo root, derived from this file's location — NOT from sys.argv. This
# module is imported by the test suite, where sys.argv belongs to pytest and
# argv[1] is whatever path pytest was pointed at (t_a6ecb731, caught in CI).
CHECKOUT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = CHECKOUT / ".github" / "workflows"

# Every workflow must build-and-gate before it publishes.
REQUIRED_JOBS = ("test", "integration", "publish")


def main() -> int:
    import yaml  # noqa: PLC0415  (imported here so the module is importable bare)

    rc = 0
    files = sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml"))
    if not files:
        print(f"no workflows under {WORKFLOWS}")
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

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
