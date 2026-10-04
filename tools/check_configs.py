"""Does each config.yaml actually load, and is there only ever one of them?

Every generator reads config.yaml each tick and now *rejects* keys it does not
know (t_6081478a), so a config that fails validation is not a soft problem: the
generator dies on its first tick. Worth checking before an image is built rather
than after a container crash-loops.

The second half of the check is the one that matters historically. Every industry
had a second copy of its config at `<industry>/standalone/config.yaml`, and both
copies drifted:

* grocery's fell **117 lines** behind while the published image kept shipping it,
  so the elasticity, stockout, weather and transport settings added since
  (t_08deeddf, t_959cd040, t_2ab1fb0a) were configured in development and absent
  from the artifact. Only the dataclass defaults kept the image working, so the
  drift was invisible right up until a default moved.
* gas-station's was a byte-identical orphan that nothing read at all.

Neither drifted *visibly*, because the copy that drifts is always the one no test
touches. So a second copy is now a hard failure, and the standalone image ships
`<industry>/config.yaml` — the same file the dev stack mounts.

Validation runs through the product's own `_apply_yaml`, so this exercises the
real path (including hourly-weight normalisation), not a re-implementation.

Usage:
    python tools/check_configs.py [checkout]
    from check_configs import run; run(Path("/some/tree"))
"""
from __future__ import annotations

import importlib
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _offline_deps  # noqa: E402  (needs the sys.path insert above)

INDUSTRIES = ("grocery", "gas-station", "support")


def load_product(checkout: pathlib.Path, industry: str):
    """Import that industry's `config` module with its own directory importable.

    Each product's generator is built with `context: ./generator`, so the module
    imports its siblings by bare name (`from config import Config`) and the
    industry directory has to be on sys.path as a plain entry — hence the
    insert/remove around the import.
    """
    gen_dir = str(checkout / industry / "generator")
    sys.path.insert(0, gen_dir)
    for name in ("config", "config_schema", "elasticity", "scenarios"):
        sys.modules.pop(name, None)
    try:
        return importlib.import_module("config")
    finally:
        sys.path.remove(gen_dir)


def _check_industry(checkout: pathlib.Path, industry: str, yaml) -> int:
    """Check one industry. Returns its failure count (0 = clean)."""
    authoritative = checkout / industry / "config.yaml"
    if not authoritative.exists():
        print(f"{industry}: FAIL — no config.yaml")
        return 1

    # A second copy is the failure mode this check exists for. Checked before
    # loading, and reported as a failure rather than a warning.
    stale = sorted(p for p in (checkout / industry).glob("standalone/config.yaml") if p.exists())
    failures = 0
    if stale:
        print(f"{industry}: FAIL — a second config.yaml exists at "
              f"{[str(p.relative_to(checkout)) for p in stale]}; nothing reads it and "
              f"it will drift. The image ships {industry}/config.yaml.")
        failures += 1

    try:
        config_mod = load_product(checkout, industry)
    except Exception as exc:  # noqa: BLE001
        print(f"{industry}: FAIL — could not import its config module: {exc}")
        return failures + 1

    # Apply to a fresh Config exactly as a live tick would, with the file's own
    # path so the error message names the file a user edits.
    cfg = config_mod.Config(conf_path=str(authoritative))
    try:
        config_mod._apply_yaml(cfg, yaml.safe_load(authoritative.read_text()) or {})
    except Exception as exc:  # noqa: BLE001
        print(f"{industry}: FAIL — {authoritative.relative_to(checkout)}: {exc}")
        failures += 1
    else:
        print(f"{industry}: OK — {authoritative.relative_to(checkout)} "
              f"({len(authoritative.read_text().splitlines())} lines)")

    return failures


def run(checkout: pathlib.Path) -> int:
    """Check every industry's config under `checkout`. Returns the failure count."""
    # uv's unpacked wheels: yaml/psycopg2 are not installed on some hosts.
    for archive in _offline_deps.archive_paths():
        if archive not in sys.path:
            sys.path.append(archive)
    import yaml  # noqa: PLC0415

    failures = 0
    for industry in INDUSTRIES:
        if not (checkout / industry / "generator" / "schema.sql").exists():
            continue
        failures += _check_industry(checkout, industry, yaml)
    return failures


def main() -> int:
    checkout = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    return 1 if run(checkout) else 0


if __name__ == "__main__":
    raise SystemExit(main())
