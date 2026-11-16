"""The API's scenario whitelist must agree with what each engine actually honours.

A scenario name lives in three places that nothing tied together:

  * the engine's per-name switch (`_apply_single_scenario` in grocery/support,
    the branch chain in gas-station's `get_scenario_context`), which decides
    what the name *does*,
  * `VALID_SCENARIOS` in ``base/api/main.py``, which decides whether the name is
    *reachable* at all through `POST /{industry}/generator/scenarios`,
    `.../scenario-schedules` and `PATCH /{industry}/generator/config`, and
  * the Streamlit UI's per-industry catalogue in ``base/ui/app.py``, which
    decides whether the operator is ever offered it.

When the whitelist and the engine drift, the engine keeps honouring a name that
the API refuses with a 400. Nothing fails: the endpoint simply says the scenario
does not exist, and
the only way to arm it is a direct `INSERT INTO control.active_scenarios`.
That is exactly what happened to `redemption_rush` (t_74280120 added it to the
grocery engine; the whitelist never learned it, found by t_ec2f1107) — and the
drift went the other way too: `grocery/config.yaml`'s cross-domain scenarios
(`inflation_pressure`, `severe_weather`, `supplier_disruption`,
`regional_peak`, `deep_discount`) had been documented in README.md as available
since verisim#12 while the API refused every one of them.

This test compares the two by parsing the source, not by importing the app:
the blocking CI test job installs pytest/psycopg2/faker/pyyaml and has no
fastapi, so `import main` would fail there while passing on a dev box.

Run:  python -m pytest grocery/generator/tests/test_scenario_api_parity.py -v
"""
import ast
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parents[3]

# Each industry's scenario engine: the module that maps a scenario NAME to its
# effect. Only these files special-case a name.
ENGINES = {
    "grocery": "grocery/generator/scenarios/scenario_engine.py",
    "gas-station": "gas-station/generator/scenarios/scenario_engine.py",
    "support": "support/generator/scenarios/scenario_engine.py",
}

API_SOURCE = "base/api/main.py"

# The Streamlit UI keeps its own catalogue of scenario cards (`SCENARIOS_BY_INDUSTRY`)
# and renders an Activate button per entry against the same routes — a third
# copy of the same list, and the one a human actually presses. It drifted too:
# the grocery tab offered only the six original scenarios.
UI_SOURCE = "base/ui/app.py"
UI_CATALOGUES = {
    "grocery": "GROCERY_SCENARIOS",
    "gas-station": "GAS_STATION_SCENARIOS",
    "support": "SUPPORT_SCENARIOS",
}

# Names the UI deliberately does not offer, each with the reason. An entry must
# still be an engine-honoured, API-accepted name (asserted below), so a stale
# exemption cannot silently hide a drift.
UI_OMITS = {
    "grocery": {
        "regional_peak": (
            "the engine only fills ctx.per_store_multipliers, which no model reads, "
            "so a card here would be a button that changes nothing"
        ),
    },
}

# The parameter each engine passes the scenario name in. A comparison against
# one of these identifiers is a scenario-name switch; `ctx.scenario_tag ==
# 'normal'` (an attribute, not a name) is deliberately not collected.
SCENARIO_PARAMS = {"scenario_name", "active_scenario", "name", "scenario"}

# Names the engine never special-cases because they are applied outside the
# per-name switch — they must still be reachable through the API.
#   * `normal` is the no-op baseline (the engine's `else` branch),
#   * `rush_hour` is automatic and stacks from `simulation_dt.hour`, so it is
#     never a branch in the switch, but switching it on (or scheduling it) is
#     how an operator pins a rush regime.
SHARED_NAMES = {"normal", "rush_hour"}


# ---------------------------------------------------------------------------
# Source readers
# ---------------------------------------------------------------------------

def _scenario_names_in_engine(path: pathlib.Path) -> set:
    """Every string constant an engine compares a scenario-name parameter to.

    Handles both `if scenario_name == 'x'` branch chains and a membership test
    `if scenario_name in {'x', 'y'}`, so a future engine shape is covered
    rather than silently skipped.
    """
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        if not (isinstance(node.left, ast.Name) and node.left.id in SCENARIO_PARAMS):
            continue
        for op, comparator in zip(node.ops, node.comparators):
            if isinstance(op, (ast.Eq, ast.In)):
                names |= _string_constants(comparator)
    return names


def _string_constants(node) -> set:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        found = set()
        for element in node.elts:
            found |= _string_constants(element)
        return found
    return set()


def _valid_scenarios(source: pathlib.Path) -> dict:
    """`VALID_SCENARIOS` parsed out of the API source, without importing it."""
    tree = ast.parse(source.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "VALID_SCENARIOS" for t in node.targets
        ):
            return {
                key.value: _string_constants(value)
                for key, value in zip(node.value.keys, node.value.values)
            }
    raise AssertionError(f"no module-level VALID_SCENARIOS in {source}")


def _engine_names(industry: str, repo: pathlib.Path = REPO) -> set:
    return _scenario_names_in_engine(repo / ENGINES[industry])


def _api_names(industry: str, repo: pathlib.Path = REPO) -> set:
    return _valid_scenarios(repo / API_SOURCE)[industry]


def _ui_catalogue(industry: str, repo: pathlib.Path = REPO) -> set:
    """The scenario keys the industry's Streamlit tab renders cards for."""
    wanted = UI_CATALOGUES[industry]
    tree = ast.parse((repo / UI_SOURCE).read_text())
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == wanted
        ):
            assert isinstance(node.value, ast.Dict)
            return {key.value for key in node.value.keys}
    raise AssertionError(f"no module-level {wanted} in {UI_SOURCE}")


def _missing_from_api(industry: str, repo: pathlib.Path = REPO) -> set:
    """Engine-honoured names the API would refuse with a 400."""
    return _engine_names(industry, repo) - _api_names(industry, repo)


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("industry", sorted(ENGINES))
def test_every_engine_scenario_is_reachable_through_the_api(industry):
    """The whitelist may not be narrower than the engine.

    A name the engine special-cases but the whitelist refuses is a lever the
    documentation advertises and the API rejects — the redemption_rush defect.
    """
    missing = _missing_from_api(industry)
    assert not missing, (
        f"{industry}: the scenario engine honours {sorted(missing)} but "
        f"VALID_SCENARIOS[{industry!r}] in {API_SOURCE} does not list them, so "
        f"POST /{industry}/generator/scenarios would answer 400 for "
        f"{sorted(missing)}. Add them to the whitelist (the engine already "
        f"merges them) or delete the branch."
    )


@pytest.mark.parametrize("industry", sorted(ENGINES))
def test_every_whitelisted_scenario_has_an_engine_branch(industry):
    """The whitelist may not be wider than the engine either.

    An accepted name with no branch falls through to the engine's `else` and
    changes nothing while `control.generator_state` reports it as active — a
    lever that reports success and does nothing.
    """
    unhandled = _api_names(industry) - _engine_names(industry) - SHARED_NAMES
    assert not unhandled, (
        f"{industry}: VALID_SCENARIOS[{industry!r}] accepts {sorted(unhandled)} "
        f"but the engine has no branch for them, so they are silently inert. "
        f"Implement the scenario or drop it from the whitelist."
    )


def test_the_whitelist_covers_every_industry_that_has_an_engine():
    """A new industry's engine without a whitelist entry gets a KeyError → 500."""
    valid = _valid_scenarios(REPO / API_SOURCE)
    assert set(valid) == set(ENGINES), (
        f"VALID_SCENARIOS declares {sorted(valid)}, the scenario engines are "
        f"{sorted(ENGINES)} — every engine needs a whitelist entry and vice versa"
    )


def test_redemption_rush_is_reachable():
    """The card's own acceptance, pinned by name (t_ec2f1107).

    The generic parity test above covers the class; this one survives a
    refactor that accidentally empties the grocery engine's branch list by
    deleting the scenario it cannot see. `redemption_rush` is the heavy
    points-spend A/B regime (t_74280120) and must be armable through the API.
    """
    assert "redemption_rush" in _engine_names("grocery")
    assert "redemption_rush" in _api_names("grocery")


@pytest.mark.parametrize("industry", sorted(ENGINES))
def test_the_ui_offers_exactly_the_scenarios_the_api_accepts(industry):
    """The Streamlit tab is the surface a human arms a scenario from.

    It renders one card per catalogue key and calls the same POST/DELETE routes,
    so a name the API accepts but the UI omits is invisible in the product, and a
    name the UI offers but the API refuses is a button that 400s.
    """
    offered = _ui_catalogue(industry)
    expected = _api_names(industry) - set(UI_OMITS.get(industry, {}))
    assert offered == expected, (
        f"{industry}: the UI tab offers {sorted(offered)}, the API accepts "
        f"{sorted(expected)}. Add the missing scenarios to "
        f"{UI_CATALOGUES[industry]} in {UI_SOURCE} (or remove the stale card)."
    )


def test_ui_exemptions_are_justified_and_really_omitted():
    """An exemption may only hide a name that exists and is deliberately absent."""
    for industry, omitted in UI_OMITS.items():
        for name, reason in omitted.items():
            assert reason.strip(), f"{industry}/{name}: an exemption needs a reason"
            assert name in _engine_names(industry), name
            assert name in _api_names(industry), name
            assert name not in _ui_catalogue(industry), (
                f"{industry}/{name}: listed as omitted but the UI offers it — "
                f"delete the exemption"
            )


# ---------------------------------------------------------------------------
# The guard has teeth
# ---------------------------------------------------------------------------

def test_the_ui_catalogue_check_fails_when_the_tab_is_stale(tmp_path):
    """The grocery tab as it stood before this card: six cards, six scenarios."""
    repo = _copy_sources(tmp_path)
    ui = repo / UI_SOURCE
    ui.write_text(_replace_assignment(
        ui.read_text(),
        "GROCERY_SCENARIOS",
        'GROCERY_SCENARIOS = {\n'
        '    "normal": {}, "rush_hour": {}, "weekend": {}, "promotion": {},\n'
        '    "holiday_week": {}, "double_coupons": {},\n'
        '}',
    ))
    missing = (
        _api_names("grocery", repo)
        - _ui_catalogue("grocery", repo)
        - set(UI_OMITS.get("grocery", {}))
    )
    assert missing == {
        "inflation_pressure",
        "severe_weather",
        "supplier_disruption",
        "deep_discount",
        "redemption_rush",
    }


def test_the_parity_check_fails_when_the_whitelist_is_stale(tmp_path):
    """Reproduce the reported defect: the pre-t_ec2f1107 grocery whitelist.

    Planted verbatim from ``main.py`` as it stood when the card was filed, so
    the check is watched failing on the shape it exists for — not merely
    asserted to pass on the fixed tree.
    """
    repo = _copy_sources(tmp_path)
    api = repo / API_SOURCE
    api.write_text(replace_grocery_whitelist(
        api.read_text(),
        '{"normal", "rush_hour", "weekend", "promotion", "holiday_week", "double_coupons"}',
    ))

    # The defect as measured 2026-11-16 on dev/106: the engine honoured six
    # names the whitelist refused, so six documented levers were unreachable.
    assert _missing_from_api("grocery", repo) == {
        "redemption_rush",
        "inflation_pressure",
        "severe_weather",
        "supplier_disruption",
        "regional_peak",
        "deep_discount",
    }


def test_the_parity_check_fails_when_the_whitelist_invents_a_name(tmp_path):
    """The reverse drift: an accepted name nobody implements."""
    repo = _copy_sources(tmp_path)
    api = repo / API_SOURCE
    api.write_text(replace_grocery_whitelist(
        api.read_text(),
        '{"normal", "rush_hour", "weekend", "promotion", "holiday_week", '
        '"double_coupons", "inflation_pressure", "severe_weather", '
        '"supplier_disruption", "regional_peak", "deep_discount", '
        '"redemption_rush", "moon_phase"}',
    ))
    assert _api_names("grocery", repo) - _engine_names("grocery", repo) - SHARED_NAMES == {"moon_phase"}


def _replace_assignment(source: str, name: str, replacement: str) -> str:
    """Replace a module-level assignment statement by its exact source span."""
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            lines = source.splitlines(keepends=True)
            start = sum(len(line) for line in lines[: node.lineno - 1]) + node.col_offset
            end = sum(len(line) for line in lines[: node.end_lineno - 1]) + node.end_col_offset
            return source[:start] + replacement + source[end:]
    raise AssertionError(f"no module-level {name} assignment")


def replace_grocery_whitelist(source: str, replacement: str) -> str:
    """Replace the grocery set in VALID_SCENARIOS, whatever it currently holds.

    The AST gives the literal's exact source span; a text search does not —
    ``"grocery":`` appears earlier in ``main.py`` (the industry→database map),
    so a naive brace scan edits the wrong dictionary.
    """
    tree = ast.parse(source)
    dict_node = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "VALID_SCENARIOS" for t in node.targets
        ):
            assert isinstance(node.value, ast.Dict)
            dict_node = node.value
            break
    assert dict_node is not None, "no module-level VALID_SCENARIOS"
    for key, value in zip(dict_node.keys, dict_node.values):
        if isinstance(key, ast.Constant) and key.value == "grocery":
            assert isinstance(value, ast.Set)
            lines = source.splitlines(keepends=True)
            start = sum(len(line) for line in lines[: value.lineno - 1]) + value.col_offset
            end = sum(len(line) for line in lines[: value.end_lineno - 1]) + value.end_col_offset
            return source[:start] + replacement + source[end:]
    raise AssertionError("the grocery whitelist entry is gone from VALID_SCENARIOS")


def _copy_sources(tmp_path: pathlib.Path) -> pathlib.Path:
    """A repo copy holding just what the checkers read."""
    repo = tmp_path / "repo"
    for relative in (*ENGINES.values(), API_SOURCE, UI_SOURCE):
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((REPO / relative).read_text())
    return repo
