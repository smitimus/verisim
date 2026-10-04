"""
Config-path acceptance for the customer dimension (t_2ffb43a0).

Run against a materialised HEAD tree by `run_tests_head.py`, so this verifies
the COMMIT rather than whatever else is uncommitted in the working tree.

The properties asserted here are the ones a schema/model change can silently
break while every unit test still passes:

  * `grocery/config.yaml`'s three `customers:` keys reach the dataclass;
  * an override actually changes the generated dimension (a key that is parsed
    and then ignored looks identical to one that works);
  * `household_size_max` bounds the draw the DDL's CHECK will accept;
  * the keys are parsed in ONE place — the key table t_6081478a introduced — so
    the next person adding a key edits one list, not two.
"""
import os

import yaml

from grocery.generator.config import Config, _apply_yaml
from grocery.generator.models import customers

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")


def _cards(n):
    return [(f"{i:08d}-0000-0000-0000-000000000000", None) for i in range(n)]


def test_real_config_yaml_reaches_the_dataclass():
    cfg_yaml = yaml.safe_load(
        open(os.path.join(REPO_ROOT, "grocery", "config.yaml"), encoding="utf-8"))
    assert "customers" in cfg_yaml, (
        "grocery/config.yaml has no `customers:` block, so the keys the "
        "generator reads can never be tuned from config"
    )
    cfg = Config()
    _apply_yaml(cfg, cfg_yaml)
    assert cfg.customers.multi_member_household_share == \
        cfg_yaml["customers"]["multi_member_household_share"]
    assert cfg.customers.household_size_max == \
        cfg_yaml["customers"]["household_size_max"]
    assert isinstance(cfg.customers.segment_shares, dict)


def test_the_shipped_config_is_the_one_grocery_config_yaml():
    """The image ships grocery/config.yaml, so there is nothing left to disagree.

    This test used to compare `grocery/standalone/config.yaml` against
    `grocery/config.yaml` and assert the `customers:` block matched — a guard
    against exactly the failure it could not prevent. The two files drifted 117
    lines apart while this test kept passing, because a key added to the
    authoritative config is not a key added to *this* block: `pricing.*`,
    `inventory.enforce_stock_availability`, `transport.*` and the whole `weather`
    section all landed on one side only, and the image shipped the other. The
    guard was green for every one of those changes.

    The drift is gone because there is one config file (t_a6ecb731): the
    standalone Dockerfile COPYs grocery/config.yaml, so the image and the dev
    stack read the same bytes and cannot disagree. What is left to assert is that
    the Dockerfile still points at that one file — the failure mode being a
    future re-introduction of a second copy.
    """
    dockerfile = open(
        os.path.join(REPO_ROOT, "grocery", "standalone", "Dockerfile"),
        encoding="utf-8").read()
    assert "COPY grocery/config.yaml /app/config.yaml" in dockerfile, (
        "the grocery image must ship grocery/config.yaml; if it points at "
        "standalone/config.yaml again, a second copy can drift from it again"
    )
    assert not os.path.exists(
        os.path.join(REPO_ROOT, "grocery", "standalone", "config.yaml")
    ), (
        "grocery/standalone/config.yaml exists again — a second copy of the "
        "config is exactly what drifted 117 lines while the image kept "
        "shipping it (t_a6ecb731); tools/check_configs.py fails on this too"
    )


def test_the_customers_block_is_read_by_the_image_config():
    """The customers keys must be in the file the image actually ships."""
    main = yaml.safe_load(
        open(os.path.join(REPO_ROOT, "grocery", "config.yaml"), encoding="utf-8"))
    assert "customers" in main, (
        "grocery/config.yaml has no `customers:` block, so the keys the "
        "generator reads can never be tuned from config"
    )
    cfg = Config()
    _apply_yaml(cfg, main)
    assert cfg.customers.household_size_max == main["customers"]["household_size_max"], (
        "the customers keys did not reach the dataclass from the config the "
        "image ships"
    )


def test_an_override_actually_changes_the_dimension():
    """A parsed-but-ignored key is indistinguishable from a working one.

    So the override is carried all the way through to the households that come
    out, not just asserted on the dataclass.
    """
    probe = {"customers": {
        "multi_member_household_share": 0.77,
        "household_size_max": 9,
        "segment_shares": {"premium_enthusiast": 5.0},
    }}
    cfg = Config()
    _apply_yaml(cfg, probe)

    assert cfg.customers.multi_member_household_share == 0.77
    assert cfg.customers.household_size_max == 9
    assert cfg.customers.segment_shares == {"premium_enthusiast": 5.0}

    # The override reaches the model's own share lookup.
    mix = customers.segment_shares(cfg)
    assert mix["premium_enthusiast"] == 5.0

    # And it reaches the grouping: at 0.77 cards must pair up.
    plan = customers.plan_households(_cards(200), cfg)
    biggest = max(len(h["member_ids"]) for h in plan)
    assert biggest >= 2, (
        f"a multi_member_household_share of 0.77 produced no multi-card "
        f"household across 200 cards (biggest was {biggest}) — the key is "
        "being parsed and then ignored"
    )


def test_household_size_max_bounds_what_the_ddl_accepts():
    """The cap must hold on the value the generator will actually write.

    `pos.customers.household_size` carries `CHECK (household_size BETWEEN 1 AND
    12)`, so a cap above 12 must clamp to 12 or the INSERT is rejected — which
    on a backfill would take the generator down at boot.
    """
    cfg = Config()
    cfg.customers.household_size_max = 99          # above the DDL's own max
    plan = customers.plan_households(_cards(300), cfg)
    assert all(h["household_size"] <= customers.MAX_HOUSEHOLD_SIZE
               for h in plan)

    cfg.customers.household_size_max = 2
    plan = customers.plan_households(_cards(300), cfg)
    assert all(h["household_size"] <= 2 for h in plan)


def test_the_customers_keys_are_parsed_in_exactly_one_place():
    """One list of keys, so the next key added does not land in a dead branch.

    t_6081478a made config.yaml parsing table-driven. A hand-written
    `data.get('customers', ...)` block alongside it would parse the same keys
    twice: harmless today (last write wins, both write the same value) and a
    genuine trap for the next person, who would edit whichever one they found.
    """
    src = open(os.path.join(REPO_ROOT, "grocery", "generator", "config.py"),
               encoding="utf-8").read()
    assert "data.get('customers'" not in src, (
        "the customers keys are parsed by BOTH the key table and a "
        "hand-written block; keep exactly one"
    )
    rows = [line for line in src.splitlines() if "('customers'," in line]
    assert len(rows) >= 3, (
        f"only {len(rows)} customers keys in the key table — expected "
        "segment_shares, multi_member_household_share and household_size_max"
    )