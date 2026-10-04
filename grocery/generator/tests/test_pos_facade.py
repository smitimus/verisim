"""
The `models/pos.py` facade must keep re-exporting every public name (t_c2eca5dd).

The POS model was split into four sibling modules. `main.py` and the other
models still reach these symbols as `pos.<name>`, so a name that quietly stops
being exported is a runtime AttributeError in the generator — not a test
failure, unless something pins the surface. This file is that pin.

It also pins the single-module-identity property that makes the split safe under
both import paths (main.py's flat `models.pos`, pytest's dotted
`grocery.generator.models.pos`): the facade imports its siblings
package-RELATIVELY, so there is exactly one copy of each file per package and a
patch on one reaches the code in the other. See the note in `pos.py`.
"""
import importlib

import pytest

import grocery.generator.models.pos as pos


# Every name `models/pos.py` exported before the split, and where it lives now.
# If a function moves again, update the right-hand side rather than deleting the
# row — the facade is the compatibility surface, so dropping a row is the bug
# this test exists to catch.
PUBLIC_SURFACE = {
    # catalog (pos_catalog)
    "seed_departments": "pos_catalog",
    "fetch_departments": "pos_catalog",
    "seed_products": "pos_catalog",
    "fetch_active_products": "pos_catalog",
    "seed_price_history": "pos_catalog",
    "maybe_update_product_prices": "pos_catalog",
    # promotions (pos_promotions)
    "seed_named_coupons": "pos_promotions",
    "seed_coupons": "pos_promotions",
    "fetch_active_coupons": "pos_promotions",
    "seed_combo_deals": "pos_promotions",
    "fetch_active_deals": "pos_promotions",
    "reconcile_promotions": "pos_promotions",
    # loyalty (pos_loyalty)
    "seed_loyalty_members": "pos_loyalty",
    "fetch_loyalty_members": "pos_loyalty",
    # transactions (pos_txn)
    "generate_pos_transactions": "pos_txn",
    "price_of_record": "pos_txn",
    "draw_cart": "pos_txn",
    "read_schema_sql": "pos_txn",
    # constants
    "PAYMENT_METHODS": "pos_txn",
    "PAYMENT_WEIGHTS": "pos_txn",
    "TIERS": "pos_loyalty",
    "DEFAULT_PROMO_HISTORY_DAYS": "pos_promotions",
    # private helpers the tests reach for
    "_promo_applies_on": "pos_promotions",
    "_as_date": "pos_promotions",
    "_promo_history_start": "pos_promotions",
    "_fetch_active_coupons": "pos_promotions",
    "_fetch_active_deals": "pos_promotions",
    "_record_loyalty_points": "pos_loyalty",
    "_fetch_departments": "pos_catalog",
    "_fetch_active_products": "pos_catalog",
    "_draw_elasticity": "pos_catalog",
    "_pick_uom": "pos_catalog",
    "_price_of": "pos_txn",
    "_applicable_promo_products": "pos_txn",
    "_dept_id_for_product": "pos_txn",
    # elasticity re-exports (tests reach them through `pos.`)
    "demand_weight": "elasticity",
    "choose_products": "elasticity",
    "sample_price_paths": "elasticity",
    "seed_elasticity_columns": "elasticity",
}

SIBLINGS = ("pos_catalog", "pos_promotions", "pos_loyalty", "pos_txn")


def _owner_module(owner):
    """The module a re-exported name must be identical to.

    `elasticity` is deliberately NOT dotted: like every other top-level import
    in the generator, the facade does `from elasticity import demand_weight`,
    which binds the flat `elasticity` module. The dotted `grocery.generator.
    elasticity` is a *second* copy of that file — a pre-existing quirk of a
    tree with no package `__init__.py` chain, not something this split
    introduced. So identity is checked against the flat module, which is the
    one actually bound.
    """
    if owner == "elasticity":
        return importlib.import_module("elasticity")
    return importlib.import_module(f"grocery.generator.models.{owner}")


@pytest.mark.parametrize("name,owner", sorted(PUBLIC_SURFACE.items()))
def test_facade_still_exports(name, owner):
    """Every pre-split public name resolves off `models.pos`."""
    assert hasattr(pos, name), (
        f"models/pos.py no longer exports {name!r}. main.py and the sibling "
        f"models reach these as `pos.{name}`, so this is a runtime "
        f"AttributeError in the generator, not just a lost export."
    )


@pytest.mark.parametrize("name,owner", sorted(PUBLIC_SURFACE.items()))
def test_facade_export_is_the_same_object(name, owner):
    """The facade must re-export, not copy — identity is what makes patching work."""
    src = _owner_module(owner)
    assert getattr(pos, name) is getattr(src, name), (
        f"pos.{name} is not the same object as {owner}.{name} — the facade "
        f"must import, not re-define or wrap."
    )


@pytest.mark.parametrize("sibling", SIBLINGS)
def test_facade_uses_relative_imports(sibling):
    """Absolute `models.<sibling>` imports would create a second module copy.

    main.py puts `grocery/generator` on sys.path and imports `models.pos`;
    pytest imports `grocery.generator.models.pos`. With absolute imports the
    siblings would be loaded twice under different names, and a test patching
    one copy would miss the writer running in the other — which is exactly how
    this split broke 25 tests before the facade was made relative.
    """
    import pathlib

    src = pathlib.Path(pos.__file__).read_text()
    offenders = [
        line.strip() for line in src.splitlines()
        if line.strip().startswith(("import models.", "from models."))
    ]
    assert not offenders, (
        "models/pos.py must import its siblings package-relatively "
        f"(`from .{sibling} import ...`), not absolutely. Found: {offenders}"
    )


def test_siblings_avoid_absolute_pos_imports():
    """The sibling modules must not reach back through the facade.

    A sibling importing `models.pos` would be circular, and importing it by
    absolute path would pull the facade in under the flat name too.
    """
    import pathlib

    root = pathlib.Path(pos.__file__).parent
    for sibling in SIBLINGS:
        src = (root / f"{sibling}.py").read_text()
        for line in src.splitlines():
            stripped = line.strip()
            if stripped.startswith(("from models.pos", "import models.pos")):
                assert "pos_catalog" in stripped or "pos_promotions" in stripped \
                    or "pos_loyalty" in stripped or "pos_txn" in stripped, (
                    f"{sibling}.py imports through the facade: {stripped!r}"
                )


def test_pos_facade_is_thin():
    """The facade should be imports and re-exports, not logic."""
    import pathlib

    src = pathlib.Path(pos.__file__).read_text()
    # no function/class definitions at all
    assert "\ndef " not in src, "models/pos.py grew a function; logic belongs in a sibling"
    assert "\nclass " not in src, "models/pos.py grew a class; it should stay a facade"
