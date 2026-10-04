"""
POS model — seeds departments, products, coupons, combo deals, loyalty members;
generates store transactions with coupon/deal application.

A facade over four sibling modules, one per concern (t_c2eca5dd):

  `models/pos_catalog`      departments, products, price_history, price moves
  `models/pos_promotions`   coupons, combo deals, validity windows, reconcile
  `models/pos_loyalty`      members, points, tiers
  `models/pos_txn`          the transaction writer + basket/price helpers

Every public name is re-exported here, so `from models import pos` callers,
`pos.generate_pos_transactions(...)` in `main.py`, and the tests that patch
`pos.execute_values` keep working unchanged. Import the sibling module directly
when you want one concern without the rest.

The elasticity re-exports (`demand_weight`, `choose_products`, ...) are here
because `tests/test_price_elasticity.py` reaches them through `pos.`

Imports below are package-relative (`from .pos_catalog import ...`) rather than
absolute (`from models.pos_catalog import ...`). This tree is imported under two
names — main.py puts `grocery/generator` on sys.path and gets `models.pos`, while
pytest imports `grocery.generator.models.pos` — and a relative import resolves
inside whichever package actually loaded, so both callers share one module object
per file. An absolute `models.*` import would create a SECOND copy of the
siblings under the flat name, and a test patching one copy would miss the code
running in the other.
"""
import logging

from psycopg2.extras import execute_values  # noqa: F401  (patch target)

from config import Config  # noqa: F401
from elasticity import (  # noqa: F401
    choose_products,
    demand_weight,
    sample_price_paths,
    seed_elasticity_columns,
)

log = logging.getLogger(__name__)

from .pos_catalog import (  # noqa: E402,F401
    _draw_elasticity,
    _fetch_active_products,
    _fetch_departments,
    _pick_uom,
    fetch_active_products,
    fetch_departments,
    maybe_update_product_prices,
    seed_departments,
    seed_price_history,
    seed_products,
)
from .pos_loyalty import (  # noqa: E402,F401
    TIERS,
    fetch_loyalty_members,
    seed_loyalty_members,
)
from .pos_promotions import (  # noqa: E402,F401
    DEFAULT_PROMO_HISTORY_DAYS,
    fetch_active_coupons,
    fetch_active_deals,
    reconcile_promotions,
    seed_combo_deals,
    seed_coupons,
    seed_named_coupons,
)
from .pos_txn import (  # noqa: E402,F401
    PAYMENT_METHODS,
    PAYMENT_WEIGHTS,
    draw_cart,
    generate_pos_transactions,
    price_of_record,
    read_schema_sql,
)

# Private helpers the tests reach for directly, and the cross-module edges:
# the txn writer calls the loyalty earn step and the promo-window filter.
from .pos_loyalty import _record_loyalty_points  # noqa: E402,F401
from .pos_promotions import (  # noqa: E402,F401
    _as_date,
    _fetch_active_coupons,
    _fetch_active_deals,
    _promo_applies_on,
    _promo_history_start,
)
from .pos_txn import (  # noqa: E402,F401
    _applicable_promo_products,
    _dept_id_for_product,
    _price_of,
)

# NOTE on `execute_values`: each sibling module does
# `from psycopg2.extras import execute_values`, exactly as every other model in
# the tree does. That means `patch('...models.pos.execute_values')` no longer
# reaches code that moved, so the tests that patch it now target the module
# that owns the function under test. `tests/test_pos_facade.py` pins the
# re-export surface so the split cannot silently drop a public name.
