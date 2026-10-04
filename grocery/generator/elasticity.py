"""
Price -> demand elasticity: the one law both channels draw products with.

Before t_08deeddf `pos.generate_pos_transactions` built a cart with
`random.choices(products, k=num_items)` — every SKU equally likely whatever its
price, whatever the weekly ad was promoting it at, whatever the scenario's
`price_modifier` said. The 90-day `price_history` backfill is seeded explicitly
"for elasticity analysis", so `data-lab`'s
`mart_product_price_elasticity` was regressing units on price and finding
whatever noise the seed happened to produce. Two channels each owning a private
volume law is exactly how t_94bbf1ce (POS 30x) and t_eb31c99f (online ~120x)
happened, so the elasticity law lives here, once, and both `models.pos` and
`models.online` import it.

THE LAW. For a SKU with reference price `p0` and own elasticity `e`, the
relative price ratio `r = price / p0` sets how attractive it is:

    weight = r ** e          (constant-elasticity, log-log)

`e < 0` is ordinary retail demand: raise the price, sell fewer. `e = -0.5` is
the default — a 10% price rise costs about 5% of units, a 40% rise costs about
17%. Read it the way a category manager would: *"our cereal is price-sensitive;
a doubling costs us 29% of its volume, our detergent barely cares."*

The product's weight is then used as the `weights=` argument of
`random.choices`, so it biases *which SKU is picked*; the basket size itself is
unaffected, because customers do not shop less because one item got dearer.
The weight is clipped to a band (see `DEMAND_WEIGHT_CLIP`) so one pathological
SKU — a cents item on a 97% discount, say — cannot absorb every basket and
starve the rest of the catalogue.

WHY A REFERENCE PRICE AT ALL. `current_price` is the price of record, which on
an ad week is the *promoted* price, so curving on it directly would compare a
$3.20 ad price against a $4.00 reference and treat the discount as a permanent
state of the world, not a change. `reference_price` is the everyday shelf price
and never moves with an ad, so the ratio is "how far today sits from normal" —
which is what both the demand law and a downstream elasticity regression need.

ESTIMABILITY. The law above is only measurable in the data if a product's own
price moves *independently* of the market's. `sample_price_paths` therefore
gives every SKU the same `market_index` for a given day (a store-wide
inflation drift) plus its own idiosyncratic step. Regress one SKU's units on
its own price and the shared component drops out; what remains is the response
to the SKU's own decision. With an independent walk per SKU (what the old
`seed_price_history` did) the two are collinear and no elasticity is
recoverable at all — coincidental, not causal.
"""
import logging
import math
import random
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2

from config import Config

log = logging.getLogger(__name__)

# The caller may pass a private `random.Random` (for a deterministic seed) or
# nothing at all, in which case the module-level `random` is used so a test's
# `random.seed(...)` governs the walk. Both expose `.gauss` / `.uniform`, which
# is the only surface used below.
RNG = Any

# Bound on a single SKU's relative pull, in units of the neutral weight of 1.0.
# The law itself is unbounded — a 97%-off item would legitimately pull ~6x —
# but a sampler that is unbounded turns one bad row into a catalogue that stops
# selling. Clipping the *weight*, not the price, keeps the sign and the
# elasticity measurable in the range that matters while refusing the tail.
DEMAND_WEIGHT_CLIP = (0.02, 25.0)

# Guard on the price ratio, mirroring pricing.price_min_ratio. A zero or
# negative price makes `r ** e` undefined (0 ** -0.5 is an error, not a
# number), so the ratio is floored before it is ever used.
MIN_PRICE_RATIO = 1e-6


def _safe_float(value, default: float = 0.0) -> float:
    """`float(value)`, or `default` for None / junk / NaN / inf."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def reference_price_of(product: Dict, fallback: Optional[float] = None) -> float:
    """The pivot price for `product`'s demand curve.

    `reference_price` is the column; when it is absent or unusable (a product
    row seeded before t_08deeddf, or a dict that never went through the
    enriched fetcher) the SKU is its own reference, which makes the ratio 1.0
    and the weight 1.0 — i.e. the old price-independent behaviour rather than
    a crash or a division by zero. `fallback` wins over `price` when given, so
    a caller that has already resolved a price of record can pass it through.
    """
    reference = _safe_float(product.get('reference_price'), 0.0)
    if reference > 0.0:
        return reference
    candidate = fallback if fallback is not None else product.get('price')
    price = _safe_float(candidate, 0.0)
    # 1.0 is the last resort: it makes the ratio 1.0 and the weight neutral,
    # which is the only safe thing to do with a row that has no usable price.
    return price if price > 0.0 else 1.0


def elasticity_of(product: Dict, cfg: Config) -> float:
    """This SKU's own elasticity, falling back to the configured default.

    A NULL column (a product row seeded before t_08deeddf) and a corrupt value
    both land on `pricing.default_price_elasticity`, so the curve is never
    evaluated against `None`.
    """
    value = product.get('price_elasticity')
    if value is None:
        return cfg.pricing.default_price_elasticity
    elasticity = _safe_float(value, cfg.pricing.default_price_elasticity)
    return elasticity


def demand_weight(product: Dict, price: Optional[float] = None,
                  cfg: Optional[Config] = None) -> float:
    """Relative attractiveness of `product` at `price`, for `random.choices`.

    `price` is the price OF RECORD for this tick — the weekly-ad
    `promoted_price` when the SKU is on ad this week, else `current_price` —
    which is what the shopper actually faces.
    """
    cfg = cfg or Config()
    reference = reference_price_of(product)
    paid = _safe_float(price if price is not None else product.get('price'), 0.0)
    if paid <= 0.0:
        # No usable price on this row: neutral, not "free" and not an error.
        return 1.0
    ratio = max(paid / reference, MIN_PRICE_RATIO)
    elasticity = elasticity_of(product, cfg)
    weight = math.exp(elasticity * math.log(ratio))
    if not math.isfinite(weight):
        return 1.0
    low, high = DEMAND_WEIGHT_CLIP
    return min(max(weight, low), high)


def price_ratio(product: Dict, price: Optional[float] = None) -> float:
    """`price / reference_price`, floored. The x-axis of the curve."""
    reference = reference_price_of(product)
    paid = _safe_float(price if price is not None else product.get('price'), 0.0)
    if paid <= 0.0 or reference <= 0.0:
        return 1.0
    return max(paid / reference, MIN_PRICE_RATIO)


def choose_products(products: Sequence[Dict], n: int,
                    ad_prices: Optional[Dict[str, float]] = None,
                    cfg: Optional[Config] = None) -> List[Dict]:
    """Pick `n` products with replacement, weighted by price elasticity.

    The single draw both channels use. `ad_prices` maps product_id -> the
    weekly-ad promoted price that is the price of record for that SKU today;
    an ad item is therefore bought because it is *cheaper*, through the same
    law as any other price change, rather than by a special case.
    """
    if not products or n <= 0:
        return []
    cfg = cfg or Config()
    ad_prices = ad_prices or {}

    weights = [
        demand_weight(product, ad_prices.get(str(product.get('product_id'))), cfg)
        for product in products
    ]
    total = sum(weights)
    if not math.isfinite(total) or total <= 0.0:
        return random.choices(products, k=n)
    return random.choices(products, weights=weights, k=n)


# ---------------------------------------------------------------------------
# The price walk that seeds price_history — with the market factor
# ---------------------------------------------------------------------------

def sample_market_index(days: int, steps: int, cfg: Config,
                        rng: Optional[Any] = None) -> List[float]:
    """A store-wide inflation index, one value per day of the window.

    Slowly drifting, bounded, and strictly positive. Every SKU on a given day
    gets this same number, so it is *the* common factor a per-product
    elasticity regression removes.
    """
    rng = rng or random
    # Enough calendar points to reach `days` back: one per `step_days`, plus
    # today. Deliberately longer than the price path so the index starts
    # slightly before the first recorded price.
    span = max(1, int(days)) + max(1, int(steps))
    index = [1.0]
    daily_step = max(0.0, 0.05 * (1.0 - cfg.pricing.price_market_factor_weight))
    for _ in range(span):
        index.append(max(0.5, min(1.8, index[-1] * (1.0 + rng.gauss(0.0, daily_step)))))
    return index


def sample_price_paths(skus: Sequence[Tuple[str, float]], days: int, steps: int,
                       cfg: Config, rng: Optional[Any] = None) -> List[Dict]:
    """Per-SKU historical price paths, each carrying the shared market factor.

    `skus` is a sequence of `(product_id, current_price)`. Returns one dict per
    SKU per step, oldest first:

        {'product_id', 'price', 'old_price', 'days_ago', 'market_index'}

    The decomposition is deliberate and is the whole point of the function:

        price(t) = current_price * market(t) * own(t)

    `market(t)` is common to every SKU on day t. `own(t)` is that SKU's own
    drift — a slow trend plus a small idiosyncratic step. Because `own` is
    never perfectly proportional to the market, a regression of this SKU's
    units on this SKU's price identifies the response to the part the market
    did not explain. That part is exactly what `demand_weight` responds to.
    """
    rng = rng or random
    days = max(1, int(days))
    steps = max(1, int(steps))
    step_days = max(1, days // steps)
    market_weight = min(max(cfg.pricing.price_market_factor_weight, 0.0), 1.0)
    floor_ratio = max(cfg.pricing.price_min_ratio, 0.0)
    market = sample_market_index(days, steps, cfg, rng)

    # The calendar the walk is recorded on, oldest first, ALWAYS ending at 0
    # (today). `days - i * step_days` alone does not reach 0 for most
    # (days, steps) pairs — 90 and 12 gives 90, 83, ... 6 — and then the
    # newest recorded price sits six days in the past at a level the live
    # `current_price` contradicts, so `mart_product_price_elasticity`'s
    # "price on day" lookup is wrong for the whole current week and the
    # history does not join to what the till actually charges. The previous
    # implementation appended the 0 explicitly; this does too.
    offsets = list(range(days, 0, -step_days))
    if offsets[-1] != 0:
        offsets.append(0)

    records: List[Dict] = []
    for product_id, current_price in skus:
        # Walk BACKWARD from the live price, so the newest point is exactly
        # `current_price` and realtime continues from the seeded history with
        # no jump. `own` is the SKU's own drift (1.0 today, perturbed going
        # back); the market factor is applied on top of it.
        own: Dict[int, float] = {0: 1.0}
        own_step = 0.02 * (1.0 - market_weight)
        rng_source = rng or random
        for days_ago in offsets:
            if days_ago == 0:
                continue
            previous = own[min(own)]
            own[days_ago] = max(0.6, min(
                1.6, previous * (1.0 + rng_source.gauss(0.0, own_step))))

        # Newest first so the `old_price` of each record is the record that
        # follows it in time — which is the order `price_history` reads in.
        points = []
        for days_ago in offsets:
            m = market[min(len(market) - 1, days_ago)]
            deviation = (own[days_ago] - 1.0) * (1.0 - market_weight)
            factor = max(floor_ratio, m * (1.0 + deviation))
            points.append({'days_ago': days_ago,
                           'price': round(max(0.01, float(current_price) * factor), 2),
                           'market_index': m})

        for i, point in enumerate(points):
            # points[0] is today: its own price IS current_price and its
            # old_price is the price it came down from.
            old_price = (points[i + 1]['price'] if i + 1 < len(points)
                         else point['price'])
            records.append({
                'product_id': product_id,
                'price': point['price'],
                'old_price': old_price,
                'days_ago': point['days_ago'],
                'market_index': point['market_index'],
            })
    return records


def seed_elasticity_columns(conn, cfg: Config) -> Dict[str, int]:
    """Add and populate the elasticity columns. Idempotent; safe on any DB.

    A `schema.sql` change only reaches a *fresh* bootstrap — an existing data
    dir keeps the schema it was initialised with (this is the same trap the
    AGENTS.md documents for the PG16 -> PG18 rebuild and for `pos.returns`).
    So the columns are added here too, and existing rows are backfilled:
    `reference_price` = the current price (no reference, no signal — the
    neutral starting point) and `price_elasticity` = the configured default
    with the configured jitter, drawn deterministically per product so a
    restart does not re-roll the catalogue's whole personality.

    NEVER CRASH THE GENERATOR OVER A MIGRATION (t_b17da778)
    -----------------------------------------------------
    This function is called from `seed_all`, i.e. *before* the main loop, so an
    exception here does not cost one tick — it costs every tick, forever. On a
    data dir whose `schema.sql` was applied by the `postgres` role (which is
    what the standalone image's `entrypoint.sh` does, via
    `su -s /bin/bash postgres -c "$PSQL -f /app/generator/schema.sql"`) the
    generator connects as $POSTGRES_USER with GRANT ALL but no ownership, and
    the ALTER raises `InsufficientPrivilege: must be owner of table products`.
    Measured on CT106 2026-10-04: the generator wrote nothing for 45+ minutes
    while `pg_isready` kept the container green, so every container-level check
    read healthy and the shortfall surfaced only when a human counted rows.

    So an unavailable ALTER degrades to a no-op with a warning naming role,
    owner and remedy — the same contract `models/customers.py::ensure_tables`
    and `models/weather.py::ensure_table` already implement for the same trap.
    Losing the main loop to protect two columns that are merely missing is a
    bad trade.

    The probe before each ALTER is load-bearing, not tidiness: on a non-owner
    role `ADD COLUMN IF NOT EXISTS` STILL raises, because Postgres checks table
    ownership before it discovers there is nothing to do. So an up-to-date data
    dir must never reach the ALTER at all, or every healthy boot logs a scary
    warning and the one real failure is buried in it.
    """
    touched = {'added': 0, 'backfilled': 0}
    # Each column is attempted independently: a volume can be half-drifted,
    # and one refusal must not cost the other.
    if _ensure_column(conn, 'pos.products', 'reference_price',
                      'NUMERIC(8,2)'):
        touched['added'] += 1
    if _ensure_column(conn, 'pos.products', 'price_elasticity',
                      'NUMERIC(4,3)'):
        touched['added'] += 1

    # The backfill is plain UPDATE and needs no ownership beyond what the
    # generator already has, so it runs regardless of how the ALTERs went. It
    # is what fills the columns the moment an operator adds them out of band.
    try:
        with conn.cursor() as cur:
            # Existing rows: adopt the live price as the reference (so nothing
            # invents a price history it did not have) and give every SKU its
            # own elasticity from the configured default + jitter.
            jitter = max(0.0, cfg.pricing.elasticity_jitter)
            cur.execute("""
                UPDATE pos.products
                   SET reference_price = current_price
                 WHERE reference_price IS NULL
            """)
            touched['backfilled'] = cur.rowcount
            if jitter:
                cur.execute("SELECT product_id::text, sku FROM pos.products "
                            "WHERE price_elasticity IS NULL")
                rows = cur.fetchall()
                for product_id, sku in rows:
                    # Seeded from (sku, default) so the same catalogue gets the
                    # same elasticities on every boot.
                    rng = random.Random('verisim-elasticity-%s' % (sku or product_id))
                    value = cfg.pricing.default_price_elasticity * (
                        1.0 + rng.uniform(-jitter, jitter))
                    cur.execute(
                        "UPDATE pos.products SET price_elasticity = %s "
                        "WHERE product_id = %s::uuid",
                        (round(value, 3), product_id),
                    )
            else:
                cur.execute("""
                    UPDATE pos.products
                       SET price_elasticity = %s
                     WHERE price_elasticity IS NULL
                """, (cfg.pricing.default_price_elasticity,))
                touched['backfilled'] += cur.rowcount
        conn.commit()
    except psycopg2.Error as exc:
        # Same trade as the ALTER: a backfill that cannot run must not stop the
        # generator. `demand_weight` falls back to the configured default when
        # `reference_price`/`price_elasticity` are absent, so the demand law
        # still runs — uniformly, which is the state this volume was in before.
        conn.rollback()
        log.warning(
            "Elasticity backfill skipped: %s. The demand curve falls back to "
            "pricing.default_price_elasticity for the whole catalogue, so it "
            "is uniform rather than per-SKU — the mart_product_price_elasticity "
            "regression will find no signal. See "
            "elasticity.seed_elasticity_columns.",
            exc.__class__.__name__,
        )
        return touched

    if touched['added'] or touched['backfilled']:
        log.info("Elasticity columns: %d added, %d backfilled",
                 touched['added'], touched['backfilled'])
    return touched


def _ensure_column(conn, table: str, column: str, coltype: str) -> bool:
    """Add `table.column` when genuinely absent. True when added.

    Failure-tolerant by design: returns False rather than raising when the
    role cannot ALTER the table, because the caller is on the boot path.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND column_name = %s
        """, (table.split('.')[0], table.split('.')[1], column))
        if cur.fetchone()[0]:
            return False        # already there — do NOT attempt the ALTER
    try:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
        conn.commit()
        return True
    except psycopg2.Error as exc:
        # Roll back so the connection is not left in an aborted transaction —
        # every later statement would fail with InFailedSqlTransaction.
        conn.rollback()
        role, owner = _current_role(conn), _table_owner(conn, table)
        log.warning(
            "Cannot add %s.%s: the generator's role (%s) does not own the "
            "table (owner is %s) — on the standalone image schema.sql is "
            "applied by the postgres role while the generator connects as %s, "
            "so this additive migration cannot land. The demand curve will run "
            "with pricing.default_price_elasticity for the whole catalogue and "
            "the price/elasticity columns stay absent, so "
            "mart_product_price_elasticity will find no signal. Run the ALTER "
            "as the table owner, or re-bootstrap the data dir from a current "
            "image. See elasticity.seed_elasticity_columns. (%s)",
            table, column, role, owner, role, exc.__class__.__name__,
        )
        return False


def _current_role(conn) -> str:
    """The role the generator is connected as, for the log message."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user")
            return str(cur.fetchone()[0])
    except Exception:                                   # noqa: BLE001
        return "<unknown>"


def _table_owner(conn, table: str) -> str:
    """Who actually owns `table`, for the log message.

    The same two lookups `models/customers.py` already does — the point of
    naming both roles is that the operator's next move depends on which of
    them is which.
    """
    schema, name = table.split('.', 1)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pg_get_userbyid(c.relowner)
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
            """, (schema, name))
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else "<unknown>"
    except Exception:                                   # noqa: BLE001
        return "<unknown>"
