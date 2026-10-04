"""
Customer / household master dimension behind the loyalty programme.

Before this module the only customer identity in the feed was
`pos.loyalty_members` — a *card*, not a person. `pos.transactions.member_id`
resolves to it or is NULL, and nothing on either side carries a demographic or
household attribute, so the marts a grocery warehouse actually wants (RFM
cohorts, basket affinity, segment penetration, household-size vs basket size)
had no conformed dimension to join to.

`pos.customers` is that dimension. Its shape follows the same rule the rest of
this codebase uses for derived state: **store the drawn attributes, compute the
aggregates.** Age band, household size and segment are drawn once per household
and never change; loyalty-card count and first-signup date are a `LEFT JOIN`
away and are therefore *not* columns, so they cannot go stale when a second
card is added to a household or a back-dated signup lands out of order.

Design notes worth knowing before changing anything here:

* **One household, many cards.** `pos.loyalty_members.customer_id` is nullable
  and *not* unique, so a household can hold several loyalty cards (two adults,
  one card each) while a card-less shopper belongs to no household at all.
  `household_size` is the household's size, so it is legitimately larger than
  the number of cards: the children and students in it never signed up.
* **The link runs through the card, not the transaction.** Adding
  `customer_id` to `pos.transactions` would denormalise a snowflake into a fat
  table and re-attribute every anonymous shopper — a behavioural change well
  beyond this card. The mart joins `transactions -> loyalty_members ->
  customers`, which also keeps `pos.customers` the same size as the member
  table it describes.
* **Attributes are internally consistent, not independently drawn.** Every
  segment carries its own age-band and household-size distribution (`SEGMENTS`
  below), so a `family_stock_up` household skews 35-44 and 4-6 people. Three
  independent draws would give every segment the same behaviour and the
  dimension would be decorative — the same defect as sampling a price and an
  elasticity independently.
* **Draws are deterministic per household.** The RNG is seeded from the
  household's first `member_id`, so a restart or a re-seed reproduces the same
  demographic profile instead of re-rolling the whole customer base's
  personality. Same idiom as `elasticity.seed_elasticity_columns`.
"""
import logging
import random
import uuid
from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------
# Kept byte-identical (modulo `IF NOT EXISTS`) to the block in `schema.sql`;
# `tests/test_customers_dimension.py::test_ddl_matches_schema_sql` enforces that.
# The `IF NOT EXISTS` copy is what runs on an EXISTING data dir: a schema.sql
# change only reaches a fresh bootstrap, so without this an install generated
# before this card has no `pos.customers` at all and the backfill would have
# nowhere to write. Same trap as `elasticity.seed_elasticity_columns`,
# `models/weather.ensure_table` and `base/api/main.py::_has_stockout_tables`.
DDL = """
CREATE TABLE IF NOT EXISTS pos.customers (
    customer_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    age_band        VARCHAR(20)  NOT NULL
                        CHECK (age_band IN ('under_25', '25_34', '35_44',
                                            '45_54', '55_64', '65_plus')),
    household_size  SMALLINT     NOT NULL CHECK (household_size BETWEEN 1 AND 12),
    segment         VARCHAR(30)  NOT NULL
                        CHECK (segment IN ('value_seeker', 'family_stock_up',
                                           'convenience', 'health_conscious',
                                           'premium_enthusiast', 'budget_constrained')),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pos_customers_segment
    ON pos.customers (segment, age_band);
"""

# The nullable FK onto the customer. Added separately from `DDL` because it
# lives on `pos.loyalty_members`, and an existing data dir has that table with
# no such column — `ALTER TABLE ... ADD COLUMN` is the only way in.
CUSTOMER_ID_DDL = """
ALTER TABLE pos.loyalty_members
    ADD COLUMN IF NOT EXISTS customer_id UUID REFERENCES pos.customers(customer_id);

CREATE INDEX IF NOT EXISTS idx_pos_members_customer
    ON pos.loyalty_members (customer_id);
"""


# uuid5 namespace for customer ids. Fixed forever: changing it would re-key
# every household on the next backfill. Never edit this line.
CUSTOMER_NAMESPACE = uuid.UUID('6f9b1d2e-4c3a-5b7d-8e91-0a2b3c4d5e6f')


# ---------------------------------------------------------------------------
# The segment taxonomy
# ---------------------------------------------------------------------------
# A segment's age-band and household-size distributions are the definition of
# what that segment *is*, not tuning knobs — a `family_stock_up` household is
# not a 65_plus one-person household. They therefore live here as named data
# (the same idiom as `TIER_DIST` in `models/pos.py` and `CONDITIONS` in
# `models/weather.py`); what an operator *does* tune — how much of the base is
# in each segment, how often a household holds more than one card — is
# config-driven via `customers:` in config.yaml.
#
# Weights are stored unnormalised and normalised at draw time, so a config
# override does not have to rescale every other segment by hand.

AGE_BANDS = ('under_25', '25_34', '35_44', '45_54', '55_64', '65_plus')

# Per segment: its share of the base, the age bands it skews to, and the
# household sizes it skews to. Weights are unnormalised.
SEGMENTS: Dict[str, Dict[str, Any]] = {
    'value_seeker': {
        'share': 0.22,
        'age_bands': {'under_25': 0.12, '25_34': 0.22, '35_44': 0.26,
                      '45_54': 0.22, '55_64': 0.13, '65_plus': 0.05},
        'household_sizes': {1: 0.34, 2: 0.34, 3: 0.16, 4: 0.09, 5: 0.05, 6: 0.02},
    },
    'family_stock_up': {
        'share': 0.20,
        'age_bands': {'under_25': 0.04, '25_34': 0.24, '35_44': 0.34,
                      '45_54': 0.22, '55_64': 0.12, '65_plus': 0.04},
        'household_sizes': {1: 0.08, 2: 0.18, 3: 0.22, 4: 0.26, 5: 0.16, 6: 0.10},
    },
    'convenience': {
        'share': 0.18,
        'age_bands': {'under_25': 0.14, '25_34': 0.34, '35_44': 0.26,
                      '45_54': 0.16, '55_64': 0.08, '65_plus': 0.02},
        'household_sizes': {1: 0.46, 2: 0.32, 3: 0.12, 4: 0.06, 5: 0.03, 6: 0.01},
    },
    'health_conscious': {
        'share': 0.16,
        'age_bands': {'under_25': 0.08, '25_34': 0.22, '35_44': 0.26,
                      '45_54': 0.24, '55_64': 0.14, '65_plus': 0.06},
        'household_sizes': {1: 0.30, 2: 0.32, 3: 0.18, 4: 0.12, 5: 0.05, 6: 0.03},
    },
    'premium_enthusiast': {
        'share': 0.14,
        'age_bands': {'under_25': 0.06, '25_34': 0.30, '35_44': 0.30,
                      '45_54': 0.20, '55_64': 0.11, '65_plus': 0.03},
        'household_sizes': {1: 0.44, 2: 0.32, 3: 0.14, 4: 0.07, 5: 0.02, 6: 0.01},
    },
    'budget_constrained': {
        'share': 0.10,
        'age_bands': {'under_25': 0.10, '25_34': 0.20, '35_44': 0.24,
                      '45_54': 0.24, '55_64': 0.15, '65_plus': 0.07},
        'household_sizes': {1: 0.46, 2: 0.32, 3: 0.12, 4: 0.06, 5: 0.03, 6: 0.01},
    },
}

# A household can never hold more cards than it has people.
MAX_HOUSEHOLD_SIZE = 12


def _weighted_choices(rng, options: Dict[Any, float]) -> Any:
    """One key of `options`, chosen in proportion to its weight.

    Normalises on the fly, so the caller may pass unnormalised weights.
    """
    keys = list(options)
    weights = [float(options[k]) for k in keys]
    total = sum(weights)
    if total <= 0:
        # An operator who zeroes every weight gets the first option rather
        # than a ZeroDivisionError three thousand members later.
        return keys[0]
    return rng.choices(keys, weights=weights, k=1)[0]


def segment_shares(cfg) -> Dict[str, float]:
    """Segment mix: the module default, overridden per key by config."""
    shares = {name: float(spec['share']) for name, spec in SEGMENTS.items()}
    configured = getattr(cfg.customers, 'segment_shares', None) or {}
    for name, weight in configured.items():
        if name not in shares:
            log.warning("Ignoring unknown customer segment %r in config", name)
            continue
        shares[name] = float(weight)
    if sum(shares.values()) <= 0:
        log.warning("Customer segment_shares sum to zero — falling back to defaults")
        return {name: float(spec['share']) for name, spec in SEGMENTS.items()}
    return shares


def draw_profile(customer_id: uuid.UUID, cfg) -> Tuple[str, int, str]:
    """`(age_band, household_size, segment)` for one household.

    A pure function of the household's id and the config, so the same
    household gets the same profile on every boot and on every re-seed. A
    mart that groups by segment and finds the profile shifting between loads
    cannot build a cohort on it at all.
    """
    rng = random.Random('verisim-customer-%s' % customer_id)
    segment = _weighted_choices(rng, segment_shares(cfg))
    spec = SEGMENTS[segment]
    age_band = _weighted_choices(rng, spec['age_bands'])

    max_size = int(getattr(cfg.customers, 'household_size_max', 6) or 6)
    max_size = max(1, min(MAX_HOUSEHOLD_SIZE, max_size))
    # Renormalise the segment's size distribution over the sizes the operator
    # actually allows, so raising the cap cannot silently favour small
    # households by leaving the tail unreachable.
    sizes = {int(size): float(weight) for size, weight in spec['household_sizes'].items()
             if int(size) <= max_size}
    if not sizes:
        sizes = {1: 1.0}
    return age_band, _weighted_choices(rng, sizes), segment


def household_customer_id(first_member_id: str) -> uuid.UUID:
    """A stable household id derived from the member that formed it."""
    return uuid.uuid5(CUSTOMER_NAMESPACE, str(first_member_id))


# ---------------------------------------------------------------------------
# Idempotent schema bring-up for an existing data dir
# ---------------------------------------------------------------------------

def ensure_tables(conn) -> bool:
    """Bring an existing data dir up to the customer dimension. Idempotent.

    A `schema.sql` change only reaches a *fresh* bootstrap, so an install
    generated before this card has neither `pos.customers` nor
    `loyalty_members.customer_id`. Two statements, and the ORDER matters:

    1. `CREATE TABLE IF NOT EXISTS pos.customers` (+ its index). The generator's
       role holds CREATE on the `pos` schema, so this always works — on a
       non-owner role as much as on the owner.
    2. `ALTER TABLE pos.loyalty_members ADD COLUMN IF NOT EXISTS customer_id`
       (+ its index). This one is conditional on the role being able to ALTER,
       because it is **not** a no-op when it cannot.

    Why the ALTER is guarded
    -----------------------
    On the standalone image the generator does NOT own `pos.loyalty_members`:
    `entrypoint.sh` applies `schema.sql` through
    `su -s /bin/bash postgres -c "$PSQL -f /app/generator/schema.sql"`, so
    every table is owned by `postgres`, while the generator connects as
    $POSTGRES_USER with GRANT ALL but no ownership. Verified on CT106
    2026-10-03: the generator role has SELECT/INSERT/UPDATE on the table and
    CREATE on the schema, but `ALTER TABLE pos.loyalty_members ADD COLUMN`
    fails with `InsufficientPrivilege`.

    `IF NOT EXISTS` does NOT rescue this. Measured on CT106: `ALTER TABLE ...
    ADD COLUMN IF NOT EXISTS` raises `InsufficientPrivilege` even when the
    column is already there, because Postgres checks table ownership before it
    discovers there is nothing to do. So a bare ALTER would raise — and log a
    scary warning — on *every* boot of a perfectly healthy, freshly
    bootstrapped container. Noise that fires on every boot is noise nobody
    reads, which would bury the one case that matters. Hence: probe the column
    first, and only attempt the ALTER when it is genuinely missing.

    This is a pre-existing blocker, not one this card introduces.
    `elasticity.seed_elasticity_columns` (t_08deeddf) issues exactly the same
    unguarded ALTER on every boot, and on CT106 the `pos.products.reference_price`
    column it is supposed to add is STILL absent from a data dir holding 525,704
    transactions — so that migration has not been landing there either, silently.
    This card does not crash the generator over a migration the codebase
    already cannot perform: it logs, degrades to a no-op, and leaves the
    supported fix (re-bootstrap the data dir from a current image) in the log
    message. The API degrades to an empty result rather than 500ing — see
    `base/api/main.py::_has_customers_table`.

    Returns True when `pos.customers` was created by this call.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'pos' AND table_name = 'customers'
        """)
        created = not cur.fetchone()[0]
        if created:
            cur.execute(DDL)

    # Only attempt the ALTER when the column is genuinely absent. See the
    # docstring: on a non-owner role the statement raises even as a no-op.
    if not has_customer_column(conn):
        role, owner = _current_role(conn), _table_owner(conn)
        try:
            with conn.cursor() as cur:
                cur.execute(CUSTOMER_ID_DDL)
            conn.commit()
        except psycopg2.errors.InsufficientPrivilege:
            conn.rollback()
            log.warning(
                "Cannot add pos.loyalty_members.customer_id: the generator's "
                "role (%s) does not own the table (owner is %s) — on the "
                "standalone image schema.sql is applied by the postgres role "
                "while the generator connects as %s. pos.customers was created "
                "but will stay EMPTY on this data dir, and "
                "/grocery/pos/customers serves an empty result. Re-bootstrap the "
                "database from a current image to get the dimension, or run the "
                "ALTER as the table owner. See models/customers.py::ensure_tables.",
                role, owner, role,
            )
    if created:
        log.info("Created pos.customers (this data dir predates the customer dimension).")
    return created


def _current_role(conn) -> str:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user")
            return str(cur.fetchone()[0])
    except Exception:
        return "<unknown>"


def _table_owner(conn) -> str:
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pg_get_userbyid(c.relowner)
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'pos' AND c.relname = 'loyalty_members'
            """)
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else "<unknown>"
    except Exception:
        return "<unknown>"


# ---------------------------------------------------------------------------
# The backfill — one code path for every member, whenever it appears
# ---------------------------------------------------------------------------

def has_customer_column(conn) -> bool:
    """True when `pos.loyalty_members.customer_id` exists on this data dir.

    A cheap catalog probe, called once per tick by `backfill_customers`. The
    generator runs a tick every 30 s, so this deliberately does NOT cache: a
    cache keyed on the connection would go stale the moment the column is
    added out of band (an operator fixing the privilege problem and re-running
    the migration by hand), and the dimension would stay empty until a restart
    for no reason. It is one indexed catalog lookup.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'pos' AND table_name = 'loyalty_members'
              AND column_name = 'customer_id'
        """)
        return cur.fetchone() is not None


def unlinked_members(conn) -> List[Tuple[str, Any]]:
    """`(member_id, signup_date)` for every loyalty card with no household.

    Ordered by signup date then member_id so the grouping below is stable: a
    household forms in the order its cards were issued, not in whatever order
    Postgres happened to return the rows.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT member_id::text, signup_date
            FROM pos.loyalty_members
            WHERE customer_id IS NULL
            ORDER BY signup_date, member_id
        """)
        return [(str(r[0]), r[1]) for r in cur.fetchall()]


def plan_households(members: List[Tuple[str, Any]], cfg) -> List[Dict[str, Any]]:
    """Group unlinked cards into households and draw each one's profile.

    Pure and DB-free, so the grouping rule is testable without a database —
    the property that matters (a household is coherent, every card is linked
    exactly once, ids are stable) is not something an INSERT test proves.
    """
    shares = getattr(cfg.customers, 'multi_member_household_share', 0.25)
    try:
        shares = float(shares)
    except (TypeError, ValueError):
        shares = 0.25
    shares = max(0.0, min(1.0, shares))

    households: List[Dict[str, Any]] = []
    pending: List[Tuple[str, Any]] = []
    # A household cannot have more people than the DDL's own CHECK allows, and
    # it cannot have more people than the operator's configured cap either —
    # both bound the card count, since cards <= people is the invariant the
    # size fix-up below maintains. Without this a `multi_member_household_share`
    # near 1.0 keeps a household open across a whole backfill and then tries to
    # write household_size = (however many cards signed up), which the CHECK
    # rejects and takes the generator down at boot.
    cap = max(1, min(MAX_HOUSEHOLD_SIZE,
                     int(getattr(cfg.customers, 'household_size_max', 6) or 6)))

    def _flush() -> None:
        if not pending:
            return
        # The household is keyed on its EARLIEST card, so adding a newer card
        # later never re-keys the household it belongs to.
        anchor = pending[0][0]
        customer_id = household_customer_id(anchor)
        age_band, household_size, segment = draw_profile(customer_id, cfg)
        households.append({
            'customer_id': str(customer_id),
            'age_band': age_band,
            # A household of N people can hold at most N cards. A draw that
            # came out smaller than the cards it now holds would be a
            # dimension that contradicts itself. `cap` already bounds the card
            # count at or below this, so the max() stays within every limit.
            'household_size': max(household_size, len(pending)),
            'segment': segment,
            'member_ids': [member_id for member_id, _ in pending],
        })
        pending.clear()

    for member_id, signup_date in members:
        # The decision belongs to the card ABOUT TO ARRIVE: it either joins the
        # household still open or starts a new one. Deciding after appending
        # instead would give share=0.0 the wrong meaning entirely — every
        # card would still pair up with the one before it, because "never
        # group" would only ever be evaluated once a pair had already formed.
        if pending:
            rng = random.Random('verisim-household-%s' % member_id)
            if len(pending) >= cap or rng.random() >= shares:
                _flush()
        pending.append((member_id, signup_date))
    _flush()
    return households


def backfill_customers(conn, cfg) -> Dict[str, int]:
    """Give every loyalty card without a household one. Idempotent.

    This is the ONLY path that creates a household, and it serves both
    callers, which is the point:

    * `seed_all` runs it at boot, so a member seeded before this card (or
      restored from an older data dir) is picked up;
    * the POS signup path runs it after the batch's `loyalty_members` INSERT,
      so a card created this tick is dimensioned this tick instead of waiting
      for the next restart.

    Both are safe because the candidate set is "cards with a NULL
    `customer_id`" and the write sets it, so a second call finds nothing to do.

    Degrades to a no-op on a data dir where `ensure_tables` could not add the
    column (see the ownership note there). A missing column must not take the
    generator's main loop down with it — the generator is the thing serving
    every other table, and losing the POS feed to protect a dimension that is
    merely absent would be a poor trade.
    """
    if not has_customer_column(conn):
        return {'created': 0, 'linked': 0}

    members = unlinked_members(conn)
    if not members:
        return {'created': 0, 'linked': 0}

    households = plan_households(members, cfg)
    customer_records = [
        (h['customer_id'], h['age_band'], h['household_size'], h['segment'])
        for h in households
    ]
    # (member_id, customer_id) for the link-back.
    link_records = [
        (member_id, h['customer_id'])
        for h in households for member_id in h['member_ids']
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO pos.customers
                (customer_id, age_band, household_size, segment)
            VALUES %s
            ON CONFLICT (customer_id) DO NOTHING
        """, customer_records, template="(%s::uuid,%s,%s,%s)")

        psycopg2.extras.execute_values(cur, """
            UPDATE pos.loyalty_members m
               SET customer_id = v.customer_id::uuid,
                   updated_at = NOW()
              FROM (VALUES %s) AS v(member_id, customer_id)
             WHERE m.member_id = v.member_id::uuid
        """, link_records, template="(%s::uuid,%s)")
    conn.commit()

    log.info("Customer dimension: %d households for %d loyalty cards "
             "(%d multi-card)", len(households), len(link_records),
             sum(1 for h in households if len(h['member_ids']) > 1))
    return {'created': len(households), 'linked': len(link_records)}


def fetch_customers(conn) -> List[Dict[str, Any]]:
    """The dimension as the API serves it, for anything the generator needs."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT c.customer_id::text AS customer_id,
                   c.age_band, c.household_size, c.segment,
                   COUNT(lm.member_id) AS loyalty_member_count,
                   MIN(lm.signup_date) AS first_signup_date
            FROM pos.customers c
            LEFT JOIN pos.loyalty_members lm ON lm.customer_id = c.customer_id
            GROUP BY c.customer_id, c.age_band, c.household_size, c.segment
        """)
        return [dict(r) for r in cur.fetchall()]


def segment_of_member(conn, member_id: str) -> Optional[str]:
    """The segment behind one loyalty card — the mart join, in one query."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.segment
            FROM pos.loyalty_members lm
            JOIN pos.customers c ON c.customer_id = lm.customer_id
            WHERE lm.member_id = %s::uuid
        """, (member_id,))
        row = cur.fetchone()
        return row[0] if row else None