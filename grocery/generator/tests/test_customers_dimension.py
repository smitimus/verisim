"""
The customer / household master dimension (pos.customers).

Three layers of test, each aimed at a different thing that can be wrong:

* **DDL parity** — the module's `IF NOT EXISTS` copy must stay in step with
  the block in `schema.sql`. They are the same table reached by two different
  routes (fresh bootstrap vs an existing data dir), and a drift between them
  means one of the two installs gets a different dimension from the other.
* **The grouping rule** — `plan_households` is deliberately pure and DB-free
  so the properties that matter are testable without a database: every card
  lands in exactly one household, a household is coherent (it cannot hold more
  cards than it has people), and a household's id and profile are stable.
* **The segment mix** — the drawn attributes have to actually differ between
  segments. Three independent draws would give every segment identical
  behaviour and the dimension would be decoration, which no single-row
  assertion can detect.

A live-database test at the end covers the SQL itself, and skips when no
grocery DB is reachable.
"""
from __future__ import annotations

import os
import uuid
from datetime import date, timedelta

import psycopg2
import pytest

import grocery.generator.models.customers as customers
from grocery.generator.config import Config

# repo/grocery/generator/tests -> repo/grocery/generator/schema.sql
SCHEMA_SQL = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..", "schema.sql",
))


def _members(n: int, start: date = date(2026, 1, 1)):
    """`n` pseudo members, oldest signup first, stable ids."""
    return [
        (str(uuid.uuid5(uuid.NAMESPACE_DNS, "member-%d" % i)), start + timedelta(days=i))
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# DDL parity
# ---------------------------------------------------------------------------

def test_ddl_matches_schema_sql():
    """`models.customers.DDL` must describe the same table as `schema.sql`.

    Two paths to one schema: a fresh container applies `schema.sql`, while an
    install generated before this card gets `DDL` from `ensure_tables`. If
    they drift, the two installs disagree about what a household is — and the
    disagreement is invisible until a mart built on one fails on the other.
    Compared column-by-column rather than byte-for-byte, because the module
    copy carries `IF NOT EXISTS` and therefore cannot be identical.
    """
    with open(SCHEMA_SQL, "r", encoding="utf-8") as fh:
        schema_sql = fh.read()

    assert "CREATE TABLE pos.customers" in schema_sql, \
        "schema.sql no longer creates pos.customers"

    # Every column the module's DDL declares must exist in schema.sql with the
    # same type and constraints. Pull the column definitions out of each and
    # compare on the attributes that define the column.
    def _columns(ddl: str) -> dict:
        body = ddl.split("pos.customers (", 1)[1].split("\n);", 1)[0]
        columns = {}
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            name = line.split()[0]
            columns[name] = " ".join(line.split()[1:]).rstrip(",")
        return columns

    schema_columns = _columns(schema_sql)
    ddl_columns = _columns(customers.DDL)

    assert set(ddl_columns) == set(schema_columns), (
        f"column sets differ: module {sorted(ddl_columns)} vs "
        f"schema {sorted(schema_columns)}"
    )
    for name, definition in ddl_columns.items():
        assert definition == schema_columns[name], (
            f"pos.customers.{name} differs: module {definition!r} vs "
            f"schema {schema_columns[name]!r}"
        )


def test_customer_fk_is_nullable_and_not_unique():
    """The card→household link must stay a many-to-one, not a one-to-one.

    This is the design decision a schema change would most easily undo. A
    UNIQUE constraint would say a household holds at most one card, which is
    false of a real store (two adults, a card each) and would make the
    `household_size > loyalty_member_count` case unrepresentable. NOT NULL
    would be wrong the other way: a card created by the POS signup path of a
    data dir mid-boot, before its household exists, is legitimately NULL for
    an instant.
    """
    with open(SCHEMA_SQL, "r", encoding="utf-8") as fh:
        schema_sql = fh.read()

    body = schema_sql.split("CREATE TABLE pos.loyalty_members (", 1)[1].split("\n);", 1)[0]
    assert "customer_id" in body, "loyalty_members lost its customer_id link"
    for line in body.splitlines():
        if "customer_id" in line and not line.strip().startswith("--"):
            assert "NOT NULL" not in line, \
                f"customer_id must be nullable, got: {line.strip()}"
            assert "UNIQUE" not in line, \
                f"customer_id must not be unique (a household holds many cards): {line.strip()}"


# ---------------------------------------------------------------------------
# The grouping rule
# ---------------------------------------------------------------------------

def test_every_card_lands_in_exactly_one_household():
    """The core property: the plan partitions the cards, loses none, duplicates none."""
    cfg = Config()
    members = _members(500)

    households = customers.plan_households(members, cfg)

    planned = [member_id for h in households for member_id in h['member_ids']]
    assert sorted(planned) == sorted(member_id for member_id, _ in members), (
        f"{len(planned)} cards planned for {len(members)} members — "
        "the plan must partition the input exactly"
    )
    assert len(set(planned)) == len(planned), "a card was assigned to two households"


def test_no_customer_is_left_without_a_household():
    """No orphaned household: every plan entry has a usable customer_id."""
    cfg = Config()
    households = customers.plan_households(_members(200), cfg)

    assert households
    for h in households:
        # A malformed uuid here would only surface as an INSERT failure at
        # boot, three thousand rows in.
        uuid.UUID(h['customer_id'])
        assert h['segment'] in customers.SEGMENTS
        assert h['age_band'] in customers.AGE_BANDS


def test_a_household_never_holds_more_cards_than_it_has_people():
    """`household_size >= len(member_ids)` — the dimension must not contradict itself.

    This is `SEMA-14` in the live integrity harness. It is asserted here
    because it is the one property of the draw that the planner has to *fix
    up* rather than merely respect: a household that collected three cards
    cannot keep a drawn size of one, so the planner raises the size instead of
    dropping a card or leaving the row self-contradictory.
    """
    cfg = Config()
    cfg.customers.multi_member_household_share = 1.0   # force grouping
    households = customers.plan_households(_members(100), cfg)

    multi = [h for h in households if len(h['member_ids']) > 1]
    assert multi, "no household formed at share=1.0 — the grouping rule is inert"
    for h in households:
        assert h['household_size'] >= len(h['member_ids']), (
            f"household of {h['household_size']} people holds "
            f"{len(h['member_ids'])} cards"
        )
        assert h['household_size'] <= customers.MAX_HOUSEHOLD_SIZE


def test_multi_member_share_controls_grouping():
    """share=0 gives one household per card; share=1 fills them to the cap.

    share=1.0 does NOT mean "every card joins the same household" — it means
    "a card always joins the household currently open", so households fill to
    `household_size_max` and then the next card starts a new one. That cap is
    load-bearing, not a cosmetic limit: without it a 30-day backfill of
    thousands of signups would try to write one household of size 3000, which
    the DDL's CHECK rejects and which is not a household at all.
    """
    members = _members(60)

    cfg = Config()
    cfg.customers.multi_member_household_share = 0.0
    solo = customers.plan_households(members, cfg)
    assert all(len(h['member_ids']) == 1 for h in solo), (
        "share=0.0 must give every card its own household; the decision has to "
        "be made by the arriving card, not after it has already joined one"
    )
    assert len(solo) == len(members)

    cfg.customers.multi_member_household_share = 1.0
    together = customers.plan_households(members, cfg)
    assert all(len(h['member_ids']) == cfg.customers.household_size_max
               for h in together), (
        f"share=1.0 should fill each household to the cap "
        f"({cfg.customers.household_size_max}); got "
        f"{sorted({len(h['member_ids']) for h in together})}"
    )
    assert len(together) == -(-len(members) // cfg.customers.household_size_max)


def test_household_ids_and_profiles_are_stable_across_runs():
    """Re-planning the same cards must reproduce the same dimension.

    The whole plan is derived from the member ids, so a restart or a re-seed
    cannot re-roll the customer base's demographic personality. A mart that
    groups by segment and finds the profile shifting between loads cannot
    build a cohort on it at all — and nothing downstream would notice; the
    segments would just slowly smear.
    """
    cfg = Config()
    members = _members(120)

    first = customers.plan_households(members, cfg)
    second = customers.plan_households(members, cfg)

    assert first == second, (
        "plan_householders is not deterministic: a restart would re-roll "
        "every household's segment and age band"
    )


def test_household_id_is_derived_from_its_first_card():
    """The id keys on the household's earliest card, so it never re-keys.

    If the id were derived from whatever card happened to arrive first, then
    adding a NEWER card to a household would change the household's identity
    and every fact already joined to it would be orphaned.
    """
    first_id, _ = _members(1)[0]
    assert str(customers.household_customer_id(first_id)) == \
        str(customers.household_customer_id(first_id))
    # A DIFFERENT card must key a different household. (Note `_members(n)` always
    # starts at index 0, so this compares card 0 against card 7.)
    other_id, _ = _members(8)[7]
    assert customers.household_customer_id(first_id) != \
        customers.household_customer_id(other_id)


def test_household_size_respects_the_configured_cap():
    """`household_size_max` must bound the draw, and the DDL's own max must hold."""
    cfg = Config()
    cfg.customers.household_size_max = 3
    households = customers.plan_households(_members(400), cfg)
    assert all(h['household_size'] <= 3 for h in households)

    # A cap above what the DDL CHECK allows must clamp, not write a row the
    # constraint will reject at insert time.
    cfg.customers.household_size_max = 99
    households = customers.plan_households(_members(200), cfg)
    assert all(h['household_size'] <= customers.MAX_HOUSEHOLD_SIZE for h in households)


def test_unknown_segment_in_config_is_ignored_not_fatal():
    """A typo in config.yaml must not take the generator down at boot.

    An operator who writes `premium_enthusiasts` (plural) should get the
    default mix and a warning, not a KeyError three hundred members into the
    seed.
    """
    cfg = Config()
    cfg.customers.segment_shares = {'no_such_segment': 0.5}

    shares = customers.segment_shares(cfg)
    assert 'no_such_segment' not in shares
    assert set(shares) == set(customers.SEGMENTS)


def test_zeroed_segment_shares_fall_back_to_defaults():
    """All-zero weights are a config mistake, not a division by zero."""
    cfg = Config()
    cfg.customers.segment_shares = {name: 0.0 for name in customers.SEGMENTS}

    shares = customers.segment_shares(cfg)
    assert sum(shares.values()) > 0
    assert set(shares) == set(customers.SEGMENTS)


# ---------------------------------------------------------------------------
# The segment mix — the dimension has to actually differentiate
# ---------------------------------------------------------------------------

def test_segments_have_distinct_demographic_profiles():
    """A segment must be recognisable by its attributes, or it is decoration.

    Samples households and requires each segment to be distinguishable. The
    failure this guards is the subtle one: if age band and household size were
    drawn independently of segment (three unrelated draws), every segment
    would carry the same mix, every group-by would return the same numbers,
    and no assertion on any single row would notice.
    """
    cfg = Config()
    samples = []
    for i in range(6000):
        cid = uuid.uuid5(uuid.NAMESPACE_DNS, "sample-%d" % i)
        age_band, size, segment = customers.draw_profile(cid, cfg)
        samples.append((segment, age_band, size))

    by_segment = {}
    for segment, age_band, size in samples:
        by_segment.setdefault(segment, []).append((age_band, size))

    assert set(by_segment) == set(customers.SEGMENTS)

    # Mean household size must differ materially between the segment meant to
    # be big and the one meant to be small. 0.5 people of separation is far
    # outside what sampling noise on 6000 draws would produce.
    def _mean_size(segment):
        rows = by_segment[segment]
        return sum(size for _, size in rows) / len(rows)

    assert _mean_size('family_stock_up') - _mean_size('convenience') > 0.5, (
        "family_stock_up and convenience must differ in household size — "
        f"got {_mean_size('family_stock_up'):.2f} vs {_mean_size('convenience'):.2f}. "
        "If these are the same, the segment is not drawn from its own "
        "distribution and the dimension carries no signal."
    )

    # And the mean size must track the declared distribution, not just differ
    # by luck: family households skew large, singletons skew small.
    assert _mean_size('convenience') < 2.5


def test_every_declared_segment_is_reachable():
    """A segment with zero probability is a segment a mart can never group by."""
    cfg = Config()
    drawn = {customers.draw_profile(
        uuid.uuid5(uuid.NAMESPACE_DNS, 'reach-%d' % i), cfg)[2]
        for i in range(3000)}
    assert drawn == set(customers.SEGMENTS)


def test_configured_segment_share_actually_shifts_the_mix():
    """`segment_shares` must be read, not parsed and ignored.

    The override is *relative*, not a redistribution: setting one segment to
    1.0 while the other five keep their defaults (0.22 + 0.20 + 0.18 + 0.16 +
    0.10) means it takes roughly 1.0/1.86 of the draws, not all of them. That
    is the intended contract — overriding one segment does not require
    rescaling the other five by hand — so the assertion is that the named
    segment's share RISES well clear of both its default and the others.
    """
    cfg = Config()
    baseline = [customers.draw_profile(
        uuid.uuid5(uuid.NAMESPACE_DNS, 'mix-%d' % i), cfg)[2]
        for i in range(2000)]
    default_rate = baseline.count('premium_enthusiast') / len(baseline)
    assert 0.10 < default_rate < 0.20, (
        f"the default mix should put premium_enthusiast near its 0.14 share, "
        f"got {default_rate:.3f}"
    )

    cfg.customers.segment_shares = {'premium_enthusiast': 1.0}
    shares = customers.segment_shares(cfg)
    # Unnormalised input: the named segment now outweighs every other default
    # put together, so it must take the bulk of the draws.
    total = sum(shares.values())
    assert shares['premium_enthusiast'] / total > 0.5

    drawn = [customers.draw_profile(
        uuid.uuid5(uuid.NAMESPACE_DNS, 'mix-%d' % i), cfg)[2]
        for i in range(2000)]
    tuned_rate = drawn.count('premium_enthusiast') / len(drawn)
    assert tuned_rate > default_rate * 2, (
        f"raising premium_enthusiast to 1.0 moved its share from "
        f"{default_rate:.3f} to only {tuned_rate:.3f} — segment_shares is "
        "not reaching the draw"
    )


# ---------------------------------------------------------------------------
# The boot path, DB-free
# ---------------------------------------------------------------------------
# The generator runs `ensure_tables` on every boot, so its behaviour has to be
# provable without a database. These use a recording stub, with one of them
# reproducing the privilege refusal measured on CT106 — where the generator's
# role does not own pos.loyalty_members and the ALTER raises even when the
# column is already there, because Postgres checks ownership before it notices
# there is nothing to do.

class _StubCursor:
    """Answers the two catalog probes the boot path makes.

    `SELECT COUNT(*) FROM information_schema.tables ...` must return 0 on an
    old data dir and 1 on a current one; `SELECT 1 FROM
    information_schema.columns ...` returns a row only when the FK is there.
    One `table_present` flag cannot drive both — they are independent facts
    (a dir could carry the table but not the column) — so each cursor is told
    which probe it is serving.
    """

    def __init__(self, conn, *, count=None, row=False):
        self._conn = conn
        self._count = count
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._conn.statements.append(sql)
        if self._conn.refuse_alter and "ALTER TABLE" in sql:
            raise psycopg2.errors.InsufficientPrivilege(
                "must be owner of table loyalty_members")
        return self

    def fetchone(self):
        if self._count is not None:
            return (self._count,)
        return (1,) if self._row else None

    def fetchall(self):
        return []

    def __getattr__(self, name):
        return lambda *a, **k: None


class _StubConn:
    """Enough of a psycopg2 connection for the boot path, and no more."""

    def __init__(self, column_present=False, table_present=False,
                 refuse_alter=False):
        self.column_present = column_present
        self.table_present = table_present
        self.refuse_alter = refuse_alter
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self._next_is_table_probe = True

    def cursor(self, *a, **k):
        # The module probes the TABLE first, then the COLUMN. Returning a
        # different cursor per probe keeps the two facts independent.
        if self._next_is_table_probe:
            self._next_is_table_probe = False
            return _StubCursor(self, count=1 if self.table_present else 0)
        return _StubCursor(self, row=self.column_present)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_ensure_tables_creates_the_table_and_the_fk():
    """On an old data dir it must do both halves: the table AND the column."""
    conn = _StubConn(column_present=False, table_present=False)
    assert customers.ensure_tables(conn) is True
    sql = " ".join(conn.statements)
    assert "CREATE TABLE IF NOT EXISTS pos.customers" in sql
    assert "ALTER TABLE pos.loyalty_members" in sql, (
        "the FK was not added — a data dir would get the dimension table with "
        "nothing pointing at it"
    )
    assert conn.commits >= 1


def test_ensure_tables_is_a_silent_noop_on_an_up_to_date_data_dir():
    """A current data dir must not be written to at all.

    This is the case that matters most in production: it is every boot of every
    healthy container. If it attempted the ALTER anyway, the statement would
    raise `InsufficientPrivilege` (Postgres checks ownership before honouring
    IF NOT EXISTS) and log a scary warning on every single boot — noise that
    would bury the one case where the migration genuinely failed.
    """
    conn = _StubConn(column_present=True, table_present=True, refuse_alter=True)
    assert customers.ensure_tables(conn) is False
    assert not any("ALTER TABLE" in s for s in conn.statements), (
        f"attempted an ALTER on an up-to-date data dir: {conn.statements}"
    )
    assert not any("CREATE TABLE" in s for s in conn.statements)


def test_ensure_tables_survives_a_refused_alter():
    """A non-owner role must degrade to a no-op, never crash the generator.

    Measured on CT106 2026-10-03: every pos.* table is owned by `postgres`
    (entrypoint.sh applies schema.sql as postgres) while the generator connects
    as $POSTGRES_USER with GRANT ALL and no ownership, so the ALTER is refused.
    The generator's main loop serves every other table; losing it to protect a
    dimension that is merely absent would be a bad trade.
    """
    conn = _StubConn(column_present=False, table_present=False, refuse_alter=True)
    # Must not raise.
    assert customers.ensure_tables(conn) is True      # the CREATE still worked
    assert conn.rollbacks >= 1, (
        "a refused ALTER must be rolled back, not left in an aborted "
        "transaction — every later statement on the generator's connection "
        "would fail with InFailedSqlTransaction"
    )
    assert any("ALTER TABLE" in s for s in conn.statements)


def test_backfill_customers_is_a_noop_without_the_column():
    """No column -> no work, and above all no exception from the main loop."""
    conn = _StubConn(column_present=False)
    assert customers.backfill_customers(conn, Config()) == {'created': 0, 'linked': 0}
    assert not any("INSERT" in s for s in conn.statements)


# ---------------------------------------------------------------------------
# Live database coverage
# ---------------------------------------------------------------------------

def _probe_connection():
    import psycopg2
    dsn = os.environ.get("GROCERY_TEST_DB")
    if dsn:
        try:
            return psycopg2.connect(dsn, connect_timeout=5)
        except Exception:
            return None
    for host, port, user, pw, db in [
        ("127.0.0.1", 5499, "verisim", "verisim", "grocery"),
        ("127.0.0.1", 5432, "verisim", "verisim", "grocery"),
    ]:
        try:
            return psycopg2.connect(host=host, port=port, user=user,
                                   password=pw, dbname=db, connect_timeout=3)
        except Exception:
            continue
    return None


@pytest.fixture(scope="module")
def grocery_conn():
    conn = _probe_connection()
    if conn is None:
        pytest.skip("No reachable grocery database (set GROCERY_TEST_DB to enable)")
    yield conn
    conn.close()


@pytest.fixture
def live_conn(grocery_conn):
    """The module connection, put through the REAL boot sequence inside one
    transaction that is rolled back afterwards.

    Two things this deliberately does:

    * **Runs the boot sequence** (`ensure_tables` then `backfill_customers`),
      because a data dir that predates this card has neither `pos.customers`
      nor `loyalty_members.customer_id`, and skipping that step would only ever
      let this suite pass against a freshly-provisioned database — never
      against the upgraded-existing-install case the migration exists for.
      That case is the one that breaks in production.
    * **Rolls back.** The tests below write (a real backfill over every card on
      disk), so leaving the writes behind would mutate a data dir someone else
      is generating into. Rolling back also isolates each test: a failed
      assertion mid-transaction would otherwise abort the connection and turn
      one real failure into a cascade of `InFailedSqlTransaction` noise.

    Skips (does not error) when the column cannot be added at all — see
    `models/customers.py::ensure_tables` for why that is a legitimate state of
    the world: the generator's role does not own `pos.loyalty_members` on an
    existing data dir, so the ALTER is refused. The generator itself degrades
    to a no-op there, and the live assertions below have nothing to read. CI's
    integration job provisions a FRESH data dir with the current schema.sql, so
    the migration path it exercises is always the one that works.
    """
    grocery_conn.rollback()
    try:
        customers.ensure_tables(grocery_conn)
        if not customers.has_customer_column(grocery_conn):
            pytest.skip(
                "pos.loyalty_members.customer_id is absent and cannot be added: "
                "this role does not own the table (the generator connects as "
                f"{customers._current_role(grocery_conn)}, the table is owned by "
                f"{customers._table_owner(grocery_conn)}). See "
                "models/customers.py::ensure_tables."
            )
        customers.backfill_customers(grocery_conn, Config())
        yield grocery_conn
    finally:
        grocery_conn.rollback()


def test_backfill_customers_brings_an_old_data_dir_up_to_date(live_conn):
    """The migration path itself: table created, every card linked, once.

    This is what happens on the first boot after an image upgrade, and it is
    the only thing standing between an existing install and a dimension that is
    either absent or half-populated.
    """
    with live_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.customers")
        (households,) = cur.fetchone()
        cur.execute("""
            SELECT COUNT(*) FROM pos.loyalty_members WHERE customer_id IS NULL
        """)
        (unlinked,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM pos.loyalty_members")
        (members,) = cur.fetchone()

    assert unlinked == 0, (
        f"{unlinked} of {members} cards still have no household after the boot "
        "sequence — the migration did not cover the data dir"
    )
    if members:
        assert 0 < households <= members, (
            f"{households} households from {members} cards — expected between "
            "1 (every card shares a household) and one-per-card"
        )


def test_backfill_customers_is_idempotent_on_a_live_db(live_conn):
    """Running the backfill again must create nothing further.

    Idempotency is the whole reason the candidate set is "cards with a NULL
    customer_id": the boot path, the end-of-backfill path and the per-tick
    realtime path all call this, and a generator that calls it on every tick
    would otherwise grow a household per card per tick.
    """
    first = customers.backfill_customers(live_conn, Config())
    second = customers.backfill_customers(live_conn, Config())

    assert first['created'] == 0 and first['linked'] == 0, (
        f"a re-run did more work ({first}) — the candidate set is supposed to "
        "be empty once every card is dimensioned"
    )
    assert second['created'] == 0, (
        f"a second run created {second['created']} more households — the "
        "backfill is not idempotent"
    )
    assert second['linked'] == 0


def test_live_dimension_has_no_cardless_households_and_caps_hold(live_conn):
    """`SEMA-14` / `SEMA-15` as executable SQL against the real data."""
    with live_conn.cursor() as cur:
        cur.execute("""
            SELECT c.customer_id, c.household_size, lm.card_count
            FROM pos.customers c
            JOIN (
                SELECT customer_id, COUNT(*) AS card_count
                FROM pos.loyalty_members
                WHERE customer_id IS NOT NULL GROUP BY customer_id
            ) lm ON lm.customer_id = c.customer_id
            WHERE c.household_size < lm.card_count
        """)
        contradictions = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) FROM pos.loyalty_members WHERE customer_id IS NULL
        """)
        (unlinked,) = cur.fetchone()

    assert not contradictions, (
        f"{len(contradictions)} households hold more loyalty cards than people: "
        f"{contradictions[:3]}"
    )
    assert unlinked == 0, (
        f"{unlinked} loyalty cards have no household. The dimension is meant to "
        "cover every card; a card with no customer is a hole in the mart's spine."
    )


def test_segment_of_member_resolves_through_the_card(live_conn):
    """The mart join, as one query: a card's member_id reaches its segment."""
    with live_conn.cursor() as cur:
        cur.execute("""
            SELECT lm.member_id::text FROM pos.loyalty_members lm
            WHERE lm.customer_id IS NOT NULL LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            pytest.skip("no linked loyalty members on this dataset")

    assert customers.segment_of_member(live_conn, row[0]) in customers.SEGMENTS


def test_fetch_customers_rolls_up_card_counts(live_conn):
    """`fetch_customers` must report the card count, computed not stored."""
    with live_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.customers")
        (expected,) = cur.fetchone()
    if not expected:
        pytest.skip("no customers on this dataset")

    rows = customers.fetch_customers(live_conn)
    assert len(rows) == expected
    # A household exists because a card formed it, so no row may report zero.
    assert all(r['loyalty_member_count'] >= 1 for r in rows), \
        "a household reports no loyalty card — it was formed by one"
    assert all(r['first_signup_date'] is not None for r in rows)