"""
The shelf-price walk: per-SKU frequency, and a price that stays anchored (t_e58b4d3a).

THE DEFECT
==========
`pos.products.current_price` ran away from its own seed at ~+11 %/day, so it and
`reference_price` (the demand curve's pivot) looked like two unrelated worlds.
Measured on the source DBs, 2026-11-16, ratio `current_price / reference_price`
over the whole 495-SKU catalogue:

    slot    catalogue seeded   at reference   median   max
    test/107  2026-10-25        0             7.27x    60.52x
    dev/106   2026-11-15        3             1.08x     1.66x

`maybe_update_product_prices` had three compounding faults:

1. `ticks_per_day = (24 * 60) / 15` — 96, a 15-minute cadence nothing ran at.
   The realtime loop ticks every 30 s (2880/day) and the backfill wrote ONE call
   per simulated day, so the same simulated day got a different price history
   depending on which writer produced it.
2. The event probability was scaled by `len(products)` (`prob * 495`), and every
   event then changed a fixed FIVE SKUs — so the published
   `product_price_change_frequency_days` governed nothing: 1.84 SKU-updates per
   tick instead of the 0.012 the config implies.
3. The step was `uniform(-0.06, +0.08)` — a +1 % mean per change — with nothing
   tying the price to `reference_price`. A pure multiplicative walk with positive
   drift compounds: 2880 ticks x 0.368 x 5 SKUs = 5,300 updates/day = 10.7
   updates/SKU/day x +1 % ~= +11 %/day, i.e. 9x in 22 days.

WHAT THESE TESTS PIN
====================
1. The expected number of changes per SKU per DAY is the same whether the day is
   written 2880 times (realtime) or 24 times (a backfill), and equals
   `1 / product_price_change_frequency_days`. The Monte-Carlo test is the one
   that would catch a re-hardcoded constant.
2. A 30-day walk keeps `median(current_price / reference_price)` at 1.00 and the
   spread inside the band the docstring promises (~/-10 % at 95 %), on the
   backfill path and the realtime path alike.
3. `p['price']` and `p['current_price']` — two keys for one column — move
   together, so an ad created mid-pass is not priced off the pass-start shelf.
4. A weekly ad is priced off `reference_price`, so a retro-materialised ad cannot
   advertise a price the shelf never had.
"""
import random
import statistics
from pathlib import Path

import pytest

import grocery.generator.models.pos as pos
import grocery.generator.models.promotions as promotions
from grocery.generator.config import Config

CATALOGUE = 495          # the measured catalogue size on test/107
FREQUENCY = 14.0         # pricing.product_price_change_frequency_days
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400
REALTIME_TICK_SECONDS = 30


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class _RecordingCursor:
    def __init__(self, conn):
        self.conn = conn
        # `psycopg2.extras.execute_values` reads `cur.connection.encoding`.
        self.connection = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        text = sql.decode() if isinstance(sql, (bytes, bytearray)) else sql
        # `ensure_current_ad` reads the new ad's id back from INSERT ... RETURNING.
        if 'RETURNING ad_id' in text:
            self.conn.next_fetchone = ('ad-fake',)

    def mogrify(self, template, args):
        # `execute_values` inlines the rows through `mogrify` and then calls
        # `execute` with the assembled statement, so the row tuples are only
        # visible here.
        self.conn.mogrified.append(args)
        return b'(row)'

    def fetchone(self):
        return self.conn.next_fetchone

    def fetchall(self):
        return self.conn.next_fetchall


class FakeConn:
    """A psycopg2-shaped connection that records statements."""

    def __init__(self):
        self.executed = []
        self.mogrified = []
        self.commits = 0
        self.next_fetchone = None
        self.next_fetchall = []
        self.encoding = 'UTF8'

    def cursor(self, *a, **k):
        return _RecordingCursor(self)

    def commit(self):
        self.commits += 1

    def statements_containing(self, needle):
        return [(sql, params) for sql, params in self.executed if needle in sql]


def _catalogue(n=CATALOGUE, price=4.00, reference=None):
    reference = price if reference is None else reference
    return [
        {'product_id': f'p{i:04d}', 'price': price,
         'current_price': price, 'reference_price': reference}
        for i in range(n)
    ]


def _walk(products, cfg, days, tick_seconds, seed):
    """Drive the real pricing law for `days`, returning per-SKU change counts."""
    random.seed(seed)
    counts = {p['product_id']: 0 for p in products}
    ticks = int(days * SECONDS_PER_DAY // tick_seconds)
    for _ in range(ticks):
        expected = pos.expected_price_changes(len(products), cfg, tick_seconds)
        for p in random.sample(products, min(len(products), pos.draw_change_count(expected))):
            p['price'] = pos.next_shelf_price(p['price'], p['reference_price'], cfg)
            p['current_price'] = p['price']
            counts[p['product_id']] += 1
    return counts


def _ratios(products):
    return sorted(p['price'] / p['reference_price'] for p in products)


# ---------------------------------------------------------------------------
# 1. The frequency contract, on both paths
# ---------------------------------------------------------------------------

def test_expected_changes_per_day_is_one_change_per_frequency_days():
    cfg = Config()
    per_day = pos.expected_price_changes(CATALOGUE, cfg, SECONDS_PER_DAY)
    assert per_day == pytest.approx(CATALOGUE / FREQUENCY)


def test_realtime_and_backfill_owe_the_same_changes_for_one_simulated_day():
    """The whole defect in one assertion: the two writers must agree.

    2880 realtime ticks of 30 s and 24 backfill hours of 3600 s cover the same
    simulated day, so they owe the same expected number of SKU updates. The old
    code hardcoded 96 ticks/day, which made realtime over-fire 30x and the
    backfill under-fire 4x.
    """
    cfg = Config()
    realtime = sum(pos.expected_price_changes(CATALOGUE, cfg, REALTIME_TICK_SECONDS)
                   for _ in range(SECONDS_PER_DAY // REALTIME_TICK_SECONDS))
    backfill = sum(pos.expected_price_changes(CATALOGUE, cfg, SECONDS_PER_HOUR)
                   for _ in range(24))
    assert realtime == pytest.approx(backfill)
    assert realtime == pytest.approx(CATALOGUE / FREQUENCY)


def test_a_drawn_number_of_skus_changes_not_a_fixed_five():
    """The count is drawn, and usually far below the catalogue size.

    A fixed five per event was half the over-firing: the config's probability
    said "one change every 14 days per SKU" and the code answered "five, now".
    """
    random.seed(11)
    cfg = Config()
    expected = pos.expected_price_changes(CATALOGUE, cfg, REALTIME_TICK_SECONDS)
    draws = [pos.draw_change_count(expected) for _ in range(2000)]
    assert max(draws) <= 4           # Poisson(0.0122) never draws five
    assert draws.count(0) > 1900     # 30 s ticks are nearly always quiet
    assert statistics.mean(draws) == pytest.approx(expected, abs=0.01)


def test_a_backfilled_day_and_a_realtime_day_write_the_same_true_frequency():
    """Monte Carlo over 10 simulated days, both writers, same catalogue size.

    Realised per-SKU/day frequency must land on 1/14 on BOTH paths. Tolerance is
    generous (a Poisson's relative sd over 495 SKUs / 10 days is ~4 %), because
    the failure being pinned is a 30x/4x arithmetic error, not sampling noise.
    """
    cfg = Config()
    target = 1.0 / FREQUENCY

    backfill = _walk(_catalogue(), cfg, days=10, tick_seconds=SECONDS_PER_HOUR, seed=5)
    realtime = _walk(_catalogue(), cfg, days=10, tick_seconds=REALTIME_TICK_SECONDS, seed=5)

    backfill_rate = sum(backfill.values()) / CATALOGUE / 10
    realtime_rate = sum(realtime.values()) / CATALOGUE / 10
    assert backfill_rate == pytest.approx(target, rel=0.15)
    assert realtime_rate == pytest.approx(target, rel=0.15)
    assert backfill_rate == pytest.approx(realtime_rate, rel=0.15)


# ---------------------------------------------------------------------------
# 2. The anchor: current_price stays near reference_price
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('tick_seconds', [SECONDS_PER_HOUR, REALTIME_TICK_SECONDS])
def test_thirty_simulated_days_keep_the_median_ratio_at_one(tick_seconds):
    """The regression itself: a month of walking cannot drift the catalogue.

    Before the fix this was 7.3x after 22 days on test/107 and 1.08x after ONE
    day on dev/106. The stated long-run spread is a median ratio of 1.00 with
    ~95 % of SKUs inside +/-10 % (stationary log sd ~= 5.2 % with the shipped
    defaults), so the band asserted here is deliberately looser than the
    measured behaviour and still 7x tighter than the defect.
    """
    cfg = Config()
    products = _catalogue()
    _walk(products, cfg, days=30, tick_seconds=tick_seconds, seed=3)
    ratios = _ratios(products)

    assert statistics.median(ratios) == pytest.approx(1.0, abs=0.03)
    assert ratios[int(len(ratios) * 0.05)] > 0.85
    assert ratios[int(len(ratios) * 0.95)] < 1.15
    assert max(ratios) < 1.5          # vs 60.52x measured on test/107
    assert min(ratios) > 0.5


def test_the_walk_has_no_systematic_drift():
    """A symmetric step has mean 0 — the old uniform(-0.06, +0.08) did not.

    +1 %/change is invisible in one step and is the entire defect at 10.7
    changes a day, so this asserts the *log* mean of many single steps.
    """
    cfg = Config()
    random.seed(17)
    steps = [pos.next_shelf_price(4.00, 4.00, cfg) for _ in range(20000)]
    mean_log = statistics.mean(__import__('math').log(p / 4.00) for p in steps)
    assert abs(mean_log) < 0.002


def test_a_runaway_price_is_clipped_at_max_ratio():
    """`price_max_ratio` is a hard stop, so a 60x cannot be written again."""
    cfg = Config()
    cfg.pricing.price_mean_reversion = 0.0            # no pull, like the old walk
    price = 4.00
    random.seed(1)
    for _ in range(500):
        price = pos.next_shelf_price(price, 4.00, cfg)
    assert price <= round(4.00 * cfg.pricing.price_max_ratio, 2)


def test_a_product_with_no_reference_price_still_walks_around_its_own_price():
    """A pre-t_08deeddf row has no reference; it must not crash the tick."""
    cfg = Config()
    random.seed(2)
    price = pos.next_shelf_price(4.00, None, cfg)
    assert 0.10 <= price <= 4.00 * cfg.pricing.price_max_ratio


def test_the_inflation_scenario_widens_the_step_and_cannot_restart_the_drift():
    cfg = Config()
    random.seed(4)
    wide = [pos.next_shelf_price(4.00, 4.00, cfg, price_mod=1.5) for _ in range(4000)]
    random.seed(4)
    narrow = [pos.next_shelf_price(4.00, 4.00, cfg, price_mod=1.0) for _ in range(4000)]
    assert statistics.pstdev(wide) > statistics.pstdev(narrow)
    import math
    assert abs(statistics.mean(math.log(p / 4.00) for p in wide)) < 0.005


# ---------------------------------------------------------------------------
# 3. `price` and `current_price` are one column
# ---------------------------------------------------------------------------

def test_a_price_change_moves_price_and_current_price_together():
    """`pos` rings the till from `price`; `promotions` prices an ad from
    `current_price`. They are the same column, and only one used to move — which
    priced an ad created on day 10 of a backfill off the PASS-START shelf price.
    """
    cfg = Config()
    random.seed(9)
    products = _catalogue(n=40)
    conn = FakeConn()

    # A whole simulated day's worth of changes, so the draw is not empty.
    changed = pos.maybe_update_product_prices(
        conn, cfg, products, sim_seconds=SECONDS_PER_DAY)

    assert changed >= 1
    assert changed == len(conn.statements_containing('UPDATE pos.products'))
    walked = [p for p in products if p['current_price'] != 4.00]
    assert walked, 'the day should have changed at least one SKU'
    for p in walked:
        assert p['price'] == p['current_price']
    # One UPDATE and one price_history row per changed SKU, and one commit.
    assert len(conn.statements_containing('INSERT INTO pos.price_history')) == changed
    assert conn.commits == 1


def test_a_quiet_tick_touches_no_rows_at_all():
    """A 30 s realtime tick owes 0.012 changes; it must not write a token row."""
    cfg = Config()
    random.seed(1)
    products = _catalogue(n=40)
    conn = FakeConn()
    quiet = sum(1 for _ in range(200)
                if pos.maybe_update_product_prices(conn, cfg, products) == 0)
    assert quiet > 190
    assert conn.commits == 0 or len(conn.executed) < 20 * 2


def test_both_call_sites_tell_the_walk_how_long_its_tick_is():
    """Source guard: a caller that forgets `sim_seconds` inherits the realtime
    cadence, which would silently under-write a backfilled day again."""
    src = (Path(pos.__file__).parent.parent / 'main.py').read_text()
    assert src.count('maybe_update_product_prices(') == 2
    # One caller is the realtime tick, one is a backfill hour.
    assert 'sim_seconds=tick_seconds' in src
    assert 'sim_seconds=SIM_HOUR_SECONDS' in src


# ---------------------------------------------------------------------------
# 4. A weekly ad is priced off the shelf's pivot, never off today's price
# ---------------------------------------------------------------------------

def test_an_ad_is_priced_off_reference_price_not_the_live_shelf():
    """A retro-materialised ad must not advertise a price the shelf never had.

    Measured on test/107: two ads materialised 2026-11-16 for weeks that ended
    36-43 days earlier advertised `promoted_price = 23.87` for a SKU whose shelf
    price during that week was 1.88 — `mart_promotion_effectiveness` read a 92 %
    discount that never happened.
    """
    conn = FakeConn()
    conn.next_fetchone = None            # no ad for this week yet
    products = [
        {'product_id': 'sku-1', 'department_name': 'Produce',
         'price': 23.87, 'current_price': 23.87, 'reference_price': 1.88},
    ]
    random.seed(6)
    promotions.ensure_current_ad(conn, __import__('datetime').date(2026, 11, 16), products)

    assert conn.mogrified, 'the ad should have been created'
    # (ad_id, product_id, promoted_price, discount_pct) — see the template in
    # `promotions.ensure_current_ad`.
    promoted, discount_pct = float(conn.mogrified[0][2]), float(conn.mogrified[0][3])
    # Priced off the reference (1.88), not the live 23.87.
    assert promoted == pytest.approx(1.88 * (1 - discount_pct / 100.0), abs=0.01)
    assert promoted < 2.00


def test_an_ad_prices_off_price_when_a_legacy_row_has_no_reference():
    assert promotions._ad_pivot_price({'price': 3.25, 'current_price': 3.25}) == 3.25
    assert promotions._ad_pivot_price({'reference_price': 2.50, 'current_price': 9.99}) == 2.50
    assert promotions._ad_pivot_price({}) == 1.0
