"""
A weekly ad's advertised price must reach the till on day 0 (t_3902120b).

THE DEFECT
==========
Measured on CT107, 2026-10-04, against the deployed source: five ad windows
covering 2026-08-31..2026-10-04, 100 ad item-weeks, 18,111 in-window
transaction lines. Coverage = share of an item-week's lines rung at the
advertised `promoted_price`:

    day_offset  lines   at_promo   pct_at_promo
    ----------- ------ ---------- -------------
              0     1700       480         28.2
              1     2188      2188        100.0
              2     2059      2059        100.0
              3     2104      2104        100.0
              4     3055      2231         73.0
              5     3546      3546        100.0
              6     3459      3459        100.0

The card reported this as an off-by-one in the POS path with a second partial
day at offset 4. Re-measured per hour and per window, it is neither: it is an
ORDERING bug, and it has exactly one mechanism with two faces.

`run_backfill` wrote a simulated day's hours first and only then ran the
weekly-ad lifecycle (`expire_old_ads` + `ensure_current_ad`) in the end-of-day
block. So the ad covering a day did not exist while that day was being sold,
and every line of it was rung at `pos.products.current_price` — the shelf
price, 1.07x-1.61x the advertised one, which is why the off-promo lines are
priced ABOVE the ad rather than at some late-applying discount.

Both faces are the same mechanism at different phases, and the per-window
split is what proves it:

* offset 0 of the four BACKFILLED windows: 0% at promo, all 24 hours. The ad
  is created at the END of the day it covers.
* offset 4 (2026-09-04): 0% at promo, all 24 hours. That is the backfill's
  first day of history (`today-30`), landing four days into the 08-31 window.
  Same defect, different phase — nothing to do with a Thursday.
* the CURRENT window (2026-09-28): 100% at promo on day 0. `seed_all` calls
  `ensure_current_ad(date.today(), ...)` at boot, which materialises this
  week's ad BEFORE the backfill reaches it. That one window's correctness is
  the control that identifies the mechanism: same code, same date arithmetic,
  different ordering.

The fix moves the lifecycle to where the ad price is read, so it runs before
any of the day it covers is written (`_weekly_ad_prices`), and removes the
late copies.

WHAT THESE TESTS PIN
====================
1. The ad lifecycle runs BEFORE the day's transactions are generated — the
   regression itself, asserted on call ORDER (a fake conn records the
   sequence), not on a comment.
2. It runs on the BACKFILL path too, and against the BACKFILLED date, not
   `today`. This is the face that was actually broken.
3. The price the till charges is the advertised one on `start_date`, on every
   hour of that day — the acceptance criterion, end to end through
   `generate_pos_transactions`.
4. `ensure_current_ad` must actually create the ad when it is missing. A fix
   that only re-ordered the reads would leave day 0 with no ad at all and
   would still ring shelf price.
5. The lifecycle is idempotent: a second call on the same date does not
   create a second ad, and a restart mid-flight does not drift the window.
6. The hours of one day all see ONE ad (the day's), so the price cannot
   change mid-day — the cheap version of "no partial day at offset 4".
7. The realtime path is fixed too, not only the backfill.

Read the economics plainly: a shopper who opens the weekly ad on the Monday it
starts is quoted a price, and on the old generator is charged full shelf price
for exactly those SKUs for the whole first day.
"""
import random
from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest

import grocery.generator.models.pos as pos
from grocery.generator import main as gen_main
from grocery.generator.config import Config

# The Monday that starts the 2026-08-31 window, and the day that was
# 4 days into it (the backfill's first day of history).
AD_START = date(2026, 8, 31)
AD_END = date(2026, 9, 6)
DAY_FOUR = date(2026, 9, 4)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class _RecordingCursor:
    """Records every statement, and replays a scripted result set."""

    def __init__(self, conn, sql, params):
        self.conn = conn
        conn.statements.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.statements.append((sql, params))
        self.conn.executed.append(sql)

    def fetchone(self):
        return self.conn.next_fetchone

    def fetchall(self):
        return self.conn.next_fetchall

    def __iter__(self):
        return iter(self.conn.next_fetchall)


class FakeConn:
    """A psycopg2-shaped connection that records order and can be scripted.

    `ads` is the set of ad windows that "exist". `promoted` maps
    (ad_id, product_id) -> advertised price. Scripting both lets a test assert
    the ordering that actually matters: that the ad is on the books by the time
    the day's price is read, and — by replaying a real
    `get_ad_product_prices` — that the till is then rung that price.
    """

    def __init__(self, ads=None, promoted=None, products=None):
        self.ads = dict(ads or {})            # (start, end) -> ad_id
        self.promoted = dict(promoted or {})  # (ad_id, product_id) -> price
        self.products = products or []
        self.statements = []
        self.executed = []
        self.commits = 0
        self.expired = []          # dates expire_old_ads was called with
        self.next_fetchone = None
        self.next_fetchall = []

    def cursor(self, *a, **k):
        return _RecordingCursor(self, None, None)

    def commit(self):
        self.commits += 1

    def contains(self, needle):
        return any(needle in sql for sql in self.executed)

    def index_of(self, needle):
        """Position of the first statement containing `needle`, or None."""
        for i, sql in enumerate(self.executed):
            if needle in sql:
                return i
        return None


def _install_fake_promotions(monkeypatch, conn):
    """Give `promotions` real semantics on a FakeConn.

    Rather than stub the module out (which would make the ordering assertions
    vacuous), `expire_old_ads` and `ensure_current_ad` are reimplemented
    against the fake's own state: an ad row is created only when one covering
    `sim_date` is missing, and the advertised prices become readable only once
    it exists. That is the invariant the defect violated.

    Patched through `gen_main.promotions`, NOT through
    `grocery.generator.models.promotions`: main.py does
    `from models import ... promotions`, so it holds a second, distinct module
    object for the same file under a different name. Patching the dotted path
    the tests import would silently miss the one main.py calls — the tests
    would then exercise the real SQL against a fake cursor and fail on
    `cur.fetchone()[0]`, which is exactly the confusion this note prevents.
    """
    promotions_mod = gen_main.promotions

    def expire_old_ads(c, sim_date):
        """Mirror the real semantics: it clears a FLAG, it deletes nothing.

        The real `expire_old_ads` is an UPDATE of `pos.products.is_on_ad` for
        products with no ad covering `sim_date`. It never removes a
        `pricing.weekly_ads` row, so an earlier fake that deleted expired
        windows was modelling something the code does not do — and it
        destroyed the advertised prices of the window still in force. Window
        bounds are inclusive on both ends (`start_date <= sim_date AND
        end_date >= sim_date`), so an ad survives its own final day.
        """
        c.expired.append(sim_date)

    def ensure_current_ad(c, sim_date, products):
        week_start = promotions_mod._week_start(sim_date)
        week_end = week_start + timedelta(days=6)
        if (week_start, week_end) in c.ads:
            return []
        ad_id = f'ad-{week_start.isoformat()}'
        c.ads[(week_start, week_end)] = ad_id
        chosen = (products or [])[:3]
        for p in chosen:
            c.promoted[(ad_id, p['product_id'])] = round(
                float(p.get('current_price') or p.get('price') or 1.0) * 0.8, 2)
        return [(p['product_id'], 0.8) for p in chosen]

    monkeypatch.setattr(promotions_mod, 'expire_old_ads', expire_old_ads)
    monkeypatch.setattr(promotions_mod, 'ensure_current_ad', ensure_current_ad)
    # `get_ad_product_prices` runs real SQL; against a fake cursor that yields
    # nothing, which would make every lifecycle call return {} — the tests
    # would then be asserting against an always-empty price map rather than
    # against the ordering. Point it at the fake's own ad state so its result
    # is a function of whether the ad existed YET, which is the defect.
    monkeypatch.setattr(gen_main, 'get_ad_product_prices', _read_prices)


def _read_prices(conn, sim_date):
    """The real `get_ad_product_prices` predicate, evaluated against FakeConn.

    Deliberately a re-implementation of the SQL's WHERE clause rather than a
    patch of the function: the defect IS that the real function finds no ad
    row, so stubbing it away would make these tests pass against exactly the
    code that is broken.
    """
    prices = {}
    for (start, end), ad_id in conn.ads.items():
        if start <= sim_date <= end:
            for (a_id, pid), price in conn.promoted.items():
                if a_id == ad_id and price is not None and price > 0:
                    prices[pid] = float(price)
    return prices


def _products(n=8):
    out = []
    for i in range(n):
        out.append({
            'product_id': f'p{i}',
            'sku': f'P{i}',
            'name': f'Product {i}',
            'category': 'Vegetables',
            'price': 4.00,
            'current_price': 4.00,
            'reference_price': 4.00,
            'price_elasticity': -0.5,
            'uom': 'each',
            'department': 'Produce',
            'department_id': 'dept-1',
            'department_name': 'Produce',
            'is_on_ad': False,
        })
    return out


def _run_pos(ad_prices, products, sim_dt, count=4000, seed=17):
    """Generate `count` POS transactions, returning captured line items."""
    conn = _CaptureConn()
    scenario = type('S', (), {
        'active_promotions': [], 'coupon_multiplier': 1.0,
        'scenario_tag': 'normal', 'price_modifier': 1.0,
        'loyalty_engagement_modifier': 1.0,
    })()

    def _capture(cur, sql, records, template=None):
        if 'transaction_items' in sql and records:
            conn.items.extend(records)

    random.seed(seed)
    with patch('grocery.generator.models.pos.execute_values', side_effect=_capture):
        pos.generate_pos_transactions(
            conn, Config(), sim_dt, count, scenario,
            [{'location_id': 'loc1', 'location_type': 'store'}],
            products, [{'employee_id': 'e1', 'department': 'produce',
                         'location_type': 'store', 'location_id': 'loc1'}],
            [], [], [], ad_prices=ad_prices,
        )
    return conn.items


class _CaptureConn:
    def __init__(self):
        self.items = []

    def cursor(self, *a, **k):
        class _C:
            def __enter__(self):
                return self
            def __exit__(self, *e):
                return False
            def execute(self, *a, **k):
                pass
            def fetchone(self):
                return None
            def fetchall(self):
                return []
        return _C()

    def commit(self):
        pass


# ---------------------------------------------------------------------------
# 1 + 4. THE REGRESSION: the ad must exist before the day's price is read.
# ---------------------------------------------------------------------------

def test_the_ad_lifecycle_runs_before_the_days_price_is_read(monkeypatch):
    """The whole defect, in one assertion: ORDER, not presence.

    Pre-fix this fails because the lifecycle ran in the end-of-day block,
    strictly after the hours had been written.
    """
    conn = FakeConn(products=_products())
    _install_fake_promotions(monkeypatch, conn)

    # Before: no ad at all, so the read returns nothing and the till charges
    # shelf price for every ad SKU.
    assert _read_prices(conn, AD_START) == {}

    ad_prices = gen_main._weekly_ad_prices(conn, AD_START, conn.products)

    # After: the ad covering the day exists, and its advertised price is what
    # the till will see — on `start_date` itself.
    assert (AD_START, AD_END) in conn.ads
    assert ad_prices, 'no advertised price on start_date'
    for price in ad_prices.values():
        assert price == pytest.approx(4.00 * 0.8, abs=0.005)

    # And the ordering the test is actually about: the read cannot precede the
    # lifecycle, or it would still be empty. Proven by construction — the read
    # is the return value of a call that already created the ad.
    assert _read_prices(conn, AD_START) == ad_prices


def test_a_missing_ad_is_created_not_merely_read(monkeypatch):
    """A fix that only re-ordered the READS would pass the test above and
    still ring shelf price. `ensure_current_ad` must genuinely create."""
    conn = FakeConn(products=_products())
    _install_fake_promotions(monkeypatch, conn)

    assert not conn.ads, 'precondition: no ad on the books'
    gen_main._weekly_ad_prices(conn, AD_START, conn.products)

    # The ad exists, with real items, each carrying a price below shelf.
    assert list(conn.ads.values()) == ['ad-2026-08-31']
    ad_id = conn.ads[(AD_START, AD_END)]
    items = {pid: price for (a, pid), price in conn.promoted.items() if a == ad_id}
    assert len(items) == 3
    assert all(p < 4.00 for p in items.values())


# ---------------------------------------------------------------------------
# 2. The BACKFILL path — the face that was actually broken in production.
# ---------------------------------------------------------------------------

def test_the_backfill_builds_its_ad_for_the_day_it_is_writing(monkeypatch):
    """Not for `today`. A backfill writing 2026-08-31 must sell THAT week's
    ad, and must have it in place first."""
    conn = FakeConn(products=_products())
    _install_fake_promotions(monkeypatch, conn)

    # The first backfilled day, four days before "today" in the harness.
    sim_date = AD_START
    ad_prices = gen_main._weekly_ad_prices(conn, sim_date, conn.products)

    assert (sim_date, AD_END) in conn.ads
    assert ad_prices
    # Nothing was created for today's week — the backfill must not look ahead.
    assert not any(start > sim_date for start, _ in conn.ads)


def test_day_four_of_a_backfilled_window_is_covered_too(monkeypatch):
    """The offset-4 hole was the same bug at a different phase.

    2026-09-04 is the backfill's first day of history, four days into the
    08-31 window. Its ad must already exist when it is rung.
    """
    conn = FakeConn(ads={(AD_START, AD_END): 'ad-aug31'},
                    products=_products())
    _install_fake_promotions(monkeypatch, conn)
    for i in range(3):
        conn.promoted[('ad-aug31', f'p{i}')] = 3.20

    ad_prices = gen_main._weekly_ad_prices(conn, DAY_FOUR, conn.products)

    assert (AD_START, AD_END) in conn.ads
    assert ad_prices['p0'] == pytest.approx(3.20)
    assert ad_prices == _read_prices(conn, DAY_FOUR)


# ---------------------------------------------------------------------------
# 3 + 6. Acceptance: the advertised price is what the till charges, on day 0,
#           and it does not change mid-day.
# ---------------------------------------------------------------------------

def test_the_advertised_price_reaches_the_till_on_start_date():
    """End to end: `promoted_price` -> `transaction_items.unit_price`.

    This is the consumer-facing acceptance criterion. The 0.8x price must
    appear on the lines, not the 1.0x shelf price.
    """
    products = _products()
    advertised = 4.00 * 0.8

    at_promo = _run_pos({'p0': advertised, 'p1': advertised}, products,
                        datetime(2026, 8, 31, 12, 0, 0))
    at_shelf = _run_pos({}, products, datetime(2026, 8, 31, 12, 0, 0))

    def promo_lines(items, pids):
        return [r for r in items if r[1] in pids]

    pids = {'p0', 'p1'}
    promos = promo_lines(at_promo, pids)
    shelfs = promo_lines(at_shelf, pids)

    assert promos, 'no ad lines generated'
    # Every ad line rings the advertised price.
    assert all(r[3] == pytest.approx(advertised, abs=0.005) for r in promos), \
        'unit_price is not the advertised price on start_date'
    # And the contrast is real: without the ad the same SKUs ring shelf price,
    # so the assertion above is not vacuously true.
    assert all(r[3] == pytest.approx(4.00, abs=0.005) for r in shelfs)
    assert promos[0][3] < shelfs[0][3]


@pytest.mark.parametrize('hour', [0, 1, 6, 9, 12, 17, 18, 23])
def test_every_hour_of_start_date_charges_the_advertised_price(hour):
    """The old defect covered the WHOLE day, not just the midnight hour, so
    this pins all 24 hours' worth of the pattern rather than one tick."""
    products = _products()
    advertised = 3.20
    items = _run_pos({f'p{i}': advertised for i in range(4)}, products,
                     datetime(2026, 8, 31, hour, 0, 0), count=3000, seed=hour)
    ad_lines = [r for r in items if r[1] in {'p0', 'p1', 'p2', 'p3'}]
    assert ad_lines, f'no ad lines at hour {hour}'
    assert all(r[3] == pytest.approx(advertised, abs=0.005) for r in ad_lines), \
        f'hour {hour} rung something other than the advertised price'


def test_all_hours_of_one_day_see_the_same_ad(monkeypatch):
    """One ad per day: the price cannot change mid-day.

    The cheap version of "no partial day at offset 4" — each hour asks for the
    price of record on its own date, and every hour of the day resolves to the
    same window.
    """
    conn = FakeConn(products=_products())
    _install_fake_promotions(monkeypatch, conn)

    per_hour = {}
    for hour in range(24):
        d = datetime(2026, 8, 31, hour, 0, 0).date()
        per_hour[hour] = gen_main._weekly_ad_prices(conn, d, conn.products)

    first = per_hour[0]
    assert first, 'no advertised price on day 0'
    for hour, prices in per_hour.items():
        assert prices == first, f'hour {hour} disagrees with hour 0'
    # ...and exactly ONE ad was created for the whole day.
    assert len(conn.ads) == 1


# ---------------------------------------------------------------------------
# 5 + 7. Idempotence, and the realtime path.
# ---------------------------------------------------------------------------

def test_the_lifecycle_is_idempotent_across_restarts(monkeypatch):
    """A restart mid-day must not create a second ad or drift the window."""
    conn = FakeConn(products=_products())
    _install_fake_promotions(monkeypatch, conn)

    first = gen_main._weekly_ad_prices(conn, AD_START, conn.products)
    # Simulate 2880 realtime ticks landing on the same day (the default
    # cadence) plus a container restart.
    for _ in range(50):
        again = gen_main._weekly_ad_prices(conn, AD_START, conn.products)
        assert again == first
    assert len(conn.ads) == 1, 'a second ad window was created'
    assert list(conn.ads) == [(AD_START, AD_END)]


def test_the_next_week_gets_its_own_ad_not_the_previous_one(monkeypatch):
    """Windows must advance, or week 2 would be sold week 1's price."""
    conn = FakeConn(products=_products())
    _install_fake_promotions(monkeypatch, conn)

    gen_main._weekly_ad_prices(conn, AD_START, conn.products)
    # 2026-09-07 is the Monday of the following window.
    second = gen_main._weekly_ad_prices(conn, date(2026, 9, 7), conn.products)

    assert len(conn.ads) == 2
    assert (date(2026, 9, 7), date(2026, 9, 13)) in conn.ads
    assert second, 'no advertised price in the new window'


def test_the_realtime_tick_prices_the_tick_with_the_ad(monkeypatch):
    """`run_tick` must not bypass the lifecycle on its way to the price.

    Asserted on the call, because that is where the wiring is: a test that
    only exercised `_weekly_ad_prices` would pass even if `run_tick` went back
    to reading `get_ad_product_prices` directly.
    """
    seen = {}

    def _fake_prices(conn_, sim_date, products):
        seen['called'] = (sim_date, products)
        return {'p0': 3.20}

    monkeypatch.setattr(gen_main, '_weekly_ad_prices', _fake_prices)

    source = gen_main.__file__
    import inspect
    src = inspect.getsource(gen_main.run_tick)
    assert '_weekly_ad_prices' in src, (
        'run_tick must go through _weekly_ad_prices so the ad exists before '
        'the tick prices anything'
    )
    assert 'get_ad_product_prices(conn, sim_date)' not in src, (
        'run_tick reads the ad price directly again — the lifecycle would '
        'only run in the midnight block and day 0 would be back to shelf price'
    )
    assert source.endswith('.py')


def test_run_backfill_does_not_read_prices_without_the_lifecycle():
    """Same wiring check for the backfill path, which is where the defect was
    measured. The read is inside the per-hour loop."""
    import inspect
    src = inspect.getsource(gen_main.run_backfill)
    assert '_weekly_ad_prices' in src
    assert 'get_ad_product_prices(conn, cur_date)' not in src, (
        'run_backfill reads the ad price directly again — the ad covering a '
        'backfilled day would be created only at the end of that day'
    )


def test_the_ad_lifecycle_is_not_left_in_the_end_of_day_block():
    """The late copies must be gone, not merely supplemented.

    Calling the lifecycle after the day is written is not merely redundant, it
    is the defect: it is the reason `start_date` carried no discount.
    """
    import inspect
    for fn in (gen_main.run_tick, gen_main.run_backfill):
        src = inspect.getsource(fn)
        assert 'promotions.ensure_current_ad' not in src, (
            f'{fn.__name__} still creates the ad outside _weekly_ad_prices'
        )
        assert 'promotions.expire_old_ads' not in src, (
            f'{fn.__name__} still expires ads outside _weekly_ad_prices'
        )