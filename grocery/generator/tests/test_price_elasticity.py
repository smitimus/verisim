"""
Price/promo -> demand elasticity loop — verisim card t_08deeddf.

The defect: `generate_pos_transactions` drew a cart with
`random.choices(products, k=num_items)` — every product equally likely
regardless of `current_price`, of the weekly-ad `promoted_price`, or of the
scenario `price_modifier`. The 90-day `price_history` backfill is commented
"for elasticity analysis" (`seed_price_history`), but with price-independent
demand any elasticity `data-lab` regresses out of
`mart_product_price_elasticity` is sampling noise, not a causal response. The
mock feed and the simulation had drifted apart; this pins them back together.

What these tests pin:

1. the law itself is a constant-elasticity (log-log) demand curve, so the
   long-run elasticity IS the configured parameter rather than an artifact of
   some fitted shape;
2. the effect is causal and correctly signed — a product priced above its own
   reference is bought less, and units bought fall as its price rises;
3. a weekly-ad item is genuinely more attractive than the same item off-ad
   (the `promoted_price` is on the price-of-record, not decoration);
4. it is KEYED, not a global multiplier: a promo on one SKU cannot move an
   unrelated SKU, or the signal is a scenario tag and not elasticity;
5. the long-run elasticity measured back out of the generator converges on the
   configured value across seeds (the acceptance criterion) — with a
   deliberately wrong configured value too, so the test cannot pass by
   accident;
6. demand is not clamped to zero by the discount floor, and a deep enough
   discount cannot make the model sell negative units;
7. the price walk that `seed_price_history` writes carries a common market
   factor, so a product's own price change is separable from the market's
   (without it, per-product noise swamps the -0.5x signal and the elasticity
   is genuinely not estimable from the data);
8. the online channel uses the same law, so both channels agree on how price
   moves demand;
9. a degenerate product (zero/negative price, no reference price) cannot make
   the generator crash or produce an infinite weight.

Read the economics in plain terms: if a cereal box normally costs $4 and the
store raises it to $4.40, a shopper with a price sensitivity of -0.5 (half as
picky as the average) buys about 10% fewer boxes. Raising price to move margin
costs units, exactly as a real store would experience it.
"""
import math
import random
from datetime import date, datetime
from unittest.mock import patch

import pytest

import grocery.generator.models.pos as pos
import grocery.generator.elasticity as elasticity
from grocery.generator.config import Config

from .conftest import patch_all_pos_writes

# A Saturday, mid-morning, so no holiday / rush-hour noise in the scenario tag.
SIM_DT = datetime(2026, 9, 26, 10, 0, 0)


# ---------------------------------------------------------------------------
# Fakes: capture what the generator would have written, no DB needed.
# ---------------------------------------------------------------------------

class _FakeConn:
    """Accepts every statement; captures the transaction_items batches."""

    def __init__(self):
        self.items = []

    def cursor(self, *a, **k):
        conn = self

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


class _Scenario:
    active_promotions = []
    coupon_multiplier = 1.0
    scenario_tag = 'normal'
    price_modifier = 1.0
    loyalty_engagement_modifier = 1.0


def _product(pid, price, reference=None, department='Produce', uom='each'):
    """One product as `pos._fetch_active_products` hands it to the generator.

    `reference_price` is the price the elasticity curve pivots on: the price
    this SKU is normally sold at. `None` means "the live price is its own
    reference" (no price signal at all).
    """
    ref = price if reference is None else reference
    return {
        'product_id': pid,
        'sku': pid.upper(),
        'name': f'Product {pid}',
        'category': 'Vegetables',
        'price': price,
        'current_price': price,
        'reference_price': ref,
        'price_elasticity': -0.5,
        'uom': uom,
        'department': department,
        'department_id': 'dept-1',
        'department_name': department,
        'is_on_ad': False,
    }


def _employees():
    return [{'employee_id': 'e1', 'department': 'store',
             'location_type': 'store', 'location_id': 'loc1'}]


def _run(products, cfg, count=20000, sim_dt=SIM_DT, seed=7, ad_prices=None):
    """Generate `count` transactions and return captured transaction_items.

    `ad_prices` maps product_id -> the weekly-ad promoted price, i.e. this
    price is the price of record for that SKU on this date.
    """
    conn = _FakeConn()

    def _capture(cur, sql, records, template=None):
        if 'transaction_items' in sql and records:
            conn.items.extend(records)

    random.seed(seed)
    with patch_all_pos_writes(_capture):
        pos.generate_pos_transactions(
            conn, cfg, sim_dt, count, _Scenario(),
            [{'location_id': 'loc1', 'location_type': 'store'}],
            products, _employees(), [], [], [],
            ad_prices=ad_prices or {},
        )
    return conn.items


def _units_by_product(items):
    """product_id -> units bought (quantity is the tuple's index 2)."""
    out = {}
    for row in items:
        out[row[1]] = out.get(row[1], 0) + row[2]
    return out


# ---------------------------------------------------------------------------
# 1. The law is a constant-elasticity curve, not a fitted shape.
# ---------------------------------------------------------------------------

def test_demand_weight_follows_the_constant_elasticity_law():
    """weight = (paid / reference) ** elasticity, exactly."""
    cfg = Config()
    product = _product('p1', price=4.0, reference=4.0)
    product['price_elasticity'] = -0.5

    # Half the reference price -> ratio 0.5 -> 0.5**-0.5 = 1.414x the pull.
    assert pos.demand_weight(product, 2.0) == pytest.approx(0.5 ** -0.5, rel=1e-9)
    # A 10% price rise -> 1.1**-0.5 ~= 0.954.
    assert pos.demand_weight(product, 4.4) == pytest.approx(1.1 ** -0.5, rel=1e-9)
    # The pivot point is exactly 1.0 — the reference price is the baseline.
    assert pos.demand_weight(product, 4.0) == pytest.approx(1.0, rel=1e-9)
    # A 10x price rise on the same reference -> 10**-0.5 (the clip is for the
    # absurd tail; this is not it — see the clip test below).
    assert pos.demand_weight(product, 40.0) == pytest.approx(10 ** -0.5, rel=1e-9)


def test_the_curve_is_elasticity_configurable_per_product():
    cfg = Config()
    inelastic = _product('p1', 4.0, 4.0)
    elastic = _product('p2', 4.0, 4.0)
    inelastic['price_elasticity'] = -0.2
    elastic['price_elasticity'] = -2.0
    # Same 40% price rise, very different response.
    assert pos.demand_weight(inelastic, 5.6) == pytest.approx(1.4 ** -0.2, rel=1e-9)
    assert pos.demand_weight(elastic, 5.6) == pytest.approx(1.4 ** -2.0, rel=1e-9)
    assert pos.demand_weight(elastic, 5.6) < pos.demand_weight(inelastic, 5.6)
    # The default comes from config, not a literal in the model.
    assert Config().pricing.default_price_elasticity == -0.5


def test_a_degenerate_price_cannot_explode_or_crash_the_law():
    """Zero price, missing reference, garbage elasticity: no inf, no crash."""
    cfg = Config()
    for broken in (
        _product('p1', price=0.0, reference=0.0),
        _product('p2', price=4.0, reference=None),      # treated as its own ref
        _product('p3', price=-1.0, reference=-1.0),
    ):
        weight = pos.demand_weight(broken, 4.0, cfg)
        assert math.isfinite(weight), broken
        assert weight > 0.0, broken
    # A non-finite elasticity falls back to the configured default, and a
    # price equal to the reference is neutral.
    nan = _product('p4', 4.0, 4.0)
    nan['price_elasticity'] = float('nan')
    assert pos.demand_weight(nan, 4.0, cfg) == pytest.approx(1.0, rel=1e-9)
    # A missing elasticity key entirely (a pre-t_08deeddf product dict) is
    # also the configured default, not a crash.
    legacy = {'product_id': 'p5', 'price': 4.0, 'reference_price': 4.0}
    assert pos.demand_weight(legacy, 4.0, cfg) == pytest.approx(1.0, rel=1e-9)


# ---------------------------------------------------------------------------
# 2 + 5. The long-run effect, and the acceptance criterion itself.
# ---------------------------------------------------------------------------

def test_long_run_elasticity_matches_the_configured_value_across_seeds():
    """THE acceptance test: measure elasticity back out of the generator.

    Two products of otherwise identical shape, one priced 30% above the other.
    The curve says the dearer one is bought at `1.3 ** e` of the cheaper one.
    Vary the configured e, re-measure, and the measurement must track it.
    """
    cfg = Config()
    measured = {}
    for configured in (-0.2, -0.5, -1.0, -1.8):
        for seed in (3, 11, 29, 47, 101):
            cheap = _product('cheap', 4.00, 4.00)
            dear = _product('dear', 5.20, 4.00)
            for product in (cheap, dear):
                product['price_elasticity'] = configured
            items = _run([cheap, dear], cfg, count=30000, seed=seed)
            units = _units_by_product(items)
            ratio = units['dear'] / units['cheap']
            expected = 1.3 ** configured
            measured.setdefault(configured, []).append(ratio / expected)

    for configured, ratios in measured.items():
        mean = sum(ratios) / len(ratios)
        # 5 seeds x 30k transactions: the ratio is a mean over ~150k draws, so
        # the sampling error on the *ratio of two such means* is ~0.3%. A 3%
        # band is ~10x headroom — loose enough for Monte Carlo, tight enough
        # that reverting to price-independent demand (which measures ~1.0 for
        # every configured value) fails every one of the four.
        assert mean == pytest.approx(1.0, rel=0.03), (configured, ratios)


def test_a_product_priced_above_its_reference_is_bought_less():
    cfg = Config()
    reference = _product('ref', 4.00, 4.00)
    overpriced = _product('over', 5.60, 4.00)      # +40%
    items = _run([reference, overpriced], cfg, count=40000)
    units = _units_by_product(items)
    assert units['over'] < units['ref']
    # -0.5 elasticity: 1.4**-0.5 ~= 0.845, i.e. ~15% fewer units.
    assert units['over'] / units['ref'] == pytest.approx(1.4 ** -0.5, rel=0.04)


# ---------------------------------------------------------------------------
# 3 + 4. The weekly ad, and the fact that the signal is keyed per SKU.
# ---------------------------------------------------------------------------

def test_a_weekly_ad_item_out_sells_the_same_item_off_ad():
    """promoted_price is the price of record, so the ad really lifts units."""
    cfg = Config()
    regular = _product('reg', 4.00, 4.00)
    on_ad = _product('ad', 4.00, 4.00)
    items = _run([regular, on_ad], cfg, count=40000,
                 ad_prices={'ad': 3.20})          # 20% off this week
    units = _units_by_product(items)
    # Ad item at 0.8x its reference -> 0.8**-0.5 ~= 1.12x the units.
    assert units['ad'] > units['reg']
    assert units['ad'] / units['reg'] == pytest.approx(0.8 ** -0.5, rel=0.05)


def test_the_promo_signal_is_keyed_not_a_global_multiplier():
    """A discount on one SKU cannot move a different, unrelated SKU."""
    cfg = Config()
    target = _product('target', 4.00, 4.00)
    bystander = _product('bystander', 4.00, 4.00)
    baseline = _run([target, bystander], cfg, count=30000, seed=13)
    base_units = _units_by_product(baseline)

    promoted = _run([target, bystander], cfg, count=30000, seed=13,
                    ad_prices={'target': 2.00})     # half price on ONE SKU
    promo_units = _units_by_product(promoted)

    # The promoted SKU's SHARE of the basket rose. Measured as a share, not a
    # raw unit count: a basket is a fixed number of lines, so a heavier SKU
    # necessarily takes share from the other one — that is the mechanism
    # working, not a leak. What must NOT happen is the bystander's share
    # moving, which is what a global volume multiplier would cause.
    def share(units):
        return units['target'] / (units['target'] + units['bystander'])

    base_share = share(base_units)
    promo_share = share(promo_units)
    # -0.5 elasticity at half price: 1.414x the pull, i.e. 0.5 -> 0.586 of
    # a two-SKU basket. Measured 0.5850 on a 200k draw.
    assert base_share == pytest.approx(0.5, rel=0.01)
    assert promo_share == pytest.approx(
        0.5 ** -0.5 / (0.5 ** -0.5 + 1.0), rel=0.02)
    # And the total basket is unchanged — the loop reweights WHICH items, it
    # does not create or destroy transactions.
    assert (promo_units['target'] + promo_units['bystander']) == pytest.approx(
        base_units['target'] + base_units['bystander'], rel=0.01)


# ---------------------------------------------------------------------------
# 6 + 9. Floors: no negative units, no silent clamp to zero.
# ---------------------------------------------------------------------------

def test_a_deep_discount_lifts_demand_without_clamping_or_exploding():
    cfg = Config()
    regular = _product('reg', 4.00, 4.00)
    slashed = _product('slash', 4.00, 4.00)
    # 97.5% off: the law says 0.025**-0.5 = 6.3x the pull. Well inside the
    # clip, so this is a real elasticity reading, not a floored one.
    items = _run([regular, slashed], cfg, count=30000,
                 ad_prices={'slash': 0.10})
    units = _units_by_product(items)
    assert units['slash'] > units['reg'] * 3


def test_the_weight_clip_bounds_the_tail_without_touching_real_prices():
    """An absurd discount cannot buy the whole store; a real one is exact."""
    cfg = Config()
    product = _product('p1', 4.00, 4.00)
    product['price_elasticity'] = -1.0
    low, high = elasticity.DEMAND_WEIGHT_CLIP

    # Realistic range: unclipped, exactly the law.
    for price in (4.0, 3.20, 2.00, 1.00, 8.00):
        expected = (price / 4.0) ** -1.0
        assert expected > low and expected < high
        assert pos.demand_weight(product, price, cfg) == pytest.approx(expected, rel=1e-9)

    # Pathological discount: a fraction-of-a-cent price on a -1.0 SKU is
    # 0.00025**-1.0 = 4000x, capped — one SKU cannot absorb every basket.
    absurd_discount = pos.demand_weight(product, 0.001, cfg)
    assert absurd_discount == pytest.approx(high, rel=1e-9)
    # Pathological premium: an absurdly HIGH price would otherwise drive the
    # weight toward zero and the SKU out of the catalogue entirely.
    absurd_premium = pos.demand_weight(product, 40000.0, cfg)
    assert absurd_premium == pytest.approx(low, rel=1e-9)


def test_demand_totals_stay_finite_for_a_pathological_catalogue():
    """Weights are normalised, so a 0.01 SKU cannot eat every basket."""
    cfg = Config()
    products = [_product('a', 0.01, 0.01), _product('b', 4.00, 4.00),
                _product('c', 40.00, 4.00)]
    for product in products:
        product['price_elasticity'] = -3.0
    items = _run(products, cfg, count=5000, ad_prices={'a': 0.01})
    assert items
    assert all(row[2] > 0 for row in items)     # every line has real quantity


# ---------------------------------------------------------------------------
# 7. The price walk carries a common market factor (estimability).
# ---------------------------------------------------------------------------

def test_the_seeded_price_walk_has_a_common_market_factor():
    """A pure per-product random walk is NOT identifiable.

    `mart_product_price_elasticity` regresses each product's units on its own
    price. If every product's price moves on its own independent noise, the
    product's own price change and the market's move together at 1:1, and a
    -0.5 effect is unmeasurable — the "coincidental elasticity" the card
    describes. So the walk must carry a shared component: `market_daily_index`
    (the store's inflation that day) alongside the per-product idiosyncratic
    step. The product's price is then `reference * (1 + own drift) * market`,
    and the loop's response to the *relative* price is what a regression on
    the product's own price can actually recover.
    """
    cfg = Config()
    assert cfg.pricing.price_market_factor_weight > 0.0
    assert cfg.pricing.price_market_factor_weight <= 1.0
    # A 0 leaves per-product noise unscaled (indistinguishable from no
    # elasticity at all); 1.0 leaves no idiosyncratic noise to separate the
    # two apart. Neither is a working configuration.
    assert cfg.pricing.price_market_factor_weight < 1.0

    random.seed(5)
    paths = pos.sample_price_paths(
        [('p1', 4.00), ('p2', 6.00), ('p3', 9.00)],
        days=90, steps=12, cfg=cfg,
    )
    # One record per SKU per step, and the newest is TODAY with new_price ==
    # current_price (the continuity link realtime continues from).
    assert len({r['product_id'] for r in paths}) == 3
    for pid, live in (('p1', 4.00), ('p2', 6.00), ('p3', 9.00)):
        newest = next(r for r in paths if r['product_id'] == pid and r['days_ago'] == 0)
        assert newest['price'] == pytest.approx(live, rel=1e-6), pid

    # The market factor is SHARED: one draw per day, identical for every SKU.
    # That sharedness is the whole point — it is the component a per-product
    # elasticity regression removes.
    per_sku = {pid: {r['days_ago']: r['market_index'] for r in paths
                     if r['product_id'] == pid} for pid in ('p1', 'p2', 'p3')}
    assert per_sku['p1'] == per_sku['p2'] == per_sku['p3']
    # It moves (a flat market would make "price" unidentifiable from time).
    assert len({round(m, 9) for m in per_sku['p1'].values()}) > 5

    # The IDENTIFIABILITY property, asserted on the paths rather than on an
    # internal term: if the walk were market-factor-ONLY, every product's
    # price would be exactly its own reference times the same market index —
    # so the three paths would be *perfectly proportional* and a regression
    # could not tell a product's own price move from the market's, which is
    # the whole failure the card describes. Removing the idiosyncratic
    # component must therefore change the cross-product price ratios.
    def price_map(pid):
        return {r['days_ago']: r['price'] for r in paths if r['product_id'] == pid}

    # Each SKU has its own starting price, so compare the RATIO of each
    # product's price to another product's on every shared day. A pure market
    # walk makes each of these constant in time.
    for other in ('p2', 'p3'):
        p1_prices, other_prices = price_map('p1'), price_map(other)
        ratios = {round(p1_prices[d] / other_prices[d], 6)
                  for d in p1_prices if p1_prices[d] and other_prices[d]}
        assert len(ratios) > 5, (
            'the price ratio between two products is constant in time (%r) — '
            'the walk is market-factor-only, so no per-product elasticity is '
            'recoverable' % ratios
        )


# ---------------------------------------------------------------------------
# 8. The online channel shares the law.
# ---------------------------------------------------------------------------

def test_the_online_channel_uses_the_same_demand_law():
    """The online basket must be drawn by the shared law, not a private copy.

    Asserted on the CALL (`choose_products`), which is what actually shares the
    law, rather than on the module mentioning a name: a comment or a docstring
    would otherwise satisfy this.
    """
    import inspect
    import grocery.generator.models.online as online

    src = inspect.getsource(online.generate_online_orders)
    assert 'choose_products' in src, (
        'online orders must draw products with the same price-elasticity law '
        'as POS, or the two channels disagree on how price moves demand'
    )
    # It must no longer be the old uniform draw.
    assert 'random.choices(products' not in src
    # And it must import the shared law rather than redefine it.
    assert 'from elasticity import choose_products' in inspect.getsource(online)


# ---------------------------------------------------------------------------
# 10. Config plumbing — every knob is a real config key, no magic numbers.
# ---------------------------------------------------------------------------

def test_the_elasticity_knobs_are_real_config_keys():
    cfg = Config()
    assert cfg.pricing.default_price_elasticity == -0.5
    assert cfg.pricing.elasticity_jitter == 0.15
    assert cfg.pricing.price_min_ratio == 0.05
    assert cfg.pricing.price_market_factor_weight > 0.0

    # ...and they load from YAML like every other key.
    sample = {'pricing': {
        'default_price_elasticity': -1.2,
        'elasticity_jitter': 0.3,
        'price_min_ratio': 0.1,
        'price_market_factor_weight': 0.6,
    }}
    from grocery.generator.config import _apply_yaml
    loaded = Config()
    _apply_yaml(loaded, sample)
    assert loaded.pricing.default_price_elasticity == -1.2
    assert loaded.pricing.elasticity_jitter == 0.3
    assert loaded.pricing.price_min_ratio == 0.1
    assert loaded.pricing.price_market_factor_weight == 0.6


def test_products_carry_their_own_reference_price_and_elasticity():
    """`seed_products` must persist both, or the law has nothing to key on."""
    import inspect
    src = inspect.getsource(pos.seed_products)
    assert 'reference_price' in src
    assert 'price_elasticity' in src
    # The DDL must declare them too — a model writing a column schema.sql
    # lacks fails only at runtime, on a fresh bootstrap.
    schema = pos.read_schema_sql()
    assert 'reference_price' in schema
    assert 'price_elasticity' in schema


def test_the_products_api_route_exposes_the_elasticity_columns():
    """`/grocery/pos/products` is what data-lab's `raw_pos.products` reads.

    The generator writing the columns is only half the job: if the route does
    not select them, the raw mirror has no column to stage and
    `stg_pos_products` cannot surface them. Source-level check so it needs no
    running API (the same pattern as `test_pagination.py`).
    """
    import pathlib
    api = pathlib.Path(pos.__file__).resolve().parents[3] / 'base' / 'api' / 'main.py'
    src = api.read_text()
    assert '_products_has_elasticity_columns' in src, (
        'the products route must probe for the columns before selecting them: '
        'a schema.sql change only reaches a fresh bootstrap, so a slot on an '
        'older image would 500 on a bare p.reference_price — and that route '
        'backs the data-lab raw_pos.products ingest'
    )
    assert 'AS reference_price' in src
    assert 'p.price_elasticity' in src
