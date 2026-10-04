"""
Configuration loader for the grocery data generator.
Reads /config/config.yaml (mounted from stacks/verisim-grocery/config.yaml)
and merges with environment variables for DB connection.
"""
import logging
import os

import yaml
from dataclasses import dataclass, field
from typing import Dict, List

from config_schema import validate_and_apply

log = logging.getLogger(__name__)


@dataclass
class VolumeConfig:
    pos_transactions_per_day_min: int = 800
    pos_transactions_per_day_max: int = 3000
    hourly_weights: List[float] = field(default_factory=lambda: [
        0.0017, 0.0008, 0.0008, 0.0008, 0.0017, 0.005, 0.0129, 0.0258,
        0.0446, 0.0646, 0.0817, 0.0896, 0.0896, 0.0704, 0.0558, 0.0483,
        0.0629, 0.0817, 0.0938, 0.075, 0.0446, 0.0283, 0.0129, 0.0067]
    )
    day_of_week_multipliers: Dict[str, float] = field(default_factory=lambda: {
        'monday': 0.88, 'tuesday': 0.85, 'wednesday': 0.90,
        'thursday': 0.95, 'friday': 1.10, 'saturday': 1.25, 'sunday': 1.15
    })


@dataclass
class LocationConfig:
    store_count: int = 3
    warehouse_count: int = 1
    store_employees_per_location_min: int = 20
    store_employees_per_location_max: int = 40
    warehouse_employees_per_location_min: int = 10
    warehouse_employees_per_location_max: int = 20


@dataclass
class LoyaltyConfig:
    signup_rate: float = 0.06
    loyalty_usage_rate: float = 0.40
    initial_member_count: int = 300


@dataclass
class CustomersConfig:
    """The customer / household master dimension (pos.customers).

    The taxonomy itself — which segments exist and which way each one's age
    and household-size distributions lean — lives in `models/customers.py` as
    named data, because that is the *definition* of a segment rather than an
    operator preference. What is tuned here is only the shape of the base:

    * `segment_shares` — relative weight per segment, overriding the module
      default for whichever keys are present. Unnormalised; the draw
      normalises, so overriding one segment does not require rescaling the
      other five.
    * `multi_member_household_share` — the chance a household keeps its next
      loyalty card instead of forming a new household. 0 means every card is
      its own household, 1 means every card joins the same one.
    * `household_size_max` — cap on the drawn household size. The segment
      distribution is renormalised over the sizes this allows, so raising the
      cap does not skew the mix toward small households.
    """
    segment_shares: Dict[str, float] = field(default_factory=dict)
    multi_member_household_share: float = 0.25
    household_size_max: int = 6


@dataclass
class PricingConfig:
    product_price_change_frequency_days: float = 14.0
    tax_rate: float = 0.07
    price_history_backfill_days: int = 90

    # --- Price -> demand elasticity (t_08deeddf) ---------------------------
    # Demand for a SKU responds to its own price relative to a reference
    # price:  units ~ (price / reference_price) ** default_price_elasticity
    # A negative elasticity is ordinary retail behaviour — raise the price,
    # sell fewer. -0.5 means a 10% price rise costs ~5% of units.
    default_price_elasticity: float = -0.5

    # Per-SKU spread around that default, drawn once at seed time. Real
    # catalogues are not homogeneous: nobody responds identically to a
    # cigarette price rise and an ice-cream price rise. 0 disables the spread
    # and makes every SKU share the default.
    elasticity_jitter: float = 0.15

    # Floor on a price relative to its reference, for the walk that seeds
    # price_history and for the demand curve alike: a price of zero (or below)
    # makes the ratio, and therefore the weight, undefined — and a real
    # grocer does not sell below cost forever either.
    price_min_ratio: float = 0.05

    # How much of a seeded price change is a market-wide move rather than a
    # SKU's own decision (0..1). A walk with no shared factor gives every
    # product an independent price path, so a product's own price change and
    # the market's move together 1:1 and a per-product elasticity regression
    # cannot separate them — the coincidental-elasticity trap t_08deeddf was
    # raised for. 0.65 leaves a real idiosyncratic component (0.35) for the
    # regression to key on. 1.0 would leave none and 0 would scale nothing;
    # neither is a working configuration, and the tests pin both ends.
    price_market_factor_weight: float = 0.65


@dataclass
class InventoryConfig:
    initial_stock_per_product: int = 200
    restock_check_frequency_hours: int = 24
    # Safety fraction: the slack a reorder carries on top of the demand it
    # expects to cover while the order is in transit. Was decorative until
    # t_959cd040 — nothing read it, while depletion floored at zero and sales
    # ignored the shelf, so a "shortage" could never even be observed. Now it
    # is the safety margin on a reorder sized from measured demand
    # (ordering.check_and_create_orders).
    restock_threshold_pct: float = 0.25
    # Days of measured demand a reorder is sized against. 1.0 means "cover
    # tomorrow at today's rate", which is the classic (q, r) lot-sizing view;
    # higher covers a longer stretch at the cost of more inventory and more
    # shrinkage exposure on perishables.
    reorder_demand_window_days: int = 1
    # Cap on a single computed reorder quantity, as a multiple of the seeded
    # reorder_qty. A SKU whose measured demand is enormous (or whose ledger has
    # no history yet) must not turn into an unbounded order line.
    reorder_qty_max_multiple: float = 4.0
    # Whether a sale is capped at on-hand. Always True in production: with it
    # off, the generator is the pre-t_959cd040 generator that sells stock it
    # does not have. It exists so the regression test can demonstrate the old
    # behaviour on demand, rather than only asserting the new one.
    enforce_stock_availability: bool = True


@dataclass
class GeneratorConfig:
    tick_interval_seconds: int = 30
    simulation_minutes_per_tick: int = 15
    # How far back a fresh database is backfilled. Also the horizon promotion
    # validity windows are back-dated to, so a promo seeded at the end of the
    # horizon covers every transaction that can reference it.
    backfill_lookback_days: int = 30


@dataclass
class CouponConfig:
    active_at_any_time: int = 8
    valid_duration_days: int = 14
    coupon_use_rate: float = 0.20


@dataclass
class TransportConfig:
    cost_per_mile: float = 1.85   # operational cost per mile (fuel + maintenance)


@dataclass
class OnlineConfig:
    orders_per_day_min: int = 90
    orders_per_day_max: int = 170
    pickup_share: float = 0.45
    service_fee_delivery: float = 5.99
    cancel_rate: float = 0.06     # per placed order, before ready
    noshow_rate: float = 0.05     # ready orders that pass window uncalled


@dataclass
class ComboDealConfig:
    active_at_any_time: int = 4
    valid_duration_days: int = 7
    combo_use_rate: float = 0.15


@dataclass
class ScenarioConfig:
    rush_hour_multiplier: float = 2.0
    rush_hour_hours: List[int] = field(default_factory=lambda: [9, 10, 11, 17, 18, 19])
    weekend_multiplier: float = 1.3
    weekend_labor_multiplier: float = 1.1
    promotion_discount_pct: float = 0.15
    promotion_departments: List[str] = field(default_factory=lambda: ['Produce', 'Dairy & Eggs', 'Snacks & Candy'])
    promotion_labor_multiplier: float = 1.15
    holiday_week_multiplier: float = 1.6
    holiday_labor_multiplier: float = 1.2
    double_coupon_multiplier: float = 2.0
    # New scenario defaults (verisim#12)
    inflation_price_modifier: float = 1.15
    inflation_loyalty_modifier: float = 0.85
    weather_volume_multiplier: float = 0.7
    weather_attendance_modifier: float = 0.75
    supply_disruption_shrinkage_modifier: float = 1.3
    regional_peak_stores: Dict[str, float] = field(default_factory=dict)
    deep_discount_price_modifier: float = 0.8


@dataclass
class WeatherConfig:
    """The synthetic weather covariate (t_2ab1fb0a).

    `severe_weather` used to be a manual-only switch, so grocery demand had no
    continuous weather covariate: the automatic calendar covered holidays only.
    These knobs describe the seeded series itself — a seasonal temperature
    swing, synoptic fronts, and the heat/pre-buy demand response. The two
    LOSS terms are deliberately NOT here: they are derived from the
    `severe_weather` scenario constants at use time (`weather.modifiers_for`) so
    the automatic series and the manual scenario cannot disagree about what a
    total storm does to the shop.

    `enabled: false` turns the whole series off and every hook becomes a no-op
    — the pre-t_2ab1fb0a generator, reachable at runtime with no code change.
    """
    enabled: bool = True

    # --- The temperature model ------------------------------------------
    # Annual mean temperature at the equator. The US band this produces is
    # 48F (45N) to 58F (26N) before the seasonal swing, which puts summer
    # highs in the mid-to-high 80s and January lows between 30F and 5F — so the
    # heat term is live in summer and genuinely below freezing up north. Both
    # ends matter: a band that never crossed `comfort_temp_f` would make the
    # heat->beverage response dead code.
    mean_temp_f: float = 72.0
    # Half the seasonal peak-to-trough swing, at 40N. Solved against the two
    # anchors that have to be true for the series to be usable: a 45N July
    # high near 89F (above comfort, so heat bites) and a 45N January low near
    # 5F (so `snow_temp_f` and the storm signal have something to act on).
    seasonal_amplitude_f: float = 29.0
    # °F the annual MEAN falls per degree of |latitude|. This is the term that
    # makes the covariate mean anything: with the mean flat and only the swing
    # growing, a 45N store bottomed out near 28F — "cold" that reads as mild.
    # 0.55 puts Minneapolis' annual mean low (~38F) near the real ~36F.
    latitude_temp_gradient_f: float = 0.55
    # Half-width of the day around its own mean: high = mean + range/2.
    diurnal_range_f: float = 18.0
    # Std-dev of the seeded day-to-day temperature anomaly (synoptic noise).
    daily_anomaly_std_f: float = 6.0
    # Used when a location row has no usable latitude (an old data dir seeded
    # before verisim#13 added the column).
    default_latitude: float = 39.8

    # --- The fronts ------------------------------------------------------
    # Storms per region-year, their length in days, and the severity band a
    # peak is drawn from. A front covers a whole REGION (see `front_seed`), so
    # every store in a state is under the same storm.
    fronts_per_year: int = 18
    front_length_min_days: int = 1
    front_length_max_days: int = 3
    front_peak_min: float = 0.35
    front_peak_max: float = 0.95
    # How much of a front's peak still shows on its first and last day. It is
    # what makes severity(tomorrow) readable on the day BEFORE the storm, which
    # is where the pre-buy bump comes from. 0 would make a front invisible
    # until the day it lands and the pre-buy signal unreachable.
    front_edge_fraction: float = 0.25
    # Peaks are biased toward winter by this much (±). Storm seasons are real.
    winter_severity_boost: float = 0.35
    # Per-store wobble in how hard the region's front lands: ± this fraction.
    # Keeps stores correlated without making them identical.
    local_severity_jitter: float = 0.20

    # --- Precipitation / cloud ------------------------------------------
    precip_base_chance: float = 0.25
    precip_severity_gain: float = 0.45
    precip_min_in: float = 0.05
    precip_max_in: float = 1.80
    # At or above this, precipitation is `rain` or `snow` rather than drizzle.
    precip_significant_in: float = 0.10
    # High temperature at or below which precipitation falls as snow.
    snow_temp_f: float = 34.0
    cloud_cover_std_pct: float = 12.0
    cloudy_threshold_pct: float = 55.0

    # --- The demand law --------------------------------------------------
    # Degrees above comfort_temp_f at which the heat term starts to bite, and
    # the demand added per degree. Heat -> beverages is the grocery case.
    comfort_temp_f: float = 78.0
    heat_demand_gain: float = 0.018
    # The storm-shop: demand added on a day whose TOMORROW is severe.
    pre_buy_gain: float = 0.25
    # Severity at or above which a store is flagged is_severe and the day is
    # tagged severe_storm. Below this a front is just weather.
    severe_threshold: float = 0.70
    # Clamps on the demand multiplier. The heat term is unbounded in the
    # temperature, so without a ceiling a 115°F day would multiply the shop by
    # 2.3; the floor keeps a cold snap's lost footfall from zeroing the day.
    demand_modifier_floor: float = 0.40
    demand_modifier_ceiling: float = 1.80
    # Attendance floors at the scenario's own value, never below this.
    attendance_modifier_floor: float = 0.30


@dataclass
class VendorConfig:
    """
    Vendors (t_57b1a1ab).

    Before this the vendor list was a hardcoded 6-element Python literal inside
    `models.inventory.seed_inventory` — unconfigurable without a code change, and
    with no properties: every vendor was equally good, because the only
    behaviour that hung off a vendor was `random.choice(SUPPLIERS)`.

    `vendors` is the catalogue: name, fulfilment model, and the three numbers
    that make one vendor behave differently from another — promised lead time
    and its spread, and the short-ship rate. Everything the rest of the model
    reads (whether a short-ship is creditable, how long a claim window is, how
    long the vendor takes to pay) travels with the vendor row so the numbers a
    mart measures are the numbers a manager can also tune.

    Defaults are the names the old literal carried, so a stock image with no
    `vendors:` block in config.yaml behaves like the pre-t_57b1a1ab generator's
    *catalogue* — though no longer like its flat 5% short rate.
    """
    vendors: List[Dict] = field(default_factory=lambda: [
        # Warehouse-shipped broadline vendors: longer lead times, and they
        # carry the majority of the catalogue.
        {'name': 'UNFI', 'code': 'UNFI', 'fulfillment_model': 'warehouse',
         'lead_time_mean_days': 2.0, 'lead_time_stddev_days': 0.6,
         'short_ship_rate': 0.06, 'credit_eligible': True, 'credit_window_days': 14},
        {'name': 'McLane Company', 'code': 'MCLN', 'fulfillment_model': 'warehouse',
         'lead_time_mean_days': 2.5, 'lead_time_stddev_days': 0.8,
         'short_ship_rate': 0.09, 'credit_eligible': True, 'credit_window_days': 14},
        {'name': 'C&S Wholesale Grocers', 'code': 'CSWG', 'fulfillment_model': 'warehouse',
         'lead_time_mean_days': 3.0, 'lead_time_stddev_days': 1.0,
         'short_ship_rate': 0.12, 'credit_eligible': False, 'credit_window_days': 10},
        {'name': 'Supervalu', 'code': 'SVAL', 'fulfillment_model': 'warehouse',
         'lead_time_mean_days': 2.0, 'lead_time_stddev_days': 0.5,
         'short_ship_rate': 0.05, 'credit_eligible': True, 'credit_window_days': 21},
        # DSD vendors own the perishable departments — they bring it to the
        # store rather than to our warehouse, so they move faster and are
        # credited on the spot.
        {'name': 'FreshFields Produce', 'code': 'FRFP', 'fulfillment_model': 'dsd',
         'lead_time_mean_days': 1.0, 'lead_time_stddev_days': 0.3,
         'short_ship_rate': 0.08, 'credit_eligible': True, 'credit_window_days': 7},
        {'name': "KeHE Distributors", 'code': 'KEHE', 'fulfillment_model': 'dsd',
         'lead_time_mean_days': 1.5, 'lead_time_stddev_days': 0.4,
         'short_ship_rate': 0.04, 'credit_eligible': True, 'credit_window_days': 14},
        {'name': 'Nash Finch', 'code': 'NASH', 'fulfillment_model': 'warehouse',
         'lead_time_mean_days': 3.5, 'lead_time_stddev_days': 1.2,
         'short_ship_rate': 0.15, 'credit_eligible': False, 'credit_window_days': 7},
    ])

    # Departments whose SKUs are delivered by DSD rather than through our
    # warehouse. Keyed by department name from `products.departments`.
    dsd_departments: List[str] = field(default_factory=lambda: ['Produce', 'Deli'])

    # How often a DSD vendor visits a store. Each DSD vendor delivers on this
    # many weekdays a week (0 = once a week), chosen evenly — real produce
    # vendors come far more often than a broadline DSD partner.
    dsd_deliveries_per_week: int = 4

    # What share of a DSD line a vendor fails to deliver, on top of
    # `short_ship_rate` — a DSD truck that misses its window leaves the shelf
    # empty the same morning, which is the perishable version of the problem.
    dsd_short_ship_bonus: float = 0.02

    # Share of a short-ship that is credited (of those that are creditable at
    # all). The rest is written off: in real grocery the claim is not always
    # worth the paperwork, and a mart that shows 100% recovery would be a lie.
    credit_claim_rate: float = 0.70

    # Share of submitted claims the vendor pays vs disputes.
    credit_pay_rate: float = 0.85

    # Days after submission before a claim we have not heard back on is
    # chased. A claim older than this is chased daily.
    credit_chase_after_days: int = 3


@dataclass
class Config:
    db_host: str = 'localhost'
    db_port: int = 5432
    db_user: str = 'verisim'
    db_password: str = 'verisim'
    db_name: str = 'grocery'
    conf_path: str = '/config/config.yaml'

    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    locations: LocationConfig = field(default_factory=LocationConfig)
    volumes: VolumeConfig = field(default_factory=VolumeConfig)
    loyalty: LoyaltyConfig = field(default_factory=LoyaltyConfig)
    customers: CustomersConfig = field(default_factory=CustomersConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    inventory: InventoryConfig = field(default_factory=InventoryConfig)
    vendors: VendorConfig = field(default_factory=VendorConfig)
    coupons: CouponConfig = field(default_factory=CouponConfig)
    combo_deals: ComboDealConfig = field(default_factory=ComboDealConfig)
    transport: TransportConfig = field(default_factory=TransportConfig)
    online: OnlineConfig = field(default_factory=OnlineConfig)
    scenarios: ScenarioConfig = field(default_factory=ScenarioConfig)
    weather: WeatherConfig = field(default_factory=WeatherConfig)

    # Department/product catalog (populated from YAML)
    departments: List[Dict] = field(default_factory=list)
    initial_product_count: int = 500


# ---------------------------------------------------------------------------
# Schema — every key this generator reads, as (yaml_path, attr_path).
# See base/config_schema.py for the rule shapes and why types are not restated
# here: each value is coerced to the annotation on the dataclass field this
# points at, so the table cannot drift from the dataclass it describes.
# ---------------------------------------------------------------------------
SCHEMA = (
    # generator
    (('generator', 'tick_interval_seconds'), ('generator', 'tick_interval_seconds')),
    (('generator', 'simulation_minutes_per_tick'), ('generator', 'simulation_minutes_per_tick')),
    (('generator', 'backfill_lookback_days'), ('generator', 'backfill_lookback_days')),
    # locations
    (('locations', 'store_count'), ('locations', 'store_count')),
    (('locations', 'warehouse_count'), ('locations', 'warehouse_count')),
    (('locations', 'store_employees_per_location'), ('locations',),
     {'min': 'store_employees_per_location_min', 'max': 'store_employees_per_location_max'}),
    (('locations', 'warehouse_employees_per_location'), ('locations',),
     {'min': 'warehouse_employees_per_location_min', 'max': 'warehouse_employees_per_location_max'}),
    # volumes
    (('volumes', 'pos_transactions_per_day'), ('volumes',),
     {'min': 'pos_transactions_per_day_min', 'max': 'pos_transactions_per_day_max'}),
    (('volumes', 'hourly_weights'), ('volumes', 'hourly_weights')),
    (('volumes', 'day_of_week_multipliers'), ('volumes', 'day_of_week_multipliers')),
    # loyalty
    (('loyalty', 'signup_rate'), ('loyalty', 'signup_rate')),
    (('loyalty', 'loyalty_usage_rate'), ('loyalty', 'loyalty_usage_rate')),
    (('loyalty', 'initial_member_count'), ('loyalty', 'initial_member_count')),
    # customers — the pos.customers household dimension (models/customers.py
    # owns the segment taxonomy; these three keys only shape the base).
    (('customers', 'segment_shares'), ('customers', 'segment_shares')),
    (('customers', 'multi_member_household_share'), ('customers', 'multi_member_household_share')),
    (('customers', 'household_size_max'), ('customers', 'household_size_max')),
    # pricing
    (('pricing', 'product_price_change_frequency_days'), ('pricing', 'product_price_change_frequency_days')),
    (('pricing', 'tax_rate'), ('pricing', 'tax_rate')),
    (('pricing', 'price_history_backfill_days'), ('pricing', 'price_history_backfill_days')),
    (('pricing', 'default_price_elasticity'), ('pricing', 'default_price_elasticity')),
    (('pricing', 'elasticity_jitter'), ('pricing', 'elasticity_jitter')),
    (('pricing', 'price_min_ratio'), ('pricing', 'price_min_ratio')),
    (('pricing', 'price_market_factor_weight'), ('pricing', 'price_market_factor_weight')),
    # inventory
    (('inventory', 'initial_stock_per_product'), ('inventory', 'initial_stock_per_product')),
    (('inventory', 'restock_check_frequency_hours'), ('inventory', 'restock_check_frequency_hours')),
    (('inventory', 'restock_threshold_pct'), ('inventory', 'restock_threshold_pct')),
    (('inventory', 'reorder_demand_window_days'), ('inventory', 'reorder_demand_window_days')),
    (('inventory', 'reorder_qty_max_multiple'), ('inventory', 'reorder_qty_max_multiple')),
    (('inventory', 'enforce_stock_availability'), ('inventory', 'enforce_stock_availability')),
    # vendors (t_57b1a1ab)
    #
    # `vendors.vendors` is a LIST OF DICTS and needs no per-field rules: a plain
    # rule owns its whole subtree and the coercer validates the inside (rule 2 in
    # config_schema's design notes), which is also how `products.departments` is
    # handled. `_set_path` REPLACES the attribute wholesale, so the merge onto
    # the declared defaults — so an omitted per-vendor key keeps its default
    # instead of silently becoming 0 — is done by `_merge_vendor_defaults`
    # below, which runs after validation and before the value is read.
    (('vendors', 'vendors'), ('vendors', 'vendors')),
    (('vendors', 'dsd_departments'), ('vendors', 'dsd_departments')),
    (('vendors', 'dsd_deliveries_per_week'), ('vendors', 'dsd_deliveries_per_week')),
    (('vendors', 'dsd_short_ship_bonus'), ('vendors', 'dsd_short_ship_bonus')),
    (('vendors', 'credit_claim_rate'), ('vendors', 'credit_claim_rate')),
    (('vendors', 'credit_pay_rate'), ('vendors', 'credit_pay_rate')),
    (('vendors', 'credit_chase_after_days'), ('vendors', 'credit_chase_after_days')),
    # coupons / combo deals
    (('coupons', 'active_at_any_time'), ('coupons', 'active_at_any_time')),
    (('coupons', 'valid_duration_days'), ('coupons', 'valid_duration_days')),
    (('coupons', 'coupon_use_rate'), ('coupons', 'coupon_use_rate')),
    (('combo_deals', 'active_at_any_time'), ('combo_deals', 'active_at_any_time')),
    (('combo_deals', 'valid_duration_days'), ('combo_deals', 'valid_duration_days')),
    (('combo_deals', 'combo_use_rate'), ('combo_deals', 'combo_use_rate')),
    # transport
    (('transport', 'cost_per_mile'), ('transport', 'cost_per_mile')),
    # online
    (('online', 'orders_per_day'), ('online',),
     {'min': 'orders_per_day_min', 'max': 'orders_per_day_max'}),
    (('online', 'pickup_share'), ('online', 'pickup_share')),
    (('online', 'service_fee_delivery'), ('online', 'service_fee_delivery')),
    (('online', 'cancel_rate'), ('online', 'cancel_rate')),
    (('online', 'noshow_rate'), ('online', 'noshow_rate')),
    # scenarios
    (('scenarios', 'rush_hour', 'volume_multiplier'), ('scenarios', 'rush_hour_multiplier')),
    (('scenarios', 'rush_hour', 'hours'), ('scenarios', 'rush_hour_hours')),
    (('scenarios', 'weekend', 'volume_multiplier'), ('scenarios', 'weekend_multiplier')),
    (('scenarios', 'weekend', 'labor_multiplier'), ('scenarios', 'weekend_labor_multiplier')),
    (('scenarios', 'holiday_week', 'volume_multiplier'), ('scenarios', 'holiday_week_multiplier')),
    (('scenarios', 'holiday_week', 'labor_multiplier'), ('scenarios', 'holiday_labor_multiplier')),
    (('scenarios', 'double_coupons', 'coupon_multiplier'), ('scenarios', 'double_coupon_multiplier')),
    (('scenarios', 'promotion', 'discount_pct'), ('scenarios', 'promotion_discount_pct')),
    (('scenarios', 'promotion', 'affected_departments'), ('scenarios', 'promotion_departments')),
    (('scenarios', 'promotion', 'labor_multiplier'), ('scenarios', 'promotion_labor_multiplier')),
    (('scenarios', 'inflation_pressure', 'price_modifier'), ('scenarios', 'inflation_price_modifier')),
    (('scenarios', 'inflation_pressure', 'loyalty_modifier'), ('scenarios', 'inflation_loyalty_modifier')),
    (('scenarios', 'severe_weather', 'volume_multiplier'), ('scenarios', 'weather_volume_multiplier')),
    (('scenarios', 'severe_weather', 'attendance_modifier'), ('scenarios', 'weather_attendance_modifier')),
    (('scenarios', 'supplier_disruption', 'shrinkage_modifier'), ('scenarios', 'supply_disruption_shrinkage_modifier')),
    (('scenarios', 'regional_peak', 'stores'), ('scenarios', 'regional_peak_stores')),
    (('scenarios', 'deep_discount', 'price_modifier'), ('scenarios', 'deep_discount_price_modifier')),
    # weather — the synthetic weather covariate (t_2ab1fb0a). Every key is
    # optional and the dataclass defaults are the working configuration, so an
    # absent `weather:` block is normal. The two `scenarios.severe_weather` keys
    # are deliberately NOT settable here: weather.modifiers_for derives its loss
    # terms from that block at use time, so the automatic series and the manual
    # scenario cannot drift apart.
    (('weather', 'enabled'), ('weather', 'enabled')),
    (('weather', 'fronts_per_year'), ('weather', 'fronts_per_year')),
    (('weather', 'front_length_min_days'), ('weather', 'front_length_min_days')),
    (('weather', 'front_length_max_days'), ('weather', 'front_length_max_days')),
    (('weather', 'mean_temp_f'), ('weather', 'mean_temp_f')),
    (('weather', 'seasonal_amplitude_f'), ('weather', 'seasonal_amplitude_f')),
    (('weather', 'latitude_temp_gradient_f'), ('weather', 'latitude_temp_gradient_f')),
    (('weather', 'diurnal_range_f'), ('weather', 'diurnal_range_f')),
    (('weather', 'daily_anomaly_std_f'), ('weather', 'daily_anomaly_std_f')),
    (('weather', 'default_latitude'), ('weather', 'default_latitude')),
    (('weather', 'front_peak_min'), ('weather', 'front_peak_min')),
    (('weather', 'front_peak_max'), ('weather', 'front_peak_max')),
    (('weather', 'front_edge_fraction'), ('weather', 'front_edge_fraction')),
    (('weather', 'winter_severity_boost'), ('weather', 'winter_severity_boost')),
    (('weather', 'local_severity_jitter'), ('weather', 'local_severity_jitter')),
    (('weather', 'precip_base_chance'), ('weather', 'precip_base_chance')),
    (('weather', 'precip_severity_gain'), ('weather', 'precip_severity_gain')),
    (('weather', 'precip_min_in'), ('weather', 'precip_min_in')),
    (('weather', 'precip_max_in'), ('weather', 'precip_max_in')),
    (('weather', 'precip_significant_in'), ('weather', 'precip_significant_in')),
    (('weather', 'snow_temp_f'), ('weather', 'snow_temp_f')),
    (('weather', 'cloud_cover_std_pct'), ('weather', 'cloud_cover_std_pct')),
    (('weather', 'cloudy_threshold_pct'), ('weather', 'cloudy_threshold_pct')),
    (('weather', 'comfort_temp_f'), ('weather', 'comfort_temp_f')),
    (('weather', 'heat_demand_gain'), ('weather', 'heat_demand_gain')),
    (('weather', 'pre_buy_gain'), ('weather', 'pre_buy_gain')),
    (('weather', 'severe_threshold'), ('weather', 'severe_threshold')),
    (('weather', 'demand_modifier_floor'), ('weather', 'demand_modifier_floor')),
    (('weather', 'demand_modifier_ceiling'), ('weather', 'demand_modifier_ceiling')),
    (('weather', 'attendance_modifier_floor'), ('weather', 'attendance_modifier_floor')),
    # products
    (('products', 'initial_count'), ('initial_product_count',)),
    (('products', 'departments'), ('departments',)),
)

# Keys this product accepts but does not read. They WARN (logged on every
# reload) rather than failing, because installs already ship them and removing
# them is a config change of its own — but nothing reads them, so they must not
# be left looking effective. Measured dead as of t_6081478a:
#   scenarios.holiday_week.coupon_use_boost
#   scenarios.double_coupons.volume_multiplier
KNOWN_UNUSED = {
    ('scenarios', 'holiday_week', 'coupon_use_boost'):
        'not read by the generator; holiday_week already forces a coupon '
        'multiplier of 1.5 in scenarios/scenario_engine.py',
    ('scenarios', 'double_coupons', 'volume_multiplier'):
        'not read; the double_coupons block scales only the coupon multiplier',
}


def _load_yaml(path: str) -> dict:
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def load_config() -> 'Config':
    conf_path = os.environ.get('CONF_PATH', '/config/config.yaml')
    cfg = Config(
        db_host=os.environ.get('POSTGRES_HOST', 'localhost'),
        db_port=int(os.environ.get('POSTGRES_PORT', 5432)),
        db_user=os.environ.get('POSTGRES_USER', 'verisim'),
        db_password=os.environ.get('POSTGRES_PASSWORD', 'verisim'),
        db_name=os.environ.get('POSTGRES_DB', 'grocery'),
        conf_path=conf_path,
    )
    _apply_yaml(cfg, _load_yaml(conf_path))
    return cfg


def reload_config(cfg: 'Config') -> 'Config':
    """Re-read the YAML and return an updated Config, keeping the DB env vars.

    A rejected document RAISES rather than returning. The caller is a generator
    mid-run; quietly continuing on the previous config would hide the very
    misconfiguration being reported. The message names every bad key, so one
    restart's worth of edits fixes it.
    """
    new_cfg = Config(
        db_host=cfg.db_host, db_port=cfg.db_port,
        db_user=cfg.db_user, db_password=cfg.db_password,
        db_name=cfg.db_name, conf_path=cfg.conf_path,
    )
    _apply_yaml(new_cfg, _load_yaml(cfg.conf_path))
    return new_cfg


def _merge_vendor_defaults(cfg: 'Config', block: dict) -> None:
    """
    Fold config.yaml's vendor entries onto the declared defaults.

    `validate_and_apply` assigns through `_set_path`, which REPLACES the
    attribute wholesale — correct for every other key, and wrong here. A config
    that tweaks one vendor's `credit_window_days` would otherwise arrive with
    that vendor's `short_ship_rate` reset to whatever `_coerce` made of an
    absent key, and the failure is silent and invisible: the vendor simply never
    shorts again.

    Entries are matched to a default by `code` (falling back to `name`), so a
    partial edit keeps every other field. An entry that matches no default is
    taken as-is — a brand-new vendor is a legitimate thing to add.

    Called from `_apply_yaml` AFTER validation, so this only ever sees a
    document the validator already accepted.
    """
    entries = block.get('vendors')
    if not entries:
        return
    # The DEFAULTS, not whatever `validate_and_apply` just wrote onto the
    # attribute. It replaces the field wholesale with the raw YAML list, so
    # reading cfg.vendors.vendors here would merge every entry against itself and
    # the "keep the declared default" behaviour this function exists for would
    # silently do nothing.
    defaults = VendorConfig().vendors
    by_key = {
        str(vendor.get('code') or vendor.get('name')): dict(vendor)
        for vendor in defaults
    }
    merged = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get('code') or entry.get('name'))
        vendor = dict(by_key.get(key, {}))
        # A key present but null means "unset this one", not "set it to null".
        vendor.update({k: v for k, v in entry.items() if v is not None})
        merged.append(vendor)
    if merged:
        cfg.vendors.vendors = merged


def _apply_yaml(cfg: 'Config', data: dict) -> None:
    if not data:
        return
    for warning in validate_and_apply(cfg, data, SCHEMA, unused=KNOWN_UNUSED,
                                      path_hint=cfg.conf_path, product='grocery'):
        log.warning('config.yaml: %s', warning)

    gen = data.get('generator', {})
    if 'tick_interval_seconds' in gen:
        cfg.generator.tick_interval_seconds = int(gen['tick_interval_seconds'])
    if 'simulation_minutes_per_tick' in gen:
        cfg.generator.simulation_minutes_per_tick = int(gen['simulation_minutes_per_tick'])
    if 'backfill_lookback_days' in gen:
        cfg.generator.backfill_lookback_days = int(gen['backfill_lookback_days'])

    loc = data.get('locations', {})
    if 'store_count' in loc:
        cfg.locations.store_count = int(loc['store_count'])
    if 'warehouse_count' in loc:
        cfg.locations.warehouse_count = int(loc['warehouse_count'])
    sepl = loc.get('store_employees_per_location', {})
    if 'min' in sepl:
        cfg.locations.store_employees_per_location_min = int(sepl['min'])
    if 'max' in sepl:
        cfg.locations.store_employees_per_location_max = int(sepl['max'])
    wepl = loc.get('warehouse_employees_per_location', {})
    if 'min' in wepl:
        cfg.locations.warehouse_employees_per_location_min = int(wepl['min'])
    if 'max' in wepl:
        cfg.locations.warehouse_employees_per_location_max = int(wepl['max'])

    vol = data.get('volumes', {})
    pos_d = vol.get('pos_transactions_per_day', {})
    if 'min' in pos_d:
        cfg.volumes.pos_transactions_per_day_min = int(pos_d['min'])
    if 'max' in pos_d:
        cfg.volumes.pos_transactions_per_day_max = int(pos_d['max'])
    if 'hourly_weights' in vol:
        cfg.volumes.hourly_weights = [float(x) for x in vol['hourly_weights']]
    if 'day_of_week_multipliers' in vol:
        cfg.volumes.day_of_week_multipliers = {k: float(v) for k, v in vol['day_of_week_multipliers'].items()}

    loy = data.get('loyalty', {})
    if 'signup_rate' in loy:
        cfg.loyalty.signup_rate = float(loy['signup_rate'])
    if 'loyalty_usage_rate' in loy:
        cfg.loyalty.loyalty_usage_rate = float(loy['loyalty_usage_rate'])
    if 'initial_member_count' in loy:
        cfg.loyalty.initial_member_count = int(loy['initial_member_count'])

    pri = data.get('pricing', {})
    if 'product_price_change_frequency_days' in pri:
        cfg.pricing.product_price_change_frequency_days = float(pri['product_price_change_frequency_days'])
    if 'tax_rate' in pri:
        cfg.pricing.tax_rate = float(pri['tax_rate'])
    if 'price_history_backfill_days' in pri:
        cfg.pricing.price_history_backfill_days = int(pri['price_history_backfill_days'])
    for key in ('default_price_elasticity', 'elasticity_jitter',
                'price_min_ratio', 'price_market_factor_weight'):
        if key in pri:
            setattr(cfg.pricing, key, float(pri[key]))

    inv = data.get('inventory', {})
    if 'initial_stock_per_product' in inv:
        cfg.inventory.initial_stock_per_product = int(inv['initial_stock_per_product'])
    if 'restock_threshold_pct' in inv:
        cfg.inventory.restock_threshold_pct = float(inv['restock_threshold_pct'])
    for key in ('reorder_demand_window_days', 'restock_check_frequency_hours'):
        if key in inv:
            setattr(cfg.inventory, key, int(inv[key]))
    for key in ('reorder_qty_max_multiple',):
        if key in inv:
            setattr(cfg.inventory, key, float(inv[key]))
    if 'enforce_stock_availability' in inv:
        cfg.inventory.enforce_stock_availability = bool(inv['enforce_stock_availability'])

    _merge_vendor_defaults(cfg, data.get('vendors', {}))

    cpn = data.get('coupons', {})
    if 'active_at_any_time' in cpn:
        cfg.coupons.active_at_any_time = int(cpn['active_at_any_time'])
    if 'valid_duration_days' in cpn:
        cfg.coupons.valid_duration_days = int(cpn['valid_duration_days'])
    if 'coupon_use_rate' in cpn:
        cfg.coupons.coupon_use_rate = float(cpn['coupon_use_rate'])

    cdl = data.get('combo_deals', {})
    if 'active_at_any_time' in cdl:
        cfg.combo_deals.active_at_any_time = int(cdl['active_at_any_time'])
    if 'valid_duration_days' in cdl:
        cfg.combo_deals.valid_duration_days = int(cdl['valid_duration_days'])
    if 'combo_use_rate' in cdl:
        cfg.combo_deals.combo_use_rate = float(cdl['combo_use_rate'])

    trp = data.get('transport', {})
    if 'cost_per_mile' in trp:
        cfg.transport.cost_per_mile = float(trp['cost_per_mile'])

    onl = data.get('online', {})
    opd = onl.get('orders_per_day', {})
    if 'min' in opd:
        cfg.online.orders_per_day_min = int(opd['min'])
    if 'max' in opd:
        cfg.online.orders_per_day_max = int(opd['max'])
    for key in ['pickup_share', 'service_fee_delivery', 'cancel_rate', 'noshow_rate']:
        if key in onl:
            setattr(cfg.online, key, float(onl[key]))

    sc = data.get('scenarios', {})
    rh = sc.get('rush_hour', {})
    if 'volume_multiplier' in rh:
        cfg.scenarios.rush_hour_multiplier = float(rh['volume_multiplier'])
    if 'hours' in rh:
        cfg.scenarios.rush_hour_hours = list(rh['hours'])

    # Existing scenarios
    we = sc.get('weekend', {})
    if 'volume_multiplier' in we:
        cfg.scenarios.weekend_multiplier = float(we['volume_multiplier'])
    if 'labor_multiplier' in we:
        cfg.scenarios.weekend_labor_multiplier = float(we['labor_multiplier'])

    hw = sc.get('holiday_week', {})
    if 'volume_multiplier' in hw:
        cfg.scenarios.holiday_week_multiplier = float(hw['volume_multiplier'])
    if 'labor_multiplier' in hw:
        cfg.scenarios.holiday_labor_multiplier = float(hw['labor_multiplier'])

    pro = sc.get('promotion', {})
    if 'labor_multiplier' in pro:
        cfg.scenarios.promotion_labor_multiplier = float(pro['labor_multiplier'])

    dc = sc.get('double_coupons', {})
    if 'coupon_multiplier' in dc:
        cfg.scenarios.double_coupon_multiplier = float(dc['coupon_multiplier'])

    # New scenarios (verisim#12)
    inf = sc.get('inflation_pressure', {})
    if 'price_modifier' in inf:
        cfg.scenarios.inflation_price_modifier = float(inf['price_modifier'])
    if 'loyalty_modifier' in inf:
        cfg.scenarios.inflation_loyalty_modifier = float(inf['loyalty_modifier'])

    sw = sc.get('severe_weather', {})
    if 'volume_multiplier' in sw:
        cfg.scenarios.weather_volume_multiplier = float(sw['volume_multiplier'])
    if 'attendance_modifier' in sw:
        cfg.scenarios.weather_attendance_modifier = float(sw['attendance_modifier'])

    sd = sc.get('supplier_disruption', {})
    if 'shrinkage_modifier' in sd:
        cfg.scenarios.supply_disruption_shrinkage_modifier = float(sd['shrinkage_modifier'])

    rp = sc.get('regional_peak', {})
    if 'stores' in rp:
        cfg.scenarios.regional_peak_stores = {str(k): float(v) for k, v in rp['stores'].items()}

    dd = sc.get('deep_discount', {})
    if 'price_modifier' in dd:
        cfg.scenarios.deep_discount_price_modifier = float(dd['price_modifier'])

    prods = data.get('products', {})
    if 'initial_count' in prods:
        cfg.initial_product_count = int(prods['initial_count'])
    if 'departments' in prods:
        cfg.departments = prods['departments']
