"""
Online orders model — e-commerce channel (pickup + delivery) (t_24fae529).

Own source schema (`online`) like a real company's web-order platform.
Basket behavior is deliberately different from in-store POS:

  * bigger baskets (weekly haul: 8-30 lines vs 1-12 in store)
  * card/mobile payment only (no cash/EBT), no coupons or combo deals
  * ordering skew to browsing hours (8am-9pm), lighter than store peaks
  * service fee on delivery; pickup has a 2-hour pickup window
  * append-only lifecycle event stream; ~3-8% of orders never complete
    (cancel before ready, no_show after ready_for_pickup window closes)

Online demand depletes store stock exactly like POS does.
"""
import random
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

ONLINE_HOURS = list(range(7, 22))          # ordering window: 7am-9pm
BASKET_WEIGHTS = [10, 12, 14, 13, 11, 9, 7, 6, 5]   # sizes 8..16+ (compressed)


def _online_count_for_tick(cfg: Config, ctx, sim_dt: datetime) -> int:
    """Orders this tick, weighted to browsing hours."""
    daily = random.randint(cfg.online.orders_per_day_min, cfg.online.orders_per_day_max)
    hour_w = cfg.volumes.hourly_weights[sim_dt.hour]
    return max(0, round(daily * hour_w * ctx.volume_multiplier))


def generate_online_orders(conn, cfg: Config, sim_dt: datetime, ctx,
                           store_locations: List[Dict], products: List[Dict],
                           members: List[Dict]) -> List[Dict]:
    """Create placed orders (status='placed', 'placed' event). Returns depletion info."""
    n = _online_count_for_tick(cfg, ctx, sim_dt)
    if n <= 0 or not store_locations or not products:
        return []

    order_records = []
    item_records = []
    depletion_info = []
    event_records = []

    member_ids = [m['member_id'] for m in members]

    for _ in range(n):
        loc = random.choice(store_locations)
        ftype = 'pickup' if random.random() < cfg.online.pickup_share else 'delivery'

        # weekly-haul basket: 8-30 lines, mostly whole units
        num_items = min(len(products), random.randint(8, 30))
        cart = random.choices(products, k=num_items)

        order_id = str(uuid.uuid4())
        subtotal = 0.0
        txn_items = []
        item_recs = []
        for product in cart:
            if product['uom'] == 'lb':
                qty = round(random.uniform(0.5, 2.0), 3)
            else:
                qty = random.choices([1, 2, 3, 4], weights=[60, 22, 12, 6])[0]
            unit_price = product['price']
            line_total = round(unit_price * qty, 2)
            subtotal += line_total
            item_recs.append((order_id, product['product_id'], qty,
                              unit_price, line_total))
            txn_items.append({'product_id': product['product_id'], 'quantity': qty})

        subtotal = round(subtotal, 2)
        service_fee = 0.0 if ftype == 'pickup' else cfg.online.service_fee_delivery
        tax = round(subtotal * cfg.pricing.tax_rate, 2)   # fee not taxed (grocery norm)
        total = round(subtotal + service_fee + tax, 2)

        payment = random.choices(['credit', 'debit', 'mobile_pay'],
                                 weights=[0.55, 0.30, 0.15])[0]
        member_id = (random.choice(member_ids)
                     if member_ids and random.random() < cfg.loyalty.loyalty_usage_rate
                     else None)

        window_start = sim_dt + timedelta(hours=2)
        window_start = window_start.replace(minute=random.choice([0, 15, 30, 45]))
        promised = None
        if ftype == 'delivery':
            promised = window_start + timedelta(hours=random.choice([2, 4]))

        order_records.append((
            order_id, loc['location_id'], member_id, sim_dt, ftype, 'placed',
            subtotal, service_fee, tax, total, payment,
            window_start if ftype == 'pickup' else None,
            (window_start + timedelta(hours=2)) if ftype == 'pickup' else None,
            promised, ctx.scenario_tag))
        item_records.extend(item_recs)
        event_records.append((order_id, 'placed', sim_dt,
                              f"Order placed via web/app — {ftype}"))
        depletion_info.append({'transaction_id': order_id, 'items': txn_items})

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO online.orders
                (order_id, location_id, member_id, placed_dt, fulfillment_type,
                 status, subtotal, service_fee, tax, total, payment_method,
                 pickup_window_start, pickup_window_end, promised_delivery_dt,
                 scenario_tag)
            VALUES %s
        """, order_records,
        template="(%s::uuid,%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        execute_values(cur, """
            INSERT INTO online.order_items
                (order_id, product_id, quantity, unit_price, line_total)
            VALUES %s
        """, item_records,
        template="(%s::uuid,%s::uuid,%s,%s,%s)")
        execute_values(cur, """
            INSERT INTO online.order_events (order_id, event_type, event_dt, note)
            VALUES %s
        """, event_records,
        template="(%s::uuid,%s,%s,%s)")
        conn.commit()

    return depletion_info


def advance_online_lifecycle(conn, cfg: Config, sim_dt: datetime) -> int:
    """
    Move open orders through their lifecycle based on elapsed time.
    Each order advances at most one stage per tick. Returns events written.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_id::text, fulfillment_type, status, placed_dt::timestamp,
                   pickup_window_end::timestamp
            FROM online.orders
            WHERE status IN ('placed','confirmed','picking','ready','out_for_delivery')
            ORDER BY placed_dt
            LIMIT 400
        """, )
        rows = cur.fetchall()

        events = []
        updates = []
        for oid, ftype, status, placed, pwe in rows:
            age_min = (sim_dt - placed).total_seconds() / 60
            if status == 'placed':
                # cancelled-any-time before ready
                if random.random() < cfg.online.cancel_rate:
                    if age_min > 5:
                        updates.append(('cancelled', None, oid))
                        events.append((oid, 'cancelled', sim_dt,
                                       'Customer cancelled from account page'))
                elif age_min >= random.randint(2, 10):
                    updates.append(('confirmed', None, oid))
                    events.append((oid, 'confirmed', sim_dt,
                                   'Order accepted, queued for picker'))
            elif status == 'confirmed':
                if age_min >= random.randint(15, 45):
                    updates.append(('picking', None, oid))
                    events.append((oid, 'picking_started', sim_dt, None))
            elif status == 'picking':
                if age_min >= random.randint(60, 120):
                    if ftype == 'pickup':
                        updates.append(('ready', None, oid))
                        events.append((oid, 'ready_for_pickup', sim_dt,
                                       'Staged in pickup area; SMS sent'))
                    else:
                        updates.append(('out_for_delivery', None, oid))
                        events.append((oid, 'driver_assigned', sim_dt, None))
                        events.append((oid, 'out_for_delivery', sim_dt,
                                       'Route 12, ETA 45 min'))
            elif status == 'ready':
                if pwe and sim_dt > pwe:
                    # Window closed: late-comers mostly collect, the rest no-show
                    # (noshow_rate gates the uncalled share; restock happens in
                    # the shrinkage model as 'returned' goods implicitly).
                    if random.random() < (1.0 - cfg.online.noshow_rate * 4):
                        updates.append(('completed', sim_dt, oid))
                        events.append((oid, 'picked_up', sim_dt,
                                       'Late pickup past window'))
                    else:
                        # no_show: window closed, goods go back to store stock
                        # later via shrinkage/restock review — completed_dt
                        # stays NULL (order was never completed).
                        updates.append(('no_show', None, oid))
                        events.append((oid, 'no_show', sim_dt,
                                       'Pickup window closed, no collection'))
                elif random.random() < 0.35:
                    updates.append(('completed', sim_dt, oid))
                    events.append((oid, 'picked_up', sim_dt, None))
            elif status == 'out_for_delivery':
                if random.random() < 0.6:
                    updates.append(('completed', sim_dt, oid))
                    events.append((oid, 'delivered', sim_dt, 'Left at door'))

        if updates:
            execute_values(cur, """
                UPDATE online.orders AS o
                SET status = v.st,
                    completed_dt = NULLIF(v.cd, '')::timestamptz,
                    updated_at = NOW()
                FROM (VALUES %s) AS v(st, cd, id)
                WHERE o.order_id = v.id::uuid
            """, [(st, cd.isoformat(sep=' ') if cd else '', oid)
                  for st, cd, oid in updates])
        if events:
            execute_values(cur, """
                INSERT INTO online.order_events (order_id, event_type, event_dt, note)
                VALUES %s
            """, events,
            template="(%s::uuid,%s,%s,%s)")
        conn.commit()
    return len(events)


def fetch_open_online_orders(conn) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_id::text, fulfillment_type, status, placed_dt
            FROM online.orders
            WHERE status IN ('placed','confirmed','picking','ready','out_for_delivery')
        """)
        return [{'order_id': r[0], 'fulfillment_type': r[1], 'status': r[2],
                 'placed_dt': r[3]} for r in cur.fetchall()]
