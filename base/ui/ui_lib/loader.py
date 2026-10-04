"""`_load_table` — map a UI table name onto its API route.

The Table Explorer's one non-trivial piece: each table has its own route shape
(paged envelope vs flat list, and which filters it accepts), and that knowledge
lives here rather than in the widget code. `NEEDS_DATES` / `NEEDS_LOCATION` are
the per-table hints the explorer uses to decide which filters to show.

Split out of `base/ui/app.py` (t_c2eca5dd).
"""
import pandas as pd

from ui_lib.api import api_get

NEEDS_DATES = {
    "pos.transactions", "pos.transaction_items", "fuel.transactions",
    "inv.receipts", "inv.receipt_items",
    "timeclock.events", "ordering.store_orders", "transport.loads",
    "support.tickets", "support.ticket_comments", "support.ticket_actions",
    "voice.calls", "chat.sessions", "survey.surveys",
    "pos.returns",
    "online.orders",
}

# Tables that support location filter
NEEDS_LOCATION = {
    "hr.employees", "pos.transactions", "pos.transaction_items",
    "fuel.transactions", "fuel.pumps", "inv.stock_levels",
    "inv.receipts", "inv.receipt_items",
    "timeclock.events", "ordering.store_orders", "transport.loads",
    "online.orders",
}



def _load_table(table: str, start_date, end_date, loc_id, limit: int, extra: dict, pfx: str):
    sd = f"{start_date}T00:00:00" if start_date else None
    ed = f"{end_date}T23:59:59" if end_date else None

    def paged(path, params):
        r = api_get(path, params)
        if not r:
            return pd.DataFrame(), 0
        return pd.DataFrame(r.get("data", [])), r.get("total", 0)

    def flat(path, params):
        r = api_get(path, params)
        if not r:
            return pd.DataFrame(), 0
        rows = r.get("data", r) if isinstance(r, dict) else r
        if not isinstance(rows, list):
            rows = []
        return pd.DataFrame(rows), len(rows)

    p: dict = {}
    if loc_id:
        p["location_id"] = loc_id
    p["limit"] = limit

    # --- Shared tables ---
    if table == "hr.locations":
        return flat(f"{pfx}/hr/locations", {})
    if table == "hr.employees":
        p.update(extra)
        return flat(f"{pfx}/hr/employees", p)
    if table == "pos.transactions":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/pos/transactions", p)
    if table == "pos.transaction_items":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/pos/transaction-items", p)
    if table == "pos.products":
        p.update(extra)
        return flat(f"{pfx}/pos/products", p)
    if table == "pos.loyalty_members":
        p.update(extra)
        return paged(f"{pfx}/pos/loyalty-members", p)
    if table == "pos.price_history":
        return flat(f"{pfx}/pos/price-history", {"limit": limit})
    if table == "inv.stock_levels":
        p.update(extra)
        return flat(f"{pfx}/inventory/stock-levels", p)
    if table == "inv.receipts":
        p.update({"start_dt": sd, "end_dt": ed})
        return flat(f"{pfx}/inventory/receipts", p)
    if table == "inv.receipt_items":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/inventory/receipt-items", p)
    if table == "inv.products":
        return flat(f"{pfx}/inventory/products", {"limit": limit})
    if table == "control.generator_state":
        r = api_get(f"{pfx}/status")
        if not r:
            return pd.DataFrame(), 0
        return pd.DataFrame([r.get("state", {})]), 1
    if table == "control.generation_stats":
        # `paged`, not `flat`: the route returns the {data,total,...} envelope (t_ac80c514)
        # so the explorer's row count is the advertised total rather than the page length.
        n = extra.get("last_n_ticks", 100)
        return paged(f"{pfx}/stats/generation", {"last_n_ticks": n})

    # --- Gas-station-only tables ---
    if table == "fuel.transactions":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/fuel/transactions", p)
    if table == "fuel.grades":
        return flat(f"{pfx}/fuel/grades", {})
    if table == "fuel.price_history":
        return flat(f"{pfx}/fuel/price-history", {"limit": limit})
    if table == "fuel.pumps":
        return flat(f"{pfx}/fuel/pumps", p)

    # --- Grocery-only tables ---
    if table == "pos.returns":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("reason"):
            p["reason"] = extra["reason"]
        return paged(f"{pfx}/pos/returns", p)
    if table == "pos.return_items":
        return paged(f"{pfx}/pos/return-items", p)
    if table == "online.orders":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("status"):
            p["status"] = extra["status"]
        if extra.get("fulfillment_type"):
            p["fulfillment_type"] = extra["fulfillment_type"]
        return paged(f"{pfx}/online/orders", p)
    if table == "online.order_items":
        return paged(f"{pfx}/online/order-items", p)
    if table == "online.order_events":
        return paged(f"{pfx}/online/order-events", p)
    if table == "pos.departments":
        return flat(f"{pfx}/pos/departments", {})
    if table == "pos.coupons":
        return flat(f"{pfx}/pos/coupons", {})
    if table == "pos.combo_deals":
        return flat(f"{pfx}/pos/combo-deals", {})
    if table == "timeclock.events":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("employee_id"):
            p["employee_id"] = extra["employee_id"]
        return paged(f"{pfx}/timeclock/events", p)
    if table == "ordering.store_orders":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/ordering/orders", p)
    if table == "ordering.store_order_items":
        return flat(f"{pfx}/ordering/orders", {"limit": limit})
    if table == "fulfillment.orders":
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/fulfillment/orders", p)
    if table == "fulfillment.items":
        return paged(f"{pfx}/fulfillment/orders", p)
    if table == "transport.trucks":
        return flat(f"{pfx}/transport/trucks", {})
    if table == "transport.loads":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/transport/loads", p)

    # --- Support-only tables ---
    if table == "support.queues":
        return flat(f"{pfx}/queues", {})
    if table == "support.categories":
        return flat(f"{pfx}/categories", {})
    if table == "support.customers":
        if extra.get("tier"):
            p["tier"] = extra["tier"]
        return paged(f"{pfx}/customers", p)
    if table == "support.tickets":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("status"):
            p["status"] = extra["status"]
        if extra.get("priority"):
            p["priority"] = extra["priority"]
        if extra.get("channel"):
            p["channel"] = extra["channel"]
        return paged(f"{pfx}/tickets", p)
    if table == "support.ticket_comments":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/ticket-comments", p)
    if table == "support.ticket_actions":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("action_type"):
            p["action_type"] = extra["action_type"]
        return paged(f"{pfx}/ticket-actions", p)
    if table == "voice.calls":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("disposition"):
            p["disposition"] = extra["disposition"]
        return paged(f"{pfx}/voice/calls", p)
    if table == "chat.sessions":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/chat/sessions", p)
    if table == "chat.messages":
        return paged(f"{pfx}/chat/messages", {"limit": limit})
    if table == "survey.surveys":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("bucket"):
            p["bucket"] = extra["bucket"]
        return paged(f"{pfx}/surveys", p)
    if table == "training.courses":
        return flat(f"{pfx}/training/courses", {})
    if table == "training.assignments":
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/training/assignments", p)

    return pd.DataFrame(), 0
