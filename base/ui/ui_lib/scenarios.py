"""
The scenario catalogue per industry — what each named scenario does to volume.

Pure data: label, icon, and the plain-English description the Scenarios tab
renders beside each preset.

Split out of `base/ui/app.py` (t_c2eca5dd).
"""
# ---------------------------------------------------------------------------
# Scenarios per industry
# ---------------------------------------------------------------------------

GAS_STATION_SCENARIOS = {
    "normal": {
        "label": "Normal",
        "icon": "🏪",
        "description": "Baseline traffic with realistic time-of-day and day-of-week patterns.",
    },
    "rush_hour": {
        "label": "Rush Hour",
        "icon": "🚗",
        "description": "2.5× volume during morning (7–9am) and evening (4–7pm) commute peaks.",
    },
    "weekend": {
        "label": "Weekend",
        "icon": "🎉",
        "description": "1.3× baseline volume. Simulates higher foot traffic on Friday and Saturday.",
    },
    "promotion": {
        "label": "Promotion",
        "icon": "🏷️",
        "description": "15% discount applied to Snacks and Beverages. Slightly higher volume.",
    },
    "fuel_spike": {
        "label": "Fuel Price Spike",
        "icon": "⬆️",
        "description": "Fuel prices increased by ~12%. Simulates a supply disruption or market event.",
    },
}

GROCERY_SCENARIOS = {
    "normal": {
        "label": "Normal",
        "icon": "🏪",
        "description": "Baseline traffic with grocery shopping patterns (morning and evening peaks).",
    },
    "rush_hour": {
        "label": "Rush Hour",
        "icon": "🚗",
        "description": "2.0× volume during after-work hours (4–7pm) and weekend mornings.",
    },
    "weekend": {
        "label": "Weekend",
        "icon": "🛒",
        "description": "1.3× baseline. Simulates higher weekend grocery shopping traffic.",
    },
    "promotion": {
        "label": "Promotion",
        "icon": "🏷️",
        "description": "15% discount on featured departments. Higher basket sizes.",
    },
    "holiday_week": {
        "label": "Holiday Week",
        "icon": "🦃",
        "description": "1.6× volume, increased produce and meat sales. Simulates Thanksgiving or holiday week shopping.",
    },
    "double_coupons": {
        "label": "Double Coupons",
        "icon": "✂️",
        "description": "Coupon value doubled. Higher loyalty member attach rate. Simulates a double-coupon event.",
    },
}

SUPPORT_SCENARIOS = {
    "normal": {
        "label": "Normal",
        "icon": "🎧",
        "description": "Baseline contact volume — business-hours curve, Monday peak, weekend dip.",
    },
    "rush_hour": {
        "label": "Rush Hour",
        "icon": "📞",
        "description": "1.6× contacts during business peak hours (9–11am, 2–4pm).",
    },
    "weekend": {
        "label": "Weekend",
        "icon": "🏖️",
        "description": "0.65× baseline volume. Reduced staffing contact flow.",
    },
    "service_outage": {
        "label": "Service Outage",
        "icon": "🔥",
        "description": "4× contact surge, negative sentiment, longer calls, higher abandonment.",
    },
    "weather_outage": {
        "label": "Weather Outage",
        "icon": "🌀",
        "description": "2.5× surge with elevated queue stress from regional disruption.",
    },
    "product_launch": {
        "label": "Product Launch",
        "icon": "🚀",
        "description": "1.8× contacts, how-to questions, longer handle times, training triggers.",
    },
    "marketing_blast": {
        "label": "Marketing Blast",
        "icon": "📣",
        "description": "2.2× contacts from a promo email — billing and account questions spike.",
    },
    "holiday_week": {
        "label": "Holiday Week",
        "icon": "🎄",
        "description": "1.4× volume with shipping/returns pressure and longer queues.",
    },
}

SCENARIOS_BY_INDUSTRY = {
    "gas-station": GAS_STATION_SCENARIOS,
    "grocery": GROCERY_SCENARIOS,
    "support": SUPPORT_SCENARIOS,
}
