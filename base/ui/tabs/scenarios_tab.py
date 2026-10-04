"""🎭 Scenarios tab.

Activate, override, and clear the named scenario presets.

Split out of `base/ui/app.py` (t_c2eca5dd). The body below is the original,
unchanged; only the `render(ctx)` wrapper is new. It still closes over this
module's globals, which is why the imports above enumerate every name it reads
— a missed import surfaces as a NameError the first time a user opens this tab,
never at import time.
"""
from ui_lib.context import Context  # noqa: F401  (documents the render signature)
import pandas as pd
import streamlit as st

from ui_lib.api import api_delete, api_get, api_patch, api_post
from ui_lib.scenarios import SCENARIOS_BY_INDUSTRY


def render(ctx):
    @st.fragment
    def _scenarios():
        st.subheader("Scenarios")

        SCENARIOS = SCENARIOS_BY_INDUSTRY[industry]

        if industry in ("grocery", "support"):
            active_scenarios_list = api_get(f"{pfx}/generator/scenarios") or []
            active_scenario_names = {s["scenario_name"] for s in active_scenarios_list}
        else:
            status_data3 = api_get(f"{pfx}/status")
            active_scenario_names = {(status_data3 or {}).get("state", {}).get("active_scenario", "normal")}

        cols = st.columns(3)
        for i, (key, info) in enumerate(SCENARIOS.items()):
            with cols[i % 3]:
                is_active = key in active_scenario_names
                badge = " ✅ Active" if is_active else ""
                with st.container(border=True):
                    st.markdown(f"### {info['icon']} {info['label']}{badge}")
                    st.caption(info["description"])
                    if industry in ("grocery", "support"):
                        if is_active:
                            if st.button(f"Deactivate", key=f"sc_off_{key}"):
                                api_delete(f"{pfx}/generator/scenarios/{key}")
                                st.rerun(scope="fragment")
                        else:
                            if st.button(f"Activate", key=f"sc_on_{key}"):
                                api_post(f"{pfx}/generator/scenarios", {"scenario_name": key})
                                st.rerun(scope="fragment")
                    else:
                        if not is_active:
                            if st.button(f"Activate {info['label']}", key=f"sc_{key}"):
                                api_patch(f"{pfx}/generator/config", {"active_scenario": key})
                                st.rerun(scope="fragment")

        if industry in ("grocery", "support"):
            st.divider()
            st.subheader("Scenario Schedules")
            st.caption("Scheduled scenarios automatically activate during backfill and realtime generation on their date range.")

            schedules = api_get(f"{pfx}/generator/scenario-schedules") or []
            if schedules:
                df_sched = pd.DataFrame(schedules)
                for _, row in df_sched.iterrows():
                    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 3, 1])
                    c1.write(row["scenario_name"].replace("_", " ").title())
                    c2.write(str(row["start_date"]))
                    c3.write(str(row["end_date"]))
                    c4.write(row.get("label") or "")
                    if c5.button("✕", key=f"del_sched_{row['schedule_id']}"):
                        api_delete(f"{pfx}/generator/scenario-schedules/{row['schedule_id']}")
                        st.rerun(scope="fragment")
            else:
                st.info("No scenario schedules. Add one below.")

            with st.expander("Add Schedule"):
                sc_col1, sc_col2, sc_col3 = st.columns(3)
                sched_scenario = sc_col1.selectbox("Scenario", [k for k in SCENARIOS if k != "normal"], key="sched_sc")
                sched_start = sc_col2.date_input("Start date", key="sched_start")
                sched_end = sc_col3.date_input("End date", key="sched_end")
                sched_label = st.text_input("Label (optional)", key="sched_label", placeholder="e.g. Summer Sale")
                if st.button("Add Schedule", type="primary"):
                    if sched_end < sched_start:
                        st.error("End date must be after start date.")
                    else:
                        api_post(f"{pfx}/generator/scenario-schedules", {
                            "scenario_name": sched_scenario,
                            "start_date": str(sched_start),
                            "end_date": str(sched_end),
                            "label": sched_label or None,
                        })
                        st.rerun(scope="fragment")

        st.divider()

        if industry == "gas-station":
            st.subheader("Current Fuel Prices")
            grades = api_get(f"{pfx}/fuel/grades")
            if grades:
                df_g = pd.DataFrame(grades)[["name", "octane_rating", "current_price", "updated_at"]]
                df_g.columns = ["Grade", "Octane", "Price/Gallon", "Last Updated"]
                df_g["Price/Gallon"] = df_g["Price/Gallon"].apply(lambda x: f"${float(x):.4f}")
                st.table(df_g)

            price_hist = api_get(f"{pfx}/fuel/price-history", {"limit": 50})
            if price_hist:
                st.subheader("Recent Fuel Price Changes")
                df_ph = pd.DataFrame(price_hist)[["grade_name", "old_price", "new_price", "changed_at"]]
                df_ph.columns = ["Grade", "Old Price", "New Price", "Changed At"]
                st.dataframe(df_ph, use_container_width=True, hide_index=True)

        else:
            st.subheader("Active Combo Deals")
            deals = api_get(f"{pfx}/pos/combo-deals")
            if deals:
                df_d = pd.DataFrame(deals)
                show_cols = [c for c in ["name", "deal_type", "trigger_qty", "deal_price", "valid_from", "valid_until"] if c in df_d.columns]
                if show_cols:
                    st.dataframe(df_d[show_cols], use_container_width=True, hide_index=True)
            else:
                st.info("No active combo deals.")

    industry = ctx.industry
    pfx = ctx.pfx
    SCHEMA_TABLES = ctx.schema_tables
    TABLE_DOCS = ctx.table_docs

    _scenarios()
