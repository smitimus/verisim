"""🗄️ Table Explorer tab.

Browse any source table with filters, paging, and CSV export.

Split out of `base/ui/app.py` (t_c2eca5dd). The body below is the original,
unchanged; only the `render(ctx)` wrapper is new. It still closes over this
module's globals, which is why the imports above enumerate every name it reads
— a missed import surfaces as a NameError the first time a user opens this tab,
never at import time.
"""
from ui_lib.context import Context  # noqa: F401  (documents the render signature)
from datetime import datetime, date, timedelta
import pandas as pd
import streamlit as st

from ui_lib.api import api_get
from ui_lib.loader import NEEDS_DATES, NEEDS_LOCATION, _load_table


def render(ctx):
    @st.fragment
    def _table_explorer():
        st.subheader("Table Explorer")


        # ── Row 1: schema + table selectors ──────────────────────────────────────
        # Store an integer index (not a string value) so the selection is always
        # valid regardless of schema length. on_change resets to 0 on schema switch
        # (single rerun — avoids the double-rerun that resets the active tab).
        def _reset_table_idx():
            st.session_state["te_table_idx"] = 0

        col_s, col_t = st.columns([2, 5])
        with col_s:
            schema = st.selectbox(
                "Schema", list(SCHEMA_TABLES.keys()),
                key="te_schema",
                on_change=_reset_table_idx,
            )

        pairs = SCHEMA_TABLES[schema]
        tkeys = [p[0] for p in pairs]
        tlabels = {p[0]: p[1] for p in pairs}
        tdisplay = [f"{k}  —  {tlabels[k]}" for k in tkeys]

        # Clamp stored index to valid range before the widget reads it
        _tidx = min(st.session_state.get("te_table_idx", 0), len(tkeys) - 1)
        st.session_state["te_table_idx"] = _tidx

        with col_t:
            selected_idx = st.selectbox(
                "Table", list(range(len(tkeys))),
                format_func=lambda i: tdisplay[i],
                key="te_table_idx",
            )

        table = tkeys[selected_idx]

        # ── Row 2: date range (only for tables that need it) ─────────────────────
        start_date = end_date = None
        if table in NEEDS_DATES:
            cd1, cd2, _ = st.columns([2, 2, 3])
            with cd1:
                start_date = st.date_input("Start date", value=date.today() - timedelta(days=1), key="ex_sd")
            with cd2:
                end_date = st.date_input("End date", value=date.today(), key="ex_ed")

        # ── Row 3: location + table-specific filters + row limit ──────────────────
        filter_slots: list = []
        if table in NEEDS_LOCATION:
            filter_slots.append("location")
        # Table-specific filters
        if table == "hr.employees":
            if industry == "gas-station":
                filter_slots += ["gs_department", "employee_status"]
            elif industry == "support":
                filter_slots += ["sp_department", "employee_status"]
            else:
                filter_slots += ["gr_department", "employee_status"]
        elif table == "pos.returns":
            filter_slots.append("return_reason")
        elif table == "online.orders":
            filter_slots += ["online_status", "online_fulfillment"]
        elif table == "pos.products":
            filter_slots.append("category")
        elif table == "pos.loyalty_members":
            filter_slots.append("loyalty_tier")
        elif table == "inv.stock_levels":
            filter_slots.append("below_reorder")
        elif table == "control.generation_stats":
            filter_slots.append("last_n_ticks")
        elif table in ("ordering.store_orders", "fulfillment.orders", "transport.loads"):
            filter_slots.append("order_status")
        elif table == "support.tickets":
            filter_slots += ["ticket_status", "ticket_priority", "ticket_channel"]
        elif table == "voice.calls":
            filter_slots.append("call_disposition")
        elif table == "survey.surveys":
            filter_slots.append("survey_bucket")
        elif table == "training.assignments":
            filter_slots.append("training_status")
        filter_slots.append("row_limit")

        fcols = st.columns(min(len(filter_slots), 4))
        selected_loc_id = None
        extra: dict = {}
        limit = 500

        for fi, slot in enumerate(filter_slots):
            with fcols[fi % 4]:
                if slot == "location":
                    locs = api_get(f"{pfx}/hr/locations") or []
                    loc_opts = {"All Locations": None}
                    for loc in locs:
                        loc_opts[loc["name"]] = loc["location_id"]
                    loc_name = st.selectbox("Location", list(loc_opts.keys()), key="ex_loc")
                    selected_loc_id = loc_opts[loc_name]

                elif slot == "gs_department":
                    dept = st.selectbox("Department", ["All", "store", "fuel", "management"], key="ex_dept")
                    if dept != "All":
                        extra["department"] = dept

                elif slot == "gr_department":
                    gr_depts = ["All", "store", "produce", "deli", "bakery", "meat", "warehouse", "transport", "management"]
                    dept = st.selectbox("Department", gr_depts, key="ex_dept")
                    if dept != "All":
                        extra["department"] = dept

                elif slot == "sp_department":
                    sp_depts = ["All", "agent", "team_lead", "qa", "training", "management"]
                    dept = st.selectbox("Department", sp_depts, key="ex_dept")
                    if dept != "All":
                        extra["department"] = dept

                elif slot == "ticket_status":
                    ts = st.selectbox("Status", ["All", "new", "open", "pending", "resolved", "closed", "cancelled"], key="ex_tstatus")
                    if ts != "All":
                        extra["status"] = ts

                elif slot == "ticket_priority":
                    tp = st.selectbox("Priority", ["All", "low", "medium", "high", "urgent"], key="ex_tprio")
                    if tp != "All":
                        extra["priority"] = tp

                elif slot == "ticket_channel":
                    tc = st.selectbox("Channel", ["All", "email", "web", "phone", "chat", "social"], key="ex_tchan")
                    if tc != "All":
                        extra["channel"] = tc

                elif slot == "call_disposition":
                    cd = st.selectbox("Disposition", ["All", "resolved", "follow_up_ticket", "transferred", "voicemail", "abandoned_customer", "abandoned_timeout"], key="ex_cdisp")
                    if cd != "All":
                        extra["disposition"] = cd

                elif slot == "survey_bucket":
                    sb = st.selectbox("sNPS Bucket", ["All", "promoter", "passive", "detractor"], key="ex_sbucket")
                    if sb != "All":
                        extra["bucket"] = sb

                elif slot == "training_status":
                    tst = st.selectbox("Status", ["All", "assigned", "in_progress", "completed", "overdue", "expired"], key="ex_trstatus")
                    if tst != "All":
                        extra["status"] = tst

                elif slot == "employee_status":
                    es = st.selectbox("Status", ["All", "active", "terminated", "on_leave"], key="ex_estatus")
                    if es != "All":
                        extra["status"] = es

                elif slot == "return_reason":
                    rr = st.selectbox("Return Reason",
                                      ["All", "defective", "wrong_item", "changed_mind",
                                       "damaged_in_transit", "price_found_lower", "other"],
                                      key="ex_rreason")
                    if rr != "All":
                        extra["reason"] = rr

                elif slot == "online_status":
                    os_ = st.selectbox("Order Status",
                                       ["All", "placed", "confirmed", "picking", "ready",
                                        "out_for_delivery", "completed", "no_show", "cancelled"],
                                       key="ex_ostatus_o")
                    if os_ != "All":
                        extra["status"] = os_

                elif slot == "online_fulfillment":
                    ft = st.selectbox("Fulfillment", ["All", "pickup", "delivery"],
                                      key="ex_oftype")
                    if ft != "All":
                        extra["fulfillment_type"] = ft

                elif slot == "category":
                    if industry == "gas-station":
                        cats = ["All", "Beverages", "Snacks", "Tobacco", "Automotive", "Health & Beauty", "Food Service", "General Merchandise"]
                    else:
                        cats = ["All", "Fresh Produce", "Dairy", "Meat & Poultry", "Bakery", "Deli", "Frozen Foods",
                                "Grocery", "Beverages", "Snacks", "Health & Beauty", "General Merchandise"]
                    cat = st.selectbox("Category", cats, key="ex_cat")
                    if cat != "All":
                        extra["category"] = cat

                elif slot == "loyalty_tier":
                    tier = st.selectbox("Tier", ["All", "bronze", "silver", "gold", "platinum"], key="ex_tier")
                    if tier != "All":
                        extra["tier"] = tier

                elif slot == "below_reorder":
                    if st.checkbox("Below reorder point only", key="ex_brp"):
                        extra["below_reorder_point"] = True

                elif slot == "last_n_ticks":
                    extra["last_n_ticks"] = st.number_input("Last N ticks", 10, 1000, 100, 10, key="ex_nticks")

                elif slot == "order_status":
                    if table == "ordering.store_orders":
                        statuses = ["All", "pending", "approved", "shipped", "delivered"]
                    elif table == "fulfillment.orders":
                        statuses = ["All", "picking", "packed", "dispatched"]
                    else:
                        statuses = ["All", "dispatched", "delivered"]
                    os_val = st.selectbox("Status", statuses, key="ex_ostatus")
                    if os_val != "All":
                        extra["status"] = os_val

                elif slot == "row_limit":
                    limit = st.number_input("Row limit", 50, 5000, 500, 50, key="ex_limit")

        # ── Table documentation (always visible, collapsed by default) ────────────
        doc = TABLE_DOCS.get(table, {})
        no_data_loaded = "ex_df" not in st.session_state or st.session_state.get("ex_table_loaded") != table
        with st.expander("📋 Table Documentation", expanded=no_data_loaded):
            if doc:
                st.markdown(f"#### {doc['title']}")
                st.caption(doc["description"])
                col_info, col_rel = st.columns([3, 2])
                with col_info:
                    st.markdown("**Columns**")
                    col_df = pd.DataFrame(doc["columns"], columns=["Column", "Type", "Description"])
                    st.dataframe(col_df, use_container_width=True, hide_index=True, height=min(35 * len(doc["columns"]) + 38, 340))
                with col_rel:
                    if doc.get("relationships"):
                        st.markdown("**Relationships**")
                        for r in doc["relationships"]:
                            st.markdown(f"- `{r}`")
                    if doc.get("notes"):
                        st.info(doc["notes"])

        # ── Load button ───────────────────────────────────────────────────────────
        if st.button("⬇ Load Data", type="primary", key="ex_load"):
            with st.spinner(f"Loading {table}…"):
                df_result, total_result = _load_table(table, start_date, end_date, selected_loc_id, limit, extra, pfx)
            st.session_state["ex_df"] = df_result
            st.session_state["ex_total"] = total_result
            st.session_state["ex_table_loaded"] = table

        # ── Results grid ──────────────────────────────────────────────────────────
        if "ex_df" in st.session_state and st.session_state.get("ex_table_loaded") == table:
            df_show = st.session_state["ex_df"]
            total_show = st.session_state.get("ex_total", len(df_show))

            if df_show.empty:
                st.info("No rows returned for the selected filters.")
            else:
                st.caption(f"Showing **{len(df_show):,}** of **{total_show:,}** total rows in `{table}`")
                st.dataframe(df_show, use_container_width=True, hide_index=True)
                csv = df_show.to_csv(index=False)
                st.download_button(
                    "⬇️ Download CSV",
                    data=csv,
                    file_name=f"{table.replace('.', '_')}_{date.today()}.csv",
                    mime="text/csv",
                    key="ex_csv",
                )

    industry = ctx.industry
    pfx = ctx.pfx
    SCHEMA_TABLES = ctx.schema_tables
    TABLE_DOCS = ctx.table_docs

    _table_explorer()
