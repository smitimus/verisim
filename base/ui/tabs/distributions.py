"""📈 Distributions tab.

Record counts by day and key classification, per industry.

Split out of `base/ui/app.py` (t_c2eca5dd). The body below is the original,
unchanged; only the `render(ctx)` wrapper is new. It still closes over this
module's globals, which is why the imports above enumerate every name it reads
— a missed import surfaces as a NameError the first time a user opens this tab,
never at import time.
"""
from ui_lib.context import Context  # noqa: F401  (documents the render signature)
from datetime import datetime, date, timedelta
import pandas as pd
import plotly.express as px
import streamlit as st

from ui_lib.api import api_get


def _support_distributions(dist, _bar):
    """Chart set for the support industry's distributions payload."""
    t_day = dist.get("tickets_by_day", [])
    if t_day:
        df_t = pd.DataFrame(t_day)
        df_t["day"] = pd.to_datetime(df_t["day"])
        fig = px.bar(df_t.melt(id_vars="day", value_vars=["ticket_count", "resolved_count"],
                               var_name="kind", value_name="count"),
                     x="day", y="count", color="kind",
                     title="Tickets per Day (created vs resolved)",
                     labels={"day": "Date", "count": "Tickets"})
        fig.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=320)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No ticket data in window.")

    c1, c2 = st.columns(2)
    with c1:
        _bar(dist.get("tickets_by_queue"), "queue_name", "ticket_count",
             "Tickets by Queue", labels={"queue_name": "Queue", "ticket_count": "Tickets"})
    with c2:
        _bar(dist.get("tickets_by_status"), "status", "ticket_count",
             "Tickets by Status", labels={"status": "Status", "ticket_count": "Tickets"})

    c3, c4 = st.columns(2)
    with c3:
        _bar(dist.get("tickets_by_priority"), "priority", "ticket_count",
             "Tickets by Priority", labels={"priority": "Priority", "ticket_count": "Tickets"})
    with c4:
        _bar(dist.get("tickets_by_channel"), "channel", "ticket_count",
             "Tickets by Channel", labels={"channel": "Channel", "ticket_count": "Tickets"})

    c5, c6 = st.columns(2)
    with c5:
        _bar(dist.get("tickets_by_sentiment"), "sentiment", "ticket_count",
             "Tickets by Sentiment", labels={"sentiment": "Sentiment", "ticket_count": "Tickets"})
    with c6:
        _bar(dist.get("employees_by_department"), "department", "employee_count",
             "Active Staff by Department", labels={"department": "Department", "employee_count": "Staff"})

    st.divider()
    st.markdown("#### Voice ACD")
    call_day = dist.get("calls_by_day", [])
    if call_day:
        df_c = pd.DataFrame(call_day)
        df_c["day"] = pd.to_datetime(df_c["day"])
        fig_c = px.bar(df_c, x="day", y="call_count",
                       title="Calls Offered per Day",
                       labels={"day": "Date", "call_count": "Calls"},
                       color_discrete_sequence=["#4C78A8"])
        fig_c2 = px.line(df_c, x="day", y="avg_wait_seconds",
                         title="Avg Queue Wait (s)",
                         labels={"day": "Date", "avg_wait_seconds": "Seconds"},
                         color_discrete_sequence=["#E45756"])
        fig_c.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=280)
        fig_c2.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=280)
        cc1, cc2 = st.columns(2)
        with cc1:
            st.plotly_chart(fig_c, use_container_width=True)
        with cc2:
            st.plotly_chart(fig_c2, use_container_width=True)
    else:
        st.info("No call data in window.")
    _bar(dist.get("calls_by_disposition"), "disposition", "call_count",
         "Calls by Disposition", labels={"disposition": "Disposition", "call_count": "Calls"})

    st.divider()
    st.markdown("#### Live Chat")
    chat_day = dist.get("chats_by_day", [])
    if chat_day:
        df_ch = pd.DataFrame(chat_day)
        df_ch["day"] = pd.to_datetime(df_ch["day"])
        fig_ch = px.bar(df_ch, x="day", y="session_count",
                        title="Chat Sessions per Day",
                        labels={"day": "Date", "session_count": "Sessions"},
                        color_discrete_sequence=["#72B7B2"])
        fig_ch.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
        st.plotly_chart(fig_ch, use_container_width=True)
    else:
        st.info("No chat data in window.")
    _bar(dist.get("chats_by_platform"), "platform", "session_count",
         "Chats by Platform", labels={"platform": "Platform", "session_count": "Sessions"})

    st.divider()
    st.markdown("#### sNPS Surveys")
    _bar(dist.get("surveys_by_bucket"), "bucket", "survey_count",
         "Survey Responses by sNPS Bucket",
         labels={"bucket": "Bucket", "survey_count": "Responses"})
    score = api_get("/support/surveys/scorecard", {"days": 30})
    if score:
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("sNPS (30d)", f"{score.get('snps') or 0:.1f}")
        s2.metric("Promoters", f"{score.get('promoter_pct') or 0:.1f}%")
        s3.metric("Detractors", f"{score.get('detractor_pct') or 0:.1f}%")
        s4.metric("Avg CSAT", f"{score.get('avg_csat') or 0:.2f}")

    st.divider()
    st.markdown("#### Training")
    _bar(dist.get("training_by_status"), "status", "assignment_count",
         "Training Assignments by Status",
         labels={"status": "Status", "assignment_count": "Assignments"})


def render(ctx):
    @st.fragment(run_every=15)
    def _distributions():
        st.subheader("Data Distributions")
        st.caption("Record counts grouped by day and key classifications. Adjust the look-back window to explore different time ranges.")

        _days_opts = {
            "Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30,
            "Last 60 days": 60, "Last 90 days": 90, "Last 180 days": 180, "Last year": 365
        }
        _days_sel = st.selectbox("Look-back window", list(_days_opts.keys()), index=2, key="dist_days")
        _days = _days_opts[_days_sel]

        dist = api_get(f"{pfx}/stats/distributions", {"days": _days})

        if not dist:
            st.warning("No distribution data available. Run the generator first.")
        else:
            def _bar(data, x, y, title, color=None, labels=None):
                if not data:
                    st.info(f"No data for: {title}")
                    return
                df = pd.DataFrame(data)
                fig = px.bar(df, x=x, y=y, title=title, color=color, labels=labels or {},
                             color_discrete_sequence=px.colors.qualitative.Safe)
                fig.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=320)
                st.plotly_chart(fig, use_container_width=True)

            if industry == "support":
                _support_distributions(dist, _bar)
                st.caption(f"Auto-refreshes every 15s · Last refresh: {datetime.now().strftime('%H:%M:%S')}")
                return

            st.markdown("#### POS Transactions")
            txn_day = dist.get("transactions_by_day", [])
            if txn_day:
                df_td = pd.DataFrame(txn_day)
                df_td["day"] = pd.to_datetime(df_td["day"])
                fig_td = px.bar(df_td, x="day", y="transaction_count",
                                title="Transactions per Day",
                                labels={"day": "Date", "transaction_count": "Transactions"},
                                color_discrete_sequence=["#4C78A8"])
                fig_td.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
                st.plotly_chart(fig_td, use_container_width=True)
            else:
                st.info("No transaction data in window.")

            dc1, dc2 = st.columns(2)
            with dc1:
                _bar(dist.get("transactions_by_store"), "store_name", "transaction_count",
                     "Transactions by Store", labels={"store_name": "Store", "transaction_count": "Transactions"})
            with dc2:
                _bar(dist.get("transactions_by_payment"), "payment_method", "transaction_count",
                     "Transactions by Payment Method",
                     labels={"payment_method": "Method", "transaction_count": "Transactions"})

            dc3, dc4 = st.columns(2)
            with dc3:
                _bar(dist.get("transactions_by_scenario"), "scenario_tag", "transaction_count",
                     "Transactions by Scenario",
                     labels={"scenario_tag": "Scenario", "transaction_count": "Transactions"})
            with dc4:
                _bar(dist.get("employees_by_department"), "department", "employee_count",
                     "Active Employees by Department",
                     labels={"department": "Department", "employee_count": "Employees"})

            if industry == "grocery":
                st.divider()
                st.markdown("#### Timeclock Events")
                tc_day = dist.get("timeclock_by_day", [])
                if tc_day:
                    df_tc = pd.DataFrame(tc_day)
                    df_tc["day"] = pd.to_datetime(df_tc["day"])
                    fig_tc = px.bar(df_tc, x="day", y="event_count",
                                    title="Timeclock Events per Day",
                                    labels={"day": "Date", "event_count": "Events"},
                                    color_discrete_sequence=["#72B7B2"])
                    fig_tc.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
                    st.plotly_chart(fig_tc, use_container_width=True)
                else:
                    st.info("No timeclock data in window.")

                gc1, gc2 = st.columns(2)
                with gc1:
                    _bar(dist.get("timeclock_by_type"), "event_type", "event_count",
                         "Events by Type",
                         labels={"event_type": "Type", "event_count": "Events"})
                with gc2:
                    _bar(dist.get("products_by_department"), "department_name", "product_count",
                         "Active Products by Department",
                         labels={"department_name": "Department", "product_count": "Products"})

                st.divider()
                st.markdown("#### Supply Chain Orders")
                ord_day = dist.get("orders_by_day", [])
                if ord_day:
                    df_od = pd.DataFrame(ord_day)
                    df_od["day"] = pd.to_datetime(df_od["day"])
                    fig_od = px.bar(df_od, x="day", y="order_count",
                                    title="Store Orders per Day",
                                    labels={"day": "Date", "order_count": "Orders"},
                                    color_discrete_sequence=["#F58518"])
                    fig_od.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
                    st.plotly_chart(fig_od, use_container_width=True)
                else:
                    st.info("No order data in window.")

                _bar(dist.get("orders_by_status"), "status", "order_count",
                     "Orders by Status",
                     labels={"status": "Status", "order_count": "Orders"})

                st.divider()
                st.markdown("#### Shrinkage / Loss")
                shr_day = dist.get("shrinkage_by_day", [])
                if shr_day:
                    df_sh = pd.DataFrame(shr_day)
                    df_sh["day"] = pd.to_datetime(df_sh["day"])
                    fig_sh = px.bar(df_sh, x="day", y="event_count",
                                    title="Shrinkage Events per Day",
                                    labels={"day": "Date", "event_count": "Events"},
                                    color_discrete_sequence=["#E45756"])
                    fig_sh.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
                    st.plotly_chart(fig_sh, use_container_width=True)
                else:
                    st.info("No shrinkage data in window.")

                _bar(dist.get("shrinkage_by_reason"), "reason", "event_count",
                     "Shrinkage by Reason",
                     labels={"reason": "Reason", "event_count": "Events"})

        st.caption(f"Auto-refreshes every 15s · Last refresh: {datetime.now().strftime('%H:%M:%S')}")

    industry = ctx.industry
    pfx = ctx.pfx
    SCHEMA_TABLES = ctx.schema_tables
    TABLE_DOCS = ctx.table_docs

    _distributions()
