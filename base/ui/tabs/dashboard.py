"""📊 Dashboard tab.

Generator status, today's counters, and the tick-ledger chart.

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

from ui_lib.api import api_get, status_badge


def render(ctx):
    @st.fragment(run_every=15)
    def _dashboard():
        status_data = api_get(f"{pfx}/status")
        today_data = api_get(f"{pfx}/stats/today")
        gen_stats = api_get(f"{pfx}/stats/generation", {"last_n_ticks": 200})

        if status_data:
            state = status_data.get("state", {})
            col_badge, col_scenario, col_tick = st.columns([2, 2, 3])
            col_badge.metric("Generator Status", status_badge(state))
            col_scenario.metric("Active Scenario", state.get("active_scenario", "—").replace("_", " ").title())
            last_tick = state.get("last_tick_at")
            col_tick.metric("Last Tick", last_tick[:19].replace("T", " ") if last_tick else "Never")
        else:
            st.error(f"Cannot reach API at `{pfx}/status`. Is the generator running?")

        st.divider()

        if today_data:
            state = (status_data or {}).get("state", {})
            if industry == "gas-station":
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("POS Transactions Today", f"{today_data.get('pos_transactions', 0):,}")
                c2.metric("Fuel Transactions Today", f"{today_data.get('fuel_transactions', 0):,}")
                c3.metric("Ticks Today", f"{today_data.get('ticks', 0):,}")
                c4.metric("Volume Multiplier", f"{state.get('volume_multiplier', 1.0):.1f}×" if status_data else "—")
            elif industry == "support":
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Tickets Today", f"{today_data.get('tickets', 0):,}")
                c2.metric("Calls Today", f"{today_data.get('calls', 0):,}")
                c3.metric("Chats Today", f"{today_data.get('chats', 0):,}")
                c4.metric("Survey Responses", f"{today_data.get('surveys', 0):,}")
                c5.metric("Volume Multiplier", f"{state.get('volume_multiplier', 1.0):.1f}×" if status_data else "—")
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("POS Transactions Today", f"{today_data.get('pos_transactions', 0):,}")
                c2.metric("Timeclock Events Today", f"{today_data.get('timeclock_events', 0):,}")
                c3.metric("Orders Today", f"{today_data.get('orders', 0):,}")
                c4.metric("Volume Multiplier", f"{state.get('volume_multiplier', 1.0):.1f}×" if status_data else "—")

        st.divider()

        if gen_stats:
            df = pd.DataFrame(gen_stats)
            if not df.empty:
                df["recorded_at"] = pd.to_datetime(df["recorded_at"])
                df = df.sort_values("recorded_at")

                col_a, col_b = st.columns(2)
                with col_a:
                    if industry == "support" and "tickets_generated" in df.columns:
                        st.subheader("Tickets Created per Tick")
                        fig = px.line(df, x="recorded_at", y="tickets_generated",
                                      color="scenario_tag",
                                      labels={"recorded_at": "Time", "tickets_generated": "Count"})
                        fig.update_layout(height=300, margin=dict(t=20, b=20))
                        st.plotly_chart(fig, use_container_width=True)
                    elif "pos_transactions_generated" in df.columns:
                        st.subheader("POS Transactions per Tick")
                        fig = px.line(df, x="recorded_at", y="pos_transactions_generated",
                                      color="scenario_tag",
                                      labels={"recorded_at": "Time", "pos_transactions_generated": "Count"})
                        fig.update_layout(height=300, margin=dict(t=20, b=20))
                        st.plotly_chart(fig, use_container_width=True)

                with col_b:
                    if industry == "gas-station" and "fuel_transactions_generated" in df.columns:
                        st.subheader("Fuel Transactions per Tick")
                        fig2 = px.line(df, x="recorded_at", y="fuel_transactions_generated",
                                       labels={"recorded_at": "Time", "fuel_transactions_generated": "Count"})
                        fig2.update_layout(height=300, margin=dict(t=20, b=20))
                        st.plotly_chart(fig2, use_container_width=True)
                    elif industry == "grocery" and "timeclock_events_generated" in df.columns:
                        st.subheader("Timeclock Events per Tick")
                        fig2 = px.line(df, x="recorded_at", y="timeclock_events_generated",
                                       labels={"recorded_at": "Time", "timeclock_events_generated": "Count"})
                        fig2.update_layout(height=300, margin=dict(t=20, b=20))
                        st.plotly_chart(fig2, use_container_width=True)
                    elif industry == "support" and "calls_generated" in df.columns:
                        st.subheader("ACD Calls per Tick")
                        fig2 = px.line(df, x="recorded_at", y="calls_generated",
                                       labels={"recorded_at": "Time", "calls_generated": "Count"})
                        fig2.update_layout(height=300, margin=dict(t=20, b=20))
                        st.plotly_chart(fig2, use_container_width=True)

                if "wall_clock_ms" in df.columns:
                    st.subheader("Tick Duration (ms)")
                    fig3 = px.bar(df.tail(50), x="recorded_at", y="wall_clock_ms",
                                  labels={"recorded_at": "Time", "wall_clock_ms": "ms"})
                    fig3.update_layout(height=200, margin=dict(t=10, b=10))
                    st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No generation stats yet. Start the generator to see live data.")

        bf = api_get(f"{pfx}/stats/backfill-progress")
        if bf and bf.get("in_progress"):
            st.subheader("Backfill Progress")
            st.progress(bf["pct_complete"] / 100,
                        text=f"{bf['pct_complete']}% — Day {bf['current']} of {bf['end']} ({bf['days_remaining']} days remaining)")

        st.divider()

        recent = api_get(f"{pfx}/stats/recent", {"minutes": 60}) if industry in ("grocery", "support") else None
        if recent and industry == "support":
            st.subheader("Last Hour")
            r1, r2, r3, r4, r5 = st.columns(5)
            r1.metric("Tickets", f"{recent.get('tickets', 0):,}")
            r2.metric("Calls", f"{recent.get('calls', 0):,}")
            r3.metric("Chats", f"{recent.get('chats', 0):,}")
            r4.metric("Survey Responses", f"{recent.get('surveys', 0):,}")
            r5.metric("Ticks", f"{recent.get('ticks', 0):,}")

            tkts = recent.get("recent_tickets", [])
            if tkts:
                df_recent = pd.DataFrame(tkts)
                if "created_dt" in df_recent.columns:
                    df_recent["created_dt"] = pd.to_datetime(df_recent["created_dt"]).dt.strftime("%H:%M:%S")
                show = [c for c in ["created_dt", "ticket_number", "subject", "queue", "status", "priority", "channel"] if c in df_recent.columns]
                st.dataframe(df_recent[show].rename(columns={
                    "created_dt": "Time", "ticket_number": "Ticket #", "subject": "Subject",
                    "queue": "Queue", "status": "Status", "priority": "Priority", "channel": "Channel"
                }), use_container_width=True, hide_index=True)
        elif recent:
            st.subheader("Last Hour")
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Transactions", f"{recent.get('pos_transactions', 0):,}")
            r2.metric("Timeclock Events", f"{recent.get('timeclock_events', 0):,}")
            r3.metric("Orders", f"{recent.get('orders', 0):,}")
            r4.metric("Ticks", f"{recent.get('ticks', 0):,}")

            txns = recent.get("recent_transactions", [])
            if txns:
                df_recent = pd.DataFrame(txns)
                if "transaction_dt" in df_recent.columns:
                    df_recent["transaction_dt"] = pd.to_datetime(df_recent["transaction_dt"]).dt.strftime("%H:%M:%S")
                if "total" in df_recent.columns:
                    df_recent["total"] = df_recent["total"].apply(lambda x: f"${float(x):.2f}" if x else "—")
                show = [c for c in ["transaction_dt", "store", "total", "payment_method", "scenario_tag"] if c in df_recent.columns]
                st.dataframe(df_recent[show].rename(columns={
                    "transaction_dt": "Time", "store": "Store", "total": "Total",
                    "payment_method": "Payment", "scenario_tag": "Scenario"
                }), use_container_width=True, hide_index=True)

        st.caption(f"Auto-refreshes every 15s · Last refresh: {datetime.now().strftime('%H:%M:%S')}")

    industry = ctx.industry
    pfx = ctx.pfx
    SCHEMA_TABLES = ctx.schema_tables
    TABLE_DOCS = ctx.table_docs

    _dashboard()
