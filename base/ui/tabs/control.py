"""⚙️ Generator Control tab.

Start / stop / pause / resume, the mode selector, and the config form.

Split out of `base/ui/app.py` (t_c2eca5dd). The body below is the original,
unchanged; only the `render(ctx)` wrapper is new. It still closes over this
module's globals, which is why the imports above enumerate every name it reads
— a missed import surfaces as a NameError the first time a user opens this tab,
never at import time.
"""
from ui_lib.context import Context  # noqa: F401  (documents the render signature)
from datetime import datetime, date, timedelta
import streamlit as st

from ui_lib.api import api_get, api_patch, api_post


def render(ctx):
    @st.fragment
    def _generator_control():
        st.subheader("Generator Control")
        status_data2 = api_get(f"{pfx}/status")
        state2 = status_data2.get("state", {}) if status_data2 else {}

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### Start / Stop")
            b1, b2, b3, b4 = st.columns(4)
            if b1.button("▶ Start", type="primary", use_container_width=True):
                mode = st.session_state.get("start_mode", "realtime")
                payload = {"mode": mode}
                if mode == "backfill":
                    bf_start = st.session_state.get("bf_start", date.today() - timedelta(days=7))
                    bf_end   = st.session_state.get("bf_end",   date.today() - timedelta(days=1))
                    if not bf_start or not bf_end:
                        st.error("Backfill requires both start and end dates.")
                        st.stop()
                    if bf_end <= bf_start:
                        st.error(f"End date ({bf_end}) must be after start date ({bf_start}).")
                        st.stop()
                    payload["backfill_start"] = str(bf_start)
                    payload["backfill_end"]   = str(bf_end)
                result = api_post(f"{pfx}/generator/start", payload)
                if result:
                    if mode == "backfill":
                        st.toast(f"Backfill queued: {payload['backfill_start']} → {payload['backfill_end']}. Generator will pick it up within {state2.get('tick_interval_seconds', 30)}s.", icon="🔵")
                    else:
                        st.toast("Generator started in realtime mode.", icon="🟢")
                    st.rerun(scope="fragment")
            if b2.button("⏹ Stop", use_container_width=True):
                api_post(f"{pfx}/generator/stop")
                st.rerun(scope="fragment")
            if b3.button("⏸ Pause", use_container_width=True):
                api_post(f"{pfx}/generator/pause")
                st.rerun(scope="fragment")
            if b4.button("▶▶ Resume", use_container_width=True):
                api_post(f"{pfx}/generator/resume")
                st.rerun(scope="fragment")

            st.markdown("#### Mode")
            mode_choice = st.radio("Generation mode", ["realtime", "backfill"], horizontal=True, key="start_mode")
            if mode_choice == "backfill":
                st.date_input("Backfill start date", value=date.today() - timedelta(days=30), key="bf_start")
                st.date_input("Backfill end date", value=date.today() - timedelta(days=1), key="bf_end")
                st.caption("Set dates above, then click ▶ Start to begin backfill.")

            st.markdown("#### Volume & Timing")
            new_multiplier = st.slider(
                "Volume multiplier", min_value=0.1, max_value=5.0, step=0.1,
                value=float(state2.get("volume_multiplier", 1.0))
            )
            tick_options = {15: "15 sec", 30: "30 sec", 60: "1 min", 300: "5 min", 600: "10 min"}
            current_tick = int(state2.get("tick_interval_seconds", 30))
            tick_choice = st.selectbox(
                "Tick interval", options=list(tick_options.keys()),
                format_func=lambda x: tick_options[x],
                index=list(tick_options.keys()).index(current_tick) if current_tick in tick_options else 1
            )
            if st.button("Save Config", type="secondary"):
                api_patch(f"{pfx}/generator/config", {
                    "volume_multiplier": new_multiplier,
                    "tick_interval_seconds": tick_choice,
                })
                st.success("Config saved.")
                st.rerun(scope="fragment")

        with col_right:
            st.markdown("#### Current State")
            if state2:
                st.json(state2)

            bf2 = api_get(f"{pfx}/stats/backfill-progress")
            if bf2 and bf2.get("in_progress"):
                st.markdown("#### Backfill Progress")
                st.progress(bf2["pct_complete"] / 100,
                            text=f"{bf2['pct_complete']}% complete — {bf2['days_remaining']} days remaining")

    industry = ctx.industry
    pfx = ctx.pfx
    SCHEMA_TABLES = ctx.schema_tables
    TABLE_DOCS = ctx.table_docs

    _generator_control()
