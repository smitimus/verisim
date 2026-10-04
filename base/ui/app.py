"""
Verisim — Multi-Industry Data Generator Control Panel
8 tabs: Dashboard, Generator Control, Scenarios, Promotions, Distributions,
Table Explorer, Documentation, Data Dictionary.

Entry point. The tab bodies live in `tabs/` and the shared vocabulary in
`ui_lib/` (split out of this file in t_c2eca5dd, which was 2793 lines holding
all eight tabs plus four large data tables):

  ui_lib/api.py        API_BASE_URL, api_get/post/patch/delete, status_badge
  ui_lib/schema.py     per-industry table docs, table lists, schema docs
  ui_lib/scenarios.py  the scenario catalogue per industry
  ui_lib/loader.py     _load_table: table name -> API route
  ui_lib/context.py    the Context each tab receives

Each tab module exposes `render(ctx)`. The context is explicit because a
`@st.fragment` closes over its defining module's globals: passing `industry`,
`pfx` and the schema tables in is what stops a moved tab from silently reading
the wrong globals.
"""
import streamlit as st

# Streamlit runs this file as a script and puts its own directory on sys.path
# (bootstrap._fix_sys_path), so `ui_lib` and `tabs` are importable by name.
from ui_lib.api import API, INDUSTRY_ORDER, get_available_industries
from ui_lib.context import Context
from ui_lib.schema import (
    SCHEMA_DOCS_BY_INDUSTRY,
    SCHEMA_TABLES_BY_INDUSTRY,
    TABLE_DOCS_BY_INDUSTRY,
)

st.set_page_config(
    page_title="Verisim — Data Generator",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Industry selector — driven by /health; hidden when only one industry is up
# ---------------------------------------------------------------------------

INDUSTRY_META = {
    "gas-station": ("⛽", "Gas Station"),
    "grocery":     ("🛒", "Grocery"),
    "support":     ("🎧", "Customer Support"),
}

available = get_available_industries()

if not available:
    st.error(f"Cannot reach API at `{API}`. Is the stack running?")
    st.stop()

if len(available) == 1:
    industry = available[0]
else:
    _labels = [INDUSTRY_META.get(s, ("🏭", s.replace("-", " ").title()))[1] for s in available]
    _sel = st.radio(
        "Industry",
        _labels,
        horizontal=True,
        key="industry_selector",
        label_visibility="collapsed",
    )
    industry = available[_labels.index(_sel)]

industry_icon, industry_label = INDUSTRY_META.get(
    industry, ("🏭", industry.replace("-", " ").title())
)
pfx = f"/{industry}"

st.title(f"{industry_icon} Verisim — {industry_label}")

ctx = Context(
    industry=industry,
    pfx=pfx,
    schema_tables=SCHEMA_TABLES_BY_INDUSTRY[industry],
    table_docs=TABLE_DOCS_BY_INDUSTRY[industry],
)

# ---------------------------------------------------------------------------
# Main layout — one module per tab, imported lazily-by-name at module scope
# ---------------------------------------------------------------------------

from tabs import control, dashboard, dictionary, distributions, docs, explorer  # noqa: E402
from tabs import promotions, scenarios_tab  # noqa: E402

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "📊 Dashboard", "⚙️ Generator Control", "🎭 Scenarios", "🏷️ Promotions",
    "📈 Distributions", "🗄️ Table Explorer", "📖 Documentation", "📚 Data Dictionary"
])

with tab1:
    dashboard.render(ctx)
with tab2:
    control.render(ctx)
with tab3:
    scenarios_tab.render(ctx)
with tab4:
    promotions.render(ctx)
with tab5:
    distributions.render(ctx)
with tab6:
    explorer.render(ctx)
with tab7:
    docs.render(ctx)
with tab8:
    dictionary.render(ctx)
