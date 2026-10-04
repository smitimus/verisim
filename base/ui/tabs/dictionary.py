"""📚 Data Dictionary tab.

Schema -> table -> column reference, straight from ui_lib.schema.

Split out of `base/ui/app.py` (t_c2eca5dd). The body below is the original,
unchanged; only the `render(ctx)` wrapper is new. It still closes over this
module's globals, which is why the imports above enumerate every name it reads
— a missed import surfaces as a NameError the first time a user opens this tab,
never at import time.
"""
from ui_lib.context import Context  # noqa: F401  (documents the render signature)
import pandas as pd
import streamlit as st

from ui_lib.schema import SCHEMA_DOCS_BY_INDUSTRY


def render(ctx):
    @st.fragment
    def _data_dictionary():
        _schema_docs = SCHEMA_DOCS_BY_INDUSTRY[industry]

        _schema_name = st.selectbox(
            "Schema", list(SCHEMA_TABLES.keys()), key="dd_schema"
        )

        _sdoc = _schema_docs.get(_schema_name, {})
        _tpairs = SCHEMA_TABLES.get(_schema_name, [])

        st.markdown(f"## {_schema_name} Schema")
        if _sdoc.get("description"):
            st.markdown(_sdoc["description"])
        if _sdoc.get("notes"):
            st.info(_sdoc["notes"])

        for _tkey, _tlabel in _tpairs:
            _tdoc = TABLE_DOCS.get(_tkey, {})
            if not _tdoc:
                continue
            st.divider()
            st.markdown(f"### {_tdoc['title']}")
            st.markdown(_tdoc["description"])
            _col_info, _col_rel = st.columns([3, 2])
            with _col_info:
                st.markdown("**Columns**")
                _col_df = pd.DataFrame(_tdoc["columns"], columns=["Column", "Type", "Description"])
                st.dataframe(
                    _col_df, use_container_width=True, hide_index=True,
                    height=min(35 * len(_tdoc["columns"]) + 38, 500),
                )
            with _col_rel:
                if _tdoc.get("relationships"):
                    st.markdown("**Relationships**")
                    for _r in _tdoc["relationships"]:
                        st.markdown(f"- `{_r}`")
                if _tdoc.get("notes"):
                    st.info(_tdoc["notes"])

    industry = ctx.industry
    pfx = ctx.pfx
    SCHEMA_TABLES = ctx.schema_tables
    TABLE_DOCS = ctx.table_docs

    _data_dictionary()
