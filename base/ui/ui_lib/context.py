"""The per-render context every tab receives.

A `@st.fragment` closes over the globals of the module that DEFINES it, so a tab
that used to close over `app.py`'s globals will, once moved, resolve free names
against its own module. Passing what each tab needs explicitly is what keeps the
move invisible: `ctx.industry` reads the same whether the fragment is defined in
`tabs/dashboard.py` or in `app.py`.

Split out of `base/ui/app.py` (t_c2eca5dd).
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Context:
    """Everything a tab needs that is not module-level reference data."""

    industry: str
    pfx: str
    schema_tables: dict
    table_docs: dict
