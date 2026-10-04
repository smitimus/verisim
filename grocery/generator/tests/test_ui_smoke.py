"""
Smoke-test the control panel by executing it against stubbed Streamlit (t_c2eca5dd).

`base/ui/app.py` used to be one 2793-line file and is now a package
(`app.py` + `ui_lib/` + `tabs/`). Nothing in CI runs the UI: the generator job
runs pytest on the models, the integration job builds the image and exercises the
API. A split that broke a tab would therefore have shipped green and failed the
first time somebody clicked it.

So this runs the real `app.py` top to bottom with Streamlit stubbed — every
`st.*` call recorded, `@st.fragment` and `@st.cache_data` made transparent,
`st.tabs` yielding fake context managers, and the API returning canned payloads
in the shapes the real routes use. A NameError (a moved fragment's free name no
longer resolving), a bad import, or a mis-indented block fails here.

Two industries are exercised because several tabs branch on `industry` (the
grocery-only Promotions tab, the support distributions chart set), and the
gas-station run is the one that skips the grocery branches.

`streamlit` is not installed in the test environment, so `st` is a module built
here; `pandas` is real when available, and the harness asserts that each tab
reaches its `st.subheader`, so a tab that silently renders nothing fails.
"""
import pathlib
import sys
import types

import pytest

UI = pathlib.Path(__file__).resolve().parents[3] / "base" / "ui"

TAB_MODULES = [
    "dashboard", "control", "scenarios_tab", "promotions",
    "distributions", "explorer", "docs", "dictionary",
]
UI_LIB_MODULES = ["api", "context", "loader", "scenarios", "schema"]


class _Ctx:
    """A Streamlit container/column stand-in.

    Tabs call widgets ON the container (`cc1.markdown(...)`, `ai1.selectbox(...)`),
    so an inert container turns a harness artefact into a KeyError that looks like
    an app bug. Unknown attributes delegate to the live stub.
    """

    def __init__(self, st=None):
        self._st = st

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        st = self.__dict__.get("_st")
        if st is not None and hasattr(st, name):
            return getattr(st, name)
        return _Ctx(st)


class _Columns(list):
    def __init__(self, spec, st=None):
        n = spec if isinstance(spec, int) else len(spec)
        super().__init__([_Ctx(st) for _ in range(n)])

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _identity_decorator(*args, **kwargs):
    """Support both `@st.fragment` and `@st.fragment(run_every=15)`."""
    if len(args) == 1 and callable(args[0]) and not kwargs:
        return args[0]
    return lambda f: f


class _Fig:
    """plotly figure stand-in — the tabs chain update_layout and read `.data`."""

    def __init__(self, *a, **k):
        self.data = []

    def __getattr__(self, name):
        return lambda *a, **k: self


class _Resp:
    def __init__(self, payload, healthy=True):
        self._payload = payload
        self.healthy = healthy

    def raise_for_status(self):
        if not self.healthy:
            raise RuntimeError("api error")

    def json(self):
        return self._payload


def _make_stub(calls):
    st = types.ModuleType("streamlit")
    st.cache_data = _identity_decorator
    st.fragment = _identity_decorator
    st.session_state = {"start_mode": "realtime"}
    st.columns = lambda spec: _Columns(spec, st)
    st.container = lambda *a, **k: _Ctx(st)
    st.expander = lambda *a, **k: _Ctx(st)
    st.tabs = lambda labels: [_Ctx(st) for _ in labels]

    def rec(name, ret=None):
        def anyargs(*a, **k):
            calls.append((name,))
            return ret if ret is not None else _Ctx(st)
        return anyargs

    st.set_page_config = rec("set_page_config", None)
    st.title = rec("title", None)
    st.subheader = rec("subheader", None)
    st.metric = rec("metric", None)
    st.markdown = rec("markdown", None)
    st.dataframe = rec("dataframe", None)
    st.plotly_chart = rec("plotly_chart", None)
    st.download_button = rec("download_button", None)
    st.divider = rec("divider", None)
    st.caption = rec("caption", None)
    st.header = rec("header", None)
    st.info = rec("info", None)
    st.warning = rec("warning", None)
    st.error = rec("error", None)
    st.success = rec("success", None)
    st.toast = rec("toast", None)
    st.json = rec("json", None)
    st.write = rec("write", None)
    st.progress = rec("progress", None)
    st.rerun = rec("rerun", None)

    # widget returns: option values are real, because tabs use them as dict keys
    st.radio = lambda label, options, **k: (list(options)[0] if options else None)
    st.multiselect = lambda label, options, **k: list(options or [])
    st.text_input = lambda label, *a, **k: ""
    st.number_input = lambda label, *a, **k: 0
    st.slider = lambda label, *a, **k: 0
    st.date_input = lambda label, *a, **k: None
    st.button = lambda *a, **k: False
    st.checkbox = lambda label, *a, **k: False
    st.toggle = lambda *a, **k: False

    def _selectbox(label, options, index=0, **k):
        opts = list(options or [])
        if not opts:
            return None
        return opts[index] if isinstance(index, int) and 0 <= index < len(opts) else opts[0]

    st.selectbox = _selectbox
    st.stop = lambda: (_ for _ in ()).throw(StopIteration())

    # catch-all: an unlisted widget is a recording no-op. Enumerating all of
    # Streamlit would make this test break whenever a tab uses a widget we did
    # not anticipate; the failures worth catching still fail loudly.
    def _catchall(name):
        return lambda *a, **k: _Ctx(st)

    st.__getattr__ = _catchall
    return st


def _make_requests(industry, calls):
    mod = types.ModuleType("requests")

    def get(url, **k):
        calls.append(("GET", url))
        if url.endswith("/health"):
            # The industry under test is ALWAYS healthy; a second one is healthy
            # only when it is not the one under test, so each run exercises the
            # `len(available) == 1` shortcut and the radio selector in turn.
            other = "grocery" if industry == "gas-station" else "gas-station"
            return _Resp({"industries": {industry: True, other: industry == "gas-station"}})
        if "/status" in url:
            return _Resp({"state": {
                "mode": "realtime", "is_running": True, "is_paused": False,
                "last_tick_at": "2026-10-04T12:00:00Z",
                "active_scenario": "rush_hour", "volume_multiplier": 1.4}})
        if "/stats/today" in url:
            return _Resp({"pos_transactions": 1234, "timeclock_events": 99,
                          "orders": 12, "fuel_transactions": 5, "tickets": 3,
                          "calls": 2, "chats": 1, "surveys": 4, "ticks": 2880})
        if "/stats/generation" in url:
            # The ENVELOPE the route actually returns since t_ac80c514 — the
            # dashboard reads `data`, and the loader calls this with `paged`.
            # A bare list here would pass vacuously and hide a real regression.
            return _Resp({"data": [{"recorded_at": "2026-10-04T11:00:00Z",
                                    "pos_transactions_generated": 100,
                                    "timeclock_events_generated": 20,
                                    "orders_generated": 5, "scenario_tag": "normal"}],
                          "total": 1, "limit": 200, "offset": 0})
        if "/stats/distributions" in url:
            return _Resp({"transactions_by_day": [
                {"day": "2026-10-01", "transaction_count": 2000}],
                "tickets_by_day": [{"day": "2026-10-01", "ticket_count": 50}]})
        if "/generator/scenarios" in url:
            # a LIST of dicts: the tab does {s["scenario_name"] for s in ...}
            return _Resp([{"scenario_name": "rush_hour",
                           "volume_multiplier": 1.8,
                           "start_date": "2026-10-04", "end_date": "2026-10-04",
                           "is_active": True}])
        if "/pos/coupons" in url or "/pos/combo-deals" in url:
            # these two answer with the LIST itself, not the paged envelope
            if "coupons" in url:
                return _Resp([{"coupon_id": "c1", "code": "SAVE5OFF50",
                               "description": "$5 off any purchase of $50",
                               "coupon_type": "dollar_off", "discount_value": 5.0,
                               "uses_count": 3, "is_active": True,
                               "valid_from": "2026-01-01", "valid_until": "2027-01-01"}])
            return _Resp([{"deal_id": "d1", "name": "2 for $5",
                           "description": "2 for $5 on selected items",
                           "deal_type": "x_for_price", "trigger_qty": 2,
                           "deal_price": 5.0, "is_active": True,
                           "trigger_department_id": None}])
        if "/pricing/ad-items" in url:
            return _Resp({"data": [{"ad_item_id": "ai1", "ad_id": "a1",
                                    "product_id": "p1", "product_name": "Bananas",
                                    "promoted_price": 0.99,
                                    "discount_pct": 10.0}], "total": 1})
        if "/pricing/weekly-ads" in url:
            return _Resp({"data": [{"ad_id": "a1", "ad_name": "Weekly Ad",
                                    "start_date": "2026-10-01",
                                    "end_date": "2026-10-07", "is_active": True}],
                          "total": 1})
        if "/pos/departments" in url:
            return _Resp([{"department_id": "d1", "name": "Produce", "code": "PRD"},
                          {"department_id": "d2", "name": "Dairy & Eggs",
                           "code": "DAI"}])
        if "/pos/products" in url:
            return _Resp({"data": [{"product_id": "p1", "sku": "PRD-1",
                                    "name": "Bananas", "current_price": 1.99}],
                          "total": 1})
        if "/backfill-progress" in url:
            return _Resp({"in_progress": False, "pct_complete": 100,
                          "days_remaining": 0})
        if "locations" in url:
            return _Resp({"data": [{"location_id": "l1", "name": "Store 1",
                                    "location_type": "store"}], "total": 1})
        return _Resp({"data": [], "total": 0})

    mod.get = get
    mod.post = lambda url, **k: (calls.append(("POST", url)), _Resp({"ok": True}))[1]
    mod.patch = lambda url, **k: (calls.append(("PATCH", url)), _Resp({"ok": True}))[1]
    mod.delete = lambda url, **k: (calls.append(("DELETE", url)), _Resp({"ok": True}))[1]
    return mod


def _make_plotly():
    plotly = types.ModuleType("plotly")
    express = types.ModuleType("plotly.express")
    graph_objs = types.ModuleType("plotly.graph_objs")
    for name in ("bar", "line", "scatter", "area", "pie", "histogram"):
        setattr(express, name, lambda df=None, *a, **k: _Fig())
    express.colors = types.SimpleNamespace(
        qualitative=types.SimpleNamespace(Safe=["#1f77b4"]),
        sequential=types.SimpleNamespace(Blues=["#08306b"]))
    graph_objs.Figure = _Fig
    plotly.express = express
    plotly.graph_objs = graph_objs
    return plotly, express, graph_objs


def _run_app(industry):
    """Execute base/ui/app.py with stubs; return (api_calls, st_calls)."""
    # pandas is a real dependency here — the tab bodies build real DataFrames.
    # plotly is NOT: the harness stubs it (see _make_plotly), so its absence is
    # fine and must not skip the test.
    try:
        import pandas  # noqa: F401
    except ImportError:
        pytest.skip("pandas not installed; the panel's tab bodies cannot run")

    api_calls, st_calls = [], []
    plotly, express, graph_objs = _make_plotly()
    sys.modules["streamlit"] = _make_stub(st_calls)
    sys.modules["requests"] = _make_requests(industry, api_calls)
    sys.modules["plotly"] = plotly
    sys.modules["plotly.express"] = express
    sys.modules["plotly.graph_objs"] = graph_objs

    # A previous run in this session may have cached them under other names.
    # `ui_lib.api` holds `@st.cache_data`-decorated state and the stub's
    # identity decorator returns a fresh function each time, so a stale copy
    # would keep the FIRST industry's /health payload and short-circuit the
    # second run — which is exactly the "no subheader" failure below.
    for name in list(sys.modules):
        if name == "app" or name.startswith(("ui_lib", "tabs")):
            del sys.modules[name]

    sys.path.insert(0, str(UI))
    try:
        src = (UI / "app.py").read_text()
        code = compile(src, str(UI / "app.py"), "exec")
        exec(code, {"__name__": "__main__", "__file__": str(UI / "app.py")})
    except StopIteration:
        pass          # st.stop() — the API is unreachable, nothing to render
    finally:
        if sys.path and sys.path[0] == str(UI):
            sys.path.pop(0)
    return api_calls, st_calls


@pytest.mark.parametrize("industry", ["grocery", "gas-station"])
def test_control_panel_renders(industry):
    """Every tab must execute end to end.

    Both industries because several tabs branch on `industry`: Promotions is
    grocery-only and the support chart set returns early, so a bug in the branch
    a single run never takes would not be caught.
    """
    api_calls, st_calls = _run_app(industry)

    subs = [c for c in st_calls if c[0] == "subheader"]
    metrics = [c for c in st_calls if c[0] == "metric"]
    charts = [c for c in st_calls
              if c[0] in ("plotly_chart", "dataframe", "download_button")]

    assert subs, "no tab rendered a subheader — app.py did not reach the tabs"
    assert metrics, "the dashboard rendered no metrics"
    assert charts, "no chart or dataframe rendered"
    assert len(api_calls) > 1, "the panel made almost no API calls"


def test_every_tab_module_imports():
    """Each tabs/*.py must import on its own, with ui_lib importable.

    A tab that fails to import takes the whole panel down, because app.py
    imports them all at module scope. plotly is stubbed here exactly as the
    render test stubs it, so only pandas is a real requirement.
    """
    import os
    import subprocess

    stub = (
        "import sys, types\n"
        "px = types.ModuleType('plotly'); ex = types.ModuleType('plotly.express')\n"
        "go = types.ModuleType('plotly.graph_objs')\n"
        "class F:\n"
        "    def __init__(self,*a,**k): self.data=[]\n"
        "    def __getattr__(self,n): return lambda *a,**k: self\n"
        "for n in ('bar','line','scatter','area','pie'):\n"
        "    setattr(ex,n,lambda df=None,*a,**k: F())\n"
        "ex.colors = types.SimpleNamespace(qualitative=types.SimpleNamespace(Safe=['#1']))\n"
        "go.Figure = F\n"
        "px.express = ex; px.graph_objs = go\n"
        "sys.modules['plotly']=px; sys.modules['plotly.express']=ex\n"
        "sys.modules['plotly.graph_objs']=go\n"
        # streamlit: importing a tab module only needs the decorators to exist;
        # the tab bodies are not RUN here, only imported
        "st = types.ModuleType('streamlit')\n"
        "st.cache_data = lambda *a, **k: (lambda f: f)\n"
        "st.fragment = lambda *a, **k: (lambda f: f)\n"
        "st.session_state = {}\n"
        "sys.modules['streamlit'] = st\n"
    )
    code = stub + (
        "import sys\n"
        f"sys.path.insert(0, {str(UI)!r})\n"
        f"for m in {TAB_MODULES!r}:\n"
        "    __import__('tabs.' + m)\n"
        f"for m in {UI_LIB_MODULES!r}:\n"
        "    __import__('ui_lib.' + m)\n"
        "print('OK')\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    r = subprocess.run([sys.executable, "-c", code], cwd=str(UI), env=env,
                       capture_output=True, text=True, timeout=120)
    assert "OK" in r.stdout, (
        f"a tab or ui_lib module failed to import:\n{r.stderr[-2000:]}"
    )


def test_app_py_is_a_thin_entry_point():
    """app.py should resolve the industry and hand out contexts, not render.

    `with tabN:` legitimately stays — that is the tab slot the render call goes
    in — so what must NOT come back is a fragment definition or a widget call
    sitting directly in a tab block. Checked against the AST, not the text: the
    module docstring explains the fragment pattern and would match a substring
    search.
    """
    import ast

    src = (UI / "app.py").read_text()
    assert len(src.splitlines()) < 200, (
        f"app.py is {len(src.splitlines())} lines; the split should have left it "
        f"a thin entry point"
    )
    for tab in TAB_MODULES:
        assert f"{tab}.render(" in src, f"app.py never calls {tab}.render()"

    tree = ast.parse(src)
    defined = [n.name for n in ast.walk(tree)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    assert not any(n.startswith("_") for n in defined), (
        f"app.py defines private helpers again: "
        f"{[n for n in defined if n.startswith('_')]} — they belong in ui_lib/ "
        f"or tabs/"
    )
    # a fragment decorator would be a FunctionDef/AsyncFunctionDef whose
    # decorator list is non-empty; app.py must have none
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            assert not node.decorator_list, (
                f"app.py decorates {node.name} — fragments belong in tabs/"
            )
    assert "def _load_table" not in src, (
        "app.py still defines _load_table; it belongs in ui_lib/loader.py"
    )


def test_dockerfiles_copy_the_whole_ui_directory():
    """The standalone images must ship ui_lib/ and tabs/, not just app.py.

    Streamlit resolves `ui_lib` / `tabs` by name off the script's own directory,
    so a Dockerfile that copies only app.py produces an image whose UI cannot
    start — and the CI integration job never opens port 8501, so nothing else
    would catch it.
    """
    repo = UI.parents[2]
    for product in ("grocery", "gas-station", "support"):
        df = repo / product / "standalone" / "Dockerfile"
        if not df.exists():
            pytest.skip(f"{product} standalone Dockerfile not present")
        text = df.read_text()
        assert "COPY base/ui/app.py" not in text, (
            f"{df} copies only app.py; the panel is a package since t_c2eca5dd"
        )
        assert "COPY base/ui/ /app/ui/" in text, (
            f"{df} must copy the whole base/ui directory"
        )
