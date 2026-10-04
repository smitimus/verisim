"""
API access for the control panel — one place that knows the base URL.

Every tab reads and writes through these helpers, so an unreachable API fails
the same way everywhere: `api_get` returns None and the tab renders its empty
state, while the mutating helpers surface the error in the UI.

`INDUSTRY_ORDER` lives here rather than in `app.py` because
`get_available_industries()` reads it: it was defined in app.py and referenced
from a function that has now moved, which is a NameError the first time the
panel loads.

Split out of `base/ui/app.py` (t_c2eca5dd).
"""
import os

import requests
import streamlit as st

API = os.environ.get("API_BASE_URL", "http://localhost:8000")

INDUSTRY_ORDER = ["grocery", "support", "gas-station"]  # preferred display order


@st.cache_data(ttl=30)
def get_available_industries():
    """
    Returns all DB-healthy industries from /health.
    Generator running/stopped state does not affect availability.
    """
    try:
        r = requests.get(f"{API}/health", timeout=5)
        r.raise_for_status()
        db_healthy = [slug for slug, ok in r.json().get("industries", {}).items() if ok]
    except Exception:
        return []

    # Return in preferred display order
    return [s for s in INDUSTRY_ORDER if s in db_healthy] + [s for s in db_healthy if s not in INDUSTRY_ORDER]


def api_get(path: str, params: dict = None):
    try:
        r = requests.get(f"{API}{path}", params=params, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def api_post(path: str, json: dict = None):
    try:
        r = requests.post(f"{API}{path}", json=json or {}, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def api_patch(path: str, json: dict):
    try:
        r = requests.patch(f"{API}{path}", json=json, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def api_delete(path: str):
    try:
        r = requests.delete(f"{API}{path}", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


# ---------------------------------------------------------------------------
# Auto-refresh logic
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Status badge helper
# ---------------------------------------------------------------------------

def status_badge(state: dict) -> str:
    if not state:
        return "🔴 API Unreachable"
    if state.get("mode") == "stopped" or not state.get("is_running"):
        return "🔴 Stopped"
    if state.get("is_paused"):
        return "🟡 Paused"
    if state.get("mode") == "backfill":
        return "🔵 Backfilling"
    return "🟢 Running"
