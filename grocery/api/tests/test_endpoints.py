import pytest
import httpx


@pytest.mark.usefixtures("ensure_api_reachable")
def test_health(api_base_url):
    resp = httpx.get(f"{api_base_url}/health", timeout=5.0)
    assert resp.status_code == 200, f"Health endpoint returned {resp.status_code}"
    data = resp.json()
    assert isinstance(data, dict), "Health response should be a JSON object"
    assert data.get("status") == "healthy", f"Expected status 'healthy', got {data.get('status')}"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_generator_status(api_base_url):
    resp = httpx.get(f"{api_base_url}/grocery/status", timeout=5.0)
    assert resp.status_code == 200, f"Generator status endpoint returned {resp.status_code}"
    data = resp.json()
    assert isinstance(data, dict), "Generator status should be a JSON object"
    assert "state" in data, "Missing 'state' in response"
    assert "today" in data, "Missing 'today' in response"
    state = data.get("state")
    today = data.get("today")
    assert isinstance(state, dict), "'state' should be an object"
    assert isinstance(today, dict), "'today' should be an object"
    assert state.get("mode") in ("realtime", "backfill", "stopped"), f"Unexpected mode: {state.get('mode')}"
    assert isinstance(state.get("is_running"), bool), "'is_running' should be a boolean"
    assert isinstance(today.get("ticks_today"), int), "'ticks_today' should be an int"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_swagger_docs(api_base_url):
    resp = httpx.get(f"{api_base_url}/docs", timeout=5.0)
    assert resp.status_code == 200, f"Docs endpoint returned {resp.status_code}"
    content_type = resp.headers.get("content-type", "")
    assert "text/html" in content_type, f"Docs endpoint should return HTML, got '{content_type}'"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_transactions_pagination(api_base_url):
    params = {"start_dt": "2026-03-01T00:00:00", "end_dt": "2026-03-02T00:00:00", "limit": 5}
    resp = httpx.get(f"{api_base_url}/grocery/pos/transactions", params=params, timeout=5.0)
    assert resp.status_code == 200, f"Transactions endpoint returned {resp.status_code}"
    data = resp.json()
    assert isinstance(data, dict), "Transactions payload should be a dict with pagination"
    assert "data" in data, "Missing 'data' key in transactions response"
    assert isinstance(data["data"], list), "Transactions 'data' field should be a list"
    assert "total" in data, "Missing 'total' key in transactions response"
    assert "limit" in data, "Missing 'limit' key in transactions response"
    assert "offset" in data, "Missing 'offset' key in transactions response"
