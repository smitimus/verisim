import pytest
import httpx


@pytest.fixture(scope="session")
def api_base_url():
    return "http://localhost:8010"


@pytest.fixture(scope="session")
def ensure_api_reachable(api_base_url):
    url = f"{api_base_url}/health"
    try:
        resp = httpx.get(url, timeout=5.0)
        if resp.status_code != 200:
            pytest.skip(f"Health check returned {resp.status_code}, skipping tests.")
        try:
            resp.json()
        except Exception:
            pass
    except Exception as e:
        pytest.skip(f"API at {url} not reachable: {e}")
    return True
