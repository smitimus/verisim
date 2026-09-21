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


@pytest.fixture
def quiesced_generator(api_base_url):
    """Pause the generator for the duration of a walk.

    Paging and row counts are only comparable while nothing is inserting: a tick that
    lands mid-walk shifts every later page by a few rows, which looks like pagination
    loss but is not. The generator is resumed even if the test fails.
    """
    with httpx.Client(base_url=api_base_url, timeout=30.0) as client:
        state = {}
        try:
            state = client.get("/grocery/status").json()["state"]
        except Exception as exc:                       # noqa: BLE001
            pytest.skip(f"generator status unavailable: {exc}")
        was_running = bool(state.get("is_running")) and not state.get("is_paused")
        if was_running:
            client.post("/grocery/generator/pause")
        try:
            yield client
        finally:
            if was_running:
                client.post("/grocery/generator/resume")
