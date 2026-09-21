import os
import time

import pytest
import httpx


@pytest.fixture(scope="session")
def api_base_url():
    # CI runs the standalone container on 8010 (see the workflow's smoke test). The
    # override exists so the same suite can be pointed at a container started by hand
    # on another port — e.g. a fresh one, mid-backfill, to reproduce the CI conditions.
    return os.environ.get("VERISIM_API_BASE_URL", "http://localhost:8010")


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


# The canary is a table every generator tick writes, so a canary count that has stopped
# moving means the tick — or the backfill day — in flight has finished writing.
SETTLE_CANARY = "/grocery/pos/transactions"
SETTLE_TIMEOUT_SECONDS = 300.0
SETTLE_SAMPLE_SECONDS = 1.5


def _row_total(client, path=SETTLE_CANARY):
    return client.get(path, params={"limit": 1}).json()["total"]


def _wait_until_settled(client, timeout=SETTLE_TIMEOUT_SECONDS,
                        sample_seconds=SETTLE_SAMPLE_SECONDS):
    """Block until nothing is inserting any more.

    ``POST /grocery/generator/pause`` is cooperative: the generator finishes the work it
    already has in flight — the current tick, or, on a container that has just booted,
    the rest of the 30-day backfill — before it honours the pause. A paused generator can
    therefore go on writing for seconds, or for minutes. Paging and row counts are only
    comparable once the writes stop, otherwise the tick that lands mid-walk shifts every
    later page and looks exactly like pagination loss (which is how main went red on
    2026-09-21). Wait for the canary count to hold still across two samples before a walk
    measures anything.
    """
    deadline = time.monotonic() + timeout
    previous = _row_total(client)
    while time.monotonic() < deadline:
        time.sleep(sample_seconds)
        current = _row_total(client)
        if current == previous:
            return
        previous = current
    pytest.fail(
        f"{SETTLE_CANARY} was still growing {timeout:.0f}s after the generator was "
        f"paused (last count {previous}) — row counts are not comparable while "
        f"something is inserting"
    )


@pytest.fixture(scope="session")
def quiesced_generator(api_base_url):
    """Pause the generator for the whole session and confirm it has actually stopped.

    Paging and row counts are only comparable while nothing is inserting: a tick that
    lands mid-walk shifts every later page by a few rows, which looks like pagination
    loss but is not. The generator is resumed even if a test fails.

    Session-scoped on purpose — pausing and resuming around every parametrised walk
    would itself keep perturbing the counts it is trying to hold still. The first test
    that needs a walk pays the one-off wait for the dataset to settle.
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
            _wait_until_settled(client)
        try:
            yield client
        finally:
            if was_running:
                client.post("/grocery/generator/resume")
