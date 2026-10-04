"""Fixtures for the gas-station API contract tests (t_a6ecb731).

The suite runs against a live container (CI starts one on port 8011). It exists
because the generator unit tests cannot see the failures this product has
actually had: nothing in gas-station/generator/tests/ starts postgres, applies
schema.sql, runs supervisord or mounts the stripped API — and every one of those
is where a broken image shows up.

The suite auto-skips when no API is up, so it is safe to run locally; in CI the
container is already verified reachable, so a skip there would be a silent hole.
"""
import os
import time

import httpx
import pytest

# CI runs the gas-station standalone container on 8011 (see the workflow's smoke
# test). The override exists so the same suite can be pointed at a container
# started by hand on another port.
API_BASE_URL = os.environ.get("VERISIM_GAS_API_BASE_URL", "http://localhost:8011")

# A table every tick writes, so a canary count that stops moving means the tick in
# flight has finished.
SETTLE_CANARY = "/gas-station/fuel/transactions"
SETTLE_TIMEOUT_SECONDS = 300.0
SETTLE_SAMPLE_SECONDS = 1.5

# The routes the gas-station image is supposed to serve. Anything else answering
# 200 means the strip script kept another industry's section.
GAS_STATION_ROUTES = (
    "/gas-station/status",
    "/gas-station/fuel/grades",
    "/gas-station/fuel/pumps",
    "/gas-station/fuel/price-history",
    "/gas-station/fuel/transactions",
    "/gas-station/fuel/transactions/summary",
)


@pytest.fixture(scope="session")
def client():
    """An HTTP client for the live gas-station API, or skip if it is not up."""
    try:
        resp = httpx.get(f"{API_BASE_URL}/health", timeout=5.0)
        if resp.status_code != 200:
            pytest.skip(f"/health returned {resp.status_code}")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"gas-station API at {API_BASE_URL} not reachable: {exc}")

    with httpx.Client(base_url=API_BASE_URL, timeout=30.0) as c:
        yield c


def row_total(c, path=SETTLE_CANARY):
    return c.get(path, params={"limit": 1}).json()["total"]


@pytest.fixture(scope="session")
def quiesced(client):
    """Pause the generator for the session and wait until it really has stopped.

    Paging is only comparable while nothing is inserting: a tick that lands
    mid-walk shifts every later page, which looks exactly like pagination loss.
    `/generator/pause` is cooperative — the generator finishes the tick (or the
    rest of the backfill) before honouring it — so a paused generator can still
    be writing. Wait for the canary count to hold still across two samples.

    Session-scoped on purpose: pausing and resuming around every walk would itself
    perturb the counts it is trying to hold still.
    """
    state = client.get("/gas-station/status").json().get("state", {})
    was_running = bool(state.get("is_running")) and not state.get("is_paused")
    if not was_running:
        yield client
        return

    client.post("/gas-station/generator/pause")

    deadline = time.monotonic() + SETTLE_TIMEOUT_SECONDS
    previous = row_total(client)
    settled = False
    while time.monotonic() < deadline:
        time.sleep(SETTLE_SAMPLE_SECONDS)
        current = row_total(client)
        if current == previous:
            settled = True
            break
        previous = current

    if not settled:
        client.post("/gas-station/generator/resume")
        pytest.fail(
            f"{SETTLE_CANARY} was still growing {SETTLE_TIMEOUT_SECONDS:.0f}s after the "
            f"generator was paused (last count {previous}) — row counts are not "
            f"comparable while something is inserting"
        )

    try:
        yield client
    finally:
        client.post("/gas-station/generator/resume")
