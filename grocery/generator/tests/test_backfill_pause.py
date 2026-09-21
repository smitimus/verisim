"""A pause or a stop has to interrupt a backfill, not wait for the range to finish.

Regression suite for verisim `t_057e3ad0`. The generation loop samples
`control.generator_state` once per iteration, and a backfill iteration used to be a
whole range: `run_backfill()` walked every remaining day in one call, so a pause
requested while a 30-day range was being generated was ignored for the rest of the
range -- minutes of writing, on the very first start of a fresh install.

That is what turned CI red on 4ec85ec: `grocery/api/tests/conftest.py::quiesced_generator`
pauses the generator and then measures row counts, and during a fresh container's
initial backfill the pause did nothing, so `total` moved under the pagination walk
("row count kept changing during the walk") and under the ingest-window test's
one-second bucket (adding the business window then "changed" the count by 551).

These tests drive `run_backfill` with a stub connection, so they need no database and
run in the blocking generator job.
"""
from datetime import date, datetime
from types import SimpleNamespace

from grocery.generator import main as gen

STARTS = dict(backfill_start_date=date(2026, 1, 1),
              backfill_end_date=date(2026, 1, 2),
              backfill_current_date=date(2026, 1, 1))


class _Control:
    """The `control.generator_state` row, mutable so a test can flip it mid-run."""

    def __init__(self, **overrides):
        self.row = {
            'state_id': 1,
            'is_running': True,
            'is_paused': False,
            'mode': 'backfill',
            'active_scenario': 'normal',
            'volume_multiplier': 1.0,
            'tick_interval_seconds': 30,
            'last_tick_at': None,
            'started_at': None,
            'updated_at': None,
            **STARTS,
        }
        self.row.update(overrides)


class _Cursor:
    def __init__(self, conn):
        self._conn = conn
        self._result = None
        # DBAPI parity: a real psycopg2 cursor exposes rowcount after execute.
        # `pos.reconcile_promotions` reads it, so the double must too.
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        self._conn.sql.append(text)
        if text.upper().startswith("SELECT") and "control.generator_state" in text:
            self._result = dict(self._conn.control.row)
        elif "max(transaction_dt)" in text:
            # `SELECT max(...)` always returns one row -- (None,) when the day is empty
            self._result = (self._conn.last_txn,)
        else:
            self._result = None

    def fetchone(self):
        return self._result

    def fetchall(self):
        return []


class _Conn:
    """Connection stub: records executed SQL, counts commits, holds the last txn."""

    def __init__(self, control, last_txn=None):
        self.control = control
        self.last_txn = last_txn
        self.sql = []
        self.commits = 0

    def cursor(self, *args, **kwargs):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def heartbeats(self):
        return sum(1 for sql in self.sql if "last_tick_at = NOW()" in sql)


def _stub_models(monkeypatch):
    """Replace every model `run_backfill` writes through with a no-op."""
    noop = lambda *args, **kwargs: None          # noqa: E731
    empty = lambda *args, **kwargs: []           # noqa: E731
    returns_empty = {
        "generate_pos_transactions", "generate_online_orders",
        "check_and_create_orders", "process_pending_orders", "generate_returns",
    }
    for module, name in [
        (gen.pos, "generate_pos_transactions"), (gen.pos, "maybe_update_product_prices"),
        (gen.pos, "seed_coupons"), (gen.pos, "seed_combo_deals"),
        (gen.pos, "reconcile_promotions"),
        (gen.inventory, "deplete_inventory"), (gen.inventory, "deplete_online_inventory"),
        (gen.online, "generate_online_orders"), (gen.online, "advance_online_lifecycle"),
        (gen.timeclock, "generate_day_events"), (gen.timeclock, "generate_events"),
        (gen.ordering, "check_and_create_orders"),
        (gen.fulfillment, "process_pending_orders"),
        (gen.transport, "dispatch_loads"), (gen.transport, "receive_delivered_loads"),
        (gen.shrinkage, "set_expiry_dates"), (gen.shrinkage, "generate_shrinkage_events"),
        (gen.promotions, "expire_old_ads"), (gen.promotions, "ensure_current_ad"),
        (gen.scheduling, "resolve_schedule_actuals"), (gen.scheduling, "generate_weekly_schedule"),
        (gen.returns, "generate_returns"), (gen.returns, "restock_returns"),
    ]:
        monkeypatch.setattr(module, name, empty if name in returns_empty else noop)

    monkeypatch.setattr(gen, "get_active_scenario_names", lambda *a, **k: [])
    monkeypatch.setattr(gen, "get_scenario_context",
                        lambda *a, **k: SimpleNamespace(volume_multiplier=1.0))


def _run(conn, control, monkeypatch, record=None):
    """Drive one run_backfill over the control row's range, no database involved."""
    _stub_models(monkeypatch)
    if record is not None:
        monkeypatch.setattr(gen.pos, "generate_pos_transactions", record)
    gen.run_backfill(conn, gen.load_config(), dict(control.row),
                     locations={'stores': [], 'warehouses': []}, employees=[],
                     departments=[], products=[], trucks=[],
                     members=[], coupons=[], deals=[])


def _recorded_hours(control, stop_after=None, **flip):
    """A `generate_pos_transactions` stand-in that records the hours it is asked for.

    `stop_after` flips the control row (`**flip`) while that hour is being written,
    which is what an operator pausing mid-backfill looks like to the generator.
    """
    hours = []

    def record(conn, cfg, sim_dt, count, scenario, *args, **kwargs):
        hours.append(sim_dt)
        if stop_after is not None and len(hours) == stop_after:
            control.row.update(flip)
        return []

    return hours, record


def test_generation_halted_only_when_asked():
    """The predicate the backfill samples: running and unpaused is not halted."""
    live = _Control().row
    assert gen.generation_halted(live) is False
    assert gen.generation_halted(dict(live, is_paused=True)) is True
    assert gen.generation_halted(dict(live, is_running=False)) is True
    assert gen.generation_halted(dict(live, mode='stopped')) is True


def test_backfill_runs_the_whole_range_when_nobody_pauses(monkeypatch):
    """2 days x 24 hours, and nothing trips the halt check on its own."""
    control = _Control()
    conn = _Conn(control)
    hours, record = _recorded_hours(control)

    _run(conn, control, monkeypatch, record)

    assert len(hours) == 48, f"expected both days to be generated, got {len(hours)} hours"
    assert conn.heartbeats() >= 48, (
        "the backfill must stamp the writer's clock (last_tick_at) as it goes, so "
        "anything outside the process can tell whether rows are still being written"
    )


def test_pause_during_a_backfill_stops_it_before_the_next_hour(monkeypatch):
    """The operator pauses while hour 3 is being written: hour 4 must not start.

    Without the halt check this walked the whole range -- both days, 48 hours -- with
    `is_paused` sitting true in the control row the whole time.
    """
    control = _Control()
    conn = _Conn(control)
    hours, record = _recorded_hours(control, stop_after=3, is_paused=True)

    _run(conn, control, monkeypatch, record)

    assert len(hours) == 3, (
        f"the pause was not honoured between batches: {len(hours)} hours generated "
        f"after the operator asked to pause (the range is 2 days = 48 hours)"
    )
    assert conn.commits > 0, "the hours already written must be committed before yielding"


def test_stop_during_a_backfill_stops_it_too(monkeypatch):
    """`generator_stop` sets is_running = FALSE -- same contract as a pause."""
    control = _Control()
    conn = _Conn(control)
    hours, record = _recorded_hours(control, stop_after=1, is_running=False)

    _run(conn, control, monkeypatch, record)

    assert len(hours) == 1, f"a stop mid-range kept generating {len(hours)} hours"


def test_pause_before_the_range_starts_writes_nothing(monkeypatch):
    """Paused when `run_backfill` is entered: no hour of the range is touched."""
    control = _Control(is_paused=True)
    conn = _Conn(control)
    hours, record = _recorded_hours(control)

    _run(conn, control, monkeypatch, record)

    assert hours == [], "a paused generator generated part of the range"


def test_resume_continues_from_the_hour_after_the_last_row(monkeypatch):
    """A resumed backfill continues where it stopped, it does not rewrite the day.

    The day-level probe (`max(transaction_dt)` for the day under construction) already
    did this for gap-fills; the pause path now depends on it, so pin it: the day's last
    row was written in hour 2, so the resumed run generates hours 3..23 and nothing
    that was already written.
    """
    control = _Control(backfill_end_date=date(2026, 1, 1))       # one day
    conn = _Conn(control, last_txn=datetime(2026, 1, 1, 2, 30, 0))
    hours, record = _recorded_hours(control)

    _run(conn, control, monkeypatch, record)

    assert [stamp.hour for stamp in hours] == list(range(3, 24))
