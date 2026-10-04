"""Behavioural proof of the consuming semantics behind t_9036dfaa's fix.

The guard in `test_schedule_actuals.py` is structural: it counts call sites. This
file proves the *reason* that count is load-bearing, by running the real
`resolve_schedule_actuals` against an in-memory `hr.schedules` double and showing
that a second same-day call finds nothing and leaves the rows alone.

If this behaviour ever changed (the call stopped consuming, or started re-rolling
rows it had already written), the structural guard's one-call-per-date rule would
no longer be the right rule to enforce, and the fix's rationale would be wrong.
"""
from datetime import date, timedelta
from types import SimpleNamespace
import random

from grocery.generator.models.scheduling import resolve_schedule_actuals

SCHED_ID = "11111111-1111-1111-1111-111111111111"


class _Row:
    def __init__(self, schedule_id):
        self.schedule_id = schedule_id
        self.status = "scheduled"


class _FakeCursors:
    """Serves hr.schedules rows and records the UPDATEs, like the model expects."""

    def __init__(self, db):
        self._db = db

    def __call__(self, *a, **kw):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        if s.startswith("SELECT schedule_id"):
            # Same predicate the model uses: yesterday, still 'scheduled'.
            want = self._db.yesterday
            self._db.selects.append((want, params))
            self._db.found = [r for r in self._db.rows if r.status == "scheduled"]
        elif s.startswith("UPDATE hr.schedules"):
            # The model binds (status, schedule_id) in that order.
            new_status, sched_id = params[0], params[1]
            self._db.updates.append((sched_id, new_status))
            for r in self._db.rows:
                if r.schedule_id == sched_id:
                    r.status = new_status

    def fetchall(self):
        return [(r.schedule_id,) for r in getattr(self._db, "found", [])]

    def fetchone(self):
        return (len(self._db.rows),)


class _FakeDB:
    def __init__(self, sim_date, n_rows=1):
        self.sim_date = sim_date
        self.yesterday = sim_date - timedelta(days=1)
        self.rows = [_Row(SCHED_ID + str(i)) for i in range(n_rows)]
        self.updates = []
        self.selects = []
        self.commits = 0
        self.found = []

    def cursor(self, *a, **kw):
        return _FakeCursors(self)

    def commit(self):
        self.commits += 1


def test_second_same_day_call_is_a_noop():
    """The bug: two calls on one date means the second one's scenario is dead."""
    db = _FakeDB(date(2026, 7, 15), n_rows=1)
    today_ctx = SimpleNamespace(attendance_modifier=1.0)
    yesterday_ctx = SimpleNamespace(attendance_modifier=0.5)

    random.seed(1234)
    resolve_schedule_actuals(db, db.sim_date, today_ctx)
    assert len(db.updates) == 1, "first call must resolve the row"
    assert db.commits == 1

    # Second call, same date, different scenario — exactly the shape main.py had.
    updates_before = len(db.updates)
    commits_before = db.commits
    random.seed(9999)  # even with different RNG, nothing may be re-rolled
    resolve_schedule_actuals(db, db.sim_date, yesterday_ctx)

    assert len(db.updates) == updates_before, (
        "the second same-day call re-rolled rows the first had already resolved"
    )
    assert db.commits == commits_before, (
        "the shadowed call committed, so it is not a pure no-op"
    )


def test_resolution_order_decides_which_weather_counts():
    """Whichever call runs first writes the status; the other cannot correct it.

    This is the user-visible consequence on the dev slot's numbers: with the
    same-day call first, every shift is scored under TODAY's attendance_modifier,
    so a storm yesterday plus a clear today records yesterday's call-outs at the
    clear-day rate and the weather covariate never reaches hr.schedules.
    """
    sim_date = date(2026, 7, 15)
    clear = SimpleNamespace(attendance_modifier=1.0)
    storm = SimpleNamespace(attendance_modifier=0.5)

    # Order as main.py had it: plain same-day first.
    buggy = _FakeDB(sim_date, n_rows=400)
    random.seed(42)
    resolve_schedule_actuals(buggy, sim_date, clear)
    resolve_schedule_actuals(buggy, sim_date, storm)
    buggy_completed = sum(1 for r in buggy.rows if r.status == "completed")

    # Order as fixed: weather-aware context first and alone.
    fixed = _FakeDB(sim_date, n_rows=400)
    random.seed(42)
    resolve_schedule_actuals(fixed, sim_date, storm)
    fixed_completed = sum(1 for r in fixed.rows if r.status == "completed")

    assert buggy_completed > fixed_completed, (
        "the storm's lower completion rate made no difference; the scenario "
        "argument is not reaching the rows"
    )
    # Sanity: the effect is material, not a rounding artifact.
    assert buggy_completed - fixed_completed > 20, (
        "completion-rate gap %d is implausibly small" % (buggy_completed - fixed_completed)
    )


def test_weather_modifier_only_applies_to_the_day_being_resolved():
    """A 0.5 attendance_modifier must move the completion rate off the 0.91 base.

    Note the arithmetic: the model computes
    `p_completed = min(0.95, max(0.5, P_COMPLETED * att_mod))`, and with
    P_COMPLETED = 0.91 that inner `max(0.5, ...)` FLOOR clamps 0.91 * 0.5 = 0.455
    up to 0.5. So a half-strength storm yields a 0.5 completion rate, not 0.455,
    and the modifier's effect saturates below att_mod = 0.55. That clamp is
    pre-existing behaviour in the model, not something this card changes — but it
    means the weather covariate's pull on attendance is gentler than a reading of
    "0.5x modifier" suggests, and the mart that reads these rows sees the floored
    rate. Pinned so a future edit to the redistribution cannot quietly make the
    weather hook inert.
    """
    sim_date = date(2026, 7, 15)
    db = _FakeDB(sim_date, n_rows=3000)
    storm = SimpleNamespace(attendance_modifier=0.5)
    random.seed(7)
    resolve_schedule_actuals(db, sim_date, storm)

    completed = sum(1 for r in db.rows if r.status == "completed")
    rate = completed / len(db.rows)
    # 0.5 exactly: the clamp floor, not the 0.455 the raw multiply would give.
    assert 0.46 < rate < 0.54, "completion rate %.3f is not the clamped ~0.50 expected" % rate

    # And confirm the base case really is 0.91, so the pair above means something.
    clear_db = _FakeDB(sim_date, n_rows=3000)
    random.seed(7)
    resolve_schedule_actuals(clear_db, sim_date, SimpleNamespace(attendance_modifier=1.0))
    clear_rate = sum(1 for r in clear_db.rows if r.status == "completed") / len(clear_db.rows)
    assert 0.88 < clear_rate < 0.94, "clear-day rate %.3f is not ~0.91" % clear_rate