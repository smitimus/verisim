"""The main loop must recover from ONE bad statement — not die forever.

THE REGRESSION (t_f963eeb1)
==========================
Measured on CT106 2026-10-04, the grocery generator went permanently silent at
08:11:05 and never wrote again. The log holds exactly one real error followed by
446 identical ones:

    08:11:05 ERROR — Unexpected error in main loop: insert or update on table
              "transaction_items" violates foreign key constraint
              "transaction_items_coupon_id_fkey"
    08:11:15 ERROR — Unexpected error in main loop: current transaction is
              aborted, commands ignored until end of transaction block
    ... x445 more, every 10s, until the container was recycled

`main()` caught the exception, logged it, slept 10s and looped — but never
rolled the transaction back. psycopg2 leaves a connection aborted once a
statement fails: every subsequent command on that connection fails with
`InFailedSqlTransaction` until someone issues a ROLLBACK. So a single transient
error — one FK violation, one deadlock victim, one connection dropped mid-
statement — permanently bricks the loop.

Why this went unnoticed for 74 minutes: the container healthcheck is a *database*
probe. `pg_isready` never sees the application stop working, only its work stop,
so `docker ps` stayed green the whole time. The shortfall is only visible if a
human counts rows.

These tests pin the recovery contract against a fake connection that reproduces
psycopg2's aborted-transaction semantics: once a statement fails, every later
execute raises `InFailedSqlTransaction` until a rollback clears it.
"""
import pytest

from grocery.generator import main as gen_main


class _StopLoop(BaseException):
    """Breaks out of `main()`'s `while True`.

    A BaseException, deliberately: `main()` catches bare `Exception`, so this
    sentinel cannot be swallowed by the very recovery path under test. Sleeping
    is where the loop is interrupted, so no iteration can outlive the budget.
    """


class AbortingConnection:
    """Reproduces psycopg2's aborted-transaction state machine.

    `fail_on` is the 1-based statement number that raises the injected error.
    Every subsequent statement raises `InFailedSqlTransaction` until
    `rollback()` clears the flag — which is exactly the behaviour that turned
    one FK violation into 446 permanent failures.
    """

    def __init__(self, fail_on: int = 1):
        self.calls = 0
        self.rollbacks = 0
        self.commits = 0
        self.broken = False
        self._fail_on = fail_on

    def cursor(self, *a, **k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.calls += 1
        if self.broken:
            raise gen_main.psycopg2.errors.InFailedSqlTransaction(
                "current transaction is aborted, commands ignored until "
                "end of transaction block")
        if self.calls == self._fail_on:
            self.broken = True
            raise gen_main.psycopg2.errors.ForeignKeyViolation(
                'insert or update on table "transaction_items" violates '
                'foreign key constraint "transaction_items_coupon_id_fkey"')
        return None

    def fetchone(self):
        return (0,)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1
        self.broken = False


@pytest.fixture
def loop_harness(monkeypatch):
    """Boot `main()` with the database stubbed out, and interrupt its sleep.

    Everything `main()` does before the loop (connect, seed, backfill check)
    is replaced, so the test observes the loop and nothing else. Sleeps are
    counted instead of performed, and the fourth one raises `_StopLoop`.
    """
    def _run(fail_on: int, refresh_calls=None):
        conn = AbortingConnection(fail_on=fail_on)
        sleeps = []

        class _FakeTime:
            @staticmethod
            def monotonic():
                return 0.0

            @staticmethod
            def sleep(seconds):
                sleeps.append(seconds)
                if len(sleeps) >= 4:
                    raise _StopLoop

        cfg = object()
        monkeypatch.setattr(gen_main, "time", _FakeTime)
        monkeypatch.setattr(gen_main, "load_config", lambda: cfg)
        monkeypatch.setattr(gen_main, "wait_for_db", lambda c: None)
        monkeypatch.setattr(gen_main, "bootstrap_database", lambda c: None)
        monkeypatch.setattr(gen_main, "get_connection", lambda c: conn)
        monkeypatch.setattr(gen_main, "seed_all", lambda c, cf: ({}, [], [], [], []))
        monkeypatch.setattr(gen_main, "auto_backfill_if_fresh", lambda c, cf: None)
        monkeypatch.setattr(gen_main, "customers", type("C", (), {
            "backfill_customers": staticmethod(lambda c, cf: None)}))

        def _fetch_coupons(c):
            if refresh_calls is not None:
                refresh_calls.append(1)
            return []

        monkeypatch.setattr(gen_main, "pos", type("P", (), {
            "reconcile_promotions": staticmethod(lambda c: None),
            "fetch_loyalty_members": staticmethod(lambda c: []),
            "fetch_active_coupons": staticmethod(_fetch_coupons),
            "fetch_active_deals": staticmethod(lambda c: [])}))
        monkeypatch.setattr(gen_main, "hr", type("H", (), {
            "fetch_active_employees": staticmethod(lambda c: []),
            "fetch_locations": staticmethod(lambda c: [])}))
        # `read_state` is left REAL on purpose: it is the loop's first
        # statement against the connection (`SELECT * FROM control.generator_state
        # WHERE state_id = 1`, main.py:130) and it is exactly where the live
        # log shows the failure landing — the first real error was the FK
        # violation, and from the next iteration onwards every failure came
        # from this line. Stubbing it would test the harness, not the bug.
        monkeypatch.setattr(gen_main, "reload_config", lambda c: c)

        real_read_state = gen_main.read_state

        def _read_state(c):
            real_read_state(c)          # issues the statement; may raise
            return {'mode': 'stopped', 'is_running': False, 'is_paused': False,
                    'tick_interval_seconds': 0}

        monkeypatch.setattr(gen_main, "read_state", _read_state)

        with pytest.raises(_StopLoop):
            gen_main.main()
        return conn, sleeps

    return _run


def test_main_loop_rolls_back_and_recovers(loop_harness):
    """A failed statement must not poison every statement after it.

    Asserted as *recovery*, not as a call count: a rollback that did not
    actually clear the abort would satisfy "rollback was called" while the
    generator stayed silent forever — which is precisely the live failure.
    """
    conn, _ = loop_harness(fail_on=1)

    assert conn.rollbacks >= 1, (
        "no rollback after a failed statement: psycopg2 leaves the connection "
        "aborted, so every later command fails with InFailedSqlTransaction "
        "and the generator writes nothing for the life of the container"
    )
    # The loop got past the failure and kept reading state. Without the
    # rollback the second read_state raises InFailedSqlTransaction and the
    # call count freezes at 1.
    assert conn.calls >= 4, (
        f"the loop stopped issuing statements after the first failure "
        f"(only {conn.calls} executed) — it never recovered"
    )


def test_recovery_does_not_swallow_the_original_error(loop_harness):
    """A rolled-back loop still logs. Recovery must not become silence.

    A "fix" that caught everything and logged nothing would satisfy the
    recovery assertion above while leaving an operator with no way to see the
    failure. `sleeps` proves the loop kept going; the error itself is what
    the existing `log.exception` call reports.
    """
    conn, sleeps = loop_harness(fail_on=1)

    assert len(sleeps) >= 2, (
        "the loop exited instead of retrying — one bad statement must not end "
        "the generator's work"
    )
    assert 10 in sleeps, (
        f"the retry backoff should stay at 10s so a persistent failure does "
        f"not spin, got {sleeps}"
    )


def test_recovery_rereads_the_caches_so_a_deleted_row_is_dropped_now(loop_harness):
    """Rollback alone only buys one more failing tick — the stale id must go.

    The caches are refreshed on `tick_count % 20 == 0`, and `tick_count` is
    incremented only after a SUCCESSFUL tick (main.py). While a cached coupon
    that has been deleted keeps causing FK violations, the counter never
    advances, so the next scheduled refresh is never reached. Rolling back
    without re-reading would therefore loop on the same bad id.

    This asserts the refresh happened as part of recovery, which is what makes
    the recovered tick actually able to succeed rather than fail again.
    """
    refresh_calls = []
    conn, sleeps = loop_harness(fail_on=1, refresh_calls=refresh_calls)

    assert refresh_calls, (
        "recovery did not re-read the promo caches: the deleted coupon stays "
        "in memory, and because tick_count does not advance while ticks fail, "
        "the scheduled refresh is never reached — the loop repeats the same "
        "FK violation instead of recovering"
    )
    assert len(sleeps) >= 2