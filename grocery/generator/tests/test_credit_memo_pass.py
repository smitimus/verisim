"""
The credit-memo BOOK — a receiving short-ship must become a claim (t_d74d67a3).

THE DEFECT
----------
`open_credit_memo` had exactly one caller (`generate_dsd_deliveries`), and the
receiving path — the one every WAREHOUSE vendor's shortfall goes through —
stopped at `inv.short_ship_events`. `advance_credit_memos` only advances memos
that already exist. So an 8-day backfill measured:

    inv.short_ship_events                    147   (all detected_source='receiving')
    inv.short_ship_events dsd_delivery         0
    inv.supplier_credit_memos                  0

147 shortfalls, zero claims, no warning. A vendor-performance mart had no
days-to-pay, no credited dollars and no rejection mix — not because no vendor
ever shorted us, but because nobody filed the paperwork.

WHAT THESE TESTS PIN
--------------------
1. Every creditable, unclaimed event inside the vendor's window is claimed by
   the daily pass — the receiving path included.
2. `claim_deadline` is derived from the EVENT's date, never from the pass's
   clock: a gap-fill pass that runs three weeks later must not re-date an old
   shortfall into a future window (`advance_credit_memos` would then submit it
   as if it were fresh, and days-to-pay would be a lie).
3. An event whose own window has closed is NOT claimed: a memo cannot be born
   expired, because `advance_credit_memos` would expire it on its first pass
   and the book would show paperwork nobody could have filed.
4. The pass is idempotent per day — a replayed backfill day opens no second
   memo (the UNIQUE on `short_ship_id` is the backstop, not the mechanism).
5. The claim decision is a function of the event, not a coin toss, so the claim
   rate means "share of creditable shorts we claim" on a replayed day too.

No database here: the pass's SQL is answered by a scripted cursor, and the DDL
half (nullability + the `short_ship_requires_its_parents` CHECK) is pinned
against a live Postgres in test_supplier_schema.py.
"""
from datetime import date, datetime, timedelta, timezone

import psycopg2.errors
import pytest

from grocery.generator.config import Config
from grocery.generator.models import suppliers

PASS_DT = datetime(2026, 10, 25, 23, 30)
SHORT_SHIP = 'abcdabcd-0000-0000-0000-000000000001'
SHORT_SHIP_2 = 'abcdabcd-0000-0000-0000-000000000002'
SUPPLIER = '11111111-0000-0000-0000-000000000001'
PRODUCT = 'aaaaaaaa-0000-0000-0000-000000000001'
STORE = 'cccccccc-0000-0000-0000-000000000003'


def _vendor(window=14, creditable=True, supplier_id=SUPPLIER):
    return {
        'supplier_id': supplier_id,
        'supplier_name': 'UNFI',
        'fulfillment_model': 'warehouse',
        'lead_time_mean_days': 2.0,
        'lead_time_stddev_days': 0.5,
        'short_ship_rate': 0.06,
        'credit_eligible': creditable,
        'credit_window_days': window,
        'credit_settle_mean_days': 8,
        'credit_settle_stddev_days': 3,
    }


class _Cursor:
    """Answers the pass's SQL from the fake's state; records every write.

    Closes on `__exit__` and refuses to execute afterwards, like the real
    thing — a `with conn.cursor() as cur:` block whose cursor is then used
    outside it raises `InterfaceError: cursor already closed` on a live
    connection (`advance_credit_memos` did exactly that, t_d74d67a3), and a
    fake that silently kept working would hide it.
    """

    def __init__(self, db):
        self.db = db
        self._rows = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True
        return False

    def execute(self, sql, params=None):
        if self.closed:
            raise psycopg2.InterfaceError('cursor already closed')
        flat = ' '.join(sql.split())
        self.db.statements.append((flat, params))
        self._rows = self.db.answer(flat, params)
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _DB:
    """
    A grocery DB holding only short-ship events and the memo book.

    `events` is `{short_ship_id: (event_date, supplier_id, is_creditable)}` and
    the fake enforces the two properties the real SQL does: an event with a memo
    already is not returned as pending, and an event that is not creditable is
    not either. That way a second pass over the same day exercises the real
    idempotence rather than a canned answer.
    """

    def __init__(self, events=None, pending_ids=None, active_suppliers=(SUPPLIER,)):
        self.events = dict(events or {})
        self.pending_ids = list(pending_ids if pending_ids is not None
                                else self.events)
        self.active_suppliers = set(active_suppliers)
        self.memos = {}                      # short_ship_id -> insert params
        self.statements = []

    def cursor(self, *args, **kwargs):
        return _Cursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    # -- the SQL the pass issues -----------------------------------------
    def answer(self, flat, params):
        if 'INSERT INTO inv.supplier_credit_memos' in flat:
            short_ship_id = params[1]
            self.memos[short_ship_id] = params
            return [('memo-%d' % len(self.memos),)]

        # The daily pass's pending list. Checked BEFORE the single-event read
        # below: both mention `se.event_dt::date`, and the pass's statement is
        # the one that joins the vendor book.
        if 'FROM inv.short_ship_events se' in flat and 'JOIN inv.suppliers' in flat:
            rows = []
            for short_ship_id in self.pending_ids:
                event = self.events.get(short_ship_id)
                if not event or not event[2] or short_ship_id in self.memos:
                    continue
                if event[1] not in self.active_suppliers:
                    continue
                rows.append((short_ship_id, event[1]))
            return rows

        # open_credit_memo's own re-read of the event it was asked to claim.
        if 'se.event_dt::date' in flat:
            short_ship_id = params[0]
            if short_ship_id in self.memos:
                return []
            event = self.events.get(short_ship_id)
            if not event or not event[2]:
                return []
            event_date, _supplier, _creditable = event
            return [(short_ship_id, PRODUCT, STORE, 20.0, 4.0, 80.0,
                     event_date)]

        return []

    # -- helpers ---------------------------------------------------------
    def pending_sql(self):
        for sql, params in self.statements:
            if 'FROM inv.short_ship_events se' in sql and 'JOIN inv.suppliers' in sql:
                return sql, params
        return None, None


def _run(db, sim_dt=PASS_DT, vendors=None):
    return suppliers.open_pending_credit_memos(
        db, Config(), sim_dt, vendors if vendors is not None else [_vendor()])


# ---------------------------------------------------------------------------
# 1. The card's measurement: a receiving short-ship becomes a claim
# ---------------------------------------------------------------------------
def test_a_receiving_short_ship_becomes_a_claim():
    """The defect in one assertion: 147 events, 0 memos. One event in, one
    memo out."""
    event_date = PASS_DT.date() - timedelta(days=1)
    db = _DB({SHORT_SHIP: (event_date, SUPPLIER, True)})

    opened = _run(db)

    assert opened == 1, 'the pass walked past an unclaimed shortfall'
    assert list(db.memos) == [SHORT_SHIP], (
        'the memo must be filed against the event that caused it')
    assert db.memos[SHORT_SHIP][1] == SHORT_SHIP


def test_a_memo_is_only_filed_for_a_creditable_event():
    """`is_creditable = FALSE` is a TERMS fact — the vendor has no returns
    agreement — and filing a claim against it would be inventing money."""
    db = _DB({SHORT_SHIP: (PASS_DT.date(), SUPPLIER, False)})
    assert _run(db) == 0
    assert db.memos == {}


def test_an_inactive_vendor_is_not_claimed_against():
    """`advance_credit_memos` already ignores memos from inactive vendors, so a
    claim opened for one could never be submitted: don't open it."""
    db = _DB({SHORT_SHIP: (PASS_DT.date(), SUPPLIER, True)},
             active_suppliers=())
    assert _run(db) == 0
    assert db.memos == {}


def test_a_vendor_missing_from_the_catalogue_is_skipped():
    db = _DB({SHORT_SHIP: (PASS_DT.date(), SUPPLIER, True)})
    assert _run(db, vendors=[]) == 0
    assert db.memos == {}


# ---------------------------------------------------------------------------
# 2. The deadline belongs to the EVENT
# ---------------------------------------------------------------------------
def test_the_claim_deadline_is_the_events_own_date_plus_the_window():
    """A gap-fill pass can run weeks after the shortfall. Dating the deadline
    from the pass would move an old claim's window into the future."""
    event_date = date(2026, 10, 17)
    db = _DB({SHORT_SHIP: (event_date, SUPPLIER, True)})

    _run(db)                                    # pass runs 2026-10-25

    deadline = db.memos[SHORT_SHIP][9]           # claim_deadline
    assert deadline == date(2026, 10, 31), (
        'the deadline must be event_dt + credit_window_days (2026-10-17 + 14), '
        'not the pass date + 14 (2026-11-08)')
    assert deadline != PASS_DT.date() + timedelta(days=14)


def test_an_event_whose_window_already_closed_is_not_claimed():
    """Three weeks old against a 14-day window: the claim could never be
    submitted, so the honest record is the short-ship event alone."""
    event_date = PASS_DT.date() - timedelta(days=21)
    db = _DB({SHORT_SHIP: (event_date, SUPPLIER, True)})

    assert _run(db) == 0
    assert db.memos == {}, (
        'a memo born expired would show up in the book as paperwork nobody '
        'could have filed')


def test_the_window_edge_is_still_claimable():
    """`event_date + window == today` is the last day the claim can be filed,
    and the CHECK on `submitted_dt::date <= claim_deadline` allows it."""
    vendor = _vendor(window=14)
    event_date = PASS_DT.date() - timedelta(days=13)
    db = _DB({SHORT_SHIP: (event_date, SUPPLIER, True)})

    assert _run(db, vendors=[vendor]) == 1


# ---------------------------------------------------------------------------
# 3. Idempotence: a replayed day opens no second memo
# ---------------------------------------------------------------------------
def test_a_replayed_day_opens_no_second_memo():
    """A backfill re-runs days whose rows exist. The memo book must not grow a
    second claim for the same shortfall — the UNIQUE on `short_ship_id` is the
    backstop, not the mechanism."""
    event_date = PASS_DT.date()
    db = _DB({SHORT_SHIP: (event_date, SUPPLIER, True)})

    first = _run(db)
    second = _run(db)
    third = _run(db, sim_dt=PASS_DT + timedelta(days=1))

    assert (first, second, third) == (1, 0, 0)
    assert len(db.memos) == 1, 'the book grew a duplicate claim'


def test_the_pass_covers_every_unclaimed_event_in_one_day():
    """Not just the first: the pass walks the whole unclaimed set."""
    today = PASS_DT.date()
    db = _DB({
        SHORT_SHIP: (today, SUPPLIER, True),
        SHORT_SHIP_2: (today - timedelta(days=2), SUPPLIER, True),
    })
    assert _run(db) == 2
    assert set(db.memos) == {SHORT_SHIP, SHORT_SHIP_2}


def test_the_pass_claims_both_detection_shapes():
    """A DSD short and a receiving short are both claims.

    The query filters on `is_creditable`, the vendor and the window — never on
    `detected_source` — so the DSD half of the book cannot quietly go empty
    again the way the receiving half did (t_d74d67a3).
    """
    today = PASS_DT.date()
    db = _DB({
        SHORT_SHIP: (today, SUPPLIER, True),          # a receiving short
        SHORT_SHIP_2: (today - timedelta(days=1), SUPPLIER, True),  # a DSD short
    })

    assert _run(db) == 2
    sql, _params = db.pending_sql()
    assert sql is not None
    assert 'detected_source' not in sql, (
        'filtering the claim pass by detection shape would leave one half of '
        'the book unwritten')


# ---------------------------------------------------------------------------
# 4. The query's own bounds (the day grain is load-bearing)
# ---------------------------------------------------------------------------
def test_the_pending_query_is_bounded_to_the_longest_claim_window():
    """The scan is bounded, not a full table read on every midnight: the window
    is the LONGEST vendor term, because nothing older can still be claimed."""
    db = _DB({SHORT_SHIP: (PASS_DT.date(), SUPPLIER, True)})
    _run(db, vendors=[_vendor(window=7), _vendor(window=30,
                                                supplier_id='22222222-0000-0000-0000-000000000002')])

    sql, params = db.pending_sql()
    assert sql is not None
    assert 'se.is_creditable' in sql
    assert 's.is_active' in sql
    assert params[1] == 30, (
        'the bound must be the longest term on the book (30), or a 30-day '
        'vendor\'s claims are dropped by a 7-day vendor\'s window')
    assert params[0] == PASS_DT.date() and params[2] == PASS_DT.date()


def test_the_pending_query_compares_days_not_instants():
    """The backfill writes a day's receiving short-ships at 23:59 while the
    vendor phase runs at 23:30, so an instant-bounded pass would leave the
    backfill's own day permanently unclaimed."""
    db = _DB({SHORT_SHIP: (PASS_DT.date(), SUPPLIER, True)})
    _run(db)
    sql, _params = db.pending_sql()
    assert sql is not None, 'the pass never asked for its pending events'
    assert 'se.event_dt::date > (%s::date - %s::int)' in sql
    assert 'se.event_dt::date <= %s::date' in sql
    assert 'se.event_dt <' not in sql, (
        'an instant bound loses the backfill day written at 23:59')
    assert 'se.event_dt >' not in sql


# ---------------------------------------------------------------------------
# 5. The claim decision is per-event, not per-run
# ---------------------------------------------------------------------------
def test_the_claim_draw_is_a_function_of_the_event():
    """Replaying a day must not change the claim book, so the decision cannot
    come from `random`."""
    for short_ship_id in (SHORT_SHIP, SHORT_SHIP_2, 'CM-any-other-id'):
        first = suppliers.claim_is_filed(short_ship_id, 0.7)
        assert first == suppliers.claim_is_filed(short_ship_id, 0.7)


def test_the_claim_draw_honours_the_configured_rate():
    """`credit_claim_rate` must still mean what it says over a book of events —
    a per-event draw, not a per-run one."""
    ids = ['short-%04d' % i for i in range(4000)]
    claimed = sum(suppliers.claim_is_filed(i, 0.7) for i in ids)
    assert 0.66 <= claimed / len(ids) <= 0.74, (
        'claim rate 0.7 produced %.3f of the book' % (claimed / len(ids)))


def test_the_claim_draw_is_bounded_at_the_ends():
    assert suppliers.claim_is_filed(SHORT_SHIP, 1.0) is True
    assert suppliers.claim_is_filed(SHORT_SHIP, 0.0) is False
    assert suppliers.claim_is_filed(SHORT_SHIP, 3.0) is True
    assert suppliers.claim_is_filed(SHORT_SHIP, -1.0) is False


def test_a_zero_claim_rate_files_nothing(caplog):
    """The dial has to reach the pass, not just the helper."""
    db = _DB({SHORT_SHIP: (PASS_DT.date(), SUPPLIER, True)})
    cfg = Config()
    cfg.vendors.credit_claim_rate = 0.0
    assert suppliers.open_pending_credit_memos(
        db, cfg, PASS_DT, [_vendor()]) == 0
    assert db.memos == {}


# ---------------------------------------------------------------------------
# 6. The window helper
# ---------------------------------------------------------------------------
def test_the_window_is_the_longest_vendor_term():
    assert suppliers.claim_window_days([_vendor(window=7),
                                        _vendor(window=30)]) == 30


def test_an_empty_vendor_book_falls_back_to_the_default_window():
    assert suppliers.claim_window_days([]) == 14
    assert suppliers.claim_window_days(None) == 14


@pytest.mark.parametrize('window,expected', [(0, 1), (5, 5), (30, 30)])
def test_a_nonsense_window_still_allows_a_claim(window, expected):
    assert suppliers.claim_window_days([_vendor(window=window)]) == expected


# ---------------------------------------------------------------------------
# 7. The lifecycle can actually resolve a claim — t_d74d67a3, found by running it
# ---------------------------------------------------------------------------
# `advance_credit_memos` read `submitted_dt` (TIMESTAMPTZ -> an AWARE datetime)
# and compared it with the naive `sim_dt`:
#
#   TypeError: can't compare offset-naive and offset-aware datetimes
#
# It fired on the first memo that reached the paid stage of a real 8-day
# backfill, and `_run_vendor_phase` swallowed it as "Vendor phase failed ...
# continuing" — the same green-and-silent failure the card's own defect had. The
# comparison never ran before because the memo book was always empty.
class _MemoDB(_DB):
    """The claim book's cursor, plus the lifecycle's reads and resolution writes."""

    def __init__(self, submitted=(), open_unexpired=(), **kwargs):
        super().__init__(**kwargs)
        self.submitted = list(submitted)
        self.open_unexpired = list(open_unexpired)
        self.updates = []

    def cursor(self, *args, **kwargs):
        return _LifecycleCursor(self)

    def answer(self, flat, params):
        if 'UPDATE inv.supplier_credit_memos' in flat:
            self.updates.append(flat)
            return []
        if "m.memo_status = 'submitted'" in flat:
            return self.submitted
        if "m.memo_status = 'open'" in flat:
            if 'm.claim_deadline <= %s::date' in flat:
                return []
            return self.open_unexpired
        return super().answer(flat, params)


class _LifecycleCursor(_Cursor):
    """`_Cursor` plus the `rowcount` the lifecycle counts its UPDATEs with."""

    rowcount = 1


def test_a_submitted_memo_resolves_against_a_naive_simulated_clock():
    """The regression: a TIMESTAMPTZ `submitted_dt` (aware) versus the naive
    simulated clock must not raise, and a claim past settlement must resolve."""
    aware_submitted = datetime(2026, 10, 18, 23, 30, tzinfo=timezone.utc)
    db = _MemoDB(submitted=[('memo-1', 'warehouse_shortage', aware_submitted,
                             0, 0)])

    counts = suppliers.advance_credit_memos(
        db, Config(), datetime(2026, 10, 25, 23, 30))

    assert counts['paid'] + counts['rejected'] == 1, (
        'a claim past its vendor settlement time did not resolve')
    assert len(db.updates) == 1


def test_a_submitted_memo_inside_its_payment_terms_is_left_alone():
    aware_submitted = datetime(2026, 10, 24, 23, 30, tzinfo=timezone.utc)
    db = _MemoDB(submitted=[('memo-1', 'warehouse_shortage', aware_submitted,
                             10, 0)])

    counts = suppliers.advance_credit_memos(
        db, Config(), datetime(2026, 10, 25, 23, 30))

    assert counts['paid'] == counts['rejected'] == 0
    assert db.updates == [], (
        'a claim two days into a ten-day settlement was resolved early — '
        'days-to-pay would be a fiction')


def test_the_vendor_phase_resolves_what_it_opens_once_the_clock_advances():
    """End to end over the phase: day one files the claim, the same midnight
    submits it, and a later day pays or disputes it. This is the whole shape the
    card's mart needs (a memo that never resolves is a finding, not a state)."""
    today = PASS_DT.date()
    db = _MemoDB(events={SHORT_SHIP: (today, SUPPLIER, True)})

    # Day 1: the pass files the memo.
    opened = suppliers.open_pending_credit_memos(
        db, Config(), PASS_DT, [_vendor()])
    assert opened == 1

    # A claim past settlement resolves on a later pass — the comparison that
    # used to raise.
    aware_submitted = datetime(today.year, today.month, today.day, 23, 30,
                               tzinfo=timezone.utc)
    later = _MemoDB(submitted=[('memo-1', 'out_of_stock', aware_submitted, 1, 0)])
    counts = suppliers.advance_credit_memos(
        later, Config(), datetime(2026, 11, 3, 23, 30))
    assert counts['paid'] + counts['rejected'] == 1
