# There is no "pre-deploy era" in verisim's weekly-ad pricing

**Status:** verified 2026-10-04 against both live slots (CT106 `dev`, CT107 `test`).
Cross-referenced by data-lab cards `t_4de1aba8` / `t_2c146d25`, and by the guard
`assert_ad_windows_predate_ad_pricing_are_not_read_as_a_discount`.

## The claim this document retires

Both data-lab cards reason about a boundary at the `price_of_record` deploy
(commit `62d16c6`, t_08deeddf, **2026-09-30 23:16 -0400**): that ad windows which
*closed before* that instant were written by code that never charged
`promoted_price`, so their lift is an artefact. `t_4de1aba8` shipped a guard
asserting on exactly that premise, and filed `t_2c146d25` because my own
measurement contradicted it.

**That premise does not hold on either slot. There is no sale-date boundary.**

## What was measured

Coverage = share of an ad line rung at its own `promoted_price`. Both slots run
the same image sha `b1589f84`, so any difference between them is dataset vintage,
not code.

### CT107 — per window × day-offset

    window start   off 0    off 1..6
    2026-08-31     0.00%   100.00%   (only offsets 4,5,6 have data)
    2026-09-07     0.00%   100.00%
    2026-09-14     0.00%   100.00%
    2026-09-21     0.00%   100.00%
    2026-09-28    100.00%  100.00%   <- the window in force when the ad was created

Split at the deploy date, as `t_2c146d25` asks:

    era           day 0    day 1-3,5,6   day 4
    pre-deploy    0.00%    100.00%       66.75%
    post-deploy  100.00%   100.00%      100.00%

The "day 4 → 66.75%" row is **not** a second defect, and 2026-09-04 is not a
day-0. Splitting CT107's 2,478 pre-deploy lines at offset 4 by window:

    window start   lines   at_promo
    2026-08-31      824      0.00%   <- 2026-09-04
    2026-09-07      822    100.00%
    2026-09-14      347    100.00%
    2026-09-21      485    100.00%
    2026-09-28      577    100.00%
    total          2,478   66.75%   (matches the era aggregate exactly)

Per-day coverage on CT107 makes the real shape plain — it is **every Monday,
0.00%**, and 100.00% on every other day:

    2026-09-04 Fri   0.00%    2026-09-14 Mon   0.00%
    2026-09-05 Sat 100.00%    2026-09-15 Tue 100.00%
    ...                          ...
    2026-09-07 Mon   0.00%    2026-09-28 Mon 100.00%  <- created by seed_all
    2026-09-08 Tue 100.00%    2026-09-29 Tue 100.00%

2026-09-04 is a **Friday**, four days into the 08-31 window, and it is the
earliest day of history on this slot (no transactions before it). Its 0.00% is
the same mechanism seen from the other end: it is the first day the backfill
wrote, so it was written before *any* ad row existed — including the 08-31
window's. Every Monday below it is instead a window's `start_date`. Both are
"ad absent while its own day is written"; which date it shows up on depends only
on the order the generator happened to walk its days. So offset 4 aggregates
0.00% (the Friday) with three healthy windows, and 824/2,478 = 33.25% off-promo
gives 66.75%. One defect shape on this slot, not two, and it is t_3902120b's.

(The backfill starts 09-04 and every ad row carries `created_at` 10-04, so no
date before 09-04 exists to be measured at all — which is why the 08-31 window
shows offsets 4,5,6 only, with no 0,1,2,3.)

### CT106 — per day, no offset grouping

    2026-08-22 .. 2026-10-02   0.00%  (every day, no exception)
    2026-10-03                  3.19%
    2026-10-04                 76.48%

**Flat zero for 42 consecutive days, then coverage appears.** A deploy-date era
boundary would produce a *partial* day at the cutover (2026-09-30) and full
coverage after it. This produces nothing at all until 10-03, two days *after*
the deploy — i.e. it lines up with **when the data was written**, not when the
sales happened.

The 10-03/10-04 tail is this slot's **partial day-0 recovery**, not the start of
a healthy era: on CT106 the realtime path is the only thing creating ads now, so
2026-10-04 (today, the window `seed_all` created) is written correctly and
10-03 is caught mid-repair. It is the same defect at a different phase, which is
why CT106 and CT107 look so different while being one bug.

## Why: the ad row is written after the day it covers

`pricing.weekly_ads.created_at` on CT107:

    window start   created_at    lag
    2026-08-31     2026-10-04    +34 days
    2026-09-07     2026-10-04    +27 days
    2026-09-14     2026-10-04    +20 days
    2026-09-21     2026-10-04    +13 days
    2026-09-28     2026-10-04    +6 days

Every ad was materialised on 2026-10-04 — the slot's regeneration date. The
backfill writes a day's transactions *before* the row that says "there is an ad
covering that day" exists, so `get_ad_product_prices(cur_date)` returns empty and
every line of that day falls back to shelf price. The next day's transactions
find the ad. That is the whole mechanism, and it is **write-time, not
sale-date**: the boundary is *the generation run*, not a commit.

CT107 shows it at week granularity (each backfilled Monday) and CT106 at day
granularity (every day of its whole history), for one reason — CT107's
`seed_all` had already created the current week's ad before the backfill ran, so
the newest window is the only healthy one; CT106 never got that head start.

This is the defect already filed and fixed as **t_3902120b** (the ad lifecycle
ran in the end-of-day block, after the day's hours were written; `_weekly_ad_prices`
now runs before them). It has no deploy-date component at all.

## Consequences for data-lab's guard

`assert_ad_windows_predate_ad_pricing_are_not_read_as_a_discount` filters
`ad_week_end < date '2026-09-30'`. On CT107 that flags 8 rows across 4 windows.
Those 8 rows are flagged **for the day-0 defect, which the guard's own header
already names as t_3902120b** — not because the window predates a deploy. Once
t_3902120b is fixed and the source is regenerated, the flagged rows are still
pre-deploy by date, so **the guard stays RED on healthy data**.

Its negative control (boundary moved off the data → PASS) proves only that the
date comparison works. It cannot prove the era it encodes is real, because no
slot exhibits one.

## What is actually true, and worth guarding

* On a slot whose history was **regenerated** (CT107): every day of every
  backfilled window after day 0 is charged the advertised price.
* On a slot whose history was **not** regenerated (CT106's vintage): nothing is,
  for the whole 42-day window.
* Both are one bug, already owned by t_3902120b.

There is no dataset in which "pre-deploy windows were written by different code
than post-deploy windows". Coverage is decided by **whether the ad row existed
when that day's hours were written**, and that is decided by the generator's
phase ordering, uniformly for every day of history.

## Recommendation to data-dev

Retire the deploy-date era premise. Either:

1. **Fix t_3902120b first, then** re-measure — if the source is repaired and
   regenerated, coverage should be 100.00% at *every* offset on *every* window
   on *both* slots, and there is no era for a guard to key on; or
2. Keep the sibling `assert_ad_items_were_charged_at_promoted_price` (categorical:
   is the advertised price rung at all per item-week) which correctly fires on
   CT106's vintage (127/149 at `at_promo = 0`) and passes on a healthy slot.

A coverage **threshold** is the only form that could describe this data, and it
was already rejected on t_4de1aba8 as a constant that fits whichever dataset is
deployed. That rejection still stands — the new fact is that the era framing
should go too.
