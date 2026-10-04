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

The "day 4 → 66.75%" row is **not** a second defect. Offset 4 is 2026-09-04 for
four windows but 2026-10-02 for the 2026-09-28 window; reconciling the 51,143
lines at offset 4 shows **633 of them come from the 08-31 window** and the other
50,510 from later windows, of which **0** are at the promoted price. The 66.75%
aggregate is the 08-31 window's day-0 defect (0.00%) diluted by the healthy
windows. There is one defect shape on this slot, not two.

### CT106 — per day, no offset grouping

    2026-08-22 .. 2026-10-02   0.00%  (every day, no exception)
    2026-10-03                  3.19%
    2026-10-04                 76.48%

**Flat zero for 42 consecutive days, then coverage appears.** A deploy-date era
boundary would produce a *partial* day at the cutover (2026-09-30) and full
coverage after it. This produces nothing at all until 10-03, two days *after*
the deploy — i.e. it lines up with **when the data was written**, not when the
sales happened.

## Why: the ad row is written after the day it covers

`pricing.weekly_ads.created_at` on CT107:

    window start   created_at    lag
    2026-08-31     2026-10-04    +34 days
    2026-09-07     2026-10-04    +27 days
    2026-09-14     2026-10-04    +20 days
    2026-09-21     2026-10-04    +13 days
    2026-09-28     2026-10-04    +6 days

Every ad was materialised on 2026-10-04 — the slot's regeneration date. The
backfill writes 2026-08-31's transactions *before* the row that says "there is an
ad covering 2026-08-31" exists, so `get_ad_product_prices(cur_date)` returns
empty and every line of that day falls back to shelf price. The next day's
transactions find the ad. That is the whole mechanism, and it is
**write-time, not sale-date**: the boundary is *the generation run*, not a
commit.

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
