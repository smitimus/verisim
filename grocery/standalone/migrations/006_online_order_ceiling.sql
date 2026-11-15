-- 006_online_order_ceiling.sql — an online order's completion lands inside its
-- own lifecycle window (t_e1278564)
--
-- (Numbered 006: `004_generator_contract.sql` is in flight from the
-- backfill-window card, t_6bd8199e, and `005_online_order_clock.sql` is
-- t_c9ea180f's. `entrypoint.sh` applies every file in name order, so the numbers
-- only have to be unique.)
--
-- WHAT THIS FIXES
-- ---------------
-- `online.orders.completed_dt` is the pass clock at the instant the lifecycle
-- completed the order. 005 closed the lower half of that band (a completion
-- cannot precede its placement); this file closes the upper half.
--
-- The pass clock is a correct completion only while the generator's timeline is
-- contiguous. The test slot's timeline had a 34-day hole — the source retention
-- window, partly re-created by a backfill — so the first pass to see a
-- 2026-10-08 23:00 order again was 2026-11-15 00:00, and that became its
-- completion: 37 days 02 h, and 56 rows over 24 h on the slot the day this
-- landed. A contiguous 10-day backfill never does this (longest span 17 h), so
-- it is the clock jump, not the promise gate or a duration distribution.
--
-- Those rows are not history worth keeping: a 37-day `avg_hours_to_fulfil` made
-- both duration bounds of `mart_online_fulfillment` red on their own, and the
-- same numbers poison every on-time / SLA metric that reads the column.
--
-- THE BAND
-- --------
--   placed_dt <= completed_dt <= placed_dt + 24 h
--
-- 24 h is `online.max_fulfillment_hours` (config.yaml). It is a KNOB, not a
-- constraint: this file deliberately adds no CHECK for it, because a table
-- constraint cannot read the config file and a hardcoded 24 h would silently
-- contradict a slot that tunes the knob. The generator derives late completions
-- from the order (`models/online._lifecycle_instant`), and the other end of the
-- band is asserted by `TIME-05` in the integrity sweep — which is where a slot
-- that raises the knob has to raise its assertion too.
--
-- REPAIR, THEN MOVE THE EVENTS WITH IT
-- ------------------------------------
-- The same pass wrote the order's whole life at the replay date, so clamping
-- only the order would leave `order_events` disagreeing with it (measured 90
-- events beyond the 24 h window on the slot, 34 of them on stages that precede
-- the completion). Two statements, in this order:
--
--   1. the completion is clamped to the ceiling;
--   2. any event of a completed order that now sits after that completion is
--      pulled back to it — the terminal event IS the completion, and a stage
--      cannot follow the delivery it led to;
--   3. an order that never completed (no_show / cancelled) has no completion to
--      anchor to, so its events are clamped to its own ceiling.
--
-- IDEMPOTENT, AND IT HAS TO BE
-- ----------------------------
-- It runs on every boot of every volume, including one this image just
-- bootstrapped, where no row is out of band: all three statements then match
-- zero rows. `updated_at` is bumped on the rows the first statement repaired, so
-- a consumer's state clock sees the change.
--
-- The ingest is keyed on `placed_dt` (incremental for `online.orders`), so the
-- repaired rows — which are behind the watermark — only reach the EDW on a
-- wide-window run; `online.order_events` is a FULL refresh and moves on the next
-- ordinary run. That re-ingest is data-lab ticket t_114dc03f.
--
-- `test_online_order_clock.py` pins this file against a table in the shape every
-- pre-card data dir has (out-of-band completions, out-of-band events, no
-- constraint) and asserts a second run is a no-op.

UPDATE online.orders
   SET completed_dt = placed_dt + interval '24 hours',
       updated_at = NOW()
 WHERE completed_dt IS NOT NULL
   AND completed_dt > placed_dt + interval '24 hours';

-- The terminal event belongs at the completion it recorded; any other event of a
-- completed order belongs at or before it. Both are covered by one statement
-- because the completion is the upper bound of the order's own stream.
UPDATE online.order_events e
   SET event_dt = o.completed_dt
  FROM online.orders o
 WHERE o.order_id = e.order_id
   AND o.completed_dt IS NOT NULL
   AND e.event_dt > o.completed_dt;

-- No completion to anchor to (a no_show pickup, a cancelled order): the same
-- late pass wrote those events too, so they are bounded by the order's window.
UPDATE online.order_events e
   SET event_dt = o.placed_dt + interval '24 hours'
  FROM online.orders o
 WHERE o.order_id = e.order_id
   AND o.completed_dt IS NULL
   AND e.event_dt > o.placed_dt + interval '24 hours';
