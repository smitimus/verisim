-- 005_online_order_clock.sql — an online order cannot be completed before it was
-- placed (t_c9ea180f)
--
-- (Numbered 005, not 004: `004_generator_contract.sql` is in flight from the
-- backfill-window card, t_6bd8199e. The entrypoint applies every file in name
-- order, so the two are independent — but two `004_` files is a trap for the
-- next person who reads the directory.)
--
-- WHAT THIS FIXES
-- ---------------
-- `online.orders.completed_dt` is the pass clock at the instant the lifecycle
-- completed the order, so it can only land before `placed_dt` if a pass advanced
-- a row whose own clock had not been reached. That is what the generator did on
-- every replay: `advance_online_lifecycle` selected every open order with no
-- bound on `placed_dt`, and the `ready` / `out_for_delivery` branches completed
-- on a bare dice roll. A backfill replaying hour 0 of a day whose later hours
-- were already written therefore stamped `completed_dt = 00:00` on orders placed
-- at 13:00. Measured on dev: 988 of 510,405 completed orders, worst -20 h 46 m;
-- 2 had already reached the EDW raw layer.
--
-- The generator fix (`models/online.advance_online_lifecycle`) refuses to touch a
-- row the clock has not reached and gates completion on the promised delivery
-- time / pickup window. `schema.sql` declares the NAMED constraint below for a
-- first run; this file is the other half — `entrypoint.sh` applies every `*.sql`
-- here as `postgres` on every boot with ON_ERROR_STOP=1, and that is the only
-- path a schema change has to a data dir that already exists.
--
-- REPAIR FIRST, THEN VALIDATE
-- ---------------------------
-- `ADD CONSTRAINT` VALIDATES the table, so on a volume that already holds the
-- violations it fails and aborts the boot (a crash-loop, not a fixed data dir).
-- The UPDATE below runs first and moves each offending completion to the first
-- instant the order could plausibly have completed under the new law:
--
--   pickup   -> 15 min after its pickup window opened
--   delivery -> 15 min before its promise
--
-- both floored at placed_dt + 45 min (an order cannot be picked, staged and
-- collected faster than that). `updated_at` is bumped so a consumer's state
-- clock sees the change — the ingest's incremental window is keyed on
-- `placed_dt`, so those rows only reach raw on a wide-window run.
--
-- IDEMPOTENT, AND IT HAS TO BE
-- ----------------------------
-- It runs on every boot of every volume, including one this image just
-- bootstrapped, where the constraint is already present and no row violates it.
-- The UPDATE matches zero rows and the ADD is guarded on `pg_constraint`.
--
-- `test_online_order_clock.py` pins this file against a table in the shape every
-- pre-card data dir has (the constraint absent, the offending rows present) and
-- asserts a second run is a no-op.

UPDATE online.orders
   SET completed_dt = GREATEST(
           placed_dt + interval '45 minutes',
           CASE WHEN fulfillment_type = 'pickup'
                THEN COALESCE(pickup_window_start, placed_dt) + interval '15 minutes'
                ELSE COALESCE(promised_delivery_dt, placed_dt + interval '45 minutes')
                     - interval '15 minutes'
           END),
       updated_at = NOW()
 WHERE completed_dt IS NOT NULL
   AND completed_dt < placed_dt;

-- The same pass wrote the EVENT that completed the order, so the same rows also
-- left `online.order_events.event_dt` before the order existed (measured 12 on
-- the test slot). A terminal event belongs at the completion it recorded, which
-- the statement above has just made legal; anything else that is still stranded
-- (a cancelled or no_show event, whose order has no completion) sits at the
-- placement — the earliest instant any event of that order can honestly carry.
--
-- data-lab loads `online.order_events` with a FULL refresh, so these values
-- reach raw on the next ingest run without a wide-window override.
UPDATE online.order_events e
   SET event_dt = GREATEST(o.completed_dt, o.placed_dt)
  FROM online.orders o
 WHERE o.order_id = e.order_id
   AND e.event_dt < o.placed_dt
   AND o.completed_dt IS NOT NULL
   AND e.event_type IN ('picked_up', 'delivered');

UPDATE online.order_events e
   SET event_dt = o.placed_dt
  FROM online.orders o
 WHERE o.order_id = e.order_id
   AND e.event_dt < o.placed_dt;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'online.orders'::regclass
          AND conname = 'online_orders_completed_after_placed'
    ) THEN
        ALTER TABLE online.orders
            ADD CONSTRAINT online_orders_completed_after_placed
            CHECK (completed_dt IS NULL OR completed_dt >= placed_dt);
    END IF;
END $$;
