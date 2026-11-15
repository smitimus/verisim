-- 003_fulfillment_lifecycle.sql — fulfillment.orders gets a real state machine
-- (t_5a408514)
--
-- WHAT THIS FIXES
-- ---------------
-- `fulfillment.orders.status` was the literal 'packed' on 16,658 of 16,658
-- rows, and its CHECK accepted only ('pending','picking','packed','loaded',
-- 'cancelled'). The generator now emits a lifecycle — received -> packed ->
-- shipped -> delivered — and writes 'shipped' from
-- `transport.dispatch_loads` and 'delivered' from
-- `transport.receive_delivered_loads`. Neither value is legal under the old
-- CHECK, so every fulfillment write on an EXISTING data dir would fail on its
-- first tick after the upgrade — on a volume that booted green, which is the
-- same shape as t_66647bbd (a green boot that fails on the first write).
--
-- `schema.sql` declares the widened, NAMED constraint for a first run. This
-- file is the other half: `entrypoint.sh` applies every `*.sql` here as
-- `postgres` on every boot (ON_ERROR_STOP=1), so this is the only path a
-- schema change has to a data dir that already exists.
--
-- IDEMPOTENT, AND IT HAS TO BE
-- ---------------------------
-- It runs on every boot of every volume, including one this image just
-- bootstrapped — where `fulfillment_orders_status_lifecycle` is already
-- present and the old `orders_status_check` is not. The guard is on the new
-- name, so a second boot does nothing at all.
--
-- No rows need repairing: every value the generator has ever written
-- ('pending', 'picking', 'packed', 'loaded', 'cancelled') stays inside the new
-- set, and the existing 16k 'packed' rows keep loading exactly as before. The
-- old rows keep their degenerate `started_at = completed_at` — that is history,
-- not something a migration should invent values for; a reseed or a forced
-- backfill replaces them (the force clear in `base/api/main.py` keys on
-- `created_at`, which is now the order's own business clock, so a forced
-- backfill of a past window finally clears these rows at all).
--
-- `test_fulfillment_lifecycle.py` pins this file against a table in the shape
-- every pre-card data dir has (an auto-named `orders_status_check`).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'fulfillment.orders'::regclass
          AND conname = 'fulfillment_orders_status_lifecycle'
    ) THEN
        -- The auto-generated name of the inline CHECK in every earlier
        -- `schema.sql`. Both branches are guarded, so a data dir that carries
        -- neither shape converges instead of failing the boot.
        ALTER TABLE fulfillment.orders
            DROP CONSTRAINT IF EXISTS orders_status_check;

        ALTER TABLE fulfillment.orders
            ADD CONSTRAINT fulfillment_orders_status_lifecycle CHECK (
                status IN ('pending','received','picked','picking',
                           'packed','loaded','shipped','delivered',
                           'cancelled')
            );
    END IF;
END
$$;
