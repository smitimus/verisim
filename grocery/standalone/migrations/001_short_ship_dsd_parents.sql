-- 001_short_ship_dsd_parents.sql — a DSD short-ship has no fulfillment (t_d74d67a3)
--
-- WHAT THIS FIXES
-- ---------------
-- A shortfall is detected in one of two places and only ONE of them has a
-- fulfillment behind it:
--
--   detected_source = 'receiving'     found at our dock on one of our own
--                                     transport.loads -> the fulfillment line
--                                     and order exist, and both columns are
--                                     NOT NULL.
--   detected_source = 'dsd_delivery'  found on the shelf after the vendor's own
--                                     truck left. There is no pallet on our
--                                     loads and no fulfillment.orders row, so
--                                     both columns are legitimately NULL.
--
-- Before this migration the two columns were NOT NULL, which is why the DSD
-- short-ship path could never insert: it had to invent
-- `00000000-0000-0000-0000-<random>` uuids for them, the foreign keys rejected
-- them, and `_run_vendor_phase` swallowed the error — so `inv.supplier_credit_memos`
-- was empty on every seed. `schema.sql` now declares the columns nullable plus a
-- `short_ship_requires_its_parents` CHECK that ties the shape to
-- `detected_source`.
--
-- WHY A MIGRATION FILE AND NOT JUST schema.sql
-- --------------------------------------------
-- `entrypoint.sh` applies `schema.sql` only on a FIRST run (it is gated on
-- `$PG_DATA/PG_VERSION` being absent), and the generator role does not own the
-- tables, so an `ALTER` from the generator is refused (`InsufficientPrivilege`,
-- measured — see models/customers.py::ensure_tables). `schema_reconcile` also
-- only adds columns/indexes/tables; it deliberately never drops or rewrites.
-- So a `DROP NOT NULL` in schema.sql would reach a fresh data dir and no
-- existing one (CT107 included), and the new INSERT would raise there while the
-- image looked healthy.
--
-- The entrypoint now applies every file in /app/migrations in name order on
-- EVERY boot, as `postgres` (the owner), after `schema.sql` on a first run and
-- after a brief start on an existing data dir. This file is therefore written
-- to be idempotent: `DROP NOT NULL` on an already-nullable column is a no-op,
-- and the constraint is added only when the catalog does not have it. Applying
-- it to a data dir this image bootstrapped does nothing at all.
--
-- The migration decision is recorded on the data-lab ticket filed with this
-- change: the alternative was wiping the data dir (mock data, but a slot's
-- backfill), and an idempotent boot-time migration keeps an existing volume
-- current without a wipe.

ALTER TABLE inv.short_ship_events
    ALTER COLUMN fulfillment_item_id DROP NOT NULL,
    ALTER COLUMN fulfillment_id      DROP NOT NULL;

-- Guarded: a data dir bootstrapped by this image already has the constraint
-- (schema.sql declares it), so the only way this branch runs is a legacy dir.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'inv.short_ship_events'::regclass
          AND conname = 'short_ship_requires_its_parents'
    ) THEN
        ALTER TABLE inv.short_ship_events
            ADD CONSTRAINT short_ship_requires_its_parents CHECK (
                (detected_source = 'receiving'
                 AND fulfillment_item_id IS NOT NULL
                 AND fulfillment_id IS NOT NULL)
                OR
                (detected_source = 'dsd_delivery'
                 AND fulfillment_item_id IS NULL
                 AND fulfillment_id IS NULL)
            );
    END IF;
END
$$;
