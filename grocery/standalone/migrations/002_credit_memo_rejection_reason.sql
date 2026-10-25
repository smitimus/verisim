-- 002_credit_memo_rejection_reason.sql — a rejected claim says why (t_ec78f608)
--
-- WHAT THIS FIXES
-- ---------------
-- `inv.supplier_credit_memos` rejected claims with a NULL `rejection_reason`
-- for three of its five short reasons. `REASON_PAY_PROFILE` carried the reason
-- text only for `delivery_missed` and `quality_reject`; the other three —
-- `warehouse_shortage` (8% of claims disputed), `out_of_stock` (12%) and
-- `weather_carrier_delay` (30%) — were `(pay_rate, None)`, and
-- `advance_credit_memos` writes that second element straight into the reject
-- UPDATE. Measured on CT107 after the 2026-10-25 reseed: 60 of 119 rejected
-- memos had no reason, and data-lab's `assert_credit_memo_timeline_is_coherent`
-- counted all 60.
--
-- The generator side is fixed in the same commit (`models/suppliers.py` now
-- carries the text for all five profiles and refuses to import a disputable
-- profile without one). This file is the two things `schema.sql` alone cannot
-- reach:
--
--   1. the ROWS already rejected with a NULL reason on an existing data dir,
--      repaired to the text the profile now defines — derived from the row's
--      own `short_reason`, which the schema constrains to the same closed set,
--      so this is a recomputation, not invented prose;
--   2. the `credit_rejected_requires_reason` CHECK itself, which `schema.sql`
--      declares but which only reaches a FIRST run.
--
-- WHY THE REPAIR HAS TO COME FIRST
-- --------------------------------
-- Adding the CHECK to a data dir that already holds NULL-reason rejections
-- fails, and a failed migration fails the boot (ON_ERROR_STOP=1) — which is the
-- honest signal, but it would leave every existing slot crash-looping. The
-- UPDATE runs before the constraint for that reason, and both are idempotent:
-- re-running on a bootstrapped dir updates nothing and finds the constraint
-- already present.
--
-- The five texts below are the same strings as `REASON_PAY_PROFILE` in
-- `grocery/generator/models/suppliers.py`. `test_supplier_schema.py` pins the
-- pair, because two copies of one mapping drift and the copy that drifts is the
-- one nothing runs.

UPDATE inv.supplier_credit_memos
   SET rejection_reason = CASE short_reason
           WHEN 'warehouse_shortage'
               THEN 'Vendor''s own warehouse shortage — claim disputed'
           WHEN 'out_of_stock'
               THEN 'Vendor could not restock — claim disputed'
           WHEN 'weather_carrier_delay'
               THEN 'Weather / carrier delay — claim disputed'
           WHEN 'delivery_missed'
               THEN 'Delivery window missed — claim disputed'
           WHEN 'quality_reject'
               THEN 'Goods rejected at our dock — claim denied'
           -- Unreachable: `short_reason` is a CHECKed closed set and the profile
           -- test pins the two mappings together. It exists so a sixth reason
           -- added ahead of this file still converges the constraint instead of
           -- failing the boot.
           ELSE 'Claim disputed'
       END,
       updated_at = NOW()
 WHERE memo_status = 'rejected'
   AND (rejection_reason IS NULL OR btrim(rejection_reason) = '');

-- Guarded: a data dir bootstrapped by this image already has the constraint
-- (schema.sql declares it), so the only way this branch runs is a legacy dir.
-- Added VALIDATED, not NOT VALID, because the UPDATE above just made every row
-- conform — a NOT VALID constraint would never be checked retroactively and
-- would let the old rows sit there looking fine.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'inv.supplier_credit_memos'::regclass
          AND conname = 'credit_rejected_requires_reason'
    ) THEN
        ALTER TABLE inv.supplier_credit_memos
            ADD CONSTRAINT credit_rejected_requires_reason CHECK (
                memo_status <> 'rejected'
                OR (rejection_reason IS NOT NULL
                    AND btrim(rejection_reason) <> '')
            );
    END IF;
END
$$;
