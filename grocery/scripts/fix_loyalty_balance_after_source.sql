-- =============================================================================
-- fix_loyalty_balance_after_source.sql
--
-- Recalculates balance_after for existing loyalty_point_transactions rows
-- in the SOURCE Postgres (Verisim) to match sequential running-sum logic.
--
-- The generator's _record_loyalty_points() was writing balance_after =
-- loyalty_members.points_balance + points_earned, where points_balance is
-- the global cumulative total that includes future realtime transactions.
-- When backfill inserted chronologically-earlier transactions after realtime
-- had already updated the member's balance, balance_after was inflated.
--
-- This migration recomputes balance_after as a running sum per member,
-- ordered by (t.transaction_dt, lpt.pt_id) using pt_id as the tiebreaker.
--
-- Safe to re-run (idempotent).
--
-- NOTE: Uses source PG column names (balance_after, not points_balance_after).
-- =============================================================================

-- Step 1: Recalculate balance_after for every row using a windowed running sum.
-- Order by (t.transaction_dt, lpt.pt_id) for source ordering.
WITH running AS (
    SELECT
        lpt.pt_id,
        lpt.member_id,
        t.transaction_dt,
        lpt.points_earned,
        lpt.points_redeemed,
        lpt.balance_after AS old_balance,
        SUM(lpt.points_earned - lpt.points_redeemed) OVER (
            PARTITION BY lpt.member_id
            ORDER BY t.transaction_dt, lpt.pt_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS new_balance
    FROM pos.loyalty_point_transactions lpt
    LEFT JOIN pos.transactions t ON t.transaction_id = lpt.transaction_id
)
UPDATE pos.loyalty_point_transactions lpt
SET balance_after = r.new_balance
FROM running r
WHERE lpt.pt_id = r.pt_id
  AND r.old_balance != r.new_balance;

-- Report how many rows were fixed
DO $$
DECLARE
    fixed INT;
BEGIN
    GET DIAGNOSTICS fixed = ROW_COUNT;
    RAISE NOTICE 'Fixed % loyalty_point_transactions balance_after values', fixed;
END $$;

-- Step 2: Update loyalty_members.points_balance to the latest balance_after
-- value for each member (so the member table reflects the true cumulative
-- balance from committed transaction history).
WITH latest AS (
    SELECT DISTINCT ON (member_id)
        member_id,
        balance_after AS true_balance
    FROM pos.loyalty_point_transactions
    ORDER BY member_id, pt_id DESC
)
UPDATE pos.loyalty_members m
SET points_balance = l.true_balance
FROM latest l
WHERE m.member_id = l.member_id::uuid
  AND m.points_balance != l.true_balance;

DO $$
DECLARE
    fixed INT;
BEGIN
    GET DIAGNOSTICS fixed = ROW_COUNT;
    RAISE NOTICE 'Updated % loyalty_members points_balance to computed true balance', fixed;
END $$;
