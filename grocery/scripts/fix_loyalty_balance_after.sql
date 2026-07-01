-- =============================================================================
-- Fix loyalty_point_transactions.balance_after for existing data
--
-- The previous generator code calculated balance_after by reading
-- loyalty_members.points_balance (the cumulative balance including future
-- transactions). When backfill processed chronologically-earlier transactions
-- after realtime had already updated the member's balance, the balance_after
-- was inflated.
--
-- This script recalculates balance_after using a cumulative window sum per
-- member, ordered by (transaction_dt, pt_id) to ensure chronological order.
-- It also updates loyalty_members.points_balance to match the final sum.
--
-- Run this once against the grocery database AFTER deploying the generator
-- fix (Verisim #9).
-- =============================================================================

WITH fixed AS (
    SELECT
        pt.pt_id,
        pt.member_id,
        COALESCE(SUM(pt.points_earned) OVER w, 0)
            - COALESCE(SUM(pt.points_redeemed) OVER w, 0) AS correct_balance_after
    FROM pos.loyalty_point_transactions pt
    LEFT JOIN pos.transactions t ON t.transaction_id = pt.transaction_id
    WINDOW w AS (
        PARTITION BY pt.member_id
        ORDER BY t.transaction_dt NULLS FIRST, pt.created_at, pt.pt_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)
UPDATE pos.loyalty_point_transactions pt
SET balance_after = f.correct_balance_after
FROM fixed f
WHERE pt.pt_id = f.pt_id
  AND pt.balance_after != f.correct_balance_after;

-- Update loyalty_members.points_balance to match the recalculated point total
UPDATE pos.loyalty_members m
SET points_balance = COALESCE(
    (SELECT SUM(points_earned) - SUM(points_redeemed)
     FROM pos.loyalty_point_transactions pt
     WHERE pt.member_id = m.member_id), 0),
    updated_at = NOW();

-- Log results
DO $$
DECLARE
    fixed_rows INTEGER;
    member_rows INTEGER;
BEGIN
    GET DIAGNOSTICS fixed_rows = ROW_COUNT;
    SELECT COUNT(*) INTO member_rows
    FROM pos.loyalty_members m
    WHERE m.points_balance != COALESCE(
        (SELECT SUM(points_earned) - SUM(points_redeemed)
         FROM pos.loyalty_point_transactions pt
         WHERE pt.member_id = m.member_id), 0);
    RAISE NOTICE 'Fixed % point_transaction rows. % members still mismatched (should be 0).',
        fixed_rows, member_rows;
END $$;
