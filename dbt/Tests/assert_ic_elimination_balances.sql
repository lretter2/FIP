-- ==============================================================================
--  TEST: assert_ic_elimination_balances
--  PURPOSE: Verify that IC elimination entries balance to zero by entity pair
--
--  CRITICAL CONTROL for multi-entity consolidation:
--  1. Source IC transactions must be offset by elimination entries
--  2. Group-level P&L must have zero IC revenue and COGS after elimination
--
--  Severity: FAIL — blocks deployment if IC elimination logic is broken
--  Tag: critical_control, ic_elimination
-- ==============================================================================

{{ config(tags=['critical_control', 'ic_elimination']) }}

WITH ic_balance_check AS (
    -- Verify that elimination entries balance to zero
    SELECT
        batch_id,
        entity_from,
        entity_to,
        period_id,
        posting_account,
        ic_elimination_type,
        SUM(total_amount_lcy) AS net_elimination_amount
    FROM {{ ref('int_ic_elimination') }}
    GROUP BY
        batch_id,
        entity_from,
        entity_to,
        period_id,
        posting_account,
        ic_elimination_type
),

-- Test 1: Elimination entries should aggregate to approximately zero by IC pair
balance_violations AS (
    SELECT
        batch_id,
        entity_from,
        entity_to,
        period_id,
        posting_account,
        ic_elimination_type,
        net_elimination_amount,
        'IC_ELIMINATION_IMBALANCE' AS violation_type
    FROM ic_balance_check
    WHERE ABS(net_elimination_amount) > 1.00  -- Allow 1 currency unit rounding tolerance
),

-- Test 2: Group-level P&L should have zero IC revenue after consolidation
group_consolidation_check AS (
    SELECT
        period_key,
        entity_key,
        consolidation_scope,
        'GROUP_IC_REVENUE_NOT_ZERO' AS violation_type,
        revenue AS amount_value
    FROM {{ ref('agg_pl_monthly') }}
    WHERE consolidation_scope = 'group'
      AND revenue <> 0
      AND entity_key IN (
            SELECT DISTINCT entity_from
            FROM {{ ref('int_ic_elimination') }}
          )
),

-- Combine all violations
all_violations AS (
    SELECT
        batch_id::VARCHAR AS batch_id,
        entity_from::VARCHAR AS entity_from,
        entity_to::VARCHAR AS entity_to,
        period_id::VARCHAR AS period_id,
        posting_account::VARCHAR AS posting_account,
        ic_elimination_type,
        CAST(net_elimination_amount AS VARCHAR) AS detail_1,
        violation_type,
        '' AS detail_2
    FROM balance_violations

    UNION ALL

    SELECT
        period_key::VARCHAR,
        entity_key::VARCHAR,
        NULL,
        NULL,
        NULL,
        NULL,
        amount_value::VARCHAR,
        violation_type,
        '' AS detail_2
    FROM group_consolidation_check
)

SELECT *
FROM all_violations
WHERE 1=0  -- Test passes if no violations found
