##############################################################################
--  dbt TEST: assert_balance_sheet_balances
--  Financial Intelligence Platform · HU GAAP
--
--  Rule    : Total Assets must equal Total Liabilities + Equity (balance sheet
--            equation: A = L + E) for every entity and every period.
--            Any imbalance greater than 1 HUF is a blocking failure.
--
--  Trigger : Runs as part of dbt test suite after agg_balance_sheet is built.
--  Severity: ERROR — pipeline halts, CFO is alerted, Power BI not refreshed.
--
--  HU GAAP accounting identity:
--    Eszközök (Assets) = Források (Liabilities + Equity)
--    Account classes 1-3 = Account class 4
##############################################################################

-- dbt tests return rows that represent failures.
-- This query must return 0 rows for the test to pass.

select
    period_key,
    entity_key,
    total_assets,
    total_liabilities_equity,
    balance_difference,
    'BALANCE_SHEET_IMBALANCE' as failure_reason
from {{ ref('agg_balance_sheet') }}

-- Aggregate to one row per period + entity (the model already has is_balanced flag)
-- We check uniquely per period/entity to avoid row-level duplicates
-- Use the is_balanced flag computed in the model itself
where is_balanced = 0
  and abs(balance_difference) > 1.00     -- tolerance: 1 HUF for rounding
