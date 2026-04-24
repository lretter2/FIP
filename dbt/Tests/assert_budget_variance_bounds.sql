##############################################################################
--  dbt SINGULAR TEST: assert_budget_variance_bounds
--  Financial Intelligence Platform · HU GAAP
--  Rule ID : DQ-011
--
--  PURPOSE
--  -------
--  Detects budget lines that deviate from prior-year actuals by more than
--  a configurable multiple (default 3×) in either direction.
--
--  This catches the most common budget upload error in Excel-driven finance
--  teams: entering amounts in the wrong unit (e.g. HUF 4.5 instead of
--  HUF 4,500,000 because the cell was pre-formatted in millions).
--  A 3× deviation from PY actuals on the same P&L line is almost always
--  either a deliberate step-change (in which case the Finance team should
--  annotate it) or a data entry error.
--
--  HU GAAP RELEVANCE
--  -----------------
--  The 2000/C Act §154 requires that annual budgets and the accompanying
--  variance reports be consistent and verifiable.  Materially wrong budgets
--  corrupt the variance analysis used by the CFO dashboard and Power BI
--  reports, leading to incorrect RAG status indicators.
--
--  TRIGGER
--  -------
--  Runs after the budget.fact_budget model is seeded/loaded (dbt seed or
--  dbt run --models budget_monthly).  Also callable standalone:
--    dbt test --select assert_budget_variance_bounds
--
--  SEVERITY : WARN
--    Does not block the Gold Zone build, but emits one failure row per
--    offending (entity, account, period) combination so the Finance team
--    can review them in the dbt test report before the Power BI refresh.
--
--  CONFIGURATION (via dbt vars — set in dbt_project.yml or --vars CLI)
--  -----------------------------------------------------------------------
--  budget_variance_max_factor : NUMERIC  (default 3.0)
--      A budget line is flagged when:
--        budget_amount > ABS(py_actual) * factor   (too high)
--        OR budget_amount > 0 AND
--           budget_amount < ABS(py_actual) / factor  (too low / suspiciously small)
--
--  budget_min_py_actual_huf : NUMERIC  (default 500000)
--      Minimum prior-year actual (in HUF) for a line to be checked.
--      Lines where |py_actual| < this threshold are excluded to avoid false
--      positives on low-volume accounts (e.g. minor sundry income accounts
--      where 3× variance is normal).
--
--  USAGE EXAMPLE
--  dbt test --select assert_budget_variance_bounds \
--           --vars '{"budget_variance_max_factor": 5.0, "budget_min_py_actual_huf": 1000000}'
##############################################################################

-- dbt tests return rows that represent failures.
-- This query must return 0 rows for the test to PASS (severity: WARN).

with

-- ── Active budget for all entities / periods ─────────────────────────────────
active_budget as (
    select
        b.entity_key,
        b.account_key,
        b.period_key,
        b.budget_amount_lcy,
        -- prior-year same-month period key (YYYYMM arithmetic)
        ((b.period_key / 100) - 1) * 100 + (b.period_key % 100) as prior_year_period_key
    from {{ ref('agg_pl_monthly') }} b  -- budget_amount_lcy is pre-joined in agg_pl_monthly
    where b.revenue_budget is not null
      and b.period_key >= cast({{ var('fiscal_year_start_month', 1) }} as int)
),

-- ── Prior-year actuals rolled up to account + entity + period ────────────────
prior_year_actuals as (
    select
        entity_key,
        account_key,
        period_key,
        sum(net_amount_lcy) as py_actual_lcy
    from {{ ref('fct_gl_transaction') }}
    group by entity_key, account_key, period_key
),

-- ── Join and apply variance factor thresholds ────────────────────────────────
flagged_lines as (
    select
        b.entity_key,
        b.account_key,
        b.period_key,
        b.budget_amount_lcy,
        py.py_actual_lcy,
        -- Variance ratio: budget / |PY actual|
        case
            when abs(py.py_actual_lcy) > 0
            then b.budget_amount_lcy / abs(py.py_actual_lcy)
            else null
        end                                                     as budget_to_py_ratio,
        {{ var('budget_variance_max_factor', 3.0) }}            as max_factor,
        {{ var('budget_min_py_actual_huf', 500000) }}           as min_py_threshold_huf
    from active_budget b
    join prior_year_actuals py
        on  py.entity_key  = b.entity_key
        and py.account_key = b.account_key
        and py.period_key  = b.prior_year_period_key
    where
        -- Only check lines with a meaningful PY actual
        abs(py.py_actual_lcy) >= {{ var('budget_min_py_actual_huf', 500000) }}
        -- Exclude zero-budget lines (Finance explicitly set them to 0 — intentional)
        and b.budget_amount_lcy <> 0
)

-- ── Return only flagged outliers ──────────────────────────────────────────────
select
    f.entity_key,
    f.account_key,
    f.period_key,
    f.budget_amount_lcy,
    f.py_actual_lcy,
    round(f.budget_to_py_ratio, 4)                              as budget_to_py_ratio,
    f.max_factor,
    case
        when f.budget_amount_lcy > abs(f.py_actual_lcy) * f.max_factor
            then 'BUDGET_TOO_HIGH'
        when f.budget_amount_lcy > 0
            and f.budget_amount_lcy < abs(f.py_actual_lcy) / f.max_factor
            then 'BUDGET_TOO_LOW'
        else 'UNKNOWN'
    end                                                         as violation_type,
    'DQ-011: Budget amount differs from PY actual by more than '
        || cast(f.max_factor as varchar)
        || 'x — likely unit error in upload'                    as failure_reason

from flagged_lines f

where
    -- Too high: budget > factor × PY actual
    f.budget_amount_lcy > abs(f.py_actual_lcy) * f.max_factor

    -- Too low: positive budget is less than 1/factor of PY actual
    or (
        f.budget_amount_lcy > 0
        and f.budget_amount_lcy < abs(f.py_actual_lcy) / f.max_factor
    )

order by
    abs(f.budget_to_py_ratio) desc nulls last,
    f.period_key,
    f.entity_key
