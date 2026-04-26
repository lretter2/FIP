##############################################################################
--  dbt TEST: assert_revenue_accounts_no_debit_balance
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (DQ-009): Under HU GAAP (2000/C Act §§ 72–77), revenue accounts
--                 (CoA account_type = 'REVENUE', normal_balance = 'C')
--                 must not carry a net debit balance at period-end.
--
--                 A debit balance on a revenue account signals:
--                   · Misclassified expense posted to a revenue GL code
--                   · Reversal without a corresponding re-posting
--                   · Sign flip error in ERP export (debit ↔ credit swap)
--                   · Missing accrual reversal from prior period
--
--  Scope   : Period-end snapshots only — aggregated per company, account,
--            fiscal year, and fiscal month.  Intra-period negative balances
--            on revenue accounts that reverse within the month are excluded.
--
--  Trigger : Runs after fct_gl_transaction and ref_coa_mapping are built.
--  Severity: WARN — alerts Finance team; does not block Gold Zone refresh.
--             Material debit balances should be escalated for GAAP review.
##############################################################################

with period_end_balances as (
    select
        f.entity_id,
        f.local_account_code,
        c.universal_node,
        c.pl_line_item,
        {{ var('fiscal_year_start_month', 1) }}   as fiscal_year_start_month,
        -- Derive fiscal period from posting_date
        year(f.posting_date)                       as fiscal_year,
        month(f.posting_date)                      as fiscal_month,
        -- Net balance: credits are positive (normal for revenue), debits are negative
        sum(f.credit_lcy - f.debit_lcy)            as net_balance_lcy,
        sum(f.credit_lcy)                          as total_credits_lcy,
        sum(f.debit_lcy)                           as total_debits_lcy,
        count(distinct f.transaction_id)            as transaction_count,
        max(f.batch_id)                             as latest_batch_id
    from {{ ref('fct_gl_transaction') }} f
    inner join {{ ref('ref_coa_mapping') }} c
        on  c.local_account_code = f.local_account_code
        and c.entity_id         = f.entity_id
    where
        c.account_type    = 'REVENUE'
        and c.normal_balance  = 'C'        -- credit-normal accounts only
        and f.posting_date is not null
    group by
        f.entity_id,
        f.local_account_code,
        c.universal_node,
        c.pl_line_item,
        year(f.posting_date),
        month(f.posting_date)
)

select
    entity_id,
    local_account_code,
    universal_node,
    pl_line_item,
    fiscal_year,
    fiscal_month,
    net_balance_lcy,
    total_credits_lcy,
    total_debits_lcy,
    transaction_count,
    latest_batch_id,
    'REVENUE_ACCOUNT_DEBIT_BALANCE' as failure_reason
from period_end_balances
where
    -- Debit balance on a credit-normal revenue account = anomaly
    net_balance_lcy < 0
