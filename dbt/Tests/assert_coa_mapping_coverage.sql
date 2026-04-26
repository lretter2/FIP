##############################################################################
--  dbt TEST: assert_coa_mapping_coverage
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (CoA-Coverage): At least 95 % of distinct local_account_codes that
--                       appear in fct_gl_transaction must have a matching
--                       entry in ref_coa_mapping (universal_node IS NOT NULL).
--
--                       Below 95 % coverage means the Gold Zone P&L and
--                       Balance Sheet models are silently dropping or
--                       misclassifying a material portion of transactions.
--
--  Why 95 % and not 100 %
--  -----------------------
--  New ERP accounts may appear in a period before the Finance team has
--  formally added them to the master CoA mapping seed (ref_coa_mapping.csv).
--  A hard 100 % threshold would block every pipeline run when a single
--  new account code is introduced.  The 5 % tolerance gives the Finance
--  team one sprint cycle to classify new accounts before the pipeline
--  raises an ERROR.  Accounts outside the tolerance window are surfaced
--  in the failure rows so they can be triaged immediately.
--
--  This test returns rows only when OVERALL coverage drops below 95 %.
--  Individual unmapped accounts are also emitted so the Finance team
--  can action them directly from the dbt test report.
--
--  Trigger : Runs after fct_gl_transaction and ref_coa_mapping are built.
--  Severity: ERROR — below-threshold coverage blocks Gold Zone refresh.
##############################################################################

with account_universe as (
    -- All distinct account codes that have posted transactions
    select
        f.entity_id,
        f.local_account_code,
        count(distinct f.transaction_id)  as transaction_count,
        sum(abs(f.net_amount_lcy))        as total_abs_amount_lcy,
        max(f.posting_date)               as last_posting_date
    from {{ ref('fct_gl_transaction') }} f
    where f.local_account_code is not null
    group by
        f.entity_id,
        f.local_account_code
),

mapped_accounts as (
    -- Account codes that exist in the CoA seed with a valid universal_node
    select
        entity_id,
        local_account_code
    from {{ ref('ref_coa_mapping') }}
    where universal_node is not null
      and trim(universal_node) <> ''
),

coverage_stats as (
    -- Per-company coverage ratio
    select
        u.entity_id,
        count(distinct u.local_account_code)                         as total_accounts,
        count(distinct m.local_account_code)                         as mapped_accounts,
        count(distinct u.local_account_code)
            - count(distinct m.local_account_code)                   as unmapped_accounts,
        cast(count(distinct m.local_account_code) as float)
            / nullif(count(distinct u.local_account_code), 0)        as coverage_ratio
    from account_universe u
    left join mapped_accounts m
        on  m.local_account_code = u.local_account_code
        and m.entity_id         = u.entity_id
    group by u.entity_id
),

failing_companies as (
    -- Companies whose coverage drops below the 95 % threshold
    select entity_id
    from coverage_stats
    where coverage_ratio < 0.95
)

-- Return each unmapped account for companies below the threshold,
-- annotated with the company-level coverage ratio so Finance can
-- prioritise which missing mappings to add first.
select
    u.entity_id,
    u.local_account_code,
    u.transaction_count,
    u.total_abs_amount_lcy,
    u.last_posting_date,
    s.total_accounts,
    s.mapped_accounts,
    s.unmapped_accounts,
    round(s.coverage_ratio * 100, 2)     as coverage_pct,
    'COA_MAPPING_BELOW_95_PCT_THRESHOLD' as failure_reason
from account_universe u
inner join failing_companies fc
    on fc.entity_id = u.entity_id
inner join coverage_stats s
    on s.entity_id  = u.entity_id
left join mapped_accounts m
    on  m.local_account_code = u.local_account_code
    and m.entity_id         = u.entity_id
where m.local_account_code is null   -- only the unmapped accounts
order by
    u.entity_id,
    u.total_abs_amount_lcy desc        -- highest-value unmapped accounts first
