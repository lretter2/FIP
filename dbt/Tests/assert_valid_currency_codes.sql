##############################################################################
--  dbt TEST: assert_valid_currency_codes
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (DQ-006): currency_local must be a valid ISO 4217 currency code
--                 that exists in config.ref_currencies.
--                 Invalid or unknown currency codes make FX conversion
--                 impossible and corrupt all EUR-denominated KPIs.
--
--  Trigger : Runs after fct_gl_transaction is built.
--  Severity: ERROR — blocks Gold Zone refresh.
##############################################################################

select
    f.transaction_id,
    f.company_id,
    f.currency_local,
    f.posting_date,
    f.batch_id,
    'INVALID_CURRENCY_CODE' as failure_reason
from {{ ref('fct_gl_transaction') }} f
where
    -- Currency code is NULL
    f.currency_local is null

    -- Currency code is not in the master reference table
    or not exists (
        select 1
        from {{ ref('ref_currencies') }} c
        where c.currency_code = f.currency_local
    )
