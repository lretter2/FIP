##############################################################################
--  dbt TEST: assert_fx_rates_available
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (DQ-007): Every non-HUF transaction must have an NBH FX rate
--                 available in config.ref_fx_rates for its posting date
--                 (or within 3 business days before — weekend/holiday gap).
--
--  Missing FX rates cause the currency_convert macro to fall back to
--  the hardcoded 1.0 rate, silently corrupting EUR-denominated metrics.
--
--  Trigger : Runs after int_fx_converted is built.
--  Severity: WARN — alerts Finance team; does not halt pipeline.
##############################################################################

select
    f.transaction_id,
    f.company_id,
    f.currency_local,
    f.posting_date,
    f.net_amount_lcy,
    f.batch_id,
    'MISSING_FX_RATE'  as failure_reason
from {{ ref('fct_gl_transaction') }} f
where
    -- Only check non-HUF transactions
    f.currency_local <> 'HUF'
    and f.currency_local is not null

    -- No NBH rate exists within 3 days before the posting date
    and not exists (
        select 1
        from {{ source('config', 'ref_fx_rates') }} r
        where r.currency_code = f.currency_local
          and r.rate_date     <= f.posting_date
          and r.rate_date     >= dateadd(day, -3, f.posting_date)
    )
