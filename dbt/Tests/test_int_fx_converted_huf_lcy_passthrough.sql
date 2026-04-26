##############################################################################
--  dbt TEST: test_int_fx_converted_huf_lcy_passthrough
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (FX-HUF-Passthrough): For transactions where currency_code = 'HUF'
--                              (or currency_code IS NULL, also treated as HUF),
--                              the FX-adjusted local amount must equal the
--                              original local amount:
--
--                              net_amount_lcy_fx_adjusted = net_amount_lcy
--
--  Background: HUF transactions do not require FCY→HUF conversion because
--  they are already expressed in the reporting currency.  The int_fx_converted
--  model handles this with a CASE statement:
--
--      when currency_code = 'HUF' or currency_code is null
--          then net_amount_lcy          ← passthrough
--      else (debit_fcy - credit_fcy) * rate_to_huf
--
--  A violation means HUF transactions are being double-converted and their
--  amounts will be inflated/deflated by the exchange rate.
--
--  Returns rows where the passthrough invariant is violated.
--  Zero rows = PASS.
--
--  Trigger  : After int_fx_converted view is created.
--  Severity : ERROR — HUF amount corruption affects all LCY-based KPIs.
##############################################################################

{{ config(tags=['intermediate', 'fx_conversion', 'hu_gaap']) }}

with fx_converted as (

    select * from {{ ref('int_fx_converted') }}

),

violations as (

    select
        transaction_id,
        entity_id,
        currency_code,
        net_amount_lcy,
        net_amount_lcy_fx_adjusted,
        abs(net_amount_lcy - net_amount_lcy_fx_adjusted) as discrepancy,
        'HUF_LCY_PASSTHROUGH_VIOLATION' as failure_reason
    from fx_converted
    where (currency_code = 'HUF' or currency_code is null)
      -- Allow 1 HUF rounding tolerance
      and abs(net_amount_lcy - net_amount_lcy_fx_adjusted) > 1.0

)

select * from violations
