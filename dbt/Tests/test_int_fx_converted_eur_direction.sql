##############################################################################
--  dbt TEST: test_int_fx_converted_eur_direction
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (FX-EUR-Direction): The EUR conversion must use MULTIPLICATION by
--                           huf_to_eur_rate (which equals 1/rate_to_huf):
--
--      net_amount_eur = net_amount_lcy * huf_to_eur_rate
--
--  This is semantically equivalent to dividing by the HUF/EUR rate
--  (e.g. 400 HUF/EUR → huf_to_eur_rate = 0.0025 EUR/HUF).
--
--  A common implementation error is to DIVIDE net_amount_lcy by huf_to_eur_rate
--  instead of multiplying.  That would produce EUR amounts ~160,000× too large.
--
--  This test verifies the arithmetic direction by checking that for any
--  transaction where we have a real (non-fallback) EUR rate:
--
--      net_amount_eur ≈ net_amount_lcy * huf_to_eur_rate   (within 1 HUF equiv.)
--
--  Implicit corollary: net_amount_lcy / net_amount_eur should approximate the
--  HUF/EUR rate, which must be between 200 and 600 for any plausible NBH rate.
--  A rate outside this range almost certainly means divide/multiply is inverted.
--
--  Returns rows where the EUR conversion arithmetic is incorrect.
--  Zero rows = PASS.
--
--  Trigger  : After int_fx_converted view is created.
--  Severity : ERROR — inverted FX direction produces ~160,000× EUR errors.
##############################################################################

{{ config(tags=['intermediate', 'fx_conversion', 'hu_gaap']) }}

with fx_converted as (

    select * from {{ ref('int_fx_converted') }}
    -- Only validate rows with a real rate (not fallback) to avoid testing
    -- the default 1/400 constant against itself
    where used_fallback_eur_rate = 0
      and net_amount_eur is not null
      and huf_to_eur_rate > 0
      -- Exclude zero-amount transactions (division undefined)
      and abs(net_amount_eur) > 0.001

),

arithmetic_check as (

    select
        transaction_id,
        entity_id,
        currency_code,
        net_amount_lcy,
        net_amount_eur,
        huf_to_eur_rate,
        -- Recompute expected EUR amount from stored rate
        round(net_amount_lcy * huf_to_eur_rate, 2) as expected_net_amount_eur,
        -- Compute implied HUF/EUR rate — should be 200–600 for valid NBH rates
        net_amount_lcy / net_amount_eur             as implied_huf_per_eur

    from fx_converted

),

violations as (

    select
        transaction_id,
        entity_id,
        currency_code,
        net_amount_lcy,
        net_amount_eur,
        expected_net_amount_eur,
        implied_huf_per_eur,
        huf_to_eur_rate,
        'EUR_CONVERSION_DIRECTION_ERROR' as failure_reason
    from arithmetic_check
    where
        -- The stored net_amount_eur must match the formula within 1 HUF tolerance
        abs(net_amount_eur - expected_net_amount_eur) > 1.0
        -- Safety net: implied rate outside plausible NBH range also fails
        or implied_huf_per_eur not between 200 and 600

)

select * from violations
