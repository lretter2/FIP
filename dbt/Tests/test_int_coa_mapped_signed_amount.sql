##############################################################################
--  dbt TEST: test_int_coa_mapped_signed_amount
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (CoA-Sign): The signed_amount_lcy field in int_coa_mapped must follow
--                   HU GAAP sign conventions by account type:
--
--    Revenue   → credit_lcy - debit_lcy  (positive = revenue increase on credit)
--    All else  → debit_lcy - credit_lcy  (positive = expense/asset on debit)
--
--  A violation means the P&L and balance sheet will show inverted sign values,
--  causing double-counting or sign-reversal errors in all downstream KPIs.
--
--  This test returns rows where signed_amount_lcy does not match the expected
--  formula for that account type.  Zero rows = PASS.
--
--  Trigger  : After int_coa_mapped view is created.
--  Severity : ERROR — incorrect sign convention breaks all KPI calculations.
##############################################################################

{{ config(tags=['intermediate', 'sign_convention', 'hu_gaap']) }}

with mapped as (

    select * from {{ ref('int_coa_mapped') }}

),

expected_signs as (

    select
        transaction_id,
        company_id,
        account_type,
        debit_lcy,
        credit_lcy,
        signed_amount_lcy,

        -- Recompute expected signed amount per HU GAAP convention
        case account_type
            when 'Revenue' then credit_lcy - debit_lcy
            else                debit_lcy  - credit_lcy
        end as expected_signed_amount_lcy

    from mapped

),

violations as (

    select
        transaction_id,
        company_id,
        account_type,
        signed_amount_lcy,
        expected_signed_amount_lcy,
        abs(signed_amount_lcy - expected_signed_amount_lcy) as discrepancy,
        'SIGN_CONVENTION_VIOLATION' as failure_reason
    from expected_signs
    -- Allow 1 HUF rounding tolerance
    where abs(signed_amount_lcy - expected_signed_amount_lcy) > 1.0

)

select * from violations
