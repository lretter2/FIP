##############################################################################
--  dbt TEST: test_int_coa_mapped_normal_balance
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (CoA-NormalBalance): The normal_balance column in int_coa_mapped must
--                             be consistent with the account_type per HU GAAP:
--
--    Account Type  | Normal Balance | Rationale
--    ------------- | -------------- | ------------------------------------------
--    Asset         | D              | Assets increase on debit
--    Expense       | D              | Expenses increase on debit
--    Cost          | D              | Cost of goods sold increases on debit
--    Revenue       | C              | Revenue increases on credit
--    Liability     | C              | Liabilities increase on credit
--    Equity        | C              | Equity increases on credit
--
--  A mismatch indicates a data-entry error in ref_coa_mapping.csv that will
--  cause all sign-convention logic downstream to produce incorrect KPIs.
--
--  Returns rows for any account with a mismatched normal balance.
--  Zero rows = PASS.
--
--  Trigger  : After int_coa_mapped view is created.
--  Severity : ERROR — incorrect normal balance corrupts CoA mapping.
##############################################################################

{{ config(tags=['intermediate', 'coa', 'hu_gaap']) }}

with mapped as (

    select distinct
        local_account_code,
        company_id,
        account_type,
        normal_balance
    from {{ ref('int_coa_mapped') }}
    where account_type is not null
      and normal_balance is not null

),

violations as (

    select
        local_account_code,
        company_id,
        account_type,
        normal_balance,
        case account_type
            when 'Asset'     then 'D'
            when 'Expense'   then 'D'
            when 'Cost'      then 'D'
            when 'Revenue'   then 'C'
            when 'Liability' then 'C'
            when 'Equity'    then 'C'
        end as expected_normal_balance,
        'NORMAL_BALANCE_MISMATCH' as failure_reason
    from mapped
    where normal_balance <>
          case account_type
              when 'Asset'     then 'D'
              when 'Expense'   then 'D'
              when 'Cost'      then 'D'
              when 'Revenue'   then 'C'
              when 'Liability' then 'C'
              when 'Equity'    then 'C'
          end

)

select * from violations
