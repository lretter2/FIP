##############################################################################
--  STAGING MODEL: stg_balance_sheet
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : bronze.raw_balance_sheet (period-end BS snapshots from ERP)
--  Target  : staging.stg_balance_sheet (view)
--  Purpose : Standardise period-end balance sheet snapshot exports from
--             the source ERP into the canonical staging format.
--
--  HU GAAP context: Balance sheet follows 2000/C Act 'A' form layout.
--  Account classes 1–4 (Assets) and 4 (Liabilities + Equity).
--  The BS snapshot is the period-end closing balance per account,
--  extracted from the ERP directly (not reconstructed from transactions).
##############################################################################

with

source as (

    select * from {{ source('bronze', 'raw_balance_sheet') }}

),

cleaned as (

    select
        -- Entity and period
        cast(company_id           as varchar(50))   as company_id,
        cast(period_year          as int)           as period_year,
        cast(period_month         as int)           as period_month,
        cast(
            concat(cast(period_year as varchar(4)),
                   right('0' + cast(period_month as varchar(2)), 2)
            ) as int
        )                                           as period_key,     -- YYYYMM

        -- Account
        cast(local_account_code   as varchar(50))   as local_account_code,
        cast(local_account_name   as varchar(200))  as local_account_name,

        -- HU GAAP account class (first digit of account code)
        cast(left(local_account_code, 1) as int)    as hu_gaap_account_class,

        -- Closing balance in HUF (local currency)
        cast(closing_balance_lcy  as decimal(18,2)) as closing_balance_lcy,
        cast(opening_balance_lcy  as decimal(18,2)) as opening_balance_lcy,

        -- Normal balance direction per HU GAAP chart of accounts
        -- Assets (1,2,3): Debit positive / Liabilities+Equity (4): Credit positive
        case
            when cast(left(local_account_code, 1) as int) in (1, 2, 3)
                then 'D'
            when cast(left(local_account_code, 1) as int) = 4
                then 'C'
            else 'D'
        end as normal_balance,

        -- Source metadata
        cast(source_system        as varchar(30))   as source_system,
        cast(extraction_timestamp as datetime2)     as extraction_timestamp,
        cast(batch_id             as varchar(50))   as batch_id,

        'HU_GAAP' as gaap_basis

    from source
    where company_id is not null
      and period_year is not null
      and local_account_code is not null

)

select * from cleaned
