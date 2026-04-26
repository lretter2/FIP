##############################################################################
--  STAGING MODEL: stg_budget
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : bronze.raw_budget (monthly budget/forecast uploads)
--  Target  : staging.stg_budget (view)
--  Purpose : Standardise budget and forecast data into the canonical staging
--             format, ready for joining against actuals in the Gold Zone.
--
--  Budget versions supported:
--    ORIGINAL_BUDGET  — Board-approved annual budget (frozen Jan 1)
--    REVISED_BUDGET   — Mid-year reforecast (typically Q2, Q3)
--    ROLLING_FORECAST — 12-month forward-looking rolling forecast
##############################################################################

with

source as (

    select * from {{ source('bronze', 'raw_budget') }}

),

cleaned as (

    select
        -- Entity and period
        cast(entity_id           as varchar(50))   as entity_id,
        cast(period_year          as int)           as period_year,
        cast(period_month         as int)           as period_month,
        cast(
            concat(cast(period_year as varchar(4)),
                   right('0' + cast(period_month as varchar(2)), 2)
            ) as int
        )                                           as period_key,     -- YYYYMM

        -- Account
        cast(local_account_code   as varchar(50))   as local_account_code,

        -- Budget version — normalise to standard values
        case upper(cast(budget_version as varchar(30)))
            when 'BUDGET'           then 'ORIGINAL_BUDGET'
            when 'ORIGINAL_BUDGET'  then 'ORIGINAL_BUDGET'
            when 'REVISED'          then 'REVISED_BUDGET'
            when 'REVISED_BUDGET'   then 'REVISED_BUDGET'
            when 'FORECAST'         then 'ROLLING_FORECAST'
            when 'ROLLING_FORECAST' then 'ROLLING_FORECAST'
            else                         'ORIGINAL_BUDGET'
        end as budget_version,

        -- Budgeted amount in HUF
        cast(budget_amount_lcy    as decimal(18,2)) as budget_amount_lcy,

        -- Metadata
        cast(created_by           as varchar(100))  as created_by,
        cast(approved_by          as varchar(100))  as approved_by,
        cast(approval_date        as date)          as approval_date,
        cast(source_system        as varchar(30))   as source_system,
        cast(extraction_timestamp as datetime2)     as extraction_timestamp,
        cast(batch_id             as varchar(50))   as batch_id,

        'HU_GAAP' as gaap_basis

    from source
    where entity_id is not null
      and local_account_code is not null
      and budget_amount_lcy is not null

)

select * from cleaned
