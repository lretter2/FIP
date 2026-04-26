##############################################################################
--  MART MODEL: agg_balance_sheet  (Gold Zone)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : stg_balance_sheet + int_coa_mapped (for taxonomy)
--  Target  : gold.agg_balance_sheet (table, full refresh monthly)
--  Purpose : Period-end balance sheet snapshots aggregated to the HU GAAP
--             taxonomy node level. Supports the 'A' form layout per 2000/C Act.
--
--  Account classes: 1-2 (Fixed Assets), 3 (Current Assets),
--                   4 (Liabilities, Provisions, Equity)
##############################################################################

{{
  config(
    materialized = 'table',
    tags         = ['gold', 'finance', 'balance_sheet', 'monthly']
  )
}}

with

bs as (

    select
        b.period_key,
        b.period_year,
        b.period_month,
        b.entity_id,
        b.local_account_code,
        b.closing_balance_lcy,
        b.opening_balance_lcy,
        b.normal_balance,
        -- Apply sign convention: Assets positive as debit, Liabilities/Equity positive as credit
        case b.normal_balance
            when 'D' then  b.closing_balance_lcy
            when 'C' then -b.closing_balance_lcy
        end as signed_closing_balance_lcy
    from {{ ref('stg_balance_sheet') }} b

),

coa as (

    select
        entity_id,
        local_account_code,
        universal_node,
        account_type,
        l1_category,
        l2_subcategory,
        l3_detail
    from {{ ref('ref_coa_mapping') }}

),

entity as (

    select entity_key, entity_id
    from {{ source('silver', 'dim_entity') }}

),

joined as (

    select
        bs.period_key,
        bs.period_year,
        bs.period_month,
        e.entity_key,
        c.universal_node,
        c.account_type,
        c.l1_category,
        c.l2_subcategory,
        c.l3_detail,
        sum(bs.signed_closing_balance_lcy)  as closing_balance_lcy,
        sum(bs.closing_balance_lcy - bs.opening_balance_lcy) as period_movement_lcy
    from bs
    join coa     c on  bs.local_account_code = c.local_account_code
                   and bs.entity_id         = c.entity_id
    join entity  e on  bs.entity_id         = e.entity_id
    group by
        bs.period_key, bs.period_year, bs.period_month,
        e.entity_key,
        c.universal_node, c.account_type, c.l1_category, c.l2_subcategory, c.l3_detail

),

-- HU GAAP balance check: Total Assets must equal Total Liabilities + Equity
balance_check as (

    select
        period_key,
        entity_key,
        sum(case when account_type = 'Asset'    then closing_balance_lcy else 0 end) as total_assets,
        sum(case when account_type in ('Liability', 'Equity')
                 then closing_balance_lcy else 0 end)                                as total_liabilities_equity
    from joined
    group by period_key, entity_key

)

select
    j.*,
    bc.total_assets,
    bc.total_liabilities_equity,
    bc.total_assets - bc.total_liabilities_equity                as balance_difference,
    case when abs(bc.total_assets - bc.total_liabilities_equity) < 1
         then cast(1 as bit) else cast(0 as bit)
    end                                                          as is_balanced,
    getdate()                                                    as dbt_loaded_at
from joined       j
join balance_check bc on  j.period_key = bc.period_key and j.entity_key = bc.entity_key
