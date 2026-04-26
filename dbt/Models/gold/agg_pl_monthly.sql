##############################################################################
--  MART MODEL: agg_pl_monthly  (Gold Zone)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : fct_gl_transaction + stg_budget
--  Target  : gold.agg_pl_monthly (table, full refresh monthly)
--  Purpose : Pre-computed monthly P&L aggregate — the primary analytics table
--             consumed by Power BI CEO and CFO dashboards via DirectQuery.
--
--  HU GAAP P&L follows the expenditure (cost-type) method as per 2000/C Act.
--  Account classes 5–9. Gross production value approach.
--
--  Columns match gold.agg_pl_monthly DDL defined in fip_schema_gold.sql.
##############################################################################

{{
  config(
    materialized = 'table',
    tags         = ['gold', 'finance', 'pl', 'monthly']
  )
}}

with

gl as (

    select
        period_year,
        period_month,
        cast(
            concat(cast(period_year as varchar(4)),
                   right('0' + cast(period_month as varchar(2)), 2)
            ) as int
        )                           as period_key,
        entity_key,
        l1_category,
        pl_line_item,
        signed_amount_lcy,
        net_amount_eur,
        is_intercompany
    from {{ ref('fct_gl_transaction') }}

),

budget as (

    select
        b.period_key,
        da.entity_key,
        b.local_account_code,
        m.l1_category,
        m.pl_line_item,
        sum(b.budget_amount_lcy) as budget_amount_lcy
    from {{ ref('stg_budget') }} b
    join {{ ref('ref_coa_mapping') }} m
        on  b.local_account_code = m.local_account_code
        and b.entity_id         = m.entity_id
    join {{ source('silver', 'dim_entity') }} da
        on  b.entity_id = da.entity_id
    where b.budget_version = 'ORIGINAL_BUDGET'
    group by b.period_key, da.entity_key, b.local_account_code, m.l1_category, m.pl_line_item

),

actuals_agg as (

    select
        period_key,
        entity_key,
        'entity' as consolidation_scope,  -- Entity-level includes ALL transactions
        sum(case when l1_category = 'Revenue'    then signed_amount_lcy else 0 end) as revenue,
        sum(case when l1_category = 'COGS'       then signed_amount_lcy else 0 end) as cogs,
        sum(case when l1_category = 'OPEX'       then signed_amount_lcy else 0 end) as opex_total,
        sum(case when pl_line_item = 'DA'        then signed_amount_lcy else 0 end) as depreciation_amortisation,
        sum(case when pl_line_item = 'Fin_Income' then signed_amount_lcy else 0 end) as financial_income,
        sum(case when pl_line_item = 'Fin_Expense' then signed_amount_lcy else 0 end) as financial_expense,
        sum(case when pl_line_item = 'Tax'       then signed_amount_lcy else 0 end) as tax_expense,
        -- EUR totals
        sum(case when l1_category = 'Revenue'    then net_amount_eur else 0 end)    as revenue_eur
    from gl
    group by period_key, entity_key

    UNION ALL

    select
        period_key,
        entity_key,
        'group' as consolidation_scope, 
        sum(case when l1_category = 'Revenue' and is_intercompany = false    then signed_amount_lcy else 0 end) as revenue,
        sum(case when l1_category = 'COGS' and is_intercompany = false       then signed_amount_lcy else 0 end) as cogs,
        sum(case when l1_category = 'OPEX' and is_intercompany = false       then signed_amount_lcy else 0 end) as opex_total,
        sum(case when pl_line_item = 'DA' and is_intercompany = false        then signed_amount_lcy else 0 end) as depreciation_amortisation,
        sum(case when pl_line_item = 'Fin_Income' and is_intercompany = false then signed_amount_lcy else 0 end) as financial_income,
        sum(case when pl_line_item = 'Fin_Expense' and is_intercompany = false then signed_amount_lcy else 0 end) as financial_expense,
        sum(case when pl_line_item = 'Tax' and is_intercompany = false       then signed_amount_lcy else 0 end) as tax_expense,
        -- EUR totals
        sum(case when l1_category = 'Revenue' and is_intercompany = false    then net_amount_eur else 0 end)    as revenue_eur
    from gl
    group by period_key, entity_key

),

prior_year as (

    select
        period_key + 100 as current_period_key,    -- shift PY period_key forward 1 year
        entity_key,
        sum(case when l1_category = 'Revenue' then signed_amount_lcy else 0 end) as revenue_py
    from gl
    group by period_key, entity_key

),

budget_agg as (

    select
        period_key,
        entity_key,
        'entity' as consolidation_scope,
        sum(case when l1_category = 'Revenue' then budget_amount_lcy else 0 end) as revenue_budget,
        sum(case when l1_category = 'COGS'   then budget_amount_lcy else 0 end)  as cogs_budget
    from budget
    group by period_key, entity_key

),

final as (

    select
        a.period_key,
        a.entity_key,
        a.consolidation_scope,  -- 'entity' | 'group' — controls IC elimination in Power BI

        -- P&L core lines (HU GAAP expenditure method)
        a.revenue,
        a.cogs,
        a.revenue - a.cogs                                        as gross_profit,
        case when a.revenue <> 0
             then round((a.revenue - a.cogs) / a.revenue * 100, 4)
             else 0
        end                                                       as gross_margin_pct,
        a.opex_total,
        (a.revenue - a.cogs) - a.opex_total                      as ebit,
        (a.revenue - a.cogs) - a.opex_total
            + a.depreciation_amortisation                        as ebitda,
        case when a.revenue <> 0
             then round(
                ((a.revenue - a.cogs) - a.opex_total + a.depreciation_amortisation)
                / a.revenue * 100, 4)
             else 0
        end                                                       as ebitda_margin_pct,
        a.financial_income - a.financial_expense                 as net_financial_items,
        (a.revenue - a.cogs) - a.opex_total
            + a.financial_income - a.financial_expense
            - a.tax_expense                                      as net_profit,

        -- Budget comparisons
        b.revenue_budget,
        a.revenue - b.revenue_budget                             as revenue_variance,
        case when b.revenue_budget <> 0
             then round((a.revenue - b.revenue_budget) / b.revenue_budget * 100, 4)
             else null
        end                                                       as revenue_variance_pct,

        -- Prior year comparisons
        py.revenue_py,
        a.revenue - py.revenue_py                                as revenue_yoy,
        case when py.revenue_py <> 0
             then round((a.revenue - py.revenue_py) / py.revenue_py * 100, 4)
             else null
        end                                                       as revenue_yoy_pct,

        -- EUR reporting
        a.revenue_eur,

        -- Metadata
        getdate()                                                as dbt_loaded_at

    from actuals_agg   a
    left join budget_agg b   on  a.period_key = b.period_key
                            and  a.entity_key = b.entity_key
                            and  b.consolidation_scope = 'entity'  -- Budget always at entity level
    left join prior_year py  on  a.period_key = py.current_period_key and a.entity_key = py.entity_key

)

select * from final
