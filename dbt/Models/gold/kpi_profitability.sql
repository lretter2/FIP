##############################################################################
--  MART MODEL: kpi_profitability  (Gold Zone · Analytics)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : agg_pl_monthly + agg_balance_sheet
--  Target  : gold.kpi_profitability (table, full refresh)
--  Purpose : All profitability and return KPIs consumed by Power BI dashboards.
--             One row per entity per period with all KPIs pre-computed.
--
--  KPIs covered (per Master Guide Part 4.1):
--    1  Gross Margin %
--    2  EBITDA
--    3  EBITDA Margin %
--    4  Net Profit Margin %
--    5  Return on Assets (quarterly annualised)
--    6  Return on Equity (quarterly annualised)
--    8  Return on Invested Capital
##############################################################################

{{
  config(
    materialized = 'table',
    tags         = ['gold', 'kpi', 'profitability']
  )
}}

with

pl as (

    select * from {{ ref('agg_pl_monthly') }}

),

bs as (

    select
        period_key,
        entity_key,
        sum(case when account_type = 'Asset'   then closing_balance_lcy else 0 end) as total_assets,
        sum(case when account_type = 'Equity'  then closing_balance_lcy else 0 end) as total_equity,
        sum(case when universal_node like 'LIABILITY.NONCURRENT.DEBT%'
                  or universal_node like 'LIABILITY.CURRENT.DEBT%'
                 then closing_balance_lcy else 0 end)                               as total_debt,
        sum(case when universal_node like 'ASSET.CURRENT.CASH%'
                 then closing_balance_lcy else 0 end)                               as cash_and_equivalents
    from {{ ref('agg_balance_sheet') }}
    group by period_key, entity_key

),

-- Rolling averages for ROA/ROE (use average of current and prior period)
bs_lag as (

    select
        bs.period_key,
        bs.entity_key,
        bs.total_assets,
        bs.total_equity,
        bs.total_debt,
        bs.cash_and_equivalents,
        lag(bs.total_assets,  1) over (partition by bs.entity_key order by bs.period_key) as prior_total_assets,
        lag(bs.total_equity,  1) over (partition by bs.entity_key order by bs.period_key) as prior_total_equity
    from bs

),

final as (

    select
        p.period_key,
        p.entity_key,

        -- Revenue headline
        p.revenue,
        p.revenue_eur,

        -- KPI 1: Gross Margin %
        p.gross_profit,
        p.gross_margin_pct,

        -- KPI 2 & 3: EBITDA and EBITDA Margin %
        p.ebitda,
        p.ebitda_margin_pct,

        -- KPI 4: Net Profit Margin %
        p.net_profit,
        case when p.revenue <> 0
             then round(p.net_profit / p.revenue * 100, 4)
             else null
        end                                                         as net_profit_margin_pct,

        -- KPI 5: Return on Assets (annualised: × 12 / months in period = 1 for monthly)
        case when (b.total_assets + b.prior_total_assets) / 2.0 <> 0
             then round(p.net_profit / ((b.total_assets + b.prior_total_assets) / 2.0) * 100, 4)
             else null
        end                                                         as roa_pct,

        -- KPI 6: Return on Equity
        case when (b.total_equity + b.prior_total_equity) / 2.0 <> 0
             then round(p.net_profit / ((b.total_equity + b.prior_total_equity) / 2.0) * 100, 4)
             else null
        end                                                         as roe_pct,

        -- KPI 8: ROIC = NOPAT / Invested Capital
        -- NOPAT = EBIT × (1 - effective tax rate, assume 9% HU corporate tax)
        -- Invested Capital = Total Equity + Net Debt
        b.total_assets,
        b.total_equity,
        b.total_debt,
        b.cash_and_equivalents,
        b.total_debt - b.cash_and_equivalents                       as net_debt,
        b.total_equity + b.total_debt - b.cash_and_equivalents      as invested_capital,

        case when (b.total_equity + b.total_debt - b.cash_and_equivalents) <> 0
             then round(
                (p.ebit * (1 - 0.09))
                / (b.total_equity + b.total_debt - b.cash_and_equivalents) * 100,
                4)
             else null
        end                                                         as roic_pct,

        -- Budget variances
        p.revenue_budget,
        p.revenue_variance,
        p.revenue_variance_pct,
        p.revenue_py,
        p.revenue_yoy_pct,

        getdate()                                                   as dbt_loaded_at

    from pl  p
    left join bs_lag b on  p.period_key = b.period_key and p.entity_key = b.entity_key

)

select * from final
