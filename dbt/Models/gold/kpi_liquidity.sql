##############################################################################
--  MART MODEL: kpi_liquidity  (Gold Zone · Analytics)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : agg_balance_sheet + agg_pl_monthly + agg_cashflow
--  Target  : gold.kpi_liquidity (table, full refresh)
--  Purpose : All liquidity, solvency, working capital, and cash flow KPIs
--             consumed by the CFO and Controller dashboards.
--
--  KPIs covered (per Master Guide Part 4.2 & 4.3):
--    10 Current Ratio
--    11 Quick Ratio
--    12 Cash Conversion Cycle (DSO + DIO - DPO)
--    13 Days Sales Outstanding (DSO)
--    14 Days Payable Outstanding (DPO)
--    15 Days Inventory Outstanding (DIO)
--    16 Net Debt / EBITDA
--    17 Interest Coverage
--    18 Debt Service Coverage
--    19 Equity Ratio
--    20 Gearing Ratio
--    21 Operating Cash Flow
--    22 Free Cash Flow
--    24 CapEx Intensity
##############################################################################

{{
  config(
    materialized = 'table',
    tags         = ['gold', 'kpi', 'liquidity', 'cashflow']
  )
}}

with

bs as (

    select
        period_key,
        entity_key,
        -- Current assets
        sum(case when universal_node like 'ASSET.CURRENT%'
                 then closing_balance_lcy else 0 end)               as current_assets,
        sum(case when universal_node like 'ASSET.CURRENT.CASH%'
                 then closing_balance_lcy else 0 end)               as cash,
        sum(case when universal_node like 'ASSET.CURRENT.RECEIVABLES%'
                 then closing_balance_lcy else 0 end)               as trade_receivables,
        sum(case when universal_node like 'ASSET.CURRENT.INVENTORY%'
                 then closing_balance_lcy else 0 end)               as inventory,
        -- Non-current assets
        sum(case when universal_node like 'ASSET.NONCURRENT%'
                 then closing_balance_lcy else 0 end)               as noncurrent_assets,
        -- Current liabilities
        sum(case when universal_node like 'LIABILITY.CURRENT%'
                 then closing_balance_lcy else 0 end)               as current_liabilities,
        sum(case when universal_node like 'LIABILITY.CURRENT.PAYABLES%'
                 then closing_balance_lcy else 0 end)               as trade_payables,
        -- Non-current liabilities
        sum(case when universal_node like 'LIABILITY.NONCURRENT%'
                 then closing_balance_lcy else 0 end)               as noncurrent_liabilities,
        sum(case when universal_node like 'LIABILITY.NONCURRENT.DEBT%'
                  or universal_node like 'LIABILITY.CURRENT.DEBT%'
                 then closing_balance_lcy else 0 end)               as total_debt,
        -- Equity
        sum(case when account_type = 'Equity'
                 then closing_balance_lcy else 0 end)               as total_equity,
        sum(case when account_type = 'Asset'
                 then closing_balance_lcy else 0 end)               as total_assets
    from {{ ref('agg_balance_sheet') }}
    group by period_key, entity_key

),

pl as (

    select
        period_key,
        entity_key,
        revenue,
        cogs,
        ebit,
        ebitda,
        net_financial_items
    from {{ ref('agg_pl_monthly') }}

),

cf as (

    select
        period_key,
        entity_key,
        operating_cash_flow,
        free_cash_flow,
        capex
    from {{ ref('agg_cashflow') }}

),

-- Rolling 12-month EBITDA for leverage ratios
ebitda_ltm as (

    select
        period_key,
        entity_key,
        sum(ebitda) over (
            partition by entity_key
            order by period_key
            rows between 11 preceding and current row
        ) as ebitda_ltm
    from pl

),

final as (

    select
        b.period_key,
        b.entity_key,

        -- KPI 10: Current Ratio (target > 1.5)
        case when b.current_liabilities <> 0
             then round(b.current_assets / b.current_liabilities, 4)
             else null
        end                                             as current_ratio,

        -- KPI 11: Quick Ratio (current assets excl. inventory)
        case when b.current_liabilities <> 0
             then round((b.current_assets - b.inventory) / b.current_liabilities, 4)
             else null
        end                                             as quick_ratio,

        -- KPI 13: DSO — Days Sales Outstanding
        case when p.revenue <> 0
             then round(b.trade_receivables / (p.revenue / 30.0), 1)
             else null
        end                                             as dso_days,

        -- KPI 14: DPO — Days Payable Outstanding
        case when p.cogs <> 0
             then round(b.trade_payables / (p.cogs / 30.0), 1)
             else null
        end                                             as dpo_days,

        -- KPI 15: DIO — Days Inventory Outstanding
        case when p.cogs <> 0
             then round(b.inventory / (p.cogs / 30.0), 1)
             else null
        end                                             as dio_days,

        -- KPI 12: Cash Conversion Cycle = DSO + DIO - DPO
        case when p.revenue <> 0 and p.cogs <> 0
             then round(
                b.trade_receivables / (p.revenue / 30.0)
                + b.inventory       / (p.cogs    / 30.0)
                - b.trade_payables  / (p.cogs    / 30.0), 1)
             else null
        end                                             as cash_conversion_cycle_days,

        -- KPI 16: Net Debt / EBITDA LTM
        b.total_debt - b.cash                           as net_debt,
        e.ebitda_ltm,
        case when e.ebitda_ltm <> 0
             then round((b.total_debt - b.cash) / e.ebitda_ltm, 2)
             else null
        end                                             as net_debt_ebitda,

        -- KPI 17: Interest Coverage = EBIT / Net Interest Expense (target > 3×)
        case when p.net_financial_items < 0 and p.net_financial_items <> 0
             then round(p.ebit / abs(p.net_financial_items), 2)
             else null
        end                                             as interest_coverage,

        -- KPI 19: Equity Ratio
        case when b.total_assets <> 0
             then round(b.total_equity / b.total_assets * 100, 2)
             else null
        end                                             as equity_ratio_pct,

        -- KPI 20: Gearing Ratio = Net Debt / (Net Debt + Equity)
        case when (b.total_debt - b.cash + b.total_equity) <> 0
             then round(
                (b.total_debt - b.cash)
                / (b.total_debt - b.cash + b.total_equity) * 100, 2)
             else null
        end                                             as gearing_ratio_pct,

        -- Balance sheet context
        b.current_assets,
        b.current_liabilities,
        b.trade_receivables,
        b.inventory,
        b.trade_payables,
        b.total_debt,
        b.total_equity,
        b.total_assets,
        b.cash,

        -- KPI 21 & 22: Cash Flow KPIs
        c.operating_cash_flow,
        c.free_cash_flow,

        -- KPI 24: CapEx Intensity
        c.capex,
        case when p.revenue <> 0
             then round(abs(c.capex) / p.revenue * 100, 2)
             else null
        end                                             as capex_intensity_pct,

        getdate()                                       as dbt_loaded_at

    from bs  b
    left join pl       p on  b.period_key = p.period_key and b.entity_key = p.entity_key
    left join cf       c on  b.period_key = c.period_key and b.entity_key = c.entity_key
    left join ebitda_ltm e on b.period_key = e.period_key and b.entity_key = e.entity_key

)

select * from final
