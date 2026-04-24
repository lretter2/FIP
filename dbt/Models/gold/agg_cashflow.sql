##############################################################################
--  MART MODEL: agg_cashflow  (Gold Zone)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : agg_pl_monthly + agg_balance_sheet
--  Target  : gold.agg_cashflow (table, full refresh monthly)
--  Purpose : Indirect method cash flow statement per HU GAAP.
--             Derives Operating / Investing / Financing cash flows from
--             P&L and balance sheet movements.
--
--  HU GAAP indirect method:
--    OCF = Net Profit + D&A ± Working Capital changes
--    ICF = CapEx + Disposals + Financial investments
--    FCF = Debt drawdowns/repayments + Equity transactions + Dividends
##############################################################################

{{
  config(
    materialized = 'table',
    tags         = ['gold', 'finance', 'cashflow', 'monthly']
  )
}}

with

pl as (

    select
        period_key,
        entity_key,
        net_profit,
        depreciation_amortisation,
        revenue,
        cogs,
        opex_total
    from {{ ref('agg_pl_monthly') }}

),

bs_current as (

    select period_key, entity_key, universal_node, closing_balance_lcy, period_movement_lcy
    from {{ ref('agg_balance_sheet') }}

),

working_capital as (

    select
        period_key,
        entity_key,
        -- Trade receivables movement (increase in AR = use of cash)
        -sum(case when universal_node like 'ASSET.CURRENT.RECEIVABLES%'
                  then period_movement_lcy else 0 end)  as delta_receivables,
        -- Inventory movement (increase in inventory = use of cash)
        -sum(case when universal_node like 'ASSET.CURRENT.INVENTORY%'
                  then period_movement_lcy else 0 end)  as delta_inventory,
        -- Trade payables movement (increase in AP = source of cash)
        sum(case when universal_node like 'LIABILITY.CURRENT.PAYABLES%'
                 then period_movement_lcy else 0 end)   as delta_payables,
        -- Other current assets/liabilities
        -sum(case when universal_node like 'ASSET.CURRENT%'
                   and universal_node not like 'ASSET.CURRENT.CASH%'
                   and universal_node not like 'ASSET.CURRENT.RECEIVABLES%'
                   and universal_node not like 'ASSET.CURRENT.INVENTORY%'
                  then period_movement_lcy else 0 end)  as delta_other_current_assets,
        sum(case when universal_node like 'LIABILITY.CURRENT%'
                  and universal_node not like 'LIABILITY.CURRENT.PAYABLES%'
                 then period_movement_lcy else 0 end)   as delta_other_current_liabilities
    from bs_current
    group by period_key, entity_key

),

investing as (

    select
        period_key,
        entity_key,
        -- CapEx: movement in PPE and Intangibles (net of D&A)
        -sum(case when universal_node like 'ASSET.NONCURRENT.PPE%'
                   or universal_node like 'ASSET.NONCURRENT.INTANGIBLE%'
                  then period_movement_lcy else 0 end)  as capex,
        -- Financial investments
        -sum(case when universal_node like 'ASSET.NONCURRENT.FINANCIAL%'
                  then period_movement_lcy else 0 end)  as financial_investments
    from bs_current
    group by period_key, entity_key

),

financing as (

    select
        period_key,
        entity_key,
        sum(case when universal_node like 'LIABILITY.NONCURRENT.DEBT%'
                 then period_movement_lcy else 0 end)   as net_debt_change,
        sum(case when universal_node like 'EQUITY%'
                  and universal_node not like 'EQUITY.RETAINED_EARNINGS%'
                 then period_movement_lcy else 0 end)   as equity_transactions
    from bs_current
    group by period_key, entity_key

),

cash_balance as (

    select
        period_key,
        entity_key,
        sum(closing_balance_lcy)                        as closing_cash,
        sum(closing_balance_lcy - period_movement_lcy)  as opening_cash
    from bs_current
    where universal_node like 'ASSET.CURRENT.CASH%'
    group by period_key, entity_key

),

final as (

    select
        pl.period_key,
        pl.entity_key,

        -- Operating Cash Flow (indirect method)
        pl.net_profit                                                       as net_profit,
        pl.depreciation_amortisation                                       as add_back_da,
        wc.delta_receivables                                               as delta_receivables,
        wc.delta_inventory                                                 as delta_inventory,
        wc.delta_payables                                                  as delta_payables,
        wc.delta_other_current_assets                                      as delta_other_current_assets,
        wc.delta_other_current_liabilities                                 as delta_other_current_liabilities,

        pl.net_profit
            + pl.depreciation_amortisation
            + wc.delta_receivables
            + wc.delta_inventory
            + wc.delta_payables
            + wc.delta_other_current_assets
            + wc.delta_other_current_liabilities                          as operating_cash_flow,

        -- Investing Cash Flow
        inv.capex                                                          as capex,
        inv.financial_investments                                          as financial_investments,
        inv.capex + inv.financial_investments                             as investing_cash_flow,

        -- Financing Cash Flow
        fin.net_debt_change                                               as net_debt_change,
        fin.equity_transactions                                           as equity_transactions,
        fin.net_debt_change + fin.equity_transactions                     as financing_cash_flow,

        -- Net cash movement
        (pl.net_profit + pl.depreciation_amortisation
            + wc.delta_receivables + wc.delta_inventory + wc.delta_payables
            + wc.delta_other_current_assets + wc.delta_other_current_liabilities)
          + (inv.capex + inv.financial_investments)
          + (fin.net_debt_change + fin.equity_transactions)               as net_cash_movement,

        -- Cash position
        cb.opening_cash                                                   as opening_cash_balance,
        cb.closing_cash                                                   as closing_cash_balance,

        -- Free Cash Flow
        (pl.net_profit + pl.depreciation_amortisation
            + wc.delta_receivables + wc.delta_inventory + wc.delta_payables
            + wc.delta_other_current_assets + wc.delta_other_current_liabilities)
          + inv.capex                                                      as free_cash_flow,

        getdate()                                                         as dbt_loaded_at

    from pl
    left join working_capital wc on  pl.period_key = wc.period_key and pl.entity_key = wc.entity_key
    left join investing       inv on pl.period_key = inv.period_key and pl.entity_key = inv.entity_key
    left join financing       fin on pl.period_key = fin.period_key and pl.entity_key = fin.entity_key
    left join cash_balance    cb  on pl.period_key = cb.period_key  and pl.entity_key = cb.entity_key

)

select * from final
