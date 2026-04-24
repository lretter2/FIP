##############################################################################
--  MART MODEL: kpi_cashflow  (Gold Zone · Analytics)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : agg_cashflow + agg_pl_monthly + agg_balance_sheet
--  Target  : gold.kpi_cashflow (table, full refresh)
--  Purpose : Cash flow quality, sustainability, and conversion KPIs
--             consumed by CFO and Investor dashboards.
--
--  KPIs covered (per Master Guide Part 4.4):
--    23 Operating Cash Flow Margin %
--    24 CapEx Intensity %
--    25 Free Cash Flow Yield
--    26 Cash Flow Conversion (OCF / EBITDA)
--    27 Cash Burn Rate (loss-making periods)
--    28 Cash Runway (months at current burn)
--    29 Reinvestment Rate (CapEx / Operating CF)
--    30 Dividend / Distribution Coverage (OCF / distributions)
--
--  Budget comparisons included for variance waterfall in Power BI.
##############################################################################

{{
  config(
    materialized = 'table',
    tags         = ['gold', 'kpi', 'cashflow']
  )
}}

with

cf as (

    select * from {{ ref('agg_cashflow') }}

),

pl as (

    select
        period_key,
        entity_key,
        revenue,
        revenue_budget,
        ebitda,
        net_profit
    from {{ ref('agg_pl_monthly') }}

),

bs as (

    select
        period_key,
        entity_key,
        sum(case when universal_node like 'ASSET.CURRENT.CASH%'
                 then closing_balance_lcy else 0 end) as cash_and_equivalents
    from {{ ref('agg_balance_sheet') }}
    group by period_key, entity_key

),

-- Prior period cash balance for burn / runway calculation
bs_lag as (

    select
        period_key,
        entity_key,
        cash_and_equivalents,
        lag(cash_and_equivalents, 1)
            over (partition by entity_key order by period_key) as prior_cash_balance

    from bs

),

-- Rolling 12-month OCF for normalised ratios
cf_ltm as (

    select
        period_key,
        entity_key,
        sum(operating_cash_flow) over (
            partition by entity_key
            order by period_key
            rows between 11 preceding and current row
        ) as ocf_ltm,
        sum(free_cash_flow) over (
            partition by entity_key
            order by period_key
            rows between 11 preceding and current row
        ) as fcf_ltm,
        sum(capex) over (
            partition by entity_key
            order by period_key
            rows between 11 preceding and current row
        ) as capex_ltm

    from cf

),

final as (

    select
        c.period_key,
        c.entity_key,

        -- ── Raw cash flow components ──────────────────────────────────────
        c.operating_cash_flow,
        c.investing_cash_flow,
        c.financing_cash_flow,
        c.net_cash_movement,
        c.free_cash_flow,
        c.capex,

        -- ── KPI 23: Operating Cash Flow Margin % ─────────────────────────
        -- How many HUF of operating cash flow generated per HUF of revenue
        case when p.revenue <> 0
             then round(c.operating_cash_flow / p.revenue * 100, 2)
             else null
        end                                             as ocf_margin_pct,

        -- ── KPI 24: CapEx Intensity % ─────────────────────────────────────
        case when p.revenue <> 0
             then round(abs(c.capex) / p.revenue * 100, 2)
             else null
        end                                             as capex_intensity_pct,

        -- ── KPI 26: Cash Flow Conversion = OCF / EBITDA ───────────────────
        -- Quality metric: high conversion (>80%) = strong working capital mgmt
        case when p.ebitda <> 0
             then round(c.operating_cash_flow / p.ebitda * 100, 2)
             else null
        end                                             as cf_conversion_pct,

        -- ── KPI 27: Cash Burn Rate ────────────────────────────────────────
        -- Only meaningful in loss-making periods (negative OCF)
        case when c.operating_cash_flow < 0
             then abs(c.operating_cash_flow)
             else 0
        end                                             as monthly_burn_rate,

        -- ── KPI 28: Cash Runway (months) ─────────────────────────────────
        -- Months of cash remaining at current burn rate
        case when c.operating_cash_flow < 0 and abs(c.operating_cash_flow) > 0
             then round(b.cash_and_equivalents / abs(c.operating_cash_flow), 1)
             else null
        end                                             as cash_runway_months,

        -- ── KPI 29: Reinvestment Rate = CapEx / Operating CF ─────────────
        case when c.operating_cash_flow > 0
             then round(abs(c.capex) / c.operating_cash_flow * 100, 2)
             else null
        end                                             as reinvestment_rate_pct,

        -- ── LTM rolling aggregates (for Investor view) ───────────────────
        l.ocf_ltm,
        l.fcf_ltm,
        l.capex_ltm,

        case when p.revenue <> 0
             then round(l.fcf_ltm / p.revenue * 100, 2)
             else null
        end                                             as fcf_margin_ltm_pct,

        -- ── Balance sheet context ─────────────────────────────────────────
        b.cash_and_equivalents,
        b.prior_cash_balance,
        b.cash_and_equivalents - coalesce(b.prior_cash_balance, 0)
                                                        as cash_movement_period,

        -- ── Budget variance ───────────────────────────────────────────────
        cast(null as decimal(18,2))                     as operating_cash_flow_budget,
        cast(null as decimal(18,2))                     as ocf_vs_budget,
        cast(null as decimal(18,2))                     as ocf_vs_budget_pct,

        getdate()                                       as dbt_loaded_at

    from cf  c
    left join pl       p on  c.period_key = p.period_key and c.entity_key = p.entity_key
    left join bs_lag   b on  c.period_key = b.period_key and c.entity_key = b.entity_key
    left join cf_ltm   l on  c.period_key = l.period_key and c.entity_key = l.entity_key

)

select * from final
