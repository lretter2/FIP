##############################################################################
--  MART MODEL: kpi_project  (Gold Zone · Analytics)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : gold.fact_gl_transaction (project_key dimension)
--  Target  : gold.kpi_project (table, full refresh)
--  Purpose : Project-level P&L, cost tracking, and margin analysis.
--             Consumed by the Controller and Project Manager dashboards.
--
--  KPIs covered (per Master Guide Part 4.5):
--    Revenue and costs by project
--    Project Gross Margin %
--    Budget vs Actual by project
--    Cumulative YTD cost per project
--    Labour vs Non-labour cost split
--    Project cost overrun flag (>5% over budget)
--
--  Note: Only populated for entities that use project_key dimension.
--        Rows with null project_key are excluded (entity-level analytics
--        only; those are handled by kpi_profitability).
##############################################################################

{{
  config(
    materialized = 'table',
    tags         = ['gold', 'kpi', 'project', 'controlling']
  )
}}

with

gl as (

    select
        cast(
            concat(cast(period_year as varchar(4)),
                   right('0' + cast(period_month as varchar(2)), 2)
            ) as int
        )                           as period_key,
        period_year                 as fiscal_year,
        period_month                as fiscal_period,
        entity_key,
        project_key,
        l1_category,
        l2_subcategory,
        pl_line_item,
        cf_classification,
        net_amount_lcy              as amount_lcy,
        net_amount_eur              as amount_eur,
        net_amount_lcy              as amount_reporting_currency,  -- HUF = reporting currency
        batch_id
    from {{ ref('fct_gl_transaction') }}
    where project_key is not null        -- project dimension required
      and project_key <> -1              -- exclude unassigned projects

),

dim_project as (

    select
        project_key,
        project_code,
        project_name,
        project_type,
        project_status,
        project_manager,
        start_date,
        planned_end_date,
        budget_amount_lcy,
        entity_id
    from {{ source('silver', 'dim_project') }}

),

-- Aggregate actual amounts per project per period
project_actuals as (

    select
        g.period_key,
        g.fiscal_year,
        g.fiscal_period,
        g.entity_key,
        g.project_key,

        -- Revenue
        sum(case when g.l1_category = 'Revenue'
                 then g.amount_lcy else 0 end)                      as project_revenue_lcy,

        -- Cost of Sales
        sum(case when g.pl_line_item = 'COGS'
                 then g.amount_lcy else 0 end)                      as project_cogs_lcy,

        -- Labour costs (cost centre sub-split if present)
        sum(case when g.l2_subcategory like '%Labour%'
                  or g.l2_subcategory like '%Payroll%'
                  or g.l2_subcategory like '%Salary%'
                 then g.amount_lcy else 0 end)                      as project_labour_cost_lcy,

        -- Non-labour operating costs
        sum(case when g.l1_category in ('Expense', 'Cost')
                  and g.l2_subcategory not like '%Labour%'
                  and g.l2_subcategory not like '%Payroll%'
                  and g.l2_subcategory not like '%Salary%'
                 then g.amount_lcy else 0 end)                      as project_nonlabour_cost_lcy,

        -- Total costs
        sum(case when g.l1_category in ('Expense', 'Cost', 'COGS')
                 then g.amount_lcy else 0 end)                      as project_total_cost_lcy,

        -- EUR equivalents
        sum(case when g.l1_category = 'Revenue'
                 then g.amount_eur else 0 end)                      as project_revenue_eur,
        sum(case when g.l1_category in ('Expense', 'Cost', 'COGS')
                 then g.amount_eur else 0 end)                      as project_total_cost_eur

    from gl  g
    group by
        g.period_key,
        g.fiscal_year,
        g.fiscal_period,
        g.entity_key,
        g.project_key

),

-- YTD cumulative costs per project (reset at start of fiscal year)
ytd_actuals as (

    select
        period_key,
        fiscal_year,
        entity_key,
        project_key,
        sum(project_revenue_lcy) over (
            partition by entity_key, project_key, fiscal_year
            order by period_key
            rows between unbounded preceding and current row
        )                                                           as ytd_revenue_lcy,
        sum(project_total_cost_lcy) over (
            partition by entity_key, project_key, fiscal_year
            order by period_key
            rows between unbounded preceding and current row
        )                                                           as ytd_total_cost_lcy,
        sum(project_labour_cost_lcy) over (
            partition by entity_key, project_key, fiscal_year
            order by period_key
            rows between unbounded preceding and current row
        )                                                           as ytd_labour_cost_lcy

    from project_actuals

),

final as (

    select
        a.period_key,
        a.fiscal_year,
        a.fiscal_period,
        a.entity_key,
        a.project_key,

        -- Project master attributes
        p.project_code,
        p.project_name,
        p.project_type,
        p.project_status,
        p.project_manager,
        p.start_date,
        p.planned_end_date,
        p.budget_amount_lcy                                         as project_budget_lcy,

        -- Current period actuals
        a.project_revenue_lcy,
        a.project_cogs_lcy,
        a.project_labour_cost_lcy,
        a.project_nonlabour_cost_lcy,
        a.project_total_cost_lcy,
        a.project_revenue_eur,
        a.project_total_cost_eur,

        -- Project Gross Profit
        a.project_revenue_lcy - a.project_cogs_lcy                 as project_gross_profit_lcy,

        -- Project Gross Margin %
        case when a.project_revenue_lcy <> 0
             then round(
                (a.project_revenue_lcy - a.project_cogs_lcy)
                / a.project_revenue_lcy * 100, 2)
             else null
        end                                                         as project_gross_margin_pct,

        -- Labour share of total cost
        case when a.project_total_cost_lcy <> 0
             then round(a.project_labour_cost_lcy / a.project_total_cost_lcy * 100, 2)
             else null
        end                                                         as labour_cost_share_pct,

        -- YTD figures
        y.ytd_revenue_lcy,
        y.ytd_total_cost_lcy,
        y.ytd_labour_cost_lcy,
        y.ytd_revenue_lcy - y.ytd_total_cost_lcy                   as ytd_project_profit_lcy,

        -- Budget vs YTD actual
        p.budget_amount_lcy                                         as budget_total_cost_lcy,
        y.ytd_total_cost_lcy - coalesce(p.budget_amount_lcy, 0)    as ytd_cost_vs_budget_lcy,

        case when p.budget_amount_lcy is not null and p.budget_amount_lcy <> 0
             then round(
                (y.ytd_total_cost_lcy - p.budget_amount_lcy)
                / abs(p.budget_amount_lcy) * 100, 2)
             else null
        end                                                         as ytd_cost_overrun_pct,

        -- Cost overrun flag: >5% over budget triggers alert
        case when p.budget_amount_lcy is not null
              and p.budget_amount_lcy > 0
              and y.ytd_total_cost_lcy > p.budget_amount_lcy * 1.05
             then 1 else 0
        end                                                         as is_cost_overrun,

        getdate()                                                   as dbt_loaded_at

    from project_actuals  a
    inner join dim_project p on  a.project_key = p.project_key
    left join ytd_actuals  y on  a.period_key  = y.period_key
                              and a.entity_key  = y.entity_key
                              and a.project_key = y.project_key

)

select * from final
