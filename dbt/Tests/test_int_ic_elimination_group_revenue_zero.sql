##############################################################################
--  dbt TEST: test_int_ic_elimination_group_revenue_zero
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (IC-GroupRevenue): After intercompany elimination, the consolidated
--                           group P&L must show ZERO intercompany revenue for
--                           any entity that participates in IC transactions.
--
--  Background: int_ic_elimination produces offsetting entries that eliminate
--  revenue/cost recorded by both legs of an intercompany transaction.  If the
--  elimination is incomplete, the group P&L will double-count revenue and COGS
--  between related entities — a HU GAAP consolidation error.
--
--  This test is complementary to assert_ic_elimination_balances.sql, which
--  checks that the elimination entries themselves balance to zero.  This test
--  checks the END-STATE: the agg_pl_monthly GROUP scope row must show zero
--  after elimination entries are applied.
--
--  Returns rows where a group-scope entity still has non-zero revenue
--  originating from intercompany transactions.  Zero rows = PASS.
--
--  Trigger  : After int_ic_elimination and agg_pl_monthly are built.
--  Severity : ERROR — IC revenue leaking into group P&L is a consolidation
--             error that inflates group revenue and COGS symmetrically.
##############################################################################

{{ config(tags=['intermediate', 'ic_elimination', 'consolidation', 'hu_gaap']) }}

with ic_entities as (

    -- Entities that participate in any IC elimination (either leg)
    select distinct entity_from as entity_key from {{ ref('int_ic_elimination') }}
    union
    select distinct entity_to   as entity_key from {{ ref('int_ic_elimination') }}

),

group_pl as (

    -- Group-scope P&L rows for IC-participating entities
    select
        p.period_key,
        p.entity_key,
        p.revenue,
        p.consolidation_scope
    from {{ ref('agg_pl_monthly') }} p
    inner join ic_entities ic
        on ic.entity_key = p.entity_key
    where p.consolidation_scope = 'group'

),

violations as (

    select
        period_key,
        entity_key,
        revenue,
        consolidation_scope,
        'GROUP_IC_REVENUE_NOT_ELIMINATED' as failure_reason
    from group_pl
    -- Revenue should be exactly zero after IC elimination; allow 1 HUF rounding
    where abs(revenue) > 1.0

)

select * from violations
