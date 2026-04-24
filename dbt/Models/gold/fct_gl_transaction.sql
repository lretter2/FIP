##############################################################################
--  MART MODEL: fct_gl_transaction  (Gold Zone)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : int_fx_converted + silver dimension tables
--  Target  : gold.fct_gl_transaction (table, incremental)
--  Purpose : The canonical central fact table for all financial analytics.
--             Every row is one GL posting line, fully keyed to conformed
--             dimensions and enriched with FX-converted amounts.
--
--  Materialisation: INCREMENTAL — appends new batches only.
--  Unique key      : transaction_id (prevents duplicates on re-run).
--  Partition       : posting_date (month-level for Synapse performance).
##############################################################################

{{
  config(
    materialized = 'incremental',
    unique_key   = 'transaction_id',
    incremental_strategy = 'merge',
    partition_by = {
      "field": "posting_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by   = ['entity_code', 'l1_category'],
    tags         = ['gold', 'finance', 'incremental']
  )
}}

with

source as (

    select * from {{ ref('int_fx_converted') }}

    {% if is_incremental() %}
    -- On incremental runs: only process batches not yet in the Gold table
    where batch_id not in (
        select distinct batch_id from {{ this }}
    )
    {% endif %}

),

dim_date as (
    select date_key, full_date, fiscal_year, fiscal_period, is_period_end
    from {{ source('silver', 'dim_date') }}
),

dim_account as (
    select account_key, local_account_code, entity_code, universal_node
    from {{ source('silver', 'dim_account') }}
),

dim_entity as (
    select entity_key, entity_code
    from {{ source('silver', 'dim_entity') }}
),

dim_cost_centre as (
    select cost_centre_key, cost_centre_code, entity_code
    from {{ source('silver', 'dim_cost_centre') }}
),

dim_currency as (
    select currency_key, currency_code
    from {{ source('silver', 'dim_currency') }}
),

dim_project as (
    select project_key, project_code, entity_code
    from {{ source('silver', 'dim_project') }}
),

final as (

    select
        -- Surrogate / business key
        s.transaction_id,
        s.batch_id,

        -- Dimension foreign keys
        dd.date_key,
        da.account_key,
        de.entity_key,
        coalesce(dcc.cost_centre_key, -1) as cost_centre_key,   -- -1 = unassigned
        coalesce(dc.currency_key,     -1) as currency_key,
        coalesce(dp.project_key,      -1) as project_key,

        -- Degenerate dimensions (kept on fact for drill-through)
        s.document_number,
        s.document_type,
        s.posting_date,
        s.period_year,
        s.period_month,

        -- Amounts — HUF (local currency)
        s.debit_lcy,
        s.credit_lcy,
        s.net_amount_lcy,
        s.signed_amount_lcy,

        -- Amounts — EUR (NBH rate)
        s.net_amount_eur,
        s.huf_to_eur_rate,

        -- FCY amounts (for foreign currency transactions)
        s.debit_fcy,
        s.credit_fcy,
        s.currency_code                     as transaction_currency,

        -- Universal taxonomy (denormalised for query performance)
        s.universal_node,
        s.account_type,
        s.l1_category,
        s.l2_subcategory,
        s.l3_detail,
        s.pl_line_item,
        s.cf_classification,

        -- Quality and compliance flags
        s.is_late_entry,
        s.is_intercompany_transaction,
        s.used_fallback_eur_rate,
        s.gaap_basis,

        -- Lineage
        s.source_system,
        s.row_hash,
        getdate()                           as dbt_loaded_at

    from source s

    left join dim_date        dd  on dd.full_date        = s.posting_date
    left join dim_account     da  on da.local_account_code = s.local_account_code
                                 and da.entity_code        = s.entity_code
    left join dim_entity      de  on de.entity_code       = s.entity_code
    left join dim_cost_centre dcc on dcc.cost_centre_code = s.cost_centre_code
                                 and dcc.entity_code       = s.entity_code
    left join dim_currency    dc  on dc.currency_code     = s.currency_code
    left join dim_project     dp  on dp.project_code      = s.project_code
                                 and dp.entity_code        = s.entity_code

)

select * from final
