##############################################################################
--  INTERMEDIATE MODEL: int_ic_elimination
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : int_fx_converted + config.ref_intercompany_pairs
--  Target  : Enriched transaction list with IC identification flags
--  Purpose : Identify and flag intercompany transactions for elimination
--            in the agg_pl_monthly aggregation layer (GROUP scope).
--
--  IC Elimination Strategy:
--  ========================
--  1. FLAG (here): Match GL transactions to registered IC entity pairs
--  2. ELIMINATE (agg_pl_monthly): GROUP scope filters is_intercompany = false
--  3. SLICER (Power BI): consolidation_scope controls ENTITY vs GROUP view
--
--  Key: We flag transactions, we don't create elimination entries.
--       Elimination happens in the aggregation layer, not here.
--
##############################################################################

{{
  config(
    materialized = 'view',
    tags         = ['intermediate', 'ic_elimination', 'consolidation'],
    description  = 'Identifies intercompany transactions via entity pair registry matching'
  )
}}

with

source_transactions as (

    -- All GL transactions from Silver layer
    select
        transaction_id,
        entity_id,
        account_code,
        posting_date,
        period_id,
        signed_amount,
        signed_amount_fcy,
        currency_code,
        is_reversal,
        source_system,
        source_id
    from {{ ref('int_fx_converted') }}

),

ic_registry as (

    -- Registered IC entity pairs with account mappings
    -- Defines which entity-account combinations are intercompany
    select
        seller_entity_id,
        buyer_entity_id,
        seller_account_code,
        buyer_account_code,
        transaction_type,
        elimination_type,
        is_active
    from {{ source('config', 'ref_intercompany_pairs') }}
    where is_active = true

),

ic_flagged as (

    -- Match source transactions against IC registry
    -- A transaction is IC if its entity + account appear in either
    -- the seller or buyer side of a registered IC pair
    select
        t.transaction_id,
        t.entity_id,
        t.account_code,
        t.posting_date,
        t.period_id,
        t.signed_amount,
        t.signed_amount_fcy,
        t.currency_code,
        t.is_reversal,
        t.source_system,
        t.source_id,

        -- IC Identification: true if matches seller or buyer side of IC pair
        case
            when (t.entity_id = ic.seller_entity_id and t.account_code = ic.seller_account_code)
              or (t.entity_id = ic.buyer_entity_id and t.account_code = ic.buyer_account_code)
            then true
            else false
        end as is_intercompany,

        -- IC Pair Classification (for audit trail and downstream filtering)
        coalesce(ic.transaction_type, 'NON_IC') as ic_transaction_type,
        coalesce(ic.elimination_type, null) as ic_elimination_type,

        -- Timestamp
        current_timestamp() as ic_evaluated_at

    from source_transactions t
    left join ic_registry ic
        on (
            (t.entity_id = ic.seller_entity_id and t.account_code = ic.seller_account_code)
            or (t.entity_id = ic.buyer_entity_id and t.account_code = ic.buyer_account_code)
        )

)

-- Return all transactions with IC flags; flows downstream to fct_gl_transaction and agg_pl_monthly
select * from ic_flagged
