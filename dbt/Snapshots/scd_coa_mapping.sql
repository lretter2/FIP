##############################################################################
--  SNAPSHOT: scd_coa_mapping  (SCD Type 2)
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : config.ref_coa_mapping (seed table + manual updates)
--  Target  : silver.scd_coa_mapping
--  Purpose : Tracks historical changes to the Chart of Accounts taxonomy
--            mapping over time using Slowly Changing Dimension Type 2.
--
--  Why this matters:
--    When the Finance Director remaps a local account code to a different
--    universal node (e.g. reclassifying an account from OPEX to COGS),
--    historical transactions should retain their ORIGINAL classification
--    for period comparability. This snapshot preserves the mapping as
--    it existed at the time each transaction was processed.
--
--  Usage in downstream models:
--    JOIN on local_account_code WHERE dbt_valid_to IS NULL
--    to get the current mapping, or
--    JOIN on local_account_code AND batch_date BETWEEN dbt_valid_from AND
--    dbt_valid_to to get the historically correct mapping.
--
--  dbt_project.yml snapshot config:
--    strategy: check
--    updated_at: valid_from
##############################################################################

{% snapshot scd_coa_mapping %}

{{
    config(
        target_schema  = 'silver',
        unique_key     = ['entity_id', 'local_account_code'],
        strategy       = 'check',
        check_cols     = [
            'universal_node',
            'account_type',
            'normal_balance',
            'l1_category',
            'l2_subcategory',
            'l3_detail',
            'pl_line_item',
            'cf_classification',
            'is_controlling',
            'is_intercompany'
        ],
        invalidate_hard_deletes = true
    )
}}

-- Source: the seed table in config schema, enriched with entity defaults
select
    -- Composite business key
    coalesce(entity_id, 'DEFAULT')             as entity_id,
    local_account_code,

    -- Taxonomy fields (tracked for changes)
    universal_node,
    local_account_name,
    account_type,
    normal_balance,
    l1_category,
    l2_subcategory,
    l3_detail,
    pl_line_item,
    cf_classification,
    is_controlling,
    is_intercompany,

    -- Metadata
    getdate()                                   as snapshot_loaded_at

from {{ ref('ref_coa_mapping') }}

{% endsnapshot %}
