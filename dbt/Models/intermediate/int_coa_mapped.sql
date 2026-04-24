##############################################################################
--  INTERMEDIATE MODEL: int_coa_mapped
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : stg_gl_transactions + ref_coa_mapping seed
--  Target  : intermediate.int_coa_mapped (view)
--  Purpose : Apply the HU GAAP universal account taxonomy mapping to every
--             staging GL transaction. This is the single most important
--             transformation — it translates local account codes (e.g. '311')
--             into universal nodes (e.g. 'ASSET.CURRENT.RECEIVABLES.TRADE')
--             that drive all KPI calculations and financial statements.
--
--  CRITICAL: Go-live readiness requires >95% mapping coverage.
--            Unmapped rows are routed to audit.dq_quarantine, not discarded.
##############################################################################

with

transactions as (

    select * from {{ ref('stg_gl_transactions') }}
    -- Only process rows that passed referential integrity check
    where is_unmapped_account = 0

),

quarantined as (

    -- Unmapped accounts go to quarantine — never silently dropped
    select
        transaction_id,
        company_id,
        local_account_code,
        posting_date,
        net_amount_lcy,
        batch_id,
        'UNMAPPED_ACCOUNT_CODE' as quarantine_reason,
        getdate()              as quarantine_timestamp
    from {{ ref('stg_gl_transactions') }}
    where is_unmapped_account = 1

),

coa_map as (

    select * from {{ ref('ref_coa_mapping') }}

),

mapped as (

    select
        -- Pass-through transaction identifiers
        t.transaction_id,
        t.batch_id,
        t.company_id,
        t.source_system,
        t.document_number,
        t.document_type,
        t.posting_date,
        t.document_date,
        t.period_year,
        t.period_month,

        -- Raw account code (preserved for audit lineage)
        t.local_account_code,

        -- Universal taxonomy from CoA mapping
        m.universal_node,
        m.account_type,
        m.normal_balance,
        m.l1_category,
        m.l2_subcategory,
        m.l3_detail,
        m.pl_line_item,
        m.cf_classification,
        m.is_controlling,
        m.is_intercompany                    as is_intercompany_account,

        -- Amounts
        t.debit_lcy,
        t.credit_lcy,
        t.net_amount_lcy,
        t.currency_code,
        t.debit_fcy,
        t.credit_fcy,

        -- Quality / lineage flags
        t.is_late_entry,
        t.is_intercompany                    as is_intercompany_transaction,
        t.row_hash,
        t.gaap_basis,

        -- P&L sign convention:
        -- Revenue accounts: credit = positive (revenue increases on credit)
        -- Expense accounts: debit = positive (expense increases on debit)
        case m.account_type
            when 'Revenue' then t.credit_lcy - t.debit_lcy   -- credit-normal
            else                t.debit_lcy  - t.credit_lcy  -- debit-normal
        end as signed_amount_lcy

    from transactions t
    inner join coa_map m
        on  t.company_id         = m.company_id
        and t.local_account_code = m.local_account_code

)

-- Emit only the mapped rows (quarantine handled via separate insert in ADF)
select * from mapped
