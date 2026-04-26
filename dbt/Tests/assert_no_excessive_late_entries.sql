##############################################################################
--  dbt TEST: assert_no_excessive_late_entries
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (DQ-010): A posting is "late" when it is booked more than
--                 {{ var('late_entry_threshold_days', 5) }} calendar days
--                 after the last day of the fiscal period it belongs to.
--
--                 Under HU GAAP the period cut-off is strict:
--                 revenue and expense must be recognised in the period
--                 earned/incurred (accrual principle, 2000/C Act § 15).
--                 Late entries in a closed period require a formal
--                 amendment submission to the statutory auditor.
--
--  How "late" is calculated
--  -------------------------
--  period_end_date  = last day of the month in posting_date's fiscal period
--  lateness_days    = ingestion_date − period_end_date
--  A posting is late when:
--      posting_date  <= period_end_date          (belongs to a past period)
--      AND ingestion_date > period_end_date + threshold  (arrived after grace)
--
--  The dbt var `late_entry_threshold_days` is set to 5 in dbt_project.yml
--  and can be overridden per run via ADF pipeline parameters.
--
--  Trigger : Runs after fct_gl_transaction is built.
--  Severity: WARN — Finance team review required; pipeline does not halt.
##############################################################################

with posting_periods as (
    select
        transaction_id,
        entity_id,
        local_account_code,
        source_system,
        posting_date,
        transaction_date,
        cast(ingestion_timestamp as date)                          as ingestion_date,

        -- Last calendar day of the fiscal period (month) the transaction belongs to
        eomonth(posting_date)                                      as period_end_date,

        -- Days between period close and when the record arrived in the warehouse
        datediff(
            day,
            eomonth(posting_date),
            cast(ingestion_timestamp as date)
        )                                                          as days_after_period_close,

        net_amount_lcy,
        document_number,
        batch_id

    from {{ ref('fct_gl_transaction') }}
    where
        posting_date       is not null
        and ingestion_timestamp is not null
)

select
    transaction_id,
    entity_id,
    local_account_code,
    source_system,
    posting_date,
    transaction_date,
    period_end_date,
    ingestion_date,
    days_after_period_close,
    net_amount_lcy,
    document_number,
    batch_id,
    'LATE_PERIOD_ENTRY'  as failure_reason
from posting_periods
where
    -- The transaction belongs to a period that had already closed when it arrived
    posting_date       <= period_end_date

    -- It arrived more than the configured grace-period days after period close
    and days_after_period_close > {{ var('late_entry_threshold_days', 5) }}

    -- Exclude future postings (those are caught by DQ-005)
    and ingestion_date >= period_end_date
