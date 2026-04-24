##############################################################################
--  dbt TEST: assert_no_duplicate_source_ids
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (DQ-008): source_system_id must be unique per company_id and
--                 source_system combination.
--                 Duplicates indicate double-loaded batches, ERP export
--                 retries that were not deduplicated, or broken Bronze
--                 ingestion idempotency guards.
--
--  Note: The existing SHA-256 hash key (transaction_id) guards against
--        full-row duplicates, but a source system can issue the same
--        source_system_id with different field values (amended postings
--        without a prior reversal), which this test catches independently.
--
--  Trigger : Runs after stg_gl_transactions is built (Silver layer).
--  Severity: ERROR — duplicate source IDs corrupt the deduplication key
--             and may cause double-counting in Gold Zone KPIs.
##############################################################################

with duplicate_source_ids as (
    select
        company_id,
        source_system,
        source_system_id,
        count(*)            as occurrence_count,
        min(batch_id)       as first_batch_id,
        max(batch_id)       as last_batch_id,
        min(posting_date)   as first_posting_date,
        max(posting_date)   as last_posting_date
    from {{ ref('stg_gl_transactions') }}
    where source_system_id is not null   -- null source IDs checked separately (DQ-001)
    group by
        company_id,
        source_system,
        source_system_id
    having count(*) > 1
)

select
    d.company_id,
    d.source_system,
    d.source_system_id,
    d.occurrence_count,
    d.first_batch_id,
    d.last_batch_id,
    d.first_posting_date,
    d.last_posting_date,
    'DUPLICATE_SOURCE_SYSTEM_ID' as failure_reason
from duplicate_source_ids d
