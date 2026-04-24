##############################################################################
--  dbt TEST: assert_valid_posting_dates
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (DQ-005): transaction_date <= posting_date <= ingestion_date.
--                 Violations indicate system clock errors, manual backdating,
--                 or ERP export bugs.
--
--  Trigger : Runs after fct_gl_transaction is built.
--  Severity: ERROR — invalid dates corrupt period assignments and KPIs.
##############################################################################

select
    transaction_id,
    company_id,
    local_account_code,
    transaction_date,
    posting_date,
    ingestion_timestamp,
    batch_id,
    case
        when transaction_date > posting_date
            then 'TRANSACTION_DATE_AFTER_POSTING_DATE'
        when posting_date > cast(ingestion_timestamp as date)
            then 'POSTING_DATE_AFTER_INGESTION_DATE'
        when transaction_date is null
            then 'NULL_TRANSACTION_DATE'
        when posting_date is null
            then 'NULL_POSTING_DATE'
    end as failure_reason
from {{ ref('fct_gl_transaction') }}
where
    -- Rule 1: transaction cannot precede posting
    transaction_date > posting_date

    -- Rule 2: posting cannot be in the future (after data was ingested)
    or posting_date > cast(ingestion_timestamp as date)

    -- Rule 3: null dates are always invalid
    or transaction_date is null
    or posting_date     is null
