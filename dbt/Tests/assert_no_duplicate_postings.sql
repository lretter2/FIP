##############################################################################
--  dbt TEST: assert_no_duplicate_postings
--  Financial Intelligence Platform · HU GAAP
--
--  Rule    : No two GL transactions may share the same SHA-256 row_hash.
--            A duplicate hash indicates either a double-load from the ERP
--            or a system error producing identical transaction records.
--
--  Trigger : Runs after fct_gl_transaction is built.
--  Severity: ERROR — duplicate postings corrupt financial statements.
--
--  Note: The row_hash is computed in stg_gl_transactions as SHA-256 of
--        (company_id, account_code, posting_date, document_number, amounts).
##############################################################################

-- Returns rows for every duplicate hash found.
-- Test passes only when 0 rows are returned.

with hash_counts as (

    select
        row_hash,
        company_id,
        count(*) as occurrence_count
    from {{ ref('fct_gl_transaction') }}
    group by row_hash, company_id
    having count(*) > 1

)

select
    f.transaction_id,
    f.company_id,
    f.local_account_code,
    f.posting_date,
    f.document_number,
    f.net_amount_lcy,
    f.batch_id,
    f.row_hash,
    hc.occurrence_count,
    'DUPLICATE_POSTING_HASH' as failure_reason
from {{ ref('fct_gl_transaction') }} f
inner join hash_counts hc
    on  f.row_hash    = hc.row_hash
    and f.company_id  = hc.company_id
order by f.row_hash, f.posting_date
