##############################################################################
--  dbt TEST: assert_no_zero_amount_transactions
--  Financial Intelligence Platform · HU GAAP
--
--  Rule (DQ-004): No transaction may have both debit_lcy and credit_lcy as 0,
--                 unless it is an explicit reversal pair (is_reversal = TRUE).
--                 Zero-value postings indicate data loading errors or ERP bugs.
--
--  Trigger : Runs after fct_gl_transaction is built.
--  Severity: WARN — flagged for Finance review; pipeline does not halt.
##############################################################################

select
    transaction_id,
    company_id,
    local_account_code,
    posting_date,
    document_number,
    debit_lcy,
    credit_lcy,
    net_amount_lcy,
    batch_id,
    'ZERO_AMOUNT_TRANSACTION' as failure_reason
from {{ ref('fct_gl_transaction') }}
where net_amount_lcy = 0
  and debit_lcy      = 0
  and credit_lcy     = 0
  and is_reversal    = 0   -- legitimate reversal pairs (debit + credit) may net to 0
