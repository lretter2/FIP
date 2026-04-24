/*******************************************************************************
 * FILE: fip_stored_procedures.sql
 * Financial Intelligence Platform · HU GAAP
 *
 * PURPOSE:
 *   Stored procedures for all DQ validation and audit write operations.
 *   ADF calls these via StoredProcedure activity, which passes parameters as
 *   proper SQL parameters — NEVER via string concatenation.
 *
 * SECURITY:
 *   This file was created to remediate the SQL injection vulnerability
 *
 * EXECUTION ORDER:
 *   Run AFTER fip_schema_audit.sql and fip_schema_bronze.sql.
 *
 * PERMISSIONS:
 *   GRANT EXECUTE ON SCHEMA::dq_procedures TO [adf-service-principal];
 *******************************************************************************/

------------------------------------------------------------
-- HELPER: Validate UUID format (prevents SQL injection)
------------------------------------------------------------
CREATE OR ALTER FUNCTION audit.fn_is_valid_uuid (@val NVARCHAR(100))
RETURNS BIT
AS
BEGIN
    -- Matches standard UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    RETURN CASE
        WHEN @val LIKE '[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]'
               + '-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]'
               + '-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]'
               + '-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]'
               + '-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]'
             THEN 1
        ELSE 0
    END;
END;
GO


------------------------------------------------------------
-- HELPER: Validate entity/company code (alphanumeric + underscore, max 50 chars)
------------------------------------------------------------
CREATE OR ALTER FUNCTION audit.fn_is_valid_entity_code (@val NVARCHAR(50))
RETURNS BIT
AS
BEGIN
    RETURN CASE
        WHEN @val NOT LIKE '%[^A-Za-z0-9_-]%' AND LEN(@val) BETWEEN 1 AND 50
             THEN 1
        ELSE 0
    END;
END;
GO


------------------------------------------------------------
-- DQ-001: Debit/Credit Balance Check
-- Returns: batch_id, entity_code, total_debits, total_credits, imbalance, dq_status
------------------------------------------------------------
CREATE OR ALTER PROCEDURE audit.usp_dq001_check_balance
    @batch_id    NVARCHAR(100),
    @entity_code NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    -- Input validation
    IF audit.fn_is_valid_uuid(@batch_id) = 0
        THROW 50001, 'Invalid batch_id format. Must be a valid UUID.', 1;
    IF audit.fn_is_valid_entity_code(@entity_code) = 0
        THROW 50002, 'Invalid entity_code format. Must be alphanumeric.', 1;

    SELECT
        batch_id,
        entity_code,
        SUM(debit_amount)                             AS total_debits,
        SUM(credit_amount)                            AS total_credits,
        ABS(SUM(debit_amount) - SUM(credit_amount))  AS imbalance,
        CASE
            WHEN ABS(SUM(debit_amount) - SUM(credit_amount)) < 1.00 THEN 'PASSED'
            ELSE 'FAILED'
        END AS dq_status
    FROM bronze.raw_gl_transactions
    WHERE batch_id    = @batch_id
      AND entity_code = @entity_code
    GROUP BY batch_id, entity_code;
END;
GO


------------------------------------------------------------
-- DQ-001: Log result to audit.data_quality_log
------------------------------------------------------------
CREATE OR ALTER PROCEDURE audit.usp_dq001_log_result
    @batch_id    NVARCHAR(100),
    @entity_code NVARCHAR(50),
    @status      NVARCHAR(10),   -- 'PASSED' | 'FAILED'
    @imbalance   DECIMAL(18,2)   -- 0.00 if passed
AS
BEGIN
    SET NOCOUNT ON;

    IF audit.fn_is_valid_uuid(@batch_id) = 0
        THROW 50001, 'Invalid batch_id format.', 1;
    IF @status NOT IN ('PASSED', 'FAILED')
        THROW 50003, 'Invalid status value. Must be PASSED or FAILED.', 1;

    INSERT INTO audit.data_quality_log
        (batch_id, rule_id, rule_name, status, detail, checked_at)
    VALUES (
        @batch_id,
        'DQ-001',
        'Debit/Credit Balance',
        @status,
        CASE @status
            WHEN 'FAILED' THEN CONCAT('Imbalance: ', CAST(@imbalance AS NVARCHAR(30)), ' HUF')
            ELSE NULL
        END,
        GETUTCDATE()
    );

    -- Quarantine entire batch on failure
    IF @status = 'FAILED'
    BEGIN
        INSERT INTO audit.quarantine
            (batch_id, entity_code, quarantine_reason, quarantined_at)
        VALUES
            (@batch_id, @entity_code, 'DEBIT_CREDIT_IMBALANCE', GETUTCDATE());
    END;
END;
GO


------------------------------------------------------------
-- DQ-002: Referential Integrity Check
-- Returns: unmapped_count
------------------------------------------------------------
CREATE OR ALTER PROCEDURE audit.usp_dq002_check_referential_integrity
    @batch_id    NVARCHAR(100),
    @entity_code NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    IF audit.fn_is_valid_uuid(@batch_id) = 0
        THROW 50001, 'Invalid batch_id format.', 1;
    IF audit.fn_is_valid_entity_code(@entity_code) = 0
        THROW 50002, 'Invalid entity_code format.', 1;

    SELECT COUNT(*) AS unmapped_count
    FROM bronze.raw_gl_transactions t
    WHERE t.batch_id    = @batch_id
      AND t.entity_code = @entity_code
      AND NOT EXISTS (
          SELECT 1
          FROM config.ref_coa_mapping m
          WHERE m.local_account_code = t.local_account_code
            AND m.entity_code        = t.entity_code
      );
END;
GO


------------------------------------------------------------
-- DQ-002: Quarantine unmapped rows + log result
------------------------------------------------------------
CREATE OR ALTER PROCEDURE audit.usp_dq002_quarantine_and_log
    @batch_id        NVARCHAR(100),
    @entity_code     NVARCHAR(50),
    @unmapped_count  INT
AS
BEGIN
    SET NOCOUNT ON;

    IF audit.fn_is_valid_uuid(@batch_id) = 0
        THROW 50001, 'Invalid batch_id format.', 1;

    -- Insert unmapped rows into quarantine
    INSERT INTO audit.quarantine
        (batch_id, entity_code, transaction_id, local_account_code, quarantine_reason, quarantined_at)
    SELECT
        @batch_id,
        t.entity_code,
        t.transaction_id,
        t.local_account_code,
        'UNMAPPED_ACCOUNT_CODE',
        GETUTCDATE()
    FROM bronze.raw_gl_transactions t
    WHERE t.batch_id    = @batch_id
      AND t.entity_code = @entity_code
      AND NOT EXISTS (
          SELECT 1
          FROM config.ref_coa_mapping m
          WHERE m.local_account_code = t.local_account_code
            AND m.entity_code        = t.entity_code
      );

    -- Log DQ result
    INSERT INTO audit.data_quality_log
        (batch_id, rule_id, rule_name, status, detail, checked_at)
    VALUES (
        @batch_id,
        'DQ-002',
        'Referential Integrity',
        CASE WHEN @unmapped_count = 0 THEN 'PASSED' ELSE 'WARNING' END,
        CONCAT('Unmapped accounts: ', @unmapped_count),
        GETUTCDATE()
    );
END;
GO


------------------------------------------------------------
-- DQ-003: Period Balance Check
-- Returns rows where opening + movements <> closing (per account)
------------------------------------------------------------
CREATE OR ALTER PROCEDURE audit.usp_dq003_check_period_balance
    @batch_id    NVARCHAR(100),
    @entity_code NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    IF audit.fn_is_valid_uuid(@batch_id) = 0
        THROW 50001, 'Invalid batch_id format.', 1;

    SELECT
        sub.local_account_code,
        ABS((sub.opening_balance_lcy + sub.period_net_movement) - sub.closing_balance_lcy) AS period_balance_gap
    FROM (
        SELECT
            bs.local_account_code,
            bs.opening_balance_lcy,
            bs.closing_balance_lcy,
            COALESCE(SUM(gl.net_amount_lcy), 0) AS period_net_movement
        FROM bronze.raw_balance_sheet bs
        LEFT JOIN bronze.raw_gl_transactions gl
               ON gl.local_account_code = bs.local_account_code
              AND gl.entity_code        = bs.entity_code
              AND gl.period_year        = bs.period_year
              AND gl.period_month       = bs.period_month
              AND gl.batch_id           = @batch_id
        WHERE bs.batch_id    = @batch_id
          AND bs.entity_code = @entity_code
        GROUP BY bs.local_account_code, bs.opening_balance_lcy, bs.closing_balance_lcy
    ) sub
    WHERE ABS((sub.opening_balance_lcy + sub.period_net_movement) - sub.closing_balance_lcy) > 1.00;
END;
GO


------------------------------------------------------------
-- DQ-003: Log period balance result
------------------------------------------------------------
CREATE OR ALTER PROCEDURE audit.usp_dq003_log_result
    @batch_id   NVARCHAR(100),
    @gap_count  INT
AS
BEGIN
    SET NOCOUNT ON;

    IF audit.fn_is_valid_uuid(@batch_id) = 0
        THROW 50001, 'Invalid batch_id format.', 1;

    INSERT INTO audit.data_quality_log
        (batch_id, rule_id, rule_name, status, detail, checked_at)
    VALUES (
        @batch_id,
        'DQ-003',
        'Period Balance Check',
        CASE WHEN @gap_count = 0 THEN 'PASSED' ELSE 'ALERT' END,
        CONCAT('Accounts with balance gap: ', @gap_count),
        GETUTCDATE()
    );
END;
GO

PRINT 'FIP DQ Stored Procedures (DQ-001..003) created successfully.';
GO


-- =============================================================================
--  DQ-004 THROUGH DQ-011 — Extended Data Quality Procedures
--
--  These procedures use PostgreSQL / Synapse Analytics plpgsql syntax and
--  depend on audit.fn_is_valid_period_id (defined in fip_schema_audit.sql).
--  Run AFTER fip_schema_audit.sql has been deployed.
--
--  Each procedure:
--    1. Validates inputs (UUID, entity code, period format) — prevents SQL injection
--    2. Executes a parameterised business-logic query (no dynamic SQL)
--    3. Writes result to audit.data_quality_log
--    4. Returns a scalar RAISE NOTICE consumable by ADF Lookup activity
-- =============================================================================


------------------------------------------------------------
--  DQ-004 : Zero-Amount Transaction Check
--  Mirrors: financial_dbt/tests/assert_no_zero_amount_transactions.sql
--  Severity: WARN — does not block pipeline
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq004_zero_amount_check (
    p_batch_id   UUID,
    p_entity_code VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_zero_count    INT;
    v_total_count   INT;
    v_dq_status     VARCHAR(10);
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-004: Invalid entity_code format: %', p_entity_code;
    END IF;

    SELECT COUNT(*) INTO v_zero_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id       = p_batch_id
      AND  entity_code    = p_entity_code
      AND  net_amount_lcy = 0
      AND  is_reversal    = FALSE;

    SELECT COUNT(*) INTO v_total_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id = p_batch_id AND entity_code = p_entity_code;

    v_dq_status := CASE WHEN v_zero_count = 0 THEN 'PASS' ELSE 'WARN' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed, failure_detail
    )
    SELECT
        p_batch_id, em.entity_id,
        'DQ-004', 'Zero-Amount Transaction Check',
        'No transaction may have net_amount_lcy = 0 unless it is an explicit reversal pair.',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'WARN' END,
        v_total_count, v_zero_count,
        CASE v_dq_status
            WHEN 'WARN' THEN jsonb_build_object('zero_count', v_zero_count)
            ELSE NULL
        END
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;

    RAISE NOTICE 'DQ-004 result: % | zero_count=% | total=%', v_dq_status, v_zero_count, v_total_count;
END;
$$;


------------------------------------------------------------
--  DQ-005 : Valid Posting Date Check
--  Mirrors: financial_dbt/tests/assert_valid_posting_dates.sql
--  Severity: ERROR — blocks Gold Zone build
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq005_posting_date_check (
    p_batch_id    UUID,
    p_entity_code VARCHAR(50),
    p_period_id   INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_invalid_count INT;
    v_total_count   INT;
    v_dq_status     VARCHAR(10);
    v_period_start  DATE;
    v_period_end    DATE;
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-005: Invalid entity_code format: %', p_entity_code;
    END IF;
    IF NOT audit.fn_is_valid_period_id(p_period_id) THEN
        RAISE EXCEPTION 'DQ-005: Invalid period_id format: %. Expected YYYYMM.', p_period_id;
    END IF;

    v_period_start := make_date(p_period_id / 100, p_period_id % 100, 1);
    v_period_end   := (v_period_start + INTERVAL '1 month - 1 day')::DATE;

    SELECT COUNT(*) INTO v_invalid_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id = p_batch_id AND entity_code = p_entity_code
      AND (
            transaction_date > posting_date
         OR posting_date > CURRENT_DATE
         OR posting_date < v_period_start - INTERVAL '31 days'
         OR posting_date > v_period_end   + INTERVAL '31 days'
      );

    SELECT COUNT(*) INTO v_total_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id = p_batch_id AND entity_code = p_entity_code;

    v_dq_status := CASE WHEN v_invalid_count = 0 THEN 'PASS' ELSE 'FAIL' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed, failure_detail
    )
    SELECT
        p_batch_id, em.entity_id,
        'DQ-005', 'Valid Posting Date Check',
        'transaction_date <= posting_date AND posting_date within ±31 days of period boundary.',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'BLOCK' END,
        v_total_count, v_invalid_count,
        CASE v_dq_status
            WHEN 'FAIL' THEN jsonb_build_object(
                'invalid_count', v_invalid_count,
                'period_start',  v_period_start,
                'period_end',    v_period_end
            )
            ELSE NULL
        END
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;
END;
$$;


------------------------------------------------------------
--  DQ-006 : Valid Currency Code Check
--  Mirrors: financial_dbt/tests/assert_valid_currency_codes.sql
--  Severity: ERROR — invalid currencies corrupt FX conversion
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq006_currency_code_check (
    p_batch_id    UUID,
    p_entity_code VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_invalid_count INT;
    v_total_count   INT;
    v_dq_status     VARCHAR(10);
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-006: Invalid entity_code format: %', p_entity_code;
    END IF;

    SELECT COUNT(*) INTO v_invalid_count
    FROM   silver.fact_gl_transaction f
    WHERE  f.batch_id = p_batch_id AND f.entity_code = p_entity_code
      AND  (
               f.original_currency IS NULL
            OR NOT EXISTS (
                   SELECT 1 FROM config.ref_currencies c
                   WHERE c.currency_code = f.original_currency
               )
           );

    SELECT COUNT(*) INTO v_total_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id = p_batch_id AND entity_code = p_entity_code;

    v_dq_status := CASE WHEN v_invalid_count = 0 THEN 'PASS' ELSE 'FAIL' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed
    )
    SELECT
        p_batch_id, em.entity_id,
        'DQ-006', 'Valid Currency Code Check',
        'original_currency must be a non-null ISO 4217 code present in config.ref_currencies.',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'BLOCK' END,
        v_total_count, v_invalid_count
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;
END;
$$;


------------------------------------------------------------
--  DQ-007 : FX Rate Availability Check
--  Mirrors: financial_dbt/tests/assert_fx_rates_available.sql
--  Severity: WARN — missing rates fall back to stale rate
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq007_fx_rate_availability (
    p_batch_id    UUID,
    p_entity_code VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_missing_count INT;
    v_dq_status     VARCHAR(10);
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-007: Invalid entity_code format: %', p_entity_code;
    END IF;

    SELECT COUNT(*) INTO v_missing_count
    FROM   silver.fact_gl_transaction f
    WHERE  f.batch_id = p_batch_id AND f.entity_code = p_entity_code
      AND  f.original_currency <> 'HUF'
      AND  NOT EXISTS (
               SELECT 1 FROM config.ref_fx_rates r
               WHERE  r.from_currency = f.original_currency
                 AND  r.to_currency   = 'HUF'
                 AND  r.rate_date BETWEEN f.posting_date - INTERVAL '3 days'
                                      AND f.posting_date + INTERVAL '1 day'
           );

    v_dq_status := CASE WHEN v_missing_count = 0 THEN 'PASS' ELSE 'WARN' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed
    )
    SELECT
        p_batch_id, em.entity_id,
        'DQ-007', 'FX Rate Availability Check',
        'Every non-HUF transaction must have an NBH rate within 3 business days of posting_date.',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'WARN' END,
        (SELECT COUNT(*) FROM silver.fact_gl_transaction
         WHERE  batch_id = p_batch_id AND entity_code = p_entity_code
           AND  original_currency <> 'HUF'),
        v_missing_count
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;
END;
$$;


------------------------------------------------------------
--  DQ-008 : Duplicate Source ID Check
--  Mirrors: financial_dbt/tests/assert_no_duplicate_source_ids.sql
--  Severity: ERROR — duplicates corrupt deduplication key
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq008_duplicate_source_id (
    p_batch_id    UUID,
    p_entity_code VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_dup_count   INT;
    v_total_count INT;
    v_dq_status   VARCHAR(10);
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-008: Invalid entity_code format: %', p_entity_code;
    END IF;

    SELECT COUNT(*) INTO v_dup_count
    FROM (
        SELECT source_system_id
        FROM   silver.fact_gl_transaction
        WHERE  batch_id = p_batch_id AND entity_code = p_entity_code
        GROUP BY source_system_id, source_system
        HAVING COUNT(*) > 1
    ) dupes;

    SELECT COUNT(*) INTO v_total_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id = p_batch_id AND entity_code = p_entity_code;

    v_dq_status := CASE WHEN v_dup_count = 0 THEN 'PASS' ELSE 'FAIL' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed
    )
    SELECT
        p_batch_id, em.entity_id,
        'DQ-008', 'Duplicate Source ID Check',
        'source_system_id must be unique per entity and source_system in this batch.',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'BLOCK' END,
        v_total_count, v_dup_count
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;
END;
$$;


------------------------------------------------------------
--  DQ-009 : Revenue Account Sign Check
--  Mirrors: financial_dbt/tests/assert_revenue_accounts_no_debit_balance.sql
--  Severity: WARN — HU GAAP sign violation, requires Finance review
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq009_revenue_sign_check (
    p_batch_id    UUID,
    p_entity_code VARCHAR(50),
    p_period_id   INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_violation_count INT;
    v_dq_status       VARCHAR(10);
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-009: Invalid entity_code format: %', p_entity_code;
    END IF;
    IF NOT audit.fn_is_valid_period_id(p_period_id) THEN
        RAISE EXCEPTION 'DQ-009: Invalid period_id: %', p_period_id;
    END IF;

    SELECT COUNT(DISTINCT f.local_account_code) INTO v_violation_count
    FROM   silver.fact_gl_transaction f
    JOIN   config.ref_coa_mapping m
           ON  m.local_account_code = f.local_account_code
           AND m.entity_code        = f.entity_code
    WHERE  f.entity_code    = p_entity_code
      AND  f.period_key     = p_period_id
      AND  m.account_type   = 'REVENUE'
      AND  m.normal_balance = 'C'
    GROUP BY f.local_account_code
    HAVING SUM(f.net_amount_lcy) > 0;

    v_dq_status := CASE WHEN v_violation_count = 0 THEN 'PASS' ELSE 'WARN' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed
    )
    SELECT
        p_batch_id, em.entity_id,
        'DQ-009', 'Revenue Account Sign Check',
        'HU GAAP: revenue accounts (normal_balance=C) must not carry a net debit period-end balance.',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'WARN' END,
        (SELECT COUNT(DISTINCT f2.local_account_code)
         FROM silver.fact_gl_transaction f2
         JOIN config.ref_coa_mapping m2
              ON m2.local_account_code = f2.local_account_code AND m2.entity_code = f2.entity_code
         WHERE f2.entity_code = p_entity_code AND f2.period_key = p_period_id
           AND m2.account_type = 'REVENUE'),
        v_violation_count
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;
END;
$$;


------------------------------------------------------------
--  DQ-010 : Excessive Late Entry Check
--  Mirrors: financial_dbt/tests/assert_no_excessive_late_entries.sql
--  Severity: WARN — alerts Finance & CFO; does not block pipeline
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq010_late_entry_check (
    p_batch_id              UUID,
    p_entity_code           VARCHAR(50),
    p_period_id             INT,
    p_late_threshold_days   INT     DEFAULT 5
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_late_count    INT;
    v_total_count   INT;
    v_late_rate_pct NUMERIC(8,4);
    v_dq_status     VARCHAR(10);
    v_period_end    DATE;
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-010: Invalid entity_code format: %', p_entity_code;
    END IF;
    IF NOT audit.fn_is_valid_period_id(p_period_id) THEN
        RAISE EXCEPTION 'DQ-010: Invalid period_id: %', p_period_id;
    END IF;
    IF p_late_threshold_days NOT BETWEEN 0 AND 365 THEN
        RAISE EXCEPTION 'DQ-010: late_threshold_days must be 0–365, got: %', p_late_threshold_days;
    END IF;

    v_period_end := (make_date(p_period_id / 100, p_period_id % 100, 1)
                     + INTERVAL '1 month - 1 day')::DATE;

    SELECT COUNT(*) INTO v_late_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id    = p_batch_id
      AND  entity_code = p_entity_code
      AND  period_key  = p_period_id
      AND  posting_date <= v_period_end
      AND  load_timestamp::date > v_period_end + p_late_threshold_days * INTERVAL '1 day';

    SELECT COUNT(*) INTO v_total_count
    FROM   silver.fact_gl_transaction
    WHERE  batch_id = p_batch_id AND entity_code = p_entity_code AND period_key = p_period_id;

    v_late_rate_pct := CASE WHEN v_total_count > 0
                            THEN (v_late_count::NUMERIC / v_total_count) * 100
                            ELSE 0 END;

    v_dq_status := CASE WHEN v_late_count = 0 THEN 'PASS' ELSE 'WARN' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed, failure_detail
    )
    SELECT
        p_batch_id, em.entity_id,
        'DQ-010', 'Excessive Late Entry Check',
        'Postings arriving more than ' || p_late_threshold_days || ' calendar days after period-end '
        || 'violate HU GAAP accrual principle (2000/C Act §15).',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'WARN' END,
        v_total_count, v_late_count,
        jsonb_build_object(
            'late_rate_pct',   v_late_rate_pct,
            'threshold_days',  p_late_threshold_days,
            'period_end_date', v_period_end
        )
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;
END;
$$;


------------------------------------------------------------
--  DQ-011 : Budget Variance Bounds Check
--  Mirrors: financial_dbt/tests/assert_budget_variance_bounds.sql
--  Severity: WARN — catches unit errors in Excel budget uploads
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE audit.usp_dq011_budget_variance_bounds (
    p_entity_code          VARCHAR(50),
    p_budget_period_id     INT,
    p_max_variance_factor  NUMERIC(5,2)    DEFAULT 3.00
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_outlier_count   INT;
    v_total_count     INT;
    v_dq_status       VARCHAR(10);
    v_prior_period_id INT;
BEGIN
    IF p_entity_code !~ '^[A-Za-z0-9_\-]{1,50}$' THEN
        RAISE EXCEPTION 'DQ-011: Invalid entity_code format: %', p_entity_code;
    END IF;
    IF NOT audit.fn_is_valid_period_id(p_budget_period_id) THEN
        RAISE EXCEPTION 'DQ-011: Invalid budget_period_id: %', p_budget_period_id;
    END IF;
    IF p_max_variance_factor NOT BETWEEN 1.01 AND 100 THEN
        RAISE EXCEPTION 'DQ-011: max_variance_factor must be 1.01–100, got: %', p_max_variance_factor;
    END IF;

    v_prior_period_id := ((p_budget_period_id / 100) - 1) * 100 + (p_budget_period_id % 100);

    SELECT COUNT(*) INTO v_outlier_count
    FROM   budget.fact_budget b
    JOIN   config.ref_entity_master em ON em.entity_code = p_entity_code
    LEFT JOIN (
        SELECT account_key, entity_key, SUM(net_amount_lcy) AS py_actual_lcy
        FROM   silver.fact_gl_transaction
        WHERE  period_key = v_prior_period_id
        GROUP BY account_key, entity_key
    ) py ON py.account_key = b.account_key AND py.entity_key = b.entity_key
    WHERE b.entity_key = em.entity_key
      AND b.period_key = p_budget_period_id
      AND py.py_actual_lcy IS NOT NULL
      AND ABS(py.py_actual_lcy) > 0
      AND (
              b.budget_amount_lcy > ABS(py.py_actual_lcy) * p_max_variance_factor
           OR (b.budget_amount_lcy > 0
               AND b.budget_amount_lcy < ABS(py.py_actual_lcy) / p_max_variance_factor)
          );

    SELECT COUNT(*) INTO v_total_count
    FROM   budget.fact_budget b
    JOIN   config.ref_entity_master em ON em.entity_code = p_entity_code
    WHERE  b.entity_key = em.entity_key AND b.period_key = p_budget_period_id;

    v_dq_status := CASE WHEN v_outlier_count = 0 THEN 'PASS' ELSE 'WARN' END;

    INSERT INTO audit.data_quality_log (
        batch_id, entity_id, rule_id, rule_name, rule_description,
        check_result, action_taken, records_checked, records_failed, failure_detail
    )
    SELECT
        NULL, em.entity_id,
        'DQ-011', 'Budget Variance Bounds Check',
        'Budget lines must not differ from prior-year actuals by more than '
        || p_max_variance_factor || '×. Catches accidental unit errors in Excel uploads.',
        v_dq_status,
        CASE v_dq_status WHEN 'PASS' THEN 'NONE' ELSE 'WARN' END,
        v_total_count, v_outlier_count,
        jsonb_build_object(
            'max_variance_factor', p_max_variance_factor,
            'prior_period_id',     v_prior_period_id,
            'budget_period_id',    p_budget_period_id,
            'outlier_count',       v_outlier_count
        )
    FROM config.ref_entity_master em
    WHERE em.entity_code = p_entity_code;
END;
$$;

COMMENT ON PROCEDURE audit.usp_dq011_budget_variance_bounds IS
    'DQ-011: Validates that newly uploaded budget figures are within a plausible '
    'multiple of prior-year actuals. Default threshold 3× flags lines where the '
    'budget is either > 3× or < 1/3 of the prior-year actual — a reliable signal '
    'for Excel unit errors (e.g. entering 4.5 instead of 4,500,000).';

-- Permissions (uncomment and adapt):
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq004_zero_amount_check       TO [adf-sp];
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq005_posting_date_check       TO [adf-sp];
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq006_currency_code_check      TO [adf-sp];
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq007_fx_rate_availability     TO [adf-sp];
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq008_duplicate_source_id      TO [adf-sp];
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq009_revenue_sign_check       TO [adf-sp];
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq010_late_entry_check         TO [adf-sp];
-- GRANT EXECUTE ON PROCEDURE audit.usp_dq011_budget_variance_bounds   TO [adf-sp];

PRINT 'FIP DQ Stored Procedures (DQ-004..011) created successfully.';
GO
