-- =============================================================================
--  FINANCIAL INTELLIGENCE PLATFORM
--  Schema: AUDIT — Pipeline Logs, DQ Results, Alerts & System Audit
--  Version 1.0 · 2026 · HU GAAP
--
--  EXECUTION ORDER: 2 of 6
--
--  WHO WRITES INTO THESE TABLES:
--    Every pipeline run writes into audit tables — never the finance team,
--    never manually. These tables are the operational heartbeat of the platform.
--
-- =============================================================================


-- =============================================================================
--  SCHEMA DEFINITION
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS audit;    -- pipeline logs, DQ results, alert tracking

COMMENT ON SCHEMA audit IS 'Infrastructure tables written to by every pipeline run: batch logs, DQ results, restatements, alerts, quarantine.';


-- =============================================================================
--  3.1 BATCH LOG
--  Every pipeline execution creates one row here before it starts.
--  All subsequent DQ results, quarantine records, and restatements
--  reference the batch_id from this table.
--  This is the master audit record for "what ran and when."
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.batch_log (
    batch_id                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    pipeline_name           VARCHAR(100)    NOT NULL,               -- ADF pipeline name e.g. 'monthly_financial_close'
    pipeline_run_id         VARCHAR(200),                           -- ADF run ID for cross-system tracing
    source_system           VARCHAR(50)     NOT NULL,               -- 'SAP' | 'Business_Central' | 'Kulcs_Soft' | 'MANUAL_CSV'
    source_file_path        TEXT,                                   -- full path in ADLS bronze zone
    source_file_hash        CHAR(64),                               -- SHA-256 of the source file for tamper detection
    source_file_row_count   INT,
    source_system_version   VARCHAR(50),                            -- ERP version string for audit
    period_id               INT,                                    -- YYYYMM of the data being processed
    fiscal_year             INT,
    pipeline_stage          VARCHAR(30)     NOT NULL,               -- 'EXTRACTION' | 'VALIDATION' | 'SILVER' | 'GOLD' | 'COMPLETE'
    status                  VARCHAR(20)     NOT NULL DEFAULT 'RUNNING', -- 'RUNNING' | 'PASSED' | 'FAILED' | 'QUARANTINED' | 'PARTIAL'
    started_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,
    duration_seconds        INT             GENERATED ALWAYS AS (
                                EXTRACT(EPOCH FROM (completed_at - started_at))::INT
                            ) STORED,
    rows_extracted          INT             DEFAULT 0,
    rows_passed_dq          INT             DEFAULT 0,
    rows_quarantined        INT             DEFAULT 0,
    rows_loaded_silver      INT             DEFAULT 0,
    error_message           TEXT,
    triggered_by            VARCHAR(100)    NOT NULL DEFAULT 'SCHEDULE', -- 'SCHEDULE' | 'MANUAL' | 'RERUN'
    triggered_by_user       VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_batch_log_entity_period ON audit.batch_log (entity_id, period_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_batch_log_status        ON audit.batch_log (status) WHERE status IN ('RUNNING', 'FAILED');

COMMENT ON TABLE  audit.batch_log IS 'Master audit record for every pipeline execution. Every DQ result, quarantine record, restatement, and alert references a batch_id from this table. Retention: permanent (satisfies 8-year Hungarian accounting law requirement).';


-- =============================================================================
--  3.2 DATA QUALITY LOG
--  Records the result of every DQ rule check for every batch.
--  Even passing checks are recorded ("green log") so auditors can
--  confirm that checks ran, not just that failures were caught.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.data_quality_log (
    dq_log_id               BIGSERIAL       PRIMARY KEY,
    batch_id                UUID            NOT NULL REFERENCES audit.batch_log(batch_id),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    rule_id                 VARCHAR(10)     NOT NULL,               -- 'DQ-001' through 'DQ-010' (from Appendix B)
    rule_name               VARCHAR(100)    NOT NULL,
    rule_description        TEXT,
    check_result            VARCHAR(10)     NOT NULL,               -- 'PASS' | 'FAIL' | 'WARN' | 'SKIP'
    action_taken            VARCHAR(20)     NOT NULL,               -- 'NONE' | 'BLOCK' | 'QUARANTINE' | 'ALERT' | 'WARN'
    records_checked         INT             DEFAULT 0,
    records_failed          INT             DEFAULT 0,
    failure_rate_pct        NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN records_checked > 0
                                     THEN (records_failed::NUMERIC / records_checked) * 100
                                     ELSE 0 END
                            ) STORED,
    failure_detail          JSONB,                                  -- sample of failing records for investigation
    checked_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_log_batch   ON audit.data_quality_log (batch_id);
CREATE INDEX IF NOT EXISTS idx_dq_log_failure ON audit.data_quality_log (check_result, rule_id) WHERE check_result IN ('FAIL', 'WARN');

COMMENT ON TABLE  audit.data_quality_log IS 'Result of every DQ rule execution. PASS results are also recorded (green log) so auditors can verify the check ran, not just that failures were caught. failure_detail JSONB stores a sample of up to 10 failing records to aid investigation without storing all bad data.';


-- =============================================================================
--  3.3 QUARANTINE TABLE
--  Records that failed DQ validation and were blocked from Silver layer.
--  Finance team reviews and manually resolves or corrects these.
--  Nothing in quarantine propagates to Gold Zone until released.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.quarantine (
    quarantine_id           BIGSERIAL       PRIMARY KEY,
    batch_id                UUID            NOT NULL REFERENCES audit.batch_log(batch_id),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    source_record           JSONB           NOT NULL,               -- full original record exactly as received
    dq_rule_failed          VARCHAR(10)     NOT NULL,               -- which DQ rule triggered quarantine
    failure_reason          TEXT            NOT NULL,               -- human-readable explanation
    source_field            VARCHAR(50),                            -- which field caused the failure
    source_value            TEXT,                                   -- the actual failing value
    quarantined_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- Resolution tracking
    resolution_status       VARCHAR(20)     NOT NULL DEFAULT 'OPEN', -- 'OPEN' | 'CORRECTED' | 'REJECTED' | 'ACCEPTED_AS_IS'
    resolved_by             VARCHAR(100),
    resolved_at             TIMESTAMPTZ,
    resolution_notes        TEXT,
    corrected_record        JSONB,                                  -- the corrected version if resolution_status = 'CORRECTED'
    released_to_batch_id    UUID            REFERENCES audit.batch_log(batch_id) -- if re-processed in a later batch
);

CREATE INDEX IF NOT EXISTS idx_quarantine_entity_open ON audit.quarantine (entity_id, resolution_status) WHERE resolution_status = 'OPEN';
CREATE INDEX IF NOT EXISTS idx_quarantine_batch       ON audit.quarantine (batch_id);

COMMENT ON TABLE  audit.quarantine IS 'Every record that fails DQ validation lands here instead of Silver layer. Finance team works through the OPEN queue. A record can be corrected (and re-released into a new batch), rejected (permanently excluded), or accepted as-is (loaded with a flag). No quarantined record affects KPIs until explicitly released.';


-- =============================================================================
--  3.4 RESTATEMENT LOG
--  When a late entry is material (see Late Entry Protocol in the guide),
--  the affected period figures are restated. Every restatement is logged
--  here with the before/after amounts for audit purposes.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.restatement_log (
    restatement_id          BIGSERIAL       PRIMARY KEY,
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    affected_period_id      INT             NOT NULL,               -- YYYYMM of the period being restated
    account_code            VARCHAR(50)     NOT NULL,
    universal_node          VARCHAR(100)    NOT NULL,
    original_amount_huf     NUMERIC(18,2)   NOT NULL,
    restated_amount_huf     NUMERIC(18,2)   NOT NULL,
    delta_amount_huf        NUMERIC(18,2)   GENERATED ALWAYS AS (restated_amount_huf - original_amount_huf) STORED,
    delta_pct               NUMERIC(10,4),                          -- delta as % of original; populated by the pipeline
    materiality_threshold   NUMERIC(10,4),                          -- the threshold that was breached
    triggering_batch_id     UUID            NOT NULL REFERENCES audit.batch_log(batch_id),
    late_entry_transaction_ids  UUID[],                             -- array of transaction_ids that caused the restatement
    restatement_reason      TEXT,
    cfo_notified            BOOLEAN         NOT NULL DEFAULT FALSE,
    cfo_notification_sent_at TIMESTAMPTZ,
    cfo_acknowledged_by     VARCHAR(100),
    cfo_acknowledged_at     TIMESTAMPTZ,
    restated_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_restatement_entity_period ON audit.restatement_log (entity_id, affected_period_id);

COMMENT ON TABLE  audit.restatement_log IS 'Audit trail for every period restatement caused by a late entry. Records original figure, restated figure, and the delta. Required by Hungarian accounting law audit obligations. CFO notification and acknowledgement are tracked here.';


-- =============================================================================
--  3.5 ALERT LOG
--  Every alert fired by the platform is recorded here with its status.
--  The weekly digest email and the Controller Ops dashboard both read
--  from this table to show open vs resolved alerts.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.alert_log (
    alert_id                BIGSERIAL       PRIMARY KEY,
    rule_id                 INT             NOT NULL REFERENCES config.ref_alert_rules(rule_id),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    period_id               INT             NOT NULL,               -- YYYYMM when alert was triggered
    batch_id                UUID            REFERENCES audit.batch_log(batch_id),
    severity                VARCHAR(10)     NOT NULL,
    alert_title             VARCHAR(200)    NOT NULL,
    kpi_name                VARCHAR(100)    NOT NULL,
    kpi_value               NUMERIC(18,4),                          -- the actual value that triggered the alert
    threshold_value         NUMERIC(18,4),                          -- the threshold from the rule
    recipients_notified     VARCHAR(500),                           -- actual list of people notified
    notification_channels   VARCHAR(100),
    notification_sent_at    TIMESTAMPTZ,
    -- Resolution
    status                  VARCHAR(20)     NOT NULL DEFAULT 'OPEN', -- 'OPEN' | 'ACKNOWLEDGED' | 'RESOLVED' | 'SUPPRESSED'
    acknowledged_by         VARCHAR(100),
    acknowledged_at         TIMESTAMPTZ,
    resolved_by             VARCHAR(100),
    resolved_at             TIMESTAMPTZ,
    resolution_notes        TEXT,
    triggered_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_log_open    ON audit.alert_log (entity_id, status) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS idx_alert_log_period  ON audit.alert_log (entity_id, period_id, severity);

COMMENT ON TABLE  audit.alert_log IS 'Every alert fired is recorded here with full lifecycle tracking from OPEN through ACKNOWLEDGED to RESOLVED. The weekly digest email reads the OPEN queue. The Controller Ops dashboard shows a RAG summary of open alerts by severity. All AI-generated commentary about anomalies references alert_ids from this table.';


-- =============================================================================
--  3.6 SYSTEM AUDIT LOG
--  Records every meaningful system action: who accessed what data,
--  what reports were generated, what AI commentary was published.
--  Write-once, append-only. Required for GDPR and SOC 2 compliance.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.system_audit_log (
    log_id                  BIGSERIAL       PRIMARY KEY,
    event_timestamp         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    event_type              VARCHAR(50)     NOT NULL,               -- 'DATA_ACCESS' | 'REPORT_EXPORT' | 'AI_COMMENTARY_PUBLISHED' | 'CONFIG_CHANGE' | 'USER_LOGIN' | 'ALERT_FIRED'
    user_id                 VARCHAR(100)    NOT NULL,               -- AAD object ID or service principal
    user_display_name       VARCHAR(200),
    entity_id               UUID            REFERENCES config.ref_entity_master(entity_id),
    resource_type           VARCHAR(50),                            -- 'DASHBOARD' | 'REPORT' | 'TABLE' | 'PIPELINE' | 'CONFIG'
    resource_name           VARCHAR(200),
    action                  VARCHAR(50)     NOT NULL,               -- 'READ' | 'EXPORT' | 'PUBLISH' | 'UPDATE' | 'DELETE'
    action_detail           JSONB,                                  -- additional context (e.g. applied filters, export format)
    ip_address              INET,
    session_id              VARCHAR(100),
    outcome                 VARCHAR(10)     NOT NULL DEFAULT 'SUCCESS', -- 'SUCCESS' | 'DENIED' | 'ERROR'
    outcome_detail          TEXT
);

-- Partitioned by month for retention management (8-year requirement)
CREATE INDEX IF NOT EXISTS idx_audit_user_date ON audit.system_audit_log (user_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event     ON audit.system_audit_log (event_type, event_timestamp DESC);

COMMENT ON TABLE  audit.system_audit_log IS 'Immutable system-wide audit trail. Written to by Power BI activity log ingestion, ADF pipeline, AI commentary publisher, and config change events. Satisfies Hungarian accounting law 8-year retention and GDPR accountability requirements. Never delete rows — use retention policies to archive to cold storage after 2 years.';


-- =============================================================================
--  VIEW: v_quarantine_open
--  Finance team working queue for unresolved quarantine records.
--  Ordered oldest-first to prevent records ageing indefinitely.
-- =============================================================================

CREATE OR REPLACE VIEW audit.v_quarantine_open AS
SELECT
    q.quarantine_id,
    e.entity_name,
    q.dq_rule_failed,
    q.failure_reason,
    q.source_field,
    q.source_value,
    q.quarantined_at,
    DATE_PART('day', NOW() - q.quarantined_at)::INT  AS age_days,
    b.source_system,
    b.period_id
FROM audit.quarantine q
JOIN audit.batch_log          b ON q.batch_id   = b.batch_id
JOIN config.ref_entity_master e ON q.entity_id  = e.entity_id
WHERE q.resolution_status = 'OPEN'
ORDER BY q.quarantined_at ASC;

COMMENT ON VIEW audit.v_quarantine_open IS 'Finance team working queue for unresolved quarantine records. Ordered oldest-first to prevent records ageing indefinitely. age_days > 30 should trigger an escalation alert.';


-- =============================================================================
--  VIEW: v_alert_summary
--  Aggregated open alert counts by entity and severity.
--  Powers the RAG (red/amber/green) status indicator on CEO and CFO dashboards.
-- =============================================================================

CREATE OR REPLACE VIEW audit.v_alert_summary AS
SELECT
    e.entity_name,
    a.severity,
    COUNT(*) AS open_count,
    MIN(a.triggered_at) AS oldest_open_alert
FROM audit.alert_log a
JOIN config.ref_entity_master e ON a.entity_id = e.entity_id
WHERE a.status = 'OPEN'
GROUP BY e.entity_name, a.severity
ORDER BY
    CASE a.severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    e.entity_name;

COMMENT ON VIEW audit.v_alert_summary IS 'Aggregated open alert counts by entity and severity. Powers the RAG (red/amber/green) status indicator on the CEO and CFO dashboards. CRITICAL or HIGH open alerts turn the indicator red.';


-- =============================================================================
--  PROCEDURE: proc_evaluate_alerts()
--  Runs after every Gold Zone dbt refresh.
--  Reads config.ref_alert_rules, evaluates each rule against the
--  current Gold Zone KPI values, and writes fired alerts to
--  audit.alert_log. Also respects the cooldown_hours to prevent
--  alert fatigue for persistent breaches.
--
--  Called by ADF pipeline step "RunAnomalyDetection" via Databricks
--  or directly via Azure SQL stored procedure call.
-- =============================================================================

CREATE OR REPLACE PROCEDURE audit.proc_evaluate_alerts(
    p_period_id     INT     DEFAULT NULL,   -- NULL = evaluate most recent period for each entity
    p_entity_id     UUID    DEFAULT NULL    -- NULL = evaluate all active entities
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rule              RECORD;
    v_kpi_value         NUMERIC(18,4);
    v_period_id         INT;
    v_entity_key        INT;
    v_last_alert_at     TIMESTAMPTZ;
    v_cooldown_cutoff   TIMESTAMPTZ;
    v_threshold_breached BOOLEAN;
    v_invalid_scope_count INT;
BEGIN
    -- Validate that all active entity-specific alert rules reference
    -- a valid entity_code. Mistyped entity_scope = 'ACME_HU2' would silently never fire.
    SELECT COUNT(*) INTO v_invalid_scope_count
    FROM config.ref_alert_rules r
    WHERE r.is_active = TRUE
      AND r.entity_scope <> 'ALL'
      AND NOT EXISTS (
          SELECT 1 FROM silver.dim_entity e
          WHERE e.entity_code = r.entity_scope AND e.is_active = TRUE
      );
    IF v_invalid_scope_count > 0 THEN
        RAISE WARNING 'proc_evaluate_alerts: % alert rule(s) have entity_scope values that do not match any active entity_code in dim_entity. These rules will never fire. Check config.ref_alert_rules.entity_scope.', v_invalid_scope_count;
    END IF;
    -- The platform uses three different entity identifiers:
    --   config.ref_entity_master.entity_id   UUID  — canonical PK for FK relationships
    --   silver.dim_entity.entity_key         INT   — integer surrogate for fact table FK joins
    --   config.ref_alert_rules.entity_scope  VARCHAR(20) — human-readable entity_code for rule config
    -- This procedure resolves all three via the CROSS JOIN below:
    --   r.entity_scope (VARCHAR code) is matched against e.entity_code from dim_entity,
    --   which carries both entity_id (UUID) and entity_key (INT) for downstream use.
    -- For each active alert rule
    FOR v_rule IN
        SELECT r.*, e.entity_id, e.entity_key, e.entity_code
        FROM   config.ref_alert_rules r
        CROSS JOIN (
            SELECT entity_id, entity_key, entity_code
            FROM   silver.dim_entity
            WHERE  is_active = TRUE
              AND  (p_entity_id IS NULL OR entity_id = p_entity_id)
        ) e
        WHERE r.is_active = TRUE
          AND (r.entity_scope = 'ALL' OR r.entity_scope = e.entity_code)  -- VARCHAR code match
    LOOP
        -- Determine period to evaluate
        IF p_period_id IS NOT NULL THEN
            v_period_id := p_period_id;
        ELSE
            SELECT MAX(period_key) INTO v_period_id
            FROM   gold.agg_pl_monthly
            WHERE  entity_key = v_rule.entity_key;
        END IF;

        -- Fetch KPI value by name (dynamic dispatch over known KPI columns)
        EXECUTE format(
            'SELECT %I FROM gold.kpi_profitability WHERE period_key = $1 AND entity_key = $2
             UNION ALL
             SELECT %I FROM gold.kpi_liquidity      WHERE period_key = $1 AND entity_key = $2
             LIMIT 1',
            v_rule.kpi_name, v_rule.kpi_name
        )
        INTO v_kpi_value
        USING v_period_id, v_rule.entity_key;

        CONTINUE WHEN v_kpi_value IS NULL;

        -- Evaluate threshold
        v_threshold_breached := CASE v_rule.operator
            WHEN '<'  THEN v_kpi_value <  v_rule.threshold_value
            WHEN '>'  THEN v_kpi_value >  v_rule.threshold_value
            WHEN '<=' THEN v_kpi_value <= v_rule.threshold_value
            WHEN '>=' THEN v_kpi_value >= v_rule.threshold_value
            WHEN '='  THEN v_kpi_value =  v_rule.threshold_value
            ELSE FALSE
        END;

        IF v_threshold_breached THEN
            -- Check cooldown — don't re-fire if already alerted recently
            v_cooldown_cutoff := NOW() - (v_rule.cooldown_hours || ' hours')::INTERVAL;

            SELECT MAX(triggered_at) INTO v_last_alert_at
            FROM   audit.alert_log
            WHERE  rule_id    = v_rule.rule_id
              AND  entity_id  = v_rule.entity_id
              AND  status     IN ('OPEN', 'ACKNOWLEDGED')
              AND  triggered_at > v_cooldown_cutoff;

            IF v_last_alert_at IS NULL THEN
                -- Fire new alert
                INSERT INTO audit.alert_log (
                    rule_id, entity_id, period_id,
                    severity, alert_title, kpi_name,
                    kpi_value, threshold_value,
                    recipients_notified, notification_channels
                ) VALUES (
                    v_rule.rule_id, v_rule.entity_id, v_period_id,
                    v_rule.severity, v_rule.alert_title, v_rule.kpi_name,
                    v_kpi_value, v_rule.threshold_value,
                    v_rule.recipient_roles, v_rule.notification_channels
                );
            END IF;
        END IF;
    END LOOP;

    -- Log procedure execution
    INSERT INTO audit.system_audit_log (event_type, user_id, action, action_detail)
    VALUES ('ALERT_FIRED', 'system/proc_evaluate_alerts', 'EXECUTE',
            jsonb_build_object('period_id', p_period_id, 'entity_id', p_entity_id));
END;
$$;

COMMENT ON PROCEDURE audit.proc_evaluate_alerts IS 'Evaluates all active alert rules against current Gold Zone KPI values. Called automatically by ADF after every Gold Zone dbt refresh. Respects cooldown_hours to prevent alert fatigue. Fires new alerts into audit.alert_log which triggers Power Automate webhook for email/Teams notification.';


-- =============================================================================
--  3.7 COMMENTARY QUEUE
--  AI-generated financial narrative approval workflow.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.commentary_queue (
    -- Identity
    queue_id                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id               UUID            NOT NULL
                                REFERENCES config.ref_entity_master(entity_id),
    period_id               INT             NOT NULL,       -- YYYYMM
    commentary_role         VARCHAR(20)     NOT NULL,       -- 'CFO' | 'CEO' | 'BOARD' | 'INVESTOR'
    language_code           CHAR(2)         NOT NULL DEFAULT 'HU',  -- 'HU' | 'EN'

    -- Content
    narrative_text          TEXT            NOT NULL,       -- AI-generated narrative
    narrative_text_en       TEXT,                           -- English translation (second LLM call)
    variance_fact_pack      JSONB           NOT NULL,       -- structured variance inputs passed to OpenAI
    prompt_version          VARCHAR(20)     NOT NULL,       -- prompt template version for reproducibility
    word_count              INT             GENERATED ALWAYS AS (
                                array_length(string_to_array(trim(narrative_text), ' '), 1)
                            ) STORED,

    -- AI metadata
    generated_by_model      VARCHAR(50)     NOT NULL,       -- e.g. 'gpt-4o-2024-08-06'
    generated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    prompt_tokens           INT,
    completion_tokens       INT,
    total_tokens            INT             GENERATED ALWAYS AS (
                                COALESCE(prompt_tokens, 0) + COALESCE(completion_tokens, 0)
                            ) STORED,
    confidence_score        NUMERIC(5,4),                   -- model self-reported confidence [0.0 – 1.0]
    materiality_flags       JSONB,                          -- which variances were flagged as material

    -- Approval workflow
    approval_status         VARCHAR(20)     NOT NULL DEFAULT 'PENDING_REVIEW',
                                            -- 'PENDING_REVIEW' | 'APPROVED' | 'REJECTED' | 'SUPERSEDED'
    submitted_for_review_at TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    reviewed_by             VARCHAR(100),   -- AAD user principal name of reviewer
    reviewed_at             TIMESTAMPTZ,
    review_notes            TEXT,           -- optional CFO annotation

    -- Publication (populated when APPROVED)
    published_at            TIMESTAMPTZ,
    published_to_gold_id    UUID,           -- FK to gold.ai_commentary.narrative_id once published

    -- Audit lineage
    batch_id                UUID            REFERENCES audit.batch_log(batch_id),
    is_retry                BOOLEAN         NOT NULL DEFAULT FALSE,
    superseded_by           UUID            REFERENCES audit.commentary_queue(queue_id),

    -- Constraints
    CONSTRAINT chk_commentary_role     CHECK (commentary_role IN ('CFO', 'CEO', 'BOARD', 'INVESTOR')),
    CONSTRAINT chk_language_code       CHECK (language_code IN ('HU', 'EN')),
    CONSTRAINT chk_approval_status     CHECK (approval_status IN (
                                           'PENDING_REVIEW', 'APPROVED', 'REJECTED', 'SUPERSEDED'
                                       )),
    CONSTRAINT chk_confidence_range    CHECK (confidence_score IS NULL
                                           OR (confidence_score BETWEEN 0.0 AND 1.0)),
    CONSTRAINT chk_period_id_format    CHECK (period_id BETWEEN 200001 AND 209912)  -- YYYYMM sanity
);

CREATE INDEX IF NOT EXISTS idx_commentary_queue_pending
    ON audit.commentary_queue (entity_id, period_id, commentary_role)
    WHERE approval_status = 'PENDING_REVIEW';

CREATE INDEX IF NOT EXISTS idx_commentary_queue_period
    ON audit.commentary_queue (period_id, commentary_role, approval_status);

CREATE INDEX IF NOT EXISTS idx_commentary_queue_batch
    ON audit.commentary_queue (batch_id)
    WHERE batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_commentary_queue_review
    ON audit.commentary_queue (approval_status, submitted_for_review_at, commentary_role);

COMMENT ON TABLE audit.commentary_queue IS
    'Approval queue for AI-generated financial narratives. commentary_generator.py inserts '
    'PENDING_REVIEW rows; the CFO Portal reads them for review; approved rows are published '
    'to gold.ai_commentary and the Power BI dashboard. REJECTED rows are retried on the next '
    'monthly close run. Superseded rows are kept for audit completeness.';

COMMENT ON COLUMN audit.commentary_queue.variance_fact_pack IS
    'Structured JSON passed to OpenAI as the "fact pack": '
    '{"period":"2026-03","revenue":{"actual":450000000,"budget":420000000,"prior_year":390000000},...}. '
    'Stored here for audit reproducibility — the exact inputs that generated this narrative.';

COMMENT ON COLUMN audit.commentary_queue.prompt_version IS
    'Version of the system prompt template (e.g. "v2.1-cfo-hu"). '
    'Enables prompt regression analysis across periods.';


-- =============================================================================
--  HELPER FUNCTION: audit.fn_is_valid_period_id(INT)
--  Returns TRUE if the integer represents a valid YYYYMM period key.
--  Reused by DQ-004 through DQ-011 stored procedures in fip_stored_procedures.sql.
-- =============================================================================

CREATE OR REPLACE FUNCTION audit.fn_is_valid_period_id (
    p_period_id  INT
)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE STRICT
AS $$
    -- YYYYMM: year 2000–2099, month 01–12
    SELECT (
        p_period_id BETWEEN 200001 AND 209912
        AND (p_period_id % 100) BETWEEN 1 AND 12
    );
$$;

COMMENT ON FUNCTION audit.fn_is_valid_period_id IS
    'Returns TRUE if the integer represents a valid YYYYMM period key '
    '(year 2000-2099, month 1-12). Used as input guard in all DQ procedures.';

-- Permissions: Service principal grants for commentary workflow and Power BI access
GRANT SELECT, INSERT, UPDATE ON audit.commentary_queue TO [commentary-sp];
GRANT SELECT ON audit.commentary_queue TO [powerbi-reader-sp];
GRANT EXECUTE ON FUNCTION audit.fn_is_valid_period_id TO [adf-sp];


-- =============================================================================
--  END OF fip_schema_audit.sql
--  Next file to run: fip_schema_bronze.sql
-- =============================================================================

-- =============================================================================
--  3.8 ANOMALY QUEUE
--  Queue table for anomaly_detector.py outputs.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.anomaly_queue (
    anomaly_id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id                UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    period_id                INT             NOT NULL,
    anomaly_class            VARCHAR(20)     NOT NULL,
    detection_method         VARCHAR(50)     NOT NULL,
    severity                 VARCHAR(10)     NOT NULL,
    kpi_affected             VARCHAR(100),
    description              TEXT            NOT NULL,
    recommended_action       TEXT,
    anomaly_score            NUMERIC(18,6),
    z_score                  NUMERIC(18,6),
    current_value            NUMERIC(18,4),
    historical_mean          NUMERIC(18,4),
    status                   VARCHAR(20)     NOT NULL DEFAULT 'OPEN',
    detected_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    resolved_by              VARCHAR(100),
    resolved_at              TIMESTAMPTZ,
    resolution_notes         TEXT,
    CONSTRAINT chk_anomaly_period CHECK (period_id BETWEEN 200001 AND 209912),
    CONSTRAINT chk_anomaly_class CHECK (anomaly_class IN ('STATISTICAL', 'STRUCTURAL', 'BEHAVIOURAL')),
    CONSTRAINT chk_anomaly_severity CHECK (severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO')),
    CONSTRAINT chk_anomaly_status CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED', 'FALSE_POSITIVE'))
);

CREATE INDEX IF NOT EXISTS idx_anomaly_queue_open
    ON audit.anomaly_queue (entity_id, period_id, severity)
    WHERE status IN ('OPEN', 'ACKNOWLEDGED');

CREATE INDEX IF NOT EXISTS idx_anomaly_queue_period
    ON audit.anomaly_queue (period_id, severity, detected_at DESC);

COMMENT ON TABLE audit.anomaly_queue IS
    'Operational anomaly queue populated by python/Anomaly_detection/anomaly_detector.py. '
    'Tracks anomaly lifecycle from OPEN to RESOLVED/FALSE_POSITIVE.';


-- =============================================================================
--  PROCEDURE: proc_publish_commentary_queue_to_gold
--  Publishes APPROVED, unpublished commentary from audit queue to gold.ai_commentary.
-- =============================================================================

CREATE OR REPLACE PROCEDURE audit.proc_publish_commentary_queue_to_gold(
    p_period_id INT DEFAULT NULL,
    p_entity_id UUID DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row RECORD;
    v_narrative_id UUID;
BEGIN
    FOR v_row IN
        SELECT
            q.queue_id,
            q.entity_id,
            q.period_id,
            q.commentary_role,
            q.language_code,
            q.narrative_text,
            q.variance_fact_pack,
            q.prompt_version,
            q.generated_by_model,
            q.generated_at,
            q.reviewed_by,
            q.reviewed_at,
            de.entity_key
        FROM audit.commentary_queue q
        JOIN silver.dim_entity de
          ON de.entity_id = q.entity_id
        WHERE q.approval_status = 'APPROVED'
          AND q.published_at IS NULL
          AND (p_period_id IS NULL OR q.period_id = p_period_id)
          AND (p_entity_id IS NULL OR q.entity_id = p_entity_id)
    LOOP
        INSERT INTO gold.ai_commentary (
            entity_id,
            entity_key,
            period_id,
            commentary_role,
            language_code,
            narrative_text,
            variance_fact_pack,
            source_queue_id,
            prompt_version,
            generated_by_model,
            generated_at,
            reviewed_by,
            reviewed_at,
            published_at
        ) VALUES (
            v_row.entity_id,
            v_row.entity_key,
            v_row.period_id,
            v_row.commentary_role,
            v_row.language_code,
            v_row.narrative_text,
            v_row.variance_fact_pack,
            v_row.queue_id,
            v_row.prompt_version,
            v_row.generated_by_model,
            v_row.generated_at,
            v_row.reviewed_by,
            v_row.reviewed_at,
            NOW()
        )
        ON CONFLICT (source_queue_id) DO NOTHING
        RETURNING narrative_id INTO v_narrative_id;

        UPDATE audit.commentary_queue
           SET published_at = COALESCE(published_at, NOW()),
               published_to_gold_id = COALESCE(published_to_gold_id, v_narrative_id)
         WHERE queue_id = v_row.queue_id;

        INSERT INTO audit.system_audit_log (event_type, user_id, entity_id, action, action_detail)
        VALUES (
            'AI_COMMENTARY_PUBLISHED',
            'system/proc_publish_commentary_queue_to_gold',
            v_row.entity_id,
            'PUBLISH',
            jsonb_build_object('queue_id', v_row.queue_id, 'period_id', v_row.period_id)
        );
    END LOOP;
END;
$$;

COMMENT ON PROCEDURE audit.proc_publish_commentary_queue_to_gold IS
    'Publishes approved commentary records from audit.commentary_queue to gold.ai_commentary '
    'and updates queue publication metadata.';

GRANT SELECT, INSERT, UPDATE ON audit.anomaly_queue TO [commentary-sp];
GRANT EXECUTE ON PROCEDURE audit.proc_publish_commentary_queue_to_gold TO [adf-sp];

-- P1-C: CREATE TABLE audit.anomaly_queue (Anomaly Detection Queue)
CREATE TABLE IF NOT EXISTS audit.anomaly_queue (
    anomaly_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    period_id INT NOT NULL,
    anomaly_class VARCHAR(50) NOT NULL, -- e.g., STATISTICAL, STRUCTURAL, BEHAVIOURAL
    detection_method VARCHAR(100) NOT NULL, -- e.g., ISOLATION_FOREST, ZSCORE, RULE_ENGINE
    severity VARCHAR(20) NOT NULL, -- e.g., CRITICAL, HIGH, MEDIUM, LOW
    kpi_affected VARCHAR(100), -- e.g., REVENUE, EBITDA, DUPLICATE_TRANSACTIONS
    description TEXT NOT NULL,
    recommended_action TEXT,
    anomaly_score NUMERIC(10,4), -- For statistical methods like Isolation Forest
    z_score NUMERIC(10,4), -- For Z-score based anomalies
    current_value NUMERIC(18,2), -- Current KPI value
    historical_mean NUMERIC(18,2), -- Historical mean for comparison
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN', -- e.g., OPEN, REVIEWED, RESOLVED, FALSE_POSITIVE
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT
);

COMMENT ON TABLE audit.anomaly_queue IS 'Stores detected anomalies from the anomaly detection agent for review and resolution.';
COMMENT ON COLUMN audit.anomaly_queue.anomaly_id IS 'Unique identifier for the anomaly record.';
COMMENT ON COLUMN audit.anomaly_queue.entity_id IS 'Business key for the entity where the anomaly was detected.';
COMMENT ON COLUMN audit.anomaly_queue.period_id IS 'Fiscal period identifier (YYYYMM) of the anomaly.';
COMMENT ON COLUMN audit.anomaly_queue.anomaly_class IS 'Classification of the anomaly (e.g., STATISTICAL, STRUCTURAL, BEHAVIOURAL).';
COMMENT ON COLUMN audit.anomaly_queue.detection_method IS 'Method used to detect the anomaly (e.g., ISOLATION_FOREST, ZSCORE, RULE_ENGINE).';
COMMENT ON COLUMN audit.anomaly_queue.severity IS 'Severity level of the anomaly (e.g., CRITICAL, HIGH, MEDIUM, LOW).';
COMMENT ON COLUMN audit.anomaly_queue.kpi_affected IS 'Specific KPI or area affected by the anomaly (e.g., REVENUE, EBITDA, DUPLICATE_TRANSACTIONS).';
COMMENT ON COLUMN audit.anomaly_queue.description IS 'Detailed description of the anomaly.';
COMMENT ON COLUMN audit.anomaly_queue.recommended_action IS 'Suggested action to investigate or resolve the anomaly.';
COMMENT ON COLUMN audit.anomaly_queue.anomaly_score IS 'Numerical score from anomaly detection algorithms (e.g., Isolation Forest outlier score).';
COMMENT ON COLUMN audit.anomaly_queue.z_score IS 'Z-score value for statistical anomalies.';
COMMENT ON COLUMN audit.anomaly_queue.current_value IS 'Current value of the affected KPI or metric.';
COMMENT ON COLUMN audit.anomaly_queue.historical_mean IS 'Historical mean value of the affected KPI for context.';
COMMENT ON COLUMN audit.anomaly_queue.status IS 'Current status of the anomaly record (e.g., OPEN, REVIEWED, RESOLVED, FALSE_POSITIVE).';
COMMENT ON COLUMN audit.anomaly_queue.detected_at IS 'Timestamp when the anomaly was detected.';
COMMENT ON COLUMN audit.anomaly_queue.reviewed_by IS 'User who reviewed the anomaly.';
COMMENT ON COLUMN audit.anomaly_queue.reviewed_at IS 'Timestamp when the anomaly was reviewed.';
COMMENT ON COLUMN audit.anomaly_queue.resolution_notes IS 'Notes on the resolution or investigation of the anomaly.';
