-- =============================================================================
--  FINANCIAL INTELLIGENCE PLATFORM
--  Schema: SILVER — Dimensions, Account Master & GL Fact Table
--  Version 1.0 · 2026 · HU GAAP
--
--  EXECUTION ORDER: 4 of 6
--  Prerequisites: fip_schema_config.sql, fip_schema_audit.sql,
--                 fip_schema_bronze.sql
--
--  WHO WRITES INTO THESE TABLES:
--    dbt pipeline — never the finance team, never manually.
--    dim_* tables are truncated and rebuilt on every pipeline run.
--    account_master is rebuilt after each CoA mapping change.
--    fact_gl_transaction is incrementally refreshed.
--
--  EXECUTION ORDER WITHIN THIS FILE:
--    1. dim_date             ← date spine; no upstream silver dependency
--    2. dim_account          ← refs config.ref_entity_master
--    3. dim_entity           ← refs config.ref_entity_master (self-ref)
--    4. dim_cost_centre      ← refs config.ref_cost_centre_master (self-ref)
--    5. dim_currency         ← refs config.ref_currencies
--    6. dim_project          ← refs config.ref_project_master, dim_cost_centre
--    7. account_master       ← governance master; refs config.ref_entity_master
--    8. fact_gl_transaction  ← central fact; refs all dims + audit.batch_log
-- =============================================================================


-- =============================================================================
--  SCHEMA DEFINITION
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;   -- cleansed, conformed, validated data

COMMENT ON SCHEMA silver IS 'Cleansed, validated, and conformed data. HU GAAP account taxonomy applied. Basis for all Gold Zone materializations.';


-- =============================================================================
--  4.1 DATE DIMENSION
--  A complete date spine from 2015-01-01 through 2035-12-31.
--  Pre-populated by a one-time dbt seed or a stored procedure call.
--  Hungarian public holidays are maintained in config.ref_hu_public_holidays
--  and JOINed in during dim_date population.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_date (
    date_key                INT             PRIMARY KEY,            -- YYYYMMDD integer for fast joins e.g. 20250115
    calendar_date           DATE            NOT NULL UNIQUE,
    year                    INT             NOT NULL,
    quarter                 INT             NOT NULL,               -- 1–4
    month                   INT             NOT NULL,               -- 1–12
    month_name              VARCHAR(20)     NOT NULL,
    month_name_hu           VARCHAR(20)     NOT NULL,
    week_of_year            INT             NOT NULL,
    day_of_week             INT             NOT NULL,               -- 1=Monday, 7=Sunday (ISO)
    day_name                VARCHAR(20)     NOT NULL,
    is_weekday              BOOLEAN         NOT NULL,
    is_hungarian_public_holiday BOOLEAN     NOT NULL DEFAULT FALSE,
    is_month_end            BOOLEAN         NOT NULL,
    is_quarter_end          BOOLEAN         NOT NULL,
    is_year_end             BOOLEAN         NOT NULL,
    -- Fiscal calendar fields (populated for the primary entity's fiscal year)
    fiscal_year             INT,
    fiscal_quarter          INT,
    fiscal_period           INT,                                    -- 1–12 fiscal period number
    fiscal_period_label     VARCHAR(20),                            -- e.g. '2025-P03'
    -- Prior period join keys (avoid runtime date arithmetic in DAX/SQL)
    prior_month_date_key    INT             REFERENCES silver.dim_date(date_key),
    prior_year_date_key     INT             REFERENCES silver.dim_date(date_key),
    prior_quarter_date_key  INT             REFERENCES silver.dim_date(date_key),
    -- Budget interpolation support
    trading_day_num         INT,                                    -- nth trading day within the month (for daily budget interpolation)
    trading_days_in_month   INT                                     -- total trading days in that month
);

COMMENT ON TABLE  silver.dim_date IS 'Complete date spine 2015–2035. Populated once at platform setup by dbt/seed. Prior period join keys (prior_month_date_key etc.) eliminate runtime date calculations in DAX and SQL, which is critical for DirectQuery performance. Hungarian public holidays are maintained separately and JOINed in.';


-- =============================================================================
--  4.2 ACCOUNT DIMENSION
--  Built by dbt from config.ref_coa_mapping.
--  Includes the full 3-level hierarchy and all line item mappings
--  needed by the Gold Zone KPI models.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_account (
    account_key             INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    account_code            VARCHAR(50)     NOT NULL,               -- source system account code
    account_code_canonical  VARCHAR(100),                           -- cleaned/padded canonical form
    account_name_hu         VARCHAR(200)    NOT NULL,
    account_name_en         VARCHAR(200),
    universal_node          VARCHAR(100)    NOT NULL,
    account_type            VARCHAR(20)     NOT NULL,               -- ASSET | LIABILITY | EQUITY | REVENUE | EXPENSE | COGS
    normal_balance          CHAR(1)         NOT NULL,               -- D | C
    l1_category             VARCHAR(100)    NOT NULL,
    l2_subcategory          VARCHAR(100),
    l3_detail               VARCHAR(100),
    pl_line_item            VARCHAR(100),
    cf_classification       VARCHAR(30),
    is_controlling          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_intercompany         BOOLEAN         NOT NULL DEFAULT FALSE,
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    valid_from              DATE            NOT NULL,
    valid_to                DATE,
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, account_code, valid_from)
);

COMMENT ON TABLE silver.dim_account IS 'Account dimension built by dbt from config.ref_coa_mapping. All fact table JOINs on account_key. The universal_node and pl_line_item columns are the primary grouping fields for P&L and balance sheet aggregations in the Gold Zone.';


-- =============================================================================
--  4.3 ENTITY DIMENSION
--  Built by dbt from config.ref_entity_master.
--  Denormalised to include all fields needed for direct queries
--  without requiring a JOIN back to the config schema.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_entity (
    entity_key              INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    entity_id               UUID            NOT NULL UNIQUE REFERENCES config.ref_entity_master(entity_id),
    entity_code             VARCHAR(20)     NOT NULL,
    entity_name             VARCHAR(200)    NOT NULL,
    entity_name_short       VARCHAR(50),
    legal_entity_type       VARCHAR(20)     NOT NULL,
    tax_id                  VARCHAR(20),
    country_code            CHAR(2)         NOT NULL,
    reporting_currency      CHAR(3)         NOT NULL,
    fiscal_year_start_month INT             NOT NULL,
    consolidation_group     VARCHAR(50),
    parent_entity_key       INT             REFERENCES silver.dim_entity(entity_key),
    consolidation_method    VARCHAR(20),
    is_active               BOOLEAN         NOT NULL,
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE silver.dim_entity IS 'Entity dimension with full hierarchy. parent_entity_key enables recursive CTE consolidation in the Gold Zone. Power BI Row-Level Security filters on entity_key to enforce per-entity data access.


-- =============================================================================
--  4.4 COST CENTRE DIMENSION
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_cost_centre (
    cost_centre_key         INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    cost_centre_id          UUID            NOT NULL UNIQUE REFERENCES config.ref_cost_centre_master(cost_centre_id),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    cost_centre_code        VARCHAR(30)     NOT NULL,
    cost_centre_name        VARCHAR(200)    NOT NULL,
    manager_name            VARCHAR(100),
    business_unit           VARCHAR(100),
    division                VARCHAR(100),
    region                  VARCHAR(100),
    is_budget_centre        BOOLEAN         NOT NULL,
    is_profit_centre        BOOLEAN         NOT NULL,
    parent_cost_centre_key  INT             REFERENCES silver.dim_cost_centre(cost_centre_key),
    is_active               BOOLEAN         NOT NULL,
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- =============================================================================
--  4.5 CURRENCY DIMENSION
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_currency (
    currency_key            INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    currency_code           CHAR(3)         NOT NULL UNIQUE REFERENCES config.ref_currencies(currency_code),
    currency_name           VARCHAR(50)     NOT NULL,
    currency_symbol         VARCHAR(5),
    decimal_places          INT             NOT NULL,
    is_reporting_currency   BOOLEAN         NOT NULL
);

INSERT INTO silver.dim_currency (currency_code, currency_name, currency_symbol, decimal_places, is_reporting_currency)
SELECT currency_code, currency_name, currency_symbol, decimal_places, is_reporting_currency
FROM   config.ref_currencies
ON CONFLICT (currency_code) DO NOTHING;


-- =============================================================================
--  4.6 PROJECT DIMENSION
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_project (
    project_key             INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    project_id              UUID            NOT NULL UNIQUE REFERENCES config.ref_project_master(project_id),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    project_code            VARCHAR(50)     NOT NULL,
    project_name            VARCHAR(200)    NOT NULL,
    project_type            VARCHAR(50),
    wbs_element             VARCHAR(50),
    cost_centre_key         INT             REFERENCES silver.dim_cost_centre(cost_centre_key),
    project_manager         VARCHAR(100),
    project_start_date      DATE,
    project_end_date        DATE,
    project_status          VARCHAR(20),
    is_active               BOOLEAN         NOT NULL
);


-- =============================================================================
--  SILVER LAYER: ACCOUNT MASTER
--
--  The governance foundation of the entire platform.
--  Populated by dbt from config.ref_coa_mapping after every mapping change.
--  Every local HU GAAP account code is mapped to a universal taxonomy node.
--  Mapping coverage must exceed 95% (APPROVED rows) before go-live.
--
--  Relationship to dim_account:
--    account_master = governance source of truth (who approved, when, why)
--    dim_account    = analytics-optimised denormalised copy (integer key for joins)
--    dbt reads account_master → writes dim_account
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.account_master (
    account_id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id              UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    local_account_code      VARCHAR(50)     NOT NULL,
    local_account_name      VARCHAR(200)    NOT NULL,               -- e.g. 'Befejezetlen termelés'
    local_account_name_en   VARCHAR(200),
    universal_node          VARCHAR(100)    NOT NULL,               -- e.g. 'ASSET.CURRENT.INVENTORY.WIP'
    account_type            VARCHAR(20)     NOT NULL,               -- 'ASSET' | 'LIABILITY' | 'EQUITY' | 'REVENUE' | 'EXPENSE' | 'COGS'
    normal_balance          CHAR(1)         NOT NULL CHECK (normal_balance IN ('D','C')),
    -- 3-level reporting hierarchy
    l1_category             VARCHAR(100)    NOT NULL,
    l2_subcategory          VARCHAR(100),
    l3_detail               VARCHAR(100),
    -- Statement line assignments
    pl_line_item            VARCHAR(100),                           -- NULL for balance sheet accounts
    -- P&L line values: 'REVENUE' | 'COGS' | 'GROSS_PROFIT' | 'OPEX' | 'EBITDA' | 'DA' | 'EBIT' | 'INTEREST' | 'EBT' | 'TAX' | 'NET_PROFIT'
    cf_classification       VARCHAR(30),                            -- 'OPERATING' | 'INVESTING' | 'FINANCING' | 'NON_CASH' | NULL
    bs_section              VARCHAR(50),                            -- Balance sheet section for statutory reporting
    -- HU GAAP statutory classification (2000/C Act form 'A' layout)
    hu_gaap_bs_line         VARCHAR(10),                            -- e.g. 'A.I.1', 'B.II.2' — statutory balance sheet line
    hu_gaap_pl_line         VARCHAR(10),                            -- e.g. '01', '02' — statutory P&L line (expenditure method)
    -- Control flags
    is_controlling          BOOLEAN         NOT NULL DEFAULT FALSE,  -- management accounting only; excluded from statutory
    is_intercompany         BOOLEAN         NOT NULL DEFAULT FALSE,  -- eliminated in group consolidation
    is_reconciling          BOOLEAN         NOT NULL DEFAULT FALSE,  -- technical/reconciling account; excluded from all reports
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    -- Temporal validity (CoA changes over time)
    valid_from              DATE            NOT NULL,
    valid_to                DATE,                                   -- NULL = currently valid
    -- Governance (mandatory — every mapping requires qualified accountant sign-off)
    mapping_rationale       TEXT,
    mapping_reviewed_by     VARCHAR(100)    NOT NULL DEFAULT 'PENDING',
    mapping_reviewed_at     TIMESTAMPTZ,
    review_status           VARCHAR(20)     NOT NULL DEFAULT 'PENDING' CHECK (review_status IN ('PENDING','APPROVED','DISPUTED')),
    -- dbt metadata
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    dbt_run_id              VARCHAR(100),                           -- dbt run identifier for lineage
    UNIQUE (entity_id, local_account_code, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_acct_master_company_code   ON silver.account_master (entity_id, local_account_code);
CREATE INDEX IF NOT EXISTS idx_acct_master_universal_node ON silver.account_master (universal_node);
CREATE INDEX IF NOT EXISTS idx_acct_master_pl_line        ON silver.account_master (pl_line_item) WHERE pl_line_item IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_acct_master_review         ON silver.account_master (review_status) WHERE review_status != 'APPROVED';

COMMENT ON TABLE  silver.account_master IS 'Governance-grade account master. Every local HU GAAP account code mapped to the universal taxonomy. Populated by dbt from config.ref_coa_mapping. review_status must be APPROVED for every row before the entity goes live — unmapped or PENDING accounts produce NULL KPIs downstream.
-- T-01 ARCHITECTURE NOTE: silver.account_master is NOT a duplicate of config.ref_coa_mapping.
--   Fields unique to account_master (not in ref_coa_mapping): hu_gaap_bs_line, hu_gaap_pl_line,
--   bs_section, is_reconciling, local_account_name_en, mapping_reviewed_by/at, dbt_run_id.
--   Fields unique to ref_coa_mapping: erp-level reviewed_by/at, created_at/updated_at audit trail.
--   NEVER edit account_master directly — dbt overwrites it. Fix source data in ref_coa_mapping.';
COMMENT ON COLUMN silver.account_master.hu_gaap_bs_line   IS 'Statutory balance sheet line reference per the 2000/C Act Schedule 1 form A layout. Used by the statutory reporting dbt model to produce the legally required balance sheet format.';
COMMENT ON COLUMN silver.account_master.hu_gaap_pl_line   IS 'Statutory P&L line reference per the 2000/C Act expenditure method (cost-type) format. Lines 01–18 matching the official form layout.';
COMMENT ON COLUMN silver.account_master.is_reconciling    IS 'TRUE for technical accounts (e.g. opening balance accounts, year-closing accounts) that must never appear in any financial statement. The dbt models filter these out before any aggregation.';


-- =============================================================================
--  SILVER LAYER: FACT TABLE — fact_gl_transaction
--
--  The cleansed, validated, conformed version of every GL transaction.
--  Built by dbt from the bronze raw tables after DQ checks pass.
--  Incrementally refreshed — new/changed transactions are appended;
--  corrections posted as reversal pairs (immutable ledger design).
--
--  This is the single source of truth for all financial analytics.
--  The Gold layer reads exclusively from here.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.fact_gl_transaction (
    -- Surrogate key
    transaction_key         BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    -- Natural / source key (for deduplication and lineage)
    source_transaction_id   VARCHAR(100)    NOT NULL,               -- ERP document number / posting ID
    source_system           VARCHAR(50)     NOT NULL,               -- 'SAP' | 'Business_Central' | 'Kulcs_Soft' | 'COBALT'
    batch_id                UUID            NOT NULL REFERENCES audit.batch_log(batch_id),
    -- Dimension keys (all populated by dbt after dim table lookups)
    date_key                INT             NOT NULL REFERENCES silver.dim_date(date_key),
    account_key             INT             NOT NULL REFERENCES silver.dim_account(account_key),
    entity_key              INT             NOT NULL REFERENCES silver.dim_entity(entity_key),
    cost_centre_key         INT             REFERENCES silver.dim_cost_centre(cost_centre_key),
    currency_key            INT             NOT NULL REFERENCES silver.dim_currency(currency_key),
    project_key             INT             REFERENCES silver.dim_project(project_key),
    -- Dates
    transaction_date        DATE            NOT NULL,               -- economic date of the transaction
    posting_date            DATE            NOT NULL,               -- date posted in the ERP
    period_id               INT             NOT NULL,               -- YYYYMM — fiscal period from ref_fiscal_calendar
    fiscal_year             INT             NOT NULL,
    fiscal_period           INT             NOT NULL,               -- 1–12
    -- Amounts (all amounts in HUF; FCY amounts preserved for audit)
    debit_lcy               NUMERIC(18,2)   NOT NULL DEFAULT 0,     -- local currency (HUF) debit
    credit_lcy              NUMERIC(18,2)   NOT NULL DEFAULT 0,     -- local currency (HUF) credit
    net_amount_lcy          NUMERIC(18,2)   NOT NULL,               -- debit_lcy - credit_lcy
    -- Foreign currency (populated if source transaction was in FCY)
    debit_fcy               NUMERIC(18,2),
    credit_fcy              NUMERIC(18,2),
    net_amount_fcy          NUMERIC(18,2),
    original_currency       CHAR(3),                                -- ISO 4217 code of the source amount
    fx_rate_used            NUMERIC(16,6),                          -- NBH rate applied to convert to HUF
    fx_rate_date            DATE,                                   -- date of the NBH rate used
    -- EUR equivalent (for management reporting only — not statutory)
    net_amount_eur          NUMERIC(18,2),
    eur_rate_used           NUMERIC(16,6),
    -- Transaction classification
    gaap_basis              VARCHAR(10)     NOT NULL DEFAULT 'HU_GAAP',
    document_type           VARCHAR(10),                            -- ERP document type code
    document_number         VARCHAR(50)     NOT NULL,               -- ERP document number
    document_line           INT,                                    -- line within the document
    posting_text            VARCHAR(200),                           -- narrative from the ERP
    reference_number        VARCHAR(50),                            -- vendor invoice / customer invoice number
    -- Flags
    is_intercompany         BOOLEAN         NOT NULL DEFAULT FALSE,
    is_opening_balance      BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE for period-opening balance carry-forwards
    is_reversal             BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE if this transaction reverses another
    reversal_of_key         BIGINT          REFERENCES silver.fact_gl_transaction(transaction_key),
    is_reversed             BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE if this transaction has been reversed
    is_late_entry           BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE if transaction_date is in a closed period
    is_adjusted             BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE if manually adjusted after initial load
    is_accrual              BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE for accrual/prepayment entries
    -- dbt metadata
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    dbt_run_id              VARCHAR(100),
    UNIQUE (source_system, source_transaction_id, document_line, entity_key)
);

-- Performance indexes for the most common query patterns
CREATE INDEX IF NOT EXISTS idx_gl_silver_entity_period   ON silver.fact_gl_transaction (entity_key, period_id, account_key);
CREATE INDEX IF NOT EXISTS idx_gl_silver_date            ON silver.fact_gl_transaction (date_key);
CREATE INDEX IF NOT EXISTS idx_gl_silver_account         ON silver.fact_gl_transaction (account_key);
CREATE INDEX IF NOT EXISTS idx_gl_silver_cost_centre     ON silver.fact_gl_transaction (cost_centre_key) WHERE cost_centre_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_gl_silver_batch           ON silver.fact_gl_transaction (batch_id);
CREATE INDEX IF NOT EXISTS idx_gl_silver_late_entries    ON silver.fact_gl_transaction (entity_key, period_id) WHERE is_late_entry = TRUE;
CREATE INDEX IF NOT EXISTS idx_gl_silver_intercompany    ON silver.fact_gl_transaction (entity_key, is_intercompany) WHERE is_intercompany = TRUE;

COMMENT ON TABLE  silver.fact_gl_transaction IS 'The central fact table for all financial analytics. Every GL posting line, cleansed and validated. Immutable ledger: corrections are reversals, never updates. Populated by dbt incrementally after each batch passes DQ checks. All dimension keys resolved — no raw account codes in this table.';
COMMENT ON COLUMN silver.fact_gl_transaction.net_amount_lcy  IS 'Signed net amount in HUF. Positive = debit balance, negative = credit balance. Revenue accounts will show negative net_amount_lcy for credits. The Gold Zone aggregations use ABS() where needed for reporting.';
COMMENT ON COLUMN silver.fact_gl_transaction.is_late_entry   IS 'TRUE when transaction_date falls in a period already closed in config.ref_fiscal_calendar. Late entries trigger the restatement protocol if the amount is material (> threshold in config).';
COMMENT ON COLUMN silver.fact_gl_transaction.is_opening_balance IS 'TRUE for carry-forward opening balance entries (nyitótételek) generated at fiscal year start. These are excluded from period movement calculations but included in closing balance calculations.';


-- =============================================================================
--  END OF fip_schema_silver.sql
--  Next file to run: fip_schema_budget.sql
-- =============================================================================
