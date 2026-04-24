-- =============================================================================
--  FINANCIAL INTELLIGENCE PLATFORM
--  Schema: BUDGET — Budget Uploads & AI Forecasts
--  Version 1.0 · 2026 · HU GAAP
--
--  EXECUTION ORDER: 5 of 6
--  Prerequisites: fip_schema_config.sql, fip_schema_silver.sql
--  NOTE: Must run BEFORE fip_schema_gold.sql because
--        gold.agg_variance_analysis references budget.ref_budget_versions.
--
--  WHO WRITES INTO THESE TABLES:
--    budget.ref_budget_versions  → Finance Director (version management)
--    budget.fact_budget          → ADF pipeline from Excel template upload
--    budget.fact_forecast        → Prophet/LSTM forecasting engine
--
-- =============================================================================


-- =============================================================================
--  SCHEMA DEFINITION
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS budget;   -- budget and forecast uploads

COMMENT ON SCHEMA budget IS 'Budget uploads and system-generated forecasts. Joined to actuals in Gold Zone KPI models.';


-- =============================================================================
--  5.1 BUDGET VERSION MASTER
--  Controls which budget version is "active" (used in variance calculations).
--  Must be created before fact_budget because fact_budget references it.
-- =============================================================================

CREATE TABLE IF NOT EXISTS budget.ref_budget_versions (
    budget_version_id       SERIAL          PRIMARY KEY,
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    fiscal_year             INT             NOT NULL,
    version_name            VARCHAR(50)     NOT NULL,               -- e.g. 'ORIGINAL_2025', 'REVISED_Q1_2025'
    version_type            VARCHAR(20)     NOT NULL DEFAULT 'ANNUAL', -- 'ANNUAL' | 'REVISED' | 'ROLLING'
    is_active               BOOLEAN         NOT NULL DEFAULT FALSE,  -- only one version active per entity per year
    approved_by             VARCHAR(100),
    approved_at             TIMESTAMPTZ,
    upload_completed_at     TIMESTAMPTZ,
    notes                   TEXT,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, fiscal_year, version_name)
);

COMMENT ON TABLE budget.ref_budget_versions IS 'Controls budget version lifecycle. Setting is_active=TRUE on a new version automatically drives the variance columns in the Gold Zone KPI models. Only one version per entity per fiscal year should be active at any time.';


-- =============================================================================
--  5.2 BUDGET FACT TABLE
--  Stores approved budgets uploaded by the finance team.
--  Multiple budget versions can coexist (e.g. ORIGINAL, REVISED_Q1).
--  The active version is controlled by budget.ref_budget_versions.
-- =============================================================================

CREATE TABLE IF NOT EXISTS budget.fact_budget (
    budget_id               BIGSERIAL       PRIMARY KEY,
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    budget_version_id       INT             NOT NULL REFERENCES budget.ref_budget_versions(budget_version_id), -- T-05 FIX: explicit FK (was comment-only)
    period_id               INT             NOT NULL,               -- YYYYMM
    account_key             INT             NOT NULL REFERENCES silver.dim_account(account_key),
    cost_centre_key         INT             REFERENCES silver.dim_cost_centre(cost_centre_key),
    project_key             INT             REFERENCES silver.dim_project(project_key),
    budget_amount_huf       NUMERIC(18,2)   NOT NULL,
    uploaded_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    uploaded_by             VARCHAR(100)    NOT NULL,
    source_file             TEXT,                                   -- path to the source Excel file
    UNIQUE (entity_id, budget_version_id, period_id, account_key, cost_centre_key, project_key)
);

CREATE INDEX IF NOT EXISTS idx_budget_entity_period ON budget.fact_budget (entity_id, period_id, budget_version_id);

COMMENT ON TABLE  budget.fact_budget IS 'Annual budget data uploaded by finance team via Excel template. Joined to actuals in Gold Zone agg_pl_monthly to compute revenue_budget, revenue_variance, and revenue_variance_pct columns. Multiple versions allow for budget revisions without losing history.';


-- =============================================================================
--  5.3 FORECAST TABLE
--  Written by the Prophet/LSTM forecasting engine
--  Stores probabilistic forecasts (median + confidence intervals).
-- =============================================================================

CREATE TABLE IF NOT EXISTS budget.fact_forecast (
    forecast_id             BIGSERIAL       PRIMARY KEY,
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    forecast_run_id         UUID            NOT NULL,               -- groups all metrics from a single model run
    model_name              VARCHAR(50)     NOT NULL,               -- 'PROPHET' | 'LSTM' | 'ARIMA' | 'NAIVE'
    model_version           VARCHAR(20),
    period_id               INT             NOT NULL,               -- YYYYMM being forecast
    kpi_name                VARCHAR(100)    NOT NULL,               -- e.g. 'revenue' | 'ebitda' | 'cash_balance'
    forecast_p50            NUMERIC(18,2)   NOT NULL,               -- median forecast (best estimate)
    forecast_p10            NUMERIC(18,2),                          -- 10th percentile (pessimistic)
    forecast_p90            NUMERIC(18,2),                          -- 90th percentile (optimistic)
    confidence_interval_pct INT             DEFAULT 80,             -- width of the confidence interval
    training_data_periods   INT,                                    -- how many historical periods used to train
    generated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_forecast_entity_period ON budget.fact_forecast (entity_id, period_id, model_name);

COMMENT ON TABLE  budget.fact_forecast IS 'Statistical forecasts generated by the Prophet/LSTM models. The p10/p50/p90 columns power the confidence band visualisations on the CFO cash flow dashboard. Multiple model_names can coexist for the same period — the dashboard shows the active model configured per entity.';


-- =============================================================================
--  END OF fip_schema_budget.sql
--  Next file to run: fip_schema_gold.sql
-- =============================================================================
