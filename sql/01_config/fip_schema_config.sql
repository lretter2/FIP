-- =============================================================================
--  FINANCIAL INTELLIGENCE PLATFORM
--  Schema: CONFIG — Reference & Configuration Tables
--  Version 1.0 · 2026 · HU GAAP
--
--  EXECUTION ORDER: 1 of 6
--  Run this file FIRST — every other schema depends on config tables.
--
--  POPULATION RESPONSIBILITY:
--    config.ref_entity_master       → Finance Director
--    config.ref_coa_mapping         → Chief Accountant + Data Engineer
--    config.ref_currencies          → Data Engineer (ISO 4217 standard list — seeded below)
--    config.ref_fiscal_calendar     → Finance Director
--    config.ref_cost_centre_master  → Controller
--    config.ref_project_master      → Controller / Project Office
--    config.ref_alert_rules         → CFO + Controller
--    config.ref_intercompany_pairs  → Finance Director (multi-entity only)
--    config.ref_hu_public_holidays  → Data Engineer (seeded below, maintain annually)
--    config.ref_fx_rates            → ADF pipeline (daily NBH feed)
-- =============================================================================


-- =============================================================================
--  SCHEMA DEFINITION
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS config;   -- reference/seed data; manually maintained

COMMENT ON SCHEMA config IS 'Reference and configuration tables. Manually seeded by finance/data team. Never overwritten by pipelines.';


-- =============================================================================
--  2.1 ENTITY MASTER
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_entity_master (
    entity_id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_code             VARCHAR(20)     NOT NULL UNIQUE,        -- short code used in partition paths, e.g. 'ACME_HU'
    entity_name             VARCHAR(200)    NOT NULL,
    entity_name_short       VARCHAR(50),
    legal_entity_type       VARCHAR(20)     NOT NULL,               -- 'Kft' | 'Zrt' | 'Nyrt' | 'Bt' | 'Kkt' | 'Egyéni_vállalkozó'
    tax_id                  VARCHAR(20),                            -- adószám (8+1+2 format)
    registration_number     VARCHAR(30),                            -- cégjegyzékszám
    country_code            CHAR(2)         NOT NULL DEFAULT 'HU',  -- ISO 3166-1 alpha-2
    reporting_currency      CHAR(3)         NOT NULL DEFAULT 'HUF', -- ISO 4217
    functional_currency     CHAR(3)         NOT NULL DEFAULT 'HUF',
    gaap_basis              VARCHAR(20)     NOT NULL DEFAULT 'HU_GAAP',
    fiscal_year_start_month INT             NOT NULL DEFAULT 1,     -- 1 = January; for non-calendar FY
    consolidation_group     VARCHAR(50),                            -- top-level group name
    parent_entity_id        UUID            REFERENCES config.ref_entity_master(entity_id),
    consolidation_method    VARCHAR(20)     DEFAULT 'FULL',         -- 'FULL' | 'EQUITY' | 'PROPORTIONAL' | 'NONE'
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    data_owner              VARCHAR(100),                           -- person responsible for this entity's data quality
    erp_system              VARCHAR(50),                            -- 'SAP' | 'Business_Central' | 'Kulcs_Soft' | 'COBALT' | 'Számlázz'
    erp_company_code        VARCHAR(20),                            -- identifier within the ERP
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  config.ref_entity_master IS 'Master list of all legal entities. Every other table with a company_id or entity_id foreign key depends on this. Populate before anything else.';
COMMENT ON COLUMN config.ref_entity_master.fiscal_year_start_month IS 'Set to 1 for calendar fiscal year (Jan–Dec). Set to 7 for July–June fiscal year, etc.';
COMMENT ON COLUMN config.ref_entity_master.consolidation_method    IS 'FULL = 100% consolidation. EQUITY = equity method only. PROPORTIONAL = proportional share. NONE = excluded from group consolidation.';


-- =============================================================================
--  2.2 CURRENCY MASTER
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_currencies (
    currency_id             SERIAL          PRIMARY KEY,
    currency_code           CHAR(3)         NOT NULL UNIQUE,        -- ISO 4217 e.g. 'HUF', 'EUR', 'USD'
    currency_name           VARCHAR(50)     NOT NULL,
    currency_name_hu        VARCHAR(50),
    currency_symbol         VARCHAR(5),
    decimal_places          INT             NOT NULL DEFAULT 2,
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    is_reporting_currency   BOOLEAN         NOT NULL DEFAULT FALSE,  -- exactly one row should be TRUE (HUF)
    nbh_feed_code           VARCHAR(10)                              -- code used in the NBH SOAP API rate feed
);

INSERT INTO config.ref_currencies (currency_code, currency_name, currency_name_hu, currency_symbol, decimal_places, is_reporting_currency, nbh_feed_code)
VALUES
    ('HUF', 'Hungarian Forint',   'Magyar Forint',   'Ft',  2, TRUE,  'HUF'),
    ('EUR', 'Euro',                'Euró',            '€',   2, FALSE, 'EUR'),
    ('USD', 'US Dollar',           'Amerikai dollár', '$',   2, FALSE, 'USD'),
    ('GBP', 'British Pound',       'Brit font',       '£',   2, FALSE, 'GBP'),
    ('CHF', 'Swiss Franc',         'Svájci frank',    'CHF', 2, FALSE, 'CHF'),
    ('CZK', 'Czech Koruna',        'Cseh korona',     'Kč',  2, FALSE, 'CZK'),
    ('PLN', 'Polish Zloty',        'Lengyel zloty',   'zł',  2, FALSE, 'PLN'),
    ('RON', 'Romanian Leu',        'Román lej',       'lei', 2, FALSE, 'RON'),
    ('RSD', 'Serbian Dinar',       'Szerb dinár',     'din', 2, FALSE, 'RSD')
ON CONFLICT (currency_code) DO NOTHING;

COMMENT ON TABLE  config.ref_currencies IS 'ISO 4217 currency master. Pipeline DQ rule DB-006 blocks any transaction with an unrecognised currency_code. Seed with all currencies that appear in any connected ERP.';
COMMENT ON COLUMN config.ref_currencies.nbh_feed_code IS 'Code as returned by the Magyar Nemzeti Bank (NBH) SOAP/REST rate feed. Used by the daily FX rate ingestion job.';


-- =============================================================================
--  2.3 NBH DAILY FX RATES
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_fx_rates (
    rate_id                 BIGSERIAL       PRIMARY KEY,
    rate_date               DATE            NOT NULL,
    currency_code           CHAR(3)         NOT NULL REFERENCES config.ref_currencies(currency_code),
    rate_to_huf             NUMERIC(16,6)   NOT NULL,               -- 1 unit of foreign currency = N HUF
    rate_source             VARCHAR(20)     NOT NULL DEFAULT 'NBH', -- 'NBH' | 'ECB' | 'MANUAL'
    is_official             BOOLEAN         NOT NULL DEFAULT TRUE,
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (rate_date, currency_code)
);

CREATE INDEX IF NOT EXISTS idx_fx_rates_date_currency ON config.ref_fx_rates (rate_date, currency_code);

COMMENT ON TABLE  config.ref_fx_rates IS 'Daily FX rates sourced from the Magyar Nemzeti Bank (NBH) official feed. All non-HUF transaction amounts in Silver layer are converted using the rate on the transaction_date. If no rate exists, DQ-007 fires a WARNING and the conversion falls back to the most recent available rate.';
COMMENT ON COLUMN config.ref_fx_rates.rate_to_huf IS 'Middle rate (középárfolyam) as published by NBH. 1 EUR = ~395 HUF for example. Always convert as: amount_huf = amount_fcy * rate_to_huf.';


-- =============================================================================
--  2.4 FISCAL CALENDAR CONFIGURATION
--  Defines how each entity's fiscal year maps to calendar months.
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_fiscal_calendar (
    fiscal_cal_id           SERIAL          PRIMARY KEY,
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    fiscal_year             INT             NOT NULL,               -- e.g. 2025
    period_num              INT             NOT NULL,               -- 1–12 (fiscal period within the year)
    calendar_year           INT             NOT NULL,
    calendar_month          INT             NOT NULL,               -- 1–12
    period_start_date       DATE            NOT NULL,
    period_end_date         DATE            NOT NULL,
    period_label            VARCHAR(20)     NOT NULL,               -- e.g. '2025-P01', '2025-JAN'
    is_closed               BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE once the period is locked in the ERP
    closed_at               TIMESTAMPTZ,                            -- when the period was locked
    closed_by               VARCHAR(100),
    UNIQUE (entity_id, fiscal_year, period_num)
);

CREATE INDEX IF NOT EXISTS idx_fiscal_cal_entity_date ON config.ref_fiscal_calendar (entity_id, period_start_date, period_end_date);

COMMENT ON TABLE  config.ref_fiscal_calendar IS 'Maps each entity''s fiscal periods to calendar dates. The dbt macro fiscal_period() uses this table to assign period_id to every transaction. Populate for at least 3 years back + 2 years forward before go-live.';
COMMENT ON COLUMN config.ref_fiscal_calendar.is_closed IS 'When TRUE, any new transaction with a transaction_date in this period is flagged as is_late_entry=TRUE by the Silver layer pipeline.';


-- =============================================================================
--  2.5 CHART OF ACCOUNTS MAPPING
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_coa_mapping (
    mapping_id              SERIAL          PRIMARY KEY,
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    local_account_code      VARCHAR(50)     NOT NULL,               -- e.g. '311', '454', '961'
    local_account_name      VARCHAR(200)    NOT NULL,               -- e.g. 'Vevőkövetelések'
    universal_node          VARCHAR(100)    NOT NULL,               -- e.g. 'ASSET.CURRENT.RECEIVABLES.TRADE'
    account_type            VARCHAR(20)     NOT NULL,               -- 'ASSET' | 'LIABILITY' | 'EQUITY' | 'REVENUE' | 'EXPENSE' | 'COGS'
    normal_balance          CHAR(1)         NOT NULL,               -- 'D' debit | 'C' credit
    l1_category             VARCHAR(100)    NOT NULL,               -- top-level group for reporting
    l2_subcategory          VARCHAR(100),
    l3_detail               VARCHAR(100),
    pl_line_item            VARCHAR(100),                           -- P&L line mapping (REVENUE | COGS | GROSS_PROFIT | OPEX | EBITDA | EBIT | EBT | NET_PROFIT)
    cf_classification       VARCHAR(30),                            -- 'OPERATING' | 'INVESTING' | 'FINANCING' | 'NON_CASH'
    is_controlling          BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE = management accounting only; excluded from statutory P&L
    is_intercompany         BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE = used for intercompany transactions; eliminated in consolidation
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    valid_from              DATE            NOT NULL,
    valid_to                DATE,                                   -- NULL = currently valid; set when account is retired
    -- Governance fields (mandatory — every mapping must be reviewed by a qualified accountant)
    mapping_rationale       TEXT,                                   -- why this account maps to this universal node
    reviewed_by             VARCHAR(100)    NOT NULL DEFAULT 'PENDING',
    reviewed_at             TIMESTAMPTZ,
    review_status           VARCHAR(20)     NOT NULL DEFAULT 'PENDING', -- 'PENDING' | 'APPROVED' | 'DISPUTED'
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, local_account_code, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_coa_entity_code  ON config.ref_coa_mapping (entity_id, local_account_code);
CREATE INDEX IF NOT EXISTS idx_coa_universal    ON config.ref_coa_mapping (universal_node);
CREATE INDEX IF NOT EXISTS idx_coa_review       ON config.ref_coa_mapping (review_status) WHERE review_status != 'APPROVED';

COMMENT ON TABLE  config.ref_coa_mapping IS 'THE most critical configuration table. Maps every local HU GAAP account code to a universal taxonomy node. Poor mapping quality directly produces wrong KPIs. Every row must have review_status=APPROVED by a qualified accountant before the entity goes live. The pipeline dashboard shows mapping coverage % prominently.'
COMMENT ON COLUMN config.ref_coa_mapping.pl_line_item IS 'P&L line assignment used by the Gold Zone agg_pl_monthly model. Must be set for all income statement accounts. Balance sheet accounts leave this NULL.';
COMMENT ON COLUMN config.ref_coa_mapping.cf_classification IS 'Cash flow statement classification. Required for the indirect method cash flow model in Gold Zone. OPERATING for most OPEX; INVESTING for CapEx; FINANCING for debt service and equity movements.';


-- =============================================================================
--  2.6 COST CENTRE MASTER
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_cost_centre_master (
    cost_centre_id          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    cost_centre_code        VARCHAR(30)     NOT NULL,
    cost_centre_name        VARCHAR(200)    NOT NULL,
    cost_centre_name_en     VARCHAR(200),
    manager_name            VARCHAR(100),
    manager_email           VARCHAR(200),
    business_unit           VARCHAR(100),                           -- e.g. 'Operations', 'Sales', 'Finance'
    division                VARCHAR(100),
    region                  VARCHAR(100),
    is_budget_centre        BOOLEAN         NOT NULL DEFAULT TRUE,   -- TRUE = has its own budget line
    is_profit_centre        BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE = also tracks revenue
    parent_cost_centre_id   UUID            REFERENCES config.ref_cost_centre_master(cost_centre_id),
    erp_cost_centre_code    VARCHAR(30),                            -- code as it appears in the ERP
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    valid_from              DATE            NOT NULL,
    valid_to                DATE,
    UNIQUE (entity_id, cost_centre_code)
);

COMMENT ON TABLE config.ref_cost_centre_master IS 'All organisational units that incur costs. Parent-child hierarchy enables roll-up from individual cost centres to divisions and business units. Power BI Row-Level Security uses this table to restrict Controller-level users to their own cost centres.';


-- =============================================================================
--  2.7 PROJECT MASTER
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_project_master (
    project_id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    project_code            VARCHAR(50)     NOT NULL,
    project_name            VARCHAR(200)    NOT NULL,
    project_type            VARCHAR(50),                            -- 'CAPEX' | 'OPEX' | 'CLIENT' | 'INTERNAL' | 'RD'
    wbs_element             VARCHAR(50),                            -- WBS element for SAP integration
    cost_centre_id          UUID            REFERENCES config.ref_cost_centre_master(cost_centre_id),
    project_manager         VARCHAR(100),
    client_name             VARCHAR(200),
    budget_amount           NUMERIC(18,2),
    project_start_date      DATE,
    project_end_date        DATE,
    project_status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE', -- 'ACTIVE' | 'COMPLETED' | 'ON_HOLD' | 'CANCELLED'
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    UNIQUE (entity_id, project_code)
);

COMMENT ON TABLE config.ref_project_master IS 'Project master for project-level cost tracking. The WBS element enables SAP CO-PS integration. All project-coded GL transactions in the Silver layer JOIN to this table via the dim_project dimension.';


-- =============================================================================
--  2.8 INTERCOMPANY REGISTRY
--  Defines which entity pairs have intercompany transactions.
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_intercompany_pairs (
    pair_id                 SERIAL          PRIMARY KEY,
    seller_entity_id        UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    buyer_entity_id         UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    seller_account_code     VARCHAR(50),                            -- the account used to record the interco sale
    buyer_account_code      VARCHAR(50),                            -- the account used to record the interco purchase
    transaction_type        VARCHAR(30)     NOT NULL,               -- 'GOODS_SALE' | 'SERVICE' | 'LOAN' | 'DIVIDEND' | 'MANAGEMENT_FEE'
    elimination_type        VARCHAR(30)     NOT NULL,               -- 'REVENUE_EXPENSE' | 'RECEIVABLE_PAYABLE' | 'INVESTMENT_EQUITY'
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    notes                   TEXT,
    UNIQUE (seller_entity_id, buyer_entity_id, transaction_type)
);

COMMENT ON TABLE config.ref_intercompany_pairs IS 'Defines all known intercompany transaction relationships within the group. The consolidation dbt model uses this registry to automatically flag and eliminate intercompany balances.'


-- =============================================================================
--  2.9 ALERT RULES CONFIGURATION
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_alert_rules (
    rule_id                 SERIAL          PRIMARY KEY,
    rule_code               VARCHAR(50)     NOT NULL UNIQUE,        -- e.g. 'CASH_BALANCE_CRITICAL'
    kpi_name                VARCHAR(100)    NOT NULL,               -- must match a column in gold.agg_pl_monthly or a KPI view
    entity_scope            VARCHAR(20)     NOT NULL DEFAULT 'ALL', -- 'ALL' or specific entity_code
    operator                VARCHAR(5)      NOT NULL,               -- '<' | '>' | '<=' | '>=' | '='
    threshold_value         NUMERIC(18,4)   NOT NULL,
    threshold_unit          VARCHAR(20)     NOT NULL DEFAULT 'ABSOLUTE', -- 'ABSOLUTE' | 'PCT_OF_BUDGET' | 'PCT_OF_PY'
    severity                VARCHAR(10)     NOT NULL,               -- 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW' | 'INFO'
    alert_title             VARCHAR(200)    NOT NULL,               -- human-readable title for the notification
    recipient_roles         VARCHAR(200)    NOT NULL,               -- comma-separated roles: 'ceo,cfo,controller,ap_team'
    notification_channels   VARCHAR(100)    NOT NULL DEFAULT 'EMAIL', -- 'EMAIL' | 'TEAMS' | 'SLACK' | 'EMAIL,TEAMS'
    cooldown_hours          INT             NOT NULL DEFAULT 24,    -- minimum hours between repeated alerts for same rule+entity
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    created_by              VARCHAR(100)    NOT NULL,
    approved_by             VARCHAR(100),                           -- CFO sign-off required for CRITICAL and HIGH rules
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Seed with the standard alert rules from the platform specification
INSERT INTO config.ref_alert_rules
    (rule_code, kpi_name, operator, threshold_value, threshold_unit, severity, alert_title, recipient_roles, cooldown_hours, created_by)
VALUES
    ('CASH_CRITICAL',       'cash_balance',         '<',  50000000, 'ABSOLUTE',     'CRITICAL', 'Cash balance below 50M HUF — immediate runway risk',    'ceo,cfo',              4,  'system'),
    ('EBITDA_MARGIN_LOW',   'ebitda_margin_pct',    '<',  0.05,     'ABSOLUTE',     'HIGH',     'EBITDA margin below 5% — profitability at risk',         'cfo',                  24, 'system'),
    ('REVENUE_VS_BUDGET',   'revenue_vs_budget_pct','<', -0.10,     'ABSOLUTE',     'HIGH',     'Revenue >10% below budget',                             'cfo,controller',       24, 'system'),
    ('DSO_EXCESSIVE',       'dso_days',             '>',  75,       'ABSOLUTE',     'MEDIUM',   'DSO exceeds 75 days — receivables collection at risk',   'controller',           48, 'system'),
    ('NET_DEBT_EBITDA',     'net_debt_ebitda',      '>',  3.5,      'ABSOLUTE',     'HIGH',     'Net Debt/EBITDA exceeds 3.5x — covenant breach risk',    'cfo',                  48, 'system'),
    ('OVERDUE_AP',          'overdue_ap_pct',       '>',  0.15,     'ABSOLUTE',     'MEDIUM',   'Overdue AP exceeds 15% of total payables',              'ap_team,controller',   48, 'system'),
    ('LARGE_GL_POSTING',    'unusual_gl_amount_huf','>', 100000000, 'ABSOLUTE',     'LOW',      'Unusual GL posting exceeding 100M HUF',                  'controller',           1,  'system'),
    ('NEGATIVE_GROWTH',     'revenue_yoy_pct',      '<',  0,        'ABSOLUTE',     'MEDIUM',   'Revenue below prior year — negative YoY growth',         'ceo,cfo',              168,'system'),
    ('EBITDA_VS_BUDGET',    'ebitda_vs_budget_pct', '<', -0.15,     'ABSOLUTE',     'HIGH',     'EBITDA >15% below budget',                              'cfo,controller',       24, 'system'),
    ('CASH_RUNWAY',         'cash_runway_days',     '<',  30,       'ABSOLUTE',     'CRITICAL', 'Cash runway below 30 days',                             'ceo,cfo',              4,  'system')
ON CONFLICT (rule_code) DO NOTHING;

COMMENT ON TABLE  config.ref_alert_rules IS 'Single source of truth for all automated alert thresholds. Evaluated after every Gold Zone refresh by the stored procedure proc_evaluate_alerts(). CFO approval required before CRITICAL or HIGH severity rules go live. cooldown_hours prevents alert fatigue for recurring breaches.';


-- =============================================================================
--  2.10 HUNGARIAN PUBLIC HOLIDAYS
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.ref_hu_public_holidays (
    holiday_date            DATE            PRIMARY KEY,
    holiday_name            VARCHAR(100)    NOT NULL,
    holiday_name_en         VARCHAR(100),
    holiday_type            VARCHAR(20)     NOT NULL DEFAULT 'NATIONAL' -- 'NATIONAL' | 'SUBSTITUTE_WORKDAY'
);

-- Seed: 2024–2030 Hungarian public holidays
INSERT INTO config.ref_hu_public_holidays (holiday_date, holiday_name, holiday_name_en, holiday_type) VALUES
        -- 2024
    ('2024-01-01', 'Újév',                                'New Year''s Day',              'NATIONAL'),
    ('2024-03-15', '1848-as forradalom ünnepe',            'National Day',                 'NATIONAL'),
    ('2024-03-29', 'Nagypéntek',                           'Good Friday',                  'NATIONAL'),
    ('2024-04-01', 'Húsvéthétfő',                          'Easter Monday',                'NATIONAL'),
    ('2024-05-01', 'A munka ünnepe',                       'Labour Day',                   'NATIONAL'),
    ('2024-05-20', 'Pünkösdhétfő',                         'Whit Monday',                  'NATIONAL'),
    ('2024-08-20', 'Az államalapítás ünnepe',              'St. Stephen''s Day',           'NATIONAL'),
    ('2024-10-23', 'Az 1956-os forradalom ünnepe',         '1956 Revolution Day',          'NATIONAL'),
    ('2024-11-01', 'Mindenszentek',                        'All Saints'' Day',             'NATIONAL'),
    ('2024-12-25', 'Karácsony első napja',                 'Christmas Day',                'NATIONAL'),
    ('2024-12-26', 'Karácsony második napja',              'Second Day of Christmas',      'NATIONAL'),
    -- 2025
    ('2025-01-01', 'Újév',                                'New Year''s Day',              'NATIONAL'),
    ('2025-03-15', '1848-as forradalom ünnepe',            'National Day',                 'NATIONAL'),
    ('2025-04-18', 'Nagypéntek',                           'Good Friday',                  'NATIONAL'),
    ('2025-04-21', 'Húsvéthétfő',                          'Easter Monday',                'NATIONAL'),
    ('2025-05-01', 'A munka ünnepe',                       'Labour Day',                   'NATIONAL'),
    ('2025-06-09', 'Pünkösdhétfő',                         'Whit Monday',                  'NATIONAL'),
    ('2025-08-20', 'Az államalapítás ünnepe',              'St. Stephen''s Day',           'NATIONAL'),
    ('2025-10-23', 'Az 1956-os forradalom ünnepe',         '1956 Revolution Day',          'NATIONAL'),
    ('2025-11-01', 'Mindenszentek',                        'All Saints'' Day',             'NATIONAL'),
    ('2025-12-25', 'Karácsony első napja',                 'Christmas Day',                'NATIONAL'),
    ('2025-12-26', 'Karácsony második napja',              'Second Day of Christmas',      'NATIONAL'),
    -- 2026
    ('2026-01-01', 'Újév',                                'New Year''s Day',              'NATIONAL'),
    ('2026-03-15', '1848-as forradalom ünnepe',            'National Day',                 'NATIONAL'),
    ('2026-04-03', 'Nagypéntek',                           'Good Friday',                  'NATIONAL'),
    ('2026-04-06', 'Húsvéthétfő',                          'Easter Monday',                'NATIONAL'),
    ('2026-05-01', 'A munka ünnepe',                       'Labour Day',                   'NATIONAL'),
    ('2026-05-25', 'Pünkösdhétfő',                         'Whit Monday',                  'NATIONAL'),
    ('2026-08-20', 'Az államalapítás ünnepe',              'St. Stephen''s Day',           'NATIONAL'),
    ('2026-10-23', 'Az 1956-os forradalom ünnepe',         '1956 Revolution Day',          'NATIONAL'),
    ('2026-11-01', 'Mindenszentek',                        'All Saints'' Day',             'NATIONAL'),
    ('2026-12-25', 'Karácsony első napja',                 'Christmas Day',                'NATIONAL'),
    ('2026-12-26', 'Karácsony második napja',              'Second Day of Christmas',      'NATIONAL'),
    -- 2027
    ('2027-01-01', 'Újév',                                'New Year''s Day',              'NATIONAL'),
    ('2027-03-15', '1848-as forradalom ünnepe',            'National Day',                 'NATIONAL'),
    ('2027-03-26', 'Nagypéntek',                           'Good Friday',                  'NATIONAL'),
    ('2027-03-29', 'Húsvéthétfő',                          'Easter Monday',                'NATIONAL'),
    ('2027-05-01', 'A munka ünnepe',                       'Labour Day',                   'NATIONAL'),
    ('2027-05-17', 'Pünkösdhétfő',                         'Whit Monday',                  'NATIONAL'),
    ('2027-08-20', 'Az államalapítás ünnepe',              'St. Stephen''s Day',           'NATIONAL'),
    ('2027-10-23', 'Az 1956-os forradalom ünnepe',         '1956 Revolution Day',          'NATIONAL'),
    ('2027-11-01', 'Mindenszentek',                        'All Saints'' Day',             'NATIONAL'),
    ('2027-12-25', 'Karácsony első napja',                 'Christmas Day',                'NATIONAL'),
    ('2027-12-26', 'Karácsony második napja',              'Second Day of Christmas',      'NATIONAL'),
    -- 2028
    ('2028-01-01', 'Újév',                                'New Year''s Day',              'NATIONAL'),
    ('2028-03-15', '1848-as forradalom ünnepe',            'National Day',                 'NATIONAL'),
    ('2028-04-14', 'Nagypéntek',                           'Good Friday',                  'NATIONAL'),
    ('2028-04-17', 'Húsvéthétfő',                          'Easter Monday',                'NATIONAL'),
    ('2028-05-01', 'A munka ünnepe',                       'Labour Day',                   'NATIONAL'),
    ('2028-06-05', 'Pünkösdhétfő',                         'Whit Monday',                  'NATIONAL'),
    ('2028-08-20', 'Az államalapítás ünnepe',              'St. Stephen''s Day',           'NATIONAL'),
    ('2028-10-23', 'Az 1956-os forradalom ünnepe',         '1956 Revolution Day',          'NATIONAL'),
    ('2028-11-01', 'Mindenszentek',                        'All Saints'' Day',             'NATIONAL'),
    ('2028-12-25', 'Karácsony első napja',                 'Christmas Day',                'NATIONAL'),
    ('2028-12-26', 'Karácsony második napja',              'Second Day of Christmas',      'NATIONAL'),
    -- 2029
    ('2029-01-01', 'Újév',                                'New Year''s Day',              'NATIONAL'),
    ('2029-03-15', '1848-as forradalom ünnepe',            'National Day',                 'NATIONAL'),
    ('2029-03-30', 'Nagypéntek',                           'Good Friday',                  'NATIONAL'),
    ('2029-04-02', 'Húsvéthétfő',                          'Easter Monday',                'NATIONAL'),
    ('2029-05-01', 'A munka ünnepe',                       'Labour Day',                   'NATIONAL'),
    ('2029-05-21', 'Pünkösdhétfő',                         'Whit Monday',                  'NATIONAL'),
    ('2029-08-20', 'Az államalapítás ünnepe',              'St. Stephen''s Day',           'NATIONAL'),
    ('2029-10-23', 'Az 1956-os forradalom ünnepe',         '1956 Revolution Day',          'NATIONAL'),
    ('2029-11-01', 'Mindenszentek',                        'All Saints'' Day',             'NATIONAL'),
    ('2029-12-25', 'Karácsony első napja',                 'Christmas Day',                'NATIONAL'),
    ('2029-12-26', 'Karácsony második napja',              'Second Day of Christmas',      'NATIONAL'),
    -- 2030
    ('2030-01-01', 'Újév',                                'New Year''s Day',              'NATIONAL'),
    ('2030-03-15', '1848-as forradalom ünnepe',            'National Day',                 'NATIONAL'),
    ('2030-04-19', 'Nagypéntek',                           'Good Friday',                  'NATIONAL'),
    ('2030-04-22', 'Húsvéthétfő',                          'Easter Monday',                'NATIONAL'),
    ('2030-05-01', 'A munka ünnepe',                       'Labour Day',                   'NATIONAL'),
    ('2030-06-10', 'Pünkösdhétfő',                         'Whit Monday',                  'NATIONAL'),
    ('2030-08-20', 'Az államalapítás ünnepe',              'St. Stephen''s Day',           'NATIONAL'),
    ('2030-10-23', 'Az 1956-os forradalom ünnepe',         '1956 Revolution Day',          'NATIONAL'),
    ('2030-11-01', 'Mindenszentek',                        'All Saints'' Day',             'NATIONAL'),
    ('2030-12-25', 'Karácsony első napja',                 'Christmas Day',                'NATIONAL'),
    ('2030-12-26', 'Karácsony második napja',              'Second Day of Christmas',      'NATIONAL')
ON CONFLICT (holiday_date) DO NOTHING;


-- =============================================================================
--  VIEW: v_mapping_coverage
--  Active mapping coverage % per entity.
--  Surfaced on Controller Ops dashboard and go-live readiness checks.
--  Platform should not go live until approved_coverage_pct >= 95 for all entities.
-- =============================================================================

CREATE OR REPLACE VIEW config.v_mapping_coverage AS
SELECT
    e.entity_code,
    e.entity_name,
    COUNT(*)                                                        AS total_accounts,
    SUM(CASE WHEN m.review_status = 'APPROVED' THEN 1 ELSE 0 END)  AS approved_mappings,
    SUM(CASE WHEN m.review_status = 'PENDING'  THEN 1 ELSE 0 END)  AS pending_mappings,
    SUM(CASE WHEN m.review_status = 'DISPUTED' THEN 1 ELSE 0 END)  AS disputed_mappings,
    ROUND(
        SUM(CASE WHEN m.review_status = 'APPROVED' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                               AS approved_coverage_pct
FROM config.ref_coa_mapping m
JOIN config.ref_entity_master e ON m.entity_id = e.entity_id
WHERE m.is_active = TRUE
  AND (m.valid_to IS NULL OR m.valid_to >= CURRENT_DATE)
GROUP BY e.entity_code, e.entity_name
ORDER BY approved_coverage_pct ASC;

COMMENT ON VIEW config.v_mapping_coverage IS 'Real-time mapping coverage KPI per entity. Surfaced on the Controller Ops dashboard and in go-live readiness checks. Platform should not go live until approved_coverage_pct >= 95 for all entities.';

-- =============================================================================
--  2.11 RLS USER-ENTITY MAP
-- =============================================================================

CREATE TABLE IF NOT EXISTS config.rls_user_entity_map (
    rls_map_id               BIGSERIAL       PRIMARY KEY,
    user_id                  VARCHAR(256)    NOT NULL,               -- AAD UPN or object id
    entity_id                UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    entity_code              VARCHAR(20)     NOT NULL,
    access_role              VARCHAR(50)     NOT NULL DEFAULT 'EntityManager',
    granted_by               VARCHAR(100)    NOT NULL DEFAULT 'system-seed',
    granted_at               TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    revoked_at               TIMESTAMPTZ,
    is_active                BOOLEAN         NOT NULL DEFAULT TRUE,
    UNIQUE (user_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_rls_user_active
    ON config.rls_user_entity_map (user_id, is_active)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_rls_entity_active
    ON config.rls_user_entity_map (entity_code, is_active)
    WHERE is_active = TRUE;

COMMENT ON TABLE config.rls_user_entity_map IS
    RLS mapping between users and legal entities. Canonical business key is entity_code; 
    'entity_id is retained as FK for referential integrity.';

-- Seed deterministic baseline mappings from entity data_owner where available.
INSERT INTO config.rls_user_entity_map (user_id, entity_id, entity_code, access_role, granted_by)
SELECT
    lower(trim(e.data_owner)) AS user_id,
    e.entity_id,
    e.entity_code,
    'EntityManager'           AS access_role,
    'bootstrap-from-entity-master'
FROM config.ref_entity_master e
WHERE e.is_active = TRUE
  AND e.data_owner IS NOT NULL
  AND trim(e.data_owner) <> ''
ON CONFLICT (user_id, entity_id) DO NOTHING;


-- =============================================================================
--  END OF fip_schema_config.sql
-- =============================================================================
_part_1_of_2_# Financial Intelligence Platform — Conversational Q&A Agent (RAG + Text-to-SQL)
===============================================================================
Part 5.4 of the Master Architecture Guide · HU GAAP

Architecture: Semantic Layer → Intent Classification → Text-to-SQL → Execution → Formatting

Pipeline:
  1. Classify user intent: kpi_lookup | trend_analysis | variance | drill_down
  2. Retrieve schema context from vector store (Azure Cognitive Search)
  3. Generate SQL via Azure OpenAI, constrained to Azure Synapse T-SQL dialect
  4. Apply RLS security filter — users see only their authorised entities
  5. Validate and execute SQL against Synapse
  6. Format response with LLM — ALWAYS include the generated SQL (trust building)

Key design principle (Master Guide 5.4):
  Always display the generated SQL query alongside the result to build trust
  and enable validation by finance professionals

Usage:
    # As API server (FastAPI)
    uvicorn financial_qa_agent:app --host 0.0.0.0 --port 8000

    # As CLI
    python financial_qa_agent.py --query "Mi volt az EBITDA margin Q1 2026-ban?"

Dependencies: openai, fastapi, uvicorn, pandas, pyodbc, azure-search-documents,
              azure-identity, pydantic

import argparse
import json
import logging
import os
import re
import sys
from typing import Any, Optional

import pandas as pd
import pyodbc
from azure.identity import ManagedIdentityCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from openai import AzureOpenAI
from pydantic import BaseModel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from db_utils import get_db_connection, get_openai_client

try:
    from fastapi import FastAPI, HTTPException, Security
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

-- ---------------------------------------------------------------------------
-- Configuration
-- ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FIP.FinancialQAAgent")

AZURE_OPENAI_ENDPOINT    = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_DEPLOYMENT  = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
AZURE_SEARCH_ENDPOINT    = os.getenv("AZURE_SEARCH_ENDPOINT", "")
AZURE_SEARCH_INDEX       = os.getenv("AZURE_SEARCH_INDEX", "fip-schema-index")
SYNAPSE_SERVER           = os.getenv("SYNAPSE_SERVER", "")
SYNAPSE_DATABASE         = os.getenv("SYNAPSE_DATABASE", "fip_dw")
MAX_RESULT_ROWS          = int(os.getenv("MAX_RESULT_ROWS", "500"))
QUERY_TIMEOUT_SECONDS    = int(os.getenv("QUERY_TIMEOUT_SECONDS", "30"))

# Intent classification categories
INTENT_CLASSES = ["kpi_lookup", "trend_analysis", "variance", "drill_down", "unknown"]

# SQL injection protection: blocked keywords
SQL_BLOCKED_KEYWORDS = [
    "DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "EXEC", "EXECUTE", "xp_", "sp_", "GRANT", "REVOKE", "OPENROWSET"
]


-- ---------------------------------------------------------------------------
-- Data models
-- ---------------------------------------------------------------------------

class QARequest(BaseModel):
    query: str
    user_id: str
    entity_code: Optional[str] = None
    language: str = "en"


class QAResponse(BaseModel):
    question: str
    answer: str
    generated_sql: str
    result_data: Optional[list[dict]] = None
    intent: str
    row_count: int
    warning: Optional[str] = None


def get_search_client() -> SearchClient:
    credential = ManagedIdentityCredential()
    return SearchClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        index_name=AZURE_SEARCH_INDEX,
        credential=credential
    )


-- ---------------------------------------------------------------------------
-- Step 1: Intent Classification
-- ---------------------------------------------------------------------------

def classify_intent(client: AzureOpenAI, user_query: str) -> str:
    
    Classify the user's financial question into one of four intent categories.
    Returns: 'kpi_lookup' | 'trend_analysis' | 'variance' | 'drill_down' | 'unknown'
    
    classification_prompt = "You are an intent classifier for a financial analytics platform.
Classify the users question into exactly one category':

- kpi_lookup: User wants a specific KPI value for a specific period (e.g., "What was EBITDA in Q3?")
- trend_analysis: User wants to see a KPI over multiple periods (e.g., "Show revenue trend for 2025")
- variance: User wants to compare actuals vs budget or vs prior year (e.g., "Why did costs increase?")
- drill_down: User wants to go from summary to detail level (e.g., "Show me the top cost centres")
- unknown: Query is not about financial data or cannot be answered by the system

Respond with ONLY the category name, nothing else.

    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": classification_prompt},
            {"role": "user",   "content": user_query}
        ],
        temperature=0,
        max_tokens=20
    )
    intent = response.choices[0].message.content.strip().lower()
    return intent if intent in INTENT_CLASSES else "unknown"


-- ---------------------------------------------------------------------------
-- Step 2: Schema Context Retrieval (Vector Search)
-- ---------------------------------------------------------------------------

def retrieve_schema_context(search_client: SearchClient, openai_client: AzureOpenAI,
                             user_query: str, k: int = 5) -> str:
    ""
    Retrieve relevant schema documentation from Azure Cognitive Search vector index.
    The index contains table descriptions, column definitions, and HU GAAP terminology.
    ""
    # Generate embedding for semantic search
    embed_response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=user_query
    )
    query_vector = embed_response.data[0].embedding

    vector_query = VectorizedQuery(
        vector=query_vector,
        k_nearest_neighbors=k,
        fields="content_vector"
    )

    results = search_client.search(
        search_text=user_query,
        vector_queries=[vector_query],
        select=["table_name", "column_name", "description", "hu_gaap_mapping", "example_values"],
        top=k
    )

    context_parts = []
    for result in results:
        context_parts.append(
            fTable: {result.get('table_name', '')} 
            fColumn: {result.get('column_name', '')} 
            fDescription: {result.get('description', '')} 
            fHU GAAP: {result.get('hu_gaap_mapping', '')} 
            fExamples: {result.get('example_values', '')}
        )

    return "\n".join(context_parts)


--  ---------------------------------------------------------------------------
-- Step 3: SQL Generation
-- ---------------------------------------------------------------------------

def get_rls_clause(user_id: str, entity_code: Optional[str]) -> tuple[str, list]:
    
    Build the Row-Level Security filter clause with parameterised values.
    Users are restricted to entities they are authorised for in config.rls_user_entity_map.
    This clause is injected into every generated SQL — cannot be bypassed.

    Returns:
        tuple: (rls_sql_clause, parameter_values) — use parameterised query execution
    
    if entity_code:
        # If a specific entity is requested, still validate user has access
        return (
            ""AND e.entity_code IN (
                SELECT entity_code FROM config.rls_user_entity_map
                WHERE user_id = ?
                  AND entity_code = ?
                  AND is_active = TRUE
            )"",

-- CREATE TABLE config.rls_user_entity_map (Row-Level Security User-Entity Mapping)
CREATE TABLE IF NOT EXISTS config.rls_user_entity_map (
    user_id VARCHAR(100) NOT NULL, -- User identifier (e.g., email, Azure AD OID)
    entity_code VARCHAR(20) NOT NULL, -- Canonical entity code
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (user_id, entity_code)
);

COMMENT ON TABLE config.rls_user_entity_map IS 'Maps users to entities for Row-Level Security (RLS) enforcement in Power BI and NLQ agents.';
COMMENT ON COLUMN config.rls_user_entity_map.user_id IS 'Identifier of the user (e.g., Azure AD Object ID or email address).';
COMMENT ON COLUMN config.rls_user_entity_map.entity_code IS 'The canonical business code of the entity the user has access to.';
COMMENT ON COLUMN config.rls_user_entity_map.is_active IS 'Indicates if the mapping is currently active.';
COMMENT ON COLUMN config.rls_user_entity_map.created_at IS 'Timestamp when the mapping was created.';
COMMENT ON COLUMN config.rls_user_entity_map.updated_at IS 'Timestamp when the mapping was last updated.';
