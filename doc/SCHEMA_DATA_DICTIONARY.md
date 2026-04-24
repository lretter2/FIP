# Financial Intelligence Platform (FIP) — Schema & Data Dictionary

**Version:** 1.0  
**Date:** 2026-04-17  
**Classification:** Internal — Data Engineering

---

## Table of Contents

1. [Overview](#overview)
2. [Schema: config](#schema-config)
3. [Schema: audit](#schema-audit)
4. [Schema: bronze](#schema-bronze)
5. [Schema: silver](#schema-silver)
6. [Schema: budget](#schema-budget)
7. [Schema: gold](#schema-gold)
8. [Column Naming Conventions](#column-naming-conventions)

---

## Overview

The Financial Intelligence Platform uses six logical schemas arranged in a medallion architecture:

| Schema | Layer | Purpose |
|--------|-------|---------|
| `config` | Reference | Master data, configuration tables, fiscal calendars, chart of accounts |
| `audit` | Cross-cutting | Pipeline audit trails, DQ logs, quarantine, alerts, AI commentary queue |
| `bronze` | Raw | Ingestion manifests tracking all raw file arrivals |
| `silver` | Conformed | Cleansed, typed, DQ-passed dimensional and fact tables |
| `budget` | Planning | Budget versions and forecast fact tables |
| `gold` | Presentation | Aggregated KPIs, reporting views, variance analysis |

**Terminology standards (platform-wide):**
- Entity identifier columns: `entity_code` (VARCHAR(20), human-readable) or `entity_id` (UUID, FK to config.ref_entity_master)
- Integer surrogate keys in silver: `entity_key`, `account_key`, `cost_centre_key`, etc.
- `period_key` = INT in YYYYMM format used as PK component in Gold aggregation tables
- `period_id` = INT in YYYYMM format used in audit, config, and budget tables (non-PK context)
- `balance_check` = generated column in `gold.agg_balance_sheet` (NOT `is_balanced`)

---

## Schema: config

The `config` schema holds all reference/master data that drives the platform's business logic. Written to by data stewards, ERP integrations, and the NBH FX rate loader pipeline.

---

### config.ref_entity_master

**Purpose:** The single source of truth for all legal entities (companies, subsidiaries) tracked by FIP. Every other table that references an entity uses `entity_id` (UUID FK) or `entity_code` (VARCHAR(20)) from this table.

**Written by:** Data stewards via admin UI; never overwritten by pipelines.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `entity_id` | UUID | NOT NULL | PK | Immutable surrogate identifier for the legal entity | — |
| `entity_code` | VARCHAR(20) | NOT NULL | UNIQUE | Human-readable short code (e.g., `HU_MAIN`, `SK_SUB01`) used in gold layer inline columns and all application queries | Gazdasági egység azonosító |
| `entity_name` | VARCHAR(200) | NOT NULL | — | Full legal name of the entity | Teljes cégnév |
| `entity_name_short` | VARCHAR(50) | NULL | — | Abbreviated display name for reports | Rövidített név |
| `legal_entity_type` | VARCHAR(50) | NULL | — | Legal form (Kft., Zrt., Bt., etc.) | Gazdálkodó szervezet típusa |
| `tax_id` | VARCHAR(30) | NULL | — | Hungarian tax number (adószám) or foreign equivalent | Adószám |
| `registration_number` | VARCHAR(50) | NULL | — | Court/company registration number | Cégjegyzékszám |
| `country_code` | CHAR(2) | NOT NULL | — | ISO 3166-1 alpha-2. DEFAULT `'HU'` | Székhely országa |
| `reporting_currency` | CHAR(3) | NOT NULL | — | Primary reporting currency. DEFAULT `'HUF'` | Könyvelési pénznem |
| `functional_currency` | CHAR(3) | NULL | — | Functional currency if different from reporting currency | Funkcionális pénznem |
| `gaap_basis` | VARCHAR(20) | NOT NULL | — | Accounting standard. DEFAULT `'HU_GAAP'`. Also: `IFRS`, `US_GAAP` | Számviteli alap |
| `fiscal_year_start_month` | INT | NOT NULL | — | Month number (1–12) when fiscal year begins. DEFAULT `1` | Üzleti év kezdő hónapja |
| `consolidation_group` | VARCHAR(100) | NULL | — | Consolidation group name for group reporting | Konszolidációs csoport |
| `parent_entity_id` | UUID | NULL | FK → self | Self-referencing FK to parent entity for hierarchy | Anyavállalat |
| `consolidation_method` | VARCHAR(20) | NOT NULL | — | DEFAULT `'FULL'`. Values: `FULL`, `PROPORTIONAL`, `EQUITY`, `NONE` | Konszolidációs módszer |
| `is_active` | BOOLEAN | NOT NULL | — | FALSE = entity is deactivated (historical data preserved) | — |
| `data_owner` | VARCHAR(100) | NULL | — | Name/email of the responsible data owner | — |
| `erp_system` | VARCHAR(50) | NULL | — | Source ERP system name (e.g., `SAP`, `NAVISION`, `KULCS`) | — |
| `erp_company_code` | VARCHAR(20) | NULL | — | Company code within the ERP system | ERP vállalatkód |
| `created_at` | TIMESTAMPTZ | NOT NULL | — | Record creation timestamp | — |
| `updated_at` | TIMESTAMPTZ | NOT NULL | — | Last update timestamp (trigger-managed) | — |

**Key Indexes:**
- `PK_ref_entity_master` on `entity_id`
- `UQ_ref_entity_master_code` on `entity_code`
- `IX_ref_entity_master_active` on `is_active` (partial: WHERE `is_active = TRUE`)

---

### config.ref_currencies

**Purpose:** Reference list of all currencies used across the platform. Drives FX rate lookup and display formatting.

**Written by:** One-time seed data load; updated by data stewards only.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `currency_id` | SERIAL | NOT NULL | PK | Auto-increment surrogate key | — |
| `currency_code` | CHAR(3) | NOT NULL | UNIQUE | ISO 4217 currency code (e.g., `HUF`, `EUR`, `USD`) | Pénznem kód |
| `currency_name` | VARCHAR(100) | NOT NULL | — | Currency name in English | Pénznem neve (EN) |
| `currency_name_hu` | VARCHAR(100) | NULL | — | Currency name in Hungarian | Pénznem neve (HU) |
| `currency_symbol` | VARCHAR(5) | NULL | — | Display symbol (e.g., `Ft`, `€`, `$`) | Pénznemjel |
| `decimal_places` | INT | NOT NULL | — | Number of decimal places. DEFAULT `2`. (0 for JPY, etc.) | — |
| `is_active` | BOOLEAN | NOT NULL | — | Whether this currency is currently in use | — |
| `is_reporting_currency` | BOOLEAN | NOT NULL | — | TRUE for HUF (platform reporting currency) | — |
| `nbh_feed_code` | VARCHAR(10) | NULL | — | National Bank of Hungary (NBH) feed identifier for FX rate download | MNB árfolyam kód |

**Key Indexes:**
- `PK_ref_currencies` on `currency_id`
- `UQ_ref_currencies_code` on `currency_code`

---

### config.ref_fx_rates

**Purpose:** Daily official FX rates sourced from the National Bank of Hungary (MNB/NBH). Used for all HUF conversion calculations in silver and gold layers. The `tr_nbh_fx_rates_daily` ADF pipeline loads this at 09:00 CET.

**Written by:** ADF pipeline `tr_nbh_fx_rates_daily` (daily at 09:00 CET).

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `rate_id` | BIGSERIAL | NOT NULL | PK | Auto-increment surrogate key | — |
| `rate_date` | DATE | NOT NULL | — | The date for which the rate applies | Árfolyam dátuma |
| `currency_code` | CHAR(3) | NOT NULL | FK → ref_currencies | ISO 4217 currency code | Pénznem kód |
| `rate_to_huf` | NUMERIC(16,6) | NOT NULL | — | Exchange rate: 1 unit of `currency_code` = N HUF | MNB középárfolyam |
| `rate_source` | VARCHAR(20) | NOT NULL | — | Source of the rate. DEFAULT `'NBH'` | Forrás |
| `is_official` | BOOLEAN | NOT NULL | — | TRUE = official NBH published rate; FALSE = estimated/interpolated | Hivatkos árfolyam |
| `ingested_at` | TIMESTAMPTZ | NOT NULL | — | Timestamp when rate was loaded into the platform | Betöltés időpontja |

**Constraints:**
- `UNIQUE(rate_date, currency_code)` — one rate per currency per day

**Key Indexes:**
- `PK_ref_fx_rates` on `rate_id`
- `UQ_ref_fx_rates_date_ccy` on `(rate_date, currency_code)`
- `IX_ref_fx_rates_date` on `rate_date DESC` (for latest-rate lookups)

---

### config.ref_fiscal_calendar

**Purpose:** Defines fiscal period boundaries for each entity. Supports entities with non-calendar fiscal years. Tracks period close status. Used by all pipelines to determine valid load windows.

**Written by:** Annual calendar generation script (run by data engineers); `closed_at`/`closed_by` updated by close management process.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `fiscal_cal_id` | SERIAL | NOT NULL | PK | Auto-increment surrogate key | — |
| `entity_id` | UUID | NOT NULL | FK → ref_entity_master | Entity this calendar record belongs to | — |
| `fiscal_year` | INT | NOT NULL | — | Fiscal year number (e.g., 2025) | Üzleti év |
| `period_num` | INT | NOT NULL | — | Period number within fiscal year (1–12) | Periódus sorszáma |
| `calendar_year` | INT | NOT NULL | — | Calendar year of this period | Naptári év |
| `calendar_month` | INT | NOT NULL | — | Calendar month number (1–12) | Naptári hónap |
| `period_start_date` | DATE | NOT NULL | — | First date of the period | Periódus kezdete |
| `period_end_date` | DATE | NOT NULL | — | Last date of the period | Periódus vége |
| `period_label` | VARCHAR(20) | NOT NULL | — | Display label (e.g., `2025-M03`) | Periódus jelölés |
| `is_closed` | BOOLEAN | NOT NULL | — | TRUE = period is closed; no further postings allowed | Lezárt időszak |
| `closed_at` | TIMESTAMPTZ | NULL | — | Timestamp when period was closed | Lezárás időpontja |
| `closed_by` | VARCHAR(100) | NULL | — | User who closed the period | Lezárta |

**Constraints:**
- `UNIQUE(entity_id, fiscal_year, period_num)`

**Key Indexes:**
- `PK_ref_fiscal_calendar` on `fiscal_cal_id`
- `UQ_ref_fiscal_calendar` on `(entity_id, fiscal_year, period_num)`
- `IX_ref_fiscal_calendar_entity_open` on `(entity_id, is_closed)` WHERE `is_closed = FALSE`

---

### config.ref_coa_mapping

**Purpose:** Chart of accounts mapping table. Maps each entity's local ERP account codes to the platform's universal account hierarchy (`universal_node`). Supports time-validity via `valid_from`/`valid_to`. Mapping must be reviewed and approved before transactions flow through.

**Written by:** Data stewards / finance team via mapping UI; review workflow managed by `review_status` lifecycle.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `mapping_id` | SERIAL | NOT NULL | PK | Auto-increment surrogate key | — |
| `entity_id` | UUID | NOT NULL | FK → ref_entity_master | Entity this mapping applies to | — |
| `local_account_code` | VARCHAR(50) | NOT NULL | — | Account code as defined in the entity's ERP | Helyi főkönyvi számla kód |
| `local_account_name` | VARCHAR(200) | NULL | — | Account name in the entity's ERP | Helyi számla megnevezése |
| `universal_node` | VARCHAR(100) | NOT NULL | — | Platform universal account hierarchy node | Egységesített számla csomópont |
| `account_type` | VARCHAR(20) | NOT NULL | — | `ASSET`, `LIABILITY`, `EQUITY`, `REVENUE`, `EXPENSE` | Számla típusa |
| `normal_balance` | CHAR(1) | NOT NULL | — | `D` (debit) or `C` (credit) — normal side of the account | Normál egyenleg oldal |
| `l1_category` | VARCHAR(100) | NULL | — | Level-1 reporting category (e.g., `REVENUE`, `OPEX`) | L1 kategória |
| `l2_subcategory` | VARCHAR(100) | NULL | — | Level-2 subcategory | L2 alkategória |
| `l3_detail` | VARCHAR(100) | NULL | — | Level-3 detail classification | L3 részletezés |
| `pl_line_item` | VARCHAR(100) | NULL | — | P&L statement line item label | E&B sor megnevezése |
| `cf_classification` | VARCHAR(50) | NULL | — | Cash flow classification: `OPERATING`, `INVESTING`, `FINANCING` | Pénzáramlás típusa |
| `is_controlling` | BOOLEAN | NOT NULL | — | TRUE = account used in controlling/cost centre reporting | Controlling számla |
| `is_intercompany` | BOOLEAN | NOT NULL | — | TRUE = intercompany elimination candidate | Csoportközi számla |
| `is_active` | BOOLEAN | NOT NULL | — | FALSE = mapping inactive (superseded by newer record) | — |
| `valid_from` | DATE | NOT NULL | — | Date from which this mapping is effective | Érvényesség kezdete |
| `valid_to` | DATE | NULL | — | Date until which this mapping is effective (NULL = open-ended) | Érvényesség vége |
| `mapping_rationale` | TEXT | NULL | — | Explanation of why this local account maps to this universal node | — |
| `reviewed_by` | VARCHAR(100) | NULL | — | Name/email of the reviewer | Felülvizsgálta |
| `reviewed_at` | TIMESTAMPTZ | NULL | — | Timestamp of review | Felülvizsgálat időpontja |
| `review_status` | VARCHAR(20) | NOT NULL | — | `PENDING`, `APPROVED`, `DISPUTED`. DEFAULT `'PENDING'` | Felülvizsgálat státusza |

**Constraints:**
- `UNIQUE(entity_id, local_account_code, valid_from)`

**Key Indexes:**
- `PK_ref_coa_mapping` on `mapping_id`
- `UQ_ref_coa_mapping` on `(entity_id, local_account_code, valid_from)`
- `IX_ref_coa_mapping_approved` on `(entity_id, is_active, review_status)` WHERE `review_status = 'APPROVED'`

---

### config.ref_cost_centre_master

**Purpose:** Master list of cost centres for each entity. Supports hierarchical cost centre trees via self-referencing `parent_cost_centre_id`. Drives cost centre reporting and budget centre classification.

**Written by:** Data stewards / HR/Finance integration.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `cost_centre_id` | UUID | NOT NULL | PK | Immutable surrogate identifier | — |
| `entity_id` | UUID | NOT NULL | FK → ref_entity_master | Owning entity | — |
| `cost_centre_code` | VARCHAR(30) | NOT NULL | — | Short code for the cost centre (e.g., `CC-SALES-HU`) | Költséghely kód |
| `cost_centre_name` | VARCHAR(200) | NOT NULL | — | Hungarian name | Költséghely megnevezése |
| `cost_centre_name_en` | VARCHAR(200) | NULL | — | English name | Költséghely neve (EN) |
| `manager_name` | VARCHAR(100) | NULL | — | Responsible manager name | Felelős vezető |
| `manager_email` | VARCHAR(200) | NULL | — | Manager email address | Felelős vezető email |
| `business_unit` | VARCHAR(100) | NULL | — | Business unit grouping | Üzleti egység |
| `division` | VARCHAR(100) | NULL | — | Division | Divízió |
| `region` | VARCHAR(50) | NULL | — | Geographic region | Régió |
| `is_budget_centre` | BOOLEAN | NOT NULL | — | TRUE = budgets can be assigned to this cost centre | Tervezési egység |
| `is_profit_centre` | BOOLEAN | NOT NULL | — | TRUE = P&L is tracked at this cost centre | Eredményközpont |
| `parent_cost_centre_id` | UUID | NULL | FK → self | Parent in the cost centre hierarchy | Szülő költséghely |
| `erp_cost_centre_code` | VARCHAR(30) | NULL | — | Cost centre code in the source ERP | ERP költséghely kód |
| `is_active` | BOOLEAN | NOT NULL | — | FALSE = deactivated | — |
| `valid_from` | DATE | NOT NULL | — | Effective from date | Érvényes ettől |
| `valid_to` | DATE | NULL | — | Effective to date (NULL = open-ended) | Érvényes eddig |

**Constraints:**
- `UNIQUE(entity_id, cost_centre_code)`

---

### config.ref_project_master

**Purpose:** Master list of projects for project-level cost tracking. Linked to cost centres and budget amounts. Drives project-level variance analysis.

**Written by:** Data stewards / project management integration.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `project_id` | UUID | NOT NULL | PK | Immutable surrogate identifier | — |
| `entity_id` | UUID | NOT NULL | FK → ref_entity_master | Owning entity | — |
| `project_code` | VARCHAR(30) | NOT NULL | — | Short project code | Projekt kód |
| `project_name` | VARCHAR(200) | NOT NULL | — | Project name | Projekt megnevezése |
| `project_type` | VARCHAR(20) | NOT NULL | — | `CAPEX`, `OPEX`, `CLIENT`, `INTERNAL`, `RD` | Projekt típusa |
| `wbs_element` | VARCHAR(50) | NULL | — | WBS element code (for SAP-based entities) | WBS elem |
| `cost_centre_id` | UUID | NULL | FK → ref_cost_centre_master | Primary cost centre | Elsődleges költséghely |
| `project_manager` | VARCHAR(100) | NULL | — | Project manager name | Projektmenedzser |
| `client_name` | VARCHAR(200) | NULL | — | Client name (for CLIENT type projects) | Ügyfél neve |
| `budget_amount` | NUMERIC(18,2) | NULL | — | Total project budget in reporting currency (HUF) | Projekt büdzsé |
| `project_start_date` | DATE | NULL | — | Planned or actual start date | Projekt kezdete |
| `project_end_date` | DATE | NULL | — | Planned or actual end date | Projekt vége |
| `project_status` | VARCHAR(20) | NOT NULL | — | DEFAULT `'ACTIVE'`. Values: `ACTIVE`, `COMPLETED`, `CANCELLED`, `ON_HOLD` | Projekt státusza |

**Constraints:**
- `UNIQUE(entity_id, project_code)`

---

### config.ref_intercompany_pairs

**Purpose:** Defines known intercompany transaction pairs for elimination in group consolidation. Maps seller-entity accounts to buyer-entity accounts for each transaction type.

**Written by:** Data stewards during consolidation setup.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `pair_id` | SERIAL | NOT NULL | PK | Auto-increment surrogate key | — |
| `seller_entity_id` | UUID | NOT NULL | FK → ref_entity_master | The selling/invoicing entity | Eladó gazdálkodó |
| `buyer_entity_id` | UUID | NOT NULL | FK → ref_entity_master | The buying/receiving entity | Vevő gazdálkodó |
| `seller_account_code` | VARCHAR(50) | NOT NULL | — | Revenue account in seller entity | Eladó bevételi számla |
| `buyer_account_code` | VARCHAR(50) | NOT NULL | — | Cost/expense account in buyer entity | Vevő ráfordítás számla |
| `transaction_type` | VARCHAR(50) | NOT NULL | — | Type of intercompany transaction (e.g., `MANAGEMENT_FEE`, `LOAN_INTEREST`, `GOODS`) | Ügylet típusa |
| `elimination_type` | VARCHAR(30) | NOT NULL | — | Elimination method: `FULL`, `PROPORTIONAL` | Eliminációs módszer |

**Constraints:**
- `UNIQUE(seller_entity_id, buyer_entity_id, transaction_type)`

---

### config.ref_alert_rules

**Purpose:** Configuration for automated KPI alert rules. Defines thresholds, recipients, and notification channels for the alert engine. Alert instances are written to `audit.alert_log`.

**Written by:** Data stewards / platform administrators after CFO approval.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `rule_id` | SERIAL | NOT NULL | PK | Auto-increment surrogate key | — |
| `rule_code` | VARCHAR(50) | NOT NULL | UNIQUE | Human-readable rule code (e.g., `RULE-LIQ-001`) | — |
| `kpi_name` | VARCHAR(100) | NOT NULL | — | Name of the KPI being monitored | — |
| `entity_scope` | VARCHAR(20) | NOT NULL | — | Entity scope. DEFAULT `'ALL'`. Or a specific `entity_code` | — |
| `operator` | VARCHAR(5) | NOT NULL | — | Comparison operator: `<`, `>`, `<=`, `>=`, `=`, `!=` | — |
| `threshold_value` | NUMERIC(18,4) | NOT NULL | — | Alert threshold value | — |
| `threshold_unit` | VARCHAR(20) | NOT NULL | — | DEFAULT `'ABSOLUTE'`. Also: `PERCENT`, `RATIO` | — |
| `severity` | VARCHAR(10) | NOT NULL | — | `HIGH`, `MEDIUM`, `LOW` | — |
| `alert_title` | VARCHAR(200) | NOT NULL | — | Alert notification title | — |
| `recipient_roles` | TEXT | NULL | — | Comma-separated roles to notify (e.g., `CFO,CONTROLLER`) | — |
| `notification_channels` | VARCHAR(100) | NOT NULL | — | DEFAULT `'EMAIL'`. Also: `TEAMS`, `EMAIL,TEAMS` | — |
| `cooldown_hours` | INT | NOT NULL | — | Minimum hours between repeat notifications. DEFAULT `24` | — |
| `is_active` | BOOLEAN | NOT NULL | — | FALSE = rule is disabled | — |
| `created_by` | VARCHAR(100) | NOT NULL | — | User who created this rule | — |
| `approved_by` | VARCHAR(100) | NULL | — | Approver of this alert configuration | — |
| `created_at` | TIMESTAMPTZ | NOT NULL | — | Record creation timestamp | — |

---

### config.ref_hu_public_holidays

**Purpose:** Official Hungarian public holiday calendar. Used by silver.dim_date to populate `is_hungarian_public_holiday`. Also used in trading day calculations.

**Written by:** Annual load script based on official government announcements.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `holiday_date` | DATE | NOT NULL | PK | Date of the public holiday | Ünnepnap dátuma |
| `holiday_name` | VARCHAR(100) | NOT NULL | — | Holiday name in Hungarian | Magyar ünnepnap megnevezése |
| `holiday_name_en` | VARCHAR(100) | NULL | — | Holiday name in English | — |
| `holiday_type` | VARCHAR(20) | NOT NULL | — | DEFAULT `'NATIONAL'`. Values: `NATIONAL`, `SUBSTITUTION` | Ünnep típusa |

---

### config.v_mapping_coverage (View)

**Purpose:** Operational view showing the percentage of active local account codes that have an approved mapping for each entity. Used by data stewards to track mapping completeness before period close.

**Definition:** Aggregates `config.ref_coa_mapping` grouped by `entity_id` showing counts of `APPROVED`, `PENDING`, `DISPUTED` mappings vs. total accounts in `silver.account_master`.

**Written by:** Read-only view; no direct writes.

---

## Schema: audit

The `audit` schema captures the complete operational audit trail of the platform. Written by pipeline orchestration, DQ procedures, and the AI commentary engine. Never deleted (permanent retention per HU accounting law).

---

### audit.batch_log

**Purpose:** One row per pipeline execution (batch). Tracks end-to-end pipeline health from extraction through silver load. The primary traceability record for every data load.

**Written by:** ADF pipelines and Databricks jobs at pipeline start (status=`RUNNING`) and completion.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `batch_id` | UUID | NOT NULL | PK | Unique batch identifier. DEFAULT `gen_random_uuid()` | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity being loaded | — |
| `pipeline_name` | VARCHAR(200) | NOT NULL | — | ADF/Databricks pipeline name | — |
| `pipeline_run_id` | VARCHAR(200) | NULL | — | ADF pipeline run GUID for cross-referencing | — |
| `source_system` | VARCHAR(50) | NOT NULL | — | Source ERP/system name (e.g., `SAP`, `NAVISION`) | — |
| `source_file_path` | TEXT | NULL | — | Full ADLS Gen2 path of the source file | — |
| `source_file_hash` | CHAR(64) | NULL | — | SHA-256 hash of source file for deduplication | — |
| `source_file_row_count` | INT | NULL | — | Row count in the source file | — |
| `source_system_version` | VARCHAR(50) | NULL | — | Version/release of the source ERP at time of extract | — |
| `period_id` | INT | NULL | — | YYYYMM period identifier for the data being loaded | — |
| `fiscal_year` | INT | NULL | — | Fiscal year of the data | — |
| `pipeline_stage` | VARCHAR(50) | NOT NULL | — | Current stage: `EXTRACT`, `VALIDATE`, `TRANSFORM`, `LOAD` | — |
| `status` | VARCHAR(20) | NOT NULL | — | DEFAULT `'RUNNING'`. Values: `RUNNING`, `SUCCESS`, `FAILED`, `PARTIAL` | — |
| `started_at` | TIMESTAMPTZ | NOT NULL | — | Pipeline start timestamp | — |
| `completed_at` | TIMESTAMPTZ | NULL | — | Pipeline completion timestamp (NULL if still running) | — |
| `duration_seconds` | INT | NULL | GENERATED STORED | Computed: `EXTRACT(EPOCH FROM (completed_at - started_at))` | — |
| `rows_extracted` | INT | NULL | — | Total rows read from source | — |
| `rows_passed_dq` | INT | NULL | — | Rows that passed all DQ checks | — |
| `rows_quarantined` | INT | NULL | — | Rows sent to `audit.quarantine` | — |
| `rows_loaded_silver` | INT | NULL | — | Rows successfully loaded to silver | — |
| `error_message` | TEXT | NULL | — | Error details if status = `FAILED` | — |
| `triggered_by` | VARCHAR(50) | NOT NULL | — | DEFAULT `'SCHEDULE'`. Values: `SCHEDULE`, `MANUAL`, `EVENT` | — |
| `triggered_by_user` | VARCHAR(100) | NULL | — | User who triggered manual run (if applicable) | — |

**Key Indexes:**
- `PK_batch_log` on `batch_id`
- `IX_batch_log_entity_period` on `(entity_id, period_id, status)`
- `IX_batch_log_started_at` on `started_at DESC`

---

### audit.data_quality_log

**Purpose:** Records the result of every DQ rule execution for every batch. One row per rule per batch. Feeds the DQ dashboard and quarantine workflow.

**Written by:** Databricks DQ engine (fip_stored_procedures.sql DQ-001 through DQ-011).

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `dq_log_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `batch_id` | UUID | NOT NULL | FK → batch_log | Parent batch | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity being checked | — |
| `rule_id` | VARCHAR(10) | NOT NULL | — | DQ rule code (e.g., `DQ-001`, `DQ-007`) | — |
| `rule_name` | VARCHAR(200) | NOT NULL | — | Human-readable rule name | — |
| `rule_description` | TEXT | NULL | — | Full description of what this rule checks | — |
| `check_result` | VARCHAR(10) | NOT NULL | — | `PASS`, `FAIL`, `WARN`, `SKIP` | — |
| `action_taken` | VARCHAR(20) | NOT NULL | — | `NONE`, `BLOCK`, `QUARANTINE`, `ALERT`, `WARN` | — |
| `records_checked` | INT | NOT NULL | — | Total records evaluated by this rule | — |
| `records_failed` | INT | NOT NULL | — | Records that failed the rule | — |
| `failure_rate_pct` | NUMERIC(6,4) | NULL | GENERATED STORED | `(records_failed::NUMERIC / NULLIF(records_checked,0)) * 100` | — |
| `failure_detail` | JSONB | NULL | — | Sample failed records and failure reasons | — |
| `checked_at` | TIMESTAMPTZ | NOT NULL | — | Timestamp of rule execution | — |

---

### audit.quarantine

**Purpose:** Holds individual records that failed DQ checks and were excluded from silver. Each quarantined record preserves the raw source payload and the reason for failure. Records can be corrected and re-released.

**Written by:** DQ engine (Databricks); resolved by data stewards.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `quarantine_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `batch_id` | UUID | NOT NULL | FK → batch_log | Batch in which this record was quarantined | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity this record belongs to | — |
| `source_record` | JSONB | NOT NULL | — | Complete raw source record as JSON | — |
| `dq_rule_failed` | VARCHAR(10) | NOT NULL | — | The DQ rule code that this record failed (e.g., `DQ-004`) | — |
| `failure_reason` | TEXT | NOT NULL | — | Human-readable explanation of failure | — |
| `source_field` | VARCHAR(100) | NULL | — | Specific field that caused the failure | — |
| `source_value` | TEXT | NULL | — | The problematic value | — |
| `quarantined_at` | TIMESTAMPTZ | NOT NULL | — | Timestamp of quarantine | — |
| `resolution_status` | VARCHAR(20) | NOT NULL | — | DEFAULT `'OPEN'`. Values: `OPEN`, `RESOLVED`, `REJECTED`, `RELEASED` | — |
| `resolved_by` | VARCHAR(100) | NULL | — | User who resolved | — |
| `resolved_at` | TIMESTAMPTZ | NULL | — | Resolution timestamp | — |
| `resolution_notes` | TEXT | NULL | — | Explanation of how/why resolved | — |
| `corrected_record` | JSONB | NULL | — | The corrected version of the record (if fixed before re-release) | — |
| `released_to_batch_id` | UUID | NULL | FK → batch_log | Batch in which the corrected record was re-processed | — |

---

### audit.restatement_log

**Purpose:** Tracks financial restatements when late-arriving transactions or corrections require previously published figures to change. Mandatory audit trail for HU GAAP compliance. Triggers CFO notification workflow.

**Written by:** Restatement detection job (Databricks); CFO acknowledgment via UI.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `restatement_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Affected entity | — |
| `affected_period_id` | INT | NOT NULL | — | YYYYMM period being restated | Érintett pénzügyi időszak |
| `account_code` | VARCHAR(50) | NULL | — | Local account code (if single-account restatement) | Könyvelési számlaszám |
| `universal_node` | VARCHAR(100) | NULL | — | Universal account node affected | — |
| `original_amount_huf` | NUMERIC(18,2) | NOT NULL | — | Original published amount in HUF | Eredeti összeg (HUF) |
| `restated_amount_huf` | NUMERIC(18,2) | NOT NULL | — | Restated (corrected) amount in HUF | Korrigált összeg (HUF) |
| `delta_amount_huf` | NUMERIC(18,2) | NULL | GENERATED STORED | `restated_amount_huf - original_amount_huf` | Eltérés (HUF) |
| `delta_pct` | NUMERIC(10,4) | NULL | — | Percentage change | Eltérés (%) |
| `materiality_threshold` | NUMERIC(18,2) | NULL | — | Threshold used to determine materiality | Lényegességi határ |
| `triggering_batch_id` | UUID | NULL | FK → batch_log | Batch that caused the restatement | — |
| `late_entry_transaction_ids` | UUID[] | NULL | — | Array of source transaction UUIDs that arrived late | — |
| `restatement_reason` | TEXT | NOT NULL | — | Business reason for the restatement | Átdolgozás oka |
| `cfo_notified` | BOOLEAN | NOT NULL | — | TRUE = CFO notification email sent | — |
| `cfo_notification_sent_at` | TIMESTAMPTZ | NULL | — | When CFO was notified | — |
| `cfo_acknowledged_by` | VARCHAR(100) | NULL | — | CFO who acknowledged | — |
| `cfo_acknowledged_at` | TIMESTAMPTZ | NULL | — | Timestamp of CFO acknowledgment | — |
| `restated_at` | TIMESTAMPTZ | NOT NULL | — | When restatement was recorded | — |

---

### audit.alert_log

**Purpose:** Records every alert fired by the alert engine. One row per alert instance. Supports acknowledgment workflow and tracks open/resolved status.

**Written by:** Alert engine (Databricks job triggered after gold refresh).

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `alert_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `rule_id` | INT | NOT NULL | FK → config.ref_alert_rules | Alert rule that fired | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity for which the alert fired | — |
| `period_id` | INT | NOT NULL | — | YYYYMM period of the alert | — |
| `batch_id` | UUID | NULL | FK → batch_log | Batch that triggered the alert evaluation | — |
| `severity` | VARCHAR(10) | NOT NULL | — | `HIGH`, `MEDIUM`, `LOW` (copied from rule at fire time) | — |
| `alert_title` | VARCHAR(200) | NOT NULL | — | Alert title (copied from rule at fire time) | — |
| `kpi_name` | VARCHAR(100) | NOT NULL | — | KPI name being alerted | — |
| `kpi_value` | NUMERIC(18,4) | NOT NULL | — | Actual KPI value at time of alert | — |
| `threshold_value` | NUMERIC(18,4) | NOT NULL | — | Threshold value at time of alert | — |
| `recipients_notified` | TEXT | NULL | — | Comma-separated list of notified users/roles | — |
| `notification_channels` | VARCHAR(100) | NULL | — | Channels used for notification | — |
| `notification_sent_at` | TIMESTAMPTZ | NULL | — | When notification was sent | — |
| `status` | VARCHAR(20) | NOT NULL | — | DEFAULT `'OPEN'`. Values: `OPEN`, `ACKNOWLEDGED`, `RESOLVED`, `SUPPRESSED` | — |
| `acknowledged_by` | VARCHAR(100) | NULL | — | User who acknowledged | — |
| `acknowledged_at` | TIMESTAMPTZ | NULL | — | Acknowledgment timestamp | — |
| `resolved_by` | VARCHAR(100) | NULL | — | User who resolved | — |
| `resolved_at` | TIMESTAMPTZ | NULL | — | Resolution timestamp | — |
| `resolution_notes` | TEXT | NULL | — | How the alert was resolved | — |
| `triggered_at` | TIMESTAMPTZ | NOT NULL | — | When the alert was fired | — |

---

### audit.system_audit_log

**Purpose:** Immutable, append-only security and access audit log. Records all user actions, data access events, configuration changes, and system events. Retained permanently per HU accounting law (Számviteli törvény 2000/C §169) and GDPR audit requirements.

**Written by:** Application middleware (all FIP modules); never deleted or updated.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `log_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `event_timestamp` | TIMESTAMPTZ | NOT NULL | — | When the event occurred | — |
| `event_type` | VARCHAR(50) | NOT NULL | — | Category: `DATA_ACCESS`, `CONFIG_CHANGE`, `USER_LOGIN`, `PIPELINE_RUN`, `ALERT_FIRED`, etc. | — |
| `user_id` | VARCHAR(200) | NOT NULL | — | Authenticated user identifier (Entra ID object ID) | — |
| `user_display_name` | VARCHAR(200) | NULL | — | Human-readable user name | — |
| `entity_id` | UUID | NULL | FK → config.ref_entity_master | Affected entity (nullable for system events) | — |
| `resource_type` | VARCHAR(100) | NOT NULL | — | Type of resource accessed (e.g., `TABLE`, `REPORT`, `PIPELINE`) | — |
| `resource_name` | VARCHAR(200) | NOT NULL | — | Name of the specific resource | — |
| `action` | VARCHAR(50) | NOT NULL | — | Action performed: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `EXECUTE`, `EXPORT` | — |
| `action_detail` | JSONB | NULL | — | Additional action details (query parameters, filter values, etc.) | — |
| `ip_address` | INET | NULL | — | Client IP address | — |
| `session_id` | VARCHAR(200) | NULL | — | Application session identifier | — |
| `outcome` | VARCHAR(20) | NOT NULL | — | DEFAULT `'SUCCESS'`. Values: `SUCCESS`, `DENIED`, `ERROR` | — |
| `outcome_detail` | TEXT | NULL | — | Error message or denial reason | — |

**Retention:** Permanent — never delete. Log Analytics workspace configured for permanent archive.

---

### audit.commentary_queue

**Purpose:** Queue and archive for AI-generated financial commentary. Stores the narrative text, GPT model metadata, token counts, and approval workflow status. Commentary is generated by `commentary_generator.py` using `gpt-4o` (version 2024-08-06) and requires human review before publication.

**Written by:** `commentary_generator.py` (Azure OpenAI integration).

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `queue_id` | UUID | NOT NULL | PK | Unique commentary identifier | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity this commentary covers | — |
| `period_id` | INT | NOT NULL | — | YYYYMM period the commentary covers | — |
| `commentary_role` | VARCHAR(20) | NOT NULL | — | CHECK: `CFO`, `CEO`, `BOARD`, `INVESTOR` — intended audience | — |
| `language_code` | CHAR(2) | NOT NULL | — | CHECK: `HU`, `EN` — language of the commentary | — |
| `narrative_text` | TEXT | NULL | — | Generated narrative in the primary language | — |
| `narrative_text_en` | TEXT | NULL | — | English narrative (if primary is HU) | — |
| `variance_fact_pack` | JSONB | NULL | — | Structured variance data used as model context | — |
| `prompt_version` | VARCHAR(20) | NOT NULL | — | Version of the prompt template used | — |
| `word_count` | INT | NULL | GENERATED | Computed from `narrative_text` word count | — |
| `generated_by_model` | VARCHAR(100) | NOT NULL | — | Model identifier (e.g., `gpt-4o-2024-08-06`) | — |
| `generated_at` | TIMESTAMPTZ | NOT NULL | — | When the commentary was generated | — |
| `prompt_tokens` | INT | NULL | — | Input token count from API response | — |
| `completion_tokens` | INT | NULL | — | Output token count from API response | — |
| `total_tokens` | INT | NULL | GENERATED | `prompt_tokens + completion_tokens` | — |
| `confidence_score` | NUMERIC(5,4) | NULL | — | Model-assigned confidence (0.0–1.0) | — |
| `materiality_flags` | JSONB | NULL | — | Flags for variances exceeding materiality thresholds | — |
| `approval_status` | VARCHAR(30) | NOT NULL | — | DEFAULT `'PENDING_REVIEW'`. Values: `PENDING_REVIEW`, `APPROVED`, `REJECTED`, `PUBLISHED` | — |
| `submitted_for_review_at` | TIMESTAMPTZ | NULL | — | When submitted for human review | — |
| `reviewed_by` | VARCHAR(100) | NULL | — | Reviewer name/email | — |
| `reviewed_at` | TIMESTAMPTZ | NULL | — | Review timestamp | — |
| `review_notes` | TEXT | NULL | — | Reviewer comments | — |
| `published_at` | TIMESTAMPTZ | NULL | — | When published (status = `PUBLISHED`) | — |
| `published_to_gold_id` | UUID | NULL | — | Reference to gold layer narrative record | — |
| `batch_id` | UUID | NULL | FK → batch_log | Pipeline batch that triggered this generation | — |
| `is_retry` | BOOLEAN | NOT NULL | — | TRUE = this is a retry of a failed generation | — |
| `superseded_by` | UUID | NULL | FK → self | UUID of the newer commentary record that supersedes this one | — |

---

## Schema: bronze

The `bronze` schema holds exactly one table. Its purpose is manifest tracking only — the actual raw file data resides in ADLS Gen2 blob storage.

---

### bronze.ingestion_manifest

**Purpose:** One row per file received from a source system. Tracks file metadata, deduplication (via hash), and processing status. The gateway record for all data entering the platform. Referenced by `audit.batch_log` for full lineage.

**Written by:** ADF blob event trigger pipeline (`tr_erp_file_arrival`) upon file arrival in `bronze/raw/{company_id}/`.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `manifest_id` | UUID | NOT NULL | PK | Unique manifest record identifier | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity that sent this file | — |
| `source_system` | VARCHAR(50) | NOT NULL | — | Source ERP/system name | — |
| `source_file_name` | VARCHAR(500) | NOT NULL | — | Original file name | — |
| `source_file_path` | TEXT | NOT NULL | — | Full ADLS Gen2 path | — |
| `source_file_size_bytes` | BIGINT | NULL | — | File size in bytes | — |
| `source_file_hash` | CHAR(64) | NOT NULL | — | SHA-256 hash for deduplication | — |
| `source_file_format` | VARCHAR(10) | NOT NULL | — | `CSV`, `XML`, `XBRL`, `OFX`, `JSON`, `PARQUET` | — |
| `period_id_detected` | INT | NULL | — | YYYYMM period detected from file name or content | — |
| `row_count_raw` | INT | NULL | — | Raw row count (excluding header if CSV) | — |
| `received_at` | TIMESTAMPTZ | NOT NULL | — | Timestamp when file was received by ADLS | — |
| `processing_status` | VARCHAR(20) | NOT NULL | — | DEFAULT `'PENDING'`. Values: `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `DUPLICATE` | — |
| `batch_id` | UUID | NULL | FK → audit.batch_log | Batch created when processing started | — |
| `duplicate_of_manifest_id` | UUID | NULL | FK → self | If `DUPLICATE`, points to the original manifest | — |

---

## Schema: silver

The `silver` schema contains cleansed, DQ-passed, typed dimensional and fact tables. All dimension tables use integer surrogate keys (`_key` suffix) internally, with UUID FKs (`_id` suffix) connecting to config schema. Data is loaded by Databricks dbt jobs.

---

### silver.dim_date

**Purpose:** Date dimension spanning all relevant periods. Pre-computed with all calendar, fiscal, and Hungarian holiday attributes. The `date_key` is an INT in YYYYMMDD format for fast range filtering in fact tables.

**Written by:** dbt seed/model (regenerated annually, updated when holiday calendar is updated).

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `date_key` | INT | NOT NULL | PK | YYYYMMDD integer (e.g., 20250315 for 2025-03-15) | — |
| `calendar_date` | DATE | NOT NULL | UNIQUE | Actual calendar date | — |
| `year` | INT | NOT NULL | — | Calendar year | — |
| `quarter` | INT | NOT NULL | — | Calendar quarter (1–4) | — |
| `month` | INT | NOT NULL | — | Calendar month (1–12) | — |
| `month_name` | VARCHAR(20) | NOT NULL | — | Month name in English | — |
| `month_name_hu` | VARCHAR(20) | NOT NULL | — | Month name in Hungarian | Hónap neve (HU) |
| `week_of_year` | INT | NOT NULL | — | ISO week number | — |
| `day_of_week` | INT | NOT NULL | — | ISO day of week: 1=Monday, 7=Sunday | — |
| `is_weekday` | BOOLEAN | NOT NULL | — | TRUE if Monday–Friday | — |
| `is_hungarian_public_holiday` | BOOLEAN | NOT NULL | — | TRUE if date is in `config.ref_hu_public_holidays` | Magyar munkaszüneti nap |
| `is_month_end` | BOOLEAN | NOT NULL | — | TRUE if last day of calendar month | — |
| `is_quarter_end` | BOOLEAN | NOT NULL | — | TRUE if last day of calendar quarter | — |
| `is_year_end` | BOOLEAN | NOT NULL | — | TRUE if December 31 | — |
| `fiscal_year` | INT | NOT NULL | — | Fiscal year (may differ from calendar year for non-Jan start entities) | Üzleti év |
| `fiscal_quarter` | INT | NOT NULL | — | Fiscal quarter (1–4) | — |
| `fiscal_period` | INT | NOT NULL | — | Fiscal period within fiscal year (1–12) | Pénzügyi időszak sorszáma |
| `fiscal_period_label` | VARCHAR(20) | NOT NULL | — | Label: e.g., `2025-FP03` | — |
| `prior_month_date_key` | INT | NULL | FK → self | `date_key` of same day in prior month | — |
| `prior_year_date_key` | INT | NULL | FK → self | `date_key` of same day in prior year | — |
| `prior_quarter_date_key` | INT | NULL | FK → self | `date_key` of same day in prior quarter | — |
| `trading_day_num` | INT | NULL | — | Sequential trading day number within the month (excludes weekends and HU holidays) | — |
| `trading_days_in_month` | INT | NULL | — | Total trading days in the month | — |

---

### silver.dim_account

**Purpose:** Conformed account dimension. One row per entity-account-validity combination. Maps local ERP account codes to the universal hierarchy. Integer `account_key` used as FK in all silver fact tables.

**Written by:** dbt model reading from `config.ref_coa_mapping` (APPROVED records only).

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `account_key` | INT | NOT NULL | PK | GENERATED ALWAYS AS IDENTITY — integer surrogate key | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity this account belongs to | — |
| `account_code` | VARCHAR(50) | NOT NULL | — | Local ERP account code | Könyvelési számlaszám |
| `account_code_canonical` | VARCHAR(50) | NULL | — | Canonical/normalized account code | — |
| `account_name_hu` | VARCHAR(200) | NULL | — | Account name in Hungarian | Számla megnevezése (HU) |
| `account_name_en` | VARCHAR(200) | NULL | — | Account name in English | — |
| `universal_node` | VARCHAR(100) | NOT NULL | — | Platform universal hierarchy node | Egységesített csomópont |
| `account_type` | VARCHAR(20) | NOT NULL | — | `ASSET`, `LIABILITY`, `EQUITY`, `REVENUE`, `EXPENSE` | Számla típusa |
| `normal_balance` | CHAR(1) | NOT NULL | — | `D` = Debit, `C` = Credit | Normál egyenleg |
| `l1_category` | VARCHAR(100) | NULL | — | Level-1 category | L1 kategória |
| `l2_subcategory` | VARCHAR(100) | NULL | — | Level-2 subcategory | L2 alkategória |
| `l3_detail` | VARCHAR(100) | NULL | — | Level-3 detail | L3 részletezés |
| `pl_line_item` | VARCHAR(100) | NULL | — | P&L line item | E&B sor |
| `cf_classification` | VARCHAR(50) | NULL | — | Cash flow classification | Pénzáramlás osztályozás |
| `is_controlling` | BOOLEAN | NOT NULL | — | Controlling account flag | — |
| `is_intercompany` | BOOLEAN | NOT NULL | — | Intercompany account flag | — |
| `is_active` | BOOLEAN | NOT NULL | — | Active/inactive flag | — |
| `valid_from` | DATE | NOT NULL | — | Effective from date | — |
| `valid_to` | DATE | NULL | — | Effective to date | — |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt model run timestamp | — |

**Constraints:**
- `UNIQUE(entity_id, account_code, valid_from)`

---

### silver.dim_entity

**Purpose:** Conformed entity dimension. Integer `entity_key` used as FK in all silver and gold fact tables. Contains `entity_code` (VARCHAR(20)) — the canonical human-readable entity identifier used in gold inline columns and all application queries.

**Written by:** dbt model reading from `config.ref_entity_master`.

**Note:** `entity_code` lives on this table. Queries filtering by entity must JOIN to `silver.dim_entity` to resolve `entity_code` — it is NOT a column on `silver.fact_gl_transaction` or other fact tables.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `entity_key` | INT | NOT NULL | PK | GENERATED ALWAYS AS IDENTITY — integer surrogate key | — |
| `entity_id` | UUID | NOT NULL | UNIQUE FK → config.ref_entity_master | UUID from config layer | — |
| `entity_code` | VARCHAR(20) | NOT NULL | — | Platform entity code (e.g., `HU_MAIN`) — used in all application layer queries | Egység kód |
| `entity_name` | VARCHAR(200) | NOT NULL | — | Full legal name | Teljes cégnév |
| `entity_name_short` | VARCHAR(50) | NULL | — | Short display name | Rövidített név |
| `legal_entity_type` | VARCHAR(50) | NULL | — | Legal form | Jogi forma |
| `tax_id` | VARCHAR(30) | NULL | — | Tax identifier | Adószám |
| `country_code` | CHAR(2) | NOT NULL | — | ISO country code | Ország kód |
| `reporting_currency` | CHAR(3) | NOT NULL | — | Reporting currency | Könyvelési pénznem |
| `fiscal_year_start_month` | INT | NOT NULL | — | Fiscal year start month | Üzleti év kezdete |
| `consolidation_group` | VARCHAR(100) | NULL | — | Consolidation group | Konszolidációs csoport |
| `parent_entity_key` | INT | NULL | FK → self | Parent entity integer key | — |
| `consolidation_method` | VARCHAR(20) | NOT NULL | — | Consolidation method | Konszolidációs módszer |
| `is_active` | BOOLEAN | NOT NULL | — | Active flag | — |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run | — |

---

### silver.dim_cost_centre

**Purpose:** Conformed cost centre dimension. Integer `cost_centre_key` used as FK in silver/gold fact tables.

**Written by:** dbt model reading from `config.ref_cost_centre_master`.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `cost_centre_key` | INT | NOT NULL | PK | GENERATED ALWAYS AS IDENTITY | — |
| `cost_centre_id` | UUID | NOT NULL | UNIQUE FK → config.ref_cost_centre_master | UUID from config | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Owning entity | — |
| `cost_centre_code` | VARCHAR(30) | NOT NULL | — | Cost centre code | Költséghely kód |
| `cost_centre_name` | VARCHAR(200) | NOT NULL | — | Hungarian name | Költséghely neve |
| `manager_name` | VARCHAR(100) | NULL | — | Manager | Felelős vezető |
| `business_unit` | VARCHAR(100) | NULL | — | Business unit | Üzleti egység |
| `division` | VARCHAR(100) | NULL | — | Division | Divízió |
| `region` | VARCHAR(50) | NULL | — | Region | Régió |
| `is_budget_centre` | BOOLEAN | NOT NULL | — | Budget centre flag | Tervező egység |
| `is_profit_centre` | BOOLEAN | NOT NULL | — | Profit centre flag | Eredményközpont |
| `parent_cost_centre_key` | INT | NULL | FK → self | Parent cost centre key | — |
| `is_active` | BOOLEAN | NOT NULL | — | Active flag | — |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run | — |

---

### silver.dim_currency

**Purpose:** Conformed currency dimension for join purposes in silver fact tables.

**Written by:** dbt seed reading from `config.ref_currencies`.

| Column Name | Data Type | Nullable | PK/FK | Description |
|-------------|-----------|----------|-------|-------------|
| `currency_key` | INT | NOT NULL | PK | GENERATED ALWAYS AS IDENTITY |
| `currency_code` | CHAR(3) | NOT NULL | UNIQUE FK → config.ref_currencies | ISO 4217 code |
| `currency_name` | VARCHAR(100) | NOT NULL | — | Currency name |
| `currency_symbol` | VARCHAR(5) | NULL | — | Symbol |
| `decimal_places` | INT | NOT NULL | — | Decimal places |
| `is_reporting_currency` | BOOLEAN | NOT NULL | — | TRUE for HUF |

---

### silver.dim_project

**Purpose:** Conformed project dimension.

**Written by:** dbt model reading from `config.ref_project_master`.

| Column Name | Data Type | Nullable | PK/FK | Description |
|-------------|-----------|----------|-------|-------------|
| `project_key` | INT | NOT NULL | PK | GENERATED ALWAYS AS IDENTITY |
| `project_id` | UUID | NOT NULL | UNIQUE FK → config.ref_project_master | UUID from config |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Owning entity |
| `project_code` | VARCHAR(30) | NOT NULL | — | Project code |
| `project_name` | VARCHAR(200) | NOT NULL | — | Project name |
| `project_type` | VARCHAR(20) | NOT NULL | — | `CAPEX`, `OPEX`, `CLIENT`, `INTERNAL`, `RD` |
| `wbs_element` | VARCHAR(50) | NULL | — | WBS element |
| `cost_centre_key` | INT | NULL | FK → dim_cost_centre | Cost centre key |
| `project_manager` | VARCHAR(100) | NULL | — | Manager name |
| `project_start_date` | DATE | NULL | — | Start date |
| `project_end_date` | DATE | NULL | — | End date |
| `project_status` | VARCHAR(20) | NOT NULL | — | `ACTIVE`, `COMPLETED`, `CANCELLED`, `ON_HOLD` |
| `is_active` | BOOLEAN | NOT NULL | — | Derived from project_status |

---

### silver.account_master

**Purpose:** Extended account master for silver layer enrichment. Contains HU GAAP–specific line item mappings (`hu_gaap_bs_line`, `hu_gaap_pl_line`) that are not in the conformed `dim_account`. Also tracks mapping review status at the silver layer.

**Written by:** dbt model; mapping review workflow.

**Known issue (M-09):** The column `company_id` in this table is a FK to `config.ref_entity_master(entity_id)` but is named `company_id` instead of the platform-standard `entity_id`. This is a deliberate legacy naming inconsistency documented as finding M-09. All new tables use `entity_id`.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `account_id` | UUID | NOT NULL | PK | Surrogate identifier | — |
| `company_id` | UUID | NOT NULL | FK → config.ref_entity_master(entity_id) | **Non-standard name** — should be `entity_id` per platform convention (see M-09) | — |
| `local_account_code` | VARCHAR(50) | NOT NULL | — | Local ERP account code | Helyi számlaszám |
| `local_account_name` | VARCHAR(200) | NULL | — | Hungarian account name | Számla neve (HU) |
| `local_account_name_en` | VARCHAR(200) | NULL | — | English account name | — |
| `universal_node` | VARCHAR(100) | NOT NULL | — | Platform universal node | Egységesített csomópont |
| `account_type` | VARCHAR(20) | NOT NULL | — | Account type | Számla típusa |
| `normal_balance` | CHAR(1) | NOT NULL | — | `D` or `C` | Normál egyenleg |
| `l1_category` | VARCHAR(100) | NULL | — | Level-1 category | L1 kategória |
| `l2_subcategory` | VARCHAR(100) | NULL | — | Level-2 | L2 alkategória |
| `l3_detail` | VARCHAR(100) | NULL | — | Level-3 | L3 részletezés |
| `pl_line_item` | VARCHAR(100) | NULL | — | P&L line | E&B sor |
| `cf_classification` | VARCHAR(50) | NULL | — | Cash flow class | Pénzáramlás |
| `bs_section` | VARCHAR(50) | NULL | — | Balance sheet section | Mérleg főcsoport |
| `hu_gaap_bs_line` | VARCHAR(10) | NULL | — | HU GAAP balance sheet line code (e.g., `A/I/1`) | Mérleg sor kód |
| `hu_gaap_pl_line` | VARCHAR(10) | NULL | — | HU GAAP P&L line code | E&B sor kód |
| `is_controlling` | BOOLEAN | NOT NULL | — | Controlling account | — |
| `is_intercompany` | BOOLEAN | NOT NULL | — | Intercompany flag | — |
| `is_reconciling` | BOOLEAN | NOT NULL | — | Reconciliation account flag | — |
| `is_active` | BOOLEAN | NOT NULL | — | Active flag | — |
| `valid_from` | DATE | NOT NULL | — | Valid from | — |
| `valid_to` | DATE | NULL | — | Valid to | — |
| `mapping_rationale` | TEXT | NULL | — | Mapping explanation | — |
| `mapping_reviewed_by` | VARCHAR(100) | NULL | — | Reviewer | — |
| `mapping_reviewed_at` | TIMESTAMPTZ | NULL | — | Review date | — |
| `review_status` | VARCHAR(20) | NOT NULL | — | `PENDING`, `APPROVED`, `DISPUTED` | — |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run | — |
| `dbt_run_id` | VARCHAR(200) | NULL | — | dbt run identifier | — |

**Constraints:**
- `UNIQUE(company_id, local_account_code, valid_from)`

---

### silver.fact_gl_transaction

**Purpose:** The primary fact table for general ledger transactions. Contains one row per GL posting line. All amounts are in both local currency (LCY = HUF for HU entities) and foreign currency (FCY = original currency). EUR amounts provided for cross-currency comparison. This is the foundation for all P&L, balance sheet, and cash flow aggregations.

**Written by:** Databricks DQ and transformation job (post-DQ-pass records only).

**Critical:** `entity_code` does NOT exist as a column on this table. To filter by entity code, JOIN to `silver.dim_entity` on `entity_key`. See audit finding M-08.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `transaction_key` | BIGINT | NOT NULL | PK | GENERATED ALWAYS AS IDENTITY | — |
| `source_transaction_id` | VARCHAR(200) | NOT NULL | — | Source ERP transaction/document number | Forrás bizonylat szám |
| `source_system` | VARCHAR(50) | NOT NULL | — | Source ERP system | ERP rendszer |
| `batch_id` | UUID | NOT NULL | FK → audit.batch_log | Load batch | — |
| `date_key` | INT | NOT NULL | FK → dim_date | Transaction date key (YYYYMMDD) | — |
| `account_key` | INT | NOT NULL | FK → dim_account | Account dimension key | — |
| `entity_key` | INT | NOT NULL | FK → dim_entity | Entity dimension key | — |
| `cost_centre_key` | INT | NULL | FK → dim_cost_centre | Cost centre key (nullable) | — |
| `currency_key` | INT | NOT NULL | FK → dim_currency | Transaction currency key | — |
| `project_key` | INT | NULL | FK → dim_project | Project key (nullable) | — |
| `transaction_date` | DATE | NOT NULL | — | Economic transaction date | Gazdasági esemény dátuma |
| `posting_date` | DATE | NOT NULL | — | ERP posting date | Könyvelés dátuma |
| `period_id` | INT | NOT NULL | — | YYYYMM fiscal period | Pénzügyi időszak |
| `fiscal_year` | INT | NOT NULL | — | Fiscal year | Üzleti év |
| `fiscal_period` | INT | NOT NULL | — | Period within fiscal year (1–12) | Periódus sorszáma |
| `debit_lcy` | NUMERIC(18,2) | NOT NULL | — | Debit amount in local currency (HUF) | Tartozik (HUF) |
| `credit_lcy` | NUMERIC(18,2) | NOT NULL | — | Credit amount in local currency (HUF) | Követel (HUF) |
| `net_amount_lcy` | NUMERIC(18,2) | NOT NULL | — | Net amount LCY: debit minus credit | Nettó összeg (HUF) |
| `debit_fcy` | NUMERIC(18,2) | NULL | — | Debit in foreign/original currency | Tartozik (eredeti pénznem) |
| `credit_fcy` | NUMERIC(18,2) | NULL | — | Credit in foreign/original currency | Követel (eredeti pénznem) |
| `net_amount_fcy` | NUMERIC(18,2) | NULL | — | Net in foreign currency | Nettó (eredeti pénznem) |
| `original_currency` | CHAR(3) | NOT NULL | — | Original transaction currency code | Eredeti pénznem |
| `fx_rate_used` | NUMERIC(16,6) | NULL | — | FX rate applied for HUF conversion | Alkalmazott árfolyam |
| `fx_rate_date` | DATE | NULL | — | Date of the FX rate used | Árfolyam dátuma |
| `net_amount_eur` | NUMERIC(18,2) | NULL | — | Net amount in EUR for cross-entity comparison | Összeg (EUR) |
| `eur_rate_used` | NUMERIC(16,6) | NULL | — | EUR rate used | EUR árfolyam |
| `gaap_basis` | VARCHAR(20) | NOT NULL | — | DEFAULT `'HU_GAAP'` | Számviteli alap |
| `document_type` | VARCHAR(20) | NULL | — | ERP document type code | Bizonylat típusa |
| `document_number` | VARCHAR(100) | NULL | — | ERP document number | Bizonylat szám |
| `document_line` | INT | NULL | — | Line number within the document | Bizonylat sor |
| `posting_text` | VARCHAR(500) | NULL | — | Posting description/memo | Könyvelési szöveg |
| `reference_number` | VARCHAR(100) | NULL | — | Reference number (e.g., invoice number) | Hivatkozási szám |
| `is_intercompany` | BOOLEAN | NOT NULL | — | Intercompany transaction flag | Csoportközi ügylet |
| `is_opening_balance` | BOOLEAN | NOT NULL | — | Opening balance entry flag | Nyitó egyenleg |
| `is_reversal` | BOOLEAN | NOT NULL | — | This record reverses another | Sztornó tétel |
| `reversal_of_key` | BIGINT | NULL | FK → self | `transaction_key` being reversed | — |
| `is_reversed` | BOOLEAN | NOT NULL | — | This record has been reversed | Sztornózott |
| `is_late_entry` | BOOLEAN | NOT NULL | — | Posted after period close | Késői könyvelés |
| `is_adjusted` | BOOLEAN | NOT NULL | — | Adjustment/correction posting | Módosítás |
| `is_accrual` | BOOLEAN | NOT NULL | — | Accrual entry flag | Elhatárolás |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run timestamp | — |
| `dbt_run_id` | VARCHAR(200) | NULL | — | dbt run identifier | — |

**Constraints:**
- `UNIQUE(source_system, source_transaction_id, document_line, entity_key)`

**Key Indexes:**
- `PK_fact_gl_transaction` on `transaction_key`
- `IX_fact_gl_entity_period` on `(entity_key, period_id)`
- `IX_fact_gl_account_key` on `account_key`
- `IX_fact_gl_posting_date` on `posting_date DESC`

---

## Schema: budget

The `budget` schema manages planned financial data including budget versions and ML-generated forecasts.

---

### budget.ref_budget_versions

**Purpose:** Version control for budget data. Allows multiple versions per entity per fiscal year (e.g., `APPROVED_2025`, `REFORECAST_Q2`). Only one version per entity/year should be `is_active = TRUE` at any time.

**Written by:** Budget upload pipeline; finance team via budget management UI.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `budget_version_id` | SERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity | — |
| `fiscal_year` | INT | NOT NULL | — | Fiscal year | Üzleti év |
| `version_name` | VARCHAR(100) | NOT NULL | — | Version label (e.g., `BOARD_APPROVED_2025`) | Változat neve |
| `version_type` | VARCHAR(20) | NOT NULL | — | DEFAULT `'ANNUAL'`. Values: `ANNUAL`, `REFORECAST_Q1`, `REFORECAST_Q2`, `REFORECAST_Q3` | Változat típusa |
| `is_active` | BOOLEAN | NOT NULL | — | DEFAULT `FALSE`. TRUE = this is the active budget for reporting | Aktív változat |
| `approved_by` | VARCHAR(100) | NULL | — | Approver name | Jóváhagyta |
| `approved_at` | TIMESTAMPTZ | NULL | — | Approval timestamp | Jóváhagyás időpontja |
| `upload_completed_at` | TIMESTAMPTZ | NULL | — | When upload was completed | Feltöltés vége |
| `notes` | TEXT | NULL | — | Version notes | Megjegyzés |
| `created_at` | TIMESTAMPTZ | NOT NULL | — | Record creation timestamp | — |

**Constraints:**
- `UNIQUE(entity_id, fiscal_year, version_name)`

---

### budget.fact_budget

**Purpose:** Stores approved budget amounts at monthly account/cost-centre/project granularity. The `period_id` column stores YYYYMM values. Budget amounts are always in HUF (`budget_amount_huf`).

**Written by:** Budget upload pipeline (Excel/CSV ingestion via ADF).

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `budget_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity (UUID, not `entity_key` INT) | — |
| `budget_version_id` | INT | NOT NULL | FK → ref_budget_versions | Budget version | — |
| `period_id` | INT | NOT NULL | — | YYYYMM period | Pénzügyi időszak |
| `account_key` | INT | NOT NULL | FK → silver.dim_account | Account dimension key | — |
| `cost_centre_key` | INT | NULL | FK → silver.dim_cost_centre | Cost centre (nullable) | — |
| `project_key` | INT | NULL | FK → silver.dim_project | Project (nullable) | — |
| `budget_amount_huf` | NUMERIC(18,2) | NOT NULL | — | Budget amount in HUF (NOT `budget_amount_lcy` — see H-06/M-07) | Tervezett összeg (HUF) |
| `uploaded_at` | TIMESTAMPTZ | NOT NULL | — | Upload timestamp | — |
| `uploaded_by` | VARCHAR(100) | NULL | — | Uploader user | — |
| `source_file` | VARCHAR(500) | NULL | — | Source file path | — |

**Constraints:**
- `UNIQUE(entity_id, budget_version_id, period_id, account_key, cost_centre_key, project_key)` — NULLS NOT DISTINCT

---

### budget.fact_forecast

**Purpose:** Stores ML model forecast outputs. One row per entity/model/period/KPI combination. Stores three forecast scenarios (P10=pessimistic, P50=base, P90=optimistic). Generated by `financial_forecaster.py` using Prophet, LSTM, ARIMA, or Naive models.

**Written by:** `financial_forecaster.py` via Databricks job.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `forecast_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `entity_id` | UUID | NOT NULL | FK → config.ref_entity_master | Entity (UUID — NOT `entity_key` INT) | — |
| `forecast_run_id` | UUID | NOT NULL | — | Groups all outputs from a single forecast run | — |
| `model_name` | VARCHAR(20) | NOT NULL | — | `PROPHET`, `LSTM`, `ARIMA`, `NAIVE` | — |
| `model_version` | VARCHAR(50) | NULL | — | Model version string | — |
| `period_id` | INT | NOT NULL | — | YYYYMM forecast period (NOT `period_key`) | — |
| `kpi_name` | VARCHAR(100) | NOT NULL | — | KPI being forecast (e.g., `REVENUE`, `EBITDA`) | — |
| `forecast_p50` | NUMERIC(18,2) | NOT NULL | — | Base case forecast amount (NOT `forecast_amount_lcy`) | Alap forgatókönyv |
| `forecast_p10` | NUMERIC(18,2) | NULL | — | Pessimistic scenario (10th percentile) | Pesszimista |
| `forecast_p90` | NUMERIC(18,2) | NULL | — | Optimistic scenario (90th percentile) | Optimista |
| `confidence_interval_pct` | INT | NOT NULL | — | Confidence interval width. DEFAULT `80` | — |
| `training_data_periods` | INT | NULL | — | Number of periods used for training | — |
| `generated_at` | TIMESTAMPTZ | NOT NULL | — | Forecast generation timestamp | — |

---

## Schema: gold

The `gold` schema contains pre-aggregated, presentation-ready tables for reporting. No FK constraints (denormalized for query performance). Uses `entity_code` (VARCHAR(20)) as an inline column rather than requiring a JOIN to dim_entity. `period_key` is used as the YYYYMM INT primary key component in all aggregate tables.

---

### gold.fact_gl_transaction

**Purpose:** Gold copy of GL transactions with denormalized entity attributes. No FK constraints — optimized for ad-hoc analytical queries. `entity_code` is stored inline (VARCHAR(20)) for direct filtering without joins. Used by `financial_qa_agent.py`.

**Written by:** dbt gold model (copy from silver with entity attribute denormalization).

**Critical:** Table name is `gold.fact_gl_transaction` (NOT `gold.fct_gl_transaction` — see H-03).

| Column Name | Data Type | Nullable | PK/FK | Description |
|-------------|-----------|----------|-------|-------------|
| `transaction_key` | BIGINT | NOT NULL | PK | Same as silver `transaction_key` |
| `entity_code` | VARCHAR(20) | NOT NULL | — | Inline entity code — enables direct WHERE filtering without JOIN |
| `entity_name` | VARCHAR(200) | NOT NULL | — | Inline entity name |
| `period_id` | INT | NOT NULL | — | YYYYMM period (also referenced as `period_key` in gold context) |
| *(all other columns identical to silver.fact_gl_transaction)* | | | | |

---

### gold.agg_pl_monthly

**Purpose:** Monthly P&L summary per entity. All line items from revenue through net profit. Includes budget comparison, prior year comparison, and YTD accumulators. All generated columns computed by database.

**Written by:** dbt gold model (monthly close trigger).

**Primary Key:** `(period_key, entity_key)` — composite.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `period_key` | INT | NOT NULL | PK | YYYYMM (e.g., 202503) | Időszak kulcs |
| `entity_key` | INT | NOT NULL | PK FK → silver.dim_entity | Entity | — |
| `revenue` | NUMERIC(18,2) | NULL | — | Total net revenue | Nettó árbevétel |
| `cogs` | NUMERIC(18,2) | NULL | — | Cost of goods sold | Anyagjellegű ráfordítások |
| `gross_profit` | NUMERIC(18,2) | NULL | GENERATED | `revenue - cogs` | Bruttó eredmény |
| `gross_margin_pct` | NUMERIC(8,4) | NULL | GENERATED | `gross_profit / NULLIF(revenue,0) * 100` | — |
| `opex_personnel` | NUMERIC(18,2) | NULL | — | Personnel costs | Személyi jellegű ráfordítások |
| `opex_depreciation` | NUMERIC(18,2) | NULL | — | Depreciation & amortization | Értékcsökkenés |
| `opex_other` | NUMERIC(18,2) | NULL | — | Other operating expenses | Egyéb ráfordítások |
| `opex_total` | NUMERIC(18,2) | NULL | GENERATED | Sum of opex components | Összes működési ráfordítás |
| `ebitda` | NUMERIC(18,2) | NULL | GENERATED | `gross_profit - opex_personnel - opex_other + other_operating_income` | EBITDA |
| `ebitda_margin_pct` | NUMERIC(8,4) | NULL | GENERATED | `ebitda / NULLIF(revenue,0) * 100` | — |
| `ebit` | NUMERIC(18,2) | NULL | GENERATED | `ebitda - opex_depreciation` | Üzemi eredmény |
| `ebit_margin_pct` | NUMERIC(8,4) | NULL | GENERATED | `ebit / NULLIF(revenue,0) * 100` | — |
| `financial_income` | NUMERIC(18,2) | NULL | — | Financial income | Pénzügyi bevételek |
| `financial_expense` | NUMERIC(18,2) | NULL | — | Financial expense | Pénzügyi ráfordítások |
| `net_interest_expense` | NUMERIC(18,2) | NULL | GENERATED | `financial_expense - financial_income` | Nettó kamatráfordítás |
| `profit_before_tax` | NUMERIC(18,2) | NULL | GENERATED | `ebit - net_interest_expense` | Adózás előtti eredmény |
| `tax_expense` | NUMERIC(18,2) | NULL | — | Corporate tax | Társasági adó |
| `net_profit` | NUMERIC(18,2) | NULL | GENERATED | `profit_before_tax - tax_expense` | Adózott eredmény |
| `net_profit_margin_pct` | NUMERIC(8,4) | NULL | GENERATED | `net_profit / NULLIF(revenue,0) * 100` | — |
| `revenue_budget` | NUMERIC(18,2) | NULL | — | Budget revenue | Tervezett árbevétel |
| `revenue_variance` | NUMERIC(18,2) | NULL | GENERATED | `revenue - revenue_budget` | Árbevétel eltérés |
| `revenue_variance_pct` | NUMERIC(8,4) | NULL | GENERATED | `revenue_variance / NULLIF(revenue_budget,0) * 100` | — |
| `revenue_py` | NUMERIC(18,2) | NULL | — | Prior year revenue | Előző év árbevétele |
| `revenue_yoy_pct` | NUMERIC(8,4) | NULL | GENERATED | YoY revenue growth % | — |
| `revenue_ytd` | NUMERIC(18,2) | NULL | — | Year-to-date revenue | ÉTD árbevétel |
| `transaction_count` | INT | NULL | — | Count of GL transactions in period | — |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run | — |

*(Budget and prior-year columns for COGS, OPEX, EBITDA, net profit follow the same revenue pattern.)*

---

### gold.agg_balance_sheet

**Purpose:** Monthly balance sheet snapshot per entity. Fully structured per HU GAAP classification (fixed assets, current assets, equity, non-current/current liabilities). `balance_check` is a generated column that MUST equal zero for a balanced balance sheet.

**Written by:** dbt gold model (monthly close trigger).

**CRITICAL:** `balance_check` is the generated column (`total_assets - total_liabilities_equity`). This is NOT named `is_balanced`. See H-02.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `period_key` | INT | NOT NULL | PK | YYYYMM | Időszak kulcs |
| `entity_key` | INT | NOT NULL | PK FK → silver.dim_entity | Entity | — |
| `intangible_assets` | NUMERIC(18,2) | NULL | — | Intangible assets | Immateriális javak |
| `tangible_assets` | NUMERIC(18,2) | NULL | — | Tangible fixed assets | Tárgyi eszközök |
| `financial_investments` | NUMERIC(18,2) | NULL | — | Long-term financial investments | Befektetett pénzügyi eszközök |
| `total_fixed_assets` | NUMERIC(18,2) | NULL | GENERATED | Sum of above | Befektetett eszközök |
| `inventory` | NUMERIC(18,2) | NULL | — | Inventory | Készletek |
| `trade_receivables` | NUMERIC(18,2) | NULL | — | Trade receivables | Vevőkövetelések |
| `other_receivables` | NUMERIC(18,2) | NULL | — | Other receivables | Egyéb követelések |
| `total_receivables` | NUMERIC(18,2) | NULL | GENERATED | Sum of receivable components | Követelések összesen |
| `securities` | NUMERIC(18,2) | NULL | — | Short-term securities | Értékpapírok |
| `cash_and_equivalents` | NUMERIC(18,2) | NULL | — | Cash and bank balances | Pénzeszközök |
| `total_current_assets` | NUMERIC(18,2) | NULL | GENERATED | Sum of current assets | Forgóeszközök |
| `accrued_income` | NUMERIC(18,2) | NULL | — | Accrued income | Aktív időbeli elhatárolások |
| `total_assets` | NUMERIC(18,2) | NULL | GENERATED | `total_fixed_assets + total_current_assets + accrued_income` | Eszközök összesen |
| `share_capital` | NUMERIC(18,2) | NULL | — | Share capital | Jegyzett tőke |
| `share_premium` | NUMERIC(18,2) | NULL | — | Share premium | Tőketartalék |
| `retained_earnings` | NUMERIC(18,2) | NULL | — | Retained earnings | Eredménytartalék |
| `revaluation_reserve` | NUMERIC(18,2) | NULL | — | Revaluation reserve | Értékelési tartalék |
| `profit_for_period` | NUMERIC(18,2) | NULL | — | Current period profit | Mérleg szerinti eredmény |
| `total_equity` | NUMERIC(18,2) | NULL | GENERATED | Sum of equity components | Saját tőke |
| `long_term_debt` | NUMERIC(18,2) | NULL | — | Long-term debt | Hosszú lejáratú kötelezettségek |
| `long_term_provisions` | NUMERIC(18,2) | NULL | — | Long-term provisions | Hosszú lejáratú céltartalékok |
| `total_non_current_liabilities` | NUMERIC(18,2) | NULL | GENERATED | `long_term_debt + long_term_provisions` | Hosszú lejáratú kötelezettségek összesen |
| `short_term_debt` | NUMERIC(18,2) | NULL | — | Short-term debt | Rövid lejáratú kötelezettségek |
| `trade_payables` | NUMERIC(18,2) | NULL | — | Trade payables | Szállítói kötelezettségek |
| `tax_liabilities` | NUMERIC(18,2) | NULL | — | Tax liabilities | Adótartozások |
| `other_current_liabilities` | NUMERIC(18,2) | NULL | — | Other current liabilities | Egyéb rövid lejáratú kötelezettségek |
| `total_current_liabilities` | NUMERIC(18,2) | NULL | GENERATED | Sum of current liability components | Rövid lejáratú kötelezettségek |
| `deferred_income` | NUMERIC(18,2) | NULL | — | Deferred income | Passzív időbeli elhatárolások |
| `total_liabilities_equity` | NUMERIC(18,2) | NULL | GENERATED | `total_equity + total_non_current_liabilities + total_current_liabilities + deferred_income` | Források összesen |
| `net_working_capital` | NUMERIC(18,2) | NULL | GENERATED | `total_current_assets - total_current_liabilities` | Nettó forgótőke |
| `net_debt` | NUMERIC(18,2) | NULL | GENERATED | `long_term_debt + short_term_debt - cash_and_equivalents` | Nettó adósság |
| `balance_check` | NUMERIC(18,2) | NULL | **GENERATED** | `total_assets - total_liabilities_equity`. **MUST BE ZERO.** (NOT `is_balanced`) | Mérleg egyensúly ellenőrzés |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run | — |

*(Prior year comparison columns: `total_assets_py`, `total_equity_py`, `trade_receivables_py`, `inventory_py`, `trade_payables_py`, `cash_and_equivalents_py`)*

---

### gold.agg_cashflow

**Purpose:** Monthly indirect cash flow statement per entity. Structured by operating/investing/financing activities per IAS 7 / HU GAAP equivalent.

**Written by:** dbt gold model.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `period_key` | INT | NOT NULL | PK | YYYYMM | — |
| `entity_key` | INT | NOT NULL | PK FK → silver.dim_entity | Entity | — |
| `net_profit` | NUMERIC(18,2) | NULL | — | Net profit (starting point) | Adózott eredmény |
| `add_back_depreciation` | NUMERIC(18,2) | NULL | — | D&A add-back | Értékcsökkenés visszaadás |
| `change_in_receivables` | NUMERIC(18,2) | NULL | — | Change in trade receivables | Követelések változása |
| `change_in_inventory` | NUMERIC(18,2) | NULL | — | Change in inventory | Készletváltozás |
| `change_in_payables` | NUMERIC(18,2) | NULL | — | Change in trade payables | Szállítók változása |
| `change_in_other_wc` | NUMERIC(18,2) | NULL | — | Other working capital changes | Egyéb forgótőke változás |
| `operating_cash_flow` | NUMERIC(18,2) | NULL | GENERATED | Sum of operating components | Működési pénzáramlás |
| `capex` | NUMERIC(18,2) | NULL | — | Capital expenditure | Beruházások |
| `asset_disposals` | NUMERIC(18,2) | NULL | — | Proceeds from asset disposals | Tárgyi eszköz értékesítés |
| `investing_cash_flow` | NUMERIC(18,2) | NULL | GENERATED | `asset_disposals - capex` | Befektetési pénzáramlás |
| `debt_drawdowns` | NUMERIC(18,2) | NULL | — | New debt raised | Hitelfelvétel |
| `debt_repayments` | NUMERIC(18,2) | NULL | — | Debt repayments | Hitel visszafizetés |
| `dividends_paid` | NUMERIC(18,2) | NULL | — | Dividends paid | Osztalék kifizetés |
| `equity_raised` | NUMERIC(18,2) | NULL | — | New equity raised | Tőkeemelés |
| `financing_cash_flow` | NUMERIC(18,2) | NULL | GENERATED | Sum of financing components | Finanszírozási pénzáramlás |
| `net_cash_movement` | NUMERIC(18,2) | NULL | GENERATED | `operating + investing + financing` | Nettó pénzmozgás |
| `free_cash_flow` | NUMERIC(18,2) | NULL | GENERATED | `operating_cash_flow - capex` | Szabad cash flow |
| `opening_cash_balance` | NUMERIC(18,2) | NULL | — | Cash at period start | Nyitó pénzeszköz |
| `closing_cash_balance` | NUMERIC(18,2) | NULL | — | Cash at period end | Záró pénzeszköz |
| `operating_cash_flow_ytd` | NUMERIC(18,2) | NULL | — | YTD operating cash flow | ÉTD működési pénzáramlás |
| `free_cash_flow_ytd` | NUMERIC(18,2) | NULL | — | YTD free cash flow | ÉTD szabad cash flow |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run | — |

---

### gold.agg_variance_analysis

**Purpose:** Detailed variance analysis at P&L line item and cost centre level. Compares actual vs. budget and actual vs. prior year. Feeds the variance analysis dashboard and AI commentary generation. Alert threshold breach flag drives the alert engine.

**Written by:** dbt gold model + alert engine post-processing.

| Column Name | Data Type | Nullable | PK/FK | Description | HU GAAP Mapping |
|-------------|-----------|----------|-------|-------------|-----------------|
| `variance_id` | BIGSERIAL | NOT NULL | PK | Auto-increment identifier | — |
| `period_key` | INT | NOT NULL | — | YYYYMM | — |
| `entity_key` | INT | NOT NULL | FK → silver.dim_entity | Entity | — |
| `cost_centre_key` | INT | NULL | FK → silver.dim_cost_centre | Cost centre (nullable) | — |
| `pl_line_item` | VARCHAR(100) | NOT NULL | — | P&L line item name | E&B sor |
| `actual_amount` | NUMERIC(18,2) | NULL | — | Actual amount | Tényleges összeg |
| `budget_amount` | NUMERIC(18,2) | NULL | — | Budget amount | Tervezett összeg |
| `budget_version_id` | INT | NULL | FK → budget.ref_budget_versions | Which budget version | — |
| `variance_amount` | NUMERIC(18,2) | NULL | GENERATED | `actual_amount - budget_amount` | Eltérés |
| `variance_pct` | NUMERIC(8,4) | NULL | GENERATED | `variance_amount / NULLIF(budget_amount,0) * 100` | Eltérés (%) |
| `is_over_budget` | BOOLEAN | NULL | GENERATED | TRUE if `variance_amount > 0` (expense overrun) | Túlköltés |
| `alert_threshold_breached` | BOOLEAN | NOT NULL | — | DEFAULT `FALSE`. TRUE = alert rule threshold exceeded | Riasztási küszöb átlépve |
| `prior_year_amount` | NUMERIC(18,2) | NULL | — | Prior year actual | Előző év tényleges |
| `yoy_variance_amount` | NUMERIC(18,2) | NULL | GENERATED | `actual_amount - prior_year_amount` | ÉoÉ eltérés |
| `yoy_variance_pct` | NUMERIC(8,4) | NULL | GENERATED | YoY percentage change | ÉoÉ eltérés (%) |
| `actual_ytd` | NUMERIC(18,2) | NULL | — | YTD actual | ÉTD tényleges |
| `budget_ytd` | NUMERIC(18,2) | NULL | — | YTD budget | ÉTD terv |
| `prior_year_ytd` | NUMERIC(18,2) | NULL | — | YTD prior year | ÉTD előző év |
| `dbt_updated_at` | TIMESTAMPTZ | NOT NULL | — | Last dbt run | — |

**Constraints:**
- `UNIQUE NULLS NOT DISTINCT(period_key, entity_key, pl_line_item, cost_centre_key)`

---

## Column Naming Conventions

| Convention | Pattern | Example | Notes |
|------------|---------|---------|-------|
| UUID primary key | `<noun>_id` UUID | `entity_id`, `batch_id` | Used in config, audit, bronze schemas |
| Integer surrogate key (silver) | `<noun>_key` INT IDENTITY | `entity_key`, `account_key` | Used in silver/gold fact/dim tables |
| Gold period PK component | `period_key` INT YYYYMM | `202503` | Gold aggregate tables |
| Non-PK period reference | `period_id` INT YYYYMM | `202503` | audit, config, budget tables |
| Entity code (string) | `entity_code` VARCHAR(20) | `HU_MAIN` | Human-readable; lives on dim_entity and gold inline |
| Entity UUID | `entity_id` UUID | `550e8400-...` | FK to config.ref_entity_master |
| Balance check column | `balance_check` | GENERATED: `total_assets - total_liabilities_equity` | MUST BE ZERO; not `is_balanced` |
| KV secret for Databricks PAT | `databricks-pat-token` | — | ADF linked service secret name in Key Vault |
