# Financial Intelligence Platform (FIP) —  Reference Guide

> **Version:** 1.0.1 | **Last Updated:** 2026-04-24 | **Compliance Scope:** HU GAAP (Act C of 2000)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [HU GAAP Compliance Scope](#2-hu-gaap-compliance-scope)
3. [Architecture Summary](#3-architecture-summary)
4. [Quick-Start Links](#4-quick-start-links)
5. [Module Index](#5-module-index)
6. [Environment Matrix](#6-environment-matrix)
7. [Tech Stack](#7-tech-stack)
8. [Repository Structure](#8-repository-structure)
9. [Getting Started Overview](#9-getting-started-overview)
10. [Key Terminology and Conventions](#10-key-terminology-and-conventions)

---

## 1. Project Overview

The **Financial Intelligence Platform (FIP)** is a cloud-native, AI-augmented financial data platform built on Microsoft Azure. It ingests, validates, transforms, and presents financial data for Hungarian entities subject to the Hungarian Generally Accepted Accounting Principles (HU GAAP) as defined by Act C of 2000 on Accounting.

FIP implements a **medallion lakehouse architecture** (Bronze → Silver → Gold) on Azure Data Lake Storage Gen2, orchestrated by Azure Data Factory, transformed by Databricks/dbt, persisted in Azure Synapse Analytics, and served to end-users via Power BI and a natural-language Q&A Agent backed by Azure OpenAI.

### Core Capabilities

| Capability | Description |
|---|---|
| **Multi-entity ingestion** | ERP (SAP B1, Cobalt, Kulcssoft), SFTP, REST APIs, blob event triggers |
| **HU GAAP compliance** | Statutory chart of accounts mapping, period-end close automation, audit trails |
| **Currency management** | Multi-currency support (HUF/EUR/USD/GBP/CHF/CZK/PLN/RON/RSD); daily NBH FX rate load |
| **AI-powered analytics** | Anomaly detection (IsolationForest + Z-score + rule engine), narrative commentary (GPT-4o), time-series forecasting (Prophet + ARIMA) |
| **Natural language Q&A** | FastAPI-based agent with intent classification, vector search, SQL generation and validation |
| **Monthly close automation** | Fully scheduled close pipeline T+0 to T+7h with eight distinct close phases |
| **Budget & Forecast** | Version-controlled budgets, rolling forecasts written back to `budget.fact_forecast` |
| **Data quality** | Quarantine pipeline, SHA-256 duplicate detection, alert rules engine with 10 pre-seeded rules |

### Business Context

FIP serves finance teams, controllers, and CFOs in Hungarian operating entities. It consolidates general ledger data from heterogeneous source systems, applies statutory mapping to the Hungarian chart of accounts, computes statutory P&L, Balance Sheet, and Cash Flow statements, and surfaces KPIs through Power BI dashboards and an interactive Q&A interface.

---

## 2. HU GAAP Compliance Scope

### Legislative Basis

Hungarian financial reporting is governed by **Act C of 2000 on Accounting** (Számviteli törvény). FIP is designed to support all statutory obligations arising from this Act for double-entry bookkeeping entities.

### Compliance Objectives

1. **Chart of Accounts Alignment** — `config.ref_coa_mapping` maps source-system account codes to the Hungarian statutory account classes (1–9). The view `config.v_mapping_coverage` provides real-time visibility into unmapped accounts.

2. **Fiscal Calendar** — `config.ref_fiscal_calendar` codifies the Hungarian fiscal year structure. `silver.dim_date` is populated from 2015 to 2035 and flags Hungarian public holidays (seeded in `config.ref_hu_public_holidays` for 2024–2030) to support statutory deadline calculations.

3. **Period-End Close** — The ADF pipeline `pl_monthly_close` automates the statutory close sequence. Period identifiers follow the `period_key` = YYYYMM INT convention in the Gold zone and `period_id` INT in audit/config/budget schemas.

4. **Multi-Currency Restatement** — All amounts in `silver.fact_gl_transaction` are stored in HUF. FX rates sourced from the National Bank of Hungary (NBH) API are loaded daily by `pl_nbh_fx_rate_load` into `config.ref_fx_rates`.

5. **Intercompany Elimination** — `config.ref_intercompany_pairs` codifies intercompany relationships to support elimination entries in consolidated statements.

6. **Audit Trail** — Every data movement is recorded in `audit.batch_log`, `audit.data_quality_log`, and `audit.system_audit_log`. Restatements are tracked in `audit.restatement_log`. Data anomalies are quarantined in `audit.quarantine` and surfaced through `audit.v_quarantine_open`.

7. **Statutory Statements** — Gold-zone aggregates (`agg_pl_monthly`, `agg_balance_sheet`, `agg_cashflow`) are aligned to the statutory P&L, Balance Sheet, and indirect-method Cash Flow formats required by Act C of 2000.

8. **Balance Sheet Integrity** — `gold.agg_balance_sheet` contains the generated column `balance_check` (computed as `assets - liabilities`). This value must equal zero; any non-zero value triggers an alert via `audit.proc_evaluate_alerts()`.

9. **Data Retention** — Bronze ADLS container has a Blob immutability policy set to **2922 days (8 years)**, satisfying the 8-year statutory document retention requirement of Act C of 2000.

---

## 3. Architecture Summary

FIP is built on an **Azure Lakehouse Medallion Architecture** with three data quality tiers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                                       	│
│  SAP B1 │ Cobalt (REST) │ Kulcssoft (SFTP) │ Számlázz.hu (REST) │ Manual  │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │ Azure Data Factory Orchestration
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw Ingestion)                            │
│  ADLS Gen2 bronze container │ ingestion_manifest (SHA-256) │ Immutable 8yr  │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │ Databricks + dbt
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (Conformed)                                │
│  Synapse: silver schema │ dim_* + fact_gl_transaction │ All amounts in HUF  │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │ dbt Gold models
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Business-Ready)                             │
│  Synapse: gold schema │ agg_pl/bs/cf/variance │ KPI views │ entity_code     │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │ DirectQuery / Q&A Agent
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CONSUMPTION LAYER                                       │
│  Power BI Premium │ financial_qa_agent.py (FastAPI) │ Alert Notifications   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Architectural Principles

- **Immutability at source** — Raw data in the Bronze layer is never overwritten; corrections flow through restatement processes with full audit trail.
- **Single currency in Silver** — All monetary amounts are converted to HUF at ingestion time, ensuring consistent aggregation.
- **entity_code as the business key** — Gold-zone tables carry `entity_code VARCHAR(20)` inline (denormalised) to avoid joins in reporting queries. Silver uses `entity_key INT IDENTITY` FK.
- **Secrets centralisation** — All credentials are stored in Azure Key Vault Premium. No secrets exist in code, configuration files, or environment files committed to version control.
- **Schema execution order** — config (1) → audit (2) → bronze (3) → silver (4) → budget (5) → gold (6). This order is strictly enforced during deployment.

---

## 4. Quick-Start Links

The FIP documentation suite consists of the following files. `README.md` (this file) is the root index.

| # | File | Description |
|---|---|---|
| 1 | `README.md` *(this file)* | Project overview, module index, environment matrix |
| 2 | `doc/SETUP_AND_DEPLOYMENT.md` | End-to-end provisioning and deployment guide |
| 3 | `doc/SCHEMA_DATA_DICTIONARY.md` | Full column-level documentation for all schemas and DDL reference |
| 4 | `doc/SECURITY_AND_COMPLIANCE.md` | Row-Level Security, Key Vault RBAC, network security, compliance |
| 5 | `doc/RUNBOOKS_AND_TROUBLES.md` | Step-by-step monthly close runbook and troubleshooting guide |
| 6 | `doc/DATA_DICTIONARY_GENERATION.md` | Automated data dictionary generation procedures |
| 7 | `doc/Incident response/IR-001_data_pipeline_failure.md` | Incident runbook: data pipeline failure |
| 8 | `doc/Incident response/IR-002_fx_rate_missing.md` | Incident runbook: FX rate missing |
| 9 | `dbt/Models/intermediate/IC_ELIMINATION_IMPLEMENTATION_REPORT.md` | Intercompany elimination implementation notes |
| 10 | `python/TENANT_AWARE_ROUTING.md` | Multi-tenant routing architecture and usage guide |
| 11 | `PowerBI/DASHBOARD_SPECIFICATIONS.md` | Power BI report and dashboard specifications |
| 12 | `PowerBI/REPORT_SOURCE_OF_TRUTH.md` | Power BI report field-to-source mapping |
| 13 | `PowerBI/Rls/RLS_ARCHITECTURE.md` | Power BI Row-Level Security architecture |

---

## 5. Module Index

### 5.1 Python Modules

| Module | Location | CLI Arguments | Primary Function | Key Dependencies |
|---|---|---|---|---|
| `anomaly_detector.py` | `python/Anomaly_detection/` | `--company_id`, `--period_key` | Detects GL anomalies via Statistical (IsolationForest + Z-score), Structural (rule engine), and Behavioural (velocity/off-hours) methods. Writes to `audit.anomaly_queue`. Fires Power Automate webhook. | scikit-learn 1.5.0, scipy 1.13.0, azure-identity 1.17.0 |
| `commentary_generator.py` | `python/Commentary/` | `--company_id`, `--period_key`, `--roles`, `--languages` | Builds Variance Fact Pack, calls Azure OpenAI gpt-4o, writes narrative commentary to `audit.commentary_queue` with status `PENDING_REVIEW`. | openai 1.30.0, pandas 2.2.0 |
| `financial_forecaster.py` | `python/Forecasting/` | `--company_id`, `--forecast_months`, `--base_period_key` | Time-series forecasting: Prophet (requires ≥12 months history) with ARIMA fallback (requires ≥6 months). Writes forecasts to `budget.fact_forecast`. Uses Key Vault secret `synapse-connection-string`. | prophet 1.1.5, statsmodels 0.14.0 |
| `financial_qa_agent.py` | `python/Rag/` | `--company_id`, `--period_key` (CLI); POST /query, GET /health (FastAPI) | 5-step pipeline: classify intent → vector search (Azure Cognitive Search index `fip-schema-index`) → generate SQL → validate (SQL_BLOCKED_KEYWORDS enforced) → execute → format response. | fastapi, openai 1.30.0, pydantic 2.7.0 |
| `tenant_secured_qa_agent.py` | `python/` | — | Multi-tenant extension of the Q&A agent with per-tenant routing, middleware authentication, and isolated database connections. | fastapi, pydantic 2.7.0 |
| `tenant_router.py` | `python/` | — | Routes incoming requests to the correct tenant context based on JWT claims or API key headers. | fastapi |
| `tenant_middleware.py` | `python/` | — | FastAPI middleware that injects tenant context into every request lifecycle. | fastapi |
| `tenant_config.py` | `python/` | — | Loads and validates per-tenant configuration (Synapse endpoint, Key Vault URI, entity scope). | pydantic 2.7.0 |
| `db_utils.py` | `python/` | — | Shared database utility functions (connection pooling, retry logic, parameterised query helpers). | pyodbc 5.1.0 |
| `generate_data_dictionary.py` | `python/tools/` | — | Introspects Synapse schemas and generates a Markdown data dictionary. | pyodbc 5.1.0, pandas 2.2.0 |

#### Shared Environment Variables

| Variable | Used By | Description |
|---|---|---|
| `AZURE_KEY_VAULT_URL` | anomaly_detector, all | Key Vault URI for secret retrieval |
| `SYNAPSE_SERVER` | anomaly_detector, commentary_generator, financial_qa_agent | Synapse dedicated SQL endpoint |
| `SYNAPSE_DATABASE` | anomaly_detector, commentary_generator, financial_qa_agent | Target database name |
| `AZURE_OPENAI_ENDPOINT` | commentary_generator, financial_qa_agent | Azure OpenAI service endpoint |
| `AZURE_OPENAI_DEPLOYMENT` | commentary_generator, financial_qa_agent | Model deployment name (e.g., `gpt-4o`) |
| `POWER_AUTOMATE_ALERT_URL` | anomaly_detector | Power Automate webhook URL for anomaly alerts |

### 5.2 SQL Schemas

| Schema | Execution Order | Purpose | Key Tables / Objects |
|---|---|---|---|
| `config` | 1 | Reference data and master data seeding | ref_entity_master, ref_currencies (9 currencies), ref_fx_rates, ref_fiscal_calendar, ref_coa_mapping, ref_cost_centre_master, ref_project_master, ref_intercompany_pairs, ref_alert_rules (10 rules), ref_hu_public_holidays (2024–2030), view: v_mapping_coverage |
| `audit` | 2 | Data quality, observability, and compliance trail | batch_log, data_quality_log, quarantine, restatement_log, alert_log, system_audit_log, commentary_queue, proc_evaluate_alerts(), fn_is_valid_period_id(), views: v_quarantine_open, v_alert_summary |
| `bronze` | 3 | Raw ingestion layer | ingestion_manifest (SHA-256 hash for duplicate detection) |
| `silver` | 4 | Conformed dimensional model | dim_date (YYYYMMDD, 2015–2035, HU holidays), dim_account (entity_id FK, account_key INT IDENTITY), dim_entity (entity_key INT IDENTITY, entity_code VARCHAR(20)), dim_cost_centre, dim_currency, dim_project, account_master (company_id FK), fact_gl_transaction (period_id YYYYMM, entity_key FK, all amounts HUF) |
| `budget` | 5 | Budget and forecast storage | ref_budget_versions, fact_budget, fact_forecast |
| `gold` | 6 | Business-ready aggregates and KPI views | fact_gl_transaction (denormalised, entity_code VARCHAR inline), agg_pl_monthly (period_key INT PK, generated columns: gross_profit, ebitda, ebit, net_profit, margins), agg_balance_sheet (period_key, balance_check generated column = assets − liabilities), agg_cashflow (indirect method), agg_variance_analysis (BIGSERIAL PK), views: kpi_profitability, kpi_liquidity, kpi_cashflow, kpi_project |

#### Alert Rules Seeded in `config.ref_alert_rules`

| Rule ID | Rule Code | Description |
|---|---|---|
| 1 | `CASH_CRITICAL` | Cash balance falls below critical threshold |
| 2 | `EBITDA_MARGIN_LOW` | EBITDA margin below minimum acceptable level |
| 3 | `REVENUE_VS_BUDGET` | Revenue deviates from budget beyond tolerance |
| 4 | `DSO_EXCESSIVE` | Days Sales Outstanding exceeds policy limit |
| 5 | `NET_DEBT_EBITDA` | Net Debt/EBITDA leverage ratio breached |
| 6 | `OVERDUE_AP` | Accounts Payable overdue beyond payment terms |
| 7 | `LARGE_GL_POSTING` | Single GL posting exceeds materiality threshold |
| 8 | `NEGATIVE_GROWTH` | Revenue growth turns negative year-over-year |
| 9 | `EBITDA_VS_BUDGET` | EBITDA deviates from budget beyond tolerance |
| 10 | `CASH_RUNWAY` | Projected cash runway falls below minimum months |

### 5.3 Bicep Infrastructure Modules

| Module File | Resources Provisioned | Deployment Stage |
|---|---|---|
| `main.bicep` | Orchestrator — references all 6 modules | Entry point |
| `keyvault.bicep` | Azure Key Vault Premium (HSM-backed) | Stage 1 |
| `loganalytics.bicep` | Log Analytics Workspace | Stage 2 |
| `storage.bicep` | ADLS Gen2 (5 containers: bronze, silver, gold, config, audit) | Stage 3 (parallel) |
| `adf.bicep` | Azure Data Factory + linked services + pipelines | Stage 3 (parallel) |
| `databricks.bicep` | Azure Databricks Premium (VNet-injected in prod) | Stage 3 (parallel) |
| `synapse.bicep` | Synapse Analytics workspace + dedicated SQL pool | Stage 4 |
| `openai.bicep` | Azure OpenAI service (gpt-4o + text-embedding-3-small) | Stage 5 |

### 5.4 ADF Pipelines and Linked Services

#### Pipelines

| Pipeline | Trigger | Schedule | Purpose |
|---|---|---|---|
| `pl_monthly_close` | Scheduled | T+0 to T+7h | Full monthly close sequence |
| `pl_nbh_fx_rate_load` | Scheduled | Daily 09:00 CET | Load NBH FX rates into `config.ref_fx_rates` |
| `pl_erp_extract` | Blob event | On file arrival | ERP data extraction from source systems |
| `pl_dq_validation` | On-demand / scheduled | Post-ingestion | Data quality validation across ingested datasets |
| A11 (commentary pipeline) | On-demand / scheduled | Post-close | Runs `commentary_generator.py` |
| A12 (anomaly pipeline) | On-demand / scheduled | Post-close | Runs `anomaly_detector.py` |

#### Linked Services

| Linked Service | Type | Authentication |
|---|---|---|
| `ls_adls_gen2` | ADLS Gen2 | Managed Identity |
| `ls_synapse_sql_pool` | Azure Synapse | Service Principal (KV: `adf-sp-client-secret`) |
| `ls_databricks` | Azure Databricks | PAT Token (KV secret: **`databricks-pat-token`**) |
| `ls_azure_key_vault` | Azure Key Vault | Managed Identity |
| `ls_azure_openai` | REST / Azure OpenAI | API Key (KV: `azure-openai-api-key`) |
| `ls_sftp_kulcssoft` | SFTP | Credentials in Key Vault |
| `ls_rest_cobalt` | REST | OAuth / credentials in Key Vault |
| `ls_rest_szamlazz` | REST | API Key in Key Vault |
| `ls_rest_sap_b1` | REST (SAP B1 Service Layer) | Credentials in Key Vault |

---

## 6. Environment Matrix

FIP is deployed across three environments, each with distinct infrastructure configuration and purpose.

| Property | `dev` | `ci` | `prod` |
|---|---|---|---|
| **Purpose** | Active development and feature testing | Ephemeral CI/CD validation (created per PR, destroyed on merge) | Production workloads serving live finance users |
| **Private Endpoints** | No | No | Yes — all services behind private endpoints |
| **Synapse SQL Pool SKU** | DW100c (auto-pause enabled) | DW100c (auto-pause enabled) | **DW1000c** (always on) |
| **Network Configuration** | Public access allowed | Public access allowed | Private DNS zones, VNet integration |
| **Databricks VNet Injection** | No | No | Yes |
| **Azure Region** | westeurope | westeurope | westeurope |
| **Log Analytics Retention** | 30 days | 7 days | 365 days |
| **Azure OpenAI** | Shared dev instance | Not provisioned | Dedicated prod instance |
| **Naming Prefix** | `fip-dev-` | `fip-ci-` | `fip-prod-` |
| **Storage Suffix** | `uniqueString(...)` appended | `uniqueString(...)` appended | `uniqueString(...)` appended |
| **Immutability Policy** | Not enforced | Not enforced | Enforced (Bronze: 2922 days) |
| **Alerting** | Disabled | Disabled | Enabled — Power Automate webhooks active |
| **Data Volume** | Synthetic / anonymised subset | Minimal fixture data | Full production GL history |

### Naming Convention

All Azure resources follow the pattern:

```
${projectCode}-${environment}-${resourceType}[-${suffix}]
```

Where:
- `projectCode` = `fip`
- `environment` = `dev` | `ci` | `prod`
- Storage accounts append a `uniqueString(resourceGroup().id)` suffix to ensure global uniqueness

---

## 7. Tech Stack

### Azure Services

| Service | Tier / SKU (Prod) | Purpose |
|---|---|---|
| **Azure Data Factory** | Standard | Orchestration: ingestion, close pipeline, FX rates, ERP extract |
| **Azure Data Lake Storage Gen2** | Standard LRS (dev) / ZRS (prod) | Medallion storage: bronze, silver, gold, config, audit containers |
| **Azure Databricks** | Premium (VNet-injected in prod) | dbt runner, Python AI workloads, notebook-based transformations |
| **Azure Synapse Analytics** | DW1000c dedicated pool (prod) | Data warehouse: all 6 SQL schemas, DirectQuery for Power BI |
| **Azure Key Vault** | Premium (HSM-backed) | Centralised secret management for all credentials |
| **Azure OpenAI Service** | gpt-4o (30K TPM), text-embedding-3-small (60K TPM) | Commentary generation, Q&A agent SQL generation, embeddings |
| **Azure Cognitive Search** | Standard | Vector index `fip-schema-index` for Q&A agent schema lookup |
| **Log Analytics Workspace** | PerGB2018 | Diagnostics aggregation for all Azure services; 365-day prod retention |
| **Power BI Premium** | Premium Per Capacity | DirectQuery over Synapse gold zone, dashboards, self-service analytics |
| **Power Automate** | Standard | Alert notifications, CFO notifications, anomaly webhooks |

### Open-Source Components and Versions

| Package | Version | Purpose |
|---|---|---|
| `azure-identity` | 1.17.0 | Managed Identity / Service Principal authentication |
| `azure-keyvault-secrets` | 4.8.0 | Key Vault secret retrieval in Python modules |
| `openai` | 1.30.0 | Azure OpenAI API client (commentary, Q&A, embeddings) |
| `pyodbc` | 5.1.0 | ODBC connectivity to Synapse SQL from Python |
| `pandas` | 2.2.0 | DataFrame manipulation in all Python modules |
| `prophet` | 1.1.5 | Primary time-series forecasting model in `financial_forecaster.py` |
| `scikit-learn` | 1.5.0 | IsolationForest anomaly detection in `anomaly_detector.py` |
| `scipy` | 1.13.0 | Z-score computation in `anomaly_detector.py` |
| `statsmodels` | 0.14.0 | ARIMA fallback forecasting in `financial_forecaster.py` |
| `fastapi` | latest | HTTP API framework for `financial_qa_agent.py` |
| `pydantic` | 2.7.0 | Data validation for FastAPI request/response models |
| `dbt-core` | latest compatible | SQL transformation framework |
| `dbt-synapse` | latest compatible | dbt adapter for Azure Synapse Analytics |

---

## 8. Repository Structure

```
FIP/                                        ← repository root
├── README.md                               ← this file
├── CODE_OF_CONDUCT.md
├── SECURITY.md
├── python-app.yml                          ← GitHub Actions CI workflow
├── FIP form_MasterGuide.docx
├── FIP_Data_Dictionary.xlsx
├── FIP_PowerBI_Implementation_Guid.docx
│
├── fip/
│   └── Infrastrucutre/                     ← note: folder name matches repo
│       └── bicep/
│           ├── main.bicep                  ← root Bicep orchestrator
│           ├── Modules/
│           │   ├── keyvault.bicep
│           │   ├── loganalytics.bicep
│           │   ├── storage.bicep
│           │   ├── adf.bicep
│           │   ├── databricks.bicep
│           │   ├── synapse.bicep
│           │   └── openai.bicep
│           └── Parameters/
│               ├── dev.bicepparam
│               ├── ci.bicepparam
│               └── prod.bicepparam
│
├── sql/
│   ├── 01_config/
│   │   └── fip_schema_config.sql           ← schema execution order: 1
│   ├── 02_audit/
│   │   └── fip_schema_audit.sql            ← schema execution order: 2
│   ├── 03_bronze/
│   │   └── fip_schema_bronze.sql           ← schema execution order: 3
│   ├── 04_silver/
│   │   └── fip_schema_silver.sql           ← schema execution order: 4
│   ├── 05_budget/
│   │   └── fip_schema_budget.sql           ← schema execution order: 5
│   ├── 06_gold/
│   │   └── fip_schema_gold.sql             ← schema execution order: 6
│   └── fip_stored_procedures.sql
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── Macros/
│   │   ├── currency_convert.sql
│   │   └── fiscal_period.sql
│   ├── Models/
│   │   ├── staging/
│   │   │   ├── stg_gl_transactions.sql
│   │   │   ├── stg_budget.sql
│   │   │   ├── stg_balance_sheet.sql
│   │   │   └── staging_schema.yml
│   │   ├── intermediate/
│   │   │   ├── int_coa_mapped.sql
│   │   │   ├── int_fx_converted.sql
│   │   │   ├── int_ic_elimination.sql
│   │   │   ├── intermediate_schema.yml
│   │   │   └── IC_ELIMINATION_IMPLEMENTATION_REPORT.md
│   │   └── gold/
│   │       ├── fct_gl_transaction.sql
│   │       ├── agg_pl_monthly.sql
│   │       ├── agg_balance_sheet.sql
│   │       ├── agg_cashflow.sql
│   │       ├── kpi_profitability.sql
│   │       ├── kpi_liquidity.sql
│   │       ├── kpi_cashflow.sql
│   │       ├── kpi_project.sql
│   │       └── gold_schema.yml
│   ├── Seeds/
│   │   ├── ref_coa_mapping.csv
│   │   ├── ref_intercompany_pairs.csv
│   │   ├── ref_coa_mapping.xlsx
│   │   └── Magyar_Szamlatukur_Teljes.xlsx
│   ├── Snapshots/
│   │   └── scd_coa_mapping.sql
│   └── Tests/
│       ├── assert_balance_sheet_balances.sql
│       ├── assert_budget_variance_bounds.sql
│       ├── assert_coa_mapping_coverage.sql
│       ├── assert_fx_rates_available.sql
│       ├── assert_ic_elimination_balances.sql
│       ├── assert_no_duplicate_postings.sql
│       ├── assert_no_duplicate_source_ids.sql
│       ├── assert_no_excessive_late_entries.sql
│       ├── assert_no_zero_amount_transactions.sql
│       ├── assert_revenue_accounts_no_debit_balance.sql
│       ├── assert_valid_currency_codes.sql
│       ├── assert_valid_posting_dates.sql
│       ├── test_int_coa_mapped_normal_balance.sql
│       ├── test_int_coa_mapped_signed_amount.sql
│       ├── test_int_fx_converted_eur_direction.sql
│       ├── test_int_fx_converted_huf_lcy_passthrough.sql
│       └── test_int_ic_elimination_group_revenue_zero.sql
│
├── python/
│   ├── requirements.txt
│   ├── requirements-test.txt
│   ├── db_utils.py
│   ├── tenant_config.py
│   ├── tenant_middleware.py
│   ├── tenant_router.py
│   ├── tenant_secured_qa_agent.py
│   ├── TENANT_AWARE_ROUTING.md
│   ├── Anomaly_detection/
│   │   └── anomaly_detector.py
│   ├── Commentary/
│   │   └── commentary_generator.py
│   ├── Forecasting/
│   │   └── financial_forecaster.py
│   ├── Rag/
│   │   └── financial_qa_agent.py
│   ├── tools/
│   │   └── generate_data_dictionary.py
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py
│       ├── test_anomaly_detector.py
│       ├── test_tenant_config.py
│       └── test_tenant_router.py
│
├── adf/
│   ├── Pipeline/
│   │   ├── pl_monthly_close.json
│   │   ├── pl_erp_extract.json
│   │   ├── pl_dq_validation.json
│   │   └── azure-pipelines.yml
│   ├── Linked_services/
│   │   ├── ls_adls_gen2.json
│   │   ├── ls_azure_key_vault.json
│   │   ├── ls_azure_openai.json
│   │   ├── ls_databricks.json
│   │   ├── ls_rest_cobalt.json
│   │   ├── ls_rest_sap_b1.json
│   │   ├── ls_rest_szamlazz.json
│   │   ├── ls_sftp_kulcssoft.json
│   │   └── ls_synapse_sql_pool.json
│   ├── Triggers/
│   │   ├── trg_monthly_close.json
│   │   └── trg_fx_rates_daily.json
│   └── Dataset/
│       ├── ds_adls_bronze_landing.json
│       ├── ds_adls_manual_upload_zone.json
│       ├── ds_bronze_landing_folder.json
│       ├── ds_rest_cobalt_api.json
│       ├── ds_rest_sap_b1_api.json
│       ├── ds_rest_szamlazz_api.json
│       ├── ds_sftp_erp_source.json
│       ├── ds_synapse_audit.json
│       ├── ds_synapse_bronze.json
│       └── ds_synapse_config.json
│
├── PowerBI/
│   ├── FIP_PowerBI_Template.pbit
│   ├── DASHBOARD_SPECIFICATIONS.md
│   ├── REPORT_SOURCE_OF_TRUTH.md
│   ├── Dax_measures/
│   │   ├── FIP_DAX_Measures.dax
│   │   └── FIP_DAX_Measures_TMSL.json
│   ├── Dax tests/
│   ├── Cicd/
│   └── Rls/
│       ├── RLS_ARCHITECTURE.md
│       ├── rls_roles.json
│       ├── monthly_close_rls_activity_fragment.json
│       ├── pl_rls_sync_adf_pipeline.json
│       ├── rls_sync_azure_function.py
│       └── sync_rls_aad.py
│
├── prompts/
│   ├── system_prompt_cfo_commentary.txt
│   ├── system_prompt_ceo_commentary.txt
│   ├── system_prompt_board_commentary.txt
│   ├── system_prompt_investor_commentary.txt
│   ├── system_prompt_hu_translation.txt
│   └── input_validation.txt
│
├── Remediation/
│   ├── Adf dataset/
│   ├── Dbt test/
│   └── Devops/
│
└── doc/
    ├── README.md
    ├── DATA_DICTIONARY_GENERATION.md
    ├── RUNBOOKS_AND_TROUBLES.md
    ├── SCHEMA_DATA_DICTIONARY.md
    ├── SECURITY_AND_COMPLIANCE.md
    ├── SETUP_AND_DEPLOYMENT.md
    └── Incident response/
        ├── IR-001_data_pipeline_failure.md
        └── IR-002_fx_rate_missing.md
```

---

## 9. Getting Started Overview

### Prerequisites

Before deploying FIP, ensure the following tools are installed and authenticated:

```bash
# Verify tool versions
python --version          # Requires 3.11+
az --version              # Azure CLI 2.55+
az bicep version          # Bicep CLI 0.24+
databricks --version      # Databricks CLI 0.200+
dbt --version             # dbt-core + dbt-synapse
```

### High-Level Deployment Sequence

1. **Clone the repository** and review `infrastructure/bicep/parameters/` for your target environment.
2. **Deploy Azure infrastructure** using Bicep in the correct module order (Key Vault first, OpenAI last). See `doc/SETUP_AND_DEPLOYMENT.md` for the full procedure.
3. **Populate Key Vault secrets** (passwords, PAT tokens, API keys, webhook URLs).
4. **Configure Databricks secret scope** backed by Key Vault (`fip-kv`).
5. **Deploy SQL schemas** in the mandatory execution order: config → audit → bronze → silver → budget → gold.
6. **Initialise and run dbt** models against the Synapse dedicated pool.
7. **Activate ADF pipelines and triggers.**
8. **Run smoke tests** to verify end-to-end data flow.
9. **Configure Power BI** DirectQuery connection to Synapse gold zone views.

For the complete step-by-step guide, refer to `doc/SETUP_AND_DEPLOYMENT.md`.

---

## 10. Key Terminology and Conventions

The following terminology is used consistently throughout all FIP documentation, code, and configuration. Deviations from these conventions should be treated as defects.

| Term | Definition | Usage Context |
|---|---|---|
| `entity_code` | Business key for a legal entity (VARCHAR(20)) | Silver `dim_entity`, Gold all tables. **Never** use `company_id` as a column name in silver or gold schemas. |
| `period_key` | Integer period identifier in YYYYMM format | Gold zone only (PK on aggregate tables, CLI argument for Python modules) |
| `period_id` | Integer period identifier | Audit, config, and budget schemas; Silver `fact_gl_transaction` |
| `databricks-pat-token` | Canonical Key Vault secret name | ADF linked service `ls_databricks` reads this secret name. **Do not** use `databricks-token` (placeholder name used only during Bicep secret initialisation) |
| `balance_check` | Generated column in `gold.agg_balance_sheet` = `assets - liabilities` | Must equal zero; monitored by `proc_evaluate_alerts()`. **Never** called `is_balanced`. |
| `company_id` | Legacy column name — only appears as FK in `silver.account_master` | No other silver or gold table uses `company_id` |
| `fip-kv` | Databricks secret scope name | Maps to Azure Key Vault; used in all Databricks notebooks |
| `fip-schema-index` | Azure Cognitive Search index name | Used by `financial_qa_agent.py` for schema vector search |
| Medallion layers | Bronze (raw) → Silver (conformed) → Gold (business-ready) | All data lineage documentation |
| `pl_monthly_close` | Primary ADF monthly close pipeline | T+0 to T+7h scheduled window |
| A11 | ADF pipeline alias for commentary generator | Runs `commentary_generator.py` |
| A12 | ADF pipeline alias for anomaly detector | Runs `anomaly_detector.py` |

---

*This document is part of the FIP Documentation Suite. For schema and data dictionary details, see `doc/SCHEMA_DATA_DICTIONARY.md`. For deployment instructions, see `doc/SETUP_AND_DEPLOYMENT.md`.*
