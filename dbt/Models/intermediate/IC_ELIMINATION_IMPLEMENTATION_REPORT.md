# Intercompany Implementation Summary
## Financial Intelligence Platform (FIP) · Phase 2 Roadmap Document

**Document version:** 1.0  
**Date:** 2026-04-17  
**Status:** Phase 1 Complete / Phase 2 Pending  
**Audience:** Data Engineering, Finance Architecture, Group Consolidation Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current State — Phase 1 Artefacts](#2-current-state--phase-1-artefacts)
3. [Data Model: Entity Master and Hierarchy](#3-data-model-entity-master-and-hierarchy)
4. [config.ref_intercompany_pairs — Structure and Seeding](#4-configref_intercompany_pairs--structure-and-seeding)
5. [Intercompany Flags Across Layers](#5-intercompany-flags-across-layers)
6. [Period Key Conventions](#6-period-key-conventions)
7. [GAP K-05: IC Elimination Not Implemented](#7-gap-k-05-ic-elimination-not-implemented)
8. [Consolidation Group Handling](#8-consolidation-group-handling)
9. [Phase 2 Implementation Roadmap](#9-phase-2-implementation-roadmap)
10. [Proposed Gold-Layer Consolidated Tables](#10-proposed-gold-layer-consolidated-tables)
11. [IC Elimination dbt Model Design](#11-ic-elimination-dbt-model-design)
12. [Testing and Validation Strategy](#12-testing-and-validation-strategy)
13. [Risk Register](#13-risk-register)

---

## 1. Executive Summary

The Financial Intelligence Platform's intercompany (IC) framework has been **partially implemented** in Phase 1. The configuration schema (`config.ref_intercompany_pairs`, `config.ref_entity_master`) is fully defined and seeded. Reference flags exist at Silver and config layers. However, the Gold zone currently contains **no consolidated group-level tables**, and the IC elimination logic has not been built into any dbt model or DDL. This is documented as **GAP K-05** in the Silver dimension comment.

The practical consequence is that any multi-entity aggregation performed against the current Gold zone (`gold.agg_pl_monthly`, `gold.kpi_profitability`, etc.) will **double-count intercompany revenues and expenses** unless the consuming query explicitly excludes IC transactions. Phase 2 must close this gap before consolidated group reporting can be trusted for statutory or management consolidation purposes.

---

## 2. Current State — Phase 1 Artefacts

### What IS implemented (Phase 1 complete)

| Layer | Artefact | Status |
|-------|----------|--------|
| Config | `config.ref_entity_master` | Defined, seeded |
| Config | `config.ref_intercompany_pairs` | Defined, seeded |
| Config | `config.ref_coa_mapping.is_intercompany` | Column present |
| Silver | `silver.dim_entity` | Defined; hierarchy columns present |
| Silver | `silver.fact_gl_transaction.is_intercompany` | Flag column present |
| Silver | `silver.dim_account.is_intercompany` | Flag column present |
| Gold | `gold.agg_pl_monthly` | Entity-level only; no group rollup |
| Gold | `gold.agg_balance_sheet` | Entity-level only; no group rollup |
| Gold | `gold.kpi_*` views | Entity-level only |

### What is NOT implemented (Phase 2 required)

| Gap ID | Description | Impact |
|--------|-------------|--------|
| K-05 | IC elimination logic in dbt / DDL | HIGH — double-counting in group aggregates |
| — | `gold.agg_consolidated_pl` (group-level table) | No statutory consolidation output |
| — | `gold.agg_consolidated_balance_sheet` | No group balance sheet |
| — | `gold.kpi_consolidated_*` views | No group KPI dashboard feed |
| — | Minority interest calculation | Required for FULL consolidation method |

---

## 3. Data Model: Entity Master and Hierarchy

### 3.1 config.ref_entity_master

This is the authoritative source for all legal entities in the platform. It drives both the Silver dimension and the Gold aggregation keys.

```sql
CREATE TABLE config.ref_entity_master (
    entity_id             UUID          NOT NULL DEFAULT gen_random_uuid(),
    entity_code           VARCHAR(20)   NOT NULL,           -- Business key used in Silver/Gold
    entity_name           VARCHAR(255)  NOT NULL,
    legal_entity_type     VARCHAR(30)   NOT NULL,           -- 'Kft'|'Zrt'|'Nyrt'|'Bt'|'Kkt'|'Egyéni_vállalkozó'
    tax_id                VARCHAR(13)   NOT NULL,           -- Hungarian adószám: 8+1+2 format (e.g. 12345678-1-42)
    consolidation_group   VARCHAR(50)   NULL,               -- Groups entities for consolidation rollup
    parent_entity_id      UUID          NULL,               -- Self-referencing FK for ownership hierarchy
    consolidation_method  VARCHAR(20)   NOT NULL DEFAULT 'NONE', -- 'FULL'|'EQUITY'|'PROPORTIONAL'|'NONE'
    is_active             BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_ref_entity_master PRIMARY KEY (entity_id),
    CONSTRAINT uq_ref_entity_master_code UNIQUE (entity_code),
    CONSTRAINT fk_ref_entity_master_parent FOREIGN KEY (parent_entity_id)
        REFERENCES config.ref_entity_master (entity_id),
    CONSTRAINT chk_legal_entity_type CHECK (legal_entity_type IN (
        'Kft', 'Zrt', 'Nyrt', 'Bt', 'Kkt', 'Egyéni_vállalkozó'
    )),
    CONSTRAINT chk_consolidation_method CHECK (consolidation_method IN (
        'FULL', 'EQUITY', 'PROPORTIONAL', 'NONE'
    ))
);
```

**Key design decisions:**

- `entity_code` (not `company_id`) is the canonical business key used in all Silver and Gold layers. The config layer uses `entity_id` (UUID) as its primary key. The distinction matters for RLS joins — see GAP M-04 in RLS_ARCHITECTURE.md.
- `consolidation_group` is a free-text grouping field (VARCHAR 50) allowing ad-hoc group definitions without requiring a separate group master table. This is a Phase 1 simplification; Phase 2 may introduce a `config.ref_consolidation_group_master` if group metadata (currency, reporting standard) is required.
- `consolidation_method` drives the elimination logic: FULL consolidation requires 100% elimination of IC balances and a minority interest calculation; EQUITY method records only the net investment; PROPORTIONAL eliminates only the ownership-share portion.
- `parent_entity_id` supports unlimited depth hierarchies (tree structure), but the current dbt models do not traverse more than two levels. Deep hierarchies (>3 levels) should be flattened during Phase 2.

### 3.2 silver.dim_entity — Dimensional Representation

The Silver dimension denormalises the config master into a surrogate-key based structure suitable for fact table joins.

```sql
CREATE TABLE silver.dim_entity (
    entity_key            INT           NOT NULL GENERATED ALWAYS AS IDENTITY,
    entity_id             UUID          NOT NULL,           -- FK → config.ref_entity_master.entity_id
    entity_code           VARCHAR(20)   NOT NULL,           -- Propagated from config; used in Gold joins
    entity_name           VARCHAR(255)  NOT NULL,
    consolidation_group   VARCHAR(50)   NULL,
    parent_entity_key     INT           NULL,               -- Self-ref to dim_entity (surrogate)
    consolidation_method  VARCHAR(20)   NOT NULL,
    legal_entity_type     VARCHAR(30)   NOT NULL,
    is_current            BOOLEAN       NOT NULL DEFAULT TRUE,
    valid_from            DATE          NOT NULL,
    valid_to              DATE          NULL,

    CONSTRAINT pk_dim_entity PRIMARY KEY (entity_key),
    CONSTRAINT uq_dim_entity_code UNIQUE (entity_code),
    CONSTRAINT fk_dim_entity_config FOREIGN KEY (entity_id)
        REFERENCES config.ref_entity_master (entity_id),
    CONSTRAINT fk_dim_entity_parent FOREIGN KEY (parent_entity_key)
        REFERENCES silver.dim_entity (entity_key)
);

/*
 * COMMENT ON TABLE silver.dim_entity:
 * K-05 GAP: Intercompany (IC) elimination logic is NOT implemented in the current
 * DDL or dbt models. This is a Phase 2 item. Multi-entity aggregation performed
 * against any Gold zone table that references entity_key WILL double-count
 * intercompany revenues and expenses until elimination models are deployed.
 * Any consolidated group report produced before Phase 2 close must include
 * a manual disclaimer and should be reconciled against external consolidation tools.
 */
```

**Critical note on `parent_entity_key`:** the self-referencing FK in `silver.dim_entity` mirrors `parent_entity_id` in `config.ref_entity_master` but uses the surrogate integer key. This enables efficient recursive CTE queries in Synapse for hierarchy traversal. However, changes to the entity hierarchy in `config.ref_entity_master` require a full reload of `dim_entity` to maintain surrogate key integrity.

---

## 4. config.ref_intercompany_pairs — Structure and Seeding

This table defines the explicit intercompany transaction relationships between entities: which seller account maps to which buyer account, and what elimination treatment applies.

```sql
CREATE TABLE config.ref_intercompany_pairs (
    pair_id               SERIAL        NOT NULL,
    seller_entity_id      UUID          NOT NULL,           -- FK → config.ref_entity_master.entity_id
    buyer_entity_id       UUID          NOT NULL,           -- FK → config.ref_entity_master.entity_id
    seller_account_code   VARCHAR(20)   NOT NULL,           -- CoA account on seller side
    buyer_account_code    VARCHAR(20)   NOT NULL,           -- CoA account on buyer side
    transaction_type      VARCHAR(30)   NOT NULL,
    elimination_type      VARCHAR(30)   NOT NULL,
    description           VARCHAR(500)  NULL,
    is_active             BOOLEAN       NOT NULL DEFAULT TRUE,
    effective_from        DATE          NOT NULL,
    effective_to          DATE          NULL,

    CONSTRAINT pk_ref_intercompany_pairs PRIMARY KEY (pair_id),
    CONSTRAINT fk_ic_pairs_seller FOREIGN KEY (seller_entity_id)
        REFERENCES config.ref_entity_master (entity_id),
    CONSTRAINT fk_ic_pairs_buyer FOREIGN KEY (buyer_entity_id)
        REFERENCES config.ref_entity_master (entity_id),
    CONSTRAINT chk_ic_transaction_type CHECK (transaction_type IN (
        'GOODS_SALE', 'SERVICE', 'LOAN', 'DIVIDEND', 'MANAGEMENT_FEE'
    )),
    CONSTRAINT chk_ic_elimination_type CHECK (elimination_type IN (
        'REVENUE_EXPENSE', 'RECEIVABLE_PAYABLE', 'INVESTMENT_EQUITY'
    ))
);
```

### Transaction Types and Their Elimination Treatment

| `transaction_type` | Typical `elimination_type` | Gold Layer Impact |
|--------------------|-----------------------------|-------------------|
| `GOODS_SALE` | `REVENUE_EXPENSE` | Eliminate IC revenue in seller P&L, eliminate IC COGS in buyer P&L |
| `SERVICE` | `REVENUE_EXPENSE` | Same as goods sale; applies to management fees booked as service income |
| `LOAN` | `RECEIVABLE_PAYABLE` | Eliminate IC receivable in lender balance sheet, IC payable in borrower balance sheet; eliminate IC interest income/expense |
| `DIVIDEND` | `INVESTMENT_EQUITY` | Eliminate dividend income in parent, eliminate distribution from subsidiary equity |
| `MANAGEMENT_FEE` | `REVENUE_EXPENSE` | Eliminate management fee income in parent, eliminate management fee expense in subsidiary |

### Seeded Reference Data (Example Pairs)

```sql
-- Example seed data illustrating the pattern
INSERT INTO config.ref_intercompany_pairs
    (seller_entity_id, buyer_entity_id, seller_account_code, buyer_account_code,
     transaction_type, elimination_type, description, effective_from)
VALUES
    -- Parent provides management services to subsidiary
    ('uuid-parent-001', 'uuid-sub-001', '9610', '8610',
     'MANAGEMENT_FEE', 'REVENUE_EXPENSE',
     'Parent → Sub-001 management fee monthly allocation', '2024-01-01'),

    -- Inter-subsidiary goods sale
    ('uuid-sub-001', 'uuid-sub-002', '9100', '8100',
     'GOODS_SALE', 'REVENUE_EXPENSE',
     'Sub-001 sells finished goods to Sub-002 distribution entity', '2024-01-01'),

    -- Parent loan to subsidiary
    ('uuid-parent-001', 'uuid-sub-001', '1680', '4580',
     'LOAN', 'RECEIVABLE_PAYABLE',
     'Intercompany loan facility — parent to Sub-001', '2024-01-01');
```

---

## 5. Intercompany Flags Across Layers

The `is_intercompany` boolean flag is propagated through multiple layers of the platform. Consistent flagging is essential for Phase 2 elimination logic.

### 5.1 silver.fact_gl_transaction

```sql
-- Relevant columns (not exhaustive)
is_intercompany     BOOLEAN   NOT NULL DEFAULT FALSE,
-- Set to TRUE during Silver load when the GL posting's account code
-- matches a seller_account_code or buyer_account_code in config.ref_intercompany_pairs
-- for the posting entity.
ic_pair_id          INT       NULL,
-- FK → config.ref_intercompany_pairs.pair_id (populated when is_intercompany = TRUE)
counterparty_entity_key  INT  NULL,
-- FK → silver.dim_entity.entity_key (the other party in the IC transaction)
```

**Flagging logic** (Silver ETL / dbt staging model):

```sql
-- dbt staging logic (pseudo-code representation)
WITH ic_lookup AS (
    SELECT
        p.pair_id,
        p.seller_entity_id,
        p.buyer_entity_id,
        p.seller_account_code,
        p.buyer_account_code
    FROM config.ref_intercompany_pairs p
    WHERE p.is_active = TRUE
        AND p.effective_from <= CURRENT_DATE
        AND (p.effective_to IS NULL OR p.effective_to >= CURRENT_DATE)
)
SELECT
    gl.*,
    CASE
        WHEN ic.pair_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_intercompany,
    ic.pair_id AS ic_pair_id
FROM raw.gl_transactions gl
LEFT JOIN ic_lookup ic
    ON  gl.entity_id IN (ic.seller_entity_id, ic.buyer_entity_id)
    AND gl.account_code IN (ic.seller_account_code, ic.buyer_account_code)
```

### 5.2 silver.dim_account

```sql
is_intercompany     BOOLEAN   NOT NULL DEFAULT FALSE,
-- Account-level flag: TRUE when the account is exclusively or typically
-- used for intercompany transactions (based on config.ref_coa_mapping)
```

### 5.3 config.ref_coa_mapping

```sql
is_intercompany     BOOLEAN   NOT NULL DEFAULT FALSE,
-- Chart-of-accounts mapping flag: set during CoA configuration
-- when the account code range represents IC clearing accounts
```

### Consistency Rule

All three `is_intercompany` flags must be consistent. A transaction should be flagged at the fact level if either:
- The account has `dim_account.is_intercompany = TRUE`, OR
- The transaction matches an active pair in `config.ref_intercompany_pairs`

Discrepancies between account-level and transaction-level flags should be logged as data quality warnings in `audit.dq_issue_log`.

---

## 6. Period Key Conventions

FIP uses two different integer representations of accounting periods depending on context. This distinction is intentional and must be preserved.

| Key Name | Data Type | Format | Used In | Example |
|----------|-----------|--------|---------|---------|
| `period_key` | INT | YYYYMM | Gold zone fact/agg tables, all KPI views | `202403` = March 2024 |
| `period_id` | INT | YYYYMM (same format) | Config tables, audit tables, budget tables | `202403` = March 2024 |

**Why two names?** The naming convention reflects the layer convention: surrogate keys in dimensional models use the `_key` suffix (even when they are not purely opaque); configuration and audit records use `_id` to indicate they reference a business-meaningful identifier within that domain. The underlying integer format (YYYYMM) is identical.

### Period key usage in intercompany context

```sql
-- Gold zone: IC elimination journal would use period_key
INSERT INTO gold.agg_ic_eliminations (period_key, entity_key, ...)
SELECT
    f.period_key,   -- YYYYMM INT
    ...
FROM silver.fact_gl_transaction f
WHERE f.is_intercompany = TRUE;

-- Config/audit: IC pair configuration versioning uses period_id
INSERT INTO audit.ic_elimination_log (period_id, pair_id, ...)
SELECT
    f.period_key AS period_id,   -- same value, different semantic name
    ...
```

---

## 7. GAP K-05: IC Elimination Not Implemented

### Official Gap Description

> **K-05 (HIGH SEVERITY):** Intercompany (IC) elimination logic is **NOT implemented** in the current DDL or dbt models. This is a Phase 2 item. Multi-entity aggregation performed against any Gold zone table that references `entity_key` **will double-count** intercompany revenues and expenses until elimination models are deployed.

This gap is formally documented in the SQL comment on `silver.dim_entity`. It must appear on every consolidated report produced before Phase 2 closure.

### Specific Double-Counting Scenarios

**Scenario 1: Management fee — Revenue double-count**

| Entity | P&L Line | Amount (HUF) | Without IC Elimination |
|--------|----------|--------------|------------------------|
| Parent | Management fee income | +50,000,000 | Counted |
| Subsidiary | Management fee expense | −50,000,000 | Counted |
| **Group total** | **Net IC impact** | **0** | **Revenue overstated by 50M** |

**Scenario 2: Inter-subsidiary goods sale — Revenue and COGS**

| Entity | P&L Line | Amount (HUF) |
|--------|----------|--------------|
| Sub-001 | Revenue (IC goods sale) | +120,000,000 |
| Sub-002 | COGS (IC goods purchase) | −100,000,000 |
| **Unrealised margin** | **Retained in group inventory** | **+20,000,000** |

Without elimination: group revenue is overstated by 120M; group COGS is understated by 100M; group inventory includes an unrealised 20M margin.

**Scenario 3: IC loan — Balance sheet**

| Entity | B/S Line | Amount (HUF) |
|--------|----------|--------------|
| Parent | IC receivable (other receivables) | +500,000,000 |
| Subsidiary | IC payable (other payables) | +500,000,000 |
| **Group B/S impact** | **Inflated by** | **+1,000,000,000** |

---

## 8. Consolidation Group Handling

### consolidation_group Field

The `consolidation_group` VARCHAR(50) field in both `config.ref_entity_master` and `silver.dim_entity` provides a flat grouping mechanism. Entities with the same `consolidation_group` value form a consolidation perimeter.

**Current usage pattern:**

```sql
-- Querying all entities within a consolidation group
SELECT
    e.entity_code,
    e.entity_name,
    e.consolidation_method,
    e.parent_entity_key
FROM silver.dim_entity e
WHERE e.consolidation_group = 'HU_GROUP_ALPHA'
  AND e.is_current = TRUE
ORDER BY e.parent_entity_key NULLS FIRST, e.entity_key;
```

**Consolidation method matrix:**

| `consolidation_method` | Ownership % | Revenue Elimination | B/S Elimination | Minority Interest |
|------------------------|-------------|---------------------|-----------------|-------------------|
| `FULL` | >50% | 100% | 100% | Yes (if <100%) |
| `PROPORTIONAL` | Any JV % | Ownership % share | Ownership % share | No |
| `EQUITY` | 20–50% | None | Replace investment with net asset share | No |
| `NONE` | <20% | None | None | No |

### Phase 1 Limitation: Flat Group Only

The current `consolidation_group` field does not store:
- Group ownership percentage
- Reporting currency for the group
- Consolidation standard (HU GAAP vs IFRS)
- Minority ownership percentages at each node

These attributes are needed for Phase 2 full consolidation and will require either extending `config.ref_entity_master` or introducing a `config.ref_consolidation_group_master` table.

---

## 9. Phase 2 Implementation Roadmap

### 9.1 Milestone Overview

| Milestone | Description | Estimated Effort | Priority |
|-----------|-------------|-----------------|----------|
| P2-M1 | Design `gold.agg_ic_eliminations` staging table | 3 days | HIGH |
| P2-M2 | Build dbt IC elimination model | 5 days | HIGH |
| P2-M3 | Create `gold.agg_consolidated_pl` | 3 days | HIGH |
| P2-M4 | Create `gold.agg_consolidated_balance_sheet` | 3 days | HIGH |
| P2-M5 | Build `gold.kpi_consolidated_*` views | 4 days | MEDIUM |
| P2-M6 | Minority interest calculation logic | 5 days | MEDIUM |
| P2-M7 | Power BI consolidated dashboard integration | 3 days | MEDIUM |
| P2-M8 | Audit and DQ validation for IC eliminations | 2 days | HIGH |
| P2-M9 | Close GAP K-05 — update dim_entity comment | 0.5 days | LOW |

**Total estimated effort:** ~29 developer-days

### 9.2 Prerequisites Before Phase 2 Start

1. Confirm all entities and IC pairs are fully seeded in `config.ref_entity_master` and `config.ref_intercompany_pairs`.
2. Validate that `silver.fact_gl_transaction.is_intercompany` flags are accurate (run the IC flag consistency check in Section 5).
3. Confirm consolidation group assignments are complete and `consolidation_method` is set correctly for all entities.
4. Decide on minority interest handling approach (separate equity line vs. off-statement).
5. Get sign-off from group Finance Controller on the elimination methodology.

### 9.3 Dependency Graph

```
config.ref_intercompany_pairs (seeded)
        │
        ▼
silver.fact_gl_transaction (is_intercompany flagged)
        │
        ▼
[P2-M1] gold.agg_ic_eliminations (elimination journal entries)
        │
        ├──► [P2-M3] gold.agg_consolidated_pl
        ├──► [P2-M4] gold.agg_consolidated_balance_sheet
        │
        ▼
[P2-M5] gold.kpi_consolidated_profitability / kpi_consolidated_liquidity
        │
        ▼
[P2-M7] Power BI Consolidated Executive Dashboard
```

---

## 10. Proposed Gold-Layer Consolidated Tables

### 10.1 gold.agg_ic_eliminations

```sql
CREATE TABLE gold.agg_ic_eliminations (
    elimination_id        BIGSERIAL     NOT NULL,
    period_key            INT           NOT NULL,           -- YYYYMM
    pair_id               INT           NOT NULL,           -- FK → config.ref_intercompany_pairs
    consolidation_group   VARCHAR(50)   NOT NULL,
    seller_entity_key     INT           NOT NULL,
    buyer_entity_key      INT           NOT NULL,
    elimination_type      VARCHAR(30)   NOT NULL,
    pl_line_item          VARCHAR(100)  NULL,               -- P&L line affected (for REVENUE_EXPENSE)
    bs_line_item          VARCHAR(100)  NULL,               -- B/S line affected (for RECEIVABLE_PAYABLE)
    elimination_amount    NUMERIC(20,2) NOT NULL,
    currency_code         VARCHAR(3)    NOT NULL DEFAULT 'HUF',
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    dbt_run_id            VARCHAR(50)   NULL,

    CONSTRAINT pk_agg_ic_eliminations PRIMARY KEY (elimination_id),
    CONSTRAINT uq_ic_elimination UNIQUE (period_key, pair_id, pl_line_item, bs_line_item)
);
```

### 10.2 gold.agg_consolidated_pl (Proposed DDL)

```sql
CREATE TABLE gold.agg_consolidated_pl (
    period_key                    INT           NOT NULL,   -- YYYYMM
    consolidation_group           VARCHAR(50)   NOT NULL,
    -- Aggregated P&L lines (post-elimination)
    revenue                       NUMERIC(20,2),
    revenue_ic_eliminated         NUMERIC(20,2),           -- Amount eliminated
    cost_of_goods_sold            NUMERIC(20,2),
    gross_profit                  NUMERIC(20,2),
    operating_expenses            NUMERIC(20,2),
    ebitda                        NUMERIC(20,2),
    depreciation_amortisation     NUMERIC(20,2),
    ebit                          NUMERIC(20,2),
    interest_expense              NUMERIC(20,2),
    interest_expense_ic_elim      NUMERIC(20,2),
    profit_before_tax             NUMERIC(20,2),
    income_tax                    NUMERIC(20,2),
    net_profit                    NUMERIC(20,2),
    minority_interest             NUMERIC(20,2),
    net_profit_attributable       NUMERIC(20,2),           -- After minority interest
    -- Margins
    gross_margin_pct              NUMERIC(10,4),
    ebitda_margin_pct             NUMERIC(10,4),
    net_profit_margin_pct         NUMERIC(10,4),
    -- Metadata
    entity_count                  INT,                     -- Number of entities consolidated
    ic_pair_count                 INT,                     -- Number of IC pairs eliminated
    created_at                    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    dbt_run_id                    VARCHAR(50),

    CONSTRAINT pk_consolidated_pl PRIMARY KEY (period_key, consolidation_group)
);
```

---

## 11. IC Elimination dbt Model Design

### 11.1 Model: `gold_agg_ic_eliminations.sql`

```sql
-- dbt model: models/gold/gold_agg_ic_eliminations.sql
-- Description: Generates IC elimination journal entries by matching
--              intercompany transaction pairs in silver.fact_gl_transaction.
-- GAP K-05 resolution — Phase 2 deliverable.

{{
    config(
        materialized = 'incremental',
        unique_key   = ['period_key', 'pair_id', 'pl_line_item', 'bs_line_item'],
        on_schema_change = 'fail'
    )
}}

WITH ic_transactions AS (
    SELECT
        f.period_key,
        f.entity_key,
        f.ic_pair_id                    AS pair_id,
        f.account_key,
        a.is_intercompany,
        a.account_code,
        p.seller_entity_id,
        p.buyer_entity_id,
        p.elimination_type,
        p.transaction_type,
        SUM(f.amount_huf)               AS gross_amount,
        de_seller.entity_key            AS seller_entity_key,
        de_buyer.entity_key             AS buyer_entity_key,
        de_seller.consolidation_group
    FROM {{ ref('silver_fact_gl_transaction') }} f
    INNER JOIN {{ ref('config_ref_intercompany_pairs') }} p
        ON f.ic_pair_id = p.pair_id
       AND p.is_active = TRUE
    INNER JOIN {{ ref('silver_dim_account') }} a
        ON f.account_key = a.account_key
    INNER JOIN {{ ref('silver_dim_entity') }} de_seller
        ON p.seller_entity_id = de_seller.entity_id
    INNER JOIN {{ ref('silver_dim_entity') }} de_buyer
        ON p.buyer_entity_id = de_buyer.entity_id
    WHERE f.is_intercompany = TRUE
    {% if is_incremental() %}
      AND f.period_key >= {{ var('incremental_period_start') }}
    {% endif %}
    GROUP BY
        f.period_key, f.entity_key, f.ic_pair_id, f.account_key,
        a.is_intercompany, a.account_code, p.seller_entity_id, p.buyer_entity_id,
        p.elimination_type, p.transaction_type,
        de_seller.entity_key, de_buyer.entity_key, de_seller.consolidation_group
),

eliminations AS (
    SELECT
        period_key,
        pair_id,
        consolidation_group,
        seller_entity_key,
        buyer_entity_key,
        elimination_type,
        CASE elimination_type
            WHEN 'REVENUE_EXPENSE' THEN transaction_type
            ELSE NULL
        END                             AS pl_line_item,
        CASE elimination_type
            WHEN 'RECEIVABLE_PAYABLE' THEN 'IC_BALANCE'
            WHEN 'INVESTMENT_EQUITY'  THEN 'IC_INVESTMENT'
            ELSE NULL
        END                             AS bs_line_item,
        -- Elimination amount = matched IC transaction (netting seller + buyer)
        SUM(gross_amount)               AS elimination_amount,
        'HUF'                           AS currency_code,
        '{{ run_started_at }}'          AS dbt_run_id
    FROM ic_transactions
    GROUP BY
        period_key, pair_id, consolidation_group,
        seller_entity_key, buyer_entity_key, elimination_type,
        pl_line_item, bs_line_item
)

SELECT * FROM eliminations
```

### 11.2 dbt Tests for IC Elimination Model

```yaml
# schema.yml — gold_agg_ic_eliminations tests
models:
  - name: gold_agg_ic_eliminations
    description: >
      Intercompany elimination journal entries. Phase 2 deliverable resolving GAP K-05.
      Each row represents the net elimination amount for one IC pair in one period.
    columns:
      - name: elimination_id
        tests:
          - unique
          - not_null
      - name: period_key
        tests:
          - not_null
          - accepted_range:
              min_value: 202001
              max_value: 209912
      - name: elimination_amount
        tests:
          - not_null
          # IC eliminations must net to zero across the pair
          - dbt_utils.expression_is_true:
              expression: "ABS(elimination_amount) > 0"
    tests:
      # Seller and buyer elimination amounts must offset (net = 0 per pair per period)
      - dbt_utils.equality:
          compare_model: ref('gold_agg_ic_eliminations')
          compare_columns: ['period_key', 'pair_id']
```

---

## 12. Testing and Validation Strategy

### 12.1 IC Flag Consistency Check

Run before Phase 2 model deployment:

```sql
-- Identify transactions where account is IC-flagged but transaction is not flagged
SELECT
    f.gl_transaction_id,
    f.period_key,
    f.entity_key,
    a.account_code,
    a.is_intercompany AS account_flag,
    f.is_intercompany AS transaction_flag,
    f.amount_huf
FROM silver.fact_gl_transaction f
INNER JOIN silver.dim_account a ON f.account_key = a.account_key
WHERE a.is_intercompany = TRUE
  AND f.is_intercompany = FALSE
ORDER BY f.period_key DESC, ABS(f.amount_huf) DESC;
```

### 12.2 Elimination Completeness Check

```sql
-- For each active IC pair, verify both sides have been eliminated
WITH pair_coverage AS (
    SELECT
        e.period_key,
        e.pair_id,
        COUNT(DISTINCT e.seller_entity_key) AS seller_count,
        COUNT(DISTINCT e.buyer_entity_key)  AS buyer_count,
        SUM(e.elimination_amount)           AS net_elimination
    FROM gold.agg_ic_eliminations e
    WHERE e.period_key = :check_period_key
    GROUP BY e.period_key, e.pair_id
)
SELECT
    p.pair_id,
    p.transaction_type,
    pc.seller_count,
    pc.buyer_count,
    pc.net_elimination,
    CASE
        WHEN ABS(pc.net_elimination) > 1 THEN 'OUT_OF_BALANCE'
        ELSE 'OK'
    END AS status
FROM config.ref_intercompany_pairs p
LEFT JOIN pair_coverage pc ON p.pair_id = pc.pair_id
WHERE p.is_active = TRUE
ORDER BY status DESC;
```

---

## 13. Risk Register

| Risk ID | Description | Likelihood | Impact | Mitigation |
|---------|-------------|-----------|--------|------------|
| R-IC-01 | IC pair configuration incomplete — new transactions not flagged | MEDIUM | HIGH | Monthly IC pair review gate before period close |
| R-IC-02 | Deep entity hierarchy (>3 levels) not handled by elimination model | LOW | HIGH | Flatten hierarchy in dim_entity during Phase 2 |
| R-IC-03 | Currency mismatch in cross-currency IC transactions | MEDIUM | MEDIUM | All amounts stored in HUF base currency; FX rates applied at Silver load |
| R-IC-04 | Minority interest calculations incorrect | MEDIUM | HIGH | External validation against consolidation tool (e.g. Tagetik) |
| R-IC-05 | Reports consumed before Phase 2 close without IC disclaimer | HIGH | HIGH | Add warning to all multi-entity Gold queries via `audit.v_alert_summary` |
| R-IC-06 | GAP K-05 not formally closed — dim_entity comment not updated | LOW | LOW | Include comment update as P2-M9 milestone acceptance criterion |

---

*Document maintained by: FIP Data Engineering Team*  
*Next review: Phase 2 kickoff*  
*Related documents: RLS_ARCHITECTURE.md, DASHBOARD_SPECIFICATIONS.md, API_DOCUMENTATION.md*