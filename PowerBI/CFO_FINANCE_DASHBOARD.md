# CFO Finance Dashboard

**Persona:** CFO · **Dataset RLS Role:** `CFO` · **Workspace:** FIP-Production  
**Page Spec:** `PowerBI/Pages/CFO_FinanceDashboard_PageSpec.json`  
**Canvas Size:** 1920 × 1080 px (16:9, 100% scale)  
**Version:** 1.0 · **Last Updated:** 2026-04-25

---

## Purpose

The CFO Finance Dashboard provides the Chief Financial Officer (and Finance Director group) with a single, consolidated view of the organisation's financial health. It surfaces the key performance indicators, variance analysis, cash flow position, and entity-level P&L decomposition needed for executive decision-making under HU GAAP.

The page is designed for both **periodic reporting** (monthly close) and **ad-hoc investigation** via interactive slicer filtering by fiscal year, period, and legal entity.

---

## RLS and Access

| Attribute | Value |
|---|---|
| Dataset RLS Role | `CFO` |
| Entity scope | All entities (unrestricted — empty DAX filter per RLS matrix spec v1.1) |
| PII visible | No |
| AAD Groups | `FIP-Role-CFO`, `FIP-Role-FinanceDirector` |

See [`PowerBI/Rls/rls_roles.json`](Rls/rls_roles.json) and [`PowerBI/Rls/RLS_ARCHITECTURE.md`](Rls/RLS_ARCHITECTURE.md) for full role configuration.

---

## Page Layout Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  HEADER BAR — "CFO Finance Dashboard · {year} · HU GAAP · FIP v1.0" │
├────────────────────────────────────────────────────────────────────┤
│  Slicers: [Fiscal Year ▼]  [Period ▼]  [Legal Entity ▼]            │
├──────────┬──────────┬──────────┬──────────┬──────────┬─────────────┤
│ Revenue  │  EBITDA  │ EBITDA % │Net Profit│  Free CF │Current Ratio│
│  KPI Card│  KPI Card│  KPI Card│  KPI Card│  KPI Card│   KPI Card  │
├──────────────────────────────┬──────────────────────────────────────┤
│  Revenue vs Budget           │  P&L Bridge (Waterfall)              │
│  Monthly Trend Line Chart    │  Revenue → EBITDA → Net Profit       │
├──────────────────┬───────────┴──────────────┬───────────────────────┤
│ EBITDA Margin vs │  Cash Flow Components    │  DSO vs DPO           │
│ Revenue Growth   │  Clustered Bar Chart     │  Line Chart           │
│ Scatter Plot     │                          │                       │
├──────────────────┴──────────────────────────┴───────────────────────┤
│  Entity P&L Summary Matrix Table (full-width)                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## KPI Cards

Six KPI cards span the full width below the slicers. Each card applies conditional formatting driven by RAG status measures.

| Card | Measure | Format | Callout / Target | RAG Rule |
|---|---|---|---|---|
| **Revenue** | `[Revenue]` | `#,0,, " M HUF"` | `[Revenue Variance %]` vs `[Revenue Budget]` | GREEN ≥ 0%, AMBER -5%–0%, RED < -5% |
| **EBITDA** | `[EBITDA]` | `#,0,, " M HUF"` | `[EBITDA Variance]` vs budget | GREEN ≥ 0%, AMBER -5%–0%, RED < -5% |
| **EBITDA Margin %** | `[EBITDA Margin %]` | `0.0%` | Current period only | No RAG |
| **Net Profit** | `[Net Profit]` | `#,0,, " M HUF"` | HU GAAP mérleg szerinti eredmény | No RAG |
| **Free Cash Flow** | `[Free Cash Flow]` | `#,0,, " M HUF"` | Operating CF − Capex | No RAG |
| **Current Ratio** | `[Current Ratio]` | `0.00` | Target ≥ 1.5 | Font: RED < 1.0, AMBER 1.0–1.5, GREEN ≥ 1.5 |

---

## Charts

### 1. Revenue vs Budget — Monthly Trend (Line Chart)

- **X-axis:** `silver_dim_date[fiscal_period]`
- **Series 1:** `[Revenue]` — solid navy (#1B3A6B), 2px, markers enabled
- **Series 2:** `[Revenue Budget]` — dashed grey (#90A4AE), 1.5px, no markers
- **Average Line:** `[Revenue]` average across selection
- **Tooltip:** adds `[Revenue Variance]` and `[Revenue Variance %]`

Purpose: Reveals in-year revenue momentum and divergence from approved budget.

---

### 2. P&L Bridge — Revenue to Net Profit (Waterfall Chart)

Visualises the step-by-step build-up from gross revenue to net profit.

| Step | Label | Type |
|---|---|---|
| 1 | Revenue | Total |
| 2 | (COGS) | Decrease |
| 3 | Gross Profit | Total |
| 4 | Other OI | Increase |
| 5 | (Personnel) | Decrease |
| 6 | (Other OpEx) | Decrease |
| 7 | EBITDA | Total |
| 8 | (D&A) | Decrease |
| 9 | Fin. Items (net) | Increase/Decrease |
| 10 | (Tax) | Decrease |
| 11 | Net Profit | Total |

**Requires:** `_PLBridgeOrder` calculated table and `[PL Bridge Amount]` / `[PL Bridge Type]` measures (see Supporting DAX section).

Colors: Increase = #2E7D32, Decrease = #C62828, Total = #1B3A6B.

---

### 3. EBITDA Margin vs Revenue Growth — by Entity (Scatter Chart)

- **X-axis:** `[Revenue YoY %]` — year-over-year revenue growth
- **Y-axis:** `[EBITDA Margin %]`
- **Details:** `silver_dim_entity[entity_name]` (one bubble per entity)
- **Size:** `[Revenue]`
- **Quadrant lines:** X = 0 (growth threshold), Y = 10% (margin threshold)
- **Tooltip:** adds `[Revenue]`, `[EBITDA]`, `[Net Profit]`

Purpose: Positions each entity in a growth/profitability quadrant for strategic portfolio assessment.

---

### 4. Cash Flow Components (Clustered Bar Chart)

- **Y-axis:** `silver_dim_date[fiscal_period]`
- **Series:** `[Operating Cash Flow]` (navy), `[Investing Cash Flow]` (blue), `[Financing Cash Flow]` (green), `[Free Cash Flow]` marker (amber)

Purpose: Shows the cash generation profile and identifies periods of cash pressure.

---

### 5. Working Capital Efficiency — DSO vs DPO (Line Chart)

- **X-axis:** `silver_dim_date[fiscal_period]`
- **Series:** `[Days Sales Outstanding]` (red, 2px), `[Days Payable Outstanding]` (blue, 2px)
- **Reference line:** 45 days (DSO target), dashed red

Purpose: Monitors debtor collection and creditor payment cycles. DSO above 45 days triggers a review.

---

## Entity P&L Summary Matrix Table

Full-width matrix at page bottom showing one row per legal entity plus a Group Total subtotal.

| Column | Measure | Format |
|---|---|---|
| Entity | `silver_dim_entity[entity_name]` | Text |
| Revenue (M HUF) | `[Revenue]` | `#,0,, " M"` |
| Budget (M HUF) | `[Revenue Budget]` | `#,0,, " M"` |
| Rev Var % | `[Revenue Variance %]` | `+0.0%;-0.0%` · font-color RAG |
| Gross Margin % | `[Gross Margin %]` | `0.0%` |
| EBITDA (M HUF) | `[EBITDA]` | `#,0,, " M"` |
| EBITDA Margin % | `[EBITDA Margin %]` | `0.0%` · data bar |
| Net Profit (M HUF) | `[Net Profit]` | `#,0,, " M"` |
| Free CF (M HUF) | `[Free Cash Flow]` | `#,0,, " M"` |
| Current Ratio | `[Current Ratio]` | `0.00` |

Conditional formatting: Revenue Variance % uses font color (green/amber/red); EBITDA Margin % uses a data bar (navy positive, red negative).

---

## AI Narrative (Smart Narrative Visual)

A Smart Narrative visual auto-generates a bullet-point executive summary for the selected period and entity filter. Dynamic values bound:

- `[Revenue]`, `[Revenue Variance %]`
- `[EBITDA Margin %]`, `[EBITDA Variance]`
- `[Free Cash Flow]`, `[Current Ratio]`

The narrative title is **"Executive Summary — AI Narrative"**. For richer AI commentary, the page may link to the Q&A Agent web UI (see `python/Rag/financial_qa_agent.py`).

---

## Supporting DAX (New Measures)

The following measures are required by this page in addition to the standard library in [`PowerBI/Dax_measures/FIP_DAX_Measures.dax`](Dax_measures/FIP_DAX_Measures.dax).

### `[Revenue YoY %]`
```dax
DIVIDE(
    [Revenue] - CALCULATE([Revenue], SAMEPERIODLASTYEAR(silver_dim_date[calendar_date])),
    ABS(CALCULATE([Revenue], SAMEPERIODLASTYEAR(silver_dim_date[calendar_date]))),
    BLANK()
)
```
*Display folder: VAR_ — Variance vs Budget · Format: `+0.0%;-0.0%`*

---

### `[EBITDA RAG]`
```dax
VAR var_pct =
    DIVIDE(
        [EBITDA Variance],
        ABS(CALCULATE(
            SUM(agg_pl_monthly[ebitda_budget]),
            NOT ISBLANK(agg_pl_monthly[ebitda_budget])
        )),
        BLANK()
    )
RETURN
    SWITCH(
        TRUE(),
        ISBLANK(var_pct), "GREY",
        var_pct >= 0.00,  "GREEN",
        var_pct >= -0.05, "AMBER",
                          "RED"
    )
```
*Display folder: RAG_ — Status Indicators*

---

### `_PLBridgeOrder` (Calculated Table)
```dax
DATATABLE(
    "step_order", INTEGER,
    "step_label",  STRING,
    {
        {1,  "Revenue"},
        {2,  "(COGS)"},
        {3,  "Gross Profit"},
        {4,  "Other OI"},
        {5,  "(Personnel)"},
        {6,  "(Other OpEx)"},
        {7,  "EBITDA"},
        {8,  "(D&A)"},
        {9,  "Fin. Items (net)"},
        {10, "(Tax)"},
        {11, "Net Profit"}
    }
)
```

---

### `[PL Bridge Amount]`
```dax
SWITCH(
    SELECTEDVALUE(_PLBridgeOrder[step_label]),
    "Revenue",        [Revenue],
    "(COGS)",         -[COGS],
    "Gross Profit",   [Gross Profit],
    "Other OI",       [Other Operating Income],
    "(Personnel)",    -[Personnel Expense],
    "(Other OpEx)",   -[Other OpEx],
    "EBITDA",         [EBITDA],
    "(D&A)",          -CALCULATE(
                          SUMX(gold_fact_gl_transaction, gold_fact_gl_transaction[net_amount_lcy]),
                          gold_fact_gl_transaction[pl_line_item] = "Depreciation"
                      ),
    "Fin. Items (net)", CALCULATE(
                          SUMX(gold_fact_gl_transaction, gold_fact_gl_transaction[net_amount_lcy]),
                          gold_fact_gl_transaction[pl_line_item] = "Financial Income"
                      ) - CALCULATE(
                          SUMX(gold_fact_gl_transaction, gold_fact_gl_transaction[net_amount_lcy]),
                          gold_fact_gl_transaction[pl_line_item] = "Financial Expense"
                      ),
    "(Tax)",          -CALCULATE(
                          SUMX(gold_fact_gl_transaction, gold_fact_gl_transaction[net_amount_lcy]),
                          gold_fact_gl_transaction[pl_line_item] = "Tax Expense"
                      ),
    "Net Profit",     [Net Profit],
    BLANK()
)
```

---

### `[PL Bridge Type]`
```dax
SWITCH(
    SELECTEDVALUE(_PLBridgeOrder[step_label]),
    "Revenue",      "total",
    "Gross Profit", "total",
    "EBITDA",       "total",
    "Net Profit",   "total",
    IF([PL Bridge Amount] >= 0, "increase", "decrease")
)
```

---

## Slicers and Interactions

| Slicer | Field | Sync |
|---|---|---|
| Fiscal Year | `silver_dim_date[fiscal_year]` | All pages |
| Period (Month) | `silver_dim_date[fiscal_period]` | All pages |
| Legal Entity | `silver_dim_entity[entity_name]` (multi-select) | All pages |

Cross-filter: selecting a period in the revenue trend chart filters the entity P&L matrix. Selecting an entity in the scatter plot filters the matrix.

---

## Bookmarks

| Bookmark | Description |
|---|---|
| **YTD View** | Filters period ≤ current month for year-to-date analysis |
| **Full Year View** | Removes period filter to show all periods in selected fiscal year |

---

## Implementation Checklist

- [ ] Add `_PLBridgeOrder` calculated table to `FIP_Main` via Tabular Editor or TMSL.
- [ ] Add four supporting DAX measures (`[Revenue YoY %]`, `[EBITDA RAG]`, `[PL Bridge Amount]`, `[PL Bridge Type]`) to `FIP_Main`.
- [ ] Import page layout from `PowerBI/Pages/CFO_FinanceDashboard_PageSpec.json` into Power BI Desktop.
- [ ] Set page background `#F5F6FA`, header textbox background `#1B3A6B`.
- [ ] Validate visual positions at 1920×1080 canvas.
- [ ] Enable Smart Narrative visual and bind the five dynamic values listed above.
- [ ] Configure slicer sync (Fiscal Year and Entity slicers set to sync across all pages).
- [ ] Add YTD View and Full Year View bookmarks.
- [ ] Verify CFO RLS role sees all entities (no DAX filter).
- [ ] Publish to `FIP-Production` workspace; confirm 4-hour auto-refresh schedule.
- [ ] Run `PowerBI/Dax_tests/run_dax_tests.py` against updated dataset to validate new measures.
- [ ] Update `PowerBI/FIP_PowerBI_Template.pbit` binary artifact with the new page.

---

## Related Files

| File | Purpose |
|---|---|
| `PowerBI/Pages/CFO_FinanceDashboard_PageSpec.json` | PBIP-compatible page layout specification |
| `PowerBI/Dax_measures/FIP_DAX_Measures.dax` | Base DAX measure library |
| `PowerBI/Dax_measures/FIP_DAX_Measures_TMSL.json` | TMSL deployment artifact |
| `PowerBI/Rls/rls_roles.json` | RLS role definitions including CFO |
| `PowerBI/DASHBOARD_SPECIFICATIONS.md` | Master dashboard architecture and publishing schedule |
| `PowerBI/REPORT_SOURCE_OF_TRUTH.md` | Source-of-truth policy |
| `python/Rag/financial_qa_agent.py` | Q&A Agent FastAPI server (includes `/ui` web interface) |
