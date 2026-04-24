#!/usr/bin/env bash
# =============================================================================
#  init_project_structure.sh
#  Financial Intelligence Platform — Project Structure Initializer
#
#  PURPOSE
#  -------
#  Creates all required project directories that are referenced in docs
#  but were missing from the repository (FINDING A-06, A-07).
#
#  Specifically initialises:
#    · docs/incident_response/  — referenced in SECURITY_AND_COMPLIANCE.md
#    · docs/vendor_dpa/         — referenced in SECURITY_AND_COMPLIANCE.md
#    · python/requirements.txt  — ensures pip install works from README.md
#    · .env.example             — verified present (FINDING A-06 resolved)
#    · financial_dbt/seeds/     — dbt seed CSV directory
#    · PowerBI/exports/         — local export staging directory
#    · data/chromadb/           — local ChromaDB persistence for RAG dev
#    · tests/                   — project-level integration test directory
#
#  USAGE
#  -----
#    chmod +x init_project_structure.sh
#    ./init_project_structure.sh                    # dry-run (shows what would be created)
#    ./init_project_structure.sh --apply            # actually creates directories & files
#    ./init_project_structure.sh --apply --verbose  # with confirmation messages
#
#  SAFE TO RE-RUN
#  --------------
#  All operations use -p flag (mkdir) or test-before-write guards.
#  Running on an already-initialised repo is a no-op.
#
#  REQUIREMENTS
#  ------------
#    bash >= 4.0, standard POSIX tools (mkdir, touch, cat, tee)
#    No external dependencies.
# =============================================================================

set -euo pipefail

# ─── Argument parsing ─────────────────────────────────────────────────────────
APPLY=false
VERBOSE=false

for arg in "$@"; do
    case $arg in
        --apply)   APPLY=true   ;;
        --verbose) VERBOSE=true ;;
        --help|-h)
            sed -n '/^# PURPOSE/,/^# =/p' "$0" | head -50
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "Usage: $0 [--apply] [--verbose]" >&2
            exit 1
            ;;
    esac
done

# ─── Resolve project root ─────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives in remediation/devops/ — project root is two levels up
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo ""
echo "========================================================"
echo "  FIP Project Structure Initializer"
echo "========================================================"
echo "  Project root : $PROJECT_ROOT"
echo "  Mode         : $([ "$APPLY" = true ] && echo 'APPLY (will create files)' || echo 'DRY-RUN (no changes)')"
echo "========================================================"
echo ""

# ─── Helper functions ─────────────────────────────────────────────────────────

# ensure_dir <relative_path> <description>
ensure_dir() {
    local rel_path="$1"
    local description="$2"
    local full_path="$PROJECT_ROOT/$rel_path"

    if [ -d "$full_path" ]; then
        [ "$VERBOSE" = true ] && echo "  ✓ EXISTS  $rel_path  ($description)"
        return 0
    fi

    if [ "$APPLY" = true ]; then
        mkdir -p "$full_path"
        echo "  + CREATED $rel_path  ($description)"
    else
        echo "  ~ MISSING $rel_path  ($description)  [dry-run: would create]"
    fi
}

# ensure_file <relative_path> <description> <content_heredoc_var>
# Creates the file only if it doesn't already exist.
ensure_file() {
    local rel_path="$1"
    local description="$2"
    local content="$3"
    local full_path="$PROJECT_ROOT/$rel_path"

    if [ -f "$full_path" ]; then
        [ "$VERBOSE" = true ] && echo "  ✓ EXISTS  $rel_path  ($description)"
        return 0
    fi

    if [ "$APPLY" = true ]; then
        # Ensure parent directory exists
        mkdir -p "$(dirname "$full_path")"
        printf '%s\n' "$content" > "$full_path"
        echo "  + CREATED $rel_path  ($description)"
    else
        echo "  ~ MISSING $rel_path  ($description)  [dry-run: would create]"
    fi
}

# check_file <relative_path> <description>
# Reports status but never creates (for verification of existing required files).
check_file() {
    local rel_path="$1"
    local description="$2"
    local full_path="$PROJECT_ROOT/$rel_path"

    if [ -f "$full_path" ]; then
        echo "  ✓ PRESENT $rel_path  ($description)"
    else
        echo "  ✗ MISSING $rel_path  ($description)  ← INVESTIGATE"
        MISSING_REQUIRED+=("$rel_path")
    fi
}


# ─── Track required-file failures ─────────────────────────────────────────────
MISSING_REQUIRED=()


# =============================================================================
#  SECTION 1 — Required directories
# =============================================================================
echo "  [1/5] DIRECTORIES"
echo ""

ensure_dir "docs/incident_response" \
    "Incident response runbooks (IR-001, IR-002, ...) — referenced in SECURITY_AND_COMPLIANCE.md"

ensure_dir "docs/vendor_dpa" \
    "Vendor Data Processing Agreements (GDPR Art.28) — referenced in SECURITY_AND_COMPLIANCE.md"

ensure_dir "python/anomaly_detection" \
    "Anomaly detection module directory"

ensure_dir "python/commentary" \
    "AI commentary generator module directory"

ensure_dir "python/forecasting" \
    "Financial forecasting module directory"

ensure_dir "python/rag" \
    "RAG / NLQ Q&A agent module directory"

ensure_dir "financial_dbt/seeds" \
    "dbt seed CSV files (ref_coa_mapping.csv, ref_fiscal_calendar.csv)"

ensure_dir "PowerBI/exports" \
    "Local staging directory for Power BI PDF/PBIX monthly exports (gitignored)"

ensure_dir "data/chromadb" \
    "ChromaDB local persistence for RAG development (gitignored)"

ensure_dir "tests" \
    "Project-level integration tests (Python pytest)"

ensure_dir "logs" \
    "Local log files for Python modules (gitignored)"

ensure_dir "remediation" \
    "Remediation deliverables from compliance report (this file lives here)"

echo ""

# =============================================================================
#  SECTION 2 — Required stub files in incident_response/
# =============================================================================
echo "  [2/5] INCIDENT RESPONSE STUBS"
echo ""

IR_README='# Incident Response Runbooks

This directory contains incident response (IR) runbooks for the Financial Intelligence Platform.

## Index

| File | Incident Type | Last Updated |
|------|--------------|--------------|
| IR-001_data_pipeline_failure.md | ADF pipeline failure / data not refreshed | See file |
| IR-002_fx_rate_missing.md | NBH FX rate not available for posting date | See file |

## Adding a New Runbook

Copy the template below and fill in each section:

```
# IR-NNN: <Incident Title>
**Severity**: P1-CRIT | P1-HIGH | P2-MED | P3-LOW
**Detection**: How is this incident detected?
**Impact**: What breaks if this is not resolved?
**Steps**: Numbered resolution steps
**Escalation**: Who to contact and when
**RCA Template**: Root cause analysis format
```

All runbooks must be reviewed by the on-call data engineer and signed off by the CFO within 48 hours of a P1 incident.
'
ensure_file "docs/incident_response/README.md" \
    "Incident response index" \
    "$IR_README"

# Stub for IR-001 (if it does not already exist from another source)
IR_001='# IR-001: ADF Data Pipeline Failure — Data Not Refreshed

**Severity**: P1-HIGH
**SLA Response**: 4 hours
**Detection**: Azure Monitor alert fires when pl_monthly_close fails; Power BI shows "Data refresh failed" banner.

## Impact
- Power BI dashboards show stale data (last successful refresh)
- CFO and CEO dashboards will display prior-period figures
- Monthly close KPIs unavailable until resolved

## Symptoms
- ADF pipeline status = FAILED in Azure Data Factory Monitor
- audit.batch_log.status = FAILED for current period
- Power BI dataset refresh history shows error

## Resolution Steps

1. **Identify failure point** — check ADF Monitor → Pipeline runs → pl_monthly_close → Failed activities
2. **Check audit.batch_log** — `SELECT * FROM audit.batch_log WHERE status = '"'"'FAILED'"'"' ORDER BY started_at DESC LIMIT 5;`
3. **Check audit.data_quality_log** — identify which DQ rule caused the block
4. **Check audit.quarantine** — review quarantined records for the failed batch
5. **If DQ failure**: Notify Finance team to review quarantine queue; re-run after resolution
6. **If infrastructure failure**: Check Azure Service Health for Synapse/ADF outages
7. **Manual retry**: ADF Monitor → Rerun from failed activity

## Escalation
- 0–2h: On-call Data Engineer
- 2–4h: Data Engineering Lead + CFO notification
- 4h+: CTO escalation; consider manual Excel extract as interim

## Post-Incident
- Complete RCA within 24 hours
- Update this runbook with new failure modes discovered
- File Azure support ticket if infrastructure-related
'
ensure_file "docs/incident_response/IR-001_data_pipeline_failure.md" \
    "ADF pipeline failure runbook" \
    "$IR_001"

IR_002='# IR-002: NBH FX Rate Missing for Posting Date

**Severity**: P2-MED
**SLA Response**: 8 hours (next business day acceptable if detected after 16:00 CET)
**Detection**: dbt test assert_fx_rates_available fails; DQ-007 WARN in audit.data_quality_log.

## Impact
- Non-HUF transactions fall back to stale FX rate (last available)
- EUR-denominated KPIs may be slightly incorrect
- Pipeline continues but accuracy is degraded

## Common Causes
- Hungarian bank holiday not in dim_date.hu_holiday_name (NBH does not publish rates on holidays)
- NBH API (https://www.mnb.hu/arfolyamok) temporarily unavailable
- New currency code introduced in source ERP not yet in config.ref_currencies

## Resolution Steps

1. **Check NBH API directly**: `curl "https://www.mnb.hu/arfolyamok" | grep EUR`
2. **Identify missing dates**: `SELECT rate_date, from_currency FROM config.ref_fx_rates WHERE rate_date BETWEEN '"'"'YYYY-MM-DD'"'"' AND '"'"'YYYY-MM-DD'"'"' AND from_currency = '"'"'EUR'"'"';`
3. **If holiday gap (expected)**: The 3-day lookback in DQ-007 should cover this automatically. Verify by checking if previous 3 days have rates.
4. **If API outage**: Manually insert prior day rates as interim; re-fetch when API recovers
5. **If new currency**: Add to config.ref_currencies and update ref_coa_mapping if needed
6. **Re-run FX fetch step**: ADF → trg_fx_rates_daily → Trigger now

## Escalation
- 0–8h: On-call Data Engineer (resolve independently)
- 8h+: Finance team notification if KPIs are materially affected
'
ensure_file "docs/incident_response/IR-002_fx_rate_missing.md" \
    "FX rate missing runbook" \
    "$IR_002"

echo ""

# =============================================================================
#  SECTION 3 — Required stub files in vendor_dpa/
# =============================================================================
echo "  [3/5] VENDOR DPA STUBS"
echo ""

VENDOR_DPA_README='# Vendor Data Processing Agreements (DPA)

This directory contains GDPR Article 28 Data Processing Agreements with all vendors
who process personal data on behalf of cLeaR Analytics Ltd.

## Required DPAs

| Vendor | Service | DPA Status | Review Date |
|--------|---------|------------|-------------|
| Microsoft Azure | Cloud infrastructure (Synapse, ADF, Key Vault, OpenAI) | ✓ Covered by Microsoft DPA | Annual |
| Microsoft Power BI | BI reporting service | ✓ Covered by Microsoft DPA | Annual |

## GDPR Compliance Notes

- cLeaR Analytics Ltd. acts as **Data Processor** (Article 28) for client financial data
- Each client must sign a DPA with cLeaR Analytics Ltd. before onboarding
- All sub-processors listed above must be disclosed to clients in the DPA
- Data residency: Azure West Europe (Netherlands) — all data remains within EU

## Adding a New Vendor DPA

1. Obtain signed DPA from vendor (or link to their standard DPA URL)
2. Add entry to the table above
3. Store signed PDF in this directory as `DPA_<VendorName>_<YYYY>.pdf`
4. Notify DPO (Data Protection Officer) and update privacy notice if sub-processor list changes

## Microsoft Azure DPA Reference

Microsoft Online Services DPA is available at:
https://www.microsoft.com/en-us/licensing/docs/view/Microsoft-Products-and-Services-Data-Protection-Addendum-DPA

The DPA covers: Azure Data Factory, Azure Synapse Analytics, Azure Key Vault,
Azure Data Lake Storage, Azure OpenAI Service, Power BI Premium.

_Last reviewed: April 2026_
'
ensure_file "docs/vendor_dpa/README.md" \
    "Vendor DPA index and GDPR compliance notes" \
    "$VENDOR_DPA_README"

echo ""

# =============================================================================
#  SECTION 4 — .gitignore additions
# =============================================================================
echo "  [4/5] .GITIGNORE VERIFICATION"
echo ""

GITIGNORE_PATH="$PROJECT_ROOT/.gitignore"

if [ "$APPLY" = true ]; then
    # Ensure .gitignore exists and contains all required patterns
    touch "$GITIGNORE_PATH"

    REQUIRED_PATTERNS=(
        ".env"
        "data/chromadb/"
        "logs/"
        "PowerBI/exports/"
        "*.pyc"
        "__pycache__/"
        ".dbt/"
        "target/"
        "dbt_packages/"
        "*.log"
        "*.tmp"
        "~\$*"          # Office lock files
    )

    ADDED_PATTERNS=()
    for pattern in "${REQUIRED_PATTERNS[@]}"; do
        if ! grep -qxF "$pattern" "$GITIGNORE_PATH" 2>/dev/null; then
            echo "$pattern" >> "$GITIGNORE_PATH"
            ADDED_PATTERNS+=("$pattern")
        fi
    done

    if [ ${#ADDED_PATTERNS[@]} -gt 0 ]; then
        echo "  + UPDATED .gitignore — added ${#ADDED_PATTERNS[@]} missing pattern(s):"
        for p in "${ADDED_PATTERNS[@]}"; do
            echo "            $p"
        done
    else
        echo "  ✓ .gitignore already contains all required patterns"
    fi
else
    echo "  ~ .gitignore — would verify/add required patterns  [dry-run]"
fi

echo ""

# =============================================================================
#  SECTION 5 — Verify required files are present (never create, just report)
# =============================================================================
echo "  [5/5] REQUIRED FILE VERIFICATION"
echo ""

check_file ".env.example"                                          "Environment template (FINDING A-06)"
check_file "docs/ARCHITECTURE.md"                                  "Platform architecture documentation"
check_file "docs/SECURITY_AND_COMPLIANCE.md"                       "Security & compliance guide"
check_file "docs/SETUP_AND_DEPLOYMENT.md"                          "Setup and deployment guide"
check_file "docs/README.md"                                        "Documentation index"
check_file "financial_dbt/dbt_project.yml"                         "dbt project configuration"
check_file "financial_dbt/profiles.yml"                            "dbt connection profiles"
check_file "adf_pipelines/pl_monthly_close.json"                   "ADF monthly close pipeline"
check_file "adf_pipelines/pl_dq_validation.json"                   "ADF DQ validation pipeline"
check_file "adf_pipelines/pl_erp_extract.json"                     "ADF ERP extract pipeline"
check_file "adf_pipelines/triggers/trg_monthly_close.json"         "ADF monthly close trigger"
check_file "adf_pipelines/datasets/ds_sftp_erp_source.json"        "Kulcs-Soft SFTP dataset (FINDING A-02)"
check_file "adf_pipelines/datasets/ds_rest_cobalt_api.json"        "COBALT REST dataset (FINDING A-02)"
check_file "adf_pipelines/datasets/ds_rest_szamlazz_api.json"      "Számlázz.hu REST dataset (FINDING A-02)"
check_file "adf_pipelines/datasets/ds_rest_sap_b1_api.json"        "SAP B1 REST dataset (FINDING A-02)"
check_file "fip/fip_schema_audit.sql"                              "Audit schema DDL"
check_file "fip/fip_stored_procedures.sql"                         "Stored procedures (DQ-001–003)"
check_file "financial_dbt/tests/assert_balance_sheet_balances.sql" "dbt test: balance sheet equation"
check_file "financial_dbt/tests/assert_no_duplicate_postings.sql"  "dbt test: duplicate hash check"
check_file "financial_dbt/tests/assert_no_zero_amount_transactions.sql"     "dbt test: DQ-004"
check_file "financial_dbt/tests/assert_valid_posting_dates.sql"             "dbt test: DQ-005"
check_file "financial_dbt/tests/assert_valid_currency_codes.sql"            "dbt test: DQ-006"
check_file "financial_dbt/tests/assert_fx_rates_available.sql"              "dbt test: DQ-007"
check_file "financial_dbt/tests/assert_no_duplicate_source_ids.sql"         "dbt test: DQ-008"
check_file "financial_dbt/tests/assert_revenue_accounts_no_debit_balance.sql" "dbt test: DQ-009"
check_file "financial_dbt/tests/assert_no_excessive_late_entries.sql"       "dbt test: DQ-010"
check_file "PowerBI/DASHBOARD_SPECIFICATIONS.md"                   "Power BI dashboard specs"
check_file "PowerBI/rls/rls_roles.json"                            "Power BI RLS role definitions"

echo ""

# =============================================================================
#  SUMMARY
# =============================================================================
echo "========================================================"
echo "  SUMMARY"
echo "========================================================"

if [ "$APPLY" = true ]; then
    echo "  Mode: APPLIED — all missing directories and stub files created."
else
    echo "  Mode: DRY-RUN — no changes made. Re-run with --apply to create."
fi

if [ ${#MISSING_REQUIRED[@]} -eq 0 ]; then
    echo "  ✓ All required files are present."
    echo ""
    echo "  Next steps:"
    echo "    1. Copy remediation/sql/fip_schema_audit_patch.sql → apply to Synapse"
    echo "    2. Copy remediation/adf_datasets/*.json → adf_pipelines/datasets/"
    echo "    3. Copy remediation/dbt_tests/assert_budget_variance_bounds.sql → financial_dbt/tests/"
    echo "    4. python remediation/dbt_tests/validate_trigger_date_fix.py --verbose"
    echo "    5. Run: dbt test --select assert_budget_variance_bounds"
    echo ""
    exit 0
else
    echo ""
    echo "  ✗ ${#MISSING_REQUIRED[@]} required file(s) are MISSING:"
    for f in "${MISSING_REQUIRED[@]}"; do
        echo "      - $f"
    done
    echo ""
    echo "  These files must be restored from git history or recreated."
    echo "  See remediation/ directory for SQL, ADF, and dbt deliverables."
    echo ""
    exit 2
fi
