"""
FIP PowerBI DAX Testing Automation
====================================
Phase 3 deliverable · Financial Intelligence Platform · HU GAAP

Executes DAX validation queries against the Power BI Premium XMLA endpoint
(or Azure Analysis Services) and asserts expected results.

Usage
-----
    # Run all tests against the production workspace:
    python run_dax_tests.py --workspace fip-prod --dataset FIP_Main

    # Run a single test suite:
    python run_dax_tests.py --workspace fip-prod --dataset FIP_Main --suite pl_measures

    # Output results to JUnit XML for Azure DevOps:
    python run_dax_tests.py --workspace fip-prod --dataset FIP_Main --output junit_results.xml

Environment variables (or .env file)
--------------------------------------
    POWERBI_XMLA_ENDPOINT       e.g. powerbi://api.powerbi.com/v1.0/myorg/FIP-Production
    AZURE_TENANT_ID             AAD tenant ID for service principal auth
    AZURE_CLIENT_ID             Service principal client ID
    AZURE_CLIENT_SECRET         Service principal client secret
    POWERBI_WORKSPACE_NAME      Workspace name (overridden by --workspace)
    POWERBI_DATASET_NAME        Dataset name (overridden by --dataset)

Requirements
------------
    pip install python-dotenv azure-identity
    pip install "adodbapi; platform_system=='Windows'"  # Windows OLEDB path
    pip install pyodbc                                  # Linux ODBC path (see below)

    # Linux — install the MSOLEDB / MSOLAP driver:
    # curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    # sudo apt-get install msodbcsql18 unixodbc-dev
"""

import os
import sys
import json
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── XMLA connection helpers ──────────────────────────────────────────────────
def get_xmla_connection(workspace: str, dataset: str) -> object:
    """
    Returns a live ADODB/pyodbc connection to the Power BI XMLA endpoint.
    Authenticates via service principal (CI) or interactive browser (developer).
    """
    from azure.identity import ClientSecretCredential, InteractiveBrowserCredential

    tenant_id     = os.environ["AZURE_TENANT_ID"]
    client_id     = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    xmla_base     = os.environ.get("POWERBI_XMLA_ENDPOINT",
                                   "powerbi://api.powerbi.com/v1.0/myorg")
    endpoint = f"{xmla_base.rstrip('/')}/{workspace}"

    if client_id and client_secret:
        cred = ClientSecretCredential(tenant_id, client_id, client_secret)
    else:
        cred = InteractiveBrowserCredential(tenant_id=tenant_id)

    token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
    conn_str = (
        f"Provider=MSOLAP;Data Source={endpoint};"
        f"Initial Catalog={dataset};"
        f"User ID=;Password={token};"
        f"Persist Security Info=True;Impersonation Level=Impersonate;"
    )

    try:
        import pyodbc
        return pyodbc.connect(conn_str)
    except ImportError:
        import adodbapi
        return adodbapi.connect(conn_str)


def run_dax(conn, dax_query: str) -> list[dict]:
    """Execute a DAX query and return rows as list of dicts."""
    cursor = conn.cursor()
    cursor.execute(dax_query)
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ── Test definitions ─────────────────────────────────────────────────────────
# Each test is a dict with keys:
#   suite       : logical grouping
#   name        : test name (shown in report)
#   dax         : DAX query (must return exactly one row with one column)
#   assert_fn   : callable(value) -> bool
#   description : human-readable test description

TESTS = [
    # ── P&L Measure Consistency ──────────────────────────────────────────────
    {
        "suite": "pl_measures",
        "name": "gross_profit_equals_revenue_minus_cogs",
        "description": "Gross Profit = Revenue - COGS for latest completed period across all entities.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR rev   = CALCULATE([Revenue],    DATEADD(dim_date[calendar_date], -1, MONTH))
                VAR cogs  = CALCULATE([COGS],        DATEADD(dim_date[calendar_date], -1, MONTH))
                VAR gp    = CALCULATE([Gross Profit],DATEADD(dim_date[calendar_date], -1, MONTH))
                RETURN IF(ABS((rev - cogs) - gp) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
    {
        "suite": "pl_measures",
        "name": "ebitda_formula_consistency",
        "description": "EBITDA = Gross Profit + Other Operating Income - Personnel - Other OpEx (excludes D&A).",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR gp          = [Gross Profit]
                VAR other_oi    = [Other Operating Income]
                VAR personnel   = [Personnel Expense]
                VAR other_opex  = [Other OpEx]
                VAR ebitda_calc = gp + other_oi - personnel - other_opex
                VAR ebitda_meas = [EBITDA]
                RETURN IF(ABS(ebitda_calc - ebitda_meas) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
    {
        "suite": "pl_measures",
        "name": "net_profit_accounting_identity",
        "description": "Net Profit = Revenue + OOI - COGS - Personnel - OtherOpEx + FinInc - FinExp - Tax. HU GAAP accrual identity.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR fin_income  = CALCULATE(
                                      SUMX(gold_fact_gl_transaction, gold_fact_gl_transaction[net_amount_lcy]),
                                      gold_fact_gl_transaction[pl_line_item] = "Financial Income"
                                  )
                VAR fin_expense = CALCULATE(
                                      SUMX(gold_fact_gl_transaction, gold_fact_gl_transaction[net_amount_lcy]),
                                      gold_fact_gl_transaction[pl_line_item] = "Financial Expense"
                                  )
                VAR tax_expense = CALCULATE(
                                      SUMX(gold_fact_gl_transaction, gold_fact_gl_transaction[net_amount_lcy]),
                                      gold_fact_gl_transaction[pl_line_item] = "Tax Expense"
                                  )
                VAR identity = [Revenue] + [Other Operating Income]
                               - [COGS] - [Personnel Expense] - [Other OpEx]
                               + fin_income - fin_expense - tax_expense
                VAR measure  = [Net Profit]
                RETURN IF(ABS(identity - measure) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
    {
        "suite": "pl_measures",
        "name": "ebitda_margin_denominator_is_revenue",
        "description": "EBITDA Margin denominator must be Revenue only (not total income). HU GAAP convention.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR margin_from_formula = DIVIDE([EBITDA], [Revenue], BLANK())
                VAR margin_measure      = [EBITDA Margin %]
                RETURN IF(ABS(margin_from_formula - margin_measure) < 0.0001, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },

    # ── Budget vs Actual ─────────────────────────────────────────────────────
    {
        "suite": "budget_variance",
        "name": "revenue_variance_sign_convention",
        "description": "Variance = Actual - Budget. Positive = Favourable for revenue.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR actual   = [Revenue]
                VAR budget   = [Revenue Budget]
                VAR variance = [Revenue Variance]
                RETURN IF(ABS((actual - budget) - variance) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
    {
        "suite": "budget_variance",
        "name": "variance_pct_blank_when_no_budget",
        "description": "Variance % must return BLANK (not error or zero) when no budget loaded.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                -- Filter to an entity/period with no budget
                VAR no_budget_period =
                    CALCULATETABLE(
                        VALUES(agg_pl_monthly[period_key]),
                        ISBLANK(agg_pl_monthly[revenue_budget])
                    )
                VAR variance_pct_when_no_budget =
                    CALCULATE([Revenue Variance %], no_budget_period)
                RETURN IF(ISBLANK(variance_pct_when_no_budget), 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },

    # ── YoY and YTD ──────────────────────────────────────────────────────────
    {
        "suite": "time_intelligence",
        "name": "yoy_revenue_uses_prior_year_same_period",
        "description": "YoY Revenue uses same calendar month prior year (period_key - 100).",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR current_rev = CALCULATE([Revenue], agg_pl_monthly[period_key] = 202502)
                VAR prior_rev   = CALCULATE([Revenue], agg_pl_monthly[period_key] = 202402)
                VAR yoy_stored  = CALCULATE([Revenue PY], agg_pl_monthly[period_key] = 202502)
                RETURN IF(ABS(prior_rev - yoy_stored) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
    {
        "suite": "time_intelligence",
        "name": "ytd_is_cumulative_sum",
        "description": "YTD Revenue for month M = SUM of revenue from Jan to M (fiscal year).",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR ytd_stored  = CALCULATE([Revenue YTD], agg_pl_monthly[period_key] = 202503)
                VAR ytd_manual  = CALCULATE([Revenue],
                                    agg_pl_monthly[period_key] >= 202501 &&
                                    agg_pl_monthly[period_key] <= 202503)
                RETURN IF(ABS(ytd_stored - ytd_manual) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },

    # ── FX Conversion ────────────────────────────────────────────────────────
    {
        "suite": "fx_conversion",
        "name": "huf_transactions_not_fx_converted",
        "description": "HUF transactions must have EUR amount = NULL or equal to HUF amount / approx rate.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR huf_txns_with_eur =
                    CALCULATE(
                        COUNTROWS(gold_fact_gl_transaction),
                        gold_fact_gl_transaction[currency_local] = "HUF",
                        NOT ISBLANK(gold_fact_gl_transaction[net_amount_eur])
                    )
                -- HUF txns should have NULL EUR amount (no FX conversion needed)
                RETURN IF(huf_txns_with_eur = 0, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
    {
        "suite": "fx_conversion",
        "name": "eur_amount_uses_multiply_not_divide",
        "description": "net_amount_eur = net_amount_lcy / rate_to_huf (divide — not multiply). Rate 400 HUF/EUR: 800 HUF = 2 EUR.",
        "dax": """
            EVALUATE
            -- Validate FX direction: pick a known EUR transaction and check direction
            ROW(
                "test_result",
                VAR sample_lcy = CALCULATE(
                    SUMX(
                        TOPN(1, gold_fact_gl_transaction, gold_fact_gl_transaction[transaction_key]),
                        gold_fact_gl_transaction[net_amount_lcy]
                    ),
                    gold_fact_gl_transaction[currency_local] = "EUR"
                )
                VAR sample_eur = CALCULATE(
                    SUMX(
                        TOPN(1, gold_fact_gl_transaction, gold_fact_gl_transaction[transaction_key]),
                        gold_fact_gl_transaction[net_amount_eur]
                    ),
                    gold_fact_gl_transaction[currency_local] = "EUR"
                )
                VAR approx_rate = DIVIDE(sample_lcy, sample_eur, BLANK())
                -- Rate should be ~300-450 (HUF/EUR range). If < 1 the divide/multiply is wrong.
                RETURN IF(approx_rate >= 200 && approx_rate <= 600, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },

    # ── RLS ──────────────────────────────────────────────────────────────────
    {
        "suite": "rls",
        "name": "rls_restricts_entity_access",
        "description": "User assigned to entity 'ACME_HU' must see zero rows for entity 'BETA_HU'.",
        "dax": """
            -- Run this test with a test user / service principal scoped to ACME_HU only
            EVALUATE
            ROW(
                "test_result",
                VAR rows_visible =
                    CALCULATE(
                        COUNTROWS(silver_dim_entity),
                        REMOVEFILTERS()
                    )
                -- With RLS active, only 1 entity should be visible
                RETURN IF(rows_visible = 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },

    # ── Balance Sheet ─────────────────────────────────────────────────────────
    {
        "suite": "balance_sheet",
        "name": "total_assets_equals_liabilities_plus_equity",
        "description": "Fundamental accounting equation: Total Assets = Total Liabilities + Total Equity.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR assets = [Total Assets]
                VAR liab   = [Total Liabilities]
                VAR equity = [Total Equity]
                -- Allow for 1 HUF rounding tolerance
                RETURN IF(ABS(assets - (liab + equity)) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
    {
        "suite": "balance_sheet",
        "name": "working_capital_formula",
        "description": "Working Capital = Current Assets - Current Liabilities.",
        "dax": """
            EVALUATE
            ROW(
                "test_result",
                VAR wc_measure  = [Working Capital]
                VAR wc_calc     = [Total Current Assets] - [Total Current Liabilities]
                RETURN IF(ABS(wc_measure - wc_calc) < 1, 1, 0)
            )
        """,
        "assert_fn": lambda v: float(v) == 1.0,
    },
]


# ── Test runner ───────────────────────────────────────────────────────────────
def run_tests(conn, suite_filter: str | None = None) -> list[dict]:
    results = []
    tests_to_run = [t for t in TESTS if suite_filter is None or t["suite"] == suite_filter]
    for test in tests_to_run:
        start = datetime.now(timezone.utc)
        result = {
            "suite":       test["suite"],
            "name":        test["name"],
            "description": test["description"],
            "status":      "UNKNOWN",
            "actual":      None,
            "error":       None,
            "duration_ms": 0,
        }
        try:
            rows = run_dax(conn, test["dax"])
            if not rows:
                result["status"] = "FAIL"
                result["error"]  = "DAX query returned zero rows"
            else:
                actual = list(rows[0].values())[0]
                result["actual"] = actual
                result["status"] = "PASS" if test["assert_fn"](actual) else "FAIL"
        except Exception as exc:
            result["status"] = "ERROR"
            result["error"]  = str(exc)
        result["duration_ms"] = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        results.append(result)
        status_icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "🔥"}.get(result["status"], "?")
        print(f"  {status_icon} [{result['suite']}] {result['name']} ({result['duration_ms']} ms)")
    return results


def write_junit_xml(results: list[dict], output_path: str):
    total    = len(results)
    failures = sum(1 for r in results if r["status"] == "FAIL")
    errors   = sum(1 for r in results if r["status"] == "ERROR")
    elapsed  = sum(r["duration_ms"] for r in results) / 1000

    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite",
                          name="FIP PowerBI DAX Tests",
                          tests=str(total), failures=str(failures),
                          errors=str(errors), time=str(elapsed))
    for r in results:
        tc = ET.SubElement(suite, "testcase",
                           classname=r["suite"], name=r["name"],
                           time=str(r["duration_ms"] / 1000))
        if r["status"] == "FAIL":
            ET.SubElement(tc, "failure", message=r["description"]).text = (
                f"Expected assertion to pass. Actual value: {r['actual']}"
            )
        elif r["status"] == "ERROR":
            ET.SubElement(tc, "error", message="Execution error").text = r["error"]
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"\nJUnit XML written to: {output_path}")


def print_summary(results: list[dict]):
    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    errored = sum(1 for r in results if r["status"] == "ERROR")
    total   = len(results)
    print(f"\n{'='*60}")
    print(f"DAX Test Results: {passed}/{total} passed | {failed} failed | {errored} errors")
    print(f"{'='*60}")
    if failed or errored:
        print("\nFailed / Error tests:")
        for r in results:
            if r["status"] in ("FAIL", "ERROR"):
                print(f"  ❌ [{r['suite']}] {r['name']}")
                if r["error"]:
                    print(f"     Error: {r['error']}")
                elif r["actual"] is not None:
                    print(f"     Actual value: {r['actual']}")
    return 0 if (failed == 0 and errored == 0) else 1


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FIP PowerBI DAX Test Runner")
    parser.add_argument("--workspace", default=os.environ.get("POWERBI_WORKSPACE_NAME", "FIP-Production"))
    parser.add_argument("--dataset",   default=os.environ.get("POWERBI_DATASET_NAME",   "FIP_Main"))
    parser.add_argument("--suite",     default=None, help="Run only this test suite")
    parser.add_argument("--output",    default=None, help="JUnit XML output path for Azure DevOps")
    parser.add_argument("--dry-run",   action="store_true", help="Print test list without connecting")
    args = parser.parse_args()

    if args.dry_run:
        suites = sorted(set(t["suite"] for t in TESTS))
        print(f"FIP DAX Test Suite — {len(TESTS)} tests across {len(suites)} suites:")
        for suite in suites:
            suite_tests = [t for t in TESTS if t["suite"] == suite]
            print(f"\n  [{suite}] — {len(suite_tests)} tests")
            for t in suite_tests:
                print(f"    • {t['name']}: {t['description']}")
        sys.exit(0)

    print(f"Connecting to: {args.workspace} / {args.dataset}")
    conn = get_xmla_connection(args.workspace, args.dataset)
    print(f"Running {len(TESTS)} DAX tests...\n")
    results = run_tests(conn, suite_filter=args.suite)
    conn.close()

    if args.output:
        write_junit_xml(results, args.output)

    sys.exit(print_summary(results))
