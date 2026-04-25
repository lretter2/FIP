#!/usr/bin/env python3
"""Deploy FIP_Main Power BI dataset to a target workspace via XMLA/TMSL."""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("FIP.XmlaDeployer")

STAGE_CONFIG = {
    "dev":  {"workspace_name_env": "POWERBI_DEV_WORKSPACE_NAME",  "workspace_name_default": "FIP-Development"},
    "test": {"workspace_name_env": "POWERBI_TEST_WORKSPACE_NAME", "workspace_name_default": "FIP-Test"},
    "prod": {"workspace_name_env": "POWERBI_PROD_WORKSPACE_NAME", "workspace_name_default": "FIP-Production"},
}

DATASET_NAME           = os.environ.get("POWERBI_DATASET_NAME", "FIP_Main")
XMLA_BASE              = os.environ.get("POWERBI_XMLA_BASE", "powerbi://api.powerbi.com/v1.0/myorg")
MIN_EXPECTED_MEASURES  = 41  # Must be updated manually when measures are added/removed (see Dax_measures/FIP_DAX_Measures.dax summary block)


def get_xmla_token() -> str:
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    tenant_id     = os.environ["AZURE_TENANT_ID"]
    client_id     = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")

    if client_id and client_secret:
        log.info("  Auth: Service Principal (ClientSecretCredential)")
        cred = ClientSecretCredential(tenant_id, client_id, client_secret)
    else:
        log.info("  Auth: DefaultAzureCredential (MSI / interactive)")
        cred = DefaultAzureCredential()

    token = cred.get_token("https://analysis.windows.net/powerbi/api/.default")
    log.info(f"  Token acquired (expires: {datetime.fromtimestamp(token.expires_on, tz=timezone.utc).isoformat()})")
    return token.token


def open_xmla_connection(workspace_name: str, dataset_name: str, token: str):
    endpoint = f"{XMLA_BASE.rstrip('/')}/{workspace_name}"
    conn_str = (
        f"Provider=MSOLAP;"
        f"Data Source={endpoint};"
        f"Initial Catalog={dataset_name};"
        f"User ID=;"
        f"Password={token};"
        f"Persist Security Info=True;"
        f"Impersonation Level=Impersonate;"
    )
    log.info(f"  Connecting to XMLA endpoint: {endpoint}")
    log.info(f"  Dataset: {dataset_name}")

    try:
        import pyodbc
        conn = pyodbc.connect(conn_str, timeout=30)
        log.info("  Connected via pyodbc")
        return conn
    except ImportError:
        pass

    try:
        import adodbapi
        conn = adodbapi.connect(conn_str)
        log.info("  Connected via adodbapi")
        return conn
    except ImportError:
        pass

    raise ImportError(
        "Neither pyodbc nor adodbapi is installed. "
        "Install pyodbc (Linux) or adodbapi (Windows) to use XMLA deployment."
    )


def execute_tmsl(conn, tmsl_command: dict) -> dict:
    cursor = conn.cursor()
    tmsl_str = json.dumps(tmsl_command)
    log.debug(f"  Executing TMSL ({len(tmsl_str)} chars)...")
    cursor.execute(tmsl_str)
    try:
        rows = cursor.fetchall()
        if rows:
            return {"status": "success", "rows": [list(r) for r in rows]}
    except Exception:
        pass  # fetchall() raises for non-query TMSL commands
    return {"status": "success"}


def load_tmsl(tmsl_path: Path) -> dict:
    if not tmsl_path.exists():
        log.error(f"TMSL file not found: {tmsl_path}")
        sys.exit(3)
    try:
        doc = json.loads(tmsl_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        log.error(f"Invalid JSON in TMSL file: {e}")
        sys.exit(3)

    if not {"createOrReplace", "alter", "sequence"} & doc.keys():
        log.error("TMSL file must contain a 'createOrReplace', 'alter', or 'sequence' command.")
        sys.exit(3)

    log.info(f"  TMSL loaded: {tmsl_path.name}")
    return doc


REQUIRED_RLS_ROLES = {"CEO", "CFO", "Controller", "CostCentreManager", "Auditor"}

POST_DEPLOY_CHECKS = [
    {
        "name": "Measures accessible",
        "dax": "EVALUATE ROW(\"count\", COUNTROWS(INFO.MEASURES()))",
        "assert": lambda rows: rows and int(list(rows[0].values())[0]) >= MIN_EXPECTED_MEASURES,
        "error_msg": f"Expected at least {MIN_EXPECTED_MEASURES} measures in the deployed dataset.",
    },
    {
        "name": "Revenue measure returns numeric",
        "dax": "EVALUATE ROW(\"val\", IF(ISNUMBER([Revenue]), 1, 0))",
        "assert": lambda rows: rows and float(list(rows[0].values())[0]) == 1.0,
        "error_msg": "[Revenue] measure does not return a numeric value.",
    },
    {
        "name": "Balance sheet equation holds",
        "dax": """
            EVALUATE ROW(
                \"check\",
                VAR diff = ABS([Total Assets] - ([Total Liabilities] + [Total Equity]))
                RETURN IF(diff < 1, 1, 0)
            )
        """,
        "assert": lambda rows: rows and float(list(rows[0].values())[0]) == 1.0,
        "error_msg": "Balance sheet equation violated: Total Assets \u2260 Total Liabilities + Total Equity.",
    },
    {
        "name": "RLS roles — all required names present",
        "dax": "EVALUATE SELECTCOLUMNS(INFO.ROLES(), \"Name\", [Name])",
        "assert": lambda rows: REQUIRED_RLS_ROLES.issubset({list(r.values())[0] for r in (rows or [])}),
        "error_msg": (
            f"One or more required RLS roles are missing. "
            f"Expected: {', '.join(sorted(REQUIRED_RLS_ROLES))}."
        ),
    },
]


def run_post_deploy_checks(conn) -> bool:
    log.info(f"\n{'─'*60}")
    log.info("  Post-deployment validation checks")
    log.info(f"{'─'*60}")

    all_passed = True
    for check in POST_DEPLOY_CHECKS:
        try:
            cursor = conn.cursor()
            cursor.execute(check["dax"].strip())
            rows = [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]
            passed = check["assert"](rows)
            icon = "✓" if passed else "✗"
            log.info(f"  {icon} {check['name']}")
            if not passed:
                log.error(f"    → {check['error_msg']}")
                all_passed = False
        except Exception as exc:
            log.error(f"  ✗ {check['name']}: ERROR — {exc}")
            all_passed = False

    return all_passed


def deploy(stage: str, tmsl_path: Path, dry_run: bool = False, verify_only: bool = False):
    cfg = STAGE_CONFIG[stage]
    workspace_name = os.environ.get(cfg["workspace_name_env"], cfg["workspace_name_default"])

    log.info(f"\n{'='*60}")
    log.info(f"  FIP Power BI XMLA Deployment")
    log.info(f"  Stage     : {stage.upper()}")
    log.info(f"  Workspace : {workspace_name}")
    log.info(f"  Dataset   : {DATASET_NAME}")
    log.info(f"  TMSL file : {tmsl_path}")
    log.info(f"  Dry run   : {dry_run}")
    log.info(f"{'='*60}\n")

    if dry_run:
        load_tmsl(tmsl_path)
        log.info(f"  TMSL structure is valid. Would deploy to '{workspace_name}/{DATASET_NAME}'.")
        return 0

    log.info("Step 1/4 — Acquiring authentication token")
    token = get_xmla_token()

    log.info("\nStep 2/4 — Connecting to XMLA endpoint")
    try:
        conn = open_xmla_connection(workspace_name, DATASET_NAME, token)
    except Exception as exc:
        log.error(f"Connection failed: {exc}")
        return 2

    try:
        if verify_only:
            log.info("\nStep 3/4 — VERIFY ONLY (skipping TMSL deployment)")
            all_ok = run_post_deploy_checks(conn)
            return 0 if all_ok else 1

        log.info("\nStep 3/4 — Loading and executing TMSL")
        tmsl = load_tmsl(tmsl_path)
        try:
            result = execute_tmsl(conn, tmsl)
            log.info(f"  TMSL executed successfully: {result.get('status')}")
        except Exception as exc:
            log.error(f"  TMSL execution failed: {exc}")
            return 1

        log.info("\nStep 4/4 — Post-deployment validation")
        all_ok = run_post_deploy_checks(conn)
    finally:
        conn.close()

    outcome = "SUCCESS" if all_ok else "FAILED"
    log.info(f"\n{'='*60}")
    log.info(f"  Deployment {outcome}: stage={stage}, workspace={workspace_name}")
    log.info(f"  Timestamp: {datetime.now(timezone.utc).isoformat()}")
    log.info(f"{'='*60}")

    return 0 if all_ok else 1


def main():
    parser = argparse.ArgumentParser(
        description="FIP Power BI XMLA Deployment — deploy DAX measures and dataset model"
    )
    parser.add_argument("--stage", choices=["dev", "test", "prod"], required=True,
                        help="Target deployment stage (dev / test / prod)")
    parser.add_argument(
        "--tmsl",
        type=Path,
        default=Path(__file__).parent.parent / "Dax_measures" / "FIP_DAX_Measures_TMSL.json",
        help="Path to TMSL JSON file (default: PowerBI/Dax_measures/FIP_DAX_Measures_TMSL.json)",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate TMSL without connecting or deploying")
    parser.add_argument("--verify-only", action="store_true",
                        help="Connect and run post-deploy checks only (no TMSL execution)")
    args = parser.parse_args()

    sys.exit(deploy(
        stage=args.stage,
        tmsl_path=args.tmsl,
        dry_run=args.dry_run,
        verify_only=args.verify_only,
    ))


if __name__ == "__main__":
    main()
