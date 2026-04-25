"""
Financial Intelligence Platform - AI Commentary Generator
=========================================================

Builds a variance fact pack from Gold KPI views, generates role-based narrative
with Azure OpenAI, and writes drafts to audit.commentary_queue for approval.

Canonical entity naming:
- Business key: entity_code
- FK key: entity_id

Usage:
    python commentary_generator.py --entity_code ACME_HU --period_key 202601 --roles CFO,CEO
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pyodbc
from jsonschema import validate, ValidationError
from openai import AzureOpenAI

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from db_utils import get_db_connection, get_openai_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("FIP.CommentaryGenerator")

AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
MATERIALITY_THRESHOLD = float(os.getenv("MATERIALITY_THRESHOLD_PCT", "5.0"))
MAX_COMMENTARY_WORDS = int(os.getenv("MAX_COMMENTARY_WORDS", "600"))
PROMPT_DIR = os.path.join(os.path.dirname(__file__), "../../prompts")
VALIDATION_SCHEMA_FILE = os.path.join(PROMPT_DIR, "input_validation.txt")

_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

_FACT_PACK_QUERY = """
    SELECT
        p.period_key,
        p.period_key - 100 AS prior_year_period_key,
        e.entity_code,
        e.entity_name,
        e.reporting_currency,
        p.revenue,
        p.revenue_py,
        p.revenue_yoy_pct,
        p.gross_profit,
        p.gross_margin_pct,
        p.ebitda,
        p.ebitda_margin_pct,
        p.net_profit,
        p.net_profit_margin_pct,
        p.revenue_budget,
        p.revenue_variance,
        p.revenue_variance_pct,
        l.current_ratio,
        l.dso_days,
        l.dpo_days,
        l.dio_days,
        l.cash_conversion_cycle,
        l.net_debt_to_ebitda,
        c.operating_cash_flow,
        c.free_cash_flow,
        c.closing_cash_balance,
        l.total_assets,
        l.total_equity,
        l.net_debt,
        l.equity_ratio_pct
    FROM gold.kpi_profitability p
    JOIN gold.kpi_liquidity     l ON p.period_key = l.period_key AND p.entity_key = l.entity_key
    JOIN gold.kpi_cashflow      c ON p.period_key = c.period_key AND p.entity_key = c.entity_key
    JOIN silver.dim_entity      e ON p.entity_key = e.entity_key
    WHERE e.entity_code = ?
      AND p.period_key = ?
"""


def _parse_period_label(period_key: int) -> str:
    year = int(str(period_key)[:4])
    month = int(str(period_key)[4:])
    return f"{_MONTH_NAMES[month - 1]} {year}"


def _is_material(value: Optional[float], threshold: float) -> bool:
    if value is None or pd.isna(value):
        return False
    return abs(float(value)) >= threshold


def _fmt_huf(value: Optional[float]) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    return f"{value / 1_000_000:.1f}M HUF"


def _fmt_pct(value: Optional[float]) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    return f"{value:.1f}%"


def _build_fact_sections(row, period_label: str, period_key: int, entity_code: str) -> dict:
    mat = lambda v: _is_material(v, MATERIALITY_THRESHOLD)
    return {
        "report_metadata": {
            "entity_code": entity_code,
            "entity_name": str(row.get("entity_name", "")),
            "period": period_label,
            "period_key": int(period_key),
            "gaap_basis": "HU GAAP (2000/C Act)",
            "reporting_currency": str(row.get("reporting_currency", "HUF")),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "materiality_threshold_pct": MATERIALITY_THRESHOLD,
        },
        "pl_summary": {
            "revenue_current": _fmt_huf(row.get("revenue")),
            "revenue_budget": _fmt_huf(row.get("revenue_budget")),
            "revenue_vs_budget_pct": _fmt_pct(row.get("revenue_variance_pct")) if mat(row.get("revenue_variance_pct")) else "within materiality",
            "revenue_vs_py_pct": _fmt_pct(row.get("revenue_yoy_pct")) if mat(row.get("revenue_yoy_pct")) else "within materiality",
            "gross_margin_pct": _fmt_pct(row.get("gross_margin_pct")),
            "ebitda_current": _fmt_huf(row.get("ebitda")),
            "ebitda_margin_pct": _fmt_pct(row.get("ebitda_margin_pct")),
            "net_profit_current": _fmt_huf(row.get("net_profit")),
            "net_profit_margin_pct": _fmt_pct(row.get("net_profit_margin_pct")),
        },
        "balance_sheet_highlights": {
            "total_assets": _fmt_huf(row.get("total_assets")),
            "total_equity": _fmt_huf(row.get("total_equity")),
            "net_debt": _fmt_huf(row.get("net_debt")),
            "equity_ratio_pct": _fmt_pct(row.get("equity_ratio_pct")),
            "net_debt_to_ebitda": f"{row.get('net_debt_to_ebitda', 0):.2f}x" if row.get("net_debt_to_ebitda") is not None else None,
        },
        "liquidity_highlights": {
            "current_ratio": f"{row.get('current_ratio', 0):.2f}x" if row.get("current_ratio") is not None else None,
            "dso_days": f"{row.get('dso_days', 0):.0f} days" if row.get("dso_days") is not None else None,
            "dpo_days": f"{row.get('dpo_days', 0):.0f} days" if row.get("dpo_days") is not None else None,
            "cash_conversion_cycle_days": f"{row.get('cash_conversion_cycle', 0):.0f} days" if row.get("cash_conversion_cycle") is not None else None,
        },
        "cash_flow": {
            "operating_cash_flow": _fmt_huf(row.get("operating_cash_flow")),
            "free_cash_flow": _fmt_huf(row.get("free_cash_flow")),
            "closing_cash_balance": _fmt_huf(row.get("closing_cash_balance")),
        },
        "alerts": [],
    }


def _build_alerts(row) -> list:
    checks = [
        ("revenue_variance_pct", "Revenue vs Budget", 10.0),
        ("revenue_yoy_pct", "Revenue vs Prior Year", 10.0),
    ]
    alerts = []
    for field, label, threshold in checks:
        val = row.get(field)
        if val is not None and not pd.isna(val) and abs(float(val)) > threshold:
            alerts.append(
                {
                    "kpi": label,
                    "value": _fmt_pct(val),
                    "alert_tag": "[ALERT]",
                    "direction": "above target" if float(val) > 0 else "below target",
                }
            )
    return alerts


def build_variance_fact_pack(conn: pyodbc.Connection, entity_code: str, period_key: int) -> dict:
    logger.info("Building Variance Fact Pack for %s period %s", entity_code, period_key)
    df = pd.read_sql(_FACT_PACK_QUERY, conn, params=[entity_code, period_key])
    if df.empty:
        raise ValueError(f"No Gold Zone data found for entity_code={entity_code}, period_key={period_key}")

    row = df.iloc[0]
    period_label = _parse_period_label(period_key)
    fact_pack = _build_fact_sections(row, period_label, period_key, entity_code)
    fact_pack["alerts"] = _build_alerts(row)

    validate_fact_pack(fact_pack)

    return fact_pack


def load_system_prompt(role: str) -> str:
    prompt_file = os.path.join(PROMPT_DIR, f"system_prompt_{role.lower()}_commentary.txt")
    if not os.path.exists(prompt_file):
        prompt_file = os.path.join(PROMPT_DIR, "system_prompt_cfo_commentary.txt")
    with open(prompt_file, "r", encoding="utf-8") as f:
        return f.read()


def load_validation_schema() -> dict:
    with open(VALIDATION_SCHEMA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_hungarian_translation_prompt() -> str:
    prompt_file = os.path.join(PROMPT_DIR, "system_prompt_hu_translation.txt")
    with open(prompt_file, "r", encoding="utf-8") as f:
        return f.read()


def validate_fact_pack(fact_pack: dict) -> None:
    try:
        schema = load_validation_schema()
        validate(instance=fact_pack, schema=schema)
        logger.info("Fact pack validation passed")
    except ValidationError as e:
        logger.warning("Fact pack validation warning: %s", e.message)
    except (OSError, json.JSONDecodeError, Exception) as e:
        logger.warning("Fact pack validation skipped due to schema error: %s", str(e))


def generate_commentary(client: AzureOpenAI, fact_pack: dict, role: str, language: str = "en") -> str:
    system_prompt = load_system_prompt(role)

    user_message = (
        "Please generate the management commentary for the following period.\n\n"
        f"VARIANCE FACT PACK:\n{json.dumps(fact_pack, indent=2, ensure_ascii=False)}\n\n"
        f"Maximum {MAX_COMMENTARY_WORDS} words."
    )

    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        temperature=0.3,
        max_tokens=1200,
        top_p=0.95,
    )

    english_commentary = response.choices[0].message.content

    if language == "hu":
        return translate_commentary_to_hungarian(client, english_commentary)

    return english_commentary


def translate_commentary_to_hungarian(client: AzureOpenAI, english_commentary: str) -> str:
    translation_prompt = load_hungarian_translation_prompt()

    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": translation_prompt},
            {"role": "user", "content": english_commentary},
        ],
        temperature=0.2,
        max_tokens=1500,
        top_p=0.95,
    )

    return response.choices[0].message.content


def write_commentary_to_queue(
    conn: pyodbc.Connection,
    entity_code: str,
    period_key: int,
    role: str,
    language: str,
    commentary: str,
    fact_pack: dict,
    prompt_version: str = "1.0",
) -> None:
    insert_sql = """
        INSERT INTO audit.commentary_queue
            (entity_id, period_id, commentary_role, language_code,
             narrative_text, variance_fact_pack, prompt_version,
             approval_status, generated_at, generated_by_model)
        VALUES (
            (SELECT entity_id FROM config.ref_entity_master WHERE entity_code = ?),
            ?, ?, ?, ?, ?::jsonb, ?, 'PENDING_REVIEW', GETUTCDATE(), ?
        )
    """
    cursor = conn.cursor()
    cursor.execute(
        insert_sql,
        (
            entity_code,
            period_key,
            role,
            language.upper(),
            commentary,
            json.dumps(fact_pack, ensure_ascii=False),
            prompt_version,
            AZURE_OPENAI_DEPLOYMENT,
        ),
    )
    conn.commit()
    cursor.close()


def parse_arguments():
    parser = argparse.ArgumentParser(description="FIP AI Commentary Generator")
    parser.add_argument("--entity_code", required=False, help="Entity code (canonical business key)")
    parser.add_argument("--company_id", required=False, help="Deprecated alias for --entity_code")
    parser.add_argument("--period_key", required=True, type=int, help="Period key (YYYYMM)")
    parser.add_argument("--roles", default="CFO", help="Comma separated roles: CFO,CEO,BOARD,INVESTOR")
    parser.add_argument("--languages", default="en", help="Comma separated languages: en,hu")
    return parser.parse_args()


def process_commentaries(
    conn,
    client,
    entity_code: str,
    period_key: int,
    roles: List[str],
    languages: List[str],
) -> List[Dict[str, Any]]:
    results = []
    fact_pack = build_variance_fact_pack(conn, entity_code, period_key)

    for role in roles:
        for lang in languages:
            try:
                commentary = generate_commentary(client, fact_pack, role, lang)
                write_commentary_to_queue(conn, entity_code, period_key, role, lang, commentary, fact_pack)
                results.append({"role": role, "language": lang, "status": "QUEUED"})
            except Exception as e:
                logger.error("Commentary generation failed role=%s lang=%s: %s", role, lang, e)
                results.append({"role": role, "language": lang, "status": "FAILED", "error": str(e)})
    return results


def main():
    args = parse_arguments()
    entity_code = args.entity_code or args.company_id
    if not entity_code:
        raise ValueError("Provide --entity_code (or legacy --company_id)")

    roles = [r.strip().upper() for r in args.roles.split(",")]
    languages = [l.strip().lower() for l in args.languages.split(",")]

    client = get_openai_client()
    with get_db_connection() as conn:
        results = process_commentaries(conn, client, entity_code, args.period_key, roles, languages)

    queued_count = sum(1 for r in results if r.get("status") == "QUEUED")
    failed_count = sum(1 for r in results if r.get("status") == "FAILED")

    summary = {
        "status": "COMPLETED",
        "period_key": args.period_key,
        "entity_code": entity_code,
        "commentaries_generated": queued_count,
        "failures": failed_count,
        "results": results,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
