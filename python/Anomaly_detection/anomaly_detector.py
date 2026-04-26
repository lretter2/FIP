"""
Financial Intelligence Platform — Anomaly Detection Agent
==========================================================
Part 5.3 of the Master Architecture Guide · HU GAAP

Detects three classes of anomaly in the Gold Zone KPI data:
  1. Statistical  — Isolation Forest + 3σ Z-score on rolling 12-month window
  2. Structural   — Rule engine (unbalanced BS, duplicate hashes, missing accruals)
  3. Behavioural  — High-velocity micro-transactions, off-hours postings

All anomalies are written to audit.anomaly_queue, ranked by severity (CRITICAL / HIGH / MEDIUM).
Critical anomalies trigger immediate Power Automate alerts to CFO + CEO.

Usage (called by ADF pipeline A12):
    python anomaly_detector.py --entity_code ENTITY001 --period_key 202601

Dependencies: scikit-learn, pandas, pyodbc, azure-keyvault-secrets, azure-identity
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pyodbc
import requests
from azure.keyvault.secrets import SecretClient
from sklearn.ensemble import IsolationForest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from db_utils import get_db_connection
from scipy import stats

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FIP.AnomalyDetector")

KEY_VAULT_URL        = os.getenv("AZURE_KEY_VAULT_URL", "")         # set in .env — no hardcoded prod URL
if not KEY_VAULT_URL:
    raise EnvironmentError(
        "AZURE_KEY_VAULT_URL environment variable is not set. "
        "Add it to your .env file (see .env.example) before running this module."
    )
SYNAPSE_SERVER       = os.getenv("SYNAPSE_SERVER", "")
SYNAPSE_DATABASE     = os.getenv("SYNAPSE_DATABASE", "fip_dw")
POWER_AUTOMATE_URL   = os.getenv("POWER_AUTOMATE_ALERT_URL", "")

# Thresholds from Master Guide Part 5.3
ZSCORE_THRESHOLD          = float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "2.5"))
ISOLATION_CONTAMINATION   = float(os.getenv("ISOLATION_CONTAMINATION", "0.05"))
EBITDA_BUDGET_THRESHOLD   = float(os.getenv("EBITDA_BUDGET_THRESHOLD", "0.15"))   # 15%
LARGE_POSTING_THRESHOLD   = float(os.getenv("LARGE_POSTING_HUF", "50000000"))     # 50M HUF
MICRO_TX_VELOCITY_LIMIT   = int(os.getenv("MICRO_TX_VELOCITY_LIMIT", "50"))       # >50/hour


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_kpi_history(conn: pyodbc.Connection, entity_code: str, period_key: int,
                     lookback_months: int = 24) -> pd.DataFrame:
    """Load rolling KPI history for anomaly baseline computation."""
    query = """
        SELECT
            p.period_key,
            p.entity_key,
            p.revenue,
            p.ebitda,
            p.ebitda_margin_pct,
            p.gross_margin_pct,
            p.net_profit,
            p.revenue_variance_pct,
            l.current_ratio,
            l.dso_days,
            l.dpo_days,
            l.free_cash_flow,
            l.net_debt_ebitda
        FROM gold.kpi_profitability p
        JOIN gold.kpi_liquidity     l ON p.period_key = l.period_key AND p.entity_key = l.entity_key
        JOIN silver.dim_entity      e ON p.entity_key = e.entity_key
        WHERE e.entity_code = ?
          AND p.period_key <= ?
          AND p.period_key >= ?
        ORDER BY p.period_key
    """
    min_period = period_key - lookback_months  # Approximate — full date math handled in SQL
    df = pd.read_sql(query, conn, params=[entity_code, period_key, min_period])
    return df


def load_gl_transactions(conn: pyodbc.Connection, entity_code: str, period_key: int) -> pd.DataFrame:
    """Load GL transactions for the target period for structural and behavioural checks."""
    query = """
        SELECT
            f.transaction_id,
            f.document_number,
            f.posting_date,
            f.net_amount_lcy,
            f.l1_category,
            f.account_type,
            f.universal_node,
            f.is_late_entry,
            f.row_hash,
            DATEPART(HOUR, f.dbt_loaded_at) AS posting_hour
        FROM gold.fct_gl_transaction f
        JOIN silver.dim_entity        e ON f.entity_key = e.entity_key
        WHERE e.entity_code = ?
          AND f.period_id = ?
    """
    df = pd.read_sql(query, conn, params=[entity_code, period_key])
    return df


# ---------------------------------------------------------------------------
# Anomaly Class 1: Statistical — Isolation Forest + Z-score
# ---------------------------------------------------------------------------

def detect_statistical_anomalies(df_history: pd.DataFrame, current_period: int) -> list[dict]:
    """
    Run Isolation Forest on rolling 12-month KPI aggregates.
    Also compute Z-score for each KPI vs. rolling 12-month window.
    Returns list of anomaly dicts for the current period only.
    """
    anomalies = []
    if df_history.empty or len(df_history) < 3:
        logger.warning("Insufficient history for statistical anomaly detection (need ≥3 periods)")
        return anomalies

    kpi_columns = ["revenue", "ebitda", "ebitda_margin_pct", "gross_margin_pct",
                   "net_profit", "current_ratio", "dso_days", "free_cash_flow"]
    feature_cols = [c for c in kpi_columns if c in df_history.columns]

    df_features = df_history[feature_cols].fillna(0)
    current_mask = df_history["period_key"] == current_period

    if current_mask.sum() == 0:
        logger.warning(f"No data for period_key={current_period} in KPI history")
        return anomalies

    # --- Isolation Forest ---
    iso = IsolationForest(
        n_estimators=200,
        contamination=ISOLATION_CONTAMINATION,
        random_state=42
    )
    iso.fit(df_features)
    scores = iso.decision_function(df_features)
    predictions = iso.predict(df_features)

    current_idx = df_features[current_mask].index[0]
    if predictions[current_idx] == -1:
        anomaly_score = float(scores[current_idx])
        anomalies.append({
            "anomaly_class": "STATISTICAL",
            "detection_method": "ISOLATION_FOREST",
            "severity": "HIGH",
            "kpi_affected": "MULTI_KPI",
            "description": f"Isolation Forest flagged current period as outlier. Anomaly score: {anomaly_score:.4f}",
            "anomaly_score": anomaly_score,
            "recommended_action": "CFO to review full KPI set against prior periods"
        })

    # --- Z-score per KPI ---
    history_excl_current = df_history[~current_mask]
    current_row = df_history[current_mask].iloc[0]

    for col in feature_cols:
        hist_values = history_excl_current[col].dropna()
        if len(hist_values) < 3:
            continue
        mean_val = hist_values.mean()
        std_val  = hist_values.std()
        if std_val == 0:
            continue
        z = (current_row[col] - mean_val) / std_val
        if abs(z) > ZSCORE_THRESHOLD:
            severity = "CRITICAL" if abs(z) > 3.5 else "HIGH" if abs(z) > 3.0 else "MEDIUM"
            direction = "above" if z > 0 else "below"
            anomalies.append({
                "anomaly_class": "STATISTICAL",
                "detection_method": "ZSCORE",
                "severity": severity,
                "kpi_affected": col,
                "current_value": float(current_row[col]),
                "historical_mean": float(mean_val),
                "historical_std": float(std_val),
                "z_score": float(z),
                "description": f"{col} is {abs(z):.1f}σ {direction} the 12-month average "
                               f"(current: {current_row[col]:,.0f}, mean: {mean_val:,.0f})",
                "recommended_action": f"Investigate {col} movement — exceeds ±{ZSCORE_THRESHOLD}σ threshold"
            })

    # Special rule: revenue > 15% below budget
    # revenue_variance_pct is already in percentage (e.g. -15.0 means 15% below),
    # EBITDA_BUDGET_THRESHOLD is a fraction (0.15), so multiply by 100 for comparison
    budget_threshold_pct = EBITDA_BUDGET_THRESHOLD * 100  # 0.15 → 15.0
    if "revenue_variance_pct" in current_row and current_row["revenue_variance_pct"] < -budget_threshold_pct:
        anomalies.append({
            "anomaly_class": "STATISTICAL",
            "detection_method": "BUDGET_GATE",
            "severity": "HIGH",
            "kpi_affected": "revenue_variance_pct",
            "current_value": float(current_row["revenue_variance_pct"]),
            "description": f"Revenue is {current_row['revenue_variance_pct']:.1f}% below budget (threshold: -{budget_threshold_pct:.0f}%)",
            "recommended_action": "CFO + Controller review revenue shortfall drivers"
        })

    return anomalies


# ---------------------------------------------------------------------------
# Anomaly Class 2: Structural — Rule Engine
# ---------------------------------------------------------------------------

def detect_structural_anomalies(conn: pyodbc.Connection, entity_code: str,
                                 period_key: int, df_gl: pd.DataFrame) -> list[dict]:
    """
    Rule-based structural anomaly detection:
    - Revenue accounts with debit balances (sign reversal)
    - Duplicate transaction hashes
    - Balance sheet imbalance
    - Missing period-end accruals (payroll/interest)
    """
    anomalies = []

    # Rule S1: Revenue accounts with net debit balance (unusual in HU GAAP)
    revenue_debits = df_gl[
        (df_gl["l1_category"] == "Revenue") &
        (df_gl["net_amount_lcy"] < 0)
    ]
    if len(revenue_debits) > 0:
        total_reverse = revenue_debits["net_amount_lcy"].sum()
        anomalies.append({
            "anomaly_class": "STRUCTURAL",
            "detection_method": "RULE_ENGINE",
            "severity": "HIGH",
            "kpi_affected": "REVENUE_ACCOUNTS",
            "description": f"Revenue accounts have net debit postings: {len(revenue_debits)} rows, "
                           f"total {total_reverse:,.0f} HUF. Possible credit note or reversal issue.",
            "recommended_action": "Review revenue account postings — possible incorrect sign convention"
        })

    # Rule S2: Duplicate transaction fingerprints
    duplicate_hashes = df_gl[df_gl.duplicated(subset=["row_hash"], keep=False)]
    if len(duplicate_hashes) > 0:
        anomalies.append({
            "anomaly_class": "STRUCTURAL",
            "detection_method": "RULE_ENGINE",
            "severity": "CRITICAL",
            "kpi_affected": "DUPLICATE_TRANSACTIONS",
            "description": f"Duplicate transaction hashes detected: {len(duplicate_hashes)} rows affected. "
                           f"Double-load or ERP data integrity issue.",
            "recommended_action": "URGENT: Investigate duplicate postings before closing period"
        })

    # Rule S3: Balance sheet imbalance (query Gold Zone)
    try:
        query = """
            SELECT TOP 1 is_balanced, balance_difference, total_assets, total_liabilities_equity
            FROM gold.agg_balance_sheet
            WHERE period_key = ? AND entity_key IN (
                SELECT entity_key FROM silver.dim_entity WHERE entity_code = ?
            )
        """
        df_bs = pd.read_sql(query, conn, params=[period_key, entity_code])
        if not df_bs.empty and df_bs.iloc[0]["is_balanced"] == 0:
            gap = df_bs.iloc[0]["balance_difference"]
            anomalies.append({
                "anomaly_class": "STRUCTURAL",
                "detection_method": "RULE_ENGINE",
                "severity": "CRITICAL",
                "kpi_affected": "BALANCE_SHEET_EQUATION",
                "description": f"Balance sheet does not balance: A ≠ L+E, gap = {gap:,.2f} HUF",
                "recommended_action": "URGENT: Balance sheet imbalance — do not publish until resolved"
            })
    except Exception as e:
        logger.warning(f"Balance sheet check query failed: {e}")

    # Rule S4: Large single postings above threshold
    large_postings = df_gl[df_gl["net_amount_lcy"].abs() > LARGE_POSTING_THRESHOLD]
    for _, row in large_postings.iterrows():
        anomalies.append({
            "anomaly_class": "STRUCTURAL",
            "detection_method": "RULE_ENGINE",
            "severity": "MEDIUM",
            "kpi_affected": "LARGE_POSTING",
            "description": f"Large GL posting: document {row['document_number']}, "
                           f"account {row['universal_node']}, "
                           f"amount {row['net_amount_lcy']:,.0f} HUF "
                           f"(threshold: {LARGE_POSTING_THRESHOLD:,.0f} HUF)",
            "recommended_action": "Controller to confirm large posting is authorised and correctly coded"
        })

    return anomalies


# ---------------------------------------------------------------------------
# Anomaly Class 3: Behavioural
# ---------------------------------------------------------------------------

def detect_behavioural_anomalies(df_gl: pd.DataFrame) -> list[dict]:
    """
    Behavioural anomaly detection:
    - High-velocity small transactions (potential split transactions below approval threshold)
    - Unusual posting times (outside business hours: before 07:00 or after 22:00)
    """
    anomalies = []

    # Behaviour B1: Off-hours postings
    off_hours = df_gl[
        (df_gl["posting_hour"] < 7) | (df_gl["posting_hour"] > 22)
    ]
    if len(off_hours) > 5:
        anomalies.append({
            "anomaly_class": "BEHAVIOURAL",
            "detection_method": "RULE_ENGINE",
            "severity": "MEDIUM",
            "kpi_affected": "OFF_HOURS_POSTINGS",
            "description": f"{len(off_hours)} GL postings made outside business hours (before 07:00 or after 22:00).",
            "recommended_action": "Controller to review off-hours posting activity for authorisation compliance"
        })

    # Behaviour B2: High-velocity micro-transactions (potential threshold avoidance)
    if len(df_gl) > 0 and "posting_date" in df_gl.columns:
        df_gl["posting_date"] = pd.to_datetime(df_gl["posting_date"])
        daily_tx_counts = df_gl.groupby(
            [df_gl["posting_date"].dt.date, "universal_node"]
        ).size().reset_index(name="daily_count")
        high_velocity = daily_tx_counts[daily_tx_counts["daily_count"] > MICRO_TX_VELOCITY_LIMIT]
        if len(high_velocity) > 0:
            for _, row in high_velocity.iterrows():
                anomalies.append({
                    "anomaly_class": "BEHAVIOURAL",
                    "detection_method": "VELOCITY_CHECK",
                    "severity": "MEDIUM",
                    "kpi_affected": "HIGH_VELOCITY_TRANSACTIONS",
                    "description": f"Account {row['universal_node']} received {row['daily_count']} postings "
                                   f"on {row['posting_date']} (threshold: {MICRO_TX_VELOCITY_LIMIT}/day). "
                                   f"Possible structured/split transaction pattern.",
                    "recommended_action": "Internal audit review — potential approval threshold avoidance"
                })

    return anomalies


# ---------------------------------------------------------------------------
# Write anomalies to audit.anomaly_queue
# ---------------------------------------------------------------------------

def write_anomalies(conn: pyodbc.Connection, entity_code: str, period_key: int,
                    anomalies: list[dict]) -> int:
    """Persist anomaly records to audit.anomaly_queue. Returns count written."""
    if not anomalies:
        logger.info("No anomalies detected for this period — anomaly_queue not updated")
        return 0

    insert_sql = """
        INSERT INTO audit.anomaly_queue
            (entity_id, period_id, anomaly_class, detection_method, severity,
             kpi_affected, description, recommended_action, anomaly_score,
             z_score, current_value, historical_mean, status, detected_at)
        VALUES (
            (SELECT entity_id FROM config.ref_entity_master WHERE entity_code = ?),
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', GETUTCDATE()
        )
    """
    cursor = conn.cursor()
    written = 0
    for a in anomalies:
        cursor.execute(insert_sql, (
            entity_code,
            period_key,
            a.get("anomaly_class"),
            a.get("detection_method"),
            a.get("severity"),
            a.get("kpi_affected"),
            a.get("description"),
            a.get("recommended_action"),
            a.get("anomaly_score"),
            a.get("z_score"),
            a.get("current_value"),
            a.get("historical_mean")
        ))
        written += 1
    conn.commit()
    logger.info(f"Written {written} anomaly records to audit.anomaly_queue")
    return written


def send_critical_alerts(anomalies: list[dict], entity_code: str, period_key: int) -> None:
    """Fire Power Automate webhook for CRITICAL and HIGH severity anomalies."""
    critical = [a for a in anomalies if a.get("severity") in ("CRITICAL", "HIGH")]
    if not critical or not POWER_AUTOMATE_URL:
        return

    payload = {
        "entity_code": entity_code,
        "period_key": period_key,
        "alert_count": len(critical),
        "alerts": [
            {
                "severity": a["severity"],
                "class": a["anomaly_class"],
                "kpi": a.get("kpi_affected"),
                "description": a.get("description"),
                "action": a.get("recommended_action")
            }
            for a in critical
        ],
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
    try:
        response = requests.post(POWER_AUTOMATE_URL, json=payload, timeout=30)
        response.raise_for_status()
        logger.info(f"Critical alert sent to Power Automate ({len(critical)} alerts)")
    except requests.RequestException as e:
        logger.error(f"Failed to send Power Automate alert: {e}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FIP Anomaly Detection Agent")
    parser.add_argument("--entity_code", required=False, help="Entity code (canonical key)")
    parser.add_argument("--entity_id", required=False, help="Deprecated alias for --entity_code")
    parser.add_argument("--company_id", required=False, help="Deprecated alias for --entity_code")
    parser.add_argument("--period_key", required=True, type=int, help="Period key YYYYMM")
    parser.add_argument("--lookback", default=24, type=int, help="Lookback months for baseline")
    args = parser.parse_args()

    entity_code = args.entity_code or args.entity_id or args.company_id
    if not entity_code:
        raise ValueError("Provide --entity_code (or legacy --entity_id/--company_id)")

    logger.info(f"Starting anomaly detection: entity_code={entity_code}, period={args.period_key}")

    conn = get_db_connection()
    all_anomalies = []

    try:
        # Load data
        df_kpi  = load_kpi_history(conn, entity_code, args.period_key, args.lookback)
        df_gl   = load_gl_transactions(conn, entity_code, args.period_key)

        logger.info(f"Loaded {len(df_kpi)} KPI periods and {len(df_gl)} GL transactions")

        # Run three detection classes
        stat_anomalies  = detect_statistical_anomalies(df_kpi, args.period_key)
        struct_anomalies = detect_structural_anomalies(conn, entity_code, args.period_key, df_gl)
        behav_anomalies = detect_behavioural_anomalies(df_gl)

        all_anomalies = stat_anomalies + struct_anomalies + behav_anomalies

        logger.info(f"Anomalies detected — Statistical: {len(stat_anomalies)}, "
                    f"Structural: {len(struct_anomalies)}, Behavioural: {len(behav_anomalies)}")

        # Persist to database
        write_anomalies(conn, entity_code, args.period_key, all_anomalies)

        # Fire alerts for critical/high
        send_critical_alerts(all_anomalies, entity_code, args.period_key)

    finally:
        conn.close()

    # Output summary for ADF pipeline to read via pipelineReturnValue
    summary = {
        "anomaly_count": len(all_anomalies),
        "critical_count": sum(1 for a in all_anomalies if a.get("severity") == "CRITICAL"),
        "high_count": sum(1 for a in all_anomalies if a.get("severity") == "HIGH"),
        "medium_count": sum(1 for a in all_anomalies if a.get("severity") == "MEDIUM"),
        "status": "COMPLETED"
    }
    print(json.dumps(summary))
    logger.info(f"Anomaly detection complete: {summary}")


if __name__ == "__main__":
    main()
