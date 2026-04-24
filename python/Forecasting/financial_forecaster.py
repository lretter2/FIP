"""
Financial Intelligence Platform — Revenue & Cost Forecasting Engine
===================================================================
Part 5.2 of the Master Architecture Guide · HU GAAP

Two-model ensemble: Prophet (primary, ≥12 months history) + ARIMA fallback (6–11 months).
Writes 12-month rolling forecasts to budget.fact_forecast; connects via Key Vault.

Usage:
    python financial_forecaster.py --entity_code ENTITY001 [--forecast_months 12] [--base_period_key 202601]
"""

import argparse
import json
import logging
import os
import sys
import uuid
import warnings
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pyodbc
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

warnings.filterwarnings("ignore")  # suppress Prophet / Stan verbosity

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FIP.Forecaster")

KEY_VAULT_URL             = os.getenv("KEY_VAULT_URL", "")
if not KEY_VAULT_URL:
    raise EnvironmentError(
        "KEY_VAULT_URL environment variable is not set. "
        "Add it to your .env file (see .env.example) before running this module."
    )
SYNAPSE_CONNECTION_SECRET = "synapse-connection-string"
FORECAST_SOURCE           = "PROPHET"
MIN_HISTORY_MONTHS        = 12
ARIMA_FALLBACK_MONTHS     = 6

FORECAST_SERIES = {
    "revenue":      "Revenue forecast",
    "cogs":         "COGS forecast",
    "gross_profit": "Gross profit forecast",
    "ebitda":       "EBITDA forecast",
    "net_profit":   "Net profit forecast",
}
CASHFLOW_SERIES = {"operating_cash_flow": "Operating cash flow forecast"}
ALL_SERIES = {**FORECAST_SERIES, **CASHFLOW_SERIES}


def get_connection() -> pyodbc.Connection:
    """Return a pyodbc connection to Azure Synapse via Key Vault connection string."""
    try:
        client = SecretClient(vault_url=KEY_VAULT_URL, credential=ManagedIdentityCredential())
        conn_str = client.get_secret(SYNAPSE_CONNECTION_SECRET).value
    except Exception:
        conn_str = os.getenv("SYNAPSE_CONNECTION_STRING")
        if not conn_str:
            raise EnvironmentError(
                "Could not retrieve Synapse connection string from Key Vault "
                "and SYNAPSE_CONNECTION_STRING env var is not set."
            )
    return pyodbc.connect(conn_str)


def fetch_actuals(conn, entity_code: str) -> pd.DataFrame:
    """Pull monthly P&L actuals for the given entity from gold.agg_pl_monthly."""
    query = """
        SELECT
            p.period_key,
            CAST(CAST(p.period_key / 100 AS VARCHAR) + '-'
                 + RIGHT('0' + CAST(p.period_key % 100 AS VARCHAR), 2) + '-01'
                 AS DATE)                AS ds,
            p.revenue,
            p.cogs,
            p.gross_profit,
            p.ebitda,
            p.net_profit,
            c.operating_cash_flow
        FROM gold.agg_pl_monthly p
        LEFT JOIN gold.agg_cashflow c
            ON  p.period_key = c.period_key
            AND p.entity_key = c.entity_key
        INNER JOIN silver.dim_entity e
            ON  p.entity_key = e.entity_key
            AND e.entity_code = ?
        ORDER BY p.period_key ASC
    """
    df = pd.read_sql(query, conn, params=[entity_code])
    logger.info("Loaded %d months of actuals for entity %s", len(df), entity_code)
    return df


def fetch_entity_key(conn, entity_code: str) -> int:
    """Return the integer entity_key for a given entity_code."""
    row = conn.execute(
        "SELECT entity_key FROM silver.dim_entity WHERE entity_code = ?",
        [entity_code]
    ).fetchone()
    if not row:
        raise ValueError(f"Entity not found in silver.dim_entity: {entity_code}")
    return row[0]


def run_prophet_forecast(
    history: pd.DataFrame,
    kpi_col: str,
    forecast_months: int,
    changepoint_prior_scale: float = 0.05,
) -> pd.DataFrame:
    """Fit Prophet on a single KPI series and return future predictions."""
    try:
        from prophet import Prophet
    except ImportError:
        raise ImportError("prophet package is required: pip install prophet")

    series = history[["ds", kpi_col]].rename(columns={kpi_col: "y"}).copy()
    series = series.dropna(subset=["y"])
    series["ds"] = pd.to_datetime(series["ds"])

    model = Prophet(
        changepoint_prior_scale=changepoint_prior_scale,
        seasonality_mode="multiplicative",
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        interval_width=0.80,
    )

    try:
        model.add_country_holidays(country_name="HU")
    except Exception:
        pass

    model.fit(series)
    future = model.make_future_dataframe(periods=forecast_months, freq="MS")
    forecast = model.predict(future)
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(forecast_months)


def run_arima_fallback(
    history: pd.DataFrame,
    kpi_col: str,
    forecast_months: int,
) -> pd.DataFrame:
    """ARIMA(1,1,1) fallback for short history series; returns [ds, yhat, yhat_lower, yhat_upper]."""
    from statsmodels.tsa.arima.model import ARIMA

    series = history[kpi_col].dropna().values
    if len(series) < ARIMA_FALLBACK_MONTHS:
        logger.warning(
            "Insufficient history (%d months) for %s — skipping ARIMA", len(series), kpi_col
        )
        return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])

    result = ARIMA(series, order=(1, 1, 1)).fit()
    forecast_obj = result.get_forecast(steps=forecast_months)
    pred = forecast_obj.predicted_mean
    conf_int = forecast_obj.conf_int(alpha=0.20)

    last_date = pd.to_datetime(history["ds"].iloc[-1])
    future_dates = pd.date_range(start=last_date, periods=forecast_months + 1, freq="MS")[1:]

    return pd.DataFrame({
        "ds":         future_dates,
        "yhat":       pred,
        "yhat_lower": conf_int.iloc[:, 0],
        "yhat_upper": conf_int.iloc[:, 1],
    })


def date_to_period_key(dt) -> int:
    """Convert a datetime to YYYYMM integer."""
    return dt.year * 100 + dt.month


def write_forecast(
    conn,
    entity_key: int,
    entity_code: str,
    kpi_col: str,
    forecast_df: pd.DataFrame,
    forecast_run_id: str,
    base_period_key: int,
) -> int:
    """Upsert forecast rows into budget.fact_forecast; returns rows written."""
    if forecast_df.empty:
        return 0

    cursor = conn.cursor()
    rows_written = 0

    for _, row in forecast_df.iterrows():
        period_key = date_to_period_key(pd.to_datetime(row["ds"]))
        fiscal_year = period_key // 100
        fiscal_period = period_key % 100

        cursor.execute(
            """
            DELETE FROM budget.fact_forecast
            WHERE entity_key = ?
              AND period_key  = ?
              AND kpi_name    = ?
              AND forecast_source = ?
            """,
            [entity_key, period_key, kpi_col, FORECAST_SOURCE]
        )

        cursor.execute(
            """
            INSERT INTO budget.fact_forecast (
                entity_key, period_key, fiscal_year, fiscal_period,
                kpi_name, forecast_amount_lcy,
                forecast_lower_bound_lcy, forecast_upper_bound_lcy,
                forecast_source, forecast_run_id,
                base_period_key, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                entity_key, period_key, fiscal_year, fiscal_period,
                kpi_col,
                float(row["yhat"]), float(row["yhat_lower"]), float(row["yhat_upper"]),
                FORECAST_SOURCE, forecast_run_id, base_period_key,
                datetime.now(timezone.utc),
            ]
        )
        rows_written += 1

    conn.commit()
    logger.info("Wrote %d forecast rows for kpi=%s, entity=%s", rows_written, kpi_col, entity_code)
    return rows_written


def run_forecast(
    entity_code: str,
    forecast_months: int = 12,
    base_period_key: Optional[int] = None,
) -> dict:
    """Run the full forecasting pipeline for one entity; return a summary dict."""
    forecast_run_id = str(uuid.uuid4())
    if base_period_key is None:
        now = datetime.now(timezone.utc)
        base_period_key = now.year * 100 + now.month

    logger.info(
        "Starting forecast run | entity=%s | months=%d | base_period=%d | run_id=%s",
        entity_code, forecast_months, base_period_key, forecast_run_id
    )

    conn = get_connection()
    entity_key = fetch_entity_key(conn, entity_code)
    actuals = fetch_actuals(conn, entity_code)

    total_rows_written = 0
    series_results = {}

    for kpi_col, description in ALL_SERIES.items():
        if kpi_col not in actuals.columns:
            logger.warning("KPI column %s not found in actuals — skipping", kpi_col)
            continue

        n_months = actuals[kpi_col].notna().sum()
        logger.info("Forecasting %s (%s) — %d months of history", description, kpi_col, n_months)

        try:
            if n_months >= MIN_HISTORY_MONTHS:
                forecast_df = run_prophet_forecast(actuals, kpi_col, forecast_months)
                method = "prophet"
            elif n_months >= ARIMA_FALLBACK_MONTHS:
                logger.info("Using ARIMA fallback for %s (only %d months available)", kpi_col, n_months)
                forecast_df = run_arima_fallback(actuals, kpi_col, forecast_months)
                method = "arima"
            else:
                logger.warning("Skipping %s — insufficient history (%d months)", kpi_col, n_months)
                series_results[kpi_col] = {"status": "skipped", "rows": 0}
                continue

            rows = write_forecast(
                conn, entity_key, entity_code, kpi_col,
                forecast_df, forecast_run_id, base_period_key
            )
            total_rows_written += rows
            series_results[kpi_col] = {"status": "ok", "method": method, "rows": rows}

        except Exception as exc:
            logger.error("Failed to forecast %s: %s", kpi_col, exc, exc_info=True)
            series_results[kpi_col] = {"status": "error", "error": str(exc)}

    conn.close()

    summary = {
        "forecast_run_id":    forecast_run_id,
        "entity_code":        entity_code,
        "base_period_key":    base_period_key,
        "forecast_months":    forecast_months,
        "total_rows_written": total_rows_written,
        "series":             series_results,
        "completed_at":       datetime.now(timezone.utc).isoformat(),
    }
    logger.info("Forecast run complete: %s", summary)
    return summary


def parse_args():
    parser = argparse.ArgumentParser(description="FIP Revenue & Cost Forecasting Engine")
    parser.add_argument("--entity_code",     required=True,  help="Entity code e.g. ACME_HU")
    parser.add_argument("--forecast_months", type=int, default=12, help="Months to forecast ahead (default 12)")
    parser.add_argument("--base_period_key", type=int, default=None, help="Base YYYYMM period (default: current month)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = run_forecast(
        entity_code=args.entity_code,
        forecast_months=args.forecast_months,
        base_period_key=args.base_period_key,
    )
    sys.exit(0 if all(v.get("status") != "error" for v in result["series"].values()) else 1)
