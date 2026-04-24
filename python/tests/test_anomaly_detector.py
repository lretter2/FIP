"""Unit tests for anomaly detector business logic.
All three detection functions operate purely on DataFrames with no database
calls, so these tests run without any Azure infrastructure.
"""
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

# Stub heavy Azure/OpenAI modules that anomaly_detector pulls in transitively
# via db_utils. The detection functions under test never call them.
for _mod in ("openai", "azure.storage.blob", "azure.storage.file_datalake",
             "tiktoken", "sqlalchemy"):
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# Provide a minimal db_utils stub so anomaly_detector's top-level import works
_db_utils_stub = ModuleType("db_utils")
_db_utils_stub.get_db_connection = MagicMock()  # type: ignore[attr-defined]
sys.modules.setdefault("db_utils", _db_utils_stub)

import pytest
import numpy as np
import pandas as pd
from typing import List, Optional

from anomaly_detector import (
    EBITDA_BUDGET_THRESHOLD,
    LARGE_POSTING_THRESHOLD,
    MICRO_TX_VELOCITY_LIMIT,
    ZSCORE_THRESHOLD,
    detect_behavioural_anomalies,
    detect_statistical_anomalies,
    detect_structural_anomalies,
)

_CURRENT_PERIOD = 202601
_BUDGET_THRESHOLD_PCT = EBITDA_BUDGET_THRESHOLD * 100  # e.g. 15.0


# ── Fixtures / builders ───────────────────────────────────────────────────────

def _make_kpi_history(
    n_history: int = 20,
    current_period: int = _CURRENT_PERIOD,
    revenue_override: Optional[float] = None,
    variance_pct_override: Optional[float] = None,
    seed: int = 42,
) -> pd.DataFrame:
    """Build a KPI history DataFrame with one row per period.

    The current_period row has metrics set to the historical mean so it does
    not trigger anomalies unless overrides are passed.
    """
    rng = np.random.default_rng(seed)
    n_total = n_history + 1
    periods = list(range(current_period - n_history, current_period)) + [current_period]

    revenue_base = 100_000_000.0
    revenue = rng.normal(revenue_base, revenue_base * 0.05, n_total)
    ebitda = revenue * rng.normal(0.30, 0.01, n_total)

    df = pd.DataFrame({
        "period_key": periods,
        "entity_key": [1] * n_total,
        "revenue": revenue,
        "ebitda": ebitda,
        "ebitda_margin_pct": rng.normal(30.0, 0.8, n_total),
        "gross_margin_pct": rng.normal(45.0, 1.0, n_total),
        "net_profit": rng.normal(18_000_000, 1_500_000, n_total),
        "current_ratio": rng.normal(1.8, 0.05, n_total),
        "dso_days": rng.normal(45.0, 2.0, n_total),
        "free_cash_flow": rng.normal(20_000_000, 1_500_000, n_total),
        "revenue_variance_pct": [0.0] * n_total,
    })

    # Clamp current period to historical mean so it is not an outlier by default
    hist_mask = df["period_key"] != current_period
    cur_mask = df["period_key"] == current_period
    for col in ["revenue", "ebitda", "ebitda_margin_pct", "gross_margin_pct",
                "net_profit", "current_ratio", "dso_days", "free_cash_flow"]:
        df.loc[cur_mask, col] = df.loc[hist_mask, col].mean()

    if revenue_override is not None:
        df.loc[cur_mask, "revenue"] = revenue_override
    if variance_pct_override is not None:
        df.loc[cur_mask, "revenue_variance_pct"] = variance_pct_override

    return df


def _make_gl_df(
    n_rows: int = 10,
    l1_category: str = "Expense",
    net_amounts: Optional[List[float]] = None,
    posting_hour: int = 10,
    posting_date: str = "2026-01-15",
    universal_node: str = "EXPENSE.OPEX",
    unique_hashes: bool = True,
) -> pd.DataFrame:
    if net_amounts is None:
        net_amounts = [1_000_000.0] * n_rows
    assert len(net_amounts) == n_rows

    return pd.DataFrame({
        "transaction_id": [f"TX{i}" for i in range(n_rows)],
        "document_number": [f"DOC{i:04d}" for i in range(n_rows)],
        "posting_date": [posting_date] * n_rows,
        "net_amount_lcy": net_amounts,
        "l1_category": [l1_category] * n_rows,
        "account_type": [l1_category] * n_rows,
        "universal_node": [universal_node] * n_rows,
        "is_late_entry": [0] * n_rows,
        "row_hash": [f"hash_{i}" if unique_hashes else "hash_dup" for i in range(n_rows)],
        "posting_hour": [posting_hour] * n_rows,
    })


# ── detect_statistical_anomalies ──────────────────────────────────────────────

class TestDetectStatisticalAnomalies:

    def test_empty_dataframe_returns_empty_list(self):
        result = detect_statistical_anomalies(pd.DataFrame(), _CURRENT_PERIOD)
        assert result == []

    def test_too_few_rows_returns_empty_list(self):
        # Function requires >= 3 rows
        df = _make_kpi_history(n_history=1)
        df = df[df["period_key"] <= _CURRENT_PERIOD - 1].head(2)
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        assert result == []

    def test_normal_period_produces_no_zscore_anomaly_for_revenue(self):
        df = _make_kpi_history(n_history=20)
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        zscore_revenue = [
            a for a in result
            if a["detection_method"] == "ZSCORE" and a["kpi_affected"] == "revenue"
        ]
        assert len(zscore_revenue) == 0

    def test_extreme_revenue_outlier_triggers_critical_zscore(self):
        df = _make_kpi_history(n_history=20)
        hist = df[df["period_key"] != _CURRENT_PERIOD]["revenue"]
        # 10 sigma above mean — well above any threshold
        df.loc[df["period_key"] == _CURRENT_PERIOD, "revenue"] = hist.mean() + 10 * hist.std()
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        zscore_hits = [
            a for a in result
            if a["detection_method"] == "ZSCORE" and a["kpi_affected"] == "revenue"
        ]
        assert len(zscore_hits) == 1
        assert zscore_hits[0]["severity"] == "CRITICAL"
        assert zscore_hits[0]["anomaly_class"] == "STATISTICAL"

    def test_3_2_sigma_outlier_severity_is_high(self):
        df = _make_kpi_history(n_history=20)
        hist = df[df["period_key"] != _CURRENT_PERIOD]["revenue"]
        # 3.2σ → HIGH (between 3.0 and 3.5)
        df.loc[df["period_key"] == _CURRENT_PERIOD, "revenue"] = hist.mean() + 3.2 * hist.std()
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        zscore_hits = [
            a for a in result
            if a["detection_method"] == "ZSCORE" and a["kpi_affected"] == "revenue"
        ]
        assert len(zscore_hits) == 1
        assert zscore_hits[0]["severity"] == "HIGH"

    def test_2_6_sigma_outlier_severity_is_medium(self):
        df = _make_kpi_history(n_history=20)
        hist = df[df["period_key"] != _CURRENT_PERIOD]["revenue"]
        # 2.6σ → MEDIUM (between ZSCORE_THRESHOLD=2.5 and 3.0)
        df.loc[df["period_key"] == _CURRENT_PERIOD, "revenue"] = hist.mean() + 2.6 * hist.std()
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        zscore_hits = [
            a for a in result
            if a["detection_method"] == "ZSCORE" and a["kpi_affected"] == "revenue"
        ]
        assert len(zscore_hits) == 1
        assert zscore_hits[0]["severity"] == "MEDIUM"

    def test_budget_gate_fires_when_revenue_variance_below_threshold(self):
        df = _make_kpi_history(n_history=20, variance_pct_override=-_BUDGET_THRESHOLD_PCT - 5)
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        budget_hits = [a for a in result if a["detection_method"] == "BUDGET_GATE"]
        assert len(budget_hits) == 1
        assert budget_hits[0]["severity"] == "HIGH"
        assert budget_hits[0]["kpi_affected"] == "revenue_variance_pct"

    def test_budget_gate_does_not_fire_when_within_threshold(self):
        df = _make_kpi_history(n_history=20, variance_pct_override=-_BUDGET_THRESHOLD_PCT + 5)
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        budget_hits = [a for a in result if a["detection_method"] == "BUDGET_GATE"]
        assert len(budget_hits) == 0

    def test_budget_gate_does_not_fire_for_positive_variance(self):
        df = _make_kpi_history(n_history=20, variance_pct_override=10.0)
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        budget_hits = [a for a in result if a["detection_method"] == "BUDGET_GATE"]
        assert len(budget_hits) == 0

    def test_all_anomalies_have_required_keys(self):
        df = _make_kpi_history(n_history=20)
        hist = df[df["period_key"] != _CURRENT_PERIOD]["revenue"]
        df.loc[df["period_key"] == _CURRENT_PERIOD, "revenue"] = hist.mean() + 10 * hist.std()
        result = detect_statistical_anomalies(df, _CURRENT_PERIOD)
        for anomaly in result:
            assert "anomaly_class" in anomaly
            assert "severity" in anomaly
            assert "description" in anomaly
            assert "recommended_action" in anomaly


# ── detect_structural_anomalies ───────────────────────────────────────────────

class TestDetectStructuralAnomalies:
    """All tests patch pd.read_sql to avoid requiring a real DB connection.

    The balance-sheet query (Rule S3) is the only part of the function that
    touches the database; the other rules operate on the in-memory DataFrame.
    """

    def _run(self, df: pd.DataFrame, bs_balanced: bool = True) -> list[dict]:
        """Run detect_structural_anomalies with a mocked DB connection."""
        conn = MagicMock()
        bs_row = pd.DataFrame([{"is_balanced": 1 if bs_balanced else 0,
                                 "balance_difference": 0.0 if bs_balanced else 999.0,
                                 "total_assets": 1_000_000.0,
                                 "total_liabilities_equity": 1_000_000.0}])
        with patch("anomaly_detector.pd.read_sql", return_value=bs_row):
            return detect_structural_anomalies(conn, "ENTITY001", _CURRENT_PERIOD, df)

    def test_clean_dataframe_produces_no_anomalies(self):
        df = _make_gl_df(n_rows=5, net_amounts=[500_000.0] * 5)
        result = self._run(df)
        assert len(result) == 0

    # Rule S1 — Revenue debit balance

    def test_s1_flags_revenue_with_negative_net_amount(self):
        df = _make_gl_df(n_rows=3, l1_category="Revenue", net_amounts=[-500_000.0] * 3)
        result = self._run(df)
        s1 = [a for a in result if a.get("kpi_affected") == "REVENUE_ACCOUNTS"]
        assert len(s1) == 1
        assert s1[0]["severity"] == "HIGH"
        assert s1[0]["anomaly_class"] == "STRUCTURAL"

    def test_s1_does_not_fire_for_expense_debits(self):
        df = _make_gl_df(n_rows=3, l1_category="Expense", net_amounts=[-500_000.0] * 3)
        result = self._run(df)
        s1 = [a for a in result if a.get("kpi_affected") == "REVENUE_ACCOUNTS"]
        assert len(s1) == 0

    def test_s1_does_not_fire_for_revenue_credits(self):
        df = _make_gl_df(n_rows=3, l1_category="Revenue", net_amounts=[500_000.0] * 3)
        result = self._run(df)
        s1 = [a for a in result if a.get("kpi_affected") == "REVENUE_ACCOUNTS"]
        assert len(s1) == 0

    # Rule S2 — Duplicate hashes

    def test_s2_flags_duplicate_row_hash(self):
        df = _make_gl_df(n_rows=4)
        df.loc[1, "row_hash"] = df.loc[0, "row_hash"]  # duplicate
        result = self._run(df)
        s2 = [a for a in result if a.get("kpi_affected") == "DUPLICATE_TRANSACTIONS"]
        assert len(s2) == 1
        assert s2[0]["severity"] == "CRITICAL"

    def test_s2_does_not_fire_for_unique_hashes(self):
        df = _make_gl_df(n_rows=4, unique_hashes=True)
        result = self._run(df)
        s2 = [a for a in result if a.get("kpi_affected") == "DUPLICATE_TRANSACTIONS"]
        assert len(s2) == 0

    # Rule S3 — Balance sheet imbalance

    def test_s3_flags_unbalanced_balance_sheet(self):
        df = _make_gl_df(n_rows=1)
        result = self._run(df, bs_balanced=False)
        s3 = [a for a in result if a.get("kpi_affected") == "BALANCE_SHEET_EQUATION"]
        assert len(s3) == 1
        assert s3[0]["severity"] == "CRITICAL"

    def test_s3_does_not_fire_when_balanced(self):
        df = _make_gl_df(n_rows=1)
        result = self._run(df, bs_balanced=True)
        s3 = [a for a in result if a.get("kpi_affected") == "BALANCE_SHEET_EQUATION"]
        assert len(s3) == 0

    # Rule S4 — Large postings

    def test_s4_flags_posting_above_threshold(self):
        large = LARGE_POSTING_THRESHOLD + 1
        df = _make_gl_df(n_rows=1, net_amounts=[large])
        result = self._run(df)
        s4 = [a for a in result if a.get("kpi_affected") == "LARGE_POSTING"]
        assert len(s4) == 1
        assert s4[0]["severity"] == "MEDIUM"

    def test_s4_flags_multiple_large_postings_individually(self):
        large = LARGE_POSTING_THRESHOLD + 1
        df = _make_gl_df(n_rows=3, net_amounts=[large, large, large])
        result = self._run(df)
        s4 = [a for a in result if a.get("kpi_affected") == "LARGE_POSTING"]
        assert len(s4) == 3

    def test_s4_does_not_fire_below_threshold(self):
        small = LARGE_POSTING_THRESHOLD - 1
        df = _make_gl_df(n_rows=2, net_amounts=[small, small])
        result = self._run(df)
        s4 = [a for a in result if a.get("kpi_affected") == "LARGE_POSTING"]
        assert len(s4) == 0

    def test_s4_checks_absolute_value(self):
        # Negative large posting should also be flagged
        large_neg = -(LARGE_POSTING_THRESHOLD + 1)
        df = _make_gl_df(n_rows=1, net_amounts=[large_neg])
        result = self._run(df)
        s4 = [a for a in result if a.get("kpi_affected") == "LARGE_POSTING"]
        assert len(s4) == 1


# ── detect_behavioural_anomalies ──────────────────────────────────────────────

class TestDetectBehaviouralAnomalies:

    def test_clean_dataframe_produces_no_anomalies(self):
        df = _make_gl_df(n_rows=10, posting_hour=10)
        assert detect_behavioural_anomalies(df) == []

    # Rule B1 — Off-hours postings

    def test_b1_fires_for_more_than_5_off_hours_postings(self):
        df = _make_gl_df(n_rows=6, posting_hour=3)  # 6 pre-7am postings
        result = detect_behavioural_anomalies(df)
        b1 = [a for a in result if a.get("kpi_affected") == "OFF_HOURS_POSTINGS"]
        assert len(b1) == 1
        assert b1[0]["severity"] == "MEDIUM"
        assert b1[0]["anomaly_class"] == "BEHAVIOURAL"

    def test_b1_does_not_fire_for_exactly_5_off_hours(self):
        # Boundary: <= 5 is not flagged (must be > 5)
        df = _make_gl_df(n_rows=5, posting_hour=4)
        result = detect_behavioural_anomalies(df)
        b1 = [a for a in result if a.get("kpi_affected") == "OFF_HOURS_POSTINGS"]
        assert len(b1) == 0

    def test_b1_fires_for_after_22_00(self):
        df = _make_gl_df(n_rows=10, posting_hour=23)
        result = detect_behavioural_anomalies(df)
        b1 = [a for a in result if a.get("kpi_affected") == "OFF_HOURS_POSTINGS"]
        assert len(b1) == 1

    def test_b1_does_not_fire_at_business_hours_boundary(self):
        # 7:00 and 22:00 are within business hours (condition is < 7 or > 22)
        for hour in [7, 22]:
            df = _make_gl_df(n_rows=10, posting_hour=hour)
            result = detect_behavioural_anomalies(df)
            b1 = [a for a in result if a.get("kpi_affected") == "OFF_HOURS_POSTINGS"]
            assert len(b1) == 0, f"Unexpected B1 anomaly at hour={hour}"

    # Rule B2 — High-velocity micro-transactions

    def _make_velocity_df(self, n_rows: int, node: str = "EXPENSE.OPEX") -> pd.DataFrame:
        return pd.DataFrame({
            "transaction_id": [f"TX{i}" for i in range(n_rows)],
            "document_number": [f"DOC{i}" for i in range(n_rows)],
            "posting_date": ["2026-01-15"] * n_rows,
            "net_amount_lcy": [999.0] * n_rows,
            "l1_category": ["Expense"] * n_rows,
            "account_type": ["Expense"] * n_rows,
            "universal_node": [node] * n_rows,
            "is_late_entry": [0] * n_rows,
            "row_hash": [f"h{i}" for i in range(n_rows)],
            "posting_hour": [10] * n_rows,
        })

    def test_b2_fires_for_count_above_velocity_limit(self):
        df = self._make_velocity_df(MICRO_TX_VELOCITY_LIMIT + 1)
        result = detect_behavioural_anomalies(df)
        b2 = [a for a in result if a.get("kpi_affected") == "HIGH_VELOCITY_TRANSACTIONS"]
        assert len(b2) == 1
        assert b2[0]["severity"] == "MEDIUM"
        assert b2[0]["anomaly_class"] == "BEHAVIOURAL"

    def test_b2_does_not_fire_at_exactly_velocity_limit(self):
        df = self._make_velocity_df(MICRO_TX_VELOCITY_LIMIT)
        result = detect_behavioural_anomalies(df)
        b2 = [a for a in result if a.get("kpi_affected") == "HIGH_VELOCITY_TRANSACTIONS"]
        assert len(b2) == 0

    def test_b2_fires_per_account_node(self):
        # Two nodes each exceeding the limit → two separate B2 anomalies
        df1 = self._make_velocity_df(MICRO_TX_VELOCITY_LIMIT + 1, node="EXPENSE.OPEX")
        df2 = self._make_velocity_df(MICRO_TX_VELOCITY_LIMIT + 1, node="EXPENSE.CAPEX")
        df = pd.concat([df1, df2], ignore_index=True)
        df["transaction_id"] = [f"TX{i}" for i in range(len(df))]
        result = detect_behavioural_anomalies(df)
        b2 = [a for a in result if a.get("kpi_affected") == "HIGH_VELOCITY_TRANSACTIONS"]
        assert len(b2) == 2

    def test_b2_velocity_is_per_day(self):
        # Split transactions across two days — neither day exceeds the limit alone
        half = MICRO_TX_VELOCITY_LIMIT // 2 + 1
        df_day1 = self._make_velocity_df(half)
        df_day2 = self._make_velocity_df(half)
        df_day1["posting_date"] = "2026-01-15"
        df_day2["posting_date"] = "2026-01-16"
        df = pd.concat([df_day1, df_day2], ignore_index=True)
        df["transaction_id"] = [f"TX{i}" for i in range(len(df))]
        result = detect_behavioural_anomalies(df)
        b2 = [a for a in result if a.get("kpi_affected") == "HIGH_VELOCITY_TRANSACTIONS"]
        assert len(b2) == 0
