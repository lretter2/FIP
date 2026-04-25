"""Unit tests for commentary_generator pure-logic functions.

All tests operate without database or Azure OpenAI connections.
External dependencies (pyodbc, openai, db_utils) are stubbed at import time.
"""
import sys
import json
from types import ModuleType
from unittest.mock import MagicMock, patch

# Stub heavy Azure / OpenAI modules before importing the module under test
for _mod in (
    "openai",
    "azure.identity",
    "azure.keyvault.secrets",
    "azure.storage.blob",
    "azure.storage.file_datalake",
    "tiktoken",
    "sqlalchemy",
    "pyodbc",
):
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# Provide a minimal db_utils stub
_db_utils_stub = ModuleType("db_utils")
_db_utils_stub.get_db_connection = MagicMock()  # type: ignore[attr-defined]
_db_utils_stub.get_openai_client = MagicMock()  # type: ignore[attr-defined]
sys.modules.setdefault("db_utils", _db_utils_stub)

import math
import pytest
import pandas as pd

from commentary_generator import (
    _parse_period_label,
    _is_material,
    _fmt_huf,
    _fmt_pct,
    _build_fact_sections,
    _build_alerts,
    write_commentary_to_queue,
    process_commentaries,
    MATERIALITY_THRESHOLD,
)


# ── _parse_period_label ────────────────────────────────────────────────────────

class TestParsePeriodLabel:
    def test_january(self):
        assert _parse_period_label(202601) == "January 2026"

    def test_december(self):
        assert _parse_period_label(202512) == "December 2025"

    def test_june(self):
        assert _parse_period_label(202406) == "June 2024"

    def test_month_and_year_components(self):
        assert _parse_period_label(202309) == "September 2023"

    @pytest.mark.parametrize("period_key,expected_month", [
        (202601, "January"),
        (202602, "February"),
        (202603, "March"),
        (202604, "April"),
        (202605, "May"),
        (202606, "June"),
        (202607, "July"),
        (202608, "August"),
        (202609, "September"),
        (202610, "October"),
        (202611, "November"),
        (202612, "December"),
    ])
    def test_all_months(self, period_key, expected_month):
        assert _parse_period_label(period_key).startswith(expected_month)


# ── _is_material ───────────────────────────────────────────────────────────────

class TestIsMaterial:
    def test_none_returns_false(self):
        assert _is_material(None, 5.0) is False

    def test_nan_returns_false(self):
        assert _is_material(float("nan"), 5.0) is False

    def test_pandas_nan_returns_false(self):
        assert _is_material(pd.NA, 5.0) is False

    def test_zero_below_threshold_returns_false(self):
        assert _is_material(0.0, 5.0) is False

    def test_value_below_threshold_returns_false(self):
        assert _is_material(4.9, 5.0) is False

    def test_value_at_threshold_returns_true(self):
        # abs(5.0) >= 5.0 → True
        assert _is_material(5.0, 5.0) is True

    def test_value_above_threshold_returns_true(self):
        assert _is_material(10.0, 5.0) is True

    def test_negative_value_uses_absolute(self):
        # abs(-8.0) = 8.0 >= 5.0 → True
        assert _is_material(-8.0, 5.0) is True

    def test_small_negative_below_threshold_returns_false(self):
        assert _is_material(-2.0, 5.0) is False


# ── _fmt_huf ───────────────────────────────────────────────────────────────────

class TestFmtHuf:
    def test_none_returns_none(self):
        assert _fmt_huf(None) is None

    def test_nan_returns_none(self):
        assert _fmt_huf(float("nan")) is None

    def test_pandas_nan_returns_none(self):
        assert _fmt_huf(pd.NA) is None

    def test_one_million(self):
        result = _fmt_huf(1_000_000.0)
        assert result == "1.0M HUF"

    def test_two_hundred_fifty_million(self):
        result = _fmt_huf(250_000_000.0)
        assert result == "250.0M HUF"

    def test_fractional_millions_rounded(self):
        result = _fmt_huf(1_500_000.0)
        assert result == "1.5M HUF"

    def test_negative_value_formatted(self):
        result = _fmt_huf(-5_000_000.0)
        assert result == "-5.0M HUF"

    def test_zero(self):
        result = _fmt_huf(0.0)
        assert result == "0.0M HUF"

    def test_returns_string(self):
        assert isinstance(_fmt_huf(1_000_000.0), str)


# ── _fmt_pct ───────────────────────────────────────────────────────────────────

class TestFmtPct:
    def test_none_returns_none(self):
        assert _fmt_pct(None) is None

    def test_nan_returns_none(self):
        assert _fmt_pct(float("nan")) is None

    def test_pandas_nan_returns_none(self):
        assert _fmt_pct(pd.NA) is None

    def test_zero_percent(self):
        assert _fmt_pct(0.0) == "0.0%"

    def test_positive_percent(self):
        assert _fmt_pct(15.5) == "15.5%"

    def test_negative_percent(self):
        assert _fmt_pct(-8.3) == "-8.3%"

    def test_one_decimal_place(self):
        result = _fmt_pct(12.3456)
        assert result == "12.3%"

    def test_returns_string(self):
        assert isinstance(_fmt_pct(10.0), str)


# ── _build_alerts ──────────────────────────────────────────────────────────────

class TestBuildAlerts:
    def _make_row(self, revenue_variance_pct=0.0, revenue_yoy_pct=0.0):
        return {
            "revenue_variance_pct": revenue_variance_pct,
            "revenue_yoy_pct": revenue_yoy_pct,
        }

    def test_no_alerts_when_within_threshold(self):
        row = self._make_row(revenue_variance_pct=5.0, revenue_yoy_pct=-5.0)
        assert _build_alerts(row) == []

    def test_revenue_vs_budget_alert_fires_above_10_pct(self):
        row = self._make_row(revenue_variance_pct=-15.0)
        alerts = _build_alerts(row)
        labels = [a["kpi"] for a in alerts]
        assert "Revenue vs Budget" in labels

    def test_revenue_vs_py_alert_fires_above_10_pct(self):
        row = self._make_row(revenue_yoy_pct=12.0)
        alerts = _build_alerts(row)
        labels = [a["kpi"] for a in alerts]
        assert "Revenue vs Prior Year" in labels

    def test_direction_above_target_for_positive_variance(self):
        row = self._make_row(revenue_variance_pct=11.0)
        alerts = _build_alerts(row)
        assert alerts[0]["direction"] == "above target"

    def test_direction_below_target_for_negative_variance(self):
        row = self._make_row(revenue_variance_pct=-11.0)
        alerts = _build_alerts(row)
        assert alerts[0]["direction"] == "below target"

    def test_alert_tag_present(self):
        row = self._make_row(revenue_variance_pct=-15.0)
        alerts = _build_alerts(row)
        assert alerts[0]["alert_tag"] == "[ALERT]"

    def test_alert_value_is_formatted_percentage(self):
        row = self._make_row(revenue_variance_pct=-15.0)
        alerts = _build_alerts(row)
        assert "%" in alerts[0]["value"]

    def test_none_value_not_alerted(self):
        row = {"revenue_variance_pct": None, "revenue_yoy_pct": None}
        assert _build_alerts(row) == []

    def test_nan_value_not_alerted(self):
        row = {"revenue_variance_pct": float("nan"), "revenue_yoy_pct": float("nan")}
        assert _build_alerts(row) == []

    def test_both_alerts_fire_when_both_exceed_threshold(self):
        row = self._make_row(revenue_variance_pct=-20.0, revenue_yoy_pct=15.0)
        assert len(_build_alerts(row)) == 2

    def test_boundary_at_exactly_10_pct_no_alert(self):
        # abs(10.0) is not > 10.0, so no alert
        row = self._make_row(revenue_variance_pct=-10.0)
        assert _build_alerts(row) == []


# ── _build_fact_sections ───────────────────────────────────────────────────────

class TestBuildFactSections:
    def _make_row(self, **overrides):
        base = {
            "entity_name": "Acme Corp",
            "reporting_currency": "HUF",
            "revenue": 100_000_000.0,
            "revenue_budget": 95_000_000.0,
            "revenue_variance_pct": 5.0,
            "revenue_yoy_pct": 3.0,
            "gross_margin_pct": 45.0,
            "ebitda": 30_000_000.0,
            "ebitda_margin_pct": 30.0,
            "net_profit": 15_000_000.0,
            "net_profit_margin_pct": 15.0,
            "total_assets": 200_000_000.0,
            "total_equity": 80_000_000.0,
            "net_debt": 50_000_000.0,
            "equity_ratio_pct": 40.0,
            "net_debt_to_ebitda": 1.67,
            "current_ratio": 1.8,
            "dso_days": 45.0,
            "dpo_days": 30.0,
            "cash_conversion_cycle": 60.0,
            "operating_cash_flow": 25_000_000.0,
            "free_cash_flow": 20_000_000.0,
            "closing_cash_balance": 15_000_000.0,
        }
        base.update(overrides)
        return base

    def test_required_top_level_sections_present(self):
        row = self._make_row()
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        for section in ["report_metadata", "pl_summary", "balance_sheet_highlights",
                        "liquidity_highlights", "cash_flow", "alerts"]:
            assert section in result, f"Missing section: {section}"

    def test_report_metadata_entity_code(self):
        row = self._make_row()
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["report_metadata"]["entity_code"] == "ACME_HU"

    def test_report_metadata_period_key(self):
        row = self._make_row()
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["report_metadata"]["period_key"] == 202601

    def test_report_metadata_entity_name(self):
        row = self._make_row(entity_name="Acme Corp")
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["report_metadata"]["entity_name"] == "Acme Corp"

    def test_report_metadata_gaap_basis(self):
        row = self._make_row()
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert "HU GAAP" in result["report_metadata"]["gaap_basis"]

    def test_pl_summary_revenue_formatted(self):
        row = self._make_row(revenue=100_000_000.0)
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["pl_summary"]["revenue_current"] == "100.0M HUF"

    def test_pl_summary_revenue_vs_budget_within_materiality(self):
        # variance_pct = 2.0 < MATERIALITY_THRESHOLD (5.0) → "within materiality"
        row = self._make_row(revenue_variance_pct=2.0)
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["pl_summary"]["revenue_vs_budget_pct"] == "within materiality"

    def test_pl_summary_revenue_vs_budget_material(self):
        # variance_pct = 10.0 >= MATERIALITY_THRESHOLD (5.0) → formatted %
        row = self._make_row(revenue_variance_pct=10.0)
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert "%" in result["pl_summary"]["revenue_vs_budget_pct"]

    def test_liquidity_dso_days_formatted(self):
        row = self._make_row(dso_days=45.0)
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["liquidity_highlights"]["dso_days"] == "45 days"

    def test_balance_sheet_net_debt_to_ebitda_formatted(self):
        row = self._make_row(net_debt_to_ebitda=2.5)
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["balance_sheet_highlights"]["net_debt_to_ebitda"] == "2.50x"

    def test_alerts_initialized_as_empty_list(self):
        row = self._make_row()
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["alerts"] == []

    def test_none_net_debt_to_ebitda_returns_none(self):
        row = self._make_row(net_debt_to_ebitda=None)
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["balance_sheet_highlights"]["net_debt_to_ebitda"] is None

    def test_none_current_ratio_returns_none(self):
        row = self._make_row(current_ratio=None)
        result = _build_fact_sections(row, "January 2026", 202601, "ACME_HU")
        assert result["liquidity_highlights"]["current_ratio"] is None


# ── write_commentary_to_queue ──────────────────────────────────────────────────

class TestWriteCommentaryToQueue:
    def _make_fact_pack(self):
        return {"report_metadata": {"period_key": 202601}, "alerts": []}

    def test_calls_cursor_execute_once(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "en", "Commentary text", self._make_fact_pack()
        )
        cursor.execute.assert_called_once()

    def test_calls_commit(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "en", "Commentary text", self._make_fact_pack()
        )
        conn.commit.assert_called_once()

    def test_cursor_closed_after_write(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "en", "Commentary text", self._make_fact_pack()
        )
        cursor.close.assert_called_once()

    def test_entity_code_in_execute_params(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "en", "Text", self._make_fact_pack()
        )
        args = cursor.execute.call_args.args
        # First positional arg to execute is SQL, second is params tuple
        params = args[1]
        assert "ACME_HU" in params

    def test_period_key_in_execute_params(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "en", "Text", self._make_fact_pack()
        )
        params = cursor.execute.call_args.args[1]
        assert 202601 in params

    def test_commentary_text_in_execute_params(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "en", "Narrative here", self._make_fact_pack()
        )
        params = cursor.execute.call_args.args[1]
        assert "Narrative here" in params

    def test_language_uppercased_in_params(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "hu", "Text", self._make_fact_pack()
        )
        params = cursor.execute.call_args.args[1]
        assert "HU" in params

    def test_fact_pack_serialized_to_json_in_params(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        fact_pack = self._make_fact_pack()
        write_commentary_to_queue(
            conn, "ACME_HU", 202601, "CFO", "en", "Text", fact_pack
        )
        params = cursor.execute.call_args.args[1]
        # The fact pack should be JSON-serialized somewhere in params
        json_param = next((p for p in params if isinstance(p, str) and "period_key" in p), None)
        assert json_param is not None
        parsed = json.loads(json_param)
        assert parsed["report_metadata"]["period_key"] == 202601


# ── process_commentaries ───────────────────────────────────────────────────────

class TestProcessCommentaries:
    def _make_fact_pack(self):
        return {
            "report_metadata": {"entity_code": "ACME_HU", "period_key": 202601},
            "pl_summary": {},
            "alerts": [],
        }

    def test_queued_result_on_success(self):
        conn = MagicMock()
        client = MagicMock()
        with patch("commentary_generator.build_variance_fact_pack", return_value=self._make_fact_pack()), \
             patch("commentary_generator.generate_commentary", return_value="commentary text"), \
             patch("commentary_generator.write_commentary_to_queue"):
            results = process_commentaries(conn, client, "ACME_HU", 202601, ["CFO"], ["en"])
        assert len(results) == 1
        assert results[0]["status"] == "QUEUED"
        assert results[0]["role"] == "CFO"
        assert results[0]["language"] == "en"

    def test_multiple_roles_produce_multiple_results(self):
        conn = MagicMock()
        client = MagicMock()
        with patch("commentary_generator.build_variance_fact_pack", return_value=self._make_fact_pack()), \
             patch("commentary_generator.generate_commentary", return_value="text"), \
             patch("commentary_generator.write_commentary_to_queue"):
            results = process_commentaries(conn, client, "ACME_HU", 202601, ["CFO", "CEO"], ["en"])
        assert len(results) == 2
        roles = [r["role"] for r in results]
        assert "CFO" in roles
        assert "CEO" in roles

    def test_multiple_languages_produce_multiple_results(self):
        conn = MagicMock()
        client = MagicMock()
        with patch("commentary_generator.build_variance_fact_pack", return_value=self._make_fact_pack()), \
             patch("commentary_generator.generate_commentary", return_value="text"), \
             patch("commentary_generator.write_commentary_to_queue"):
            results = process_commentaries(conn, client, "ACME_HU", 202601, ["CFO"], ["en", "hu"])
        assert len(results) == 2
        langs = [r["language"] for r in results]
        assert "en" in langs
        assert "hu" in langs

    def test_cross_product_of_roles_and_languages(self):
        conn = MagicMock()
        client = MagicMock()
        with patch("commentary_generator.build_variance_fact_pack", return_value=self._make_fact_pack()), \
             patch("commentary_generator.generate_commentary", return_value="text"), \
             patch("commentary_generator.write_commentary_to_queue"):
            results = process_commentaries(
                conn, client, "ACME_HU", 202601, ["CFO", "CEO"], ["en", "hu"]
            )
        assert len(results) == 4

    def test_failed_result_on_generate_commentary_exception(self):
        conn = MagicMock()
        client = MagicMock()
        with patch("commentary_generator.build_variance_fact_pack", return_value=self._make_fact_pack()), \
             patch("commentary_generator.generate_commentary", side_effect=Exception("OpenAI timeout")), \
             patch("commentary_generator.write_commentary_to_queue"):
            results = process_commentaries(conn, client, "ACME_HU", 202601, ["CFO"], ["en"])
        assert results[0]["status"] == "FAILED"
        assert "OpenAI timeout" in results[0]["error"]

    def test_failed_result_on_write_exception(self):
        conn = MagicMock()
        client = MagicMock()
        with patch("commentary_generator.build_variance_fact_pack", return_value=self._make_fact_pack()), \
             patch("commentary_generator.generate_commentary", return_value="text"), \
             patch("commentary_generator.write_commentary_to_queue", side_effect=Exception("DB error")):
            results = process_commentaries(conn, client, "ACME_HU", 202601, ["CFO"], ["en"])
        assert results[0]["status"] == "FAILED"

    def test_one_failure_does_not_prevent_other_roles(self):
        conn = MagicMock()
        client = MagicMock()

        def _gen_side_effect(client, fact_pack, role, lang):
            if role == "CFO":
                raise Exception("CFO failed")
            return "text"

        with patch("commentary_generator.build_variance_fact_pack", return_value=self._make_fact_pack()), \
             patch("commentary_generator.generate_commentary", side_effect=_gen_side_effect), \
             patch("commentary_generator.write_commentary_to_queue"):
            results = process_commentaries(conn, client, "ACME_HU", 202601, ["CFO", "CEO"], ["en"])
        statuses = {r["role"]: r["status"] for r in results}
        assert statuses["CFO"] == "FAILED"
        assert statuses["CEO"] == "QUEUED"
