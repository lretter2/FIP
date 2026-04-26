"""Unit tests for tenant_secured_qa_agent helper functions.

Only pure logic helpers (build_tenant_aware_query, validate_nl_query, validate_sql)
are covered here.  FastAPI endpoint tests require a running server and are out of
scope for this unit-test suite.
"""
import pytest
import sys
from types import ModuleType
from unittest.mock import MagicMock

# Stub heavy modules before importing the module under test
for _mod in (
    "pyodbc",
    "openai",
    "azure.identity",
    "azure.keyvault.secrets",
    "azure.storage.blob",
    "azure.storage.file_datalake",
    "tiktoken",
    "sqlalchemy",
):
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

_db_utils_stub = ModuleType("db_utils")
_db_utils_stub.get_db_connection = MagicMock()  # type: ignore[attr-defined]
_db_utils_stub.get_openai_client = MagicMock()  # type: ignore[attr-defined]
sys.modules.setdefault("db_utils", _db_utils_stub)

from fastapi import HTTPException

from tenant_config import TenantDatabase, TenantIsolationModel
from tenant_router import TenantContext


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_db(
    tenant_id: str = "tenant_1",
    isolation_model: TenantIsolationModel = TenantIsolationModel.SCHEMA_PER_TENANT,
) -> TenantDatabase:
    return TenantDatabase(
        tenant_id=tenant_id,
        tenant_name="Acme Corp",
        server="srv",
        database="fip_dw",
        schema=f"{tenant_id}_",
        isolation_model=isolation_model,
    )


def _make_context(
    tenant_id: str = "tenant_1",
    isolation_model: TenantIsolationModel = TenantIsolationModel.SCHEMA_PER_TENANT,
) -> TenantContext:
    return TenantContext(
        tenant_id=tenant_id,
        user_id="alice@corp.com",
        entity_id=None,
        database=_make_db(tenant_id, isolation_model),
        request_id="r1",
    )


# ── build_tenant_aware_query ───────────────────────────────────────────────────

class TestBuildTenantAwareQuery:
    """Tests for build_tenant_aware_query in tenant_secured_qa_agent."""

    def setup_method(self):
        # Import after stubbing heavy modules used by tenant_secured_qa_agent.
        import tenant_secured_qa_agent as qa
        self.qa = qa

    def _run(self, context: TenantContext, query: str):
        return self.qa.build_tenant_aware_query(context, query)

    # Schema prefix injection

    def test_from_gold_prefixed(self):
        ctx = _make_context("tenant_1")
        sql, _ = self._run(ctx, "SELECT * FROM gold.fct_gl_transaction")
        assert "FROM tenant_1_.gold." in sql
        assert "FROM gold." not in sql

    def test_from_silver_prefixed(self):
        ctx = _make_context("tenant_1")
        sql, _ = self._run(ctx, "SELECT * FROM silver.dim_entity")
        assert "FROM tenant_1_.silver." in sql
        assert "FROM silver." not in sql

    def test_join_gold_prefixed(self):
        ctx = _make_context("tenant_1")
        sql, _ = self._run(ctx, "SELECT * FROM gold.t1 JOIN gold.t2 ON t1.id = t2.id")
        assert "JOIN tenant_1_.gold." in sql

    def test_join_silver_prefixed(self):
        ctx = _make_context("tenant_1")
        sql, _ = self._run(ctx, "SELECT * FROM gold.t JOIN silver.d ON t.k = d.k")
        assert "JOIN tenant_1_.silver." in sql

    def test_left_join_gold_prefixed(self):
        ctx = _make_context("tenant_1")
        sql, _ = self._run(ctx, "SELECT * FROM gold.t LEFT JOIN gold.u ON t.id = u.id")
        assert "LEFT JOIN tenant_1_.gold." in sql

    def test_left_join_silver_prefixed(self):
        ctx = _make_context("tenant_1")
        sql, _ = self._run(ctx, "SELECT * FROM gold.t LEFT JOIN silver.d ON t.k = d.k")
        assert "LEFT JOIN tenant_1_.silver." in sql

    def test_different_tenant_uses_own_prefix(self):
        ctx = _make_context("tenant_2")
        sql, _ = self._run(ctx, "SELECT * FROM gold.fct_gl_transaction")
        assert "FROM tenant_2_.gold." in sql
        assert "FROM tenant_1_.gold." not in sql

    def test_rls_only_model_no_prefix(self):
        ctx = _make_context("tenant_1", isolation_model=TenantIsolationModel.RLS_ONLY)
        sql, _ = self._run(ctx, "SELECT * FROM gold.fct_gl_transaction")
        # RLS_ONLY: get_schema_prefix() returns "" so no prefix is injected
        assert "FROM gold.fct_gl_transaction" in sql

    def test_returns_tuple(self):
        ctx = _make_context()
        result = self._run(ctx, "SELECT 1")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_params_is_list(self):
        ctx = _make_context()
        _, params = self._run(ctx, "SELECT 1")
        assert isinstance(params, list)

    def test_rls_filter_appended_to_query(self):
        ctx = _make_context("tenant_1")
        sql, params = self._run(ctx, "SELECT * FROM gold.fct_gl_transaction")
        # apply_rls_filter adds a WHERE or AND clause
        assert "WHERE" in sql.upper() or "AND" in sql.upper()

    def test_tenant_id_present_in_rls_params(self):
        ctx = _make_context("tenant_1")
        _, params = self._run(ctx, "SELECT * FROM gold.fct_gl_transaction")
        assert "tenant_1" in params

    def test_unrelated_table_references_unchanged(self):
        """Tables without gold./silver. prefix are not modified."""
        ctx = _make_context("tenant_1")
        sql, _ = self._run(ctx, "SELECT * FROM audit.anomaly_queue")
        assert "FROM audit.anomaly_queue" in sql


# ── validate_nl_query ─────────────────────────────────────────────────────────

class TestValidateNlQuery:
    """Tests for validate_nl_query — ensures raw SQL is rejected."""

    def setup_method(self):
        import tenant_secured_qa_agent as qa
        self.validate = qa.validate_nl_query

    # Happy paths — natural-language questions must pass

    def test_natural_language_question_passes(self):
        self.validate("What was the EBITDA margin in Q1 2026?")

    def test_natural_language_trend_passes(self):
        self.validate("Show me the revenue trend for the last 6 months")

    def test_natural_language_comparison_passes(self):
        self.validate("How does actual OPEX compare to budget?")

    # Blocked: queries that start with SQL statement keywords

    def test_select_statement_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("SELECT * FROM gold.fct_gl_transaction")
        assert exc_info.value.status_code == 422

    def test_with_cte_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert exc_info.value.status_code == 422

    def test_insert_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("INSERT INTO gold.t VALUES (1, 2)")
        assert exc_info.value.status_code == 422

    def test_update_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("UPDATE gold.t SET x = 1")
        assert exc_info.value.status_code == 422

    def test_delete_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("DELETE FROM gold.t")
        assert exc_info.value.status_code == 422

    # Blocked: queries that contain dangerous keywords mid-string

    def test_drop_keyword_in_nl_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("drop table please")
        assert exc_info.value.status_code == 422

    def test_truncate_keyword_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("TRUNCATE the budget table")
        assert exc_info.value.status_code == 422

    def test_exec_keyword_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("EXEC sp_rename 'old', 'new'")
        assert exc_info.value.status_code == 422

    # Case-insensitivity

    def test_lowercase_select_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("select id from users")
        assert exc_info.value.status_code == 422

    def test_mixed_case_drop_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            self.validate("DrOp table sensitive_data")
        assert exc_info.value.status_code == 422


# ── validate_sql ──────────────────────────────────────────────────────────────

class TestValidateSql:
    """Tests for validate_sql — verifies LLM-generated SQL safety checks."""

    def setup_method(self):
        import tenant_secured_qa_agent as qa
        self.validate = qa.validate_sql

    def test_valid_select_passes(self):
        sql = "SELECT period_key, entity_name FROM gold.kpi_profitability JOIN silver.dim_entity e ON kpi.entity_key = e.entity_key"
        ok, reason = self.validate(sql)
        assert ok is True
        assert reason == "OK"

    def test_valid_with_cte_passes(self):
        sql = "WITH base AS (SELECT 1 AS x) SELECT x FROM base"
        ok, _ = self.validate(sql)
        assert ok is True

    def test_drop_blocked(self):
        ok, reason = self.validate("DROP TABLE gold.sensitive")
        assert ok is False
        assert "DROP" in reason

    def test_delete_blocked(self):
        ok, reason = self.validate("DELETE FROM gold.fct")
        assert ok is False

    def test_non_select_blocked(self):
        ok, reason = self.validate("EXEC xp_cmdshell('dir')")
        assert ok is False

    def test_unauthorised_schema_blocked(self):
        ok, reason = self.validate("SELECT * FROM bronze.raw_data")
        assert ok is False
        assert "bronze" in reason.lower()

    def test_config_tenant_company_map_allowed_for_rls(self):
        # Only the specific RLS mapping table should be allowed here; this test
        # must not document arbitrary config.* access as acceptable.
        sql = "SELECT * FROM gold.t JOIN config.tenant_company_map m ON t.entity_key = m.entity_key WHERE m.tenant_id = ?"
        ok, _ = self.validate(sql)
        assert ok is True
