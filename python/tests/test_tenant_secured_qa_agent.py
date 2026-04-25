"""Unit tests for tenant_secured_qa_agent helper functions.

Only pure logic helpers (build_tenant_aware_query) are covered here.
FastAPI endpoint tests require a running server and are out of scope
for this unit-test suite.
"""
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
        company_id=None,
        database=_make_db(tenant_id, isolation_model),
        request_id="r1",
    )


# ── build_tenant_aware_query ───────────────────────────────────────────────────

class TestBuildTenantAwareQuery:
    """Tests for build_tenant_aware_query in tenant_secured_qa_agent."""

    def setup_method(self):
        # Import after stubbing modules; also patch the module-level tenant_router
        # used inside build_tenant_aware_query
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
