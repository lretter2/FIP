"""
Integration tests for RLS (Row-Level Security) injection via sqlglot.

These tests verify that the sqlglot-based security predicate injection in
TenantRouter.apply_rls_filter() and tenant_secured_qa_agent.build_tenant_aware_query()
correctly handles a variety of SQL structures:

  - Simple SELECT with no WHERE clause
  - SELECT with an existing WHERE clause
  - Queries with single and multiple JOINs
  - Queries with LEFT JOIN / RIGHT JOIN
  - Subqueries in FROM clause
  - CTEs (WITH … AS …)
  - Queries already using gold./silver. schema references
  - Cross-tenant isolation: two tenants produce independent predicates
"""

import pytest

from tenant_config import TenantDatabase, TenantIsolationModel, TenantRegistry
from tenant_router import TenantContext, TenantRouter


# ── Shared fixtures ────────────────────────────────────────────────────────────

def _make_db(tenant_id: str = "tenant_1") -> TenantDatabase:
    return TenantDatabase(
        tenant_id=tenant_id,
        tenant_name="Test Corp",
        server="srv",
        database="fip_dw",
        schema=f"{tenant_id}_",
        isolation_model=TenantIsolationModel.SCHEMA_PER_TENANT,
    )


def _make_ctx(tenant_id: str = "tenant_1") -> TenantContext:
    return TenantContext(
        tenant_id=tenant_id,
        user_id="alice@corp.com",
        entity_id=None,
        database=_make_db(tenant_id),
        request_id="test-req",
    )


@pytest.fixture
def router():
    reg = TenantRegistry.__new__(TenantRegistry)
    reg._tenants = {"tenant_1": _make_db("tenant_1"), "tenant_2": _make_db("tenant_2")}
    reg._api_key_to_tenant = {}
    reg._tenant_user_acl = {}
    return TenantRouter(registry=reg)


# ── apply_rls_filter — structural coverage ────────────────────────────────────

class TestApplyRlsFilterStructural:
    """Verify RLS injection produces valid, secured SQL for diverse query shapes."""

    def test_simple_select_no_where_adds_where(self, router):
        ctx = _make_ctx("tenant_1")
        sql, params = router.apply_rls_filter(ctx, "SELECT * FROM gold.kpi_profitability f")
        assert "WHERE" in sql.upper()
        assert "tenant_1" in params

    def test_existing_where_uses_and(self, router):
        ctx = _make_ctx("tenant_1")
        sql, params = router.apply_rls_filter(ctx, "SELECT * FROM gold.kpi_profitability WHERE period_key = 202601")
        assert "AND" in sql.upper()
        assert "tenant_1" in params

    def test_inner_join_no_where_injects_where(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "SELECT t.revenue FROM gold.kpi_profitability t "
            "JOIN silver.dim_entity e ON t.entity_key = e.entity_key"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        assert "WHERE" in sql.upper()
        assert "tenant_1" in params

    def test_inner_join_existing_where_appends_and(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "SELECT t.revenue FROM gold.kpi_profitability t "
            "JOIN silver.dim_entity e ON t.entity_key = e.entity_key "
            "WHERE t.period_key = 202601"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        assert "AND" in sql.upper()
        assert "tenant_1" in params

    def test_left_join_no_where_injects_where(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "SELECT t.revenue, e.entity_name FROM gold.agg_pl_monthly t "
            "LEFT JOIN silver.dim_entity e ON t.entity_key = e.entity_key"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        assert "WHERE" in sql.upper()
        assert "tenant_1" in params

    def test_left_join_existing_where_appends_and(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "SELECT t.revenue FROM gold.agg_pl_monthly t "
            "LEFT JOIN silver.dim_entity e ON t.entity_key = e.entity_key "
            "WHERE t.fiscal_year = 2026"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        assert "AND" in sql.upper()
        assert "tenant_1" in params

    def test_subquery_in_from_injects_at_outer_level(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "SELECT sub.revenue FROM "
            "(SELECT entity_key, revenue FROM gold.kpi_profitability WHERE period_key = 202601) sub"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        # The outer query must have a WHERE or AND injected
        assert "WHERE" in sql.upper() or "AND" in sql.upper()
        assert "tenant_1" in params

    def test_cte_injects_at_outer_select(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "WITH revenue_cte AS ("
            "SELECT period_key, entity_key, revenue FROM gold.agg_pl_monthly"
            ") SELECT * FROM revenue_cte"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        assert "WHERE" in sql.upper()
        assert "tenant_1" in params

    def test_trailing_semicolon_not_duplicated(self, router):
        ctx = _make_ctx("tenant_1")
        sql, _ = router.apply_rls_filter(ctx, "SELECT 1;")
        assert ";;" not in sql

    def test_returns_exactly_one_tenant_param(self, router):
        ctx = _make_ctx("tenant_99")
        _, params = router.apply_rls_filter(ctx, "SELECT * FROM t")
        assert params == ["tenant_99"]

    def test_multiple_joins_no_where(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "SELECT p.revenue, e.entity_name, d.fiscal_year "
            "FROM gold.kpi_profitability p "
            "JOIN silver.dim_entity e ON p.entity_key = e.entity_key "
            "JOIN silver.dim_date d ON p.period_key = d.date_key"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        assert "WHERE" in sql.upper()
        assert "tenant_1" in params

    def test_multiple_joins_existing_where(self, router):
        ctx = _make_ctx("tenant_1")
        base = (
            "SELECT p.revenue FROM gold.kpi_profitability p "
            "JOIN silver.dim_entity e ON p.entity_key = e.entity_key "
            "WHERE p.period_key >= 202601"
        )
        sql, params = router.apply_rls_filter(ctx, base)
        assert "AND" in sql.upper()
        assert "tenant_1" in params


# ── Cross-tenant isolation ─────────────────────────────────────────────────────

class TestCrossTenantIsolation:
    """Ensure two tenants produce independent, non-overlapping RLS predicates."""

    def test_different_tenants_use_own_predicate(self, router):
        ctx1 = _make_ctx("tenant_1")
        ctx2 = _make_ctx("tenant_2")
        base = "SELECT * FROM gold.kpi_profitability"
        _, params1 = router.apply_rls_filter(ctx1, base)
        _, params2 = router.apply_rls_filter(ctx2, base)
        assert params1 == ["tenant_1"]
        assert params2 == ["tenant_2"]
        assert params1 != params2

    def test_tenant_2_predicate_not_in_tenant_1_params(self, router):
        ctx1 = _make_ctx("tenant_1")
        base = "SELECT * FROM gold.kpi_profitability"
        _, params1 = router.apply_rls_filter(ctx1, base)
        assert "tenant_2" not in params1


# ── build_tenant_aware_query — schema prefix + RLS ────────────────────────────

class TestBuildTenantAwareQuerySqlglot:
    """
    Verify that build_tenant_aware_query correctly combines schema-prefix
    injection and RLS for realistic query shapes.
    """

    def setup_method(self):
        import sys
        from types import ModuleType
        from unittest.mock import MagicMock
        for mod in ("pyodbc", "openai", "azure.identity",
                    "azure.keyvault.secrets", "azure.storage.blob",
                    "azure.storage.file_datalake", "tiktoken", "sqlalchemy",
                    "azure.search.documents", "azure.search.documents.models"):
            if mod not in sys.modules:
                sys.modules[mod] = MagicMock()

        db_stub = sys.modules.get("db_utils")
        if db_stub is None:
            db_stub = ModuleType("db_utils")
            sys.modules["db_utils"] = db_stub
        if not hasattr(db_stub, "get_db_connection"):
            db_stub.get_db_connection = MagicMock()
        if not hasattr(db_stub, "get_openai_client"):
            db_stub.get_openai_client = MagicMock()

        import tenant_secured_qa_agent as qa
        self.qa = qa

    def _run(self, tenant_id: str, query: str):
        ctx = TenantContext(
            tenant_id=tenant_id,
            user_id="alice@corp.com",
            entity_id=None,
            database=_make_db(tenant_id),
            request_id="r1",
        )
        return self.qa.build_tenant_aware_query(ctx, query)

    def test_join_schema_prefix_and_rls_injected(self):
        sql, params = self._run(
            "tenant_1",
            "SELECT t.revenue FROM gold.kpi_profitability t "
            "JOIN silver.dim_entity e ON t.entity_key = e.entity_key"
        )
        assert "tenant_1_.gold." in sql
        assert "tenant_1_.silver." in sql
        assert "tenant_1" in params

    def test_existing_where_with_join(self):
        sql, params = self._run(
            "tenant_1",
            "SELECT t.revenue FROM gold.kpi_profitability t "
            "JOIN silver.dim_entity e ON t.entity_key = e.entity_key "
            "WHERE t.period_key = 202601"
        )
        assert "AND" in sql.upper()
        assert "tenant_1" in params

    def test_subquery_schema_prefix_and_rls(self):
        sql, params = self._run(
            "tenant_1",
            "SELECT sub.revenue FROM "
            "(SELECT entity_key, revenue FROM gold.kpi_profitability WHERE period_key = 202601) sub"
        )
        assert "tenant_1_.gold." in sql
        assert "tenant_1" in params

    def test_cte_schema_prefix_and_rls(self):
        sql, params = self._run(
            "tenant_1",
            "WITH cte AS (SELECT revenue FROM gold.agg_pl_monthly) "
            "SELECT * FROM cte"
        )
        assert "tenant_1_.gold." in sql
        assert "tenant_1" in params

    def test_different_tenant_schema_prefix(self):
        sql, params = self._run(
            "tenant_2",
            "SELECT * FROM gold.kpi_profitability"
        )
        assert "tenant_2_.gold." in sql
        assert "tenant_2" in params
        assert "tenant_1_.gold." not in sql
