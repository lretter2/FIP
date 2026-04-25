"""Unit tests for TenantRouter — the central multi-tenant security layer."""
import time
import pytest
import jwt as pyjwt

import tenant_router as tr
from tenant_config import TenantDatabase, TenantIsolationModel, TenantRegistry
from tenant_router import (
    TenantAuthenticationError,
    TenantContext,
    TenantNotFoundError,
    TenantRouter,
)

_TEST_SECRET = "test-secret-key-for-unit-tests"
_TEST_ALGORITHM = "HS256"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_jwt(payload: dict, secret: str = _TEST_SECRET) -> str:
    return pyjwt.encode(payload, secret, algorithm=_TEST_ALGORITHM)


def _bearer(token: str) -> str:
    return f"Bearer {token}"


def _make_db(tenant_id: str = "tenant_1") -> TenantDatabase:
    return TenantDatabase(
        tenant_id=tenant_id,
        tenant_name="Acme Corp",
        server="srv",
        database="fip_dw",
        schema=f"{tenant_id}_",
        isolation_model=TenantIsolationModel.SCHEMA_PER_TENANT,
    )


def _make_registry(*tenant_ids: str) -> TenantRegistry:
    """Build a fresh TenantRegistry without touching lru_cache."""
    reg = TenantRegistry.__new__(TenantRegistry)
    reg._tenants = {tid: _make_db(tid) for tid in tenant_ids}
    reg._api_key_to_tenant = {}
    reg._tenant_user_acl = {}
    return reg


@pytest.fixture
def registry():
    reg = _make_registry("tenant_1", "tenant_2")
    reg._api_key_to_tenant["api_key_t1"] = "tenant_1"
    return reg


@pytest.fixture
def router(registry, monkeypatch):
    monkeypatch.setattr(tr, "JWT_SECRET", _TEST_SECRET)
    monkeypatch.setattr(tr, "JWT_ALGORITHM", _TEST_ALGORITHM)
    return TenantRouter(registry=registry)


# ── extract_tenant_from_jwt ───────────────────────────────────────────────────

class TestExtractTenantFromJwt:
    def test_valid_token_returns_all_claims(self, router):
        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com", "company_id": "E001"})
        result = router.extract_tenant_from_jwt(token)
        assert result["tenant_id"] == "tenant_1"
        assert result["user_id"] == "alice@corp.com"
        assert result["company_id"] == "E001"

    def test_token_without_company_id_defaults_to_none(self, router):
        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        result = router.extract_tenant_from_jwt(token)
        assert result["company_id"] is None

    def test_token_without_user_id_defaults_to_unknown(self, router):
        token = _make_jwt({"tenant_id": "tenant_1"})
        result = router.extract_tenant_from_jwt(token)
        assert result["user_id"] == "unknown"

    def test_missing_tenant_claim_raises(self, router):
        token = _make_jwt({"user_id": "alice@corp.com"})
        with pytest.raises(TenantAuthenticationError, match="missing required claim"):
            router.extract_tenant_from_jwt(token)

    def test_wrong_secret_raises(self, router):
        token = _make_jwt({"tenant_id": "tenant_1"}, secret="wrong-secret")
        with pytest.raises(TenantAuthenticationError):
            router.extract_tenant_from_jwt(token)

    def test_expired_token_raises(self, router):
        token = _make_jwt({"tenant_id": "tenant_1", "exp": int(time.time()) - 60})
        with pytest.raises(TenantAuthenticationError):
            router.extract_tenant_from_jwt(token)

    def test_malformed_token_raises(self, router):
        with pytest.raises(TenantAuthenticationError):
            router.extract_tenant_from_jwt("not.a.jwt")

    def test_empty_token_raises(self, router):
        with pytest.raises(TenantAuthenticationError):
            router.extract_tenant_from_jwt("")


# ── extract_tenant_from_api_key ───────────────────────────────────────────────

class TestExtractTenantFromApiKey:
    def test_known_key_returns_tenant_id(self, router):
        result = router.extract_tenant_from_api_key("api_key_t1")
        assert result == "tenant_1"

    def test_unknown_key_returns_none(self, router):
        assert router.extract_tenant_from_api_key("no_such_key") is None

    def test_empty_key_returns_none(self, router):
        assert router.extract_tenant_from_api_key("") is None


# ── extract_tenant_from_header ────────────────────────────────────────────────

class TestExtractTenantFromHeader:
    def test_header_present_returns_tenant_id(self, router):
        result = router.extract_tenant_from_header({"X-Tenant-ID": "tenant_1"})
        assert result == "tenant_1"

    def test_header_absent_returns_none(self, router):
        assert router.extract_tenant_from_header({}) is None

    def test_different_header_name_returns_none(self, router):
        assert router.extract_tenant_from_header({"X-Tenant": "tenant_1"}) is None


# ── validate_tenant ───────────────────────────────────────────────────────────

class TestValidateTenant:
    def test_existing_tenant_returns_db(self, router):
        db = router.validate_tenant("tenant_1")
        assert db.tenant_id == "tenant_1"

    def test_nonexistent_tenant_raises(self, router):
        with pytest.raises(TenantNotFoundError, match="not registered"):
            router.validate_tenant("ghost_tenant")


# ── validate_user_access ──────────────────────────────────────────────────────

class TestValidateUserAccess:
    def test_empty_user_id_denied(self, router):
        assert router.validate_user_access("tenant_1", "") is False

    def test_unknown_user_id_literal_denied(self, router):
        # "unknown" is the default when user_id is not provided (e.g. API key path)
        assert router.validate_user_access("tenant_1", "unknown") is False

    def test_no_acl_configured_allows_any_named_user(self, router):
        # When no ACL is set for the tenant, any real user is allowed (fail-open in dev)
        assert router.validate_user_access("tenant_1", "alice@corp.com") is True

    def test_user_in_acl_allowed(self, router, registry):
        registry.grant_user_access("tenant_1", "alice@corp.com")
        assert router.validate_user_access("tenant_1", "alice@corp.com") is True

    def test_user_not_in_acl_denied(self, router, registry):
        registry.grant_user_access("tenant_1", "alice@corp.com")
        assert router.validate_user_access("tenant_1", "eve@corp.com") is False

    def test_cross_tenant_access_denied_when_acl_enforced(self, router, registry):
        # Give tenant_1 an ACL with alice; give tenant_2 an ACL with bob only.
        # alice must be denied for tenant_2 even though she is allowed for tenant_1.
        registry.grant_user_access("tenant_1", "alice@corp.com")
        registry.grant_user_access("tenant_2", "bob@corp.com")
        assert router.validate_user_access("tenant_1", "alice@corp.com") is True
        assert router.validate_user_access("tenant_2", "alice@corp.com") is False


# ── route_request ─────────────────────────────────────────────────────────────

class TestRouteRequest:
    def _jwt_header(self, payload: dict) -> str:
        return _bearer(_make_jwt(payload))

    def test_jwt_path_succeeds(self, router):
        header = self._jwt_header({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        ctx = router.route_request(header, {})
        assert ctx.tenant_id == "tenant_1"
        assert ctx.user_id == "alice@corp.com"
        assert ctx.is_authenticated is True

    def test_jwt_path_passes_company_id(self, router):
        header = self._jwt_header({"tenant_id": "tenant_1", "user_id": "alice@corp.com", "company_id": "E001"})
        ctx = router.route_request(header, {})
        assert ctx.company_id == "E001"

    def test_jwt_path_unknown_tenant_raises(self, router):
        header = self._jwt_header({"tenant_id": "ghost", "user_id": "alice@corp.com"})
        with pytest.raises(TenantNotFoundError):
            router.route_request(header, {})

    def test_jwt_path_acl_denial_raises(self, router, registry):
        registry.grant_user_access("tenant_1", "alice@corp.com")
        header = self._jwt_header({"tenant_id": "tenant_1", "user_id": "eve@corp.com"})
        with pytest.raises(TenantAuthenticationError, match="not authorized"):
            router.route_request(header, {})

    def test_no_auth_at_all_raises(self, router):
        with pytest.raises(TenantAuthenticationError, match="Missing tenant"):
            router.route_request(None, {})

    def test_api_key_without_user_id_is_denied(self, router):
        # API key path sets user_id="unknown" which fails validate_user_access
        with pytest.raises(TenantAuthenticationError):
            router.route_request("api_key_t1", {})

    def test_header_fallback_without_user_id_is_denied(self, router):
        # X-Tenant-ID header path also leaves user_id="unknown"
        with pytest.raises(TenantAuthenticationError):
            router.route_request(None, {"X-Tenant-ID": "tenant_1"})

    def test_context_contains_database(self, router):
        header = self._jwt_header({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        ctx = router.route_request(header, {})
        assert ctx.database.tenant_id == "tenant_1"

    def test_request_id_propagated_to_context(self, router):
        header = self._jwt_header({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        ctx = router.route_request(header, {}, request_id="trace-abc-123")
        assert ctx.request_id == "trace-abc-123"


# ── apply_rls_filter ──────────────────────────────────────────────────────────

class TestApplyRlsFilter:
    def _ctx(self, tenant_id: str = "tenant_1") -> TenantContext:
        return TenantContext(
            tenant_id=tenant_id,
            user_id="alice",
            company_id=None,
            database=_make_db(tenant_id),
            request_id="r1",
        )

    def test_adds_where_when_none_present(self, router):
        ctx = self._ctx()
        query = "SELECT * FROM gold.fct_gl_transaction f"
        modified, params = router.apply_rls_filter(ctx, query)
        assert "WHERE" in modified
        assert params.count("tenant_1") >= 1

    def test_appends_and_when_where_already_present(self, router):
        ctx = self._ctx()
        query = "SELECT * FROM t WHERE period_key = 202601"
        modified, params = router.apply_rls_filter(ctx, query)
        assert "AND" in modified
        assert "tenant_1" in params

    def test_tenant_id_injected_into_params(self, router):
        ctx = self._ctx(tenant_id="tenant_99")
        _, params = router.apply_rls_filter(ctx, "SELECT 1")
        assert "tenant_99" in params

    def test_trailing_semicolon_handled(self, router):
        ctx = self._ctx()
        query = "SELECT * FROM t;"
        modified, _ = router.apply_rls_filter(ctx, query)
        # Should not produce double semicolons mid-query
        assert modified.count(";") <= 1 or modified.endswith(";")

    def test_returns_tuple_of_query_and_params(self, router):
        ctx = self._ctx()
        result = router.apply_rls_filter(ctx, "SELECT 1")
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], str)
        assert isinstance(result[1], list)


# ── get_schema_prefix ─────────────────────────────────────────────────────────

class TestGetSchemaPrefix:
    def test_schema_per_tenant_prefix(self, router):
        ctx = TenantContext(
            tenant_id="tenant_1",
            user_id="alice",
            company_id=None,
            database=_make_db("tenant_1"),
            request_id="r1",
        )
        assert router.get_schema_prefix(ctx) == "tenant_1_."

    def test_rls_only_prefix_is_empty(self, router):
        db = TenantDatabase(
            tenant_id="tenant_rls",
            tenant_name="RLS Tenant",
            server="srv",
            database="db",
            schema="tenant_rls_",
            isolation_model=TenantIsolationModel.RLS_ONLY,
        )
        ctx = TenantContext(
            tenant_id="tenant_rls",
            user_id="alice",
            company_id=None,
            database=db,
            request_id="r1",
        )
        assert router.get_schema_prefix(ctx) == ""


# ── require_tenant decorator ──────────────────────────────────────────────────

class TestRequireTenant:
    def _make_context(self) -> TenantContext:
        return TenantContext(
            tenant_id="tenant_1",
            user_id="alice",
            company_id=None,
            database=_make_db("tenant_1"),
            request_id="r1",
        )

    def test_raises_when_no_context_kwarg(self, router):
        @router.require_tenant
        def my_func():
            return "ok"

        with pytest.raises(TenantAuthenticationError, match="TenantContext not provided"):
            my_func()

    def test_passes_through_when_context_provided(self, router):
        @router.require_tenant
        def my_func(context=None):
            return f"tenant:{context.tenant_id}"

        ctx = self._make_context()
        result = my_func(context=ctx)
        assert result == "tenant:tenant_1"

    def test_preserves_function_name(self, router):
        @router.require_tenant
        def my_special_func():
            pass

        assert my_special_func.__name__ == "my_special_func"

    def test_passes_positional_and_keyword_args_through(self, router):
        @router.require_tenant
        def my_func(x, y, context=None):
            return x + y

        ctx = self._make_context()
        result = my_func(1, 2, context=ctx)
        assert result == 3


# ── Extraction strategies (standalone) ────────────────────────────────────────

class TestJWTExtractionStrategy:
    def test_valid_bearer_token_returns_auth_result(self, router, monkeypatch):
        from tenant_router import JWTExtractionStrategy
        strategy = JWTExtractionStrategy(router)
        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        result = strategy.extract(f"Bearer {token}", {})
        assert result is not None
        assert result.tenant_id == "tenant_1"
        assert result.user_id == "alice@corp.com"
        assert result.method == "JWT"

    def test_non_bearer_header_returns_none(self, router):
        from tenant_router import JWTExtractionStrategy
        strategy = JWTExtractionStrategy(router)
        assert strategy.extract("api_key_t1", {}) is None

    def test_none_header_returns_none(self, router):
        from tenant_router import JWTExtractionStrategy
        strategy = JWTExtractionStrategy(router)
        assert strategy.extract(None, {}) is None

    def test_invalid_jwt_returns_none_not_raises(self, router):
        from tenant_router import JWTExtractionStrategy
        strategy = JWTExtractionStrategy(router)
        # Malformed token — strategy should swallow the error and return None
        result = strategy.extract("Bearer not.a.jwt", {})
        assert result is None

    def test_company_id_propagated(self, router):
        from tenant_router import JWTExtractionStrategy
        strategy = JWTExtractionStrategy(router)
        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com", "company_id": "E001"})
        result = strategy.extract(f"Bearer {token}", {})
        assert result.company_id == "E001"


class TestAPIKeyExtractionStrategy:
    def test_known_api_key_returns_auth_result(self, router):
        from tenant_router import APIKeyExtractionStrategy
        strategy = APIKeyExtractionStrategy(router)
        result = strategy.extract("api_key_t1", {})
        assert result is not None
        assert result.tenant_id == "tenant_1"
        assert result.method == "API_KEY"

    def test_unknown_api_key_returns_none(self, router):
        from tenant_router import APIKeyExtractionStrategy
        strategy = APIKeyExtractionStrategy(router)
        assert strategy.extract("no_such_key", {}) is None

    def test_bearer_prefixed_header_returns_none(self, router):
        from tenant_router import APIKeyExtractionStrategy
        strategy = APIKeyExtractionStrategy(router)
        # Bearer prefix belongs to JWT strategy, not API key
        token = _make_jwt({"tenant_id": "tenant_1"})
        assert strategy.extract(f"Bearer {token}", {}) is None

    def test_none_header_returns_none(self, router):
        from tenant_router import APIKeyExtractionStrategy
        strategy = APIKeyExtractionStrategy(router)
        assert strategy.extract(None, {}) is None

    def test_user_id_set_to_unknown(self, router):
        from tenant_router import APIKeyExtractionStrategy
        strategy = APIKeyExtractionStrategy(router)
        result = strategy.extract("api_key_t1", {})
        assert result.user_id == "unknown"


class TestHeaderExtractionStrategy:
    def test_x_tenant_id_header_returns_auth_result(self, router):
        from tenant_router import HeaderExtractionStrategy
        strategy = HeaderExtractionStrategy(router)
        result = strategy.extract(None, {"X-Tenant-ID": "tenant_1"})
        assert result is not None
        assert result.tenant_id == "tenant_1"
        assert result.method == "HEADER"

    def test_missing_header_returns_none(self, router):
        from tenant_router import HeaderExtractionStrategy
        strategy = HeaderExtractionStrategy(router)
        assert strategy.extract(None, {}) is None

    def test_wrong_header_name_returns_none(self, router):
        from tenant_router import HeaderExtractionStrategy
        strategy = HeaderExtractionStrategy(router)
        assert strategy.extract(None, {"X-Tenant": "tenant_1"}) is None

    def test_user_id_set_to_unknown(self, router):
        from tenant_router import HeaderExtractionStrategy
        strategy = HeaderExtractionStrategy(router)
        result = strategy.extract(None, {"X-Tenant-ID": "tenant_1"})
        assert result.user_id == "unknown"
