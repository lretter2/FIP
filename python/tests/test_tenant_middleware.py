"""Unit tests for TenantContextMiddleware and related helpers.

Uses FastAPI TestClient to simulate HTTP requests through the middleware
without requiring real Azure infrastructure.
"""
import sys
import pytest

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from tenant_config import TenantDatabase, TenantIsolationModel, TenantRegistry
from tenant_router import (
    TenantRouter,
    TenantContext,
    TenantAuthenticationError,
    TenantNotFoundError,
)
from tenant_middleware import (
    TenantContextMiddleware,
    TenantSecurityHeaders,
    get_tenant_context,
    get_request_id,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

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
    reg = TenantRegistry.__new__(TenantRegistry)
    reg._tenants = {tid: _make_db(tid) for tid in tenant_ids}
    reg._api_key_to_tenant = {}
    reg._tenant_user_acl = {}
    return reg


def _build_app(router: TenantRouter) -> FastAPI:
    """Build a minimal FastAPI app with the middleware and a simple test endpoint."""
    app = FastAPI()
    app.add_middleware(TenantContextMiddleware, router=router)

    @app.get("/test")
    async def test_endpoint(request: Request):
        ctx: TenantContext = request.state.tenant_context
        return {"tenant_id": ctx.tenant_id, "user_id": ctx.user_id}

    return app


def _make_jwt_router(monkeypatch, tenant_ids=("tenant_1",)) -> TenantRouter:
    """Return a TenantRouter with a known test secret for JWT tests."""
    import tenant_router as tr
    monkeypatch.setattr(tr, "JWT_SECRET", "test-secret-key-for-unit-tests-32b")
    monkeypatch.setattr(tr, "JWT_ALGORITHM", "HS256")
    registry = _make_registry(*tenant_ids)
    return TenantRouter(registry=registry)


def _make_jwt(payload: dict, secret: str = "test-secret-key-for-unit-tests-32b") -> str:
    import jwt as pyjwt
    return pyjwt.encode(payload, secret, algorithm="HS256")


# ── TenantContextMiddleware.dispatch ───────────────────────────────────────────

class TestTenantContextMiddlewareDispatch:

    def test_successful_jwt_request_returns_200(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        response = client.get("/test", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 200

    def test_successful_request_body_contains_tenant_id(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        response = client.get("/test", headers={"Authorization": f"Bearer {token}"})
        assert response.json()["tenant_id"] == "tenant_1"

    def test_successful_request_sets_x_tenant_id_response_header(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        response = client.get("/test", headers={"Authorization": f"Bearer {token}"})
        assert response.headers.get("x-tenant-id") == "tenant_1"

    def test_successful_request_sets_x_request_id_response_header(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "alice@corp.com"})
        response = client.get("/test", headers={"Authorization": f"Bearer {token}"})
        assert "x-request-id" in response.headers

    def test_missing_auth_returns_401(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/test")
        assert response.status_code == 401

    def test_missing_auth_error_body_contains_unauthorized(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/test")
        assert "Unauthorized" in response.text

    def test_unknown_tenant_in_jwt_returns_404(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        token = _make_jwt({"tenant_id": "ghost_tenant", "user_id": "alice@corp.com"})
        response = client.get("/test", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 404

    def test_404_response_body_contains_not_found(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        token = _make_jwt({"tenant_id": "ghost_tenant", "user_id": "alice@corp.com"})
        response = client.get("/test", headers={"Authorization": f"Bearer {token}"})
        assert "Not Found" in response.text

    def test_acl_denial_returns_401(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        router.registry.grant_user_access("tenant_1", "alice@corp.com")
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        token = _make_jwt({"tenant_id": "tenant_1", "user_id": "eve@corp.com"})
        response = client.get("/test", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 401

    def test_response_includes_request_id_header_on_auth_failure(self, monkeypatch):
        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/test")
        assert "x-request-id" in response.headers

    def test_unexpected_exception_returns_500(self, monkeypatch):
        """Middleware should catch unexpected errors and return 500."""
        from unittest.mock import MagicMock, patch

        router = _make_jwt_router(monkeypatch)
        app = _build_app(router)
        client = TestClient(app, raise_server_exceptions=False)

        with patch.object(router, "route_request", side_effect=RuntimeError("unexpected")):
            response = client.get("/test")
        assert response.status_code == 500


# ── get_tenant_context ─────────────────────────────────────────────────────────

class TestGetTenantContext:

    @pytest.mark.asyncio
    async def test_raises_when_context_not_in_state(self):
        """get_tenant_context raises TenantAuthenticationError when middleware did not run."""
        from unittest.mock import MagicMock
        request = MagicMock()
        # Simulate missing tenant_context attribute
        del request.state.tenant_context
        request.state = MagicMock(spec=[])  # no attributes at all

        with pytest.raises(TenantAuthenticationError, match="not properly initialized"):
            await get_tenant_context(request)

    @pytest.mark.asyncio
    async def test_returns_context_when_present(self):
        from unittest.mock import MagicMock
        ctx = TenantContext(
            tenant_id="tenant_1",
            user_id="alice",
            company_id=None,
            database=_make_db(),
            request_id="r1",
        )
        request = MagicMock()
        request.state.tenant_context = ctx
        result = await get_tenant_context(request)
        assert result is ctx


# ── get_request_id ─────────────────────────────────────────────────────────────

class TestGetRequestId:
    def test_returns_request_id_when_present(self):
        from unittest.mock import MagicMock
        request = MagicMock()
        request.state.request_id = "trace-abc-123"
        assert get_request_id(request) == "trace-abc-123"

    def test_returns_unknown_when_absent(self):
        from unittest.mock import MagicMock
        request = MagicMock(spec=["state"])
        # state has no request_id attribute
        request.state = MagicMock(spec=[])
        assert get_request_id(request) == "unknown"


# ── TenantSecurityHeaders ──────────────────────────────────────────────────────

class TestTenantSecurityHeaders:
    def _make_context(self, tenant_id: str = "tenant_1") -> TenantContext:
        return TenantContext(
            tenant_id=tenant_id,
            user_id="alice",
            company_id=None,
            database=_make_db(tenant_id),
            request_id="r1",
        )

    def test_cache_control_header_set(self):
        from fastapi.responses import Response
        response = Response()
        ctx = self._make_context()
        TenantSecurityHeaders.add_to_response(response, ctx)
        assert "no-cache" in response.headers["cache-control"]

    def test_no_store_in_cache_control(self):
        from fastapi.responses import Response
        response = Response()
        ctx = self._make_context()
        TenantSecurityHeaders.add_to_response(response, ctx)
        assert "no-store" in response.headers["cache-control"]

    def test_pragma_no_cache(self):
        from fastapi.responses import Response
        response = Response()
        ctx = self._make_context()
        TenantSecurityHeaders.add_to_response(response, ctx)
        assert response.headers["pragma"] == "no-cache"

    def test_expires_zero(self):
        from fastapi.responses import Response
        response = Response()
        ctx = self._make_context()
        TenantSecurityHeaders.add_to_response(response, ctx)
        assert response.headers["expires"] == "0"

    def test_x_tenant_id_set(self):
        from fastapi.responses import Response
        response = Response()
        ctx = self._make_context(tenant_id="tenant_2")
        TenantSecurityHeaders.add_to_response(response, ctx)
        assert response.headers["x-tenant-id"] == "tenant_2"

    def test_vary_header_includes_authorization(self):
        from fastapi.responses import Response
        response = Response()
        ctx = self._make_context()
        TenantSecurityHeaders.add_to_response(response, ctx)
        assert "Authorization" in response.headers["vary"]

    def test_returns_response_object(self):
        from fastapi.responses import Response
        response = Response()
        ctx = self._make_context()
        result = TenantSecurityHeaders.add_to_response(response, ctx)
        assert result is response
