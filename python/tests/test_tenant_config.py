"""Unit tests for TenantRegistry and TenantDatabase."""
import pytest
from tenant_config import (
    TenantDatabase,
    TenantIsolationModel,
    TenantRegistry,
)


def _make_db(
    tenant_id: str = "tenant_1",
    schema: str = "tenant_1_",
    model: TenantIsolationModel = TenantIsolationModel.SCHEMA_PER_TENANT,
) -> TenantDatabase:
    return TenantDatabase(
        tenant_id=tenant_id,
        tenant_name="Acme Corp",
        server="fip-synapse.sql.azuresynapse.net",
        database="fip_dw",
        schema=schema,
        isolation_model=model,
    )


def _make_registry(monkeypatch, tenants="", api_keys="", users="") -> TenantRegistry:
    """Instantiate a fresh TenantRegistry with env vars set via monkeypatch."""
    monkeypatch.setenv("TENANTS", tenants)
    monkeypatch.setenv("TENANT_API_KEYS", api_keys)
    monkeypatch.setenv("TENANT_USERS", users)
    # Clear unrelated env vars that might bleed in from outer environment
    monkeypatch.delenv("SYNAPSE_SERVER", raising=False)
    monkeypatch.delenv("SYNAPSE_DATABASE", raising=False)
    return TenantRegistry()


class TestTenantDatabase:
    def test_schema_prefix_schema_per_tenant(self):
        db = _make_db(model=TenantIsolationModel.SCHEMA_PER_TENANT)
        assert db.get_schema_prefix() == "tenant_1_."

    def test_schema_prefix_rls_only(self):
        db = _make_db(model=TenantIsolationModel.RLS_ONLY)
        assert db.get_schema_prefix() == ""

    def test_schema_prefix_database_per_tenant(self):
        db = _make_db(model=TenantIsolationModel.DATABASE_PER_TENANT)
        assert db.get_schema_prefix() == ""

    def test_get_connection_string_includes_server_and_database(self):
        db = _make_db()
        cs = db.get_connection_string()
        assert "fip-synapse.sql.azuresynapse.net" in cs
        assert "fip_dw" in cs

    def test_get_connection_string_enforces_encryption(self):
        db = _make_db()
        cs = db.get_connection_string()
        assert "Encrypt=yes" in cs
        assert "TrustServerCertificate=no" in cs

    def test_get_connection_string_override(self):
        db = _make_db()
        db.connection_string = "custom://override"
        assert db.get_connection_string() == "custom://override"

    def test_str_representation(self):
        ctx_str = str(_make_db())
        # __str__ not defined on dataclass, but basic instantiation works
        assert "tenant_1" in repr(_make_db())


class TestTenantRegistryEnvParsing:
    def test_empty_env_yields_empty_registry(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        assert registry.list_tenants() == {}

    def test_single_tenant_parsed_from_env(self, monkeypatch):
        registry = _make_registry(monkeypatch, tenants="tenant_1:ENTITY001")
        t1 = registry.get_tenant("tenant_1")
        assert t1 is not None
        assert t1.tenant_id == "tenant_1"
        assert t1.schema == "tenant_1_"
        assert t1.isolation_model == TenantIsolationModel.SCHEMA_PER_TENANT

    def test_multiple_tenants_parsed_from_env(self, monkeypatch):
        registry = _make_registry(
            monkeypatch,
            tenants="tenant_1:ENTITY001,tenant_2:ENTITY002",
        )
        assert registry.get_tenant("tenant_1") is not None
        assert registry.get_tenant("tenant_2") is not None
        assert len(registry.list_tenants()) == 2

    def test_api_keys_parsed_from_env(self, monkeypatch):
        registry = _make_registry(
            monkeypatch,
            tenants="tenant_1:ENTITY001",
            api_keys="api_key_abc=tenant_1",
        )
        result = registry.get_tenant_by_api_key("api_key_abc")
        assert result is not None
        assert result.tenant_id == "tenant_1"

    def test_unknown_api_key_returns_none(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        assert registry.get_tenant_by_api_key("no_such_key") is None

    def test_acl_parsed_from_env_single_user(self, monkeypatch):
        registry = _make_registry(
            monkeypatch,
            tenants="tenant_1:ENTITY001",
            users="tenant_1:alice@corp.com",
        )
        assert registry.is_user_authorized("tenant_1", "alice@corp.com") is True
        assert registry.is_user_authorized("tenant_1", "eve@corp.com") is False

    def test_acl_parsed_from_env_multiple_users(self, monkeypatch):
        registry = _make_registry(
            monkeypatch,
            tenants="tenant_1:ENTITY001",
            users="tenant_1:alice@corp.com|bob@corp.com",
        )
        assert registry.is_user_authorized("tenant_1", "alice@corp.com") is True
        assert registry.is_user_authorized("tenant_1", "bob@corp.com") is True

    def test_acl_parsed_for_multiple_tenants(self, monkeypatch):
        registry = _make_registry(
            monkeypatch,
            tenants="tenant_1:E1,tenant_2:E2",
            users="tenant_1:alice@x.com;tenant_2:bob@y.com",
        )
        assert registry.is_user_authorized("tenant_1", "alice@x.com") is True
        assert registry.is_user_authorized("tenant_2", "bob@y.com") is True
        # Cross-tenant access must not bleed
        assert registry.is_user_authorized("tenant_1", "bob@y.com") is False
        assert registry.is_user_authorized("tenant_2", "alice@x.com") is False


class TestTenantRegistryCrud:
    def test_register_tenant_runtime(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        db = _make_db(tenant_id="new_tenant")
        registry.register_tenant(db)
        assert registry.get_tenant("new_tenant") is db

    def test_get_unknown_tenant_returns_none(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        assert registry.get_tenant("ghost") is None

    def test_validate_tenant_exists_true(self, monkeypatch):
        registry = _make_registry(monkeypatch, tenants="tenant_1:E1")
        assert registry.validate_tenant_exists("tenant_1") is True

    def test_validate_tenant_exists_false(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        assert registry.validate_tenant_exists("ghost") is False

    def test_list_tenants_returns_copy(self, monkeypatch):
        registry = _make_registry(monkeypatch, tenants="tenant_1:E1")
        tenants = registry.list_tenants()
        tenants.clear()
        # Original registry should still have the tenant
        assert registry.get_tenant("tenant_1") is not None


class TestTenantRegistryAcl:
    def test_has_acl_false_when_no_entries(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        assert registry.has_acl_for_tenant("tenant_1") is False

    def test_has_acl_true_after_grant(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        registry.grant_user_access("tenant_1", "alice@corp.com")
        assert registry.has_acl_for_tenant("tenant_1") is True

    def test_grant_user_access_idempotent(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        registry.grant_user_access("tenant_1", "alice@corp.com")
        registry.grant_user_access("tenant_1", "alice@corp.com")
        assert registry.is_user_authorized("tenant_1", "alice@corp.com") is True

    def test_revoke_user_access(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        registry.grant_user_access("tenant_1", "alice@corp.com")
        registry.revoke_user_access("tenant_1", "alice@corp.com")
        assert registry.is_user_authorized("tenant_1", "alice@corp.com") is False

    def test_revoke_nonexistent_user_is_safe(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        registry.revoke_user_access("tenant_1", "ghost@corp.com")  # should not raise

    def test_is_user_authorized_unknown_tenant_returns_false(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        assert registry.is_user_authorized("no_such_tenant", "user@x.com") is False

    def test_acl_isolation_between_tenants(self, monkeypatch):
        registry = _make_registry(monkeypatch)
        registry.grant_user_access("tenant_1", "alice@corp.com")
        # alice authorized for tenant_1 but NOT tenant_2
        assert registry.is_user_authorized("tenant_1", "alice@corp.com") is True
        assert registry.is_user_authorized("tenant_2", "alice@corp.com") is False
