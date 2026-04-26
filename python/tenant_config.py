"""
Tenant Configuration and Database Mapping
==========================================

Manages the mapping of tenants (companies) to their isolated database/schema environments.
This is the central configuration that prevents data cross-contamination.

Supports three deployment models:
  1. Database-per-tenant (each tenant has isolated SQL Server database)
  2. Schema-per-tenant (shared SQL Server, isolated schemas per tenant)
  3. Row-Level Security (shared database/schema, filtered by tenant_id)

For FIP, we use Schema-per-tenant with RLS as additional layer.
"""

import os
import logging
from functools import lru_cache
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class TenantIsolationModel(str, Enum):
    """Supported tenant isolation strategies."""
    DATABASE_PER_TENANT = "database"      # Separate SQL Server database per tenant
    SCHEMA_PER_TENANT = "schema"          # Shared database, separate schemas
    RLS_ONLY = "rls"                      # Shared database/schema, Row-Level Security


@dataclass
class TenantDatabase:
    """Configuration for a single tenant's database connection."""
    tenant_id: str                        # Unique tenant identifier (e.g., 'tenant_1', 'ENTITY001')
    tenant_name: str                      # Human-readable name
    server: str                           # SQL Server hostname
    database: str                         # Database name
    schema: str                           # Schema prefix (e.g., 'tenant_1_', empty for RLS)
    connection_string: Optional[str] = None  # Override full conn string
    isolation_model: TenantIsolationModel = TenantIsolationModel.SCHEMA_PER_TENANT

    def get_connection_string(self) -> str:
        """Build ODBC connection string with encryption and auth."""
        if self.connection_string:
            return self.connection_string

        return (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.server};"
            f"Database={self.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

    def get_schema_prefix(self) -> str:
        """Get schema prefix for tenant isolation (for schema-per-tenant model)."""
        if self.isolation_model == TenantIsolationModel.SCHEMA_PER_TENANT:
            return f"{self.schema}."
        return ""


class TenantRegistry:
    """
    Central registry mapping API keys/JWTs to tenant configurations.

    In production, this would be loaded from Azure Key Vault or a config service.
    For now, it's environment-based for demonstation.
    """

    def __init__(self):
        self._tenants: Dict[str, TenantDatabase] = {}
        self._api_key_to_tenant: Dict[str, str] = {}
        # ACL: maps tenant_id -> set of authorized user_ids.
        # Mirrors the production query:
        #   SELECT 1 FROM config.user_access_control
        #   WHERE user_id = ? AND tenant_id = ? AND is_active = 1
        self._tenant_user_acl: Dict[str, set] = {}
        self._load_from_env()

    def _load_from_env(self):
        """Load tenant configuration from environment variables."""
        # Default Synapse server (shared infrastructure)
        synapse_server = os.getenv("SYNAPSE_SERVER", "fip-synapse.sql.azuresynapse.net")
        synapse_db = os.getenv("SYNAPSE_DATABASE", "fip_dw")

        # Parse tenant definitions from env
        # Format: TENANTS="tenant_1:ENTITY001,tenant_2:ENTITY002"
        tenants_env = os.getenv("TENANTS", "")
        api_keys_env = os.getenv("TENANT_API_KEYS", "")

        if tenants_env:
            for tenant_spec in tenants_env.split(","):
                parts = tenant_spec.strip().split(":")
                if len(parts) >= 2:
                    tenant_id = parts[0].strip()
                    entity_id = parts[1].strip()

                    # Schema-per-tenant: use tenant_id as schema prefix
                    self._tenants[tenant_id] = TenantDatabase(
                        tenant_id=tenant_id,
                        tenant_name=f"Tenant {tenant_id}",
                        server=synapse_server,
                        database=synapse_db,
                        schema=f"{tenant_id}_",  # Schema prefix: tenant_1_silver, tenant_1_gold, etc.
                        isolation_model=TenantIsolationModel.SCHEMA_PER_TENANT
                    )

        # Parse API key to tenant mapping
        # Format: API_KEY_1=tenant_1,API_KEY_2=tenant_2
        if api_keys_env:
            for mapping in api_keys_env.split(","):
                parts = mapping.strip().split("=")
                if len(parts) == 2:
                    api_key = parts[0].strip()
                    tenant_id = parts[1].strip()
                    self._api_key_to_tenant[api_key] = tenant_id

        # Parse tenant ACL mapping
        # Format: TENANT_USERS="tenant_1:alice@x.com|bob@x.com;tenant_2:carol@y.com"
        acl_env = os.getenv("TENANT_USERS", "")
        if acl_env:
            for tenant_spec in acl_env.split(";"):
                parts = tenant_spec.strip().split(":", 1)
                if len(parts) == 2:
                    tenant_id = parts[0].strip()
                    users = {u.strip() for u in parts[1].split("|") if u.strip()}
                    if users:
                        self._tenant_user_acl.setdefault(tenant_id, set()).update(users)

        if self._tenants:
            logger.info(f"Loaded {len(self._tenants)} tenants from environment")
        if self._tenant_user_acl:
            logger.info(
                f"Loaded ACL entries for {len(self._tenant_user_acl)} tenants"
            )

    def register_tenant(self, tenant_config: TenantDatabase):
        """Register a new tenant (runtime configuration)."""
        self._tenants[tenant_config.tenant_id] = tenant_config
        logger.info(f"Registered tenant: {tenant_config.tenant_id}")

    def get_tenant(self, tenant_id: str) -> Optional[TenantDatabase]:
        """Retrieve tenant configuration by ID."""
        return self._tenants.get(tenant_id)

    def get_tenant_by_api_key(self, api_key: str) -> Optional[TenantDatabase]:
        """Resolve tenant from API key."""
        tenant_id = self._api_key_to_tenant.get(api_key)
        if tenant_id:
            return self.get_tenant(tenant_id)
        return None

    def list_tenants(self) -> Dict[str, TenantDatabase]:
        """List all registered tenants."""
        return self._tenants.copy()

    def validate_tenant_exists(self, tenant_id: str) -> bool:
        """Check if tenant is registered."""
        return tenant_id in self._tenants

    def grant_user_access(self, tenant_id: str, user_id: str):
        """Grant a user access to a tenant (in-memory ACL entry)."""
        self._tenant_user_acl.setdefault(tenant_id, set()).add(user_id)

    def revoke_user_access(self, tenant_id: str, user_id: str):
        """Revoke a user's access to a tenant."""
        users = self._tenant_user_acl.get(tenant_id)
        if users:
            users.discard(user_id)

    def has_acl_for_tenant(self, tenant_id: str) -> bool:
        """Whether any ACL entries are configured for this tenant."""
        return bool(self._tenant_user_acl.get(tenant_id))

    def is_user_authorized(self, tenant_id: str, user_id: str) -> bool:
        """
        Check whether a user is authorized to access a tenant.

        Equivalent to:
          SELECT 1 FROM config.user_access_control
          WHERE user_id = ? AND tenant_id = ? AND is_active = 1
        """
        users = self._tenant_user_acl.get(tenant_id)
        if not users:
            return False
        return user_id in users


@lru_cache(maxsize=1)
def get_registry() -> TenantRegistry:
    """Get the global tenant registry (singleton, thread-safe via lru_cache)."""
    return TenantRegistry()


# Initialization helpers for testing
def setup_test_tenants():
    """Setup test tenants for development/testing."""
    registry = get_registry()

    # Clear existing
    registry._tenants.clear()
    registry._api_key_to_tenant.clear()

    # Register test tenants
    registry.register_tenant(TenantDatabase(
        tenant_id="tenant_1",
        tenant_name="Acme Corp",
        server="fip-synapse.sql.azuresynapse.net",
        database="fip_dw",
        schema="tenant_1_",
        isolation_model=TenantIsolationModel.SCHEMA_PER_TENANT
    ))

    registry.register_tenant(TenantDatabase(
        tenant_id="tenant_2",
        tenant_name="BigCorp Ltd",
        server="fip-synapse.sql.azuresynapse.net",
        database="fip_dw",
        schema="tenant_2_",
        isolation_model=TenantIsolationModel.SCHEMA_PER_TENANT
    ))

    # Map API keys
    registry._api_key_to_tenant["api_key_tenant_1"] = "tenant_1"
    registry._api_key_to_tenant["api_key_tenant_2"] = "tenant_2"

    logger.info("Test tenants configured")
