"""
Tenant-Aware Router
===================

Core isolation logic ensuring tenant data never mixes:
  1. Extract tenant from authentication context (JWT, API key)
  2. Validate tenant exists and user has access
  3. Route request to tenant-specific database/schema
  4. Inject tenant context into all downstream operations
  5. Prevent cross-tenant queries via RLS filters

This is the CENTRAL SECURITY LAYER for multi-tenant isolation.
All API requests MUST pass through this router.
"""

import logging
import jwt
import os
from typing import Optional, Dict, Any, Callable, List
from functools import lru_cache, wraps
from dataclasses import dataclass

from tenant_config import TenantRegistry, TenantDatabase, get_registry

logger = logging.getLogger(__name__)

# JWT configuration (from environment)
JWT_SECRET = os.environ.get("JWT_SECRET")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_TENANT_CLAIM = os.getenv("JWT_TENANT_CLAIM", "tenant_id")


@dataclass
class TenantContext:
    """
    Request context with tenant information.
    This object is passed through the entire request lifecycle.
    """
    tenant_id: str                  # Unique tenant identifier
    user_id: str                    # User making the request
    company_id: Optional[str]       # Optional: specific company within tenant
    database: TenantDatabase        # Tenant's database configuration
    request_id: str                 # Trace ID for logging
    is_authenticated: bool = True   # Authentication status

    def __str__(self):
        return f"TenantContext(tenant_id={self.tenant_id}, user_id={self.user_id}, company_id={self.company_id})"


class TenantAuthenticationError(Exception):
    """Raised when tenant authentication fails."""
    pass


@dataclass
class AuthResult:
    """Carries the extracted authentication data from a single strategy."""
    tenant_id: str
    user_id: str
    method: str
    company_id: Optional[str] = None


class TenantNotFoundError(Exception):
    """Raised when tenant doesn't exist."""
    pass


class JWTExtractionStrategy:
    """Handles 'Authorization: Bearer <jwt>' headers."""

    def __init__(self, router: "TenantRouter") -> None:
        self._router = router

    def extract(
        self,
        authorization_header: Optional[str],
        headers: Dict[str, str],
    ) -> Optional[AuthResult]:
        if not authorization_header or not authorization_header.lower().startswith("bearer "):
            return None
        token = authorization_header[7:].strip()
        try:
            auth_data = self._router.extract_tenant_from_jwt(token)
        except TenantAuthenticationError:
            logger.debug("JWT parsing failed, trying alternative methods")
            return None
        return AuthResult(
            tenant_id=auth_data["tenant_id"],
            user_id=auth_data["user_id"],
            company_id=auth_data.get("company_id"),
            method="JWT",
        )


class APIKeyExtractionStrategy:
    """Handles bare API keys in the Authorization header (no 'Bearer ' prefix)."""

    def __init__(self, router: "TenantRouter") -> None:
        self._router = router

    def extract(
        self,
        authorization_header: Optional[str],
        headers: Dict[str, str],
    ) -> Optional[AuthResult]:
        if not authorization_header or authorization_header.lower().startswith("bearer "):
            return None
        tenant_id = self._router.extract_tenant_from_api_key(authorization_header.strip())
        if not tenant_id:
            return None
        return AuthResult(tenant_id=tenant_id, user_id="unknown", method="API_KEY")


class HeaderExtractionStrategy:
    """Handles X-Tenant-ID header (development fallback)."""

    def __init__(self, router: "TenantRouter") -> None:
        self._router = router

    def extract(
        self,
        authorization_header: Optional[str],
        headers: Dict[str, str],
    ) -> Optional[AuthResult]:
        tenant_id = self._router.extract_tenant_from_header(headers)
        if not tenant_id:
            return None
        return AuthResult(tenant_id=tenant_id, user_id="unknown", method="HEADER")


class TenantRouter:
    """
    Router that extracts tenant information and enforces isolation.
    Entry point for all API requests.
    """

    def __init__(
        self,
        registry: Optional[TenantRegistry] = None,
        strategies: Optional[List] = None,
    ) -> None:
        self.registry = registry or get_registry()
        if strategies is not None:
            self._strategies: List = strategies
        else:
            default_strategies: List = [
                JWTExtractionStrategy(self),
                APIKeyExtractionStrategy(self),
            ]
            if os.getenv("ALLOW_HEADER_AUTH", "false").lower() == "true":
                default_strategies.append(HeaderExtractionStrategy(self))
            self._strategies = default_strategies

        jwt_enabled = any(
            isinstance(strategy, JWTExtractionStrategy)
            for strategy in self._strategies
        )
        self.jwt_secret: Optional[str] = None
        if jwt_enabled:
            self.jwt_secret = os.environ.get("JWT_SECRET")
            if not self.jwt_secret:
                raise ValueError(
                    "JWT_SECRET environment variable is required when "
                    "JWTExtractionStrategy is enabled"
                )
            global JWT_SECRET
            JWT_SECRET = self.jwt_secret

        logger.info("TenantRouter initialized")

    # ─────────────────────────────────────────────────────────────────────────
    # Authentication: Extract tenant from various sources
    # ─────────────────────────────────────────────────────────────────────────

    def _authenticate(
        self,
        authorization_header: Optional[str],
        headers: Dict[str, str],
        company_id: Optional[str],
    ) -> AuthResult:
        """Iterate strategies in priority order and return the first match."""
        for strategy in self._strategies:
            result = strategy.extract(authorization_header, headers)
            if result is not None:
                if result.company_id is None and company_id is not None:
                    result.company_id = company_id
                return result
        raise TenantAuthenticationError(
            "Missing tenant identification. Provide: "
            "JWT token (Authorization: Bearer <token>) OR "
            "API key OR X-Tenant-ID header"
        )

    def extract_tenant_from_jwt(self, token: str) -> Dict[str, Any]:
        """
        Extract tenant information from JWT token.

        Expected JWT payload:
        {
          "tenant_id": "tenant_1",
          "user_id": "user@example.com",
          "company_id": "ENTITY001"  (optional)
        }

        Raises:
          TenantAuthenticationError: If token is invalid or missing tenant_id
        """
        if not JWT_SECRET:
            raise TenantAuthenticationError("JWT_SECRET environment variable is not configured")
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])

            if JWT_TENANT_CLAIM not in payload:
                raise TenantAuthenticationError(
                    f"JWT token missing required claim: {JWT_TENANT_CLAIM}"
                )

            return {
                "tenant_id": payload[JWT_TENANT_CLAIM],
                "user_id": payload.get("user_id", "unknown"),
                "company_id": payload.get("company_id"),
            }
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            raise TenantAuthenticationError(f"Invalid JWT token: {e}")

    def extract_tenant_from_api_key(self, api_key: str) -> Optional[str]:
        """
        Resolve tenant from API key using the registry.

        API keys are opaque; users don't know which tenant they map to.
        Format: Bearer api_key_xyz

        Returns:
          Tenant ID if found, None otherwise
        """
        tenant_db = self.registry.get_tenant_by_api_key(api_key)
        if tenant_db:
            logger.debug(f"Resolved API key to tenant: {tenant_db.tenant_id}")
            return tenant_db.tenant_id
        return None

    def extract_tenant_from_header(self, headers: Dict[str, str]) -> Optional[str]:
        """
        Extract tenant ID from explicit X-Tenant-ID header.
        This is lower security than JWT but useful for development.
        """
        tenant_id = headers.get("X-Tenant-ID")
        if tenant_id:
            logger.debug(f"Extracted tenant from header: {tenant_id}")
        return tenant_id

    # ─────────────────────────────────────────────────────────────────────────
    # Authorization: Validate tenant access
    # ─────────────────────────────────────────────────────────────────────────

    def validate_tenant(self, tenant_id: str) -> TenantDatabase:
        """
        Validate that tenant exists in registry.

        Raises:
          TenantNotFoundError: If tenant not registered
        """
        tenant_db = self.registry.get_tenant(tenant_id)
        if not tenant_db:
            logger.error(f"Tenant not found: {tenant_id}")
            raise TenantNotFoundError(f"Tenant '{tenant_id}' not registered")

        return tenant_db

    def validate_user_access(self, tenant_id: str, user_id: str) -> bool:
        """
        Validate that user has access to tenant.

        Checks the registry's ACL (loaded from the TENANT_USERS env var or
        registered at runtime via TenantRegistry.grant_user_access). This
        mirrors the production query:
            SELECT 1 FROM config.user_access_control
            WHERE user_id = ? AND tenant_id = ? AND is_active = 1

        When no ACL is configured for a tenant the call falls back to allow
        (with a warning) so that environments without an explicit ACL — e.g.
        local development — keep working. As soon as any user is granted
        access the ACL becomes enforcing for that tenant.
        """
        if not user_id or user_id == "unknown":
            logger.warning(
                f"Denying access to tenant {tenant_id}: missing user_id"
            )
            return False

        if not self.registry.has_acl_for_tenant(tenant_id):
            logger.warning(
                f"No ACL configured for tenant {tenant_id}; "
                f"allowing user {user_id} (configure TENANT_USERS to enforce)"
            )
            return True

        authorized = self.registry.is_user_authorized(tenant_id, user_id)
        if authorized:
            logger.debug(f"User {user_id} authorized for tenant {tenant_id}")
        else:
            logger.warning(
                f"User {user_id} denied access to tenant {tenant_id} (not in ACL)"
            )
        return authorized

    # ─────────────────────────────────────────────────────────────────────────
    # Route: Create tenant context
    # ─────────────────────────────────────────────────────────────────────────

    def route_request(
        self,
        authorization_header: Optional[str],
        headers: Dict[str, str],
        company_id: Optional[str] = None,
        request_id: str = "unknown"
    ) -> TenantContext:
        """
        Main routing function: Extract tenant and build context.

        Priority order for tenant extraction:
          1. JWT token (Authorization: Bearer <token>)
          2. API key (Authorization: Bearer <api_key>)
          3. X-Tenant-ID header (development only)

        Args:
          authorization_header: Value of Authorization header
          headers: All request headers
          company_id: Optional company_id from request body/params
          request_id: Trace ID for logging

        Returns:
          TenantContext with all tenant info

        Raises:
          TenantAuthenticationError: If no valid auth provided
          TenantNotFoundError: If tenant doesn't exist
        """

        auth = self._authenticate(authorization_header, headers, company_id)

        tenant_db = self.validate_tenant(auth.tenant_id)

        if not self.validate_user_access(auth.tenant_id, auth.user_id):
            logger.warning(f"Request {request_id}: User {auth.user_id} denied access to {auth.tenant_id}")
            raise TenantAuthenticationError(f"User {auth.user_id} not authorized for tenant {auth.tenant_id}")

        context = TenantContext(
            tenant_id=auth.tenant_id,
            user_id=auth.user_id,
            company_id=auth.company_id,
            database=tenant_db,
            request_id=request_id,
            is_authenticated=True,
        )

        logger.info(
            f"Request {request_id}: Routed to {context} via {auth.method}",
            extra={"tenant_id": auth.tenant_id, "user_id": auth.user_id},
        )

        return context

    # ─────────────────────────────────────────────────────────────────────────
    # Database Operations: Enforce schema isolation
    # ─────────────────────────────────────────────────────────────────────────

    def get_schema_prefix(self, context: TenantContext) -> str:
        """
        Get the schema prefix for tenant-specific table references.

        Schema-per-tenant model:
          - Shared database (fip_dw)
          - Separate schemas per tenant (tenant_1_silver, tenant_1_gold, etc.)
          - All queries automatically prefixed with schema

        Example:
          Query: SELECT * FROM silver.dim_entity
          Becomes: SELECT * FROM tenant_1_silver.dim_entity
        """
        return context.database.get_schema_prefix()

    def apply_rls_filter(
        self,
        context: TenantContext,
        base_query: str
    ) -> tuple[str, list]:
        """
        Apply Row-Level Security filter to prevent cross-tenant queries.

        This is a DEFENSE IN DEPTH measure in case schema isolation fails.
        Every query gets tenant_id filtering automatically.

        Args:
          context: Request context with tenant info
          base_query: Original SQL query

        Returns:
          (modified_query, parameter_values) for parameterized execution
        """

        # Inject RLS WHERE clause if not already present
        rls_clause = f"WHERE e.tenant_id = ? OR c.company_id IN (SELECT company_id FROM config.tenant_company_map WHERE tenant_id = ?)"

        if "WHERE" not in base_query.upper():
            modified_query = base_query.rstrip(";") + f"\n{rls_clause};"
            params = [context.tenant_id, context.tenant_id]
        else:
            modified_query = base_query.rstrip(";") + f"\nAND e.tenant_id = ?;"
            params = [context.tenant_id]

        return modified_query, params

    # ─────────────────────────────────────────────────────────────────────────
    # Middleware: Decorator for protecting endpoints
    # ─────────────────────────────────────────────────────────────────────────

    def require_tenant(self, func: Callable) -> Callable:
        """
        Decorator to protect an endpoint with tenant routing.

        Usage:
          @app.get("/query")
          @router.require_tenant
          async def query_endpoint(request: Request, context: TenantContext):
              # context is automatically injected
              ...
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract from request (FastAPI specific handling done in middleware)
            # This is a basic fallback for non-FastAPI contexts
            if "context" in kwargs:
                return func(*args, **kwargs)
            raise TenantAuthenticationError("TenantContext not provided")

        return wrapper


@lru_cache(maxsize=1)
def get_router() -> TenantRouter:
    """Get the global tenant router (singleton, thread-safe via lru_cache)."""
    return TenantRouter()
