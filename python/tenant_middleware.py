"""
FastAPI Tenant Middleware
==========================

Middleware that automatically extracts tenant context from every HTTP request
and makes it available throughout the request lifecycle.

This middleware MUST be registered first in the FastAPI app to ensure
all subsequent operations are tenant-aware.
"""

import logging
import uuid
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from tenant_router import (
    TenantRouter,
    TenantContext,
    TenantAuthenticationError,
    TenantNotFoundError,
    get_router
)

logger = logging.getLogger(__name__)

# Request-scoped context storage
_request_context: Optional[TenantContext] = None


class TenantContextMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware that extracts tenant information from each request
    and stores it in the request state.

    Every request gets:
      1. A unique request_id for tracing
      2. Extracted tenant context (via TenantRouter)
      3. Automatic error handling for auth failures

    Usage:
      app = FastAPI()
      app.add_middleware(TenantContextMiddleware, router=get_router())

    The tenant context is then available in endpoints via:
      from fastapi import Request
      async def my_endpoint(request: Request):
          context: TenantContext = request.state.tenant_context
          tenant_id = context.tenant_id
    """

    def __init__(self, app: ASGIApp, router: Optional[TenantRouter] = None):
        super().__init__(app)
        self.router = router or get_router()

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request: extract tenant, store in state, proceed.
        """

        # Generate unique request ID for tracing
        request_id = str(uuid.uuid4())

        try:
            # Extract authentication header
            auth_header = request.headers.get("Authorization")

            # Get query/body parameters for company_id (optional)
            company_id = request.query_params.get("company_id")

            # Route request to tenant
            context = self.router.route_request(
                authorization_header=auth_header,
                headers=dict(request.headers),
                company_id=company_id,
                request_id=request_id
            )

            # Store context in request state for access in endpoints
            request.state.tenant_context = context
            request.state.request_id = request_id

            logger.info(
                f"[{request_id}] {request.method} {request.url.path} "
                f"→ {context.tenant_id} / {context.user_id}"
            )

            # Process request
            response = await call_next(request)

            # Add tenant info to response headers (debugging)
            response.headers["X-Tenant-ID"] = context.tenant_id
            response.headers["X-Request-ID"] = request_id

            return response

        except TenantAuthenticationError as e:
            logger.warning(f"[{request_id}] Authentication failed: {e}")
            return Response(
                content=f'{{"error": "Unauthorized", "message": "{str(e)}"}}',
                status_code=401,
                media_type="application/json",
                headers={"X-Request-ID": request_id}
            )

        except TenantNotFoundError as e:
            logger.warning(f"[{request_id}] Tenant not found: {e}")
            return Response(
                content=f'{{"error": "Not Found", "message": "{str(e)}"}}',
                status_code=404,
                media_type="application/json",
                headers={"X-Request-ID": request_id}
            )

        except Exception as e:
            logger.error(f"[{request_id}] Unexpected error in middleware: {e}", exc_info=True)
            return Response(
                content=f"{{'error': 'Internal Server Error', 'message': 'Authentication processing failed'}}",
                status_code=500,
                media_type="application/json",
                headers={"X-Request-ID": request_id}
            )


async def get_tenant_context(request: Request) -> TenantContext:
    """
    Dependency injection helper for FastAPI endpoints.

    Usage in endpoint:
      from fastapi import Depends
      from tenant_middleware import get_tenant_context
      from tenant_router import TenantContext

      @app.get("/data")
      async def get_data(context: TenantContext = Depends(get_tenant_context)):
          # context is automatically available
          return {"tenant_id": context.tenant_id}
    """
    if not hasattr(request.state, "tenant_context"):
        raise TenantAuthenticationError("TenantContextMiddleware not properly initialized")

    return request.state.tenant_context


def get_request_id(request: Request) -> str:
    """Get the request ID for logging/tracing."""
    return getattr(request.state, "request_id", "unknown")


class TenantSecurityHeaders:
    """
    Helper to add security headers to responses.
    Prevents caching of tenant-specific data.
    """

    @staticmethod
    def add_to_response(response: Response, context: TenantContext):
        """
        Add tenant-aware security headers.

        - No-Cache: Prevent browsers from caching tenant data
        - X-Tenant-ID: Debugging/tracing
        - Vary: Cache key should include tenant
        """
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["X-Tenant-ID"] = context.tenant_id
        response.headers["Vary"] = "Authorization, X-Tenant-ID"
        return response
