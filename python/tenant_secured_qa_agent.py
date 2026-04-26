"""
Tenant-Secured Financial Q&A Agent

Multi-tenant Q&A API with complete data isolation per tenant.
Each request is authenticated via JWT/API key, routed to the tenant's schema,
and all queries are automatically schema-prefixed and RLS-filtered.

Usage:
    uvicorn tenant_secured_qa_agent:app --host 0.0.0.0 --port 8000
"""

import logging
import os
import sys
from typing import Optional, List

import pandas as pd
import pyodbc
from pydantic import BaseModel

from db_utils import get_db_connection as _msi_db_connection

try:
    from fastapi import FastAPI, HTTPException, Depends, Request
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

from tenant_config import get_registry, setup_test_tenants
from tenant_router import TenantRouter, TenantContext, TenantAuthenticationError
from tenant_middleware import TenantContextMiddleware, get_tenant_context

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FIP.TenantSecuredQAAgent")

AZURE_OPENAI_ENDPOINT    = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_DEPLOYMENT  = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
SYNAPSE_SERVER           = os.getenv("SYNAPSE_SERVER", "fip-synapse.sql.azuresynapse.net")
SYNAPSE_DATABASE         = os.getenv("SYNAPSE_DATABASE", "fip_dw")
MAX_RESULT_ROWS          = int(os.getenv("MAX_RESULT_ROWS", "500"))
QUERY_TIMEOUT_SECONDS    = int(os.getenv("QUERY_TIMEOUT_SECONDS", "30"))
TEST_MODE                = os.getenv("TEST_MODE", "false").lower() == "true"


class QARequest(BaseModel):
    query: str
    user_id: str
    entity_id: Optional[str] = None
    language: str = "en"


class QAResponse(BaseModel):
    question: str
    answer: str
    generated_sql: str
    result_data: Optional[list[dict]] = None
    intent: Optional[str] = None
    tenant_id: Optional[str] = None
    row_count: int
    warning: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    tenant_isolation_model: str


app = FastAPI(
    title="FIP Tenant-Secured Financial Q&A",
    description="Multi-tenant financial analytics with complete data isolation",
    version="1.0.0"
)

registry = get_registry()
if TEST_MODE:
    setup_test_tenants()
    logger.info("TEST MODE: Using test tenants")

tenant_router = TenantRouter(registry)
app.add_middleware(TenantContextMiddleware, router=tenant_router)


def get_db_connection(context: TenantContext) -> pyodbc.Connection:
    """Get database connection for tenant's schema using Managed Identity."""
    try:
        return _msi_db_connection(conn_str_override=context.database.get_connection_string())
    except Exception as e:
        logger.error(f"[{context.request_id}] Database connection failed: {e}")
        raise ConnectionError(f"Could not connect to tenant database: {e}")


def build_tenant_aware_query(context: TenantContext, base_query: str) -> tuple[str, list]:
    """Prefix table names with tenant schema and inject RLS filter."""
    schema_prefix = tenant_router.get_schema_prefix(context)

    replacements = [
        ("FROM gold.",       f"FROM {schema_prefix}gold."),
        ("FROM silver.",     f"FROM {schema_prefix}silver."),
        ("JOIN gold.",       f"JOIN {schema_prefix}gold."),
        ("JOIN silver.",     f"JOIN {schema_prefix}silver."),
        ("LEFT JOIN gold.",  f"LEFT JOIN {schema_prefix}gold."),
        ("LEFT JOIN silver.", f"LEFT JOIN {schema_prefix}silver."),
    ]

    tenant_aware_query = base_query
    for pattern, replacement in replacements:
        tenant_aware_query = tenant_aware_query.replace(pattern, replacement)

    rls_query, rls_params = tenant_router.apply_rls_filter(context, tenant_aware_query)

    logger.info(
        f"[{context.request_id}] Query transformed for tenant {context.tenant_id}: "
        f"schema_prefix={schema_prefix}"
    )
    return rls_query, rls_params


def execute_query(context: TenantContext, sql_query: str, parameters: List = None) -> pd.DataFrame:
    """Execute query with tenant isolation (schema prefix + RLS + parameterized)."""
    if parameters is None:
        parameters = []

    conn = None
    try:
        conn = get_db_connection(context)
        logger.info(
            f"[{context.request_id}] Executing query for {context.tenant_id}: "
            f"{sql_query[:100]}..."
        )
        df = pd.read_sql(sql_query, conn, params=parameters, timeout=QUERY_TIMEOUT_SECONDS)
        logger.info(f"[{context.request_id}] Query returned {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[{context.request_id}] Query execution failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


@app.get("/health", response_model=HealthResponse)
async def health_check(context: TenantContext = Depends(get_tenant_context)):
    return HealthResponse(
        status="healthy",
        version="1.0.0",
        tenant_isolation_model="schema-per-tenant"
    )


@app.post("/api/v1/query", response_model=QAResponse)
async def query_endpoint(
    request_data: QARequest,
    context: TenantContext = Depends(get_tenant_context)
) -> QAResponse:
    """Execute a tenant-isolated financial data query."""
    try:
        user_query = request_data.query
        logger.info(f"[{context.request_id}] {context.user_id} querying {context.tenant_id}: {user_query}")

        tenant_aware_query, rls_params = build_tenant_aware_query(context, user_query)
        df = execute_query(context, tenant_aware_query, rls_params)
        result_data = df.head(MAX_RESULT_ROWS).to_dict(orient="records")

        return QAResponse(
            question=request_data.query,
            answer=f"Returned {len(df)} rows from {context.database.schema} schema",
            generated_sql=tenant_aware_query,
            result_data=result_data,
            tenant_id=context.tenant_id,
            row_count=len(df)
        )
    except Exception as e:
        logger.error(f"[{context.request_id}] Query failed: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")


@app.get("/api/v1/tenant/info")
async def tenant_info(context: TenantContext = Depends(get_tenant_context)):
    """Get information about the current tenant."""
    return {
        "tenant_id":       context.tenant_id,
        "tenant_name":     context.database.tenant_name,
        "user_id":         context.user_id,
        "entity_id":      context.entity_id,
        "schema_prefix":   tenant_router.get_schema_prefix(context),
        "database_server": context.database.server,
        "database_name":   context.database.database,
        "isolation_model": context.database.isolation_model.value,
        "request_id":      context.request_id
    }


@app.get("/api/v1/tenant/list")
async def list_available_tenants():
    """List all registered tenants (admin endpoint)."""
    tenants = registry.list_tenants()
    return {
        "count": len(tenants),
        "tenants": [
            {
                "tenant_id":      t.tenant_id,
                "tenant_name":    t.tenant_name,
                "database":       t.database,
                "schema":         t.schema,
                "isolation_model": t.isolation_model.value
            }
            for t in tenants.values()
        ]
    }


@app.exception_handler(TenantAuthenticationError)
async def tenant_auth_exception_handler(request: Request, exc: TenantAuthenticationError):
    return JSONResponse(
        status_code=401,
        content={
            "error":      "Unauthorized",
            "message":    str(exc),
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")
    logger.error(f"[{request_id}] Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error":      "Internal Server Error",
            "message":    "An unexpected error occurred",
            "request_id": request_id
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
