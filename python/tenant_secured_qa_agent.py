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
import re
import sys
from typing import Optional, List

import pandas as pd
import pyodbc
from pydantic import BaseModel

from db_utils import get_db_connection as _msi_db_connection, get_openai_client as _msi_openai_client

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

# ---------------------------------------------------------------------------
# SQL validation — aligned with financial_qa_agent.py
# ---------------------------------------------------------------------------

# Keywords that must never appear in user input or generated SQL.
SQL_BLOCKED_KEYWORDS = [
    "DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "EXEC", "EXECUTE", "xp_", "sp_", "GRANT", "REVOKE", "OPENROWSET",
]

# Compiled pattern: matches strings that open with a SQL statement keyword.
_SQL_STATEMENT_RE = re.compile(
    r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)

# System prompt used to generate T-SQL from natural language.
_SQL_GENERATION_SYSTEM_PROMPT = """You are a T-SQL expert for an Azure Synapse Analytics financial data warehouse.
Generate a T-SQL SELECT query to answer the user's financial question.

Available tables:
- gold.kpi_profitability (period_key, entity_key, revenue, ebitda, ebitda_margin_pct, gross_margin_pct, net_profit, roic_pct)
- gold.kpi_liquidity (period_key, entity_key, current_ratio, dso_days, dpo_days, operating_cash_flow, free_cash_flow)
- gold.agg_pl_monthly (period_key, entity_key, revenue, cogs, gross_profit, ebitda, net_profit, revenue_budget)
- gold.agg_balance_sheet (period_key, entity_key, universal_node, account_type, closing_balance_lcy)
- silver.dim_entity (entity_key, entity_code, entity_name, reporting_currency)
- silver.dim_date (date_key, full_date, fiscal_year, fiscal_period, quarter_number)

Rules:
- Always JOIN silver.dim_entity e ON <fact>.entity_key = e.entity_key
- Always include period_key and entity_name in SELECT
- Use TOP 500 to limit results
- Only query gold.* and silver.dim_* tables — never bronze.*
- Return ONLY the SQL query — no explanation, no markdown fences."""


def validate_nl_query(query: str) -> None:
    """
    Reject inputs that are raw SQL rather than natural-language questions.

    Raises HTTPException(422) if the input starts with a SQL statement keyword
    or contains any blocked DDL/DML keyword.  This prevents the query endpoint
    from being used as a direct SQL executor.
    """
    if _SQL_STATEMENT_RE.match(query):
        raise HTTPException(
            status_code=422,
            detail="The 'query' field must be a natural-language question, not a SQL statement.",
        )
    query_upper = query.upper()
    for keyword in SQL_BLOCKED_KEYWORDS:
        if re.search(r"\b" + re.escape(keyword) + r"\b", query_upper):
            raise HTTPException(
                status_code=422,
                detail=f"Query contains a disallowed keyword: {keyword}.",
            )


def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Validate LLM-generated SQL for safety before execution.

    Returns (is_safe, reason).  Aligned with financial_qa_agent.validate_sql.
    """
    sql_upper = sql.upper()
    for keyword in SQL_BLOCKED_KEYWORDS:
        if re.search(r"\b" + re.escape(keyword) + r"\b", sql_upper):
            return False, f"SQL contains blocked keyword: {keyword}"

    stripped = sql_upper.strip()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        return False, "Generated SQL must start with SELECT or WITH (CTE)"

    allowed_schemas = {"GOLD", "SILVER"}
    allowed_config_tables = {"TENANT_COMPANY_MAP"}
    object_refs = re.findall(r"\b(?:FROM|JOIN)\s+(\w+)\.(\w+)\b", sql_upper)
    for schema, table in object_refs:
        if schema in allowed_schemas:
            continue
        if schema == "CONFIG" and table in allowed_config_tables:
            continue
        if schema == "CONFIG":
            return False, f"Query references unauthorised config table: {schema}.{table}"
        return False, f"Query references unauthorised schema: {schema}"

    return True, "OK"


def _generate_sql_from_nl(context: TenantContext, user_query: str) -> str:
    """
    Translate a natural-language financial question into T-SQL via Azure OpenAI.

    Raises HTTPException(503) when the OpenAI service is unavailable.
    """
    try:
        client = _msi_openai_client()
    except Exception as exc:
        logger.error(f"[{context.request_id}] Failed to initialise OpenAI client: {exc}")
        raise HTTPException(
            status_code=503,
            detail="SQL generation service is temporarily unavailable.",
        )

    try:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": _SQL_GENERATION_SYSTEM_PROMPT},
                {"role": "user",   "content": user_query},
            ],
            temperature=0.1,
            max_tokens=800,
        )
        if not response.choices:
            raise ValueError("Azure OpenAI returned an empty choices list")
        sql = response.choices[0].message.content.strip()
        logger.debug(f"[{context.request_id}] Raw generated SQL: {sql}")
        return sql
    except Exception:
        logger.exception(f"[{context.request_id}] SQL generation failed")
        raise HTTPException(
            status_code=503,
            detail=f"SQL generation service is temporarily unavailable. Request ID: {context.request_id}",
        )


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
    """
    Answer a natural-language financial question with tenant-isolated data.

    The endpoint:
      1. Rejects raw SQL input (natural-language questions only).
      2. Generates T-SQL via Azure OpenAI from the question.
      3. Validates the generated SQL for safety.
      4. Applies tenant schema prefix and RLS filter.
      5. Executes the query and returns the results.
    """
    try:
        user_query = request_data.query
        logger.info(f"[{context.request_id}] {context.user_id} querying {context.tenant_id}: {user_query}")

        # Step 1: Reject raw SQL — only natural-language questions are accepted.
        validate_nl_query(user_query)

        # Step 2: Generate T-SQL from the natural-language question via Azure OpenAI.
        generated_sql = _generate_sql_from_nl(context, user_query)
        logger.info(f"[{context.request_id}] Generated SQL ({len(generated_sql)} chars)")

        # Step 3: Validate the generated SQL for safety before executing it.
        is_safe, reason = validate_sql(generated_sql)
        if not is_safe:
            logger.warning(f"[{context.request_id}] Generated SQL failed validation: {reason}")
            raise HTTPException(status_code=400, detail=f"SQL validation failed: {reason}")

        # Step 4: Apply tenant schema prefix and RLS filter.
        tenant_aware_query, rls_params = build_tenant_aware_query(context, generated_sql)

        # Step 5: Execute the query.
        df = execute_query(context, tenant_aware_query, rls_params)
        result_data = df.head(MAX_RESULT_ROWS).to_dict(orient="records")

        return QAResponse(
            question=user_query,
            answer=f"Returned {len(df)} rows from {context.database.schema} schema",
            generated_sql=tenant_aware_query,
            result_data=result_data,
            tenant_id=context.tenant_id,
            row_count=len(df)
        )
    except HTTPException:
        raise
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
async def list_available_tenants(context: TenantContext = Depends(get_tenant_context)):
    """List all registered tenants (admin endpoint — requires authentication)."""
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
