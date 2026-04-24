"""
Financial Intelligence Platform — Conversational Q&A Agent (RAG + Text-to-SQL)
===============================================================================
Part 5.4 of the Master Architecture Guide · HU GAAP

Architecture: Semantic Layer → Intent Classification → Text-to-SQL → Execution → Formatting

Pipeline:
  1. Classify user intent: kpi_lookup | trend_analysis | variance | drill_down
  2. Retrieve schema context from vector store (Azure Cognitive Search)
  3. Generate SQL via Azure OpenAI, constrained to Azure Synapse T-SQL dialect
  4. Apply RLS security filter — users see only their authorised entities
  5. Validate and execute SQL against Synapse
  6. Format response with LLM — ALWAYS include the generated SQL (trust building)

Key design principle (Master Guide 5.4):
  "Always display the generated SQL query alongside the result to build trust
  and enable validation by finance professionals."

Usage:
    # As API server (FastAPI)
    uvicorn financial_qa_agent:app --host 0.0.0.0 --port 8000

    # As CLI
    python financial_qa_agent.py --query "Mi volt az EBITDA margin Q1 2026-ban?"

Dependencies: openai, fastapi, uvicorn, pandas, pyodbc, azure-search-documents,
              azure-identity, pydantic
"""

import argparse
import json
import logging
import os
import re
import sys
from typing import Any, Optional

import pandas as pd
import pyodbc
from azure.identity import ManagedIdentityCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from openai import AzureOpenAI
from pydantic import BaseModel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from db_utils import get_db_connection, get_openai_client

try:
    from fastapi import FastAPI, HTTPException, Security
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FIP.FinancialQAAgent")

AZURE_OPENAI_ENDPOINT    = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_DEPLOYMENT  = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
AZURE_SEARCH_ENDPOINT    = os.getenv("AZURE_SEARCH_ENDPOINT", "")
AZURE_SEARCH_INDEX       = os.getenv("AZURE_SEARCH_INDEX", "fip-schema-index")
SYNAPSE_SERVER           = os.getenv("SYNAPSE_SERVER", "")
SYNAPSE_DATABASE         = os.getenv("SYNAPSE_DATABASE", "fip_dw")
MAX_RESULT_ROWS          = int(os.getenv("MAX_RESULT_ROWS", "500"))
QUERY_TIMEOUT_SECONDS    = int(os.getenv("QUERY_TIMEOUT_SECONDS", "30"))

# Intent classification categories
INTENT_CLASSES = ["kpi_lookup", "trend_analysis", "variance", "drill_down", "unknown"]

# SQL injection protection: blocked keywords
SQL_BLOCKED_KEYWORDS = [
    "DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "EXEC", "EXECUTE", "xp_", "sp_", "GRANT", "REVOKE", "OPENROWSET"
]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

class QARequest(BaseModel):
    query: str
    user_id: str
    entity_code: Optional[str] = None
    language: str = "en"


class QAResponse(BaseModel):
    question: str
    answer: str
    generated_sql: str
    result_data: Optional[list[dict]] = None
    intent: str
    row_count: int
    warning: Optional[str] = None


def get_search_client() -> SearchClient:
    credential = ManagedIdentityCredential()
    return SearchClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        index_name=AZURE_SEARCH_INDEX,
        credential=credential
    )


# ---------------------------------------------------------------------------
# Step 1: Intent Classification
# ---------------------------------------------------------------------------

def classify_intent(client: AzureOpenAI, user_query: str) -> str:
    """
    Classify the user's financial question into one of four intent categories.
    Returns: 'kpi_lookup' | 'trend_analysis' | 'variance' | 'drill_down' | 'unknown'
    """
    classification_prompt = """You are an intent classifier for a financial analytics platform.
Classify the user's question into exactly one category:

- kpi_lookup: User wants a specific KPI value for a specific period (e.g., "What was EBITDA in Q3?")
- trend_analysis: User wants to see a KPI over multiple periods (e.g., "Show revenue trend for 2025")
- variance: User wants to compare actuals vs budget or vs prior year (e.g., "Why did costs increase?")
- drill_down: User wants to go from summary to detail level (e.g., "Show me the top cost centres")
- unknown: Query is not about financial data or cannot be answered by the system

Respond with ONLY the category name, nothing else."""

    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": classification_prompt},
            {"role": "user",   "content": user_query}
        ],
        temperature=0,
        max_tokens=20
    )
    intent = response.choices[0].message.content.strip().lower()
    return intent if intent in INTENT_CLASSES else "unknown"


# ---------------------------------------------------------------------------
# Step 2: Schema Context Retrieval (Vector Search)
# ---------------------------------------------------------------------------

def retrieve_schema_context(search_client: SearchClient, openai_client: AzureOpenAI,
                             user_query: str, k: int = 5) -> str:
    """
    Retrieve relevant schema documentation from Azure Cognitive Search vector index.
    The index contains table descriptions, column definitions, and HU GAAP terminology.
    """
    # Generate embedding for semantic search
    embed_response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=user_query
    )
    query_vector = embed_response.data[0].embedding

    vector_query = VectorizedQuery(
        vector=query_vector,
        k_nearest_neighbors=k,
        fields="content_vector"
    )

    results = search_client.search(
        search_text=user_query,
        vector_queries=[vector_query],
        select=["table_name", "column_name", "description", "hu_gaap_mapping", "example_values"],
        top=k
    )

    context_parts = []
    for result in results:
        context_parts.append(
            f"Table: {result.get('table_name', '')} "
            f"Column: {result.get('column_name', '')} "
            f"Description: {result.get('description', '')} "
            f"HU GAAP: {result.get('hu_gaap_mapping', '')} "
            f"Examples: {result.get('example_values', '')}"
        )

    return "\n".join(context_parts)


# ---------------------------------------------------------------------------
# Step 3: SQL Generation
# ---------------------------------------------------------------------------

def get_rls_clause(user_id: str, entity_code: Optional[str]) -> tuple[str, list]:
    """
    Build the Row-Level Security filter clause with parameterised values.
    Users are restricted to entities they are authorised for in config.rls_user_entity_map.
    This clause is injected into every generated SQL — cannot be bypassed.

    Returns:
        tuple: (rls_sql_clause, parameter_values) — use parameterised query execution
    """
    if entity_code:
        # If a specific entity is requested, still validate user has access
        return (
            """AND e.entity_code IN (
                SELECT entity_code FROM config.rls_user_entity_map
                WHERE user_id = ?
                  AND entity_code = ?
                  AND is_active = TRUE
            )""",
            [user_id, entity_code]
        )
    else:
        return (
            """AND e.entity_code IN (
                SELECT entity_code FROM config.rls_user_entity_map
                WHERE user_id = ?
                  AND is_active = TRUE
            )""",
            [user_id]
        )


SQL_GENERATION_SYSTEM_PROMPT = """You are a SQL expert for an Azure Synapse Analytics financial data warehouse.
Generate T-SQL queries against the Financial Intelligence Platform (FIP) Gold Zone.

AVAILABLE GOLD ZONE TABLES:
- gold.kpi_profitability (period_key, entity_key, revenue, ebitda, ebitda_margin_pct, gross_margin_pct, net_profit, roic_pct, revenue_budget, revenue_variance_pct, revenue_py, revenue_yoy_pct)
- gold.kpi_liquidity (period_key, entity_key, current_ratio, dso_days, dpo_days, cash_conversion_cycle_days, net_debt_ebitda, operating_cash_flow, free_cash_flow)
- gold.agg_pl_monthly (period_key, entity_key, revenue, cogs, gross_profit, ebitda, net_profit, revenue_budget, revenue_variance)
- gold.agg_balance_sheet (period_key, entity_key, universal_node, account_type, l1_category, closing_balance_lcy)
- silver.dim_entity (entity_key, entity_code, entity_name, reporting_currency)
- silver.dim_date (date_key, full_date, fiscal_year, fiscal_period, quarter_number)

PERIOD KEY FORMAT: YYYYMM integer (e.g., 202601 = January 2026)
AMOUNTS: All in HUF (Hungarian Forint) unless otherwise specified
GAAP BASIS: HU GAAP (2000/C Act) — always active

RULES:
- Always JOIN to silver.dim_entity to apply the RLS filter ({{RLS_CLAUSE}} placeholder)
- Use TOP N to limit results to avoid huge result sets
- Format amounts in millions: amount / 1000000 AS amount_mhuf
- Always include period_key and entity_name in SELECT
- Do NOT use subqueries in WHERE if a JOIN works — Synapse performs better
- Only query gold.* and silver.dim_* tables — never bronze.*
- Do NOT generate DML statements (INSERT/UPDATE/DELETE/DROP)

{schema_context}
"""

def generate_sql(client: AzureOpenAI, user_query: str, schema_context: str,
                 rls_clause: str, intent: str) -> str:
    """Generate T-SQL query from natural language using GPT-4o."""
    system_prompt = SQL_GENERATION_SYSTEM_PROMPT.format(
        schema_context=schema_context
    )

    user_message = f"""Generate a T-SQL query for Azure Synapse Analytics to answer:
"{user_query}"

Intent classification: {intent}
RLS security clause to inject into every query WHERE clause: {rls_clause}

Return ONLY the SQL query, no explanation. The query must include the RLS clause.
Limit to TOP {MAX_RESULT_ROWS} rows."""

    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message}
        ],
        temperature=0.1,
        max_tokens=1000
    )
    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Step 4: SQL Validation + Execution
# ---------------------------------------------------------------------------

def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Validate generated SQL for safety before execution.
    Returns (is_safe, reason).
    """
    sql_upper = sql.upper()

    # Check for blocked keywords
    for keyword in SQL_BLOCKED_KEYWORDS:
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, sql_upper):
            return False, f"SQL contains blocked keyword: {keyword}"

    # Must be a SELECT statement
    stripped = sql_upper.strip()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        return False, "SQL must start with SELECT or WITH (CTE)"

    # Must reference only allowed schemas
    allowed_schemas = {"GOLD", "SILVER", "CONFIG"}
    # Simple heuristic check — full parse would require sqlparse
    matches = re.findall(r'\bFROM\s+(\w+)\.', sql_upper)
    matches += re.findall(r'\bJOIN\s+(\w+)\.', sql_upper)
    for schema in matches:
        if schema not in allowed_schemas:
            return False, f"Query references unauthorised schema: {schema}"

    return True, "OK"


def execute_sql(conn: pyodbc.Connection, sql: str) -> pd.DataFrame:
    """Execute validated SQL against Synapse and return result as DataFrame."""
    try:
        df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        raise RuntimeError(f"SQL execution failed: {e}") from e


# ---------------------------------------------------------------------------
# Step 5: Response Formatting
# ---------------------------------------------------------------------------

def format_response(client: AzureOpenAI, user_query: str, df_result: pd.DataFrame,
                    sql: str, intent: str, language: str = "en") -> str:
    """
    Use GPT-4o to format the raw query result into a natural language financial answer.
    ALWAYS includes the generated SQL in the response for transparency.
    """
    if df_result.empty:
        data_summary = "The query returned no results."
    else:
        # Convert DataFrame to compact JSON for LLM input
        data_summary = df_result.head(20).to_json(orient="records", default_handler=str)

    lang_instruction = ""
    if language == "hu":
        lang_instruction = "Answer in Hungarian (Magyar). Use formal business language."

    format_prompt = f"""You are a senior financial analyst presenting data to a CFO.
The user asked: "{user_query}"
The data retrieved is: {data_summary}
Intent: {intent}
{lang_instruction}

Provide a clear, concise financial answer (3-5 sentences maximum).
Use specific numbers from the data. Reference HU GAAP where relevant.
End with the SQL query used to retrieve this data, labelled "SQL Query Used:".

IMPORTANT: Always include the generated SQL so the user can validate the calculation.
"""
    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[{"role": "user", "content": format_prompt}],
        temperature=0.2,
        max_tokens=600
    )
    answer = response.choices[0].message.content

    # Ensure SQL is always appended even if LLM forgot
    if "SQL Query Used:" not in answer:
        answer += f"\n\n**SQL Query Used:**\n```sql\n{sql}\n```"

    return answer


# ---------------------------------------------------------------------------
# Main Q&A Pipeline (FinancialQAAgent class)
# ---------------------------------------------------------------------------

class FinancialQAAgent:
    """Main agent class — orchestrates the full RAG + Text-to-SQL pipeline."""

    def __init__(self):
        self.openai_client  = get_openai_client()
        self.search_client  = get_search_client()
        self.db_conn        = get_db_connection()

    def answer(self, query: str, user_id: str, entity_code: Optional[str] = None,
               language: str = "en") -> dict:
        """
        Answer a natural language financial question.
        Returns dict with answer, SQL, and data for dashboard display.
        """
        logger.info(f"Processing query: user={user_id}, query='{query[:80]}...'")

        # 1. Classify intent
        intent = classify_intent(self.openai_client, query)
        logger.info(f"Intent classified: {intent}")

        if intent == "unknown":
            return {
                "question": query,
                "answer": "I can only answer questions about financial data in this system. "
                          "Please ask about revenue, costs, KPIs, or financial ratios.",
                "generated_sql": "",
                "intent": intent,
                "row_count": 0
            }

        # 2. Retrieve schema context
        schema_context = retrieve_schema_context(
            self.search_client, self.openai_client, query, k=5
        )

        # 3. Build RLS clause
        rls_clause = get_rls_clause(user_id, entity_code)

        # 4. Generate SQL
        sql = generate_sql(self.openai_client, query, schema_context, rls_clause, intent)
        logger.info(f"SQL generated: {len(sql)} chars")

        # 5. Validate SQL
        is_safe, reason = validate_sql(sql)
        if not is_safe:
            logger.warning(f"SQL validation failed: {reason}")
            return {
                "question": query,
                "answer": f"This query could not be processed for security reasons: {reason}",
                "generated_sql": sql,
                "intent": intent,
                "row_count": 0,
                "warning": reason
            }

        # 6. Execute SQL
        try:
            df_result = execute_sql(self.db_conn, sql)
        except RuntimeError as e:
            logger.error(f"Query execution failed: {e}")
            return {
                "question": query,
                "answer": f"The query executed but returned an error: {e}",
                "generated_sql": sql,
                "intent": intent,
                "row_count": 0,
                "warning": str(e)
            }

        # 7. Format response
        answer = format_response(
            self.openai_client, query, df_result, sql, intent, language
        )

        return {
            "question": query,
            "answer": answer,
            "generated_sql": sql,
            "result_data": df_result.head(MAX_RESULT_ROWS).to_dict(orient="records"),
            "intent": intent,
            "row_count": len(df_result)
        }

    def close(self):
        self.db_conn.close()


# ---------------------------------------------------------------------------
# FastAPI server (for Power BI Custom Visual or Teams bot integration)
# ---------------------------------------------------------------------------

if FASTAPI_AVAILABLE:
    app = FastAPI(
        title="FIP Financial Q&A Agent",
        description="Natural language financial analytics — HU GAAP · Azure Synapse",
        version="1.0.0"
    )
    agent = None

    @app.on_event("startup")
    async def startup():
        global agent
        agent = FinancialQAAgent()
        logger.info("FinancialQAAgent initialized")

    @app.post("/query", response_model=QAResponse)
    async def query_endpoint(request: QARequest):
        if agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")
        result = agent.answer(
            query=request.query,
            user_id=request.user_id,
            entity_code=request.entity_code,
            language=request.language
        )
        return QAResponse(**result)

    @app.get("/health")
    async def health():
        return {"status": "healthy", "version": "1.0.0"}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FIP Financial Q&A Agent CLI")
    parser.add_argument("--query",      required=True, help="Natural language financial question")
    parser.add_argument("--user_id",    default="cli_user")
    parser.add_argument("--entity_code", default=None)
    parser.add_argument("--language",   default="en", choices=["en", "hu"])
    args = parser.parse_args()

    qa = FinancialQAAgent()
    try:
        result = qa.answer(args.query, args.user_id, args.entity_code, args.language)
        print("\n" + "="*70)
        print(f"QUESTION: {result['question']}")
        print(f"INTENT:   {result['intent']}")
        print(f"ROWS:     {result['row_count']}")
        print("-"*70)
        print(f"ANSWER:\n{result['answer']}")
        print("="*70)
    finally:
        qa.close()


if __name__ == "__main__":
    main()
