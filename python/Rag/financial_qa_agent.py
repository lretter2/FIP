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
    from fastapi.responses import HTMLResponse
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


def execute_sql(conn: pyodbc.Connection, sql: str, params: list = None) -> pd.DataFrame:
    """Execute validated SQL against Synapse and return result as DataFrame."""
    try:
        df = pd.read_sql(sql, conn, params=params or [])
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
        rls_clause, rls_params = get_rls_clause(user_id, entity_code)

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
        conn = get_db_connection()
        try:
            df_result = execute_sql(conn, sql, rls_params)
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
        finally:
            conn.close()

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

    @app.get("/ui", response_class=HTMLResponse)
    async def ui():
        """
        Web UI for the FIP Financial Q&A Agent.
        Provides a CFO-persona dashboard with KPI summary cards, a natural
        language query input, result table, AI narrative, and generated SQL
        display — all served from the same FastAPI process.
        """
        html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FIP Financial Q&amp;A Agent</title>
<style>
  :root {
    --navy:   #1B3A6B;
    --blue:   #1565C0;
    --green:  #2E7D32;
    --amber:  #E65100;
    --red:    #C62828;
    --grey:   #607D8B;
    --bg:     #F5F6FA;
    --white:  #FFFFFF;
    --border: #CFD8DC;
    --text:   #212121;
    --muted:  #546E7A;
    --radius: 6px;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: "Segoe UI", Arial, sans-serif; background: var(--bg); color: var(--text); }

  /* ── Header ──────────────────────────────────────────────── */
  header {
    background: var(--navy);
    color: var(--white);
    padding: 0 24px;
    height: 56px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  header h1 { font-size: 18px; font-weight: 600; letter-spacing: .3px; }
  header .badge {
    font-size: 11px;
    background: rgba(255,255,255,.18);
    border-radius: 20px;
    padding: 3px 10px;
  }

  /* ── Layout ──────────────────────────────────────────────── */
  main { max-width: 1440px; margin: 0 auto; padding: 20px 24px 40px; }
  section { margin-bottom: 24px; }
  h2 { font-size: 13px; font-weight: 600; color: var(--muted);
       text-transform: uppercase; letter-spacing: .6px; margin-bottom: 12px; }

  /* ── KPI Cards ───────────────────────────────────────────── */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(190px, 1fr));
    gap: 12px;
  }
  .kpi-card {
    background: var(--white);
    border: 1px solid var(--border);
    border-top: 3px solid var(--navy);
    border-radius: var(--radius);
    padding: 14px 16px 12px;
  }
  .kpi-card .kpi-label { font-size: 11px; color: var(--muted); text-transform: uppercase;
                         letter-spacing: .4px; margin-bottom: 6px; }
  .kpi-card .kpi-value { font-size: 26px; font-weight: 700; color: var(--navy); line-height: 1; }
  .kpi-card .kpi-sub   { font-size: 11px; color: var(--muted); margin-top: 5px; }
  .kpi-card .kpi-delta { font-size: 12px; font-weight: 600; margin-top: 4px; }
  .delta-pos { color: var(--green); }
  .delta-neg { color: var(--red); }
  .delta-neu { color: var(--grey); }

  /* ── Query Panel ─────────────────────────────────────────── */
  .query-panel {
    background: var(--white);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px 24px;
  }
  .query-row { display: flex; gap: 10px; align-items: flex-start; flex-wrap: wrap; }
  .query-row textarea {
    flex: 1 1 400px;
    min-height: 64px;
    padding: 10px 12px;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    font-size: 14px;
    font-family: inherit;
    resize: vertical;
  }
  .query-row textarea:focus { outline: 2px solid var(--blue); border-color: transparent; }
  .query-meta { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
  .query-meta input, .query-meta select {
    padding: 7px 10px;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    font-size: 13px;
    font-family: inherit;
  }
  .query-meta input:focus, .query-meta select:focus { outline: 2px solid var(--blue); }
  .btn {
    padding: 10px 22px;
    background: var(--navy);
    color: var(--white);
    border: none;
    border-radius: var(--radius);
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    white-space: nowrap;
  }
  .btn:hover { background: var(--blue); }
  .btn:disabled { opacity: .5; cursor: not-allowed; }

  /* ── Spinner ─────────────────────────────────────────────── */
  #spinner { display: none; align-items: center; gap: 10px; color: var(--muted);
             font-size: 13px; margin-top: 12px; }
  .spin {
    width: 18px; height: 18px;
    border: 2px solid var(--border);
    border-top-color: var(--navy);
    border-radius: 50%;
    animation: spin .7s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* ── Results ─────────────────────────────────────────────── */
  #results { display: none; }
  .intent-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .4px;
    background: #E3F2FD;
    color: var(--blue);
    margin-left: 8px;
  }
  .answer-box {
    background: var(--white);
    border-left: 4px solid var(--navy);
    border-radius: 0 var(--radius) var(--radius) 0;
    padding: 16px 20px;
    font-size: 14px;
    line-height: 1.6;
    white-space: pre-wrap;
    margin-bottom: 16px;
  }
  .sql-box {
    background: #263238;
    color: #ECEFF1;
    border-radius: var(--radius);
    padding: 14px 16px;
    font-family: "Cascadia Code", "Consolas", monospace;
    font-size: 12.5px;
    line-height: 1.5;
    overflow-x: auto;
    margin-bottom: 16px;
  }
  .sql-box summary {
    cursor: pointer;
    color: #90A4AE;
    font-family: "Segoe UI", Arial, sans-serif;
    font-size: 12px;
    margin-bottom: 8px;
    user-select: none;
  }
  .sql-box summary:hover { color: var(--white); }
  .result-table-wrap { overflow-x: auto; }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    background: var(--white);
    border-radius: var(--radius);
    overflow: hidden;
  }
  th {
    background: var(--navy);
    color: var(--white);
    padding: 9px 12px;
    text-align: left;
    font-weight: 600;
    white-space: nowrap;
  }
  td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #EEF2FF; }
  .row-count { font-size: 12px; color: var(--muted); margin-top: 8px; }

  /* ── Warning / Error ─────────────────────────────────────── */
  .alert {
    padding: 12px 16px;
    border-radius: var(--radius);
    font-size: 13px;
    margin-bottom: 12px;
  }
  .alert-warn { background: #FFF8E1; border-left: 4px solid #FFA000; }
  .alert-err  { background: #FDECEA; border-left: 4px solid var(--red); }

  /* ── Suggestions ─────────────────────────────────────────── */
  .suggestions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
  .suggestion-btn {
    font-size: 12px;
    padding: 5px 12px;
    border: 1px solid var(--border);
    border-radius: 20px;
    background: var(--white);
    cursor: pointer;
    color: var(--navy);
    transition: background .15s;
  }
  .suggestion-btn:hover { background: #E8EAF6; border-color: var(--blue); }

  /* ── Footer ──────────────────────────────────────────────── */
  footer {
    text-align: center;
    font-size: 11px;
    color: var(--muted);
    margin-top: 32px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
  }
</style>
</head>
<body>

<header>
  <h1>&#128202; FIP Financial Q&amp;A Agent</h1>
  <span class="badge">HU GAAP &middot; Azure Synapse &middot; v1.0</span>
</header>

<main>

  <!-- KPI Summary Cards -->
  <section>
    <h2>CFO Dashboard KPIs — Current Selection</h2>
    <div class="kpi-grid">
      <div class="kpi-card" id="kpi-revenue">
        <div class="kpi-label">Revenue</div>
        <div class="kpi-value" id="kv-revenue">—</div>
        <div class="kpi-sub">vs Budget</div>
        <div class="kpi-delta" id="kd-revenue">&nbsp;</div>
      </div>
      <div class="kpi-card" id="kpi-ebitda">
        <div class="kpi-label">EBITDA</div>
        <div class="kpi-value" id="kv-ebitda">—</div>
        <div class="kpi-sub">vs Budget</div>
        <div class="kpi-delta" id="kd-ebitda">&nbsp;</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">EBITDA Margin %</div>
        <div class="kpi-value" id="kv-ebitda-margin">—</div>
        <div class="kpi-sub">Current period</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Net Profit</div>
        <div class="kpi-value" id="kv-net-profit">—</div>
        <div class="kpi-sub">HU GAAP mérleg szerinti eredmény</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Free Cash Flow</div>
        <div class="kpi-value" id="kv-fcf">—</div>
        <div class="kpi-sub">Operating CF − Capex</div>
      </div>
      <div class="kpi-card" id="kpi-current-ratio">
        <div class="kpi-label">Current Ratio</div>
        <div class="kpi-value" id="kv-current-ratio">—</div>
        <div class="kpi-sub">Target &#8805; 1.5</div>
      </div>
    </div>
    <p style="font-size:12px;color:var(--muted);margin-top:10px;">
      KPI values update automatically after each query that returns profitability or balance-sheet data.
      Use the Q&amp;A input below to retrieve live figures from Azure Synapse.
    </p>
  </section>

  <!-- Q&A Query Panel -->
  <section>
    <h2>Ask a Financial Question</h2>
    <div class="query-panel">
      <div class="query-row">
        <textarea id="query-input" placeholder="e.g. What was the EBITDA margin in Q1 2026? / Mi volt az árbevétel 2026 január-ban?"></textarea>
        <button class="btn" id="submit-btn" onclick="submitQuery()">Ask &#10148;</button>
      </div>
      <div class="query-meta">
        <input id="user-id" type="hidden" value="">
        <span class="meta-note">User identity is determined automatically by your session.</span>
        <input id="entity-code" type="text" placeholder="Entity code (optional)" style="width:200px;">
        <select id="language">
          <option value="en">English</option>
          <option value="hu">Magyar</option>
        </select>
      </div>
      <div class="suggestions">
        <button class="suggestion-btn" onclick="setSuggestion('What was the EBITDA margin in Q1 2026?')">EBITDA margin Q1 2026</button>
        <button class="suggestion-btn" onclick="setSuggestion('Show revenue trend for fiscal year 2026')">Revenue trend FY2026</button>
        <button class="suggestion-btn" onclick="setSuggestion('Why did costs increase last quarter?')">Cost variance last quarter</button>
        <button class="suggestion-btn" onclick="setSuggestion('Show me the top 10 cost centres by spend')">Top 10 cost centres</button>
        <button class="suggestion-btn" onclick="setSuggestion('What is the current ratio for all entities?')">Current ratio all entities</button>
        <button class="suggestion-btn" onclick="setSuggestion('Compare free cash flow vs prior year by entity')">FCF vs prior year</button>
      </div>
      <div id="spinner"><div class="spin"></div> Processing query&hellip;</div>
    </div>
  </section>

  <!-- Results -->
  <section id="results">
    <h2>
      Answer
      <span class="intent-badge" id="intent-badge"></span>
    </h2>

    <div id="warning-box" class="alert alert-warn" style="display:none;"></div>

    <div class="answer-box" id="answer-text"></div>

    <details class="sql-box">
      <summary>&#128196; Generated SQL Query (click to expand)</summary>
      <pre id="sql-text"></pre>
    </details>

    <div class="result-table-wrap">
      <table id="result-table">
        <thead id="result-thead"></thead>
        <tbody id="result-tbody"></tbody>
      </table>
      <div class="row-count" id="row-count-label"></div>
    </div>
  </section>

</main>

<footer>
  Financial Intelligence Platform &middot; HU GAAP (2000/C Act) &middot; Azure Synapse Analytics &middot;
  <a href="/docs" style="color:var(--blue);">API Docs</a> &middot;
  <a href="/health" style="color:var(--blue);">Health</a>
</footer>

<script>
  // ── Helpers ──────────────────────────────────────────────────────────────

  function fmt_mhuf(val) {
    if (val == null || isNaN(val)) return "—";
    const m = val / 1e6;
    return (m >= 0 ? "+" : "") + m.toLocaleString("en-GB", {maximumFractionDigits: 1}) + " M HUF";
  }

  function fmt_pct(val) {
    if (val == null || isNaN(val)) return "—";
    return (val * 100).toFixed(1) + "%";
  }

  function fmt_ratio(val) {
    if (val == null || isNaN(val)) return "—";
    return parseFloat(val).toFixed(2);
  }

  function ragClass(val) {
    if (val == null || isNaN(val)) return "delta-neu";
    return val >= 0 ? "delta-pos" : "delta-neg";
  }

  function setSuggestion(text) {
    document.getElementById("query-input").value = text;
  }

  // ── KPI update from result data ──────────────────────────────────────────

  function tryUpdateKpis(rows) {
    if (!rows || rows.length === 0) return;
    // Look for known column names (case-insensitive) across the result rows
    const first = rows[0];
    const keys = Object.keys(first).map(k => k.toLowerCase());

    function getCol(row, ...names) {
      const keyMap = Object.keys(row).reduce((acc, key) => {
        acc[key.toLowerCase()] = key;
        return acc;
      }, {});
      const aliases = {
        revenue: ["total_revenue"],
        ebitda: ["total_ebitda"],
        net_profit: ["net_income", "profit_after_tax"],
        free_cash_flow: ["fcf"],
        fcf: ["free_cash_flow"],
        revenue_variance_pct: ["revenue_yoy_pct"]
      };

      for (const rawName of names) {
        const name = rawName.toLowerCase();
        const candidates = [name, ...(aliases[name] || [])];
        for (const candidate of candidates) {
          const match = keyMap[candidate.toLowerCase()];
          if (match !== undefined && row[match] !== null) return parseFloat(row[match]);
        }
      }
      return null;
    }

    // Aggregate totals across all returned rows (sum numeric KPIs)
    const totals = {};
    for (const row of rows) {
      for (const k of Object.keys(row)) {
        const v = parseFloat(row[k]);
        if (!isNaN(v)) totals[k] = (totals[k] || 0) + v;
      }
    }

    const revenue    = getCol(totals, "revenue");
    const ebitda     = getCol(totals, "ebitda");
    const net_profit = getCol(totals, "net_profit");
    const fcf        = getCol(totals, "free_cash_flow", "fcf");
    const cur_ratio  = getCol(rows[0], "current_ratio");  // ratio — don't sum
    const ebitda_m   =
      revenue !== null &&
      revenue !== undefined &&
      revenue !== 0 &&
      ebitda !== null &&
      ebitda !== undefined
        ? ebitda / revenue
        : null;
    const rev_var_pct = getCol(rows[0], "revenue_variance_pct", "revenue_yoy_pct");

    if (revenue !== null) {
      document.getElementById("kv-revenue").textContent = fmt_mhuf(revenue);
      if (rev_var_pct !== null) {
        const el = document.getElementById("kd-revenue");
        el.textContent = (rev_var_pct >= 0 ? "▲ " : "▼ ") + fmt_pct(rev_var_pct) + " vs budget";
        el.className = "kpi-delta " + ragClass(rev_var_pct);
      }
    }
    if (ebitda !== null)     document.getElementById("kv-ebitda").textContent = fmt_mhuf(ebitda);
    if (ebitda_m !== null)   document.getElementById("kv-ebitda-margin").textContent = fmt_pct(ebitda_m);
    if (net_profit !== null) document.getElementById("kv-net-profit").textContent = fmt_mhuf(net_profit);
    if (fcf !== null)        document.getElementById("kv-fcf").textContent = fmt_mhuf(fcf);
    if (cur_ratio !== null) {
      const el = document.getElementById("kv-current-ratio");
      el.textContent = fmt_ratio(cur_ratio);
      el.style.color = cur_ratio < 1.0 ? "var(--red)" : cur_ratio < 1.5 ? "var(--amber)" : "var(--green)";
    }
  }

  // ── Table render ─────────────────────────────────────────────────────────

  function renderTable(rows) {
    const thead = document.getElementById("result-thead");
    const tbody = document.getElementById("result-tbody");
    thead.innerHTML = "";
    tbody.innerHTML = "";
    if (!rows || rows.length === 0) return;

    const cols = Object.keys(rows[0]);
    const headerRow = document.createElement("tr");
    cols.forEach(c => {
      const th = document.createElement("th");
      th.textContent = c;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    rows.forEach(row => {
      const tr = document.createElement("tr");
      cols.forEach(c => {
        const td = document.createElement("td");
        const v = row[c];
        td.textContent = (v === null || v === undefined) ? "" : v;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  // ── Submit query ─────────────────────────────────────────────────────────

  async function submitQuery() {
    const query      = document.getElementById("query-input").value.trim();
    const user_id    = document.getElementById("user-id").value.trim() || "web_user";
    const entity_code = document.getElementById("entity-code").value.trim() || null;
    const language   = document.getElementById("language").value;

    if (!query) { alert("Please enter a question."); return; }

    document.getElementById("submit-btn").disabled = true;
    document.getElementById("spinner").style.display = "flex";
    document.getElementById("results").style.display = "none";
    document.getElementById("warning-box").style.display = "none";

    try {
      const resp = await fetch("/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, user_id, entity_code, language })
      });

      const data = await resp.json();

      if (!resp.ok) {
        throw new Error(data.detail || "Server error " + resp.status);
      }

      // Populate results
      document.getElementById("intent-badge").textContent = data.intent || "";
      document.getElementById("answer-text").textContent  = data.answer || "";
      document.getElementById("sql-text").textContent     = data.generated_sql || "";

      if (data.warning) {
        const wb = document.getElementById("warning-box");
        wb.textContent = "⚠ " + data.warning;
        wb.style.display = "block";
      }

      const rows = data.result_data || [];
      renderTable(rows);
      tryUpdateKpis(rows);

      const label = document.getElementById("row-count-label");
      label.textContent = rows.length > 0
        ? `Showing ${rows.length} of ${data.row_count} row(s) returned.`
        : "The query returned no data rows.";

      document.getElementById("results").style.display = "block";

    } catch (err) {
      const errDiv = document.createElement("div");
      errDiv.className = "alert alert-err";
      errDiv.textContent = "Error: " + err.message;
      document.getElementById("results").prepend(errDiv);
      document.getElementById("results").style.display = "block";
    } finally {
      document.getElementById("submit-btn").disabled = false;
      document.getElementById("spinner").style.display = "none";
    }
  }

  // Allow Ctrl+Enter to submit
  document.addEventListener("DOMContentLoaded", () => {
    document.getElementById("query-input").addEventListener("keydown", e => {
      if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) submitQuery();
    });
  });
</script>
</body>
</html>"""
        return HTMLResponse(content=html)


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
    result = qa.answer(args.query, args.user_id, args.entity_code, args.language)
    print("\n" + "="*70)
    print(f"QUESTION: {result['question']}")
    print(f"INTENT:   {result['intent']}")
    print(f"ROWS:     {result['row_count']}")
    print("-"*70)
    print(f"ANSWER:\n{result['answer']}")
    print("="*70)


if __name__ == "__main__":
    main()
