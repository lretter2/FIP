"""
Shared Azure database and OpenAI connection utilities.

Single authoritative implementation of Managed Identity token injection for
pyodbc (SQL_COPT_SS_ACCESS_TOKEN / attr 1256). All modules that need a
standard Synapse connection or an Azure OpenAI client should import from here.

Excluded: financial_forecaster.py (retrieves a full connection string from
Key Vault) and tenant_secured_qa_agent.py's get_db_connection (tenant-context-
aware; delegates here via conn_str_override).
"""

import logging
import os

import pyodbc
from azure.identity import ManagedIdentityCredential
from openai import AzureOpenAI

logger = logging.getLogger(__name__)

SYNAPSE_SERVER   = os.getenv("SYNAPSE_SERVER", "")
SYNAPSE_DATABASE = os.getenv("SYNAPSE_DATABASE", "fip_dw")
AZURE_OPENAI_ENDPOINT    = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")


def get_db_connection(
    server: str = None,
    database: str = None,
    conn_str_override: str = None,
) -> pyodbc.Connection:
    """
    Open an Azure Synapse connection authenticated via Managed Identity.

    Injects the MSI access token via SQL_COPT_SS_ACCESS_TOKEN (pyodbc attr 1256).
    Pass conn_str_override to supply a pre-built connection string (e.g. from
    Key Vault or a tenant context) while still using MSI token injection.
    """
    credential = ManagedIdentityCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("utf-16-le")
    token_struct = bytes([
        len(token_bytes) & 0xFF,
        (len(token_bytes) >> 8) & 0xFF,
        (len(token_bytes) >> 16) & 0xFF,
        (len(token_bytes) >> 24) & 0xFF,
    ])
    if conn_str_override:
        conn_str = conn_str_override
    else:
        srv = server or SYNAPSE_SERVER
        db  = database or SYNAPSE_DATABASE
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={srv};Database={db};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
    return pyodbc.connect(conn_str, attrs_before={1256: token_struct + token_bytes})


def get_openai_client(
    endpoint: str = None,
    api_version: str = None,
) -> AzureOpenAI:
    """Azure OpenAI client authenticated via Managed Identity."""
    credential = ManagedIdentityCredential()
    token = credential.get_token("https://cognitiveservices.azure.com/.default")
    return AzureOpenAI(
        azure_endpoint=endpoint or AZURE_OPENAI_ENDPOINT,
        api_version=api_version or AZURE_OPENAI_API_VERSION,
        azure_ad_token=token.token,
    )
