# Financial Intelligence Platform (FIP) — Security and Compliance  Reference

**Classification:** Internal — Confidential  
**Platform:** Azure (West Europe region)  
**Compliance Framework:** HU GAAP (2000/C Act), GDPR  
**Document Version:** 1.0  
**Last Updated:** 2026-04-17  
**Audience:** Platform Engineers, Security Engineers, DBA Team, Compliance Officers

---

## Table of Contents

1. [Architecture Security Overview](#1-architecture-security-overview)
2. [Managed Identity Authentication Flows](#2-managed-identity-authentication-flows)
3. [pyodbc MSI Token Injection Pattern](#3-pyodbc-msi-token-injection-pattern)
4. [Key Vault Secret Naming Convention](#4-key-vault-secret-naming-convention)
5. [RBAC Model](#5-rbac-model)
6. [Row-Level Security Enforcement](#6-row-level-security-enforcement)
7. [Network Isolation and Private Connectivity](#7-network-isolation-and-private-connectivity)
8. [HU GAAP Data Residency and Retention](#8-hu-gaap-data-residency-and-retention)
9. [GDPR Considerations](#9-gdpr-considerations)
10. [Audit Logging Coverage](#10-audit-logging-coverage)
11. [SQL Injection Protection](#11-sql-injection-protection)
12. [AI Governance](#12-ai-governance)
13. [Known Security Findings](#13-known-security-findings)
14. [Security Checklist for Releases](#14-security-checklist-for-releases)

---

## 1. Architecture Security Overview

The Financial Intelligence Platform is deployed exclusively in the **Azure West Europe** region within a single Azure Active Directory tenant. All compute, storage, and AI services communicate over private endpoints or VNet-injected paths. No data leaves the tenant boundary during normal operations; the sole external data path is the NBH (National Bank of Hungary) API for exchange rates, which is read-only and fetched over HTTPS from a Service Endpoint-enabled subnet.

### Defence-in-Depth Layers

| Layer | Control |
|---|---|
| Network perimeter | Private endpoints (KV, ADLS, Synapse, OpenAI), VNet injection (Databricks), NSG rules |
| Identity | Azure AD-only principal authentication; MSI preferred; service principals for SDK auth |
| Authorization | Azure RBAC (storage, KV, compute); Synapse SQL roles; application-level RLS |
| Data at rest | Azure Storage Service Encryption (SSE) with platform-managed keys; infrastructure encryption layer enabled on ADLS |
| Data in transit | TLS 1.2 minimum enforced on all endpoints; `minimumTlsVersion=TLS1_2` on ADLS |
| Secrets management | Azure Key Vault Premium (HSM-backed); RBAC model; soft-delete 90 days; purge protection permanently enabled |
| Audit | Log Analytics (365-day prod / 90-day dev), system_audit_log (append-only, never delete) |
| AI outputs | Commentary never auto-published; approval workflow in audit.commentary_queue |

### Immutability and Tamper-Evidence (HU GAAP Requirement)

ADLS bronze container is configured with **WORM immutability** for **2922 days (8 years)** in compliance with section 169(2) of the 2000/C Act (Számviteli törvény). This makes every raw inbound file tamper-evident from the moment it lands. Silver and Gold zone data derive from these immutable sources and are protected via Synapse row-security and audit logging.

---

## 2. Managed Identity Authentication Flows

### 2.1 System Assigned Managed Identities

The following services use **System Assigned Managed Identities (SAMI)**:

| Service | Identity Principal Name (pattern) | Roles Granted |
|---|---|---|
| Azure Data Factory | `fip-adf-<env>` | Storage Blob Data Contributor (ADLS), Key Vault Secrets User, Synapse SQL Administrator |
| Azure Synapse Analytics | `fip-synapse-<env>` | Storage Blob Data Contributor (ADLS), Key Vault Secrets User |
| Azure OpenAI | `fip-openai-<env>` | Key Vault Secrets User |

ADF uses its SAMI to authenticate all linked service connections. Every linked service references Key Vault for credentials rather than storing secrets inline. The SAMI is granted **Key Vault Secrets User** (read-only) so ADF can retrieve secrets at pipeline runtime but cannot create or delete them.

### 2.2 Databricks Workspace Managed Identity

Databricks clusters authenticate to Azure services using the **Databricks workspace-level managed identity** (sometimes called the Databricks MSI). This identity is granted:

- **Storage Blob Data Contributor** on the ADLS Gen2 account (for reading bronze and writing silver/gold staging paths)
- **Key Vault Secrets User** on the FIP Key Vault (for reading secrets via secret scopes)

Databricks jobs retrieve secrets via the `fip-kv` secret scope backed by Azure Key Vault:

```python
# Standard pattern used in all Databricks notebooks and jobs
pat_token = dbutils.secrets.get(scope="fip-kv", key="databricks-pat-token")
```

The secret scope is created once during platform bootstrap and must use the **Key Vault-backed** type (not Databricks-native) to ensure secrets are HSM-protected.

### 2.3 ADF Linked Service Authentication Flow

```
ADF Pipeline Trigger
       │
       ▼
ADF Linked Service (ls_synapse / ls_adls / ls_databricks)
       │  references Key Vault secret by name
       ▼
Azure Key Vault (RBAC: ADF SAMI = Secrets User)
       │  returns secret value at runtime
       ▼
Target service authenticates with the retrieved credential
```

For Databricks specifically, `ls_databricks` retrieves the secret named **`databricks-pat-token`** from Key Vault. This is the canonical name; do not use aliases or alternate names in any linked service definition.

### 2.4 Azure OpenAI Authentication

Azure OpenAI is configured with `disableLocalAuth=false` to retain API key authentication, which is required by the Azure OpenAI Python SDK used in `commentary_generator.py` and `financial_forecaster.py`. The API key is stored in Key Vault under the name **`azure-openai-api-key`** and is retrieved at runtime via the Databricks secret scope or the ADF linked service.

Although local auth is enabled, **MSI-based auth is preferred** for all new integrations. The `disableLocalAuth=false` setting is retained only for SDK compatibility and is subject to review when the SDK supports full MSI auth flows without fallback.

**AI governance note:** All OpenAI API calls are logged via RequestResponse diagnostic settings in Log Analytics. This captures the model name, token counts, and timestamps for every call, which is required for AI governance and cost attribution.

### 2.5 Synapse Analytics Authentication

Synapse is configured with `azureADOnlyAuthentication=false`. This is an intentional exception to support:

- **dbt** (data build tool) which uses ODBC/JDBC connections that may not support full Azure AD interactive flows in CI/CD
- **Power BI** embedded datasets which use service principal ODBC connections

Despite `azureADOnlyAuthentication=false`, all platform service accounts authenticate via Azure AD. SQL authentication credentials (if any exist for break-glass scenarios) are rotated on a quarterly schedule and stored in Key Vault under `synapse-admin-password`.

The Synapse workspace enforces:
- `preventDataExfiltration=true` (managed VNet prevents outbound to non-approved targets)
- `allowedAadTenantIdsForLinking=[tenantId]` (prevents cross-tenant linked service attacks)

---

## 3. pyodbc MSI Token Injection Pattern

### 3.1 The Correct Pattern

When a Python process running on Databricks or Azure VM connects to Synapse Dedicated SQL Pool via `pyodbc`, it must inject an MSI-acquired bearer token into the ODBC connection attributes. The connection string alone does not carry the token; it must be provided via `attrs_before`.

The correct, production-approved pattern (as implemented in `anomaly_detector.py` and `commentary_generator.py`) is:

```python
import struct
import pyodbc
from azure.identity import ManagedIdentityCredential

def get_db_connection(synapse_conn_str: str) -> pyodbc.Connection:
    """
    Establish a pyodbc connection to Synapse using MSI token injection.
    
    The attrs_before={1256: ...} injection is MANDATORY for Azure AD token
    authentication with pyodbc + SQL Server ODBC Driver 17/18.
    Attribute 1256 is SQL_COPT_SS_ACCESS_TOKEN.
    """
    credential = ManagedIdentityCredential()
    token = credential.get_token("https://database.windows.net/.default")
    
    # Token must be encoded as UTF-16-LE bytes prefixed with a 4-byte length
    token_bytes = token.token.encode("UTF-16-LE")
    token_struct = struct.pack("=i", len(token_bytes))
    
    conn = pyodbc.connect(
        synapse_conn_str,
        attrs_before={1256: token_struct + token_bytes}
    )
    return conn
```

**Why `attrs_before={1256: ...}` is required:**  
ODBC attribute `1256` maps to `SQL_COPT_SS_ACCESS_TOKEN` in the Microsoft ODBC Driver for SQL Server. When this attribute is set before the connection is opened, the driver substitutes the provided token for username/password authentication. Without this attribute, the driver attempts standard SQL authentication using the username and password fields in the connection string, which will be absent or incorrect for MSI-based connections, resulting in a "Login failed" or "Invalid authorization specification" error.

### 3.2 HIGH-Priority Bug: MSI Token Injection Missing in financial_qa_agent.py

**Bug ID:** BUG-003  
**Severity:** HIGH  
**Component:** `financial_qa_agent.py`  
**Status:** Open — requires immediate remediation before production deployment

`financial_qa_agent.py`'s `get_db_connection()` function calls `pyodbc.connect(conn_str)` **without** the `attrs_before={1256: token_struct + token_bytes}` injection. This means the QA agent will silently fail authentication against Synapse in production, returning a login error rather than query results.

**Buggy code (current):**

```python
# INCORRECT — will fail in production with Azure AD auth
def get_db_connection(conn_str: str) -> pyodbc.Connection:
    conn = pyodbc.connect(conn_str)  # No token injection
    return conn
```

**Fixed code:**

```python
# CORRECT — matches pattern in anomaly_detector.py and commentary_generator.py
import struct
import pyodbc
from azure.identity import ManagedIdentityCredential

def get_db_connection(conn_str: str) -> pyodbc.Connection:
    credential = ManagedIdentityCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("UTF-16-LE")
    token_struct = struct.pack("=i", len(token_bytes))
    conn = pyodbc.connect(
        conn_str,
        attrs_before={1256: token_struct + token_bytes}
    )
    return conn
```

**Impact:** Without this fix, every query executed by the financial QA agent fails at the connection stage. The agent is invoked by end users through the Power BI Q&A interface and by ADF pipelines. Failure manifests as an unhandled exception that may expose connection string details in logs.

**Remediation priority:** Must be fixed before the financial_qa_agent is enabled in production. Ticket to be raised against the platform engineering backlog with CRITICAL priority.

---

## 4. Key Vault Secret Naming Convention

The FIP Key Vault uses a kebab-case naming convention. All secret names are lowercase with hyphens. **No underscores, no camelCase, no abbreviations.** Secret names must match exactly as documented here; ADF linked services, Databricks secret scope calls, and application code reference these canonical names.

### 4.1 Canonical Secret Name Table

| Canonical Secret Name | Consumer(s) | Purpose | Rotation Policy |
|---|---|---|---|
| `databricks-pat-token` | ADF `ls_databricks`, Databricks jobs | Databricks REST API personal access token for ADF linked service | 90 days |
| `synapse-connection-string` | `anomaly_detector.py`, `commentary_generator.py`, `financial_qa_agent.py`, `financial_forecaster.py` | Full ODBC connection string for Synapse Dedicated SQL Pool | On change |
| `synapse-admin-password` | Break-glass procedure, dbt CI/CD | Synapse SQL admin password (SQL auth fallback) | 90 days |
| `azure-openai-api-key` | `commentary_generator.py`, `financial_forecaster.py` | Azure OpenAI service API key | 180 days |
| `power-automate-alert-url` | `anomaly_detector.py` | Power Automate HTTP webhook URL for alert notifications | On change |
| `nbh-api-url` | Exchange rate ingestion pipeline | National Bank of Hungary FX rate API base URL | Static / on change |

### 4.2 Key Vault Access Pattern

All secrets are retrieved at **runtime**, never at deployment time or baked into Docker images, Databricks notebooks, or ADF ARM templates. The retrieval pattern is:

**From Databricks (Python):**
```python
synapse_conn_str = dbutils.secrets.get(scope="fip-kv", key="synapse-connection-string")
openai_api_key   = dbutils.secrets.get(scope="fip-kv", key="azure-openai-api-key")
```

**From ADF (linked service JSON):**
```json
{
  "type": "AzureKeyVault",
  "store": { "referenceName": "ls_keyvault", "type": "LinkedServiceReference" },
  "secretName": "databricks-pat-token"
}
```

**From Python (azure-identity SDK):**
```python
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential

kv_url = "https://fip-kv-prod.vault.azure.net/"
client = SecretClient(vault_url=kv_url, credential=ManagedIdentityCredential())
secret = client.get_secret("synapse-connection-string")
conn_str = secret.value
```

### 4.3 Key Vault Configuration

| Setting | Value |
|---|---|
| SKU | Premium (HSM-backed keys) |
| Authorization model | Azure RBAC (not legacy access policies) |
| Soft delete retention | 90 days |
| Purge protection | Enabled permanently |
| Network access | Private endpoint only (prod); public access disabled |
| Diagnostic logs | Sent to Log Analytics (AuditEvent category) |

---

## 5. RBAC Model

### 5.1 Key Vault RBAC

| Principal | Role | Scope | Purpose |
|---|---|---|---|
| DBA AAD Group (`fip-dba-group`) | Key Vault Secrets Officer | Key Vault resource | Create, read, update, delete secrets; manage secret versions |
| Platform Service Principal (`fip-platform-sp`) | Key Vault Secrets User | Key Vault resource | Read secrets at runtime (CI/CD pipelines, bootstrap scripts) |
| ADF System Assigned MI | Key Vault Secrets User | Key Vault resource | Read secrets at pipeline runtime |
| Synapse System Assigned MI | Key Vault Secrets User | Key Vault resource | Read secrets at runtime |
| OpenAI System Assigned MI | Key Vault Secrets User | Key Vault resource | Read secrets at runtime |
| Databricks Workspace MI | Key Vault Secrets User | Key Vault resource | Populate secret scope at bootstrap |

**No other principals have Key Vault access.** Developer accounts access Key Vault via the DBA group membership for dev/test environments only. Production Key Vault access for individual users is granted only via a Privileged Identity Management (PIM) eligible assignment with 1-hour max duration and justification required.

### 5.2 ADLS (Storage) RBAC

| Principal | Role | Scope |
|---|---|---|
| Platform Owner account | Storage Blob Data Owner | Storage account |
| ADF SAMI | Storage Blob Data Contributor | Storage account |
| Synapse SAMI | Storage Blob Data Contributor | Storage account |
| Databricks Workspace MI | Storage Blob Data Contributor | Storage account |
| Power BI Service Principal | Storage Blob Data Reader | Gold container only |
| DBA Group | Storage Blob Data Contributor | Dev/test environments |

`allowSharedKeyAccess=false` is enforced on the ADLS account. This means all access must go through Azure AD-based authorization (RBAC or ACL). Shared access signature (SAS) tokens cannot be generated, which prevents key-based data exfiltration. `allowBlobPublicAccess=false` prevents anonymous reads on any container.

### 5.3 Synapse SQL Roles

| Principal | Synapse Role | Granted At |
|---|---|---|
| ADF SAMI | `sysadmin` equivalent via SQL Administrator | Workspace level |
| dbt Service Principal | `db_owner` on `silver`, `gold` databases | Database level |
| Power BI Service Principal | `db_datareader` on `gold` database | Database level |
| Power BI Service Principal | `Synapse Compute Operator` | Workspace level (for serverless SQL) |
| Report Viewer AAD Group | `db_datareader` on `gold` database | Database level |
| DBA Group | `db_owner` on all databases | Database level |

### 5.4 Databricks RBAC

Databricks uses a cluster policy named `fip-job-cluster-policy` that enforces:

- Auto-termination after **30 minutes** of inactivity (no always-on clusters in prod)
- Minimum DBR version: 14.3 LTS
- Allowed node types: `Standard_D4s_v3` (driver), `Standard_D8s_v3` (workers)
- No SSH access
- No cluster-init scripts from external URLs (only approved DBFS paths)

Job clusters are created per-run and destroyed on completion. Interactive clusters are permitted only in dev workspaces with explicit team lead approval.

---

## 6. Row-Level Security Enforcement

### 6.1 Design Overview

The FIP enforces entity-level data isolation at the application layer using a parameterised RLS clause pattern. All user-facing queries (financial QA agent, Power BI datasets, Synapse views) pass through an RLS filter that restricts results to entities the authenticated user is authorized to see.

The authorization mapping is stored in `config.rls_user_entity_map`. This table associates Azure AD user object IDs (or group object IDs) with permitted `entity_code` values.

> **Architecture Gap (MEDIUM finding):** `config.rls_user_entity_map` is referenced in `financial_qa_agent.py` and the RLS documentation, but **no DDL definition exists in any schema file**. The table must be defined and seeded before the QA agent can be deployed. See Section 13.2 for the remediation action.

### 6.2 Proposed DDL for rls_user_entity_map

```sql
CREATE TABLE config.rls_user_entity_map (
    map_id          INT             IDENTITY(1,1) PRIMARY KEY,
    aad_object_id   NVARCHAR(36)    NOT NULL,       -- Azure AD user or group OID
    principal_type  NVARCHAR(10)    NOT NULL        -- 'USER' or 'GROUP'
                    CHECK (principal_type IN ('USER', 'GROUP')),
    entity_code     VARCHAR(20)     NOT NULL,        -- FK to config.ref_entity_master
    access_level    NVARCHAR(20)    NOT NULL        -- 'READ' or 'FULL'
                    CHECK (access_level IN ('READ', 'FULL')),
    granted_by      NVARCHAR(100)   NOT NULL,
    granted_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    expires_at      DATETIME2       NULL,           -- NULL = no expiry
    is_active       BIT             NOT NULL DEFAULT 1,
    CONSTRAINT fk_rls_entity_code
        FOREIGN KEY (entity_code) REFERENCES config.ref_entity_master(entity_code)
);

CREATE INDEX ix_rls_aad_object_id ON config.rls_user_entity_map (aad_object_id)
    WHERE is_active = 1;
```

### 6.3 get_rls_clause() Pattern

`financial_qa_agent.py` implements `get_rls_clause()` which generates a parameterised subquery injected into every user-submitted SQL query:

```python
def get_rls_clause(user_aad_oid: str) -> tuple[str, list]:
    """
    Returns an SQL fragment and parameter list that restricts results
    to entity_codes the given AAD user (or their groups) can access.
    
    Usage: inject into WHERE clause of all user-facing queries.
    NEVER format user OID into SQL directly — always use parameterised query.
    """
    rls_sql = """
        entity_code IN (
            SELECT entity_code
            FROM config.rls_user_entity_map
            WHERE aad_object_id = ?
              AND is_active = 1
              AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
        )
    """
    return rls_sql, [user_aad_oid]
```

The calling code always appends this clause with `AND` to the WHERE predicate of any query that touches entity-scoped data. The user's AAD OID is resolved from the authenticated session token (Azure AD bearer token claims), never from a user-supplied parameter.

### 6.4 RLS in Power BI

Power BI datasets use Synapse SQL views that embed RLS at the view definition layer using `SESSION_CONTEXT()` populated by the Power BI gateway service principal. For embedded reports:

1. The Power BI embedded token is generated with the user's AAD OID in the effective identity.
2. The Synapse view calls `SESSION_CONTEXT(N'user_oid')` and joins against `config.rls_user_entity_map`.
3. Results are filtered server-side before being returned to the Power BI visual layer.

This means RLS is enforced even if a user exports data from a Power BI visual — the export contains only the rows the user's identity is authorized to see.

---

## 7. Network Isolation and Private Connectivity

### 7.1 Private Endpoints (Production)

All production service endpoints are exposed exclusively via Azure Private Endpoints connected to the FIP VNet. Public endpoint access is disabled for all services listed below.

| Service | Private Endpoint Sub-resource | Private DNS Zone |
|---|---|---|
| Azure Key Vault | `vault` | `privatelink.vaultcore.azure.net` |
| ADLS Gen2 | `blob`, `dfs` | `privatelink.blob.core.windows.net`, `privatelink.dfs.core.windows.net` |
| Synapse SQL Pool | `sql` | `privatelink.sql.azuresynapse.net` |
| Synapse Dev endpoint | `dev` | `privatelink.dev.azuresynapse.net` |
| Azure OpenAI | `account` | `privatelink.openai.azure.com` |
| Azure Data Factory | `dataFactory` | `privatelink.datafactory.azure.net` |

Private DNS zones are linked to the FIP VNet and the hub VNet (if hub-spoke topology is used). DNS resolution for all `privatelink.*` zones routes to private IP addresses only.

### 7.2 Databricks VNet Injection

The Databricks workspace is deployed using **VNet injection** rather than a Databricks-managed VNet. This places Databricks driver and worker nodes inside the FIP VNet, giving them:

- Direct access to private endpoints (no public internet traversal for KV, ADLS, Synapse)
- Outbound traffic governed by the FIP NSG and (optionally) Azure Firewall
- No inbound public internet access to cluster nodes

VNet injection requires two dedicated subnets:
- `snet-dbr-public` — driver and gateway nodes (must have `Microsoft.Databricks/workspaces` delegation)
- `snet-dbr-private` — worker nodes

Both subnets have `No Public IP` (NPIP) enabled in the cluster policy, which forces all cluster outbound traffic through a NAT Gateway or Azure Firewall rather than individual public IPs.

### 7.3 Synapse Managed VNet and Data Exfiltration Prevention

Synapse is deployed with a **managed VNet** and `preventDataExfiltration=true`. This setting:

- Prevents Synapse from creating outbound connections to any Azure resource outside the approved tenant
- Requires explicit **managed private endpoint approvals** for every data source Synapse needs to connect to (ADLS, Key Vault, etc.)
- `allowedAadTenantIdsForLinking=[tenantId]` restricts which AAD tenants can be referenced in linked services, preventing cross-tenant data movement

Managed private endpoint approvals must be reviewed and approved by the DBA group or Platform Owner. New approvals trigger an alert in Log Analytics.

### 7.4 Network Security Group Rules

The FIP VNet NSG contains the following relevant rules (in priority order):

| Priority | Name | Direction | Source | Destination | Port | Action |
|---|---|---|---|---|---|---|
| 100 | AllowAzureServicesInbound | Inbound | AzureCloud | VirtualNetwork | 443 | Allow |
| 200 | AllowDatabricksIntraCluster | Inbound | VirtualNetwork | VirtualNetwork | Any | Allow |
| 300 | DenyAllInbound | Inbound | Any | Any | Any | Deny |
| 100 | AllowAzureMonitorOutbound | Outbound | VirtualNetwork | AzureMonitor | 443 | Allow |
| 110 | AllowKeyVaultOutbound | Outbound | VirtualNetwork | AzureKeyVault | 443 | Allow |
| 120 | AllowStorageOutbound | Outbound | VirtualNetwork | Storage | 443 | Allow |
| 130 | AllowSqlOutbound | Outbound | VirtualNetwork | Sql | 1433 | Allow |
| 200 | DenyAllOutbound | Outbound | Any | Any | Any | Deny |

---

## 8. HU GAAP Data Residency and Retention

### 8.1 Region Constraint

All FIP data, compute, and AI services are deployed in the **Azure West Europe** (Amsterdam) region. Data at rest never leaves this region. Cross-region replication (GRS) is disabled on ADLS to enforce this constraint.

Hungarian law (2000/C Act §169) requires that accounting records be available for inspection by Hungarian authorities. Deploying in West Europe (EU) satisfies data residency requirements while keeping latency acceptable for on-premises ERP connections.

### 8.2 Retention Requirements

| Data Category | Minimum Retention (HU GAAP) | FIP Configuration |
|---|---|---|
| Raw GL transactions (bronze) | 8 years (2922 days) | ADLS WORM immutability: 2922 days |
| Processed financial data (silver/gold) | 8 years | Synapse SQL retention + soft-delete policy |
| Audit logs (`audit.system_audit_log`) | 8 years (tamper-evident) | Append-only table; no DELETE granted to any role |
| Commentary (`audit.commentary_queue`) | 8 years | Append-only; superseded rather than deleted |
| Log Analytics (prod) | 365 days (operational) | Configured; raw data archived to ADLS for 8-year retention |
| Log Analytics (dev/CI) | 90 days | Configured |

**Key compliance note:** HU GAAP §169 requires that electronic records be **tamper-evident**. The FIP satisfies this through:
1. WORM immutability on bronze ADLS container
2. `audit.system_audit_log` is append-only (no UPDATE or DELETE permissions granted)
3. ADLS `requireInfrastructureEncryption=true` adds a second layer of AES-256 encryption

### 8.3 Late Entry Protocol

When `bronze.raw_gl_transactions.is_late_entry = TRUE` (posting date > period-end date), the following controls apply:

- **Immaterial late entries**: Processed normally through the pipeline; flagged in `audit.system_audit_log` with `event_type = 'CONFIG_CHANGE'` (journal adjustment category)
- **Material late entries**: Routed to `audit.restatement_log`; CFO notification triggered via Power Automate webhook (`power-automate-alert-url`); period must be formally restated before Gold layer is published

DQ rule **DQ-010** enforces: postings more than 5 days after period-end violate the HU GAAP accrual principle (§15). DQ-010 violations are raised as HIGH-severity anomalies in `audit.anomaly_queue` and block pipeline progression until resolved or waived by CFO.

### 8.4 DQ Rule Coverage for HU GAAP

| Rule | Description | HU GAAP Reference |
|---|---|---|
| DQ-009 | Revenue accounts must not have net debit period-end balance | §70 (revenue recognition) |
| DQ-010 | Postings >5 days after period-end = accrual violation | §15 (accrual principle) |
| DQ-001 | Trial balance debit = credit | §39 (double-entry bookkeeping) |
| DQ-002 | All accounts mapped in COA | §49 (chart of accounts) |
| DQ-003 | No orphaned transactions (no entity match) | §2 (entity completeness) |

---

## 9. GDPR Considerations

### 9.1 Personal Data Inventory

The FIP handles financial data, not personal data in the primary data flows. However, the following components touch personal data (GDPR Art. 4):

| Component | Personal Data Element | Lawful Basis | Retention |
|---|---|---|---|
| `audit.system_audit_log` | `user_id` (AAD UPN or OID) | Legitimate interest (accountability, security) | 8 years (HU GAAP alignment) |
| `audit.commentary_queue` | `generated_by_model` + approver identity | Legitimate interest (AI governance) | 8 years |
| Log Analytics | Azure AD sign-in logs, user activity | Legitimate interest (security monitoring) | 365 days (prod) |

No raw personal data (names, national IDs, contact details) is stored in financial tables. Entity-level data is identified by `entity_code` (a business identifier), not personal identifiers.

### 9.2 Minimal Data Principle in AI Prompts

`commentary_generator.py` sends financial data to Azure OpenAI. The **Variance Fact Pack** (a structured JSON object containing KPI values, period comparisons, and variance percentages) is the only data transmitted to the OpenAI API. The following data is **never** included in LLM prompts:

- Raw GL transaction details (individual debit/credit lines)
- Employee-level data
- Customer or supplier names
- Bank account details
- Any data that could directly identify a natural person

The Variance Fact Pack is assembled in `commentary_generator.py` by querying pre-aggregated `gold.*` views and includes only aggregate financial metrics at the entity/period level.

```python
# Example Variance Fact Pack structure (anonymised at entity_code level)
variance_fact_pack = {
    "entity_code": "HU001",
    "period_key": 202503,          # YYYYMM INT — Gold zone convention
    "revenue_actual": 1_250_000_000,
    "revenue_budget": 1_100_000_000,
    "revenue_variance_pct": 13.6,
    "ebitda_actual": 187_500_000,
    "ebitda_margin_pct": 15.0,
    "cash_balance_lcy": 320_000_000
}
```

### 9.3 system_audit_log — GDPR Accountability

`audit.system_audit_log` records every data access event for GDPR accountability (Art. 5(2) — accountability principle). The log captures:

- `event_type`: One of `DATA_ACCESS`, `REPORT_EXPORT`, `AI_COMMENTARY_PUBLISHED`, `CONFIG_CHANGE`, `USER_LOGIN`, `ALERT_FIRED`
- `user_id`: AAD UPN of the acting user
- `entity_id`: UUID of the financial entity accessed
- `resource_type`: The type of resource (e.g., `gold.fact_pl`, `audit.commentary_queue`)
- `action`: Specific operation performed
- `outcome`: `SUCCESS` or `FAILURE`

**This table must never have rows deleted.** The table has no DELETE or UPDATE permissions granted to any database role. Any request to delete rows from `system_audit_log` must be escalated to the Data Protection Officer (DPO) and requires a formal data subject rights assessment.

### 9.4 Data Subject Rights

In the event of a Subject Access Request (SAR) or erasure request:

1. **Access request**: Query `audit.system_audit_log WHERE user_id = '<aad_upn>'` to enumerate all data access events for the subject.
2. **Erasure request**: Financial accounting data under HU GAAP is exempt from GDPR erasure rights per Recital 65 (legal obligation basis). Audit log entries are exempt per legitimate interest basis. Document the exemption in the DPO's SAR register.
3. **Portability request**: Export relevant `gold.*` views filtered by the user's authorized entities via the standard reporting pipeline.

---

## 10. Audit Logging Coverage

### 10.1 Azure Platform Logs (Log Analytics)

All Azure services send diagnostic logs to the central Log Analytics workspace (`fip-logs-prod`). Retention is **365 days** for prod and **90 days** for dev/CI. A **5 GB daily ingestion cap** is configured to prevent runaway cost; an alert fires at 80% of the cap.

| Service | Log Categories Captured |
|---|---|
| Azure Key Vault | `AuditEvent` (all secret reads, writes, deletes) |
| Azure Data Factory | `ActivityRuns`, `PipelineRuns`, `TriggerRuns`, `SandboxPipelineRuns` |
| Azure Synapse | `SynapseRbacOperations`, `GatewayApiRequests`, `BuiltinSqlReqsEnded`, `SQLSecurityAuditEvents` |
| Azure OpenAI | `RequestResponse`, `Audit` |
| ADLS Gen2 | `StorageRead`, `StorageWrite`, `StorageDelete` |
| Azure AD | `SignInLogs`, `AuditLogs` |
| Databricks | `Cluster`, `Notebook`, `Job`, `Workspace` |

### 10.2 Key KQL Queries for Security Monitoring

**Failed Key Vault secret reads (potential credential probing):**
```kql
AzureDiagnostics
| where ResourceType == "VAULTS"
| where OperationName == "SecretGet"
| where ResultType == "Failed"
| summarize FailedAttempts=count() by CallerIPAddress, bin(TimeGenerated, 1h)
| where FailedAttempts > 5
| order by FailedAttempts desc
```

**Anomaly detector or QA agent login failures (BUG-003 symptom):**
```kql
AppExceptions
| where ExceptionType contains "pyodbc"
| where ExceptionMessage contains "Login failed" or ExceptionMessage contains "Invalid authorization"
| project TimeGenerated, AppRoleName, ExceptionMessage, OperationId
| order by TimeGenerated desc
```

**ADF pipeline failures in last 24 hours:**
```kql
ADFPipelineRun
| where Status == "Failed"
| where TimeGenerated > ago(24h)
| project TimeGenerated, PipelineName, RunId, FailureType, ErrorCode, ErrorMessage
| order by TimeGenerated desc
```

**OpenAI token usage by pipeline (AI cost attribution):**
```kql
AzureDiagnostics
| where ResourceType == "ACCOUNTS" and Category == "RequestResponse"
| extend ModelName = tostring(parse_json(properties_s).model)
| extend PromptTokens = toint(parse_json(properties_s).usage.prompt_tokens)
| extend CompletionTokens = toint(parse_json(properties_s).usage.completion_tokens)
| summarize TotalPromptTokens=sum(PromptTokens), TotalCompletionTokens=sum(CompletionTokens)
    by ModelName, bin(TimeGenerated, 1d)
| order by TimeGenerated desc
```

**Synapse data access audit (HU GAAP accountability):**
```kql
AzureDiagnostics
| where Category == "SQLSecurityAuditEvents"
| where action_name_s in ("SELECT", "INSERT", "UPDATE", "DELETE")
| project TimeGenerated, server_instance_name_s, database_name_s, 
          schema_name_s, object_name_s, statement_s, session_server_principal_name_s
| order by TimeGenerated desc
```

### 10.3 Application-Level Audit (system_audit_log)

Application events that Log Analytics cannot capture (user-level data access decisions, AI commentary publication events, alert firings) are written to `audit.system_audit_log` by the application layer:

```sql
-- Standard INSERT pattern for application audit logging
INSERT INTO audit.system_audit_log (
    event_type,         -- DATA_ACCESS | REPORT_EXPORT | AI_COMMENTARY_PUBLISHED |
                        -- CONFIG_CHANGE | USER_LOGIN | ALERT_FIRED
    user_id,            -- Azure AD UPN (e.g., 'analyst@company.hu')
    entity_id,          -- UUID from config.ref_entity_master
    resource_type,      -- e.g., 'gold.fact_pl', 'audit.commentary_queue'
    action,             -- e.g., 'SELECT', 'APPROVE_COMMENTARY', 'FIRE_ALERT'
    outcome,            -- 'SUCCESS' or 'FAILURE'
    event_at            -- SYSUTCDATETIME()
)
VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME());

-- NEVER: UPDATE audit.system_audit_log
-- NEVER: DELETE FROM audit.system_audit_log
-- This table is write-once append-only (HU GAAP §169 tamper-evidence requirement)
```

---

## 11. SQL Injection Protection

### 11.1 Stored Procedure Input Validation

All Synapse stored procedures that accept user-supplied or pipeline-supplied parameters validate input using two platform functions before using parameters in dynamic SQL:

**`fn_is_valid_uuid(input NVARCHAR(36)) RETURNS BIT`**  
Validates that the input matches the UUID format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` using a regular expression. Returns 1 if valid, 0 otherwise.

**`fn_is_valid_entity_code(input NVARCHAR(50)) RETURNS BIT`**  
Validates that the input matches the `entity_code` pattern (alphanumeric, underscores, hyphens, max 20 characters). Returns 1 if valid, 0 otherwise. Also verifies the code exists in `config.ref_entity_master`.

**Usage pattern in stored procedures:**
```sql
CREATE PROCEDURE gold.usp_refresh_entity_summary
    @entity_code NVARCHAR(50),
    @period_key  INT
AS
BEGIN
    -- Validate inputs before any SQL execution
    IF dbo.fn_is_valid_entity_code(@entity_code) = 0
        THROW 50001, 'Invalid entity_code format or unknown entity.', 1;
    
    IF @period_key < 200001 OR @period_key > 209912
        THROW 50002, 'period_key out of valid range (YYYYMM format required).', 1;
    
    -- Safe to use @entity_code in parameterised query
    SELECT * FROM gold.fact_pl
    WHERE entity_code = @entity_code
      AND period_key  = @period_key;
END;
```

**Known compliance gap — BUG-001:** `usp_dq001`, `usp_dq002`, and `usp_dq003` currently accept `@company_id NVARCHAR(50)` without calling `fn_is_valid_entity_code()`. This is both a functional bug (wrong column name) and a security gap (no input validation). The fix described in Section 13 (BUG-001) also adds the validation call.

### 11.2 SQL Injection Protection in financial_qa_agent.py

`financial_qa_agent.py` allows authenticated users to submit natural-language queries that are translated to SQL by an LLM and executed against Synapse. Multiple layers of protection prevent SQL injection and unauthorized data access:

**Layer 1 — Blocked keyword list (`SQL_BLOCKED_KEYWORDS`)**

The agent maintains a list of SQL keywords that are never permitted in generated queries:

```python
SQL_BLOCKED_KEYWORDS = [
    # DDL — never allowed
    "DROP", "CREATE", "ALTER", "TRUNCATE", "RENAME",
    # DML mutations — read-only agent
    "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT",
    # Privilege escalation
    "GRANT", "REVOKE", "DENY",
    # Dangerous execution
    "EXEC", "EXECUTE", "XP_CMDSHELL", "SP_EXECUTESQL",
    "OPENROWSET", "OPENDATASOURCE", "BULK INSERT",
    # Information schema probing
    "SYS.OBJECTS", "SYS.COLUMNS", "INFORMATION_SCHEMA",
    "SYS.SQL_LOGINS", "SYS.DATABASE_PRINCIPALS",
]

def validate_generated_sql(sql: str) -> bool:
    """
    Returns True if the SQL is safe to execute.
    Called on every LLM-generated query before execution.
    """
    sql_upper = sql.upper().strip()
    
    # Must start with SELECT or WITH (CTEs)
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise ValueError("Only SELECT and WITH queries are permitted.")
    
    # Check for blocked keywords
    for keyword in SQL_BLOCKED_KEYWORDS:
        if keyword in sql_upper:
            raise ValueError(f"Blocked SQL keyword detected: {keyword}")
    
    return True
```

**Layer 2 — Schema allowlist**

The agent only permits queries against an explicit allowlist of schemas and views. Any reference to system schemas, raw bronze tables, or config tables with sensitive data is blocked:

```python
ALLOWED_SCHEMAS = {"gold", "silver"}
ALLOWED_VIEWS = {
    "gold.fact_pl",
    "gold.fact_balance_sheet",
    "gold.agg_balance_sheet",
    "gold.fact_cashflow",
    "gold.dim_period",
    "silver.dim_entity",
    "gold.ai_commentary",
}
```

**Layer 3 — RLS injection**

Every query has the RLS clause appended (see Section 6.3) using parameterised queries. User-supplied values are never formatted directly into SQL strings.

**Layer 4 — Read-only database user**

The financial QA agent connects with a dedicated read-only database principal (`fip_qa_reader`) that has only `db_datareader` on the `gold` and `silver` schemas. Even if all other layers failed, the database user cannot execute DDL or DML.

---

## 12. AI Governance

### 12.1 Commentary Generation Principles

AI-generated financial commentary is produced by `commentary_generator.py` (ADF pipeline **A11**, monthly close step 6). The following governance principles apply without exception:

1. **No auto-publication**: Commentary is never automatically published to Power BI dashboards or distributed to stakeholders. Every generated narrative must pass through the approval workflow.
2. **Human-in-the-loop**: CFO or designate must explicitly approve commentary via the CFO Portal before it is moved to `gold.ai_commentary`.
3. **Audit trail**: Every generated commentary is logged in `audit.commentary_queue` with the model name, generation timestamp, and the Variance Fact Pack used as input.
4. **Prompt logging**: All OpenAI API calls are captured via Azure OpenAI RequestResponse diagnostic settings in Log Analytics, providing a complete prompt-response audit trail.

### 12.2 audit.commentary_queue Approval Workflow

```
commentary_generator.py generates narrative
            │
            ▼
INSERT into audit.commentary_queue
   (approval_status = 'PENDING_REVIEW')
            │
            ▼
CFO Portal polls for PENDING_REVIEW rows
            │
       ┌────┴────┐
       │         │
    APPROVE   REJECT
       │         │
       ▼         ▼
approval_status  approval_status
= 'APPROVED'     = 'REJECTED'
       │         │
       ▼         └─► Re-tried next cycle
INSERT into           (new row inserted)
gold.ai_commentary
+ system_audit_log
  event: AI_COMMENTARY_PUBLISHED
```

If `commentary_generator.py` is re-run for the same `entity_code` + `period_key` combination (e.g., after a restatement), any existing `PENDING_REVIEW` row for that combination is updated to `SUPERSEDED` before the new draft is inserted.

### 12.3 Variance Fact Pack — Minimal Data Design

The Variance Fact Pack sent to OpenAI contains **only pre-aggregated, anonymised financial metrics**. No transaction-level, personal, or customer data is included. This satisfies the GDPR minimal data principle and limits the exposure in the event of an API-side data breach.

The fact pack JSON is stored in `audit.commentary_queue.variance_fact_pack` (JSONB column) alongside the generated commentary, providing full reproducibility for audit — an auditor can re-run the prompt with the stored fact pack to verify the model's output.

### 12.4 Model Version Tracking

`audit.commentary_queue.generated_by_model` records the exact model deployment name and version (e.g., `gpt-4o-2024-08-06`) used for each commentary. This enables:

- Reproducibility audits if a model is updated
- Cost attribution per model version
- Detection of unexpected model changes (should alert if model version changes mid-period)

---

## 13. Known Security Findings

This section documents all open security findings identified during platform audit. Each finding has an assigned severity, a status, and a remediation action.

### 13.1 FINDING-001: SHIR Auth Key Exposure in adf.bicep Output

**Severity:** HIGH  
**Component:** `adf.bicep` (Bicep IaC template)  
**Status:** Open — remediation required before next deployment

**Description:**  
`adf.bicep` contains the following output declaration:

```bicep
// CURRENT — VULNERABLE
output shirAuthKey string = shir.properties.typeProperties.authorizationKey
```

This exposes the Self-Hosted Integration Runtime (SHIR) registration/authorization key in the Azure deployment history. The deployment history is accessible to **any principal with `Microsoft.Resources/deployments/read` on the resource group**, which typically includes all Contributors and Readers. The SHIR auth key grants the ability to register a malicious on-premises IR node to the ADF workspace, enabling data interception from ERP source connections.

**Remediation:**

1. Remove the output from `adf.bicep`:
   ```bicep
   // FIXED — remove the output entirely
   // Do NOT output shirAuthKey
   ```

2. Retrieve the auth key via a separate API call with explicit permissions:
   ```bash
   # Retrieve SHIR auth key — requires Microsoft.DataFactory/factories/integrationruntimes/listAuthKeys/action
   az datafactory integration-runtime list-auth-key \
     --resource-group fip-rg-prod \
     --factory-name fip-adf-prod \
     --name fip-shir-prod
   ```

3. Store the retrieved key in Key Vault immediately and reference it from Key Vault in the SHIR registration script. Do not leave it in shell history.

4. **Immediate action:** Rotate the SHIR auth key now (even before the Bicep fix) to invalidate any keys captured in existing deployment history:
   ```bash
   az datafactory integration-runtime regenerate-auth-key \
     --resource-group fip-rg-prod \
     --factory-name fip-adf-prod \
     --name fip-shir-prod \
     --key-name authKey1
   ```

### 13.2 FINDING-002: Missing rls_user_entity_map DDL

**Severity:** MEDIUM  
**Component:** Schema definitions, `financial_qa_agent.py`  
**Status:** Open — DDL must be created and table seeded before QA agent deployment

**Description:**  
`financial_qa_agent.py` references `config.rls_user_entity_map` in `get_rls_clause()`, but no CREATE TABLE statement for this table exists in any schema definition file. If the QA agent is deployed without this table, every query will fail with a "object not found" error, and more critically, the RLS layer will be non-functional — meaning a fallback code path could return unfiltered results.

**Remediation:**

1. Create `config.rls_user_entity_map` using the DDL in Section 6.2.
2. Add the DDL to the schema migration files managed by dbt or the Synapse SQL deployment pipeline.
3. Seed the table with entity access mappings for all current Power BI/QA agent users before the first production deployment.
4. Implement an integration test that verifies the QA agent returns zero rows for an unknown OID (i.e., the RLS clause correctly filters all results when the user has no mappings).

### 13.3 FINDING-003: MSI Token Injection Missing in financial_qa_agent.py

**Severity:** HIGH  
**Component:** `financial_qa_agent.py`  
**Status:** Open — blocks production deployment of QA agent

**Description:**  
`get_db_connection()` in `financial_qa_agent.py` does not inject the MSI bearer token via `attrs_before={1256: ...}`. This causes all database connections to fail with an authentication error in environments using Azure AD-only auth (production and UAT).

**Full remediation code:** See Section 3.2 above.

**Testing:** After applying the fix, verify with:
```python
# Smoke test — should return a non-empty result
conn = get_db_connection(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT TOP 1 entity_code FROM config.ref_entity_master")
row = cursor.fetchone()
assert row is not None, "Connection succeeded but no entities found"
print(f"Connection test passed. First entity_code: {row[0]}")
```

---

## 14. Security Checklist for Releases

Before any production deployment, the release engineer must verify all items in this checklist. The checklist must be completed and signed off by the Platform Security Lead and the DBA Lead.

### Pre-Deployment Security Checklist

```
INFRASTRUCTURE
[ ] adf.bicep does not output shirAuthKey or any other secrets
[ ] All Key Vault secrets referenced in linked services use canonical names (Section 4.1)
[ ] Private endpoints exist for all production services (Section 7.1)
[ ] Databricks cluster policy enforces 30-min auto-termination
[ ] ADLS allowSharedKeyAccess=false confirmed
[ ] ADLS minimumTlsVersion=TLS1_2 confirmed

IDENTITY AND ACCESS
[ ] ADF, Synapse, OpenAI SAMIs have correct RBAC assignments (Section 5)
[ ] No human user accounts have direct Key Vault Secrets Officer in prod
    (must use PIM eligible assignment)
[ ] config.rls_user_entity_map exists and is seeded
[ ] financial_qa_agent.py uses MSI token injection (BUG-003 remediated)
[ ] fip_qa_reader database user is read-only (db_datareader only)

CODE SECURITY
[ ] No hardcoded secrets in any Python script or Databricks notebook
[ ] All stored procedures call fn_is_valid_entity_code() on entity_code params
[ ] usp_dq001/002/003 updated to use entity_code (BUG-001 remediated)
[ ] financial_qa_agent.py SQL_BLOCKED_KEYWORDS list reviewed and current

DATA COMPLIANCE
[ ] Bronze ADLS WORM immutability = 2922 days confirmed
[ ] audit.system_audit_log has no DELETE or UPDATE permissions on any role
[ ] Log Analytics retention: prod=365d, dev=90d confirmed
[ ] commentary_generator.py Variance Fact Pack reviewed for PII exclusion

AI GOVERNANCE
[ ] audit.commentary_queue has no auto-approval trigger
[ ] OpenAI RequestResponse diagnostics enabled and flowing to Log Analytics
[ ] generated_by_model column populated correctly in commentary_queue INSERT
[ ] CFO Portal access list reviewed and current
```

---

*Document maintained by the FIP Platform Engineering team. Security findings should be reported to the Platform Security Lead and tracked in the platform backlog with appropriate severity classification. This document is reviewed quarterly and after any significant architectural change.*
