# Financial Intelligence Platform (FIP) — Setup and Deployment Guide

> **Version:** 1.0.0 | **Last Updated:** 2026-04-17 | **Applies To:** dev / ci / prod environments

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Preparation](#2-repository-preparation)
3. [Azure Resource Provisioning (Bicep)](#3-azure-resource-provisioning-bicep)
4. [Post-Deployment Configuration](#4-post-deployment-configuration)
5. [Databricks Secret Scope Setup](#5-databricks-secret-scope-setup)
6. [Key Vault Secret Population](#6-key-vault-secret-population)
7. [SQL Schema Deployment](#7-sql-schema-deployment)
8. [dbt Project Initialisation](#8-dbt-project-initialisation)
9. [ADF Pipeline Configuration](#9-adf-pipeline-configuration)
10. [Environment-Specific Overrides](#10-environment-specific-overrides)
11. [Python Module Configuration](#11-python-module-configuration)
12. [Smoke Test Checklist](#12-smoke-test-checklist)
13. [Rollback and Disaster Recovery](#13-rollback-and-disaster-recovery)

---

## 1. Prerequisites

Before beginning deployment, ensure the following tools are installed at the required versions, and that your Azure identity has the required RBAC assignments.

### 1.1 Required Tools

| Tool | Minimum Version | Installation |
|---|---|---|
| **Python** | 3.11+ | https://www.python.org/downloads/ |
| **Azure CLI** | 2.55.0+ | `winget install Microsoft.AzureCLI` or `brew install azure-cli` |
| **Bicep CLI** | 0.24.0+ | `az bicep install` (installed via Azure CLI) |
| **Databricks CLI** | 0.200.0+ | `pip install databricks-cli` |
| **dbt-core** | Latest stable | `pip install dbt-core dbt-synapse` |
| **sqlcmd** | 17.x or 18.x | https://aka.ms/sqlcmd-download |
| **Git** | 2.40.0+ | https://git-scm.com/ |

### 1.2 Python Dependencies

Install all Python module dependencies from the project requirements file:

```bash
pip install -r python/requirements.txt
```

Key packages and pinned versions:

```
azure-identity==1.17.0
azure-keyvault-secrets==4.8.0
openai==1.30.0
pyodbc==5.1.0
pandas==2.2.0
prophet==1.1.5
scikit-learn==1.5.0
scipy==1.13.0
statsmodels==0.14.0
fastapi
pydantic==2.7.0
```

### 1.3 Azure RBAC Requirements

The deploying identity (Service Principal or user account) requires the following role assignments **before** running Bicep deployments:

| Scope | Role | Purpose |
|---|---|---|
| Subscription or Resource Group | `Contributor` | Create and manage all Azure resources |
| Subscription or Resource Group | `User Access Administrator` | Assign RBAC roles to managed identities |
| Azure AD | `Application Administrator` | Register service principals (if not using existing SP) |

### 1.4 Authentication

```bash
# Login to Azure CLI
az login

# Set target subscription
az account set --subscription "<SUBSCRIPTION_ID>"

# Verify context
az account show
```

---

## 2. Repository Preparation

### 2.1 Clone and Configure

```bash
git clone https://your-org@dev.azure.com/your-org/fip/_git/fip
cd fip

# Copy environment parameter template
cp infrastructure/bicep/parameters/dev.bicepparam.example \
   infrastructure/bicep/parameters/dev.bicepparam
```

### 2.2 Parameter Files

Each environment has a corresponding `.bicepparam` file. Edit the values for your target environment:

```bicep
// infrastructure/bicep/parameters/dev.bicepparam
using '../main.bicep'

param projectCode = 'fip'
param environment = 'dev'
param location = 'westeurope'
param enablePrivateEndpoints = false
param synapseSkuName = 'DW100c'
param synapseAutoPauseEnabled = true
param logAnalyticsRetentionDays = 30
param deployOpenAI = true
```

```bicep
// infrastructure/bicep/parameters/prod.bicepparam
using '../main.bicep'

param projectCode = 'fip'
param environment = 'prod'
param location = 'westeurope'
param enablePrivateEndpoints = true
param synapseSkuName = 'DW1000c'
param synapseAutoPauseEnabled = false
param logAnalyticsRetentionDays = 365
param deployOpenAI = true
```

```bicep
// infrastructure/bicep/parameters/ci.bicepparam
using '../main.bicep'

param projectCode = 'fip'
param environment = 'ci'
param location = 'westeurope'
param enablePrivateEndpoints = false
param synapseSkuName = 'DW100c'
param synapseAutoPauseEnabled = true
param logAnalyticsRetentionDays = 7
param deployOpenAI = false   // CI does not provision OpenAI
```

---

## 3. Azure Resource Provisioning (Bicep)

Resources must be deployed in a specific order due to dependency chains. The following sequence is **mandatory**. Do not attempt to deploy all modules simultaneously.

### Stage 1 — Key Vault (must be first)

Key Vault must exist before any other module that references secrets. Deploy it independently:

```bash
az deployment group create \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --template-file infrastructure/bicep/modules/keyvault.bicep \
  --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
  --name "deploy-keyvault-$(date +%Y%m%d%H%M%S)"
```

After deployment, capture the Key Vault resource ID and URI for use in subsequent steps:

```bash
KV_RESOURCE_ID=$(az keyvault show \
  --name "fip-${ENVIRONMENT}-kv" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --query id --output tsv)

KV_URI=$(az keyvault show \
  --name "fip-${ENVIRONMENT}-kv" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --query properties.vaultUri --output tsv)

echo "KV Resource ID: ${KV_RESOURCE_ID}"
echo "KV URI: ${KV_URI}"
```

### Stage 2 — Log Analytics

Log Analytics must be deployed before Databricks, Synapse, and ADF so that diagnostic settings can reference the workspace:

```bash
az deployment group create \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --template-file infrastructure/bicep/modules/loganalytics.bicep \
  --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
  --name "deploy-loganalytics-$(date +%Y%m%d%H%M%S)"
```

### Stage 3 — ADLS Gen2, ADF, and Databricks (parallel)

These three modules have no dependencies on each other and can be deployed in parallel:

```bash
# Deploy ADLS Gen2
az deployment group create \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --template-file infrastructure/bicep/modules/storage.bicep \
  --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
  --name "deploy-storage-$(date +%Y%m%d%H%M%S)" &

# Deploy Azure Data Factory
az deployment group create \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --template-file infrastructure/bicep/modules/adf.bicep \
  --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
  --name "deploy-adf-$(date +%Y%m%d%H%M%S)" &

# Deploy Databricks
az deployment group create \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --template-file infrastructure/bicep/modules/databricks.bicep \
  --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
  --name "deploy-databricks-$(date +%Y%m%d%H%M%S)" &

# Wait for all three to complete
wait
echo "Stage 3 deployments complete"
```

#### ADLS Container Verification

After storage deployment, verify all five containers exist and that the Bronze immutability policy is configured:

```bash
STORAGE_ACCOUNT=$(az storage account list \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --query "[?starts_with(name, 'fip${ENVIRONMENT}')].name" \
  --output tsv)

# Verify containers
for CONTAINER in bronze silver gold config audit; do
  az storage container show \
    --account-name "${STORAGE_ACCOUNT}" \
    --name "${CONTAINER}" \
    --auth-mode login \
    --query "name" --output tsv
done

# Verify Bronze immutability policy (prod only)
az storage container immutability-policy show \
  --account-name "${STORAGE_ACCOUNT}" \
  --container-name "bronze"
# Expected: immutabilityPeriodSinceCreationInDays = 2922
```

### Stage 4 — Synapse Analytics

Synapse depends on ADLS Gen2 (managed identity data lake access) and Key Vault (for admin password):

```bash
az deployment group create \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --template-file infrastructure/bicep/modules/synapse.bicep \
  --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
  --name "deploy-synapse-$(date +%Y%m%d%H%M%S)"
```

After deployment, resume the dedicated SQL pool if it is paused (dev/ci):

```bash
az synapse sql pool resume \
  --workspace-name "fip-${ENVIRONMENT}-synapse" \
  --name "fip_dw" \
  --resource-group "rg-fip-${ENVIRONMENT}"
```

### Stage 5 — Azure OpenAI

Deploy last; OpenAI is only required in dev and prod (not ci):

```bash
if [ "${ENVIRONMENT}" != "ci" ]; then
  az deployment group create \
    --resource-group "rg-fip-${ENVIRONMENT}" \
    --template-file infrastructure/bicep/modules/openai.bicep \
    --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
    --name "deploy-openai-$(date +%Y%m%d%H%M%S)"
fi
```

### Full Orchestrated Deployment (using main.bicep)

For environments where all modules must be deployed together (e.g., a fresh prod environment), `main.bicep` orchestrates all 6 modules with the correct `dependsOn` chain:

```bash
az deployment group create \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --template-file infrastructure/bicep/main.bicep \
  --parameters infrastructure/bicep/parameters/${ENVIRONMENT}.bicepparam \
  --name "deploy-fip-full-$(date +%Y%m%d%H%M%S)" \
  --verbose
```

> **Note:** `main.bicep` provisions 6 modules (keyvault, loganalytics, storage, adf, databricks, synapse, openai). The `openai` module in `main.bicep` is conditional on the `deployOpenAI` parameter.

---

## 4. Post-Deployment Configuration

### 4.1 Grant ADF Managed Identity Access to ADLS

```bash
ADF_MI_OBJECT_ID=$(az datafactory show \
  --name "fip-${ENVIRONMENT}-adf" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --query identity.principalId --output tsv)

az role assignment create \
  --assignee "${ADF_MI_OBJECT_ID}" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/rg-fip-${ENVIRONMENT}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}"
```

### 4.2 Grant Synapse Managed Identity Access to ADLS

```bash
SYNAPSE_MI_OBJECT_ID=$(az synapse workspace show \
  --name "fip-${ENVIRONMENT}-synapse" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --query identity.principalId --output tsv)

az role assignment create \
  --assignee "${SYNAPSE_MI_OBJECT_ID}" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/rg-fip-${ENVIRONMENT}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}"
```

### 4.3 Grant Key Vault Access to ADF and Synapse Managed Identities

```bash
az keyvault set-policy \
  --name "fip-${ENVIRONMENT}-kv" \
  --object-id "${ADF_MI_OBJECT_ID}" \
  --secret-permissions get list

az keyvault set-policy \
  --name "fip-${ENVIRONMENT}-kv" \
  --object-id "${SYNAPSE_MI_OBJECT_ID}" \
  --secret-permissions get list
```

---

## 5. Databricks Secret Scope Setup

The Databricks secret scope `fip-kv` must be backed by Azure Key Vault. This allows Databricks notebooks and Python workloads to retrieve secrets without storing them in Databricks configuration.

### 5.1 Configure Databricks CLI

```bash
# Get Databricks workspace URL
DATABRICKS_HOST=$(az databricks workspace show \
  --name "fip-${ENVIRONMENT}-dbr" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --query workspaceUrl --output tsv)

databricks configure --token
# Enter host: https://${DATABRICKS_HOST}
# Enter token: <your-databricks-personal-access-token>
```

### 5.2 Create the Key Vault-Backed Secret Scope

```bash
databricks secrets create-scope \
  --scope fip-kv \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "${KV_RESOURCE_ID}" \
  --dns-name "${KV_URI}"
```

> **Important:** The scope name `fip-kv` is the canonical name used across all Databricks notebooks and Python modules. Do not use a different name.

### 5.3 Verify the Secret Scope

```bash
# List scopes — should show fip-kv
databricks secrets list-scopes

# List secrets within the scope
databricks secrets list --scope fip-kv
```

### 5.4 Reference Secrets in Databricks Notebooks

Within Databricks notebooks, secrets are accessed via:

```python
synapse_conn = dbutils.secrets.get(scope="fip-kv", key="synapse-connection-string")
dbr_pat = dbutils.secrets.get(scope="fip-kv", key="databricks-pat-token")
```

---

## 6. Key Vault Secret Population

The following secrets must be manually populated in Azure Key Vault after deployment. Bicep creates the Key Vault but does not populate sensitive values.

### 6.1 Required Secrets

| Secret Name | Description | Who Creates It |
|---|---|---|
| `synapse-admin-password` | Synapse SQL dedicated pool admin password | DBA / Platform team |
| `synapse-connection-string` | Full ODBC/JDBC connection string for Synapse | Platform team |
| `databricks-pat-token` | Databricks Personal Access Token (used by ADF `ls_databricks`) | Databricks admin |
| `azure-openai-api-key` | Azure OpenAI API key | Platform team |
| `power-automate-alert-url` | Power Automate webhook URL for anomaly/alert notifications | Integration team |
| `power-automate-cfo-notify-url` | Power Automate webhook URL for CFO notifications | Integration team |
| `adf-sp-client-secret` | ADF Service Principal client secret (for Synapse linked service) | Platform team |
| `nbh-api-url` | National Bank of Hungary FX rate API URL | Platform team |

> **Note on naming:** The Bicep `main.bicep` initialises a placeholder secret named `databricks-token` during infrastructure setup. The **canonical runtime secret name** used by the ADF linked service `ls_databricks` is **`databricks-pat-token`**. Ensure the PAT is stored under the `databricks-pat-token` key name.

### 6.2 Populate Secrets via Azure CLI

```bash
# Synapse admin password
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "synapse-admin-password" \
  --value "<STRONG_PASSWORD>"

# Synapse connection string
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "synapse-connection-string" \
  --value "Server=tcp:fip-${ENVIRONMENT}-synapse.sql.azuresynapse.net,1433;Database=fip_dw;Authentication=ActiveDirectoryPassword;..."

# Databricks PAT — canonical name used by ADF ls_databricks
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "databricks-pat-token" \
  --value "<DATABRICKS_PAT>"

# Azure OpenAI API key
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "azure-openai-api-key" \
  --value "<OPENAI_API_KEY>"

# Power Automate alert webhook
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "power-automate-alert-url" \
  --value "<POWER_AUTOMATE_WEBHOOK_URL>"

# Power Automate CFO notification webhook
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "power-automate-cfo-notify-url" \
  --value "<CFO_WEBHOOK_URL>"

# ADF Service Principal client secret
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "adf-sp-client-secret" \
  --value "<SP_CLIENT_SECRET>"

# NBH API URL
az keyvault secret set \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "nbh-api-url" \
  --value "https://api.mnb.hu/arfolyam/..."
```

### 6.3 Verify Secret Accessibility

```bash
# Test retrieval (requires Key Vault Secret User role on your identity)
az keyvault secret show \
  --vault-name "fip-${ENVIRONMENT}-kv" \
  --name "databricks-pat-token" \
  --query "value" --output tsv
```

---

## 7. SQL Schema Deployment

SQL schemas must be deployed in strict execution order. Deploying out of order will cause FK constraint failures and missing object errors.

### 7.1 Mandatory Execution Order

```
1. config       →   Reference data, master data, seeded lookup tables
2. audit        →   Batch logging, data quality, quarantine, alert engine
3. bronze       →   Raw ingestion manifest
4. silver       →   Dimensional model (dims + facts)
5. budget       →   Budget versions, fact_budget, fact_forecast
6. gold         →   Business-ready aggregates and KPI views
7. stored_procedures  →  audit.proc_evaluate_alerts and supporting objects
```

### 7.2 Pre-Deployment: Synapse Connection Test

```bash
sqlcmd \
  -S "fip-${ENVIRONMENT}-synapse.sql.azuresynapse.net" \
  -d "fip_dw" \
  -U "sqladmin" \
  -P "${SYNAPSE_ADMIN_PASSWORD}" \
  -Q "SELECT @@VERSION"
```

### 7.3 Deploy Each Schema

```bash
#!/bin/bash
SYNAPSE_SERVER="fip-${ENVIRONMENT}-synapse.sql.azuresynapse.net"
DB="fip_dw"
USER="sqladmin"
PASS="${SYNAPSE_ADMIN_PASSWORD}"

SCHEMAS=(
  "01_config"
  "02_audit"
  "03_bronze"
  "04_silver"
  "05_budget"
  "06_gold"
)

for SCHEMA_DIR in "${SCHEMAS[@]}"; do
  echo "=== Deploying schema: ${SCHEMA_DIR} ==="
  for SQL_FILE in sql/${SCHEMA_DIR}/*.sql; do
    echo "  Executing: ${SQL_FILE}"
    sqlcmd \
      -S "${SYNAPSE_SERVER}" \
      -d "${DB}" \
      -U "${USER}" \
      -P "${PASS}" \
      -i "${SQL_FILE}" \
      -b  # Exit on error
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed on ${SQL_FILE}. Aborting."
      exit 1
    fi
  done
  echo "=== Schema ${SCHEMA_DIR} deployed successfully ==="
done

# Deploy stored procedures last
echo "=== Deploying stored procedures ==="
sqlcmd -S "${SYNAPSE_SERVER}" -d "${DB}" -U "${USER}" -P "${PASS}" \
  -i sql/02_audit/proc_evaluate_alerts.sql -b

echo "=== All schemas deployed successfully ==="
```

### 7.4 Verify Schema Deployment

```sql
-- Run in Synapse dedicated pool to verify all schemas exist
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('config', 'audit', 'bronze', 'silver', 'budget', 'gold')
ORDER BY schema_name;

-- Verify config seeding (currencies)
SELECT currency_code FROM config.ref_currencies ORDER BY currency_code;
-- Expected: CHF, CZK, EUR, GBP, HUF, PLN, RON, RSD, USD  (9 rows)

-- Verify alert rules seeding
SELECT COUNT(*) AS rule_count FROM config.ref_alert_rules;
-- Expected: 10

-- Verify dim_date population
SELECT MIN(date_key), MAX(date_key), COUNT(*) FROM silver.dim_date;
-- Expected: 20150101, 20351231, approximately 7671 rows

-- Verify balance_check generated column exists
SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT
FROM information_schema.columns
WHERE table_schema = 'gold'
  AND table_name = 'agg_balance_sheet'
  AND column_name = 'balance_check';
```

### 7.5 Important Schema Notes

- `silver.account_master` contains a column named `company_id` (FK). This is the **only** column in the silver or gold schema that uses the name `company_id`. All other entity references use `entity_code` (gold, denormalised) or `entity_key` (silver, surrogate FK).
- `silver.dim_entity` uses `entity_key INT IDENTITY` as PK and `entity_code VARCHAR(20)` as the business key.
- `gold.agg_pl_monthly` uses `period_key INT` as PK (YYYYMM format).
- `gold.agg_variance_analysis` uses `BIGSERIAL` (or equivalent `BIGINT IDENTITY`) as PK because `cost_centre_key` is nullable, preventing it from being used as a natural key.
- `gold.agg_balance_sheet` contains `balance_check` as a **generated column** (not a stored column). It equals `assets - liabilities` and must always equal zero for a balanced balance sheet.

---

## 8. dbt Project Initialisation

dbt is used to orchestrate the Bronze → Silver → Gold transformation pipeline. The dbt project runs on Databricks compute and targets the Synapse Analytics dedicated pool.

### 8.1 Install dbt

```bash
pip install dbt-core dbt-synapse
dbt --version
```

### 8.2 Configure dbt Profiles

Create or edit `~/.dbt/profiles.yml`:

```yaml
fip:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: synapse
      driver: 'ODBC Driver 18 for SQL Server'
      server: fip-dev-synapse.sql.azuresynapse.net
      port: 1433
      database: fip_dw
      schema: silver
      authentication: ActiveDirectoryPassword
      user: "{{ env_var('SYNAPSE_USER') }}"
      password: "{{ env_var('SYNAPSE_PASSWORD') }}"
      connect_timeout: 30

    prod:
      type: synapse
      driver: 'ODBC Driver 18 for SQL Server'
      server: fip-prod-synapse.sql.azuresynapse.net
      port: 1433
      database: fip_dw
      schema: silver
      authentication: ActiveDirectoryMSI
      connect_timeout: 30
```

### 8.3 Initialise the Project

```bash
cd dbt/

# Test connection
dbt debug --target dev

# Install dbt packages
dbt deps

# Run Silver dimension models
dbt run --models silver.dims --target dev

# Run Silver fact models (depends on dims)
dbt run --models silver.facts --target dev

# Run Gold aggregate models (depends on silver facts)
dbt run --models gold.aggregates --target dev

# Run Gold KPI views
dbt run --models gold.kpis --target dev

# Run all tests
dbt test --target dev

# Generate and serve documentation
dbt docs generate --target dev
dbt docs serve --port 8080
```

### 8.4 dbt Model Execution Order

```
bronze (source data in Synapse) ─┐
                                  ├─→ silver.dim_date
                                  ├─→ silver.dim_entity        (entity_key, entity_code)
                                  ├─→ silver.dim_account       (entity_id FK)
                                  ├─→ silver.dim_cost_centre
                                  ├─→ silver.dim_currency
                                  ├─→ silver.dim_project
                                  └─→ silver.fact_gl_transaction (period_id YYYYMM)
                                          │
                                          ├─→ gold.fact_gl_transaction  (entity_code inline)
                                          ├─→ gold.agg_pl_monthly       (period_key INT PK)
                                          ├─→ gold.agg_balance_sheet    (balance_check gen col)
                                          ├─→ gold.agg_cashflow         (indirect method)
                                          └─→ gold.agg_variance_analysis (BIGSERIAL PK)
                                                  │
                                                  ├─→ gold.kpi_profitability
                                                  ├─→ gold.kpi_liquidity
                                                  ├─→ gold.kpi_cashflow
                                                  └─→ gold.kpi_project
```

---

## 9. ADF Pipeline Configuration

### 9.1 Linked Service Configuration

After ADF is deployed via Bicep, verify all linked services are configured and connected.

#### `ls_databricks` — Critical Secret Name

The Databricks linked service must be configured to read the PAT token from Key Vault using the exact secret name `databricks-pat-token`:

```json
{
  "name": "ls_databricks",
  "type": "AzureDatabricks",
  "typeProperties": {
    "domain": "https://fip-${environment}-dbr.azuredatabricks.net",
    "accessToken": {
      "type": "AzureKeyVaultSecret",
      "store": {
        "referenceName": "ls_azure_key_vault",
        "type": "LinkedServiceReference"
      },
      "secretName": "databricks-pat-token"
    }
  }
}
```

> **Critical:** The secret name must be exactly `databricks-pat-token`. Any other name (such as `databricks-token`) will cause authentication failures in all Databricks-linked ADF activities.

#### All Linked Services

| Linked Service | Connection Type | Auth Method | Key Secret |
|---|---|---|---|
| `ls_adls_gen2` | ADLS Gen2 | Managed Identity | — |
| `ls_synapse_sql_pool` | Azure Synapse | Service Principal | `adf-sp-client-secret` |
| `ls_databricks` | Azure Databricks | PAT from Key Vault | `databricks-pat-token` |
| `ls_azure_key_vault` | Azure Key Vault | Managed Identity | — |
| `ls_azure_openai` | REST | API Key from Key Vault | `azure-openai-api-key` |
| `ls_sftp_kulcssoft` | SFTP | Credentials in KV | (KV-managed) |
| `ls_rest_cobalt` | REST | OAuth in KV | (KV-managed) |
| `ls_rest_szamlazz` | REST | API Key in KV | (KV-managed) |
| `ls_rest_sap_b1` | REST (SAP B1 SL) | Credentials in KV | (KV-managed) |

### 9.2 Test All Linked Service Connections

In the ADF Studio (Author blade → Manage → Linked services), click **Test connection** for each linked service listed above. All connections must show "Connection successful" before proceeding.

### 9.3 Pipeline Trigger Activation

Activate triggers in the following order:

```bash
# 1. Activate blob-event trigger for ERP extract (must be first — connects to ADLS events)
az datafactory trigger start \
  --factory-name "fip-${ENVIRONMENT}-adf" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --trigger-name "trigger_erp_extract_blob_event"

# 2. Activate daily FX rate load trigger (09:00 CET)
az datafactory trigger start \
  --factory-name "fip-${ENVIRONMENT}-adf" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --trigger-name "trigger_nbh_fx_rate_daily"

# 3. Activate monthly close pipeline trigger (scheduled, T+0 to T+7h window)
az datafactory trigger start \
  --factory-name "fip-${ENVIRONMENT}-adf" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --trigger-name "trigger_monthly_close"
```

### 9.4 Pipeline Schedule Reference

| Pipeline | Trigger Type | Schedule | Notes |
|---|---|---|---|
| `pl_monthly_close` | Scheduled | Business day 1 of each month | T+0 start, 7-hour window (T+0 to T+7h) |
| `pl_nbh_fx_rate_load` | Scheduled | Daily 09:00 CET | Loads NBH FX rates into `config.ref_fx_rates` |
| `pl_erp_extract` | Blob event | On file arrival in bronze container | Event-driven; triggers on new ERP source files |
| A11 (commentary) | Depends on `pl_monthly_close` | Post-close (T+6h) | Runs `commentary_generator.py` via Databricks activity |
| A12 (anomaly) | Depends on `pl_monthly_close` | Post-close (T+5h) | Runs `anomaly_detector.py` via Databricks activity |

---

## 10. Environment-Specific Overrides

### 10.1 Dev Environment

- Synapse pool SKU: **DW100c** with auto-pause after 60 minutes of inactivity.
- No private endpoints — all services accessible over public internet (secured by firewall rules).
- Log Analytics retention: **30 days**.
- Bronze immutability policy: **not enforced** (allows test data to be deleted).
- Alerting webhooks: **disabled** — Power Automate URLs may point to test flows that log but do not notify.
- OpenAI: shared dev instance at reduced TPM limits.

```bash
# Dev-specific: enable Synapse auto-pause
az synapse sql pool update \
  --workspace-name "fip-dev-synapse" \
  --name "fip_dw" \
  --resource-group "rg-fip-dev" \
  --auto-pause-delay 60
```

### 10.2 CI Environment

- Ephemeral — created on PR open, destroyed on PR merge/close.
- Synapse SKU: **DW100c**, auto-pause 15 minutes.
- OpenAI: **not deployed**. CI tests must mock all OpenAI calls.
- Log Analytics retention: **7 days**.
- Use fixture data only — never connect to live source systems.
- CI pipeline should run `dbt test` and SQL schema validation scripts.

```bash
# CI teardown — run at end of CI pipeline
az group delete \
  --name "rg-fip-ci" \
  --yes \
  --no-wait
```

### 10.3 Prod Environment

- Synapse SKU: **DW1000c**, auto-pause **disabled** (always-on for DirectQuery performance).
- Private endpoints enabled for: Key Vault, ADLS Gen2, Synapse, Databricks, OpenAI.
- Log Analytics retention: **365 days**.
- Bronze immutability policy: **2922 days (8 years)** — enforced and locked.
- All Power Automate webhooks active.
- Alerting: `audit.proc_evaluate_alerts()` runs as part of `pl_monthly_close`.
- VNet injection for Databricks Premium cluster.

```bash
# Prod: verify private endpoint connectivity
az network private-endpoint list \
  --resource-group "rg-fip-prod" \
  --query "[].{name:name, provisioningState:provisioningState}" \
  --output table
```

---

## 11. Python Module Configuration

### 11.1 Environment Variables

Each Python module requires the following environment variables. Set these in the execution environment (Databricks cluster environment variables, ADF activity settings, or local `.env` for development).

#### `anomaly_detector.py`

```bash
export AZURE_KEY_VAULT_URL="https://fip-${ENVIRONMENT}-kv.vault.azure.net/"
export SYNAPSE_SERVER="fip-${ENVIRONMENT}-synapse.sql.azuresynapse.net"
export SYNAPSE_DATABASE="fip_dw"
export POWER_AUTOMATE_ALERT_URL="$(az keyvault secret show \
  --vault-name fip-${ENVIRONMENT}-kv \
  --name power-automate-alert-url \
  --query value -o tsv)"
```

Run:

```bash
python python/anomaly_detector.py \
  --company_id "HU001" \
  --period_key 202601
```

#### `commentary_generator.py`

```bash
export AZURE_OPENAI_ENDPOINT="https://fip-${ENVIRONMENT}-openai.openai.azure.com/"
export AZURE_OPENAI_DEPLOYMENT="gpt-4o"
export SYNAPSE_SERVER="fip-${ENVIRONMENT}-synapse.sql.azuresynapse.net"
export SYNAPSE_DATABASE="fip_dw"
```

Run:

```bash
python python/commentary_generator.py \
  --company_id "HU001" \
  --period_key 202601 \
  --roles "CFO,Controller" \
  --languages "hu,en"
```

#### `financial_forecaster.py`

```bash
# financial_forecaster.py retrieves synapse-connection-string from Key Vault directly
export AZURE_KEY_VAULT_URL="https://fip-${ENVIRONMENT}-kv.vault.azure.net/"
```

Run:

```bash
python python/financial_forecaster.py \
  --company_id "HU001" \
  --forecast_months 12 \
  --base_period_key 202512
```

> **Model selection logic:** If ≥12 months of history are available, Prophet is used as the primary model. If only ≥6 months are available, ARIMA fallback is used. Fewer than 6 months of history will cause the module to abort with an error.

#### `financial_qa_agent.py` (FastAPI)

```bash
export AZURE_KEY_VAULT_URL="https://fip-${ENVIRONMENT}-kv.vault.azure.net/"
export AZURE_OPENAI_ENDPOINT="https://fip-${ENVIRONMENT}-openai.openai.azure.com/"
export AZURE_OPENAI_DEPLOYMENT="gpt-4o"
export SYNAPSE_SERVER="fip-${ENVIRONMENT}-synapse.sql.azuresynapse.net"
export SYNAPSE_DATABASE="fip_dw"
export AZURE_COGNITIVE_SEARCH_ENDPOINT="https://fip-${ENVIRONMENT}-search.search.windows.net"
export AZURE_COGNITIVE_SEARCH_INDEX="fip-schema-index"

# Start FastAPI server
uvicorn python.financial_qa_agent:app --host 0.0.0.0 --port 8000

# Health check
curl http://localhost:8000/health

# Example query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What was the EBITDA margin for HU001 in January 2026?"}'
```

---

## 12. Smoke Test Checklist

Run the following checks after each deployment to verify end-to-end system health. Each check should be performed in order; a failure at any stage should be investigated before proceeding.

### Layer 1: Infrastructure

- [ ] All 6 Azure resource modules show `Succeeded` in Azure Portal deployment history
- [ ] All 5 ADLS containers (bronze, silver, gold, config, audit) exist
- [ ] Bronze container immutability policy = 2922 days (prod only)
- [ ] All 9 ADF linked services show "Connection successful"
- [ ] `ls_databricks` connects successfully using Key Vault secret `databricks-pat-token`
- [ ] Databricks secret scope `fip-kv` lists all required secrets
- [ ] Synapse dedicated pool `fip_dw` is online

### Layer 2: Config Schema

```sql
-- All ref tables seeded
SELECT 'ref_currencies' AS tbl, COUNT(*) AS cnt FROM config.ref_currencies
UNION ALL SELECT 'ref_alert_rules', COUNT(*) FROM config.ref_alert_rules
UNION ALL SELECT 'ref_hu_public_holidays', COUNT(*) FROM config.ref_hu_public_holidays;
-- Expected: currencies=9, alert_rules=10, holidays=rows for 2024-2030
```

- [ ] `config.ref_currencies` has 9 rows (HUF, EUR, USD, GBP, CHF, CZK, PLN, RON, RSD)
- [ ] `config.ref_alert_rules` has 10 rows
- [ ] `config.v_mapping_coverage` view is selectable without error

### Layer 3: Audit Schema

```sql
-- Verify audit objects exist
SELECT OBJECT_NAME(object_id) AS obj_name, type_desc
FROM sys.objects
WHERE schema_id = SCHEMA_ID('audit')
ORDER BY type_desc, obj_name;
```

- [ ] `audit.batch_log` table exists and is empty (or has test rows)
- [ ] `audit.proc_evaluate_alerts` procedure compiles without error
- [ ] `audit.fn_is_valid_period_id` function returns expected results:

```sql
SELECT audit.fn_is_valid_period_id(202601);  -- Expected: 1 (valid)
SELECT audit.fn_is_valid_period_id(202613);  -- Expected: 0 (invalid month)
SELECT audit.fn_is_valid_period_id(99999);   -- Expected: 0 (invalid)
```

### Layer 4: Silver Schema

```sql
SELECT MIN(date_key) AS min_dt, MAX(date_key) AS max_dt, COUNT(*) AS row_count
FROM silver.dim_date;
-- Expected: 20150101 / 20351231 / ~7671 rows
```

- [ ] `silver.dim_date` spans 2015-01-01 to 2035-12-31
- [ ] `silver.dim_entity` table exists (entity_key IDENTITY, entity_code VARCHAR(20))
- [ ] `silver.fact_gl_transaction` table exists with period_id column (YYYYMM INT)

### Layer 5: Gold Schema

```sql
-- Verify balance_check generated column
SELECT TOP 5 period_key, balance_check
FROM gold.agg_balance_sheet
ORDER BY period_key DESC;
-- balance_check should equal 0 for all rows if data exists

-- Verify period_key INT type on agg_pl_monthly
SELECT COLUMN_NAME, DATA_TYPE
FROM information_schema.columns
WHERE table_schema = 'gold'
  AND table_name = 'agg_pl_monthly'
  AND column_name = 'period_key';
-- Expected: period_key / int
```

- [ ] `gold.agg_balance_sheet.balance_check` generated column exists
- [ ] `gold.agg_pl_monthly` uses `period_key INT` as PK
- [ ] All four KPI views (`kpi_profitability`, `kpi_liquidity`, `kpi_cashflow`, `kpi_project`) are selectable

### Layer 6: dbt

```bash
dbt debug --target dev
dbt run --models silver --target dev --full-refresh
dbt test --models silver --target dev
```

- [ ] `dbt debug` shows all connections healthy
- [ ] `dbt run` completes with 0 errors
- [ ] `dbt test` passes all schema tests

### Layer 7: ADF Pipeline

```bash
# Trigger a manual run of the FX rate pipeline
az datafactory pipeline create-run \
  --factory-name "fip-${ENVIRONMENT}-adf" \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --name "pl_nbh_fx_rate_load"
```

- [ ] `pl_nbh_fx_rate_load` manual run completes successfully
- [ ] `config.ref_fx_rates` is populated with today's FX rates
- [ ] `audit.batch_log` records the pipeline run

### Layer 8: Python Modules

```bash
# Anomaly detector health check (dry run with test period)
python python/anomaly_detector.py --company_id "HU001" --period_key 202601

# FastAPI health endpoint
curl http://localhost:8000/health
# Expected: {"status": "healthy", "version": "..."}
```

- [ ] `anomaly_detector.py` runs without credential errors
- [ ] `financial_qa_agent.py` `/health` endpoint returns HTTP 200
- [ ] Azure Cognitive Search index `fip-schema-index` is queryable

---

## 13. Rollback and Disaster Recovery

### Schema Rollback

If a SQL schema deployment fails partway through:

1. Identify the last successfully executed script from `audit.batch_log`.
2. Drop any partially created objects using `DROP TABLE IF EXISTS` / `DROP VIEW IF EXISTS`.
3. Re-run from the last known-good script.

### Bicep Deployment Rollback

Azure Resource Manager retains deployment history. To redeploy from a previous template version:

```bash
# List previous deployments
az deployment group list \
  --resource-group "rg-fip-${ENVIRONMENT}" \
  --query "[].{name:name, provisioningState:properties.provisioningState, timestamp:properties.timestamp}" \
  --output table
```

### Secret Rotation

If a secret must be rotated:

1. Update the secret value in Key Vault: `az keyvault secret set --vault-name ... --name ... --value <NEW_VALUE>`
2. ADF linked services using Key Vault references will automatically pick up the new value on the next pipeline run.
3. For `databricks-pat-token`: regenerate the Databricks PAT in the Databricks UI, then update Key Vault. No ADF changes required.

---

*For architecture details, see `ARCHITECTURE.md`. For the monthly close runbook, see `RUNBOOK_MONTHLY_CLOSE.md`.*
