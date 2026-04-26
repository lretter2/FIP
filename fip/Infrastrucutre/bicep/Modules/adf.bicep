/*
  ============================================================================
  Module: Azure Data Factory
  Financial Intelligence Platform · HU GAAP

  ADF orchestrates the monthly close pipeline (pl_monthly_close.json):
    T+0   — ERP file extraction trigger
    T+1h  — Bronze landing validation
    T+2h  — Data quality checks (pl_dq_validation)
    T+3h  — dbt Silver run (via Databricks activity)
    T+5h  — dbt Gold run
    T+6h  — AI commentary + anomaly detection (Databricks activities)
    T+7h  — Power BI dataset refresh
    Day 2 — CFO notification via Power Automate

  Uses System Assigned Managed Identity — no credentials stored in ADF.
  All linked service credentials reference Key Vault secret URIs.
  ============================================================================
*/

param factoryName string
param location string
param keyVaultUri string
param logAnalyticsWorkspaceId string
param enablePrivateEndpoints bool = true
param tags object

// ============================================================================
// DATA FACTORY
// ============================================================================

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: location
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: {
    publicNetworkAccess: enablePrivateEndpoints ? 'Disabled' : 'Enabled'
    globalParameters: {
      environment: { type: 'String', value: contains(factoryName, 'prod') ? 'prod' : 'dev' }
      keyVaultUri: { type: 'String', value: keyVaultUri }
    }
    encryption: {
      identity: { userAssignedIdentity: null }   // Uses system-assigned MI for CMK
      vaultBaseUrl: keyVaultUri
      keyName: 'adf-cmk'
      keyVersion: ''
    }
  }
}

// ============================================================================
// LINKED SERVICES — defined in ADF as Key Vault-referenced connections
// Note: Full linked service JSON is deployed via ADF CI/CD pipeline,
// not via Bicep (ADF has its own ARM/JSON deployment mechanism).
// Source of truth: adf_pipelines/linked_services/*.json
//
// Storage & Compute:
//   ls_adls_gen2           — ADLS Gen2, unified storage (bronze/silver/gold), MI auth
//   ls_synapse_sql_pool    — Azure Synapse dedicated SQL pool, MI auth
//   ls_databricks          — Azure Databricks, PAT token from KV secret 'databricks-pat-token'
//   ls_azure_key_vault     — Azure Key Vault (base linked service for all secrets)
//   ls_azure_openai        — Azure OpenAI, API key from KV secret
//
// ERP Source Systems:
//   ls_sftp_kulcssoft      — SFTP for Kulcs-Soft CSV export, SSH key from KV
//   ls_rest_cobalt         — REST API for COBALT GL export, API key from KV
//   ls_rest_szamlazz       — REST API for Számlázz.hu invoice export (no auth)
//   ls_rest_sap_b1         — REST API for SAP B1 Service Layer, Basic auth from KV
// ============================================================================

// ============================================================================
// SELF-HOSTED INTEGRATION RUNTIME (for on-premises ERP systems)
// Required for: Kulcs-Soft, COBALT, or any on-premises accounting software
// ============================================================================

resource shir 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = {
  parent: dataFactory
  name: 'ir-fip-onprem'
  properties: {
    type: 'SelfHosted'
    description: 'Self-hosted IR for on-premises ERP connectivity (Kulcs-Soft, COBALT, etc.)'
  }
}

// Azure Integration Runtime (cloud-to-cloud) — auto-resolve, no config needed
// Default ADF IR is used for: ADLS, Synapse, REST API (NBH), Databricks

// ============================================================================
// TRIGGERS — defined via ADF CI/CD pipeline deployment
// The following trigger schedules are deployed via the ADF pipeline JSON:
//
//   tr_monthly_close_trigger:
//     Type: Schedule
//     Recurrence: Monthly, 1st day of each month at 00:00 CET
//     Pipeline: pl_monthly_close
//
//   tr_nbh_fx_rates_daily:
//     Type: Schedule
//     Recurrence: Daily at 09:00 CET (after NBH publishes daily rates)
//     Pipeline: pl_nbh_fx_rate_load
//
//   tr_erp_file_arrival:
//     Type: Storage Event
//     Storage: adls-bronze, path: raw/{company_id}/
//     Event: BlobCreated
//     Pipeline: pl_erp_extract (on-demand file-arrival trigger)
// ============================================================================

// ============================================================================
// DIAGNOSTIC SETTINGS
// ============================================================================

resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${factoryName}'
  scope: dataFactory
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      { category: 'ActivityRuns',   enabled: true }
      { category: 'PipelineRuns',   enabled: true }
      { category: 'TriggerRuns',    enabled: true }
      { category: 'SandboxActivityRuns', enabled: true }
      { category: 'SandboxPipelineRuns', enabled: true }
    ]
    metrics: [{ category: 'AllMetrics', enabled: true }]
  }
}

// ============================================================================
// OUTPUTS
// ============================================================================

output factoryName   string = dataFactory.name
output factoryId     string = dataFactory.id
output principalId   string = dataFactory.identity.principalId
