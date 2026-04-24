/*
  ============================================================================
  CI Deployment Parameters
  Financial Intelligence Platform · HU GAAP

  Used by Azure DevOps pipeline for automated PR validation and integration
  tests. Points to a lightweight Azure SQL Database (serverless) — not a
  dedicated Synapse pool — to keep CI costs minimal.

  IMPORTANT:
    - Private endpoints are DISABLED in CI (simpler pipeline connectivity)
    - All IP ranges open — CI runners have dynamic IPs
    - Databricks and OpenAI modules deploy at minimal SKU
    - This environment is ephemeral: torn down after each pipeline run

  Usage (in Azure DevOps pipeline YAML):
    az deployment group create \
      --resource-group rg-fip-ci-$(Build.BuildId) \
      --template-file ../main.bicep \
      --parameters @ci.bicepparam
  ============================================================================
*/

using '../main.bicep'

param environment = 'ci'
param projectCode = 'fip'
param location    = 'westeurope'

// ---- IDENTITIES ----
// CI uses a dedicated CI service principal (lower permissions than prod)
// Populated via Azure DevOps variable group: FIP-CI-Variables
param platformSpObjectId = '$(CI_PLATFORM_SP_OBJECT_ID)'
param dbaGroupObjectId   = '$(CI_DBA_GROUP_OBJECT_ID)'
param powerBiSpObjectId  = '$(CI_POWERBI_SP_OBJECT_ID)'
param tenantId           = '$(AZURE_TENANT_ID)'

// ---- NETWORK ----
// Private endpoints OFF in CI — Azure DevOps hosted agents have dynamic IPs
param enablePrivateEndpoints          = false
param vnetResourceId                  = ''
param privateEndpointSubnetResourceId = ''

// ---- FIREWALL ----
// Open to all — CI hosted agents have dynamic public IPs
// Azure DevOps will inject specific runner IP if available via pipeline step
param allowedIpRanges = [
  '0.0.0.0-255.255.255.255'   // CI only — NEVER use in dev or prod
]

// ---- TAGS ----
param tags = {
  project:      'FinancialIntelligencePlatform'
  environment:  'ci'
  costCentre:   'IT-ANALYTICS'
  owner:        'DataEngineering'
  gaapBasis:    'HU_GAAP'
  version:      '1.0-ci'
  autoShutdown: 'true'
  ephemeral:    'true'
  buildId:      '$(Build.BuildId)'
}
