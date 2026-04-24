/*
  ============================================================================
  Financial Intelligence Platform — Azure Infrastructure (Bicep)
  Main Deployment Entry Point
  Version 1.0 · 2026 · HU GAAP

  Deploys the complete Azure-native lakehouse stack:
    Layer 1  — Azure Data Factory (ingestion orchestration)
    Layer 2  — ADLS Gen2 (Bronze / Silver / Gold storage zones)
    Layer 3  — Azure Databricks (ELT compute + dbt runner)
    Layer 4  — Azure Synapse Analytics (serving layer / DWH)
    Layer 5  — Power BI Premium (via existing workspace — not deployed here)
    Layer 6  — Azure OpenAI (AI commentary + RAG agent)
    Layer 8  — Azure Key Vault, Log Analytics, Private Endpoints (governance)

  Deploy command:
    az deployment group create \
      --resource-group rg-fip-prod \
      --template-file main.bicep \
      --parameters @parameters/prod.bicepparam \
      --confirm-with-what-if

  Required pre-conditions:
    1. Resource group must exist
    2. Azure OpenAI must be requested/approved in your subscription (capacity limited)
    3. Databricks workspace requires contributor on the managed RG
  ============================================================================
*/

targetScope = 'resourceGroup'

// ============================================================================
// PARAMETERS
// ============================================================================

@description('Deployment environment (prod | dev | ci)')
@allowed(['prod', 'dev', 'ci'])
param environment string = 'prod'

@description('Short identifier used in all resource names (e.g. "fip")')
@maxLength(8)
param projectCode string = 'fip'

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Azure AD object ID of the platform service principal')
param platformSpObjectId string

@description('Azure AD object ID of the DBA/admin group')
param dbaGroupObjectId string

@description('Azure AD object ID of the Power BI service principal')
param powerBiSpObjectId string

@description('Azure AD tenant ID')
param tenantId string = subscription().tenantId

@description('IP ranges allowed through network firewall rules (e.g. office VPN ranges)')
param allowedIpRanges array = []

@description('Enable private endpoints for all data services (recommended for prod)')
param enablePrivateEndpoints bool = (environment == 'prod')

@description('Virtual network resource ID for private endpoint injection (required if enablePrivateEndpoints=true)')
param vnetResourceId string = ''

@description('Subnet resource ID for private endpoints')
param privateEndpointSubnetResourceId string = ''

@description('Tags applied to all resources')
param tags object = {
  project: 'FinancialIntelligencePlatform'
  environment: environment
  costCentre: 'IT-ANALYTICS'
  owner: 'DataEngineering'
  gaapBasis: 'HU_GAAP'
  version: '1.0'
}

// ============================================================================
// VARIABLES — naming convention: {projectCode}-{component}-{environment}
// ============================================================================

var namePrefix       = '${projectCode}-${environment}'
var storageNameSuffix = uniqueString(resourceGroup().id)  // ensures global uniqueness

// ============================================================================
// MODULE: KEY VAULT (deploy first — all other modules reference it)
// ============================================================================

module keyVault 'modules/keyvault.bicep' = {
  name: 'deploy-keyvault'
  params: {
    keyVaultName: 'kv-${namePrefix}-${take(storageNameSuffix, 6)}'
    location: location
    tenantId: tenantId
    platformSpObjectId: platformSpObjectId
    dbaGroupObjectId: dbaGroupObjectId
    enablePrivateEndpoints: enablePrivateEndpoints
    privateEndpointSubnetResourceId: privateEndpointSubnetResourceId
    tags: tags
  }
}

// ============================================================================
// MODULE: LOG ANALYTICS WORKSPACE (deploy early — all modules send diagnostics)
// ============================================================================

module logAnalytics 'modules/loganalytics.bicep' = {
  name: 'deploy-loganalytics'
  params: {
    workspaceName: 'log-${namePrefix}'
    location: location
    retentionInDays: environment == 'prod' ? 365 : 90
    tags: tags
  }
}

// ============================================================================
// MODULE: ADLS Gen2 — Bronze / Silver / Gold / Config zones
// ============================================================================

module storage 'modules/storage.bicep' = {
  name: 'deploy-storage'
  params: {
    storageAccountName: 'adls${projectCode}${environment}${take(storageNameSuffix, 6)}'
    location: location
    platformSpObjectId: platformSpObjectId
    adfPrincipalId: adf.outputs.principalId
    databricksPrincipalId: databricks.outputs.principalId
    allowedIpRanges: allowedIpRanges
    enablePrivateEndpoints: enablePrivateEndpoints
    privateEndpointSubnetResourceId: privateEndpointSubnetResourceId
    logAnalyticsWorkspaceId: logAnalytics.outputs.workspaceId
    tags: tags
  }
  dependsOn: [adf, databricks, keyVault, logAnalytics]
}
// ============================================================================
// MODULE: AZURE DATA FACTORY
// ============================================================================

module adf 'modules/adf.bicep' = {
  name: 'deploy-adf'
  params: {
    factoryName: 'adf-${namePrefix}'
    location: location
    keyVaultUri: keyVault.outputs.keyVaultUri
    logAnalyticsWorkspaceId: logAnalytics.outputs.workspaceId
    enablePrivateEndpoints: enablePrivateEndpoints
    tags: tags
  }
  dependsOn: [keyVault, logAnalytics]
}

// ============================================================================
// MODULE: AZURE DATABRICKS
// ============================================================================

module databricks 'modules/databricks.bicep' = {
  name: 'deploy-databricks'
  params: {
    workspaceName: 'dbw-${namePrefix}'
    location: location
    keyVaultUri: keyVault.outputs.keyVaultUri
    keyVaultResourceId: keyVault.outputs.keyVaultResourceId
    logAnalyticsWorkspaceId: logAnalytics.outputs.workspaceId
    enableVnetInjection: enablePrivateEndpoints
    vnetResourceId: vnetResourceId
    tags: tags
  }
  dependsOn: [keyVault, logAnalytics]
}

// ============================================================================
// MODULE: AZURE SYNAPSE ANALYTICS
// ============================================================================

module synapse 'modules/synapse.bicep' = {
  name: 'deploy-synapse'
  params: {
    workspaceName: 'syn-${namePrefix}'
    sqlPoolName: 'fip_dw'
    location: location
    adlsResourceId: storage.outputs.storageResourceId
    adlsFileSystemName: 'gold'
    platformSpObjectId: platformSpObjectId
    dbaGroupObjectId: dbaGroupObjectId
    powerBiSpObjectId: powerBiSpObjectId
    keyVaultUri: keyVault.outputs.keyVaultUri
    logAnalyticsWorkspaceId: logAnalytics.outputs.workspaceId
    allowedIpRanges: allowedIpRanges
    enablePrivateEndpoints: enablePrivateEndpoints
    privateEndpointSubnetResourceId: privateEndpointSubnetResourceId
    sqlAdminPassword: keyVault.outputs.synapseAdminPasswordSecretUri
    tags: tags
  }
  dependsOn: [storage, keyVault, logAnalytics]
}

// ============================================================================
// MODULE: AZURE OPENAI
// ============================================================================

module openAI 'modules/openai.bicep' = {
  name: 'deploy-openai'
  params: {
    accountName: 'oai-${namePrefix}'
    location: location   // Note: Azure OpenAI availability varies by region
    enablePrivateEndpoints: enablePrivateEndpoints
    privateEndpointSubnetResourceId: privateEndpointSubnetResourceId
    logAnalyticsWorkspaceId: logAnalytics.outputs.workspaceId
    platformSpObjectId: platformSpObjectId
    tags: tags
  }
  dependsOn: [logAnalytics]
}

// ============================================================================
// OUTPUTS — referenced by post-deployment configuration scripts
// ============================================================================

output storageAccountName      string = storage.outputs.storageAccountName
output storageAccountId        string = storage.outputs.storageResourceId
output synapseWorkspaceName    string = synapse.outputs.workspaceName
output synapseSqlEndpoint      string = synapse.outputs.sqlEndpoint
output databricksWorkspaceUrl  string = databricks.outputs.workspaceUrl
output adfName                 string = adf.outputs.factoryName
output openAIEndpoint          string = openAI.outputs.endpoint
output keyVaultUri             string = keyVault.outputs.keyVaultUri
output logAnalyticsWorkspaceId string = logAnalytics.outputs.workspaceId
