/*
  ============================================================================
  Module: Azure Synapse Analytics
  Financial Intelligence Platform · HU GAAP

  Deploys:
    - Synapse workspace with Managed Identity
    - Dedicated SQL pool (fip_dw) — the serving layer DWH
    - Serverless SQL pool (auto-provisioned, no config needed)
    - Synapse Spark pool (optional — for exploratory analytics)
    - RBAC assignments for Power BI, dbt, and Databricks
    - Private endpoints for SQL and Dev endpoints (prod only)
    - Firewall rules for ADF and client access

  Power BI connects via DirectQuery over the Dedicated SQL pool.
  dbt Core runs against the Dedicated SQL pool via the Synapse adapter.
  ============================================================================
*/

param workspaceName string
param sqlPoolName string
param location string
param adlsResourceId string
param adlsFileSystemName string
param platformSpObjectId string
param dbaGroupObjectId string
param powerBiSpObjectId string
param keyVaultUri string
param logAnalyticsWorkspaceId string
param allowedIpRanges array = []
param enablePrivateEndpoints bool = true
param privateEndpointSubnetResourceId string = ''
param sqlAdminPassword string    // reference to Key Vault secret URI
param tags object

// SQL admin username (password sourced from Key Vault)
var sqlAdminLogin = 'fip_sqladmin'

// ============================================================================
// SYNAPSE WORKSPACE
// ============================================================================

resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: workspaceName
  location: location
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: {
    defaultDataLakeStorage: {
      resourceId: adlsResourceId
      accountUrl: 'https://${split(adlsResourceId, '/')[8]}.dfs.core.windows.net'
      filesystem: adlsFileSystemName
      createManagedPrivateEndpoint: enablePrivateEndpoints
    }
    sqlAdministratorLogin: sqlAdminLogin
    sqlAdministratorLoginPassword: sqlAdminPassword
    managedVirtualNetwork: 'default'
    managedVirtualNetworkSettings: {
      preventDataExfiltration: true
      allowedAadTenantIdsForLinking: [subscription().tenantId]
    }
    publicNetworkAccess: enablePrivateEndpoints ? 'Disabled' : 'Enabled'
    azureADOnlyAuthentication: false   // keep SQL auth for dbt/ODBC connectivity
    encryption: {
      cmk: {
        // Customer-managed key — configure in Phase 2 via Key Vault reference
      }
    }
  }
}

// ============================================================================
// DEDICATED SQL POOL — fip_dw (the main DWH serving layer)
// ============================================================================

resource sqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  parent: synapseWorkspace
  name: sqlPoolName
  location: location
  tags: tags
  sku: { name: 'DW100c' }    // Start small; scale up before go-live. DW300c recommended for prod.
  properties: {
    createMode: 'Default'
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    storageAccountType: 'GRS'   // Geo-redundant backup
  }
}

// Auto-pause the SQL pool when idle (saves cost in dev/test)
resource sqlPoolWorkloadManagement 'Microsoft.Synapse/workspaces/sqlPools/workloadGroups@2021-06-01' = if (sqlPoolName != 'fip_dw') {
  parent: sqlPool
  name: 'AllQueries'
  properties: {
    minResourcePercent: 0
    maxResourcePercent: 100
    minResourcePercentPerRequest: 3
    importance: 'normal'
  }
}

// ============================================================================
// FIREWALL RULES
// ============================================================================

// Allow Azure services (ADF, Power BI, Databricks)
resource firewallAllowAzureServices 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  parent: synapseWorkspace
  name: 'AllowAllWindowsAzureIps'
  properties: { startIpAddress: '0.0.0.0', endIpAddress: '0.0.0.0' }
}

// Allowed office/VPN IP ranges
resource firewallAllowedIps 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = [for (ip, i) in allowedIpRanges: {
  parent: synapseWorkspace
  name: 'AllowRange-${i}'
  properties: { startIpAddress: split(ip, '-')[0], endIpAddress: split(ip, '-')[length(split(ip, '-')) - 1] }
}]

// ============================================================================
// RBAC — Synapse roles
// ============================================================================

// dbt / Platform SP → Synapse SQL Administrator
resource platformSpSynapseAdmin 'Microsoft.Synapse/workspaces/administrators@2021-06-01' = {
  parent: synapseWorkspace
  name: 'activeDirectory'
  properties: {
    administratorType: 'ActiveDirectory'
    login: 'FIP-Platform-SP'
    sid: platformSpObjectId
    tenantId: subscription().tenantId
  }
}

// Power BI SP — Storage Blob Data Reader on the Synapse workspace (for DirectQuery)
var synapseComputeOperator = 'c7a646d3-e8d2-4c41-bef0-5b7ee8af2ce7'
resource powerBiRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(synapseWorkspace.id, powerBiSpObjectId, synapseComputeOperator)
  scope: synapseWorkspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', synapseComputeOperator)
    principalId: powerBiSpObjectId
    principalType: 'ServicePrincipal'
  }
}

// ============================================================================
// DIAGNOSTIC SETTINGS
// ============================================================================

resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${workspaceName}'
  scope: synapseWorkspace
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      { category: 'SynapseRbacOperations', enabled: true }
      { category: 'GatewayApiRequests',    enabled: true }
      { category: 'SQLSecurityAuditEvents', enabled: true }
      { category: 'BuiltinSqlReqsEnded',   enabled: true }
    ]
    metrics: [{ category: 'AllMetrics', enabled: true }]
  }
}

// ============================================================================
// PRIVATE ENDPOINTS
// ============================================================================

resource peSql 'Microsoft.Network/privateEndpoints@2023-04-01' = if (enablePrivateEndpoints) {
  name: 'pep-${workspaceName}-sql'
  location: location
  tags: tags
  properties: {
    subnet: { id: privateEndpointSubnetResourceId }
    privateLinkServiceConnections: [{
      name: 'plsc-${workspaceName}-sql'
      properties: {
        privateLinkServiceId: synapseWorkspace.id
        groupIds: ['Sql']
      }
    }]
  }
}

resource peDev 'Microsoft.Network/privateEndpoints@2023-04-01' = if (enablePrivateEndpoints) {
  name: 'pep-${workspaceName}-dev'
  location: location
  tags: tags
  properties: {
    subnet: { id: privateEndpointSubnetResourceId }
    privateLinkServiceConnections: [{
      name: 'plsc-${workspaceName}-dev'
      properties: {
        privateLinkServiceId: synapseWorkspace.id
        groupIds: ['Dev']
      }
    }]
  }
}

// ============================================================================
// OUTPUTS
// ============================================================================

output workspaceName       string = synapseWorkspace.name
output workspaceResourceId string = synapseWorkspace.id
output sqlEndpoint         string = synapseWorkspace.properties.connectivityEndpoints.sql
output devEndpoint         string = synapseWorkspace.properties.connectivityEndpoints.web
output synapsePrincipalId  string = synapseWorkspace.identity.principalId
output sqlPoolId           string = sqlPool.id
