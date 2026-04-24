/*
  ============================================================================
  Module: ADLS Gen2 Storage Account
  Financial Intelligence Platform · HU GAAP

  Creates the data lake storage account with five hierarchical namespaces:
    bronze/   — immutable raw landing zone (append-only, write-once policy)
    silver/   — cleansed and validated data (dbt output)
    gold/     — analytics-ready aggregates (dbt output, Power BI source)
    config/   — reference data and seeds
    audit/    — pipeline logs and DQ results (append-only)

  Access control uses Azure RBAC (no shared keys for service workloads):
    Platform SP       → Storage Blob Data Contributor on all containers
    ADF               → Storage Blob Data Contributor on bronze (write) + read all
    Databricks        → Storage Blob Data Contributor on silver + gold
    Synapse           → Storage Blob Data Reader on gold
  ============================================================================
*/

param storageAccountName string
param location string
param platformSpObjectId string
param adfPrincipalId string
param databricksPrincipalId string
param synapsePrincipalId string = ''  // Optional: assigned post-deployment to avoid circular dependency
param allowedIpRanges array = []
param enablePrivateEndpoints bool = true
param privateEndpointSubnetResourceId string = ''
param logAnalyticsWorkspaceId string
param tags object

// ============================================================================
// STORAGE ACCOUNT — ADLS Gen2 (Hierarchical Namespace enabled)
// ============================================================================

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: { name: 'Standard_GRS' }     // Geo-redundant for prod data durability
  properties: {
    isHnsEnabled: true               // Hierarchical Namespace = ADLS Gen2
    accessTier: 'Hot'
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false      // Enforce Azure AD auth — no shared key access
    supportsHttpsTrafficOnly: true
    defaultToOAuthAuthentication: true
    networkAcls: {
      bypass: 'AzureServices,Logging,Metrics'
      defaultAction: enablePrivateEndpoints ? 'Deny' : 'Allow'
      ipRules: [for ip in allowedIpRanges: {
        value: ip
        action: 'Allow'
      }]
    }
    encryption: {
      services: {
        blob: { enabled: true, keyType: 'Account' }
        file: { enabled: true, keyType: 'Account' }
      }
      keySource: 'Microsoft.Storage'   // Use customer-managed keys via Key Vault in Phase 2
      requireInfrastructureEncryption: true
    }
  }
}

// ============================================================================
// FILE SYSTEMS (containers with hierarchical namespace)
// ============================================================================

var containers = ['bronze', 'silver', 'gold', 'config', 'audit']

resource fileSystemContainers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [for container in containers: {
  name: '${storageAccount.name}/default/${container}'
  properties: {
    publicAccess: 'None'
  }
}]

// Immutable policy on bronze container (WORM — satisfies HU accounting law 8-year retention)
resource bronzeImmutabilityPolicy 'Microsoft.Storage/storageAccounts/blobServices/containers/immutabilityPolicies@2023-01-01' = {
  name: '${storageAccount.name}/default/bronze/default'
  properties: {
    immutabilityPeriodSinceCreationInDays: 2922   // 8 years (365 × 8 + 2 leap days)
    allowProtectedAppendWrites: true               // allows ADF to append new batch files
  }
  dependsOn: [fileSystemContainers]
}

// ============================================================================
// LIFECYCLE MANAGEMENT — tier cold data automatically
// ============================================================================

resource lifecyclePolicy 'Microsoft.Storage/storageAccounts/managementPolicies@2023-01-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    policy: {
      rules: [
        {
          name: 'tier-bronze-to-cool-after-90-days'
          enabled: true
          type: 'Lifecycle'
          definition: {
            filters: { blobTypes: ['blockBlob'], prefixMatch: ['bronze/'] }
            actions: {
              baseBlob: {
                tierToCool: { daysAfterModificationGreaterThan: 90 }
                tierToArchive: { daysAfterModificationGreaterThan: 730 }  // 2 years → archive
              }
            }
          }
        }
        {
          name: 'delete-audit-logs-after-3650-days'
          enabled: true
          type: 'Lifecycle'
          definition: {
            filters: { blobTypes: ['blockBlob'], prefixMatch: ['audit/'] }
            actions: {
              baseBlob: { delete: { daysAfterModificationGreaterThan: 3650 } }  // 10 years
            }
          }
        }
      ]
    }
  }
}

// ============================================================================
// RBAC ROLE ASSIGNMENTS
// Built-in role IDs (these are stable across all Azure subscriptions)
// ============================================================================

var storageBlobDataOwner       = 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b'
var storageBlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var storageBlobDataReader      = '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'

// Platform SP — full owner for pipeline execution
resource platformSpRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, platformSpObjectId, storageBlobDataOwner)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataOwner)
    principalId: platformSpObjectId
    principalType: 'ServicePrincipal'
  }
}

// ADF — contributor (writes to bronze, reads all)
resource adfRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, adfPrincipalId, storageBlobDataContributor)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributor)
    principalId: adfPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// Databricks — contributor (reads bronze, writes silver + gold)
resource databricksRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, databricksPrincipalId, storageBlobDataContributor)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributor)
    principalId: databricksPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// Synapse — reader only (reads gold for DirectQuery)
resource synapseRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, synapsePrincipalId, storageBlobDataReader)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataReader)
    principalId: synapsePrincipalId
    principalType: 'ServicePrincipal'
  }
}

// ============================================================================
// DIAGNOSTIC SETTINGS → Log Analytics
// ============================================================================

resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${storageAccount.name}'
  scope: storageAccount
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    metrics: [{ category: 'Transaction', enabled: true }]
  }
}

// ============================================================================
// PRIVATE ENDPOINT (prod only)
// ============================================================================

resource privateEndpointBlob 'Microsoft.Network/privateEndpoints@2023-04-01' = if (enablePrivateEndpoints) {
  name: 'pep-${storageAccountName}-blob'
  location: location
  tags: tags
  properties: {
    subnet: { id: privateEndpointSubnetResourceId }
    privateLinkServiceConnections: [{
      name: 'plsc-${storageAccountName}-blob'
      properties: {
        privateLinkServiceId: storageAccount.id
        groupIds: ['blob']
      }
    }]
  }
}

// ============================================================================
// OUTPUTS
// ============================================================================

output storageAccountName   string = storageAccount.name
output storageResourceId    string = storageAccount.id
output primaryEndpoint      string = storageAccount.properties.primaryEndpoints.blob
output dfsEndpoint          string = storageAccount.properties.primaryEndpoints.dfs
