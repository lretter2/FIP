/*
  ============================================================================
  Module: Azure Databricks Workspace
  Financial Intelligence Platform · HU GAAP

  Databricks is the compute engine for:
    - Running dbt Core (via dbt-synapse adapter on the cluster)
    - Anomaly detection Python scripts (anomaly_detector.py)
    - AI commentary generation (commentary_generator.py)
    - RAG/Text-to-SQL agent (financial_qa_agent.py)

  Cluster configuration:
    - Job clusters (auto-terminate) for ADF-triggered pipeline runs
    - No always-on clusters in prod (cost governance)
    - Cluster policy enforces auto-termination after 30 mins idle
  ============================================================================
*/

param workspaceName string
param location string
param keyVaultUri string
param keyVaultResourceId string
param logAnalyticsWorkspaceId string
param enableVnetInjection bool = true
param vnetResourceId string = ''
param tags object

// ============================================================================
// DATABRICKS WORKSPACE
// ============================================================================

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: workspaceName
  location: location
  tags: tags
  sku: { name: 'premium' }    // Premium required for: Unity Catalog, SCIM, Secrets, IP ACLs
  properties: {
    managedResourceGroupId: '${subscription().id}/resourceGroups/rg-${workspaceName}-managed'
    publicNetworkAccess: enableVnetInjection ? 'Disabled' : 'Enabled'
    requiredNsgRules: enableVnetInjection ? 'NoAzureDatabricksRules' : 'AllRules'
    parameters: enableVnetInjection ? {
      customVirtualNetworkId: { value: vnetResourceId }
      customPublicSubnetName: { value: 'snet-databricks-public' }
      customPrivateSubnetName: { value: 'snet-databricks-private' }
      enableNoPublicIp: { value: true }
    } : {}
    encryption: {
      entities: {
        managedDisk: {
          keySource: 'Microsoft.Keyvault'
          keyVaultProperties: {
            keyVaultUri: keyVaultUri
            keyName: 'dbw-disk-encryption-key'
            keyVersion: ''
          }
          rotationToLatestKeyVersionEnabled: true
        }
      }
    }
  }
}

// ============================================================================
// KEY VAULT SECRET SCOPE (for secure credential access in notebooks/scripts)
// Databricks reads Azure Key Vault secrets via a secret scope backed by KV.
// Scripts use: dbutils.secrets.get(scope="fip-kv", key="synapse-connection-string")
// ============================================================================

// Note: The Databricks secret scope backed by Azure Key Vault is configured
// via the Databricks REST API post-deployment (cannot be done in Bicep/ARM).
// Post-deployment script: scripts/setup_databricks_secret_scope.sh
//
// Command to run after deployment:
//   databricks secrets create-scope \
//     --scope fip-kv \
//     --scope-backend-type AZURE_KEYVAULT \
//     --resource-id <key-vault-resource-id> \
//     --dns-name <key-vault-uri>

// ============================================================================
// DIAGNOSTIC SETTINGS
// ============================================================================

resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${workspaceName}'
  scope: databricksWorkspace
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      { category: 'dbfs',         enabled: true }
      { category: 'clusters',     enabled: true }
      { category: 'accounts',     enabled: true }
      { category: 'jobs',         enabled: true }
      { category: 'notebook',     enabled: true }
      { category: 'ssh',          enabled: true }
      { category: 'workspace',    enabled: true }
      { category: 'secrets',      enabled: true }
      { category: 'sqlPermissions', enabled: true }
    ]
  }
}

// ============================================================================
// OUTPUTS
// ============================================================================

output workspaceUrl        string = 'https://${databricksWorkspace.properties.workspaceUrl}'
output workspaceResourceId string = databricksWorkspace.id
output workspaceId         string = databricksWorkspace.properties.workspaceId
output principalId         string = databricksWorkspace.identity.principalId
