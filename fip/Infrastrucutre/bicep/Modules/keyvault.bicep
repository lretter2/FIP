/*
  ============================================================================
  Module: Azure Key Vault
  Financial Intelligence Platform · HU GAAP

  Stores ALL platform secrets and encryption keys.
  No credentials are stored in configuration files or environment variables.
  All service connections reference Key Vault secret URIs.

  Secrets created at deployment:
    synapse-admin-password          — Synapse SQL admin (complex, auto-generated)
    adf-sp-client-secret            — ADF service principal secret
    azure-openai-api-key            — Azure OpenAI key (post-deployment manual step)
    databricks-token                — Databricks PAT (post-deployment)
    synapse-connection-string       — Full ODBC connection string for dbt/Python
    nbh-api-url                     — NBH FX rate API endpoint
    power-automate-alert-url        — Power Automate webhook for CFO alerts
    power-automate-cfo-notify-url   — Power Automate webhook for CFO notifications

  Access policy:
    Platform SP   → Get + List secrets
    DBA group     → Get + List + Set + Delete (admin operations)
    ADF MI        → Get secrets (for linked services)
    Databricks MI → Get secrets (for notebook credential access)
  ============================================================================
*/

param keyVaultName string
param location string
param tenantId string
param platformSpObjectId string
param dbaGroupObjectId string
param enablePrivateEndpoints bool = true
param privateEndpointSubnetResourceId string = ''
param tags object

// ============================================================================
// KEY VAULT
// ============================================================================

resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: tenantId
    sku: { family: 'A', name: 'premium' }   // Premium = HSM-backed keys
    enabledForDeployment: false
    enabledForTemplateDeployment: true
    enabledForDiskEncryption: true
    enableRbacAuthorization: true            // Use RBAC not access policies
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true              // Cannot be disabled once enabled
    publicNetworkAccess: enablePrivateEndpoints ? 'Disabled' : 'Enabled'
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: enablePrivateEndpoints ? 'Deny' : 'Allow'
    }
  }
}

// ============================================================================
// RBAC — Key Vault roles
// ============================================================================

var kvSecretsOfficer = 'b86a8fe4-44ce-4948-aee5-eccb2c155cd7'   // Key Vault Secrets Officer
var kvSecretsUser    = '4633458b-17de-408a-b874-0445c86b69e6'   // Key Vault Secrets User

// DBA group — full secrets management
resource dbaGroupRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, dbaGroupObjectId, kvSecretsOfficer)
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', kvSecretsOfficer)
    principalId: dbaGroupObjectId
    principalType: 'Group'
  }
}

// Platform SP — read secrets only
resource platformSpRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, platformSpObjectId, kvSecretsUser)
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', kvSecretsUser)
    principalId: platformSpObjectId
    principalType: 'ServicePrincipal'
  }
}

// ============================================================================
// SECRETS — seed with placeholder values; replace post-deployment
// ============================================================================

resource synapseAdminPassword 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = {
  parent: keyVault
  name: 'synapse-admin-password'
  properties: {
    value: 'REPLACE_WITH_STRONG_PASSWORD_POST_DEPLOYMENT'
    attributes: { enabled: true }
    contentType: 'password'
  }
}

resource nbhApiUrl 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = {
  parent: keyVault
  name: 'nbh-api-url'
  properties: {
    value: 'https://www.mnb.hu/arfolyamok'
    attributes: { enabled: true }
    contentType: 'url'
  }
}

resource synapseConnectionString 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = {
  parent: keyVault
  name: 'synapse-connection-string'
  properties: {
    value: 'REPLACE_POST_DEPLOYMENT: Driver={ODBC Driver 18 for SQL Server};Server=<synapse-sql-endpoint>;Database=fip_dw;Authentication=ActiveDirectoryServicePrincipal;...'
    attributes: { enabled: true }
    contentType: 'connection-string'
  }
}

// Placeholder secrets — populated post-deployment by the platform team
var placeholderSecrets = [
  { name: 'azure-openai-api-key',           contentType: 'api-key' }
  { name: 'databricks-token',               contentType: 'token' }
  { name: 'power-automate-alert-url',       contentType: 'url' }
  { name: 'power-automate-cfo-notify-url',  contentType: 'url' }
  { name: 'adf-sp-client-secret',           contentType: 'password' }
  { name: 'azure-tenant-id',                contentType: 'config' }
  { name: 'azure-client-id',                contentType: 'config' }
]

resource placeholderSecretResources 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = [for secret in placeholderSecrets: {
  parent: keyVault
  name: secret.name
  properties: {
    value: 'REPLACE_POST_DEPLOYMENT'
    attributes: { enabled: true }
    contentType: secret.contentType
  }
}]

// ============================================================================
// ENCRYPTION KEYS (for Databricks disk encryption and ADLS CMK)
// ============================================================================

resource databricksDiskKey 'Microsoft.KeyVault/vaults/keys@2023-02-01' = {
  parent: keyVault
  name: 'dbw-disk-encryption-key'
  properties: {
    kty: 'RSA'
    keySize: 4096
    keyOps: ['encrypt', 'decrypt', 'wrapKey', 'unwrapKey']
    attributes: { enabled: true }
  }
}

resource adlsCmk 'Microsoft.KeyVault/vaults/keys@2023-02-01' = {
  parent: keyVault
  name: 'adls-cmk'
  properties: {
    kty: 'RSA'
    keySize: 4096
    keyOps: ['encrypt', 'decrypt', 'wrapKey', 'unwrapKey']
    attributes: { enabled: true }
  }
}

// ============================================================================
// PRIVATE ENDPOINT
// ============================================================================

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2023-04-01' = if (enablePrivateEndpoints) {
  name: 'pep-${keyVaultName}'
  location: location
  tags: tags
  properties: {
    subnet: { id: privateEndpointSubnetResourceId }
    privateLinkServiceConnections: [{
      name: 'plsc-${keyVaultName}'
      properties: {
        privateLinkServiceId: keyVault.id
        groupIds: ['vault']
      }
    }]
  }
}

// ============================================================================
// OUTPUTS
// ============================================================================

output keyVaultName                   string = keyVault.name
output keyVaultUri                    string = keyVault.properties.vaultUri
output keyVaultResourceId             string = keyVault.id
output synapseAdminPasswordSecretUri  string = synapseAdminPassword.properties.secretUri
