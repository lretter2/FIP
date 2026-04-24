/*
  ============================================================================
  Module: Azure OpenAI Service
  Financial Intelligence Platform · HU GAAP

  Deploys Azure OpenAI with:
    - GPT-4o deployment (commentary generator + RAG agent)
    - text-embedding-3-small (for RAG vector embeddings)

  IMPORTANT: Azure OpenAI requires capacity approval from Microsoft.
  Request capacity before deployment: https://aka.ms/oai/access

  Data governance:
    - Private endpoint only (no data leaves the Azure tenant)
    - Audit logging enabled (all prompts + responses logged per AI governance rule)
    - No public endpoint access in production
    - Input validation layer in Python prevents prompt injection
  ============================================================================
*/

param accountName string
param location string
param enablePrivateEndpoints bool = true
param privateEndpointSubnetResourceId string = ''
param logAnalyticsWorkspaceId string
param platformSpObjectId string
param tags object

// ============================================================================
// AZURE OPENAI ACCOUNT
// ============================================================================

resource openAIAccount 'Microsoft.CognitiveServices/accounts@2023-05-01' = {
  name: accountName
  location: location
  tags: tags
  kind: 'OpenAI'
  identity: { type: 'SystemAssigned' }
  sku: { name: 'S0' }
  properties: {
    customSubDomainName: accountName
    publicNetworkAccess: enablePrivateEndpoints ? 'Disabled' : 'Enabled'
    networkAcls: {
      defaultAction: enablePrivateEndpoints ? 'Deny' : 'Allow'
      bypass: 'AzureServices'
    }
    restrictOutboundNetworkAccess: enablePrivateEndpoints
    disableLocalAuth: false     // API key auth still needed for some SDK versions
  }
}

// ============================================================================
// MODEL DEPLOYMENTS
// ============================================================================

// GPT-4o — primary model for commentary generation and RAG Q&A
resource gpt4oDeployment 'Microsoft.CognitiveServices/accounts/deployments@2023-05-01' = {
  parent: openAIAccount
  name: 'gpt-4o'
  sku: {
    name: 'Standard'
    capacity: 30   // 30K tokens per minute — suitable for monthly commentary workload
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: 'gpt-4o'
      version: '2024-08-06'    // Structured output support
    }
    versionUpgradeOption: 'OnceCurrentVersionExpired'
    raiPolicyName: 'Microsoft.Default'
  }
}

// text-embedding-3-small — for RAG vector embeddings (financial schema index)
resource embeddingDeployment 'Microsoft.CognitiveServices/accounts/deployments@2023-05-01' = {
  parent: openAIAccount
  name: 'text-embedding-3-small'
  sku: {
    name: 'Standard'
    capacity: 60   // 60K tokens per minute — embeddings are fast/cheap
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: 'text-embedding-3-small'
      version: '1'
    }
    versionUpgradeOption: 'OnceCurrentVersionExpired'
    raiPolicyName: 'Microsoft.Default'
  }
  dependsOn: [gpt4oDeployment]   // Deploy sequentially to avoid quota conflicts
}

// ============================================================================
// RBAC — Cognitive Services OpenAI User
// ============================================================================

var cognitiveServicesOpenAIUser = '5e0bd9bd-7b93-4f28-af87-19fc36ad61bd'

resource platformSpRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(openAIAccount.id, platformSpObjectId, cognitiveServicesOpenAIUser)
  scope: openAIAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', cognitiveServicesOpenAIUser)
    principalId: platformSpObjectId
    principalType: 'ServicePrincipal'
  }
}

// ============================================================================
// DIAGNOSTIC SETTINGS — AI governance requirement
// All prompts and responses are logged for audit and compliance
// ============================================================================

resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${accountName}'
  scope: openAIAccount
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      { category: 'Audit',    enabled: true }
      { category: 'RequestResponse', enabled: true }   // logs prompts + responses
    ]
    metrics: [{ category: 'AllMetrics', enabled: true }]
  }
}

// ============================================================================
// PRIVATE ENDPOINT
// ============================================================================

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2023-04-01' = if (enablePrivateEndpoints) {
  name: 'pep-${accountName}'
  location: location
  tags: tags
  properties: {
    subnet: { id: privateEndpointSubnetResourceId }
    privateLinkServiceConnections: [{
      name: 'plsc-${accountName}'
      properties: {
        privateLinkServiceId: openAIAccount.id
        groupIds: ['account']
      }
    }]
  }
}

// ============================================================================
// OUTPUTS
// ============================================================================

output endpoint          string = openAIAccount.properties.endpoint
output accountId         string = openAIAccount.id
output accountName       string = openAIAccount.name
output principalId       string = openAIAccount.identity.principalId
output gpt4oDeployment   string = gpt4oDeployment.name
output embeddingDeployment string = embeddingDeployment.name
