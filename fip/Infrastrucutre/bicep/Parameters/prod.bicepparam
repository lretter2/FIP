/*
  ============================================================================
  Production Deployment Parameters
  Financial Intelligence Platform · HU GAAP

  Usage:
    az deployment group create \
      --resource-group rg-fip-prod \
      --template-file ../main.bicep \
      --parameters @prod.bicepparam

  SECURITY: This file must NOT contain actual secrets or passwords.
  All sensitive values reference Azure Key Vault secrets after initial setup.
  ============================================================================
*/

using '../main.bicep'

// ---- ENVIRONMENT ----
param synapseSqlSku = 'DW1000c' 
param enablePrivateEndpoints = true
param projectCode = 'fip'
param location    = 'westeurope'   // Frankfurt region — lowest latency from Hungary

// ---- IDENTITIES (replace with real Object IDs from Azure AD) ----
// Get with: az ad sp show --id <app-id> --query id -o tsv
param platformSpObjectId = 'REPLACE_WITH_PLATFORM_SP_OBJECT_ID'
param dbaGroupObjectId   = 'REPLACE_WITH_DBA_GROUP_OBJECT_ID'
param powerBiSpObjectId  = 'REPLACE_WITH_POWERBI_SP_OBJECT_ID'
param tenantId           = 'REPLACE_WITH_AZURE_AD_TENANT_ID'

// ---- NETWORK (private endpoints enabled for production) ----
param enablePrivateEndpoints           = true
param vnetResourceId                   = '/subscriptions/REPLACE/resourceGroups/rg-fip-network/providers/Microsoft.Network/virtualNetworks/vnet-fip-prod'
param privateEndpointSubnetResourceId  = '/subscriptions/REPLACE/resourceGroups/rg-fip-network/providers/Microsoft.Network/virtualNetworks/vnet-fip-prod/subnets/snet-private-endpoints'

// ---- FIREWALL ----
// Add office/VPN IP ranges in "start-end" format
param allowedIpRanges = [
  '203.0.113.0-203.0.113.255'   // Replace with actual office IP range
]

// ---- TAGS ----
param tags = {
  project:     'FinancialIntelligencePlatform'
  environment: 'DW1000c'
  costCentre:  'IT-ANALYTICS'
  owner:       'DataEngineering'
  gaapBasis:   'HU_GAAP'
  version:     '1.0'
  deployedBy:  'Bicep'
  dataClass:   'Confidential-Financial'
  gdprScope:   'true'
  retentionPolicy: '8-years-HU-GAAP'
}
