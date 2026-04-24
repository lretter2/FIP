/*
  ============================================================================
  Development Deployment Parameters
  Financial Intelligence Platform · HU GAAP

  Dev environment uses Azure SQL Database (serverless) instead of Synapse
  dedicated pool — no infrastructure to manage, scales to zero when idle.
  Private endpoints disabled in dev to simplify local connectivity.
  ============================================================================
*/

using '../main.bicep'

param environment = 'dev'
param projectCode = 'fip'
param location    = 'westeurope'

param platformSpObjectId = 'REPLACE_WITH_PLATFORM_SP_OBJECT_ID'
param dbaGroupObjectId   = 'REPLACE_WITH_DBA_GROUP_OBJECT_ID'
param powerBiSpObjectId  = 'REPLACE_WITH_POWERBI_SP_OBJECT_ID'
param tenantId           = 'REPLACE_WITH_AZURE_AD_TENANT_ID'

// Private endpoints OFF in dev (easier local developer access)
param enablePrivateEndpoints          = false
param vnetResourceId                  = ''
param privateEndpointSubnetResourceId = ''

// Allow developer machines
param allowedIpRanges = [
  '0.0.0.0-255.255.255.255'   // REPLACE — dev only, do NOT use in prod
]

param tags = {
  project:     'FinancialIntelligencePlatform'
  environment: 'dev'
  costCentre:  'IT-ANALYTICS'
  owner:       'DataEngineering'
  gaapBasis:   'HU_GAAP'
  version:     '1.0-dev'
  autoShutdown: 'true'
}
