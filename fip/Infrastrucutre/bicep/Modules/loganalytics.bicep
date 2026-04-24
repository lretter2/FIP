
/*
  ============================================================================
  Module: Log Analytics Workspace
  Financial Intelligence Platform · HU GAAP

  Central observability hub — all Azure resource diagnostics flow here.
  Used by:
    - ADF pipeline run logs (activity + trigger runs)
    - Synapse query + RBAC audit logs
    - Databricks cluster + job logs
    - Key Vault access logs
    - Azure OpenAI audit logs (AI governance requirement)
    - Power BI activity log (ingested via Logic App in Phase 2)
  ============================================================================
*/

param workspaceName string
param location string
param retentionInDays int = 365
param tags object

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: workspaceName
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: retentionInDays
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
      disableLocalAuth: false
    }
    workspaceCapping: { dailyQuotaGb: 5 }   // Hard cap to prevent runaway costs
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

// Application Insights (linked to Log Analytics)
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: 'appi-${workspaceName}'
  location: location
  tags: tags
  kind: 'other'
  properties: {
    Application_Type: 'other'
    WorkspaceResourceId: logAnalyticsWorkspace.id
    IngestionMode: 'LogAnalytics'
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

output workspaceId           string = logAnalyticsWorkspace.id
output workspaceName         string = logAnalyticsWorkspace.name
output appInsightsId         string = appInsights.id
output appInsightsKey        string = appInsights.properties.InstrumentationKey
output appInsightsConnString string = appInsights.properties.ConnectionString
