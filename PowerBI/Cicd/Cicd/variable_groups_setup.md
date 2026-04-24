# Azure DevOps Variable Groups — Setup Guide

Required for the `azure-pipelines-powerbi.yml` CI/CD pipeline.

## 1. `fip-powerbi-common` (shared — not protected)

| Variable | Example Value | Description |
|---|---|---|
| `AZURE_TENANT_ID` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | AAD tenant ID |
| `AZURE_CLIENT_ID` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | Service principal for CI (PowerBI.ReadWrite.All scope) |
| `AZURE_CLIENT_SECRET` | *(secret)* | SP client secret — mark as secret |
| `POWERBI_DEPLOYMENT_PIPELINE_ID` | `xxxxxxxx-...` | Power BI Deployment Pipeline ID (from PBI Admin portal) |
| `POWERBI_DEV_WORKSPACE` | `FIP-Development` | Dev workspace name |
| `POWERBI_DEV_DATASET` | `FIP_Main_Dev` | Dev dataset name |

## 2. `fip-powerbi-test` (TEST environment — restricted to DeployTest stage)

| Variable | Example Value | Description |
|---|---|---|
| `POWERBI_TEST_WORKSPACE_ID` | `xxxxxxxx-...` | GUID of TEST workspace |
| `POWERBI_TEST_WORKSPACE_NAME` | `FIP-Test` | TEST workspace name |
| `POWERBI_TEST_DATASET_ID` | `xxxxxxxx-...` | GUID of TEST dataset |
| `POWERBI_TEST_DATASET_NAME` | `FIP_Main_Test` | TEST dataset name |
| `SYNAPSE_TEST_SERVER` | `fip-test.sql.azuresynapse.net` | TEST Synapse SQL endpoint |

## 3. `fip-powerbi-prod` (PROD environment — protected, approval gate)

| Variable | Example Value | Description |
|---|---|---|
| `POWERBI_PROD_WORKSPACE_ID` | `xxxxxxxx-...` | GUID of PROD workspace |
| `POWERBI_PROD_DATASET_ID` | `xxxxxxxx-...` | GUID of PROD dataset |
| `SYNAPSE_PROD_SERVER` | `fip-prod.sql.azuresynapse.net` | PROD Synapse SQL endpoint — never hardcoded in pipeline |

## Service Principal Requirements

The SP must have the following Power BI scopes:
- `Dataset.ReadWrite.All`
- `Report.ReadWrite.All`
- `Workspace.ReadWrite.All`
- `Pipeline.Deploy`

Grant access: Power BI Admin portal → Tenant settings → Allow service principals to use Power BI APIs.

## Environment Approval Gates

Configure in Azure DevOps → Environments:
- `FIP-PowerBI-TEST` — auto-approve (runs on every main merge)
- `FIP-PowerBI-PROD` — requires approval from CFO or Platform Engineering lead
