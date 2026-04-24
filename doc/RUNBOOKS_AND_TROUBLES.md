# Runbooks and Troubleshooting Guide

**Operational procedures, common issues, diagnostic steps, and resolution procedures for the Financial Intelligence Platform.**

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Monthly Close Runbook](#monthly-close-runbook)
3. [Common Issues & Solutions](#common-issues--solutions)
4. [Monitoring & Alerts](#monitoring--alerts)
5. [Performance Troubleshooting](#performance-troubleshooting)
6. [Backup & Recovery](#backup--recovery)
7. [Escalation Procedures](#escalation-procedures)

---

## Daily Operations

### Start of Day (6:00 AM UTC)

**Duration**: 10 minutes

```bash
# 1. Check overnight pipeline status
# Go to: Azure Portal → Data Factory → Pipeline runs
# Expected: All overnight pipelines completed with status "Succeeded"

# 2. Verify data freshness
sqlcmd -S your-server.sql.azuresynapse.net -d fip_dw -U admin -P $SA_PASSWORD <<EOF
SELECT
  table_name,
  MAX(updated_at) AS last_update,
  DATEDIFF(HOUR, MAX(updated_at), GETUTCDATE()) AS hours_stale
FROM (
  SELECT 'gold.revenue_metrics' AS table_name, MAX(created_at) AS updated_at FROM gold.revenue_metrics
  UNION ALL
  SELECT 'gold.expense_analysis', MAX(created_at) FROM gold.expense_analysis
  UNION ALL
  SELECT 'gold.budget_variance', MAX(created_at) FROM gold.budget_variance
) t
GROUP BY table_name
ORDER BY last_update DESC;
EOF

# 3. Check for data quality alerts
# Review Application Insights queries:
customEvents
| where name == "DataQualityAlert"
| where timestamp > ago(24h)
| summarize AlertCount = count() by tostring(customDimensions.alert_type)

# 4. Review error logs
# Go to: Azure Portal → Monitor → Logs
# Run query to check for errors in past 24h
traces
| where severityLevel >= 2
| where timestamp > ago(24h)
| summarize ErrorCount = count() by tostring(customDimensions.component)
| order by ErrorCount desc
```

**If issues found**: Jump to [Common Issues & Solutions](#common-issues--solutions)

### End of Day (6:00 PM UTC)

```bash
# 1. Verify production data integrity
dbt freshness  # Check data sources last loaded

# 2. Generate backup confirmation
az sql db backup list \
  --resource-group fip-prod-rg \
  --server fip-server \
  --database fip_dw \
  --query "[0:3].{Status, DateTime}" \
  -o table

# 3. Archive old application logs
python scripts/archive_logs.py --older-than 30 --destination blob-archive

# 4. Check on-call engineer contact info for next shift
# Verify: PagerDuty escalation policy, email notification list
```

### Weekly (Every Monday, 9:00 AM)

```bash
# 1. Access review
# Data owners review their users' access logs
sqlcmd -S your-server.sql.azuresynapse.net -d fip_dw <<EOF
SELECT
  principal_name,
  action_id,
  COUNT(*) AS action_count,
  MIN(event_time) AS first_access,
  MAX(event_time) AS last_access
FROM fn_get_audit_file('*/audit/*', DEFAULT, DEFAULT)
WHERE event_time >= DATEADD(DAY, -7, CAST(GETUTCDATE() AS DATE))
GROUP BY principal_name, action_id
HAVING COUNT(*) > 0
ORDER BY principal_name, action_count DESC;
EOF

# 2. dbt freshness and test results
cd financial_dbt
dbt freshness --select source:*
dbt test --select tag:critical | tee test_results_$(date +%Y%m%d).txt

# 3. Check Databricks cluster health
# Portal: Databricks → Clusters → Health tab
# Verify: No failed node health checks, CPU/memory utilization normal
```

### Monthly (1st Business Day)

See [Monthly Close Runbook](#monthly-close-runbook) below.

---

## Monthly Close Runbook

**Target**: Complete by 10:00 AM (8 hours after pipeline start at 2:00 AM)

### Pre-Close Checklist (Previous Day, 5:00 PM)

```bash
# [ ] Verify ERP data export ready
# [ ] Confirm no blocking GL locks in source system
# [ ] Notify finance team of maintenance window
# [ ] Verify backup from previous month completed
# [ ] Check disk space on ADLS (target: > 50% free)

az storage account show \
  --resource-group fip-prod-rg \
  --name fipprodstorage \
  --query "properties.{Status: provisioningState, Kind, AccessTier}"
```

### Hour 1: Data Extraction (2:00 - 3:00 AM)

**Pipeline**: `pl_erp_extract`

```bash
# Monitor progress
az datafactory pipeline-run list \
  --factory-name fip-adf \
  --resource-group fip-prod-rg \
  --query "[0]" \
  -o table

# Check for errors
az datafactory pipeline-run show \
  --factory-name fip-adf \
  --resource-group fip-prod-rg \
  --run-id YOUR_RUN_ID \
  | jq '.status, .failureMessage'

# If extraction fails:
# 1. Check ERP connectivity
ping erp-api.company.hu

# 2. Verify credentials in Key Vault
az keyvault secret show --vault-name fip-prod-kv --name erp-api-key

# 3. Check ADLS capacity
az storage account show-usage \
  --resource-group fip-prod-rg \
  --name fipprodstorage

# 4. Review detailed logs
az datafactory pipeline-run query-by-factory \
  --factory-name fip-adf \
  --filter-parameters "{'time': '2025-03-01'}" \
  -o json | jq '.value[0].runDimension'
```

**Expected Output**:
- Files copied to ADLS: `/raw/2025/03/gl_transactions.parquet` (~50MB)
- Row counts logged: GL: 500K, AP: 300K, AR: 200K
- No transformation errors

### Hour 2: Data Transformation (3:00 - 4:00 AM)

**Process**: dbt transforms Bronze → Silver → Gold

```bash
# Monitor dbt run (running on Databricks)
# Method 1: Check dbt artifacts
tail -f financial_dbt/logs/dbt.log

# Method 2: Check Databricks job run
databricks jobs get-run --job-id 123456 | jq '.state'

# If transformation fails:

# 1. Check dbt compilation
cd financial_dbt
dbt compile --select tag:critical

# 2. View compile errors
cat logs/dbt.log | grep ERROR

# 3. Check model SQL (look for syntax errors)
cat target/compiled/fip_models/models/marts/revenue_detail.sql

# 4. Test individual model
dbt run --select gold.revenue_metrics --profiles-dir ./

# 5. Check data freshness (source not updated?)
dbt freshness --select source:erp_system.gl_transactions

# 6. Rerun with verbose logging
dbt run -v --select tag:critical 2>&1 | tee troubleshoot.log
```

**Expected Output**:
- Models run: 45 models
- Tests passed: 102 tests
- Execution time: 25 minutes
- No failures or warnings

### Hour 3: Quality Validation (4:00 - 5:00 AM)

**Pipeline**: `pl_dq_validation`

```bash
# Run data quality checks
python python/utils/run_data_quality_checks.py \
  --environment prod \
  --schema gold \
  --output json > dq_report.json

# Check results
cat dq_report.json | jq '.summary'

# If quality issues found:

# 1. Identify problematic tables
jq '.failures[] | select(.severity=="HIGH")' dq_report.json

# 2. Query sample bad data
sqlcmd -S fip-server.sql.azuresynapse.net -d fip_dw <<EOF
-- Find NULL values where not expected
SELECT TOP 100 * FROM gold.revenue_metrics
WHERE customer_key IS NULL
  OR revenue_amount IS NULL;
EOF

# 3. Check source data
SELECT TOP 100 * FROM bronze.gl_transactions
WHERE gl_account IS NULL;

# 4. Review transformations that could introduce NULLs
grep -n "ISNULL\|COALESCE\|CASE WHEN" financial_dbt/models/marts/*.sql

# 5. If acceptable, document exception
python scripts/document_dq_exception.py \
  --test_id "NULL_IN_GL_ACCOUNT" \
  --reason "Legacy system sends nulls for historical accounts" \
  --approved_by "CFO"
```

**Expected Output**:
- All critical checks pass
- No rows with unexpected NULLs
- Data volume growth < 5% (unexpected growth flags potential duplicates)
- Aggregate totals match GL balances (within 0.01%)

### Hour 4: AI Commentary (5:00 - 6:00 AM)

**Process**: Generate financial narratives for multiple audiences

```bash
# Run commentary generation
python python/commentary/generate_commentary.py \
  --period "2025-03" \
  --audiences "CFO,CEO,BOARD,INVESTOR" \
  --language "en,hu" \
  --output-format markdown

# Check generated files
ls -la output/commentary_2025-03_*

# If commentary generation fails:

# 1. Check API connectivity
curl -H "Authorization: Bearer $AZURE_OPENAI_API_KEY" \
  https://fip-prod-openai.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2024-02-01

# 2. Check model deployment
az cognitiveservices account deployment show \
  --resource-group fip-prod-rg \
  --name fip-prod-openai \
  --deployment-name gpt-4o

# 3. Verify data available for commentary
SELECT COUNT(*) FROM gold.revenue_metrics WHERE fiscal_month = 202503;
SELECT COUNT(*) FROM gold.budget_variance WHERE fiscal_month = 202503;

# 4. Check token limits (if query takes too long)
# Reduce lookback period or data granularity

# 5. Review logs
tail -100 python/logs/commentary.log
```

**Expected Output**:
- 4 narratives generated (CFO, CEO, Board, Investor)
- 2 languages (English + Hungarian)
- ~2000 words per narrative
- Stored in database and email queue

### Hour 5: Power BI Refresh (6:00 - 6:30 AM)

**Process**: Power BI automatically refreshes from Gold schema

```bash
# Check Power BI service refresh status
# Portal: Power BI Service → Workspaces → FIP Analytics → Settings → Refresh
# Expected: Dataset refresh completed, last run < 10 minutes ago

# Manual refresh (if needed)
python scripts/refresh_power_bi.py \
  --workspace "FIP Analytics" \
  --datasets "all"

# If refresh fails:

# 1. Check Synapse SQL endpoint
sqlcmd -S fip-server.sql.azuresynapse.net -d fip_dw -Q "SELECT 1"

# 2. Verify Power BI gateway connection
# Portal: Power BI → Settings → Gateways → On-premises data gateway
# Check: Status "Online", last heartbeat < 1 min ago

# 3. Check Power BI dataset settings
# Portal: Power BI → Dataset settings → Data source credentials
# Verify: Credentials not expired, correct database name

# 4. Review refresh logs
# Portal: Power BI → Dataset settings → Refresh history
# View: Error details, credentials, execution time

# 5. Manual data source test
# PowerBI Desktop → Get Data → SQL Server
# Test connection with correct credentials
```

**Expected Output**:
- All datasets refresh succeeded
- Revenue report loads in < 3 seconds
- Dashboard tiles update within 2 hours

### Hour 6-8: Distribution & Validation (6:30 - 10:00 AM)

```bash
# 1. Send stakeholder notifications
python scripts/send_close_notifications.py \
  --period "2025-03" \
  --recipients "finance-team@company.hu,cfo@company.hu"

# 2. Validate reports match source data
# Spot-check: Pick 5 GL accounts, verify Gold balance = GL detail sum
SELECT
  account_key,
  SUM(amount) AS gold_balance
FROM gold.gl_balance_detail
WHERE fiscal_month = 202503
GROUP BY account_key
ORDER BY ABS(gold_balance) DESC
LIMIT 5;

-- Compare to GL master
SELECT account_key, closing_balance FROM config.gl_master WHERE month = 202503;

# 3. Finance team manual review
# Action: Finance team signs off on close in spreadsheet
# Requirement: < 0.1% variance to GL master

# 4. Archive close outputs
tar -czf close_2025-03_$(date +%Y%m%d_%H%M%S).tar.gz \
  output/commentary_* \
  output/reports_* \
  dq_report.json

# 5. Update close checklist
# Go to: Confluence → Monthly Close Checklist → March 2025
# Mark: All sections complete, signed by CFO

# 6. Schedule retrospective
# Meeting: "March 2025 Close Retrospective"
# Time: Next day, 10:00 AM
# Attendees: Data team, Finance team, DevOps
```

---

## Common Issues & Solutions

### Issue: Pipeline Fails with "Connection Refused"

**Error message**:
```
ActivityFailure: Connection refused to ERP API
```

**Diagnostics**:
```bash
# 1. Test ERP API connectivity
curl -v https://erp-api.company.hu/api/gl/transactions

# 2. Check firewall rules
az network nsg rule list --resource-group fip-prod-rg --nsg-name fip-nsg

# 3. Verify VPN/ExpressRoute status
az network express-route show --resource-group fip-prod-rg --name er-fip

# 4. Check ADF Integration Runtime
az datafactory integration-runtime list --factory-name fip-adf --resource-group fip-prod-rg
```

**Solutions**:

1. **If ERP API down**: Contact ERP admin, wait for restart
2. **If firewall blocked**: Add ADF IP to ERP firewall whitelist
3. **If VPN down**: Restart ExpressRoute connection via portal
4. **If Integration Runtime offline**: Restart the self-hosted IR

### Issue: dbt Test Fails: "Duplicate Rows in Key Field"

**Error message**:
```
dbt test: dbt_expectations.expect_table_columns_to_match_ordered_list
FAILED: 123 rows with duplicate keys
```

**Diagnostics**:
```bash
# 1. Identify duplicates
SELECT
  source_id,
  COUNT(*) AS duplicate_count
FROM bronze.gl_transactions
GROUP BY source_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;

# 2. Check source extraction
# Were records extracted twice from ERP?
SELECT COUNT(*) FROM bronze.gl_transactions WHERE source_id IN ('12345');

# 3. Check dbt seed data (reference data)
SELECT source_id, COUNT(*) FROM [seeds.dim_gl_account] GROUP BY source_id;
```

**Solutions**:

1. **If duplicates in source**:
   ```sql
   -- Remove duplicates (keep first occurrence)
   WITH cte AS (
     SELECT *,
       ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY _loaded_at) AS rn
     FROM bronze.gl_transactions
   )
   DELETE FROM cte WHERE rn > 1;
   ```

2. **If duplicates in reference data**:
   ```bash
   # Regenerate seed
   dbt seed --select [seed_name] --full-refresh
   ```

3. **Rerun dbt test**:
   ```bash
   dbt test --select dbt_expectations.expect_table_columns_to_match_ordered_list
   ```

### Issue: SQL Query Times Out (> 30 min)

**Symptom**: Power BI refresh hangs, dashboard unresponsive

**Diagnostics**:
```bash
# 1. Check query execution plan
SET STATISTICS TIME ON;
SELECT * FROM gold.revenue_metrics WHERE fiscal_month = 202503;

# 2. Check index fragmentation
SELECT
  OBJECT_NAME(ps.object_id) AS [Table],
  i.name AS [Index],
  ps.avg_fragmentation_in_percent,
  ps.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps
INNER JOIN sys.indexes i ON ps.object_id = i.object_id
  AND ps.index_id = i.index_id
WHERE ps.avg_fragmentation_in_percent > 10
ORDER BY ps.avg_fragmentation_in_percent DESC;

# 3. Check table statistics
DBCC SHOW_STATISTICS (gold.revenue_metrics, pk_revenue_metrics);

# 4. Look for missing indexes
SELECT
  CONVERT(decimal(18,2), user_seeks * avg_total_user_cost * (avg_user_impact * 0.01))
    AS improvement_measure,
  mid.name AS index_name,
  id.equality_columns,
  id.inequality_columns
FROM sys.dm_db_missing_index_details id
INNER JOIN sys.dm_db_missing_index_groups ig
  ON id.index_handle = ig.index_handle
INNER JOIN sys.dm_db_missing_index_groups_stats igs
  ON ig.index_group_id = igs.group_handle
ORDER BY improvement_measure DESC;
```

**Solutions**:

1. **Rebuild fragmented indexes**:
   ```sql
   ALTER INDEX idx_revenue_fact REBUILD;
   -- Or reorganize if < 30% fragmented
   ALTER INDEX idx_revenue_fact REORGANIZE;
   ```

2. **Update statistics**:
   ```sql
   UPDATE STATISTICS gold.revenue_metrics;
   ```

3. **Add recommended indexes**:
   ```sql
   CREATE INDEX idx_revenue_customer_date
   ON gold.revenue_metrics (customer_key, revenue_date)
   INCLUDE (revenue_amount);
   ```

4. **Reduce query complexity**:
   - Materialize intermediate results
   - Create aggregated table for Power BI
   - Use query caching

5. **Scale up resources**:
   ```bash
   # Increase DWU (Data Warehouse Units)
   az sql dw update \
     --resource-group fip-prod-rg \
     --server fip-server \
     --name fip_dw \
     --service-objective DW1000c  # From DW500c
   ```

### Issue: Python Script ImportError

**Error message**:
```
ModuleNotFoundError: No module named 'langchain'
```

**Solutions**:

```bash
# 1. Activate virtual environment
source venv/bin/activate

# 2. Reinstall dependencies
pip install -r python/requirements.txt --upgrade

# 3. Check installed versions
pip list | grep langchain

# 4. Run again
python python/commentary/generate_commentary.py
```

### Issue: Azure OpenAI Rate Limit Exceeded

**Error message**:
```
RateLimitError: Quota exceeded for model gpt-4o
```

**Diagnostics**:
```bash
# Check quota usage
az cognitiveservices account list-usages \
  --resource-group fip-prod-rg \
  --name fip-prod-openai \
  | jq '.value[] | select(.name.value=="Gpt4TokensPerMinute")'
```

**Solutions**:

1. **Request quota increase**: Contact Azure support
2. **Implement rate limiting**: Space out API calls
   ```python
   import time
   for item in items:
     response = openai_client.chat.completions.create(...)
     time.sleep(2)  # 2-second delay between calls
   ```
3. **Use batch API**: Azure OpenAI batch endpoint (cheaper, slower)

---

## Monitoring & Alerts

### Key Metrics to Monitor

| Metric | Target | Check Interval | Alert Threshold |
|--------|--------|---|---|
| **Pipeline Success Rate** | > 99% | Daily | < 95% |
| **Data Freshness** | < 4 hours | Hourly | > 6 hours |
| **Query Performance (P99)** | < 5 seconds | Hourly | > 30 seconds |
| **Synapse DWU Utilization** | 60-80% | Hourly | > 95% (scale up) |
| **Backup Success** | 100% | Daily | Any failure |
| **Error Rate** | < 0.1% | Hourly | > 1% |

### Setting Up Alerts (Application Insights)

```bash
# Alert: Pipeline failure
az monitor metrics alert create \
  --name "ADF Pipeline Failure Alert" \
  --resource-group fip-prod-rg \
  --resource fip-adf \
  --condition "avg ActivityFailures > 0" \
  --window-size 1h \
  --evaluation-frequency 5m \
  --action "email" \
  --email-address "devops@company.hu"

# Alert: High query latency
az monitor metrics alert create \
  --name "Synapse Query Latency High" \
  --resource-group fip-prod-rg \
  --resource fip-server \
  --condition "avg QueryExecutionTime > 30000" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --action "pagerduty" \
  --webhook-url "https://events.pagerduty.com/..."
```

### Dashboard Creation (Power BI)

```
Create monitoring dashboard:
- Pipeline execution status (daily trend)
- Data quality score (0-100)
- Query performance (histogram)
- Resource utilization (CPU, memory, storage)
- User activity (access log heat map)

Refresh: Hourly
Audience: Data ops, platform team
```

---

## Performance Troubleshooting

### Slow dbt Run

```bash
# 1. Profile execution time
dbt run --debug 2>&1 | grep "Completed in"

# 2. Identify slow models
dbt run-operation generate_model_yaml  # Show timing per model

# 3. Reduce model count
dbt run --select tag:critical  # Run only critical models

# 4. Check Databricks cluster
# Portal: Databricks → Clusters → Driver/Worker utilization
# Action: Scale up cluster size or increase worker count

# 5. Materalization optimization
# Change ephemeral models to view/table:
{{ config(materialized='view') }}  # For small tables used once
{{ config(materialized='table') }}  # For large tables used repeatedly
```

### Slow Power BI Dashboard

```bash
# 1. Check data model
# PowerBI Desktop → Model view → Check relationships and table size

# 2. Reduce granularity
# Instead of: daily revenue by customer (1M rows)
# Use: weekly revenue by customer (52K rows)

# 3. Add aggregation tables
-- Pre-aggregate for dashboard
CREATE TABLE gold.revenue_daily_summary AS
SELECT
  DATE_TRUNC(DAY, revenue_date) AS revenue_date,
  customer_key,
  SUM(revenue_amount) AS daily_revenue
FROM gold.revenue_metrics
GROUP BY DATE_TRUNC(DAY, revenue_date), customer_key;

# 4. Cache queries
# PowerBI: File → Options & Settings → Performance Analyzer
# Enable: "Save query result cache"
```

---

## Backup & Recovery

### Backup Status Check

```bash
# Automated backups (Azure-managed)
az sql db backup list \
  --resource-group fip-prod-rg \
  --server fip-server \
  --database fip_dw \
  --query "[0:5]" \
  -o table

# Expected: Multiple backups per day (hourly), all successful
```

### Point-in-Time Restore

```bash
# Restore to previous point (e.g., before accidental deletion)
az sql db restore \
  --resource-group fip-prod-rg \
  --server fip-server \
  --name fip_dw_restored \
  --source-server fip-server \
  --source-database fip_dw \
  --restore-point-in-time "2025-03-15T12:00:00Z"

# Wait for restore to complete (~5-10 minutes)
az sql db show \
  --resource-group fip-prod-rg \
  --server fip-server \
  --name fip_dw_restored \
  --query "status"

# Verify data
sqlcmd -S fip-server.sql.azuresynapse.net -d fip_dw_restored -Q "SELECT COUNT(*) FROM gold.revenue_metrics"

# Swap with production (if restore is good)
# Manual: Update connection strings to restored database
# Or: Rename databases (production → _old, restored → production)
```

---

## Escalation Procedures

### On-Call Escalation Levels

**Level 1** (Support team): Data access issues, password resets, documentation

**Level 2** (Data engineering): Query performance, pipeline debugging, dbt errors

**Level 3** (Infrastructure team): Azure resource issues, networking, backups

**Level 4** (Leadership): High-impact incidents, compliance breaches, customer impact

### Incident Escalation

```
Issue detected (Auto alert or manual report)
  │
  ├─► Severity Low (P4): Support team, SLA 48h
  │
  ├─► Severity Medium (P3): Data engineering, SLA 8h
  │
  ├─► Severity High (P2): Infrastructure + Data eng, SLA 4h
  │   └─► Page on-call engineer via PagerDuty
  │
  └─► Severity Critical (P1): All hands, SLA 1h
      ├─► Declare incident in Slack
      ├─► Create war room call (Zoom)
      ├─► Assign incident commander
      └─► Notify VP of Engineering
```

**Contact Information**:
```
On-Call Data Engineer: PagerDuty schedule "FIP Data"
On-Call DevOps: PagerDuty schedule "FIP Infrastructure"
VP Engineering: @vp-engineering (Slack)
Security Team: security@company.hu
```

---

**Last Updated**: April 2025 | **Next Review**: October 2025

**Questions?** Contact Data Operations team | data-ops@company.hu
