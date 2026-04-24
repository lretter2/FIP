# IR-001 — Data Pipeline Failure

**Incident Type:** ADF Pipeline / dbt Build Failure  
**Severity:** P1 (Gold Zone unavailable) / P2 (delayed refresh)  
**Owner:** Platform Engineering + Finance DQ team  
**Last Reviewed:** 2026-04-10

---

## 1. Symptoms

- Power BI dashboards show stale data or "Refresh failed" banner
- ADF Monitor shows pipeline run in `Failed` state
- dbt test alerts in Azure Monitor / Teams channel `#fip-data-alerts`
- Gold Zone tables have `last_refresh` timestamp > 4 hours behind SLA

---

## 2. Immediate Triage (first 15 minutes)

1. **Check ADF Monitor** → open the failed pipeline run → identify the first failed activity
2. **Check dbt test results** in the Azure Synapse workspace or dbt Cloud run logs
3. **Check Azure Monitor Alerts** for Synapse SQL pool resource exhaustion or connectivity issues
4. **Verify Bronze landing zone** — confirm SFTP/REST ingestion completed for the relevant batch

### Quick commands

```bash
# Check latest batch status in audit log (run in Synapse Studio)
SELECT TOP 20 *
FROM audit.pipeline_run_log
ORDER BY run_start_time DESC;

# Check DQ rule failures for the latest batch
SELECT *
FROM audit.dq_rule_results
WHERE batch_id = '<latest_batch_id>'
  AND status   = 'FAIL'
ORDER BY rule_id;
```

---

## 3. Common Failure Scenarios

| Failure | Likely Cause | Remediation |
|---|---|---|
| DQ-001 FAIL (imbalance) | ERP export truncated mid-batch | Re-trigger ERP connector; delete partial batch |
| DQ-006 ERROR (invalid currency) | New currency code not in `ref_currencies` seed | Add code to seed CSV; run `dbt seed` |
| FX rate WARN (DQ-007) | NBH rate feed not refreshed | Manually trigger `pl_nbh_fx_rate_refresh` ADF pipeline |
| Synapse connection timeout | SQL pool paused (dev environment) | Resume pool via Azure Portal or CLI |
| ADF trigger misfired | Daylight saving time edge case | Verify `trg_monthly_close.json` fired with correct `period_month` |

---

## 4. Escalation

| Severity | Escalate to | SLA |
|---|---|---|
| P1 — Gold Zone down > 2 h | Platform Engineering on-call | 15 min response |
| P1 — Data corruption confirmed | Finance Controller + Audit team | Immediate |
| P2 — Delayed refresh | Platform Engineering (business hours) | 4 h response |
| DQ ERROR blocking Gold refresh | Finance DQ team lead | 2 h response |

---

## 5. Post-Incident

- Document root cause in `audit.incident_log` table
- Update this runbook if a new failure pattern was encountered
- File a dbt test gap if the failure was not caught before Gold Zone load
