# IR-002 — Missing FX Rate (DQ-007 Alert)

**Incident Type:** Data Quality Warning — NBH FX Rate Gap  
**Severity:** P2 (EUR/USD KPIs will be incorrect until resolved)  
**Owner:** Finance Operations  
**Last Reviewed:** 2026-04-10

---

## 1. Symptoms

- dbt test `assert_fx_rates_available` returns rows (WARN severity)
- Azure Monitor alert fires on metric `dq_warn_count > 0` for rule DQ-007
- EUR-denominated KPIs in Power BI show unexpected flat values or zeros

---

## 2. Root Cause Options

1. **NBH (Magyar Nemzeti Bank) API was unreachable** during the nightly rate refresh
2. **Weekend / public holiday gap** — the `pl_nbh_fx_rate_refresh` pipeline did not back-fill correctly
3. **New currency code** posted in transactions but not yet in `config.ref_fx_rates`
4. **Rate table truncated** by a failed `dbt seed` or ETL load

---

## 3. Remediation Steps

### Option A — Re-trigger the rate refresh pipeline

```
ADF → Author → Pipelines → pl_nbh_fx_rate_refresh → Trigger Now
Parameters: rate_date = <missing date in YYYY-MM-DD format>
```

### Option B — Manual back-fill via Synapse Studio

```sql
-- Insert missing rate manually (use NBH official closing rate)
INSERT INTO config.ref_fx_rates (currency_code, rate_date, rate_to_huf, source)
VALUES ('EUR', '2026-03-28', 398.45, 'NBH_MANUAL_BACKFILL');
```

### Option C — Re-run `dbt seed` if the rates table was truncated

```bash
dbt seed --select ref_fx_rates --full-refresh
```

---

## 4. Verification

After remediation, re-run the dbt test to confirm resolution:

```bash
dbt test --select assert_fx_rates_available
```

Expected result: 0 rows returned (all non-HUF transactions now have a rate).

---

## 5. Prevention

- Monitor NBH API availability via Azure Monitor HTTP probe
- Enable `pl_nbh_fx_rate_refresh` retry policy (currently: 3 retries, 30 min interval)
- Consider caching the previous business day's rate as a fallback in `currency_convert.sql`
