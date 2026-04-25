# Copilot Instructions — Financial Intelligence Platform (FIP)

## Purpose

These instructions guide GitHub Copilot across all files in this repository.
The FIP is a multi-tenant financial analytics platform targeting **Hungarian GAAP (HU GAAP)**
compliance, built on Azure (Synapse Analytics, ADF, Key Vault, Databricks) with Python,
dbt, Bicep, and Power BI.

---

## 1. Security — Critical Issues

- **Never** hardcode secrets, connection strings, API keys, or credentials; all secrets must
  be read from Azure Key Vault or environment variables.
- Prevent SQL injection: use parameterised queries or SQLAlchemy ORM — never f-string or
  `.format()` interpolation into raw SQL.
- Every API endpoint that touches financial data must validate the `TenantContext` (see
  `python/tenant_router.py`) before touching any database.
- Validate and sanitise all user-supplied inputs (amounts, account codes, date ranges)
  before use in queries or AI prompts.
- JWT tokens must be verified with `pyjwt`; never decode without signature verification.
- Row-Level Security (RLS) filters must be present on every Synapse query that returns
  tenant or entity data.

---

## 2. Multi-Tenant Architecture

- All requests **must** flow through `TenantRouter`; cross-tenant data leakage is the
  highest-severity defect category.
- `TenantContext` objects must be constructed once per request and propagated explicitly —
  never stored in global or module-level variables.
- Database/schema names are tenant-specific; always derive them from `TenantDatabase`
  rather than hardcoding.
- Unit tests for tenant-aware code must verify that one tenant cannot access another
  tenant's data (use `pytest` fixtures from `python/tests/conftest.py`).

---

## 3. Python Standards

- **Python 3.10+** is the minimum (CI workflow pins 3.10; `requirements.txt` targets 3.11+; new code should target 3.11+ to use the latest type-annotation syntax, but must not break on 3.10).
- Follow PEP 8; the CI linter is `flake8` with `--max-line-length=127`.
- Use type annotations for all function signatures and public class attributes.
- Functions should be focused and under 50 lines; split larger functions into helpers.
- Use `structlog` for structured JSON logging; never use `print()` in production code.
- Use `tenacity` for retry logic around Azure SDK and OpenAI API calls.
- Handle Azure SDK exceptions explicitly (`azure.core.exceptions.*`); do not swallow
  broad `Exception` unless re-raised or logged with full context.
- Async FastAPI endpoints must `await` all I/O; never call blocking code from async
  context without `asyncio.to_thread`.

---

## 4. SQL / dbt Standards

- Add model `description` entries and tests in the appropriate zone-specific dbt docs file
  (for example `staging_schema.yml`, `intermediate_schema.yml`, or `gold_schema.yml`),
  rather than assuming a file literally named `schema.yml`. Configure model `schema`
  (via `+schema:`) and repo-level tags in `dbt/dbt_project.yml`.
- Staging models materialise as **views**; Gold Zone models as **tables** with
  `GRANT SELECT ON {{ this }} TO powerbi_reader` post-hooks.
- HU GAAP fiscal year starts January 1; use `vars.fiscal_year_start_month` — never
  hardcode the month.
- Reporting currency is HUF; FX conversion must use MNB (Magyar Nemzeti Bank) rates via the
  `nbh_api_url` variable — never a static rate.
- Tests in `dbt/Tests/` must cover `not_null`, `unique`, and `accepted_values` for every
  primary-key and status column; test severity is `error` (blocking).
- Avoid `SELECT *` in production models; list columns explicitly.
- Use `{{ ref() }}` and `{{ source() }}` macros; never hardcode schema-qualified table
  names in model SQL.

---

## 5. Azure Infrastructure (Bicep / ADF)

- All Bicep modules must accept `location`, `resourcePrefix`, and `tags` parameters; never
  hardcode resource names.
- Key Vault references (`@Microsoft.KeyVault(...)`) must be used for secrets in ARM/Bicep
  — never plaintext parameter values.
- ADF pipelines that move financial data must include a Data Quality validation activity
  (`pl_dq_validation`) before loading to Silver or Gold zones.
- Storage accounts and Synapse workspaces must have public-network-access disabled by
  default; use private endpoints.

---

## 6. Financial Domain Rules

- All monetary amounts must be stored and computed in **HUF** unless a currency field is
  explicitly present.
- Materiality thresholds: revenue anomalies > 0.5% (`materiality_threshold_revenue`),
  OPEX anomalies > 2.0% (`materiality_threshold_opex`).
- Late-entry threshold is 5 calendar days past period-end (`late_entry_threshold_days`);
  flag but do not reject late postings.
- Z-score threshold for anomaly detection is 2.5 (`anomaly_zscore_threshold`).
- HU GAAP chart-of-accounts mapping must use `ref_coa_mapping` seed; never create ad-hoc
  account mappings inline.

---

## 7. Performance

- Avoid N+1 queries; batch database calls and use `pandas` bulk reads where possible.
- Cache Key Vault secret reads with `@lru_cache` or `functools.cache`; secrets do not
  change between requests.
- Use `asyncio`/`httpx` for concurrent external API calls (e.g. MNB FX, OpenAI).
- dbt incremental models (`+incremental_strategy: merge`) must be used for large fact
  tables; full refresh is only acceptable for seed/reference tables.

---

## 8. Testing Standards

- New features require unit tests in `python/tests/` using `pytest`.
- Mock all Azure services (Key Vault, Synapse, Blob Storage) with `unittest.mock.MagicMock`
  or `pytest-mock`; never make live Azure calls in unit tests.
- Use the shared stubs in `python/tests/conftest.py` for `db_utils` and set
  `AZURE_KEY_VAULT_URL` via `os.environ.setdefault` before importing modules.
- Test names must follow the pattern `test_<function>_<scenario>` (e.g.
  `test_detect_anomalies_returns_empty_list_when_no_data`).
- Tests must cover happy-path, error/exception paths, and boundary conditions.
- Run `pytest python/tests/` locally before opening a pull request.

---

## 9. Code Review Style

- Be specific and actionable; reference line numbers and suggest concrete fixes.
- Explain the *why* behind each recommendation.
- Acknowledge good patterns — especially correct tenant-isolation and RLS usage.
- Flag any deviation from HU GAAP rules, tenant-isolation requirements, or secret-handling
  policies as **blocking** issues.
- Ask clarifying questions when the business intent of a financial calculation is unclear.

---

Always prioritise security vulnerabilities (especially tenant data leakage) and financial
accuracy issues above all other review concerns.
