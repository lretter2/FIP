# FIP Power BI Dashboard Specifications
**Phase 3 Deliverable — 5 Persona Dashboards**
Financial Intelligence Platform · HU GAAP · Version 1.1

---

## Canonical Persona Decision
Supported dataset persona set and RLS roles:
- `CEO`
- `CFO`
- `Controller`
- `CostCentreManager`
- `Auditor`

`Board` and `Investor` are audience slices delivered through CEO/CFO report pages and approved commentary views, not standalone RLS roles.

---

## Dashboard Architecture Overview
All dashboards connect to a single certified dataset (`FIP_Main`) with composite model behavior.

```
Power BI Service (Premium P1)
├── Dataset: FIP_Main
│   ├── Gold facts/aggregates (DirectQuery)
│   └── Silver dimensions + selected refs (Import)
│
├── Dashboard 1: CEO Executive
├── Dashboard 2: CFO Finance
├── Dashboard 3: Controller Operations
├── Dashboard 4: Cost Centre Manager
└── Dashboard 5: Auditor Read-Only
```

RLS is enforced through `silver_dim_entity` and `silver_dim_cost_centre` depending on role.

---

## Persona-to-Role Map

| Dashboard | Primary Persona | Dataset RLS Role |
|---|---|---|
| CEO Executive | CEO (Board consumers share this view) | `CEO` |
| CFO Finance | CFO (Investor consumers share curated pages) | `CFO` |
| Controller Operations | Controller | `Controller` |
| Cost Centre Manager | BU/Department manager | `CostCentreManager` |
| Auditor Read-Only | External/statutory auditor | `Auditor` |

---

## RLS Role Matrix

| Role | Entities | Cost Centres | Audit Tables | PII Visible |
|---|---|---|---|---|
| `CEO` | Consolidation group | All | Summary only | No |
| `CFO` | All dataset entities | All | Full operational views | No |
| `Controller` | All dataset entities | All | Full (incl. quarantine-facing ops visuals) | Limited |
| `CostCentreManager` | Assigned scope | Assigned only | None | No |
| `Auditor` | Engagement-assigned entities | All | Read-only aggregates | No |

Source role mapping file: `PowerBI/Rls/rls_roles.json`.

---

## Report Source-of-Truth Policy
Current repository policy:
- **Template source-of-truth artifact:** `PowerBI/FIP_PowerBI_Template.pbit`
- A full `.pbix`/`.pbip` project is **not** currently committed as canonical source.

If the team later adopts PBIP, this section must be updated and CI/CD switched to PBIP-first validation.

---

## Publishing Schedule (unchanged)

| Dashboard | Workspace | Refresh | Owner |
|---|---|---|---|
| CEO Executive | FIP-Production | On pipeline trigger | Platform Eng |
| CFO Finance | FIP-Production | Every 4h + on-demand | Platform Eng |
| Controller Ops | FIP-Production | Near-real-time (push) | Platform Eng |
| Cost Centre Mgr | FIP-Production | Daily 06:00 CET | Platform Eng |
| Auditor ReadOnly | FIP-Audit | Daily 07:00 CET (audit period only) | Platform Eng |
