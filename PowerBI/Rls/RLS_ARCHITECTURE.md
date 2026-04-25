# Power BI RLS Architecture
**Financial Intelligence Platform · Phase 3 · HU GAAP**

---

## Canonical Persona and Role Set
The supported Power BI dataset persona set is:
- `CEO`
- `CFO`
- `Controller`
- `CostCentreManager`
- `Auditor`

`Board` and `Investor` are reporting audiences, not separate dataset RLS roles.
They consume curated CEO/CFO views and approved commentary slices.

---

## Two-Layer Security Model

```
Layer 1 — Azure Active Directory
  AAD Groups (managed by IT)
    └─ FIP-Role-CEO
    └─ FIP-Role-CFO
    └─ FIP-Role-Controller
    └─ FIP-Role-CostCentreManager
    └─ FIP-Role-ExternalAuditor

        ↓ sync_rls_aad.py

Layer 2 — Power BI Dataset RLS
  Dataset Roles (declared in rls_roles.json)
    └─ CEO
    └─ CFO
    └─ Controller
    └─ CostCentreManager
    └─ Auditor
```

---

## RLS Filter Matrix

| Role | DAX Filter | Propagation |
|---|---|---|
| `CEO` | `silver_dim_entity[consolidation_group] IN VALUES(config_ref_entity_master[consolidation_group])` | Group-level access |
| `CFO` | *(no filter — sees all entities in dataset scope; intentional per spec v1.1)* | Full entity access |
| `Controller` | *(no filter — sees all entities in dataset scope; intentional per spec v1.1)* | Full entity and audit-access visuals |
| `CostCentreManager` | `silver_dim_cost_centre[manager_name] = USERPRINCIPALNAME()` | Cost-centre scoped |
| `Auditor` | `silver_dim_entity[entity_code] IN {"ACME_HU", "BETA_HU"}` | Engagement-scoped read-only |

---

## CostCentreManager Notes
Applied at `silver_dim_cost_centre` level.

Relationship chain:
`silver_dim_cost_centre` <-> `silver_fact_gl_transaction` (cost_centre_key) <-> `gold_fact_gl_transaction`

Because `gold.agg_pl_monthly` is entity-level, CostCentreManager visuals requiring CC-level slicing must use `gold.fact_gl_transaction`-based measures.

---

## Sync Process
Daily at 05:00 CET:
1. Read `rls_roles.json`
2. Resolve AAD group members via Microsoft Graph
3. Compare to current dataset role members
4. Update role memberships via Power BI REST API
5. Write audit log to `rls_sync_audit.jsonl`

Manual run:
```bash
python PowerBI/Rls/sync_rls_aad.py --dry-run
python PowerBI/Rls/sync_rls_aad.py
python PowerBI/Rls/sync_rls_aad.py --role CFO
```

---

## Compliance Rules
1. Quarterly access review of `rls_sync_audit.jsonl` by Controller.
2. Auditor access must be time-bounded; remove from `FIP-Role-ExternalAuditor` post-audit.
3. Separation of duties: avoid overlapping `CostCentreManager` + `Controller` memberships.
4. Break-glass manual changes must be incident-logged.
