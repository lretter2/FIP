# Power BI Report Source of Truth

Current decision (2026-04-20):
- The canonical report template artifact is `PowerBI/FIP_PowerBI_Template.pbit`.
- A full PBIX/PBIP project is not currently planned as the checked-in source artifact.

Implications:
- RLS roles and security policy are declared in `PowerBI/Rls/rls_roles.json`.
- Dataset semantic changes are tracked via TMSL and DAX assets in `PowerBI/Dax_measures/`.
- CI/CD must treat `.pbit` + TMSL + RLS JSON as the deployable source bundle.

If the project moves to PBIP in the future, update this file and the CI/CD pipeline accordingly.

---

## Known Architectural Gaps

### GAP-001: FIP_PowerBI_Template.pbit is binary and unauditable in git

**Status:** Open — tracked, no remediation required at this time.

`FIP_PowerBI_Template.pbit` is a binary Power BI template file. Binary files cannot be diffed or reviewed in pull requests, meaning visual and report-layer changes are not auditable through standard code review.

**Accepted risk:** The report layer is considered stable. DAX measures and TMSL assets are maintained in plain-text source files and are auditable in git. RLS role definitions and membership intent are declared in `PowerBI/Rls/rls_roles.json`, but the effective dataset role filters enforced in the deployed artifact are not currently fully validated or enforced from those text files alone.

**Future remediation:** Migrate to PBIP (Power BI Project) format, which stores report definitions as human-readable JSON/XML files and enables diff-based code review of visual and report-layer changes. When this migration occurs, update this file and the CI/CD pipeline accordingly.
