# Power BI Report Source of Truth

Current decision (2026-04-20):
- The canonical report template artifact is `PowerBI/FIP_PowerBI_Template.pbit`.
- A full PBIX/PBIP project is not currently planned as the checked-in source artifact.

Implications:
- RLS roles and security policy are declared in `PowerBI/Rls/rls_roles.json`.
- Dataset semantic changes are tracked via TMSL and DAX assets in `PowerBI/Dax_measures/`.
- CI/CD must treat `.pbit` + TMSL + RLS JSON as the deployable source bundle.

If the project moves to PBIP in the future, update this file and the CI/CD pipeline accordingly.
