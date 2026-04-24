# Data Dictionary Generation

`FIP_Data_Dictionary.xlsx` is a generated artifact.

- Source of truth: `sql/**/*.sql` and `dbt/Models/**/*.sql`
- Generator script: `python/tools/generate_data_dictionary.py`
- Regeneration command:

```powershell
python python/tools/generate_data_dictionary.py
```

Do not maintain the workbook manually. Regenerate it after schema/model changes.
