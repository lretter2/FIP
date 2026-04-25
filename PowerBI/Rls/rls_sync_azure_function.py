"""
rls_sync_azure_function.py
FIP Power BI RLS Sync — Azure Function HTTP Trigger Wrapper
Phase 3 CI/CD Deliverable

PURPOSE
-------
Wraps sync_rls_aad.py as an Azure Function with HTTP trigger so that ADF
can invoke RLS synchronisation via a Web Activity (no custom container or
Azure Batch required).

DEPLOYMENT
----------
1. Create Azure Function App (Python 3.11, Consumption plan)
   az functionapp create --name fip-rls-sync-func --resource-group fip-rg \
       --storage-account fipstorage --consumption-plan-location westeurope \
       --runtime python --runtime-version 3.11 --functions-version 4

2. Set application settings (Key Vault references preferred):
   AZURE_TENANT_ID          = @Microsoft.KeyVault(SecretUri=...)
   AZURE_CLIENT_ID          = @Microsoft.KeyVault(SecretUri=...)
   AZURE_CLIENT_SECRET      = @Microsoft.KeyVault(SecretUri=...)
   POWERBI_PROD_WORKSPACE_ID = @Microsoft.KeyVault(SecretUri=...)
   POWERBI_PROD_DATASET_ID   = @Microsoft.KeyVault(SecretUri=...)

3. Deploy function code:
   func azure functionapp publish fip-rls-sync-func

4. Get function URL and store in ADF global parameter: rls_sync_function_url
   func azure functionapp list-functions fip-rls-sync-func --show-keys

HTTP INTERFACE
--------------
POST /api/rls_sync
Body (JSON):
    {
        "dry_run":     false,          // optional, default false
        "role_filter": null,           // optional, sync only this role
        "include_log": false           // optional, return captured log (redacted); default false
    }

Response (JSON):
    {
        "sync_status":  "success",
        "roles_synced": 5,
        "total_changes": 3,
        "timestamp":    "2026-04-10T05:00:12Z",
        "detail":       [...],
        "log":          "<redacted log — only present when include_log=true>"
    }

Response on failure (HTTP 500):
    {
        "sync_status":   "failed",
        "error_message": "...",
        "timestamp":     "...",
        "log":           "<redacted log — only present when include_log=true>"
    }

SECURITY NOTES
--------------
- Log output may contain UPNs/email addresses (PII). Log is only returned
  when the caller explicitly sets include_log=true, is redacted (emails
  replaced with <redacted@domain>) and truncated to LOG_MAX_CHARS characters.
- The x-functions-key header authenticates callers. ADF WebActivity should
  store the key in a Key Vault-linked ADF parameter, not in plaintext.
"""

import json
import logging
import os
import re
import sys
import io
from datetime import datetime, timezone

import azure.functions as func

# Add parent dir so sync_rls_aad module is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Maximum characters of log text returned in responses (prevents very large payloads).
LOG_MAX_CHARS = 8_000

# Regex matching email addresses so they can be redacted before leaving the function.
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def _sanitise_log(raw: str) -> str:
    """Redact email addresses and truncate the captured log to LOG_MAX_CHARS."""
    redacted = _EMAIL_RE.sub(lambda m: f"<redacted@{m.group(0).split('@', 1)[1]}>", raw)
    if len(redacted) > LOG_MAX_CHARS:
        redacted = redacted[-LOG_MAX_CHARS:]  # keep the most-recent (tail) portion
        redacted = f"[...truncated to last {LOG_MAX_CHARS} chars...]\n" + redacted
    return redacted


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function entry point — HTTP trigger."""
    start_ts = datetime.now(timezone.utc)
    logging.info("FIP RLS Sync function triggered")

    # Parse request body
    try:
        body        = req.get_json() if req.get_body() else {}
        dry_run     = bool(body.get("dry_run", False))
        role_filter = body.get("role_filter", None)
        include_log = bool(body.get("include_log", False))
    except ValueError:
        dry_run     = False
        role_filter = None
        include_log = False

    # Capture log output for response
    log_stream  = io.StringIO()
    log_handler = logging.StreamHandler(log_stream)
    log_handler.setLevel(logging.INFO)
    logging.getLogger("FIP.RLSSync").addHandler(log_handler)

    try:
        # Import and run the sync logic
        from sync_rls_aad import sync_roles

        # Monkey-patch to capture statistics
        changes_tracker = {"total": 0, "roles": []}
        original_log_fn = None

        import sync_rls_aad as _rls_module
        original_log = _rls_module.log_sync_result

        def patched_log(role, added, removed, dr):
            changes_tracker["total"] += len(added) + len(removed)
            changes_tracker["roles"].append({
                "role": role,
                "added_count": len(added),
                "removed_count": len(removed),
                "dry_run": dr,
            })
            original_log(role, added, removed, dr)

        _rls_module.log_sync_result = patched_log

        sync_roles(role_filter=role_filter, dry_run=dry_run)

        _rls_module.log_sync_result = original_log

        response_body = {
            "sync_status":    "success",
            "roles_synced":   len(changes_tracker["roles"]),
            "total_changes":  changes_tracker["total"],
            "dry_run":        dry_run,
            "timestamp":      start_ts.isoformat(),
            "detail":         changes_tracker["roles"],
        }
        if include_log:
            response_body["log"] = _sanitise_log(log_stream.getvalue())

        logging.info(f"RLS sync succeeded: {changes_tracker['total']} changes across {len(changes_tracker['roles'])} roles")

        return func.HttpResponse(
            json.dumps(response_body),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as exc:
        logging.error(f"RLS sync failed: {exc}", exc_info=True)
        response_body = {
            "sync_status":   "failed",
            "error_message": str(exc),
            "timestamp":     start_ts.isoformat(),
        }
        if include_log:
            response_body["log"] = _sanitise_log(log_stream.getvalue())
        return func.HttpResponse(
            json.dumps(response_body),
            status_code=500,
            mimetype="application/json",
        )
