"""
FIP Power BI RLS — AAD Group Sync
====================================
Phase 3 deliverable · Financial Intelligence Platform

Reads rls_roles.json and syncs AAD group members into the corresponding
Power BI dataset RLS roles via the Power BI REST API.

This script runs:
  • Daily at 05:00 CET (before dashboard refresh) via ADF trigger
  • On-demand after any AAD group membership change
  • As part of the Azure DevOps CI/CD pipeline after PROD deployment

Why this script is needed
--------------------------
Power BI RLS role members must be managed via the API or admin portal.
Manually updating role membership when employees join/leave is error-prone
and a SOX ITGc control failure risk. This script makes RLS membership
declarative (defined in rls_roles.json) and auditable (all changes logged).

Usage
-----
    python sync_rls_aad.py                    # sync all roles
    python sync_rls_aad.py --role CFO         # sync single role
    python sync_rls_aad.py --dry-run          # show what would change

Environment variables (or .env file)
--------------------------------------
    AZURE_TENANT_ID
    AZURE_CLIENT_ID              Service principal with UserRead.All + Group.Read.All
    AZURE_CLIENT_SECRET
    POWERBI_PROD_WORKSPACE_ID
    POWERBI_PROD_DATASET_ID
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import date, datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("FIP.RLSSync")

ROLES_FILE = Path(__file__).parent / "rls_roles.json"


# ── Azure / Power BI auth helpers ────────────────────────────────────────────
def get_pbi_token() -> str:
    from azure.identity import ClientSecretCredential, DefaultAzureCredential
    tenant_id     = os.environ["AZURE_TENANT_ID"]
    client_id     = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    if client_id and client_secret:
        cred = ClientSecretCredential(tenant_id, client_id, client_secret)
    else:
        cred = DefaultAzureCredential()
    return cred.get_token("https://analysis.windows.net/powerbi/api/.default").token


def get_graph_token() -> str:
    from azure.identity import ClientSecretCredential, DefaultAzureCredential
    tenant_id     = os.environ["AZURE_TENANT_ID"]
    client_id     = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    if client_id and client_secret:
        cred = ClientSecretCredential(tenant_id, client_id, client_secret)
    else:
        cred = DefaultAzureCredential()
    return cred.get_token("https://graph.microsoft.com/.default").token


def graph_get(path: str, token: str) -> dict:
    import urllib.request
    url = f"https://graph.microsoft.com/v1.0{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def pbi_request(method: str, path: str, token: str, body: dict | None = None) -> dict | None:
    import urllib.request, urllib.error
    url = f"https://api.powerbi.com/v1.0/myorg{path}"
    data = json.dumps(body).encode() if body else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read()
            return json.loads(content) if content else None
    except urllib.error.HTTPError as e:
        log.error(f"HTTP {e.code} for {method} {url}: {e.read().decode()}")
        raise


# ── AAD group → member UPN resolution ────────────────────────────────────────
def resolve_group_members(group_name: str, graph_token: str) -> list[str]:
    """Returns list of UPNs (email addresses) for all members of an AAD group."""
    # Find group by display name
    result = graph_get(
        f"/groups?$filter=displayName eq '{group_name}'&$select=id,displayName",
        graph_token
    )
    groups = result.get("value", [])
    if not groups:
        log.warning(f"AAD group not found: {group_name}")
        return []

    group_id = groups[0]["id"]
    # Get transitive members (includes nested groups)
    members_resp = graph_get(
        f"/groups/{group_id}/transitiveMembers?$select=userPrincipalName,displayName&$top=999",
        graph_token
    )
    upns = []
    for member in members_resp.get("value", []):
        if upn := member.get("userPrincipalName"):
            upns.append(upn.lower())

    log.info(f"  Group '{group_name}' → {len(upns)} members")
    return upns


# ── Power BI RLS member management ───────────────────────────────────────────
def get_current_rls_members(workspace_id: str, dataset_id: str,
                             role_name: str, pbi_token: str) -> list[str]:
    """Returns current RLS role members (UPNs) from Power BI dataset."""
    path = f"/groups/{workspace_id}/datasets/{dataset_id}/roles"
    roles = pbi_request("GET", path, pbi_token) or {}
    for role in roles.get("value", []):
        if role.get("name") == role_name:
            return [
                m.get("emailAddress", "").lower()
                for m in role.get("members", [])
                if m.get("emailAddress")
            ]
    return []


def set_rls_members(workspace_id: str, dataset_id: str,
                    role_name: str, upns: list[str], pbi_token: str):
    """Replaces RLS role members in Power BI dataset."""
    path = f"/groups/{workspace_id}/datasets/{dataset_id}/roles/{role_name}/members"
    payload = {
        "value": [{"emailAddress": upn} for upn in upns]
    }
    # Power BI API: PUT replaces all members
    pbi_request("PUT", path, pbi_token, payload)
    log.info(f"  Synced role '{role_name}' — {len(upns)} members set")


# ── Audit log ─────────────────────────────────────────────────────────────────
def log_sync_result(role: str, added: list, removed: list, dry_run: bool):
    ts = datetime.now(timezone.utc).isoformat()
    entry = {
        "timestamp": ts,
        "role": role,
        "dry_run": dry_run,
        "added_count": len(added),
        "removed_count": len(removed),
        "added": added,
        "removed": removed,
    }
    log_path = Path(__file__).parent / "rls_sync_audit.jsonl"
    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")


# ── Audit expiry check ────────────────────────────────────────────────────────
def check_audit_expiry(role_def: dict) -> bool:
    """Returns True if the role is within its active period (or has no expiry set).
    Returns False if audit_expiry_date is set and has passed, logging a critical warning.
    """
    expiry_str = role_def.get("audit_expiry_date")
    if not expiry_str:
        return True
    try:
        expiry = date.fromisoformat(expiry_str)
    except ValueError:
        log.error(f"  Invalid audit_expiry_date '{expiry_str}' for role '{role_def['role_name']}' — treating as expired.")
        return False
    if date.today() > expiry:
        log.critical(
            f"  ⛔ Role '{role_def['role_name']}' AUDIT PERIOD EXPIRED (expiry: {expiry_str}). "
            "New member additions are BLOCKED. Remove members from the AAD group and update "
            "audit_expiry_date in rls_roles.json to re-enable access."
        )
        return False
    log.info(f"  Audit expiry check: role '{role_def['role_name']}' active until {expiry_str}")
    return True



def sync_roles(role_filter: str | None = None, dry_run: bool = False):
    workspace_id = os.environ["POWERBI_PROD_WORKSPACE_ID"]
    dataset_id   = os.environ["POWERBI_PROD_DATASET_ID"]

    if not workspace_id or not dataset_id:
        raise EnvironmentError(
            "POWERBI_PROD_WORKSPACE_ID and POWERBI_PROD_DATASET_ID must be set. "
            "Add them to your .env file (see .env.example)."
        )

    roles_config = json.loads(ROLES_FILE.read_text())["roles"]

    pbi_token   = get_pbi_token()
    graph_token = get_graph_token()

    total_synced = 0
    total_changes = 0

    for role_def in roles_config:
        role_name = role_def["role_name"]

        if role_filter and role_name != role_filter:
            continue

        log.info(f"\n📋 Syncing role: {role_name}")

        # For time-bounded roles (e.g. Auditor), enforce the expiry date.
        # If expired, skip adding new members and surface a critical warning.
        role_active = check_audit_expiry(role_def)

        # Collect target members from AAD groups
        target_upns: set[str] = set()
        if role_active:
            for group_name in role_def.get("aad_groups", []):
                members = resolve_group_members(group_name, graph_token)
                target_upns.update(members)

            # Add any directly specified email addresses
            for email in role_def.get("members_by_email", []):
                target_upns.add(email.lower())

        # Compare with current PBI membership
        current_upns = set(get_current_rls_members(workspace_id, dataset_id, role_name, pbi_token))
        added   = sorted(target_upns - current_upns)
        removed = sorted(current_upns - target_upns)

        if not added and not removed:
            log.info(f"  ✅ No changes required ({len(current_upns)} members)")
        else:
            if added:
                log.info(f"  ➕ Adding   {len(added):2d} members: {', '.join(added[:3])}{'...' if len(added) > 3 else ''}")
            if removed:
                log.info(f"  ➖ Removing {len(removed):2d} members: {', '.join(removed[:3])}{'...' if len(removed) > 3 else ''}")

            if not dry_run:
                set_rls_members(workspace_id, dataset_id, role_name, sorted(target_upns), pbi_token)
            else:
                log.info("  🔍 DRY RUN — no changes applied")

            total_changes += len(added) + len(removed)

        log_sync_result(role_name, added, removed, dry_run)
        total_synced += 1

    log.info(f"\n{'='*50}")
    log.info(f"Sync complete: {total_synced} roles processed, {total_changes} member changes {'(DRY RUN)' if dry_run else 'applied'}")


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FIP Power BI RLS AAD Sync")
    parser.add_argument("--role",    default=None, help="Sync only this role name")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    args = parser.parse_args()

    try:
        sync_roles(role_filter=args.role, dry_run=args.dry_run)
    except Exception as e:
        log.error(f"Sync failed: {e}")
        sys.exit(1)
