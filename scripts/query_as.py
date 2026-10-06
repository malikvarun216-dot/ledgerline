"""Run SQL on the SQL warehouse AS the `ledgerline-ci` service principal, and print what it gets.

    python scripts/query_as.py "SELECT current_user()"
    python scripts/query_as.py --check governance      # the Session 12 grant / mask / filter checks

Why this exists (Session 12): the human owns every table, and an owner sees everything — so a
grant, a column mask or a row filter can never be tested from the owner's own session. This
script signs in as the service principal (OAuth machine-to-machine: client id + secret from
`.env`, never the human's token) and runs statements through the SQL Statement Execution API.
A refusal is printed, not raised: "permission denied" is often the expected answer.

Standard library only (urllib), so it needs nothing the project does not already install.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generators._common import load_local_env

GOVERNANCE_CHECKS = [
    ("who am I", "SELECT current_user() AS me, is_account_group_member('analysts') AS analyst, "
     "is_account_group_member('pii_readers') AS pii_reader"),
    ("vault: masked columns", "SELECT customer_name, customer_email, customer_phone, email_hash "
     "FROM workspace.pii.customer_contact ORDER BY customer_unique_id LIMIT 3"),
    ("vault: rows the row filter lets through",
     "SELECT count(*) AS rows FROM workspace.pii.customer_contact"),
    ("vault: states seen", "SELECT c.customer_state, count(*) AS n FROM workspace.pii.customer_contact v "
     "JOIN workspace.silver.customer c USING (customer_unique_id) GROUP BY 1 ORDER BY 2 DESC LIMIT 3"),
    ("raw table (no grant)", "SELECT count(*) FROM workspace.pii.customer_contact_raw"),
    ("forget list (no grant)", "SELECT count(*) FROM workspace.pii.erasure_requests"),
    ("silver.customer (no grant)", "SELECT count(*) FROM workspace.silver.customer"),
    ("write to the vault (SELECT only)", "DELETE FROM workspace.pii.customer_contact WHERE 1 = 0"),
]


def token(host: str, client_id: str, secret: str) -> str:
    basic = base64.b64encode(f"{client_id}:{secret}".encode()).decode()
    request = urllib.request.Request(
        f"{host}/oidc/v1/token",
        data=urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode(),
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["access_token"]


def api(host: str, bearer: str, method: str, path: str, body: dict | None = None) -> dict:
    request = urllib.request.Request(
        f"{host}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        return {"http_error": error.code, "body": error.read().decode()[:400]}


def run(host: str, bearer: str, warehouse: str, statement: str) -> str:
    result = api(host, bearer, "POST", "/api/2.0/sql/statements",
                 {"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s"})
    if "http_error" in result:
        return f"HTTP {result['http_error']}: {result['body']}"
    state = result.get("status", {}).get("state")
    if state != "SUCCEEDED":
        message = result.get("status", {}).get("error", {}).get("message", "")
        return f"{state}: {message.splitlines()[0][:300] if message else result}"
    columns = [c["name"] for c in result["manifest"]["schema"]["columns"]]
    rows = result.get("result", {}).get("data_array", []) or []
    return "\n".join(["  " + " | ".join(columns), *("  " + " | ".join(map(str, r)) for r in rows)])


def main() -> int:
    load_local_env()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("statement", nargs="?", help="one SQL statement")
    parser.add_argument("--check", choices=["governance"], help="run a named list of checks")
    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    if not (host and client_id and secret):
        raise SystemExit("needs DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET in .env")

    bearer = token(host, client_id, secret)
    warehouses = api(host, bearer, "GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not warehouses:
        raise SystemExit("the service principal sees no SQL warehouse: give it 'Can use' on the warehouse")
    warehouse = warehouses[0]["id"]
    print(f"as {client_id} on warehouse {warehouses[0]['name']}")

    checks = GOVERNANCE_CHECKS if args.check else [("statement", args.statement)]
    for label, statement in checks:
        print(f"\n-- {label}\n{run(host, bearer, warehouse, statement)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
