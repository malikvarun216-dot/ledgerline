"""A DB-API cursor on the Databricks SQL warehouse, as the `ledgerline-ci` service principal (S28).

OAuth machine-to-machine from .env (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET;
DATABRICKS_SQL_HTTP_PATH optional) — the identity dbt's `databricks` target signs in as, never the
owner's token. Used by snapshot_nights.py (--target databricks) and diff_gold.py.
"""

from __future__ import annotations

import os

DEFAULT_HTTP_PATH = "/sql/1.0/warehouses/e1b5287e7502261b"  # Serverless Starter Warehouse


def cursor():
    from databricks.sdk.core import Config, oauth_service_principal

    from databricks import sql

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")

    def provider():
        return oauth_service_principal(Config(host=f"https://{host}",
                                              client_id=os.environ["DATABRICKS_CLIENT_ID"],
                                              client_secret=os.environ["DATABRICKS_CLIENT_SECRET"]))

    return sql.connect(server_hostname=host,
                       http_path=os.environ.get("DATABRICKS_SQL_HTTP_PATH", DEFAULT_HTTP_PATH),
                       credentials_provider=provider).cursor()
