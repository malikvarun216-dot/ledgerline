"""Ask the live Schema Registry which schema changes it would accept — without registering anything.

Why this exists
---------------
A compatibility mode is a contract: it decides which schema changes the registry
accepts. Until Session 7 nobody had chosen one — both subjects silently used
Confluent's default, ``BACKWARD`` — and a contract nobody has seen refuse
anything is an assumption, not a contract (decisions.md, Session 6).

This script uses the registry's *test* endpoint, which answers "would this
schema be accepted?" and changes nothing. It checks the generator's current
schema, then three hand-made changes that each probe one rule:

* **add a field with a default**  — safe in every mode;
* **add a field with no default** — breaks BACKWARD: a reader on the new schema
  cannot fill the field in for old messages that never had it;
* **remove a field that has no default** — breaks FORWARD: a reader still on
  the old schema cannot fill it in for new messages that no longer carry it.
  ``BACKWARD`` alone lets this one through.

    python scripts/check_schema_compat.py orders
    python scripts/check_schema_compat.py inventory.cdc

Reads ``.env`` for the registry URL and key. Registers nothing, changes no
setting — every request is a GET or the test endpoint.
"""

from __future__ import annotations

import argparse
import base64
import copy
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generators import inventory_cdc, order_events
from generators._common import _kafka_config_from_env, load_local_env

SCHEMAS = {"orders": order_events.AVRO_SCHEMA, "inventory.cdc": inventory_cdc.AVRO_SCHEMA}

# A required field with no default in each schema — the one the "remove" probe drops.
REQUIRED_FIELD = {"orders": "order_status", "inventory.cdc": "stock_qty"}


def _call(cfg: dict[str, str], method: str, path: str, body: Any = None) -> tuple[int, Any]:
    auth = base64.b64encode(cfg["schema.registry.basic.auth.user.info"].encode()).decode()
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        cfg["schema.registry.url"] + path,
        data=data,
        method=method,
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]


def probes(topic: str) -> dict[str, dict[str, Any]]:
    current = SCHEMAS[topic]
    with_default = copy.deepcopy(current)
    with_default["fields"].append({"name": "channel", "type": "string", "default": "web"})
    no_default = copy.deepcopy(current)
    no_default["fields"].append({"name": "channel", "type": "string"})
    removed = copy.deepcopy(current)
    removed["fields"] = [f for f in removed["fields"] if f["name"] != REQUIRED_FIELD[topic]]
    return {
        "current generator schema": current,
        "add field WITH default": with_default,
        "add field, NO default": no_default,
        f"remove required '{REQUIRED_FIELD[topic]}'": removed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("topic", choices=sorted(SCHEMAS))
    args = parser.parse_args()

    load_local_env()
    cfg = _kafka_config_from_env()
    subject = f"{args.topic}-value"

    _, global_cfg = _call(cfg, "GET", "/config")
    status, subject_cfg = _call(cfg, "GET", f"/config/{subject}")
    _, versions = _call(cfg, "GET", f"/subjects/{subject}/versions")
    print(f"\n{subject}")
    print(f"  global mode        {global_cfg}")
    print(f"  subject mode       {subject_cfg if status == 200 else 'not set (inherits global)'}")
    print(f"  versions           {versions}\n")

    # No version in the path: test against ALL versions, as the subject's mode requires
    # (for a *_TRANSITIVE mode that is every version, not just the latest).
    for name, schema in probes(args.topic).items():
        status, answer = _call(
            cfg,
            "POST",
            f"/compatibility/subjects/{subject}/versions?verbose=true",
            {"schema": json.dumps(schema)},
        )
        if status == 200 and isinstance(answer, dict):
            verdict = "ACCEPTED" if answer.get("is_compatible") else "REFUSED"
            why = "; ".join(answer.get("messages") or [])[:200]
        else:
            verdict, why = f"HTTP {status}", str(answer)
        print(f"  {name:32} {verdict:9} {why}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
