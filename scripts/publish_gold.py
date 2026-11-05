"""Build tonight's Gold where nobody reads it, test it there, and publish it only if every test passes.

    python scripts/publish_gold.py                          # production: GOLD_AUDIT -> GOLD
    python scripts/publish_gold.py --audit-only             # write + audit, never publish
    python scripts/publish_gold.py --target ci --schema S25_WAP   # LEDGERLINE_CI.S25_WAP_AUDIT -> S25_WAP

Write-audit-publish (Session 25; decisions.md "Gold is published by write-audit-publish"). Drill 3 B5:
`dbt build` runs a model's tests AFTER the model has replaced its live table, so a night that fails a
test was already readable in 4 Gold tables. Here the night never reaches GOLD unless it is green:

  1. seed     the incremental facts are cloned GOLD -> GOLD_AUDIT, so each night starts from what is
              published, never from a previous audit (which may hold a failed night);
  2. audit    `dbt build` with --vars {wap: true}: the same selection as the old nightly build (staging
              views in place, every test), but Gold lands in GOLD_AUDIT (generate_schema_name);
  3. stop     on any failure, exit 1: GOLD is untouched;
  4. publish  every Gold table cloned GOLD_AUDIT -> GOLD (zero-copy, keeps tags and the row policy),
              views re-created in GOLD, then the governance tests run on GOLD itself.

Snapshots are not part of it: they run before, one Silver night per run (snapshot_nights.py).
The publish is not atomic across tables (seconds of a new fact beside an old mart; accepted).
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from run_dbt import main as dbt  # noqa: E402

from generators._common import load_local_env  # noqa: E402

# The container mounts the repo read-only and points dbt at DBT_TARGET_PATH (constraints.md);
# the WAP manifests live beside dbt's own target, never inside the repository there.
STATE = Path(os.environ.get("DBT_TARGET_PATH") or ROOT / "dbt_project" / "target") / "wap_state"
WAP = ["--vars", "{wap: true}"]
INCREMENTALS = ["path:models/gold,config.materialized:incremental"]
GOLD_TABLES = ["path:models/gold,config.materialized:table", *INCREMENTALS]
GOVERNANCE_TESTS = ["gold_governed", "gold_tables_permanent"]


def steps(target: str, audit_only: bool = False) -> list[tuple[str, list[str]]]:
    """The dbt commands, in order. Each must succeed before the next runs."""
    t = ["--target", target]
    published, audited = str(STATE / "published"), str(STATE / "audit")
    plan = [
        ("manifest of what is published", ["parse", "--target-path", published, *t]),
        ("manifest of the audit schema", ["parse", "--target-path", audited, *WAP, *t]),
        ("seed: incremental facts, published -> audit",
         ["clone", "--select", *INCREMENTALS, "--state", published, "--full-refresh", *WAP, *t]),
        ("write + audit: build in the audit schema, every test",
         ["build", "--selector", "everything_but_snapshots", *WAP, *t]),
    ]
    if audit_only:
        return plan
    return [
        *plan,
        ("publish: tables, audit -> published",
         ["clone", "--select", *GOLD_TABLES, "--state", audited, "--full-refresh", *t]),
        ("publish: views re-created", ["run", "--select", "path:models/gold,config.materialized:view", *t]),
        ("check the published schema: governance", ["test", "--select", *GOVERNANCE_TESTS, *t]),
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", default="snowflake", choices=["snowflake", "ci"])
    parser.add_argument("--schema", help="ci only: the run's schema (published); audit = <schema>_AUDIT")
    parser.add_argument("--raw", help="ci only: read LEDGERLINE_CI.<raw>.SILVER_CHANGES instead of RAW")
    parser.add_argument("--audit-only", action="store_true", help="write and audit, never publish")
    args = parser.parse_args(argv)
    load_local_env(ROOT / ".env")
    if args.target == "ci":
        if not args.schema:
            parser.error("--target ci needs --schema")
        os.environ["DBT_CI_SCHEMA"] = args.schema.upper()
        if args.raw:  # production always reads LEDGERLINE.RAW (_sources.yml)
            os.environ["DBT_RAW_DATABASE"], os.environ["DBT_RAW_SCHEMA"] = "LEDGERLINE_CI", args.raw.upper()
    elif args.schema or args.raw:
        parser.error("--schema and --raw are for --target ci only")

    shutil.rmtree(STATE, ignore_errors=True)
    before_publish = len(steps(args.target, audit_only=True))
    for i, (name, command) in enumerate(steps(args.target, args.audit_only)):
        print(f"\n=== {name}", flush=True)
        if dbt(command) != 0:
            state = "untouched" if i < before_publish else "PARTLY PUBLISHED: read it before analysts do"
            print(f"\nWAP STOPPED at: {name}. The published schema is {state}.")
            return 1
    print("\nWAP: audited" + ("" if args.audit_only else " and published"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
