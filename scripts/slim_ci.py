"""Slim CI for the dbt project: build and test only what a change touched, against production data.

    python scripts/slim_ci.py                       # this checkout vs origin/dev, as the CI user
    python scripts/slim_ci.py --base origin/main
    python scripts/slim_ci.py --list-only           # print what would be built, build nothing
    python scripts/slim_ci.py --keep                # leave the CI schema for a look afterwards

GitHub Actions runs exactly this on every pull request (.github/workflows/dbt_ci.yml); the laptop
runs the same thing, so a red CI run can be reproduced here line for line. Decisions.md, Session 16
"dbt CI builds land in a database of their own" and "Slim CI …".

What it does, and why each step:

1. **Production's manifest, from the base branch's code.** dbt decides what changed by comparing
   two manifests (its compiled picture of the project). The base branch's dbt project is taken out
   of git (`git archive`, no second checkout) and *parsed* — not run — for the production target.
   `dbt parse` does not connect to Snowflake, so this costs nothing.
2. **`state:modified+`** = the models, tests and seeds whose code or config differ from the base,
   plus everything downstream of them (`+`). Snapshots are left out: they replay Silver one night
   per run (decisions.md, Session 14) and cannot be rebuilt honestly in a throwaway schema.
3. **`dbt clone` the changed INCREMENTAL models from production first.** A changed incremental
   model built fresh in an empty schema would run its *first-build* path only; cloning production's
   table (zero-copy — no bytes copied) makes the CI run take the *incremental* path, the one that
   runs every night.
4. **`dbt build --defer`**: everything selected is built and tested in the run's own schema in
   LEDGERLINE_CI; every `ref()` to a model NOT selected is read from production instead of being
   rebuilt (that is what "defer" means). The CI user may read production and cannot write it.
5. **Drop the run's schema**, pass or fail (`drop_ci_schema`, which refuses any other target).

Signs in as LEDGERLINE_CI_SVC (target `ci`): SNOWFLAKE_CI_USER / SNOWFLAKE_CI_PRIVATE_KEY_PATH.
"""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECT = ROOT / "dbt_project"
sys.path.insert(0, str(ROOT))

from generators._common import load_local_env  # noqa: E402

SELECT = "state:modified+"
INCREMENTAL = "state:modified+,config.materialized:incremental"


def base_project(ref: str, into: Path) -> Path:
    """The dbt project as it is on ``ref``, unpacked into ``into`` (git archive: no worktree)."""
    tar = subprocess.run(
        ["git", "archive", "--format=tar", ref, "dbt_project"], cwd=ROOT, capture_output=True, check=True
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(tar)) as t:
        t.extractall(into, filter="data")
    return into / "dbt_project"


def dbt(args: list[str], project: Path = PROJECT) -> tuple[bool, object]:
    from dbt.cli.main import dbtRunner

    print(f"\n$ dbt {' '.join(args)}", flush=True)
    result = dbtRunner().invoke([*args, "--project-dir", str(project), "--profiles-dir", str(PROJECT)])
    return result.success, result.result


def selected(state: Path, select: str) -> list[str]:
    ok, nodes = dbt(["ls", "--select", select, "--exclude", "resource_type:snapshot",
                     "--state", str(state), "--target", "ci", "--resource-type", "all", "--quiet"])
    if not ok:
        raise SystemExit("dbt ls failed — see above")
    return sorted(nodes or [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", default="origin/dev", help="git ref production is built from")
    parser.add_argument("--schema", help="CI schema name (default: CI_LOCAL_<time> / from GitHub)")
    parser.add_argument("--list-only", action="store_true", help="print the selection, build nothing")
    parser.add_argument("--keep", action="store_true", help="do not drop the CI schema at the end")
    args = parser.parse_args(argv)

    load_local_env(ROOT / ".env")
    if not (PROJECT / "profiles.yml").exists():
        raise SystemExit("dbt_project/profiles.yml missing — copy it from profiles.yml.example")
    os.environ["DBT_CI_SCHEMA"] = (args.schema or os.environ.get("DBT_CI_SCHEMA")
                                   or f"CI_LOCAL_{time.strftime('%Y%m%d_%H%M%S')}").upper()
    print(f"base {args.base}; CI schema LEDGERLINE_CI.{os.environ['DBT_CI_SCHEMA']}")

    with tempfile.TemporaryDirectory(prefix="ledgerline_slim_ci_") as tmp:
        base = base_project(args.base, Path(tmp))
        state = Path(tmp) / "state"
        # The production target's manifest: relations resolve to LEDGERLINE.STAGING / GOLD — what
        # --defer reads and what dbt clone copies. Parse only; no connection is opened.
        ok, _ = dbt(["parse", "--target", "snowflake", "--target-path", str(state)], project=base)
        if not ok:
            raise SystemExit(f"could not parse the dbt project at {args.base}")

        changed = selected(state, SELECT)
        print(f"\nselected by {SELECT}: {len(changed)}")
        for name in changed:
            print(f"  {name}")
        if args.list_only or not changed:
            print("nothing to build" if not changed else "--list-only: nothing built")
            return 0

        try:
            if selected(state, INCREMENTAL):
                ok, _ = dbt(["clone", "--select", INCREMENTAL, "--state", str(state), "--target", "ci"])
                if not ok:
                    return 1
            ok, _ = dbt(["build", "--select", SELECT, "--exclude", "resource_type:snapshot",
                         "--defer", "--state", str(state), "--target", "ci"])
            print(f"\nslim CI: {'PASSED' if ok else 'FAILED'}")
            return 0 if ok else 1
        finally:
            if not args.keep:
                dbt(["run-operation", "drop_ci_schema", "--target", "ci"])


if __name__ == "__main__":
    sys.exit(main())
