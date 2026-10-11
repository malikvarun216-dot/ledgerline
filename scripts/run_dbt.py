"""Run dbt on the ledgerline project with the settings from ``.env``.

    python scripts/run_dbt.py debug
    python scripts/run_dbt.py build
    python scripts/run_dbt.py snapshot --vars "{as_of_night: 3}"

Why a wrapper (Session 14): the dbt profile reads the Snowflake account, user and key path with
``env_var()``, and dbt does not read ``.env`` itself. Without this, each run would need the values
exported in the shell first — or the profile would hold them, and it is one file away from git.
This loads ``.env`` (existing environment variables win), points dbt at ``dbt_project/`` for both
the project and the profile, and passes every other argument through unchanged.

Named ``run_dbt``, not ``dbt``: a script called ``dbt.py`` would be imported in place of the dbt
package itself, because Python puts the script's own folder first on the import path.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECT = ROOT / "dbt_project"
sys.path.insert(0, str(ROOT))

from generators._common import load_local_env  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print(__doc__)
        return 2
    load_local_env(ROOT / ".env")
    if not (PROJECT / "profiles.yml").exists():
        raise SystemExit("dbt_project/profiles.yml missing — copy it from profiles.yml.example")

    from dbt.cli.main import dbtRunner

    # Two-word commands ("source freshness", "docs generate"): the folder flags go after both words.
    n = 2 if args[0] in {"source", "docs"} and len(args) > 1 else 1
    command, rest = args[:n], args[n:]
    dirs = ["--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    if command[0] in {"init", "--version"}:
        dirs = []
    result = dbtRunner().invoke([*command, *dirs, *rest])
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
