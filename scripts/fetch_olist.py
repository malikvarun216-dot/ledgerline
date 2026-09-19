"""Download the Olist dataset into data/raw/ and validate it against the contract.

What it does
------------
1. Reads your Kaggle credentials from ``~/.kaggle/kaggle.json`` (the classic
   username+key format), a bearer-style token file dropped in the same folder
   (Kaggle's newer personal-token flow), or KAGGLE_USERNAME/KAGGLE_KEY /
   KAGGLE_API_TOKEN env vars. **This script never asks you to type a
   credential and never prints one** — you create the token file, it reads it.
2. Downloads ``olistbr/brazilian-ecommerce`` (~45MB zipped) over the Kaggle
   public API and unzips the nine CSVs into ``data/raw/``.
3. Validates every file against ``generators/olist.py`` — the column contract is
   a hard failure, a row-count difference is reported as drift.

Why raw urllib instead of the ``kaggle`` package
------------------------------------------------
The ``kaggle`` package authenticates at *import* time and raises before any of
our error handling can run, which turns a missing-token case into a confusing
stack trace. One HTTP call with Basic auth has no such landmine and adds no
dependency.

Usage
-----
    python scripts/fetch_olist.py              # download + validate
    python scripts/fetch_olist.py --check-only # validate what is already there
    python scripts/fetch_olist.py --force      # re-download over existing files
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generators.olist import (
    DEFAULT_RAW_DIR,
    KAGGLE_DATASET,
    TABLES,
    validate,
)

API_URL = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET}"

TOKEN_HELP = """
No Kaggle API token found.

  1. Sign in at https://www.kaggle.com  (a free account is enough)
  2. Go to  https://www.kaggle.com/settings  ->  API  ->  "Create New Token"
     That downloads a file called kaggle.json
  3. Move it to:  {target}
  4. Accept the dataset terms once, at:
     https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
  5. Re-run:  python scripts/fetch_olist.py

Alternatively, set KAGGLE_USERNAME and KAGGLE_KEY in your environment.
"""

# Kaggle's classic flow gives you kaggle.json = {"username": ..., "key": ...},
# used as HTTP Basic auth. Some accounts now see a newer personal-token flow
# instead, which hands you a single bearer-style string (observed starting
# with "KGAT_") with no separate username. Both are supported: a Credentials
# tuple is either ("basic", username, key) or ("bearer", token).
Credentials = tuple  # (mode, *values) — see read_credentials


def read_credentials() -> Credentials:
    """Locate Kaggle credentials. Env vars win over the file.

    Three shapes are accepted, in this order:
      1. KAGGLE_USERNAME + KAGGLE_KEY env vars      -> ("basic", user, key)
      2. KAGGLE_API_TOKEN env var                   -> ("bearer", token)
      3. ~/.kaggle/kaggle.json ({"username","key"})  -> ("basic", user, key)
      4. ~/.kaggle/<anything else>, single-line file -> ("bearer", token)

    (4) exists because some accounts now see Kaggle's newer personal-token
    flow, which hands back one bearer-style string (observed starting with
    "KGAT_") instead of the classic username+key pair. Whichever file is
    found, its raw content is treated as a bearer token if it fails to parse
    as the classic JSON shape.
    """
    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY")
    if username and key:
        return ("basic", username, key)

    bearer_env = os.environ.get("KAGGLE_API_TOKEN")
    if bearer_env:
        return ("bearer", bearer_env)

    config_dir = Path(os.environ.get("KAGGLE_CONFIG_DIR", Path.home() / ".kaggle"))
    json_path = config_dir / "kaggle.json"
    if json_path.exists():
        try:
            token = json.loads(json_path.read_text(encoding="utf-8"))
            return ("basic", token["username"], token["key"])
        except (json.JSONDecodeError, KeyError) as exc:
            raise SystemExit(
                f"{json_path} is not a valid Kaggle token file "
                f"(expected JSON with 'username' and 'key'): {exc}"
            ) from exc

    # No kaggle.json — look for any other single file dropped in the same
    # folder (e.g. saved as "access_token") and read it as a bearer token.
    #
    # PowerShell's `>` redirect historically writes UTF-16LE with a BOM, not
    # UTF-8, so a token pasted via `"..." > access_token` needs both encodings
    # tried rather than assuming UTF-8 and failing on the BOM byte.
    if config_dir.is_dir():
        candidates = [p for p in config_dir.iterdir() if p.is_file()]
        if len(candidates) == 1:
            raw_bytes = candidates[0].read_bytes()
            for encoding in ("utf-8-sig", "utf-16", "utf-8"):
                try:
                    raw = raw_bytes.decode(encoding).strip()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise SystemExit(
                    f"{candidates[0]} could not be decoded as text "
                    f"(tried utf-8-sig, utf-16, utf-8)."
                )
            if raw:
                return ("bearer", raw)

    raise SystemExit(TOKEN_HELP.format(target=json_path))


def download_zip(creds: Credentials, dest: Path) -> None:
    """Stream the dataset zip to ``dest``. Credentials go in the header only."""
    if creds[0] == "basic":
        _, username, key = creds
        basic = base64.b64encode(f"{username}:{key}".encode()).decode()
        auth_header = f"Basic {basic}"
    else:
        _, token = creds
        auth_header = f"Bearer {token}"

    request = urllib.request.Request(
        API_URL,
        headers={"Authorization": auth_header, "User-Agent": "ledgerline/0.1"},
    )

    print(f"  downloading {KAGGLE_DATASET} ...")
    try:
        with urllib.request.urlopen(request, timeout=120) as response, dest.open("wb") as out:
            total = 0
            while chunk := response.read(1 << 20):
                out.write(chunk)
                total += len(chunk)
                print(f"\r    {total / 1e6:6.1f} MB", end="", flush=True)
    except urllib.error.HTTPError as exc:
        # Map the two failures that actually happen to actionable messages,
        # rather than letting a bare 403 look like a broken script.
        if exc.code in (401, 403):
            raise SystemExit(
                f"\nKaggle rejected the request ({exc.code}).\n"
                "  - 401: the token is wrong or expired -> create a new one\n"
                "  - 403: you have not accepted the dataset terms yet -> open\n"
                f"    https://www.kaggle.com/datasets/{KAGGLE_DATASET} and click Download once\n"
            ) from exc
        raise SystemExit(f"\nKaggle returned HTTP {exc.code}: {exc.reason}") from exc
    print()


def extract(zip_path: Path, raw_dir: Path) -> list[str]:
    """Unzip only the contracted CSVs, flat, into raw_dir."""
    wanted = {spec.filename for spec in TABLES.values()}
    written: list[str] = []
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.namelist():
            name = Path(member).name
            if name not in wanted:
                continue
            target = raw_dir / name
            with archive.open(member) as src, target.open("wb") as dst:
                dst.write(src.read())
            written.append(name)
    return written


def report(raw_dir: Path) -> int:
    """Print the validation table. Returns a process exit code."""
    reports = validate(raw_dir)
    width = max(len(r.name) for r in reports)
    print(f"\n  {'table'.ljust(width)}   {'rows':>9}   {'expected':>9}   status")
    print(f"  {'-' * width}   {'-' * 9}   {'-' * 9}   ------")

    failures, drift = 0, 0
    for item in reports:
        if not item.present:
            status = "MISSING"
            failures += 1
        elif item.missing_columns:
            status = f"BAD SCHEMA - missing {item.missing_columns}"
            failures += 1
        elif item.row_delta:
            status = f"ok (row drift {item.row_delta:+,})"
            drift += 1
        else:
            status = "ok"
        rows = f"{item.rows:,}" if item.rows is not None else "-"
        print(f"  {item.name.ljust(width)}   {rows:>9}   {item.expected_rows:>9,}   {status}")

    print()
    if failures:
        print(f"  {failures} table(s) failed the column contract.")
        return 1
    if drift:
        print(
            f"  All columns present. {drift} table(s) differ in row count from the\n"
            "  published figures — Kaggle has re-uploaded this dataset before, so\n"
            "  record the actual counts in docs/progress.md rather than assuming a bug."
        )
    else:
        print("  All tables match the contract on columns and row counts.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--check-only", action="store_true", help="validate, do not download")
    parser.add_argument("--force", action="store_true", help="re-download over existing files")
    args = parser.parse_args()

    raw_dir: Path = args.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    if not args.check_only:
        already = [f for f in (spec.filename for spec in TABLES.values()) if (raw_dir / f).exists()]
        if already and not args.force:
            print(f"  {len(already)}/{len(TABLES)} CSVs already in {raw_dir} — skipping download.")
            print("  Use --force to re-download.")
        else:
            creds = read_credentials()
            with tempfile.TemporaryDirectory() as tmp:
                zip_path = Path(tmp) / "olist.zip"
                download_zip(creds, zip_path)
                written = extract(zip_path, raw_dir)
            print(f"  extracted {len(written)} CSVs to {raw_dir}")

    return report(raw_dir)


if __name__ == "__main__":
    raise SystemExit(main())
