"""Make the landing zone forget personal data on its own: an S3 lifecycle rule on the PII lane.

    python scripts/set_pii_lifecycle.py --profile admin            # plan: prints, changes nothing
    python scripts/set_pii_lifecycle.py --profile admin --apply    # writes the rule, reads it back

Why this exists (decisions.md, Session 12): the dims landing zone is kept forever and is
write-once — that is what makes Bronze rebuildable from S3. Personal data must not live forever,
so it lands in a lane of its own, ``ledgerline/pii/``, and this rule deletes every file there
``--days`` after it was written. An erasure request then has a deadline it can meet without
rewriting anything: the platform deletes the person from its tables at once, and the raw copy
in S3 is gone within ``--days`` (GDPR asks for "without undue delay", at most one month).

What it does, read-first:

* reads the bucket's existing lifecycle rules and versioning state;
* **keeps every other rule** — ``put_bucket_lifecycle_configuration`` replaces the whole list,
  so writing only our rule would silently delete anyone else's;
* adds or replaces one rule, ``ledgerline-pii-expire``, on prefix ``ledgerline/pii/``;
* if versioning is on, also expires old versions — otherwise "deleted" files stay as hidden
  versions forever, which is exactly the copy an erasure must not leave behind.

The scoped generator user (``ledgerline-dev``) deliberately cannot change bucket settings, so
this is run with the account owner's profile, by hand, like ``check_uc_role_policy.py``.
"""

from __future__ import annotations

import argparse
import json
import sys

BUCKET = "ledgerline-landing-dev-fffc8b65"
PREFIX = "ledgerline/pii/"
RULE_ID = "ledgerline-pii-expire"


def planned_rule(days: int, versioned: bool) -> dict:
    rule = {
        "ID": RULE_ID,
        "Status": "Enabled",
        "Filter": {"Prefix": PREFIX},
        "Expiration": {"Days": days},
    }
    if versioned:
        # A current object's expiry only adds a delete marker on a versioned bucket; the bytes
        # stay as a noncurrent version. This removes those too, a day later.
        rule["NoncurrentVersionExpiration"] = {"NoncurrentDays": 1}
    return rule


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", default=None, help="AWS profile with s3:PutLifecycleConfiguration")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--apply", action="store_true", help="write the rule (default: plan only)")
    args = parser.parse_args()

    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.Session(profile_name=args.profile).client("s3")

    try:
        existing = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET)["Rules"]
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
            raise
        existing = []
    versioning = s3.get_bucket_versioning(Bucket=BUCKET).get("Status", "Never enabled")
    versioned = versioning in ("Enabled", "Suspended")

    rule = planned_rule(args.days, versioned)
    others = [r for r in existing if r.get("ID") != RULE_ID]
    replacing = len(others) != len(existing)

    print(f"bucket      {BUCKET}")
    print(f"versioning  {versioning}")
    print(f"rules now   {len(existing)}: {[r.get('ID') for r in existing]}")
    print(f"kept        {len(others)}: {[r.get('ID') for r in others]}")
    print(f"{'replace' if replacing else 'add':<11} {json.dumps(rule)}")

    if not args.apply:
        print("\nplan only - nothing changed. Re-run with --apply to write it.")
        return 0

    s3.put_bucket_lifecycle_configuration(
        Bucket=BUCKET, LifecycleConfiguration={"Rules": [*others, rule]}
    )
    after = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET)["Rules"]
    mine = [r for r in after if r.get("ID") == RULE_ID]
    print(f"\nwritten. rules now {len(after)}: {[r.get('ID') for r in after]}")
    print(f"read back   {json.dumps(mine[0], default=str) if mine else 'MISSING'}")
    return 0 if mine and len(after) == len(others) + 1 else 1


if __name__ == "__main__":
    sys.exit(main())
