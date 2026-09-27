"""Ask AWS whether the Unity Catalog role allows exactly what Databricks will send. Changes nothing.

    python scripts/check_uc_role_policy.py            # uses the default (ambient) AWS profile
    python scripts/check_uc_role_policy.py --profile admin

Why this exists (incidents.md, 2026-09-26): Databricks could assume the role but not read the
bucket. The policy scoped `ListBucket` by `s3:prefix` to `ledgerline/` and `ledgerline/*`; Unity
Catalog's validation lists `ledgerline` — no trailing slash — and got an implicit deny. The
prevention rule said "run simulate-principal-policy against the request shapes that system
might send". Until Drill 1 that was prose. This is the rule as code.

What it checks, all through IAM's policy simulator (read-only: it evaluates, it grants nothing):

* every request shape Unity Catalog sends for the external location is ALLOWED;
* the role stays read-only and prefix-scoped (writes, deletes, other prefixes are DENIED);
* the Session 6 policy shape — the live policy with the bare `ledgerline` prefix removed — is
  DENIED for the bare prefix. That proves this check would have caught the incident.

Needs an identity that may read IAM (`iam:SimulatePrincipalPolicy`, `iam:SimulateCustomPolicy`,
`iam:GetRolePolicy`, `iam:ListRolePolicies`, `iam:ListAttachedRolePolicies`, `iam:GetPolicy*`).
The scoped `ledgerline-dev` generator user deliberately cannot, so this is run with the account
owner's profile, by hand.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
import urllib.parse

ACCOUNT = "240939827246"
ROLE = "ledgerline-uc-landing-read"
BUCKET = "ledgerline-landing-dev-fffc8b65"
PREFIX = "ledgerline"
BUCKET_ARN = f"arn:aws:s3:::{BUCKET}"

# (label, action, resource, s3:prefix or None, expected decision)
CASES = [
    ("UC lists the location, no trailing slash (S6)", "s3:ListBucket", BUCKET_ARN, PREFIX, True),
    ("UC lists the location with a trailing slash", "s3:ListBucket", BUCKET_ARN, f"{PREFIX}/", True),
    ("Auto Loader lists one dimension", "s3:ListBucket", BUCKET_ARN, f"{PREFIX}/dims/customer/", True),
    (
        "Auto Loader reads a dump",
        "s3:GetObject",
        f"{BUCKET_ARN}/{PREFIX}/dims/customer/dump_date=2017-05-02/customer.csv",
        None,
        True,
    ),
    ("Drill 1 scratch is readable", "s3:GetObject", f"{BUCKET_ARN}/{PREFIX}/drill1/x.csv", None, True),
    ("listing the bucket root is refused", "s3:ListBucket", BUCKET_ARN, "", False),
    ("listing another prefix is refused", "s3:ListBucket", BUCKET_ARN, "other/", False),
    ("reading another prefix is refused", "s3:GetObject", f"{BUCKET_ARN}/other/x.csv", None, False),
    ("writing is refused (read-only role)", "s3:PutObject", f"{BUCKET_ARN}/{PREFIX}/dims/x.csv", None, False),
    ("deleting is refused (read-only role)", "s3:DeleteObject", f"{BUCKET_ARN}/{PREFIX}/x.csv", None, False),
]


def _context(prefix):
    if prefix is None:
        return []
    return [{"ContextKeyName": "s3:prefix", "ContextKeyValues": [prefix], "ContextKeyType": "string"}]


def _allowed(result):
    return result["EvaluationResults"][0]["EvalDecision"] == "allowed"


def live_policy_documents(iam):
    docs = []
    for name in iam.list_role_policies(RoleName=ROLE)["PolicyNames"]:
        doc = iam.get_role_policy(RoleName=ROLE, PolicyName=name)["PolicyDocument"]
        docs.append(json.loads(urllib.parse.unquote(doc)) if isinstance(doc, str) else doc)
    for attached in iam.list_attached_role_policies(RoleName=ROLE)["AttachedPolicies"]:
        policy = iam.get_policy(PolicyArn=attached["PolicyArn"])["Policy"]
        arn, default = attached["PolicyArn"], policy["DefaultVersionId"]
        version = iam.get_policy_version(PolicyArn=arn, VersionId=default)
        doc = version["PolicyVersion"]["Document"]
        docs.append(json.loads(urllib.parse.unquote(doc)) if isinstance(doc, str) else doc)
    return docs


def session6_shape(doc):
    """The live policy with the bare prefix removed from every s3:prefix condition."""
    broken = copy.deepcopy(doc)
    removed = 0
    statements = broken["Statement"] if isinstance(broken["Statement"], list) else [broken["Statement"]]
    for statement in statements:
        for operator in statement.get("Condition", {}).values():
            values = operator.get("s3:prefix")
            if values is None:
                continue
            values = [values] if isinstance(values, str) else values
            kept = [v for v in values if v != PREFIX]
            removed += len(values) - len(kept)
            operator["s3:prefix"] = kept
    return broken, removed


def main() -> int:
    import boto3

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile)
    print(f"checking as {session.client('sts').get_caller_identity()['Arn']} (read-only simulation)\n")
    iam = session.client("iam")
    role_arn = f"arn:aws:iam::{ACCOUNT}:role/{ROLE}"

    failures = 0
    for label, action, resource, prefix, expect in CASES:
        result = iam.simulate_principal_policy(
            PolicySourceArn=role_arn,
            ActionNames=[action],
            ResourceArns=[resource],
            ContextEntries=_context(prefix),
        )
        decision = result["EvaluationResults"][0]["EvalDecision"]
        ok = _allowed(result) == expect
        failures += not ok
        shown = f" prefix={prefix!r}" if prefix is not None else ""
        print(f"{'OK  ' if ok else 'FAIL'}  {label:55} {action}{shown} -> {decision}")

    print("\nre-trigger: the Session 6 policy shape (bare prefix removed)")
    docs = live_policy_documents(iam)
    triggered = False
    for doc in docs:
        broken, removed = session6_shape(doc)
        if not removed:
            continue
        result = iam.simulate_custom_policy(
            PolicyInputList=[json.dumps(broken)],
            ActionNames=["s3:ListBucket"],
            ResourceArns=[BUCKET_ARN],
            ContextEntries=_context(PREFIX),
        )
        decision = result["EvaluationResults"][0]["EvalDecision"]
        triggered = not _allowed(result)
        verdict = "OK  " if triggered else "FAIL"
        print(f"{verdict}  ListBucket prefix={PREFIX!r} under the S6 shape -> {decision}")
    if not triggered:
        failures += 1
        print("FAIL  could not reproduce the S6 deny: no bare-prefix condition in the live policy")

    print(f"\n{'ALL CHECKS PASS' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
