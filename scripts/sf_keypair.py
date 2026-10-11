"""Make the key pair the laptop's Snowflake service user signs in with. Run once, by the human.

    python scripts/sf_keypair.py
    python scripts/sf_keypair.py --user LEDGERLINE_CI_SVC --out ~/.snowflake/ledgerline_ci_svc.p8

The second form (Session 16) makes the CI user's key: same script, another identity, another file.

Why a key pair (decisions.md, Session 13 "Laptop scripts reach Snowflake as a service user"):
the scripts need to sign in without a person — no password to type, no MFA prompt. Snowflake
service users (`TYPE = SERVICE`) cannot have a password at all; they sign in with a key pair.
The PRIVATE key stays in a file on this machine, outside the repository. The PUBLIC key is not a
secret: it is pasted into Snowflake, which then trusts whoever holds the matching private key.

What it does:
* writes a new 2048-bit RSA private key to ``~/.snowflake/ledgerline_svc.p8`` — refuses if the
  file already exists (a second run would silently lock the scripts out until the new public
  key is pasted; rotation is a deliberate act, see ``--out``);
* prints the ``ALTER USER`` statement carrying the public key, and its fingerprint. After the
  paste, Snowflake's ``DESC USER`` shows ``RSA_PUBLIC_KEY_FP`` — it must equal the printed one.

The private key itself is never printed.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

DEFAULT_OUT = Path.home() / ".snowflake" / "ledgerline_svc.p8"
USER = "LEDGERLINE_SVC"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="where the private key goes")
    parser.add_argument("--user", default=USER, help="the Snowflake user the ALTER USER line names")
    args = parser.parse_args(argv)
    args.out = args.out.expanduser()  # PowerShell passes "~" to a program unexpanded

    if args.out.exists():
        print(f"refused: {args.out} already exists. Delete it on purpose, or pass --out for a new one.")
        return 1

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_der = key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_bytes(private_pem)

    public_b64 = base64.b64encode(public_der).decode()
    fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(public_der).digest()).decode()
    print(f"private key written: {args.out}  (never commit, never paste)")
    print()
    print("Paste this into the Snowflake worksheet (it is the PUBLIC key):")
    print()
    print(f"ALTER USER {args.user} SET RSA_PUBLIC_KEY = '{public_b64}';")
    print()
    print(f"Expected fingerprint (DESC USER {args.user} -> RSA_PUBLIC_KEY_FP): {fingerprint}")
    print()
    print("Then add to .env:")
    var = "SNOWFLAKE_PRIVATE_KEY_PATH" if args.user == USER else "SNOWFLAKE_CI_PRIVATE_KEY_PATH"
    print(f"{var}={args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
