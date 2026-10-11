-- ledgerline — Snowflake: the service user the laptop scripts sign in as (Session 13)
--
-- Run in a Snowsight worksheet, role picker on ACCOUNTADMIN, everything selected. Then run the
-- ALTER USER line that `python scripts/sf_keypair.py` printed (it carries the public key).
--
-- TYPE = SERVICE: a user for a program, not a person. It cannot have a password and is never
-- asked for MFA; it signs in with a key pair only. It gets the two functional roles and nothing
-- else — not SYSADMIN, not ACCOUNTADMIN (decisions.md, Session 13).

USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS LEDGERLINE_SVC
  TYPE = SERVICE
  COMMENT = 'ledgerline laptop scripts (scripts/sf.py): key pair only, functional roles only';
ALTER USER LEDGERLINE_SVC SET
  DEFAULT_ROLE = LEDGERLINE_LOADER
  DEFAULT_WAREHOUSE = LEDGERLINE_WH
  DEFAULT_NAMESPACE = LEDGERLINE.RAW;

GRANT ROLE LEDGERLINE_LOADER      TO USER LEDGERLINE_SVC;
GRANT ROLE LEDGERLINE_TRANSFORMER TO USER LEDGERLINE_SVC;

-- Check: TYPE = SERVICE, HAS_PASSWORD = false; after the ALTER USER ... RSA_PUBLIC_KEY line,
-- RSA_PUBLIC_KEY_FP equals the fingerprint the script printed.
DESC USER LEDGERLINE_SVC;
