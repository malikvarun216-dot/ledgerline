-- ledgerline — repair (Session 18, incidents.md 2026-10-18 "The first governed Gold build failed"):
-- swap GOLD's masking policy for one created with EXEMPT_OTHER_POLICIES = TRUE.
--
-- Why: the tag PII_CLASS puts the mask on every text column of GOLD, customer_state included, and
-- Snowflake refuses a masked column as a row access policy argument (391819) unless the mask is
-- exempt. The option can only be set when a policy is created, so this is a new policy, swapped
-- onto the tag in ONE statement (FORCE): GOLD is never unmasked in between. 09_governance.sql now
-- creates the exempt policy from the start; this file is for the account that ran the first version.
--
-- Run in a Snowsight worksheet with the role picker on ACCOUNTADMIN, everything selected
-- (Ctrl+A, then Ctrl+Enter). Run once.

USE ROLE LEDGERLINE_GOVERNOR;
USE WAREHOUSE LEDGERLINE_WH;

CREATE MASKING POLICY IF NOT EXISTS LEDGERLINE.GOVERNANCE.MASK_TEXT_EXEMPT AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('LEDGERLINE_TRANSFORMER') OR IS_ROLE_IN_SESSION('LEDGERLINE_CI_RUNNER') THEN val
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('LEDGERLINE.GOVERNANCE.PII_CLASS') = 'public' THEN val
    ELSE '***'
  END
  EXEMPT_OTHER_POLICIES = TRUE
  COMMENT = 'Bound to tag PII_CLASS. Pipeline roles see clear text; readers see public columns only. A filter on a masked column matches nothing (0 rows, not an error). Exempt, so a row policy may read a column it covers.';

-- One statement: the tag's STRING policy is replaced, never removed.
ALTER TAG LEDGERLINE.GOVERNANCE.PII_CLASS SET MASKING POLICY LEDGERLINE.GOVERNANCE.MASK_TEXT_EXEMPT FORCE;

-- The old policy is attached to nothing now.
DROP MASKING POLICY LEDGERLINE.GOVERNANCE.MASK_TEXT;

-- Check: one row, the exempt policy on the tag.
SELECT policy_name, ref_entity_name, ref_entity_domain
FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.POLICY_REFERENCES(
  REF_ENTITY_NAME => 'LEDGERLINE.GOVERNANCE.PII_CLASS', REF_ENTITY_DOMAIN => 'tag'));
