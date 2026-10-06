# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Who may see the vault, and how much — grants, column masks, a row filter
# MAGIC
# MAGIC Three Unity Catalog controls on `workspace.pii.customer_contact`, each answering a different
# MAGIC question:
# MAGIC
# MAGIC - **Grant** — *may you read this table at all?* `analysts` get `SELECT` on the vault only:
# MAGIC   not the raw table, not the forget list.
# MAGIC - **Column mask** — *which value do you see in this column?* A function run for every row, for
# MAGIC   every reader. Members of `pii_readers` see the value; everyone else sees the email's salted
# MAGIC   hash, the phone's last four digits, the name's first letter.
# MAGIC - **Row filter** — *which rows exist for you?* `analysts` see only customers from São Paulo
# MAGIC   (`SP`), looked up in `silver.customer` — a regional support team.
# MAGIC
# MAGIC **Masks and filters apply to the owner too** (grants do not): so the functions let
# MAGIC `pii_readers` through, and every notebook that *writes* PII asserts it runs as a member.
# MAGIC Under the filter a MERGE would see only SP customers and insert the others again; an erasure
# MAGIC `DELETE` would delete nothing and its own check — reading through the same filter — would pass.
# MAGIC
# MAGIC Groups `analysts` (member: the `ledgerline-ci` service principal) and `pii_readers` (member:
# MAGIC the human) are made in Settings → Identity and access. What a non-member sees is checked from
# MAGIC the laptop, as the service principal: `python scripts/query_as.py --check governance`.
# MAGIC Safe to "Run all" again: every statement replaces what it set.

# COMMAND ----------

VAULT = "workspace.pii.customer_contact"
F_EMAIL = "workspace.pii.mask_email"
F_PHONE = "workspace.pii.mask_phone"
F_NAME = "workspace.pii.mask_name"
F_REGION = "workspace.pii.region_filter"

me = spark.sql(
    "SELECT current_user() AS me, is_account_group_member('pii_readers') AS account_group, "
    "is_member('pii_readers') AS workspace_group"
).first()
print(f"running as {me.me}: is_account_group_member('pii_readers') = {me.account_group}, "
      f"is_member('pii_readers') = {me.workspace_group}")
assert me.account_group, (
    "add yourself to pii_readers first: masks and the row filter apply to the owner too, and the "
    "PII notebooks that write (contact_vault, erase) run as you"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Grants — the table, not the schema's other tables

# COMMAND ----------

for statement in (
    "GRANT USE CATALOG ON CATALOG workspace TO `analysts`",
    "GRANT USE SCHEMA ON SCHEMA workspace.pii TO `analysts`",
    f"GRANT SELECT ON TABLE {VAULT} TO `analysts`",
):
    spark.sql(statement)
    print(statement)
display(spark.sql(f"SHOW GRANTS ON TABLE {VAULT}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Column masks — the function decides, per reader, per row

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE FUNCTION {F_EMAIL}(email STRING, email_hash STRING)
    RETURNS STRING
    COMMENT 'pii_readers see the email; everyone else its salted hash (still joinable)'
    RETURN CASE WHEN is_account_group_member('pii_readers') THEN email ELSE email_hash END"""
)
spark.sql(
    f"""CREATE OR REPLACE FUNCTION {F_PHONE}(phone STRING)
    RETURNS STRING
    COMMENT 'pii_readers see the phone; everyone else the last four digits'
    RETURN CASE WHEN is_account_group_member('pii_readers') THEN phone
                ELSE concat('+55 ** *****-', right(phone, 4)) END"""
)
spark.sql(
    f"""CREATE OR REPLACE FUNCTION {F_NAME}(name STRING)
    RETURNS STRING
    COMMENT 'pii_readers see the name; everyone else its first letter'
    RETURN CASE WHEN is_account_group_member('pii_readers') THEN name ELSE concat(left(name, 1), '***') END"""
)
spark.sql(f"ALTER TABLE {VAULT} ALTER COLUMN customer_email SET MASK {F_EMAIL} USING COLUMNS (email_hash)")
spark.sql(f"ALTER TABLE {VAULT} ALTER COLUMN customer_phone SET MASK {F_PHONE}")
spark.sql(f"ALTER TABLE {VAULT} ALTER COLUMN customer_name SET MASK {F_NAME}")
print("masks set on customer_email, customer_phone, customer_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Row filter — a lookup in another table
# MAGIC
# MAGIC The vault has no state column; the filter looks the person up in `silver.customer`. Whether the
# MAGIC reader needs `SELECT` on that table too is one of the things this session checks (the
# MAGIC service principal is given none).

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE FUNCTION {F_REGION}(id STRING)
    RETURNS BOOLEAN
    COMMENT 'pii_readers see every row; analysts only customers from SP'
    RETURN is_account_group_member('pii_readers')
        OR EXISTS (SELECT 1 FROM workspace.silver.customer c
                   WHERE c.customer_unique_id = id AND c.customer_state = 'SP')"""
)
spark.sql(f"ALTER TABLE {VAULT} SET ROW FILTER {F_REGION} ON (customer_unique_id)")
print(f"row filter {F_REGION} set on {VAULT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What the owner sees now, and whether any of it made a Delta commit

# COMMAND ----------

display(spark.sql(f"SELECT customer_name, customer_email, customer_phone FROM {VAULT} ORDER BY 1 LIMIT 3"))
print(f"rows the owner sees: {spark.table(VAULT).count()}")
display(spark.sql(f"DESCRIBE TABLE EXTENDED {VAULT}").where(
    "col_name IN ('Row Filter', 'Column Masks') OR data_type LIKE '%mask%' OR col_name LIKE '%Mask%'"
))
display(spark.sql(f"DESCRIBE HISTORY {VAULT}").select("version", "timestamp", "operation"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The job identity — `ledgerline-jobs` runs the PII job (decisions.md, Session 12)
# MAGIC
# MAGIC The bundle job `ledgerline-pii` runs as this service principal, deployed by `ledgerline-ci`. It
# MAGIC is in `pii_readers` (it must see every row it writes) and gets exactly what the two notebooks
# MAGIC touch: the `pii` schema's tables and checkpoint volume, and reading the landing files. Nothing
# MAGIC in `bronze` or `silver`. Deliberately not granted up front: `CREATE …` on the catalog and the
# MAGIC salt secret — the notebooks only `CREATE … IF NOT EXISTS` objects that exist, and read the salt
# MAGIC only when a new night arrives. The first job run shows whether Free Edition agrees.

# COMMAND ----------

RUNNER = "18160145-ee5d-427a-8553-ef099e1ec870"  # ledgerline-jobs — an id, not a secret
for statement in (
    f"GRANT USE CATALOG ON CATALOG workspace TO `{RUNNER}`",
    f"GRANT USE SCHEMA, SELECT, MODIFY, READ VOLUME, WRITE VOLUME ON SCHEMA workspace.pii TO `{RUNNER}`",
    f"GRANT READ FILES ON EXTERNAL LOCATION ledgerline_landing TO `{RUNNER}`",
):
    spark.sql(statement)
    print(statement)
display(spark.sql("SHOW GRANTS ON SCHEMA workspace.pii"))