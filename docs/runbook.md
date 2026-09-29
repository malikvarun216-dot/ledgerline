# Runbook

Known failure modes and their fixes — the operational counterpart to
`incidents.md`. An incident is a story; a runbook entry is a procedure.

A failure earns a runbook entry once it has happened twice, or once it is
likely to recur on a fresh machine or a new environment.

**Format:**

```
## <Symptom as you'd actually see it>
**Where:** which component (Databricks job / dbt / Confluent / generator)
**Cause:** one line
**Fix:**
1. step
2. step
**Verify:** how you know it worked
```

---

## Cost emergency — unexpected Databricks spend

**Where:** Databricks workspace
**Cause:** almost always a cluster left running. A single node running a
week ≈ $120 against a ~$22 project budget.
**Fix:**
1. Databricks → Compute → terminate every running cluster
2. Confirm auto-terminate is set to 10 min on each cluster definition
3. Check Jobs → Runs for a streaming job left in a continuous trigger
4. AWS Console → Billing → confirm the spike source is EC2 under Databricks
**Verify:** Compute page shows zero running clusters; budget alert clears.

> Streaming sessions (3, 9) are the highest-risk — a `writeStream` without
> `availableNow` or a termination call runs until stopped.

---

<!-- Append further entries below as failures recur. -->

## Databricks Git folder: Pull stops with "Resolve conflict(s) before merge"

**Where:** Databricks Git folder (`ledgerline`, branch `dev`)
**Cause:** Databricks re-saves a notebook when it is opened or run. If the
committed file differs from its save format (the `[tool.databricks.environment]`
header, no final newline), the workspace copy shows as modified ("M"), and a
Pull whose changes touch the same lines conflicts. Most likely after a new
notebook is added without the header, or if Databricks starts writing a
different `environment_version`.
**Fix:**
1. Click **Abort**. Never "Resolve with Genie", never merge: the workspace is a
   consumer of git, its side is always discarded.
2. In the Git dialog's **Changes** list, open one "M" file's diff. Expect only
   the header block and "No newline at end of file". A real code edit → stop
   and find out who made it before discarding anything.
3. Tick all changed files → ⋮ → **Discard all changes** → **Pull**.
4. Prevent the next one: if the diff showed a header the repo lacks (new
   notebook, or a new `environment_version`), commit that form from the
   laptop. `tests/test_notebooks.py` fails until every notebook matches.
**Verify:** Pull shows "Successfully pulled changes"; after running any
notebook, the Git dialog still says "No changed files".

