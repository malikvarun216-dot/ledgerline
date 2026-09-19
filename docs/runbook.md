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
