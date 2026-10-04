# Coverage map — Databricks, Snowflake, dbt and production patterns

**The binding plan from Session 10 to the end** (decided 2026-10-02, Session 9;
decisions.md, "Coverage map …"). Every Databricks, Snowflake and dbt topic worth
knowing for interviews and production has one row, a weight, a status and a
session. Nothing in the plan lives only in a chat: a topic is either built,
taught as theory at a named session, or skipped with a reason written here.

This file is a **living status table**, edited in place (like the experiment
tracker in `progress.md`), not an append-only log. The reasoning behind it is in
`decisions.md`; the evidence for each ✓ is in that session's `progress.md`
entry.

## How it is used — the rules

1. **Session start** (CLAUDE.md, start-of-session item 6): read every row bound
   to this session. Each **B** row is built this session, or carried **by name**
   in `### Next` with the reason. A silent skip is what happened to `exp_02`
   (incidents.md 2026-09-30) — this file exists so it cannot happen again.
2. **B?** rows: verify on the platform first. If it works, build it. If the
   platform refuses, record the finding in CLAUDE.md's verified-constraints table
   and turn the row into **T** at the same session — never drop it.
3. **Session end, before the check:** each **T** row of the session gets a short
   theory note and one interview question in `learning.md` Part C, section
   "C11. Theory-only topics". The row's status becomes `T ✓ S#`.
4. **The session that changes a row updates it** (`B S10` → `✓ S10`). Drills
   cross-check their attack list against this file and the experiment tracker.
5. **Calendar rule:** every Databricks-only row is done **before the Snowflake
   trial starts** (S13). The trial's 30 days are the one binding clock.
6. The Snowflake trial is created as **Enterprise edition** (masking, row access
   policies, multi-cluster warehouses and materialized views need it), and the
   edition actually granted is recorded as verified.

**Legend.** Weight (interview frequency and production importance): ★★★ asked
almost every time / core production; ★★ often; ★ sometimes.
Status: **✓ S#** built and verified · **B S#** build · **B? S#** build if the
platform allows (verify first, else T) · **T S#** theory only, taught at that
session · **X** skip (no honest use here).

---

## 1. Databricks

| Topic | Weight | Status |
|---|---|---|
| Delta MERGE, Change Data Feed, time travel, OPTIMIZE / VACUUM, Predictive Optimization | ★★★ | ✓ S6–S9 |
| Structured Streaming: checkpoints, `foreachBatch`, exactly-once, `AvailableNow` | ★★★ | ✓ S7–S9 |
| Auto Loader: schema, rescued data, re-delivered files | ★★★ | ✓ S6, Drill 1 |
| Jobs, retries, failure emails; SQL Alerts | ★★ | ✓ S9 |
| `RESTORE` / repair by time travel | ★★★ | ✓ S10 (exp_04 D: guard off → 53 bad events → `RESTORE`; the change feed shows it as 53 `delete` rows) |
| Concurrent writes: optimistic concurrency, conflict exceptions, isolation levels | ★★★ | ✓ S10 (row-level concurrency with deletion vectors; same rows / no deletion vectors → conflict; retry) |
| Delta constraints (`NOT NULL`, `CHECK`) — enforced; `PRIMARY KEY` informational on both platforms (corrected S10) | ★★ | ✓ S10 (production: `status_known`, `money_not_negative`, `stock_not_negative`; NULL counts as a violation) |
| Watermarks, stream-stream join, `dropDuplicatesWithinWatermark` vs `MERGE` | ★★★ | B S11 |
| Lakeflow Declarative Pipelines (DLT): expectations; `AUTO CDC` / `APPLY CHANGES` as SCD1 (vs the S9 hand-written MERGE) and SCD2 (vs the dbt snapshot) | ★★★ | B S11 |
| Delta schema evolution (`mergeSchema`, column mapping) | ★★ | B S11 |
| AI/BI dashboard on the SQL warehouse — pipeline health (merge logs, stream progress, held rows) | ★★ | B S11 |
| Unity Catalog grants, groups, service principals | ★★★ | B? S12 |
| Column masks, row filters, tags | ★★ | B? S12 |
| Asset Bundles deployed from CI | ★★★ | B? S12 |
| ML: feature table with point-in-time joins, MLflow experiment, model in UC (no serving) | ★★ (point-in-time ★★★) | B? S12b — optional session |
| `SHALLOW` / `DEEP CLONE` (vs Snowflake's zero-copy clone) | ★★ | B S16 |
| Liquid clustering vs partitioning vs Z-order; data skipping | ★★★ | B S21 — on a large sample table, never Olist (Session 0: Olist is too small, "that session would be theater") |
| Spark performance: broadcast vs shuffle joins, skew, AQE, small files, reading the query profile | ★★★ | B S21 (mostly reading plans — serverless hides most settings) |
| System tables (billing, audit) | ★★ | B? S21 |
| Delta Sharing; Lakehouse Federation | ★★ | B? S20 — optional session |
| UniForm / Iceberg read by Snowflake | ★★ | B? S20 — optional; the most doubtful on Free Edition (tables live in Databricks-managed storage) |
| Genie | ★ | B after the trial |
| Model serving, vector search, Databricks Apps | ★ | X — no honest use |

## 2. Snowflake (trial: Enterprise edition)

| Topic | Weight | Status |
|---|---|---|
| Warehouses: size, auto-suspend, resource monitors | ★★★ | B S13 (day one) |
| Roles: hierarchy, custom roles, future grants, managed-access schemas | ★★★ | B S13 (day one) |
| Stages, file formats, `COPY INTO` options (`ON_ERROR`, `VALIDATION_MODE`) | ★★★ | B S13 |
| `COPY INTO` load history — an already-loaded file is skipped (64 days); and a re-delivered file with **new content**? (compare with Auto Loader's Drill 1 skip) | ★★★ | B S13 |
| Snowpipe auto-ingest vs `COPY` (14-day history; needs an S3 event notification on the bucket) | ★★★ | B S13 |
| `VARIANT` / `FLATTEN` — land raw, then type | ★★★ | B S13 |
| Primary keys are not enforced; MERGE with duplicate source rows (vs Delta, which refuses only duplicates matching an *existing* row — exp_04 A1/A2) | ★★★ | B S13–S14 |
| MERGE + anti-join delete (no `NOT MATCHED BY SOURCE` in Snowflake) | ★★★ | B Gold (S14–S15) |
| Zero-copy clone, CI gating (write-audit-publish) | ★★★ | B S16 |
| Time Travel, `UNDROP` | ★★★ | B S18 |
| Masking policies, row access policies, tag-based masking | ★★ | B S18 (Enterprise) |
| Streams (standard vs append-only), Tasks (DAGs, serverless), Dynamic Tables vs materialized views | ★★★ | B S19 — every Task suspended at the end (cost guard) |
| Snowpark, UDFs, stored procedures | ★★ | B S19 (small) |
| Micro-partitions, pruning, clustering keys | ★★★ | B S21 (TPC-DS sample) |
| The three caches (result, warehouse, metadata) | ★★★ | B S21 |
| Query Profile: spilling, exploding joins | ★★★ | B S21 |
| Multi-cluster warehouses: scale up vs scale out | ★★ | B S21 (small, Enterprise) |
| `ACCOUNT_USAGE`, `ACCESS_HISTORY` | ★★ | B S21 |
| Data sharing, Marketplace | ★★ | B? S20 — sharing needs a second account |
| Iceberg tables on our own S3 | ★★ | B? S20 — optional |
| Search optimization, query acceleration | ★ | T S21 |

## 3. dbt, orchestration, CI

| Topic | Weight | Status |
|---|---|---|
| Snapshots: timestamp vs check strategy, hard deletes, business time vs run time | ★★★ | B S14 (exp_05) |
| Incremental: merge, delete+insert, append, microbatch; lookback windows | ★★★ | B S15, S17 (exp_06) |
| Tests, unit tests, model contracts, source freshness | ★★★ | B S14–S15 |
| Slim CI (`state:modified`, defer) + clone | ★★ | B S16 |
| Airflow: DAG, backfill, idempotent tasks, sensors, retries | ★★★ | B S15–S16 |
| One dbt project, two targets (Snowflake + Databricks) | ★★ | B S14 (Snowflake), after the trial (Databricks) |
| CI: lint + tests | ★★ | ✓ S3 |

## 4. What will be missed — theory only

**Blocked by Free Edition (Databricks).**

| Topic | Weight | Status |
|---|---|---|
| Classic clusters: sizing, autoscaling, spot instances, job vs all-purpose clusters, pools, policies | ★★★ | T ✓ S10 |
| Continuous / `ProcessingTime` triggers, latency tuning | ★★★ | T S11 |
| Spark UI and most Spark settings (serverless manages them) | ★★★ | T S21 |
| Photon on vs off (serverless always runs it) | ★★ | T S21 |
| Account console: SCIM, account groups, multiple workspaces | ★★ | T S12 |
| Networking: PrivateLink, customer VPC, IP access lists, NAT (the S4 cost lesson) | ★★ | T S12 |
| Terraform provider | ★★ | T S12 |
| Disaster recovery: deep-clone replication, multi-region | ★★ | T S16 |
| Customer-managed keys | ★ | T S12 |
| Lakeflow Connect managed connectors | ★ | T S11 |

**Blocked by the Snowflake trial** (one account, 30 days).

| Topic | Weight | Status |
|---|---|---|
| Replication / failover (needs a second account) | ★★ | T S18 |
| SSO, SCIM, network policies (a wrong policy locks you out) | ★★ | T S13 |
| Snowpipe Streaming + Kafka connector (needs Kafka Connect; costs money on Confluent) | ★★ | T S13 |
| Fail-safe recovery (only Snowflake support can do it) | ★ | T S18 |
| PrivateLink, Tri-Secret Secure (Business Critical edition) | ★ | T S13 |
| Hybrid tables, Cortex AI | ★ | T S19 |

**No honest use in this project.**

| Topic | Weight | Status |
|---|---|---|
| Kafka internals: partitions, consumer groups, rebalancing, **log compaction** (a natural fit for a CDC topic) | ★★★ | T ✓ S10 |
| Debezium in operation: initial snapshot, log positions, schema history, tombstones (we copy its message shape, not its operations) | ★★★ | T ✓ S10 |
| Lambda vs Kappa architecture; real-time latency trade-offs | ★★ | T S11 |
| Model serving, vector search | ★ | X |

## 5. Production patterns — what the project covers

- **Built:** exactly-once replay, idempotent writes, quarantine (dead-letter
  table), delete circuit breaker, write-once landing, merge log, provenance
  headers, schema-registry contract, denylist repair, answer-key reconciliation,
  chain check, hold-the-row, alarms; **S10:** accumulating snapshot (Silver orders), newest event from the log (no resurrected deletes), CHECK constraints, repair by `RESTORE`, conflict + retry.
- **Planned:** SCD2 (S14), late data with lookback (S17), backfill (Airflow,
  S15–S16), write-audit-publish (clone gating, S16), GDPR erasure with
  crypto-shredding (S12, S18), cost guards (S13), governance — grants and masks
  (S12, S18).
- **Theory only:** disaster recovery, multi-region, network isolation.

## 6. Session by session — the full plan

Existing plan items (decisions.md since Session 0) and the additions from this
map are in one list; additions in **bold**. Experiments come from the tracker in
`progress.md`.

| Session | Builds | Theory hooks (T rows) |
|---|---|---|
| **S10** ✓ | Silver orders (`MERGE INTO` on `order_id`); **exp_04** (CDC correctness: dedup, `seq` guard, late update after a delete, contamination → tie guard in the stream → repair); **`RESTORE` repair, a concurrent-write conflict, `CHECK` constraints** | classic clusters; Kafka internals + log compaction; Debezium in operation |
| **Drill 2** | attacks on Silver (dedup, MERGE correctness, out-of-order, reconciliation 112,806 → 112,650), incident regression, **lineage check, alarm regression (`alarm_test`)** | — |
| **S11** | Lakeflow expectations + **`AUTO CDC` as SCD1 and SCD2**; windowed streaming (stream-stream join, watermarks, `dropDuplicatesWithinWatermark` vs `MERGE`); **schema evolution; pipeline-health dashboard** | continuous triggers / latency; Lambda vs Kappa; Lakeflow Connect |
| **S12** | GDPR, lakehouse half (synthetic PII, pseudonymise, erasure) + **UC grants, column masks, row filters, tags, Asset Bundles from CI** (all B?) | account console; networking; Terraform; customer-managed keys |
| S12b (optional) | **ML point-in-time features, MLflow, model in UC** | — |
| — | **Gate: every Databricks-only row above done before the Snowflake trial starts** | — |
| **S13** | Snowflake day one: Enterprise trial, cost guards (auto-suspend, resource monitor), **roles**; bridge (CDF export incl. `delete` rows → Parquet → stage → `COPY INTO`); **load-history test, Snowpipe beside `COPY`, `VARIANT` first, PK demo** | SSO / SCIM / network policies; Snowpipe Streaming; PrivateLink |
| **S14–S16** | dbt snapshot (**exp_05**, `hard_deletes` decided), incremental, **tests / unit tests / contracts / freshness**, MERGE + anti-join, zero-copy clone CI + **slim CI + Delta clone**, Airflow DAG with backfill | disaster recovery |
| **S17** | **exp_06** late arrival + lookback, **dbt microbatch** | — |
| **S18** | GDPR, Snowflake half: masking + **row access + tag-based masking**, Time Travel / `UNDROP`, erasure | replication / failover; Fail-safe |
| **S19** | Streams & Tasks, Dynamic Tables **vs materialized views**, **Snowpark (small)**; all Tasks suspended | hybrid tables; Cortex AI |
| S20 (optional) | **Sharing on both platforms, Lakehouse Federation, Iceberg / UniForm** (all B?) | — |
| **S21** | performance and cost on both: pruning, clustering keys, Query Profile, **the three caches, spilling, multi-cluster, liquid clustering on a large sample, Spark plans, system tables, `ACCOUNT_USAGE`** | search optimization; Spark UI; Photon |
| **Drill 3** | attacks on Gold (late data, SCD2 point-in-time, the whole pipeline replayed from empty) | — |
| After the trial | Gold on Databricks via dbt's second target, **Genie** | — |

**Cost of the additions:** about 10–12 hours spread over existing sessions, two
optional sessions (S12b, S20), and roughly 10 minutes per theory hook.
**Calendar risk:** S13–S21 plus Drill 3 is about ten sessions inside the trial's
30 days — at Sessions 8–9's pace (~2 days each) about 20 days, so little slack.
