# Architectural Decisions

One entry per architectural choice, written at the moment the decision is made.
Every entry names the rejected alternative — a decision without a rejected
alternative is just a description.

Append-only. Never edit a past entry to hide a wrong turn; append a correction
below it instead.

**Format:**

```
## <Decision title> (Session N)
- Chosen: what was chosen
- Rejected: what was rejected, and why
- Trade-off: what was accepted in exchange
```

---

## Paid Databricks over Free Edition (Session 0)
- Chosen: Databricks Premium on AWS, pay-as-you-go from the start (~$22 total estimate)
- Rejected: **Databricks Free Edition ($0)** — two verified blockers made it structurally unsuitable. Outbound internet is restricted to trusted domains, so an external Kafka broker cannot be reached; and custom external locations are unsupported (default storage only), so the Silver→Snowflake bridge could not use an own S3 bucket. Both would have forced workarounds: a local Spark hop and a UC Volume sync seam.
- Rejected: **trial-first sequencing** — the 14-day free-DBU trial is full Databricks, but planning sessions around its clock adds scheduling pressure for ~$10 of saving. (The trial typically starts automatically at signup, so the free DBUs likely apply anyway.)
- Trade-off: real money (~$22) and **forgotten-cluster risk** — a single node left running a week ≈ $120, six times the entire project budget. Mitigated by mandatory 10-minute auto-terminate, single-node dev, serverless preference, and budget alarms configured in Session 2 *before* any compute runs.
- Second-order gain: cluster configuration, instance-type selection, autoscaling, Photon, and Jobs-vs-All-Purpose cost decisions become available. Free Edition's serverless-only model hides all of that, and warehouse/cluster sizing is common interview ground.

### Correction (2026-09-26, Session 6) — both "verified blockers" were disproved by running code

The entry above rejected Free Edition on "two verified blockers". **Session 6
ran into both and neither held:**

1. *"An external Kafka broker cannot be reached."* A Free Edition serverless
   notebook read all **394,090** records of `orders` from Confluent Cloud,
   without identity verification.
2. *"Custom external locations are unsupported (default storage only)."* A
   Unity Catalog storage credential and a read-only external location on
   `s3://ledgerline-landing-dev-fffc8b65/ledgerline/` were created, and a
   notebook read **8,395** customer rows through them.

Which of two things happened cannot be told apart from here: either the
Session 0 research conflated "custom **workspace** storage locations" (still
listed as unsupported) with external locations and read "restricted to trusted
domains" as "cannot reach Kafka", or Free Edition changed between then and now.
What can be said: the word **"verified" was attached to documentation reading,
not to a test** — the same gap the Session 6 Free Edition entry closes by
reading data first. The project now runs on Free Edition; the second-order
loss this entry names (clusters, instance types, Jobs vs All-Purpose) is real
and is recorded there.

## Platform split — Databricks owns Bronze+Silver, Snowflake owns Gold (Session 0)
- Chosen: two platform-native halves. Databricks for ingest and reconciliation (Unity Catalog, Delta MERGE, Structured Streaming, DLT, Delta ops); Snowflake for the warehouse layer (dbt, Streams & Tasks, Dynamic Tables, zero-copy clone, query profile).
- Rejected: **single-platform (Databricks only)** — simpler and cheaper, and every pattern including `WHEN NOT MATCHED BY SOURCE` works on Delta. But it forfeits the Snowflake cost-optimization surface (micro-partition pruning, clustering, warehouse sizing), which is a distinct skill and half the stated goal.
- Rejected: **single-platform (Snowflake only)** — loses Spark Structured Streaming and `foreachBatch`, which is precisely where the exactly-once pattern lives.
- Rejected: **Snowflake as "just where dbt points"** — would have given Snowflake breadth without depth.
- Trade-off: a bridge to build and maintain (Databricks → S3 → Snowflake external stage), and two platforms to keep in sync.

## Three source shapes, inventory CDC derived from order_items (Session 0)
- Chosen: three sources chosen for distinct **temporal shape**, not just distinct transport — immutable facts (orders), slowly-changing where history matters (dimension dumps), fast-changing where only current state matters (inventory). Inventory stock is seeded per `(product_id, seller_id)` — a real Olist grain — with decrements **driven by actual order events**, plus restock `U` and delisting `D` ops carrying a monotonic `seq`.
- Rejected: **a second CDC feed off the same customer/product data** — two pipelines owning customer truth is a real architectural smell, and it makes "which source feeds the Gold SCD2 snapshot?" unanswerable. It also yields two sources with the *same* temporal shape, defeating the point.
- Rejected: **independent synthetic stock jitter** — simpler generator, but the CDC feed becomes noise running alongside the pipeline rather than part of it, and the out-of-order scenario has to be manufactured instead of arising naturally from concurrent channel decrements.
- Trade-off: inventory must be synthesized (Olist has no stock table), so the generator carries correctness responsibility. Bought back by causal coupling, which enables a genuine cross-source reconciliation test at Gold: units sold must tie out against cumulative stock decrements.

## Confluent Cloud Basic for Kafka (Session 0)
- Chosen: Confluent Cloud Basic — $0 base, $400 trial credits over 30 days, and **$0 at zero consumption after the credits lapse**, so no third clock to race alongside Snowflake's 30 days. Includes Schema Registry.
- Rejected: **local Docker Kafka** — Databricks runs in AWS and cannot reach a broker on a laptop behind NAT. This only became visible after choosing paid Databricks.
- Rejected: **AWS MSK Serverless** — ~$540/month for cluster capacity alone. Disqualifying at 25× the entire project budget.
- Rejected: **self-managed Kafka on EC2 t3.small** (~$1 total) — genuinely cheaper, but adds broker ops (listeners, security groups, advertised addresses) that aren't the point of this project, and "ran a single-broker Kafka on EC2" carries less weight than managed Confluent Cloud.
- Trade-off: a third SaaS account to manage. Bought back with Schema Registry, which adds Avro and schema-evolution handling at no extra cost.

## Olist plus TPC-DS, nothing purchased (Session 0)
- Chosen: Olist (Kaggle, ~120MB) as the semantic core; **Snowflake's built-in `SNOWFLAKE_SAMPLE_DATA` TPC-DS at SF100+** for the cost-optimization session only.
- Rejected: **purchasing a dataset** — buying data does not improve a portfolio project. What matters is whether the data supports the patterns, and Olist supplies what cannot be faked: order status lifecycle with multiple timestamps, and customer location that genuinely varies across orders for the same `customer_unique_id` (real SCD2 material, not synthetic).
- Rejected: **swapping in the 285M-event REES46 clickstream** — it is clickstream (view/cart/purchase), not orders-with-line-items, so it breaks the causal coupling between the order stream and the inventory CDC feed. 15GB of local friction for zero pattern gain.
- Trade-off: Olist's ~100k orders are too small for a meaningful micro-partition pruning demonstration — that session would be theater. Resolved by using TPC-DS for the perf exercise specifically, where benchmark data is the *correct* tool and costs nothing (shared to all accounts, no storage or load charge).

## dbt-native data quality over Great Expectations (Session 0)
- Chosen: dbt tests + `dbt-utils` + `dbt-expectations` + `dbt source freshness`, with generic tests written alongside their models rather than deferred to a separate quality pass.
- Rejected: **a separate Great Expectations job** — not because GE is worse, but because **jobpulse already demonstrates it**. Building it again shows repetition; building the alternative shows range and the judgment to choose between them.
- Trade-off: dbt-native tests run inside the transformation DAG, so they cannot validate data *before* it reaches the warehouse the way a standalone GE job can. Accepted because Bronze/Silver validation lives on the Databricks side (DLT expectations, Session 11), which covers the pre-warehouse gap from the other direction.

## WHEN NOT MATCHED BY SOURCE DELETE belongs to the snapshot path, not CDC (Session 0)
- Chosen: `WHEN NOT MATCHED BY SOURCE ... DELETE` applies to **Silver dimensions**, fed by the nightly full dump. CDC deletes are handled **in-band** via `op = 'D'`.
- Rejected: the original plan's placement of this clause on the **inventory CDC** path. Applied to a CDC micro-batch it is actively destructive: a CDC batch contains only *changed* rows, so every unchanged target row has no matching source row and gets deleted — truncating the table to whatever moved in the last few minutes.
- Consequence: a **Silver dimensions layer was added**, which also closed a gap where Bronze dimensions jumped straight to Gold SCD2 with no Silver step.
- Trade-off: one more table to maintain. Bought back with a sharper lesson — the same source now runs through two write patterns, one handling deletions for free (partition overwrite) and one silently missing them (plain MERGE).

## Late-arrival experiment belongs at Gold, not Bronze (Session 0)
- Chosen: the late-arrival proof runs against the **Gold dbt incremental** model, where `where event_ts > (select max(event_ts) from {{ this }})` is the high-water-mark filter that actually drops the row. Fix is a lookback window.
- Rejected: the original plan's placement at **Bronze streaming ingest**. Bronze is append-only with no watermark, so a late event isn't dropped there — it just lands late. The experiment would have had no failure to observe.
- Trade-off: none. The experiment simply moves to where the failure is real.

## MERGE at Silver for dimensions, not overwrite (Session 0)
- Chosen: Silver dimensions are built with MERGE rather than a blind overwrite from the Bronze snapshot.
- Rejected: **overwrite at Silver** — genuinely simpler, and many teams would do exactly this from a full dump. Rejected because a blind overwrite destroys `_first_seen_at` / ingestion-lineage columns and rewrites the whole table when only a handful of rows changed.
- Trade-off: MERGE is more code and needs explicit delete handling (see the entry above) where overwrite gets it free. This is the point — it is the deliberate failure the project sets out to demonstrate.

## Auto Loader on the dimension dumps, not the order stream (Session 0)
- Chosen: Auto Loader (`cloudFiles`) + `Trigger.AvailableNow` + `replaceWhere` ingests the nightly dimension dumps landing in S3.
- Rejected: **Auto Loader on the order stream as a second ingest path**. With real Kafka available this would have required the generator to dual-write the same events to both Kafka and S3 — an artificial seam existing only to demonstrate a feature, which violates the project's rule that no pattern is forced in for coverage.
- Rejected: **plain `spark.read` on the dump directory** — reprocesses every file on every run, has no schema-drift handling, and no rescued-data column.
- Trade-off: `Trigger.AvailableNow` on a streaming source is a less obvious idiom for a nightly batch than a plain read, so the reasoning needs documenting. It is the standard Databricks answer for batch-over-cloud-files.

## dbt project carries two targets (Session 0)
- Chosen: `profiles.yml` defines both a `snowflake` target (primary) and a `databricks` target (secondary).
- Rejected: **Snowflake target only** — simpler, but the Snowflake trial expires after 30 days and takes the entire Gold layer with it, leaving the project undemoable at exactly the point it would be shown to someone.
- Trade-off: models must stay adapter-portable, which rules out Snowflake-only syntax in shared models. Snowflake-specific work (Streams, Tasks, Dynamic Tables, clustering, query profile) is deliberately confined to Session 19 and 21 and documented with captured evidence rather than kept runnable.

## No git worktrees (Session 0)
- Chosen: work directly on the `dev` branch.
- Rejected: **the jobpulse worktree + copy-back workflow**. jobpulse's own `incidents.md` records worktree/tfstate drift as a *recurring* incident across two separate chats, and the file-copy ritual existed solely to work around it.
- Trade-off: no isolation between concurrent sessions. Acceptable because sessions here are sequential, and nothing in this project needs a second checkout. Removes an entire incident class already paid for once.

## Generators write through a sink protocol, not directly to Kafka/S3 (Session 1)
- Chosen: `MessageSink` and `BlobSink` protocols in `generators/_common.py`, with an offline twin for each (`JsonlSink`, `MemorySink`, `LocalBlobSink`) and a cloud implementation (`KafkaAvroSink`, `S3BlobSink`). Generator logic is written against the protocol and never names a transport.
- Rejected: **calling `confluent_kafka.Producer` and `boto3` directly in each generator.** Simpler by a layer, but Session 1 runs before any cloud account exists (Session 2), so the generators would have been unrunnable and untestable on the day they were written. It also forces every unit test to mock a broker or a bucket.
- Trade-off: one indirection layer, and the cloud sinks stay unverified until Session 3 — code that has never touched a real broker. Bought back by 84 tests that run with no credentials at all, and by `LocalBlobSink` mirroring the S3 key layout exactly, so switching sinks changes nothing downstream.
- Consequence: the cloud SDK imports sit inside `__init__`, not at module scope, so the modules import on a machine with neither library installed. `test_generators_import_without_any_cloud_sdk_installed` pins that, because CI depends on it.

## Deterministic hash event IDs, not uuid4 (Session 1)
- Chosen: `event_id = sha256(natural_key)[:32]`, where the natural key is e.g. `(source, order_id, event_type, event_ts)`. A replay of the generator over the same CSV produces byte-identical events.
- Rejected: **`uuid4()` per event.** The standard choice, and wrong here: a fresh random ID per run means replaying produces *different* rows, so a dedup-on-ID at Silver can never be demonstrated to work. Only Kafka's offset mechanism would ever be under test, and the content path would stay unprovable.
- Trade-off: the natural key must genuinely be unique, and a collision is silent. Mitigated with a `\x1f` separator — joining fields without one makes `("ab","c")` and `("a","bc")` hash identically — and a distinct sentinel for `None`, so an absent value and a blank one stay different.

## CDC `seq` is derived from event time, not counted (Session 1)
- Chosen: `seq = event_ts_micros * 1000 + index_within_that_microsecond`.
- Rejected: **an incrementing counter persisted to a state file.** CLAUDE.md names "`seq` not actually monotonic across generator restarts" as a danger zone, and a counter is exactly how that happens — restart, counter resets, an old event carries a higher `seq` than a newer one and clobbers it straight through the `s.seq > t.seq` guard. Persisting the counter fixes that but adds hidden state that can drift or be deleted.
- Trade-off: needs a tiebreaker for events sharing a timestamp, capped at 1000 per microsecond — beyond that `assign_seq` raises rather than emitting a duplicate. Olist is second-granularity so the headroom is enormous, but the failure is loud rather than silent if that ever changes.

## Order events fan out, one per real lifecycle timestamp (Session 1)
- Chosen: each Olist order becomes up to four events (`created` / `approved` / `shipped` / `delivered`) from its four real timestamp columns, plus a flagged synthetic terminal event for `canceled` / `unavailable`.
- Rejected: **one event per order.** Silver's `MERGE INTO ... ON order_id` would then have nothing to do — every row an insert, a MERGE that is an insert in disguise. Fanning out means an order arrives and then *updates* three more times, which is what makes the MERGE, the idempotency proof and the late-arrival experiment real rather than decorative.
- Trade-off: `canceled` has no timestamp column in Olist, so one is invented (last real event + 1s). Rather than hide that, the event carries `ts_is_synthetic = true`.

## Kafka key is the entity, not the event (Session 1)
- Chosen: `order_id` for the orders topic, `sku_key` for the CDC topic.
- Rejected: **keying on `event_id`, or round-robin partitioning.** Kafka guarantees ordering only *within* a partition, and the key picks the partition. Keying per event scatters one order's lifecycle across partitions, so a consumer can legitimately see `delivered` before `created` — and for CDC it would make the `seq` guard load-bearing for basic correctness rather than a belt-and-braces check against genuine out-of-order arrival.
- Trade-off: a hot key (a very popular SKU) concentrates load on one partition. Irrelevant at Olist's scale; the honest answer at scale is a composite key or more partitions.

## `dim_updated_at` is carried forward on unchanged rows (Session 1)
- Chosen: each night's dump hashes every row's *business attributes*, compares against the previous dump read back from the sink, and **keeps the previous timestamp when nothing changed**.
- Rejected: **stamping every row with the run time.** The obvious implementation, and it silently destroys the Gold layer: a dbt snapshot with `strategy='timestamp'` would see all ~96k customers as changed every night and open a new SCD2 version for each. Nothing errors; the history is just junk. CLAUDE.md lists "`dbt_valid_from` reflecting run time instead of business time" as a danger zone, and this is the upstream cause of it.
- Trade-off: the generator must read back the previous dump, so it holds state in the sink rather than being purely functional. That is also what lets it run one night at a time instead of only as a single batch.

## Customer dimension replays real history; products and sellers are synthetic (Session 1)
- Chosen: an asymmetric change model. Customers get a genuine as-of-date timeline — Olist really does record different cities for the same `customer_unique_id` across their orders, so a dump for date D shows each person's last observation at or before D, with `dim_updated_at` set to that real observation time. Products and sellers are static in Olist, so their changes are deterministic synthetic edits (seeded, reproducible), preferring to backfill a genuinely-null `product_category_name`.
- Rejected: **a uniform synthetic change model across all three dimensions.** Simpler and symmetrical, but it throws away the one piece of real SCD2 evidence the dataset contains — which is a stated reason Olist was chosen over a clickstream set in the first place.
- Trade-off: the asymmetry has to be explained rather than assumed, and it is easy to forget which half is real. Recorded here and in the module docstring: **the customer SCD2 evidence is real; the product/seller evidence is fabricated.**

## Inventory reconciliation reads delta sign, not a reason column (Session 1)
- Chosen: the CDC record carries `prev_stock_qty` and `stock_qty` (a flattened Debezium before/after image). Units sold is reconstructed as `-sum(delta where delta < 0)`. Restocks are strictly positive and seeds have no predecessor, so a negative delta is a sale by construction. `change_reason` exists for console readability and **nothing in the pipeline may read it**.
- Rejected: **a `change_reason` column the pipeline depends on.** It would make the reconciliation trivially true — comparing the generator's own label against itself — rather than a genuine second derivation. A real CDC feed carries no such column, so depending on one would also be unfaithful to the pattern.
- Rejected: **the full Debezium envelope** (nested `before` / `after` records). More faithful, but the Avro union nesting buys complexity without teaching anything this project needs.
- Trade-off: the generator now owes an invariant it must never break — no downward movement may exist that is not a sale. Asserted in `test_restocks_are_strictly_positive`.

## Sales are grouped per (order, SKU), not per unit (Session 1)
- Chosen: a three-unit line produces one stock movement of -3.
- Rejected: **one CDC event per unit**, mirroring Olist's one-row-per-unit `order_items` shape exactly. It would inflate the feed threefold and make the Session 10 in-batch-dedup exercise artificially easy, since near-duplicate keys would be everywhere by default.
- Trade-off: the order stream and the CDC stream now carry different grains (units vs movements), so the reconciliation has to state explicitly that it compares *units sold* against *summed decrements*, not row counts against row counts.

## CI installs an explicit dependency list, not requirements-dev.txt (Session 1)
- Chosen: `.github/workflows/ci.yml` installs `pandas`, `pytest`, `ruff` by name.
- Rejected: **`pip install -r requirements-dev.txt`**, the obvious choice. That file pulls pyspark, delta-spark, databricks-connect and three dbt adapters through `-r requirements.txt` — several hundred MB, none of it imported by anything the suite runs. A local dry-run resolve of the full set was attempted and **timed out on download**, so whether it even resolves cleanly is currently unknown; writing CI that assumes it does would be asserting something unverified.
- Trade-off: the CI list and `requirements-dev.txt` can drift. Guarded from both ends — `test_generators_import_without_any_cloud_sdk_installed` fails if a cloud import is hoisted to module scope, and the workflow carries an explicit instruction to widen the list in the same commit that adds a test needing Spark or dbt. A test skipped for a missing dependency is a test that is not running.

## Kaggle download over raw urllib, not the `kaggle` package (Session 1)
- Chosen: `scripts/fetch_olist.py` makes one HTTPS call with Basic auth, reading the token the user created at `~/.kaggle/kaggle.json`.
- Rejected: **the `kaggle` pip package.** It authenticates at *import* time and raises before any error handling can run, turning "you have no token yet" into a stack trace instead of instructions. It is also a whole dependency for a single HTTP GET.
- Trade-off: HTTP status handling is hand-written. Mapped to the two failures that actually occur — 401 (bad token) and 403 (dataset terms not accepted) — because a bare `HTTPError: 403` reads like a broken script rather than a missing click.

## CDC seed timestamp is per-SKU, not shared across the dataset (Session 1, corrected against real data)
- Chosen: each `(product_id, seller_id)` SKU is seeded one day before *its own* first sale.
- Rejected: **one shared seed instant for every SKU** (`min(order_purchase_timestamp)` across the whole dataset), the original Session 1 design. It broke against the real 99k-order dataset on first run: 34,448 distinct SKUs all landed on one microsecond, exceeding `assign_seq`'s 1000-per-microsecond tiebreaker and crashing the generator outright.
- Trade-off: none identified. Per-SKU seeding is strictly more realistic (a seller lists a product when they start selling it, not on day one of the marketplace) and fixes the collision as a side effect rather than by widening the tiebreaker to paper over it. Full incident in `docs/incidents.md`.

## Attribute hash normalizes numeric values before comparing (Session 1, corrected against real data)
- Chosen: `attribute_hash` in `dim_dumps.py` canonicalizes any Python or numpy int/float to an integer-string form before hashing, so `225`, `225.0`, `np.int64(225)`, and `np.float64(225.0)` all compare equal.
- Rejected: **hashing `str(value)` directly against whatever the frame's current dtype happens to be**, the original Session 1 design. It silently broke against the real dataset: `read_previous` reads last night's dump back through plain `pd.read_csv`, which upcasts an entire integer column to float64 the instant any row in it is null — and real `product_weight_g` has scattered nulls. Every product's weight/dimension columns looked "changed" every night, defeating the entire `dim_updated_at` carry-forward mechanism the moment real, null-bearing data was used. Invisible through 84 passing tests because the 4-row fixture had no nulls in a numeric column.
- Trade-off: a genuine fraction (`225.5`) must still compare unequal to `225` — verified directly (`test_attribute_hash_is_stable_across_int_and_float_representations`), not just inferred from the pipeline-level fix. Full incident in `docs/incidents.md`.

## Kaggle credentials support both the classic and the newer personal-token format (Session 1, real download)
- Chosen: `scripts/fetch_olist.py` accepts `kaggle.json` (`{"username","key"}`, HTTP Basic auth) **or** a single bearer-style token file dropped in the same folder under any name (observed as `KGAT_...`, HTTP Bearer auth), plus the matching env vars for each.
- Rejected: **only the classic `kaggle.json` format**, the original design. The account used for this project's real download only exposed the newer personal-token flow — a single token string, no separate username/key pair — so the classic-only script could not authenticate at all against real credentials.
- Trade-off: two auth code paths to maintain instead of one, and Kaggle's exact rules for which accounts see which flow are undocumented from the client side. Accepted because the alternative was a script that could not run against the actual token the project has.

## Cloud sinks are verified offline against fakes, not deferred to the account (Session 2)

**In plain words:** two pieces of Session 1 code had never run once - the thing that writes to S3, and the schemas describing what goes on the Kafka topics. The plan was to run them for the first time in Session 3, against the real services. Instead we ran them now against **fakes**: `moto` pretends to be S3 inside the test process, and `fastavro` checks the schemas against real generator output with no broker involved. Both are free and instant. The first run found a bug that had been sitting there since Session 1.

- Chosen: verify `S3BlobSink` against **moto** and both Avro schemas against **fastavro**, in Session 2, with no AWS account, no Confluent cluster and no Schema Registry in existence.
- Rejected: **waiting for Session 3**, the original plan — first exercise the cloud sinks against the real services once the accounts exist. It looked like the honest sequencing (test the real thing against the real thing) and it was the expensive one. It would have put the first execution of never-run code on the far side of a Snowflake 30-day calendar and a live broker, where a one-line schema or prefix bug is debugged through a serializer, a registry and a cluster at once. It also concentrates every unknown into the same session.
- Rejected: **a hand-rolled fake S3 client.** Zero install and no CI change, but a fake written by the same person who wrote the sink encodes the same misunderstanding twice. `moto` implements S3's actual semantics — including the 1000-key `ListObjectsV2` cap that the sink's paginator exists for, and which a hand-rolled dict-backed fake would never reproduce.
- Trade-off: three more CI dependencies (`moto[s3]`, `boto3`, `fastavro`) and the suite goes from ~9s to ~31s, most of it the deliberate 1005-object pagination test. Paid for immediately: the first run of `S3BlobSink` found a real content-corruption bug in `LocalBlobSink` (see `incidents.md`) that had been present since Session 1 and invisible to all 87 tests.
- Consequence: `KafkaAvroSink.send` is still unexecuted — `confluent_kafka` ships no mock Schema Registry client, so the serializer path genuinely needs Session 3. What is now verified is the part that would actually have broken: the schemas against real generator payloads.

## Line endings are pinned in three places, not left to the platform (Session 2)

**In plain words:** a text file's invisible line-ending characters differ between Windows and Linux, and three separate layers here were each willing to 'helpfully' rewrite them. That meant **the exact bytes of a data file depended on whose laptop made it** - `\r\n` from Windows, `\n` from Linux, for identical input. Databricks reads these files in Session 6. So all three layers are now pinned to one answer instead of asking the operating system.

- Chosen: `to_csv(lineterminator="\n")` in the generator, byte-mode reads and writes in `LocalBlobSink`, `newline=""` in `JsonlSink`, and a `.gitattributes` setting `* text=auto eol=lf` plus explicit `eol=lf` on `*.csv` / `*.jsonl` / `*.json` / `*.avsc`.
- Rejected: **fixing only `LocalBlobSink`**, which is where the failing test pointed. That closes the sink-vs-sink divergence and leaves the deeper one open: `pandas.to_csv()` terminates lines with `os.linesep` even when returning a string, so the *bytes uploaded to S3* would still have been CRLF from a Windows machine and LF from Linux CI. Auto Loader reads those files in Session 6.
- Rejected: **relying on `core.autocrlf`.** It is set to `true` on this machine, which is what produced a genuinely mixed working tree (`generators/_common.py` checked out CRLF, `docs/decisions.md` LF) while `git status` stayed clean, because git normalizes on commit. A setting that hides the inconsistency it creates is not a guard. `.gitattributes` travels with the repo; a local git config does not.
- Trade-off: four lines of `.gitattributes` and one more thing to explain. Against that: a data platform whose artifact bytes depend on the contributor's OS has no reproducibility claim at all, and this project leans on byte-identical replay for the exactly-once experiment.

## CI asserts its optional dependencies are present, rather than trusting the skip (Session 2)

**In plain words:** some tests only run if an optional library is installed. If it isn't, those tests don't fail - they **quietly don't run**, and the suite still prints green. Measured directly: with the libraries hidden, 24 tests vanished and the run still looked like a pass. So CI now checks the libraries are there *before* running any tests, and fails loudly if they aren't.

- Chosen: a CI step that imports `moto`, `boto3` and `fastavro` and fails the build if any is missing, running **before** `pytest`.
- Rejected: **letting the `@pytest.mark.skipif` guards do their job.** They do exactly what they promise — verified directly by simulating absence: 3 passed, **24 skipped**, zero errors. That is the problem. A suite reporting green with 24 tests silently not running is indistinguishable from a suite that ran them, and the S3 and Avro coverage would evaporate the moment someone trimmed the install list to speed CI up.
- Rejected: **installing `-r requirements-dev.txt` in CI** so the deps arrive implicitly. Still rejected for the Session 1 reason (pyspark, delta-spark, databricks-connect and three dbt adapters, none of them imported), and it would not help: an implicit dependency that vanishes still vanishes silently.
- Trade-off: the CI install list and `requirements-dev.txt` can still drift, and the assertion must be widened by hand whenever a new optional dependency appears. Accepted because the failure is now loud and names the missing package, instead of being a number in the pytest summary nobody reads.
- Caveat recorded honestly: **CI has still never executed.** There is no git remote. This workflow is written, YAML-parsed locally, and unrun.

## Region is ap-south-1, inherited from the existing account (Session 3)
- Chosen: **ap-south-1 (Mumbai)** for the S3 landing bucket, and the Databricks workspace must match it.
- Rejected: **us-east-1**, which `.env.example` had carried since Session 1 as an unexamined default. It is the cheapest region and has the widest service coverage, but the AWS account already exists in ap-south-1 with jobpulse's buckets and state in it. Splitting regions means every Databricks read of the landing bucket crosses a region boundary and pays egress on each byte, forever, to save fractions of a cent per GB-month on storage.
- Trade-off: ap-south-1 is marginally pricier per unit and occasionally lags on new-service availability. Both are irrelevant at this project's scale; cross-region transfer on every Bronze load is not.
- Note: caught only because the jobpulse cost investigation surfaced the account's actual region. Had the bucket been created from the template's default, the mistake would have been invisible until the first Databricks read.

## Cost guard excludes credits and is tiered, replacing the default zero-spend budget (Session 3)
- Chosen: two budgets - `ledgerline-monthly` ($20, alerts at 50/80/100% actual plus 100% forecasted) and `ledgerline-daily-spike` ($2/day, actual) - both with **`IncludeCredit: false`**, emailing the address actually read.
- Rejected: **relying on the pre-existing `My Zero-Spend Budget`**. It has two defects. It includes credits, so it reports **$0.00** on an account genuinely spending $11.65 - verified side by side, the new budget reads $11.65 and the old one $0.00 at the same instant. And its threshold is $1 ABSOLUTE, so on an account that legitimately spends ~$19/mo it is permanently in breach and trains you to ignore it.
- Rejected: **a monthly budget alone.** A forgotten cluster costs ~$7-18/day; a monthly total takes a week to cross a threshold. The daily budget catches the same event within ~24h against a measured $0.06/day baseline (~33x headroom).
- Rejected: **AWS Budget Actions** (auto-apply a deny policy or stop instances at a threshold). It is the only guard here that does not require a human, but it is blunt enough to kill a session mid-run. Deferred, not dismissed; revisit if a session ever overruns.
- Trade-off: both budgets are account-wide, so jobpulse's residual ~$3.30/mo counts against the $20. Accepted deliberately - a total-account ceiling is the number that actually matters.
- **Correction to the premise, same session:** the first version of this entry blamed the alert's *delivery address*. The human corrected it - the alerts were read and consciously ignored during a busy period. That changes the conclusion, not just the detail: notification-based guards compete for attention and lose. The budgets are therefore recorded as a **backstop**, and the real guards remain the structural ones that need no human - 10-minute auto-terminate, `Trigger.AvailableNow`, Jobs Compute over All-Purpose. The $70 instance is the proof: hand-launched outside terraform, it had no auto-terminate, and signal was never the missing piece.

## Generators authenticate as a scoped IAM user, not the machine's admin key (Session 3)
- Chosen: IAM user **`ledgerline-dev`** with a single customer-managed policy - `ListBucket`/`GetBucketLocation` on one bucket ARN, `GetObject`/`PutObject`/`DeleteObject` on that bucket's `/*` ARN. Keys live in gitignored `.env`, which `S3BlobSink` reads explicitly.
- Rejected: **the ambient `~/.aws/credentials` default profile**, which on this machine is `varun-admin` - a long-lived, never-expiring, plaintext admin key. Any generator bug, any dependency, any process running as the user inherits full account control. `S3BlobSink` writing CSVs does not need the ability to terminate instances.
- Rejected: **IAM Identity Center / `aws sso login`**, which is the genuinely correct answer - temporary STS credentials instead of permanent ones. Rejected only on setup cost for a solo project; a long-lived key is a knowing downgrade, recorded as such rather than glossed.
- Rejected: **an IAM role.** A role needs an identity to assume it - an instance profile, a Lambda execution role, a service account. The generators run on a laptop, which has no AWS-native identity to trade in.
- Trade-off: a second credential to rotate, and a failure mode where a blank `.env` silently falls back to the admin key. `.env.example` now warns about exactly that.
- Verified, not asserted: the credential writes/lists/deletes in its own bucket, and is **denied** on another bucket, `ec2:DescribeInstances`, `s3:ListAllMyBuckets` and `iam:ListUsers`. Note there is no `Deny` statement anywhere - all four are blocked by implicit deny.

## The `reviews` row-count contract stands at 99,224 (Session 3)
- Chosen: **keep `expected_rows=99_224`.** The recorded drift is withdrawn - it was never real.
- Rejected: **updating the contract to 104,719**, the number recorded in Session 1 as the dataset's actual size. That number is a count of physical lines, not of CSV records. 3,852 review comments contain embedded newlines totalling exactly 5,495, and `99,224 + 5,495 = 104,719`. Adopting it would have written a measurement error into the contract permanently and made the correct file fail validation.
- Rejected: **leaving it open for another session.** It had already survived three, precisely because it was filed as an external fact about Kaggle rather than as a claim about our own measurement.
- Trade-off: none. Full incident entry in `incidents.md`, including the live consequence for Session 4 - Spark and Auto Loader default to `multiLine=false` and will corrupt exactly these 3,852 records without raising an error.

## The CI runner is pinned to ubuntu-24.04, not `ubuntu-latest` (Session 3)
- Chosen: **`runs-on: ubuntu-24.04`**, plus `actions/checkout@v5` and `actions/setup-python@v6`.
- Rejected: **`ubuntu-latest`**, which the first-ever CI run itself warned about - GitHub migrates that label to Ubuntu 26 on **2026-10-19**. `latest` is not a version, it is a promise to change on someone else's schedule. The migration swaps the OS, system libraries, the default Python build and the C toolchain that pandas/pyarrow wheels resolve against. A green suite would turn red with no commit in between, and the first instinct would be to hunt the bug in our code.
- Rejected: **treating the two annotations as cosmetic.** The Node 20 deprecation is genuinely minor. The runner migration is not, and it arrives in the same week as the AWS credit expiry (2026-10-17) - two environment changes at once is exactly when a mysterious failure costs the most to diagnose.
- Trade-off: the pin must now be bumped by hand, and a stale runner eventually loses support. That is the intended cost - a deliberate bump that fails loudly beats an automatic one that fails mysteriously.
- Consistency note: this is the same principle already recorded in "CI installs an explicit dependency list, not requirements-dev.txt". The runner was the largest unpinned surface left in the pipeline, and it was unpinned only because nobody had looked - CI had never executed until this session.
- **Open, deliberately not decided here:** the install list uses floors (`pandas>=2.2`, `ruff>=0.6`, ...), not pins (`==`). Floors take upstream fixes automatically at the cost of reproducibility; an upstream release can redden CI with no local change. `pyproject.toml`'s explicit ruff rule set covers changed *defaults* but not changed behaviour within a rule. Raised in Session 3, left to a session that has a reason to settle it.

## The recall log and background sheet stay local, and history was rewritten to match (Session 3)
- Chosen: `docs/learning.md` and `docs/background.md` are **gitignored**, and the repository's history was rewritten with `git filter-repo` so they appear in **no commit**, not merely in the latest one.
- Rejected: **`git rm --cached` alone.** It stops future tracking but leaves both files fully readable inside every commit that already contained them — and both had already been pushed. This is the same property that makes a leaked secret unrecoverable by deletion: once pushed, it lives in the history until the history is rewritten.
- Rejected: **leaving them published.** `learning.md` records recall-check answers, including wrong ones; `background.md` is prior work experience. Both are one person's working notes. The project's story is told by `decisions.md`, `incidents.md`, `progress.md` and `runbook.md`, and none of it depends on them.
- Rejected: **deferring the rewrite until the repo goes public.** History rewriting gets monotonically more expensive with every commit. Six commits is the cheapest it will ever be, and a portfolio project is very likely to be made public eventually.
- Trade-off: every commit SHA changed and a force push was required. Safe only because the repo is private with a single author and no other clones — this would be unacceptable on a shared branch. It also stripped the remote and the branch's upstream tracking, both re-established by hand.
- Consequence, handled: `progress.md` cites `learning.md` eight times and is itself published, so those references now point at a file a reader cannot open. Rather than editing eight past entries — which rule 4 forbids — a preamble note in `progress.md` states that the recall log is deliberately local-only. The citations stand as written, so the record still shows the check happened.
- Verified, not assumed: every commit on `origin/dev` was walked individually and neither file appears in any tree. Checking only the tip commit would have proven nothing, since a file can survive a rewrite by persisting in an intermediate commit.

## `.env` is loaded at entry points only, and the S3 sink refuses the ambient fallback (Session 4)

**In plain words:** the scoped AWS key from Session 3 was sitting in a file that
no code opened, so the generator was quietly using the machine's admin key
instead. Two changes: something now actually reads `.env`, and the S3 sink
refuses to run at all if the scoped key is missing - rather than falling back to
whatever credentials the machine happens to have.

- Chosen: `load_local_env()` in `_common.py`, called from the `main()` of each
  generator and nowhere else; `S3BlobSink` builds its client via
  `_s3_client_from_env()`, which **raises** when `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` are absent and passes `AWS_REGION` explicitly.
- Rejected: **calling `load_dotenv()` inside `_kafka_config_from_env()` and
  `_s3_client_from_env()`**, which is where it first went. It is more convenient
  - any caller gets config without thinking - and it is wrong. It was caught
  immediately by `test_kafka_config_names_every_missing_variable`: the test
  clears the four variables to assert the error names them all, and the function
  loaded them straight back off disk, so the error path became unreachable.
  A library function that reads an untracked file inherits the developer's
  machine into every test result.
- Rejected: **keeping boto3's default credential chain and relying on the
  `.env.example` warning.** That warning has been accurate and prominent since
  Session 3 and prevented nothing, because the failure is a success: the call
  works, as the wrong identity. This is the same conclusion Session 3 reached
  about budget alerts - a guard needing a human to read it is not a guard - and
  the structural version is to delete the fallback.
- Rejected: **`boto3.Session(profile_name="ledgerline-dev")`.** It would work,
  but it moves the credential into `~/.aws/credentials`, which is the file we
  are trying to stop reading, and it is invisible to CI and to Databricks later.
- Trade-off: constructing `S3BlobSink` now requires credentials in the
  environment, so an interactive experiment cannot lean on an ambient profile.
  That is the intended cost. Tests are unaffected - they inject a `moto` client
  through `client=`, which is also precisely why they never caught this.
- Consequence recorded honestly: the S3 tests all bypass the credential branch
  by design. Test coverage of `S3BlobSink` says nothing about which identity it
  authenticates as, and the new
  `test_s3_sink_refuses_to_fall_back_to_ambient_admin_credentials` covers only
  the refusal, not the success path. Proving the *scoped* key is the one in use
  needs a real call against the real bucket.

## Confluent tier and cluster authorization are fixed at creation (Session 4)

**In plain words:** Confluent's Basic cluster is free at rest; Standard is about
$385/month. A cluster was first created as Standard by accident. You cannot
change a cluster from Standard down to Basic - only up - so it had to be deleted
and remade. And choosing Basic has a second, non-obvious effect: it changes how
you grant permissions.

- Chosen: **Basic**, AWS, `ap-south-1`, recreated after the first cluster came up
  Standard. Topics `orders` and `inventory.cdc`, 3 partitions each, on
  `cluster_0` (`lkc-2255zy2`).
- Rejected: **keeping the Standard cluster.** ~$385/month against a ~$22 total
  project budget - 17x the entire budget, monthly - buying a 99.99% SLA,
  infinite storage and audit logs, none of which this project uses. Trial
  credits would have hidden it for about a month and then stopped hiding it.
- Verified constraint, not re-discoverable cheaply: Confluent documents only
  *"you can upgrade from Basic to a Standard cluster at any time"*. There is no
  downgrade. The tier is a creation-time commitment, so correcting it costs a
  delete and a rebuild - trivial at 0 topics, not trivial later.
- Second-order consequence, **not chosen deliberately and worth naming**:
  Confluent's docs state that RBAC on *granular Kafka resources* - topics,
  consumer groups, transactional IDs - is supported only on Standard,
  Enterprise, Dedicated and Freight. **Basic is excluded.** So picking Basic for
  cost moved topic-level authorization off RBAC and onto ACLs, a different
  console surface entirely. A cost decision silently became a security-model
  decision.

## `CloudClusterAdmin` on one cluster, instead of per-topic ACLs (Session 4)

**In plain words:** the generator's identity needs permission to write to two
topics. The precise way to grant that on a Basic cluster is an ACL per topic.
We instead granted a broader role covering the whole cluster, because it is one
click in the console and ACLs are not. The credential can therefore delete the
topics it writes to.

- Chosen: service account `serviceacc_ledgerline` (`sa-5w01oxq`) holds
  **`CloudClusterAdmin` on `cluster_0`** plus **`DeveloperWrite` on all schema
  subjects** in environment `default`, with two separately scoped API keys - one
  for the cluster, one for Schema Registry.
- Rejected: **`WRITE` + `DESCRIBE` ACLs on `orders` and `inventory.cdc`.** This
  is the correct least-privilege answer and remains the target. Deferred because
  Basic pushes topic authorization onto ACLs, which are not reachable from the
  role-assignment screens, and the session was already several console detours
  deep. `DESCRIBE` matters as much as `WRITE`: a producer that can write but not
  fetch topic metadata fails at startup and reads like a connection fault.
- Rejected: **an API key on the human user account**, which is what the first
  cluster key actually was before it was deleted. It inherits everything that
  user can do across the whole organisation - the exact shape of the
  ambient-admin-key problem Session 3 rejected on AWS.
- Trade-off: `CloudClusterAdmin` can delete topics and create API keys on that
  cluster. It is bounded to one cluster in one environment, not the org, but it
  is broader than the generator needs, and it is a **knowing downgrade** recorded
  as such - the same framing as Session 3's note that a long-lived IAM key is a
  knowing downgrade from Identity Center.
- Revisit: when the first produce is green. Note for that day, from Confluent's
  docs - removing a role binding does **not** delete API keys created under it,
  so the keys must be revoked deliberately rather than assumed gone.

## Databricks is deferred out of Session 4 on idle cost, not on effort (Session 4)

**In plain words:** creating a Databricks workspace on AWS also creates a NAT
gateway in your own AWS account. That bills about $33-41 a month, around the
clock, whether or not a cluster ever runs - one and a half to two times this
project's entire budget, for something sitting idle. None of the cost guards
this project has built would catch it.

- Chosen: **do not create the Databricks workspace in Session 4.** Session 4's
  actual deliverable is the first live Kafka produce and the first real S3 write;
  Bronze ingest is Session 5/6.
- Rejected: **creating it now because the signup was on the session plan.**
  Databricks' automated workspace configuration provisions a cross-account IAM
  role, an S3 bucket and a customer-managed VPC into your AWS account, and its
  stated requirements include an available **NAT gateway**. AWS prices a NAT
  gateway at $0.045/hour (their own worked example; `ap-south-1` is ~$0.056), so
  ~$33-41/month at 730 hours - before a single DBU.
- The reason this matters more than the number: **every cost guard in this
  project targets cluster time.** 10-minute auto-terminate, single-node dev,
  serverless preference, Jobs over All-Purpose, `Trigger.AvailableNow` - all of
  them control compute that starts and stops. A NAT gateway is none of those. It
  is the same structural shape as the `jobpulse-dashboard-dev` instance from
  Session 3: a resource created outside the thing that enforces shutdown, which
  is why it ran 146 days and cost ~$70.
- Trade-off: Bronze work cannot start until the workspace exists, so a later
  session carries the setup instead. Against that, a week of idle NAT for a
  workspace nobody opens is ~$8-10 of a ~$22 budget spent on nothing.
- Decided in advance for when it is created: **pay via the linked AWS Marketplace
  account rather than a credit card.** Databricks supports either. Marketplace
  routes DBU charges through the AWS bill, which means Session 3's
  `ledgerline-monthly` ($20) and `ledgerline-daily-spike` ($2/day) budgets see
  them. With a credit card those budgets only ever see EC2, and every DBU is
  invisible to every guard already built.
- Verified while checking this: Databricks **discontinued the Standard tier** for
  new AWS customers and auto-upgraded remaining Standard workspaces to Premium on
  2025-10-01. The AWS tier table now lists only Premium and Enterprise, so
  "Premium" is the floor rather than an upgrade to choose. Also noted: the
  published list price for Data Engineering DBUs currently reads $0.15/DBU and
  Interactive $0.40/DBU, against the ~$0.22 and ~$0.55-0.65 recorded in
  `CLAUDE.md`. Left as a flagged discrepancy rather than silently adopting the
  friendlier number - rates vary by region and by serverless vs classic, and EC2
  is billed separately by AWS either way.

### Correction (2026-09-26, Session 6) — the Marketplace payment route does not exist for this account

The "decided in advance" bullet above chose AWS Marketplace billing so that
DBUs would appear in the AWS budgets. **That route is not available here.** The
account's Service provider is *Amazon Web Services India Private Limited*, and
AWS Marketplace does not accept cards from Indian (AISPL) accounts. The subscribe
page refused with "invalid or unsupported payment method". Full account in
`incidents.md`; the replacement choice is the Session 6 entry at the end of this
file. The reasoning in the bullet — budgets can only guard what lands on the AWS
bill — was sound; the premise that the route existed was never checked against
this account.

## Every Kafka client names itself; the default `client.id` is refused (Session 4)

**In plain words:** Confluent's monitoring screens show which clients are
connected to the cluster. All of ours showed up as the same word - `rdkafka` -
because none of them said who they were. With two generators and a couple of
debug scripts running, the Clients page read `rdkafka`, `rdkafka`, `rdkafka`,
and there was no way to tell which was which.

- Chosen: `KafkaAvroSink` sets `client.id`, passed by each generator as
  `ledgerline-{SOURCE}` - so `ledgerline-order_events` and
  `ledgerline-inventory_cdc`. When no id is passed it falls back to one derived
  from the topic names, so the default can never be reached.
- Rejected: **librdkafka's default**, which is the literal string `rdkafka`. It
  costs nothing and works fine with exactly one client. Observed failing in
  Session 4 with three: Confluent's Stream Lineage reported
  `producer rdkafka (2)` and the cluster reported `Clients 3`, none of which
  could be attributed to a source. At the full 394,090 + 158,396 events across
  two topics, "which client is lagging" has to be answerable, and it was not.
- Rejected: **setting it per-run with a timestamp or PID**, which would make
  every execution unique. That is better for tracing one run and worse for
  everything else - the Clients view becomes a list of dead one-offs, and you
  cannot ask "how is the order generator behaving over time" because it is
  never the same client twice.
- Trade-off: two generators sharing a cluster now collide in the metrics if
  they are ever run concurrently from two machines, since the id is per-source
  rather than per-process. Accepted deliberately: aggregating by source is the
  question actually worth asking here, and a per-process id can be added later
  by suffixing without changing the scheme.
- Guarded: `test_kafka_sink_never_uses_the_default_client_id` asserts the
  producer config sets `client.id` and that the fallback contains no `rdkafka`.
  It tests the *fallback*, because the generators pass an explicit id today and
  the branch that could regress is the one where a caller forgets.

## The full produce appends to the existing topics rather than recreating them (Session 5)

**In plain words:** the two topics already held Session 4's small test run. The
full produce could either be added on top, or the topics could be deleted and
rebuilt so the counts came out round. We added on top. Deleting a topic so a
number looks tidy is editing the record, and the messy number turned out to be
the most informative thing this session produced.

- Chosen: **produce the full run into the existing topics.** `orders` now holds
  394,293 records with 394,090 distinct `event_id` — the extra 203 are Session
  4's run replayed byte-for-byte. `inventory.cdc` holds 158,439 with 158,399
  distinct.
- Rejected: **delete and recreate both topics for a clean baseline.** It would
  have made the readback read 394,090 of 394,090 and measured partition skew on
  exactly one produce. Against that: 203 extra records in 394,293 is 0.05% and
  moves the skew figure by nothing measurable, and the duplicates are direct
  evidence for the Session 1 deterministic-id decision — a replay produces the
  *same* events, which is what `exp_01` has to assert on. With `uuid4` the
  topic would read 394,293 distinct ids and the duplication would be invisible
  in the data.
- What this bought that a clean topic would have hidden: the CDC half did
  **not** come back as clean duplicates. It came back with 43 keys carrying a
  tied `seq`, which is a real defect in how a partial run interacts with the
  Silver guard, found only because the old data was still there to collide
  with. Recorded as an incident. Recreating the topics would have deleted the
  evidence before anyone read it.
- Trade-off: every future count on these topics carries a +203/+93 offset that
  has to be explained rather than read off. Accepted, and the arithmetic is
  written down in `progress.md` so the explanation is one lookup rather than a
  re-derivation.

### Correction (2026-09-26) — the topics were recreated after all, on a new account

This entry chose to append to the existing topics rather than recreate them,
and the reasoning holds for the decision as it was faced. It was overtaken by
events: the Confluent account became unreachable and everything was rebuilt on
a new one, so the topics are clean regardless of what was chosen.

What the decision got right is worth keeping even though it was undone. The
203 duplicates it preserved are the reason the CDC collision was found at all —
a clean topic would have had nothing to collide with, and the `seq` defect
would have surfaced in Session 9 against data nobody could trace to a smoke
test. The choice not to tidy paid for itself within the hour, then the tidying
happened anyway for an unrelated reason.

The rebuilt topics read 394,090 of 394,090 distinct and 158,346 of 158,346
distinct. See the migration addendum in `progress.md`.

## The topic verifier assigns partitions and never commits (Session 5)

**In plain words:** to check what actually landed on a topic you have to read
it, and reading a Kafka topic normally leaves a mark — a consumer group, a
committed offset, a position the next reader starts from. A tool that changes
what it measures is not a measurement. This one reads without joining a group
and without committing anything.

- Chosen: `scripts/inspect_topic.py` calls **`assign()`** on every partition at
  offset 0, with `enable.auto.commit=False`, and reads until each partition
  reaches the high watermark it fetched up front.
- Rejected: **`subscribe()` with a consumer group**, which is the normal way to
  consume. It joins the group protocol, triggers a rebalance, and leaves a
  group visible in the console with committed offsets attached. The second run
  would then start where the first stopped and print zero records — and the
  natural reading of "zero records" is "the topic is empty", not "I moved my
  own bookmark". That misreading is on the carried weak-spot list from Session
  4 as an unanswered question, which is part of why it is worth building the
  version that cannot produce it.
- Rejected: **poll until nothing arrives for N seconds**, the usual shortcut
  for "read to the end". It cannot distinguish a finished read from a stalled
  one, which is precisely the confusion that cost Session 4 three minutes of
  silence. High watermarks make the end of the topic a number fetched before
  the read starts.
- Consequence worth keeping: because it never joins a group, the verifier needs
  no consumer-group ACL — only `READ` and `DESCRIBE` on the topic. The
  least-privilege set got smaller as a side effect of the correctness choice.
- Trade-off: `assign()` does not follow partition additions. If either topic is
  ever expanded past 3 partitions, the verifier reads the partitions that exist
  at the moment it starts and would silently miss a fourth. Acceptable while
  partition count is fixed by the cluster's Basic tier and stated here so it is
  not discovered as a mystery undercount.

### Correction (Session 5, same session) — the verifier DOES need a consumer-group ACL

The entry above says, as a selling point of assigning rather than subscribing:

> "because it never joins a group, the verifier needs no consumer-group ACL —
> only `READ` and `DESCRIBE` on the topic. The least-privilege set got smaller
> as a side effect of the correctness choice."

**That is wrong, and it was tested the same session.** Rebuilt on a new
Confluent account with exactly those topic-only ACLs, the verifier failed on
the first read:

```
GROUP_AUTHORIZATION_FAILED — FindCoordinator response error
```

**What the reasoning got right and what it got wrong.** `assign()` does avoid
the group *rebalance* protocol — no JoinGroup, no SyncGroup, no partition
reassignment, no committed offsets. All of that stands. What it does not avoid
is the **group coordinator lookup**. librdkafka resolves a coordinator for
whatever `group.id` is configured, and it does so eagerly, before any commit
is attempted and regardless of `enable.auto.commit=False`. That lookup is
itself authorized against the consumer-group resource. So the permission is
needed for a call that happens *before* the behaviour the permission is
usually about.

**Fix applied:** one more ACL, `READ` on consumer group `ledgerline-verifier-`
with pattern **PREFIXED** — prefixed rather than literal because the verifier
names its group per topic (`ledgerline-verifier-orders`,
`ledgerline-verifier-inventory-cdc`).

**The design choice itself was not wrong.** Assigning instead of subscribing is
still correct, and still for the stated reason: a verifier must not leave
committed offsets behind, or its second run reads zero records and the natural
misreading is "the topic is empty". What was wrong was a *bonus* claimed on top
of it — that the correctness choice also shrank the permission set. It did not.

**Why the mistake is worth keeping.** It came from reasoning about a client
library's behaviour from its API surface instead of running it. `assign()`
versus `subscribe()` is a real distinction and the inference from it was
plausible; it was simply never executed against a broker that would refuse.
Session 4's carry-forward list already flags *mechanism inside a client
library* as the weakest area on the list, twice over — this is a third
instance of the same class, and the first one caught by a permission denial
rather than by a wrong number.

**Prevention rule: a permission set derived by reading code is a hypothesis
until a credential refuses something.** The way to test it is to grant exactly
the derived set and run the real workload — which is what happened here, only
by accident rather than by design. Deriving grants from the calls a program
makes is still the right method; it just produces a claim that has to be
executed, not a conclusion.

## A partial CDC run is refused to Kafka; a partial order run is not (Session 5)

**In plain words:** running the CDC generator over the first 50 orders and
producing that to the shared topic corrupts it, because the small run and the
full run disagree about starting stock levels while stamping their events with
identical sequence numbers. Running the *order* generator over the first 50
orders and producing that is harmless. The generators are therefore treated
differently on purpose, which looks inconsistent and is not.

- Chosen: `inventory_cdc.py` **refuses `--limit` together with `--sink kafka`**
  unless `--allow-partial` is passed, and the refusal explains the mechanism
  rather than only denying it.
- Why the two sources differ, measured rather than assumed:
  - an **order's lifecycle** is built from that order's own timestamps and does
    not depend on how many other orders were loaded, so a `--limit` run is a
    true prefix. All 203 records Session 4 left on `orders` came back as
    byte-identical duplicates;
  - a **SKU's seed stock** is `headroom x lifetime demand` measured over the
    orders the run was given, so the same SKU seeds at 14 units from 50 orders
    and 66 units from 99,441 — while carrying the same `seq`, because `seq`
    derives from event time and event time does not depend on `--limit`.
- Rejected: **guarding both generators for consistency.** Tidier, and wrong: it
  would forbid a safe and useful thing (smoke-producing a few hundred orders)
  on the strength of a resemblance. A test asserts the asymmetry so that
  "make them the same" has to argue with something.
- Rejected: **making `seq` unique across runs**, for instance by mixing in a run
  id or a wall-clock stamp. That would break replay determinism, which is the
  property `exp_01` exists to assert and the reason `assign_seq` derives `seq`
  from event time in the first place. The problem is not that `seq` is
  insufficiently unique; it is that two different datasets were put in one
  topic.
- Rejected: **cleaning the 43 poisoned SKUs off the topic.** They are better
  material than a pristine feed: Session 9's `seq` guard and in-batch dedup now
  have a real tie to fail on, and Session 10 can assert the bug is present
  before fixing it, which is a stated success criterion.
- Trade-off: `--allow-partial` exists, so the guard is a speed bump rather than
  a wall. Deliberate — Session 10 contaminates the topic on purpose, and a
  guard with no override would be worked around by editing the source, which is
  worse than an audited flag.

## Confluent authorization splits into a writer and a reader, by ACL (Session 5, designed — not yet applied)

**In plain words:** today one identity can do everything to the cluster,
including delete the topics it writes to. The replacement is two identities: one
that can only write the two topics, and one that can only read them. The
verifier that checks what landed then *cannot* alter what it is checking — a
property the code currently only promises.

**Status: designed and recorded, not applied.** The Confluent CLI is not
installed on this machine and the change is a console action. Written down now,
at the moment the decision was made, rather than after it is executed — the
discipline's first rule. `progress.md` records it under *not done*.

- Chosen: retire **`CloudClusterAdmin`** and replace it with per-topic ACLs
  across **two** service accounts:

  | Identity | ACL | Why |
  |---|---|---|
  | `sa-ledgerline-producer` | `WRITE`, `DESCRIBE` on `orders` and `inventory.cdc` | what the two generators do |
  | `sa-ledgerline-verifier` | `READ`, `DESCRIBE` on `orders` and `inventory.cdc` | what `inspect_topic.py` does |

- Derived from the code, not from a template. Each grant traces to a call:
  - `DESCRIBE` — both clients fetch topic metadata at startup. A producer that
    can `WRITE` but not `DESCRIBE` fails before sending anything and the error
    reads like a connection fault, which is the trap the Session 4 entry
    flagged in advance.
  - `WRITE` — `Producer.produce`.
  - `READ` — the verifier's fetches, plus `get_watermark_offsets`.
  - **No consumer-group ACL for either.** This is the part worth noticing: the
    verifier `assign()`s partitions instead of subscribing, so it never joins
    the group protocol and never commits. A design choice made for correctness
    — a verifier must not disturb what it measures — turned out to shrink the
    permission set as well. The two arguments point the same way, which is
    usually a sign the design is right.
  - **No `IDEMPOTENT_WRITE` on the cluster.** The producers run with
    `enable.idempotence=true`, which on older brokers required a cluster-level
    grant; since KIP-679 (Kafka 3.0) topic `WRITE` covers it, and Confluent
    Cloud is well past that. Named here because if the produce breaks after the
    switch, this is the first thing to suspect.
- Rejected: **one service account holding both read and write ACLs.** Half the
  console work and it keeps the current single-credential setup. Rejected
  because it throws away the only enforcement available: with one identity,
  "the verifier does not write to the topic" is a claim about code that could be
  changed by an edit; with two, it is a claim about a credential that cannot.
  The same reasoning as Session 3's scoped S3 user — and Session 4 proved that
  reasoning is not theoretical, because the scoped user existed for a whole
  session while the code quietly authenticated as an admin.
- Rejected: **keeping `CloudClusterAdmin` now that the produce is green.** It
  was accepted in Session 4 as a knowing downgrade with an explicit revisit
  condition: "when the first produce is green." It is green — 394,090 and
  158,346 events landed and were read back. The condition fired, so the
  downgrade expires rather than quietly becoming permanent.
- **Keys must be revoked deliberately, not assumed gone.** Carried from the
  Session 4 entry and still the trap: removing a role binding does **not**
  delete API keys created under it. The old cluster key keeps working with
  whatever the account can still do. Order of operations matters — create and
  test the new credentials *before* deleting the old ones, or a wrong ACL set
  locks the project out of its own cluster with no working key to fix it.
- Trade-off: two more credentials in `.env` and two more things to rotate, on a
  project whose Kafka clients are two generators and one script. Accepted: the
  split is the interview-defensible answer and the cost is a few lines of
  configuration, not ongoing effort.

## The AWS account was upgraded from the Free plan to the Paid plan (Session 6)

**In plain words:** the AWS account this whole project runs on was on AWS's
Free plan, which **closes the account on a fixed date** — Oct 17, 2026, 22 days
out — or when its credits run out, whichever comes first. No document in five
sessions had recorded that. It was found on the console home page while getting
ready to create the Databricks workspace, and the account was upgraded to the
Paid plan the same day (confirmed by AWS email, 2026-09-26).

- Chosen: **upgrade to the Paid plan** before creating anything on Databricks.
- Why it was forced, not merely preferred — two independent reasons:
  1. **The closure date would have taken the project down mid-build.** The S3
     landing bucket, both budgets, the scoped `ledgerline-dev` user and any
     Databricks workspace network all live in this account. Oct 17 falls around
     Session 8-9, in the middle of Silver.
  2. **Free plan accounts cannot subscribe to paid AWS Marketplace offers**
     (AWS docs: Free plans exclude "certain AWS Marketplace offers that can
     incur charges"). Session 5 chose Marketplace billing for Databricks
     precisely so its charges land in `ledgerline-monthly` and
     `ledgerline-daily-spike`. On the Free plan that route does not exist.
- Credits are not lost: AWS applies the remaining **$68.53** to future bills
  after upgrading, until they expire. At the measured September run-rate
  (forecast **$12.42** for the month) the credits were never going to be the
  constraint — **the calendar was**, the same shape as the Snowflake trial.
- Rejected: **stay on the Free plan and sign up to Databricks directly with a
  card** (it offers a free trial). Avoids the upgrade, but fixes neither
  problem: the account still closes Oct 17, and card-billed DBUs are invisible
  to every AWS budget built in Session 3 — the exact gap the Marketplace choice
  existed to close.
- Consequence: credits no longer cap spend. On the Free plan, running out of
  credits stopped the account; on the Paid plan it starts charging the card.
  The `IncludeCredit: false` budgets were already measuring gross spend, so
  they need no change — they were the right design for exactly this moment.
- Why nothing caught it for five sessions: every AWS action so far was done as
  `varun-admin`, and **`varun-admin` cannot see billing** — the Cost and usage
  panel reads "Access denied" for it. The free-plan notice and the countdown
  only render for a principal with billing access. Session 3's Cost Explorer
  work evidently ran as root and did not look at the plan banner.
- Rule taken from it: **when a project depends on an account, record the
  account's own expiry alongside the trials.** A 30-day Snowflake trial was
  tracked from day one because it was chosen; the AWS clock was inherited from
  jobpulse, so nobody chose it and nobody wrote it down.
- **Verified on the console, not only by email.** After the upgrade the Cost
  and usage panel no longer carries the sentence "Credits cover your free plan
  costs. Your access to AWS services will end when credits are depleted or free
  period ends", the Upgrade action is gone, and credits still read **$68.53**.
  "Days remaining" now reads *Unable to load* rather than a date — the widget
  has no countdown left to render. Weaker evidence on its own than a blank
  field would be, but consistent with the email and the removed sentence.

## Databricks is billed directly by Databricks, not through AWS Marketplace (Session 6)

**In plain words:** the account was signed up for at databricks.com, on its
14-day free trial ($400 of usage credit), instead of through AWS Marketplace.
Marketplace was the plan, but it refuses cards on Indian AWS accounts. The cost
is that **Databricks' own charges never appear in the AWS budgets**, so a budget
inside the Databricks account console has to do that job instead.

- Chosen: **direct Databricks signup, 14-day trial**, workspace created into
  the existing AWS account (`240939827246`, `ap-south-1`, Premium) with the
  Quickstart CloudFormation template. After the trial it rolls onto
  pay-as-you-go, billed by Databricks to the card.
- Rejected: **AWS Support → Pay By Invoice, then Marketplace.** Keeps the one
  property the Session 4 decision wanted — DBUs on the AWS bill — but AWS says
  the switch can take up to seven days, and Databricks work would wait for it.
  Worth less than it looks, too: during the trial DBUs are paid from trial
  credit, so the AWS budgets would read $0 for Databricks until the trial ended
  either way.
- Rejected: **Databricks Free Edition.** Serverless-only and no NAT gateway, so
  it is the cheapest option by far. Not chosen because it was not verified that
  Free Edition can reach an external Kafka cluster or a customer S3 bucket, and
  those two connections are all of Bronze. Left open, not dismissed — it is the
  right thing to check if the NAT cost becomes the problem.
- What the AWS budgets still see: the NAT gateway, the EC2 instances clusters
  run on, the workspace's S3 root bucket. What they do **not** see: DBUs.
- Guard that replaces the lost visibility: **a budget alert in the Databricks
  account console**, created before the first cluster runs. Already listed in
  `CLAUDE.md` as a planned guard; it is now the only guard on DBU spend rather
  than a second one.
- Consequence: three clocks now run at once — Databricks trial (~2026-10-10,
  then pay-as-you-go, no stop), Snowflake trial (30 days from whenever it is
  started; not started), and the NAT gateway (~$1.10/day from workspace
  creation until the network stack is deleted). The plan that follows from
  them: do the Databricks sessions (6-12) back to back inside the trial, tear
  down the workspace network, then start Snowflake for Gold at Session 13.
- Account identity: the Databricks account is registered to an email address
  the owner controls long-term, not necessarily the AWS one. Recorded because
  Session 5's Confluent account was lost with its email and had to be rebuilt
  from scratch.

### Correction (2026-09-26, same session) — the direct-signup trial above was never created

The entry above records a direct Databricks signup on the 14-day trial. **That
did not happen.** Signing in at databricks.com landed in an existing
**Databricks Free Edition** account on the same email, and the next entry
records why the project stayed there. The trial reasoning above is left as
written; it is still the fallback if Free Edition stops being enough.

## Databricks runs on Free Edition, proven by reading both sources, not by reading the docs (Session 6)

**In plain words:** Databricks' free, no-expiry, serverless-only tier turned
out to reach both of this project's sources — the S3 landing bucket and the
Confluent Kafka cluster — so the project uses it instead of a paid workspace.
**No NAT gateway, no trial clock, $0 for Databricks.** The docs did not say it
would work; two notebook reads proved it did.

- Chosen: **Databricks Free Edition** (workspace `dbc-7ce3c403-9521`,
  metastore `metastore_aws_us_east_2`), serverless compute only.
- The decision rule, set by the human before testing: *if both sources can
  be read, Free Edition; if either fails, the 14-day trial.* Both passed:
  - **S3:** Unity Catalog storage credential `ledgerline_landing_read` →
    IAM role `ledgerline-uc-landing-read` (read-only, prefix-scoped to
    `ledgerline/`) → external location `ledgerline_landing` (read-only, file
    events off). A notebook read `ledgerline/dims/customer/` and returned
    **8,395 rows**, equal to the three customer CSVs parsed locally.
  - **Kafka:** credentials stored as **Unity Catalog secrets** in
    `workspace.ledgerline_secrets` (`kafka_api_key`, `kafka_api_secret`,
    `sr_api_key`, `sr_api_secret`), created in the browser — no CLI. A batch
    read of `orders` returned **394,090 records**, split 131,458 / 132,187 /
    130,445 across the three partitions.
- Why the docs could not settle it: the Free Edition limitations page never
  mentions storage credentials, external locations or Kafka — neither allowed
  nor forbidden — and says outbound internet is "restricted to a limited set of
  trusted domains". Silence is not support, so it was tested. Identity
  verification (which widens egress) was **not** needed.
- Rejected: **the 14-day trial, workspace in this AWS account.** Classic
  clusters, the account console, and a workspace beside the data in
  `ap-south-1`. Costs a NAT gateway at ~$1.10/day from creation, a trial that
  rolls into pay-as-you-go around 2026-10-10, and DBU spend invisible to the
  AWS budgets (Marketplace is closed to this AISPL account). For a ~$22
  project, paying that to get what the tests showed is already available was
  not justified.
- Rejected: **waiting up to seven days for AWS Pay By Invoice, then
  Marketplace.** Solves only billing visibility, which Free Edition makes moot.
- What is given up, named rather than glossed — the interview surface this
  project loses:
  - **No classic clusters.** No instance types, no single-node vs multi-node,
    no hands-on 10-minute auto-terminate, no Jobs Compute vs All-Purpose
    pricing. These become things explained, not things done.
  - **No account console**, so no Databricks budget alerts and no
    account-level APIs. Nothing to guard, since nothing is billed.
  - **Quotas:** 5 concurrent job tasks, one active pipeline per type, one
    2X-Small SQL warehouse. Sufficient for this data volume.
- What is gained besides cost: serverless only permits `AvailableNow` / `Once`
  triggers — a `ProcessingTime` stream raises
  `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. **The forgotten always-on stream,
  which `CLAUDE.md` names as the project's highest cost risk, cannot be
  started here.** And Unity Catalog secrets are more Databricks surface than
  the classic secret scopes would have been.
- Cross-region, by accident not design: the workspace is in **us-east-2
  (Ohio)**; the bucket and Confluent are in **ap-south-1 (Mumbai)**. Free
  Edition does not let you pick. Inter-region S3 transfer is cents at this
  volume.
- **Unexplained, recorded rather than guessed:** each full read of `orders`
  (~0.1 GB) took **~3 min 43 s**. Serverless start-up and a Mumbai→Ohio
  internet path are the suspects; not measured. During the first run I
  misread "0 rows read, 3 tasks running" as blocked worker egress. It was just
  slow — the rows-read counter only moves when tasks finish.
- Risks accepted: Free Edition is **non-commercial use only** (fine for a
  learning project) and **accounts inactive for extended periods may be
  deleted** — the same class of loss as the Session 5 Confluent account.
  Mitigation is structural: everything that matters lives in git and S3, and
  Bronze is rebuildable from both.
- Carried from the test: it reused the consumer-group prefix
  `ledgerline-verifier-`, which already had a READ ACL, so the test measured
  connectivity and nothing else. **Bronze gets its own group prefix and ACL**
  before `stream_orders.py` runs.
- Consequence for the plan: the Databricks clock is gone. The Session 6
  "three clocks" reasoning reduces to one — Snowflake's 30 days, still not
  started, still for Session 13.

## Scope grows by three: Airflow, windowed streaming, and GDPR erasure (Session 6)

**In plain words:** three skills the project did not cover are added — an
Airflow pipeline that runs every platform end to end, streaming with windows
and watermarks, and GDPR-style "delete this customer everywhere". Each was
added only after finding a place where a real shop with this data would
genuinely use it, per the thesis. Roughly 3-4 extra sessions, $0 extra.
Placement below is tentative and will be fixed when each session opens.

### 1. Airflow orchestrates the whole pipeline, across platforms

- Chosen: **one Airflow DAG** (local, Docker) that runs produce → Databricks
  Bronze/Silver jobs → Parquet export → Snowflake `COPY INTO` → `dbt run` →
  reconciliation check. Its strongest use is **backfill**: dimension dumps are
  partitioned by `dump_date`, so the DAG's logical date `{{ ds }}` drives
  `replaceWhere dump_date = '{{ ds }}'`, and `airflow dags backfill` re-runs
  any past night idempotently. Replayability becomes a demonstration, not a
  claim.
- Rejected: **Databricks Workflows alone.** Honest note: Workflows *can* run a
  dbt task against Snowflake, so this is not "impossible otherwise". Rejected
  because it puts orchestration inside the one platform that is on a free tier
  whose inactive accounts may be deleted, and because an orchestrator that is
  not also one of the systems it coordinates is the more common production
  shape. Workflows still get used for the Databricks-internal jobs themselves.
- Rejected: **Snowflake Tasks as the orchestrator.** In-warehouse only; they
  are already planned for Streams & Tasks inside Gold and cannot drive
  Databricks.
- Placement: after Gold's first model exists (~Session 15+), so the DAG spans
  both platforms from its first run.

### 2. Windowed streaming, built where the two topics genuinely meet

- Chosen, two pieces:
  - **Stream-stream join of `orders` with `inventory.cdc`, with watermarks**:
    "order lines with no matching stock decrement within 1 hour". The two
    topics are causally coupled by design, so this is the streaming form of the
    batch reconciliation already proven (112,650 units = 112,650).
  - **`dropDuplicatesWithinWatermark` vs `MERGE` for Silver order dedup, built
    both.** Expected result: the watermark version silently keeps a duplicate
    that arrives after the watermark (a replay days later), while `MERGE` on
    `event_id` catches it. A deliberate-failure comparison with a recorded
    winner.
- Rejected: **keeping reconciliation batch-only (Gold).** Cheaper, but leaves
  watermarks, event time vs processing time, and state cleanup as theory —
  the in-scope Streaming items nothing else in the project builds.
- Constraint: Free Edition allows only `AvailableNow`/`Once` triggers, so
  "real time" here means state carried in the checkpoint between runs, not
  seconds of latency. Stated up front so nobody claims otherwise.
- Placement: after Silver inventory exists (~Session 11).

### 3. GDPR / PII: erasure through layers built to never forget

- Chosen: the customer generator adds **deterministic synthetic PII** (name,
  email, phone), seeded by `customer_unique_id` so every run produces the same
  values. Then: pseudonymise in Silver (salted hash), keep raw PII in one
  restricted table, and build **right to erasure** for one customer across
  Bronze, Silver, Gold and Snowflake.
- Why it is hard, which is why it is worth building: **Kafka topics are
  immutable** and **Delta time travel keeps deleted rows in old files until
  `VACUUM`**; Snowflake adds Time Travel and Fail-safe on top. The candidate
  answer is **crypto-shredding** (encrypt PII with a per-customer key, delete
  the key), to be weighed against physical deletes when that session opens.
- Masking: Snowflake masking policies (trial accounts are Enterprise edition);
  Unity Catalog column masks **only if Free Edition supports them — to be
  verified, not assumed**, given this session's experience with undocumented
  Free Edition behaviour.
- Rejected: **leaving the data PII-free.** Olist was anonymised before
  publication, so there is nothing to protect — which would make every GDPR
  claim theoretical. Rejected also: **importing real-looking PII from another
  dataset**; synthetic, deterministic, and labelled as such is the honest
  version.
- Placement: lakehouse half after Silver (~Session 12); Snowflake half inside
  the Gold window.

### Cost to the calendar

Databricks now has no clock, so items 2 and 3's lakehouse half cost time only.
The Snowflake trial's 30 days are the one binding clock: GDPR masking/erasure
in Snowflake and the Airflow DAG both add work inside that window, and should
be planned before the trial is started, not discovered during it.

## Four smaller additions folded into existing sessions: quarantine, schema contract, CDF, stream monitoring (Session 6)

**In plain words:** four more skills are added, none big enough for its own
session — each rides inside work already planned. Bad rows get a table of
their own instead of vanishing; a breaking schema change gets refused on
purpose; Silver's export reads only what changed; and every stream reports
whether it is falling behind.

- **Quarantine / dead-letter table — Bronze (S7), with DLT expectations (S11).**
  A Kafka message that will not decode, or lacks its key, lands in a
  `*_quarantine` table with a `reason` column and its Kafka coordinates
  (partition, offset) — never silently dropped. Rejected: **failing the whole
  batch** on one bad record, which turns one corrupt message into a stopped
  pipeline; and **dropping bad rows**, which makes the loss invisible.
- **Schema evolution as an enforced contract — Kafka side (S7).**
  Set an explicit Schema Registry compatibility mode on both subjects (the
  current mode has never been checked, only defaulted), then run a
  deliberate experiment: register a breaking change and assert it is refused.
  Optionally a CI step that checks a schema against the registry before merge.
  Rejected: **relying on the default** — a contract nobody has seen refuse
  anything is an assumption, not a contract.
- **Change Data Feed — Silver → bridge export (Gold window).**
  Silver tables are MERGE-written, so a plain stream over them fails or
  re-reads rewritten files. Enable CDF and export only
  `insert`/`update_postimage` rows since the last exported version.
  Rejected: **re-exporting whole Silver tables** each run — simpler, and
  correct, but it copies unchanged rows every time and hides the
  "what changed since version N" skill the pattern exists to teach.
- **Streaming monitoring — every Bronze/Silver stream (S7 onward).**
  Record each batch's `StreamingQueryProgress` (input rows, batch duration,
  offsets behind latest) to a small Delta table, and add one check that fails
  when the backlog grows run over run. Rejected: **Kafka consumer-group lag
  tools**, which read offsets committed to Kafka — Spark keeps its offsets in
  the checkpoint, so those tools report stale or no lag for Spark readers.
- Considered and **left out on purpose**, recorded so the omission reads as a
  judgment rather than a gap: per-key stateful processing
  (`applyInPandasWithState`) — this data has no cumulative counters that need
  state across batches; and event-clock repair — timestamps come from the
  dataset, not from faulty devices. Building either would be coverage for its
  own sake, against the project thesis.

## Bronze dimension tables: unpartitioned, `replaceWhere` on a column, every value a string (Session 6)

**In plain words:** each nightly dump is loaded into a Bronze table in a way
that makes loading the same night twice harmless — the second load replaces
that night's rows instead of adding a copy. The tables are deliberately not
partitioned, and every column stays text exactly as the file had it.

- Chosen: Auto Loader (`AvailableNow`) → `foreachBatch` → overwrite with
  `replaceWhere dump_date IN (<dates in this batch>)` into
  `workspace.bronze.{customer,product,seller}`. Two independent guarantees:
  the **checkpoint** stops a normal re-run from re-reading a file, and
  **`replaceWhere`** makes a re-read harmless if the checkpoint is ever lost
  or a night is backfilled.
- Chosen: **no `PARTITIONED BY`.** ~117K rows across 9 dumps; partitioning by
  date would produce many tiny files for no pruning benefit. `replaceWhere`
  is a predicate Delta evaluates, not a partition operation, so it works
  without partitions — and Delta checks every written row matches it, so a
  row with a stray date fails the write instead of landing silently.
- Rejected: **partition by `dump_date` + dynamic partition overwrite.** The
  textbook shape, and correct, but at this size it is a small-file problem
  created on purpose. Revisit only if a dimension grows by orders of magnitude.
- Rejected: **plain append.** Simplest, and exactly-once as long as the
  checkpoint survives. It does not survive a checkpoint reset — every night
  would double — and Airflow backfill (planned) re-processes nights by design.
- Chosen: **strings only** (`cloudFiles.inferColumnTypes=false`). Bronze is the
  faithful record; typing belongs in Silver, where a bad value can be flagged
  rather than silently nulled by an inference guess at ingest.
- Chosen: **the file's own `dump_date` column is the key, and must equal the
  folder name** (`dump_date=YYYY-MM-DD`). The generator writes both. Auto
  Loader's automatic folder-to-column inference is switched off
  (`cloudFiles.partitionColumns=""`) so the two cannot collide, and every batch
  asserts they agree — a disagreement would make `replaceWhere` replace the
  wrong night.
- Named constraint, so it is not rediscovered as an incident: this is safe
  only because **one night = one file = one batch**. If a night ever arrived as
  several files split across batches, the later batch would replace the
  earlier one's rows. The replace unit must equal the arrival unit.

### Correction (2026-09-28, Drill 1) — a backfilled night was never re-read, so `replaceWhere` never got the chance to make it harmless

The entry above says `replaceWhere` makes a re-read harmless "if the
checkpoint is ever lost **or a night is backfilled**". The checkpoint half is
true, proven twice (Session 6 and Drill 1: every file re-read, no count
moved). The backfill half was never true with these settings: Auto Loader
tracks files **by path**, and with its default (`cloudFiles.allowOverwrites =
false`) a corrected night re-delivered at the same path is skipped without a
word. Drill 1 showed it on scratch — five corrected seller cities, 0 landed,
no write, no error — and production had the same setting. Fixed by turning
`allowOverwrites` on (decision entry, Drill 1). Also measured: the "no
partitions" choice means a one-night replace **rewrites every file shared with
other nights**, copying their rows (a 3,095-row night replaced as a 6,190-row
write, values unchanged). Negligible at this size; it is the cost that choice
named.

## Databricks runs notebooks from a Git folder, not from hand-imported copies (Session 6)

**In plain words:** the Databricks workspace now holds a clone of the GitHub
repo, and notebooks run from there. A fix goes laptop → `git push` → **Pull**
in Databricks, so the code that runs is always a commit in git — not a file
someone imported by hand and then forgot to re-import.

- Chosen: **Databricks Git folder** on `ledgerline`, branch `dev` (the repo's
  default), full checkout. Authenticated with a GitHub fine-grained token
  scoped to **this one repo, Contents: Read-only**, stored in Databricks
  (Linked accounts). Expires 2026-12-25; Pull fails after that until renewed.
- Why now: the first Bronze fix had to be re-imported by hand to reach the
  workspace, which is exactly how the running copy drifts from the reviewed
  one. It had already happened once within an hour.
- Rejected: **manual import per change.** Zero setup, and a guaranteed drift.
- Deferred, not rejected: **Asset Bundles deployed from GitHub Actions on
  push** — fully automatic, and it also defines scheduled jobs. Already in the
  plan; needs a workspace token in GitHub secrets, and whether Free Edition
  permits that is unverified.
- Rejected: **sparse checkout** (only `databricks/`). Built for large repos;
  this one is ~55 files because raw data is gitignored, and later notebooks
  may import shared code from outside `databricks/`.
- Rule that goes with it: **code changes are never committed from inside
  Databricks.** The token is read-only, so it cannot push anyway — the
  workspace is a consumer of git, not a second place to author code.

## Live data is treated as production: the contaminated topic is repaired downstream, not deleted (Session 6)

**In plain words:** 159 wrong events landed on `inventory.cdc` from the test
suite. The quick fix was to delete the topic and regenerate it. It was
rejected: the project now treats its live topics the way a company treats
production — where deleting a shared topic is almost never allowed — so the
topic stays as it is, and every downstream layer excludes the bad events
using a published denylist. Mistakes like this one are treated as material,
not embarrassments.

- Standing principle, at the human's instruction: **aim for what a real
  production team would do.** Experiments and the mistakes they cause are the
  point of the project; the response to a mistake is the production response,
  even when a sandbox shortcut exists.
- What was measured first (step 2 of any incident response — identify before
  fixing): every live record was compared with a clean local regeneration.
  279 extra records = **120 exact duplicates** of real events (harmless — Silver
  dedups on `event_id`) + **159 contaminated records** (53 distinct
  `event_id`s × 3 runs). They touch **23 SKUs**, and **0 SKUs end with the
  wrong final stock**: the bad events carry early `seq` values that later,
  correct events supersede. What *is* wrong is anything computed from the raw
  log: units sold reads **112,806** against a true **112,650**; excluding the
  denylist and deduplicating on `event_id` gives exactly **112,650**.
- Chosen: **denylist + downstream filtering.** The 53 event IDs, with how they
  were identified, are committed at
  `ops/incidents/2026-09-26_inventory_cdc_denylist.json`. Bronze (S7) ingests
  the topic **unfiltered** — it is the faithful record, including the
  mistake. Silver (S9) excludes the denylist before its MERGE. The reconciliation
  must read 112,650 from the filtered data.
- Rejected: **delete, recreate, re-produce the topic.** Clean and fast here,
  because the generator can rebuild every event. In production the topic is
  often the only copy of history and other teams read it; this would also
  erase the evidence the incident entry is about.
- Rejected: **compensating events** (publish corrections). The right tool
  when current state is wrong — here current state is already right for all
  23 SKUs. What is wrong is history and totals, and adding more events cannot
  retract ones already counted; it would make the log's arithmetic worse.
- Rejected: **a repair topic** (`inventory.cdc.v2` minus the bad events, then
  move consumers). Real, and the answer when many consumers can't each add a
  filter. Here there is one consumer (Bronze), so a denylist is the smaller,
  reversible change.
- Named gaps this exposed, each a candidate for its session:
  - **No provenance on events.** The bad runs could only be found by
    re-generating the truth and diffing. A per-run ID in a message header
    (candidate for S7) would make it one query.
  - **The verifier overstates ties.** Its "43 keys with a tied seq" counts
    identical duplicates as conflicts; only 23 keys have genuinely different
    events at the same `seq`. It should report the two separately.
  - **Test and live credentials were the same.** The laptop `.env` holds the
    one writer key. Production would give dev/test a credential with no write
    permission on live topics. Candidate: a dev-only service account whose
    ACLs cover a `dev.` topic prefix only.
- Consequence for Session 10: the CDC-correctness experiment (`exp_04`) now
  has a real contamination to work on — assert the raw totals are wrong
  (112,806), apply the denylist, assert they are right (112,650).


## Drill sessions between build phases: break the guarantees on purpose, and re-break every past incident (Session 6)

**In plain words:** after each major layer is finished, one session is spent
not building anything but *attacking* what was built — replaying data,
crashing jobs mid-run, deleting checkpoints — to prove the idempotency,
replay and dedup guarantees actually hold. The same session re-triggers every
past incident on purpose to confirm its guard still catches it. Production
teams call these **game days** and **incident regression**.

- Chosen: three drills, named rather than numbered so existing references to
  S9, S10, S13 and S17 stay correct:
  - **Drill 1 — after Session 7** (Bronze complete): replay, idempotency,
    exactly-once into Bronze, and incident regression.
  - **Drill 2 — after Session 10** (Silver complete): dedup, MERGE correctness,
    out-of-order events, end-to-end reconciliation (including the S6
    denylist: 112,806 → 112,650).
  - **Drill 3 — after Gold, inside the Snowflake window**: late data, SCD2
    point-in-time, the whole pipeline replayed from empty.
- Each drill has three parts: (1) break each guarantee on purpose and assert
  it holds — or assert the bug first, then fix; (2) re-trigger every past
  incident on the data path and confirm its guard fires — **an incident with
  no automated guard is itself a finding**, fixed in the drill; (3) a
  discussion-style revision of the question-bank items the drill touched.
- Why between phases, not at the end: a guarantee can only be attacked once
  the layer providing it exists, and a weakness found right after a layer is
  cheaper than one found three layers later. Why not only inside build
  sessions: build sessions test what they build; nothing re-checks earlier
  guards against later changes — which is how the Session 5 guard's own test
  became the Session 6 contamination.
- Rejected: **relying on the six deliberate-failure experiments alone.** They
  each prove one pattern once, in the session that builds it. They do not
  re-run past incidents, and nothing re-runs them after later changes.
- Rejected: **a single drill at the end of the project.** Too late to be
  cheap, and it would collide with the Snowflake trial's closing days.
- Rejected: **numbering drills as sessions** (S8 = drill). Renumbering would
  silently break every existing "Session 9 / 10 / 13 / 17" reference in the
  docs and the deferred question bank.
- Known gap going in: the S5 incident "CDC event count 50 too high" has **no
  automated guard** — no test asserts the total of 158,346. Drill 1 builds it.
- Consistent with the Session 6 principle "treat live systems as production":
  drills run against the live topics and tables, with before/after counts,
  not against copies.

## Kafka Bronze: decoded record + raw bytes + Kafka coordinates, one shared writer, 50,000-offset batches (Session 7)

**In plain words:** each Kafka message becomes one Bronze row holding three
things — the decoded fields, the original bytes exactly as they arrived, and
the message's address on the topic (partition + offset). Messages that will
not decode go to a separate quarantine table instead of stopping the load. Both
topics use the same code; only the names and expected counts differ.

- Chosen: `databricks/bronze/_kafka_bronze.py` (loaded with `%run`) holds read →
  decode → write → monitor → verify; `stream_orders.py` and `stream_cdc.py` are
  thin notebooks that call it. Writes are Delta **appends** with
  `txnAppId` + `txnVersion = batch_id`, `Trigger.AvailableNow`.
- Chosen: **keep the raw Avro bytes (`_raw_value`) next to the decoded columns.**
  Decoding is an interpretation that can be wrong — for example a reader schema
  that silently drops a field a newer producer added — and Kafka does not keep
  the original forever. With the bytes in Bronze, a wrong decode is redone from
  Bronze, not from a topic that may have expired. Cost: Bronze roughly doubles
  in size, ~0.1 GB → ~0.2 GB, on storage that costs nothing here.
- Rejected: **decoded columns only.** Smaller, and the common tutorial shape.
  Once the topic's retention passes, a decoding mistake becomes permanent.
- Rejected: **raw bytes only, decode in Silver.** The purist "Bronze = exactly
  what arrived" answer. Rejected because the Session 6 decision puts quarantine
  at Bronze, and quarantine needs a decode attempt to know what failed.
- Chosen: **quarantine is a separate table**, not a flag column on the Bronze
  table. With a flag, every downstream reader must remember
  `WHERE _quarantine_reason IS NULL`, and one that forgets feeds null keys into
  Silver's MERGE. A separate table cannot leak by omission. Trade-off: two
  writes per micro-batch, so each batch is read from Kafka twice (caching on
  serverless is not relied on). Measured on the first run, not assumed.
- Chosen: **`maxOffsetsPerTrigger = 50,000`**, so one `AvailableNow` run is
  ~8 batches for `orders` and ~4 for `inventory.cdc` instead of one. Several
  batches give `txnVersion` something to count and give the crash experiment a
  batch to kill in the middle of a run.
- Chosen: **the checkpoint path and the app id carry one shared generation
  (`v1`), and a checkpoint is never deleted in place.** Delete the checkpoint
  but keep the app id, and the new stream numbers batches from 0 while Delta
  remembers the app id at ~7 — it silently skips batches 0–7 as "already
  written". Bumping the generation starts a new stream from `earliest`, which
  re-reads the whole topic: a rebuild, never a quiet restart. **No guard is
  built for this yet, on purpose:** Drill 1 must first show the silent skip
  happening (assert the bug present), then add the guard.
- Chosen: **the exactly-once proof is a crash between the Delta commit and the
  checkpoint commit** (`exp_01`), not a checkpoint deletion. Deleting the
  checkpoint with the same app id would *also* show zero duplicates — because
  Delta skips everything, which is the trap above, not the guarantee. The crash
  is the one situation `txnVersion` exists for: Spark re-runs batch N with
  identical offsets, and Delta must skip it. `exp_01` runs the crash twice on
  scratch tables — plain append (asserts batch N doubled), then with the txn
  options (asserts zero duplicates) — so the bug is shown present before the fix.
- Chosen: **no deduplication in Bronze**, for either topic. Duplicate *events*
  (same `event_id`) are real history — the producer sent them twice — and stay.
  Bronze guarantees each *message* (partition, offset) exactly once; Silver
  guarantees each *event* exactly once.

### Correction (2026-09-27, same session) — the crash is now recreated from the checkpoint, not staged

The entry above says `exp_01` proves exactly-once with "a crash between the
Delta commit and the checkpoint commit", raised inside the batch. That was
built and run: the bug half worked (49,999 duplicates, job reported success),
but Databricks fails any notebook command in which a stream died, even when
the code caught it, so "Run all" never reached the fix (incidents.md,
2026-09-27). `exp_01` now **loads cleanly, deletes the last `commits/N` file,
and restarts** — the exact on-disk state such a crash leaves. The reasoning
in the entry stands: this is still the one situation `txnVersion` exists
for, and still the reason a checkpoint deletion would be the wrong proof. The
replayed batch is now the last one (7) instead of batch 2.

## Topic retention raised from 1 week to 3 months, on both topics (Session 7)

**In plain words:** Kafka deletes messages after a set time — its *retention*.
Both topics were on Confluent's default of **1 week**, which nobody had
checked in six sessions. Every message was written on 2026-09-26, so both
topics would have started emptying around **2026-10-03** — before Drill 1,
which replays from them. Retention is now **3 months**, which keeps the data
until about **2026-12-25**.

- Found by the Session 7 start-of-session re-check, not by a failure. The
  project's own key **cannot read topic settings**:
  `describe_configs` → `TOPIC_AUTHORIZATION_FAILED` (it holds READ/WRITE, not
  DESCRIBE_CONFIGS). The value was read in the console: `retention.ms = 1 week`,
  `retention.bytes = Infinite`, `cleanup.policy = delete`, on both topics.
- Why it was invisible: every check so far counted what is *on* the topic, and
  every count was right. Nothing asked how long it would stay. The Session 5
  cost note ("~$0.01/month at rest") quietly assumed the data stays forever.
- Chosen (by the human, in the console): **`retention.ms` = 3 months** on
  `orders` and `inventory.cdc`. Verified afterwards with the read-only
  watermark check: 394,090 and 158,625 messages, lowest offset still 0,
  oldest message still 2026-09-26 — the change touched no data.
- Rejected: **keep 1 week and treat Bronze as the only long-term copy.** The
  usual production arrangement — Kafka is a buffer, the lake is the history —
  and the reason Bronze keeps raw bytes. Rejected *for now* because the rule
  production actually sizes retention by is "longer than the longest time a
  reader can be down, plus any planned replay". Here the reader is a job run
  by hand in sessions days apart, and Drill 1, `exp_01` and Session 10 all
  replay from the topic.
- Rejected: **infinite retention** — Claude's recommendation, at ~$0.01/month.
  The human chose 3 months instead; the reason is recorded below once given,
  not guessed here. What 3 months does cover: Drill 1, Silver (S8–S10) and the
  Session 10 experiments, with weeks to spare.
- Consequence to carry: **after ~2026-12-25 the topics empty themselves**, and
  Bronze (raw bytes kept) becomes the only copy of the 159 contaminated
  records. The clean events stay reproducible from the generators; the
  contamination is reproducible too (`inventory_cdc.py --limit 50
  --allow-partial`, three runs), only with different `produced_at` values.
- Guard already in place for the day it matters: Bronze reads with
  `failOnDataLoss = true`, so if retention ever deletes messages Bronze had
  not read yet, the stream stops with an error instead of skipping them.

## Bronze gets no consumer-group ACL — tested by being allowed, not assumed (Session 7)

**In plain words:** the plan was to give Bronze's Kafka reader a new
permission on its consumer-group name before its first run. A probe read the
`orders` topic under that name **without** the permission, and was not
refused. So the permission is not granted. The laptop verifier needed one in
Session 5 for what looks like the same job; the difference is the client
library, not the job.

- Evidence: a Free Edition serverless batch read of 15 records from `orders`
  with `groupIdPrefix = "ledgerline-bronze-"`. The service account's only
  consumer-group ACL is READ on prefix `ledgerline-verifier-`, which does not
  match. 15 rows came back, decoded (next entry).
- Why the two readers differ: the Session 5 verifier uses **librdkafka**
  (`confluent_kafka`), which looks up a group coordinator eagerly for any
  configured `group.id` — and that lookup is authorized against the group.
  Spark uses the **Java** client: offsets are fetched on the driver with an
  AdminClient, and executors `assign()` partitions and never commit. Spark's
  own docs say so (3.1+: "executors never done group based authorization");
  the probe is what made it a fact rather than a quotation.
- Rejected: **grant the READ ACL on `ledgerline-bronze-` anyway**, as planned.
  It costs one console click and would make the first run certain. Rejected
  because a grant shown to be unnecessary is exactly what least privilege
  removes — and granting it would have hidden this finding.
- Not yet proven: the probe was a batch read (`spark.read`); Bronze is a
  stream (`readStream`). Both use the same offset reader, so the result is
  expected to carry over. **Confirmed or refuted by the first `stream_orders`
  run** — recorded in `progress.md` either way.
- Consequence: Session 5's prevention rule ("a permission set derived by
  reading code is a hypothesis until a credential refuses something") applies
  in both directions — here the test showed a grant was *not* needed.

## Avro is decoded with `from_avro` + Schema Registry on Free Edition (Session 7)

**In plain words:** each Kafka message starts with the ID of the schema it was
written with. Spark's `from_avro`, given the registry address and a read-only
use of the registry key, looks that schema up and decodes the message. This
was listed as unverified on Free Edition; the probe decoded 15 `orders`
messages correctly (`schema_id` 100001, `event_ts` as a timestamp, `items` as
an array of records).

- Chosen: `from_avro(data, subject=..., schemaRegistryAddress=..., options)`
  with `mode = PERMISSIVE`, so an undecodable payload becomes NULL (then
  quarantined) instead of failing the batch.
- Rejected: **strip the 5-byte header and decode against a schema pinned in
  the notebook** — the open-source workaround, and the fallback had the
  registry been unreachable. It decodes every message with one fixed schema,
  so a producer's new schema version would be read wrongly without an error.
- Rejected: **decode on the laptop with `confluent_kafka`** before landing —
  it moves ingestion out of Databricks, the platform this layer exists to use.
- Observed and not yet explained: **1 min 25 s** for the 15-record probe
  (two Kafka reads plus one registry call). The time is overhead per query,
  not per record — consistent with Session 6's 3 min 43 s for the whole topic.

## Predictive Optimization stays on; tables set their own time-travel window (Session 7)

**In plain words:** Databricks turned out to be maintaining our tables on its
own — merging small files, and (on a timer) permanently deleting data files
the table no longer uses. It stays on. But deleting old files is what ends
*time travel* — reading a table as it was at an earlier version — so every
table we might need to look back into now says how long its old files must
be kept, instead of silently inheriting 7 days.

- Found: `bronze.orders` version 9 was an `OPTIMIZE` nobody ran — user
  `e58e5c14-…` (a service principal, not a person), 80 minutes after the load,
  24 files (~42 MB, 8 batches × 3 partitions) rewritten as ~1. `DESCRIBE TABLE
  EXTENDED` → `Predictive Optimization: ENABLE (inherited from METASTORE
  metastore_aws_us_east_2)` — so every managed table here has it, including
  every future Silver table.
- Why it matters: Predictive Optimization also runs `VACUUM`, which deletes
  unreferenced data files older than `delta.deletedFileRetentionDuration`
  (default **7 days**). The Delta log keeps entries for
  `delta.logRetentionDuration` (default 30 days), but time travel needs the
  data files too, so the effective window was 7 days — never chosen, never
  written down. Session 10's repair-by-time-travel and the GDPR work both rely
  on it; `CLAUDE.md` names "VACUUM breaking time travel" as a danger zone.
- Chosen (by the human): **keep Predictive Optimization on, and set
  `delta.deletedFileRetentionDuration = 'interval 30 days'` on every Bronze
  table**, in code, from the notebook that creates it — so a rebuilt table
  gets it too. 30 days matches the default log retention, so data files and
  log entries now run out together; a longer file window would buy nothing
  while the log still expires at 30. Silver tables choose their own window
  when Session 8 creates them.
- Rejected: **turn it off for our schemas and run OPTIMIZE / VACUUM by hand.**
  The most hands-on, and it would let us trigger the VACUUM danger ourselves.
  Rejected because it is not how a team on Unity Catalog runs — Databricks
  recommends it on — and small files pile up the first time someone forgets.
  Manual `OPTIMIZE` / `VACUUM` / `RESTORE` stay available for experiments.
- Rejected: **decide in Session 8.** Nothing reads Bronze history yet — but the
  7-day clock on `bronze.orders`' replaced files started at 07:27 today, so
  waiting *is* choosing 7 days, by default.
- Trade-off: files replaced by OPTIMIZE or overwritten by `replaceWhere` are
  kept 30 days instead of 7 — a few tens of MB here, on storage Free Edition
  does not bill.

## Schema Registry contract: `BACKWARD_TRANSITIVE` on both subjects, chosen by asking who updates first (Session 7)

**In plain words:** a compatibility mode is the rule for which schema changes
the registry accepts. Until today both topics silently used Confluent's
default, `BACKWARD`, checked against the latest version only. We now set
**`BACKWARD_TRANSITIVE`** on purpose: a new schema must be readable by a
reader on it against **every** old version, because our reader always has the
newest schema and sometimes re-reads the whole topic.

- The question that decides a mode: **when the schema changes, who has the
  newer version, the writer or the reader?** BACKWARD = the reader updates
  first (new reader, old messages). FORWARD = the writer updates first (old
  reader, new messages). FULL = no order promised, both must hold.
- Our answer, from how the code works: Bronze's `from_avro` fetches the
  subject's newest schema at the start of every run, so **our reader is never
  older than the writer** — the BACKWARD case, every time. And a Bronze
  rebuild re-reads the topic from `earliest`, meeting messages of every past
  version — so the check must be **TRANSITIVE**, not only against the latest.
- Measured first, registering nothing (`scripts/check_schema_compat.py`, the
  registry's test endpoint), against both subjects under today's `BACKWARD`:
  add a field **with** a default → accepted; add a field **without** one →
  refused (`READER_FIELD_MISSING_DEFAULT_VALUE`); **remove a required field**
  (`order_status`, `stock_qty`) → **accepted**. That last one is the gap
  BACKWARD knowingly leaves.
- Verified on a real write: registering the "no default" version of
  `orders-value` returned **409** (`40901 … incompatible with an earlier
  schema`), versions `[1]` before and after. The contract refuses writes, not
  just tests.
- Rejected: **`FULL_TRANSITIVE`** — Claude's first recommendation, withdrawn
  the same session when the human asked why. It protects against an *older*
  reader meeting newer messages, which this setup does not have, and it costs
  real flexibility: FULL refuses even `int → long`, the widening that BACKWARD
  allows. Revisit if a reader ever pins an old schema (another team, a
  long-running stream that does not restart).
- Rejected: **plain `BACKWARD`** (the implicit default) — identical today with
  one version per subject, but after three versions it would check only
  v3-against-v2 while a rebuild reads v1 messages too.
- Rejected: **`FORWARD`** — the writer-first model; it would let a producer add
  a required field that a rebuild cannot fill in for old messages.
  **`NONE`** — no contract at all.
- The gap, and where it is guarded: under BACKWARD a producer may *remove* a
  field; Bronze would then land blanks for it. The field's meaning belongs to
  Silver, so the guard lives there — `order_status` must never be blank — as a
  DLT expectation in Session 11. A field removal is otherwise done in two
  steps (give it a default, move readers off, then remove).
- Found on the way: our registry key holds `DeveloperWrite` — it may register
  schemas — but setting the mode returned **403, `denied operation
  WriteCompatibility`**. A key that publishes schemas cannot loosen the rules
  it is checked against; that separation is correct and stays. The mode is set
  by the human in the Confluent console.
- **Verified set** (same session), with the read-only probe after the human
  changed it in the console: `orders-value` and `inventory.cdc-value` both
  report subject mode `BACKWARD_TRANSITIVE` (global stays `BACKWARD`);
  verdicts unchanged — with one version per subject, BACKWARD and
  BACKWARD_TRANSITIVE check the same thing. They diverge from version 3.

## Every produced message carries a provenance header: run id, producer, scope (Drill 1)

**In plain words:** from now on, every message a generator puts on Kafka
carries a small label saying *which run* sent it (a random id, new every
time the generator starts), *which program* sent it, and whether the run was
*full or partial*. The label rides in the message's headers, not in its data.
In Session 6, finding the test suite's 159 bad messages took a full
regeneration of the topic and a diff. With this label, a bad run is found
with one query: `GROUP BY run_id`.

- Chosen: three Kafka headers, set by `KafkaAvroSink` on every `produce()`:
  - `ledgerline.run_id`: a `uuid4`, one per sink (one per generator run),
    printed in the run summary so it can be written down in `progress.md`.
  - `ledgerline.producer`: the client id, e.g. `ledgerline-order_events`.
  - `ledgerline.scope`: `full`, or `limit=N` for a partial run.
- Why now: Drill 1 puts deliberate duplicates on `orders` (two 10-order
  partial runs). Without a label they would be indistinguishable from each
  other and from the originals except by `produced_at`, and that is exactly
  the situation Session 6 had to reconstruct by hand.
- Bronze needs no change: it has read headers into `_kafka_headers` since
  Session 7 (`includeHeaders = true`). The 394,090 + 158,625 messages already
  on the topics have no headers, and that stays true of them.
- Rejected: **provenance as Avro fields** (a `run_id` field in the envelope,
  with a default). Allowed under `BACKWARD_TRANSITIVE`, but it is a schema
  version on both subjects for data that describes the *delivery*, not the
  *event*. The same order event sent twice is still one business fact. Every
  decoded record, Silver's included, would carry transport metadata.
- Rejected: **a deterministic run id** (a hash of the arguments). Session 6's
  contamination was three runs with *identical* arguments; a hash would give
  all three the same id. The label has to tell two identical runs apart, so it
  is random.
- Rejected: **inferring runs from `produced_at`** (each run is a narrow time
  window). It is what Session 6 had, and it only works while runs are far
  apart in time. Two runs a minute apart would merge.
- Trade-off, recorded: headers are **outside the schema contract**. The
  registry checks nothing about them, and a producer that forgets them breaks
  no compatibility rule. The guard is in code instead: the sink sets them
  itself, and a test asserts that every `produce()` passes them.

## A stream with no history may only write into empty tables — the checkpoint-reset guard (Drill 1)

**In plain words:** a Kafka Bronze stream remembers its position in two places —
Spark's checkpoint folder, and Delta's note inside the table ("app X has
written up to batch N"). Reset one without the other and data is silently lost
or silently doubled. The guard is one rule, checked before any stream starts:
**if the checkpoint has never planned a batch, every table the stream writes to
must be empty.** Otherwise the run is refused with an error that names both
mistakes and the one safe fix.

- Chosen: `refuse_unsafe_reset()` in `_kafka_bronze.py`, called by
  `run_stream()` after the tables are created and before `.start()`. "No
  history" means no `offsets/N` file — not "no `commits/N`", because a crash
  inside the very first batch leaves `offsets/0` without `commits/0`, and that
  restart is safe (txnVersion covers it).
- What it refuses, both silent without it:
  - **checkpoint deleted, same app id** → batch ids restart at 0 while Delta
    remembers the app at ~7, so batches 0–7 are skipped as "already written",
    including any new messages they now contain (Drill 1 part 2 shows it);
  - **new generation, old table** → nothing is skipped and the whole topic is
    appended again.
- What it lets through: a first-ever run (new checkpoint, table just created
  empty), every normal restart, and `exp_01`'s crash replay (checkpoint has
  history). The only safe reset is a rebuild: new `GENERATION` (checkpoint and
  app id together) into an empty table.
- `guard_reset=False` exists only so the drill can show the trap first, the
  same shape as `idempotent=False` for `exp_01`. Bronze never passes it.
- Rejected: **use the streaming query id as `txnAppId`** — Databricks' own
  suggestion. The query id lives in the checkpoint's `metadata` file, so
  deleting the checkpoint changes the app id automatically. That turns silent
  *loss* into silent *duplication* (the second case above); the empty-table
  rule is still needed. It is also circular on a first run — the id exists only
  once the query has started, after the writer function was built.
- Rejected: **read the app's last `txnVersion` from the Delta log and compare
  it with the checkpoint.** The precise check, but `txn` actions are not in
  `DESCRIBE HISTORY`, and the log of a Unity Catalog *managed* table is not
  readable by path from a notebook. The empty-table rule needs neither, and it
  catches the new-app-id case too, which a txn comparison would not.
- Rejected: **no guard, rely on the rule "never delete a checkpoint in place"**
  (the Session 7 position). A rule in a docstring is prose; Session 3 already
  showed prose does not execute. Session 7 deliberately left the guard out so
  the drill could show the bug present first.
- Cost: one `dbutils.fs.ls` and, only on a fresh checkpoint, one
  `LIMIT 1` count per table — before a run that takes minutes.

## Bronze checks compare against the broker's end offsets, not numbers written into the notebook (Drill 1)

**In plain words:** Session 7's Bronze notebooks asserted "the table has
394,090 rows" — a number counted on the laptop that day and typed in. That is
right only while the topic never changes. Drill 1 adds 74 legitimate messages,
and every one of those checks (plus `exp_01`'s) would have failed on correct
data. The checks now ask the broker where each partition ends in this run and
require every offset from 0 to that end exactly once.

- Chosen: `topic_end(progress_rows)` — the `latestOffset` Spark fetched from
  Kafka when the run started, per partition. `check_coordinates` is unchanged:
  rows = distinct offsets = span = that end. Offsets start at 0 here, so the
  end is also the count.
- Facts about the **dataset** stay pinned, because the dataset is closed:
  394,090 distinct `event_id`s, 99,441 `created`, 112,650 units (counted one row
  per `event_id`). A new rule ties the two: **rows − distinct events = rows
  carrying a provenance header** — every duplicate is labelled, every labelled
  row is a duplicate.
- Rejected: **update the frozen numbers after each produce.** Cheap, and it
  makes every legitimate change look like a failure until someone edits a
  notebook. That trains people to edit the expectation instead of reading the
  failure.
- Trade-off: the expected count now comes through Spark rather than an
  independent engine. The broker is still the source (Spark only relays its
  offsets), and the laptop verifier stays the independent cross-check, run by
  hand in drills.

## Notebooks pin the serverless environment version in source, and are committed exactly as Databricks saves them (Drill 1)

**In plain words:** Databricks quietly rewrites a notebook whenever it is
opened or run: it adds a four-line header naming the **serverless
environment version** (the bundle of Python, PySpark and library versions the
notebook runs on — here version 6), and it drops the file's final newline. Our
git copies had neither, so every run left the Git folder "modified", and the
first Pull that touched the same lines stopped on a merge conflict. Every
notebook now carries that header in git and ends the way Databricks ends it,
so running a notebook changes nothing and a Pull cannot conflict on it.

- Chosen (by the human): the header
  `# /// script` / `# [tool.databricks.environment]` /
  `# environment_version = "6"` / `# ///` directly after
  `# Databricks notebook source`, in all 11 notebooks; no final newline;
  ruff's W292 ("no newline at end of file") ignored for notebook paths only.
  `tests/test_notebooks.py` fails if any notebook — including every future
  Silver one — lacks the header, pins a different version, or ends with a
  newline.
- The more important half: **the platform version is now part of the code.**
  Session 7 lost time to PySpark on the platform (4.3.0.dev0) behaving
  differently from the version read on the laptop. Until now nothing recorded
  which environment the code expects; had Databricks moved the workspace
  default, every notebook would have silently changed runtime. Moving to 7 is
  now a reviewed commit, with the tests saying where.
- Rejected: **discard local changes before every Pull** (what Drill 1 did to
  get unstuck). Free, but it is a manual step before every deploy, and a merge
  editor in the middle of a drill is exactly where the wrong side gets kept.
- Rejected: **pin the environment in a job definition or an Asset Bundle
  instead.** The production answer once jobs exist, but there are no jobs yet,
  and Bundles deployed from CI are still unverified on Free Edition. Revisit
  when Airflow or Bundles run these notebooks — the pin then moves there and
  the tests follow it.
- Not verified yet: that Databricks leaves a notebook untouched when the
  header is already there. The first run after the next Pull shows it: the
  Git dialog must say "No changed files".

## Bronze dims re-read a night that is re-delivered at the same path (`allowOverwrites`) (Drill 1)

**In plain words:** when a nightly dump is sent again with corrections, under
the same file name, Bronze now picks it up. Before, Auto Loader remembered the
file name and skipped it, so Bronze kept the old values while the landing
zone held the new ones, and nothing said they disagreed. Re-reading is safe
because each night is written with `replaceWhere`: the corrected night
replaces the old one; it is never added beside it.

- Chosen (by the human): `ingest(..., allow_overwrites=True)` in production
  `autoload_dims`. Auto Loader then re-processes a file whose modification
  time changed.
- Evidence (`drills/drill1_dims_2`, scratch): default → corrected rows 0,
  WRITE commits 1 → 1; `allowOverwrites` → one write, `dump_date IN
  ('2017-05-02')` only, 5 corrected rows, counts unchanged, the other night's
  values unchanged; a plain re-run after it wrote nothing.
- The two settings only work as a pair: `allowOverwrites` on a plain append
  would *add* the corrected night beside the old one. That is why Databricks
  warns the option can duplicate data, and why it is safe here.
- Rejected: **keep the default and make backfill a procedure** (reset the dims
  checkpoint, which re-reads everything harmlessly). Correct, and proven today,
  but it depends on someone knowing a correction arrived — which is exactly the
  signal that is missing. Until then Bronze and the landing zone disagree
  silently.
- Rejected: **deliver corrections under a new file name** in the same night's
  folder. Auto Loader would read it, but the night would then be two files,
  possibly in two batches, and the second `replaceWhere` would wipe the first
  batch's rows — the "one night = one file = one batch" rule above.
- Cost, accepted: a byte-identical re-put (the generator's `--nights N` rewrites
  every earlier night, as it did today) now triggers a harmless re-write of
  those nights. It could be avoided by making landing write-once; not worth a
  generator change while a re-read costs a few seconds.

## Dims landing is write-once, and deletions start on a named night (`--delete-from`) (Session 8)

**In plain words:** the dims generator used to rewrite every earlier night each
time it ran. With the same arguments that was harmless (identical bytes); with
different arguments it would quietly rewrite history, and Bronze — which now
re-reads a rewritten file — would follow. Now the generator works out every
night in memory first, leaves a night already in the landing zone alone when
its bytes are identical, and **writes nothing at all** if any landed night would
change, unless that night is named with `--redeliver <date>`. Deletions can be
started on a chosen night, so night 5 can drop rows without touching nights 1–4.

- Chosen: `--delete-from YYYY-MM-DD` — deletions begin on the first night on or
  after that date; earlier nights compute an empty deletion set, so they
  regenerate byte-identical. Night 5 (2017-12-28) is to be landed with
  `--nights 5 --delete-per-night 100 --delete-from 2017-12-28`: expected 100
  products, 100 sellers and 19 customers gone from the dump (of the 100
  customers sampled, 19 are on night 4; 25 would only have appeared on night 5
  and now never appear; the rest have not placed an order yet).
- Chosen: **plan, check, then write.** All nights are generated against an
  in-memory overlay of the sink; only if every already-landed night is
  byte-identical (or named in `--redeliver`) are the new nights written. A
  refused run leaves the landing zone exactly as it was.
- Rejected: **`--nights 5 --delete-per-night 100` as the generator stood.**
  Deletions are cumulative from night 2 (`deleted_keys` loops `1..night`), so
  that command computes different nights 2–4 (rows missing) and rewrites them;
  with `allowOverwrites` on, Bronze would replace its history to match, and no
  check would say so — the landing zone is Bronze's source of truth.
- Rejected: **skip existing nights silently, whatever their content.** Simpler,
  but it hides the exact mistake the rule exists for: arguments that disagree
  with what has landed. A refusal names the night that would change.
- Rejected: **a boolean `--force`.** A correction is for one night; a flag that
  unlocks every night is the foot-gun this replaces.
- Property kept: the whole landed history is reproducible from one command
  (recorded in `progress.md`), which is what "rebuildable from git + S3" needs.

## Silver dims apply one night at a time, oldest first, recorded in a merge log (Session 8)

**In plain words:** Silver holds each customer, product and seller as they are
now. It gets there by applying the nightly dumps in date order, one `MERGE` per
night, and writes one line per applied night to a small log table (night,
rows, inserted / updated / deleted, table version). A night older than the last
one applied is never applied — it would roll Silver back in time.

- Chosen: `workspace.silver.dims_merge_log`; pending nights = Bronze nights
  newer than the last logged night, plus the last logged night itself when
  Bronze holds a newer copy of it (a corrected re-delivery — Drill 1 made Bronze
  accept those, so Silver must too). A corrected *older* night is reported,
  not applied.
- Why the log need not be atomic with the MERGE: applying the same full
  snapshot twice converges to the same table (second pass: 0 / 0 / 0). A crash
  between the MERGE and the log line only means that night is applied again,
  harmlessly. Contrast Kafka Bronze, where an append is not idempotent and
  needed `txnAppId`.
- Rejected: **apply only the newest night.** The current state comes out
  right, but every step in between is squashed: a customer who moved twice
  between runs shows one move, and Silver's commit history — what Change Data
  Feed exports to Gold — loses the intermediate versions.
- Rejected: **stream from Bronze.** Bronze dims are written with
  `replaceWhere` overwrites, so a plain Delta stream over them fails on the
  first rewrite; a night is a batch, and an orchestrator's `{{ ds }}` (Airflow,
  later) maps onto "apply night D" directly.

## Silver dims `MERGE`: update only real changes, delete by absence, change feed on (Session 8)

**In plain words:** a row is updated only when one of its values actually
differs, a key missing from tonight's full dump is deleted
(`WHEN NOT MATCHED BY SOURCE DELETE`), and Delta's Change Data Feed records
exactly which rows each night changed.

- Chosen: `WHEN MATCHED AND NOT (t.a <=> s.a AND …) THEN UPDATE` over every
  business column plus `dim_updated_at` (`<=>` is equality that treats two
  NULLs as equal). Silver mirrors what the file says; it does not trust the
  source's timestamp to announce every change.
- Chosen: `delta.enableChangeDataFeed = true` **at creation** — the feed only
  records changes made after it is switched on, so enabling it later loses
  the first nights for good. Plus `delta.deletedFileRetentionDuration = 30 days`
  (Session 7's rule; change files are vacuumed on the same clock, so any CDF
  consumer must read within 30 days).
- Chosen: lineage columns only a MERGE can keep — `_first_seen_dump_date`
  (set on insert, never updated), `_last_changed_dump_date`, `_source_file`.
  This is the reason given in Session 0 for MERGE over overwrite.
- Rejected: **unconditional `WHEN MATCHED THEN UPDATE SET *`.** Correct final
  state, but every matched row is rewritten every night: products would report
  32,951 updates instead of 50, the change feed would carry 32,951 update
  pairs, and `_last_changed_dump_date` would mean nothing.
- Rejected: **compare `dim_updated_at` only** (like the CDC `seq` guard).
  Cheaper, but a value changed without a timestamp bump would be silently
  ignored — the source's contract would be trusted, not checked.
- Noted for the Gold window: CDF now emits `delete` rows. The Session 6 plan
  to export only `insert` / `update_postimage` would drop every deletion on
  the way to Snowflake.

## A bad value fails the whole night; a snapshot source is never filtered (Session 8)

**In plain words:** Silver types every column (text → number, timestamp). If
any value will not convert, the whole night is refused — the row is not set
aside. In this MERGE a row missing from tonight's file *means* "delete", so
setting one bad row aside would delete that customer from Silver.

- Chosen: before each MERGE, assert per night: 0 NULL keys, 0 duplicate keys,
  0 values that were present as text but failed `try_cast`. Any failure stops
  that dimension before its MERGE; Silver keeps the previous night.
- Rejected: **a quarantine table, as Kafka Bronze has (Session 7).** Right
  there, because a missing Kafka row means nothing; wrong here, because
  `NOT MATCHED BY SOURCE DELETE` turns every filtered row into a delete. The
  same rule covers any `WHERE` on the MERGE source, and DLT's
  `expect_or_drop` (Session 11) on a snapshot path.
- Rejected: **dedup duplicate keys with `row_number()`**, as the CDC path does
  (Session 9). In CDC several rows per key are expected; in a full snapshot a
  duplicate key is a broken file, and picking one silently hides it.
- Trade-off: one bad value delays the whole dimension a night. Accepted — a
  day-old dimension is visible and recoverable; a silently deleted customer is
  neither.

### Revision (2026-10-01, same session) — hold the bad row, not the whole night; refuse the night only when the file looks broken

**In plain words:** the human asked whether refusing a whole night for one bad
row is too much. It is, at real scale: one junk value in 43,690 rows would hold
back 43,689 good updates, and a feed with one bad row every night would never
update Silver at all. The danger was never the bad row itself — it was the bad
row **disappearing from the MERGE's source**, which the delete clause reads as
"this seller left". So the bad row stays in the source and is simply not
allowed to change anything.

- Chosen (by the human), to build in **Session 9**: every row of the night
  stays in the MERGE source with a flag `_ok`. The MERGE becomes
  `WHEN MATCHED AND s._ok AND <changed> THEN UPDATE`,
  `WHEN NOT MATCHED AND s._ok THEN INSERT`, `WHEN NOT MATCHED BY SOURCE THEN
  DELETE` unchanged. A bad row's key is present, so it is never deleted; it is
  not `_ok`, so it keeps yesterday's values (or, if new, waits). Each held row
  is copied to a **rejected-rows table** (night, key, column, raw value), the
  merge log gains a `held` count, and an alarm fires. A corrected re-delivery
  of the night (Drill 1's `allowOverwrites` + this session's correction path)
  applies it.
- The rule in one line: **filter the changes, never the keys.**
- Still refuses the **whole night**: more than **1%** of rows bad (a shifted
  column, a wrong delimiter — the file itself is broken), any duplicate or
  NULL key (which of two S2 rows is true cannot be decided row by row), an
  empty file. The same shape as Snowflake's `COPY INTO ... ON_ERROR =
  SKIP_FILE_<n>%`: tolerate a few bad rows, reject the file above a threshold.
- Rejected: **keep refusing the night on any bad value** (the entry above).
  Safe, but it turns one bad row into a stopped dimension — the exact argument
  Session 6 used to reject "fail the batch" for Kafka quarantine, which this
  entry failed to apply to itself.
- Rejected: **load the bad value as NULL with a flag.** Keeps the row moving,
  but Silver would then say "this seller's zip is unknown" — a change that never
  happened — and Gold's SCD2 would record it, then record it again when fixed.
- Trade-off, accepted: for a while Silver is mixed — most rows at night 6, a
  held row at night 5. Visible through `_last_changed_dump_date`, the `held`
  count and the alarm; without the alarm a held row could stay stale silently,
  which is why the two are built together.
- Until Session 9 the strict rule above stays in production: safe, only strict.

## Databricks raises the alarms now (a scheduled Job + a freshness alert); Airflow takes over orchestration later (Session 8)

**In plain words:** today a refused night shows only as a red notebook cell —
if nobody opens the notebook, nobody knows. Two alarms are needed: one when a
run **fails** (a refused night), and one when **nothing happens** (no file
arrived, or the job never ran — a failure alarm cannot fire for a run that did
not happen). Both are built inside Databricks in Session 9.

- Chosen: `merge_dims` (and `autoload_dims` before it) as a scheduled
  **Databricks Job** with a failure email; a **Databricks SQL Alert** on
  `silver.dims_merge_log` — "no night applied in the last 26 hours" — as the
  freshness alarm. Whether Free Edition sends job-failure emails is
  **unverified**; checked when built.
- Rejected: **CloudWatch.** AWS's monitoring watches AWS resources; Silver runs
  on Databricks compute it cannot see, except through custom metrics pushed by
  hand. Wrong tool for this layer.
- Rejected: **wait for Airflow** (Session 6 decision, ~Session 15+). Airflow's
  real value is stopping everything downstream — no export to Snowflake, no dbt
  build — when Silver refuses a night, and it will take over orchestration.
  But seven sessions with no alarm at all is too long.
- Kept after Airflow arrives: the freshness alert stays, as a second line of
  defence that does not depend on the orchestrator itself running. It is also
  the "scheduled completeness check" carried since Drill 1.
- Adds Databricks surface area (Jobs, SQL Alerts), the project's stated
  preference.

### Revision (2026-10-01, Session 9) — the "nothing happened" alarm watches Silver falling behind Bronze, not the calendar

**In plain words:** the entry above planned a freshness alarm, "no night applied
in the last 26 hours". In this project nights are landed by hand, months of
business time apart and days of real time apart — so that alarm would be red
every day, with nothing wrong. A check that cannot return healthy trains people
to ignore it (incidents.md 2026-09-20, second prevention rule). The alarm
instead asks: **has Bronze been holding data for more than 26 hours that Silver
has not applied?** It is green on a quiet day and red when Silver is stuck.

- Chosen, two Databricks SQL Alerts, their queries versioned in
  `databricks/alerts/`:
  - `silver_behind_bronze.sql` — sources (customer, product, seller, inventory
    CDC) whose Bronze data landed over 26 hours ago and is still not in Silver.
    A refused night shows here too, a day later, if nobody acted on the job's
    failure email.
  - `dims_rows_held.sql` — rows held by each dimension's latest applied night
    (the `held` count). The "an alarm fires" half of hold-the-row: without it a
    held row could stay stale silently.
- Plus the scheduled **Job** with a failure email, as planned: a refused night,
  a refused CDC batch (a `seq` tie), or any pinned check going red fails a task.
- Rejected: **the calendar alarm, kept with a longer window** (say 7 days). Still
  red between hand-landed nights, just less often; the noise is the problem,
  not its rate.
- Rejected: **a heartbeat table written by every job run**, alarmed when stale.
  It catches "the job never ran", which the lag alarm only catches once data is
  waiting. Real, but it is what an orchestrator's own scheduling monitor does;
  Airflow (later) takes it, and the lag alarm stays as the check that does not
  depend on the orchestrator running.
- What it cannot see, recorded: **no file arrived at all.** In production that
  is the most common failure of a nightly feed; here nothing arrives on a
  schedule, so there is nothing to compare against. When landing is scheduled
  (Airflow), the calendar alarm becomes meaningful and is added then.

## Delete circuit breaker: a night may delete at most 5% of a Silver dimension (Session 8)

**In plain words:** `WHEN NOT MATCHED BY SOURCE DELETE` trusts the file to be
complete. A truncated dump — half a file, a failed export — would delete half of
Silver in one green run. So before each MERGE, Silver counts how many of its
rows tonight's file would delete, and refuses if that is more than 5% of the
table, unless that night is explicitly allowed.

- Chosen: `MAX_DELETE_FRACTION = 0.05`, checked with one anti-join before the
  MERGE; override per dimension and night, never global. Night 5 is expected
  to delete 100 / 3,095 sellers (3.2%), 100 / 32,951 products and 19 / 22,497
  customers — all under it.
- The shape it guards is already in this repo's history: the 2026-09-19
  incident (dumps drained to zero rows) is exactly a snapshot that shrank for
  a reason unrelated to real deletions; the generator's `MAX_DELETED_FRACTION`
  exists for the same reason one layer up.
- Rejected: **no guard, trust the file.** Bronze checks the table against the
  landing zone, so a truncated *file* passes every Bronze check.
- Rejected: **a fixed row count** (e.g. "never more than 500"). Wrong as soon as
  the table grows or a small table (sellers) meets a legitimate batch.
- Trade-off: a genuine mass delisting stops the pipeline until a person allows
  that night. Accepted — that is a decision a person should see.

## Zip prefixes are restored to five digits in Silver, not fixed in the generator (Session 8)

**In plain words:** the landed dumps lost the leading zero of every zip that
starts with 0 (`09790` → `9790`; incident 2026-09-30). Silver puts it back with
`lpad(zip, 5, '0')` and checks every zip is then five digits. The generator is
left as it is.

- Chosen: normalise in Silver, the layer whose job is "typed and standard".
  Lossless here: all 99,441 raw customer zips and all 3,095 seller zips are
  exactly five digits, so padding restores the original exactly.
- Rejected: **fix the generator and carry on.** Night 5 would carry `09790`
  where night 4 has `9790`; the generator's carry-forward would see a change and
  stamp a new `dim_updated_at` for every such customer (thousands), and Gold's
  SCD2 would open a version for each move that never happened. In production
  the source is not ours to fix anyway; the fix belongs upstream, and the
  defence belongs here.
- Rejected: **fix the generator and regenerate every night.** Rewrites landed
  history — refused by the write-once rule above, deliberately.
- Consequence: a source-side format fix is itself a change event. If the source
  ever starts sending five digits, Silver's padding makes it a non-event.

## Silver inventory CDC: one stream from Bronze, two tables, every write idempotent by its own condition (Session 9)

**In plain words:** Silver reads Bronze `inventory_cdc` as a stream. For each
micro-batch it drops the 53 denylisted events, keeps one copy of each event,
refuses the batch if two *different* events claim the same position for a SKU,
then writes two tables: a clean **event log** (one row per event, nothing
interpreted) and the **current stock** per SKU (the CDC `MERGE`: op flags,
latest event per SKU, `seq` guard). Nothing in it needs Kafka-style
`txnVersion` protection, because every write is safe to repeat.

- Chosen: `spark.readStream.table("workspace.bronze.inventory_cdc")` →
  `foreachBatch` → `Trigger.AvailableNow`, checkpoint in the volume, generation
  `v1` like Bronze. Bronze is append-only, so a plain Delta stream over it works
  (Silver dims could not do this — Bronze dims are rewritten by `replaceWhere`).
- Chosen, per batch, in this order:
  1. **denylist** — `event_id NOT IN` the 53 ids from
     `ops/incidents/2026-09-26_inventory_cdc_denylist.json`, passed into the
     batch function as a Python list (on serverless the batch runs in a cloned
     session; a temp view from the notebook is not relied on);
  2. **one copy per `event_id`** — the topic holds 120 exact re-sends;
  3. **tie refusal** — two different `event_id`s with one `(sku_key, seq)`, in
     the batch or against the log, fail the batch (entry below);
  4. **event log** `silver.inventory_events` — `MERGE … WHEN NOT MATCHED INSERT`
     on `event_id`. `change_reason` is **not carried**: Session 1 said nothing in
     the pipeline may read it, and a column that is not there cannot be read;
  5. **current stock** `silver.inventory` — dedup to the highest `seq` per SKU
     (`row_number() … = 1`), then the three-clause MERGE with the `seq` guard on
     **all three** (CLAUDE.md's version guards only the update; a delete older
     than the row it would delete must not delete it either);
  6. **chain breaks** counted for the batch (entry below), and one line appended
     to `silver.inventory_cdc_log` with `txnAppId` / `txnVersion = batch_id`.
- Why no `txnVersion` on the two MERGEs: a replayed batch changes nothing.
  Events already in the log match and are not inserted; the stock MERGE finds
  `s.seq = t.seq`, which is not `>`; a `D` for a row already deleted finds no
  row. Contrast Bronze, where an append is not repeatable and needed it. The
  log line *is* an append, so it gets the Bronze treatment.
- The stock MERGE reads the **whole** deduped batch, not only the events that
  were new to the log. If a run dies between the two MERGEs, the replay finds
  every event already logged — filtering to "new" would leave the stock
  un-applied forever.
- Rejected: **a batch job with a high-water mark** (`_ingested_at`, or offsets
  per partition). It re-implements what the checkpoint does, and a mark kept by
  hand is how late rows get skipped (the Gold experiment, exp_06).
- Rejected: **stock only, no event log.** Gold's reconciliation (units sold ↔
  decrements) needs every decrement, not the last after-image; the chain check
  needs the history; and Gold reads Silver, never Bronze.
- Rejected: **two streams, one per table.** Twice the checkpoints and Bronze
  reads, and the two tables could drift to different batches.

## Silver inventory mirrors the source's after-images; the broken chain is counted, not repaired (Session 9)

**In plain words:** on 237 SKUs the feed's "stock before" / "stock after" do
not follow its own `seq` order (incidents.md, 2026-10-01), and 26 SKUs would
end 27 units off the generator's own arithmetic. Silver still keeps, for each
SKU, the "stock after" of its highest-`seq` event — what the source says the
stock is now — and counts every broken link, so the defect is visible and any
*new* break fails the checks.

- Chosen: the standard after-image MERGE; `chain_breaks` per batch in
  `silver.inventory_cdc_log` (an event whose `prev_stock_qty` differs from the
  `stock_qty` of the event before it, same SKU, by `seq`, read from the event
  log); the verification pins the known baseline: **1,423 breaks, 237 SKUs, 26
  SKUs / 27 units** between after-image and deltas.
- Rejected: **fix the generator.** The topic holds these events and is treated
  as production. A fixed generator would compute different stock values — and,
  since `event_id` hashes `stock_qty`, different ids — for ~1,400 events, so a
  clean regeneration (how the denylist was found) would call correct-as-
  delivered events contaminated. Same reasoning as the zips (Session 8): the
  fix belongs upstream, the defence here. A test pins the defect.
- Rejected: **stock = seed + sum of deltas** (rebuild from the event log). It
  gives the generator's intended number on all 26 SKUs, but it is a different
  pattern: one lost or doubled event makes the sum wrong forever, where an
  after-image corrects itself at the SKU's next event. It would also make
  `seq` and the after-images pointless — the CDC MERGE is what this layer
  exists to build.
- Rejected: **refuse a batch with chain breaks, or hold those SKUs.** The
  history cannot change, so the stream would stop for good on 237 SKUs.
- Trade-off, accepted: Silver serves 26 stock levels the source's own deltas
  disagree with, by 27 units in total. Visible in the log and the checks;
  recorded rather than hidden.

## Silver inventory deletes hard (`DELETE`), not with a tombstone; a late update after a delete is a known gap, bound to exp_04 (Session 9)

**In plain words:** when a `D` arrives, the SKU's row is removed. The risk:
the row's `seq` goes with it, so if an *older* update for that SKU arrives in a
later batch, nothing remembers the delete and the MERGE inserts the SKU again.
The alternative — keep the row with `_deleted = true` (a tombstone) — prevents
that, at a cost.

- Chosen: hard delete, guarded by `s.seq > t.seq`.
- Why it is safe here, and only here: the Kafka key is `sku_key`, so a SKU's
  events share one partition in `seq` order; Bronze appends them in offset
  order; Silver's Delta stream reads Bronze versions in order. An older event
  can only reach Silver after a newer one if the producer emitted it out of
  order — which `--out-of-order-pct` does on purpose. **This is an assumption
  about the source, not a guarantee of the MERGE**, and Session 10's `exp_04`
  tests it: a late update after a delete, assert the SKU comes back, then
  decide the fix with evidence.
- Rejected: **tombstones now** (`_deleted`, keep `seq`). What Lakeflow's
  `APPLY CHANGES` does internally — it is the S11 comparison. But every reader
  must then remember `WHERE NOT _deleted`, the same leak-by-omission Session 7
  rejected for a quarantine flag column; and tombstones need their own
  clean-up. Not worth it before the failure has been shown.
- Rejected: **ignore `D`** (keep delisted SKUs). Upsert is not sync — exp_03's
  lesson, one layer over.

### Correction (2026-10-02, Session 10) — the gap was real, and it is closed without tombstones

exp_04 C showed it: X's update arriving after X's newer delete put X back in
the stock (`extra=1` against the log's newest events). The fix is not a
tombstone in the stock table but reading each SKU's newest event from the
**event log**, which already keeps every delete — entry "The stock MERGE takes
each SKU's newest event from the event log" (Session 10). The hard delete
stays.

## Two different events at one `(sku_key, seq)` fail the batch (Session 9)

**In plain words:** `seq` is the order. If two different events claim the same
place in it for one SKU, no rule can say which one happened — Silver stops and
waits for a person, who adds the wrong one to the denylist.

- Chosen: after the denylist and the per-`event_id` dedup, count
  `(sku_key, seq)` pairs with more than one `event_id`, inside the batch and
  against the event log. Any → the batch raises, nothing is written, and the
  stream retries the same batch on the next run until the denylist covers it.
  With the denylist: 0. Without it: the 2026-09-26 contamination fires it.
- Rejected: **keep the first** — what `s.seq > t.seq` does on its own, in
  silence (the Session 5 incident).
- Rejected: **keep the latest Kafka offset.** The contamination was produced
  *after* the clean run, so "latest" picks the wrong stock on every tied SKU.
- Rejected: **hold just that SKU and carry on** (the Session 8 "hold the row"
  rule). There, a held row keeps yesterday's value and the rest of the file is
  fine. Here, two producers wrote the same keys — the topic is wrong, not a
  row — and both events would sit in the event log, counting one sale twice.

## The delete branch is driven by 100 delist events on the live topic, produced after Silver's first run (Session 9)

**In plain words:** the topic has no `D` events, so Silver's delete clause
would never run. The generator already knows how to delist SKUs; a new flag
sends **only** those 100 events, and they go to the live topic after Silver
has built its stock, so the `DELETE` hits rows that exist.

- Chosen: `inventory_cdc.py --sink kafka --delist-count 100 --delists-only`.
  The run builds the full event set (its other events are identical to the
  topic's — checked: same ids, same `seq`) and sends only the `D`s. Scope
  header `full;only=D`. 100 SKUs, 2,452 units of stock removed; every `D`
  carries a `seq` above its SKU's last event; stock goes 34,448 SKUs → 34,348.
- Why after Silver's first run: produced first, a SKU's `D` would land in the
  same batch as its history, dedup would keep only the `D`, and `NOT MATCHED
  AND op = 'D'` would simply not insert it. Correct — and the `DELETE` clause
  would still never have run. Same idea as night 5 in Session 8.
- Rejected: **a scratch topic or a hand-built batch.** Proves the SQL, not the
  pipeline; exp_04 does that on scratch tables.
- Rejected: **regenerate the topic with `--delist-count 100`.** Rewrites
  production history.

## Held dims rows are kept per night, replaced on each apply — not an append-only log (Session 9)

**In plain words:** `silver.dims_rejected_rows` answers "which rows of night D
were held the last time D was applied". A corrected re-delivery of D replaces
D's rows — with nothing, if the correction fixed them.

- Chosen: written with `replaceWhere` on `dimension` and `dump_date` — exp_02's
  rule for a re-writable unit. Skipped when nothing is held and nothing is there
  to clear (an empty overwrite is still a commit).
- Rejected: **append every held row on every apply.** A full audit trail, but a
  re-delivery would leave the fixed row looking still-held, and the alarm query
  would have to work out which attempt is current. The history of earlier
  attempts stays in the table's Delta history (30 days).

## Coverage map: every Databricks / Snowflake / dbt topic gets a row, a weight and a session — built, or taught as theory (Session 9)

**In plain words:** the human asked whether the project covers what Databricks
and Snowflake interviews and production actually ask about. Honest answer: the
data-engineering core yes, the analytics, governance, loading and performance
sides only partly. So every topic worth knowing now has one row in
`docs/coverage.md` with an interview weight and a session. Where the free tiers
or the project's data make a topic impossible or dishonest to build, it is
**taught as theory** at a named session (a short note and one interview question
in `learning.md`), so nothing is missing without being known to be missing. The
file is binding: a session reads its rows at the start, the same way the
experiment tracker works since `exp_02` went unscheduled.

- Chosen: `docs/coverage.md` as a living status table (built ✓ / build B /
  build-if-allowed B? / theory T / skip X, each bound to a session), CLAUDE.md
  start-of-session item 6 to read it, theory notes in `learning.md` Part C
  "C11. Theory-only topics", and a calendar rule — every Databricks-only row is
  done before the Snowflake trial starts.
- Chosen: the Snowflake trial is created as **Enterprise edition** — masking,
  row access policies, multi-cluster warehouses and materialized views need it.
- Origin, recorded honestly: the map started as a proposal from a conversation
  outside this session, reviewed here against the repo. Four corrections were
  made before adopting it:
  - **Liquid clustering moves from S10 (Silver orders) to S21** on a large
    sample table. Silver orders will be one file, so "files read" would be 1 vs
    1 — and Session 0 already decided Olist is too small for a pruning demo.
  - **Photon is theory only.** Free Edition is serverless, and serverless always
    runs Photon; there is nothing to compare against.
  - **Jobs and alerts are built**, not planned (S9).
  - **Free Edition features the proposal assumed** — UC groups and service
    principals, Delta Sharing, system tables, feature engineering, UniForm read
    by Snowflake — are **B?**: verified first, turned into T with a recorded
    finding if refused.
- Added in review, missing from the proposal: Spark performance (joins, skew,
  AQE, query profile — ★★★, no session built it); `AUTO CDC … STORED AS SCD TYPE
  2` vs the dbt snapshot; `COPY INTO` with a re-delivered file of new content
  (the cross-platform twin of Drill 1's Auto Loader skip); Snowpipe's S3 event
  notification and 14-day history; suspending every Snowflake Task as a cost
  guard; Snowpark (small); and every item of the existing plan the proposal's
  session list had left out (S10 Silver orders + exp_04, S11 windowed streaming
  and DLT, Drills 2 and 3, the CDF export including `delete` rows).
- Rejected: **a new session per topic.** Breaks the Snowflake trial's 30-day
  calendar and the human's "finish sooner" goal. Additions fold into existing
  sessions; only two optional sessions are new (S12b ML features, S20 interop).
- Rejected: **build everything, A/B where possible.** The human, explicitly:
  not every topic needs building; what matters is knowing what exists and how an
  interview asks about it. A theory hook costs ~10 minutes; building blocked or
  dishonest features would be coverage for its own sake, against the thesis.
- Rejected: **skip the analytics, governance and ML side.** Leaves common
  interview questions unanswerable, and Free Edition already offers most of it.
- Rejected: **ML model tuning, serving, vector search.** No honest use here. ML
  stays only as the pipeline feeding a model — point-in-time features, no
  training on future data — which is a data-engineering problem.
- Rejected: **keep the plan in `### Next` only.** That is where `exp_02` was
  lost: a promise with no binding is not a plan.
- Trade-off: about 10–12 extra hours across sessions plus two optional
  sessions, and S13–S21 plus Drill 3 (~10 sessions) inside 30 trial days.
  Accepted, with the gate that Databricks-only work finishes first.

## Silver orders: one row per order, one column per lifecycle step — not "the last event wins" (Session 10)

**In plain words:** an order sends up to four events (`created`, `approved`,
`shipped`, `delivered`, or a synthetic `canceled` / `unavailable`). Silver keeps
one row per order with **a timestamp column for each step**; each event fills
its own column, and the status is the furthest step reached. The obvious
alternative — treat it like the stock feed, keep the newest event and let it
overwrite the row — gives the wrong status on 93 orders, because Olist's own
timestamps sometimes run backwards.

- Chosen: `silver.orders` (an *accumulating snapshot*, Kimball's name for a row
  that grows as a process moves) + `silver.order_items` (one row per unit, from
  the `created` event's items). Per batch: one copy per `event_id` → refuse a
  step with two different times → fold the batch to one row per order (a
  `groupBy` with `max(CASE …)` per step — the in-batch dedup for this MERGE) →
  `MERGE … ON order_id` where `WHEN MATCHED` fires only if the batch brings a
  step Silver lacks, each column `coalesce(t, s)`, status recomputed → items
  insert-only on `(order_id, order_item_id)` → one log line with `txnVersion`.
- Measured on the laptop first (pinned in `tests/test_headline_numbers.py`):
  - "the last event to arrive wins" disagrees on **93** orders: 61 would read
    `approved` for an order that was delivered (approval timestamped after
    delivery in Olist), 23 `shipped` for delivered, 9 `approved` for shipped;
  - **1,305** orders have two steps at the same second (often `created` and
    `approved`), so an `event_ts` used as a `seq` with `s.seq > t.seq` drops one
    of them when they land in different batches — a tie is not "greater";
  - **166** orders arrive with `shipped` first (carrier date before purchase).
    A column per step does not care: arrival order cannot change the result, so
    there is no `seq` and no guard to get wrong. The opposite of the CDC MERGE.
  - A first count said 104, not 93: the laptop's sort broke those 1,305 ties at
    random. Ties broken by arrival order (the Kafka offset), it is 93 every run.
- Chosen: **`status` is derived from the steps**, precedence `canceled`,
  `unavailable`, `delivered`, `shipped`, `approved`, `created`. The status Olist
  printed on the row is kept as `source_status` and not used: every event
  carries the order's **final** status, a fact from the future while the replay
  runs, and it disagrees on **623** orders — 314 `invoiced` and 301
  `processing` (no event says either) and 8 `delivered` with no delivery time.
- Rejected: **the newest event per order overwrites the row** (the S9 CDC
  shape). Wrong on 93 orders, drops a step on the 1,305 ties, and loses `items`
  and earlier times on every update — the newest event carries neither.
- Rejected: **an order event log beside the snapshot** (as inventory has).
  Inventory needs every decrement for the reconciliation and the chain check;
  here every event *is* one column, so the snapshot loses nothing but re-sends
  and `produced_at`. Gold's incremental reads `_merged_at`.
- Rejected: **items as an array column on `orders`.** Gold would explode it on
  every read, and the reconciliation is per SKU, at the unit grain.
- Trade-off: a new event type needs a new column (schema change); a generic
  "latest event" table would not. Accepted — the lifecycle is fixed by Olist.

## Silver orders' first run reads Bronze from version 0, three files a batch (Session 10)

**In plain words:** by default a new stream over a Delta table reads the whole
table as it is now in one go. Bronze `orders` already holds every event, so
every order would reach Silver complete in one batch: 99,441 inserts, zero
updates — the MERGE would be an insert in disguise, exactly what Session 1
fanned the events out to avoid. Starting at Bronze's version 0 makes Silver
read Bronze's history commit by commit, as it would have if Silver had been
running since Bronze's first batch.

- Chosen: `startingVersion = 0`, `maxFilesPerTrigger = 3` (Bronze wrote each
  Kafka batch as 3 files, one per partition), first run only — after that the
  checkpoint decides. Silver's log records the Bronze `_batch_id` range each
  batch read; the verification recomputes inserts and updates per Bronze batch
  from Bronze alone and must match exactly.
- Why it is possible: Bronze's 8 original commits were replaced by an automatic
  `OPTIMIZE` (v9, 2026-09-27), but their files are kept until VACUUM — 30 days
  since Session 7's retention setting. **A new generation after ~2026-10-27
  must drop `startingVersion`** and accept one big first batch.
- Rejected: **the default snapshot read.** Correct final state, but the
  `WHEN MATCHED` clause — the point of the layer — would never run on live data
  (Session 9's CDC update clause is still in that state).
- Rejected: **produce new order events after the first build** (S9's delist
  trick). There are no new real orders to send; inventing lifecycle events would
  put data on the live topic that the dataset does not contain.

## Delta CHECK constraints only for facts true at every moment, not for the source's quality (Session 10)

**In plain words:** a Delta `CHECK` constraint makes every write that breaks
it fail — the whole write, nothing committed — whoever writes. That is right
for "this can never happen in our tables", and wrong for "the source should
not do this": the source does it, and the stream would stop for good.

- Chosen: `silver.orders` `status_known` (status is one of the six steps);
  `silver.order_items` `money_not_negative` (price and freight ≥ 0; laptop min
  0.85 and 0.00); `order_id` / `(order_id, order_item_id)` `NOT NULL`.
  Added from the notebook that creates the table, checked first (every `ALTER`
  is a commit); a rule that exists with different text is refused.
- Rejected: **`created_at IS NOT NULL`** — true at the end, false for the 166
  orders whose `shipped` arrives first: the batch holding that `shipped` would
  fail forever. A constraint must hold after every batch, not only the last.
- Rejected: **`shipped_at >= approved_at`, `delivered_at >= shipped_at`** —
  Olist breaks them 1,359 and 23 times. Those are facts about the source, counted
  (like the CDC chain breaks), not rules to refuse it by.
- Rejected: **no constraints, the code is correct.** A constraint is checked by
  the engine on every writer — a hand-run `UPDATE`, a future job — not only by
  this pipeline.
- Note for Snowflake (S13): Snowflake enforces `NOT NULL` only and has no
  `CHECK`. Databricks' `PRIMARY KEY` / `FOREIGN KEY` are informational too —
  "enforced, unlike Snowflake's primary key" in coverage.md compares the wrong
  things; the honest line is *Delta enforces CHECK and NOT NULL; neither
  platform enforces a primary key*. To be shown on the platform this session.

## exp_04's contamination runs on scratch tables fed with Bronze's real contaminated rows, not on the live topic (Session 10)

**In plain words:** the experiment needs bad events to push through Silver's
CDC code. They already exist: 159 contaminated rows from Session 6 sit in
Bronze. exp_04 copies them, with the clean events of the same 23 SKUs, into
scratch tables and runs the production Silver code over them. Nothing new is
sent to the live topic.

- Chosen (by the human, 2026-10-02): scratch. Four steps on the production
  `make_batch_writer`: guard on, no denylist → the batch is refused; denylist
  added → the same batch retried and applied; guard off → the bad events reach
  the event log; `RESTORE` → the log is repaired and equals the guarded run.
- Rejected: **a new partial run on the live topic** (the Session 5/6 plan). It
  would show the real 06:00 job failing and emailing — but that email path was
  already proven by `alarm_test` (S9), the tie guard was already shown to have
  53 real ties to catch (S9 Verify 5), and a refused batch writes nothing, so
  `RESTORE` would still need scratch tables. Against that, the topic and Bronze
  would carry ~50 more bad events forever and every reader a longer denylist. A
  production team does not put known-bad data into a production log on purpose.
- Trade-off: the tie guard has still never fired inside the *live* stream —
  only in exp_04's scratch stream, with the same code.

## The stock MERGE takes each SKU's newest event from the event log, not from the batch (Session 10)

**In plain words:** when a SKU is deleted, its stock row is gone, and with it
the `seq` that would tell Silver "anything older than this is history". An
update that arrives after the delete, but happened before it, then matches
nothing and is inserted — the SKU comes back. Silver already keeps every event,
deletes included, in its event log. So the stock MERGE now asks the log, not
the batch, "what is this SKU's newest event?" — and for that SKU the answer is
the delete.

- Chosen: `newest_in_log(events, events_table)` — the batch's SKUs joined to
  the whole event log (which already holds the batch: it is written first),
  `row_number()` over `seq` descending, `= 1`. `make_batch_writer(newest_from=
  "log")` is the default; `"batch"` stays only so exp_04 C can show the bug.
- Evidence (exp_04, 2026-10-02): with the batch's newest, X (`157b4fa7…`:
  `I` 14 → `U` 13 → `U` 12 → `D`) came back at stock 12 after its late 3rd
  event; with the log's newest, X stayed deleted and the batch did nothing
  (`newest_is_delete 1`, inserted / updated / deleted 0). On the live feed,
  **53** events arrive behind their SKU's order — every one of them the
  denylisted 2026-09-26 contamination; **0** clean ones. So nothing changes
  for any data Silver has seen; the guarantee now holds by construction
  instead of by an assumption about the producer.
- Rejected: **tombstones** (keep the row with `_deleted = true` and its `seq`).
  What Lakeflow's `APPLY CHANGES` does, and the S11 comparison. Every reader
  of the stock would have to filter them (leak-by-omission, as Session 7 said of
  a quarantine flag), and they need their own clean-up — to solve a problem the
  event log already solves.
- Rejected: **keep the ordering assumption and add a detector** (count events
  arriving below their SKU's highest `seq`, alarm on > 0). Cheap, and it was
  the plan if the fix had been expensive. It is not: the fix costs one join of
  the batch's SKUs to the log, and it removes the failure instead of reporting
  it after the stock is wrong.
- Trade-off: each batch reads the log for the SKUs it touches (on the first
  build, all 34,448 — the same work the batch dedup did). The `seq` guard stays
  on all three clauses: with the log's newest event it should never fire, and
  if it does, something else is wrong.

### Addendum (2026-10-04, same session) — the log's newest event also covers what the `seq` guard covers

exp_04's re-run showed it by accident: with the guard switched off and the
newest event taken from the log, a late older update for A changed nothing (A
stayed at its 3rd event, 26), because the log already held the newer event and
the MERGE's source for A was that newer event. So for out-of-order arrivals the
`seq` guard is now a second line of defence, not the only one. It stays: it
costs nothing, and it is what keeps the MERGE correct if anyone ever runs it
with `newest_from="batch"` again. exp_04 B1b pins this behaviour.

## Silver streams refuse a checkpoint reset into a log that already has lines (Drill 2)

**In plain words:** if someone deletes a Silver stream's checkpoint — its
memory of which Bronze rows it has applied — the data is not harmed: every
Silver MERGE is safe to run twice. But the stream's one-line-per-batch log is
keyed by a batch number that restarts at 0, and that log is what the "Silver is
behind Bronze" alarm counts. Drill 2 showed the two ways it breaks: keep the old
app id and the log silently stops recording (the alarm fires for rows Silver
already has); give it a new app id and every row is recorded twice (the alarm
can no longer fire at all). So a Silver stream with no history now refuses to
start unless its log is empty.

- Chosen: `refuse_unsafe_reset(checkpoint, app_id, log_table)` in
  `_merge_inventory_cdc` and `_merge_orders`, called by `run_silver_cdc` /
  `run_silver_orders` before `readStream`: if the checkpoint has no `offsets/N`
  file and the log holds a row, raise `UnsafeStreamReset`. `guard_reset=False`
  exists only so the drill can show the trap first. A rebuild is: new
  `GENERATION` (checkpoint and app id together) **and an empty log** (archive
  the old one first if its history matters: `CREATE TABLE … AS SELECT`, then
  `TRUNCATE TABLE`).
- Evidence (`drills/drill2_cdc`, scratch tables, 2026-10-04): life 1 applied
  158,625 Bronze rows (one batch, `batch_id 0`). 100 delists landed; checkpoint
  deleted, **same app id**: the event-log MERGE inserted **100**, stock went to
  **34,348 / 991,530** (= production, `extra 0 missing 0`) — and the log still had
  **1 line, 158,625 rows**, one WRITE commit, against 158,725 in Bronze. The
  production alert query, pointed at those tables, said **1**. Then a **new app
  id**: a second line, **317,350** rows logged for 158,725; with 100 more rows
  waiting and never applied, the alert said **0**. With the guard on, both resets
  were refused before any stream started; production's two checkpoints have
  history and pass.
- Why only the log, not Bronze's rule ("a stream with no history may only write
  into empty tables"): the drill shows the data tables are safe to replay into
  — demanding they be empty forces a full rebuild the reset never needed, and
  after ~2026-10-27 a Silver orders rebuild can no longer read Bronze's history
  commit by commit (VACUUM removes the replaced files), so it would lose the
  "inserted, then updated" order of every order.
- Rejected: **no guard — "the data is safe, the log is bookkeeping".** The log
  is the input to the lag alarm. R1 made the alarm lie in the noisy direction (a
  false page); R2 made it lie in the quiet direction: Silver could fall 158,625
  rows behind Bronze with nothing firing. A blind alarm is worse than none,
  because people stop looking.
- Rejected: **record the generation in every log line and alarm per
  generation.** Fixes the double count of a new app id, not the silent skip of
  a kept one; adds a column, a migration of old lines, and a harder alert query.
  The guard is ten lines and covers both.
- Same shape as Bronze's guard (Drill 1), different reason: Bronze would lose
  *data*, Silver loses its *record* of the data. Both come from the same two
  memories — Spark's checkpoint and Delta's `txnAppId → txnVersion` — being reset
  separately.

## Silver dims refuse to run when the merge log has lost a dimension's lines (Drill 2)

**In plain words:** the dims path has no stream and no checkpoint; its memory
is the merge log, which says which night Silver shows. If that log loses a
dimension's lines while the table keeps its rows, the next run starts again from
the first night and MERGEs every old night over today's table. Drill 2 did
exactly that: customer was stopped by the 5% delete breaker, but seller replayed
its whole history with no error — 100 deleted sellers inserted and deleted
again, 273 updates — into the change feed Gold will read. So a dimension with
no line in the log now refuses to run unless its table is empty.

- Chosen: `refuse_lost_log(dim, table, log)` in `_merge_dims`, called by
  `apply_dim` before any night (`guard_reset=False` only for the drill);
  `MergeLogLost` is collected by `merge_dims.run_all` like the other refusals, so
  the Job fails and emails. Two ways out, both shown in the drill: **restore the
  log** with time travel (`RESTORE TABLE … TO VERSION AS OF n` → "nothing new",
  no Silver version moved), or **rebuild the dimension from an empty table**
  (delete its log lines, `TRUNCATE` the table, run) — part S showed a rebuild
  reproduces production's merge log and every column exactly.
- Evidence (`drills/drill2_dims`, scratch schemas, 2026-10-04): customer
  `would delete 43,689 of 43,690 rows (100.0%)` → refused, version 5 → 5. Seller
  `2016-09-04: +100 ~98 -0`, then the four golden nights; change feed since the
  loss `insert 100, update_postimage 273, delete 100`; end state = production on
  every column. With the guard: both refused before any MERGE; production's three
  dimensions pass.
- Rejected: **rely on the delete breaker.** It stopped customer only because
  customer's first night held one row. Seller's and product's first nights hold
  every key, so nothing is deleted and the breaker sees a normal night — the
  damage (history rewritten in the change feed, deleted rows briefly back) is
  invisible to it.
- Rejected: **rebuild automatically when the log is empty.** Right only if the
  loss was real and total; a log emptied by mistake (a wrong `DELETE`) is better
  restored than rebuilt over, and the choice needs a person who knows which.
- Same rule as the two stream guards, stated once for Silver: **a writer with no
  memory may only write into an empty target.**

## A corrected old night is alarmed, not applied (Drill 2)

**In plain words:** Silver dims shows the newest night and never goes back in
time, so a correction to an *older* night (re-delivered with `--redeliver`) is
deliberately not applied. Drill 2 found that nobody was told: the Job stayed
green, the lag alarm counted only nights newer than the last one applied, and
the only trace was one printed line. The alarm query now counts it
(`old_nights_changed`), and the response is a person's: rebuild that dimension
from an empty table.

- Chosen: a fourth part of `databricks/alerts/silver_behind_bronze.sql` — nights
  older than the last applied whose Bronze copy is newer than the one the merge
  log recorded (or which were never applied); the query now also returns each
  part as its own column. Response: rebuild the dimension (delete its log lines,
  `TRUNCATE`, run). That re-records the night and clears the count.
- What the correction can and cannot change: Silver's **current** state comes
  from the newest night, so an old-night correction leaves it untouched. It
  changes the **record of history** — that night's merge-log numbers and the
  change feed — and that is what Gold's history (S13–S14) is built from. Whether
  that matters is why a person decides.
- Rejected: **fail the Job.** It would block every newer night for a correction
  that cannot change the current state, and fail daily until someone acts.
- Rejected: **apply it.** Silver would go back to 2017-01-02 and forward again
  only on the next new night — the "never backwards" rule of Session 8 exists to
  prevent exactly that.
- Rejected: **a separate alert.** Same audience, same first question ("what has
  Bronze got that Silver does not?"); one query with one column per part is
  easier to keep in step with the code than two.
- Later: the Airflow backfill (S15–S16) is the automated form of "rebuild from
  that night".

### Correction (2026-10-04, same session) — the current row's lineage does move

The entry above says an old-night correction "leaves [Silver's current state]
untouched". True of every business value, not of the row's lineage. The drill's
rebuild with the corrected 2017-01-02 ended with **one** seller different from
production: the victim, whose `_last_changed_dump_date` became 2017-05-02 — it
changed on the corrected night and changed back on the next one (merge log
`(0, 26, 0)` and `(0, 51, 0)` against production's 25 and 50). So the
correction reaches today's table too, through the lineage columns that record
history. The decision stands; the reason for a person to look is stronger.

## Lakeflow (DLT) runs beside production Silver in its own schema, as AUTO CDC on inventory and expectations on both streams — not as a replacement (Session 11)

**In plain words:** Databricks' declarative pipelines (DLT, now "Lakeflow
Declarative Pipelines") let you *declare* "keep the current stock per SKU from
this change feed" in about ten lines, where production Silver inventory does it
by hand in about 570. To learn what that declaration actually does, it is built
**next to** production: the same Bronze rows, its own schema
(`workspace.silver_dlt`), and a notebook that compares the two row by row. The
numbers decide what it is good for; nothing in production reads it.

- Chosen: one pipeline, `ledgerline-dlt`, two plain Python source files in
  `databricks/dlt/`:
  - `inventory_apply_changes.py` — `inventory_cdc_checked` (Bronze rows +
    three expectations), then `create_auto_cdc_flow` twice from it: **SCD1**
    (`inventory_scd1`, the twin of `silver.inventory`) and **SCD2**
    (`inventory_scd2`, one row per stock level, `__START_AT` / `__END_AT` = `seq`).
  - `orders_expectations.py` — `orders_checked` with the Session 7 guard
    (`order_status` never blank) and a real data defect (775 orders placed with
    no items) whose action is a pipeline setting: `warn`, `drop` or `fail`.
- **The denylist becomes an `expect_or_drop`**, not a filter hidden in code:
  the pipeline's own event log then counts the 159 copies it removes, every run.
  It is read from `ops/incidents/` through a required setting
  (`ledgerline.repo_root`) — a pipeline that cannot find it stops rather than
  running without it.
- **No in-batch dedup is written before AUTO CDC, on purpose.** Bronze holds 120
  exact re-sent copies. Whether AUTO CDC handles two identical rows for one
  `(sku_key, seq)` — and what it does with two *different* ones (the 53
  contaminated events, in a deliberate run with the denylist off) — is the
  question; production's MERGE refuses the second case (`find_problems`).
- **Predictions are computed from Bronze and production Silver before any
  pipeline table is read** (`databricks/checks/dlt_vs_silver.py`, cell 1):
  checked rows 158,725 − 159; SCD1 = `silver.inventory` (34,348 SKUs / 991,530
  units); SCD2 = 158,346 versions (one per I/U event), 34,348 open, 100 closed by
  a delete; every version's end = `lead(seq)` over production's event log.
- Rejected: **replace production Silver inventory with the pipeline.** Silver's
  hand-written path has properties nothing here has shown AUTO CDC to have: a
  refusal on tied `seq`, a chain check, the merge log the lag alarm counts, the
  newest event *from the whole log* (no resurrected deletes). A rewrite that
  loses one of these silently is the failure this project exists to catch. The
  comparison may move pieces later; it does not start by moving them.
- Rejected: **AUTO CDC for orders too.** AUTO CDC keeps, per key, the row with
  the highest sequence — "the newest event wins", the model Session 10 rejected
  on numbers (93 wrong statuses; Olist's own times run backwards). Orders get
  expectations only.
- Rejected: **a separate pipeline per source.** Free Edition allows one active
  pipeline per type; one pipeline with two files runs both in one update.
- Rejected: **the pipeline writing into `workspace.silver`.** Its tables would
  sit beside production tables of almost the same name, and a full refresh of
  the pipeline drops and rebuilds what it owns. Its own schema keeps a full
  refresh harmless.

### Findings (2026-10-05, same session) — what the side-by-side showed

Four pipeline updates (`3ba4c8a4` normal, `307b9339` `fail` rule, `2109f6e2`
denylist off, `4e8c32e8` restored) and `databricks/checks/dlt_vs_silver`:

- **AUTO CDC SCD1 = production `silver.inventory`, row for row**: 34,348 SKUs,
  0 on one side only, 0 with a different stock **or** `seq`, 991,530 units.
- **AUTO CDC SCD2 = `lead(seq)` over production's event log, version for
  version**: 158,346 versions, 0 missing / extra / different end / different
  event, 34,348 open, 100 closed by a delist, 0 duplicate starts.
- **Exact duplicates are handled for you**: the 120 re-sent copies reached AUTO
  CDC (158,566 rows, 158,446 events) and made no extra version and no error.
- **Ties are not**: two different events at one `(sku_key, seq)` were accepted
  silently (incidents.md, same day). Production's refusal stays the guard.
- **What the declaration gave for free**: per-expectation pass / fail counts in
  the pipeline's own event log (`event_log()`), a lineage graph, retries and
  checkpoints managed. What it took away: time travel on its tables (refused on
  a pipeline streaming table) — the repair is a re-run from Bronze.
- So the decision above stands, now on evidence: production Silver stays
  hand-written; the pipeline stays beside it as the comparison and as the
  answer to "why not DLT?" — *it equals our MERGE on clean data, and silently
  decides the one case our MERGE refuses.*

## Windowed streaming is measured on Bronze's own history replayed, as an experiment — not added to production (Session 11)

**In plain words:** a watermark tells a stream "nothing older than *newest time
seen minus a delay* will come any more", so it can forget old state — and a row
that still arrives behind that line is dropped without an error. Whether that
bet is safe depends on how disordered the data really is, so it is measured on
our real data: Bronze's `orders` and `inventory_cdc` history replayed exactly as
Bronze wrote it, one commit per micro-batch, and the result compared with the
batch answer. Production keeps its MERGE-based dedup and batch reconciliation.

- Chosen: `experiments/windowed_streaming.py`, scratch tables
  `workspace.silver.s11_*`, new checkpoints each run.
  - **Part A, dedup three ways** on the same replay: `dropDuplicatesWithinWatermark`
    on **event time** (`event_ts`), on **arrival time** (`_kafka_timestamp`), and
    an insert-only `MERGE` on `event_id` (Silver orders' way). Then one
    genuinely new event arrives with a 2016 event time, through a second source.
  - **Part B, the two topics joined**: order lines `LEFT OUTER JOIN` sale
    decrements on SKU within 1 hour of the order, watermarks on both sides,
    the sales side deduplicated with `dropDuplicatesWithinWatermark`; one sale
    removed on purpose so exactly one line must come out unmatched; run with a
    tight (1 minute) and a loose (1 day) watermark.
  - Predictions from Bronze in batch SQL first: per batch, the rows at or behind
    *(newest time in earlier batches) − delay*.
- Why replay, not a fresh produce: the disorder that decides a watermark's cost
  is the one Bronze really holds — three partitions cut into ~50,000-message
  batches that do not end at the same moment of event time, and Drill 1's 74
  re-sends that carry 2016 event times but arrived on 2026-09-28. A new produce
  would cost Confluent money and could only show a disorder we designed.
- Rejected: **a streaming reconciliation table in production.** Free Edition
  runs `AvailableNow` once a day, so "real time" is state carried in a checkpoint
  between daily runs — no fresher than the batch check Silver already runs (per
  SKU, 0 mismatched, Drill 2). It would add a second checkpoint to guard and a
  watermark to tune for no gain in freshness.
- Rejected: **`dropDuplicatesWithinWatermark` in Silver orders instead of
  `MERGE`** — the Session 6 expectation is that it either drops late genuine
  events (event time) or keeps late duplicates (arrival time); Part A measures
  which and how many.
- Rejected: **`dropDuplicates` with no watermark** — exact, but its state grows
  with every event ever seen and is never cleaned; on a stream that runs for
  years that is a memory leak with a checkpoint. `MERGE` keeps the same memory
  in a Delta table, where it is queryable and costs storage, not executor RAM.

## Kafka Bronze accepts an added field as a new column (`mergeSchema`); a type change still stops it (Session 11)

**In plain words:** the Schema Registry contract chosen in Session 7
(`BACKWARD_TRANSITIVE`) lets a producer add a field with a default. Bronze
decodes every message with the subject's newest schema, so the next batch would
carry a new column — and Bronze's plain append would refuse it with a schema
mismatch, stopping ingestion of a change the contract had already approved.
Bronze now appends with `mergeSchema`: the new field becomes a column (older
rows read it as NULL). A field changing type (e.g. `int` → `long`, which the
contract also allows) still fails the batch, so a person looks at it.

- Chosen: `make_batch_writer(..., merge_schema=True)` in `_kafka_bronze` —
  `.option("mergeSchema", "true")` on both appends (data and quarantine), beside
  `txnAppId` / `txnVersion`. `merge_schema=False` exists only for the experiment
  that shows the refusal first (`experiments/delta_schema_evolution`, same
  session), like `idempotent=False` for exp_01.
- Why Bronze and not Silver: Bronze's job is to hold every record the contract
  allows (it already keeps the raw bytes); the registry is the gate, not
  Bronze. Silver selects its columns by name, so a new Bronze column changes
  nothing downstream until someone maps it — the expected order of work.
- Rejected: **keep failing on any schema change** (fail loud, redeploy). It
  stops all ingestion — every message behind the new one waits in Kafka (3-month
  retention) — for a change the contract already checked, and the fix is always
  the same one-line option. A loud failure is worth it only when a person has a
  real decision to make.
- Rejected: **type widening on as well** (`delta.enableTypeWidening`). It would
  let `int → long` through silently too; a type change touches Silver's typed
  columns and the Parquet the Gold bridge exports, so it should stop and get a
  person. Revisit if a widening ever happens for real.
- Rejected: **Auto Loader-style "fail once, then evolve"** (`addNewColumns`): it
  is Auto Loader's mode for files; Kafka Bronze has no schema location to
  update, and a failure email for an approved change is noise.
- Expected downstream, **not yet verified**: a Delta table gaining a column is
  a schema change for every stream reading it (Silver orders and CDC): the
  stream stops once and picks up the new schema on restart. On the daily Job
  that is one failed attempt absorbed by serverless's automatic retry — to be
  seen when a field is first added for real, or in Drill 3.

### Correction (2026-10-06, same session) — a type change does not stop Bronze

The entry above says "a field changing type (`int` → `long`) still fails the
batch, so a person looks at it". Wrong — `experiments/delta_schema_evolution`
E2, first run: the production writer (`mergeSchema` on) appended five rows whose
`schema_version` arrived as `long` with **no error**; the version moved 1 → 2
and the column **stayed `int`** — Delta kept the table's type and cast the
values into it. Values that fit an `int` land unchanged, so nothing looks
wrong. What a value too big for an `int` does is measured next (E2, second run)
and recorded below; until then the claim "a type change gets a person" has no
guard behind it.
