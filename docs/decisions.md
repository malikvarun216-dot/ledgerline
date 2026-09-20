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
