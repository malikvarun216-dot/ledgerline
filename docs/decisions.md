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

