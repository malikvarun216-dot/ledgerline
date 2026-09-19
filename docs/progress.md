# Progress

Chronological session log. Updated at the end of every session with what was
**actually verified** — not merely what was built.

---

## Session 0 — Repo scaffold + docs discipline
Date: 2026-09-19

### Built
- `git init` on branch `dev` (no remote yet)
- `CLAUDE.md` — self-loading project instructions, **gitignored** (local only)
- `.gitignore` — excludes `CLAUDE.md`, `data/`, secrets, `dbt_project/profiles.yml`, Spark/Delta artifacts
- `requirements.txt` — generators, Databricks SDK, dbt with both adapters
- `requirements-dev.txt` — pytest, ruff, pyspark, delta-spark, moto, freezegun
  (honours the jobpulse prevention rule: pin dev deps somewhere installable)
- `docs/decisions.md` — **11 seeded entries** from the architecture conversation, each naming its rejected alternative
- `docs/incidents.md` — header + format, ready to append
- `docs/learning.md` — header + format + in-scope/out-of-scope table for the learning check
- `docs/runbook.md` — header + format + one pre-written entry (cost emergency / forgotten cluster)
- `docs/progress.md` — this file
- Directory tree + empty stubs at every path the architecture calls for

### Architecture decided (full detail in `decisions.md`)
- **Name:** ledgerline
- **Platform split:** Databricks (paid, Premium/AWS) owns Bronze+Silver; Snowflake owns Gold
- **Three sources:** order events (Confluent Kafka) · nightly dimension dumps (S3) · inventory CDC (Confluent Kafka, derived from real `order_items`)
- **Two corrections to the original mapping:** `WHEN NOT MATCHED BY SOURCE DELETE` moved off the CDC path onto a new Silver dimensions layer; late-arrival experiment moved from Bronze to the Gold dbt incremental
- **Budget:** ~$22 total. Confluent Basic $0 at rest, Snowflake trial $0, Databricks is the only real spend

### Verified
- `git check-ignore -v` on four paths — `CLAUDE.md` ignored ✓, `data/raw/*.csv` ignored ✓, `data/.gitkeep` reachable ✓, `tests/fixtures/*.csv` reachable ✓
- All five `docs/` files present with correct headers
- `decisions.md` contains 11 entries, every one naming its rejected alternative
- 36 files staged-visible to git; `CLAUDE.md` correctly absent from that set
- Stub files exist at every path in the planned tree
- **No pipeline code written** — this was the intent of Session 0

### Incidents
- `!data/.gitkeep` negation was silently unreachable under a `data/` exclusion — git does not descend into an excluded directory. Fixed with `data/*`. Full entry in `incidents.md`; prevention rule now covers all future negations.

### Learning check
Three architecture questions raised and **deferred, not skipped** — see the
deferred bank in `learning.md`. Session 0 built scaffolding (out of scope for
the check), and the decisions made were not yet implemented, so the questions
would have tested absorption of a conversation rather than retention of a
pattern. Bound to Sessions 9 and 17, where they can be answered from having
built the thing.

### Not done (deliberately)
- No cloud accounts created yet (Databricks / Confluent / Snowflake) — Session 2
- No Olist data downloaded — Session 1
- CI workflow files are empty stubs — filled when there is something to lint and test
- No commit made — read-only git; commit is yours

### Next
**Session 1 — Olist acquisition + three source generators**
- Download Olist to `data/raw/`
- `generators/order_events.py` — Kafka producer, synthetic event-time clock, Avro + Schema Registry
- `generators/dim_dumps.py` — nightly full CSV → S3, controlled clock, `dim_updated_at`, supports row deletion
- `generators/inventory_cdc.py` — stock per `(product_id, seller_id)`, I/U/D + monotonic `seq`, decrements driven by real `order_items`
- Unit tests for all three
- ⚠️ Expect the **`customer_id` vs `customer_unique_id`** gotcha: Olist `customer_id` is per-order (~99k rows), `customer_unique_id` is the actual person (~96k). SCD2 must key on the latter or every order looks like a new customer. Log it in `incidents.md` when it bites.
- Ends with the first learning check → `learning.md`

---

## Session 1 — Olist contract + three source generators
Date: 2026-09-19

### Built
- `generators/olist.py` — the **schema contract**. Nine tables, contracted columns, expected row counts, date/dtype coercion. Every reader goes through `load()`; nothing else names a path or calls `read_csv`.
- `generators/_common.py` — shared plumbing: `ReplayClock` (business time vs wall clock), `MessageSink`/`BlobSink` protocols with offline twins (`JsonlSink`, `MemorySink`, `LocalBlobSink`) and cloud implementations (`KafkaAvroSink`, `S3BlobSink`), `deterministic_id`, the five-field envelope.
- `generators/order_events.py` — order lifecycle fan-out, globally sorted by event time, keyed by `order_id`, items on `created` only. Avro schema included.
- `generators/dim_dumps.py` — nightly full snapshots to `dims/<dim>/dump_date=YYYY-MM-DD/`, keyed on **`customer_unique_id`**, with `dim_updated_at` carried forward on unchanged rows and deterministic deletion.
- `generators/inventory_cdc.py` — Debezium-style I/U/D op log, decrements driven by real `order_items`, `seq` derived from event time, optional out-of-order emission for Session 10.
- `scripts/fetch_olist.py` — Kaggle download + contract validation. Never handles a credential itself; reads the token the human creates.
- `pyproject.toml` — explicit ruff rule set and pytest config, so a ruff upgrade changing its defaults cannot silently turn CI red.
- `.github/workflows/ci.yml` — ruff + pytest + an Avro-parse step.
- `.env.example`, `tests/conftest.py` (a 7-order miniature Olist), six test modules.

### Verified
- **84 tests pass, `ruff check .` clean.** Suite runs in ~9s.
- Three generator CLIs run end to end against fixture data, as subprocesses, from `tests/test_cli_smoke.py` — not just as library calls.
- **Cross-source reconciliation ties out: 9 units sold = 9 units rebuilt from CDC deltas alone.** `units_sold_from_cdc` reads only before/after images — no `change_reason`, no join back to orders — so it is a genuine second derivation.
- The reconciliation still ties out with emission order disturbed 40%, and with restocks on or off.
- `dim_updated_at` carry-forward confirmed on the case that matters: CU_STAYER orders again from the *same* city on a later date, and the timestamp does **not** move. Regenerating the same nights in a fresh directory produces byte-identical timestamps, proving it is not the run clock.
- Customer dimension collapses 7 order-level `customer_id` rows to 4 people, and `customer_id` does not leak into the dimension.
- Event IDs and `seq` values are byte-identical across two generator runs — replay determinism, which the exactly-once experiment depends on.
- **Generators import with `confluent_kafka` and `boto3` blocked**, confirming CI's minimal dependency list is safe (`test_generators_import_without_any_cloud_sdk_installed`).
- Both Avro schemas parse and carry the five shared envelope fields.

### Built but NOT verified — carry to Session 2/3
- `KafkaAvroSink` — written, never touched a broker. No Confluent account exists yet.
- `S3BlobSink` — written, never touched a bucket. Not even tested with `moto` yet.
- `scripts/fetch_olist.py` **download path** — only `--check-only` is exercised. The HTTP call, the 401/403 handling and the unzip are unrun.
- `.github/workflows/ci.yml` — never executed. There is no git remote, so this has not run once.
- All expected row counts in `generators/olist.py` are **unconfirmed against the real download.**

### Not done
- **Olist is not downloaded.** Acquisition method was decided (Kaggle API token) and the script is written, but no `kaggle.json` exists yet. Everything above was built and tested against `tests/conftest.py`, a 7-order miniature Olist built to contain the project's gotchas.
- `requirements-dev.txt` full-set resolution is **unknown** — a local dry-run timed out on download, so whether pyspark + delta-spark + databricks-connect + three dbt adapters coexist is untested. It bites in Session 4, not now.

### Incidents
Two, both found by the first end-to-end CLI run and both invisible to the library-level tests that preceded it:
1. **Dimension dumps drained to zero rows.** The deletion pool was sampled from *tonight's* rows rather than the fixed key universe, so the "cumulative" set recomputed differently every night. Fixed with a stable universe plus a 25% cap.
2. **Generator reported changes its own output did not contain.** Changes were applied before deletions, so an edited row could be deleted from the same dump. Fixed by filtering first, then transforming.

Full entries with prevention rules in `incidents.md`. The direct consequence: `tests/test_cli_smoke.py` now exists, because the CLI path was the only place these were visible.

### Correction to Session 0
Session 0's entry says `decisions.md` contains **11** seeded entries. It contains **12** — the miscount is recorded here rather than edited above, per the append-only rule.

### Learning check
Posed at the end of the session — 5 questions across write-from-memory, explain-the-why and predict-the-failure. Results in `learning.md`.

### Next (superseded in part — see addendum below)
**Session 2 — cloud accounts, cost guards, and first real data**
- ~~Run `python scripts/fetch_olist.py` once `kaggle.json` is in place~~ — done same-session, see addendum
- ~~Re-run all three generators against the real 99k-order dataset~~ — done same-session, see addendum
- AWS account + S3 bucket + **budget alarm before any compute runs**
- Databricks Premium workspace, Unity Catalog, 10-minute auto-terminate on every cluster — non-negotiable
- Confluent Cloud Basic cluster + Schema Registry; create topics `orders` and `inventory.cdc`
- First real exercise of `KafkaAvroSink` and `S3BlobSink` — still built blind, still Session 2
- Push to a remote so CI actually runs for the first time

---

### Addendum — real Olist download, same session
Date: 2026-09-19

The Kaggle token step happened sooner than planned, so the real-data work
originally slated for Session 2 was pulled forward and done here instead.

**What changed:**
- Account only exposed Kaggle's newer personal-token flow (a bare `KGAT_...`
  string, not classic `kaggle.json`). `scripts/fetch_olist.py` now accepts
  either shape — see `decisions.md`.
- `python scripts/fetch_olist.py` **succeeded against the real dataset.**
  8 of 9 tables match the contract's row counts exactly. `reviews` differs
  (104,719 actual vs 99,224 expected, +5,495) — Kaggle has re-uploaded this
  dataset before; recorded as drift, not a bug.
- Running all three generators against the real 99,441-order dataset surfaced
  **two real bugs that all 84 fixture-driven tests had passed straight
  through:**
  1. `inventory_cdc.py` crashed outright — 34,448 real SKUs all seeded at one
     shared timestamp collided past `assign_seq`'s 1000-per-microsecond
     tiebreaker. Fixed: each SKU now seeds before *its own* first sale.
  2. `dim_dumps.py` silently reported ~32,940 of ~32,941 products as
     "changed" every single night, when `--change-rate 25` should touch
     about 25. Root cause: `pd.read_csv` upcasts an entire integer column to
     float64 the moment any row in it is null, and real `product_weight_g`
     has scattered nulls — `str(225) != str(225.0)`, so the hash saw a
     change that never happened. Fixed: numeric values are normalized
     before hashing.

**Both are logged as full incidents with prevention rules in
`docs/incidents.md`, and both are now regression-tested** — including with
data shapes the fixture doesn't naturally produce (1,200 synthetic SKUs to
force a seq collision; a null placed in a numeric column to force the dtype
upcast). **87 tests pass, ruff clean.**

**Re-verified against real data after both fixes:**
- `order_events.py`: 99,441 orders → 394,090 events, 1,234 synthetic
  terminal events, 775 orders with no items, 1,382 non-monotonic lifecycles
  (genuine Olist data-quality facts, not generator bugs)
- `inventory_cdc.py`: 34,448 SKUs, 158,396 events — **reconciliation ties out
  exactly: 112,650 units sold = 112,650 units rebuilt from CDC deltas alone**
- `dim_dumps.py`: 6 nights over the full dataset, product/seller `changed`
  counts now track `--change-rate` correctly (~25–50/night, not ~32,940)

**Not done:**
- `KafkaAvroSink` / `S3BlobSink` still untouched by a real broker or bucket
- `requirements-dev.txt` full-set resolution still unknown
- CI still never executed (no remote yet)

**Lesson carried to `learning.md`:** two real bugs, both invisible through a
full green test suite, both because the fixture's data shape (no numeric
nulls, only 4 SKUs) couldn't produce the failure. Testing against real data
at real cardinality is not optional polish — it is the only way some bugs are
reachable at all.

### Next
**Session 2 — cloud accounts and cost guards**
- AWS account + S3 bucket + **budget alarm before any compute runs**
- Databricks Premium workspace, Unity Catalog, 10-minute auto-terminate on every cluster — non-negotiable
- Confluent Cloud Basic cluster + Schema Registry; create topics `orders` and `inventory.cdc`
- First real exercise of `KafkaAvroSink` and `S3BlobSink`
- Push to a remote so CI actually runs for the first time
- Record `reviews` row-count drift (104,719 vs 99,224) as a standing fact if it persists after a future re-download

---

## Session 2 - offline verification of the two cloud sinks
Date: 2026-09-19

**In plain words:** the generators write their output through a *sink* - just
"the place output goes". Each sink exists twice: a real one that talks to the
cloud, and a local stand-in that writes the same files to a folder so
everything can be tested with no account and no cost. The real ones, written in
Session 1, had **never run a single line.**

This session ran them for the first time - not against AWS or Kafka, but
against fakes that behave like them. The very first run found a bug that had
been there since Session 1 and that every one of the 87 existing tests had
passed straight through: **the local stand-in and the real S3 sink were writing
different files from identical input.** The local files had a blank line
between every row. Nothing caught it because the only code that ever read those
files back silently ignores blank lines.

Then, testing the *new* tests by deliberately breaking things, one break slipped
through - proving a test that looked like a safety net wasn't one. Fixed too.

Nothing was deployed, nothing cost money, and two never-executed pieces of code
are now a good deal less frightening.

### Scope change, decided at the start
`progress.md` scoped Session 2 as cloud accounts + cost guards. Account
creation, credential entry and billing configuration are not things Claude
does, so the session was re-scoped **by explicit choice: skip cloud entirely,
offline verification only.** All AWS / Databricks / Confluent / remote-push
work moves to Session 3.

The substitute goal: take the two pieces of Session 1 code that had *never
executed a single line* - `S3BlobSink` and the Avro schemas - and verify them
against fakes, before the accounts exist rather than after.

### Built
- `tests/test_s3_blob_sink.py` - 9 tests running `S3BlobSink` against `moto`.
  Covers the prefix-strip contract `read_previous` depends on, the 1000-key
  `ListObjectsV2` pagination cap, neighbouring-prefix leakage, the empty-prefix
  branch, and byte-for-byte equality between `LocalBlobSink` and `S3BlobSink`.
- `tests/test_avro_contract.py` - 18 tests putting **real generator output**
  through both Avro schemas with `fastavro`. Undeclared-field detection,
  missing-required-field detection, null-union round trips, integer-width
  checking, and the `_kafka_config_from_env` failure path.
- `.gitattributes` - pins `eol=lf` repo-wide and explicitly on `*.csv`,
  `*.jsonl`, `*.json`, `*.avsc`.
- `.github/workflows/ci.yml` - install list widened to `moto[s3]`, `boto3`,
  `fastavro`, plus a new step that **fails the build if those imports are
  missing**, and `--strict-markers` on pytest.
- `requirements-dev.txt` - `fastavro` pinned explicitly.

### Fixed
- `LocalBlobSink.put_text` / `get_text` - now byte-mode, not text-mode.
- `JsonlSink` - opens with `newline=""`.
- `dim_dumps.write_night` - `to_csv(lineterminator="\n")`, so a dump's bytes no
  longer depend on the OS that produced it.

### Verified
- **114 tests pass, `ruff check .` clean.** Suite ~31s, up from ~9s; ~5s of the
  increase is the deliberate 1005-object pagination test.
- `LocalBlobSink` and `S3BlobSink` produce **byte-identical content** for every
  key across a 3-night generator run - the substitutability claim in
  `decisions.md`, which was prose until now and was **false**.
- `dim_updated_at` carry-forward survives the S3 round trip, and the row that
  genuinely changes (`cu_mover`, sao paulo -> rio de janeiro) still moves. Both
  directions asserted, so the test cannot pass against a generator that froze
  every timestamp.
- Dump files written against the real 99,441-order dataset contain **zero CR
  bytes and zero blank lines**, checked at the byte level, not through pandas.
- Both Avro schemas accept every record both generators emit, including null
  unions (`items`, `prev_stock_qty`, `estimated_delivery_date`).
- The Avro suite was **mutation-tested**: `seq: long -> int`,
  `prev_stock_qty: ["null","int"] -> "int"`, and deleting `ts_is_synthetic`
  from the orders schema. Mutations 2 and 3 were caught; **mutation 1 was not**,
  which exposed a real gap in the new suite - see below.
- The `skipif` guards are genuinely function-level: with `moto`, `boto3` and
  `fastavro` simulated absent, the result is **3 passed, 24 skipped, 0 errors**
  - modules import, nothing collapses. This is also the evidence for why the
  new CI assertion step exists.
- `.gitattributes` produces no phantom diff; `git check-attr` confirms `eol=lf`
  resolves on source, docs and CSV alike.

### Incidents
One, and it was found by the very first execution of `S3BlobSink`:
**the two blob sinks stored different bytes for the same input, on Windows
only.** `pandas.to_csv()` emits `os.linesep`; `Path.write_text` then translates
again, producing `\r\r\n` on disk and `\n\n` on read-back. Every dump
`LocalBlobSink` ever wrote on Windows had a stray CR and a blank line between
rows. Invisible to all 87 previous tests because `pd.read_csv` defaults to
`skip_blank_lines=True` - the only reader that ever looked simply tolerated it.
Platform-conditional, so it would have been green in CI and red locally.

Full entry with prevention rule in `incidents.md`, including a postscript: the
first attempt to *write* that entry reproduced the identical bug one layer up,
converting all of `incidents.md` from LF to CRLF and turning a 27-line append
into a 165-line whole-file diff.

### The mutation that escaped, and what it cost to find
`test_seq_needs_the_full_64_bit_range` was written to protect `seq`'s `long`
declaration. Mutating the schema to `int` left **all 16 tests green**. Avro
encodes `int` and `long` with the identical zig-zag varint and `fastavro` does
not range-check against the declared type on write, so a 1.5e18 value
round-trips intact. The test was checking Python values, not the schema.

This matters beyond the test: Schema Registry enforces the declared type for
compatibility, and Spark's `from_avro` maps Avro `int` to `IntegerType` - so
the truncation `fastavro` declined to perform would have happened in Bronze,
in Session 3, on a cluster. Replaced with
`test_no_integer_field_is_declared_narrower_than_its_values`, which reads the
schema; all three mutations are now caught.

### Built but NOT verified - carry to Session 3
- **`KafkaAvroSink.send` has still never executed.** `confluent_kafka` 2.6.1
  ships no mock Schema Registry client, so the serializer, the wire framing and
  the producer config genuinely need a live cluster. What is now verified is the
  schema-vs-payload contract, which is the part that would have broken.
- **CI has still never run.** No git remote exists. `ci.yml` is written and
  parses as valid YAML locally; it is otherwise unexecuted, and the new
  optional-dependency assertion is unexercised.
- `requirements-dev.txt` full-set resolution is **still unknown** - the Session 1
  dry-run timeout was never retried. `moto[s3]` was installed standalone and
  upgraded `cffi` 1.17.1 -> 2.1.1; `confluent_kafka` still imports and the suite
  stayed green, but the full pyspark + delta-spark + three-dbt-adapter set
  remains unresolved.

### Not done
- No cloud accounts, no S3 bucket, no budget alarm, no Databricks workspace, no
  Confluent cluster, no topics, no git remote. All deliberate - see scope above.
- The `reviews` row-count drift (104,719 actual vs 99,224 contracted, +5,495)
  was **not** re-examined this session; it stands as recorded in Session 1 and
  still wants a decision about whether to update the contract.

### Learning check
Four questions asked, results logged in `learning.md`: **1 partial, 3 missed.**
A fifth was drafted and pulled before asking - it duplicated D2, already bound
to Session 9, and Silver does not exist yet.

The partial is question 1, and the half that landed is the harder one: Kafka's
exactly-once guarantee stops at the consumer boundary, so Bronze->Silver must
own its own idempotency. The half missed was *why* the event ID is a hash -
answered as "more unique", where the actual property is reproducibility.

All four carry to Session 3, including question 1 - the standing rule is that a
concept stays on the re-test list until answered correctly twice.

### Background sheet added
Prior work experience was supplied at the end of this session. `docs/background.md`
condenses it into a reference sheet: the patterns already worked with at Jio, plus
an explicit split between "already shipped at work, so aim the session at the
failure modes" and "no prior exposure, build from the ground up".

It is reference, not a checklist, and explicitly **not a discount.** `CLAUDE.md`
records the rule that matters: the background sheet **adds** a failure-mode
question on top of the fundamental one, and is never grounds for skipping or
softening a topic. Prior exposure means a pattern gets hammered harder, not
waved through.

Session 2 is the evidence. Three of four check questions landed on patterns that
sheet lists as already shipped at work, and came back partial or blank. **Prior
exposure predicted nothing about recall**, which is precisely why no leniency
clause exists anywhere in the check.

**Scope correction, same session:** the first version of this was a
`docs/resume_map.md` that tiered every resume line by verb strength and tracked
"defensibility" per claim. That was over-built for what was asked and aimed at
interview prep rather than at the experiments the project is for. Replaced with
the reference sheet, and `CLAUDE.md`'s learning-check section reverted to
category-driven selection. Recorded here rather than silently dropped.

### Next
**Session 3 - cloud accounts, cost guards, and the first live produce**
- AWS account + S3 bucket + **budget alarm before any compute runs**
- Databricks Premium workspace, Unity Catalog, 10-minute auto-terminate on
  every cluster - non-negotiable
- Confluent Cloud Basic cluster + Schema Registry; topics `orders` and
  `inventory.cdc`
- **First execution of `KafkaAvroSink.send`** against a live cluster - the last
  wholly-unrun piece of Session 1
- `S3BlobSink` against a real bucket; it should be uneventful, which is the
  point of having done Session 2
- Push to a remote so CI runs for the first time, and confirm the new
  optional-dependency step actually fails when it should
- Decide the `reviews` drift: update the contract or record it as standing
- **Open the check with the carried-forward items**, highest first: exactly-once
  Bronze ingest - what `txnAppId`/`txnVersion` actually deduplicate, and why that
  is not dedup on `event_id`. Then the Avro declared-type question, which is the
  most self-contained of the four.
