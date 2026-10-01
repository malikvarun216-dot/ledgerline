# Progress

Chronological session log. Updated at the end of every session with what was
**actually verified** — not merely what was built.

## Deliberate-failure experiments — tracker (read at every session start)

Added Session 8, after `exp_02` went unscheduled for two sessions (incidents.md
2026-09-30). Each experiment is due in the session that builds its layer. If a
session ends without it, `### Next` carries it **by name**. Every drill checks
its attack list against this table. Status is updated in the session that
changes it.

| # | Experiment | Layer / pattern | Due | Status |
|---|---|---|---|---|
| exp_01 | exactly-once replay (`txnAppId` / `txnVersion`) | Bronze Kafka | S7 | **done** S7; re-run Drill 1 |
| exp_02 | partition overwrite: append doubles, predicate-less overwrite wipes | Bronze dims (`replaceWhere`) | **S6 — missed**; S8 | **done** S8 (two sessions late) |
| exp_03 | MERGE gap: a deleted row survives a plain MERGE | Silver dims | S8 | **done** S8; G rewritten + H added S9 (hold the bad row) |
| exp_04 | CDC correctness: dedup, `seq` guard, deliberate contamination; + late update after a delete (S9) | Silver CDC (built S9) | **S10** | not started — layer exists, due next session |
| exp_05 | SCD2 point-in-time: run time vs business time | Gold dims (dbt snapshot) | the session that builds the snapshot (Gold, S13+) | not started |
| exp_06 | late arrival dropped by a high-water mark; lookback fixes it | Gold facts (dbt incremental) | S17 (deferred D3) | not started |

**The rest of the plan is bound the same way** — since Session 9, every
Databricks / Snowflake / dbt topic has a row, a weight and a session in
[`docs/coverage.md`](coverage.md): built, or taught as theory at a named
session. Read its rows for the session at every start (CLAUDE.md item 6).

> **On references to `docs/learning.md` below.** That file is the
> end-of-session recall check — answers, wrong answers and carried-forward weak
> spots. It is **deliberately local-only and not in this repository**: it is one
> person's working notes, not part of the project's record. Entries below cite
> it because it exists on the author's machine and is written every session
> exactly as the discipline requires. The citations are left as written rather
> than scrubbed, per the append-only rule — a reader should be able to see that
> the check happened, even though its contents are not published.

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

---

## Session 3 — AWS cost guards, a scoped credential, and the first CI run
Date: 2026-09-20

**In plain words:** this session started as a cost emergency in the *other*
project and turned into the AWS half of Session 3. A `t3.micro` called
`jobpulse-dashboard-dev` had been running continuously since 26 April — 146
days, roughly **$70** — serving a dashboard nobody was looking at. It was
created by hand, so it was not in jobpulse's terraform, so `terraform destroy`
would never have touched it and nothing in that repo recorded its existence.
It was 75% of the AWS bill.

Two guards had already fired and neither worked. A `$1` zero-spend budget had
been emailing since April; the alerts were read and consciously set aside during
a busy stretch. A CloudWatch alarm watched the nightly pipeline fail five nights
running and reached nobody who acted. The lesson is not "use a better address" —
it is that **a guard requiring human attention is not a guard.** What was
missing on that instance was structural: no auto-terminate, because it was
launched outside the tooling that would have enforced one.

Once the AWS account was open anyway, the Session 3 plan stopped being blocked:
the account already existed, in `ap-south-1`. Budgets, a least-privilege
credential and the landing bucket all landed. And the `reviews` "dataset drift"
that had been carried as an open question since Session 1 turned out never to
have existed.

### Scope, as it actually ran
Planned as cloud accounts + cost guards + first live Kafka produce. What
happened: the jobpulse cost work came first at the human's request, which
surfaced that **AWS already existed** — so the AWS half ran in full. Databricks
and Confluent still require signups that are not Claude's to do, so the live
produce moves to Session 4 again.

The learning check ran **after** the build rather than before it, at the human's
explicit request. Recorded in `learning.md` as an ordering deviation, not a skip
— every carried item was asked and graded.

### Built
- **Two budgets.** `ledgerline-monthly` ($20; 50/80/100% actual plus 100%
  forecasted) and `ledgerline-daily-spike` ($2/day actual). Both set
  `IncludeCredit: false`.
- **`ledgerline-dev` IAM user** plus customer-managed policy `ledgerline-dev-s3`
  — two statements, because `s3:ListBucket` is bucket-level (bare ARN) and
  `GetObject`/`PutObject`/`DeleteObject` are object-level (`/*`).
- **`ledgerline-landing-dev-fffc8b65`** in `ap-south-1`: all four public-access
  blocks on, SSE-S3 with bucket keys, and a lifecycle rule aborting incomplete
  multipart uploads after 7 days (they bill as storage but never appear in
  `ListObjects`).
- **`.env`** (gitignored) holding the scoped key; the secret was written
  straight to disk and never printed to the session transcript.
- **`.env.example`** — region corrected `us-east-1` → `ap-south-1`, and
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` added with a warning that leaving
  them blank silently falls back to this machine's **admin** key.
- **`.github/workflows/deploy.yml`** was 0 bytes. GitHub rejects an empty
  workflow with "No event triggers defined in `on`" and renders it as a failed
  run, which would have made the first-ever CI result unreadable. Replaced with
  a `workflow_dispatch`-only stub.
- **`ci.yml` pinned** to `ubuntu-24.04`, `actions/checkout@v5`,
  `actions/setup-python@v6`.

### Verified
- **First-ever CI run: SUCCESS, 34s**, `lint-and-test` green. This closes a
  carry-forward open since Session 1 — `ci.yml` had never executed once.
- **114 tests pass, `ruff check .` clean** locally, unchanged from Session 2.
- **The scoped credential was proven, not asserted.** As `ledgerline-dev`:
  PUT/LIST/DELETE succeed in its own bucket; `s3://jobpulse-bronze-dev/`,
  `ec2:DescribeInstances`, `s3:ListAllMyBuckets` and `iam:ListUsers` are all
  **denied**. There is no `Deny` statement in the policy — all four are blocked
  by implicit deny.
- **The two budgets disagree by design, and that is the proof.** Read at the
  same instant, `My Zero-Spend Budget` reports **$0.00** and `ledgerline-monthly`
  reports **$11.65**. The only difference is `IncludeCredit`.
- **jobpulse spend fell from $0.51/day to $0.06/day**, visible in Cost Explorer
  daily granularity: Sep 14–17 steady at $0.509, Sep 19 $0.060 after the
  teardown. ~$186/yr → ~$22/yr.
- **Nothing sensitive reached GitHub.** Audited *before* pushing and confirmed
  after: 54 files on the remote, `.env` / `CLAUDE.md` / `profiles.yml` all
  absent, zero raw Olist CSVs (~120MB stayed local), and **zero `AKIA` matches
  across the entire commit history** — not just the working tree, because a
  `.gitignore` added after a commit removes nothing.
- **The `reviews` drift was disproved arithmetically**, not argued:
  99,224 parsed records + 5,495 newlines embedded in quoted fields = 104,719
  physical lines, to the row. 3,852 review comments contain a newline.

### Built but NOT verified — carry to Session 4
- **The CI pin itself is unverified.** `ubuntu-24.04`, `checkout@v5` and
  `setup-python@v6` were written after the green run and have not been through
  CI. If a major version does not resolve, the next push fails — deliberately
  visible rather than silent.
- **`KafkaAvroSink.send` has still never executed.** Third session running. No
  Confluent account exists; `confluent_kafka` 2.6.1 ships no mock Schema
  Registry, so this genuinely needs a live cluster.
- **`S3BlobSink` has still not touched the real bucket.** The bucket and the
  credential now exist and the credential is proven, but the generator has not
  been pointed at it.
- **`requirements-dev.txt` full-set resolution remains unknown.** CI installs an
  explicit list, not that file, so a green CI says nothing about whether pyspark
  + delta-spark + three dbt adapters coexist.

### Not done
- No Databricks workspace, no Confluent cluster, no topics. Signups are not
  Claude's to perform.
- **AWS Budget Actions** (auto-apply a deny policy at a threshold) considered
  and deliberately deferred — it is the only guard here needing no human, but
  blunt enough to kill a session mid-run. Recorded in `decisions.md`.
- The `>=` floors vs `==` pins question in CI's install list was **raised and
  deliberately left open**, not silently decided.

### Incidents
One, and it had been sitting in these docs for three sessions:
**the `reviews` "dataset drift" was a line count, not a row count.** Session 1
recorded 104,719 actual vs 99,224 expected and attributed it to Kaggle
re-uploading the dataset. The contract was right all along. What made it durable
is that the explanation was *plausible* — Kaggle really does re-upload datasets
— so it was filed as an external fact rather than as a claim about our own
measurement, and external facts do not get re-examined. Full entry with
prevention rule in `incidents.md`, including the live consequence for Session 4:
Spark and Auto Loader default to `multiLine=false` and will shred exactly these
3,852 records **without raising an error**.

### Decisions
Five appended to `decisions.md` (now 34 entries): the `ap-south-1` region and
why cross-region beats marginal storage price; the credit-excluding tiered
budget and its correction about *why* the old alarm failed; the scoped IAM user
including the honest note that a long-lived key is a knowing downgrade from
Identity Center; the `reviews` contract standing at 99,224; and the CI runner
pin.

### Learning check
Nine questions — five carried from Session 2, one from this session's bug, three
SAA-relevant ones appended at the human's request and all tied to work actually
done today. **1 correct, 4 partial, 3 missed, 1 deferred.**

Session 2 was 1 partial and 3 missed, so four concepts moved up a grade.
**Q4 (`seq` across restarts) was unattempted in Session 2 and fully correct
here** — 1 of the 2 correct answers needed to retire it.

One question was **deferred as the asker's error**: Q6 tested Auto Loader, which
has not been built. Logged as D4 in the deferred bank, bound to the Bronze
dimension-ingest session. Full results and corrections in `learning.md`.

### Next
**Session 4 — Confluent, Databricks, and the first live produce**
- Confluent Cloud Basic cluster + Schema Registry; topics `orders` and
  `inventory.cdc`. **First execution of `KafkaAvroSink.send`** — unrun since
  Session 1 and now three sessions overdue.
- Point `S3BlobSink` at `ledgerline-landing-dev-fffc8b65` using the scoped
  credential rather than the ambient admin key. Should be uneventful; that is
  the return on Session 2's offline work.
- Databricks Premium workspace in **`ap-south-1`** — must match the bucket, or
  every Bronze read pays cross-region egress. **10-minute auto-terminate on
  every cluster**, which is the structural guard the $70 instance did not have.
- Confirm the CI pin survives a push before building on it.
- **Open the check with the carried weak spots**, highest first: exactly-once
  Bronze and the batch-vs-row *unit* (missed twice now), then `IncludeCredit`,
  then the S3 bucket-vs-object ARN. Re-ask only the FORWARD third of the Avro
  question. `seq` across restarts needs one more correct answer to retire.

## Session 4 — Confluent live, the first produce, and a credential that was never used
Date: 2026-09-20

**In plain words:** the Kafka code written in Session 1 ran for the first time
today, and both topics now hold real events that were read back and counted.
Getting there turned up two things that had been quietly wrong for sessions.
The scoped AWS user created in Session 3 — tested, proved, documented — **was
not being used by anything**, because no code ever read `.env`; the generator
was authenticating as the machine's admin key. And the first produce hung in
total silence for three minutes because the bootstrap address carried the REST
port instead of the Kafka one, and nothing in the send path had a timeout.

### Scope, as it actually ran
Planned as Confluent + Databricks + the first live produce. Confluent and the
produce landed in full. **Databricks was deliberately not created** — checking
its setup first revealed a standing cost the project's guards do not cover
(below). Snowflake stays untouched by design; its 30-day calendar should not
start before Gold.

The learning check moved to the end of the session as a **standing convention**
from this session onward, at the human's instruction — recorded in `CLAUDE.md`
and `learning.md` as an order change, not a reduction.

### Built
- **Confluent Cloud, live**: environment `default`, **Basic** cluster `cluster_0`
  (`lkc-2255zy2`), AWS `ap-south-1`, topics `orders` and `inventory.cdc` at 3
  partitions each. Stream Governance Essentials for Schema Registry.
- **Service account `serviceacc_ledgerline`** (`sa-5w01oxq`) with two separately
  scoped API keys — one for the cluster, one for Schema Registry — and role
  grants `CloudClusterAdmin` on `cluster_0` plus `DeveloperWrite` on all schema
  subjects.
- **`load_local_env()`** in `generators/_common.py`, called from all three
  generators' `main()` and deliberately not from library functions.
- **`_s3_client_from_env()`** — `S3BlobSink` now builds its client from the
  scoped `.env` credential and **raises** when it is absent, instead of letting
  boto3 walk its default chain to an admin key. Region passed explicitly.
- **`KafkaAvroSink.flush(timeout=60.0)`** — checks the undelivered count that
  `Producer.flush(timeout)` returns and raises naming it, instead of blocking
  forever.
- Two new tests: the S3 admin-fallback refusal, and that an already-set
  environment variable beats the `.env` file.

### Verified
- **`KafkaAvroSink.send` executed for the first time** — unrun since Session 1
  and three sessions overdue. 50 orders produced **187 events** to `orders`.
- **Read back independently, not trusted from the generator's own summary.** A
  separate consumer deserialized through Schema Registry: **187 messages, 187
  distinct `event_id`** — no duplicates. Breakdown `created 50 · approved 50 ·
  shipped 41 · delivered 39 · canceled 7`, summing to 187 against 50 orders.
  Partitions 50/62/75.
- **The key is the entity, as decided in Session 1** — sample message key
  `2e7a8482f6fb09756ca50c10d7bfc047` equals its own `order_id`. For
  `inventory.cdc` the key is `product_id|seller_id`.
- **Business time and processing time are visibly separate in real data**:
  `event_ts` 2016-09-04T21:15:19Z against `produced_at` 2026-09-20T07:34:59Z.
- **`inventory.cdc`: 93 events, 93 distinct `event_id`**, from 43 SKUs — 43
  seeds (`I`), 49 sales and 1 restock (`U`). The cross-source invariant already
  ties out at the generator: **units sold per `order_items` = 52, rebuilt from
  CDC deltas = 52.**
- **`S3BlobSink` touched the real bucket for the first time** — 3 nights of
  dimension dumps to `s3://ledgerline-landing-dev-fffc8b65/ledgerline/dims/`,
  **9 objects**, Hive-style `dump_date=YYYY-MM-DD` partitions, in 12 seconds.
- **The identity was proved, not assumed.** `sts:GetCallerIdentity` through the
  same constructor the generator uses returns
  `arn:aws:iam::240939827246:user/ledgerline-dev`. This is the check Session 3
  did not make — it verified the *policy* by acting as that user, which said
  nothing about which credential the code picks up.
- **The Session 1 dtype bug stays fixed against real S3 read-back**: `changed`
  counts of 25 and 50, not 32,940.
- **The Session 2 line-ending pin holds against real S3**: a dump read back from
  the bucket contains **0 CRLF** bytes.
- **116 tests pass, `ruff check .` clean**, after all changes.

### Built but NOT verified — carry to Session 5
- **The CI pin is still unverified.** `ubuntu-24.04`, `checkout@v5`,
  `setup-python@v6` have still not been through a run; nothing has been pushed
  since Session 3. Now four doc files and four source files are uncommitted too.
- **`KafkaAvroSink.flush`'s new timeout path has never fired.** The success path
  ran today; the raise did not. It is `# pragma: no cover` for the same reason
  as before — it needs a broker that fails.
- **Only 50 orders were produced, not 99,441.** The full fan-out (394,090 order
  events, 158,396 CDC events) has not been run, so nothing is known about
  throughput, partition skew at scale, or Basic's eCKU behaviour under load.
- **`requirements-dev.txt` full-set resolution remains unknown** — unchanged
  from Session 3.

### Not done
- **No Databricks workspace**, deliberately. See the decision entry: workspace
  creation provisions a **NAT gateway** into the AWS account at ~$33–41/month
  running 24/7, which is 1.5–2× the entire project budget for an idle resource
  that **no auto-terminate covers**. Same structural shape as the $70 instance
  from Session 3. Deferred until Bronze work actually needs it.
- **No Snowflake account** — the 30-day calendar binds, and Gold is Session 13.
- Per-topic ACLs. `CloudClusterAdmin` is a knowing downgrade, recorded as such.

### Incidents
Two, both found before they could do damage, and both about code that had never
executed:
1. **`.env` was never loaded, so the S3 sink ran as the admin key it was built
   to avoid.** `python-dotenv` has been in `requirements.txt` since Session 0 and
   `load_dotenv()` was never called. Invisible because Session 3 verified the
   credential rather than the code path, and because all S3 tests inject a
   `moto` client through `client=` — so the one line that chooses an identity is
   the one line no test reached.
2. **The first produce hung 180s in silence on port 443.** That is Confluent's
   REST endpoint, so the client read `HTTP/1.1...` as a Kafka frame and reported
   "Invalid response size 1213486160" — a number which *is* the ASCII bytes
   `HTTP` — and advised raising `receive.message.max.bytes`. Confidently wrong
   advice. Compounded by `flush()` having no timeout, which turned a
   misconfiguration into a hang rather than an error.

### Decisions
Five appended to `decisions.md` (now 39 entries): `.env` loaded at entry points
only and the S3 admin fallback deleted; Confluent tier fixed at creation, with
the non-obvious consequence that Basic excludes topic-level RBAC;
`CloudClusterAdmin` over per-topic ACLs as a knowing downgrade; and Databricks
deferred on idle cost with AWS Marketplace billing decided in advance so the
Session 3 budgets can actually see DBU spend.

Five rows added to `CLAUDE.md`'s *Verified platform constraints* table.

### Addendum — the topic now holds deliberate duplicates
Written after the entry above. Verifying the `client.id` change meant producing
5 more orders, which were **already among the first 50**. Because `event_id` is
a hash of the natural key rather than a `uuid4`, those events are byte-identical
replays, not new data.

The topic now reads **203 messages, 187 distinct `event_id`** - exactly 16
duplicates. That is the Session 1 deterministic-ID decision demonstrating itself
on a live topic: a replay produces the *same* events, so a dedup at Silver has
something real to remove and `exp_01` has something real to assert. With
`uuid4` the count would read 203 distinct ids and the duplication would be
undetectable from the data alone.

Recorded rather than cleaned up. The duplicates are useful, and Session 5's
full produce will replace this topic's contents anyway.

### Learning check
Six questions, one over the standing 3-5 at the human's explicit request to
widen into the session's own tooling - Kafka commits, sync vs async. **1 correct,
5 partial, none unattempted throughout.** Session 3 was 1 correct / 4 partial /
3 missed, and Session 2 was 1 partial / 3 missed, so this is the first check
where every question had a half that landed.

The headline: **the exactly-once batch-vs-row unit was answered correctly.** It
had been the top carried item for two sessions - partial in Session 2, an honest
blank in Session 3. **`seq` monotonicity across restarts retires**, correct twice
running.

The two weakest answers were both about mechanism inside a client library rather
than a data pattern: why a buffered `produce()` reports success while the real
failure surfaces later in `flush()`, and what `enable.idempotence` actually
scopes to. Both are new to the list because this is the first session that ran a
real Kafka client.

One thing recorded rather than glossed: **Q1 had to be reframed before asking**,
because answering an earlier question about consumer groups and offsets had
already supplied part of its ground. Full results, corrections and the
carry-forward list in `learning.md`.

### Correction — the CI pin WAS verified, and this entry said otherwise
Written 2026-09-20, same session, after the human showed the Actions page.

The "Built but NOT verified" list above states that the CI pin is unverified and
that **"nothing has been pushed since Session 3."** Both were false when
written.

GitHub Actions shows five runs, all on `dev`:

| Run | Commit | Title | Result |
|---|---|---|---|
| ci #5 | `6670905` | first live kafka produce, scoped s3 credential, named kafka clients | **green, 36s** |
| ci #4 | `d5384a6` | keep learning and background docs local only | green, 46s |
| ci #3 | `e235376` | keep learning and background docs local only | not green - see below |
| ci #2 | `7db33d2` | pin ci runner to ubuntu-24.04, bump deprecated actions | **green, 35s** |
| ci #1 | `3fa442c` | aws cost guards, scoped s3 credential, resolve reviews drift | green, 34s |

**ci #2 is the pin commit and it passed.** ci #5 then passed again on today's
work, running `ubuntu-24.04` + `actions/checkout@v5` + `actions/setup-python@v6`
- so the pin is verified twice over, on Linux, including the optional-dependency
assertion that proves `moto`/`boto3`/`fastavro` actually installed rather than
the S3 and Avro suites quietly skipping.

**How the error happened, because that is the useful part.** Session 3's entry
recorded the pin as unverified, which was true at the moment it was written -
the pin was added *after* that session's green run. The human then pushed, and
ci #2 ran green. Session 4 opened, read Session 3's `### Next`, and **carried the
claim forward without re-checking it.** The same shape as the `reviews` drift in
`incidents.md`: a statement that was true once, inherited rather than
re-verified, and repeated until something external contradicted it. It survived
because it cost nothing to believe and nothing in the repository disagreed -
`git log` shows commits, not whether CI ran on them.

**Prevention rule:** a carry-forward item that names an *external* system's state
- CI, a cloud console, a billing page - must be re-checked against that system at
the start of the session that carries it, not copied from the previous entry.
Local `git log` cannot answer "did CI pass", so its silence is not evidence.

**Open, not resolved here: ci #3 (`e235376`).** It is the only run that is not
green. Note the SHA mismatch against local history - ci #2's `7db33d2` has no
local counterpart, because Session 3's `git filter-repo` rewrite changed every
pre-rewrite SHA (local now holds `81a2efc` for that same commit message). The
most likely explanation is that ci #3 was **cancelled** rather than failed:
either the force-push orphaned its commit, or `ci.yml`'s own
`concurrency: cancel-in-progress: true` superseded it, since #3 and #4 share a
title and are 28 seconds apart. **Not confirmed** - `gh` is not installed on this
machine and the repository is private. If it turns out to be a genuine failure
rather than a cancellation, it earns an `incidents.md` entry about what a history
rewrite does to in-flight CI.

### Next
**Session 5 — Databricks workspace, and the full-scale produce**
- Push first, and **confirm the CI pin survives** before building on it. Eight
  files are uncommitted.
- Create the Databricks workspace, **ap-south-1**, paying via **AWS
  Marketplace** so DBUs land inside the existing budgets. Note the NAT gateway
  starts billing the moment it exists — so create it in a session that uses it.
- **Run the full produce**: 394,090 order events and 158,396 CDC events. Watch
  partition skew and eCKU behaviour; Basic's first eCKU is free.
- Tighten Confluent authorization from `CloudClusterAdmin` to per-topic
  `WRITE`/`DESCRIBE` ACLs, and revoke keys deliberately — removing a role
  binding does not delete keys created under it.
- **D4 becomes answerable** once Auto Loader reads the S3 dumps — the 3,852
  embedded-newline reviews and `multiLine=false`.

---

## Session 5 — the full produce, and a smoke test that poisoned the topic
Date: 2026-09-20

**In plain words:** both topics now hold the whole dataset — 394,090 order
events and 158,346 CDC events — produced, read back independently and counted.
The produce itself was uneventful, which was the surprise: it took 11 seconds
and the backpressure handling written for it never fired. The two findings came
from checking rather than from building. A number every document in this
project has repeated since Session 1 was wrong by fifty. And Session 4's
harmless-looking 50-order smoke run turned out to have left 43 SKUs on the CDC
topic that Silver's `seq` guard will silently resolve the wrong way.

### Scope, as it actually ran
Planned as the full produce plus the Confluent ACL tightening, with Databricks
deliberately excluded — the workspace provisions a NAT gateway billing ~$33-41
a month regardless of use, so it waits for a session that opens with Bronze.
The produce landed in full. The ACL work is **designed and recorded but not
applied**: the Confluent CLI is not installed and the change is a console
action. D4 stays deferred and needs rebinding, since it cannot be asked until
Auto Loader exists.

### Start-of-session external re-check
The Session 4 prevention rule says a carry-forward naming an external system
must be re-checked against that system, not copied. Doing that immediately
retired two items:

- **"Eight files are uncommitted" was already stale.** `git status` showed only
  `docs/progress.md` modified, and `git log origin/dev..dev` was empty — `dev`
  was level with the remote. The push the plan called for had already happened.
- **ci #3 is recorded as unconfirmed, not resolved.** `gh` is still not
  installed and the repository is private, so the cancellation hypothesis
  stands as a hypothesis. Dropped from the carry list at the human's direction
  rather than left open indefinitely. It earns an incident entry only if it is
  ever shown to be a genuine failure.

### Built
- **`KafkaAvroSink.send` survives a full local queue.** `produce()` does not
  send — it appends to a 100,000-record buffer that a background thread drains,
  and raises `BufferError` when that buffer is full. The retry polls to let the
  drain catch up, counts each wait, and is **bounded at 30 attempts** so a dead
  broker raises instead of reproducing the Session 4 silent hang.
- **`ProgressTicker`** in `_common.py`, wired into both generators above a
  25,000-event floor. Writes to stderr so stdout stays a clean summary. A
  394,090-record loop that prints nothing is indistinguishable from a hang.
- **Delivery failures are counted in full but kept as a 10-item sample**, so a
  broker outage cannot build a 394,090-string list at the moment there is least
  memory to spare. `flush()` now reports the true count.
- **`scripts/inspect_topic.py`** — the Kafka twin of `inspect_stream.py`, and
  the thing Session 4 did its verification with and then threw away. Assigns
  partitions rather than subscribing, never commits, and reads to high
  watermarks fetched up front. Reuses `inspect_stream`'s report functions, so a
  topic and a file can be compared line for line.
- **`inventory_cdc.py` refuses `--limit` with `--sink kafka`** unless
  `--allow-partial` is passed. `order_events.py` deliberately has no such
  guard; the asymmetry is measured and test-asserted.
- **Two stale `# pragma: no cover - needs a broker` markers removed** from
  `send` and `_on_delivery`. They no longer need a broker, and a pragma is a
  claim about the code that should stop being made when it stops being true.
- **125 tests pass** (117 at session start), `ruff check .` clean.

### Verified
- **The full produce landed and was read back independently.** 394,090 order
  events in 11.4s (~34,600/s); 158,346 CDC events in 3.6s (~43,900/s). The
  readback is a separate consumer deserializing through Schema Registry, not
  the generator's own summary.
- **Partition skew is a non-issue at scale, and the small-run figure was
  noise.** `orders` spreads 131,508 / 132,259 / 130,526 across three
  partitions — **worst drift 0.7% off even**, over 99,441 distinct keys.
  `inventory.cdc` is 1.6% over 34,448 keys. The same measurement on Session 4's
  50-order run read **26.1% skewed**, which was never skew at all: three
  partitions and fifty keys cannot come out even. Worth keeping as the
  concrete reminder that a distribution statistic on a small sample measures
  the sample size.
- **The arithmetic is internally consistent across two independent counts.**
  `orders` reads 394,293 records against 394,090 distinct ids — exactly the 203
  Session 4 left, replayed byte-for-byte. It ties out in the sub-counts too:
  `created` 99,496 = 99,441 orders + 55 replayed, and line-item units 112,709 =
  112,650 + 59.
- **Cross-source reconciliation ties out at full scale, for the first time
  against the whole dataset**: units sold per `order_items` = **112,650**,
  rebuilt from CDC deltas alone = **112,650**. This is a stated success
  criterion for the project.
- **The generator is deterministic, proved rather than assumed.** Two
  consecutive full CDC runs produce identical `event_id` ordering and identical
  `seq` ordering, all 158,346 ids distinct. The files' md5s differ — correctly,
  because `produced_at` is processing time — which is why the comparison is on
  the id sequence, not the bytes.
- **394,090 is confirmed** against real data: 99,441 orders, 1,234 synthetic
  terminal events, 775 orders with no items, 1,382 non-monotonic lifecycles.

### Built but NOT verified — carry to Session 6
- **The queue-full retry never fired.** `queue_full_waits` came back **0** on
  both produces: at ~35-56K records/s the 100,000-record buffer was never the
  constraint. The branch is unit-tested against a fake producer and has never
  run against a real full queue. Honest status: guarded, not exercised.
- **`KafkaAvroSink.flush`'s timeout path still has not fired** — unchanged from
  Session 4, and for the same reason. It needs a broker that fails.
- **The ACL split is designed, not applied.** No Confluent CLI on this machine;
  `CloudClusterAdmin` is still in force and the old keys are still live.
- **`requirements-dev.txt` full-set resolution remains unknown** — unchanged
  since Session 3.

### Not done
- **No Databricks workspace**, deliberately and for the second session running.
  The reasoning is unchanged and recorded in Session 4's decision entry: a
  workspace provisions a NAT gateway at ~$33-41/month, 24/7, that no
  auto-terminate covers. Create it in the session that opens with Bronze.
- **No Snowflake account** — the 30-day calendar binds and Gold is Session 13.
- **Confluent ACLs not applied.** Designed in full, including the read/write
  split and the order of operations, and recorded as a decision.

### Incidents
Two, both found by checking a number rather than by anything failing:

1. **The CDC event count in three sessions of docs was 50 too high.** Every
   document says 158,396; the generator produces **158,346**, and has since the
   commit that first recorded it — `git diff` across the only two commits that
   ever touched the file shows `load_local_env()` and a `client_id` argument,
   neither of which can change an event count. A one-digit transcription error
   that propagated into Session 4's entry, into `decisions.md`, and into this
   session's plan as a production target. It survived because **no test asserts
   the total**: every invariant the suite checks is scale-free and passes at
   either number.

2. **Session 4's 50-order smoke run poisoned 43 SKUs on the live CDC topic.** A
   `--limit` CDC run is not a prefix of the full run — it is a different
   dataset. Seed stock is `headroom x lifetime demand` measured over the orders
   the run was handed, so a SKU seeds at 14 units from 50 orders and 66 from
   99,441, while carrying **the same `seq`**, because `seq` derives from event
   time and event time does not depend on `--limit`. 53 colliding pairs across
   43 keys. Silver's guard is `s.seq > t.seq` — *strictly* greater — so a tie
   updates nothing, the correct value is discarded, and a MERGE that matches no
   rows raises nothing. Left on the topic deliberately as Session 9/10 material.

### Decisions
Four appended, bringing `decisions.md` to **43 real entries** (counted this
session rather than carried — Session 4's "39" was one short, the same class of
error as the incident above): the full produce appends rather than recreating
the topics; the verifier assigns and never commits; partial CDC runs are
refused to Kafka while partial order runs are not; and the Confluent
authorization split into a writer and a reader identity, designed but not yet
applied.

### A note on where the session's value came from
Nothing broke. The produce worked first time, the reconciliation tied out, the
skew was even, and the code written to handle backpressure was never needed.
Both findings came from **reading the output carefully rather than checking
that it appeared** — a count that was 50 off, and a `distinct seq` line that
was 93 short of the record count. Either could have been skimmed past as
roughly right. The session's own verifier also failed this test once and had to
be corrected: it printed `seq monotonic overall: False`, which is a file-shaped
check applied to a partitioned topic where global ordering cannot hold and
means nothing. A check that returns `False` on healthy data is worse than no
check, because it teaches you to look away from the line directly above the
real evidence.

### Addendum — the whole cluster was rebuilt on a new Confluent account
Written 2026-09-26, six days after the entry above.

**In plain words:** the email address behind the original Confluent account was
lost, so the console could not be reached. Rather than keep digging, the
project moved to a new account and rebuilt everything from scratch. That took
about forty minutes of console work and **fifteen seconds of producing**,
because the data is regenerated from local CSVs. Nothing was lost that the
generators could not remake — which is the first time that design property has
been tested by anything other than choice.

**Everything above about the old cluster still happened.** The numbers, the
skew measurements, the 43 poisoned SKUs — all real, all measured. They are
simply no longer *present*, because the topic they lived on is unreachable.
Left in place rather than rewritten, per Rule 4.

#### New identifiers, replacing every ID recorded above

| | Old (unreachable) | New |
|---|---|---|
| Environment | `default` | `default` (`env-6n6516`) |
| Cluster | `lkc-2255zy2` | **`lkc-k8xr0m2`** |
| Service account | `sa-5w01oxq` | **`sa-nyomq06`** |
| Schema Registry | — | **`lsrc-dowq8jy`** |

Same shape otherwise: Basic, AWS, `ap-south-1`, two topics at 3 partitions,
Stream Governance Essentials.

#### The ACL split is no longer "designed, not applied" — it is applied

The decision entry written earlier this session records the authorization
design as recorded-but-not-executed. **It has now been executed**, on the new
account, and with one correction to the design (see the correction under that
entry). Final permission set:

| Resource | Name | Pattern | Operation |
|---|---|---|---|
| Topic | `orders` | LITERAL | WRITE, READ |
| Topic | `inventory.cdc` | LITERAL | WRITE, READ |
| Consumer group | `ledgerline-verifier-` | PREFIXED | READ |
| Schema Registry subjects | all, in `default` | — | `DeveloperWrite` |

**No `CloudClusterAdmin`, and no role binding of any kind on the cluster.** The
knowing downgrade accepted in Session 4 was never re-incurred: the new account
was built least-privilege from the first key, so there was nothing to retire.
The `Granted permissions` table for this service account holds exactly one
row, scoped to Schema Registry.

#### Two authorization facts learned by being denied, not by reading

1. **A Schema Registry API key authenticates but authorizes nothing.** With a
   valid SR key and no role binding, the first produce failed with
   `User is denied operation Write on Subject: orders-value` (403, SR code
   40301). The key proves identity; `DeveloperWrite` grants permission. They
   are separate steps and creating the key does not imply the second.
2. **`assign()` does not avoid the consumer-group ACL.** Covered in full in the
   correction under the verifier decision. Short version: skipping the
   rebalance protocol does not skip the group *coordinator lookup*, and that
   lookup is authorized against the group resource.

Both failed fast with messages that named the operation and the resource —
worth contrasting with Session 4's wrong-port hang, which named nothing and
cost three minutes.

#### Re-verified on the new cluster, from an empty start

- `orders`: **394,090 records, 394,090 distinct `event_id`** — no duplicates,
  because there is no Session 4 smoke run underneath this time. `created` =
  99,441, exactly the distinct order count. Line-item units **112,650**.
- `inventory.cdc`: **158,346 records, 158,346 distinct**, 34,448 seed inserts —
  exactly one per SKU. Reconciliation ties out again: **112,650 = 112,650**.
- **All 34,448 keys have ascending `seq`. Zero ties.** The 43 poisoned SKUs are
  gone, and the guard built this session is what will keep them gone.
- Partition spread is *identical* to the old cluster's — 33,162 / 33,356 /
  32,923 distinct keys, worst drift 0.7%. Same keys, same hash, same
  partitions, different cluster. A small but real demonstration that Kafka's
  default partitioner is deterministic rather than merely even.
- Produce throughput roughly doubled: **62,130/s** for orders against 34,645/s
  on the old cluster, **52,502/s** for CDC against 43,875/s. Same code, same
  laptop, same region, six days apart. Not investigated; recorded because an
  unexplained 1.8x is worth not pretending to understand. `queue_full_waits`
  was **0** again, so the local buffer still was not the constraint.

#### Consequence for Session 9/10 — better, not worse

The seq-collision incident recorded earlier this session ends by saying the 43
poisoned SKUs were left on the topic deliberately, as material for Session 9's
MERGE guard and Session 10's assert-the-bug-first experiment. **They are no
longer there.** That is an improvement: Session 10 can now recreate the
contamination *on purpose* with the `--allow-partial` flag built this session,
which makes it one of the six planned deliberate failures instead of an
accident being preserved. The incident entry stands as a true account of what
happened; only its closing paragraph is superseded.

#### Cost, now measured rather than estimated

The console shows Basic's real rates: **1st eCKU free**, then $0.15/eCKU-hr;
**data in/out $0.06/GB**; **storage $0.09/GB-month**. This project moves about
0.2 GB in and out per full rebuild and stores about 0.1 GB, so the entire
produce-and-verify cycle costs **roughly two cents**, against $400 of trial
credit. `CLAUDE.md` says "Confluent Basic is $0 at rest"; it is closer to
**$0.01/month** at rest with this data volume. Immaterial to the budget, and
corrected because a number that is nearly right is still a number that was
never checked.

#### One resource deleted that nobody asked for

Creating the environment auto-provisioned a **Flink compute pool**
(`default.env-6n6516.ap-south-1`), Running, 0 current CFUs, 50 max. This
project does not use Flink in any session. It was deleted. Structurally the
same concern as the Databricks NAT gateway deferred twice this session: a
billable resource created as a side effect of something else, which none of
the project's cost guards watch because none of them started it.

### Next
**Session 6 — Databricks workspace and the first Bronze ingest**

The workspace has now been deferred twice, both times correctly. It stops being
correct the moment a session opens with Bronze work, because the NAT gateway
bills from creation whether or not anything reads from it. So this session
creates it *and uses it the same day*, or defers it a third time and does
something else — not create it and leave it idle.

- **Create the Databricks workspace**: `ap-south-1`, Premium (the floor — AWS
  Standard was discontinued 2025-10-01), paid via **AWS Marketplace** so DBUs
  land inside Session 3's `ledgerline-monthly` and `ledgerline-daily-spike`
  budgets. Via a credit card they are invisible to every guard built so far.
- **10-minute auto-terminate on the cluster before running anything.** The
  named highest-risk item in this project: a single node left a week is ~$120
  against a ~$22 budget.
- **Bronze dimension ingest** — Auto Loader + `Trigger.AvailableNow` +
  `replaceWhere` over the 9 objects already in
  `s3://ledgerline-landing-dev-fffc8b65/ledgerline/dims/`.
- **D4 becomes answerable here**, and must be re-bound in `learning.md` from
  "expected ~S5" to this session: Auto Loader against a CSV with 3,852 newlines
  inside quoted fields, `multiLine=false` by default, what lands in Bronze and
  what it costs to fix.
- **Bronze order-event ingest** from the `orders` topic — 394,293 records
  waiting, of which 203 are deliberate duplicates. Bronze appends; the dedup is
  Silver's job in Session 8.

**Carried, and each one is a claim to re-check rather than copy:**
- **Apply the Confluent ACL split** designed this session. Create and test the
  two new credentials *before* deleting the old ones, or a wrong ACL set locks
  the project out of its own cluster. Removing the `CloudClusterAdmin` binding
  does **not** revoke keys created under it.
- **The queue-full retry has still never fired** (`queue_full_waits` = 0 at
  35-56K/s). Not a gap to close by producing more — it fires when a broker is
  slow, not when a topic is large. Leave it guarded and unexercised, and say so.
- **43 poisoned SKUs sit on `inventory.cdc`** with tied `seq` values. Deliberate
  Session 9/10 material, not damage to repair. Anyone reading a `seq` tie in
  Bronze should find the incident entry, not a mystery.
- **`requirements-dev.txt` full-set resolution** — unknown since Session 3.
- **ci #3** stays unconfirmed-cancelled. Only reopen it if evidence appears.

---

## Session 6 — Databricks lands on Free Edition, and the first Bronze tables
Date: 2026-09-26

**In plain words:** the plan was a paid Databricks workspace inside the AWS
account. Two walls stood in the way: the AWS account was on a Free plan that
would have closed it on Oct 17, and — once upgraded — AWS Marketplace refused
the card because the account is billed by AWS's Indian entity. So the project
tested Databricks' free tier instead, proved it can read both the S3 bucket
and the Kafka cluster, and moved there. **$0 for Databricks, no NAT gateway.**
Then the first Bronze tables were built — the three nightly dimension dumps —
and proven safe to re-load: forcing a full re-read of all 9 files left every
count unchanged.

### Scope, as it actually ran
Planned: create a paid Databricks workspace (`ap-south-1`, via Marketplace),
Bronze dimension ingest, Bronze order-event ingest. Actual: most of the session
went to getting a Databricks that could reach the data at all. Bronze dims
landed and were verified; **Kafka Bronze moves to Session 7.** D4 could not be
asked (below).

### Start-of-session re-check
Three items in Session 5's carry list were already stale, and were retired
rather than copied: the Confluent ACL split had been **applied** in the
addendum; the 43 poisoned SKUs are **gone** with the old cluster; and `orders`
holds **394,090** records, not 394,293 — the new cluster has no replayed
duplicates. Session 5 itself had been committed (`bbec5d7`).

### Built
- **AWS account upgraded** from the Free plan to Paid (would have closed
  2026-10-17). Credits of $68.53 carried over.
- **IAM role `ledgerline-uc-landing-read`** — trusted by Databricks' Unity
  Catalog role with this account's external ID, self-assuming, read-only and
  prefix-scoped to `ledgerline/`.
- **Unity Catalog, in Free Edition** (workspace `dbc-7ce3c403-9521`, us-east-2):
  storage credential `ledgerline_landing_read`; read-only external location
  `ledgerline_landing` (file events off); UC secrets
  `workspace.ledgerline_secrets.{kafka_api_key, kafka_api_secret, sr_api_key,
  sr_api_secret}`, created entirely in the browser; schema `workspace.bronze`
  with a `checkpoints` volume.
- **`databricks/bronze/autoload_dims.py`** — Auto Loader (`AvailableNow`) →
  `foreachBatch` → `replaceWhere dump_date IN (...)`, strings only, lineage
  columns, a guard that the file's `dump_date` equals its folder name, and
  built-in count and idempotency checks.
- **Databricks Git folder** on the GitHub repo (branch `dev`), with a
  read-only, single-repo GitHub token. Notebooks now run from git.
- `pyproject.toml`: ruff ignores `F821` under `databricks/` (runtime-injected
  `spark`, `dbutils`, `display`). `ruff check .` clean; **the full pytest suite
  passes**, unchanged in count (no new tests this session — the new code is a
  notebook, verified in Databricks below).

### Verified
- **Free Edition reads S3 through Unity Catalog:** `ledgerline/dims/customer/`
  → **8,395 rows**, equal to the three CSVs parsed locally (1 + 326 + 8,068).
- **Free Edition reads Kafka through UC secrets:** `orders` → **394,090
  records**, 131,458 / 132,187 / 130,445 per partition. Raw count only — **Avro
  decoding was not tested.** Each full read took ~3 min 43 s, unexplained.
- **Bronze dims land exactly:** customer 1 / 326 / 8,068; product 32,951 × 3;
  seller 3,095 × 3 — per night, per table, equal to local counts.
- **Re-loading is harmless, proven two ways:** a plain re-run wrote nothing
  (no new files → no commit); a forced replay with every checkpoint deleted
  re-read all 9 files and **left every count unchanged**. `DESCRIBE HISTORY` on
  `bronze.customer` shows it: v0 create (empty), v1 first load and v2 replay,
  both `WRITE` / `Overwrite` with predicate `dump_date IN (3 nights)`.
- Run from the **Git-folder copy**, not an imported one.

### Built but NOT verified
- **Avro decode from Kafka** (`from_avro` + Schema Registry on serverless) —
  documented as supported, not run.
- **Asset Bundles / CI deploy to Free Edition** — whether a workspace token for
  GitHub Actions is allowed is unknown.
- **Unity Catalog column masks on Free Edition** — needed for the GDPR work,
  unverified.

### Not done
- **Kafka Bronze** (`stream_orders.py`, `stream_cdc.py`) → Session 7.
- **D4** — the dims contain zero embedded newlines; the 3,852-newline review
  text was never uploaded. Re-bound to Session 7.
- **Bronze consumer-group ACL** — today's Kafka test borrowed the
  `ledgerline-verifier-` prefix; Bronze needs its own.

### Incidents
Three, each found by something failing, logged when it happened:
1. **Marketplace refused the card** — the account's Service provider is *Amazon
   Web Services India Private Limited*; Marketplace does not accept Indian
   cards. The Session 4 "pay via Marketplace" plan never had a route.
2. **Databricks could assume the role but not read** — `ListBucket` was scoped
   to prefixes `ledgerline/` and `ledgerline/*`; the validation lists
   `ledgerline`. One missing form, `implicitDeny`.
3. **The re-load test broke the one path it was built to test** —
   `tableExists()` inside `foreachBatch` (a cloned session on serverless)
   answered False for an existing table. The first load and the plain re-run
   were both green and **neither had executed `replaceWhere` once.**

### Decisions
Seven new entries and four corrections. New: AWS plan upgrade; direct
Databricks billing (never executed — corrected); **Free Edition, proven by
reading both sources**; scope +3 (Airflow, windowed streaming, GDPR erasure);
four folded-in additions (quarantine, schema contract, CDF, stream monitoring);
the Bronze dims design; the Git folder. Corrections: the Session 4 Marketplace
bullet; the direct-signup entry; and the **Session 0 "Paid Databricks over Free
Edition" entry, whose two "verified blockers" both turned out false** when
code was run against them. `decisions.md` now holds **51** real entries.

### Also changed
- **Learning check convention** (human's instruction): old weak spots move to a
  status-tracked **question bank** in `learning.md`; the end-of-session check
  discusses the session's own work. `CLAUDE.md` updated to match.
- `CLAUDE.md` verified-facts table gained five rows (AISPL, Free Edition,
  UC access proven, the `ListBucket` prefix rule, the 3m43s read).

### Cost
Databricks **$0**. AWS: no billable resource created — one IAM role, and a few
cents at most of cross-region S3 reads (Mumbai bucket, Ohio workspace).
Confluent: two ~0.1 GB reads, ~$0.01.

### Next
**Session 7 — Kafka Bronze: exactly-once from `orders` and `inventory.cdc`**

- **Confluent first:** a READ ACL on a new consumer-group prefix
  (`ledgerline-bronze-`), so Bronze stops borrowing the verifier's.
- **`stream_orders.py` / `stream_cdc.py`** — `readStream` Kafka →
  `foreachBatch` → Delta append with **`txnAppId` + `txnVersion = batch_id`**,
  `AvailableNow`. Prove it the same way as today: force a replay and show no
  duplicates by `(partition, offset)`.
- **Avro decode** via `from_avro` + Schema Registry (UC secrets `sr_*`) —
  the one Kafka piece not yet run on Free Edition.
- Folded in, per today's decisions: a **quarantine table** for records that
  will not decode; an explicit **Schema Registry compatibility mode** plus one
  refused breaking change; **streaming progress** recorded to a small Delta
  table.
- **D4:** land a reviews CSV and load it with Auto Loader defaults vs
  `multiLine` — then D4 is answerable.

**Carried, each a claim to re-check rather than copy:**
- `CLAUDE.md` **Platform split** and **Budget** sections still describe a paid
  Premium workspace with clusters and auto-terminate. Stale since today;
  needs rewriting for Free Edition.
- GitHub token for the Git folder **expires 2026-12-25**.
- Free Edition **deletes long-inactive accounts** — everything must stay
  rebuildable from git + S3.
- Queue-full retry still never fired; `requirements-dev.txt` resolution still
  unknown; ci #3 still unconfirmed.

### Addendum — CI went red, and the test suite had been publishing to Kafka
Written the same day, after the entry above.

**In plain words:** the push of this session's work turned CI red (2 of 122
tests). Chasing why found something worse: on the laptop, one of those tests
was publishing a 50-order partial CDC run to the **live** `inventory.cdc`
topic every time the suite ran. It ran three times today — twice by Claude.
The entry above says the suite passes; it did, and that green was part of how
the problem stayed invisible. Full account in `incidents.md`.

- **Measured:** `inventory.cdc` = **158,625** records (clean is 158,346): 279
  extra = 3 x 93, **43 keys with a tied `seq`** again. `orders` untouched
  (394,090 / 394,090 distinct).
- **Fixed:** autouse fixture `no_live_services` in `tests/conftest.py` — no
  test can read `.env`, see a live credential, or fall back to
  `~/.aws/credentials`. The partial-run guard now fires before any data load
  (the actual CI failure: CI has no `data/raw`). The "allowed" test uses
  fixture data and asserts it stops at the Kafka sink.
- **Verified:** suite green and lint clean locally; refusal works with a
  non-existent `--raw-dir`; **the live topic re-counted after a full local
  test run: still 158,625** — the suite no longer reaches it. CI result on the
  next push: not yet seen.
- **Not done:** the topic is still contaminated — cleaning it needs a
  human-approved delete + recreate + re-produce.
- `CLAUDE.md` rewritten for Free Edition: platform split, budget section, two
  stale facts, and a standing note that the **Snowflake bullet must be rewritten
  the same way — from verified facts — when the trial is created (Session 13).**

- **CI confirmed green after the fix** — GitHub Actions run #8 (`80e3eb0`, the
  fix) and #9 (`40960eb`) passed; #6 and #7 were the red runs.
- **Why the failing tests reached CI only now:** the Session 5 commit
  (`bbec5d7`) has no CI run of its own — it was pushed together with the
  Session 6 Bronze commit, and Actions runs only the newest commit of a push.
  The tests written in Session 5 were first exercised by CI in run #6.
- **ci #3, carried as unconfirmed since Session 4, is closed:** the Actions
  list shows #3 with the *cancelled* icon, not the failure icon #6/#7 carry,
  and #3/#4 are the same commit message a minute apart with #4 green —
  consistent with `cancel-in-progress: true`. Not a failure; removed from the
  carry list.
- **Two candidates raised by "what would production do?"** — not decided,
  to weigh when their sessions open: (1) **S7:** stamp a provenance header (a
  per-run ID) on every generated event, so a bad run can be found by query
  rather than by symptoms; (2) **S10:** make the deliberate-contamination
  experiment end with the production fix — a compensating event, or a Silver
  repair via Delta time travel — rather than recreating the topic.

### Next — revised: Drill 1 follows Session 7
Recorded the same day, after the `### Next` above. Session 7 is unchanged.
Added (decision entry "Drill sessions between build phases"):

**Drill 1 — after Session 7, before Silver (Session 8)**

*Part 1 — break the guarantees on purpose, prove they hold*
- Dims replay: re-deliver an existing night's file with changed content →
  `replaceWhere` replaces that night only; other nights untouched.
- New data only: the generator adds a 4th night → Auto Loader loads that one
  file.
- Crash mid-load: stop the Kafka Bronze job partway, restart → no gaps and no
  duplicates by `(partition, offset)`.
- The `txnAppId` trap: delete the checkpoint but keep the same app ID →
  assert Delta **silently skips** batches (data loss, no error); then fix with
  a new app ID.
- Producer duplicates: run the order generator twice → Bronze keeps both
  (correct for Bronze); count duplicates by `event_id`.
- Contaminated CDC: Bronze holds all 158,625; the denylist matches exactly 159.

*Part 2 — re-break past incidents, confirm each guard fires*
- S2 CRLF bytes · S3 `.env` not loaded → admin key · S4 wrong port / silent
  hang · S5 partial CDC run to Kafka · S6 S3 prefix without trailing slash ·
  S6 `tableExists` in `foreachBatch` · S6 tests publishing to live (count the
  topic before and after the suite).
- **S5 "CDC count 50 off" has no guard** → build a test asserting 158,346.

*Part 3 — revision discussion:* bank items B1, B2, B4, B5, B10, B11, B13, B14.

**Carried into Session 7 from this addendum:** the production-style remediation
(Bronze ingests `inventory.cdc` unfiltered; denylist at
`ops/incidents/2026-09-26_inventory_cdc_denylist.json`), and the candidates for
a provenance header, a dev-only Kafka credential, and a verifier that reports
duplicates and genuine `seq` conflicts separately. **Remove "clean the topic"
from any list — the topic stays as it is, by decision.**

---

## Session 7 — Kafka Bronze, exactly-once proven by a deliberate crash
Date: 2026-09-27

**In plain words:** both Kafka topics now land in Bronze — every message
exactly once, checked offset by offset against the topic itself. The
exactly-once setting was then proven the hard way: an experiment made a
stream "crash" at the worst moment, twice — without the setting the batch
landed twice (44,099 duplicates, job reported success), with it Delta skipped
the repeat (0 duplicates). Along the way the session found things nobody had
checked: the topics would have deleted themselves within a week, Databricks
was maintaining our tables on its own, and three pieces of our own monitoring
and quarantine code were wrong in ways a green run could not show.

### Scope, as it actually ran
Planned: Kafka Bronze for both topics with `txnAppId`/`txnVersion`, Avro
decode, quarantine, compatibility mode + a refused change, stream monitoring,
D4. Done: all except **D4**, which moves to Drill 1. Added on the way:
retention, Predictive Optimization, and `exp_01` (the first of the six
deliberate-failure experiments).

### Start-of-session re-check
Read-only from the laptop: `orders` 394,090 and `inventory.cdc` 158,625
records, every partition's lowest offset still 0; Schema Registry global
`BACKWARD`, no subject-level mode, one version each (ids 100001 / 100002).
**New:** the project key cannot read topic settings (`TOPIC_AUTHORIZATION_FAILED`),
so retention had never been seen. Read in the console: **1 week** on both.

### Built
- `databricks/bronze/_kafka_bronze.py` (shared, loaded with `%run`): Kafka
  `readStream` → `from_avro` with Schema Registry (`PERMISSIVE`) → split good /
  quarantine → Delta append with `txnAppId` + `txnVersion = batch_id`,
  `AvailableNow`, `maxOffsetsPerTrigger = 50,000`, `failOnDataLoss = true`,
  headers kept, raw Avro bytes kept (`_raw_value`). Tables, the progress table
  and a 30-day file retention are set up before any stream starts. Checks:
  per-partition rows = distinct offsets = span; per-batch rows = distinct
  offsets; backlog; a quarantine self-test that drives six broken messages.
- `databricks/bronze/stream_orders.py`, `stream_cdc.py`: thin notebooks with
  the expected counts. CDC is ingested **unfiltered**; the notebook proves the
  denylist against Bronze.
- `experiments/exp_01_exactly_once_replay.py`: load, delete the last
  `commits/N` from the checkpoint (the exact state a crash leaves), restart;
  plain append vs txn options, on scratch tables.
- `scripts/check_schema_compat.py`: asks the live registry which changes it
  would accept; registers nothing.
- `autoload_dims.py` also sets the 30-day retention (see Not verified).
- Tables: `workspace.bronze.{orders, orders_quarantine, inventory_cdc,
  inventory_cdc_quarantine, stream_progress}`; scratch `exp01_{bug,fix}` (+ quarantine).

### Verified
- **Bronze `orders`:** 394,090 rows; per partition 131,458 / 132,187 /
  130,445 with rows = distinct offsets = span (no duplicates, no gaps); 394,090
  distinct `event_id`; 99,441 `created`; 112,650 units; 1 schema id; 0
  quarantined; 8 batches (49,999 ×5, 49,998 ×2, 44,099), each written once.
- **Bronze `inventory_cdc`:** 158,625 rows; 52,838 / 53,699 / 52,088, all
  contiguous; 158,399 distinct `event_id`; 34,448 SKUs; inserts 34,577 (=
  34,448 + 3 × 43), updates 124,048 (= 123,898 + 3 × 50), **deletes 0**; 4
  batches. **Contamination reproduced exactly by a second engine:** 159
  denylisted rows, 53 ids; units sold raw **112,806**, after denylist and one
  row per `event_id` **112,650**, the laptop's Session 6 numbers.
- **Plain re-runs write nothing**, both topics, repeatedly (history unchanged).
- **Quarantine routes every broken case correctly** (asserted): truncated
  payload → decode failed; wrong magic byte → not Confluent format; unknown
  schema id → decode failed; null value → tombstone; missing key → missing
  key; real message → decoded.
- **`from_avro` looks up the schema id in each message** (unknown id 999,999
  → blanks), so a future version-2 message is read with version 2's schema.
- **Spark's Kafka reader needs no consumer-group ACL** (probe under a group
  prefix with no ACL; confirmed by every streaming run since).
- **`exp_01` (deliberate failure #1):** bug: restart re-ran batch 7, table
  438,189 rows, **44,099 duplicates**, extra `WRITE` v10, no error. Fix:
  restart re-ran batch 7, **394,090 rows, 0 duplicates**, no extra commit.
  Replay 66 s vs **8 s** (Delta skips from its log before reading Kafka).
- **Retention:** both topics 1 week → **3 months** (human, console); data
  untouched after the change (re-counted).
- **Predictive Optimization** is on for every table (inherited from the
  metastore); it ran `OPTIMIZE` on `bronze.orders` (24 files → 1). All four
  Kafka Bronze tables now carry `delta.deletedFileRetentionDuration = 30 days`
  (`SET TBLPROPERTIES` in history).
- **Monitoring columns checked against the table:** `rows_by_offsets` exact,
  backlog exact (108,626 → 58,628 → 8,629 → 0); `numInputRows` **0 for every
  batch** on serverless `foreachBatch`; per-batch durations **do not reconcile**
  (520 s reported inside a ~270 s run).
- **Schema Registry:** probes: add with default accepted, add without default
  refused, remove a required field accepted (the BACKWARD gap). One real
  registration of a breaking change → **409**, versions `[1]` before and
  after. Registry key refused changing the mode (**403 WriteCompatibility**).
  Mode set to **`BACKWARD_TRANSITIVE`** by the human and read back on both
  subjects.
- `ruff` clean, `pytest` green locally after every change.

### Built but NOT verified
- **30-day retention on the three dimension tables:** code in
  `autoload_dims.py`, not yet run.
- **`check_backlog` can fail:** it compares end-of-run backlogs, which are 0
  on fixed topics; it has never been given a growing backlog.
- **The two Bronze writes per batch** were assumed to read Kafka twice. The
  exp_01 stack trace shows Spark Connect wrapping the batch in
  `dataFrameCachingWrapper`, so probably not. Not measured.

### Not done
- **D4** (reviews CSV with 3,852 embedded newlines through Auto Loader):
  carried twice; re-bound to Drill 1.
- A read-only Kafka identity for Bronze (recommended; not decided). The
  Databricks secret still holds the one key that can also write both topics.

### Incidents (5 new entries, one deliberate)
1. Progress log crashed: the first batch's start offset arrives as the *text*
   `"null"`. My source-reading guess (PySpark 4.0.1) was wrong; a diagnostic
   on the platform (PySpark 4.3.0.dev0) found it.
2. Quarantine could never fire: `PERMISSIVE` returns a record of blanks, not
   NULL. Found by the self-test before any bad message existed.
3. Monitoring said "0 rows" for a 158,625-row load: `numInputRows` is 0 on
   serverless `foreachBatch`; durations also unreliable.
4. exp_01's staged crash made Databricks fail the cell (any stream that dies
   during a command fails it, caught or not), so exp_01 now recreates the
   crash *state* instead.
5. **DELIBERATE, exp_01**, both results above.

Retention (found by the re-check) and Predictive Optimization (found by
reading table history) are recorded as decisions, not incidents.

### Decisions
Seven new: Kafka Bronze design (raw bytes kept, separate quarantine table,
50,000-offset batches, checkpoint and app id share a generation, the crash
proof) plus its correction; retention 1 week → 3 months; no consumer-group
ACL; `from_avro` + registry; Predictive Optimization stays on with 30-day
file retention; `BACKWARD_TRANSITIVE` (FULL_TRANSITIVE withdrawn when
questioned).

### Unexplained, recorded rather than guessed
- Per-batch `duration_ms` is about 2× the real gap between commits.
- `statsOnLoad: true` on `inventory_cdc` writes, `false` on `orders`.
- About 65–70 s per batch whatever its size (8,629 rows took ~49 s); the
  diagnostic showed 11.9 s for a batch that read nothing, 2.9 s of it asking
  Kafka for the latest offsets.

### Cost
Databricks $0. Confluent: roughly 0.5 GB of reads across the loads and
experiments, a few cents. AWS: nothing new.

### Next
**Drill 1: attack Bronze on purpose, and re-trigger every past incident**
(plan in the Session 6 addendum above). Added by Session 7:
- **The `txnAppId` trap:** delete a checkpoint but keep the app id, and assert
  Delta silently skips batches (data loss, no error), on scratch tables. Then
  build the guard the Kafka Bronze decision deliberately left out.
- "Crash mid-load" is now `exp_01`; re-run it after any change to the Bronze
  writer.
- **D4** (bound here), and the unexplained durations.
- Before the "run the order generator twice" attack: decide the **provenance
  header** (a per-run id on every message). Bronze already stores headers.
- Run `autoload_dims` once (Run all *above* the forced-replay cells) to apply
  the 30-day retention.

**Carried, each a claim to re-check:**
- **Topics delete themselves around 2026-12-25** (3-month retention); Bronze's
  raw bytes are then the only copy of the contamination.
- **Silver (S9): the topic has no `D` events** (generator default
  `--delist-count 0`), so the MERGE's delete branch must be driven on purpose,
  like today's quarantine.
- **Silver (S11):** a DLT expectation that `order_status` is never blank, the
  guard for the gap `BACKWARD_TRANSITIVE` leaves.
- Read-only Kafka identity for Bronze; dev-only Kafka credential; verifier
  reporting duplicates vs genuine `seq` conflicts separately.
- Scratch leftover: checkpoint `checkpoints/scratch/s7_progress_probe`.
- GitHub token for the Git folder expires 2026-12-25.

---

## Drill 1 — attacking Bronze on purpose, and re-breaking every past incident
Date: 2026-09-27 / 28 (named, not numbered — Session 8 is still Silver dims)

**In plain words:** nothing new was built for the pipeline's own sake. Every
guarantee Bronze claims was attacked on purpose, and every past incident was
put back to see whether its guard still fires. The headline: deleting a Kafka
stream's checkpoint while keeping its app id **silently lost 74 new
messages** — the job succeeded, and even a restart could not bring them back;
a guard now refuses that reset before any stream starts. Three more things
nobody had checked turned out wrong: a corrected nightly dump re-delivered
under the same name was **ignored** by Bronze; three incident guards had **no
test that could fail**; and CI had **never run** the producer tests.

### Scope, as it actually ran
Planned (Session 6 addendum + Session 7 `### Next`): Part 1 attacks, Part 2
incident regression, Part 3 revision, provenance header first, D4, dims
retention. All done, plus two things found on the way: the Git-folder conflict
(and the environment pin that fixes it) and the corrected-re-delivery bug.

### Start-of-drill re-check (read-only)
`orders` 394,090 (131,458 / 132,187 / 130,445), `inventory.cdc` 158,625,
lowest offset 0, oldest message 2026-09-26 — unchanged since Session 7. 125
tests pass, ruff clean.

### Built
- **Provenance header** (`generators/_common.py`): every produced message
  carries `ledgerline.run_id` (uuid4 per run, printed in the summary),
  `ledgerline.producer`, `ledgerline.scope` (`full` / `limit=N`).
- **Checkpoint-reset guard** (`_kafka_bronze.refuse_unsafe_reset`): a
  checkpoint with no `offsets/` may only write into empty tables.
- **Bronze checks against the broker** (`topic_end`, `header_sql`):
  `stream_orders`, `stream_cdc`, `exp_01` no longer hardcode 394,090 /
  158,625; `stream_orders` asserts rows − distinct events = labelled rows, and
  shows who sent the extras in one query.
- **Dims:** `_autoload_dims` (library) split from `autoload_dims`
  (production). Production checks every night against a fresh read of the
  landing zone plus pinned golden nights; the checkpoint-deleting cell moved
  to the drill; **`allowOverwrites` on**.
- **Drill notebooks** (`drills/`): the trap (`_drill1_trap_common`,
  `drill1_trap_1_life1`, `drill1_trap_2_reset`), dims + D4 (`drill1_dims_1`,
  `drill1_dims_2`), and the laptop script `land_drill1_scratch.py` (writes
  only under `ledgerline/drill1/`).
- **Guards for incidents that had none:** platform-independent CRLF test;
  two `flush` tests (the S4 hang); a test of the live-service fixture itself
  (`block_live_services` split out); `scripts/check_uc_role_policy.py` (S6
  IAM, via the policy simulator); `tests/test_headline_numbers.py` (S5:
  394,090 / 158,346 = 34,448 + 102,425 + 21,473 + 0 / 112,650 units; needs the
  real CSVs, so it skips in CI — the same totals are asserted in Databricks
  against the topics).
- **CI** installs `confluent-kafka` and asserts it imports.
- **Every notebook pins `environment_version = "6"`** and is committed in
  Databricks' own save format; `tests/test_notebooks.py` keeps it so.
- Tests: 125 → **139**. Ruff clean.

### Verified
- **The trap, bug first** (scratch): life 1 → 394,090 rows, Delta at app v1
  batch 7. Two labelled partial runs → **74** new messages (16 / 30 / 28 per
  partition; laptop read-back: all 74 carry all three headers, 37 per run, the
  same 37 `event_id`s twice). Checkpoint deleted, same app id, guard off →
  life 2 planned batches 0–7 to the new end, **table 394,090 → 394,090, data
  commits 8 → 8, no error**; **17 s** for 8 batches against life 1's **531 s**
  (skipped from the log, Kafka never read). The 74 named by a batch read of
  exactly those offsets: 0 in the table, all 74 duplicates of events already
  present — harmless here by luck. A normal restart afterwards: 0 batches.
- **The guard:** both unsafe resets refused, no stream started (`v2/offsets`
  empty). **Safe reset** (new generation, empty table): 131,474 / 132,217 /
  130,473 = **394,164**, every offset once, all 74 present. The rebuild's
  batches were 49,998 ×6, 49,999, 44,177 against life 1's 49,999 ×5, 49,998
  ×2, 44,099 — a batch number does not mean the same messages twice.
- **Real Bronze `orders`:** one batch of **74** (17 s); 394,164 rows,
  394,090 distinct events, **74 labelled**, 99,441 created, 112,650 units;
  provenance query: 394,090 unlabelled + 37 + 37. Quarantine self-test still
  routes all six cases. Plain re-run writes nothing.
- **4th dims night on the live path:** the generator's rewrite of nights 1–3
  was byte-identical (all 9 ETags unchanged, only LastModified moved); the 4th
  night's 3 files equal a local generation too. Production `autoload_dims`
  wrote **one commit per table, `dump_date IN ('2017-08-30')` only**
  (customer 22,497); old nights still carry their 2026-09-20 file times —
  the rewrites were not re-read. `cloud_files_state` lists 4 files per table.
- **30-day retention on the dims tables** (Session 7's unverified item):
  `SET TBLPROPERTIES` in history.
- **Dims checkpoint lost** (live): every file re-read, one write per table
  replacing all 4 nights (30,892 / 131,804 / 12,380), **counts unchanged**.
- **Corrected re-delivery** (scratch): default → **0 corrected rows, no
  write, no error** (the bug); `allowOverwrites` → one write for 2017-05-02
  only, 5 corrected rows, counts unchanged. The write reported 6,190 rows for a
  3,095-row night: the unpartitioned table rewrote the file it shared with the
  other night, copying those rows unchanged. Production then ran with
  `allowOverwrites` on and wrote nothing (4 WRITE commits before and after).
- **D4:** default options **104,162 rows** = the file's non-blank physical
  lines; 4,938 fragment rows (5,495 embedded line breaks − 557 blank lines;
  `bad_scores` exactly 4,938); **`_rescued_data` caught 0**; no error.
  `multiLine` alone: 99,249 (still wrong). `multiLine` + `escape='"'`:
  **99,224, matches pandas on all six checks**.
- **Incident regression (put the bug back, run the guard):** S3 fires, S5
  fires; **S2 was silent** (fixed: now fires); S4 and S6 had no test (now
  fire). S6 live-publish: after six suite runs `inventory.cdc` still 158,625
  and `orders` 394,090 + exactly the 74 labelled. **S6 IAM:** 10 request
  shapes as expected, and the Session 6 policy shape reproduces
  `implicitDeny` for prefix `ledgerline`. S5 headline numbers: all three
  tests pass on the real data.
- **Environment pin:** after the pin, running `drill1_dims_2` left the Git
  folder at "No changed files".
- **`exp_01` re-run after the writer change** (topic now 394,164): bug — restart re-ran
  batch [7], **438,341 rows, 44,177 duplicates**, extra `WRITE` v10, job reported success;
  fix — batch [7] re-ran, **394,164 rows, 0 duplicates**, no extra commit. Replay **63 s vs
  7 s**. The guard let both fresh loads and both crash replays through.

### Built but NOT verified
- **CI with `confluent-kafka`:** pushed; the Actions log should now show the
  producer tests run, none skipped. Not seen.
- `check_backlog` still never given a growing backlog.

### Not done
- The unexplained S7 items (per-batch durations ≈ 2× real; `statsOnLoad`) —
  one new data point only (below).
- The read-only Kafka identity for Bronze and a dev-only credential — still
  candidates.

### Incidents (4 new entries, one deliberate)
1. **DELIBERATE:** checkpoint deleted, app id kept → 74 messages lost, no error.
2. Three incident guards had no test that could fail; CI skipped the producer
   tests; the S6 IAM prefix had no automated guard.
3. A Git-folder Pull stopped on a conflict in files nobody had edited
   (Databricks re-saves notebooks: environment header + no final newline).
4. A corrected nightly dump re-delivered under the same name was ignored by
   Bronze (Auto Loader tracks files by path).

### Decisions
Six new: provenance header; checkpoint-reset guard; Bronze checks against the
broker's offsets; notebooks pin `environment_version = "6"`; dims read
re-delivered nights (`allowOverwrites`); plus a **correction** under the
Session 6 dims entry (the "backfill is harmless" claim was never true).

### Unexplained, recorded rather than guessed
- D4: default and `multiLine` both count 3,838 messages containing a line
  break, 14 short of the true 3,852.
- `statsOnLoad: true` on `bronze.orders` v11 (the 74-row batch), `false` on
  v1–v8. v11 is the first write after Predictive Optimization's `OPTIMIZE`
  (v9) and the retention change (v10). And in `exp_01`: every write of the
  fix table (txn options) `true`, of the bug table (plain append) `false`,
  minutes apart — but Session 7's `bronze.orders`, also txn, was `false`.
- Predictive Optimization ran `OPTIMIZE` (`auto: true`) on the scratch seller
  table two seconds after the one-night rewrite.
- `print` inside `foreachBatch` never reaches the notebook on serverless (the
  batch function runs in a separate process); table history is the evidence.

### Cost
Databricks $0. Confluent: ~1 GB of reads (full `orders` loads for the trap and
exp_01) — a few cents; 74 messages produced. AWS: 3 new dims objects + 9
byte-identical rewrites + 3 scratch objects (~15 MB), cents.

### Hands-on checks after the drill
- Done: `DESCRIBE HISTORY` on `exp01_bug` (v2–v10, v10 = batch 7 again) vs
  `exp01_fix` (stops at v9); `bronze.customer` (v4 = `2017-08-30` only, v5 =
  all 4 nights after the checkpoint reset); `cloud_files_state` from the **SQL
  Editor** too — 4 files, sizes equal to S3, `create_time` = the 19:00 rewrite,
  so Auto Loader keys its memory on path + modification time.
- **Skipped at the human's request:** the Confluent message-detail view of the
  headers (the laptop read-back already showed all 74 carry them) and the
  GitHub Actions log — so **CI running the producer tests stays unverified**
  (carried in `### Next`).

### Learning check
Five questions, discussed, after a plain-words summary of the drill (asked
for by the human). ◐ reset trap (outcome ✓, batch-number mechanism missed);
❌ no-duplicates/no-gaps SQL (second miss — re-explained from the notebooks'
own output, plus how production runs a broker comparison); ❌ CSV line breaks
(a three-part question did not land; the one-example version was answered
"1 row", it is 2); ❌ why a checkpoint reset was harmless for dims; ◐ / ✓
producer idempotence and why Bronze keeps duplicates. Full log in
`learning.md` Part B; bank updated (B4 now 1 of 2; B16 missed ×2; new B18,
B19).

### Next
**Session 8 — Silver dims: `MERGE` + `WHEN NOT MATCHED BY SOURCE DELETE`**

Decide first, before touching the generator:
- **The dims generator rewrites every earlier night on each run, and
  production now re-reads rewritten files (`allowOverwrites`).** Identical
  bytes were harmless today; a run with *different arguments* (for example
  `--delete-per-night`) would rewrite nights 1–4 with different content, and
  Bronze would silently replace its history to match. Make landing write-once
  (skip nights already in S3) before any argument changes.
- **All 4 nights have 0 deleted rows** (`--delete-per-night 0`), so the
  `NOT MATCHED BY SOURCE DELETE` branch Session 8 exists to teach never fires
  on real data. Drive it on purpose — a 5th night with deletions, landed
  write-once — the same way the quarantine was driven in S7.
- Silver tables choose their own time-travel window (S7 decision).
- `exp_03` (the MERGE gap: a stale row survives a plain MERGE) belongs here.

**Carried, each a claim to re-check:**
- **CI:** the Actions log for the pushes since `09d0e0b` should show the
  producer tests running (not skipped). Not yet seen.
- **Scheduled completeness check** (table vs broker watermarks, alert on
  missing) — the production form of today's in-notebook check; arrives with
  a scheduler (Airflow, in scope since S6).
- Question-bank re-tests fitted to S8's SQL: **B16** (write it from memory),
  B19, B18.
- Topics delete their oldest messages from ~**2026-12-25**; the GitHub token
  for the Git folder expires the same day.
- Read-only Kafka identity for Bronze; dev-only Kafka credential; verifier
  reporting duplicates vs genuine `seq` conflicts separately.
- Scratch leftovers (harmless, kept as evidence): tables
  `bronze.drill1_trap*`, `drill1_seller`, `drill1_reviews_*`, `exp01_*`;
  checkpoints under `checkpoints/drill1/`; S3 `ledgerline/drill1/`; S7's
  `checkpoints/scratch/s7_progress_probe`.

---

## Session 8 — Silver dimensions: the MERGE that deletes by absence
Date: 2026-09-30

**In plain words:** Silver now holds every customer, product and seller as
they are *now*, built by applying the nightly dumps one at a time with a
`MERGE` that inserts new rows, updates changed ones, and **deletes rows missing
from the night's full file**. To give that delete something real to do, a 5th
night was landed with 219 rows removed — without touching the four nights
already delivered, which the generator can no longer rewrite by accident. The
deliberate experiment `exp_03` showed the gap the delete clause closes (100
deleted sellers left in Silver by a plain MERGE, job green) and then broke the
fix's own assumptions on purpose. On the way: every zip code starting with 0
had lost that zero somewhere between Olist and the landing zone, in every
night, unnoticed for eight sessions. And near the end the human asked about
`exp_02` — the Bronze half of the same story, due in Session 6 and never
scheduled by anyone. It was built and run the same day (a plain append doubled
a re-delivered night; an overwrite without a predicate deleted the other
night), and all six experiments are now bound to sessions in a tracker.

### Scope, as it actually ran
Planned (Drill 1's `### Next`): decide write-once landing first, drive the
delete branch with a 5th night, Silver dims with `NOT MATCHED BY SOURCE`,
Silver retention, `exp_03`. All done. Added: the zip defect (found before
writing a single cast), the delete circuit breaker, the merge log, Change Data
Feed from the first commit. Then, at the human's question, `exp_02` and the
experiment tracker.

### Start-of-session re-check (read-only)
The four landed nights regenerated on the laptop; the generator's own report
became the answer key for Silver's MERGE (inserts / updates / deletes per
night). **0 deletions on every night** — the clause this session exists for had
nothing to delete. Raw Olist zips: 99,441 / 99,441 customers and 3,095 / 3,095
sellers have five digits; the landed dumps do not (incident).

### Built
- **Generator** (`generators/dim_dumps.py`): `--delete-from DATE` (deletions
  start on a named night; earlier nights regenerate byte-identical);
  **write-once landing** — all nights planned in memory against an overlay of
  the sink, a landed night left alone when identical, **the whole run refused,
  nothing written** if a landed night would change, unless named in
  `--redeliver DATE`; a `landed` column in the summary.
- **Night 5 landed**, 2017-12-28, by one command that reproduces the whole
  landed history from nothing:
  `python generators/dim_dumps.py --sink s3 --bucket ledgerline-landing-dev-fffc8b65 --prefix ledgerline --nights 5 --stride-days 120 --delete-per-night 100 --delete-from 2017-12-28`
- `autoload_dims`: pinned golden counts for night 5 (43,690 / 32,851 / 2,995).
- **Silver** (`databricks/silver/_merge_dims` library, `merge_dims`
  production): typing from Bronze strings (zip padded to five digits, ints,
  timestamps, session time zone pinned to UTC); a per-night contract (no NULL
  or duplicate keys, no value that fails its cast, nothing ever filtered out);
  the **5% delete circuit breaker**; the MERGE with `WHEN MATCHED AND NOT (all
  columns <=>)`, `WHEN NOT MATCHED`, `WHEN NOT MATCHED BY SOURCE DELETE`;
  lineage columns (`_first_seen_dump_date`, `_last_changed_dump_date`,
  `_source_file`); `silver.dims_merge_log` (one line per applied night); tables
  created with Change Data Feed on and 30-day file retention. Four checks: MERGE
  counts vs the generator's pandas report, Silver vs the newest night on every
  column both ways, the change feed vs the log, a no-op re-run.
- **`exp_03`** (deliberate failure #2 of six), parts A–F; part **G** (a bad
  night refused before its MERGE, then applied once corrected) added at the
  end — `apply_dim` / `plan_nights` / `create_merge_log` take a `log` table so
  it runs the production function on scratch schemas.
- **`exp_02`** (deliberate failure #3 of six; due in Session 6 and missed —
  incident), parts A–E, on scratch Bronze tables with production's `write_batch`.
- **Experiment tracker** at the top of this file; `CLAUDE.md` start-of-session
  item 5 reads it.
- Tests: 139 → **146** (write-once: identical re-run writes nothing, extending
  by one night writes only it, a history-changing run writes nothing and names
  the night, `--delete-from` keeps earlier nights byte-identical and the history
  is reproducible from one command, `--redeliver` is per night; plus the zip
  defect pinned on purpose). `test_notebooks` skips a tracked file deleted in the
  working tree. The empty stub `merge_dims_nmbs.py` replaced by `merge_dims.py`.

### Verified
- **Guards fail when removed:** with write-once taken out in memory, 5 of the
  new tests go red; with `first_night` ignored, 2 do.
- **Rehearsal on a byte-identical local copy** (all 12 S3 ETags = local MD5s):
  the old command (no `--delete-from`) **refused, naming 8 landed files, local
  checksums unchanged**; the real command wrote 3 files, 12 `identical`.
- **Live landing:** the same table line for line; afterwards all 12 old objects
  have the **same ETag and the same LastModified (2026-09-27 19:00)** — not even
  re-uploaded; 3 new objects byte-identical to the rehearsal. Night 5: customer
  43,690 (+21,212 new, 33 changed, **19 gone**), product 32,851 (50 changed,
  **100 gone**), seller 2,995 (50 changed, **100 gone**).
- **Bronze:** one `WRITE` per table, `dump_date IN ('2017-12-28')` only (customer
  v6); all three tables equal the landing zone and the pinned counts; a re-run
  wrote nothing (5 → 5 WRITE commits). The 12 old files were not re-read.
- **Silver, first run:** 15 MERGEs, one per dimension-night; **all 15 equal the
  generator's report** (e.g. customer 2017-12-28 `(21212, 33, 19)`, seller
  `(0, 50, 100)`); Silver equals night 5 on every column of every row
  (`extra=0 missing=0`, 43,690 / 32,851 / 2,995 rows, keys unique); **0 zips
  not five digits** after padding 9,572 customer and 1,002 seller zips; the
  change feed equals the log on all 15 versions, every update paired; a re-run
  wrote nothing (versions and 15 log rows unchanged).
- **Zip padding is lossless** (laptop, raw files read as text): 43,690 / 43,690
  and 2,995 / 2,995 padded zips equal the original.
- **`exp_03`:** A bug present — `+0 ~50 -0`, 3,095 rows, **100 stale sellers**
  with their old values, no error; B fix — `-100`, Silver = night; C —
  `0 / 0 / 0`, **version still 2 → 3**; D — one row set aside → deleted, and
  re-applied as a *new* row (first seen 2017-08-30 → 2017-12-28); E — breaker
  refused **1,643 of 3,095 (53.1%)**, nothing written; off → 1,643 deleted, no
  error; F — **2,995 updates** vs 50 for the same final table.
- **`exp_02`:** A append — night **5,989** rows, **2,994 sellers twice**, the
  retracted seller still there, no error; B overwrite without a predicate —
  table = that one night, **3,095 rows of the other night deleted**, recorded as
  `CREATE OR REPLACE TABLE AS SELECT`; C `replaceWhere` — `{3,095, 2,994}`, the
  retracted seller gone for free, the other night identical, 2,994 rows
  written (no copying: separate files); D — the folder check **fired for the
  first time**; bypassed, the wrong night was replaced and **100 sellers
  vanished from the table**; E — Delta refused a stray row
  (`DELTA_REPLACE_WHERE_MISMATCH`), nothing written.
- **`exp_03` G (2026-10-01):** a night with three planted problems was refused
  before its MERGE, naming all three (`bad__seller_zip_code_prefix: 1`,
  `bad__dim_updated_at: 1`, `duplicate_keys: 1`); Silver stayed at the clean
  night (version 1), nothing logged for the bad night; the corrected night was
  applied by the next run (`~50 -100`, version 2). **`merge_dims` re-run after
  the `log` parameter change:** nothing new, all checks pass, versions and the
  15 log rows unchanged.
- `ruff` clean, `pytest` 146 passed.

### Built but NOT verified
- **Deletion vectors** as the reason a MERGE copies no unchanged rows
  (`numTargetRowsCopied: 0`; exp_03's 50-row update added one 6,118-byte file;
  the one-row delete added 0 files). Consistent with them, not checked:
  `SHOW TBLPROPERTIES` for `delta.enableDeletionVectors`.
- **The correction path** (a re-delivered newest night is re-applied; an older
  corrected night is reported, not applied) — written, never driven.
- **The contract check refusing a night** (bad cast, duplicate key) — never
  given a bad night; the breaker was (exp_03 E).

### Not done
- `exp_02` was listed here as "never scheduled" — then built and run the same
  session, after the human asked why it was missed (incident).
- Silver's `_merged_at` / `applied_at` are run times by design; nothing yet
  reads them.

### Incidents (4 new entries, two deliberate)
1. Zip prefixes lost their leading zero in every landed dump (fixed in Silver,
   verified lossless).
2. **DELIBERATE, exp_03**, results above.
3. `exp_02` was never scheduled: the six experiments had no session binding,
   and Drill 1 attacked `replaceWhere`'s success path only. Tracker added.
4. **DELIBERATE, exp_02**, results above.

### Decisions (7 new, all Session 8, plus one revision)
Write-once landing + `--delete-from`; apply nights one at a time with a merge
log; MERGE updates only real changes, deletes by absence, change feed on; a bad
value fails the night, a snapshot source is never filtered; the 5% delete
breaker; zips restored in Silver, not in the generator; alarms in Databricks
now (Job + freshness alert), Airflow later. **Revised at the human's
question** ("isn't refusing the whole night too much?" — yes): hold the bad
row, refuse the night only above 1% or on broken keys — built in Session 9.

### Unexplained, recorded rather than guessed
- **`silver.seller` v5 is an `OPTIMIZE` with `auto: true`, 2 s after the
  night-4 MERGE** (21:58:12 → 21:58:14). Product's night 5 is also v6 (its v5
  not read, presumably the same); customer's is v5 — no extra commit. That
  timing looks like **auto compaction** (Delta compacting straight after a
  write), not Predictive Optimization (Session 7: 80 minutes later, run by a
  service principal). If so, Drill 1's note crediting an `auto: true` OPTIMIZE
  two seconds after a write to Predictive Optimization was the same thing,
  misattributed. The row's `userName` would settle it; not read.
  `merge_night` reads "the MERGE after version N", so the extra commit was
  harmless — reading "the latest version" would have logged the OPTIMIZE.
- **`silver.customer` was at version 7 on 2026-10-01** (night 5 was v5;
  nothing of ours wrote since). Two commits not made by this project —
  probably Predictive Optimization (an OPTIMIZE, or a VACUUM, which records
  START and END as two versions). Not read; hands-on list.

### Cost
Databricks $0. AWS: 3 new objects (~6.7 MB), a few GETs for the write-once
comparison — cents. Confluent: nothing.

### Learning check
**Skipped by choice, logged.** The human (2026-10-01): "am skipping the Qs
for now … will address later … focusing on finishing this project sooner."
A plain-words summary was given twice (the second slower, on request) and
the three lessons re-explained with a three-seller example; no question was
answered. Six prepared questions parked in `learning.md` as **B20–B25**
(status `parked` — answerable now, set aside by choice; not `deferred`).

### Hands-on checks (offered, not yet done)
- `DESCRIBE HISTORY workspace.silver.customer` — what are v6 and v7?
  (unexplained above); and `userName` on `silver.seller` v5 (auto
  compaction vs Predictive Optimization).
- `SHOW TBLPROPERTIES workspace.silver.seller` — is
  `delta.enableDeletionVectors` true? (the "0 rows copied" explanation).
- `SELECT * FROM table_changes('workspace.silver.seller', 6)` — the 100
  `delete` rows of night 5, as the Gold export will see them.
- Catalog Explorer → `workspace.silver` — four tables, CDF on, lineage
  from `bronze.*`.

### Next
**Session 9 — Silver inventory CDC: `MERGE` on op flags, in-batch dedup, `seq`
guard.** The other side of today's lesson: in CDC, absence means *nothing*,
so `NOT MATCHED BY SOURCE` would be catastrophic there, and a duplicate key is
normal (dedup), where today it was a broken file (fail).
- Deferred D1 and D2 become answerable (both bound to S9).
- **The topic has no `D` events** (generator `--delist-count 0`): drive the
  delete branch on purpose, the way night 5 drove today's.
- The 43 poisoned SKUs (tied `seq`) and the denylist
  (`ops/incidents/2026-09-26_inventory_cdc_denylist.json`) meet Silver here.

**Also Session 9, decided at the end of Session 8 (decisions.md):**
- **Silver dims: hold the bad row, not the night.** Every row stays in the MERGE
  source with `_ok`; `UPDATE` / `INSERT` only when `_ok`; a rejected-rows table;
  a `held` count in the merge log; the whole night still refused above **1%**
  bad rows, on any duplicate or NULL key, or on an empty file. Rewrite
  `exp_03` G for the new rule, plus a part **H**: one bad row held, the night
  applied, that seller **not deleted**.
- **Alarms, in Databricks:** `autoload_dims` + `merge_dims` as a scheduled
  **Job** with a failure email (Free Edition support unverified — check); a
  **SQL Alert** on `silver.dims_merge_log`: no night applied in 26 hours.

**Carried, each a claim to re-check:**
- **For the Gold window:** Silver's change feed emits `delete` rows; the Session
  6 export plan (`insert` / `update_postimage` only) would drop every deletion.
  Also: dbt snapshot `hard_deletes` must be decided, or Gold keeps the 219
  deleted rows as current forever — today's bug, one layer up.
- Deletion vectors (above); the OPTIMIZE `userName`.
- CI: the Actions log for the producer tests (since Drill 1) — still not seen.
- Topics delete their oldest messages from ~**2026-12-25**; the Git-folder
  token expires the same day.
- Read-only Kafka identity for Bronze; dev-only Kafka credential.
- Scratch leftovers: `bronze.exp02_{append,overwrite,replacewhere,mislabelled,stray}`
  and `silver.exp03_{bug,fix,truncated,all}` (rebuilt by each
  experiment run), plus Drill 1's list above.

---

## Session 9 — Silver inventory CDC: the MERGE where absence means nothing
Date: 2026-10-01 → 10-02

**In plain words:** Silver now holds the current stock of every SKU (one
product sold by one seller), built from the inventory change feed: each change
says "this SKU's stock went from X to Y" with an order number (`seq`) and a
flag (`I`nsert / `U`pdate / `D`elete). The opposite of Session 8: here a SKU
missing from a batch means *nothing changed*, deletes arrive as `D` events, and
two rows for one SKU in a batch are normal (keep the newest). The 53 events the
test suite wrongly put on the topic in Session 6 are dropped by their published
denylist, and a new guard refuses any batch where two different events claim
the same place in a SKU's order — all 53 would have tripped it. To give the
delete clause real work, 100 delist events were sent to the live topic after
Silver's first build, and Silver deleted exactly those 100 SKUs. On the way,
the laptop answer key found that the feed's own "stock before / stock after"
disagree with its `seq` order on 237 SKUs — every check had passed because all
of them read deltas. Then the two items carried from Session 8: Silver dims now
**hold** a bad row instead of refusing the whole night, and the pipeline has
alarms — a scheduled Job that emails on failure and two SQL alerts — each one
seen firing, by email.

### Scope, as it actually ran
Planned (Session 8's `### Next`): Silver CDC with dedup and the `seq` guard,
drive the delete branch, the denylist; then dims hold-the-row and the alarms.
All done. Added: the answer key (laptop, pandas) before any Spark, which found
the chain defect (incident); the tie guard; the event log; a revision of the
alarm plan (a calendar alarm would never be green here).

### Start-of-session re-check (read-only)
The full CDC log regenerated on the laptop as the answer key: 158,346 events,
34,448 SKUs, stock 993,982, units sold 112,650; with 100 delists 34,348 SKUs,
991,530. Adding delists changes no other event (same ids, same `seq`).
**1,423 broken before/after links on 237 SKUs** (incident).

### Built
- **Generator:** `--delists-only` (builds every event, sends only the `D`s;
  scope header `full;only=D`); `run_scope(limit, only=…)`; module docstring
  states the unheld invariant. Tests 146 → **152** (delists-only sends only
  `D`s, changes no other event, refused without a count, CLI writes only
  deletes, scope string; the chain defect pinned by a constructed SKU).
- **Silver CDC** (`databricks/silver/_merge_inventory_cdc` library,
  `merge_inventory_cdc` production): a Delta stream from Bronze
  `inventory_cdc`, `foreachBatch`, `AvailableNow`, checkpoint in the new volume
  `workspace.silver.checkpoints`, generation `v1`. Per batch: denylist → one
  copy per `event_id` → tie / bad-op refusal → insert-only MERGE into
  `silver.inventory_events` → newest event per SKU → the three-clause MERGE
  into `silver.inventory` with `s.seq > t.seq` on **all three** clauses →
  chain breaks counted → one line in `silver.inventory_cdc_log`
  (`txnAppId`/`txnVersion`). CDF on, 30-day retention, `change_reason` not
  carried. Five checks: Bronze rows seen once, event log = Bronze − denylist;
  stock vs the laptop answer key and vs a batch window query over the log;
  the chain baseline; change feed vs log; the tie guard over all of Bronze
  with and without the denylist. Plus a no-op re-run.
- **Bronze `stream_cdc`:** checks expect the delists by their provenance
  label (every labelled message a delist, every delist labelled).
- **Silver dims, hold the bad row** (`_merge_dims`): `_ok` per row; update and
  insert need `s._ok`, the delete clause unchanged — a held key is present, so
  never deleted; `silver.dims_rejected_rows` (one row per key and column, raw
  text, `replaceWhere` per dimension and night); `held` in the merge log
  (`ALTER TABLE ADD COLUMNS`; older lines NULL); night refused whole only for
  > 1% bad rows, a NULL or duplicate key, or no rows. `merge_dims` reports held
  rows; Verify 2 leaves held keys out.
- **exp_03 G rewritten, H added:** G1 a duplicate key, G2 2% bad zips — both
  refused whole; H one bad zip held, the night applied, then the corrected
  file re-delivered.
- **Alarms:** Job `ledgerline-silver` (id 788110523503206): `autoload_dims →
  merge_dims`, `stream_cdc → merge_inventory_cdc`, serverless, daily 06:00
  IST, email on failure. SQL Alerts `ledgerline silver behind bronze`
  (`sources_behind > 0`) and `ledgerline dims rows held` (`rows_held > 0`),
  daily 07:00 (set at the end; not seen in a screenshot); queries versioned
  in `databricks/alerts/`. Test notebook
  `alarm_test` kept in the user folder (outside git) for Drill 2.

### Verified
- **Silver CDC, first build (batch 0, one micro-batch):** `bronze_rows
  158,625 · denied 159 · duplicate_copies 120 · events 158,346 · skus 34,448 ·
  inserted 34,448 · updated 0 · deleted 0 · chain_breaks 1,423`. Event log
  158,346 rows = distinct ids, 0 denylisted. Stock **34,448 / 993,982** = the
  laptop; `extra=0 missing=0` against the window recomputation. Chain report
  **1,423 / 237 / 26 SKUs / 27 units**, units sold **112,650**. Change feed v1
  `(34,448, 0, 0)` = log. **Tie guard:** without the denylist **53** ties in the
  batch and **53** against the log — every contaminated event sits exactly on a
  clean event's `seq`; with it `{}`. Re-run: commits 2/2/2 unchanged.
- **Delist produce + delete branch:** Bronze read 100 new messages in one
  batch (topic end 158,725: +35 / +33 / +32 per partition), 0 quarantined,
  `deletes = labelled_rows = delist_scope_rows = 100`, distinct 158,499, clean
  158,446, units still 112,650; re-run nothing. Silver **batch 1:** `100 rows ·
  0 denied · 100 events · 100 newest are D · inserted 0 · deleted 100 · chain
  breaks 0`; stock **34,348 / 991,530** = the laptop; change feed v2
  `(0, 0, 100)`; chain report unchanged (no delisted SKU is a broken one, as
  predicted); re-run commits 3/3/3 unchanged.
- **Dims, production run of the new code:** only `added column held`; nothing
  new ×3; held 0/0/0; all 15 nights = the generator; Silver = night 5 on every
  column; feed = log; re-run versions `{7, 6, 8}` and 15 log rows unchanged;
  rejected-rows table empty.
- **exp_03 (A–F unchanged, then):** G1 `{'duplicate_keys': 1}` refused; G2
  `{'bad_rows': '60 of 2,995 (2.0%)', 'bad__seller_zip_code_prefix': 60}`
  refused; Silver kept the clean night, nothing logged; corrected night
  `+0 ~50 -100`. **H:** `+0 ~49 -100, held 1`; seller `056b4ada…` still in
  Silver with its old values (zip 26379, queimados/RJ, last changed
  2017-08-30); one rejected row `ABCDE`; the corrected re-delivery (newer file
  time) re-applied the newest night: `+0 ~1 -0, held 0`, rejected rows cleared
  — **Session 8's correction path, driven for the first time.**
- **Alarms, each seen firing by email:** the Job ran green (4 tasks, 5 m 58 s);
  a deliberate failing task (`alarm_test`) → email from
  `prod-monitoring@databricks.com` within a minute, naming the task; each SQL
  alert, set to `>= 0` and run → **Triggered** + email from
  `noreply@databricks.com`; both queries return 0 on today's data; set back
  to `> 0` → OK. **Free Edition sends both kinds of email** (was unverified).
- `ruff` clean, `pytest` 152 passed.

### Built but NOT verified
- **The CDC MERGE's update clause and its "older event ignored" path have
  never run on live data:** the first build was one batch (every SKU new →
  insert), the second only deletes. exp_04 (S10) drives both.
- **The tie guard inside the stream** never raised `BatchRefused`; only the
  same function, read-only, over Bronze (Verify 5).
- **The alarms on a real condition:** both SQL alerts were fired by the `>= 0`
  trick, not by a real lag or a real held row in production (H's held row was
  in a scratch log).
- **An older corrected night is reported, not applied** — still never driven.

### Not done
- The delist run's `run id` was not recorded here; it is in Bronze:
  `SELECT DISTINCT cast(try_element_at(map_from_entries(_kafka_headers),
  'ledgerline.run_id') AS STRING) FROM workspace.bronze.inventory_cdc WHERE op = 'D'`.

### Incidents (1 new)
1. The CDC feed's before/after images break their own `seq` order on 237
   SKUs; every check passed because every check read deltas. Fixed in Silver
   by counting, not repairing; the generator pinned.

### Decisions (7 new, plus one revision)
Silver CDC: one stream, two tables, every write idempotent by its own
condition; mirror the source's after-images and count the broken chain; hard
delete with the `seq` guard (late update after a delete bound to exp_04); a
`seq` tie fails the batch; 100 delists on the live topic after the first
build; held dims rows replaced per night; **the coverage map** — every
Databricks / Snowflake / dbt topic bound to a session as built or theory
(`docs/coverage.md`, at the human's request: "a strict plan now, nothing to
skip like exp_02"). **Revision:** the "nothing happened" alarm watches Silver
falling behind Bronze, not the calendar — nights land by hand here, so a
26-hour calendar alarm would never be green.

### Seen, recorded rather than guessed
- **A failed serverless job task ran twice** ("2 attempts") with no retry
  configured — presumably a default retry on serverless. Harmless here: every
  task is safe to repeat (Bronze `txnVersion`, Silver's idempotent MERGEs, the
  dims merge log). It also means a refused night fails twice before the email.
- **`silver.seller` v7 `SET TBLPROPERTIES`
  (`delta.workloadBasedColumns.deltaFileStatistics = seller_zip_code_prefix`)
  and v8 `COMPUTE STATS`, 16 s apart** — commits nobody here made; probably
  Predictive Optimization choosing a column our queries filter on (the zip
  format check) and collecting statistics. The likely answer to Session 8's
  "two commits on `silver.customer`"; `userName` still not read.
- **"Run task" (▶ on one task) disables the other tasks for that run**; the
  failure email still fires for the job.
- The new SQL alert editor runs the **saved** alert, not unsaved edits; its
  column list is empty until the query has run in the editor.

### Cost
Databricks $0 (Free Edition; Job, serverless tasks, SQL warehouse for the
alerts). Confluent: 100 messages. AWS: nothing new.

### Learning check
**Skipped by choice, logged.** The human, 2026-10-02: "skip the test" — the
priority is a strict plan and finishing the project. A plain-words summary was
given twice (the second longer, with examples, on request), then the real row
shapes of the event log and the stock table. Five questions were prepared and
not answered; they are parked in `learning.md`: the CDC MERGE from memory
(**B3**, already in the bank, missed S4), and **B26–B29** — in-batch dedup in
PySpark, `NOT MATCHED BY SOURCE` on today's 100-row delete batch (was deferred
**D1**), two rows for one SKU without dedup (was deferred **D2**), and why the
112,650 tie-out could not see the broken chain. D1 and D2 move from `deferred`
to `parked`: Session 9 made them answerable.

### Hands-on checks (offered)
- `DESCRIBE HISTORY workspace.silver.inventory` — v1 MERGE (34,448 inserted),
  v2 MERGE (100 deleted): `numTargetRowsInserted` / `numTargetRowsDeleted`.
- `SELECT * FROM table_changes('workspace.silver.inventory', 2)` — the 100
  `delete` rows, as the Gold export will see them.
- `SELECT * FROM workspace.silver.inventory_cdc_log` — the two batches.
- One broken SKU, by `seq`: `SELECT seq, op, prev_stock_qty, stock_qty FROM
  workspace.silver.inventory_events WHERE sku_key LIKE 'eba7488e%' ORDER BY seq`
  — the 86 that appears before the restock that makes it.
- `DESCRIBE HISTORY workspace.silver.seller` — `userName` on v5 (OPTIMIZE) and
  v7/v8 (stats): Databricks or us?
- Jobs & Pipelines → `ledgerline-silver` → Runs (the green run, the failed
  `alarm_test` run, tomorrow's 06:00); Alerts → both alerts' run history.

### Next
**Session 10 — Silver orders (`MERGE INTO` on `order_id`), and `exp_04`.**
The last Silver pattern: order events fan out (`created` / `approved` /
`shipped` / `delivered`), so an order arrives and then updates three times —
the MERGE that makes the S6/S7 order pipeline mean something. Silver is then
complete, and Drill 2 follows. **Every S10 row of `docs/coverage.md` is bound
here** — built this session or carried by name, never dropped:
- **Silver orders** — `MERGE INTO` on `order_id`, the latest lifecycle state per
  order, dedup on `event_id`.
- **`RESTORE` / repair by time travel** — ends exp_04's contamination part with
  the production fix (Session 6 named it as the alternative to recreating a topic).
- **A concurrent-write conflict** — two MERGEs into one table at once, the
  exception Delta raises, and what isolation level allows (a CLAUDE.md danger
  zone never triggered).
- **Delta `CHECK` / `NOT NULL` constraints** — enforced, unlike Snowflake's
  primary key (15 minutes).
- **Theory hooks** (end of session, `learning.md` C11): classic clusters (sizing,
  autoscaling, spot, job vs all-purpose); Kafka internals and log compaction;
  Debezium in operation.
- **`exp_04` (due S10, by name)** — on scratch tables with production's
  `make_batch_writer` switches (`dedup`, `seq_guard`, `refuse`):
  - no in-batch dedup: a batch with two rows for an *existing* SKU → Delta
    refuses ("multiple source rows matched"); for a *new* SKU → inserted twice,
    no error (B25 / D2, unverified expectation);
  - no `seq` guard + out-of-order emission → an older event clobbers newer stock;
  - **a late update after a delete resurrects the SKU** (hard delete forgets
    the `seq`) → decide tombstone vs ordering assumption with evidence;
  - the update clause and "older event ignored" path, which live data has never
    exercised;
  - deliberate contamination (Session 5's partial run) → the tie guard fires
    *in the stream*, the job fails, the email arrives, a denylist entry fixes it
    — the production response end to end, closed by `RESTORE`. Decide first:
    live topic (the S6 plan) or a scratch topic.
- Parked questions from S9 (B3, B26–B29) are fair game where S10's work touches
  them (exp_04 answers B28 by running it).

**After S10:** Drill 2, then S11 → S21 exactly as `docs/coverage.md` section 6
lists them. Gate before S13: every Databricks-only row done before the
Snowflake trial starts.

**Carried, each a claim to re-check:**
- Confirm the `alarm_test` task is gone from the Job (else every 06:00 run
  fails) and both alerts have their daily schedule.
- **For the Gold window:** Silver's change feed emits `delete` rows on dims
  *and* inventory now; the Session 6 export plan (`insert` /
  `update_postimage` only) would drop every one. dbt snapshot `hard_deletes`
  still to decide.
- Deletion vectors (`SHOW TBLPROPERTIES`); the OPTIMIZE / stats `userName`.
- CI: the Actions log for the producer tests — still not seen.
- Topics delete their oldest messages from ~**2026-12-25**; the Git-folder
  token expires the same day.
- Read-only Kafka identity for Bronze; dev-only Kafka credential.
- Scratch leftovers: `workspace.exp03_{bronze,silver}`,
  `workspace.exp03h_{bronze,silver}` (rebuilt by each exp_03 run), plus the
  Session 8 and Drill 1 lists.
