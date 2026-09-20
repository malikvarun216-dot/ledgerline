# Progress

Chronological session log. Updated at the end of every session with what was
**actually verified** — not merely what was built.

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
