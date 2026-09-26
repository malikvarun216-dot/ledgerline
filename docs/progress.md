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
