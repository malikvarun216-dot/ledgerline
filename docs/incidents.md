# Incidents

Every bug and every **deliberately triggered** failure gets an entry, written
*when it happens* — not reconstructed later. Six of the entries in this file
will be experiments we broke on purpose to observe the failure mode.

Entries are append-only. Never edit a past entry to hide a wrong turn —
append a correction below it instead.

**Format:**

```
## [YYYY-MM-DD] — <short title>
- What happened: symptoms observed
- What I thought was wrong: the hypothesis before finding root cause
- Root cause: what was actually wrong
- Fix: what solved it
- Prevention rule: the guard that stops it recurring
- Lesson: one line, interview-ready
```

**Interview template this feeds:**

> "The hardest bug was [incident]. Symptoms: [X]. I initially thought [Y],
> but the root cause was [Z]. Fix: [W]. Now I always [prevention]."

---

## [2026-09-19] — `!data/.gitkeep` negation silently ignored under `data/`
- What happened: `.gitignore` had `data/` followed by `!data/.gitkeep`, intending to keep the empty directory in the repo while excluding its contents. `git status` never showed `data/.gitkeep`, and `git check-ignore -v data/.gitkeep` reported it as ignored by the `data/` rule.
- What I thought was wrong: negation ordering — that `!data/.gitkeep` needed to come before `data/`, or that the rules were conflicting.
- Root cause: **git does not descend into an excluded directory.** When a *directory* is excluded (`data/`), git stops walking it entirely, so no negation pattern for anything inside can ever be evaluated. Ordering is irrelevant — the rule is structural.
- Fix: exclude the directory's *contents* instead of the directory itself — `data/*` followed by `!data/.gitkeep`. Git still descends into `data/`, so the negation is reachable.
- Prevention rule: when a `.gitignore` negation is meant to rescue a file inside an ignored directory, exclude with `dir/*`, never `dir/`. Verify every negation with `git check-ignore -v <path>` rather than trusting `git status` silence — an absent file looks identical to a correctly-ignored one.
- Lesson: `git status` showing nothing is not proof a rule works. `check-ignore -v` names the exact line that matched, which is the only way to tell "correctly ignored" from "silently unreachable".

## [2026-09-19] — dimension dumps drained to zero rows; deletion set was not actually deterministic
- What happened: first end-to-end run of `generators/dim_dumps.py` with `--delete-per-night 1` over 5 nights. The customer dimension went 2 → 1 → 1 → 0 → 0 rows and the seller dimension went 2 → 1 → 0. Every dimension emptied. The customer dimension should have *grown* over time, since people enter it as they place their first order.
- What I thought was wrong: that `--delete-per-night 1` was simply too aggressive for a 4-row fixture — a scale artifact, not a bug.
- Root cause: two things, and the scale only made them visible. (1) `run()` passed `frame[spec.key].tolist()` — **tonight's** rows — as the pool to sample deletions from. `deleted_keys` replays nights 1..N to build the cumulative set, so when the pool changes between nights (the customer dimension grows every night), the replay of night 2 samples from a different pool each time and selects **different keys**. The set was never cumulative and never deterministic; it was recomputed wrongly on every night. (2) There was no floor: nothing stopped the cumulative set from covering every key in the dimension.
- Fix: pass the **full key universe**, fixed once before the first night (`sorted(set(timeline[CUSTOMER.key]))` etc.), so the replay is stable regardless of which rows exist tonight. Added `MAX_DELETED_FRACTION = 0.25` capping the cumulative deletion set, so a dimension can never be emptied. On a 2-row dimension the cap rounds to 0 and deletion is correctly refused.
- Prevention rule: **any value derived by replaying a sequence must be derived from a pool that does not change between replays.** If a function loops `for n in range(1, night+1)` to rebuild state, every input to that loop must be fixed for the whole run, not read from the current iteration. Assert determinism directly: same seed + same night + a reordered or duplicated pool must give the same answer (`test_deletion_ignores_which_rows_happen_to_exist_tonight`).
- Lesson: "deterministic from a seed" is a claim about the *whole* input, not just the seed. A stable seed feeding a drifting pool is not deterministic, and the failure is invisible until something in the pool grows.

## [2026-09-19] — generator reported synthetic changes its own output did not contain
- What happened: same run. The dump summary printed `changed = 0` for products on every night while `--change-rate 1` was set, and the generator's internal report simultaneously claimed `synthetic_changes = 1`. Two numbers from the same run disagreeing.
- What I thought was wrong: that the change model wasn't firing — that `apply_synthetic_changes` was returning the frame untouched.
- Root cause: it fired correctly. `run()` applied synthetic changes **first** and deleted rows **second**, so a row could be edited and then removed from the same dump. The edit was counted as a change and then thrown away. With a 4-product fixture and a growing deletion set the collision went from likely to certain.
- Fix: reordered to delete first, then mutate the survivors. A reported change now always lands in the file.
- Prevention rule: when a pipeline stage both **filters** and **transforms**, filter first. A transform applied to rows that a later step discards is work that cannot be observed, and any count taken from it is a lie about the output. Where both counts are reported, assert them equal (`test_changes_are_applied_after_deletions_not_before`).
- Lesson: when a generator's own summary disagrees with the artifact it produced, trust the artifact and go looking for a discarded write. The summary is describing intent; the file is describing what happened.

## [2026-09-19] — first real download: 34,448 SKUs collided on one seed timestamp
- What happened: first run of `generators/inventory_cdc.py` against the real 99,441-order Olist dataset. It crashed: `RuntimeError: more than 1000 events at 1472937319000000 — widen the seq tiebreaker`. The 4-SKU fixture never surfaced this.
- What I thought was wrong: initially read as `assign_seq`'s guard being too conservative for real data — that 1000 slots per microsecond just wasn't enough headroom.
- Root cause: it was not a headroom problem, it was a modeling problem. Every SKU was seeded at one *shared* global timestamp — `min(order_purchase_timestamp across the whole dataset) - 1 day`. The real dataset has 34,448 distinct `(product_id, seller_id)` pairs, so all 34,448 seed events landed on the exact same microsecond. `assign_seq`'s tiebreaker correctly refused to fabricate 34k distinct `seq` values for one instant — the guard did its job; the input to it was wrong.
- Fix: each SKU now seeds a day before **its own** first sale (`sku_sales["order_purchase_timestamp"].min()`), not before the dataset's global first sale. This is also the more honest model — a seller lists a product when they start selling it, not on day one of the whole marketplace — and it naturally spreads 34k SKUs across ~25 months instead of stacking them on one instant.
- Prevention rule: **a "seed" or "initialize" step for N independent entities must not share one timestamp** unless the domain genuinely guarantees simultaneity. A shared instant is both an unrealistic model and, whenever seq/ordering derives from time, a correctness bug waiting for N to cross whatever collision guard exists. Test the *shape* against real cardinality, not just fixture cardinality — the fixture had 4 SKUs and could never have hit a 1000-per-microsecond ceiling regardless of the bug being present.
- Lesson: a guard that fires loudly (`RuntimeError`) is doing its job even when the fix isn't in the guard. The instinct to widen the tiebreaker would have hidden a real design flaw behind a bigger number, and the flaw would still have been there — just quieter.

## [2026-09-19] — first real download: almost every product row falsely reported as "changed" every night
- What happened: same real-dataset run, `generators/dim_dumps.py --nights 6 --change-rate 25`. Night 2 reported `changed = 32,940` for the product dimension out of 32,941 rows total, although `--change-rate 25` should touch about 25 rows. Every subsequent night showed the same near-total "changed" count.
- What I thought was wrong: suspected `apply_synthetic_changes` was somehow applying to every row instead of the sampled 25 — the count looked like the change model itself was broken.
- Root cause: the change model was fine. `carry_forward_updated_at`'s hash comparison (`attribute_hash`) stringified each attribute and hashed the join — and `str(225)` (`int64`, the dtype produced by a fresh load) is not equal to `str(225.0)` (`float64`, the dtype `read_previous` gets back from a **plain** `pd.read_csv()` on last night's dump). Plain `pd.read_csv` upcasts an entire integer column to float64 the moment **any** row in that column is null — and real Olist's `product_weight_g` genuinely has scattered nulls. So virtually every product's weight/dimension columns silently flipped from `225` to `225.0` on every read-back, and every one of those rows looked "changed" even though nothing about it had. The 4-row fixture never had a null in a numeric column, so this was invisible in every test until real data supplied one.
- Fix: `attribute_hash` now normalizes any numeric value (Python or numpy int/float) to a canonical integer-string form before hashing, so `225`, `225.0`, `np.int64(225)`, and `np.float64(225.0)` all hash identically, while a genuine change (`226`, or a real fraction like `225.5`) still hashes differently.
- Prevention rule: **never hash or compare raw `str(value)` across a value that has crossed a CSV round-trip.** CSV has no integer type — every read-back is a re-inference, and pandas' inference depends on the *whole column's* content (one null anywhere upcasts everything), not just the row you're looking at. Normalize to a canonical form before comparing, and test the comparison with mixed dtypes directly (`test_attribute_hash_is_stable_across_int_and_float_representations`), not just through the pipeline that happens to produce one dtype today.
- Lesson: this bug was invisible through 84 passing tests and a full CLI smoke-test pass, because every one of them used data with zero nulls in the compared numeric columns. A fixture is only as strong as the data shapes it contains — a "successful" green suite over synthetic data proved nothing about a null-bearing column until the pipeline actually saw one. This is the same lesson as the two Session 1 incidents, at a different data shape.

## [2026-09-19] — the two blob sinks stored different bytes for the same input, on Windows only

**In plain words:** the generator writes its nightly CSVs through a thing called a *sink* - just "the place output goes". There are two: `S3BlobSink` writes to real S3 in the cloud, and `LocalBlobSink` writes the same files to a folder on the laptop so everything can be tested with no AWS account. The local one is supposed to be an exact stand-in. It wasn't. Windows ends a line with two invisible characters, Linux with one - and here **two separate pieces of code each added the line ending, so every row got a doubled one.** The local files came out with a blank line between every record; the S3 files came out correct. Same generator, same input, two different files. Nobody noticed because the only thing that ever read those files back was `pandas.read_csv`, which skips blank lines without saying anything.

- What happened: first execution of `S3BlobSink` (Session 1 wrote it; it had never run a line). A new test ran the dimension generator against `LocalBlobSink` and a `moto`-backed `S3BlobSink` and compared every key's content. The keys matched exactly; the **content did not**. The local sink read back `'...2017-01-31\n\n'` where S3 read back `'...2017-01-31\r\n'`.
- What I thought was wrong: an encoding mismatch in `S3BlobSink` — that `put_text`'s explicit `.encode("utf-8")` was doing something `LocalBlobSink` was not, and that the S3 side was the deviant one.
- Root cause: backwards. `S3BlobSink` was correct and `LocalBlobSink` was corrupting the file, through two composed translations nobody wrote:
  1. `pandas.to_csv()` terminates lines with **`os.linesep`** even when returning a string rather than writing to a path — so on Windows the generator's own output already contained `\r\n`.
  2. `Path.write_text` opens in **text mode**, which rewrites every `\n` to `\r\n` on the way out. Applied to a string that already contained `\r\n`, it produced **`\r\r\n`** on disk. `read_text`'s universal-newline decoding then collapsed `\r\r\n` back to `\n\n`.

  So every dump file `LocalBlobSink` wrote on Windows carried a stray CR and a blank line between every data row, and `S3BlobSink` — which encodes and decodes UTF-8 directly and translates nothing — did not. `decisions.md` claims `LocalBlobSink` "mirrors the S3 key layout exactly, so switching sinks changes nothing downstream". That claim was false, and had been false since Session 1.
- Why nothing caught it: `pd.read_csv` defaults to `skip_blank_lines=True`, so `read_previous` parsed the malformed file correctly and all 87 tests passed straight through it. The corruption was real but invisible to the only reader that ever looked at the file.
- The nastier half: it is **platform-conditional**. On Linux `write_text` performs no translation, so both sinks would have held `\r\n` and the comparison test would have been **green in CI and red on the developer's machine** — while the bytes landing in S3 still differed depending on who ran the generator.
- Fix: three places. `LocalBlobSink.put_text`/`get_text` now use `write_bytes`/`read_bytes`, so no translation layer exists at all. `JsonlSink` opens with `newline=""`. And `to_csv` pins `lineterminator="\n"`, so a dump's bytes no longer depend on the OS that produced it.
- Prevention rule: **a sink and its offline twin must be compared on content, not just on keys, and any file whose bytes are an interface must be written in binary mode.** Text mode is a silent, platform-conditional transform, and `write_text`/`read_text` round-trip cleanly on one machine — which is exactly what hides it. Where two implementations of one protocol exist, assert they are interchangeable directly (`test_local_and_s3_sinks_produce_byte_identical_dumps`) instead of asserting each separately against its own expectations.
- Lesson: "the round trip works" is a weaker claim than it sounds. `write_text` then `read_text` is self-consistent even while the file on disk is malformed, because the same translation runs in both directions. The bug only surfaces when a *second* implementation reads the same bytes without that translation. A local twin that is never byte-compared against the real thing is a twin by assertion only.
- Postscript: writing this entry reproduced the same bug one layer up — `Path.write_text` silently converted all of `incidents.md` from LF to CRLF, turning a 27-line append into a 165-line whole-file diff. Repaired by writing bytes. The prevention rule earns its keep on the first day it exists.

## [2026-09-20] - the `reviews` "dataset drift" was a line count, not a row count

**In plain words:** Session 1 recorded that the reviews table had 104,719 rows
when the contract expected 99,224, and wrote it down as Kaggle having
re-uploaded the dataset. It hadn't. The file has exactly 99,224 records. The
extra 5,495 are **line breaks inside customer review comments** - someone typed
a comment, pressed Enter a few times, and the CSV stores that newline inside
the quoted field. Counting lines in the file counts those too. Counting records
does not.

- What happened: `docs/progress.md` (Session 1 addendum) records "`reviews` differs (104,719 actual vs 99,224 expected, +5,495) - Kaggle has re-uploaded this dataset before; recorded as drift, not a bug." It was carried forward as an open decision through Sessions 1, 2 and into 3.
- What I thought was wrong: nothing - that was the problem. It was recorded as an *external* fact about the upstream dataset, so there was nothing in the repo to suspect.
- Root cause: a physical **line** count was compared against a parsed **record** count. `review_comment_message` is free text, 3,852 rows contain at least one newline, and those newlines total exactly 5,495. `99,224 + 5,495 = 104,719`, to the row. A CSV record and a CSV line are only the same thing when no field contains a line break.
- Fix: none needed in code - the contract's `expected_rows=99_224` was correct from the start and `generators/olist.py` validates through `pd.read_csv`, which parses quoting properly. The fix is to the *record*: drift is withdrawn, contract stands.
- Why nothing caught it: the wrong number was plausible. Kaggle really does re-upload datasets, so "+5,495 rows" read as an upstream change rather than a measurement error - and an upstream fact gets recorded, not investigated. It also never contradicted anything: no test asserts on reviews, and `GENERATOR_TABLES` excludes it, so no code path ever disagreed with the claim.
- Prevention rule: **never compare a count produced by one parser against a count produced by another.** `wc -l`, `Get-Content | Measure-Object -Line`, and a text-editor line number all count physical lines; `pd.read_csv`, Spark and every real CSV reader count records. Where a row count is the assertion, produce both numbers with the *same* reader. And before attributing a discrepancy to an upstream source, reproduce it locally - an external explanation is unfalsifiable from inside the repo, which is exactly why it survives.
- Lesson: a plausible explanation is the most durable kind of wrong. "Kaggle re-uploaded it" cost nothing to believe and explained the number perfectly, so it sat in the docs for three sessions. The arithmetic that disproved it took one command.
- Consequence for Session 4: Spark's CSV reader defaults to **`multiLine=false`**, and Auto Loader inherits that default. Pointed at a file like this it splits those 3,852 comments across row boundaries and produces corrupted records with **no error raised**. The dimension dumps have no free-text columns so they are safe today, but any reader that later touches a text-bearing CSV needs `multiLine=true` - and paying for it, because `multiLine=true` forces the file to be read by a single task and kills parallelism.

## [2026-09-20] - `.env` was never loaded, so the S3 sink ran as the admin key it was built to avoid

**In plain words:** Session 3 created a locked-down AWS user, `ledgerline-dev`,
that can touch exactly one bucket and nothing else. It was tested and proved:
denied on EC2, denied on other buckets, denied on IAM. Its keys went into
`.env`. `docs/decisions.md` then recorded that the generator "authenticates as a
scoped IAM user, not the machine's admin key."

**No code ever read `.env`.** `python-dotenv` has been in `requirements.txt`
since Session 0 and every docstring says config "comes from `.env`", but
`load_dotenv()` was never called anywhere. So the scoped key sat in a file
nothing opened, and `boto3.client("s3")` - called with no arguments - walked its
default credential chain to `~/.aws/credentials`, which on this machine is
`varun-admin`: a never-expiring plaintext admin key.

The generator would have worked perfectly. As the wrong identity.

- What happened: found while preparing the first live Kafka produce, before any
  cloud call was made. `grep -rn "dotenv" generators/ scripts/` returned exactly
  one hit - a comment in `requirements.txt`. Nothing in `generators/`,
  `scripts/` or `tests/` calls `load_dotenv()`.
- What I thought was wrong: nothing yet - this was found by reading the code
  path ahead of running it, not by a failure. The Kafka half would have failed
  loudly (`RuntimeError: Kafka sink needs KAFKA_BOOTSTRAP_SERVERS...` against a
  fully populated `.env`), and chasing *that* is what surfaced the S3 half.
- Root cause: two separate assumptions that never met. `_kafka_config_from_env`
  reads `os.environ` and assumes something put `.env` there. `S3BlobSink` called
  `boto3.client("s3")` with no credentials and inherited boto3's fallback chain.
  Neither is wrong on its own; together they mean a correct `.env` changes
  nothing about which identity actually authenticates.
- Fix: `load_local_env()` in `generators/_common.py`, called from the `main()`
  of all three generators - **not** from the library functions, for a reason
  recorded below. Separately, `S3BlobSink` now builds its client through
  `_s3_client_from_env()`, which **raises** when `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` are absent rather than letting boto3 find something
  else. Region is passed explicitly too, defaulting to `ap-south-1`.
- Why nothing caught it: **Session 3 verified the credential, not the code path
  that uses it.** Every check ran the AWS CLI *as* `ledgerline-dev` and
  confirmed the policy denied what it should deny. All of that was true and none
  of it touched `S3BlobSink`. The 27 S3 tests inject a `moto` client through the
  `client=` parameter, so they never execute the credential branch at all - the
  one line that decides identity is the one line no test reached.
- The second, quieter defect: the first fix called `load_local_env()` *inside*
  `_kafka_config_from_env()`. That broke
  `test_kafka_config_names_every_missing_variable`, which deletes the four
  variables and asserts the error names all four - the function silently
  reloaded them from the real `.env` on disk and raised nothing. A library
  function that reads an untracked file makes its own error paths unreachable
  whenever the developer happens to have that file. Moved to `main()` only.
- Prevention rule: **a claim about *which identity* runs a call must be tested
  through the same constructor production uses.** Injecting a fake client is the
  right way to test behaviour and the wrong way to test authentication - the
  injection skips the decision. Where a default exists that is broader than the
  intended credential, delete the default: make the absent case raise, so the
  unsafe path cannot be taken silently. And **never call a config loader from a
  library function** - only from an entry point - or tests inherit the
  developer's machine.
- Lesson: `.env.example` carried the warning *"Leaving these blank falls back to
  ~/.aws/credentials, which on this machine is an ADMIN key"* since Session 3.
  It was accurate, prominent, and had no effect, because prose does not execute.
  The same sentence as a `raise` would have failed the first run. This is the
  Session 3 lesson at a different layer: a guard that depends on someone reading
  it is not a guard.

## [2026-09-20] - the first live produce hung for 180 seconds in silence; the port was the REST endpoint

**In plain words:** the very first attempt to send events to Kafka produced no
output at all and was killed after three minutes. The cause was one wrong
number: the bootstrap address ended in `:443` instead of `:9092`. Port 443 is
Confluent's **REST** endpoint - it speaks HTTP, not the Kafka wire protocol - so
the client sent a Kafka request and got an HTTP response back. It then read the
first four bytes of `HTTP/1.1 200 ...` as a message-length field, got
1,213,486,160, and complained that the message was too big.

That number **is** the four ASCII characters `H`, `T`, `T`, `P`. The error text
advised raising `receive.message.max.bytes`, which would have changed nothing
and sent the next hour in exactly the wrong direction.

- What happened: `python generators/order_events.py --sink kafka --limit 50`
  printed nothing for 180s and was terminated. No error, no partial output, no
  indication of which of three stages had stalled.
- What I thought was wrong: credentials or the brand-new grants - the service
  account had been given `CloudClusterAdmin` minutes earlier, so a permissions
  problem was the obvious suspect.
- Root cause: two independent problems, and the first one hid the second.
  1. `KAFKA_BOOTSTRAP_SERVERS` was `...confluent.cloud:443`. The cluster page
     shows a **REST endpoint** and a **Bootstrap server** one above the other,
     and both begin `lkc-2255zy2.ap-south-1.aws.pub...`, so they are easy to
     confuse when the visible part is identical. The broker is on **9092**.
  2. `KafkaAvroSink.flush()` called `Producer.flush()` with **no timeout**.
     librdkafka retries a transport failure indefinitely, which is right for a
     transient blip and useless for a misconfiguration - neither one ever
     raises. So the wrong port did not produce an error; it produced silence.
- How it was actually found: by probing the two endpoints separately rather than
  re-running the generator. A TLS handshake to the bootstrap host succeeded in
  0.22s and an authenticated `GET /subjects` returned `200 ["orders-value"]`.
  That second result was the real clue - the subject already existed, so the
  failed run had got as far as registering a schema and died *after* it.
  `Producer.list_topics(timeout=20)` then surfaced the actual librdkafka error,
  because unlike `flush()` it takes a timeout and raises.
- Fix: `:443` -> `:9092` in `.env`. Separately, `KafkaAvroSink.flush()` now takes
  a `timeout` (default 60s), checks the count of undelivered messages that
  `Producer.flush(timeout)` returns, and raises naming that count - and naming
  the port confusion, since it is the likeliest cause.
- Why nothing caught it: nothing could. `KafkaAvroSink.send` and `flush` are
  both marked `# pragma: no cover - needs a broker` and had never executed in
  three sessions. Session 2 deliberately verified the *schemas* offline with
  `fastavro` because `confluent_kafka` ships no mock Schema Registry, and that
  was the right call - it found a real bug. But it could not test transport, and
  transport is where this lived. The 116-test suite is silent on this code by
  construction, not by oversight.
- Prevention rule: **every blocking cloud call gets an explicit timeout, and the
  error must name the count or the endpoint.** A library that retries forever
  converts a configuration mistake into a hang, and a hang carries no
  information - it is the most expensive failure mode per byte of diagnostic.
  When a run produces no output, probe each hop independently with a short
  timeout rather than re-running the whole thing with a longer one.
- Lesson: **an error message can be confidently wrong.** "Invalid response size
  1213486160 - increase receive.message.max.bytes" is generated by code that has
  already assumed it is talking to a broker, so it can only describe the problem
  in broker terms. Decoding the number to `HTTP` took one line and identified
  the fault exactly; following the message's advice would have led nowhere. When
  a suggested fix looks disproportionate to the symptom, check whether the
  component reporting it is entitled to an opinion.

## [2026-09-20] — the CDC event count in three sessions of docs was 50 too high

**In plain words:** every document in this project says the inventory CDC feed
produces **158,396** events. It produces **158,346**. The generator has been
right the whole time; the number written down beside it was wrong by fifty,
from the first session that recorded it, and nothing ever re-derived it. It
reached Session 5's plan as a target to produce against.

- What happened: Session 1 ran the CDC generator over the full dataset and
  recorded "34,448 SKUs, 158,396 events" in `progress.md`. Session 4 quoted that
  figure twice more — once in its own "not verified" list, once in the Session 5
  plan — and `decisions.md` quotes it a fourth time inside the `client.id`
  entry. Session 5 ran the generator to confirm the target before producing to
  Kafka and got 158,346.
- What I thought was wrong: the generator. A 50-event drift against a recorded
  figure looks like non-determinism, which would be serious — `exp_01` asserts
  on replay producing byte-identical events, so a generator that drifts between
  runs invalidates the whole exactly-once experiment.
- Root cause: a transcription error. `158,346` → `158,396` is one digit, `4`
  to `9`. The generator's event-producing logic is **unchanged since the commit
  that first recorded the number**: `git diff` across the only two commits that
  ever touched `inventory_cdc.py` shows two additions, `load_local_env()` and a
  `client_id` argument, neither of which can alter an event count.
- Fix: none in code. The correction is appended to `progress.md` and the true
  figure is 158,346, which decomposes exactly: 34,448 seeds + 102,425 sales +
  21,473 restocks + 0 delists.
- Verified rather than assumed, because "the docs are wrong" is the convenient
  conclusion and deserved more resistance than "the code is wrong":
  two consecutive full runs produce **identical `event_id` ordering and
  identical `seq` ordering**, all 158,346 ids distinct, `seq` monotonic across
  the whole feed. The files' md5s differ — correctly, because `produced_at` is
  processing time and moves every run — which is why the comparison is on the
  id sequence and not on the bytes.
- Why nothing caught it: **no test asserts the total.** The suite checks
  reconciliation (units sold = units rebuilt from deltas), seq monotonicity,
  dedup behaviour and op-flag handling, all of which are true at any scale and
  all of which pass at 158,346 exactly as they would at 158,396. The count is
  the one property that appears only in prose. A number that lives solely in a
  document cannot be contradicted by a test suite, however green.
- Prevention rule: **a headline number that appears in a plan must be
  re-derived by running the thing, not copied from the entry that first said
  it.** The same shape as the CI carry-forward corrected at the end of Session
  4, with one extra turn of the screw: that number described an external system
  and so at least had an obvious owner to go and ask. This one was derivable
  locally in 28 seconds for three sessions and still nobody ran it, because a
  number already written down does not look like a question.
- Lesson: the reconciliation tying out — 112,650 units sold against 112,650
  rebuilt from CDC deltas — is the load-bearing invariant, and it was correct in
  Session 1 and is correct now. Getting the *important* number right is not
  evidence that the number next to it is right. They were written in the same
  sentence and only one of them was ever checked again.

## [2026-09-20] — a 50-order smoke run poisoned 43 SKUs on the live CDC topic, and `seq` cannot tell

**In plain words:** Session 4 produced a small 50-order CDC run to the real
topic to check the plumbing worked. Session 5 produced the full 99,441-order
run to the same topic. Those two runs disagree about how much stock 43 SKUs
started with — and they stamp that disagreement with the **same `seq`**, so the
one guard built to resolve exactly this kind of conflict cannot see it. Silver
will keep whichever arrived first and throw the other away without saying so.

- What happened: the full produce landed 158,346 CDC events on a topic that
  already held 93 from Session 4. Reading the topic back reported
  `distinct seq 158,346 of 158,439` — every one of Session 4's `seq` values
  already existed in the full run — and 43 of 34,448 keys carrying a tied,
  non-ascending `seq`.
- What I thought was wrong: the generator's `seq` derivation, or a restart
  resetting a counter. Both wrong, and the second is specifically the failure
  `assign_seq` was designed to be immune to. It still is: `seq` is derived from
  event time, not counted, so there is no counter to reset. That immunity is
  real and it is not what failed.
- Root cause: **a `--limit` run is not a prefix of the full run — it is a
  different dataset that shares keys and timestamps.** Seed stock is
  `headroom x lifetime demand`, and lifetime demand is measured over whatever
  orders the run was handed. The same SKU seeded from 50 orders gets
  `stock_qty=14`; seeded from 99,441 it gets `stock_qty=66`. Meanwhile `seq` is
  a pure function of event time, and event time does not depend on `--limit`.
  So the two events collide exactly:

      sku_key c1488892...|1554a685...   seq 1472937319000000000
        50-order run:  op=I seed  prev=None -> stock=14
        full run:      op=I seed  prev=None -> stock=66

  Measured: 53 colliding pairs across 43 SKUs, against 40 Session 4 events that
  *are* honest byte-identical replays.
- Why this is worse than a duplicate: Silver's guard is
  `WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET *` — **strictly** greater. A
  tie is not greater, so the MERGE matches the row and updates nothing. The
  correct full-run stock level is discarded in favour of the partial-run one
  already in the target, and a MERGE that updates zero rows is not an error. No
  exception, no log line, no row count that looks wrong. Just a stock level
  that is quietly 14 instead of 66, forever.
- Fix: `inventory_cdc.py` now **refuses `--limit` together with `--sink kafka`**
  unless `--allow-partial` is passed. The refusal names the mechanism rather
  than just saying no, and names the escape hatch, because Session 10 will want
  to contaminate the topic deliberately. Three tests cover it: that it fires,
  that the flag opens it, and that `order_events.py` deliberately has no
  equivalent.
- The asymmetry is measured, not assumed: **orders genuinely are a prefix.** An
  order's lifecycle is built from its own timestamps and does not depend on how
  many other orders were loaded, so all 203 records Session 4 left on `orders`
  are byte-identical duplicates — which is exactly why the full readback says
  394,293 records against 394,090 distinct ids. Adding the same guard to both
  generators would have looked consistent and been wrong.
- Why nothing caught it: three separate reasons stacked, and the third is the
  one worth keeping.
  1. **No Silver exists yet.** The guard that this breaks is written in Session
     9. The damage was done in Session 4 and would have surfaced five sessions
     later, against data nobody would still connect to a smoke test.
  2. **Every invariant the suite checks is scale-free.** Reconciliation,
     monotonicity within a run, dedup, op handling — all hold at 93 events and
     at 158,346. None of them is a statement about *two runs sharing a topic*,
     and a topic is not something a unit test has.
  3. **The verifier printed `seq monotonic overall: False` and that line was
     meaningless.** It is a file-shaped check: a JSONL file is written in emit
     order, so global ascent is fair. A topic is read three partitions
     interleaved and can never be globally ascending, whatever the data says.
     A check that returns `False` for healthy data trains you to ignore it, and
     it was sitting directly above the tied-`seq` evidence.
- Prevention rule: **never produce a partial run of a state-carrying source
  into a shared topic.** Append-only event sources (orders) tolerate it because
  a subset is a prefix. Any source that derives a value from the *scope* of its
  input — a seed, an opening balance, a high-water mark, anything computed from
  "all the data I was given" — does not, and the resulting events are
  indistinguishable from legitimate ones at the point where it matters. Second
  rule, from reason 3: **a health check must be capable of returning True.**
  Before trusting a check, confirm it passes on data known to be good;
  otherwise it is decoration that costs attention.
- Left on the topic deliberately, not cleaned up. The 43 poisoned SKUs are
  better material for Session 9 than a pristine feed would be: the in-batch
  dedup and the `seq` guard now have a real tie to fail on, and Session 10 can
  assert the bug is present before fixing it — which is a stated success
  criterion for this project. Recorded here so that a future session finds an
  explanation rather than a mystery.

### Correction (2026-09-26) — the 43 poisoned SKUs are no longer on any reachable topic

The entry above closes by saying the poisoned SKUs were "left on the topic
deliberately, not cleaned up", as material for Session 9 and 10. That was true
when written. It is no longer true, for a reason unrelated to the incident: the
Confluent account was lost and the project rebuilt on a new one, so the topic
holding them is unreachable. The rebuilt `inventory.cdc` reads **34,448 of
34,448 keys with ascending `seq`, zero ties**.

**Everything else in the entry stands.** The collision was real, measured at 53
pairs across 43 keys, and the mechanism is unchanged — a `--limit` CDC run is a
different dataset, not a prefix, because seed stock is derived from the scope
of the input while `seq` is derived from event time.

**The guard built in response is what matters now, and it survived the
rebuild:** `inventory_cdc.py` still refuses `--limit` with `--sink kafka`
unless `--allow-partial` is passed, and the three tests still cover it. The
defect cannot recur by accident on the new cluster.

**For Session 9/10 this is better, not worse.** Recreating the contamination is
now one deliberate command rather than an artifact being carefully preserved:

```
python generators/inventory_cdc.py --sink kafka --limit 50 --allow-partial
```

run before the full produce. That turns it into one of the project's six
planned deliberate failures, which is what it should have been in the first
place — the original was an accident that happened to be useful.

**Prevention rule, unchanged and now the whole value of the entry:** never
produce a partial run of a state-carrying source into a shared topic. The
evidence is gone; the rule and the guard are not.

<!-- Append further entries below this line. -->
