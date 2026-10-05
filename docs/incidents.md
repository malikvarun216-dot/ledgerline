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

## [2026-09-26] — Databricks could not be subscribed on AWS Marketplace; the account is Indian and Marketplace rejects Indian cards

**In plain words:** Session 5 decided to pay for Databricks through AWS
Marketplace so its charges would show up in the AWS budgets. On the day, the
subscribe page refused with "You can't accept this offer without a valid
payment method". The card was fine. The account is billed by AWS's Indian
entity, and **AWS Marketplace does not accept credit or debit cards from Indian
accounts at all**. The plan was built on a payment route this account never had.

- What happened: `Try for free` on the Databricks listing reached the terms
  page, then showed *"You used an invalid or unsupported payment method during
  your last attempt to create an agreement"*. `Subscribe` stayed disabled.
  This was minutes after upgrading the account from the Free plan to the Paid
  plan, which removed the first blocker and exposed this one.
- What I thought was wrong: first, the Free plan, which does block paid
  Marketplace offers and was genuinely present. Fixing that was necessary and
  not sufficient. Second, a card problem on the user's side. Also wrong.
- Root cause: **Account → Service provider reads "Amazon Web Services India
  Private Limited"** (AISPL). Under RBI rules on payment aggregators storing
  card data, AWS Marketplace stopped accepting cards on file for AISPL
  customers in 2022. The only supported route is switching the account to
  **Pay By Invoice** through AWS Customer Service, which AWS says can take up to
  seven days to become usable.
- Why nothing caught it: every earlier AWS action in this project — S3, IAM,
  budgets — is a first-party AWS service, and those accept the card normally.
  Only a **third-party purchase** goes through the path that checks the
  billing entity, and this was the project's first. The Session 5 decision
  was researched against Databricks' and AWS's general documentation, which
  describes Marketplace billing as universally available; the India exception
  lives in a Marketplace blog post, not on the Databricks listing or the
  subscribe page. The error message itself blames the payment method and never
  names the billing entity.
- Fix: none applied yet — the payment route is a decision, recorded
  separately once made.
- Prevention rule: **before choosing a billing route, read the account's
  Service provider line.** Which legal entity bills the account decides what can
  be bought through it, and that is a one-line check on the Account page that
  no amount of product documentation substitutes for. More generally: a plan
  that depends on "it will show up on the AWS bill" must be checked against
  *this* account, not against AWS in the abstract.

## [2026-09-26] — Databricks could assume the IAM role but could not read the bucket; the list permission was scoped one character too tightly

**In plain words:** Databricks was given an AWS role that may read the
`ledgerline/` folder of the landing bucket. It could log in as that role, but
its "can I read this?" check failed with *Permission Denied*. The role allowed
listing files under the prefix `ledgerline/` — with a slash — and the check
asked for `ledgerline` without one. One missing character, and AWS says no.

- What happened: creating the Unity Catalog external location
  `s3://ledgerline-landing-dev-fffc8b65/ledgerline/` ran Databricks' built-in
  validation. **Assume Role, Self Assume Role, External ID Condition: Success.
  Read: Failed.** Plus two File Events failures, which are a separate optional
  feature (below).
- What I checked and ruled out, read-only via the AWS CLI: the inline policy
  *was* attached and pasted correctly; the bucket uses SSE-S3 (AES256), not
  KMS, so no decrypt permission was missing; the bucket has no bucket policy
  that could deny.
- Root cause: the `s3:ListBucket` statement carried a condition
  `s3:prefix` in `["ledgerline/", "ledgerline/*"]`. A list request with prefix
  `ledgerline` matches neither. `aws iam simulate-principal-policy` with
  `s3:prefix=ledgerline` returned **`implicitDeny`**. Adding `"ledgerline"` to
  the list — the only change made — turned Read, List and Path Exists green.
  That the validation sends the no-slash form is inferred from the fix, not
  observed: S3 list calls are data events and are not in CloudTrail by default.
- Why nothing caught it: the policy was written to be least-privilege, and it
  was correct for every request *I* imagined, all of which named a folder the
  way a human writes one — with a trailing slash. The request that failed came
  from software normalising the path differently. The error named neither the
  action nor the prefix, only "Permission Denied — check the path and the
  credential", which points at the two things that were fine.
- File Events failures: Databricks can subscribe to S3 event notifications
  (SNS/SQS) so Auto Loader learns of new files without listing. The role was
  deliberately given no SNS/SQS permissions; Auto Loader falls back to
  directory listing, which for 9 files is free. Recorded as a choice, not a
  fault.
- End-to-end proof, not just a green validation: a notebook on Free Edition
  serverless read `ledgerline/dims/customer/` and returned **8,395 rows** —
  the same count as the three customer CSVs downloaded and parsed locally
  (1 + 326 + 8,068).
- Prevention rule: **when an S3 policy scopes `ListBucket` by `s3:prefix`,
  list the prefix both with and without the trailing slash** —
  `["p", "p/", "p/*"]`. And before handing a scoped policy to another system,
  run `aws iam simulate-principal-policy` against the request shapes that
  system might send, not only the ones you would send.

## [2026-09-26] — the Bronze re-load test failed because "does this table exist?" answered No for a table that existed

**In plain words:** the Bronze notebook decided between "create the table" and
"replace this night's rows" by asking Databricks whether the table already
existed. Inside the streaming batch function it answered **No** for a table
that had been created minutes earlier, so the code tried to create it again
and Databricks refused. The replace path — the one the whole design depends
on — had never actually run.

- What happened: first load and plain re-run both passed with exact counts
  (customer 8,395; product 98,853; seller 9,285). The forced replay —
  checkpoints deleted so Auto Loader re-reads all 9 files — failed with
  `TABLE_OR_VIEW_ALREADY_EXISTS: bronze.customer`, raised from the
  create-table branch (cell line 27).
- Root cause, as far as verified: `spark.catalog.tableExists(...)`, called
  inside `foreachBatch`, returned `False` for an existing Unity Catalog table.
  On serverless, `foreachBatch` runs in a separate *cloned* Spark Connect
  session (the error names it). **Why** the check is wrong there was not
  investigated; the observation is what is recorded.
- Why nothing caught it: the two earlier runs could not reach the broken
  branch. The first load legitimately took the create path; the plain re-run
  found no new files, so `foreachBatch` was never called at all. **Two green
  checks, zero executions of `replaceWhere`.** A passing test only covers the
  code it actually ran — and here the test designed to exercise the replace
  path was the first thing that did.
- Fix: removed the branch. Each empty table is created once, before the stream
  starts, in the notebook's own session; `foreachBatch` now always writes with
  `replaceWhere`, including the first-ever batch. One path, exercised every
  run.
- Prevention rule: **don't branch on catalog state inside `foreachBatch`.** Set
  up tables before the stream starts, and keep the batch function a single
  write path. More generally: when a design has a "normal" path and a
  "recovery" path, make sure a test actually drives the recovery path — a
  re-run with nothing new to process proves nothing about it.

## [2026-09-26] — the test suite published to the live Kafka topic, re-poisoning 43 SKUs; CI failed for a different reason and hid it

**In plain words:** one of the tests written in Session 5 to *protect* the CDC
topic from partial runs was itself producing a partial run to the real topic
every time the tests ran on the laptop. It ran three times today, and put the
same 43 conflicting stock values back onto `inventory.cdc` that the Session 5
incident had been about. CI showed a red build that pointed somewhere else
entirely.

- What happened: `dev` went red on GitHub Actions — 2 failed, 120 passed —
  both in `tests/test_inventory_cdc.py`, both with
  `olist_orders_dataset.csv not found`. Investigating why those two tests
  needed real data led to asking what they do on a machine that **has** it.
- The contamination, measured with the read-only verifier: `inventory.cdc`
  holds **158,625** records against a clean **158,346** — **279 extra, exactly
  3 × 93** (three 50-order runs). **43 keys with a tied `seq`**, 226 duplicate
  `event_id`s, every record `produced_at 2026-09-26`. `orders` checked too:
  394,090 records, 394,090 distinct — untouched.
- Root cause, two parts:
  1. `test_partial_cdc_run_is_allowed_when_asked_for_explicitly` runs
     `inventory_cdc.main()` with `--sink kafka --limit 50 --allow-partial`.
     `main()` calls `load_local_env()`, which reads `.env` — on the laptop
     that file holds the **real** Confluent keys, and `confluent_kafka` is
     installed. The test's docstring assumed "no broker in tests"; on a
     developer machine there was one, fully authorized.
  2. The refusal guard sat **after** the full Olist load, so even the
     *refusal* test needed `data/raw/` — which CI does not have (gitignored,
     ~120 MB). That is the CI failure.
- Who ran it: pytest ran locally in Session 5 and **twice in Session 6 by
  Claude**, as a routine check after editing the Bronze notebook and
  `pyproject.toml`. Nobody knew the suite could produce.
- Why nothing caught it — the valuable part:
  - **CI could never see it.** CI has no `.env` and no `confluent_kafka`, so
    there the test fails at import and passes the assertion. The one
    environment that could have exposed the produce was the one where the
    test "passed".
  - **The local suite was green.** A produce that succeeds is not an error;
    the test's assertion (`code != 0 or message == ""`) was satisfied either
    way.
  - **The test's intent and its effect were opposite**, and the docstring
    described the intent. It was written to prove the door *can* open for
    Session 10's deliberate contamination — and it opened the door for real.
  - The `.env` loader was added in Session 4 precisely so CLIs would find
    their keys. Tests that call a CLI's `main()` inherit that.
- Fix, three parts:
  1. `tests/conftest.py`: an **autouse fixture `no_live_services`** — for every
     test, `.env` is never read, Kafka / Schema Registry / Databricks /
     Snowflake credentials are removed, and AWS gets throwaway keys with no
     credentials file (so a stray boto3 call cannot fall back to the admin key
     in `~/.aws/credentials`).
  2. `inventory_cdc.main()`: the partial-run guard now runs **before** any data
     is loaded — it depends only on arguments.
  3. The "allowed" test uses the fixture Olist and asserts it stops **at the
     Kafka sink** (`ImportError` or the missing-credentials `RuntimeError`),
     so "got further than expected" is a red test, not a produce.
- Verified after the fix: full suite green, lint clean; the refusal fires with
  a non-existent `--raw-dir`; and **the live topic was re-counted after a full
  local test run: still 158,625** — the suite no longer reaches it.
- Not yet done: the topic itself is still contaminated. Cleaning it is a
  separate, human-approved action (delete + recreate + re-produce), recorded
  when taken.
- Prevention rule: **tests must be unable to reach a live service, by
  construction — not by the absence of credentials on the machine running
  them.** Any test that calls a CLI entry point inherits whatever that entry
  point loads (`.env`, `~/.aws`, installed clients). Isolate at the fixture
  level, for every test, and prove it with a before/after count on the real
  system. Second rule: **a test that exercises a "dangerous on purpose" path
  must assert *where* it stops**, so that stopping later is a failure.

### Correction (2026-09-26, same day) — the topic is not being cleaned, and the damage is smaller than stated

The entry above says the topic "is still contaminated — cleaning it is a
separate, human-approved action (delete + recreate + re-produce)". **That
plan was dropped.** The project now treats live topics as production, so the
topic stays and downstream layers exclude the bad events (decision entry,
"Live data is treated as production").

The entry also repeats the verifier's "43 keys with a tied `seq`". **That
overstates it.** Comparing every live record with a clean regeneration: 120 of
the 279 extra records are exact duplicates of real events, which also show up
as "tied" but carry identical content. The genuine contamination is **159
records, 53 distinct event IDs, touching 23 SKUs** — and **no SKU ends with the
wrong final stock**, because the bad events sit at early `seq` values that
correct later events supersede. Totals computed from the raw log are wrong
(112,806 units against 112,650). The denylist is at
`ops/incidents/2026-09-26_inventory_cdc_denylist.json`.

## [2026-09-27] — Bronze `orders` loaded perfectly, but the per-batch progress log crashed, and my error handler hid where

**In plain words:** the first real Kafka → Bronze run read all 394,090 order
messages into `workspace.bronze.orders` exactly once — every count matched the
topic. But the small piece of code that writes down *how each batch went*
(rows, time, backlog) crashed with `'NoneType' object has no attribute
'items'`, so its table, `workspace.bronze.stream_progress`, was never created.
The next check reads that table, so it failed with `TABLE_OR_VIEW_NOT_FOUND`
and the notebook stopped. The data is right; the monitoring is broken.

- What happened, in order: `created workspace.bronze.orders` and
  `..._quarantine`; three "Spark Server has not sent updates … in 60 seconds …
  typically not a problem" notices (batches took over a minute each); then
  `bronze.orders: could not record progress: AttributeError: 'NoneType' object
  has no attribute 'items'`. The verify cell then passed every data check —
  p0/p1/p2 = 131,458 / 132,187 / 130,445 with rows = distinct offsets = span,
  0 quarantined, 394,090 distinct `event_id`, 99,441 `created`, 112,650 units,
  one schema id — and failed at `check_batches_not_doubled` on the missing
  table. The quarantine self-test, the plain re-run and the history cell were
  skipped.
- Impact on data: **none.** Exactly-once for this run is already proven by the
  coordinate check — in every partition, rows = distinct offsets = (max − min +
  1), so every offset landed once and none is missing. The per-batch check was
  extra evidence, not the guarantee.
- What I thought first: my own offset parser (`_offsets`) choking on a
  missing start offset for the first batch.
- What reading PySpark 4.0.1's source showed (not yet confirmed on the
  platform): on Spark Connect, `query.recentProgress` rebuilds each report with
  `StreamingQueryProgress.fromJson`, which calls
  `j["observedMetrics"].items()` whenever that key is present — a `null` there
  raises exactly this error, *inside PySpark*, before my code sees anything.
  Separately, in 4.0 the progress object became a subclass of `dict` and
  stores each offset as `str(...)` of a Python dict (`"{'orders': {...}}"`,
  or `"None"`), which is not JSON — so my parser would have failed next even
  if PySpark had not. Two stacked defects, the first masking the second.
- Why nothing caught it before the real run: the Session 7 probe tested the
  three unknowns I *recognised* (registry reachable, `from_avro`, group ACL).
  The shape of Spark's progress object was an unknown I did not recognise — I
  wrote the parser from memory of the pre-4.0 format and never ran it. And the
  handler I wrote so that "bookkeeping never hides the real failure" printed
  only the exception message, **dropping the traceback** — so it hid the one
  fact needed to fix it: which line failed.
- Fix: pending precise identification (a small diagnostic stream that prints
  the traceback and the raw progress JSON). Recorded below when done.
- Prevention rule: **a catch-all handler prints the full traceback, never just
  the message** — "don't crash" must not mean "don't say where". And **code
  that parses another library's output is run against one real object before
  a real job depends on it**: the probe step covers every shape the job reads,
  not only the ones that feel risky.

### Root cause identified (2026-09-27, same session) — it was my parser, and my reading of PySpark's source was wrong

**In plain words:** Spark reports "where did this batch start reading?" For the
very first batch there is no answer, and on this platform the report says so
with the **four-letter text `"null"`**, not with an empty value. My code only
treated an *empty* value as "no answer". The text `"null"` is not empty, so the
code parsed it, got nothing back, and then tried to read offsets out of that
nothing — the crash.

- How it was identified: a diagnostic stream over the last 30 `orders`
  messages printed (A) the raw report as the server sends it and (B) what
  PySpark turns it into. Platform: **PySpark 4.3.0.dev0**. (A) has
  `"startOffset":null` and `"observedMetrics":{}`; (B) `recentProgress`
  worked, is a `dict`, and holds `startOffset = 'null'` (a string) and
  `endOffset = '{"orders":{"0":131458,…}}'` (a JSON string).
- **Correction to the entry above.** It named, from PySpark **4.0.1**'s
  source, a null `observedMetrics` crashing inside PySpark, plus offsets stored
  as Python-dict text. **Both were wrong for this platform**: `observedMetrics`
  arrives as `{}`, offsets arrive as proper JSON, and PySpark raised nothing.
  My first guess — my own parser and the missing first start offset — was the
  right one. Reading the source of a *different version* is a hypothesis, not
  an identification; only the diagnostic made it one.
- Fix, three parts:
  1. `_offsets` returns None for None, JSON null *and* the text `"null"`.
     Checked on the laptop against the exact values the diagnostic printed.
  2. `_as_dict` reads `progress.json` — the server's own JSON, Spark's
     documented format — instead of PySpark's converted copy, whose value
     types have already changed once (dict subclass since 4.0).
  3. `check_batches_not_doubled` no longer reads the progress table. It
     compares, per `_batch_id`, rows in the table against distinct Kafka
     offsets — **a correctness check now reads the data, never the
     monitoring**. The progress table (created up front now, so it can never be
     missing) stays what it should have been: monitoring.
  Plus the handler now prints the full traceback.
- Also seen in the diagnostic, recorded because it explains the slowness: one
  batch that read nothing still took **11.9 s** — 2.9 s just to ask Kafka for
  the latest offsets (Ohio → Mumbai), 0.4 s planning, 0.4 s writing the
  checkpoint. The per-batch cost is mostly fixed overhead. Its
  `numInputRows = 0` is not a problem: the diagnostic's batch function did
  nothing with the data, and Spark only reads what something uses.
- Prevention rule, sharpened: **identify on the platform, not from the
  source.** When a library's behaviour is in question, print what the running
  system actually returns before writing or fixing code against it.

## [2026-09-27] — a corrupt Kafka message would have landed in Bronze as a row of blanks instead of going to quarantine; the self-test caught it before any arrived

**In plain words:** Bronze is meant to send any message it cannot decode to a
separate quarantine table. The check for "could not decode" asked *"is the
decoded record empty (NULL)?"*. But when Spark's Avro decoder fails in the
forgiving mode we use, it does not hand back NULL — it hands back a record
with **every field blank**. That record is not NULL, so the check said
"decoded fine", and a corrupt message would have been written into
`bronze.orders` as a row of blanks. A deliberate test fed it a broken message
and it came back "decoded OK".

- What happened: `quarantine_self_test` feeds `decode()` one real `orders`
  message and five broken copies. Result: real → decoded OK; **truncated
  payload → decoded OK** (should be quarantined); wrong magic byte → not
  Confluent wire format; **unknown schema id → decoded OK** (open — see below);
  null value → tombstone; missing key → missing key.
- Root cause: `from_avro(..., mode="PERMISSIVE")` returns, for a record-shaped
  schema, a record whose fields are all NULL when decoding fails — not a NULL
  record. That is also why `printSchema` in the probe showed **every** field
  `nullable = true`, including fields the Avro schema declares required: the
  decoder forces the whole schema nullable so it can hand back blanks.
  `decode()` tested `_record IS NULL`, which can therefore never be true for a
  failed decode. The field-level behaviour is confirmed by the next run, which
  prints `event_id` per case.
- Why nothing caught it:
  - The live topics hold **zero** corrupt messages, so the real load never
    reached this branch. Its green result said nothing about it — the Session 6
    lesson again: a passing run only covers the code it actually ran.
  - The docs phrase it as "corrupt records are processed as null result".
    "A null result" and "a result whose fields are null" read the same in a
    sentence and behave differently in a `WHERE`.
  - The self-test that found it **printed its results but asserted nothing**,
    so the notebook carried on. It was caught because a human read the output.
- Impact on data: **none.** `bronze.orders` has 394,090 rows and 394,090
  distinct `event_id`s. `count(DISTINCT …)` ignores NULLs, so a single blank
  row would have made the two numbers differ. No corrupt message has ever
  been on the topic.
- Fix: "decode failed" now means `_record.event_id IS NULL`. `event_id` is a
  required, non-nullable string in both schemas, so a valid message always has
  one. The self-test now **asserts** the expected route for each case and stops
  the notebook if any is wrong.
- Open question, deliberately not assumed: the **unknown schema id** case
  (id 999,999, which does not exist in the registry, in front of a valid
  body). Either the decoder looked the id up, failed, and returned blanks —
  then the new check quarantines it — or it ignored the id and decoded the body
  with the subject's own schema, which would matter the day a second schema
  version exists. The next run prints the decoded `event_id` for that case,
  which tells the two apart.
- Prevention rule: **detect a failed decode by a field every valid record must
  have, never by "is the record null".** And **a self-test asserts its expected
  outcomes** — a test that only prints relies on someone reading it.

### Confirmed by the next run (2026-09-27, same session)

The fixed self-test passed every assertion, and printed `event_id` per case:
real message → decoded OK, `event_id=515e2866…`; **truncated payload → avro
decode failed, `event_id=None`** (the blank-record behaviour, now caught);
wrong magic byte → not Confluent wire format; null value → tombstone; missing
key → missing key (its body decoded fine, `event_id=515e2866…`, and was still
routed to quarantine).

**The open question is closed:** unknown schema id → **avro decode failed,
`event_id=None`**. The body was a valid `orders` record, so a decoder that
ignored the id would have returned the real `event_id`. It returned blanks —
so `from_avro` with the registry **looks up the schema id carried in each
message** and decodes with that exact writer schema. Consequence for schema
evolution: a future version-2 message will be read with version 2's schema,
and a message whose id the registry does not know is quarantined, not
misread.

## [2026-09-27] — Spark's monitoring said "0 rows" for a load that wrote 158,625

**In plain words:** the first run that recorded progress properly — Bronze
`inventory.cdc` — printed `4 progress report(s), 0 batch(es) with data, 0
rows, 270s`. The table beside it held all 158,625 rows, checked offset by
offset. The data was right; the monitoring number was wrong. On this platform,
Spark's own "input rows" figure is 0 for every batch whose work happens inside
`foreachBatch`.

- What happened: 4 batches wrote 49,999 / 49,998 / 49,999 / 8,629 rows
  (coordinate check OK in every partition), yet every progress report carried
  `numInputRows = 0`. My run summary counted batches and rows from that field,
  so it reported nothing.
- What is certain: `numInputRows` is 0 for batches that demonstrably read and
  wrote tens of thousands of rows. The diagnostic earlier today showed 0 too,
  which I explained then as "the batch function did nothing" — true, but it was
  also this.
- Likely mechanism (not verified): on serverless the `foreachBatch` function
  runs in a separate, cloned session, so the reads it triggers are separate
  queries whose row counts are not credited back to the streaming query's
  progress.
- Why nothing caught it: the first `orders` run lost its progress to the parser
  bug, and every later run had no new data — where 0 is the *correct* answer.
  A metric that is always 0 looks healthy on idle runs. This is the Session 5
  rule from the other side: a health check must be able to return True on good
  data — and a metric must be checked against ground truth once before it is
  trusted.
- Fix: the summary now counts from **offsets** (`end − start` per partition,
  the `rows_by_offsets` column recorded since the first version), and a batch
  "has data" when its start and end offsets differ. `numInputRows` stays in the
  table, labelled as unreliable here. The one gap: a brand-new stream's first
  batch has no start offset in the report, so its rows are shown as unknown
  rather than guessed.
- Prevention rule: **validate every monitoring number against the data once,
  on a run that moved data, before relying on it.** Here that is one query:
  `rows_by_offsets` per batch against rows per `_batch_id` in the table.

### Validated the same hour — rows and backlog hold, durations do not

The prevention rule above, applied at once: every monitoring column for
`bronze.inventory_cdc` put next to the table itself.

| batch | rows in table | rows_by_offsets | numInputRows | reported s | backlog after |
|---|---|---|---|---|---|
| 0 | 49,999 | *(no start offset)* | 0 | 93 | 108,626 |
| 1 | 49,998 | 49,998 | 0 | 166 | 58,628 |
| 2 | 49,999 | 49,999 | 0 | 162 | 8,629 |
| 3 | 8,629 | 8,629 | 0 | 99 | 0 |

- **`rows_by_offsets` is exact** for every batch that has a start offset.
- **Backlog is exact**: 158,625 minus the running total after each batch
  (108,626 → 58,628 → 8,629 → 0).
- **`numInputRows` is 0 throughout** — the finding, confirmed.
- **Durations do not reconcile.** The four batches report 520 s in total, but
  the run took ~270 s: the run's own stopwatch said 270 s, and the table's
  commits run from 07:38:51 (create) to 07:43:24 (last batch). Micro-batches
  run one after another, so 520 s of batches cannot fit into 270 s. Each
  reported figure is roughly double the gap between that batch's commit and
  the one before it. **Not explained**; recorded rather than guessed. Until it
  is, per-batch timing comes from Delta commit timestamps, and no check reads
  `duration_ms`. Carried to Drill 1.

## [2026-09-27] — exp_01 proved the bug, then Databricks failed the cell anyway, and "Run all" never reached the fix

**In plain words:** exp_01's first half crashed a stream on purpose, caught
the crash, restarted, and showed exactly the predicted damage — the crashed
batch in the table twice. All of its checks passed and it printed
`BUG PRESENT`. Then Databricks added its own line — *"ERROR: Some streams
terminated before this command could finish!"* — and marked the whole cell as
failed. "Run all" stops at a failed cell, so the second half, the fix, never
ran.

- The evidence that did land (deliberate failure, bug half):
  crash raised right after batch 2 reached Delta; restart re-ran batches 2–7
  (294,092 rows by offsets = 49,999 × 3 + 49,998 × 2 + 44,099); final table
  **444,089 rows, 394,090 distinct offsets, 49,999 duplicates**; batch 2 =
  99,998 rows for 49,999 offsets; rows present before the restart = 149,997 =
  batches 0–2, proving the crash fell after the commit. The job itself
  reported success.
- Root cause: a Databricks notebook watches every stream a command starts.
  If any of them ends with an error while the command runs, the notebook
  fails the command at the end — **whether or not the code caught the error**.
  From the notebook's point of view a caught stream failure is still a stream
  failure. Our `try/except` handled it in Python; the platform reported it
  anyway.
- Why nothing caught it before: this was the first time any code here failed
  a stream on purpose. Every earlier failure path was driven in batch code
  (the quarantine self-test) or not at all.
- Fix: exp_01 no longer stages the crash. It loads cleanly, then deletes the
  last `commits/N` file from the checkpoint — leaving exactly what a crash
  between the Delta write and the checkpoint commit leaves (rows in Delta,
  `offsets/N` present, `commits/N` missing) — and restarts. No stream fails,
  the replay is deterministic, and both variants assert that batch N really
  was re-run, so "0 duplicates" cannot mean "nothing ran". The crash hook
  (`crash_after_batch`) was removed from the shared Bronze writer: production
  code should not carry an off-switch built for one experiment.
- Prevention rule: **to test recovery, recreate the state a failure leaves
  behind, not the failure event itself** — the state is deterministic, and the
  platform does not treat it as a real failure. And before injecting a
  failure, find out what the platform does with it: here, a notebook fails any
  command in which a stream died, caught or not.

## [2026-09-27] — DELIBERATE (exp_01): a crash after the Delta write re-runs the batch; without txnAppId it lands twice, with it Delta skips it

**In plain words:** the first of the project's six planned deliberate
failures. A Kafka → Delta stream was made to "crash" at the worst possible
moment — after a batch's rows were saved, before Spark recorded the batch as
finished. On restart Spark re-ran that batch. With a plain append the
44,099 rows went into the table a second time and **the job reported
success**. With `txnAppId` / `txnVersion` set, Delta recognised the batch as
already written and skipped it. Same code, same data, same crash; the only
difference was two write options.

- How the crash was made: load the whole `orders` topic cleanly (8 batches),
  then delete the checkpoint's last `commits/7` file — leaving batch 7's rows
  in Delta and `offsets/7` in the checkpoint but no "done" marker, exactly
  what a real crash in that window leaves — and restart
  (`experiments/exp_01_exactly_once_replay.py`, scratch tables only).
- Result, **bug** (plain append): restart re-ran batch **[7]**, 44,099 rows by
  offsets → table **438,189 rows** for 394,090 distinct offsets = **44,099
  duplicates**; batch 7 = 88,198 rows for 44,099 offsets. History: v2–v9 the
  eight batches, then **v10, a second `WRITE` of 44,099 rows**. No error
  anywhere.
- Result, **fix** (`txnAppId = exp01.fix.<run>`, `txnVersion = batch_id`):
  restart re-ran batch **[7]**, 44,099 rows by offsets → table **394,090 rows,
  0 duplicates**. History stops at v9: the replay made **no commit**.
- The replay took **66 s** without the options and **8 s** with them. Delta
  checks "has app X already written version 7?" against its own log *before*
  running the write — and the batch's rows are only read from Kafka when the
  write runs — so the skipped replay never touched Kafka. The guarantee is a
  metadata lookup, not a data comparison.
- Why the bug is dangerous, stated plainly: nothing fails. The duplicate
  batch is a normal, successful append. Only a count against the source —
  here, rows vs distinct `(partition, offset)` — shows it.
- Same pattern, first attempt: staging the crash as an exception inside the
  batch gave the identical doubling (batch 2, 49,999 duplicates) but made the
  notebook fail the cell — see the entry above.
- Prevention rule: **every streaming write that is not itself idempotent
  (MERGE, `replaceWhere`) carries `txnAppId` + `txnVersion = batch_id`**, and
  the app id changes whenever the checkpoint does (Drill 1 shows what happens
  when it does not). Proven by `exp_01`, which must be re-run after any change
  to the Bronze writer.

<!-- Append further entries below this line. -->

## [2026-09-28] — DELIBERATE (Drill 1): checkpoint deleted, app id kept — 74 new messages lost, no error, and a restart could not bring them back

**In plain words:** a stream remembers where it is in two places: Spark's
checkpoint folder, and Delta's note inside the table ("app v1 has written up
to batch 7"). Drill 1 deleted the first and kept the second. The restarted
stream counted batches from 0 again, Delta saw numbers it had already
recorded, and skipped every batch — including the one that now held 74 new
messages. The job succeeded, the table did not change, and a normal restart
afterwards wrote nothing, because the new checkpoint now said those offsets
were done.

- How it was made (`drills/drill1_trap_1_life1`, `drill1_trap_2_reset`,
  scratch tables only): life 1 loaded `orders` (394,090) into
  `bronze.drill1_trap` with app id `drill1.trap.v1` — batches 0–7. Then two
  labelled 10-order partial runs put **74** messages on the topic (run ids
  `ea7bb387…` and `370891cc…`, 37 each). Then the checkpoint was deleted and
  the stream restarted with the same app id and the new guard switched off.
- Result, bug: life 2 planned batches **0–7** and reached the topic's new end
  (131,458 / 132,187 / 130,445 → 131,474 / 132,217 / 130,473). Table **394,090
  rows before and after**; data commits **8 before, 8 after** — Delta wrote
  nothing. Life 2 took **17 s** for 8 batches against life 1's **531 s**:
  every skip was decided from the log, before any Kafka data was read.
- The lost messages, named: a batch read of exactly the offsets past life 1's
  end returned 74 messages, all labelled, from 2 runs, **0 in the table**.
  All 74 were duplicates of events already present — so here the loss cost
  nothing, **by luck**. Had they been new orders they would be gone.
- A normal restart (guard on) afterwards: 0 batches with data, 6 s. The loss is
  permanent from the stream's point of view; only a rebuild recovers it.
- Why it is invisible: batch 7 held 44,099 messages in life 1 and more in life
  2, but Delta compares only the number. The rebuild's own batches came out
  49,998 ×6, 49,999, 44,177 against life 1's 49,999 ×5, 49,998 ×2, 44,099 —
  Spark sizes each batch in proportion to what each partition holds, so a
  batch number does not mean the same messages twice.
- Fix: `refuse_unsafe_reset()` in `_kafka_bronze.py` (decisions.md, Drill 1):
  a checkpoint with no `offsets/` may only write into empty tables. With it on,
  both unsafe resets were **refused before any stream started** — "checkpoint
  deleted, same app id" and "new generation, old table" (`v2/offsets` stayed
  empty). The safe reset — new generation into an empty table — gave **394,164
  rows, every offset once, all 74 present**.
- Prevention rule: **a stream's two memories are reset together or not at
  all, and the code refuses anything else.** A reset is a rebuild: new
  checkpoint, new app id, empty table. More generally: when two systems each
  keep half of a guarantee's state, check that the halves agree before
  trusting either.

## [2026-09-28] — Drill 1: three incident guards had no test that could fail, and CI skipped the producer tests

**In plain words:** Drill 1 put five old bugs back on purpose, one at a time,
and ran the tests meant to guard against each. Two of the guards were never
tested at all. For the third, the test existed but was aimed at the wrong
half of the fix, so the bug went back in and the suite stayed green. Separately, CI had
never run five of the producer tests, because the library they need was not
installed there.

- Method: a script replaced one line of guard code with the old bug, ran that
  guard's test, restored the file byte for byte, and reported whether the
  test failed.
- Fired as designed: **S3** (S3 sink falls back to the ambient admin key) and
  **S5** (partial CDC run produced to Kafka).
- **S2, silent.** With `lineterminator="\n"` removed from the dims generator,
  every test passed on Windows, where the bug lives. The guard test compares
  the local sink with the S3 sink; both are handed the *same* string, so a CRLF
  string stored twice still compares equal. It tests the sink half of the
  Session 2 fix, never the generator half. Confirmed that pandas 2.2.3 on this
  machine emits `\r\n` without the pin.
- **S4, no test.** The bounded `flush()` that turns the Session 4 wrong-port
  hang into an error was `# pragma: no cover` "because it needs a broker that
  fails". It does not: `flush(timeout)` just returns how many messages are
  still queued, and a fake producer can return 3.
- **S6, the fixture itself untested.** On a laptop where no credential is
  exported, weakening `no_live_services` changes nothing any test can observe;
  it only matters on the day a key *is* in the environment.
- **CI:** `confluent_kafka` is in neither the CI install list nor its "must be
  present" check, so every `@needs_confluent` test (queue-full backpressure
  since Session 5; provenance headers since today) was skipped there. `-ra`
  lists the skips; the run is still green.
- Why nothing caught it: each guard was written in the session that hit the
  bug, and tested against that session's reproduction. Nothing ever asked
  whether its test would fail *if the guard were removed*.
- Fix: `test_dump_bytes_are_the_same_on_every_machine` (forces `os.linesep` to
  `\r\n`, so it fails on Linux CI too); two `flush` tests on a stuck fake
  producer; `test_no_test_can_see_a_live_credential`, which plants every
  credential and runs the fixture's function (`block_live_services`, split out
  of the fixture for this); CI installs `confluent-kafka` and asserts it
  imports. Re-run with the bugs put back: **all five now fail when they should.**
- Also found without a guard: the **S6 IAM prefix** — its prevention rule said
  "run `simulate-principal-policy`" and nothing did. Now
  `scripts/check_uc_role_policy.py`, which also replays the Session 6 policy
  shape to show it would have caught it.
- Prevention rule: **a guard is proven by breaking it, not by its test
  passing.** For every incident guard, put the bug back once and watch the test
  fail. A test that stays green with the bug present is decoration.

## [2026-09-28] — a Git-folder Pull stopped on a merge conflict in files nobody had edited

**In plain words:** pulling Drill 1's second commit into Databricks stopped
with "resolve conflicts before merge" in `autoload_dims.py`. Nobody had edited
any notebook in the workspace. Databricks itself had: every notebook that had
been opened or run was re-saved with a new header and without its final
newline, so six files counted as locally modified, and the one whose last
lines had also changed upstream could not be merged automatically.

- What happened: Pull stashed the six "modified" files, applied the two new
  commits, and could not re-apply the stash to `autoload_dims.py`: both sides
  had changed its last lines. The dialog offered "Resolve with Genie" and a
  merge editor showing old and new code side by side.
- What I thought was wrong: an accidental edit in the workspace. The diff of
  `stream_orders.py` said otherwise — two hunks, no code: a four-line
  `[tool.databricks.environment]` / `environment_version = "6"` header after
  line 1, and "No newline at end of file".
- Root cause: Databricks' notebook save format differs from the files in git
  in exactly those two ways. Every notebook that is run drifts from git, and it
  stays invisible until a Pull touches the same lines.
- Why nothing caught it earlier: Sessions 6 and 7 pulled only before running,
  or pulled changes that did not touch the lines Databricks rewrites. The
  "M" markers were there; nothing looked at them.
- Fix: aborted the merge, read one diff to confirm nothing real would be lost,
  discarded all six local changes, pulled cleanly (`0a9ccfd`). Then, as a
  decision, committed every notebook in Databricks' own save format.
- Prevention rule: **never resolve a Git-folder conflict by merging —
  the workspace is a consumer of git, so its side is discarded after one look
  at the diff.** And **keep committed notebooks byte-identical to what the
  platform saves** (`tests/test_notebooks.py`), so a run leaves nothing to
  merge.

## [2026-09-28] — Drill 1: a corrected nightly dump, re-delivered under the same name, was silently ignored by Bronze

**In plain words:** Session 6 claimed that re-loading a night is harmless
"if the checkpoint is ever lost or a night is backfilled". Drill 1 tested the
second half for the first time: a seller dump was re-delivered with five
corrected cities, same file name. Bronze did not pick it up. No error, no
write — Bronze kept the old cities, the landing zone held the new ones, and
nothing anywhere reported the difference.

- How it was made (scratch only, `drills/land_drill1_scratch.py` +
  `drills/drill1_dims_2`): two real seller nights copied to
  `ledgerline/drill1/redelivery/`, loaded; then night 2017-05-02 rewritten in
  place with five `(corrected)` cities — 231,780 → 231,840 bytes, new ETag,
  same path — and the stream re-run with production settings.
- Result: counts unchanged, **0 corrected rows, WRITE commits 1 → 1**.
- Root cause: Auto Loader remembers files **by path**. With
  `cloudFiles.allowOverwrites = false` (the default, and production's
  setting), a path already in its checkpoint is skipped even when the file's
  content and modification time have changed. `replaceWhere` would have made
  the re-read safe; the re-read never happened.
- Why nothing caught it: the Session 6 proof of "re-reading is harmless"
  deleted the **checkpoint**, which makes Auto Loader forget every path. That
  proves the checkpoint-loss case and says nothing about a re-delivery with the
  checkpoint intact — the case a vendor's correction actually produces. The
  same shape as Session 6's own lesson: a green run can execute none of the
  code you care about.
- Fix: `allowOverwrites` on in production `autoload_dims` (decisions.md,
  Drill 1). Proven on scratch first: one write, `dump_date IN ('2017-05-02')`
  only, 5 corrected rows, counts and the other night unchanged.
- Prevention rule: **test each recovery claim through the path that really
  produces it.** "A backfill is harmless" must be tested with a backfill (same
  path, new content, checkpoint intact), not with a checkpoint reset that
  happens to share its outcome.

## [2026-09-30] — zip codes lost their leading zero between Olist and the landing zone, and every check passed

**In plain words:** Brazilian zip prefixes are five digits, and about a quarter
of them start with 0 (`09790`). The dims generator reads them as numbers, and a
number has no leading zero — so every landed dump says `9790`. Bronze stored
that faithfully. Found at the start of Session 8 while looking at real values
before designing Silver's typing; nothing had flagged it in eight sessions.

- What happened: raw Olist has **99,441 / 99,441** customer zips and **3,095 /
  3,095** seller zips at exactly five digits. The landed dumps have short zips
  on every night: customers 0 / 1, 57 / 326, 1,599 / 8,068, **4,783 / 22,497**
  (2017-08-30); sellers **1,027 / 3,095** on every night.
- What I thought was wrong: nothing — the check was a look at real values
  before writing casts, not a bug hunt.
- Root cause: the Olist contract in `generators/olist.py` (Session 1) types
  `customer_zip_code_prefix`, `seller_zip_code_prefix` and
  `geolocation_zip_code_prefix` as `Int64`. `to_csv` then writes the integer.
  A zip is an identifier that happens to use digits, not a quantity.
- Why nothing caught it: every test and every check compares **counts and
  keys** — rows per night, distinct ids, offsets. None looks at the *format* of
  a value. The test fixtures build their frames with the same `Int64` cast
  (`tests/conftest.py`), so the tests agree with the bug by construction.
  Bronze keeps strings exactly as the file had them, which is right, and which
  means the defect arrived in Bronze intact.
- Fix: in Silver, not in the generator — `lpad(zip, 5, '0')`, lossless because
  every original is five digits, plus a check that every zip is five digits
  after padding. Fixing the generator mid-stream would make night 5 disagree
  with night 4 on thousands of rows and open fake SCD2 versions in Gold
  (decisions.md, Session 8). The landed files stay as delivered.
- Prevention rule: **an identifier made of digits is text — never let a reader
  cast or infer it as a number** (zip, phone, account number, any code with a
  meaningful leading zero). Silver asserts the five-digit format; a generator
  test pins the current four-digit behaviour so that "just fixing" the
  generator fails loudly and points at the decision that explains why not.
- Lesson: "rows match" is not "values match". A pipeline can be exactly-once,
  gap-free and faithfully wrong.

### Fix verified (2026-09-30, same session)
Silver's first run padded 57 / 1,599 / 4,783 / **9,572** customer zips (nights
2–5) and **1,027 / 1,002** seller zips; afterwards `zips not five digits = 0` on
all three tables. "Lossless" checked on the laptop rather than asserted:
**43,690 / 43,690** padded customer zips and **2,995 / 2,995** seller zips on
night 5 equal the raw Olist value for the same person or seller, rebuilt from
the raw files read as text.

## [2026-09-30] — DELIBERATE (exp_03): a plain MERGE kept 100 deleted sellers in Silver, reported success, and raised no error

**In plain words:** the first deliberate experiment on Silver. A MERGE with
only the two usual clauses — "matched → update", "not matched → insert" — was
given a night in which 100 sellers had disappeared from the file. All 100
stayed in Silver, with their old values, and the MERGE reported success. Adding
the third clause, `WHEN NOT MATCHED BY SOURCE THEN DELETE`, removed exactly
those 100. The same notebook then broke the fix's own assumptions on purpose.

- How it was made (`experiments/exp_03_merge_gap_stale_row`, scratch tables
  `workspace.silver.exp03_*`, the production code with one switch per part):
  seller nights 2017-08-30 (3,095) → 2017-12-28 (2,995) from real Bronze; the
  generator's own report for that step is 50 changed, 100 gone, 0 new.
- Results:
  - **A. bug** (`delete_missing=False`): `+0 ~50 -0`, **3,095 rows, 100 stale**,
    every stale row holding its 2017-08-30 values. No error.
  - **B. fix**: `+0 ~50 -100`, 2,995 rows, Silver equals the night on every
    column of every row.
  - **C. same night again**: `0 / 0 / 0` — and **the table version still moved
    (2 → 3)**: Delta commits a MERGE that changes nothing.
  - **D. one row set aside** before the MERGE (what a quarantine does): that
    seller **deleted** (`-1`); re-applying the full night brought it back as
    `+1`, with `_first_seen_dump_date` **2017-08-30 → 2017-12-28** — the row
    returned, its history did not.
  - **E. half a file**: the breaker refused — would delete **1,643 of 3,095
    (53.1%)** — and nothing was written. With the breaker off: **1,643
    deleted, no error**.
  - **F. update every match** (`only_changed=False`): **2,995 updates** and
    2,995 change-feed rows for the same final table, against 50 and 50.
- Root cause of the bug: both usual MERGE clauses are about rows **in** the
  source. A row that is only in the target matches neither, so nothing ever
  looks at it. On a full snapshot, that absence is the deletion signal.
- Why nothing would catch it in production: every count the MERGE reports is
  true (0 deleted *is* what it did), the table has no duplicate keys, and every
  row in it is a real seller that once existed. Only a comparison of the whole
  table against the night's file (production's Verify 2: `extra=0`) shows 100
  rows too many.
- Fix: the production MERGE (`databricks/silver/_merge_dims`) carries
  `WHEN NOT MATCHED BY SOURCE THEN DELETE`, behind the 5% delete breaker and
  the whole-night contract check (decisions.md, Session 8).
- Prevention rule: **on a full snapshot, check the table against the file both
  ways, not just the MERGE's counts** — a missed delete shows only as rows the
  file lacks. And the clause's two dangers each get a guard: a truncated file
  (breaker) and a filtered source (bad values fail the night, never dropped).
- Lesson: an insert-or-update MERGE is an *upsert*, not a *sync*. It can
  never shrink a table, so on a snapshot source it silently turns every
  deletion into a permanent stale row.

## [2026-09-30] — a planned deliberate-failure experiment (exp_02) was never scheduled; its layer was finished two sessions earlier

**In plain words:** the project promises six experiments that break a pattern
on purpose. One of them, `exp_02` (partition overwrite: what `replaceWhere`
protects against), belongs to Bronze dims — built in Session 6. It was never
run: not in Session 6, not in Drill 1, whose whole job was to attack every
Bronze guarantee. Nobody noticed until the human asked "did we do exp_02?" at
the end of Session 8.

- What happened: `experiments/exp_02_partition_overwrite_deletes.py` has been
  an empty file since `6701b93` (2026-09-19). The pattern it tests shipped in
  Session 6 and was attacked in Drill 1 — but only on its **success path**
  (a re-delivered night replaces that night only). The two **bug** variants
  the experiment exists for — a plain append (the night doubles) and an
  overwrite without a predicate (one night replaces the whole table) — were
  never run. The second is named in `CLAUDE.md`'s own danger zones.
- What I thought was wrong: nothing; `progress.md` Session 8 listed it under
  "Not done" as "never scheduled", which described the miss without asking why.
- Root cause: **the six experiments were created as stubs and never bound to
  sessions.** The only record of them was the end-of-project success criterion
  "all six pass". Each got done only when some session's `### Next` happened to
  name it: `exp_01` rode on Session 7's topic, `exp_03` was named by Drill 1's
  `### Next`, `exp_04` by a Session 6 decision. `exp_02` was named nowhere.
  Session 6 — the session that built `replaceWhere` — was consumed by the Free
  Edition switch and three incidents, and its plan never mentioned it.
- Why nothing caught it: an empty stub looks exactly like "not yet". No list
  put each experiment next to the layer it tests and whether that layer
  exists, so "the layer was finished two sessions ago" was never visible. And
  Drill 1's plan was written from the guarantees, not cross-checked against
  the experiment list, so "attack every guarantee" passed with the bug-first
  half missing.
- Fix: `exp_02` built and run in Session 8 (results appended below when run).
  An **experiment tracker** — all six, each with its layer, the session it is
  due in and its status — at the top of `progress.md`.
- Prevention rule: **every planned deliverable that spans sessions is bound to
  a session the day it is created, in a tracker read at every session start.**
  `CLAUDE.md` start-of-session now includes the tracker: an experiment whose
  layer exists and whose status is not "done" is either built that session or
  carried *by name* in `### Next`. Every drill cross-checks its attack list
  against the tracker.
- Lesson: a goal stated only at the finish line gets done only when it happens
  to be on the way. Anything worth promising needs a date, not just a place in
  the success criteria.
- Fix verified (same day): `exp_02` ran in Session 8 — entry below. Tracker
  status updated to done.

## [2026-09-30] — DELIBERATE (exp_02): re-writing one night by append doubled it; by overwrite without a predicate, it deleted every other night

**In plain words:** Bronze keeps every night's dump side by side, and a night
can be delivered again (a correction, a replay). The experiment wrote a
corrected night — the same night with one seller retracted — three ways. A
plain append put it beside the first copy (every seller twice, the retracted
one still there). A plain overwrite replaced the **whole table** with that one
night. Only `replaceWhere dump_date IN (<that night>)`, what production does,
replaced exactly that night — and the retracted seller disappeared without any
delete logic, because the new copy of the night simply does not contain it.

- How it was made (`experiments/exp_02_partition_overwrite_deletes`, scratch
  tables `workspace.bronze.exp02_*`): each table holds two real seller nights
  from Bronze (2017-08-30: 3,095; 2017-12-28: 2,995), written by production's
  `write_batch`. The re-delivery: 2017-12-28 minus seller
  `0015a82c2db000af6aaaf3ae2ecb0532` (2,994 rows).
- Results:
  - **A. append:** night 2017-12-28 = **5,989** rows, **2,994 sellers twice**,
    the retracted seller still present. No error.
  - **B. overwrite, no predicate:** the table = `{2017-12-28: 2,994}` — **night
    2017-08-30's 3,095 rows deleted**. History records it as `CREATE OR
    REPLACE TABLE AS SELECT` (`isV1SaveAsTableOverwrite: true`), not `WRITE`:
    `saveAsTable` in overwrite mode replaces the table.
  - **C. `replaceWhere` (production):** `{3,095, 2,994}`, retracted seller gone,
    the other night identical on every column, the night equal to the
    corrected file. Last commit: predicate `dump_date IN ('2017-12-28')`,
    **2,994 rows written** — the nights sat in separate files, so nothing was
    copied (Drill 1's 6,190 was the shared-file case).
  - **D. mislabelled file** (folder `2017-12-28`, rows `2017-08-30`):
    production's folder check **refused** — the first time it has ever fired.
    With the check bypassed: night 2017-08-30 now holds 2017-12-28's sellers,
    and **100 sellers that existed only on 2017-08-30 exist nowhere** in the
    table.
  - **E. stray row** (one row dated 2017-08-30 in a write replacing
    2017-12-28): Delta refused — `DELTA_REPLACE_WHERE_MISMATCH` /
    `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES`; nothing written.
- Root cause of both bugs: a write mode that does not name *which* rows it
  replaces. Append replaces nothing, so a second copy is a duplicate. Overwrite
  replaces everything, so one night is the whole table. `replaceWhere` names
  the night — and the night's name must come from somewhere trustworthy, which
  is what D and E guard.
- Why nothing would catch the bugs in production: A and B both report success
  with plausible metrics (`numOutputRows 2,994`). A shows up only as rows per
  key > 1; B only as a night missing from the table — Bronze's per-night
  comparison against the landing zone (`autoload_dims` Verify) catches both,
  which is why that check exists.
- Fix: production already uses `replaceWhere` with the folder check
  (Session 6); this experiment is the first proof that the alternatives fail
  and that both guards fire.
- Prevention rule: **a re-writable unit of data (a night, a partition) is
  always written with a predicate that names exactly that unit, and the name is
  checked against a second source (the folder) before the write.** Never
  `mode("overwrite")` on a table that holds more than one unit.
- Lesson: `replaceWhere` handles deletions for free *inside the unit it
  replaces*; a MERGE over the whole table needs `NOT MATCHED BY SOURCE` to do
  the same (exp_03). Same data, same deletion, two layers, two mechanisms.

## [2026-10-01] — the CDC feed's "stock before" and "stock after" disagree with its own `seq` order on 237 SKUs, and every check passed

**In plain words:** in a change log, each event for a SKU should start where
the previous one ended: "stock before" of event n equals "stock after" of event
n−1. Ours does not, 1,423 times. The generator applies a restock to its running
stock *straight after* the sale that triggers it, but stamps the restock **one
hour later**. Any sale inside that hour gets an earlier `seq` than the restock,
yet a "stock before" that already includes it. Found at the start of Session 9,
computing on the laptop what Silver must hold before building Silver.

- What happened: the clean log (158,346 events, regenerated locally) read in
  `seq` order per SKU has **1,423 broken links on 237 of 34,448 SKUs**. Example,
  SKU `eba7488e…|620c87c1…`:

      seq …2553  sale     prev 62 → 61
      seq …3641  sale     prev 86 → 85     ← 86 exists only after the restock
      seq …6153  restock  prev 61 → 86     ← stamped 1 h after the sale that caused it

  Consequence for a CDC MERGE (keep the after-image of the highest `seq`):
  **26 SKUs end on the wrong stock, 27 units in total** — Silver would sum to
  **993,982** where the generator's own deltas sum to **993,955**.
- What I thought was wrong: nothing — the check was the answer key for Silver,
  not a bug hunt. The first number (1,423 chain breaks) looked like a bug in
  the check script; one SKU printed in `seq` order showed it was the data.
- Root cause: `build_cdc_events` (Session 1) walks each SKU's sales in time
  order and updates `running` as it goes; a restock is appended at
  `sale_ts + 1 h` with `prev = running`, and `running += 25` **immediately**.
  `assign_seq` then sorts every event by `event_ts`. When the next sale is less
  than an hour after the restock-triggering one, sorting moves it in front of
  the restock — but its images were computed after it. The images follow
  generation order; `seq` follows time order. A real database cannot do this:
  the after-image *is* the row at commit, and the log is in commit order.
- Why nothing caught it: **every check reads deltas, and a delta does not
  depend on order.** Units sold is rebuilt as `-sum(stock − prev)` over sales
  — each event's own two numbers — so it ties out (112,650) whatever order the
  events are in. The tests check seq monotonicity, seeds, no clamping, no
  negative stock, the tie-out: all per-event or per-sum. None compares one
  event with the event before it. The test fixture restocks after the third
  sale of the only SKU with three sales, so no sale ever lands inside the hour.
- Fix: in Silver, not in the generator (decisions.md, Session 9, "Silver
  inventory mirrors the source's after-images …"). The topic holds these events
  and is treated as production; a fixed generator would regenerate different
  stock values and event ids for ~1,400 events that are correct-as-delivered,
  and "compare with a clean regeneration" — how the Session 6 denylist was
  found — would then flag them. Silver applies the standard after-image MERGE,
  **counts chain breaks per batch** in its log, and its verification pins the
  known baseline (1,423 / 237 / 26 SKUs / 27 units) so that any new break fails.
- Prevention rule: **a change log carrying before- and after-images must be
  checked as a chain — `prev` of each event equals `stock` of the event before
  it for the same key, in log order.** That is the one check that sees order;
  totals and deltas never will. A generator test pins the current behaviour,
  so "just fixing" it fails loudly and points here.
- Lesson: a reconciliation built on deltas proves the *sum* is right and
  nothing about the *order*. The before-image exists so a consumer can check
  it was handed events in the order they happened.

## [2026-10-02] — exp_04 failed a stream on purpose inside a notebook cell, and Databricks failed the cell — the exp_01 incident, repeated with its prevention rule already written

**In plain words:** exp_04's step A1 had to show a MERGE being refused, so it
ran the Silver stream with dedup switched off, expecting the stream to die and
the code to catch it. The stream died as planned and the code caught it — and
Databricks still marked the cell as failed (*"Some streams terminated before
this command could finish!"*), so "Run all" stopped at the first experiment.
This exact behaviour was found on 2026-09-27 (exp_01), and its prevention rule
says what to do instead. I did not apply it.

- What happened: `workspace.silver.exp04_a1_*` built batch 0; batch 1 ran with
  `dedup=False`; the stream failed with `[STREAM_FAILED] … Found error inside
  foreachBatch Python process`; `run()` returned the text; the notebook added
  its own error and failed the command. Nothing after A1 ran. The production
  run of `merge_inventory_cdc` just before it was unaffected (it passed).
- What I thought was wrong: nothing — the design assumed a caught exception
  is a handled one, which is true in Python and not in a Databricks notebook.
- Root cause: a Databricks notebook fails any command in which a stream it
  started ended with an error, caught or not (incidents.md, 2026-09-27). The
  rule lived only in `incidents.md`, which a session start reads by its
  headings. `CLAUDE.md`'s verified-constraints table — the "do not re-discover
  these" list read in full every session — did not have it.
- Fix: in exp_04, a step that must fail no longer runs a stream. It calls the
  stream's own batch function (`make_batch_writer`) on exactly the rows the
  next micro-batch would read (`pending()`, the latest Bronze append by time
  travel) with the batch id the stream would use. The checkpoint does not move,
  so the next real run reads the same rows — the retry a failed micro-batch
  gets. Steps expected to succeed still run the real stream.
- Prevention rule: **a platform behaviour that broke a run goes into
  `CLAUDE.md`'s verified-constraints table the day it is found, not only into
  `incidents.md`.** Added now: "a stream that dies inside a notebook command
  fails the command, caught or not". Before writing an experiment that fails
  something on purpose, search `incidents.md` for the failure kind.
- Lesson: a prevention rule nobody reads at the moment of the next decision
  does not prevent anything; put it where that decision is made.

## [2026-10-02] — DELIBERATE (exp_04): every guard of Silver inventory removed in turn — a refused MERGE, a silent double insert, an old change overwriting a new one, a deleted SKU brought back, contaminated history

**In plain words:** Silver keeps the current stock per SKU from a change feed,
and four things keep it right: one row per SKU per batch (dedup), "only newer
changes apply" (the `seq` guard), "two events at one place in the order stop
the batch" (the tie refusal), and — found missing — "a delete is remembered".
exp_04 took each away on scratch tables, with the production code and real
events, and asserted the damage before the fix.

- What happened, part by part (A = `001795ec…`, stock 28 → 27 → 26 → 25;
  N = `001b72df…`, 38 → 36; X = `157b4fa7…`, 14 → 13 → 12 → delete):
  - **A1, no dedup, SKU already in Silver** → `[DELTA_MULTIPLE_SOURCE_ROW_
    MATCHING_TARGET_ROW_IN_MERGE]`. The stock stayed at A's 1st event (28) —
    but the event log already had the batch's 4 events: it is written first,
    in its own commit. Retried with dedup: A = 26, N = 36, `events_inserted 0`.
  - **A2, no dedup, SKU new to Silver** → **no error, N inserted twice**. Delta
    only refuses several source rows that *match* one target row; two inserts of
    one key are not checked. Silent, and worse than A1's loud failure.
  - **B, no `seq` guard** → A's late 2nd event (27) overwrote its 3rd (26).
    With the guard: ignored (`updated 0`), then a newer 4th event updated it
    to 25 — the UPDATE path and the "older event ignored" path, run for the
    first time.
  - **C, late update after a newer delete** → X came back at 12; the
    log-based check saw it (`extra=1`). Fixed by `newest_from="log"`.
  - **D, tie refusal on Bronze's real contamination** (159 rows = 53 events ×
    3, on 23 SKUs whose clean history is 330 events) → refused,
    `{'tied_seq_vs_log': 53}`, nothing written; the denylist retry dropped all
    159. Guard off: the 53 reached the event log (330 → 383, `chain_breaks 53`),
    units sold 281 instead of 250 — **and the stock was identical** to the
    guarded run, because every bad event sits on an early `seq` the guard
    ignores. `RESTORE … TO VERSION AS OF 1` removed the one file the bad batch
    added (7,840 bytes): log = the guarded run on every column, units sold 250.
- What I thought was wrong: nothing — deliberate. Two expectations had never
  been seen: A2 (B25's answer, now verified) and what a `RESTORE` does to the
  change feed.
- Root cause, per guard: Delta's MERGE checks ambiguity only among matches
  (A); a MERGE has no memory but the target row (B, C); and a history can be
  wrong while the current state is right (D) — which is why the stock alone
  would never have shown the contamination.
- Fix: A, B, D were already production behaviour (S9). C is new: the stock
  MERGE reads each SKU's newest event from the event log (decisions.md,
  Session 10).
- Seen for the first time: **a `RESTORE` writes `delete` rows into the change
  feed** — 53, one per row it removed. Gold, which will read Silver through the
  feed (S13), sees a repair as deletes; the export must carry `delete` rows (a
  carried item since S9, now with a second reason).
- Prevention rule: each guard is a switch on the production batch function,
  exercised by exp_04 against real events; any change to `_merge_inventory_cdc`
  re-runs exp_04 and `merge_inventory_cdc`'s five checks.
- Lesson: the dangerous failure is the quiet one — A1 threw an error, A2 and C
  wrote wrong rows with none. Test the case where nothing complains.

## [2026-10-04] — exp_04's "bug present" step stopped showing the bug, because the fix for part C also fixed part B

**In plain words:** exp_04 part B removes the `seq` guard and expects an old
change to overwrite newer stock. On the re-run after production switched to
"take each SKU's newest event from the event log" (part C's fix), the old change
no longer overwrote anything — A stayed at its newer stock, and the assertion
"expected the older event to overwrite" failed. Nothing was broken: the new
default protects against that case too, so the experiment could no longer see
the bug it was written to show.

- What happened: `run(b1, seq_guard=False)` → A = `(…3000000000, 26)`, the
  3rd event's stock, not the late 2nd's 27. "Run all" stopped there; C and D
  were skipped.
- What I thought was wrong: briefly, that the guard switch was ignored. It was
  not: with the log's newest event, the MERGE's source for A *is* the 3rd
  event (the log holds 1st, 2nd, 3rd; the 3rd has the highest `seq`), so even
  an unguarded `UPDATE` writes the 3rd's values again.
- Root cause: part B depended on a default (`newest_from`) without naming it.
  When the default changed, the experiment silently started testing something
  else.
- Fix: B1 names Session 9's path (`newest_from="batch"`) and shows the bug; a
  new B1b keeps the guard off with the production default and asserts A stays
  at its 3rd event — this run's accidental finding, kept as a check.
- Prevention rule: **an experiment step that shows a bug names every switch it
  depends on**, so a change of production default cannot quietly turn "bug
  present" into "testing the fix".
- Lesson: reading the newest event from the log made the `seq` guard a second
  line of defence for plain out-of-order arrival, not just for deletes — found
  because a bug-first assertion failed instead of passing for the wrong reason.

## [2026-10-04] — DELIBERATE (delta_concurrency_constraints): two MERGEs at once, and writes that break a rule — what Delta refuses, and what it lets through

**In plain words:** two things CLAUDE.md lists as danger zones had never
happened here: two jobs writing one table at the same moment, and a write that
breaks a table rule. Both were caused on purpose on scratch copies of Silver.
Delta refused what it should — and two of its answers were not what standard
SQL would predict.

- What happened, concurrency (two threads, one MERGE each, started at one
  instant; a scratch copy of `silver.inventory`, 34,348 rows in **one file**):
  - deletion vectors **on**, different rows → **both committed**, both having
    read v0 — Databricks checked rows, not files (*row-level concurrency*);
  - deletion vectors on, **same** rows → one committed; the other
    `[DELTA_CONCURRENT_APPEND.ROW_LEVEL_CHANGES] … modified the same rows`;
  - deletion vectors **off**, different rows → one failed,
    `[DELTA_CONCURRENT_APPEND.WITHOUT_HINT]`: without deletion vectors a MERGE
    rewrites the whole file, so rows it never touched still collide;
  - the loser retried alone → committed (2,000 rows changed); retried again →
    `updated 0`. Isolation level in every history row: `WriteSerializable`.
- What happened, constraints (copies of `silver.orders` / `order_items` with the
  production rules): status `lost` refused, nothing committed; one bad price in
  a 1,000-row MERGE refused the whole MERGE (price total unchanged, 13,591,643.70);
  adding `CHECK (shipped_at >= approved_at)` refused with **3,156** violating
  rows; a NULL status refused; a `PRIMARY KEY` accepted with **no Delta commit**,
  and a duplicate order id then **inserted fine**.
- What I thought was wrong: nothing — deliberate. Two expectations were open
  and are now answered: **a CHECK that evaluates to NULL is a violation** (3,156
  = 1,359 real + 1,797 with a NULL side; standard SQL would pass those), and
  **the primary key is not enforced** on Databricks either.
- Root cause: optimistic concurrency checks, at commit, what a writer read and
  rewrote against what committed since — at file grain, or row grain with
  deletion vectors. Constraints are evaluated per row on every write, in the
  same transaction, so one bad row fails all of it.
- Fix: none needed in production. What it changes: CHECK rules are written for
  never-NULL columns (all three are) or as `col IS NULL OR …`; a writer that
  can collide (S12's GDPR erasure `DELETE` against the daily MERGE) must retry,
  and can, because every Silver write is idempotent by its own condition.
- Prevention rule: a rule the source itself breaks (1,359 orders shipped before
  approval) is counted, never enforced; and every Silver writer stays safe to
  repeat, because the only answer to a write conflict is "run it again".
- Lesson: "enforced" and "declared" are different words — Delta enforces CHECK
  and NOT NULL, and only declares a primary key.

## [2026-10-04] — DELIBERATE (Drill 2): a Silver stream's checkpoint deleted — no data lost, but the lag alarm first lied loudly, then went blind

**In plain words:** Drill 1 deleted a Bronze stream's checkpoint and lost 74
messages. The same attack on Silver lost nothing — the 100 rows waiting were
applied correctly — but the stream's log, which the "Silver is behind Bronze"
alarm counts, silently stopped recording them, so the alarm fired for rows
Silver already had. "Fixing" that with a new app id made the log count every
row twice, and the alarm then stayed quiet with 100 rows really waiting.

- What happened (`drills/drill2_cdc`, scratch copies, production read-only):
  life 1 → log `batch 0, 158,625 rows`, stock 34,448 / 993,982. 100 delists
  appended; checkpoint deleted; same app id → event-log MERGE `inserted 100`,
  stock 34,348 / 991,530 (= production), log **unchanged: 1 line, 158,625**,
  alert query on those tables **1**. Checkpoint deleted again, new app id → log
  `(0, 158,625 …)`, `(0, 158,725, 0, 0, 0)` = **317,350**; 100 more rows landed
  and were never applied → alert **0**. No error anywhere.
- What I thought was wrong: nothing — the prediction, written in the notebook
  before it ran, was "no data lost; the log is". Confirmed on every number.
- Root cause: the MERGEs carry no `txnVersion` (each is safe to repeat by its
  own condition), so a replay changes nothing. The log append does carry it,
  keyed by a batch id that restarts at 0 in a new checkpoint: Delta skips every
  line up to the batch it remembers for that app id. A new app id remembers
  nothing, so everything is logged again. The alert compares *counts* (Bronze
  rows older than 26 h vs rows the log has seen), so it reads a short log as lag
  and a doubled log as health.
- **Why nothing caught it:** the data checks all pass — stock, event log and
  change feed are right. The only wrong thing is the bookkeeping that the alarm
  trusts, and nothing checks the bookkeeping against the data.
- Fix: `refuse_unsafe_reset` in both Silver stream libraries (decisions.md,
  "Silver streams refuse a checkpoint reset …"). Seen firing: both resets refused
  before any stream started, the log untouched; production's checkpoints pass.
- Prevention rule: **a stream with no history may only write into an empty
  log** — a rebuild resets checkpoint, app id and log together. The drill keeps
  the bug visible through a named switch (`guard_reset=False`).
- Lesson: an idempotent write protects the data, not the audit trail around it.
  Every record keyed by a counter that can restart needs the same protection as
  the data it describes.

## [2026-10-04] — Drill 2's "new broken link" step counted 0 breaks: the event it fed in was a query over the table it writes to

**In plain words:** to re-trigger the 2026-10-01 incident, the drill made one
new event for a healthy SKU — "its newest event, plus one" — and fed it to the
production batch function. The batch inserted it, and then the chain check
reported 0 breaks while the whole-log report said 1,424. The event had been
defined as a query over the very event log the batch writes to; Spark re-runs a
query every time it is used, and after the insert "the newest event, plus one"
was a different event — one the log did not hold.

- What happened: line `batch_id 5 · events_inserted 1 · updated 1 ·
  chain_breaks 0`; `chain_report` → 1,424; the cell's assertion failed and
  "Run all" stopped before the last two display cells. Everything before it had
  passed.
- What I thought was wrong: nothing in production. The chain check joins the
  log to the batch's own `event_id`s; the batch DataFrame now named an event id
  that does not exist.
- Root cause: a Spark DataFrame is a recipe, not a result. The batch function
  uses its input several times (refusal check, counts, MERGE, newest-in-log,
  chain check), each a fresh run of the recipe. A real micro-batch is fixed —
  its recipe reads a fixed range of Bronze, which the batch never writes — so
  production cannot hit this. exp_04's `pending()` is also safe: it reads a fixed
  table version.
- **Why nothing caught it sooner:** every earlier drill step fed the batch
  function from production Bronze, which the scratch batch does not write; this
  was the first input built from the batch's own target.
- Fix: the event is frozen into rows first (`createDataFrame(recipe.collect(),
  recipe.schema)`), and the step asserts "+1 break" instead of a pinned 1,424,
  so it can be re-run on the same tables.
- Prevention rule: **anything fed to a batch function by hand is materialised
  first** — rows, or a fixed table version — never a live query over a table
  that function writes.
- Lesson: "DataFrame" reads like a noun; in Spark it is a verb, run again at
  every action.

## [2026-10-04] — DELIBERATE (Drill 2): Bronze's history applied to Silver newest first — production held; Session 9's code brought back all 100 deleted SKUs

**In plain words:** Silver claims arrival order does not matter: for orders
because each lifecycle step has its own column, for inventory because each
SKU's newest event comes from the whole log. The drill fed every Bronze batch to
the production batch functions in the worst order a replay could produce —
newest first. Production ended identical to the real Silver tables. Session 9's
inventory code, run the same way, resurrected every one of the 100 delisted
SKUs.

- What happened, inventory (`drills/drill2_cdc` B): Bronze batches `[0..4]`
  applied `[4, 3, 2, 1, 0]`; inserts 0 / 3,727 / 12,699 / 9,136 / 8,786, every
  batch equal to the prediction computed from Bronze alone; stock and event log
  vs production `(0, 0)`; whole-log chain report 1,423 / 237 / 26 / 27, units
  112,650. With `newest_from="batch"`: stock **34,448 / 993,982** — the
  pre-delist total to the unit — `extra 100`, all 100 delisted SKUs back.
- What happened, orders (`drills/drill2_orders` B): batches `[8..0]`; the
  prediction formula first reproduced production's log in Bronze's own order
  (13,178 / 12,782 + 1,352 / … / batch 8 `(0, 0)`), then matched all 9 batches
  backwards (batch 8 first: 10 orders, 12 items); updates 18,587 both ways, in
  different batches; orders and items vs production `(0, 0)`. Only lineage moved:
  12 items taken from Drill 1's re-sent copies — exactly the 12 units on re-sent
  `created` events.
- What I thought was wrong: nothing — both claims predicted to hold, Session 9's
  path predicted to fail.
- Root cause (Session 9 path): a delete removes the row and its `seq`; newest
  first, every `D` arrives before the SKU exists, matches nothing, and each older
  batch then inserts the SKU again (exp_04 C, now at full scale).
- Seen, not a bug: **the per-batch chain count is exact only in order** — 1,415
  summed per batch vs the true 1,423. A link is checked when its later event
  arrives, against what the log holds then; newest first, the 8 broken links that
  straddle a batch boundary are never checked. Production reads in order; the
  whole-log report is the number to trust.
- **Why nothing had caught the Session 9 gap before exp_04:** production's
  stream has only ever read Bronze in order, where the gap cannot fire.
- Fix: none needed (production's `newest_from="log"` since Session 10).
- Prevention rule: **a claim that order does not matter is tested in the worst
  order, on all the data**, not argued from the design.
- Lesson: an accumulating snapshot needs no ordering guard at all; a CDC mirror
  needs one *and* a memory of deletes — the two Silver tables sit at opposite
  ends of the same question.

## [2026-10-04] — DELIBERATE (Drill 2): the dims merge log lost — customer stopped by the breaker, seller replayed its whole history with no error

**In plain words:** the merge log is the dims path's memory of which night
Silver shows. Deleting it (on scratch copies) made the next run start again from
the first night. Customer was saved by the 5% delete breaker only because its
first night held one customer. Seller's first night holds every seller, so the
breaker saw nothing wrong: 100 deleted sellers were inserted again, 98 changed
sellers put back to their first values, then four nights replayed — ending right,
with the whole history written a second time into the change feed.

- What happened (`drills/drill2_dims` L1): customer `would delete 43,689 of
  43,690 rows (100.0%) … Nothing was merged`, version 5 → 5. Seller
  `2016-09-04: +100 ~98 -0`, `2017-01-02: +0 ~25`, `+0 ~50` ×2, `2017-12-28: +0
  ~50 -100`; change feed `insert 100, update_postimage 273, delete 100`; vs
  production `(0, 0)`. No error.
- What I thought was wrong: nothing — predicted in the notebook before the run,
  `(100, 98, 0)` computed from Bronze and Silver alone.
- Root cause: `plan_nights` trusts the log for "where is Silver?"; with no line,
  "nowhere" — so every night is new. The breaker guards row *counts*, and a
  replayed first night that holds every key deletes nothing.
- **Why nothing caught it:** the end state is correct, every check that compares
  Silver with the newest night passes, and the only damage is in history — the
  change feed — which nothing checks yet.
- Fix: `refuse_lost_log` (decisions.md, Drill 2). Seen firing for both
  dimensions; then the production repair: `RESTORE` the log (15 files back) →
  "nothing new" ×3, no Silver version moved.
- Prevention rule: **a writer with no memory may only write into an empty
  target** — the same rule as the two stream guards.
- Lesson: a correct end state can hide a wrong history, and Gold will read the
  history.

## [2026-10-04] — Drill 2: a corrected old night was silently ignored — Job green, alarm at 0, one printed line

**In plain words:** a dims night older than the one Silver shows was
re-delivered with one city corrected. Silver, by design, does not go back in
time and did not apply it. But nothing told anyone: the Job would have stayed
green, and the "Silver behind Bronze" alarm only counted nights *newer* than the
last one applied. The correction sat in Bronze, unused, with one line of
notebook output as its only trace.

- What happened (`drills/drill2_dims` O, first run): `seller 2017-01-02:
  changed in Bronze, but older than what Silver shows — NOT applied`; `plan:
  apply [], report ['2017-01-02']`; seller version 13 → 13; lag alarm **0**.
- What I thought was wrong: the rule itself is right (decisions.md Session 8,
  "never backwards"); what was missing is anyone hearing about it.
- Root cause: the report path was written as a `print`, and the alarm (Session 9
  revision) was designed around "a night not yet applied", not "a night applied
  and since changed".
- **Why nothing caught it:** no correction of an old night had ever happened —
  dims landing became write-once in Session 8 and `--redeliver` has never been
  used on a night older than the newest.
- Fix: the alarm query counts `old_nights_changed` (decisions.md, Drill 2); the
  drill asserts the Session 9 part still says 0 and the new part says 1, then
  rebuilds seller from empty and sees the count clear.
- Prevention rule: **every "not applied" path ends in something a person will
  see** — a failed Job or an alarm count — never only a printed line.
- Lesson: `print` in a scheduled job is a message to nobody.

## [2026-10-05] — DELIBERATE (Session 11, Lakeflow): `expect_or_fail` on a full refresh — the update stopped at the first bad row, and left the table empty

**In plain words:** the Lakeflow pipeline's `orders_checked` table has a rule,
"every `created` event carries items", that 777 real Bronze rows break (775
Olist orders placed with no items, plus re-sent copies). The rule was switched
from *count them* to *fail the run* and the table was rebuilt from scratch. The
run stopped at the first bad row, as designed — and the rebuild had already
emptied the table, so a table that held 394,164 good rows an hour earlier now
held **0**, with no data written back.

- What happened: pipeline `ledgerline-dlt`, setting `ledgerline.items_rule =
  fail`, "Select tables for refresh" → `orders_checked` only → **Run with full
  refresh**. Update `307b9339…` failed in seconds:
  `EXPECTATION_VIOLATION.VERBOSITY_ALL`, SQL state `22000`, "Flow
  'workspace.silver_dlt.orders_checked' failed to meet the expectation.
  Violated expectations: 'created_has_items'", with the whole row: order
  `809a282b…`, `created` 2016-09-13 15:24:19, `order_status canceled`,
  `items null`, **partition 2, offset 130446**. The three inventory tables:
  "Omitted" (not selected, untouched). Afterwards `SELECT count(*)` → **0**.
  `DESCRIBE HISTORY`: v0 `CREATE TABLE`, v1 `DLT SETUP`, v2 `STREAMING UPDATE`
  (the first run's 394,164 rows), then the full refresh: v3 **`DLT REFRESH`**,
  v4 `DLT SETUP`, v5 `SET TBLPROPERTIES` (`delta.enableRowTracking = true`) —
  and **no write after it**.
- What I thought was wrong: nothing — deliberate. The open question was what a
  failed full refresh leaves behind; predicted (before the count) "0 rows, the
  reset committed, nothing after it". Right.
- Root cause: a full refresh is **two transactions, not one**: the reset
  (`DLT REFRESH`, committed at the start) and the reprocessing (each micro-batch
  its own commit). `expect_or_fail` refuses the micro-batch that holds a bad
  row, so that commit never happens — the reset already has.
- Fix: none needed — the pipeline's own copy, rebuilt in the next run with the
  rule back on `warn`. What it changes for any real use: a full refresh of a
  table with an `expect_or_fail` rule can leave readers an **empty table** until
  someone fixes the data or the rule.
- Prevention rule: `expect_or_fail` only on rules the **source** cannot break
  (here `event_type_known`, `op_known` — 0 failures on every Bronze row); a rule
  real data breaks is `warn` (count it) or `drop` (remove and count it). Before
  a full refresh, check the rules against the source with a plain query — the
  same count the check notebook makes (`created_no_items`).
- Lesson: "the run failed, so nothing changed" is false for a full refresh — the
  reset is its own commit, and it is the one that succeeds.

### Follow-up (2026-10-05, same hour) — the old rows cannot be read back by time travel

`SELECT count(*) FROM workspace.silver_dlt.orders_checked VERSION AS OF 2` (the
version holding the 394,164 rows) failed on the SQL warehouse: "assertion
failed: The reconciliation query was not resolved for the StreamingTable or
MaterializedView." The files of version 2 still exist (nothing has VACUUMed
them), but a pipeline-owned streaming table is not read like a plain Delta
table, and `VERSION AS OF` is refused. So the repair for a DLT table is **re-run
the pipeline from its source** — not `RESTORE` or time travel, which repaired
hand-written Silver in Session 10 (exp_04 D). That holds only while the source
still holds everything: here Bronze does, and Bronze is append-only.

## [2026-10-05] — DELIBERATE (Session 11, `experiments/dlt_auto_cdc_ties`): AUTO CDC fed two different events at one `(sku_key, seq)` — it kept one, said nothing, and happened to keep the right one

**In plain words:** Bronze still holds 53 contaminated stock events, each sharing
a SKU and a sequence number with a real event but carrying a different stock.
Production Silver refuses such a batch before its MERGE and emails a person.
The same rows were fed to Lakeflow's AUTO CDC on purpose. It accepted them with
no error and no warning, and at each of the 53 ties kept one event. It kept the
real one every time — for a reason we can narrow to two candidates but not
prove, and that nothing in our code decided.

- What happened: pipeline `ledgerline-dlt`, setting `ledgerline.apply_denylist =
  false`, "Run pipeline with full table refresh" (update `2109f6e2…`): all four
  tables green, the pipeline's error / warning counters 0, the event log's
  `WARN` / `ERROR` rows only Run 2's deliberate failure. `inventory_cdc_checked`
  158,725 rows (159 contaminated copies, 53 events, all let through).
  `experiments/dlt_auto_cdc_ties`: `53 ties: rows 53, no_real_twin 0,
  stock_differs 53, kept_real 53, kept_contaminated 0, neither 0`. SCD2
  `versions 158,346, open 34,348, distinct_starts 158,346` — one version per
  `(sku_key, seq)`, the same count as the clean run; SCD1 vs production
  `34,348 SKUs, 0 different`. Which rule picked the winner (SQL Editor, each
  kept event against the dropped one): `same_partition 53, kept_arrived_first
  53, kept_higher_stock 53, kept_larger_event_id 31`.
- What I thought was wrong: nothing — deliberate. **Predicted** "no error, no
  warning; one version per `(sku, seq)`; one of the two kept, which one not
  decided by anything we wrote; SCD1 unchanged". Right on all four. The
  prediction expected a mix of real and contaminated winners; it was 53 real.
- Root cause: AUTO CDC orders each key's changes by `sequence_by` and needs one
  distinct change per sequence value — Lakeflow's documentation states this as
  a requirement, and the run shows it is **not checked**. At a tie it keeps one
  row by a rule of its own. The data fits two rules equally: every real event
  **arrived first** (the contamination was published by a test run after the
  full produce, on 2026-09-26; both copies sit on one partition, since the topic
  is keyed by SKU) **and** has the **higher stock**. Comparing event ids is ruled
  out (31 of 53, a coin flip).
- Fix: none to the pipeline — the denylist is back on (`apply_denylist` removed,
  update `4e8c32e8…`: 159 dropped again, `databricks/checks/dlt_vs_silver` green).
- Prevention rule: **a sequence tie is refused before AUTO CDC, never left to
  it.** If Silver inventory ever moves to AUTO CDC, `find_problems`'
  `tied_seq_vs_log` check moves with it — as an `expect_or_fail` on a view that
  counts distinct events per `(sku_key, seq)` over the batch, or as the
  hand-written check before the flow. Exact copies (same event twice) are safe
  to leave to it: the 120 re-sent copies made no extra version in any run.
- Lesson: a declarative tool that is right on your data has not shown you its
  rule — "it kept the real event 53 of 53" holds only while the real event
  arrives first, and a replay is exactly when that stops being true.

## [2026-10-06] — DELIBERATE (Session 11, `experiments/windowed_streaming`): watermarks on Bronze's replayed history — every table right, and two of three predictions wrong

**In plain words:** Bronze's order history was replayed through three
deduplication methods and a stream-stream join, with watermarks tight and
loose. Every output matched the batch answer. The predictions, computed from
Bronze in SQL first, said a 1-hour event-time watermark would lose 6,718 real
events and a 1-minute join watermark 1,851 order lines; both lost **0**. The
predictions were wrong about *which* watermark Spark uses to call a row late,
and about what moves a watermark. The real cost of a watermark showed up only
in the one place it was designed to: a genuinely new event arriving late was
dropped without a word.

- What happened (run `92336d00`, 2026-10-06): Bronze `orders` replays as 9
  batches — 8 of ~50,000 (each ~3 months of event time; neighbours overlap by
  ~1.5 days, e.g. batch 0 ends 2017-06-14 14:43, batch 1 starts 2017-06-13
  03:50) and Drill 1's 74 re-sends (37 events, 2016 event times, arrived
  2026-09-27 18:43). **All 394,090 originals reached Kafka within 6 seconds**
  (2026-09-26 08:36:07 → 08:36:13). Dedup, `event_ts` watermark 1 h / arrival
  (`_kafka_timestamp`) watermark 1 h / insert-only MERGE on `event_id`: each
  **394,090 rows = 394,090 events, 0 duplicates, 0 Bronze events lost**. One new
  event with a 2016 event time, arriving now: event-time **dropped it**; arrival
  and MERGE **kept it** (394,091). Join, order lines ⟕ sale decrements within 1 h,
  one sale removed on purpose: batch answer 102,425 lines, 102,424 matched, 1
  unmatched; stream with a 1-minute and with a 1-day watermark: **102,424 / 1 /
  0 never written**, both. Dedup before the join (two stateful operators
  chained) ran on serverless with no error.
- What I thought was wrong (the predictions, written before the run): event
  time would lose **6,718** first copies (rows at or behind *newest event time in
  all earlier batches − 1 h*); arrival time would keep **~37** re-sends as
  duplicates (originals long forgotten); the tight join would never write
  **1,851** lines.
- Root cause, two mechanisms:
  1. **A row is judged late against the previous batch's watermark, not the
     current one.** Spark (3.4+) keeps two per micro-batch: one to evict state
     (*newest time through the previous batch − delay*) and one to drop late
     rows (the previous batch's eviction watermark — one batch older still), so
     that chained stateful operators do not drop each other's output. The
     progress report shows it: `dedup_arrival` batch 2 ran under
     `2026-09-26T07:36:07.85`, batch **0**'s newest arrival (08:36:07.859) − 1 h.
     Against batches two back — 3 months behind — no row was late. With
     3-month batches the delay setting (1 minute vs 1 day) made **no difference**:
     batch size, not delay, decided lateness.
  2. **A watermark moves only when newer data arrives.** The arrival-time dedup
     held **all 394,090 ids in state** (state rows 394,090 at batches 7–9): the
     history arrived in 6 seconds, then nothing for 34 hours but the re-sends,
     so the watermark never passed the originals and the re-sends met them in
     state. On a live topic with steady traffic the watermark would have passed
     them within hours and the re-sends would have come through as duplicates.
- Why the right answers are partly accidents: the event-time dedup removed the
  74 re-sends **as late rows, not as duplicates** — the same rule that then
  dropped the genuinely new late event. The arrival-time dedup was right only
  because the topic was quiet. MERGE was right by construction: its memory is
  the target table, and it never forgets.
- Fix: none to production — Silver orders keeps its MERGE dedup (decisions.md,
  same session).
- Prevention rule: **a watermark is sized from live arrival disorder, never from
  a replay or backfill** — a replay's batches are months wide and its watermark
  lags two of them; predict lateness against the *previous* batch's watermark.
  Dedup that must be exact (every genuine event kept, every copy removed) is a
  MERGE on the event id, not a watermark.
- Lesson: "1 hour" in a watermark is one hour of *data*, not of clock — a quiet
  stream never forgets, and a replayed one never calls anything late.

### Follow-up (2026-10-06, same run) — what the progress reports showed

- Join, per micro-batch: `join_loose` batch 2 ran under 2017-06-13 14:43:13 —
  batch 0's newest order (2017-06-14 14:43:13) − 1 day: the watermark is two
  batches back here too. **Peak state 194,169 rows (1-day delay) vs 193,661
  (1-minute)** — 508 apart. The stock stream's batches cover ~9 months of event
  time against ~3 for orders, so sale rows wait in state for orders to catch
  up; state then drains to 3 rows by batch 9. The delay barely moved it — the
  speed difference between the two streams set it.
- `dropped_by_watermark` **60 at batch 3 and 20 at batch 8, the same in both
  runs**, while no order line went missing (0 never written): all 80 were
  re-sent copies (Drill 1's order re-sends in batch 8; the stock feed's re-sends,
  most likely, in batch 3 — not checked row by row) — removed as late, not as
  duplicates, the same as the event-time dedup in part A.
- `dedup_merge`: watermark null, state 0 in every batch — the MERGE path keeps
  no Spark state; its memory is the target table.

## [2026-10-06] — DELIBERATE (Session 11, `experiments/delta_schema_evolution`): each schema change the registry allows, written into Delta — one silent drop, one late refusal, one broken change feed

**In plain words:** the Schema Registry contract lets a producer add a field,
remove one, or widen a type. Each was pushed into a scratch copy of Bronze with
the production writer, then on into a Silver-style MERGE and a change-feed
reader. A new field was refused by the old Bronze writer (now accepted), a
widened type was accepted silently until the first value too big for the old
type, a new field vanished from `MERGE … INSERT *` with no error, and renaming a
column made the change feed unreadable across the rename.

- What happened (scratch `workspace.silver.s11_evo_*`, 2026-10-06):
  - **E1, a field added** (`channel`): Bronze's writer as it was
    (`merge_schema=False`) → `DELTA_METADATA_MISMATCH`, version unchanged. As it
    is now (`mergeSchema`) → 84 rows: 74 older with `channel` NULL, 10 with
    `'web'`; the same batch replayed → still 84 (`txnVersion` unaffected).
  - **E2, `schema_version` `int` → `long`**: values that fit → **written, column
    stays `int`, no error**; 3,000,000,000 → `CAST_OVERFLOW_IN_TABLE_INSERT`,
    nothing written. `delta.enableTypeWidening` → the retry wrote it, column
    `bigint`.
  - **E3, the field reaching a Silver MERGE**: `MERGE … WHEN NOT MATCHED THEN
    INSERT *` → **no error, `channel` simply absent** from the target. (The first
    run's source held 16 rows for 10 events — my mistake — and the plain MERGE
    inserted all 16: a new key inserted twice, silently, as exp_04 A2 showed.)
  - **E4, rename and drop**: without column mapping → `DELTA_UNSUPPORTED_RENAME_COLUMN`.
    With `delta.columnMapping.mode = 'name'`: 2 files / 1,318,323 bytes before
    and after; `RENAME COLUMN` and `DROP COLUMNS` commits with empty metrics.
  - **E5, the change feed across them**: `table_changes(t, 1)` →
    `DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE … between version 1 and 5`;
    from version 5 on → reads (130 pre-images, 130 post-images).
- What I thought was wrong: two predictions failed. **E2**: predicted "refused
  even with `mergeSchema`" — it was accepted (the decision written on that
  prediction got a correction the same hour). **E3**'s evolving half failed on
  my own duplicated source, not on Delta.
- Root cause: `mergeSchema` *adds* columns and otherwise **keeps the table's
  types, casting incoming values into them**; ANSI mode turns a value that does
  not fit into an error, so the cast is safe but late. `INSERT *` / `UPDATE SET *`
  mean "every *target* column, from the source column of the same name" — a
  source column the target lacks has nowhere to go. A rename or drop changes
  what a column *is* between two versions, so the change feed refuses to return
  rows that span it (it cannot give both sides one schema).
- Why none of it would be seen: the widened type and the dropped field both
  write successfully, and the rows look right; the overflow arrives with some
  later value; the change-feed failure arrives only when a reader spans the
  rename — in Session 13's export, the first run after a rename.
- Fix: production Bronze appends with `mergeSchema` (decisions.md, Session 11);
  production Silver already lists its columns by name (a new Bronze column is
  ignored until mapped, by design).
- Prevention rule: **a Silver column is never renamed or dropped in place** — a
  rename is a new column (both kept, then the old one dropped after every reader
  of the change feed has passed that version), and the S13 export treats any
  rename / drop version as a new baseline (full reload). A type widening reaching
  Bronze is fixed by enabling type widening on that table, then re-running the
  refused batch — never by casting the producer's values down.
- Lesson: schema evolution fails in three different places — at the write, at
  the first value that does not fit, or at the first reader that spans the
  change — and only the first is where the change happened.

## [2026-10-06] — `workspace.silver` reached 80% of Unity Catalog's 100-tables-per-schema quota, almost all of it experiment scratch — found by Databricks' email, not by us

**In plain words:** Unity Catalog allows at most 100 tables in one schema.
Production Silver is about a dozen tables, but every experiment and drill since
Session 8 has created its scratch tables in the same schema — exp_04 alone
makes 36 — and nothing removed them or counted them. Databricks emailed at 80%.
At 100, the next `CREATE TABLE` in `workspace.silver` would fail, whether a
production rebuild or an experiment asked for it.

- What happened: email from Databricks, 2026-10-06 02:33 IST, "Databricks
  account is approaching a Unity Catalog resource quota": *"Your Databricks
  account has reached 80% of the 100 Table per Schema quota in the following
  Schema: workspace.silver"*. Counted per prefix: (below, once measured).
- Why nothing caught it: no check, alert or test counts tables per schema;
  `progress.md` has carried a growing "scratch leftovers" list since Session 8 as
  prose, which nobody acts on; each experiment rebuilds its own tables, so no
  single run ever looks like a lot. The quota is not in CLAUDE.md's verified
  constraints — it had never been met.
- Measured (`information_schema.tables`, same hour): **`workspace.silver` 84
  tables = 11 production + 73 scratch** (exp04 36, drill2 17, s11 10, dx 6,
  exp03 4); `workspace.bronze` 23 = 8 production + 15 scratch (drill1 6, exp02 5,
  exp01 4). Experiments that already used schemas of their own (`exp03_*`,
  `exp03h_*`, `drill2_*` schemas: 3–5 tables each) are nowhere near it.
  `workspace.silver_dlt` holds 9: our 4 declared tables plus 5 the pipeline made
  itself (4 with names starting `__`, AUTO CDC's internal storage, and one
  `event…` table).
- Root cause: scratch tables were written into the production schemas because
  that is where the code they exercise reads and writes; the quota was never
  part of the design.
- Fix: `databricks/ops/drop_scratch` removes the 88 scratch tables (plan first,
  production by name, unknown tables stop it); new experiments write to their own
  schema (decisions.md, same day); the 11 older notebooks move at Drill 3.
- Prevention rule: **no experiment or drill creates a table in `bronze` or
  `silver`** — each gets its own schema, dropped as a unit; and tables per schema
  are on the pipeline-health dashboard against the 100 quota, beside Databricks'
  own email at 80%.
- Lesson: a quota nobody has met is not in anyone's checklist — the side effects
  of experiments pile up in exactly the place production depends on.
