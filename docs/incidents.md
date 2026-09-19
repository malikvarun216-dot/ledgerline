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

<!-- Append further entries below this line. -->
