# ledgerline

A multi-source e-commerce data platform built to exercise every major
idempotency and write pattern — **each assigned to the layer and source where
it is the honest industry-standard choice**, not forced in for coverage.

*ledger* is the thesis: a correct, auditable record of what happened.
*line* nods to order line items.

Where two patterns could both work, both are built, and the choice is recorded
with its rejected alternative. That comparative judgment is the point.

---

## Architecture

Three sources, chosen for distinct **temporal shape** rather than distinct
transport:

| Source | Transport | Temporal shape |
|---|---|---|
| Order events | Confluent topic `orders` | Immutable facts — append-only, never updated |
| Customer / product / seller dumps | Nightly CSV → S3 | Slowly-changing — *history matters* |
| Inventory stock CDC | Confluent topic `inventory.cdc` | Fast-changing — *only current state matters* |

The inventory feed is **causally coupled** to the order stream: every stock
decrement traces back to a real `olist_order_items` row. That is what makes the
cross-source reconciliation — units sold ↔ summed decrements — a genuine check
rather than two unrelated numbers.

**Platform split.** Databricks owns Bronze + Silver (Unity Catalog, Delta MERGE,
Structured Streaming, Auto Loader, DLT). Snowflake owns Gold (dbt snapshots and
incrementals, Streams & Tasks, Dynamic Tables, query profile). The bridge is
Parquet in S3 read through a Snowflake external stage.

### Pattern-to-layer mapping

| Layer | Source | Pattern |
|---|---|---|
| Bronze | Order events | Append-only, exactly-once via Kafka offsets (`txnAppId` / `txnVersion`) |
| Bronze | Dimension dumps | Partition overwrite — Auto Loader + `Trigger.AvailableNow` + `replaceWhere` |
| Bronze | Inventory CDC | Append-only raw op log — Bronze does **not** interpret ops |
| Silver | Order events | `MERGE INTO` on `order_id` |
| Silver | Dimensions | `MERGE` + `WHEN NOT MATCHED BY SOURCE DELETE` |
| Silver | Inventory CDC | `MERGE` on op flags + in-batch dedup + `seq` guard |
| Gold | Dimensions | SCD2 via dbt snapshot (`timestamp` strategy) |
| Gold | Order facts | dbt incremental `MERGE` (`unique_key`) + lookback window |

`WHEN NOT MATCHED BY SOURCE DELETE` is on the **snapshot** path only. Applied to
a CDC micro-batch it is destructive: a CDC batch contains only *changed* rows,
so every unchanged target row has no matching source row and gets deleted. CDC
deletes arrive in-band as `op = 'D'`.

---

## Status

**Drill 2 done; Session 11 next** (2026-10-04). Live on Confluent Cloud
(`orders`, `inventory.cdc`), S3 (nightly dimension dumps) and Databricks Free
Edition:

- **Bronze** — every source, exactly once (Kafka offsets + `txnVersion`;
  Auto Loader + `replaceWhere`), with quarantine and provenance headers.
- **Silver** — dimensions by snapshot MERGE with delete-by-absence (a bad row is
  held, not the night); inventory by CDC MERGE (op flags, in-batch dedup, `seq`
  guard, a tie guard, the newest event read from the log so a delete is never
  undone); orders as one row per order that fills in as its events arrive.
  Delta `CHECK` constraints on orders, order items and stock; per SKU, units ordered = units the
  stock feed decremented (34,448 SKUs, 0 mismatched).
- **Alarms** — a daily Databricks Job (three Bronze → Silver chains) with a
  failure email and two SQL alerts, each seen firing.
- **Drills** — Bronze (Drill 1) and Silver (Drill 2) attacked on purpose: replays,
  checkpoint resets, history applied backwards, lost logs; every guard seen
  firing, and a regression Job that re-runs every experiment.
- **Experiments** — 4 of 6 deliberate failures done (exactly-once replay,
  partition overwrite, the MERGE gap, CDC correctness), plus concurrent writes
  and constraint violations caused on purpose.
- **Not started** — Gold (Snowflake + dbt), Airflow.

The remaining plan — every Databricks / Snowflake / dbt topic, built or taught as
theory, each bound to a session — is [`docs/coverage.md`](docs/coverage.md).
See [`docs/progress.md`](docs/progress.md) for what has been *verified* as
opposed to merely built — the distinction is maintained deliberately.

---

## Quick start

```bash
python -m pip install -r requirements-dev.txt
```

### 1. Get the data

Olist is on Kaggle and needs a free API token — create it at
[kaggle.com/settings](https://www.kaggle.com/settings) → API → *Create New
Token*, save `kaggle.json` to `~/.kaggle/`, then accept the dataset terms once
at [the dataset page](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

```bash
python scripts/fetch_olist.py
```

Validates every file against the column contract in `generators/olist.py`.
Re-run any time with `--check-only`.

### 2. Run the generators

```bash
python generators/order_events.py --limit 2000
```

```bash
python generators/inventory_cdc.py --limit 2000 --delist-count 5
```

```bash
python generators/dim_dumps.py --nights 6 --stride-days 120 --delete-per-night 20
```

All three default to local output (`data/streams/`, `data/dumps/`) and need no
credentials. Add `--sink kafka` or `--sink s3` from Session 2 onward.

`inventory_cdc.py` exits non-zero if the reconciliation fails, so it doubles as
a check.

### 3. Tests and lint

```bash
python -m pytest tests/
```

```bash
python -m ruff check .
```

---

## Documentation

`docs/` is meant to tell the project's whole story without the chat
history. A per-session recall log is kept alongside these but is personal
working notes rather than project record, so it is not in the repo:

| File | What it holds |
|---|---|
| [`decisions.md`](docs/decisions.md) | Every architectural choice **with its rejected alternative** |
| [`incidents.md`](docs/incidents.md) | Every bug and deliberate breakage, each ending in a prevention rule |
| [`progress.md`](docs/progress.md) | Session log — what was *verified*, not what was built; the experiment tracker |
| [`coverage.md`](docs/coverage.md) | The plan: every Databricks / Snowflake / dbt topic with a weight and a session — built, or theory || [`runbook.md`](docs/runbook.md) | Procedures for known failure modes, including the cost emergency |

Six of the incidents will be **deliberate** — patterns broken on purpose to
observe the failure mode, each with a runnable assertion that the bug is
present before the fix.

---

## Cost

Target **~$22 total**. Databricks runs on **Free Edition** ($0, serverless only)
since Session 6; Confluent Basic is about $0.01/month at rest and cents per full
produce; S3 is cents; the Snowflake trial is $0 but its **30-day calendar** is
the binding limit. AWS budget alarms ($20/month, $2/day) exclude credits.

The risk is no longer a forgotten cluster (there are none on Free Edition —
though **a single node left running a week is ~$120, six times the budget**,
stays true knowledge). It is resources created as side effects — a Flink pool
from a new Confluent environment, a Snowflake warehouse without auto-suspend,
a Snowflake Task left running.

If spend looks wrong, `docs/runbook.md` has the emergency procedure.

---

## Data

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— ~100k orders, 2016-09 to 2018-10. Chosen for two things that cannot be faked:
a real order status lifecycle with multiple timestamps, and customer location
that genuinely varies across orders for the same `customer_unique_id`.

`data/` is gitignored. Nothing in this repo redistributes the dataset.
