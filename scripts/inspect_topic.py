"""Read a Confluent topic back and re-derive its shape, without disturbing it.

Why this exists
---------------
``scripts/inspect_stream.py`` does this for a local JSONL file. This is its
Kafka twin, and it exists because Session 4 verified the first live produce
with a consumer typed into a throwaway file, which was then lost. The numbers
it printed are in ``progress.md``; the code that produced them is not, so
nobody can re-run the check or argue with it.

At 394,090 records the numbers matter more and the temptation to trust the
producer's own summary is stronger. The producer counts what it *enqueued*.
Only a consumer can count what actually landed.

    python scripts/inspect_topic.py orders
    python scripts/inspect_topic.py inventory.cdc

Two design choices worth stating
--------------------------------
**It assigns partitions; it does not subscribe.** Subscribing joins a consumer
group, and a consumer group is state the broker remembers — committed offsets,
a rebalance, a group that shows up in the console afterwards. A verifier that
changes what it verifies is not a verifier. Assigning explicitly also makes
"read to the end" exact: the end is the high watermark, read once up front,
rather than "no message arrived for a few seconds".

**It never commits.** This follows from assigning rather than subscribing, and
it is also the point: if this committed, the second run would start from where
the first stopped and print zero records, and the natural reading of that is
"the topic is empty" rather than "I moved my own bookmark".

What it reports that the producer cannot
----------------------------------------
* **records per partition** — the skew number. The key is ``order_id`` for
  orders and ``product_id|seller_id`` for CDC, so an even spread is evidence
  the partitioner is hashing the key the way the Session 1 decision assumed.
* **distinct ``event_id`` against total** — duplicates are visible here and
  nowhere else, because deterministic ids make a replay byte-identical.
* everything ``inspect_stream`` reports, from the same functions, so a topic
  and a file can be compared line for line.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from inspect_stream import show_cdc, show_envelope, show_orders

from generators._common import ProgressTicker, load_local_env

# Long enough to ride out a slow rebalance-free fetch, short enough that a
# misconfiguration surfaces as an error rather than as the Session 4 silence.
POLL_TIMEOUT_SECONDS = 30.0


def _consumer(group_suffix: str) -> Any:
    from confluent_kafka import Consumer

    from generators._common import _kafka_config_from_env

    cfg = _kafka_config_from_env()
    return Consumer(
        {
            "bootstrap.servers": cfg["bootstrap.servers"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": cfg["sasl.username"],
            "sasl.password": cfg["sasl.password"],
            # Required by the client even when assigning, and never used to
            # join a group. Named so it is obvious in the console that this is
            # a read-only checker and not a pipeline consumer.
            "group.id": f"ledgerline-verifier-{group_suffix}",
            "client.id": f"ledgerline-verifier-{group_suffix}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def _deserializer(topic: str) -> Any:
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    from generators._common import _kafka_config_from_env

    cfg = _kafka_config_from_env()
    registry = SchemaRegistryClient(
        {
            "url": cfg["schema.registry.url"],
            "basic.auth.user.info": cfg["schema.registry.basic.auth.user.info"],
        }
    )
    # No schema string is passed: the writer's schema id travels in the message
    # and the registry is asked for it. Pinning a reader schema here would hide
    # exactly the compatibility problem this project wants to be able to see.
    return AvroDeserializer(registry)


def normalize_logical_types(value: dict[str, Any]) -> dict[str, Any]:
    """Turn Avro logical-type objects back into the ints the JSONL path holds.

    The same event does not come back as the same Python object depending on
    where it is read from, and that is not a bug in either reader:

    * from JSONL, ``event_ts`` is the integer 1473024919000000, because JSON
      has no timestamp type and the generator wrote micros;
    * from Kafka, ``event_ts`` is ``datetime(2016, 9, 4, 21, 15, 19, tzinfo=utc)``,
      because the Avro schema declares ``logicalType: timestamp-micros`` and the
      deserializer honours it.

    Found the first time this script reused ``inspect_stream``'s report
    functions, which divide by 1e6 and got a ``datetime``. Normalizing here,
    rather than making those functions accept both, keeps one definition of
    what the numbers mean — and the file remains the reference shape.

    Worth carrying into Bronze: Spark reading these messages will land a
    timestamp column, not a long, for the same reason.
    """
    normalized = dict(value)
    for field in ("event_ts", "produced_at", "estimated_delivery_date"):
        current = normalized.get(field)
        if isinstance(current, datetime):
            normalized[field] = int(current.timestamp() * 1_000_000)
    return normalized


def read_topic(topic: str, limit: int | None = None) -> tuple[list[dict[str, Any]], Counter, Counter]:
    """Consume ``topic`` from the start to its high watermarks.

    Returns the decoded values, a per-partition record count, and a per-partition
    count of the *keys* seen, which is what distinguishes "one partition got
    more records" from "one partition got more distinct entities".
    """
    from confluent_kafka import TopicPartition
    from confluent_kafka.serialization import MessageField, SerializationContext

    consumer = _consumer(topic.replace(".", "-"))
    decode = _deserializer(topic)

    metadata = consumer.list_topics(topic, timeout=POLL_TIMEOUT_SECONDS)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        raise SystemExit(f"topic {topic!r} not found on the cluster")
    partitions = sorted(metadata.topics[topic].partitions)

    # Watermarks up front: the end of the topic is a number, not the absence of
    # a message. A poll-until-quiet loop cannot tell a finished read from a
    # stalled one, which is the same confusion that cost Session 4 three
    # minutes.
    ends: dict[int, int] = {}
    total_expected = 0
    for partition in partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=POLL_TIMEOUT_SECONDS
        )
        ends[partition] = high
        total_expected += max(0, high - low)
        print(f"  partition {partition}: offsets {low:,} .. {high:,}", file=sys.stderr)

    consumer.assign([TopicPartition(topic, partition, 0) for partition in partitions])

    values: list[dict[str, Any]] = []
    per_partition: Counter = Counter()
    keys_per_partition: dict[int, set] = {partition: set() for partition in partitions}
    done: set[int] = {p for p in partitions if ends[p] <= 0}
    ticker = ProgressTicker(
        limit or total_expected, label=f"{topic} records", stream=sys.stderr
    )

    try:
        while len(done) < len(partitions):
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is None:
                raise SystemExit(
                    f"no message for {POLL_TIMEOUT_SECONDS}s with "
                    f"{len(partitions) - len(done)} partition(s) short of their "
                    f"watermark — read {len(values):,} of ~{total_expected:,}"
                )
            if message.error():
                raise SystemExit(f"consume error: {message.error()}")

            partition = message.partition()
            value = decode(
                message.value(), SerializationContext(topic, MessageField.VALUE)
            )
            values.append(normalize_logical_types(value))
            per_partition[partition] += 1
            if message.key() is not None:
                keys_per_partition[partition].add(message.key().decode("utf-8"))
            ticker.tick()

            if message.offset() >= ends[partition] - 1:
                done.add(partition)
            if limit and len(values) >= limit:
                break
    finally:
        # close() on an assigned consumer leaves no group state behind; it is
        # here to release the connection, not to check anything in.
        consumer.close()

    ticker.done()
    return values, per_partition, Counter(
        {partition: len(keys) for partition, keys in keys_per_partition.items()}
    )


def show_partitions(per_partition: Counter, keys_per_partition: Counter) -> None:
    """Print the spread, and say how far off even it is.

    Skew is reported as a percentage of the even share rather than as raw
    counts, because "partition 2 holds 131,940" means nothing on its own and
    "+0.4% off even" means everything.
    """
    total = sum(per_partition.values())
    partitions = sorted(per_partition)
    even = total / len(partitions) if partitions else 0
    print("\n  records per partition:")
    for partition in partitions:
        count = per_partition[partition]
        drift = (count - even) / even * 100 if even else 0
        print(
            f"      p{partition}  {count:>9,}  {drift:>+6.1f}% off even"
            f"   {keys_per_partition[partition]:>8,} distinct keys"
        )
    if even:
        worst = max(abs(per_partition[p] - even) / even * 100 for p in partitions)
        verdict = "even" if worst < 5 else "SKEWED"
        print(f"  worst partition drift    {worst:>8.1f}%  <- {verdict}")


def show_cdc_ordering(values: list[dict[str, Any]]) -> None:
    """Check ``seq`` ordering per key, which is the only place it can hold.

    ``inspect_stream`` prints "seq monotonic overall" and that is a fair check
    **for a file**, which is written in emit order. It is meaningless for a
    topic and this script's first run proved it by printing ``False`` in red
    letters for a feed that is perfectly well ordered.

    Kafka orders records *within a partition*, not across a topic. Reading
    three partitions interleaved produces a sequence that is not globally
    ascending and never could be, whatever the data looks like. The property
    Silver actually depends on is narrower and does hold:

        WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET *

    compares ``seq`` between two rows **with the same sku_key**, and a sku_key
    hashes to exactly one partition, so its events arrive in order. So that is
    what gets checked here.

    Also reports keys where two records share one ``seq``, because a strictly-
    greater guard silently discards the second — which is a wrong value that
    persists rather than an error that surfaces.
    """
    by_key: dict[str, list[tuple[int, int]]] = {}
    for index, value in enumerate(values):
        by_key.setdefault(value["sku_key"], []).append((index, value["seq"]))

    out_of_order = 0
    tied = 0
    tied_keys: list[str] = []
    for key, entries in by_key.items():
        seqs = [seq for _, seq in entries]
        if seqs != sorted(seqs):
            out_of_order += 1
        if len(set(seqs)) != len(seqs):
            tied += 1
            if len(tied_keys) < 3:
                tied_keys.append(key)

    print(f"\n  keys                     {len(by_key):>8,}")
    print(
        f"  keys with seq ascending  {len(by_key) - out_of_order:>8,}"
        f"   <- the property Silver's guard needs"
    )
    if out_of_order:
        print(f"  keys OUT OF ORDER        {out_of_order:>8,}")
    if tied:
        print(
            f"  keys with a TIED seq     {tied:>8,}"
            "   <- 's.seq > t.seq' drops the second, silently"
        )
        for key in tied_keys:
            print(f"      {key}")


def main() -> int:
    load_local_env()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("topic", help="orders | inventory.cdc")
    parser.add_argument(
        "--limit", type=int, default=None, help="stop after N records (a smoke read)"
    )
    args = parser.parse_args()

    print(f"\nreading {args.topic!r} from the beginning", file=sys.stderr)
    values, per_partition, keys_per_partition = read_topic(args.topic, args.limit)
    if not values:
        parser.error(f"topic {args.topic!r} held no records")

    source = values[0].get("source", "?")
    print(f"\n{args.topic}  (source = {source})")
    print("-" * 60)
    show_envelope(values)
    show_partitions(per_partition, keys_per_partition)

    if source == "order_events":
        show_orders(values)
    elif source == "inventory_cdc":
        show_cdc(values)
        show_cdc_ordering(values)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
