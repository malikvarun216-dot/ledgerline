"""Shared plumbing for the three source generators.

Why a shared module
-------------------
All three generators need the same four things, and getting any of them subtly
different between sources would poison every downstream comparison:

* a **replay clock** that separates business time from wall-clock time,
* a **sink** they can write to without a cloud account existing yet,
* **deterministic event IDs**, so a replay produces byte-identical events,
* one **envelope convention**, so Bronze can treat all three feeds alike.

The interesting one is the third. See :func:`deterministic_id`.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

# Every generator stamps these. Bronze can read any of the three feeds without
# knowing which one it is looking at.
ENVELOPE_FIELDS = ("event_id", "event_ts", "produced_at", "source", "schema_version")

SCHEMA_VERSION = 1

# Olist timestamps carry no timezone. We declare them UTC once, here, rather
# than letting each generator guess — a generator that quietly used local time
# would shift every event by the machine's offset and break event-time joins
# across sources in a way that is very hard to see.
ASSUMED_TZ = UTC

_UNIT_SEPARATOR = "\x1f"
# None and "" are different states (an absent category vs a blank one), so they
# must not hash alike. Neither byte can appear in Olist data.
_NULL_SENTINEL = "\x00"


# --------------------------------------------------------------------------
# Deterministic identity
# --------------------------------------------------------------------------


def deterministic_id(*parts: Any) -> str:
    """Stable 128-bit hex ID derived from a natural key.

    Why not ``uuid4()``: a fresh random ID per run means replaying the
    generator produces *different* rows, so a dedup-on-ID at Silver can never
    be shown to work — you would only ever be testing Kafka's offset mechanism.
    With a deterministic ID, replay is provable from the content side too, and
    the exactly-once experiment (experiments/exp_01) has something real to
    assert against.

    The ``\\x1f`` separator matters. Joining with no separator makes
    ``("ab", "c")`` and ``("a", "bc")`` hash identically — a silent collision
    between two genuinely different events. Unit Separator cannot appear in
    Olist IDs, dates, or status strings.

    ``None`` maps to its own sentinel rather than to ``""``, so a missing value
    and a blank value stay distinguishable — an absent product category and an
    empty one are different states.
    """
    joined = _UNIT_SEPARATOR.join(_NULL_SENTINEL if p is None else str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:32]


def to_micros(value: datetime) -> int:
    """datetime -> microseconds since epoch, the Avro ``timestamp-micros`` unit."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=ASSUMED_TZ)
    return int(value.timestamp() * 1_000_000)


def from_micros(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=ASSUMED_TZ)


def envelope(
    event_id: str,
    event_ts: datetime,
    source: str,
    produced_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the five common fields.

    ``event_ts`` is business time — when the thing happened. Watermarks key on
    this. ``produced_at`` is wall-clock at emission — processing time. Keeping
    both on every record is what makes the event-time vs processing-time
    distinction observable in the data rather than just discussed.
    """
    return {
        "event_id": event_id,
        "event_ts": to_micros(event_ts),
        "produced_at": to_micros(produced_at or datetime.now(tz=ASSUMED_TZ)),
        "source": source,
        "schema_version": SCHEMA_VERSION,
    }


# --------------------------------------------------------------------------
# Replay clock
# --------------------------------------------------------------------------


@dataclass
class ReplayClock:
    """Maps Olist business time onto wall-clock time at a compression ratio.

    Olist spans ~25 months (2016-09 to 2018-10). Replaying that at real speed
    is useless; replaying it with no notion of time at all loses the ability to
    demo a live stream. So business time is *compressed*: ``speedup`` business
    seconds elapse per wall-clock second.

    ``speedup=0`` means "no pacing at all" — emit as fast as the CPU allows.
    That is the default for writing files and for tests; a live Kafka demo uses
    something like ``speedup=2_592_000`` (one month per second).
    """

    business_start: datetime
    speedup: float = 0.0
    wall_start: datetime | None = None

    def __post_init__(self) -> None:
        if self.business_start.tzinfo is None:
            self.business_start = self.business_start.replace(tzinfo=ASSUMED_TZ)
        if self.wall_start is None:
            self.wall_start = datetime.now(tz=ASSUMED_TZ)
        if self.speedup < 0:
            raise ValueError("speedup must be >= 0 (0 means unpaced)")

    @property
    def paced(self) -> bool:
        return self.speedup > 0

    def wall_for(self, business_ts: datetime) -> datetime:
        """Wall-clock instant at which ``business_ts`` should be emitted."""
        if business_ts.tzinfo is None:
            business_ts = business_ts.replace(tzinfo=ASSUMED_TZ)
        if not self.paced:
            return self.wall_start  # type: ignore[return-value]
        elapsed_business = (business_ts - self.business_start).total_seconds()
        return self.wall_start + _seconds(elapsed_business / self.speedup)  # type: ignore[operator]

    def sleep_until(self, business_ts: datetime) -> float:
        """Block until this event is due. Returns the seconds actually slept.

        Never sleeps for an event already past due — a generator that fell
        behind should catch up, not accumulate the lag.
        """
        if not self.paced:
            return 0.0
        due = self.wall_for(business_ts)
        delay = (due - datetime.now(tz=ASSUMED_TZ)).total_seconds()
        if delay <= 0:
            return 0.0
        time.sleep(delay)
        return delay


def _seconds(count: float):
    from datetime import timedelta

    return timedelta(seconds=count)


# --------------------------------------------------------------------------
# Message sinks  (order events, inventory CDC)
# --------------------------------------------------------------------------


class MessageSink(Protocol):
    """Where a stream generator writes records.

    Exists because no Confluent account is created until Session 2, and because
    unit tests must not need a broker. The generators are written against this
    protocol, so the same code path produces local JSONL today and Avro-to-Kafka
    in Session 3 with no edit to generator logic.
    """

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


class JsonlSink:
    """Newline-delimited JSON on local disk, one file per topic.

    The offline twin of :class:`KafkaAvroSink`. Deliberately records the message
    key alongside the value: the key is what determines Kafka partitioning, so
    dropping it would make the local file an unfaithful stand-in for the topic.
    """

    def __init__(self, out_dir: Path | str) -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._handles: dict[str, Any] = {}
        self.counts: dict[str, int] = {}

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if topic not in self._handles:
            path = self.out_dir / f"{topic}.jsonl"
            self._handles[topic] = path.open("w", encoding="utf-8")
            self.counts[topic] = 0
        self._handles[topic].write(json.dumps({"key": key, "value": value}, default=str) + "\n")
        self.counts[topic] += 1

    def flush(self) -> None:
        for handle in self._handles.values():
            handle.flush()

    def close(self) -> None:
        self.flush()
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()


class MemorySink:
    """Collects messages in a list. Used by the test suite only."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.messages.append((topic, key, value))

    def flush(self) -> None:  # pragma: no cover - nothing buffered
        pass

    def close(self) -> None:  # pragma: no cover - nothing to close
        pass

    def for_topic(self, topic: str) -> list[dict[str, Any]]:
        return [value for t, _, value in self.messages if t == topic]

    def keys_for_topic(self, topic: str) -> list[str]:
        return [key for t, key, _ in self.messages if t == topic]


class KafkaAvroSink:
    """Confluent Cloud producer with Avro serialization via Schema Registry.

    Built in Session 1, first exercised against a real cluster in Session 3 —
    there is no Confluent account yet, so nothing here is verified against a
    live broker. Treat it as unproven until progress.md says otherwise.

    Config comes from the environment (``.env``, never committed):
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET,
        SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET
    """

    def __init__(self, schemas: dict[str, str], config: dict[str, str] | None = None) -> None:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer

        cfg = config or _kafka_config_from_env()
        registry = SchemaRegistryClient(
            {
                "url": cfg["schema.registry.url"],
                "basic.auth.user.info": cfg["schema.registry.basic.auth.user.info"],
            }
        )
        self._producer = Producer(
            {
                "bootstrap.servers": cfg["bootstrap.servers"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": cfg["sasl.username"],
                "sasl.password": cfg["sasl.password"],
                # Idempotent producer: dedupes broker-side retries so a network
                # blip cannot duplicate a record in the topic. Cheap, and it
                # keeps "exactly once" honest on the produce side too.
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
            }
        )
        self._key_serializer = StringSerializer("utf_8")
        self._value_serializers = {
            topic: AvroSerializer(registry, schema_str) for topic, schema_str in schemas.items()
        }
        self.counts: dict[str, int] = {}
        self._errors: list[str] = []

    def _on_delivery(self, err, msg) -> None:  # pragma: no cover - needs a broker
        if err is not None:
            self._errors.append(str(err))

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:  # pragma: no cover
        from confluent_kafka.serialization import MessageField, SerializationContext

        payload = self._value_serializers[topic](
            value, SerializationContext(topic, MessageField.VALUE)
        )
        self._producer.produce(
            topic=topic,
            key=self._key_serializer(key),
            value=payload,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)
        self.counts[topic] = self.counts.get(topic, 0) + 1

    def flush(self) -> None:  # pragma: no cover - needs a broker
        self._producer.flush()
        if self._errors:
            raise RuntimeError(f"{len(self._errors)} delivery failures; first: {self._errors[0]}")

    def close(self) -> None:  # pragma: no cover - needs a broker
        self.flush()


def _kafka_config_from_env() -> dict[str, str]:
    required = {
        "bootstrap.servers": "KAFKA_BOOTSTRAP_SERVERS",
        "sasl.username": "KAFKA_API_KEY",
        "sasl.password": "KAFKA_API_SECRET",
        "schema.registry.url": "SCHEMA_REGISTRY_URL",
    }
    config: dict[str, str] = {}
    missing: list[str] = []
    for key, env_name in required.items():
        value = os.environ.get(env_name)
        if not value:
            missing.append(env_name)
        else:
            config[key] = value
    if missing:
        raise RuntimeError(
            f"Kafka sink needs {', '.join(missing)} in the environment. "
            "Create a .env from .env.example (Session 2 sets up Confluent)."
        )
    config["schema.registry.basic.auth.user.info"] = (
        f"{os.environ.get('SCHEMA_REGISTRY_API_KEY', '')}:"
        f"{os.environ.get('SCHEMA_REGISTRY_API_SECRET', '')}"
    )
    return config


# --------------------------------------------------------------------------
# Blob sinks  (nightly dimension dumps)
# --------------------------------------------------------------------------


class BlobSink(Protocol):
    """Where the nightly dumps land. Needs read-back, not just write.

    ``dim_dumps`` must see last night's dump to decide whether a row's content
    actually changed — that is what keeps ``dim_updated_at`` a *business*
    timestamp instead of a run timestamp.
    """

    def put_text(self, key: str, text: str) -> None: ...
    def get_text(self, key: str) -> str: ...
    def list_keys(self, prefix: str) -> list[str]: ...


class LocalBlobSink:
    """Writes dumps to a local directory, mirroring the S3 key layout exactly."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put_text(self, key: str, text: str) -> None:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def get_text(self, key: str) -> str:
        return (self.root / key).read_text(encoding="utf-8")

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root
        return sorted(
            str(p.relative_to(base)).replace("\\", "/")
            for p in base.rglob("*")
            if p.is_file() and str(p.relative_to(base)).replace("\\", "/").startswith(prefix)
        )


class S3BlobSink:
    """The same layout in S3. Auto Loader reads this prefix in Session 6."""

    def __init__(self, bucket: str, prefix: str = "", client: Any = None) -> None:
        import boto3

        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self._s3 = client or boto3.client("s3")

    def _full(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put_text(self, key: str, text: str) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=self._full(key), Body=text.encode("utf-8"))

    def get_text(self, key: str) -> str:
        response = self._s3.get_object(Bucket=self.bucket, Key=self._full(key))
        return response["Body"].read().decode("utf-8")

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self._s3.get_paginator("list_objects_v2")
        found: list[str] = []
        strip = len(self.prefix) + 1 if self.prefix else 0
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._full(prefix)):
            for obj in page.get("Contents", []):
                found.append(obj["Key"][strip:])
        return sorted(found)


def chunked(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    """Yield fixed-size chunks. Used for progress reporting on long runs."""
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
