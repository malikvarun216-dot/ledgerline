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
import sys
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
# Local configuration
# --------------------------------------------------------------------------

_env_loaded = False


def load_local_env(path: str | Path = ".env") -> bool:
    """Load ``.env`` into ``os.environ``. Call once, from a CLI entry point.

    Session 4 found this missing. ``requirements.txt`` has carried
    ``python-dotenv`` since Session 0 and every docstring here says config
    "comes from ``.env``", but **nothing ever read the file.** Two different
    failures came out of that, and only one of them was loud:

    * the Kafka sink raised "needs KAFKA_BOOTSTRAP_SERVERS" against a fully
      populated ``.env`` — annoying, but it tells you immediately;
    * ``boto3.client("s3")`` found no credentials in the environment and fell
      through its default chain to ``~/.aws/credentials``, which on this
      machine is an **admin** key. It worked perfectly, as the wrong identity.

    Existing environment variables win (``override=False``): a value exported
    in the shell, or set by a test, must beat the file on disk.
    """
    global _env_loaded
    if _env_loaded:
        return True
    try:
        from dotenv import load_dotenv
    except ImportError:  # pragma: no cover - dotenv is in requirements.txt
        return False
    _env_loaded = load_dotenv(path, override=False)
    return _env_loaded


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


class ProgressTicker:
    """Say something every N records, so a long run is visibly alive.

    Session 4's first produce sat silent for 180 seconds and was killed. The
    cause was a wrong port, but the reason it cost three minutes is that
    *silence looks the same as work*. 187 records finish before anyone wonders;
    394,090 do not.

    So this prints a line every ``every`` records with the count, the rate and
    an estimate of what is left. It writes to **stderr**, not stdout, so the
    generator's summary table stays a clean artifact that can be piped or
    diffed while the progress goes to the terminal.

    Deliberately not a progress bar: the output is meant to survive being
    scrolled back through and pasted into an incident entry, and a bar that
    redraws itself in place leaves nothing behind.
    """

    def __init__(
        self,
        total: int,
        every: int = 25_000,
        label: str = "records",
        stream: Any | None = None,
    ) -> None:
        self.total = total
        self.every = max(1, every)
        self.label = label
        self.stream = stream if stream is not None else sys.stderr
        self.seen = 0
        self.started = time.monotonic()

    def tick(self, count: int = 1) -> None:
        """Count ``count`` more records, printing when a boundary is crossed."""
        previous = self.seen
        self.seen += count
        if self.seen // self.every > previous // self.every:
            self._line()

    def _line(self) -> None:
        elapsed = time.monotonic() - self.started
        rate = self.seen / elapsed if elapsed > 0 else 0.0
        parts = [f"  {self.seen:>9,} / {self.total:,} {self.label}"]
        if self.total:
            parts.append(f"{self.seen / self.total:>5.0%}")
        parts.append(f"{rate:>8,.0f}/s")
        remaining = self.total - self.seen
        if remaining > 0 and rate > 0:
            parts.append(f"eta {remaining / rate:>5.0f}s")
        print("   ".join(parts), file=self.stream, flush=True)

    def done(self) -> float:
        """Print the final tally and return the elapsed seconds."""
        elapsed = time.monotonic() - self.started
        rate = self.seen / elapsed if elapsed > 0 else 0.0
        print(
            f"  {self.seen:>9,} {self.label} in {elapsed:,.1f}s  ({rate:,.0f}/s)",
            file=self.stream,
            flush=True,
        )
        return elapsed


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
            # newline="" disables the text-mode translation that would make a
            # Windows-written JSONL file CRLF-terminated and a Linux-written
            # one LF-terminated, from identical generator output.
            self._handles[topic] = path.open("w", encoding="utf-8", newline="")
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

    # How long ``send`` will wait out a full local queue before deciding the
    # broker is not draining at all. 30 x 1s: a healthy cluster empties a
    # 100,000-record queue in far less than one poll, so thirty consecutive
    # polls that free nothing is not slowness.
    _QUEUE_FULL_ATTEMPTS = 30
    _QUEUE_FULL_POLL_SECONDS = 1.0

    # Delivery failures are kept for the message, not for the record — at
    # 394,090 records a broker-wide outage would otherwise build a list of
    # 394,090 near-identical strings in memory while the real count is the
    # only part anyone reads.
    _MAX_KEPT_ERRORS = 10

    def __init__(
        self,
        schemas: dict[str, str],
        config: dict[str, str] | None = None,
        client_id: str | None = None,
    ) -> None:
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
        # Name the client. librdkafka defaults client.id to "rdkafka", so every
        # producer this project runs appears under one indistinguishable label
        # in Confluent's Clients and Stream Lineage views — observed in Session
        # 4 as "producer rdkafka (2)", with no way to tell which generator, or a
        # debug script, was which. Falls back to the topic names so the id is
        # never the default even when a caller forgets to pass one.
        self.client_id = client_id or f"ledgerline-{'-'.join(sorted(schemas))}"
        self._producer = Producer(
            {
                "bootstrap.servers": cfg["bootstrap.servers"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": cfg["sasl.username"],
                "sasl.password": cfg["sasl.password"],
                "client.id": self.client_id,
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
        self.error_count = 0
        self.queue_full_waits = 0

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.error_count += 1
            if len(self._errors) < self._MAX_KEPT_ERRORS:
                self._errors.append(str(err))

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Serialize and enqueue one record, waiting out a full local queue.

        ``produce()`` does not send anything. It appends to librdkafka's local
        queue, which a background thread drains to the broker. The queue holds
        ``queue.buffering.max.messages`` records — 100,000 by default — and
        ``produce()`` raises ``BufferError`` once it is full.

        Session 4 produced 187 records and never came close. Session 5 produces
        394,090, and a generator reading a local CSV is much faster than a TLS
        round-trip to ap-south-1, so the application *will* outrun the broker
        and the queue *will* fill. The fix is not a bigger queue — it is to
        stop producing and let the drain catch up, which is what ``poll()``
        does: it serves delivery callbacks and frees the slots they held.

        Bounded on purpose. An unbounded retry loop would turn a dead broker
        back into the Session 4 silent hang, which is the specific failure this
        class already exists to prevent. After ``_QUEUE_FULL_ATTEMPTS`` of
        polling with nothing draining, the broker is not accepting and saying
        so beats waiting.
        """
        from confluent_kafka.serialization import MessageField, SerializationContext

        payload = self._value_serializers[topic](
            value, SerializationContext(topic, MessageField.VALUE)
        )
        encoded_key = self._key_serializer(key)

        for _ in range(self._QUEUE_FULL_ATTEMPTS):
            try:
                self._producer.produce(
                    topic=topic,
                    key=encoded_key,
                    value=payload,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # Backpressure, not an error: the broker is simply slower than
                # this loop. Counted because "how often did we outrun the
                # broker" is the throughput number worth having afterwards.
                self.queue_full_waits += 1
                self._producer.poll(self._QUEUE_FULL_POLL_SECONDS)
        else:
            raise RuntimeError(
                f"local producer queue still full for topic {topic!r} after "
                f"{self._QUEUE_FULL_ATTEMPTS} polls of "
                f"{self._QUEUE_FULL_POLL_SECONDS}s "
                f"({self.queue_full_waits:,} waits so far, "
                f"{sum(self.counts.values()):,} records enqueued). "
                "Nothing is draining — the broker is unreachable, throttling, "
                "or rejecting writes for this credential."
            )

        self._producer.poll(0)
        self.counts[topic] = self.counts.get(topic, 0) + 1

    def flush(self, timeout: float = 60.0) -> None:  # pragma: no cover - needs a broker
        """Wait for delivery, but give up rather than block forever.

        Session 4: a bare ``flush()`` has no timeout, and librdkafka retries a
        transport failure indefinitely. Pointed at the wrong port, the first
        live produce sat in silence for 180 seconds and was killed — no error,
        no partial output, nothing naming the problem. The retry loop is correct
        behaviour for a transient blip and useless for a misconfiguration,
        because neither one ever raises.

        ``Producer.flush(timeout)`` returns the number of messages still
        undelivered, so a stall becomes a message that says how many.
        """
        remaining = self._producer.flush(timeout)
        if remaining:
            raise RuntimeError(
                f"{remaining} message(s) still undelivered after {timeout}s. "
                "Usually the broker is unreachable or the credential is wrong — "
                "check KAFKA_BOOTSTRAP_SERVERS uses the Kafka port (9092), "
                "not the REST endpoint (443)."
            )
        if self.error_count:
            raise RuntimeError(f"{self.error_count} delivery failures; first: {self._errors[0]}")

    def close(self) -> None:  # pragma: no cover - needs a broker
        self.flush()


def _kafka_config_from_env() -> dict[str, str]:
    # Deliberately does NOT call load_local_env(). A library function that reads
    # .env off disk makes its own behaviour depend on an untracked file: a test
    # that clears these variables would silently get them back, and the error
    # path below would stop being reachable. main() loads the file; this reads
    # only the environment it is handed.
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
    """Writes dumps to a local directory, mirroring the S3 key layout exactly.

    Reads and writes **bytes**, not text, and that is not a stylistic choice.
    ``Path.write_text`` opens in text mode, which on Windows rewrites every
    ``\\n`` to ``\\r\\n`` on the way out and every ``\\r\\n`` to ``\\n`` on the way
    back. ``S3BlobSink`` encodes and decodes UTF-8 directly and performs no such
    translation, so the two sinks stored *different bytes for the same input* —
    on Windows only. See the [2026-09-19] incident.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put_text(self, key: str, text: str) -> None:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(text.encode("utf-8"))

    def get_text(self, key: str) -> str:
        return (self.root / key).read_bytes().decode("utf-8")

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
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self._s3 = client if client is not None else _s3_client_from_env()

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


def _s3_client_from_env() -> Any:
    """Build an S3 client from explicit ``.env`` credentials, or refuse.

    ``boto3.client("s3")`` with no arguments is not neutral — it walks a
    credential chain that ends at ``~/.aws/credentials``, which on this machine
    is a never-expiring **admin** key. So a blank or unloaded ``.env`` does not
    fail; it silently runs the generator as an account administrator, which is
    exactly what Session 3 created ``ledgerline-dev`` to avoid.

    ``.env.example`` warned about this in prose. A warning that the code does
    not enforce is a comment, so this raises instead: no scoped credential, no
    client. Tests inject their own client and never reach here.
    """
    import boto3

    # Same reasoning as _kafka_config_from_env: no load_local_env() here.
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "S3BlobSink needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env "
            "(the scoped ledgerline-dev credential). Refusing to fall back to the "
            "ambient ~/.aws/credentials profile, which is an admin key."
        )
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "ap-south-1"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


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
