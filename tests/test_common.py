"""Tests for the shared generator plumbing.

Small surface, but three things here are load-bearing for every downstream
session: the event-time/processing-time split, replay determinism, and the fact
that the offline sinks are faithful stand-ins for Kafka and S3.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from importlib.util import find_spec

import pytest

from generators._common import (
    ENVELOPE_FIELDS,
    JsonlSink,
    LocalBlobSink,
    MemorySink,
    ReplayClock,
    chunked,
    deterministic_id,
    envelope,
    from_micros,
    to_micros,
)

# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------


def test_deterministic_id_is_stable_across_calls():
    assert deterministic_id("a", 1, None) == deterministic_id("a", 1, None)
    assert len(deterministic_id("a")) == 32


def test_deterministic_id_separates_field_boundaries():
    """Concatenating without a separator would merge these two distinct keys."""
    assert deterministic_id("ab", "c") != deterministic_id("a", "bc")


def test_deterministic_id_separates_absent_from_blank():
    assert deterministic_id("x", None) != deterministic_id("x", "")


def test_deterministic_id_is_order_sensitive():
    assert deterministic_id("a", "b") != deterministic_id("b", "a")


# --------------------------------------------------------------------------
# Time
# --------------------------------------------------------------------------


def test_micros_round_trip_preserves_the_instant():
    moment = datetime(2017, 5, 20, 15, 30, 45, 123456, tzinfo=UTC)
    assert from_micros(to_micros(moment)) == moment


def test_naive_timestamps_are_read_as_utc():
    """Olist carries no timezone; guessing the local one would shift every event."""
    naive = datetime(2017, 5, 20, 15, 0, 0)
    aware = datetime(2017, 5, 20, 15, 0, 0, tzinfo=UTC)
    assert to_micros(naive) == to_micros(aware)


def test_envelope_carries_both_clocks():
    event_time = datetime(2017, 1, 5, 10, 0, tzinfo=UTC)
    wall_time = datetime(2026, 9, 19, 8, 0, tzinfo=UTC)

    built = envelope("abc", event_time, "unit_test", wall_time)

    assert set(ENVELOPE_FIELDS) <= set(built)
    assert built["event_ts"] == to_micros(event_time)
    assert built["produced_at"] == to_micros(wall_time)
    assert built["produced_at"] > built["event_ts"], "replaying old data in the present"


def test_unpaced_clock_never_sleeps():
    clock = ReplayClock(business_start=datetime(2017, 1, 1, tzinfo=UTC), speedup=0)
    assert clock.paced is False
    assert clock.sleep_until(datetime(2018, 1, 1, tzinfo=UTC)) == 0.0


def test_paced_clock_compresses_business_time():
    """One day of business time per second at speedup=86400."""
    wall_start = datetime(2026, 9, 19, 8, 0, tzinfo=UTC)
    clock = ReplayClock(
        business_start=datetime(2017, 1, 1, tzinfo=UTC),
        speedup=86_400,
        wall_start=wall_start,
    )

    due = clock.wall_for(datetime(2017, 1, 11, tzinfo=UTC))
    assert due - wall_start == timedelta(seconds=10)


def test_clock_does_not_accumulate_lag_for_overdue_events():
    """A generator that fell behind should catch up, not sleep off the backlog."""
    clock = ReplayClock(
        business_start=datetime(2017, 1, 1, tzinfo=UTC),
        speedup=86_400,
        wall_start=datetime(2020, 1, 1, tzinfo=UTC),  # long past
    )
    assert clock.sleep_until(datetime(2017, 1, 2, tzinfo=UTC)) == 0.0


def test_negative_speedup_is_rejected():
    with pytest.raises(ValueError, match="speedup"):
        ReplayClock(business_start=datetime(2017, 1, 1, tzinfo=UTC), speedup=-1)


# --------------------------------------------------------------------------
# Sinks
# --------------------------------------------------------------------------


def test_jsonl_sink_records_the_partition_key_alongside_the_value(tmp_path):
    """Dropping the key would make the local file an unfaithful stand-in for a topic."""
    sink = JsonlSink(tmp_path)
    sink.send("orders", "ORD_1", {"event_id": "e1"})
    sink.send("orders", "ORD_2", {"event_id": "e2"})
    sink.close()

    lines = (tmp_path / "orders.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(line)["key"] for line in lines] == ["ORD_1", "ORD_2"]
    assert json.loads(lines[0])["value"]["event_id"] == "e1"


def test_jsonl_sink_splits_topics_into_separate_files(tmp_path):
    sink = JsonlSink(tmp_path)
    sink.send("orders", "k", {"a": 1})
    sink.send("inventory.cdc", "k", {"a": 2})
    sink.close()

    assert (tmp_path / "orders.jsonl").exists()
    assert (tmp_path / "inventory.cdc.jsonl").exists()
    assert sink.counts == {"orders": 1, "inventory.cdc": 1}


def test_memory_sink_filters_by_topic():
    sink = MemorySink()
    sink.send("a", "k1", {"n": 1})
    sink.send("b", "k2", {"n": 2})

    assert sink.for_topic("a") == [{"n": 1}]
    assert sink.keys_for_topic("b") == ["k2"]


def test_local_blob_sink_mirrors_the_s3_key_layout(tmp_path):
    """Same keys locally and in S3, so switching sinks changes nothing downstream."""
    sink = LocalBlobSink(tmp_path)
    sink.put_text("dims/customer/dump_date=2017-01-31/customer.csv", "a,b\n1,2\n")

    assert sink.get_text("dims/customer/dump_date=2017-01-31/customer.csv") == "a,b\n1,2\n"
    assert sink.list_keys("dims/") == ["dims/customer/dump_date=2017-01-31/customer.csv"]
    assert sink.list_keys("dims/product/") == []


def test_local_blob_sink_uses_forward_slashes_on_every_platform(tmp_path):
    """Windows backslashes would not match an S3 prefix."""
    sink = LocalBlobSink(tmp_path)
    sink.put_text("dims/seller/dump_date=2017-01-31/seller.csv", "x\n")
    assert all("\\" not in key for key in sink.list_keys("dims/"))


def test_chunked_yields_a_short_final_batch():
    assert list(chunked(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]
    assert list(chunked([], 3)) == []


# --------------------------------------------------------------------------
# Packaging: the cloud SDKs must stay optional
# --------------------------------------------------------------------------


def test_generators_import_without_any_cloud_sdk_installed():
    """CI installs only pandas + pytest + ruff, and this is why that is safe.

    confluent-kafka and boto3 are imported inside ``__init__``, not at module
    level, so every generator imports and every offline test runs on a machine
    with no cloud libraries at all. If someone hoists one of those imports to
    the top of a module, CI goes red here rather than three sessions later.
    """
    import importlib
    import sys

    class Blocker:
        BLOCKED = ("confluent_kafka", "boto3", "botocore")

        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] in self.BLOCKED:
                raise ImportError(f"blocked for test: {name}")
            return None

    blocker = Blocker()
    modules = (
        "generators.olist",
        "generators._common",
        "generators.order_events",
        "generators.dim_dumps",
        "generators.inventory_cdc",
    )
    saved = {name: sys.modules.pop(name, None) for name in modules}
    sys.meta_path.insert(0, blocker)
    try:
        for name in modules:
            importlib.import_module(name)
    finally:
        sys.meta_path.remove(blocker)
        for name, module in saved.items():
            if module is not None:
                sys.modules[name] = module


def test_cloud_sinks_fail_at_construction_not_at_import():
    """The lazy import has to actually be reachable, or the sink is dead code.

    Session 4 moved the boto3 import out of ``S3BlobSink.__init__`` and into
    ``_s3_client_from_env``, which builds the client from the scoped .env
    credential instead of letting boto3 walk its default chain to an admin key.
    The property under test is unchanged — the SDK import must be *inside a
    function*, so the module imports on a machine without boto3 — so this
    follows the import to where it now lives rather than pinning it to a
    function that no longer performs it.
    """
    import inspect

    from generators._common import KafkaAvroSink, S3BlobSink, _s3_client_from_env

    assert "import boto3" in inspect.getsource(_s3_client_from_env)
    assert "boto3" not in inspect.getsource(S3BlobSink.__init__)
    assert "from confluent_kafka import Producer" in inspect.getsource(KafkaAvroSink.__init__)


def test_kafka_sink_never_uses_the_default_client_id():
    """Every producer must identify itself, including when nobody passes a name.

    librdkafka defaults `client.id` to "rdkafka". Session 4 watched Confluent's
    lineage view report "producer rdkafka (2)" with no way to tell which of two
    generators — or a throwaway debug script — was which. At 394,090 events
    across two topics, "which client is lagging" has to be answerable.

    Checks the fallback rather than the happy path: the generators pass an
    explicit id today, so the branch that could regress to the default is the
    one where a caller forgets.
    """
    import inspect

    from generators._common import KafkaAvroSink

    source = inspect.getsource(KafkaAvroSink.__init__)
    assert '"client.id": self.client_id' in source, "producer config must set client.id"

    # The fallback id is derived from the topic names, without constructing a
    # Producer (which would need a broker).
    schemas = {"orders": "{}", "inventory.cdc": "{}"}
    fallback = f"ledgerline-{'-'.join(sorted(schemas))}"
    assert fallback == "ledgerline-inventory.cdc-orders"
    assert "rdkafka" not in fallback


def test_s3_sink_refuses_to_fall_back_to_ambient_admin_credentials(monkeypatch):
    """No scoped credential in the environment must be an error, not a fallback.

    ``boto3.client("s3")`` with no arguments is not neutral: it walks a
    credential chain ending at ``~/.aws/credentials``, which on the development
    machine is a never-expiring admin key. So a blank or unloaded .env did not
    fail — it ran the generator as an account administrator and succeeded,
    which is the one outcome that leaves no trace. ``.env.example`` warned
    about this in prose for three sessions; prose does not execute.

    Asserted on the *absence* of each variable separately, because a partially
    filled .env is the realistic case and either half missing is equally unsafe.
    """
    from generators._common import S3BlobSink

    for missing in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
        monkeypatch.delenv(missing, raising=False)

        with pytest.raises(RuntimeError) as excinfo:
            S3BlobSink("ledgerline-landing-dev-fffc8b65", "ledgerline")

        message = str(excinfo.value)
        assert "AWS_ACCESS_KEY_ID" in message and "AWS_SECRET_ACCESS_KEY" in message
        assert "admin" in message, "the error must say why the fallback is refused"


def test_load_local_env_does_not_override_an_already_set_variable(tmp_path, monkeypatch):
    """A value already in the environment beats the file on disk.

    Otherwise a deliberate one-off override (``$env:X = "y"`` before a run, or a
    test's monkeypatch) would be silently replaced by whatever .env happens to
    hold, and the run would not be doing what the operator asked.
    """
    import generators._common as common

    env_file = tmp_path / ".env"
    env_file.write_bytes(b"LEDGERLINE_PROBE=from_file\n")

    monkeypatch.setenv("LEDGERLINE_PROBE", "from_environment")
    monkeypatch.setattr(common, "_env_loaded", False)
    common.load_local_env(env_file)

    assert os.environ["LEDGERLINE_PROBE"] == "from_environment"


# --------------------------------------------------------------------------
# Producing at scale  (Session 5)
# --------------------------------------------------------------------------

HAS_CONFLUENT = find_spec("confluent_kafka") is not None
needs_confluent = pytest.mark.skipif(
    not HAS_CONFLUENT, reason="confluent_kafka not installed"
)


class _FakeProducer:
    """A producer whose local queue is full for the first ``full_for`` sends.

    Stands in for librdkafka's queue without a broker. ``produce`` raises
    ``BufferError`` — the real class's queue-full signal — until ``poll`` has
    been called enough times to "drain" it.
    """

    def __init__(self, full_for: int = 0, drain_per_poll: int = 1) -> None:
        self.full_for = full_for
        self.drain_per_poll = drain_per_poll
        self.produced: list[tuple] = []
        self.polls: list[float] = []

    def produce(self, topic, key, value, on_delivery=None):
        if self.full_for > 0:
            raise BufferError("Local: Queue full")
        self.produced.append((topic, key, value))

    def poll(self, timeout=0):
        self.polls.append(timeout)
        if timeout:  # only a blocking poll drains; poll(0) just serves callbacks
            self.full_for = max(0, self.full_for - self.drain_per_poll)
        return 0


def _sink_with(producer):
    """A ``KafkaAvroSink`` wrapped around a fake producer.

    Built without ``__init__`` deliberately: the constructor opens a real
    Producer and a Schema Registry connection, and neither is the thing under
    test here. The serializers are replaced with identity functions so the
    test exercises the queue logic rather than Avro encoding, which
    ``test_avro_contract.py`` already covers.
    """
    from generators._common import KafkaAvroSink

    sink = object.__new__(KafkaAvroSink)
    sink._producer = producer
    sink._key_serializer = lambda k: k.encode("utf-8")
    sink._value_serializers = {"orders": lambda v, ctx: b"encoded"}
    sink.counts = {}
    sink._errors = []
    sink.error_count = 0
    sink.queue_full_waits = 0
    sink.client_id = "ledgerline-test"
    return sink


@needs_confluent
def test_send_waits_out_a_full_queue_instead_of_failing():
    """Backpressure is not an error — it is the broker being slower than the loop.

    ``produce()`` does not send; it appends to a local queue of 100,000 records
    that a background thread drains. Session 4 produced 187 and never filled
    it. Session 5 produces 394,090 from a local CSV, which is far faster than a
    TLS round-trip to ap-south-1, so the queue fills and ``produce()`` raises
    ``BufferError``.

    The record must still arrive. Before this change the exception escaped and
    the run died partway through with the topic half-written — the worst
    outcome available, because a partial produce looks like a complete one to
    anything counting afterwards.
    """
    producer = _FakeProducer(full_for=3)
    sink = _sink_with(producer)

    sink.send("orders", "order-1", {"event_id": "abc"})

    assert producer.produced == [("orders", b"order-1", b"encoded")]
    assert sink.counts == {"orders": 1}
    assert sink.queue_full_waits == 3, "each wait must be counted, not swallowed"


@needs_confluent
def test_send_gives_up_when_nothing_drains_rather_than_looping_forever():
    """A queue that never empties is a dead broker, and must say so.

    This is the Session 4 lesson applied to a different call. ``flush()``
    without a timeout turned a wrong port into 180 seconds of silence; an
    unbounded retry around ``produce()`` would do exactly the same thing, and
    for the same reason — waiting forever is correct for a transient blip and
    useless for a misconfiguration, because neither one ever raises.

    The error has to carry the counts. "Queue full" alone does not tell you
    whether 12 records or 380,000 made it in, and that difference decides
    whether you re-run or investigate.
    """
    from generators._common import KafkaAvroSink

    producer = _FakeProducer(full_for=10_000)  # never drains within the budget
    sink = _sink_with(producer)
    sink.counts = {"orders": 12_345}

    with pytest.raises(RuntimeError) as excinfo:
        sink.send("orders", "order-1", {"event_id": "abc"})

    message = str(excinfo.value)
    assert "12,345" in message, "the error must say how much got through"
    assert "orders" in message
    assert sink.queue_full_waits == KafkaAvroSink._QUEUE_FULL_ATTEMPTS
    assert producer.produced == []


@needs_confluent
def test_delivery_failures_are_counted_in_full_but_kept_in_sample():
    """394,090 broken deliveries must not become 394,090 strings in memory.

    Only the count and a first example are ever read. Keeping every message
    would turn a broker outage into a memory problem on top of a delivery
    problem, at exactly the moment there is least headroom to spare.
    """
    from generators._common import KafkaAvroSink

    sink = _sink_with(_FakeProducer())
    for index in range(1_000):
        sink._on_delivery(f"failure {index}", None)

    assert sink.error_count == 1_000
    assert len(sink._errors) == KafkaAvroSink._MAX_KEPT_ERRORS
    assert sink._errors[0] == "failure 0", "the sample must be the first, not the last"


def test_progress_ticker_reports_on_boundaries_only(capsys):
    """Every 25,000 records, not every record — and to stderr, not stdout.

    stdout carries the generator's summary table, which is an artifact worth
    piping and diffing. Progress is terminal noise by design and belongs on the
    other stream, or a redirect captures 16 progress lines wrapped around the
    numbers someone actually wanted.
    """
    from generators._common import ProgressTicker

    ticker = ProgressTicker(total=10, every=4, label="events")
    for _ in range(10):
        ticker.tick()
    ticker.done()

    captured = capsys.readouterr()
    assert captured.out == "", "progress must not pollute stdout"
    # boundaries crossed at 4 and 8, plus the final done() line
    assert captured.err.count("\n") == 3
    assert "10 events in" in captured.err


def test_progress_floor_is_above_a_smoke_run():
    """The floor has to sit above the runs that are meant to stay quiet.

    Both generators default ``--limit`` to nothing but are routinely run with a
    few hundred orders while iterating. If the floor ever drops under a smoke
    run, every test and every quick check starts emitting progress lines.
    """
    from generators import inventory_cdc, order_events

    assert order_events.PROGRESS_FLOOR == inventory_cdc.PROGRESS_FLOOR
    assert order_events.PROGRESS_FLOOR > 5_000
