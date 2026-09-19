"""Tests for the shared generator plumbing.

Small surface, but three things here are load-bearing for every downstream
session: the event-time/processing-time split, replay determinism, and the fact
that the offline sinks are faithful stand-ins for Kafka and S3.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

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
    """The lazy import has to actually be reachable, or the sink is dead code."""
    import inspect

    from generators._common import KafkaAvroSink, S3BlobSink

    assert "import boto3" in inspect.getsource(S3BlobSink.__init__)
    assert "from confluent_kafka import Producer" in inspect.getsource(KafkaAvroSink.__init__)
