"""The Avro schemas against the records the generators actually emit.

Why this file exists
--------------------
CI already runs `--print-schema` for both generators, which proves the schemas
are *valid Avro*. It proves nothing about whether they describe the data. A
schema can be flawless and still reject every record the generator produces —
a field declared `long` that arrives as a `datetime`, a nullable column
declared non-null, a field added to the payload and never added to the schema.

That failure has a specific cost here. Session 3 registers these schemas with
Confluent Schema Registry and produces against a live cluster, on a trial
clock, with `KafkaAvroSink` — code that has still never executed. A schema
mismatch discovered *there* is debugged through a broker, a registry and a
serializer at once. Discovered here it is a one-line diff, and it costs
nothing because `fastavro` needs no account, no broker and no registry.

`fastavro` is an optional dependency, so every test is guarded at **function**
level — a module-level `importorskip` would skip the file silently.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from importlib.util import find_spec

import pytest

from generators import inventory_cdc, order_events
from generators._common import ENVELOPE_FIELDS, MemorySink

HAS_FASTAVRO = find_spec("fastavro") is not None
needs_fastavro = pytest.mark.skipif(not HAS_FASTAVRO, reason="fastavro not installed")

SCHEMAS = {
    "orders": order_events.AVRO_SCHEMA,
    "inventory.cdc": inventory_cdc.AVRO_SCHEMA,
}


def _emit_orders(frames) -> list[dict]:
    """Drive the real order generator and collect what it puts on the topic."""
    sink = MemorySink()
    events, _ = order_events.build_events(frames["orders"], frames["order_items"])
    order_events.emit(events, sink)
    return sink.for_topic(order_events.DEFAULT_TOPIC)


def _emit_cdc(frames) -> list[dict]:
    """Same for the inventory CDC generator."""
    sink = MemorySink()
    events, _ = inventory_cdc.build_cdc_events(frames["order_items"], frames["orders"])
    inventory_cdc.emit(events, sink)
    return sink.for_topic(inventory_cdc.DEFAULT_TOPIC)


EMITTERS = {"orders": _emit_orders, "inventory.cdc": _emit_cdc}


def _round_trip(schema: dict, records: list[dict]) -> list[dict]:
    """Serialize with the schema and read straight back, as Kafka would."""
    import fastavro

    parsed = fastavro.parse_schema(schema)
    buffer = io.BytesIO()
    fastavro.writer(buffer, parsed, records)
    buffer.seek(0)
    return list(fastavro.reader(buffer))


# --------------------------------------------------------------------------
# Shape
# --------------------------------------------------------------------------


@needs_fastavro
@pytest.mark.parametrize("topic", sorted(SCHEMAS))
def test_schema_parses_and_carries_the_shared_envelope(topic: str) -> None:
    import fastavro

    schema = SCHEMAS[topic]
    fastavro.parse_schema(schema)

    declared = {field["name"] for field in schema["fields"]}
    assert set(ENVELOPE_FIELDS) <= declared, (
        f"{topic} is missing envelope fields {set(ENVELOPE_FIELDS) - declared}; "
        "Bronze reads all three feeds through the same envelope"
    )


@needs_fastavro
@pytest.mark.parametrize("topic", sorted(SCHEMAS))
def test_schema_survives_a_json_string_round_trip(topic: str) -> None:
    """`KafkaAvroSink` receives `json.dumps(AVRO_SCHEMA)`, not the dict.

    `AvroSerializer` takes a schema *string*. Anything that survives as a
    Python dict but not through JSON — a tuple, a non-string key, a `datetime`
    default — fails at sink construction in Session 3, not here.
    """
    import fastavro

    as_string = json.dumps(SCHEMAS[topic])
    fastavro.parse_schema(json.loads(as_string))


# --------------------------------------------------------------------------
# The schemas against real generator output
# --------------------------------------------------------------------------


@needs_fastavro
def test_every_order_event_the_generator_emits_round_trips(olist_frames) -> None:
    """The assertion that matters: real payloads, not hand-written samples.

    A hand-written record proves the schema matches whatever the test author
    imagined. Driving the actual generator proves it matches what Session 3
    will put on the wire.
    """
    emitted = _emit_orders(olist_frames)

    assert emitted, "the generator produced nothing — the round trip would be vacuous"

    decoded = _round_trip(order_events.AVRO_SCHEMA, emitted)

    assert len(decoded) == len(emitted)
    assert [r["event_id"] for r in decoded] == [r["event_id"] for r in emitted]


@needs_fastavro
def test_every_cdc_event_the_generator_emits_round_trips(olist_frames) -> None:
    emitted = _emit_cdc(olist_frames)

    assert emitted, "the generator produced nothing — the round trip would be vacuous"

    decoded = _round_trip(inventory_cdc.AVRO_SCHEMA, emitted)

    assert len(decoded) == len(emitted)
    assert [r["seq"] for r in decoded] == [r["seq"] for r in emitted]


@needs_fastavro
@pytest.mark.parametrize("topic", sorted(SCHEMAS))
def test_payload_has_no_field_the_schema_does_not_declare(topic, olist_frames) -> None:
    """Avro drops undeclared fields **silently** — no error, no warning.

    This is the failure mode that would survive Session 3 unnoticed: add a
    column to the generator, forget the schema, and the field simply never
    reaches Bronze. Nothing errors; a column is just missing downstream.
    """
    emitted = EMITTERS[topic](olist_frames)
    declared = {field["name"] for field in SCHEMAS[topic]["fields"]}
    undeclared: set[str] = set()
    for record in emitted:
        undeclared |= set(record) - declared

    assert not undeclared, (
        f"{topic} emits {sorted(undeclared)}, which the schema does not declare — "
        "Avro would drop these silently and Bronze would never see them"
    )


@needs_fastavro
@pytest.mark.parametrize("topic", sorted(SCHEMAS))
def test_every_declared_field_without_a_default_is_actually_emitted(topic, olist_frames) -> None:
    """The mirror image: a required field the generator never sets.

    `fastavro` raises on a missing required field, so the round-trip tests
    above would already catch it — but only with a stack trace pointing at the
    serializer. This names the field.
    """
    emitted = EMITTERS[topic](olist_frames)
    required = {f["name"] for f in SCHEMAS[topic]["fields"] if "default" not in f}
    for record in emitted:
        missing = required - set(record)
        assert not missing, f"{topic} record {record.get('event_id')} omits {sorted(missing)}"


# --------------------------------------------------------------------------
# The cases most likely to be wrong
# --------------------------------------------------------------------------


@needs_fastavro
def test_timestamps_are_integer_micros_not_datetimes(olist_frames) -> None:
    """`timestamp-micros` accepts a `datetime` *and* an `int`, and they differ.

    `fastavro` will happily encode a `datetime` for a `timestamp-micros` field,
    converting it — so a generator emitting datetimes would round-trip fine
    here and still be wrong, because `to_micros`/`from_micros` and every
    downstream event-time comparison assume integer micros. Pinning the Python
    type is the only way this stays honest.
    """
    for record in _emit_orders(olist_frames):
        assert isinstance(record["event_ts"], int), type(record["event_ts"])
        assert isinstance(record["produced_at"], int), type(record["produced_at"])
        assert not isinstance(record["event_ts"], bool)


@needs_fastavro
def test_nullable_fields_round_trip_as_null(olist_frames) -> None:
    """The fixture's null-bearing rows, through the union types.

    `ORD_PARTIAL` has no `order_approved_at` and `ORD_CANCEL` no delivery, so
    non-`created` events carry `items = None`; the CDC seed insert carries
    `prev_stock_qty = None`. A union declared as bare `"int"` instead of
    `["null", "int"]` fails exactly on these rows and on no others.
    """
    orders = _emit_orders(olist_frames)
    cdc = _emit_cdc(olist_frames)

    assert any(r.get("items") is None for r in orders), "no null `items` — union untested"
    assert any(r.get("prev_stock_qty") is None for r in cdc), "no seed insert — union untested"

    for schema, records in ((order_events.AVRO_SCHEMA, orders), (inventory_cdc.AVRO_SCHEMA, cdc)):
        decoded = _round_trip(schema, records)
        for before, after in zip(records, decoded, strict=True):
            for field in ("items", "prev_stock_qty", "estimated_delivery_date"):
                if field in before and before[field] is None:
                    assert after[field] is None, f"{field} did not survive as null"


@needs_fastavro
def test_seq_needs_the_full_64_bit_range(olist_frames) -> None:
    """`seq` is declared `long`, and it has to be.

    `seq = event_ts_micros * 1000 + index`, which for 2017 timestamps is
    ~1.5e18 — comfortably past `int`'s 2^31 ceiling. Declaring it `int` would
    not be a rounding error; every value would overflow.
    """
    values = [r["seq"] for r in _emit_cdc(olist_frames)]

    assert values
    assert max(values) > 2**31, "fixture seq values no longer exercise the long range"
    assert max(values) < 2**63 - 1


INT32_MAX = 2**31 - 1

# Avro types that hold at most a signed 32-bit integer. `long` and the
# `timestamp-*` logical types (which are longs) are deliberately absent.
_NARROW_INT_TYPES = {"int"}


def _declared_type(field: dict) -> object:
    """The field's type with any `["null", T]` union unwrapped to T."""
    declared = field["type"]
    if isinstance(declared, list):
        non_null = [t for t in declared if t != "null"]
        return non_null[0] if len(non_null) == 1 else declared
    return declared


@needs_fastavro
@pytest.mark.parametrize("topic", sorted(SCHEMAS))
def test_no_integer_field_is_declared_narrower_than_its_values(topic, olist_frames) -> None:
    """A declared `int` that receives a 64-bit value — caught here or nowhere.

    Found by deliberately mutating `seq` from `long` to `int`: **all sixteen
    other tests in this file still passed.** Avro encodes `int` and `long`
    with the identical zig-zag varint, and `fastavro` does not range-check
    against the declared type on write, so the value round-trips intact and
    every round-trip assertion stays green.

    The declared type is not decorative, though. Schema Registry enforces it
    for compatibility, and Spark's `from_avro` maps Avro `int` to
    `IntegerType` — so the truncation that fastavro declined to perform
    happens in Bronze instead, on a cluster, in Session 3.

    Checking the *values* is therefore not enough; this checks the *schema*.
    """
    emitted = EMITTERS[topic](olist_frames)
    assert emitted, "nothing emitted — the check would be vacuous"

    for field in SCHEMAS[topic]["fields"]:
        # A logical type is a dict and a multi-branch union is a list; neither
        # is hashable, so the membership test has to be type-guarded first.
        declared = _declared_type(field)
        if not isinstance(declared, str) or declared not in _NARROW_INT_TYPES:
            continue
        name = field["name"]
        seen = [r[name] for r in emitted if isinstance(r.get(name), int)]
        if not seen:
            continue
        assert max(map(abs, seen)) <= INT32_MAX, (
            f"{topic}.{name} is declared Avro `int` (max {INT32_MAX:,}) but the "
            f"generator emits {max(seen):,}. fastavro will not complain; Spark's "
            "from_avro maps this to IntegerType and truncates it in Bronze."
        )


# --------------------------------------------------------------------------
# Config, without a cluster
# --------------------------------------------------------------------------


def test_kafka_config_names_every_missing_variable(monkeypatch) -> None:
    """The first thing that will happen in Session 3 is this error.

    It must name *all* the missing variables at once — reporting them one per
    run turns setup into four round trips through a failing command.
    """
    from generators._common import _kafka_config_from_env

    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_API_KEY",
        "KAFKA_API_SECRET",
        "SCHEMA_REGISTRY_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        _kafka_config_from_env()

    message = str(excinfo.value)
    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_API_KEY",
        "KAFKA_API_SECRET",
        "SCHEMA_REGISTRY_URL",
    ):
        assert name in message, f"{name} missing from the error; setup becomes trial and error"


def test_kafka_config_builds_registry_auth_from_its_own_credentials(monkeypatch) -> None:
    """The Schema Registry key is a *separate* credential from the cluster key.

    Using the cluster key for the registry is the documented Confluent
    footgun — it fails with an opaque 401. `.env.example` warns about it; this
    pins that the code reads the registry pair and not the cluster pair.
    """
    from generators._common import _kafka_config_from_env

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "pkc-test.example:9092")
    monkeypatch.setenv("KAFKA_API_KEY", "CLUSTER_KEY")
    monkeypatch.setenv("KAFKA_API_SECRET", "CLUSTER_SECRET")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "https://psrc-test.example")
    monkeypatch.setenv("SCHEMA_REGISTRY_API_KEY", "SR_KEY")
    monkeypatch.setenv("SCHEMA_REGISTRY_API_SECRET", "SR_SECRET")

    config = _kafka_config_from_env()

    assert config["schema.registry.basic.auth.user.info"] == "SR_KEY:SR_SECRET"
    assert config["sasl.username"] == "CLUSTER_KEY"
    assert "CLUSTER_KEY" not in config["schema.registry.basic.auth.user.info"]


def test_produced_at_is_wall_clock_and_event_ts_is_business_time(olist_frames) -> None:
    """The distinction the whole project rests on, asserted once.

    Olist's business time is 2016-2018; `produced_at` is now. If these ever
    collapse into one value, every watermark and event-time-vs-processing-time
    demonstration downstream becomes untestable.
    """
    records = _emit_orders(olist_frames)

    now_micros = int(datetime.now(tz=UTC).timestamp() * 1_000_000)
    for record in records:
        assert record["event_ts"] < record["produced_at"], "business time is not in the past"
        assert abs(record["produced_at"] - now_micros) < 60 * 60 * 1_000_000
