"""`S3BlobSink` against a mock S3, before any AWS account exists.

Why this file exists
--------------------
`S3BlobSink` was written in Session 1 and had never executed a single line —
no bucket, no `moto`, nothing. It is not decoration: it is the transport for
the nightly dimension dumps, and Databricks Auto Loader reads exactly the key
layout it produces (Session 6). Databricks runs in AWS and cannot see a
laptop's filesystem, so `LocalBlobSink` is the *twin*, not the deployment.

The load-bearing contract is narrower than "it can write a file", and it is
what these tests pin:

    read_previous() calls sink.list_keys(prefix), then feeds a key straight
    back into sink.get_text(key).

`S3BlobSink.list_keys` strips `len(self.prefix) + 1` characters off every key
it returns, and `get_text` adds the prefix back on. If those two disagree by a
single character, `read_previous` still *finds* last night's dump and then
fails — or worse, silently finds nothing, reports every row as new, and
destroys the `dim_updated_at` carry-forward that the whole Gold SCD2 layer
rests on. That failure mode is invisible against `LocalBlobSink`, which has no
prefix concept at all.

`moto` is an optional dependency, so every test here is guarded at **function**
level. A module-level `importorskip` would skip the entire file silently —
the jobpulse prevention rule.
"""

from __future__ import annotations

import io
from datetime import date
from importlib.util import find_spec

import pandas as pd
import pytest

from generators._common import LocalBlobSink, S3BlobSink
from generators.dim_dumps import ROOT_PREFIX, UPDATED_AT, dump_key, read_previous, run

HAS_MOTO = find_spec("moto") is not None
needs_moto = pytest.mark.skipif(not HAS_MOTO, reason="moto not installed")

BUCKET = "ledgerline-test"
PREFIX = "ledgerline"

START = date(2017, 1, 31)
STRIDE = 28


@pytest.fixture
def s3_server():
    """A mocked S3 with the test bucket already created.

    Credentials are deliberately fake and set on the environment: `moto`
    intercepts before anything leaves the process, but botocore still refuses
    to build a request without *some* credential, and an unset region picks up
    whatever the developer's real `~/.aws/config` says.
    """
    import boto3
    from moto import mock_aws

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


# --------------------------------------------------------------------------
# The key-layout contract
# --------------------------------------------------------------------------


@needs_moto
def test_put_then_get_round_trips_text(s3_server) -> None:
    sink = S3BlobSink(BUCKET, PREFIX, client=s3_server)
    sink.put_text("dims/customer/dump_date=2017-01-31/customer.csv", "a,b\n1,2\n")

    assert sink.get_text("dims/customer/dump_date=2017-01-31/customer.csv") == "a,b\n1,2\n"


@needs_moto
def test_object_lands_under_the_prefix_in_the_real_bucket(s3_server) -> None:
    """The stored key is prefixed; the key the caller uses is not.

    Asserted against the raw boto3 client rather than through the sink, so this
    checks what is actually in the bucket instead of re-checking the sink's own
    arithmetic against itself.
    """
    sink = S3BlobSink(BUCKET, PREFIX, client=s3_server)
    sink.put_text("dims/seller/dump_date=2017-01-31/seller.csv", "x\n")

    stored = [o["Key"] for o in s3_server.list_objects_v2(Bucket=BUCKET)["Contents"]]
    assert stored == ["ledgerline/dims/seller/dump_date=2017-01-31/seller.csv"]


@needs_moto
def test_list_keys_returns_keys_get_text_can_consume_directly(s3_server) -> None:
    """The exact contract `read_previous` depends on, asserted in isolation.

    A prefix-strip off by one character passes every "can it write a file"
    test and fails only here.
    """
    sink = S3BlobSink(BUCKET, PREFIX, client=s3_server)
    written = {
        "dims/customer/dump_date=2017-01-31/customer.csv": "night one",
        "dims/customer/dump_date=2017-02-28/customer.csv": "night two",
    }
    for key, body in written.items():
        sink.put_text(key, body)

    listed = sink.list_keys(f"{ROOT_PREFIX}/customer/")

    assert listed == sorted(written)
    # The round trip, not just the shape: every listed key is usable as-is.
    assert {key: sink.get_text(key) for key in listed} == written


@needs_moto
def test_list_keys_with_no_prefix_configured(s3_server) -> None:
    """`prefix=""` takes the other branch of `_full` and of the strip."""
    sink = S3BlobSink(BUCKET, "", client=s3_server)
    sink.put_text("dims/product/dump_date=2017-01-31/product.csv", "p\n")

    assert sink.list_keys("dims/") == ["dims/product/dump_date=2017-01-31/product.csv"]
    assert sink.get_text("dims/product/dump_date=2017-01-31/product.csv") == "p\n"


@needs_moto
def test_a_neighbouring_prefix_does_not_leak_into_the_listing(s3_server) -> None:
    """S3 prefix matching is raw string matching, not path matching.

    `ledgerline` is a string prefix of `ledgerline-backup`, so a sink that
    built its search prefix by concatenation without the separator would return
    another environment's dumps — and `get_text` would then strip the wrong
    number of characters off them.
    """
    ours = S3BlobSink(BUCKET, PREFIX, client=s3_server)
    theirs = S3BlobSink(BUCKET, "ledgerline-backup", client=s3_server)

    ours.put_text("dims/customer/dump_date=2017-01-31/customer.csv", "ours")
    theirs.put_text("dims/customer/dump_date=2017-01-31/customer.csv", "theirs")

    assert ours.list_keys("dims/") == ["dims/customer/dump_date=2017-01-31/customer.csv"]
    assert ours.get_text("dims/customer/dump_date=2017-01-31/customer.csv") == "ours"


@needs_moto
def test_list_keys_paginates_past_the_1000_object_cap(s3_server) -> None:
    """`ListObjectsV2` returns at most 1000 keys per call.

    The sink uses a paginator, so this passes today. It is pinned because the
    obvious "simplification" to a single `list_objects_v2` call would truncate
    silently at exactly 1000 — `read_previous` would take `max()` over a
    truncated candidate list and carry forward from the wrong night. Six
    nights of Olist will never reach this, which is precisely why nothing else
    in the suite would ever catch it.
    """
    sink = S3BlobSink(BUCKET, PREFIX, client=s3_server)
    for n in range(1005):
        s3_server.put_object(
            Bucket=BUCKET, Key=f"{PREFIX}/bulk/obj_{n:05d}.txt", Body=b"x"
        )

    listed = sink.list_keys("bulk/")

    assert len(listed) == 1005
    assert listed[0] == "bulk/obj_00000.txt"
    assert listed[-1] == "bulk/obj_01004.txt"


# --------------------------------------------------------------------------
# Substitutability: the local twin must not be a different thing
# --------------------------------------------------------------------------


@needs_moto
def test_local_and_s3_sinks_produce_byte_identical_dumps(olist_frames, tmp_path, s3_server) -> None:
    """The same generator run against both sinks, compared key for key.

    `decisions.md` claims `LocalBlobSink` "mirrors the S3 key layout exactly,
    so switching sinks changes nothing downstream". Until now that was an
    assertion in prose. If it is false, every test in the suite has been
    validating a layout that Auto Loader will never see.
    """
    local = LocalBlobSink(tmp_path / "dumps")
    remote = S3BlobSink(BUCKET, PREFIX, client=s3_server)

    for sink in (local, remote):
        run(olist_frames, sink, start=START, nights=3, stride_days=STRIDE, change_rate=1)

    local_keys = local.list_keys(f"{ROOT_PREFIX}/")
    remote_keys = remote.list_keys(f"{ROOT_PREFIX}/")

    assert local_keys == remote_keys
    assert local_keys, "the run produced no dumps at all — the comparison would be vacuous"
    for key in local_keys:
        assert local.get_text(key) == remote.get_text(key), f"content differs at {key}"


# --------------------------------------------------------------------------
# The reason the layout matters: carry-forward across an S3 round trip
# --------------------------------------------------------------------------


@needs_moto
def test_read_previous_finds_last_nights_dump_through_s3(olist_frames, s3_server) -> None:
    remote = S3BlobSink(BUCKET, PREFIX, client=s3_server)
    run(olist_frames, remote, start=START, nights=3, stride_days=STRIDE, change_rate=0)

    frame, found = read_previous(remote, "customer", date(2017, 3, 28))

    assert found == date(2017, 2, 28), "picked the wrong night, or found none at all"
    assert frame is not None and not frame.empty


@needs_moto
def test_dim_updated_at_carries_forward_through_the_s3_round_trip(
    olist_frames, tmp_path, s3_server
) -> None:
    """The assertion the whole file is for.

    `dim_updated_at` must be *business* time, not run time. The carry-forward
    that keeps it so depends on reading last night's CSV back — and on S3 that
    read now crosses a `list_keys` strip, a `get_text` re-prefix, and a
    UTF-8 encode/decode that `LocalBlobSink` never performs.

    The Session 1 incident (`str(225)` vs `str(225.0)`) lived in exactly this
    CSV round trip. Proving the local path survives it proves nothing about
    the path that will actually run in Session 6.
    """
    local = LocalBlobSink(tmp_path / "dumps")
    remote = S3BlobSink(BUCKET, PREFIX, client=s3_server)

    for sink in (local, remote):
        run(olist_frames, sink, start=START, nights=4, stride_days=STRIDE, change_rate=0)

    def rows(sink, dump_date: date) -> dict[str, tuple[str, str]]:
        """{key: (city, dim_updated_at)} for one night's customer dump."""
        frame = pd.read_csv(io.StringIO(sink.get_text(dump_key("customer", dump_date))))
        return {
            row["customer_unique_id"]: (row["customer_city"], str(row[UPDATED_AT]))
            for _, row in frame.iterrows()
        }

    first = rows(remote, START)
    last = rows(remote, date(2017, 4, 25))

    # Only rows whose business attributes are identical across the two nights.
    # `cu_mover` deliberately relocates (sao paulo -> rio de janeiro), so its
    # timestamp *must* move; including it would assert the opposite of what
    # the fixture was built to demonstrate.
    unchanged = [k for k, (city, _) in last.items() if k in first and first[k][0] == city]
    assert unchanged, "no customer was unchanged across all four nights — nothing is proven"

    for key in unchanged:
        assert last[key][1] == first[key][1], (
            f"dim_updated_at moved on {key}, whose city never changed, after an "
            "S3 round trip — a dbt snapshot would open a new SCD2 version for it"
        )

    # The row that genuinely changed must still move, or the test above would
    # pass just as well against a generator that froze every timestamp.
    movers = [k for k, (city, _) in last.items() if k in first and first[k][0] != city]
    assert movers, "the fixture's relocating customer vanished — see tests/conftest.py"
    for key in movers:
        assert last[key][1] != first[key][1]

    # And the S3 path agrees with the local path it is supposed to mirror.
    assert last == rows(local, date(2017, 4, 25))
