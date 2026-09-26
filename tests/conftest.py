"""Shared fixtures — a miniature Olist built to contain the project's gotchas.

The fixture is deliberately tiny (7 orders, 4 people, 4 products, 2 sellers) but
every row
is there for a reason. If you change it, keep the properties below true or
several tests stop testing what they claim to:

* ``CU_MOVER`` has **two** ``customer_id`` values with **different cities**.
  That is the real SCD2 material and the ``customer_id`` vs
  ``customer_unique_id`` trap in one row pair.
* ``ORD_OVERLAP`` is placed while ``ORD_MULTI`` is still in flight, so the
  two lifecycles interleave — without it the stream is a sorted concatenation
  of per-order runs and never proves global event-time ordering.
* ``ORD_MULTI`` has three units of the same product from one seller, so
  "units sold" and "distinct products" differ and a grain slip is visible.
* ``ORD_CANCEL`` is ``canceled`` with no delivery timestamps, exercising the
  synthetic terminal event.
* ``ORD_PARTIAL`` has a null ``order_approved_at`` in the middle of its
  lifecycle, so gaps are not assumed contiguous.
* ``PROD_NOCAT`` has a null category — a field the dimension dumps later "fix",
  which is what drives a genuine ``dim_updated_at`` change.
* ``(PROD_A, SELLER_1)`` is sold in three separate orders, so the CDC feed has
  cumulative decrements to accumulate rather than a single step.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Every credential a generator could find. Cleared for every test.
_LIVE_CREDENTIALS = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "AWS_PROFILE",
    "AWS_SESSION_TOKEN",
)


@pytest.fixture(autouse=True)
def no_live_services(monkeypatch, tmp_path):
    """No test may reach a real broker, registry or bucket — ever.

    Session 6 found the suite producing to the live Confluent topic. A test ran
    ``inventory_cdc.main()`` with ``--sink kafka``; ``main()`` reads ``.env``,
    and on a developer machine that file holds real keys and
    ``confluent_kafka`` is installed. So a test whose docstring expected "no
    broker in tests" instead published a 50-order partial CDC run — three
    times — re-creating the 43 tied-``seq`` SKUs of the Session 5 incident.
    CI never showed it: CI has neither ``.env`` nor ``confluent_kafka``.

    Three layers, so that no single slip reopens the door:

    * ``.env`` is never read (``load_local_env`` sees it as already loaded);
    * the credentials it would have supplied are removed from the process;
    * AWS gets throwaway keys and no credentials file, so a stray boto3 call
      fails authentication instead of falling back to ``~/.aws/credentials``
      — an admin key on the author's machine.

    A test that needs a variable sets it itself with ``monkeypatch.setenv``,
    which runs after this fixture and wins.
    """
    from generators import _common

    monkeypatch.setattr(_common, "_env_loaded", True)
    for name in _LIVE_CREDENTIALS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing-not-a-real-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing-not-a-real-secret")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-aws-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-aws-config"))

# Stable IDs, readable in failure output. Real Olist IDs are 32-char hashes;
# these are short on purpose so an assertion diff is legible.
CU_MOVER = "cu_mover"
CU_STAYER = "cu_stayer"
CU_SOLO = "cu_solo"
CU_DOOMED = "cu_doomed"  # deleted from a later dump: NMBS DELETE material


@pytest.fixture
def customers() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # Same person, two orders, two cities. Keying SCD2 on customer_id
            # would make these look like two different customers.
            ("cid_1", CU_MOVER, 1000, "sao paulo", "SP"),
            ("cid_2", CU_MOVER, 2000, "rio de janeiro", "RJ"),
            ("cid_3", CU_STAYER, 3000, "curitiba", "PR"),
            ("cid_4", CU_STAYER, 3000, "curitiba", "PR"),
            ("cid_5", CU_SOLO, 4000, "belo horizonte", "MG"),
            ("cid_6", CU_DOOMED, 5000, "salvador", "BA"),
            # second order for CU_SOLO, same city: a repeat customer whose
            # attributes did NOT change, so SCD2 must not open a new version
            ("cid_7", CU_SOLO, 4000, "belo horizonte", "MG"),
        ],
        columns=[
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    ).astype({"customer_zip_code_prefix": "Int64"})


@pytest.fixture
def orders() -> pd.DataFrame:
    def ts(value: str | None):
        return pd.Timestamp(value) if value else pd.NaT

    return pd.DataFrame(
        [
            # order_id, customer_id, status, purchase, approved, carrier, delivered, estimated
            (
                "ORD_MULTI", "cid_1", "delivered",
                ts("2017-01-05 10:00:00"), ts("2017-01-05 10:30:00"),
                ts("2017-01-07 08:00:00"), ts("2017-01-12 14:00:00"), ts("2017-01-20 00:00:00"),
            ),
            (
                # placed while ORD_MULTI is still in flight, so the two
                # lifecycles interleave in the stream the way real ones do
                "ORD_OVERLAP", "cid_7", "delivered",
                ts("2017-01-06 09:00:00"), ts("2017-01-06 09:30:00"),
                ts("2017-01-08 08:00:00"), ts("2017-01-11 12:00:00"), ts("2017-01-19 00:00:00"),
            ),
            (
                "ORD_SIMPLE", "cid_2", "delivered",
                ts("2017-02-10 09:00:00"), ts("2017-02-10 09:15:00"),
                ts("2017-02-11 07:00:00"), ts("2017-02-15 16:00:00"), ts("2017-02-22 00:00:00"),
            ),
            (
                # canceled: no carrier/delivery timestamps at all
                "ORD_CANCEL", "cid_3", "canceled",
                ts("2017-03-01 12:00:00"), ts("2017-03-01 12:05:00"),
                pd.NaT, pd.NaT, ts("2017-03-10 00:00:00"),
            ),
            (
                # approved is null but it shipped and delivered anyway
                "ORD_PARTIAL", "cid_4", "delivered",
                ts("2017-04-02 08:00:00"), pd.NaT,
                ts("2017-04-04 09:00:00"), ts("2017-04-09 11:00:00"), ts("2017-04-18 00:00:00"),
            ),
            (
                "ORD_REPEAT", "cid_5", "delivered",
                ts("2017-05-20 15:00:00"), ts("2017-05-20 15:20:00"),
                ts("2017-05-22 10:00:00"), ts("2017-05-27 13:00:00"), ts("2017-06-03 00:00:00"),
            ),
            (
                # only ever purchased; nothing else happened
                "ORD_OPEN", "cid_6", "processing",
                ts("2017-06-11 18:00:00"), pd.NaT, pd.NaT, pd.NaT, ts("2017-06-25 00:00:00"),
            ),
        ],
        columns=[
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )


@pytest.fixture
def order_items() -> pd.DataFrame:
    """One row per UNIT, as Olist stores it.

    (PROD_A, SELLER_1) totals 5 units across three orders: 3 + 1 + 1.
    (PROD_B, SELLER_1) totals 2 units across two orders.
    """
    rows = [
        ("ORD_MULTI", 1, "PROD_A", "SELLER_1", "2017-01-10 10:00:00", 50.0, 10.0),
        ("ORD_MULTI", 2, "PROD_A", "SELLER_1", "2017-01-10 10:00:00", 50.0, 10.0),
        ("ORD_MULTI", 3, "PROD_A", "SELLER_1", "2017-01-10 10:00:00", 50.0, 10.0),
        ("ORD_OVERLAP", 1, "PROD_B", "SELLER_1", "2017-01-11 09:00:00", 120.0, 15.0),
        ("ORD_SIMPLE", 1, "PROD_B", "SELLER_1", "2017-02-14 09:00:00", 120.0, 15.0),
        ("ORD_CANCEL", 1, "PROD_NOCAT", "SELLER_2", "2017-03-06 12:00:00", 30.0, 5.0),
        ("ORD_PARTIAL", 1, "PROD_A", "SELLER_1", "2017-04-07 08:00:00", 50.0, 10.0),
        ("ORD_PARTIAL", 2, "PROD_C", "SELLER_2", "2017-04-07 08:00:00", 80.0, 12.0),
        ("ORD_REPEAT", 1, "PROD_A", "SELLER_1", "2017-05-25 15:00:00", 55.0, 10.0),
        # ORD_OPEN has no items — an order that exists with nothing under it.
    ]
    frame = pd.DataFrame(
        rows,
        columns=[
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )
    frame["shipping_limit_date"] = pd.to_datetime(frame["shipping_limit_date"])
    return frame.astype({"order_item_id": "Int64"})


@pytest.fixture
def products() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("PROD_A", "cama_mesa_banho", 40, 300, 2, 500, 20, 10, 15),
            ("PROD_B", "informatica_acessorios", 55, 800, 4, 1200, 30, 12, 25),
            # null category: the dumps "fix" this on a later night, which is a
            # real content change and must move dim_updated_at.
            ("PROD_NOCAT", None, 30, 150, 1, 300, 15, 8, 10),
            ("PROD_C", "esporte_lazer", 45, 400, 3, 900, 25, 15, 20),
        ],
        columns=[
            "product_id",
            "product_category_name",
            "product_name_lenght",  # sic — the source misspells it
            "product_description_lenght",  # sic
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    ).astype(
        {
            "product_name_lenght": "Int64",
            "product_description_lenght": "Int64",
            "product_photos_qty": "Int64",
            "product_weight_g": "Int64",
            "product_length_cm": "Int64",
            "product_height_cm": "Int64",
            "product_width_cm": "Int64",
        }
    )


@pytest.fixture
def sellers() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("SELLER_1", 10000, "sao paulo", "SP"),
            ("SELLER_2", 20000, "campinas", "SP"),
        ],
        columns=["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    ).astype({"seller_zip_code_prefix": "Int64"})


@pytest.fixture
def olist_frames(customers, orders, order_items, products, sellers) -> dict[str, pd.DataFrame]:
    """All five generator inputs, keyed by the logical names in generators/olist.py."""
    return {
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "products": products,
        "sellers": sellers,
    }


@pytest.fixture
def raw_dir(tmp_path, olist_frames) -> Path:
    """Materialize the fixture as CSVs laid out exactly like data/raw/.

    Lets tests exercise the real loader — column contract, dtype coercion and
    date parsing included — instead of handing generators pre-built DataFrames
    and never proving the CSV path works.
    """
    from generators.olist import TABLES

    target = tmp_path / "raw"
    target.mkdir()
    for name, frame in olist_frames.items():
        frame.to_csv(target / TABLES[name].filename, index=False)
    return target
