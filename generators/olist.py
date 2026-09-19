"""Olist dataset contract — the single place that knows what the raw CSVs look like.

Why this module exists
----------------------
Three generators, a fetch script and a test suite all read the same nine CSVs.
If each one spelled the column names itself, a typo would surface as a
``KeyError`` deep inside a generator at run time, and the Olist column names
are *exactly* the kind that invite typos (``product_name_lenght`` really is
misspelled in the source data).

So the schema is declared once, here, and every reader goes through
:func:`load`. A missing or renamed column fails loudly at load time, naming the
file and the column, instead of silently producing a generator that emits
half-empty events.

How it fits
-----------
``scripts/fetch_olist.py`` downloads into ``data/raw/`` and validates against
this contract. ``generators/*.py`` call :func:`load` and never touch a path or
a ``read_csv`` themselves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

# Repo-root-relative default. Overridable everywhere via --raw-dir.
DEFAULT_RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# The Kaggle dataset slug, used by scripts/fetch_olist.py.
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"


@dataclass(frozen=True)
class TableSpec:
    """What one Olist CSV must contain for this project to work.

    ``columns``       — every column we depend on. Absence is a hard failure.
    ``expected_rows`` — the published row count. A mismatch is a *warning*,
                        not an error: Kaggle has re-uploaded this dataset
                        before, and a slightly different count is a data
                        observation, not a broken contract. Counts stay marked
                        unverified until a real download confirms them.
    ``date_columns``  — parsed to datetime64 on load, so no generator has to
                        remember which strings are really timestamps.
    """

    filename: str
    columns: tuple[str, ...]
    expected_rows: int
    date_columns: tuple[str, ...] = field(default=())
    dtypes: dict[str, str] = field(default_factory=dict)


TABLES: dict[str, TableSpec] = {
    "customers": TableSpec(
        filename="olist_customers_dataset.csv",
        # customer_id is PER ORDER (~99k). customer_unique_id is the actual
        # PERSON (~96k). Keying the SCD2 snapshot on customer_id makes every
        # order look like a brand-new customer. See docs/decisions.md.
        columns=(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ),
        expected_rows=99_441,
        dtypes={"customer_zip_code_prefix": "Int64"},
    ),
    "orders": TableSpec(
        filename="olist_orders_dataset.csv",
        # Four nullable timestamps = the status lifecycle. This is what cannot
        # be faked, and it is why Olist was chosen over a clickstream set.
        columns=(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
        expected_rows=99_441,
        date_columns=(
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
    ),
    "order_items": TableSpec(
        filename="olist_order_items_dataset.csv",
        # One row per UNIT, not per line: order_item_id runs 1..n within an
        # order. So units sold = row count, which is what the cross-source
        # reconciliation test ties against cumulative CDC decrements.
        columns=(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ),
        expected_rows=112_650,
        date_columns=("shipping_limit_date",),
        dtypes={"order_item_id": "Int64", "price": "float64", "freight_value": "float64"},
    ),
    "products": TableSpec(
        filename="olist_products_dataset.csv",
        # 'lenght' is misspelled in the source data. Do not 'fix' it here —
        # the contract must match the file on disk, not the file we wish for.
        columns=(
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
        expected_rows=32_951,
        dtypes={
            "product_name_lenght": "Int64",
            "product_description_lenght": "Int64",
            "product_photos_qty": "Int64",
            "product_weight_g": "Int64",
            "product_length_cm": "Int64",
            "product_height_cm": "Int64",
            "product_width_cm": "Int64",
        },
    ),
    "sellers": TableSpec(
        filename="olist_sellers_dataset.csv",
        columns=("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
        expected_rows=3_095,
        dtypes={"seller_zip_code_prefix": "Int64"},
    ),
    "payments": TableSpec(
        filename="olist_order_payments_dataset.csv",
        columns=(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ),
        expected_rows=103_886,
        dtypes={
            "payment_sequential": "Int64",
            "payment_installments": "Int64",
            "payment_value": "float64",
        },
    ),
    "reviews": TableSpec(
        filename="olist_order_reviews_dataset.csv",
        columns=(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ),
        expected_rows=99_224,
        date_columns=("review_creation_date", "review_answer_timestamp"),
        dtypes={"review_score": "Int64"},
    ),
    "geolocation": TableSpec(
        filename="olist_geolocation_dataset.csv",
        columns=(
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ),
        expected_rows=1_000_163,
        dtypes={"geolocation_zip_code_prefix": "Int64"},
    ),
    "category_translation": TableSpec(
        filename="product_category_name_translation.csv",
        columns=("product_category_name", "product_category_name_english"),
        expected_rows=71,
    ),
}

# Only these four are needed to run the generators. geolocation (1M rows),
# payments and reviews are loaded by nothing in Session 1 — they are in the
# contract because they are in the download, not because we use them yet.
GENERATOR_TABLES = ("customers", "orders", "order_items", "products", "sellers")


class OlistSchemaError(RuntimeError):
    """A raw CSV is missing, or is missing a column the project depends on."""


def path_for(name: str, raw_dir: Path | str | None = None) -> Path:
    """Absolute path of one raw CSV. Raises on an unknown logical name."""
    if name not in TABLES:
        raise OlistSchemaError(
            f"unknown Olist table {name!r}; known: {', '.join(sorted(TABLES))}"
        )
    root = Path(raw_dir) if raw_dir is not None else DEFAULT_RAW_DIR
    return root / TABLES[name].filename


def load(name: str, raw_dir: Path | str | None = None) -> pd.DataFrame:
    """Load one Olist table, enforcing the column contract.

    Returns only the contracted columns, in contract order, with date columns
    already parsed. Extra columns in the file are dropped rather than passed
    through — a generator should never accidentally depend on a column the
    contract does not name.
    """
    spec = TABLES[name]
    csv_path = path_for(name, raw_dir)
    if not csv_path.exists():
        raise OlistSchemaError(
            f"{csv_path} not found. Run:  python scripts/fetch_olist.py"
        )

    frame = pd.read_csv(csv_path, dtype=str, keep_default_na=True)

    missing = [c for c in spec.columns if c not in frame.columns]
    if missing:
        raise OlistSchemaError(
            f"{spec.filename} is missing contracted column(s): {missing}. "
            f"Found: {list(frame.columns)}"
        )

    frame = frame[list(spec.columns)].copy()

    for column in spec.date_columns:
        frame[column] = pd.to_datetime(frame[column], errors="coerce")
    for column, dtype in spec.dtypes.items():
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(dtype)

    return frame


@dataclass
class TableReport:
    name: str
    present: bool
    rows: int | None
    expected_rows: int
    missing_columns: list[str]

    @property
    def ok(self) -> bool:
        """Structurally sound. Row-count drift is reported but not fatal."""
        return self.present and not self.missing_columns

    @property
    def row_delta(self) -> int | None:
        return None if self.rows is None else self.rows - self.expected_rows


def validate(raw_dir: Path | str | None = None, names: tuple[str, ...] | None = None) -> list[TableReport]:
    """Check every contracted CSV without loading it fully into typed form.

    Reads only the header plus a row count, so validating the 1M-row
    geolocation file stays cheap.
    """
    reports: list[TableReport] = []
    for name in names or tuple(TABLES):
        spec = TABLES[name]
        csv_path = path_for(name, raw_dir)
        if not csv_path.exists():
            reports.append(TableReport(name, False, None, spec.expected_rows, list(spec.columns)))
            continue
        header = pd.read_csv(csv_path, nrows=0)
        missing = [c for c in spec.columns if c not in header.columns]
        with csv_path.open("r", encoding="utf-8", errors="replace") as handle:
            rows = sum(1 for _ in handle) - 1  # minus header
        reports.append(TableReport(name, True, rows, spec.expected_rows, missing))
    return reports
