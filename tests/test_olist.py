"""Tests for the Olist schema contract.

The contract's job is to turn "a column got renamed or dropped" into a loud
failure naming the file and the column, instead of a KeyError surfacing three
modules deep inside a generator.
"""

from __future__ import annotations

import pandas as pd
import pytest

from generators import olist


def test_loader_returns_contracted_columns_in_contract_order(raw_dir):
    frame = olist.load("orders", raw_dir)
    assert list(frame.columns) == list(olist.TABLES["orders"].columns)


def test_date_columns_are_parsed_not_left_as_strings(raw_dir):
    orders = olist.load("orders", raw_dir)
    assert pd.api.types.is_datetime64_any_dtype(orders["order_purchase_timestamp"])
    # a genuinely absent timestamp stays absent rather than becoming epoch zero
    assert orders.set_index("order_id").loc["ORD_OPEN", "order_approved_at"] is pd.NaT


def test_numeric_columns_survive_nulls_as_nullable_ints(raw_dir):
    """Olist has null weights and photo counts; float64 coercion would be lossy."""
    products = olist.load("products", raw_dir)
    assert str(products["product_weight_g"].dtype) == "Int64"
    assert str(products["product_photos_qty"].dtype) == "Int64"


def test_the_misspelled_column_is_contracted_as_misspelled():
    """'lenght' is wrong in the source. The contract matches the file, not the wish."""
    assert "product_name_lenght" in olist.TABLES["products"].columns
    assert "product_name_length" not in olist.TABLES["products"].columns


def test_customer_contract_carries_both_ids():
    """Losing customer_unique_id here would silently break SCD2 downstream."""
    columns = olist.TABLES["customers"].columns
    assert "customer_id" in columns
    assert "customer_unique_id" in columns


def test_missing_file_names_the_fix(tmp_path):
    with pytest.raises(olist.OlistSchemaError, match="fetch_olist"):
        olist.load("orders", tmp_path)


def test_missing_column_names_the_file_and_the_column(raw_dir, tmp_path):
    broken = tmp_path / "broken"
    broken.mkdir()
    frame = pd.read_csv(raw_dir / olist.TABLES["customers"].filename)
    frame.drop(columns=["customer_unique_id"]).to_csv(
        broken / olist.TABLES["customers"].filename, index=False
    )

    with pytest.raises(olist.OlistSchemaError) as caught:
        olist.load("customers", broken)
    assert "customer_unique_id" in str(caught.value)
    assert olist.TABLES["customers"].filename in str(caught.value)


def test_unknown_table_name_is_rejected():
    with pytest.raises(olist.OlistSchemaError, match="unknown Olist table"):
        olist.path_for("not_a_table")


def test_extra_columns_in_the_file_are_dropped(raw_dir, tmp_path):
    """A generator must not accidentally depend on an uncontracted column."""
    extended = tmp_path / "extended"
    extended.mkdir()
    frame = pd.read_csv(raw_dir / olist.TABLES["sellers"].filename)
    frame["surprise_column"] = "x"
    frame.to_csv(extended / olist.TABLES["sellers"].filename, index=False)

    loaded = olist.load("sellers", extended)
    assert "surprise_column" not in loaded.columns


def test_validate_reports_missing_and_present_without_loading_everything(raw_dir):
    reports = {r.name: r for r in olist.validate(raw_dir)}

    assert reports["orders"].present and reports["orders"].ok
    assert reports["orders"].rows == 7
    assert reports["orders"].row_delta == 7 - olist.TABLES["orders"].expected_rows

    # the fixture only materializes the five generator inputs
    assert reports["geolocation"].present is False
    assert reports["geolocation"].ok is False


def test_generator_tables_are_all_in_the_contract():
    assert set(olist.GENERATOR_TABLES) <= set(olist.TABLES)
