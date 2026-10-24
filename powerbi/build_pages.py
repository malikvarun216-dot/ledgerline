"""Write the Power BI report's pages from a short spec, so the boards live in git as code (Session 23).

    python powerbi/build_pages.py

Rewrites every page under ``ledgerline.Report/definition/pages/`` (Power BI Desktop closed, or
closed without saving afterwards — it would overwrite these files). Then open ``ledgerline.pbip``.
It is the starting layout: once pages are rearranged and saved in Power BI Desktop, Desktop is the
editor, and running this again would put the starting layout back.

The JSON shape (schema versions, ``Entity`` + ``Property``, ``queryRef``) is copied from a visual Power
BI Desktop 2.159 saved itself. Every number a board shows is a measure from the semantic model, and
each measure divides sums the way the dbt mart does; ``answer_key.sql`` is what they must equal.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

PAGES_DIR = Path(__file__).resolve().parent / "ledgerline.Report" / "definition" / "pages"
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"

# A field is m(table, name) for a measure or c(table, name) for a column.
# A visual is (type, title, {role: [fields]}, (x, y, width, height)) on a 1280 x 720 page.
DP, LR, SS = "DELIVERY_PROMISE", "LATE_VS_RATING", "SELLER_SCORECARD"
CT, CC, DQ = "COMPLAINT_TRENDS", "CUSTOMER_CURRENT", "DQ_DELIVERY_PROMISE"


def m(table: str, name: str) -> tuple:
    return ("m", table, name)


def c(table: str, name: str) -> tuple:
    return ("c", table, name)


def card(title: str, field: tuple, box: tuple) -> tuple:
    return ("card", title, {"Values": [field]}, box)


def chart(vtype: str, title: str, category: tuple, y: tuple, box: tuple, series: tuple | None = None):
    roles = {"Category": [category], "Y": [y]}
    if series:
        roles["Series"] = [series]
    return (vtype, title, roles, box)


def table(title: str, fields: list, box: tuple) -> tuple:
    return ("tableEx", title, {"Values": fields}, box)


def slicer(title: str, field: tuple, box: tuple) -> tuple:
    return ("slicer", title, {"Values": [field]}, box)


PAGES = [
    ("Promise accuracy", [
        card("Delivered orders", m(DP, "Delivered orders"), (20, 20, 190, 110)),
        card("Late orders", m(DP, "Late orders"), (225, 20, 190, 110)),
        card("Late share", m(DP, "Late share"), (430, 20, 190, 110)),
        card("Promised days (avg)", m(DP, "Avg promised days"), (635, 20, 190, 110)),
        card("Actual days (avg)", m(DP, "Avg actual days"), (840, 20, 190, 110)),
        card("Days late, when late (avg)", m(DP, "Avg days late (late orders)"), (1045, 20, 215, 110)),
        chart("lineChart", "Late share by purchase month", c(DP, "PURCHASE_MONTH"), m(DP, "Late share"),
              (20, 150, 620, 270)),
        chart("clusteredBarChart", "Late share by customer state", c(DP, "CUSTOMER_STATE"),
              m(DP, "Late share"), (660, 150, 600, 550)),
        table("Orders by how early or late", [c(DP, "LATENESS_BUCKET"), m(DP, "Delivered orders")],
              (20, 440, 620, 260)),
    ]),
    ("Late vs rating", [
        chart("clusteredColumnChart", "Average score: on time vs late", c(LR, "IS_LATE"), m(LR, "Avg score"),
              (20, 20, 400, 300)),
        chart("clusteredColumnChart", "Share of 1-2 star reviews: on time vs late", c(LR, "IS_LATE"),
              m(LR, "Low score share"), (440, 20, 400, 300)),
        card("Rated before the parcel arrived", m(LR, "Rated before arrival"), (860, 20, 400, 140)),
        slicer("Customer state", c(LR, "CUSTOMER_STATE"), (860, 180, 400, 140)),
        table("By lateness bucket (right: sum / count; WRONG: average of the stored averages)",
              [c(LR, "LATENESS_BUCKET"), m(LR, "Reviewed orders"), m(LR, "Avg score"),
               m(LR, "Low score share"), m(LR, "Avg score WRONG (average of averages)")],
              (20, 340, 1240, 360)),
    ]),
    ("Seller scorecard", [
        card("Seller-months", m(SS, "Seller-months"), (20, 20, 250, 110)),
        card("Sellers", m(SS, "Sellers"), (290, 20, 250, 110)),
        slicer("Tier", c(SS, "TIER"), (560, 20, 700, 110)),
        table("Seller-months by tier", [c(SS, "TIER"), m(SS, "Seller-months"), m(SS, "Sellers")],
              (20, 150, 520, 220)),
        chart("clusteredColumnChart", "Seller-months by tier", c(SS, "TIER"), m(SS, "Seller-months"),
              (20, 390, 520, 310)),
        table("Sellers (filter with the tier slicer)",
              [c(SS, "SELLER_ID"), m(SS, "Seller-months"), m(SS, "Seller late rate"),
               m(SS, "Seller avg score"), m(SS, "Seller revenue")],
              (560, 150, 700, 550)),
    ]),
    ("Complaint trends", [
        chart("lineChart", "Reviews per month by AI topic", c(CT, "REVIEW_MONTH"), m(CT, "Review count"),
              (20, 20, 1240, 330), series=c(CT, "TOPIC")),
        chart("clusteredBarChart", "Negative reviews by topic", c(CT, "TOPIC"), m(CT, "Negative reviews"),
              (20, 370, 500, 330)),
        table("Topics",
              [c(CT, "TOPIC"), m(CT, "Review count"), m(CT, "Negative reviews"), m(CT, "Negative share"),
               m(CT, "Review avg score")],
              (540, 370, 720, 330)),
    ]),
    ("Governance check", [
        card("Customers this role can see", m(CC, "Customers"), (20, 20, 400, 140)),
        card("Of them, city masked (***)", m(CC, "Cities masked"), (440, 20, 400, 140)),
        table("Customers by state (the row policy decides which states appear)",
              [c(CC, "CUSTOMER_STATE"), m(CC, "Customers")], (20, 180, 820, 520)),
    ]),
    # DirectQuery: these visuals send SQL to Snowflake on every draw (decisions.md S23). The ADBC driver
    # has a known count-distinct issue; the two distinct counts must both equal answer_key's.
    ("DirectQuery probe", [
        card("Distinct customers - DirectQuery (Snowflake counts)", m(DQ, "DQ distinct customers"),
             (20, 20, 400, 140)),
        card("Distinct customers - Import (Power BI counts)", m(DP, "Distinct customers"),
             (440, 20, 400, 140)),
        card("Delivered orders - DirectQuery", m(DQ, "DQ delivered orders"), (860, 20, 400, 140)),
        table("By state - DirectQuery",
              [c(DQ, "CUSTOMER_STATE"), m(DQ, "DQ delivered orders"), m(DQ, "DQ late orders"),
               m(DQ, "DQ distinct customers")],
              (20, 180, 820, 520)),
    ]),
]


def _id(*parts: str) -> str:
    """Stable 20-hex names, so a rebuild changes only what the spec changed."""
    return hashlib.sha1("/".join(parts).encode()).hexdigest()[:20]


def _field(kind: str, table: str, name: str) -> dict:
    ref = {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}
    return {
        "field": {"Measure" if kind == "m" else "Column": ref},
        "queryRef": f"{table}.{name}",
        "nativeQueryRef": name,
    }


def _literal(value: str) -> dict:
    return {"expr": {"Literal": {"Value": value}}}


def _visual(page: str, i: int, vtype: str, title: str, roles: dict, box: tuple) -> dict:
    x, y, w, h = box
    # Cards show whole numbers: the default rounds to "96K", which hid that 95,696 = 95,696.
    units = {}
    if vtype == "card":
        units = {"objects": {"labels": [{"properties": {"labelDisplayUnits": _literal("1D")}}]}}
    return {
        "$schema": f"{SCHEMA}/visualContainer/2.14.0/schema.json",
        "name": _id(page, str(i), title),
        "position": {"x": x, "y": y, "z": i, "height": h, "width": w, "tabOrder": i},
        "visual": {
            "visualType": vtype,
            "query": {"queryState": {
                role: {"projections": [_field(*f) for f in fields]} for role, fields in roles.items()
            }},
            **units,
            "visualContainerObjects": {"title": [{"properties": {
                "show": _literal("true"),
                "text": _literal("'" + title.replace("'", "''") + "'"),
            }}]},
            "drillFilterOtherVisuals": True,
        },
    }


def main() -> int:
    shutil.rmtree(PAGES_DIR)
    PAGES_DIR.mkdir()
    order = []
    for name, visuals in PAGES:
        pid = _id(name)
        order.append(pid)
        pdir = PAGES_DIR / pid
        (pdir / "visuals").mkdir(parents=True)
        page = {"$schema": f"{SCHEMA}/page/2.1.0/schema.json", "name": pid, "displayName": name,
                "displayOption": "FitToPage", "height": 720, "width": 1280}
        (pdir / "page.json").write_text(json.dumps(page, indent=2), encoding="utf-8", newline="\n")
        for i, spec in enumerate(visuals):
            v = _visual(name, i, *spec)
            vdir = pdir / "visuals" / v["name"]
            vdir.mkdir()
            (vdir / "visual.json").write_text(json.dumps(v, indent=2), encoding="utf-8", newline="\n")
        print(f"{name}: {len(visuals)} visuals")
    meta = {"$schema": f"{SCHEMA}/pagesMetadata/1.1.0/schema.json", "pageOrder": order,
            "activePageName": order[0]}
    (PAGES_DIR / "pages.json").write_text(json.dumps(meta, indent=2), encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
