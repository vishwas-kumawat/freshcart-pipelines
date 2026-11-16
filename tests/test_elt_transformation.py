from datetime import date
from decimal import Decimal
from pathlib import Path

from elt import mock_data
from elt.transformations import (
    convert_to_inr,
    deduplicate_orders,
    fx_rate_for_date,
    loyalty_points,
    normalize_and_validate_order,
    normalize_email,
    parse_amount,
    parse_order_date,
)


ROOT = Path(__file__).resolve().parents[1]


def _normalized_orders():
    orders = []
    for source, records in (
        ("web", mock_data.WEB_ORDERS),
        ("mobile", mock_data.MOBILE_ORDERS),
    ):
        orders.extend(
            normalize_and_validate_order(source, record)
            for record in records
        )
    return orders


def test_web_and_mobile_formats_normalize_to_common_fields():
    web, _ = normalize_and_validate_order("web", mock_data.WEB_ORDERS[0])
    mobile, _ = normalize_and_validate_order(
        "mobile", mock_data.MOBILE_ORDERS[0]
    )

    assert web["order_id"] == "ORD-1001"
    assert web["email"] == "priya.r@gmail.com"
    assert web["amount"] == Decimal("1250.00")
    assert web["currency"] == "INR"
    assert mobile["order_id"] == "ORD-1002"
    assert mobile["email"] == "arjun.k@yahoo.in"
    assert mobile["amount"] == Decimal("42.50")
    assert mobile["currency"] == "USD"


def test_email_date_and_amount_normalization():
    assert normalize_email("  Priya.R@Gmail.com ") == "priya.r@gmail.com"
    assert parse_order_date("01/09/2026") == date(2026, 9, 1)
    assert parse_order_date("Sep 3, 2026") == date(2026, 9, 3)
    assert parse_amount("₹ 1,250.00") == Decimal("1250.00")
    assert parse_amount("-500") == Decimal("-500.00")
    assert parse_amount("not-an-amount") is None


def test_baseline_validation_rejects_only_expected_orders():
    normalized = _normalized_orders()
    rejected = {
        order["order_id"]
        for order, reasons in normalized
        if reasons
    }

    assert len(normalized) == 8
    assert rejected == {"ORD-1004", "ORD-1006"}
    assert len([order for order, reasons in normalized if not reasons]) == 6
    assert not next(
        reasons for order, reasons in normalized
        if order["order_id"] == "ORD-1003"
        and order["source"] == "web"
    )


def test_rejection_reasons_cover_negative_amount_email_and_pincode():
    reasons_by_order = {
        order["order_id"]: reasons
        for order, reasons in _normalized_orders()
        if reasons
    }

    assert "NEGATIVE_AMOUNT" in reasons_by_order["ORD-1004"]
    assert "INVALID_EMAIL" in reasons_by_order["ORD-1006"]
    assert "INVALID_PINCODE" in reasons_by_order["ORD-1006"]


def test_deduplication_selects_newest_record_and_resolves_ties():
    normalized = [
        order
        for order, reasons in _normalized_orders()
        if not reasons
    ]

    deduplicated = deduplicate_orders(normalized)
    selected = {order["order_id"]: order for order in deduplicated}

    assert len(deduplicated) == 4
    assert selected["ORD-1003"]["source"] == "mobile"
    assert selected["ORD-1003"]["amount"] == Decimal("2100.00")
    assert selected["ORD-1002"]["source"] == "mobile"


def test_fx_forward_fill_and_baseline_gold_revenue_and_points():
    rates = {
        date(2026, 9, 1): Decimal("83.20"),
        date(2026, 9, 3): Decimal("83.10"),
    }
    assert fx_rate_for_date(rates, date(2026, 9, 2)) == Decimal("83.20")
    assert fx_rate_for_date(rates, date(2026, 9, 4)) == Decimal("83.10")
    assert fx_rate_for_date(rates, date(2026, 8, 31)) is None

    normalized = [
        order
        for order, reasons in _normalized_orders()
        if not reasons
    ]
    orders = deduplicate_orders(normalized)
    tiers = {
        "ORD-1001": 1,
        "ORD-1002": 1,
        "ORD-1003": 3,
        "ORD-1005": 2,
    }
    total_revenue = Decimal("0.00")
    total_points = 0
    for order in orders:
        rate = (
            Decimal("1")
            if order["currency"] == "INR"
            else fx_rate_for_date(rates, order["order_date"])
        )
        amount_inr = convert_to_inr(order["amount"], rate)
        total_revenue += amount_inr
        total_points += loyalty_points(
            amount_inr,
            tiers[order["order_id"]],
        )

    assert total_revenue == Decimal("7785.00")
    assert total_points == 126
    assert loyalty_points(Decimal("899.00"), 2) == 16


def test_sql_is_merge_based_and_uses_required_dedup_ordering():
    clean_sql = (ROOT / "elt" / "sql" / "10_orders_clean.sql").read_text(
        encoding="utf-8"
    )
    dedup_sql = (ROOT / "elt" / "sql" / "20_orders_dedup.sql").read_text(
        encoding="utf-8"
    )
    gold_sql = (ROOT / "elt" / "sql" / "40_gold.sql").read_text(
        encoding="utf-8"
    )

    assert "ARRAY_CONSTRUCT_COMPACT" in clean_sql
    assert "MERGE INTO FRESHCART.QUARANTINE.ORDERS" in clean_sql
    assert "QUALIFY ROW_NUMBER()" in dedup_sql
    assert "UPDATED_AT DESC NULLS LAST" in dedup_sql
    assert "target.ROW_HASH <> source.ROW_HASH" in gold_sql
    assert "PUBLISHED_AT = CURRENT_TIMESTAMP()" in gold_sql