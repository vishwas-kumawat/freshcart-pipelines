"""Pure-Python equivalents of the FreshCart order transformation rules."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import re
from typing import Any, Iterable


_EMAIL_PATTERN = re.compile(
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)
_PINCODE_PATTERN = re.compile(r"^\d{6}$")
_AMOUNT_CHARACTERS = re.compile(r"[^0-9.\-]")
_WHITESPACE = re.compile(r"\s+")
_ORDER_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%b %d, %Y",
)


def normalize_email(value: Any) -> str | None:
    """Trim and lowercase an email; return None for an empty value."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def normalize_name(value: Any) -> str | None:
    """Collapse whitespace and apply title case to a customer name."""
    if value is None:
        return None
    normalized = _WHITESPACE.sub(" ", str(value).strip())
    return normalized.title() or None


def parse_order_date(value: Any) -> date | None:
    """Parse the date formats used by the web and mobile order feeds."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    for date_format in _ORDER_DATE_FORMATS:
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue
    return None


def parse_amount(value: Any) -> Decimal | None:
    """Parse an amount while preserving decimal points and negative signs."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    normalized = _AMOUNT_CHARACTERS.sub("", text)
    if not normalized:
        return None
    try:
        return Decimal(normalized).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def normalize_order(source: str, raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize either supported order payload into common field names."""
    if source.lower() == "mobile":
        customer = raw.get("customer") or {}
        total = raw.get("total") or {}
        amount_value = total.get("value")
        currency_value = total.get("currency")
        email_value = customer.get("email")
        name_value = customer.get("name")
        order_id = raw.get("orderId")
        order_date_value = raw.get("orderDate")
        pincode_value = raw.get("deliveryPin")
        updated_value = raw.get("updatedAt")
    elif source.lower() == "web":
        amount_value = raw.get("amount")
        currency_value = raw.get("currency")
        email_value = raw.get("cust_email")
        name_value = raw.get("cust_name")
        order_id = raw.get("order_id")
        order_date_value = raw.get("order_date")
        pincode_value = raw.get("pincode")
        updated_value = raw.get("updated_at")
    else:
        raise ValueError(f"Unsupported order source: {source}")

    order_date = parse_order_date(order_date_value)
    updated_at = None
    if updated_value is not None:
        text = str(updated_value).strip()
        try:
            updated_at = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            for date_format in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
            ):
                try:
                    updated_at = datetime.strptime(text, date_format)
                    break
                except ValueError:
                    continue

    return {
        "order_id": str(order_id).strip() if order_id is not None else None,
        "email": normalize_email(email_value),
        "customer_name": normalize_name(name_value),
        "order_date": order_date,
        "amount": parse_amount(amount_value),
        "currency": (
            str(currency_value).strip().upper()
            if currency_value is not None and str(currency_value).strip()
            else None
        ),
        "pincode": (
            str(pincode_value).strip()
            if pincode_value is not None
            else None
        ),
        "updated_at": updated_at,
        "source": source.lower(),
        "raw": raw,
    }


def validate_order(order: dict[str, Any]) -> list[str]:
    """Return all validation failures without rejecting a missing amount."""
    reasons = []
    if not order.get("order_id"):
        reasons.append("MISSING_ORDER_ID")
    email = order.get("email")
    if not email or not _EMAIL_PATTERN.fullmatch(email):
        reasons.append("INVALID_EMAIL")
    if order.get("order_date") is None:
        reasons.append("INVALID_ORDER_DATE")
    amount = order.get("amount")
    if amount is not None and amount < 0:
        reasons.append("NEGATIVE_AMOUNT")
    if amount is None and order.get("amount_provided"):
        reasons.append("INVALID_AMOUNT")
    pincode = order.get("pincode")
    if not pincode or not _PINCODE_PATTERN.fullmatch(pincode):
        reasons.append("INVALID_PINCODE")
    return reasons


def normalize_and_validate_order(
    source: str, raw: dict[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    """Normalize an order and return its validation reasons."""
    order = normalize_order(source, raw)
    amount_value = (
        (raw.get("total") or {}).get("value")
        if source.lower() == "mobile"
        else raw.get("amount")
    )
    order["amount_provided"] = (
        amount_value is not None and str(amount_value).strip() != ""
    )
    return order, validate_order(order)


def deduplicate_orders(
    orders: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Keep the newest record per order, preferring mobile on exact ties."""
    source_priority = {"web": 0, "mobile": 1}
    latest = {}
    for order in orders:
        key = order["order_id"]
        current = latest.get(key)
        candidate_rank = (
            order.get("updated_at") or datetime.min,
            source_priority.get(order.get("source"), -1),
            str(order.get("run_id") or ""),
            str(order.get("src_file") or ""),
            int(order.get("src_row") or 0),
        )
        current_rank = (
            (
                current.get("updated_at") or datetime.min,
                source_priority.get(current.get("source"), -1),
                str(current.get("run_id") or ""),
                str(current.get("src_file") or ""),
                int(current.get("src_row") or 0),
            )
            if current is not None
            else None
        )
        if current is None or candidate_rank > current_rank:
            latest[key] = order
    return sorted(latest.values(), key=lambda order: order["order_id"])


def loyalty_points(amount_inr: Decimal, multiplier: int) -> int:
    """Award one point per full INR 100, multiplied by customer tier."""
    hundreds = (amount_inr / Decimal(100)).to_integral_value(
        rounding=ROUND_FLOOR
    )
    return int(hundreds) * multiplier


def fx_rate_for_date(
    rates: dict[date, Decimal], target_date: date
) -> Decimal | None:
    """Return the latest available rate on or before a target date."""
    eligible_dates = [rate_date for rate_date in rates if rate_date <= target_date]
    if not eligible_dates:
        return None
    return rates[max(eligible_dates)]


def convert_to_inr(amount: Decimal, rate: Decimal) -> Decimal:
    """Convert to INR and round to the Gold table's two decimal places."""
    return (amount * rate).quantize(Decimal("0.01"))