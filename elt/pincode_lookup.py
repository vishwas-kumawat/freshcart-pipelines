"""Incremental pincode lookups and Bronze ingestion."""

from datetime import date
from typing import Any

from . import extract
from .snowflake_io import load_file


MISSING_PINCODES_SQL = """
SELECT DISTINCT orders.PINCODE
FROM FRESHCART.SILVER.ORDERS_DEDUP AS orders
LEFT JOIN FRESHCART.REF.PINCODE_GEO AS geo
  ON geo.PINCODE = orders.PINCODE
WHERE REGEXP_LIKE(orders.PINCODE, '^[0-9]{6}$')
  AND geo.PINCODE IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM FRESHCART.BRONZE.PINCODE_RAW AS raw_pin
    WHERE raw_pin.RAW:pincode::STRING = orders.PINCODE
      AND LOWER(raw_pin.RAW:response[0]:Status::STRING) = 'success'
  )
ORDER BY orders.PINCODE
"""


def _missing_pincodes(connection) -> list[str]:
    cursor = connection.cursor()
    try:
        cursor.execute(MISSING_PINCODES_SQL)
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def load_missing_pincodes(
    connection,
    run_id: str,
    mock: bool = True,
    run_date: date | None = None,
) -> int:
    """Fetch, land, and load only missing pincode responses."""
    pins = _missing_pincodes(connection)
    if not pins:
        print("Pincode lookup: no missing pincodes")
        return 0

    session = extract.http_session()
    try:
        records: list[dict[str, Any]] = extract.pincodes(
            session,
            pins,
            mock=mock,
        )
    finally:
        session.close()

    path = extract.write_landing(
        "pincode",
        records,
        run_id,
        run_date or date.today(),
    )
    if path is None:
        raise RuntimeError(
            "Pincode lookup returned no records for requested pins"
        )

    load_file(connection, path, "pincode")
    return len(records)