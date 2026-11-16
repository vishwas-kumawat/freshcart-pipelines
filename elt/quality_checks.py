"""Snowflake quality checks and baseline pipeline metrics."""


QUALITY_CHECKS = {
    "validity_accounted_for": """
        SELECT COUNT(*)
        FROM FRESHCART.SILVER.ORDERS_CHECKED AS checked
        WHERE (
            checked.IS_VALID
            AND NOT EXISTS (
                SELECT 1
                FROM FRESHCART.SILVER.ORDERS_CLEAN AS clean
                WHERE clean.INGEST_SEQ = checked.INGEST_SEQ
            )
        ) OR (
            NOT checked.IS_VALID
            AND NOT EXISTS (
                SELECT 1
                FROM FRESHCART.QUARANTINE.ORDERS AS rejected
                WHERE rejected.RUN_ID = checked.RUN_ID
                  AND rejected.ORDER_ID = checked.ORDER_ID
            )
        )
    """,
    "deduplicated_and_gold_order_ids_unique": """
        SELECT
          (SELECT COUNT(*) FROM (
            SELECT ORDER_ID
            FROM FRESHCART.SILVER.ORDERS_DEDUP
            GROUP BY ORDER_ID
            HAVING COUNT(*) > 1
          )) +
          (SELECT COUNT(*) FROM (
            SELECT ORDER_ID
            FROM FRESHCART.GOLD.FCT_ORDERS
            GROUP BY ORDER_ID
            HAVING COUNT(*) > 1
          ))
    """,
    "gold_orders_have_customers": """
        SELECT COUNT(*)
        FROM FRESHCART.GOLD.FCT_ORDERS AS gold
        WHERE NOT EXISTS (
            SELECT 1
            FROM FRESHCART.DIM.CUSTOMERS_SCD2 AS customer
            WHERE customer.CUST_ID = gold.CUST_ID
              AND customer.EMAIL = gold.EMAIL
              AND gold.ORDER_DATE >= customer.EFFECTIVE_FROM
              AND (
                customer.EFFECTIVE_TO IS NULL
                OR gold.ORDER_DATE <= customer.EFFECTIVE_TO
              )
        )
    """,
    "gold_orders_have_fx_rates": """
        SELECT COUNT(*)
        FROM FRESHCART.GOLD.FCT_ORDERS
        WHERE FX_RATE IS NULL
    """,
    "gold_orders_have_geography": """
        SELECT COUNT(*)
        FROM FRESHCART.GOLD.FCT_ORDERS AS gold
        WHERE NOT EXISTS (
            SELECT 1
            FROM FRESHCART.REF.PINCODE_GEO AS geo
            WHERE geo.CITY = gold.CITY
              AND geo.STATE = gold.STATE
              AND geo.REGION = gold.REGION
        )
    """,
}


METRICS_SQL = """
SELECT
  (SELECT COUNT(*) FROM FRESHCART.BRONZE.ORDERS_RAW) AS BRONZE_ORDERS,
  (SELECT COUNT(*) FROM FRESHCART.SILVER.ORDERS_CLEAN) AS CLEAN_ORDERS,
  (SELECT COUNT(*) FROM FRESHCART.SILVER.ORDERS_DEDUP) AS DEDUP_ORDERS,
  (SELECT COUNT(*) FROM FRESHCART.GOLD.FCT_ORDERS) AS GOLD_ORDERS,
  (SELECT COUNT(*) FROM FRESHCART.QUARANTINE.ORDERS) AS QUARANTINE_ORDERS,
  (SELECT COUNT_IF(NOT IS_VALID)
   FROM FRESHCART.SILVER.ORDERS_CHECKED) AS REJECTED_ORDERS,
  (SELECT COUNT(*) FROM FRESHCART.SILVER.ORDERS_CHECKED) AS CHECKED_ORDERS,
  (SELECT COALESCE(SUM(AMOUNT_INR), 0)
   FROM FRESHCART.GOLD.FCT_ORDERS) AS GOLD_REVENUE,
  (SELECT COALESCE(SUM(LOYALTY_POINTS), 0)
   FROM FRESHCART.GOLD.FCT_ORDERS) AS LOYALTY_POINTS
"""


def _scalar(connection, sql: str):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchone()
        if result is None:
            raise RuntimeError("Quality-check query returned no result")
        return result[0]
    finally:
        cursor.close()


def quality_checks(connection) -> dict[str, object]:
    """Run all five blocking checks, print metrics, and return the report."""
    checks = {
        name: int(_scalar(connection, sql) or 0)
        for name, sql in QUALITY_CHECKS.items()
    }

    cursor = connection.cursor()
    try:
        cursor.execute(METRICS_SQL)
        metrics_row = cursor.fetchone()
        if metrics_row is None:
            raise RuntimeError("Metrics query returned no result")
    finally:
        cursor.close()

    (
        bronze_orders,
        clean_orders,
        dedup_orders,
        gold_orders,
        quarantine_orders,
        rejected_orders,
        checked_orders,
        gold_revenue,
        loyalty_points,
    ) = metrics_row
    rejection_percentage = (
        round(100.0 * rejected_orders / checked_orders, 2)
        if checked_orders
        else 0.0
    )
    metrics = {
        "bronze_orders": bronze_orders,
        "clean_orders": clean_orders,
        "deduplicated_orders": dedup_orders,
        "gold_orders": gold_orders,
        "quarantine_orders": quarantine_orders,
        "rejection_percentage": rejection_percentage,
        "gold_revenue": gold_revenue,
        "loyalty_points": loyalty_points,
    }

    print("\nQuality checks:")
    for name, failures in checks.items():
        print(f"  {name}: {failures} failure(s)")
    print("\nPipeline metrics:")
    for name, value in metrics.items():
        print(f"  {name}: {value}")

    failed_checks = {
        name: count for name, count in checks.items() if count != 0
    }
    if failed_checks:
        raise RuntimeError(f"Pipeline quality checks failed: {failed_checks}")

    return {"checks": checks, "metrics": metrics}