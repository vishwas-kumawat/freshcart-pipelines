MERGE INTO FRESHCART.GOLD.FCT_ORDERS AS target
USING (
  WITH fx_matches AS (
    SELECT
      orders.ORDER_ID,
      rates.RATE_TO_INR,
      ROW_NUMBER() OVER (
        PARTITION BY orders.ORDER_ID
        ORDER BY rates.RATE_DATE DESC
      ) AS RATE_RANK
    FROM FRESHCART.SILVER.ORDERS_DEDUP AS orders
    LEFT JOIN FRESHCART.SILVER.FX_RATES AS rates
      ON rates.CURRENCY = orders.CURRENCY
      AND rates.RATE_DATE <= orders.ORDER_DATE
  ),
  enriched AS (
    SELECT
      orders.ORDER_ID,
      orders.ORDER_DATE,
      customer.CUST_ID,
      orders.EMAIL,
      customer.TIER,
      multiplier.MULTIPLIER,
      orders.AMOUNT,
      orders.CURRENCY,
      fx.RATE_TO_INR AS FX_RATE,
      ROUND(orders.AMOUNT * fx.RATE_TO_INR, 2) AS AMOUNT_INR,
      FLOOR(
        (orders.AMOUNT * fx.RATE_TO_INR) / 100
      ) * multiplier.MULTIPLIER AS LOYALTY_POINTS,
      (orders.AMOUNT * fx.RATE_TO_INR) >= 5000 AS IS_HIGH_VALUE,
      geo.CITY,
      geo.STATE,
      geo.REGION,
      orders.UPDATED_AT
    FROM FRESHCART.SILVER.ORDERS_DEDUP AS orders
    LEFT JOIN FRESHCART.DIM.CUSTOMERS_SCD2 AS customer
      ON customer.EMAIL = orders.EMAIL
      AND orders.ORDER_DATE >= customer.EFFECTIVE_FROM
      AND (
        customer.EFFECTIVE_TO IS NULL
        OR orders.ORDER_DATE <= customer.EFFECTIVE_TO
      )
    LEFT JOIN FRESHCART.REF.TIER_MULTIPLIER AS multiplier
      ON UPPER(multiplier.TIER) = UPPER(customer.TIER)
    LEFT JOIN fx_matches AS fx
      ON fx.ORDER_ID = orders.ORDER_ID
      AND fx.RATE_RANK = 1
    LEFT JOIN FRESHCART.REF.PINCODE_GEO AS geo
      ON geo.PINCODE = orders.PINCODE
  )
  SELECT
    enriched.*,
    HASH(
      ORDER_ID,
      ORDER_DATE,
      CUST_ID,
      EMAIL,
      TIER,
      MULTIPLIER,
      AMOUNT,
      CURRENCY,
      FX_RATE,
      AMOUNT_INR,
      LOYALTY_POINTS,
      IS_HIGH_VALUE,
      CITY,
      STATE,
      REGION,
      UPDATED_AT
    ) AS ROW_HASH
  FROM enriched
) AS source
ON target.ORDER_ID = source.ORDER_ID
WHEN MATCHED AND target.ROW_HASH <> source.ROW_HASH THEN UPDATE SET
  ORDER_DATE = source.ORDER_DATE,
  CUST_ID = source.CUST_ID,
  EMAIL = source.EMAIL,
  TIER = source.TIER,
  MULTIPLIER = source.MULTIPLIER,
  AMOUNT = source.AMOUNT,
  CURRENCY = source.CURRENCY,
  FX_RATE = source.FX_RATE,
  AMOUNT_INR = source.AMOUNT_INR,
  LOYALTY_POINTS = source.LOYALTY_POINTS,
  IS_HIGH_VALUE = source.IS_HIGH_VALUE,
  CITY = source.CITY,
  STATE = source.STATE,
  REGION = source.REGION,
  UPDATED_AT = source.UPDATED_AT,
  ROW_HASH = source.ROW_HASH,
  PUBLISHED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  ORDER_ID,
  ORDER_DATE,
  CUST_ID,
  EMAIL,
  TIER,
  MULTIPLIER,
  AMOUNT,
  CURRENCY,
  FX_RATE,
  AMOUNT_INR,
  LOYALTY_POINTS,
  IS_HIGH_VALUE,
  CITY,
  STATE,
  REGION,
  UPDATED_AT,
  ROW_HASH,
  PUBLISHED_AT
) VALUES (
  source.ORDER_ID,
  source.ORDER_DATE,
  source.CUST_ID,
  source.EMAIL,
  source.TIER,
  source.MULTIPLIER,
  source.AMOUNT,
  source.CURRENCY,
  source.FX_RATE,
  source.AMOUNT_INR,
  source.LOYALTY_POINTS,
  source.IS_HIGH_VALUE,
  source.CITY,
  source.STATE,
  source.REGION,
  source.UPDATED_AT,
  source.ROW_HASH,
  CURRENT_TIMESTAMP()
);