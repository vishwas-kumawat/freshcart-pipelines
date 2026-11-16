CREATE TABLE IF NOT EXISTS FRESHCART.SILVER.FX_RATES (
  CURRENCY STRING,
  RATE_DATE DATE,
  RATE_TO_INR NUMBER(18,6),
  SOURCE STRING,
  RUN_ID STRING,
  INGEST_SEQ NUMBER
);

MERGE INTO FRESHCART.SILVER.FX_RATES AS target
USING (
  WITH observed AS (
    SELECT
      UPPER(TRIM(br.RAW:base::STRING)) AS CURRENCY,
      TRY_TO_DATE(rates.KEY::STRING) AS RATE_DATE,
      TRY_TO_DECIMAL(rates.VALUE:INR::STRING, 18, 6) AS RATE_TO_INR,
      br.SOURCE,
      br.RUN_ID,
      br.INGEST_SEQ
    FROM FRESHCART.BRONZE.FX_RAW AS br,
      LATERAL FLATTEN(INPUT => br.RAW:rates) AS rates
    WHERE rates.VALUE:INR IS NOT NULL
      AND TRY_TO_DATE(rates.KEY::STRING) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        UPPER(TRIM(br.RAW:base::STRING)),
        TRY_TO_DATE(rates.KEY::STRING)
      ORDER BY br.INGEST_SEQ DESC
    ) = 1
  ),
  bounds AS (
    SELECT
      MIN(RATE_DATE) AS START_DATE,
      GREATEST(
        MAX(RATE_DATE),
        (SELECT MAX(ORDER_DATE) FROM FRESHCART.SILVER.ORDERS_DEDUP)
      ) AS END_DATE
    FROM observed
  ),
  currencies AS (
    SELECT DISTINCT CURRENCY
    FROM (
      SELECT CURRENCY FROM observed
      UNION ALL
      SELECT 'INR'
    )
  ),
  numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS DAY_OFFSET
    FROM TABLE(GENERATOR(ROWCOUNT => 36600))
  ),
  calendar AS (
    SELECT
      DATEADD(day, numbers.DAY_OFFSET, bounds.START_DATE)::DATE AS RATE_DATE
    FROM numbers
    CROSS JOIN bounds
    WHERE numbers.DAY_OFFSET
      <= DATEDIFF(day, bounds.START_DATE, bounds.END_DATE)
  ),
  dense_rates AS (
    SELECT
      currencies.CURRENCY,
      calendar.RATE_DATE,
      IFF(currencies.CURRENCY = 'INR', 1, observed.RATE_TO_INR)
        AS RATE_TO_INR,
      observed.SOURCE,
      observed.RUN_ID,
      observed.INGEST_SEQ
    FROM currencies
    CROSS JOIN calendar
    LEFT JOIN observed
      ON observed.CURRENCY = currencies.CURRENCY
      AND observed.RATE_DATE = calendar.RATE_DATE
  ),
  carried_rates AS (
    SELECT
      CURRENCY,
      RATE_DATE,
      LAST_VALUE(RATE_TO_INR) IGNORE NULLS OVER (
        PARTITION BY CURRENCY
        ORDER BY RATE_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS RATE_TO_INR,
      SOURCE,
      RUN_ID,
      INGEST_SEQ
    FROM dense_rates
  )
  SELECT
    CURRENCY,
    RATE_DATE,
    RATE_TO_INR,
    SOURCE,
    RUN_ID,
    INGEST_SEQ
  FROM carried_rates
  WHERE RATE_TO_INR IS NOT NULL
) AS source
ON target.CURRENCY = source.CURRENCY
  AND target.RATE_DATE = source.RATE_DATE
WHEN MATCHED THEN UPDATE SET
  RATE_TO_INR = source.RATE_TO_INR,
  SOURCE = source.SOURCE,
  RUN_ID = source.RUN_ID,
  INGEST_SEQ = source.INGEST_SEQ
WHEN NOT MATCHED THEN INSERT (
  CURRENCY,
  RATE_DATE,
  RATE_TO_INR,
  SOURCE,
  RUN_ID,
  INGEST_SEQ
) VALUES (
  source.CURRENCY,
  source.RATE_DATE,
  source.RATE_TO_INR,
  source.SOURCE,
  source.RUN_ID,
  source.INGEST_SEQ
);