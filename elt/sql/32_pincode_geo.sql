MERGE INTO FRESHCART.REF.PINCODE_GEO AS target
USING (
  WITH locations AS (
    SELECT
      COALESCE(
        TRIM(br.RAW:pincode::STRING),
        TRIM(post_office.VALUE:Pincode::STRING)
      ) AS PINCODE,
      COALESCE(
        TRIM(post_office.VALUE:Name::STRING),
        TRIM(post_office.VALUE:District::STRING)
      ) AS CITY,
      TRIM(post_office.VALUE:State::STRING) AS STATE,
      COALESCE(
        br.EXTRACTED_AT::TIMESTAMP_NTZ,
        br.LOADED_AT::TIMESTAMP_NTZ,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
      ) AS UPDATED_AT
    FROM FRESHCART.BRONZE.PINCODE_RAW AS br,
      LATERAL FLATTEN(
        INPUT => br.RAW:response[0]:PostOffice
      ) AS post_office
    WHERE LOWER(br.RAW:response[0]:Status::STRING) = 'success'
  ),
  enriched AS (
    SELECT
      locations.PINCODE,
      locations.CITY,
      locations.STATE,
      region.REGION,
      locations.UPDATED_AT
    FROM locations
    LEFT JOIN FRESHCART.REF.STATE_REGION AS region
      ON UPPER(TRIM(region.STATE)) = UPPER(TRIM(locations.STATE))
    WHERE REGEXP_LIKE(locations.PINCODE, '^[0-9]{6}$')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY locations.PINCODE
      ORDER BY locations.UPDATED_AT DESC, locations.CITY, locations.STATE
    ) = 1
  )
  SELECT PINCODE, CITY, STATE, REGION, UPDATED_AT
  FROM enriched
) AS source
ON target.PINCODE = source.PINCODE
WHEN MATCHED AND (
  NVL(target.CITY, '') <> NVL(source.CITY, '')
  OR NVL(target.STATE, '') <> NVL(source.STATE, '')
  OR NVL(target.REGION, '') <> NVL(source.REGION, '')
) THEN UPDATE SET
  CITY = source.CITY,
  STATE = source.STATE,
  REGION = source.REGION,
  UPDATED_AT = source.UPDATED_AT
WHEN NOT MATCHED THEN INSERT (
  PINCODE,
  CITY,
  STATE,
  REGION,
  UPDATED_AT
) VALUES (
  source.PINCODE,
  source.CITY,
  source.STATE,
  source.REGION,
  source.UPDATED_AT
);