CREATE OR REFRESH MATERIALIZED VIEW silver_customer_profiles
COMMENT "Cleansed customer profiles with masked PII and normalized geography"
AS
SELECT
  raw_customer_id AS customer_id,
  first_name,
  last_name,
  regexp_replace(lower(email), '(^.).*(@.*$)', '$1***$2') AS masked_email,
  regexp_replace(phone, '(\\d{0,3})\\d+(\\d{2})$', '$1****$2') AS masked_phone,
  concat(substr(physical_address, 1, 6), '***') AS masked_address,
  city,
  state,
  CASE
    WHEN upper(trim(country)) IN ('US', 'USA') THEN 'United States'
    ELSE initcap(trim(country))
  END AS normalized_country,
  CASE
    WHEN lower(trim(continent)) IN ('north america', 'northamerica') THEN 'North America'
    WHEN lower(trim(continent)) = 'south america' THEN 'South America'
    WHEN lower(trim(continent)) = 'oceania' THEN 'Oceania'
    WHEN lower(trim(continent)) = 'asia' THEN 'Asia'
    WHEN lower(trim(continent)) = 'europe' THEN 'Europe'
    WHEN lower(trim(continent)) = 'africa' THEN 'Africa'
    ELSE initcap(trim(continent))
  END AS normalized_continent,
  zip_code,
  gender
FROM bronze_customer_profiles;
