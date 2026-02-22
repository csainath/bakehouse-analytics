CREATE OR REFRESH MATERIALIZED VIEW bronze_customer_profiles
COMMENT "Raw customer records persisted from samples.bakehouse.sales_customers"
AS
SELECT
  CAST(customerID AS STRING) AS raw_customer_id,
  first_name,
  last_name,
  email_address AS email,
  phone_number AS phone,
  address AS physical_address,
  city,
  state,
  country,
  continent,
  CAST(postal_zip_code AS STRING) AS zip_code,
  gender,
  current_timestamp() AS _ingested_at,
  'samples.bakehouse.sales_customers' AS _source_table
FROM samples.bakehouse.sales_customers;
