CREATE OR REFRESH MATERIALIZED VIEW silver_franchise_ops
COMMENT "Validated franchise operations with normalized country naming"
AS
SELECT
  raw_store_id AS store_id,
  store_name,
  operational_size,
  district,
  CASE
    WHEN upper(trim(country)) IN ('US', 'USA') THEN 'United States'
    ELSE initcap(trim(country))
  END AS normalized_country,
  primary_supplier_id,
  latitude,
  longitude
FROM bronze_franchise_ops;
