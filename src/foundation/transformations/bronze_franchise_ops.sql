CREATE OR REFRESH MATERIALIZED VIEW bronze_franchise_ops
COMMENT "Raw franchise operations persisted from samples.bakehouse.sales_franchises"
AS
SELECT
  CAST(franchiseID AS STRING) AS raw_store_id,
  name AS store_name,
  size AS operational_size,
  district,
  country,
  CAST(supplierID AS STRING) AS primary_supplier_id,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  current_timestamp() AS _ingested_at,
  'samples.bakehouse.sales_franchises' AS _source_table
FROM samples.bakehouse.sales_franchises;
