CREATE OR REFRESH MATERIALIZED VIEW bronze_supply_chain
COMMENT "Raw supplier records persisted from samples.bakehouse.sales_suppliers"
AS
SELECT
  CAST(supplierID AS STRING) AS raw_supplier_id,
  name AS supplier_name,
  ingredient AS provided_ingredients,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  approved AS approval_status,
  continent,
  city,
  district,
  size,
  current_timestamp() AS _ingested_at,
  'samples.bakehouse.sales_suppliers' AS _source_table
FROM samples.bakehouse.sales_suppliers;
