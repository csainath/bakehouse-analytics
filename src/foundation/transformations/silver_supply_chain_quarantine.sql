CREATE OR REFRESH MATERIALIZED VIEW silver_supply_chain_quarantine
COMMENT "Supplier records filtered out due to non-approved compliance status"
AS
SELECT
  raw_supplier_id AS supplier_id,
  supplier_name,
  provided_ingredients,
  latitude,
  longitude,
  approval_status,
  continent,
  city,
  district,
  size
FROM bronze_supply_chain
WHERE upper(trim(approval_status)) <> 'Y';
