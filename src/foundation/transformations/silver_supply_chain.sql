CREATE OR REFRESH MATERIALIZED VIEW silver_supply_chain
COMMENT "Approved supplier records allowed for downstream analytics"
AS
SELECT
  raw_supplier_id AS supplier_id,
  supplier_name,
  provided_ingredients,
  latitude,
  longitude,
  true AS is_approved,
  CASE
    WHEN lower(trim(continent)) IN ('north america', 'northamerica') THEN 'North America'
    WHEN lower(trim(continent)) = 'south america' THEN 'South America'
    WHEN lower(trim(continent)) = 'oceania' THEN 'Oceania'
    WHEN lower(trim(continent)) = 'asia' THEN 'Asia'
    WHEN lower(trim(continent)) = 'europe' THEN 'Europe'
    WHEN lower(trim(continent)) = 'africa' THEN 'Africa'
    ELSE initcap(trim(continent))
  END AS normalized_continent,
  city,
  district,
  size
FROM bronze_supply_chain
WHERE upper(trim(approval_status)) = 'Y';
