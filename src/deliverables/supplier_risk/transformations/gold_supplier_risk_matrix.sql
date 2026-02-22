CREATE OR REFRESH MATERIALIZED VIEW gold_supplier_risk_matrix
COMMENT "Supply chain risk model based on dependency concentration and distance"
AS
WITH franchise_dependencies AS (
  SELECT
    s.supplier_id,
    COUNT(DISTINCT f.store_id) AS dependent_franchise_count,
    AVG(
      2 * 6371 * ASIN(
        SQRT(
          POWER(SIN(RADIANS(f.latitude - s.latitude) / 2), 2) +
          COS(RADIANS(s.latitude)) * COS(RADIANS(f.latitude)) *
          POWER(SIN(RADIANS(f.longitude - s.longitude) / 2), 2)
        )
      )
    ) AS avg_geographic_distance
  FROM projects.bakehouse_analytics.silver_supply_chain s
  LEFT JOIN projects.bakehouse_analytics.silver_franchise_ops f
    ON s.supplier_id = f.primary_supplier_id
  GROUP BY s.supplier_id
),
criticality AS (
  SELECT
    supplier_id,
    CASE
      WHEN lower(provided_ingredients) IN ('cacao', 'cocoa butter', 'cane sugar', 'vanilla', 'oats') THEN 5
      WHEN lower(provided_ingredients) IN ('almonds', 'hazelnuts', 'pecans', 'pistachios', 'coffee') THEN 3
      ELSE 2
    END AS ingredient_criticality_score
  FROM projects.bakehouse_analytics.silver_supply_chain
)
SELECT
  s.supplier_id,
  COALESCE(fd.dependent_franchise_count, 0) AS dependent_franchise_count,
  COALESCE(fd.avg_geographic_distance, 0.0) AS avg_geographic_distance,
  c.ingredient_criticality_score,
  CASE
    WHEN COALESCE(fd.dependent_franchise_count, 0) >= 3
      AND (
        COALESCE(fd.avg_geographic_distance, 0.0) >= 5000
        OR c.ingredient_criticality_score >= 5
      ) THEN true
    WHEN COALESCE(fd.dependent_franchise_count, 0) >= 1
      AND c.ingredient_criticality_score >= 5 THEN true
    ELSE false
  END AS is_high_risk_flag
FROM projects.bakehouse_analytics.silver_supply_chain s
LEFT JOIN franchise_dependencies fd
  ON s.supplier_id = fd.supplier_id
LEFT JOIN criticality c
  ON s.supplier_id = c.supplier_id;
