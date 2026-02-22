CREATE OR REFRESH MATERIALIZED VIEW gold_test_supplier_risk_quality
COMMENT "Supplier risk quality tests embedded in deliverable pipeline"
AS
WITH missing_suppliers AS (
  SELECT s.supplier_id
  FROM projects.bakehouse_analytics.silver_supply_chain s
  LEFT JOIN projects.bakehouse_analytics.gold_supplier_risk_matrix g
    ON s.supplier_id = g.supplier_id
  WHERE g.supplier_id IS NULL
),
tests AS (
  SELECT
    'missing_suppliers_in_risk_matrix' AS test_name,
    COUNT(*) AS failed_records
  FROM missing_suppliers

  UNION ALL

  SELECT
    'criticality_out_of_range' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_supplier_risk_matrix
  WHERE ingredient_criticality_score NOT IN (2, 3, 5)

  UNION ALL

  SELECT
    'negative_distance' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_supplier_risk_matrix
  WHERE avg_geographic_distance < 0

  UNION ALL

  SELECT
    'null_primary_fields' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_supplier_risk_matrix
  WHERE supplier_id IS NULL OR is_high_risk_flag IS NULL
)
SELECT
  test_name,
  failed_records,
  CASE WHEN failed_records = 0 THEN true ELSE false END AS test_passed,
  current_timestamp() AS validated_at
FROM tests;
