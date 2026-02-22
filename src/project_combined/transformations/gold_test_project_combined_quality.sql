CREATE OR REFRESH MATERIALIZED VIEW gold_test_project_combined_quality
COMMENT "Project combined quality tests embedded in project pipeline"
AS
WITH daily_count AS (
  SELECT COUNT(*) AS cnt
  FROM projects.bakehouse_analytics.gold_daily_franchise_health
),
scorecard_count AS (
  SELECT COUNT(*) AS cnt
  FROM projects.bakehouse_analytics.gold_bakehouse_project_scorecard
),
tests AS (
  SELECT
    'scorecard_row_count_matches_daily_health' AS test_name,
    ABS((SELECT cnt FROM daily_count) - (SELECT cnt FROM scorecard_count)) AS failed_records

  UNION ALL

  SELECT
    'null_primary_keys' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_bakehouse_project_scorecard
  WHERE store_id IS NULL OR reporting_date IS NULL

  UNION ALL

  SELECT
    'negative_core_metrics' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_bakehouse_project_scorecard
  WHERE total_daily_revenue < 0 OR total_items_sold < 0 OR avg_transaction_value < 0
)
SELECT
  test_name,
  failed_records,
  CASE WHEN failed_records = 0 THEN true ELSE false END AS test_passed,
  current_timestamp() AS validated_at
FROM tests;
