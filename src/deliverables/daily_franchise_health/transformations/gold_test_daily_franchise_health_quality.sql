CREATE OR REFRESH MATERIALIZED VIEW gold_test_daily_franchise_health_quality
COMMENT "Daily franchise health quality tests embedded in deliverable pipeline"
AS
WITH expected AS (
  SELECT
    store_id,
    CAST(date_trunc('DAY', transaction_timestamp) AS DATE) AS reporting_date,
    ROUND(SUM(total_price), 2) AS total_daily_revenue,
    SUM(quantity) AS total_items_sold,
    ROUND(AVG(total_price), 2) AS avg_transaction_value
  FROM projects.bakehouse_analytics.silver_pos_transactions
  GROUP BY store_id, CAST(date_trunc('DAY', transaction_timestamp) AS DATE)
),
actual AS (
  SELECT *
  FROM projects.bakehouse_analytics.gold_daily_franchise_health
),
mismatch AS (
  SELECT
    COALESCE(e.store_id, a.store_id) AS store_id,
    COALESCE(e.reporting_date, a.reporting_date) AS reporting_date,
    e.total_daily_revenue AS expected_total_daily_revenue,
    a.total_daily_revenue AS actual_total_daily_revenue,
    e.total_items_sold AS expected_total_items_sold,
    a.total_items_sold AS actual_total_items_sold,
    e.avg_transaction_value AS expected_avg_transaction_value,
    a.avg_transaction_value AS actual_avg_transaction_value
  FROM expected e
  FULL OUTER JOIN actual a
    ON e.store_id = a.store_id
   AND e.reporting_date = a.reporting_date
  WHERE
    e.store_id IS NULL
    OR a.store_id IS NULL
    OR e.total_daily_revenue <> a.total_daily_revenue
    OR e.total_items_sold <> a.total_items_sold
    OR e.avg_transaction_value <> a.avg_transaction_value
),
tests AS (
  SELECT
    'aggregation_mismatch_vs_silver' AS test_name,
    COUNT(*) AS failed_records
  FROM mismatch

  UNION ALL

  SELECT
    'negative_metrics' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_daily_franchise_health
  WHERE total_daily_revenue < 0
     OR total_items_sold < 0
     OR avg_transaction_value < 0

  UNION ALL

  SELECT
    'null_store_or_date' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_daily_franchise_health
  WHERE store_id IS NULL OR reporting_date IS NULL
)
SELECT
  test_name,
  failed_records,
  CASE WHEN failed_records = 0 THEN true ELSE false END AS test_passed,
  current_timestamp() AS validated_at
FROM tests;
