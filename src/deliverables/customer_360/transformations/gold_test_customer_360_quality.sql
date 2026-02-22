CREATE OR REFRESH MATERIALIZED VIEW gold_test_customer_360_quality
COMMENT "Customer 360 quality tests embedded in deliverable pipeline"
AS
WITH expected_spend AS (
  SELECT
    customer_id,
    ROUND(SUM(total_price), 2) AS total_lifetime_spend
  FROM projects.bakehouse_analytics.silver_pos_transactions
  GROUP BY customer_id
),
spend_mismatch AS (
  SELECT
    COALESCE(e.customer_id, g.customer_id) AS customer_id
  FROM expected_spend e
  FULL OUTER JOIN projects.bakehouse_analytics.gold_customer_360 g
    ON e.customer_id = g.customer_id
  WHERE
    e.customer_id IS NULL
    OR g.customer_id IS NULL
    OR e.total_lifetime_spend <> g.total_lifetime_spend
),
tests AS (
  SELECT
    'duplicate_customer_rows' AS test_name,
    COUNT(*) AS failed_records
  FROM (
    SELECT customer_id
    FROM projects.bakehouse_analytics.gold_customer_360
    GROUP BY customer_id
    HAVING COUNT(*) > 1
  )

  UNION ALL

  SELECT
    'null_customer_id' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_customer_360
  WHERE customer_id IS NULL

  UNION ALL

  SELECT
    'spend_mismatch_vs_silver' AS test_name,
    COUNT(*) AS failed_records
  FROM spend_mismatch

  UNION ALL

  SELECT
    'null_preference_fields_for_transacting_customers' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_customer_360
  WHERE preferred_store_location IS NULL OR most_frequent_product IS NULL
)
SELECT
  test_name,
  failed_records,
  CASE WHEN failed_records = 0 THEN true ELSE false END AS test_passed,
  current_timestamp() AS validated_at
FROM tests;
