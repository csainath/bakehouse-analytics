CREATE OR REFRESH MATERIALIZED VIEW gold_test_sentiment_trends_quality
COMMENT "Sentiment trend quality tests embedded in deliverable pipeline"
AS
WITH tests AS (
  SELECT
    'sentiment_score_out_of_bounds' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_store_sentiment_trends
  WHERE customer_satisfaction_score < -1 OR customer_satisfaction_score > 1

  UNION ALL

  SELECT
    'invalid_reporting_period_span' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_store_sentiment_trends
  WHERE datediff(reporting_period_end, reporting_period_start) <> 6

  UNION ALL

  SELECT
    'null_store_or_period' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.gold_store_sentiment_trends
  WHERE store_id IS NULL
     OR reporting_period_start IS NULL
     OR reporting_period_end IS NULL
)
SELECT
  test_name,
  failed_records,
  CASE WHEN failed_records = 0 THEN true ELSE false END AS test_passed,
  current_timestamp() AS validated_at
FROM tests;
