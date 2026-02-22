CREATE OR REFRESH MATERIALIZED VIEW gold_store_sentiment_trends
COMMENT "Weekly franchise sentiment trend from cleansed review chunks"
AS
WITH exploded AS (
  SELECT
    store_id,
    submission_date,
    explode(chunked_review_text) AS review_chunk
  FROM projects.bakehouse_analytics.silver_customer_reviews
),
scored AS (
  SELECT
    store_id,
    submission_date,
    CASE
      WHEN review_chunk RLIKE '(great|excellent|love|amazing|good|friendly|fresh|delicious|perfect|best)' THEN 1.0
      WHEN review_chunk RLIKE '(bad|terrible|awful|slow|cold|stale|poor|worst|disappoint|rude)' THEN -1.0
      ELSE 0.0
    END AS sentiment_score
  FROM exploded
)
SELECT
  store_id,
  CAST(date_trunc('WEEK', submission_date) AS DATE) AS reporting_period_start,
  CAST(date_add(date_trunc('WEEK', submission_date), 6) AS DATE) AS reporting_period_end,
  ROUND(AVG(sentiment_score), 4) AS customer_satisfaction_score
FROM scored
GROUP BY
  store_id,
  CAST(date_trunc('WEEK', submission_date) AS DATE),
  CAST(date_add(date_trunc('WEEK', submission_date), 6) AS DATE);
