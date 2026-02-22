CREATE OR REFRESH MATERIALIZED VIEW bronze_customer_reviews
COMMENT "Raw customer review text persisted from samples.bakehouse.media_customer_reviews"
AS
SELECT
  CAST(new_id AS STRING) AS raw_review_id,
  review AS review_text,
  CAST(review_date AS DATE) AS submission_date,
  CAST(franchiseID AS STRING) AS franchise_id,
  current_timestamp() AS _ingested_at,
  'samples.bakehouse.media_customer_reviews' AS _source_table
FROM samples.bakehouse.media_customer_reviews;
