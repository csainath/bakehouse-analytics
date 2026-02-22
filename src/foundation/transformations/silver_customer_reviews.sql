CREATE OR REFRESH MATERIALIZED VIEW silver_customer_reviews
COMMENT "Cleansed review text split into analytical chunks"
AS
WITH cleansed AS (
  SELECT
    raw_review_id AS review_id,
    regexp_replace(lower(review_text), '[^a-z0-9\\s\\.!?]', ' ') AS clean_text,
    submission_date,
    franchise_id AS store_id
  FROM bronze_customer_reviews
),
chunked AS (
  SELECT
    review_id,
    filter(
      transform(
        split(regexp_replace(clean_text, '[\\.!?]+', '||'), '\\\\|\\\\|'),
        x -> trim(x)
      ),
      x -> length(x) > 0
    ) AS chunked_review_text,
    submission_date,
    store_id
  FROM cleansed
)
SELECT
  review_id,
  chunked_review_text,
  submission_date,
  store_id
FROM chunked;
