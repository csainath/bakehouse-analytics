CREATE OR REFRESH MATERIALIZED VIEW gold_customer_360
COMMENT "Customer 360 summary with spend, preferred store, and preferred product"
AS
WITH spend AS (
  SELECT
    t.customer_id,
    ROUND(SUM(t.total_price), 2) AS total_lifetime_spend
  FROM projects.bakehouse_analytics.silver_pos_transactions t
  GROUP BY t.customer_id
),
store_counts AS (
  SELECT
    t.customer_id,
    f.store_name AS preferred_store_location,
    COUNT(*) AS txn_count
  FROM projects.bakehouse_analytics.silver_pos_transactions t
  INNER JOIN projects.bakehouse_analytics.silver_franchise_ops f
    ON t.store_id = f.store_id
  GROUP BY t.customer_id, f.store_name
),
store_pref AS (
  SELECT
    customer_id,
    preferred_store_location,
    row_number() OVER (
      PARTITION BY customer_id
      ORDER BY txn_count DESC, preferred_store_location
    ) AS rn
  FROM store_counts
),
product_counts AS (
  SELECT
    customer_id,
    product_sold AS most_frequent_product,
    COUNT(*) AS product_count
  FROM projects.bakehouse_analytics.silver_pos_transactions
  GROUP BY customer_id, product_sold
),
product_pref AS (
  SELECT
    customer_id,
    most_frequent_product,
    row_number() OVER (
      PARTITION BY customer_id
      ORDER BY product_count DESC, most_frequent_product
    ) AS rn
  FROM product_counts
)
SELECT
  s.customer_id,
  s.total_lifetime_spend,
  sp.preferred_store_location,
  pp.most_frequent_product
FROM spend s
LEFT JOIN store_pref sp
  ON s.customer_id = sp.customer_id
 AND sp.rn = 1
LEFT JOIN product_pref pp
  ON s.customer_id = pp.customer_id
 AND pp.rn = 1;
