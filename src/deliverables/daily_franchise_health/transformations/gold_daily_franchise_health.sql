CREATE OR REFRESH MATERIALIZED VIEW gold_daily_franchise_health
COMMENT "Daily store-level revenue and transaction health metrics"
AS
SELECT
  store_id,
  CAST(date_trunc('DAY', transaction_timestamp) AS DATE) AS reporting_date,
  ROUND(SUM(total_price), 2) AS total_daily_revenue,
  SUM(quantity) AS total_items_sold,
  ROUND(AVG(total_price), 2) AS avg_transaction_value
FROM projects.bakehouse_analytics.silver_pos_transactions
GROUP BY
  store_id,
  CAST(date_trunc('DAY', transaction_timestamp) AS DATE);
