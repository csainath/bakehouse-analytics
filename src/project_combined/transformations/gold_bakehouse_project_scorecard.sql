CREATE OR REFRESH MATERIALIZED VIEW gold_bakehouse_project_scorecard
COMMENT "Project-level combined output joining all deliverable gold assets"
AS
WITH customer_rollup AS (
  SELECT
    preferred_store_location AS store_name,
    COUNT(*) AS active_customer_count,
    AVG(total_lifetime_spend) AS avg_lifetime_spend
  FROM projects.bakehouse_analytics.gold_customer_360
  GROUP BY preferred_store_location
),
risk_by_store AS (
  SELECT
    f.store_id,
    MAX(CASE WHEN r.is_high_risk_flag THEN 1 ELSE 0 END) AS has_high_risk_supplier
  FROM projects.bakehouse_analytics.silver_franchise_ops f
  LEFT JOIN projects.bakehouse_analytics.gold_supplier_risk_matrix r
    ON f.primary_supplier_id = r.supplier_id
  GROUP BY f.store_id
)
SELECT
  d.store_id,
  f.store_name,
  d.reporting_date,
  d.total_daily_revenue,
  d.total_items_sold,
  d.avg_transaction_value,
  COALESCE(s.customer_satisfaction_score, 0.0) AS weekly_sentiment_score,
  COALESCE(c.active_customer_count, 0) AS active_customer_count,
  ROUND(COALESCE(c.avg_lifetime_spend, 0.0), 2) AS avg_lifetime_spend,
  CASE WHEN COALESCE(r.has_high_risk_supplier, 0) = 1 THEN true ELSE false END AS supply_risk_alert
FROM projects.bakehouse_analytics.gold_daily_franchise_health d
LEFT JOIN projects.bakehouse_analytics.silver_franchise_ops f
  ON d.store_id = f.store_id
LEFT JOIN projects.bakehouse_analytics.gold_store_sentiment_trends s
  ON d.store_id = s.store_id
 AND d.reporting_date BETWEEN s.reporting_period_start AND s.reporting_period_end
LEFT JOIN customer_rollup c
  ON f.store_name = c.store_name
LEFT JOIN risk_by_store r
  ON d.store_id = r.store_id;
