CREATE OR REFRESH MATERIALIZED VIEW silver_test_foundation_quality
COMMENT "Foundation data quality tests embedded in the foundation pipeline"
AS
WITH tests AS (
  SELECT
    'duplicate_transaction_ids' AS test_name,
    COUNT(*) AS failed_records
  FROM (
    SELECT transaction_id
    FROM projects.bakehouse_analytics.silver_pos_transactions
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
  )

  UNION ALL

  SELECT
    'invalid_customer_fk_in_transactions' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.silver_pos_transactions t
  LEFT JOIN projects.bakehouse_analytics.silver_customer_profiles c
    ON t.customer_id = c.customer_id
  WHERE c.customer_id IS NULL

  UNION ALL

  SELECT
    'invalid_store_fk_in_transactions' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.silver_pos_transactions t
  LEFT JOIN projects.bakehouse_analytics.silver_franchise_ops f
    ON t.store_id = f.store_id
  WHERE f.store_id IS NULL

  UNION ALL

  SELECT
    'unapproved_suppliers_in_silver_supply' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.silver_supply_chain
  WHERE COALESCE(is_approved, false) <> true

  UNION ALL

  SELECT
    'approved_suppliers_in_quarantine' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.silver_supply_chain_quarantine
  WHERE upper(trim(approval_status)) = 'Y'

  UNION ALL

  SELECT
    'unmasked_email_patterns' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.silver_customer_profiles
  WHERE masked_email NOT LIKE '_***%@%'

  UNION ALL

  SELECT
    'unmasked_phone_patterns' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.silver_customer_profiles
  WHERE masked_phone NOT LIKE '%****%'

  UNION ALL

  SELECT
    'reviews_without_chunks' AS test_name,
    COUNT(*) AS failed_records
  FROM projects.bakehouse_analytics.silver_customer_reviews
  WHERE chunked_review_text IS NULL OR size(chunked_review_text) = 0
)
SELECT
  test_name,
  failed_records,
  CASE WHEN failed_records = 0 THEN true ELSE false END AS test_passed,
  current_timestamp() AS validated_at
FROM tests;
