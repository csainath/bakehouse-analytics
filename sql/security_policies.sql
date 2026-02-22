CREATE OR REPLACE FUNCTION projects.bakehouse_analytics.mask_pii(value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('admins') THEN value
  WHEN value IS NULL THEN NULL
  ELSE concat(substr(value, 1, 2), '***')
END;

-- Apply after bronze tables exist.
-- ALTER TABLE projects.bakehouse_analytics.bronze_customer_profiles
-- ALTER COLUMN email SET MASK projects.bakehouse_analytics.mask_pii;
-- ALTER TABLE projects.bakehouse_analytics.bronze_customer_profiles
-- ALTER COLUMN phone SET MASK projects.bakehouse_analytics.mask_pii;
-- ALTER TABLE projects.bakehouse_analytics.bronze_customer_profiles
-- ALTER COLUMN physical_address SET MASK projects.bakehouse_analytics.mask_pii;
