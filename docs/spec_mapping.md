# Technical Spec Mapping

## Source Coverage (samples.bakehouse)
- Customer Records: `sales_customers`
- Franchise Operations: `sales_franchises`
- Supply Chain: `sales_suppliers`
- Point of Sale: `sales_transactions`
- Customer Feedback: `media_customer_reviews`

## Medallion Tables
- Bronze: `bronze_customer_profiles`, `bronze_franchise_ops`, `bronze_supply_chain`, `bronze_pos_transactions`, `bronze_customer_reviews`
- Silver: `silver_customer_profiles`, `silver_franchise_ops`, `silver_supply_chain`, `silver_supply_chain_quarantine`, `silver_pos_transactions`, `silver_customer_reviews`
- Gold: `gold_daily_franchise_health`, `gold_customer_360`, `gold_supplier_risk_matrix`, `gold_store_sentiment_trends`, `gold_bakehouse_project_scorecard`

## Deliverable Pipelines
- Foundation pipeline: Bronze + Silver layer
- Deliverable 1 pipeline: `gold_daily_franchise_health`
- Deliverable 2 pipeline: `gold_customer_360`
- Deliverable 3 pipeline: `gold_supplier_risk_matrix`
- Deliverable 4 pipeline: `gold_store_sentiment_trends`
- Project pipeline: `gold_bakehouse_project_scorecard` (combines all deliverables)

## Embedded Test Views
- `silver_test_foundation_quality`
- `gold_test_daily_franchise_health_quality`
- `gold_test_customer_360_quality`
- `gold_test_supplier_risk_quality`
- `gold_test_sentiment_trends_quality`
- `gold_test_project_combined_quality`
