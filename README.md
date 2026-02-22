# Bakehouse Analytics

Databricks Lakehouse project implementing the Bakehouse technical specification with medallion data pipelines and governed analytics tables.

## Scope
- Base source data: `samples.bakehouse`
- Target catalog/schema: `projects.bakehouse_analytics`
- Table naming convention: medallion prefixes (`bronze_*`, `silver_*`, `gold_*`)
- Pipeline strategy: separate Spark Declarative Pipelines for each deliverable + one project-combined pipeline

## Repository Layout
- `databricks.yml`: bundle configuration
- `resources/*.pipeline.yml`: six pipeline definitions
- `src/foundation/transformations/*.sql`: bronze and silver logic
- `src/deliverables/*/transformations/*.sql`: individual gold deliverables
- `src/project_combined/transformations/*.sql`: combined project output
- `sql/bootstrap_uc.sql`: catalog/schema bootstrap
- `sql/security_policies.sql`: PII masking function scaffold

## Pipelines
1. `bakehouse_foundation_pipeline`
2. `bakehouse_daily_franchise_health_pipeline`
3. `bakehouse_customer_360_pipeline`
4. `bakehouse_supplier_risk_pipeline`
5. `bakehouse_sentiment_trends_pipeline`
6. `bakehouse_project_combined_pipeline`

## Deployment
```bash
databricks bundle validate

databricks bundle deploy -t dev

databricks bundle run bakehouse_foundation_pipeline -t dev
databricks bundle run bakehouse_daily_franchise_health_pipeline -t dev
databricks bundle run bakehouse_customer_360_pipeline -t dev
databricks bundle run bakehouse_supplier_risk_pipeline -t dev
databricks bundle run bakehouse_sentiment_trends_pipeline -t dev
databricks bundle run bakehouse_project_combined_pipeline -t dev
```
