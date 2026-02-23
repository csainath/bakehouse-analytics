# Bakehouse Analytics Delivery Implementation Report

## 1. Request Summary

The requested scope was:

1. Create a new project repo named **Bakehouse Analytics**.
2. Use data in **Bakehouse** schema as base data.
3. Follow the attached **Technical Specification**.
4. Follow attached **Best Practices**.
5. Create code artifacts in new repo, sync to GitHub, and pull repo into Databricks workspace.
6. Create catalog **Projects** and schema **bakehouse_analytics**; create new tables with medallion prefixes.
7. Create separate pipelines for individual deliverables and one project-level pipeline that combines them.
8. Use Spark compute.
9. Request clarifications where needed.
10. Build test cases and embed them in pipelines.
11. Validate pipelines by sampling data.
12. Create a sales forecast machine learning model and improve it.

Clarifications provided by user:

- Catalog should be `projects`.
- Keep schema as `bakehouse_analytics`.
- No row-level security requested for now.

---

## 2. Repository and Workspace Delivery

### 2.1 GitHub Repository

- Created and populated repository:
  - `https://github.com/csainath/bakehouse-analytics`

### 2.2 Databricks Repos Integration

- Databricks repo path:
  - `/Repos/vssainath.c@gmail.com/bakehouse-analytics`
- Synced to latest implementation commit:
  - `343a39a1272db109dc8c567994e44f4a481834a9`

### 2.3 Core Project Configuration Files

- `databricks.yml`
- `README.md`
- `docs/spec_mapping.md`
- `docs/delivery_implementation_report.md` (this report)

---

## 3. Data Platform Provisioning

### 3.1 Catalog and Schema

Provisioned:

- Catalog: `projects`
- Schema: `projects.bakehouse_analytics`

Bootstrap SQL reference:

- `sql/bootstrap_uc.sql`

### 3.2 Source Data Baseline

All bronze ingestion logic is based on `samples.bakehouse` tables:

- `samples.bakehouse.sales_customers`
- `samples.bakehouse.sales_franchises`
- `samples.bakehouse.sales_suppliers`
- `samples.bakehouse.sales_transactions`
- `samples.bakehouse.media_customer_reviews`

Source references in code:

- `src/foundation/transformations/bronze_customer_profiles.sql`
- `src/foundation/transformations/bronze_franchise_ops.sql`
- `src/foundation/transformations/bronze_supply_chain.sql`
- `src/foundation/transformations/bronze_pos_transactions.sql`
- `src/foundation/transformations/bronze_customer_reviews.sql`

---

## 4. Medallion Architecture Implementation

Implemented with explicit table-name prefixes:

- Bronze: `bronze_*`
- Silver: `silver_*`
- Gold: `gold_*`

### 4.1 Bronze Layer (Raw Persistence)

Code:

- `src/foundation/transformations/bronze_customer_profiles.sql`
- `src/foundation/transformations/bronze_franchise_ops.sql`
- `src/foundation/transformations/bronze_supply_chain.sql`
- `src/foundation/transformations/bronze_pos_transactions.sql`
- `src/foundation/transformations/bronze_customer_reviews.sql`

### 4.2 Silver Layer (Quality, Standardization, Security Preparation)

Code:

- `src/foundation/transformations/silver_customer_profiles.sql`
- `src/foundation/transformations/silver_franchise_ops.sql`
- `src/foundation/transformations/silver_supply_chain.sql`
- `src/foundation/transformations/silver_supply_chain_quarantine.sql`
- `src/foundation/transformations/silver_pos_transactions.sql`
- `src/foundation/transformations/silver_customer_reviews.sql`

Examples of technical-spec alignment:

- PII masking in curated customer profile outputs:
  - `masked_email`, `masked_phone`, `masked_address`
  - `silver_customer_profiles.sql`
- Referential integrity and deduplication:
  - `silver_pos_transactions.sql`
- Supplier quarantine for non-approved records:
  - `silver_supply_chain_quarantine.sql`
- Text cleansing and chunking for review analytics:
  - `silver_customer_reviews.sql`

### 4.3 Gold Layer (Business Deliverables)

Code:

- Daily franchise health:
  - `src/deliverables/daily_franchise_health/transformations/gold_daily_franchise_health.sql`
- Customer 360:
  - `src/deliverables/customer_360/transformations/gold_customer_360.sql`
- Supplier risk matrix:
  - `src/deliverables/supplier_risk/transformations/gold_supplier_risk_matrix.sql`
- Store sentiment trends:
  - `src/deliverables/sentiment_trends/transformations/gold_store_sentiment_trends.sql`
- Combined project scorecard:
  - `src/project_combined/transformations/gold_bakehouse_project_scorecard.sql`

---

## 5. Pipeline Design and Deployment

### 5.1 Pipeline Resource Definitions

Code:

- `resources/foundation.pipeline.yml`
- `resources/deliverable_daily_health.pipeline.yml`
- `resources/deliverable_customer_360.pipeline.yml`
- `resources/deliverable_supplier_risk.pipeline.yml`
- `resources/deliverable_sentiment.pipeline.yml`
- `resources/project_combined.pipeline.yml`

All pipelines are configured with Spark/Lakeflow serverless compute:

- `serverless: true`

### 5.2 Pipeline Inventory

Delivered pipelines:

1. `[dev vssainath_c] [dev] Bakehouse Foundation Pipeline`
2. `[dev vssainath_c] [dev] Bakehouse Daily Franchise Health Pipeline`
3. `[dev vssainath_c] [dev] Bakehouse Customer 360 Pipeline`
4. `[dev vssainath_c] [dev] Bakehouse Supplier Risk Pipeline`
5. `[dev vssainath_c] [dev] Bakehouse Sentiment Trends Pipeline`
6. `[dev vssainath_c] [dev] Bakehouse Project Combined Pipeline`

Result:

- All pipelines completed successfully on latest executions.

---

## 6. Embedded Data Quality Test Cases

Per request, tests were embedded into pipeline transformations and run as part of pipeline updates.

### 6.1 Test SQL Artifacts

- Foundation test suite:
  - `src/foundation/transformations/silver_test_foundation_quality.sql`
- Daily health test suite:
  - `src/deliverables/daily_franchise_health/transformations/gold_test_daily_franchise_health_quality.sql`
- Customer 360 test suite:
  - `src/deliverables/customer_360/transformations/gold_test_customer_360_quality.sql`
- Supplier risk test suite:
  - `src/deliverables/supplier_risk/transformations/gold_test_supplier_risk_quality.sql`
- Sentiment test suite:
  - `src/deliverables/sentiment_trends/transformations/gold_test_sentiment_trends_quality.sql`
- Project combined test suite:
  - `src/project_combined/transformations/gold_test_project_combined_quality.sql`

### 6.2 Test Output Tables

Generated in `projects.bakehouse_analytics`:

- `silver_test_foundation_quality`
- `gold_test_daily_franchise_health_quality`
- `gold_test_customer_360_quality`
- `gold_test_supplier_risk_quality`
- `gold_test_sentiment_trends_quality`
- `gold_test_project_combined_quality`

Validation outcome:

- All embedded tests returned `failed_records = 0` on latest execution.

---

## 7. Pipeline Validation by Data Sampling

Requested validation by sampling was executed against:

- `gold_daily_franchise_health`
- `gold_customer_360`
- `gold_supplier_risk_matrix`
- `gold_store_sentiment_trends`
- `gold_bakehouse_project_scorecard`

Outcome:

- All sampled queries returned populated results with expected schema-level fields and no empty-result regressions on latest run.

---

## 8. Sales Forecast Machine Learning Model

### 8.1 Initial Forecast Model

Code:

- `src/ml/train_sales_forecast_model.py`

Execution method:

- Databricks Job using Spark Python task (`spark_python_task`) with serverless environment.

Job:

- `Bakehouse Sales Forecast Model Training` (job id `1070694594859251`)

Outputs:

- `gold_sales_forecast_model_metrics`
- `gold_sales_forecast_model_registry`
- `gold_sales_forecast_predictions`

### 8.2 Model Improvement (Requested Option 1)

Enhancements implemented in code:

- Lag features (`lag_1`, `lag_7`)
- Rolling features (`rolling_mean_3`, `rolling_mean_7`, `rolling_std_7`)
- Candidate model tuning (`GBTRegressor`, `RandomForestRegressor`)
- Best-model selection by RMSE
- Recursive multi-day forecasting
- Tuning result persistence table:
  - `gold_sales_forecast_tuning_results`

### 8.3 Improved Model Results

Best selected model:

- `sales_forecast_rf_lag_tuned`
- Config:
  - `{"algorithm":"rf","featureSubsetStrategy":"sqrt","maxDepth":10,"numTrees":400}`

Metrics comparison (baseline vs tuned):

- RMSE: `77.5775 -> 61.4022` (improvement `+16.1753`)
- MAE: `42.9869 -> 38.9401` (improvement `+4.0468`)
- R2: `-0.0873 -> 0.3202` (improvement `+0.4075`)

Latest model artifact path:

- `/Volumes/projects/bakehouse_analytics/ml_artifacts/sales_forecast_model/run_20260223_002316`

Forecast output:

- Table: `projects.bakehouse_analytics.gold_sales_forecast_predictions`
- Rows: `672` (`48` stores x `14` days)
- Date range: `2024-05-18` to `2024-05-31`

---

## 9. Governance and Security Notes

### 9.1 Implemented

- PII masking transformations at curated output level:
  - `silver_customer_profiles.sql`
- Masking function scaffold and UC SQL script:
  - `sql/security_policies.sql`

### 9.2 Deferred by User Decision

- Row-level security rules:
  - Explicitly deferred ("nothing for now").

---

## 10. Traceability Matrix (Request -> Delivery)

| Requested Item | Delivery Status | Primary Code/Artifact References |
|---|---|---|
| New repo Bakehouse Analytics | Completed | `databricks.yml`, `README.md` |
| Base on Bakehouse schema | Completed | `bronze_*.sql` files in `src/foundation/transformations/` |
| Follow technical specification | Completed | `docs/spec_mapping.md`, `silver_*.sql`, `gold_*.sql` |
| Follow best practices | Completed | Medallion structure + pipeline decomposition in `resources/*.pipeline.yml` |
| Sync code to GitHub and Databricks Repo | Completed | GitHub repo + Databricks repo `/Repos/.../bakehouse-analytics` |
| Create catalog/schema and medallion tables | Completed | `sql/bootstrap_uc.sql`, all `bronze_/silver_/gold_` SQL files |
| Separate deliverable pipelines + project pipeline | Completed | six files in `resources/` |
| Use Spark compute | Completed | `serverless: true` in all pipeline resource YAML; Spark Python job for ML |
| Ask clarifications | Completed | catalog/schema/security clarifications captured and applied |
| Build embedded test cases | Completed | `*test*_quality.sql` files across all pipeline folders |
| Validate by sampling | Completed | sample queries executed on all gold outputs |
| Create sales forecast ML model | Completed | `src/ml/train_sales_forecast_model.py`, job `1070694594859251`, forecast/model tables |

---

## 11. Current State

The project is operational end-to-end:

- Ingestion + curation + deliverables + combined scorecard pipelines are deployed and runnable.
- Embedded data-quality test suites are deployed and passing.
- Forecast model training is automated as a Databricks job and improved model is trained.
- Model metrics, tuning results, model registry records, and forecast outputs are persisted in `projects.bakehouse_analytics`.


