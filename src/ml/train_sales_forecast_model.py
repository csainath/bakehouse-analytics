from datetime import datetime

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F

CATALOG = "projects"
SCHEMA = "bakehouse_analytics"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_franchise_health"
MODEL_METRICS_TABLE = f"{CATALOG}.{SCHEMA}.gold_sales_forecast_model_metrics"
FORECAST_TABLE = f"{CATALOG}.{SCHEMA}.gold_sales_forecast_predictions"
MODEL_REGISTRY_TABLE = f"{CATALOG}.{SCHEMA}.gold_sales_forecast_model_registry"
MODEL_BASE_PATH = "/Volumes/projects/bakehouse_analytics/ml_artifacts/sales_forecast_model"
FORECAST_HORIZON_DAYS = 14


def build_features(df, min_date):
    return (
        df.withColumn("label", F.col("total_daily_revenue").cast("double"))
        .withColumn("day_of_week", F.dayofweek("reporting_date").cast("double"))
        .withColumn("day_of_month", F.dayofmonth("reporting_date").cast("double"))
        .withColumn("month", F.month("reporting_date").cast("double"))
        .withColumn("week_of_year", F.weekofyear("reporting_date").cast("double"))
        .withColumn("is_weekend", F.when(F.dayofweek("reporting_date").isin(1, 7), 1.0).otherwise(0.0))
        .withColumn(
            "trend_index",
            F.datediff(F.col("reporting_date"), F.lit(min_date)).cast("double"),
        )
    )


def main():
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")

    base_df = (
        spark.table(SOURCE_TABLE)
        .select("store_id", "reporting_date", "total_daily_revenue")
        .where(F.col("total_daily_revenue").isNotNull())
    )

    min_date = base_df.select(F.min("reporting_date").alias("min_date")).collect()[0]["min_date"]
    feature_df = build_features(base_df, min_date)

    max_date = feature_df.select(F.max("reporting_date").alias("max_date")).collect()[0]["max_date"]
    split_date = spark.sql(f"SELECT date_sub('{max_date}', 7) AS split_date").collect()[0]["split_date"]

    train_df = feature_df.where(F.col("reporting_date") <= F.lit(split_date))
    test_df = feature_df.where(F.col("reporting_date") > F.lit(split_date))

    indexer = StringIndexer(inputCol="store_id", outputCol="store_id_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["store_id_idx"], outputCols=["store_id_ohe"])
    assembler = VectorAssembler(
        inputCols=[
            "store_id_ohe",
            "day_of_week",
            "day_of_month",
            "month",
            "week_of_year",
            "is_weekend",
            "trend_index",
        ],
        outputCol="features",
    )

    regressor = GBTRegressor(
        labelCol="label",
        featuresCol="features",
        maxIter=120,
        maxDepth=5,
        stepSize=0.05,
        seed=42,
    )

    pipeline = Pipeline(stages=[indexer, encoder, assembler, regressor])
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df).select(
        "store_id", "reporting_date", "label", F.col("prediction").alias("predicted_revenue")
    )

    rmse = RegressionEvaluator(labelCol="label", predictionCol="predicted_revenue", metricName="rmse").evaluate(predictions)
    mae = RegressionEvaluator(labelCol="label", predictionCol="predicted_revenue", metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol="label", predictionCol="predicted_revenue", metricName="r2").evaluate(predictions)

    metrics_df = spark.createDataFrame(
        [
            {
                "model_name": "gbt_sales_forecast",
                "train_rows": train_df.count(),
                "test_rows": test_df.count(),
                "rmse": float(rmse),
                "mae": float(mae),
                "r2": float(r2),
                "trained_at": datetime.utcnow(),
            }
        ]
    )
    metrics_df.write.mode("append").saveAsTable(MODEL_METRICS_TABLE)

    stores_df = feature_df.select("store_id").distinct()
    future_dates_df = spark.sql(
        f"""
        SELECT explode(sequence(date_add(date('{max_date}'), 1), date_add(date('{max_date}'), {FORECAST_HORIZON_DAYS}))) AS reporting_date
        """
    )

    future_df = (
        stores_df.crossJoin(future_dates_df)
        .withColumn("total_daily_revenue", F.lit(None).cast("double"))
    )

    future_features = build_features(
        future_df.select("store_id", "reporting_date", "total_daily_revenue")
        ,
        min_date,
    )

    future_predictions = (
        model.transform(future_features)
        .select(
            "store_id",
            F.col("reporting_date").alias("forecast_date"),
            F.round(F.col("prediction"), 2).alias("predicted_daily_revenue"),
        )
        .withColumn("model_name", F.lit("gbt_sales_forecast"))
        .withColumn("forecast_generated_at", F.current_timestamp())
    )

    future_predictions.write.mode("overwrite").saveAsTable(FORECAST_TABLE)

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    model_path = f"{MODEL_BASE_PATH}/run_{run_ts}"
    model.write().overwrite().save(model_path)

    registry_df = spark.createDataFrame(
        [
            {
                "model_name": "gbt_sales_forecast",
                "model_path": model_path,
                "trained_at": datetime.utcnow(),
                "rmse": float(rmse),
                "mae": float(mae),
                "r2": float(r2),
            }
        ]
    )
    registry_df.write.mode("append").saveAsTable(MODEL_REGISTRY_TABLE)

    print("Sales forecast model trained successfully")
    print(f"RMSE: {rmse:.4f}")
    print(f"MAE: {mae:.4f}")
    print(f"R2: {r2:.4f}")
    print(f"Model path: {model_path}")


if __name__ == "__main__":
    main()
