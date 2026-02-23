from datetime import datetime, timedelta, timezone
import json
import math
import statistics

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "projects"
SCHEMA = "bakehouse_analytics"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_franchise_health"
MODEL_METRICS_TABLE = f"{CATALOG}.{SCHEMA}.gold_sales_forecast_model_metrics"
FORECAST_TABLE = f"{CATALOG}.{SCHEMA}.gold_sales_forecast_predictions"
MODEL_REGISTRY_TABLE = f"{CATALOG}.{SCHEMA}.gold_sales_forecast_model_registry"
TUNING_RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.gold_sales_forecast_tuning_results"
MODEL_BASE_PATH = "/Volumes/projects/bakehouse_analytics/ml_artifacts/sales_forecast_model"
FORECAST_HORIZON_DAYS = 14
TEST_DAYS = 7


def build_training_frame(base_df, min_date, max_date):
    stores_df = base_df.select("store_id").distinct()

    dates_df = spark.sql(
        f"""
        SELECT explode(sequence(date('{min_date}'), date('{max_date}'))) AS reporting_date
        """
    )

    panel_df = (
        stores_df.crossJoin(dates_df)
        .join(base_df, on=["store_id", "reporting_date"], how="left")
        .fillna({"total_daily_revenue": 0.0})
    )

    w = Window.partitionBy("store_id").orderBy("reporting_date")

    feature_df = (
        panel_df.withColumn("label", F.col("total_daily_revenue").cast("double"))
        .withColumn("day_of_week", F.dayofweek("reporting_date").cast("double"))
        .withColumn("day_of_month", F.dayofmonth("reporting_date").cast("double"))
        .withColumn("month", F.month("reporting_date").cast("double"))
        .withColumn("week_of_year", F.weekofyear("reporting_date").cast("double"))
        .withColumn("is_weekend", F.when(F.dayofweek("reporting_date").isin(1, 7), 1.0).otherwise(0.0))
        .withColumn("trend_index", F.datediff(F.col("reporting_date"), F.lit(min_date)).cast("double"))
        .withColumn("lag_1", F.lag("label", 1).over(w))
        .withColumn("lag_7", F.lag("label", 7).over(w))
        .withColumn("rolling_mean_3", F.avg("label").over(w.rowsBetween(-3, -1)))
        .withColumn("rolling_mean_7", F.avg("label").over(w.rowsBetween(-7, -1)))
        .withColumn("rolling_std_7", F.coalesce(F.stddev_samp("label").over(w.rowsBetween(-7, -1)), F.lit(0.0)))
    )

    usable_df = feature_df.where(F.col("lag_7").isNotNull() & F.col("rolling_mean_7").isNotNull())
    return usable_df, panel_df


def build_estimator(config):
    if config["algorithm"] == "gbt":
        return GBTRegressor(
            labelCol="label",
            featuresCol="features",
            seed=42,
            maxIter=config["maxIter"],
            maxDepth=config["maxDepth"],
            stepSize=config["stepSize"],
            maxBins=config["maxBins"],
        )

    return RandomForestRegressor(
        labelCol="label",
        featuresCol="features",
        seed=42,
        numTrees=config["numTrees"],
        maxDepth=config["maxDepth"],
        featureSubsetStrategy=config["featureSubsetStrategy"],
    )


def tune_model(train_df, test_df):
    feature_columns = [
        "store_id_ohe",
        "day_of_week",
        "day_of_month",
        "month",
        "week_of_year",
        "is_weekend",
        "trend_index",
        "lag_1",
        "lag_7",
        "rolling_mean_3",
        "rolling_mean_7",
        "rolling_std_7",
    ]

    candidates = [
        {"algorithm": "gbt", "maxIter": 120, "maxDepth": 4, "stepSize": 0.05, "maxBins": 64},
        {"algorithm": "gbt", "maxIter": 180, "maxDepth": 5, "stepSize": 0.03, "maxBins": 64},
        {"algorithm": "gbt", "maxIter": 140, "maxDepth": 6, "stepSize": 0.04, "maxBins": 128},
        {"algorithm": "rf", "numTrees": 250, "maxDepth": 8, "featureSubsetStrategy": "auto"},
        {"algorithm": "rf", "numTrees": 400, "maxDepth": 10, "featureSubsetStrategy": "sqrt"},
    ]

    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    tuning_results = []
    best = None

    for config in candidates:
        indexer = StringIndexer(inputCol="store_id", outputCol="store_id_idx", handleInvalid="keep")
        encoder = OneHotEncoder(inputCols=["store_id_idx"], outputCols=["store_id_ohe"])
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        estimator = build_estimator(config)

        pipeline = Pipeline(stages=[indexer, encoder, assembler, estimator])
        model = pipeline.fit(train_df)
        pred = model.transform(test_df)

        rmse = float(evaluator_rmse.evaluate(pred))
        mae = float(evaluator_mae.evaluate(pred))
        r2 = float(evaluator_r2.evaluate(pred))

        result = {
            "algorithm": config["algorithm"],
            "config_json": json.dumps(config, sort_keys=True),
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
            "evaluated_at": datetime.now(timezone.utc),
        }
        tuning_results.append(result)

        if best is None or rmse < best["rmse"]:
            best = {
                "model": model,
                "config": config,
                "rmse": rmse,
                "mae": mae,
                "r2": r2,
                "predictions": pred,
            }

    tuning_df = spark.createDataFrame(tuning_results)
    tuning_df.write.mode("overwrite").saveAsTable(TUNING_RESULTS_TABLE)

    return best


def recursive_forecast(best_model, panel_df, min_date, max_date, model_name):
    store_hist = {}
    historical_rows = (
        panel_df.select("store_id", "reporting_date", "total_daily_revenue")
        .orderBy("store_id", "reporting_date")
        .collect()
    )

    for row in historical_rows:
        store_hist.setdefault(row["store_id"], []).append((row["reporting_date"], float(row["total_daily_revenue"])))

    forecast_rows = []

    for step in range(1, FORECAST_HORIZON_DAYS + 1):
        target_date = max_date + timedelta(days=step)
        model_input_rows = []

        for store_id, history in store_hist.items():
            values = [v for _, v in history]

            lag_1 = values[-1] if len(values) >= 1 else 0.0
            last7 = values[-7:] if len(values) >= 7 else values
            lag_7 = values[-7] if len(values) >= 7 else (sum(last7) / len(last7) if last7 else 0.0)
            last3 = values[-3:] if len(values) >= 3 else values
            rolling_mean_3 = sum(last3) / len(last3) if last3 else 0.0
            rolling_mean_7 = sum(last7) / len(last7) if last7 else 0.0
            rolling_std_7 = statistics.pstdev(last7) if len(last7) >= 2 else 0.0

            model_input_rows.append(
                {
                    "store_id": store_id,
                    "reporting_date": target_date,
                    "day_of_week": float(target_date.isoweekday() % 7 + 1),
                    "day_of_month": float(target_date.day),
                    "month": float(target_date.month),
                    "week_of_year": float(target_date.isocalendar()[1]),
                    "is_weekend": 1.0 if target_date.isoweekday() in (6, 7) else 0.0,
                    "trend_index": float((target_date - min_date).days),
                    "lag_1": float(lag_1),
                    "lag_7": float(lag_7),
                    "rolling_mean_3": float(rolling_mean_3),
                    "rolling_mean_7": float(rolling_mean_7),
                    "rolling_std_7": float(rolling_std_7),
                }
            )

        model_input_df = spark.createDataFrame(model_input_rows)

        step_predictions = (
            best_model.transform(model_input_df)
            .select(
                "store_id",
                F.col("reporting_date").alias("forecast_date"),
                F.round(F.col("prediction"), 2).alias("predicted_daily_revenue"),
            )
            .collect()
        )

        for pred in step_predictions:
            pred_value = float(pred["predicted_daily_revenue"])
            store_id = pred["store_id"]
            forecast_date = pred["forecast_date"]

            store_hist[store_id].append((forecast_date, pred_value))
            forecast_rows.append(
                {
                    "store_id": store_id,
                    "forecast_date": forecast_date,
                    "predicted_daily_revenue": pred_value,
                    "model_name": model_name,
                    "forecast_horizon_day": step,
                }
            )

    forecast_df = spark.createDataFrame(forecast_rows).withColumn("forecast_generated_at", F.current_timestamp())
    forecast_df.write.mode("overwrite").saveAsTable(FORECAST_TABLE)


def main():
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")

    base_df = (
        spark.table(SOURCE_TABLE)
        .select("store_id", "reporting_date", "total_daily_revenue")
        .where(F.col("total_daily_revenue").isNotNull())
    )

    min_max = base_df.select(
        F.min("reporting_date").alias("min_date"), F.max("reporting_date").alias("max_date")
    ).collect()[0]
    min_date = min_max["min_date"]
    max_date = min_max["max_date"]

    training_df, panel_df = build_training_frame(base_df, min_date, max_date)

    total_days = (max_date - min_date).days + 1
    holdout_days = TEST_DAYS if total_days > 14 else max(3, math.floor(total_days / 3))
    split_date = max_date - timedelta(days=holdout_days)

    train_df = training_df.where(F.col("reporting_date") <= F.lit(split_date))
    test_df = training_df.where(F.col("reporting_date") > F.lit(split_date))

    if train_df.count() == 0 or test_df.count() == 0:
        raise ValueError("Insufficient train/test rows after feature engineering. Expand historical window.")

    best = tune_model(train_df, test_df)

    selected_algorithm = best["config"]["algorithm"]
    model_name = f"sales_forecast_{selected_algorithm}_lag_tuned"

    metrics_df = spark.createDataFrame(
        [
            {
                "model_name": model_name,
                "train_rows": train_df.count(),
                "test_rows": test_df.count(),
                "rmse": float(best["rmse"]),
                "mae": float(best["mae"]),
                "r2": float(best["r2"]),
                "trained_at": datetime.now(timezone.utc),
            }
        ]
    )
    metrics_df.write.mode("append").saveAsTable(MODEL_METRICS_TABLE)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    model_path = f"{MODEL_BASE_PATH}/run_{run_ts}"
    best["model"].write().overwrite().save(model_path)

    registry_df = spark.createDataFrame(
        [
            {
                "model_name": model_name,
                "model_path": model_path,
                "trained_at": datetime.now(timezone.utc),
                "rmse": float(best["rmse"]),
                "mae": float(best["mae"]),
                "r2": float(best["r2"]),
            }
        ]
    )
    registry_df.write.mode("append").saveAsTable(MODEL_REGISTRY_TABLE)

    recursive_forecast(best["model"], panel_df, min_date, max_date, model_name)

    print("Sales forecast model trained successfully")
    print(f"Selected algorithm: {selected_algorithm}")
    print(f"Selected config: {json.dumps(best['config'], sort_keys=True)}")
    print(f"RMSE: {best['rmse']:.4f}")
    print(f"MAE: {best['mae']:.4f}")
    print(f"R2: {best['r2']:.4f}")
    print(f"Model path: {model_path}")


if __name__ == "__main__":
    main()
