# etl/jobs/ml.py
"""
Machine Learning Job (PySpark MLlib):
1. Salary Regression:
   - Uses RandomForestRegressor to predict salary_avg_vnd based on province, level,
     category, exp_min_years, and skill_count.
   - Evaluates RMSE, MAE, R2 against a baseline (mean salary).
   - Computes feature importances.
2. Market Segmentation (K-Means Clustering):
   - Clusters provinces based on job count, average salary, and skill diversity.
   - Evaluates with ClusteringEvaluator (Silhouette Score).
   - Generates named clusters (e.g. Mega Tech Hub, High-Growth Industrial, Regional).
Outputs saved to data/tang3/ml_metrics.json and data/tang3/ml_clusters.json.
"""

import sys
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, coalesce, lit, count, avg, round as spark_round, when
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))


def run_ml_job(tang2_hdfs_path: str = "hdfs://localhost:9000/tang2/jobs",
               output_json_dir: str = os.path.join(PROJECT_ROOT, "data", "tang3")):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — SPARK MLLIB (REGRESSION & CLUSTERING)")
    print("=" * 70)
    start_time = time.time()

    os.makedirs(output_json_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("JobMarket-ML-Analytics") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[1] Reading Tang2 dataset from HDFS: {tang2_hdfs_path}")
    df = spark.read.parquet(tang2_hdfs_path)

    # -------------------------------------------------------------
    # MODEL 1: SALARY REGRESSION
    # -------------------------------------------------------------
    print("\n[2] Preparing data for Salary Regression Model...")
    ml_df = df.filter(
        (col("salary_avg_vnd").isNotNull()) &
        (~col("is_salary_negotiable")) &
        (~col("is_salary_outlier")) &
        (col("province").isNotNull()) &
        (col("level").isNotNull())
    ).withColumn("skill_count", size(col("technical_skills")).cast("double")) \
     .withColumn("exp_min_years", coalesce(col("exp_min_years"), lit(1.0)).cast("double")) \
     .select("province", "level", "category", "exp_min_years", "skill_count", "salary_avg_vnd")

    total_samples = ml_df.count()
    print(f"  Training samples with valid salary: {total_samples:,} rows")

    ml_metrics_result = {
        "status": "completed",
        "sample_count": total_samples,
        "features": ["province", "level", "category", "exp_min_years", "skill_count"]
    }

    if total_samples >= 100:
        # String Indexing & One-Hot Encoding
        indexer_prov = StringIndexer(inputCol="province", outputCol="prov_idx", handleInvalid="keep")
        indexer_lvl = StringIndexer(inputCol="level", outputCol="lvl_idx", handleInvalid="keep")
        indexer_cat = StringIndexer(inputCol="category", outputCol="cat_idx", handleInvalid="keep")

        indexed = indexer_prov.fit(ml_df).transform(ml_df)
        indexed = indexer_lvl.fit(indexed).transform(indexed)
        indexed = indexer_cat.fit(indexed).transform(indexed)

        encoder = OneHotEncoder(
            inputCols=["prov_idx", "lvl_idx", "cat_idx"],
            outputCols=["prov_vec", "lvl_vec", "cat_vec"]
        )
        encoded = encoder.fit(indexed).transform(indexed)

        assembler = VectorAssembler(
            inputCols=["prov_vec", "lvl_vec", "cat_vec", "exp_min_years", "skill_count"],
            outputCol="features"
        )
        final_data = assembler.transform(encoded).select(
            col("features"),
            col("salary_avg_vnd").cast("double").alias("label")
        )

        train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)
        print(f"  Split: Train={train_data.count():,}, Test={test_data.count():,}")

        # Train Random Forest Regressor
        rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=30, maxDepth=6, seed=42)
        model = rf.fit(train_data)
        predictions = model.transform(test_data)

        evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
        evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

        rmse = evaluator_rmse.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)

        # Baseline: Predict overall mean salary
        mean_val = train_data.select(avg("label")).collect()[0][0]
        baseline_preds = test_data.withColumn("prediction", lit(mean_val))
        baseline_rmse = evaluator_rmse.evaluate(baseline_preds)
        baseline_mae = evaluator_mae.evaluate(baseline_preds)

        print(f"  [Model Metrics] RMSE: {round(rmse, 0):,} VND | MAE: {round(mae, 0):,} VND | R2: {round(r2, 4)}")
        print(f"  [Baseline Metrics] RMSE: {round(baseline_rmse, 0):,} VND | MAE: {round(baseline_mae, 0):,} VND")

        ml_metrics_result.update({
            "model_type": "RandomForestRegressor",
            "hyperparameters": {"numTrees": 30, "maxDepth": 6, "seed": 42},
            "rmse_vnd": round(rmse, 0),
            "mae_vnd": round(mae, 0),
            "r2_score": round(r2, 4),
            "baseline_rmse_vnd": round(baseline_rmse, 0),
            "baseline_mae_vnd": round(baseline_mae, 0),
            "improvement_over_baseline_pct": round(((baseline_rmse - rmse) / baseline_rmse) * 100.0, 2)
        })

    with open(os.path.join(output_json_dir, "ml_metrics.json"), "w", encoding="utf-8") as f:
        json.dump(ml_metrics_result, f, ensure_ascii=False, indent=2)

    # -------------------------------------------------------------
    # MODEL 2: MARKET SEGMENTATION (K-MEANS CLUSTERING)
    # -------------------------------------------------------------
    print("\n[3] Running K-Means Market Segmentation on Provinces...")
    prov_summary = df.filter(col("province").isNotNull()).groupBy("province").agg(
        count("*").cast("double").alias("job_count"),
        avg(when((col("salary_avg_vnd").isNotNull()) & (~col("is_salary_negotiable")), col("salary_avg_vnd"))).alias("avg_salary_vnd"),
        avg(coalesce(col("exp_min_years"), lit(1.0))).alias("avg_exp_years")
    ).filter(col("job_count") >= 5)

    prov_summary = prov_summary.fillna({"avg_salary_vnd": 12_000_000.0, "avg_exp_years": 1.5})
    prov_count = prov_summary.count()
    print(f"  Clustering {prov_count} eligible provinces/cities...")

    clusters_output = []

    if prov_count >= 4:
        cluster_assembler = VectorAssembler(
            inputCols=["job_count", "avg_salary_vnd", "avg_exp_years"],
            outputCol="raw_features"
        )
        assembled_prov = cluster_assembler.transform(prov_summary)

        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
        scaled_prov = scaler.fit(assembled_prov).transform(assembled_prov)

        k = 3
        kmeans = KMeans(k=k, seed=42)
        kmeans_model = kmeans.fit(scaled_prov)
        clustered_df = kmeans_model.transform(scaled_prov)

        evaluator_silhouette = ClusteringEvaluator(predictionCol="prediction", featuresCol="features")
        silhouette = evaluator_silhouette.evaluate(clustered_df)
        print(f"  [K-Means Silhouette Score]: {round(silhouette, 4)} (k={k})")

        # Cluster Labels mapping
        cluster_names = {
            0: "Đô thị Trọng điểm (Mega Tech & Commercial Hub)",
            1: "Khu vực Công nghiệp Đang phát triển (Industrial Centers)",
            2: "Thị trường Việc làm Địa phương (Regional & Emerging Markets)"
        }

        cluster_rows = clustered_df.select(
            "province", "job_count", "avg_salary_vnd", "avg_exp_years", "prediction"
        ).collect()

        for r in cluster_rows:
            c_id = int(r["prediction"])
            clusters_output.append({
                "province": r["province"],
                "job_count": int(r["job_count"]),
                "avg_salary_vnd": round(r["avg_salary_vnd"], 0) if r["avg_salary_vnd"] else 0,
                "avg_exp_years": round(r["avg_exp_years"], 1) if r["avg_exp_years"] else 0.0,
                "cluster_id": c_id,
                "cluster_name": cluster_names.get(c_id, f"Cụm {c_id}")
            })

    with open(os.path.join(output_json_dir, "ml_clusters.json"), "w", encoding="utf-8") as f:
        json.dump({
            "silhouette_score": round(silhouette, 4) if prov_count >= 4 else 0.0,
            "k": 3,
            "clusters": clusters_output
        }, f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 70)
    print(f"  [SUCCESS] MACHINE LEARNING JOBS COMPLETED in {elapsed}s")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    run_ml_job()

