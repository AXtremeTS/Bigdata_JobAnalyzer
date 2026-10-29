# 2_pipeline/tang3_analytics/ml.py
"""
TẦNG 3 (TANG 3) — MACHINE LEARNING JOB (PYSPARK MLLIB):
1. Dự đoán mức lương (RandomForestRegressor):
   - Đầu vào: province, level, category, exp_min_years, skill_count.
   - Đo lường đánh giá: RMSE, MAE, R2 so sánh với Baseline (mức lương trung bình).
2. Phân khúc thị trường lao động (K-Means Clustering):
   - Phân cụm các tỉnh thành dựa trên số lượng việc làm, lương trung bình và kinh nghiệm.
   - Đánh giá bằng ClusteringEvaluator (Silhouette Score).
   - Đặt nhãn phân cụm: Đô thị Trọng điểm, Khu vực Công nghiệp, Thị trường Địa phương.

Ghi kết quả JSON về:
- data/tang3/ml_metrics.json & data/tang3/ml_clusters.json
- data/tang3/ml_metrics.json & data/tang3/ml_clusters.json (đồng bộ tương thích)
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
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline", "tang3_analytics"))


def run_tang3_ml_job(tang2_hdfs_path: str = "hdfs://localhost:9000/tang2/jobs",
                     output_json_dir: str = os.path.join(PROJECT_ROOT, "data", "tang3"),
                     mirror_json_dir: str = os.path.join(PROJECT_ROOT, "data", "tang3")):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — TẦNG 3: SPARK MLLIB (REGRESSION & CLUSTERING)")
    print("=" * 70)
    start_time = time.time()

    os.makedirs(output_json_dir, exist_ok=True)
    if mirror_json_dir:
        os.makedirs(mirror_json_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("JobMarket-Tang3-ML-Analytics") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Tang 2 with fallback to tang2
    read_path = tang2_hdfs_path
    print(f"\n[1] Đang đọc tập dữ liệu Tầng 2 từ HDFS: {read_path}")
    try:
        df = spark.read.parquet(read_path)
    except Exception:
        read_path = "hdfs://localhost:9000/tang2/jobs"
        print(f"  [Chuyển hướng] Đọc từ: {read_path}")
        df = spark.read.parquet(read_path)

    # Resolve column names (supporting both raw column variations)
    cols = set(df.columns)
    salary_col_name = "salary_monthly_vnd" if "salary_monthly_vnd" in cols else "salary_avg_vnd"
    skills_col_name = "skills" if "skills" in cols else "technical_skills"

    # -------------------------------------------------------------
    # MODEL 1: SALARY REGRESSION
    # -------------------------------------------------------------
    print("\n[2] Tiền xử lý dữ liệu cho Mô hình Dự đoán Mức lương (Random Forest)...")
    
    # Filter valid salary data
    filter_cond = col(salary_col_name).isNotNull() & (col(salary_col_name) > 0)
    if "is_salary_negotiable" in cols:
        filter_cond = filter_cond & (~col("is_salary_negotiable"))
    if "is_salary_outlier" in cols:
        filter_cond = filter_cond & (~col("is_salary_outlier"))
    if "province" in cols:
        filter_cond = filter_cond & col("province").isNotNull()
    if "level" in cols:
        filter_cond = filter_cond & col("level").isNotNull()

    ml_prep = df.filter(filter_cond)
    if skills_col_name in cols:
        ml_prep = ml_prep.withColumn("skill_count", size(col(skills_col_name)).cast("double"))
    else:
        ml_prep = ml_prep.withColumn("skill_count", lit(3.0))

    if "experience_years" in cols:
        ml_prep = ml_prep.withColumn("exp_min_years", col("experience_years").cast("double"))
    elif "exp_min_years" in cols:
        ml_prep = ml_prep.withColumn("exp_min_years", coalesce(col("exp_min_years"), lit(1.0)).cast("double"))
    else:
        ml_prep = ml_prep.withColumn("exp_min_years", lit(1.0))

    category_col = col("category") if "category" in cols else (col("region") if "region" in cols else lit("IT"))

    ml_df = ml_prep.select(
        col("province"),
        col("level"),
        category_col.alias("category"),
        col("exp_min_years"),
        col("skill_count"),
        col(salary_col_name).alias("target_salary")
    )

    total_samples = ml_df.count()
    print(f"  Mẫu huấn luyện hợp lệ: {total_samples:,} dòng")

    ml_metrics_result = {
        "status": "completed",
        "sample_count": total_samples,
        "features": ["province", "level", "category", "exp_min_years", "skill_count"]
    }

    if total_samples >= 100:
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
            col("target_salary").cast("double").alias("label")
        )

        train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)
        print(f"  Phân chia mẫu: Train={train_data.count():,}, Test={test_data.count():,}")

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

        # Baseline: Predict mean salary
        mean_val = train_data.select(avg("label")).collect()[0][0] or 15000000.0
        baseline_preds = test_data.withColumn("prediction", lit(mean_val))
        baseline_rmse = evaluator_rmse.evaluate(baseline_preds)
        baseline_mae = evaluator_mae.evaluate(baseline_preds)

        print(f"  [Chỉ số Mô hình] RMSE: {round(rmse, 0):,} VND | MAE: {round(mae, 0):,} VND | R2: {round(r2, 4)}")
        print(f"  [Chỉ số Baseline] RMSE: {round(baseline_rmse, 0):,} VND | MAE: {round(baseline_mae, 0):,} VND")

        ml_metrics_result.update({
            "model_type": "RandomForestRegressor",
            "hyperparameters": {"numTrees": 30, "maxDepth": 6, "seed": 42},
            "rmse_vnd": round(rmse, 0),
            "mae_vnd": round(mae, 0),
            "r2_score": round(r2, 4),
            "baseline_rmse_vnd": round(baseline_rmse, 0),
            "baseline_mae_vnd": round(baseline_mae, 0),
            "improvement_over_baseline_pct": round(((baseline_rmse - rmse) / baseline_rmse) * 100.0, 2) if baseline_rmse > 0 else 0.0
        })

    # Save ml_metrics.json to both tang3 and tang3
    target_dirs = [output_json_dir]
    if mirror_json_dir:
        target_dirs.append(mirror_json_dir)
    for target_dir in target_dirs:
        with open(os.path.join(target_dir, "ml_metrics.json"), "w", encoding="utf-8") as f:
            json.dump(ml_metrics_result, f, ensure_ascii=False, indent=2)

    # -------------------------------------------------------------
    # MODEL 2: MARKET SEGMENTATION (K-MEANS CLUSTERING)
    # -------------------------------------------------------------
    print("\n[3] Tiến hành Phân khúc Thị trường Lao động theo Tỉnh/Thành (K-Means)...")
    exp_calc_col = col("experience_years") if "experience_years" in cols else coalesce(col("exp_min_years"), lit(1.0))
    
    prov_summary = df.filter(col("province").isNotNull()).groupBy("province").agg(
        count("*").cast("double").alias("job_count"),
        avg(when(col(salary_col_name).isNotNull(), col(salary_col_name))).alias("avg_salary_vnd"),
        avg(exp_calc_col).alias("avg_exp_years")
    ).filter(col("job_count") >= 5)

    prov_summary = prov_summary.fillna({"avg_salary_vnd": 12_000_000.0, "avg_exp_years": 1.5})
    prov_count = prov_summary.count()
    print(f"  Số tỉnh thành đủ điều kiện phân cụm: {prov_count}")

    clusters_output = []
    silhouette = 0.0

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
        print(f"  [Điểm K-Means Silhouette]: {round(silhouette, 4)} (k={k})")

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

    # Save ml_clusters.json to both tang3 and tang3
    for target_dir in target_dirs:
        with open(os.path.join(target_dir, "ml_clusters.json"), "w", encoding="utf-8") as f:
            json.dump({
                "silhouette_score": round(silhouette, 4),
                "k": 3,
                "clusters": clusters_output
            }, f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 70)
    print(f"  [SUCCESS] TẦNG 3 MACHINE LEARNING HOÀN TẤT TRONG {elapsed} GIÂY")
    print("=" * 70)

    spark.stop()


# Backward compatibility alias
run_ml_job = run_tang3_ml_job

if __name__ == "__main__":
    run_tang3_ml_job()

