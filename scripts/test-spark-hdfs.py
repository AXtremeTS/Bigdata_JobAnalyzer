# scripts/test-spark-hdfs.py
# Verify Apache Spark reading directly from HDFS with 1 NameNode + 3 DataNodes
import sys
import time

print("=" * 60)
print("  SPARK ON HDFS VERIFICATION TEST (1 NAMENODE + 3 DATANODES)")
print("=" * 60)

try:
    from pyspark.sql import SparkSession
    import pyspark
except ImportError:
    print("[ERROR] PySpark not installed in current Python environment.")
    print("Installing pyspark...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark==3.5.1"])
    from pyspark.sql import SparkSession
    import pyspark

print(f"PySpark Version: {pyspark.__version__}")

# Initialize Spark Session configured for Local Master & HDFS
spark = SparkSession.builder \
    .appName("JobMarket-HDFS-Verification") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

hdfs_path = "hdfs://localhost:9000/raw/jobs/vietjobs.csv"
print(f"\n[1] Reading raw dataset directly from HDFS: {hdfs_path}")
start_time = time.time()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv(hdfs_path)

elapsed = round(time.time() - start_time, 2)
print(f"  [OK] Successfully loaded DataFrame in {elapsed}s")

print("\n[2] Dataset Schema:")
df.printSchema()

print("\n[3] Row Count & Sample Data:")
total_rows = df.count()
print(f"  Total records in HDFS: {total_rows:,} rows")

print("\n[4] First 5 Records:")
df.select("job_title", "location", "salary", "country").show(5, truncate=50)

print("\n" + "=" * 60)
print(f"  [SUCCESS] Spark directly read {total_rows} rows from HDFS!")
print("=" * 60)

spark.stop()
