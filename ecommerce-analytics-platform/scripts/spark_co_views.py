from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, collect_set, size
import os
import glob

# Initialize Spark with moderate resources
spark = SparkSession.builder \
    .appName("ProductCoViews") \
    .master("local[1]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()

# Configuration
OUTPUT_DIR = "E:\\BigData FinalProject\\generated_data"
SAMPLE_FILE = os.path.join(OUTPUT_DIR, "sessions_1.json")  # Single session file

# Verify file existence
if not os.path.exists(SAMPLE_FILE):
    raise FileNotFoundError(f"No file found at {SAMPLE_FILE}. Please run dataset_generator.py.")

# Convert input path to file:/// format
sample_file = f"file:///{SAMPLE_FILE.replace('\\', '/')}"
print("Processing file:", sample_file)

# Load and clean session data
sessions_df = spark.read.json(sample_file)
print("Initial session count:", sessions_df.count())

# Clean data: Remove rows with missing user_id or viewed_products
sessions_df = sessions_df.dropna(subset=["user_id", "viewed_products"])
sessions_df = sessions_df.filter(size(col("viewed_products")) > 0)
print("Cleaned session count:", sessions_df.count())

# Calculate products frequently viewed together
product_views = sessions_df.select("session_id", explode("viewed_products").alias("product_id"))

co_views = product_views.alias("p1").join(
    product_views.alias("p2"),
    (col("p1.session_id") == col("p2.session_id")) & (col("p1.product_id") < col("p2.product_id")),
    "inner"
).groupBy(col("p1.product_id").alias("product1"), col("p2.product_id").alias("product2")) \
 .count() \
 .orderBy("count", ascending=False)

# Show top 5 co-viewed product pairs
print("Top 5 Co-Viewed Product Pairs:")
co_views.show(5, truncate=False)

# Save results to Parquet with file:/// path
output_path = f"file:///{os.path.join(OUTPUT_DIR, 'co_viewed_products.parquet').replace('\\', '/')}"
co_views.write.mode("overwrite").parquet(output_path)

# Stop Spark
spark.stop()