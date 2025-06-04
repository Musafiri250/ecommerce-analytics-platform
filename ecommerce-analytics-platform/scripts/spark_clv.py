from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("CustomerLifetimeValue") \
    .master("local[1]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .getOrCreate()

# Configuration
OUTPUT_DIR = "E:\\BigData FinalProject\\generated_data"
TRANS_FILE = f"file:///{os.path.join(OUTPUT_DIR, 'transactions_1.json').replace('\\', '/')}"
SESS_FILE = f"file:///{os.path.join(OUTPUT_DIR, 'sessions_1.json').replace('\\', '/')}"

# Verify file existence
if not os.path.exists(os.path.join(OUTPUT_DIR, 'transactions_1.json')):
    raise FileNotFoundError(f"No file found at {TRANS_FILE}")
if not os.path.exists(os.path.join(OUTPUT_DIR, 'sessions_1.json')):
    raise FileNotFoundError(f"No file found at {SESS_FILE}")

# Load data
transactions_df = spark.read.json(TRANS_FILE)
sessions_df = spark.read.json(SESS_FILE)

# Calculate CLV: Total spending and session count per user
clv = transactions_df.groupBy("user_id").agg({"total": "sum"}) \
                    .join(sessions_df.groupBy("user_id").count(), "user_id") \
                    .withColumnRenamed("sum(total)", "total_spent") \
                    .withColumnRenamed("count", "session_count")

# Show results
print("Customer Lifetime Value:")
clv.show(10, truncate=False)

# Save results
output_path = f"file:///{os.path.join(OUTPUT_DIR, 'clv.parquet').replace('\\', '/')}"
clv.write.mode("overwrite").parquet(output_path)

# Stop Spark
spark.stop()