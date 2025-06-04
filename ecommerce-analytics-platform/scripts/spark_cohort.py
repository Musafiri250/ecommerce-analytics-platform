from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("CohortAnalysis") \
    .master("local[1]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .getOrCreate()

# Configuration
OUTPUT_DIR = "E:\\BigData FinalProject\\generated_data"
USERS_FILE = f"file:///{os.path.join(OUTPUT_DIR, 'users.json').replace('\\', '/')}"
TRANS_FILE = f"file:///{os.path.join(OUTPUT_DIR, 'transactions_1.json').replace('\\', '/')}"

# Load data
users_df = spark.read.json(USERS_FILE)
transactions_df = spark.read.json(TRANS_FILE)

# Extract registration month
users_df = users_df.withColumn("registration_month", col("registration_date").substr(1, 7))

# Cohort analysis: Spending by registration month
cohort_spending = transactions_df.join(users_df, "user_id") \
                                .groupBy("registration_month") \
                                .agg({"total": "sum"}) \
                                .orderBy("registration_month")

# Show results
print("Cohort Spending by Registration Month:")
cohort_spending.show(truncate=False)

# Save results
output_path = f"file:///{os.path.join(OUTPUT_DIR, 'cohort_spending.parquet').replace('\\', '/')}"
cohort_spending.write.mode("overwrite").parquet(output_path)

# Stop Spark
spark.stop()