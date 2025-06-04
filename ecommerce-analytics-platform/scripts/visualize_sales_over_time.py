import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# Pipeline to aggregate revenue by month
pipeline_sales_over_time = [
    {"$project": {
        "year_month": {"$substr": ["$timestamp", 0, 7]},  # Extract YYYY-MM
        "total": 1
    }},
    {"$group": {
        "_id": "$year_month",
        "total_revenue": {"$sum": "$total"}
    }},
    {"$sort": {"_id": 1}},
    {"$limit": 12}
]

result = list(db.transactions.aggregate(pipeline_sales_over_time))
client.close()

# Convert to DataFrame
df = pd.DataFrame(result).rename(columns={"_id": "year_month"})
df["total_revenue"] = df["total_revenue"].round(2)

# Plot line chart
plt.figure(figsize=(10, 6))
plt.plot(df["year_month"], df["total_revenue"], marker="o", color="royalblue")
plt.xlabel("Month (YYYY-MM)")
plt.ylabel("Total Revenue ($)")
plt.title("Sales Performance Over Time (Monthly Revenue)")
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.savefig("E:\\BigData FinalProject\\sales_over_time.png")
plt.show()