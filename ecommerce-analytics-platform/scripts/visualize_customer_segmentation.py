import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# User segmentation pipeline (from Aggregation.py)
pipeline_user_segmentation = [
    {"$group": {
        "_id": "$user_id",
        "total_transactions": {"$sum": 1},
        "total_spent": {"$sum": "$total"}
    }},
    {"$lookup": {
        "from": "users",
        "localField": "_id",
        "foreignField": "user_id",
        "as": "user"
    }},
    {"$unwind": "$user"},
    {"$group": {
        "_id": "$user.geo_data.country",
        "avg_spent": {"$avg": "$total_spent"},
        "user_count": {"$sum": 1}
    }},
    {"$sort": {"user_count": -1}},
    {"$limit": 10}
]

result = list(db.transactions.aggregate(pipeline_user_segmentation))
client.close()

# Convert to DataFrame
df = pd.DataFrame(result).rename(columns={"_id": "country"})
df["avg_spent"] = df["avg_spent"].round(2)

# Plot dual-axis bar chart
fig, ax1 = plt.subplots(figsize=(12, 6))
ax1.bar(df["country"], df["user_count"], color="skyblue", label="User Count")
ax1.set_xlabel("Country")
ax1.set_ylabel("User Count", color="skyblue")
ax1.tick_params(axis="y", labelcolor="skyblue")
plt.xticks(rotation=45)

ax2 = ax1.twinx()
ax2.plot(df["country"], df["avg_spent"], color="darkorange", marker="o", label="Avg Spending")
ax2.set_ylabel("Average Spending ($)", color="darkorange")
ax2.tick_params(axis="y", labelcolor="darkorange")

plt.title("Customer Segmentation: Top 10 Countries by User Count and Avg Spending")
fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.9))
plt.tight_layout()
plt.savefig("E:\\BigData FinalProject\\customer_segmentation_country.png")
plt.show()