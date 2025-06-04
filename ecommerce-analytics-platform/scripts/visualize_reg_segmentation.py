import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# Pipeline to segment users by registration month
pipeline_reg_segmentation = [
    {"$group": {
        "_id": "$user_id",
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
        "_id": {"$substr": ["$user.registration_date", 0, 7]},  # YYYY-MM
        "user_count": {"$sum": 1},
        "avg_spent": {"$avg": "$total_spent"}
    }},
    {"$sort": {"_id": 1}},
    {"$limit": 12}
]

result = list(db.transactions.aggregate(pipeline_reg_segmentation))
client.close()

# Convert to DataFrame
df = pd.DataFrame(result).rename(columns={"_id": "reg_month"})
df["avg_spent"] = df["avg_spent"].round(2)

# Plot dual-axis bar chart
fig, ax1 = plt.subplots(figsize=(12, 6))
ax1.bar(df["reg_month"], df["user_count"], color="purple", label="User Count")
ax1.set_xlabel("Registration Month (YYYY-MM)")
ax1.set_ylabel("User Count", color="purple")
ax1.tick_params(axis="y", labelcolor="purple")
plt.xticks(rotation=45)

ax2 = ax1.twinx()
ax2.plot(df["reg_month"], df["avg_spent"], color="gold", marker="o", label="Avg Spending")
ax2.set_ylabel("Average Spending ($)", color="gold")
ax2.tick_params(axis="y", labelcolor="gold")

plt.title("Customer Segmentation by Registration Month")
fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.9))
plt.tight_layout()
plt.savefig("E:\\BigData FinalProject\\customer_segmentation_reg_month.png")
plt.show()