import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# Pipeline to aggregate revenue by category
pipeline_sales_by_category = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "total_revenue": {"$sum": "$items.subtotal"}
    }},
    {"$lookup": {
        "from": "products",
        "localField": "_id",
        "foreignField": "product_id",
        "as": "product"
    }},
    {"$unwind": "$product"},
    {"$lookup": {
        "from": "categories",
        "localField": "product.category_id",
        "foreignField": "category_id",
        "as": "category"
    }},
    {"$unwind": "$category"},
    {"$group": {
        "_id": "$category.name",
        "total_revenue": {"$sum": "$total_revenue"}
    }},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 10}
]

result = list(db.transactions.aggregate(pipeline_sales_by_category))
client.close()

# Convert to DataFrame
df = pd.DataFrame(result).rename(columns={"_id": "category_name"})
df["total_revenue"] = df["total_revenue"].round(2)

# Plot bar chart
plt.figure(figsize=(12, 6))
plt.bar(df["category_name"], df["total_revenue"], color="teal")
plt.xlabel("Category")
plt.ylabel("Total Revenue ($)")
plt.title("Sales Performance by Category (Top 10)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("E:\\BigData FinalProject\\sales_by_category.png")
plt.show()