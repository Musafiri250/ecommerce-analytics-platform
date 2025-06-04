import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# Run top-selling products pipeline
pipeline_top_products = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "total_quantity": {"$sum": "$items.quantity"},
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
    {"$project": {
        "product_id": "$_id",
        "product_name": "$product.name",
        "category_name": "$category.name",
        "total_quantity": 1,
        "total_revenue": 1
    }},
    {"$sort": {"total_revenue": -1}},  # Sort by revenue
    {"$limit": 10}
]

result = list(db.transactions.aggregate(pipeline_top_products))
client.close()

# Convert to DataFrame
df = pd.DataFrame(result)
top_products = df[['product_name', 'total_revenue']].head(10)

# Plot bar chart
plt.figure(figsize=(12, 6))
plt.bar(top_products['product_name'], top_products['total_revenue'], color='salmon')
plt.xlabel('Product Name')
plt.ylabel('Total Revenue ($)')
plt.title('Top 10 Products by Revenue')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('E:\\BigData FinalProject\\top_products.png')
plt.show()