import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connect to MongoDB for purchase data
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# Aggregate purchased quantities
pipeline_purchases = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "total_quantity": {"$sum": "$items.quantity"}
    }},
    {"$sort": {"total_quantity": -1}}
]

purchase_result = list(db.transactions.aggregate(pipeline_purchases))
client.close()

# Load co-view data
co_view_path = "E:\\BigData FinalProject\\generated_data\\co_viewed_products.parquet"
co_view_df = pd.read_parquet(co_view_path)

# Aggregate co-view counts per product
co_view_df["product"] = co_view_df[["product1", "product2"]].values.tolist()
co_view_flat = co_view_df.explode("product")[["product", "count"]]
co_view_agg = co_view_flat.groupby("product")["count"].sum().reset_index()

# Merge with purchase data
purchase_df = pd.DataFrame(purchase_result).rename(columns={"_id": "product", "total_quantity": "purchased_quantity"})
merged_df = co_view_agg.merge(purchase_df, on="product", how="inner").head(50)  # Top 50 for clarity

# Plot scatter chart
plt.figure(figsize=(10, 6))
plt.scatter(merged_df["count"], merged_df["purchased_quantity"], color="crimson", alpha=0.6)
plt.xlabel("Co-View Count")
plt.ylabel("Purchased Quantity")
plt.title("Co-View vs. Purchase Correlation")
for i, row in merged_df.iterrows():
    plt.annotate(row["product"], (row["count"], row["purchased_quantity"]), fontsize=8)
plt.grid(True)
plt.tight_layout()
plt.savefig("E:\\BigData FinalProject\\co_view_purchase_correlation.png")
plt.show()