from pymongo import MongoClient
import pprint

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# Pipeline 1: Top-Selling Products
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
    {"$sort": {"total_quantity": -1}},
    {"$limit": 10}
]

print("Testing Top-Selling Products Pipeline:")
# Test unwind and group
unwind_results = db.transactions.aggregate([{"$unwind": "$items"}, {"$limit": 5}])
print("Sample unwound items:")
for doc in unwind_results:
    pprint.pprint(doc)
results_top_products = db.transactions.aggregate(pipeline_top_products)
print("\nTop 10 Products by Quantity Sold:")
for result in results_top_products:
    pprint.pprint(f"Product: {result['product_name']}, Category: {result['category_name']}, "
                  f"Quantity Sold: {result['total_quantity']}, Revenue: {result['total_revenue']}")

# Pipeline 2: User Segmentation
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
        "avg_transactions": {"$avg": "$total_transactions"},
        "avg_spent": {"$avg": "$total_spent"},
        "user_count": {"$sum": 1}
    }},
    {"$sort": {"user_count": -1}}
]

print("\nTesting User Segmentation Pipeline:")
# Test group by user_id
group_results = db.transactions.aggregate([{"$group": {"_id": "$user_id", "total_transactions": {"$sum": 1}}}, {"$limit": 5}])
print("Sample grouped transactions by user:")
for doc in group_results:
    pprint.pprint(doc)
results_user_segmentation = db.transactions.aggregate(pipeline_user_segmentation)
print("\nUser Segmentation by Country:")
for result in results_user_segmentation:
    pprint.pprint(f"Country: {result['_id']}, Users: {result['user_count']}, "
                  f"Avg Transactions: {result['avg_transactions']:.2f}, Avg Spent: {result['avg_spent']:.2f}")

client.close()