from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]

# Check document counts
print("Document counts:")
print(f"Users: {db.users.count_documents({})}")
print(f"Products: {db.products.count_documents({})}")
print(f"Transactions: {db.transactions.count_documents({})}")

# Sample a document from each collection
print("\nSample user:")
print(db.users.find_one())
print("\nSample product:")
print(db.products.find_one())
print("\nSample transaction:")
print(db.transactions.find_one())

client.close()