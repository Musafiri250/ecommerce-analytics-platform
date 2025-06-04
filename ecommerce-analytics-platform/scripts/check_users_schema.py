from pymongo import MongoClient
import pprint

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]
print("Sample user with all fields:")
pprint.pprint(db.users.find_one())
client.close()