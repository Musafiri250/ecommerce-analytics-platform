import json
import os
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm
import glob

# Configuration
OUTPUT_DIR = "generated_data"  # Directory where dataset files are stored
CHUNK_SIZE = 10_000  # Number of documents to insert per batch
DB_NAME = "ecommerce_analytics"
COLLECTIONS = {
    "users": "users.json",
    "categories": "categories.json",
    "products": "products_updated.json",
    "sessions": "sessions_*.json",
    "transactions": "transactions_*.json"
}

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]

def load_json_file(filepath):
    """Load a JSON file and return its contents."""
    with open(filepath, "r") as f:
        return json.load(f)

def load_chunked_data(file_pattern, collection_name):
    """Load data from multiple JSON files, checking for duplicates in sessions."""
    files = glob.glob(file_pattern)
    collection = db[collection_name]
    seen_session_ids = set() if collection_name == "sessions" else None

    for file in files:
        data = load_json_file(file)
        filtered_data = []
        
        if collection_name == "sessions":
            # Check for duplicate session_ids
            for doc in data:
                session_id = doc.get("session_id")
                if session_id not in seen_session_ids:
                    seen_session_ids.add(session_id)
                    filtered_data.append(doc)
                else:
                    print(f"Skipping duplicate session_id: {session_id} in {file}")
        else:
            filtered_data = data

        for i in tqdm(range(0, len(filtered_data), CHUNK_SIZE), desc=f"Loading {file} into {collection_name}"):
            chunk = filtered_data[i:i + CHUNK_SIZE]
            if chunk:
                try:
                    collection.insert_many(chunk, ordered=False)
                except DuplicateKeyError as e:
                    print(f"Duplicate key error in {file}: {e}. Skipping duplicates.")
                    for doc in chunk:
                        try:
                            collection.insert_one(doc)
                        except DuplicateKeyError:
                            print(f"Skipped individual duplicate in {file}: {doc.get('session_id') or doc.get('transaction_id')}")
        print(f"Loaded {file} into {collection_name} ({len(filtered_data):,} documents)")

def create_indexes():
    """Create indexes with error handling."""
    try:
        db.users.create_index("user_id", unique=True)
        print("Created index on users.user_id")
    except Exception as e:
        print(f"Failed to create index on users.user_id: {e}")

    try:
        db.products.create_index("product_id", unique=True)
        db.products.create_index("category_id")
        print("Created indexes on products.product_id, products.category_id")
    except Exception as e:
        print(f"Failed to create product indexes: {e}")

    try:
        db.categories.create_index("category_id", unique=True)
        print("Created index on categories.category_id")
    except Exception as e:
        print(f"Failed to create index on categories.category_id: {e}")

    try:
        db.sessions.create_index("session_id", unique=True)
        db.sessions.create_index("user_id")
        db.sessions.create_index("start_time")
        print("Created indexes on sessions.session_id, sessions.user_id, sessions.start_time")
    except Exception as e:
        print(f"Failed to create session indexes: {e}. Ensure no duplicate session_ids.")

    try:
        db.transactions.create_index("transaction_id", unique=True)
        db.transactions.create_index("user_id")
        db.transactions.create_index("session_id")
        db.transactions.create_index("timestamp")
        print("Created indexes on transactions.transaction_id, transactions.user_id, transactions.session_id, transactions.timestamp")
    except Exception as e:
        print(f"Failed to create transaction indexes: {e}")

def main():
    print("Starting data loading into MongoDB...")

    # Drop existing collections to start fresh
    for collection_name in COLLECTIONS.keys():
        db[collection_name].drop()
        print(f"Dropped collection {collection_name}")

    # Load single-file collections
    for collection_name, filename in COLLECTIONS.items():
        if collection_name in ["users", "categories", "products"]:
            filepath = os.path.join(OUTPUT_DIR, filename)
            if os.path.exists(filepath):
                data = load_json_file(filepath)
                collection = db[collection_name]
                for i in tqdm(range(0, len(data), CHUNK_SIZE), desc=f"Loading {collection_name}"):
                    chunk = data[i:i + CHUNK_SIZE]
                    if chunk:
                        try:
                            collection.insert_many(chunk, ordered=False)
                        except DuplicateKeyError as e:
                            print(f"Duplicate key error in {collection_name}: {e}. Inserting individually.")
                            for doc in chunk:
                                try:
                                    collection.insert_one(doc)
                                except DuplicateKeyError:
                                    print(f"Skipped duplicate in {collection_name}: {doc.get('user_id') or doc.get('product_id') or doc.get('category_id')}")
                print(f"Loaded {collection_name} ({len(data):,} documents)")
            else:
                print(f"File {filepath} not found")

    # Load chunked session and transaction files
    load_chunked_data(os.path.join(OUTPUT_DIR, COLLECTIONS["sessions"]), "sessions")
    load_chunked_data(os.path.join(OUTPUT_DIR, COLLECTIONS["transactions"]), "transactions")

    # Create indexes
    create_indexes()

    # Verify document counts
    print("\nFinal document counts:")
    for collection_name in COLLECTIONS.keys():
        count = db[collection_name].count_documents({})
        print(f"{collection_name}: {count:,} documents")

    print("Data loading complete!")

if __name__ == "__main__":
    try:
        main()
    finally:
        client.close()