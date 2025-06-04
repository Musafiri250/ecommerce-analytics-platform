import json
import random
import uuid
import threading
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm
import os

# Initialize Faker and seeds
fake = Faker()
np.random.seed(42)
random.seed(42)
Faker.seed(42)

# Configuration
NUM_USERS = 10_000
NUM_PRODUCTS = 5_000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = 500_000
NUM_SESSIONS = 2_000_000
TIMESPAN_DAYS = 90
CHUNK_SIZE_SESSIONS = 100_000  # Save sessions every 100,000
CHUNK_SIZE_TRANSACTIONS = 50_000  # Save transactions every 50,000
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2  # Fail-safe
OUTPUT_DIR = "generated_data"

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- ID Generators ---
def generate_session_id():
    return f"sess_{uuid.uuid4().hex[:10]}"

def generate_transaction_id():
    return f"txn_{uuid.uuid4().hex[:12]}"

# --- Inventory Management ---
class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)

# --- Helper Functions ---
def determine_page_type(position, previous_pages):
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    prev_page = previous_pages[-1]["page_type"] if previous_pages else "home"
    transitions = {
        "home": (["category_listing", "search", "product_detail"], [0.5, 0.3, 0.2]),
        "category_listing": (["product_detail", "category_listing", "search", "home"], [0.7, 0.1, 0.1, 0.1]),
        "search": (["product_detail", "search", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
        "product_detail": (["product_detail", "cart", "category_listing", "search", "home"], [0.3, 0.3, 0.2, 0.1, 0.1]),
        "cart": (["checkout", "product_detail", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
        "checkout": (["confirmation", "cart", "home"], [0.8, 0.1, 0.1]),
        "confirmation": (["home", "product_detail", "category_listing"], [0.6, 0.2, 0.2])
    }
    options, weights = transitions.get(prev_page, (["home"], [1.0]))
    return random.choices(options, weights=weights)[0]

def get_page_content(page_type, products_list, categories_list, inventory):
    if page_type == "product_detail":
        attempts = 0
        while attempts < 10:
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                category_id = product["category_id"]
                category = next((c for c in categories_list if c["category_id"] == category_id), None)
                return product, category
            attempts += 1
        product = random.choice(products_list)
        category_id = product["category_id"]
        category = next((c for c in categories_list if c["category_id"] == category_id), None)
        return product, category
    elif page_type == "category_listing":
        return None, random.choice(categories_list)
    return None, None

# --- Category Generation ---
def generate_categories():
    categories = []
    for cat_id in tqdm(range(NUM_CATEGORIES), desc="Generating categories"):
        category = {
            "category_id": f"cat_{cat_id:03d}",
            "name": fake.company(),
            "subcategories": [
                {
                    "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
                    "name": fake.bs(),
                    "profit_margin": round(random.uniform(0.1, 0.4), 2)
                } for sub_id in range(random.randint(3, 5))
            ]
        }
        categories.append(category)
    with open(f"{OUTPUT_DIR}/categories.json", "w") as f:
        json.dump(categories, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))
    return categories

# --- Product Generation ---
def generate_products(categories):
    products = []
    product_creation_start = datetime.now() - timedelta(days=TIMESPAN_DAYS*2)
    for prod_id in tqdm(range(NUM_PRODUCTS), desc="Generating products"):
        category = random.choice(categories)
        base_price = round(random.uniform(5, 500), 2)
        price_history = []
        initial_date = fake.date_time_between(
            start_date=product_creation_start,
            end_date=product_creation_start + timedelta(days=TIMESPAN_DAYS//3)
        )
        price_history.append({"price": base_price, "date": initial_date.isoformat()})
        for _ in range(random.randint(0, 2)):
            price_change_date = fake.date_time_between(start_date=initial_date, end_date="now")
            new_price = round(base_price * random.uniform(0.8, 1.2), 2)
            price_history.append({"price": new_price, "date": price_change_date.isoformat()})
            initial_date = price_change_date
        price_history.sort(key=lambda x: x["date"])
        current_price = price_history[-1]["price"]
        products.append({
            "product_id": f"prod_{prod_id:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": category["category_id"],
            "base_price": current_price,
            "current_stock": random.randint(10, 1000),
            "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
            "price_history": price_history,
            "creation_date": price_history[0]["date"]
        })
    with open(f"{OUTPUT_DIR}/products.json", "w") as f:
        json.dump(products, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))
    return products

# --- User Generation ---
def generate_users():
    users = []
    for user_id in tqdm(range(NUM_USERS), desc="Generating users"):
        reg_date = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS*3}d", end_date=f"-{TIMESPAN_DAYS}d")
        users.append({
            "user_id": f"user_{user_id:06d}",
            "geo_data": {
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": fake.country_code()
            },
            "registration_date": reg_date.isoformat(),
            "last_active": fake.date_time_between(start_date=reg_date, end_date="now").isoformat()
        })
    with open(f"{OUTPUT_DIR}/users.json", "w") as f:
        json.dump(users, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))
    return users

# --- Session and Transaction Generation ---
def generate_sessions_and_transactions(users, products, categories):
    inventory = InventoryManager(products)
    session_chunk = []
    transaction_chunk = []
    session_counter = 0
    transaction_counter = 0
    iteration = 0
    converted_sessions = []

    print("Generating sessions and transactions...")
    with tqdm(total=NUM_SESSIONS + NUM_TRANSACTIONS, desc="Progress") as pbar:
        while (session_counter < NUM_SESSIONS or transaction_counter < NUM_TRANSACTIONS) and iteration < MAX_ITERATIONS:
            iteration += 1

            # Session Generation
            if session_counter < NUM_SESSIONS:
                user = random.choice(users)
                session_id = generate_session_id()
                session_start = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now")
                session_duration = random.randint(30, 3600)
                page_views = []
                viewed_products = set()
                cart_contents = {}
                time_slots = sorted([0] + [random.randint(1, session_duration-1) for _ in range(random.randint(3, 10))] + [session_duration])

                for i in range(len(time_slots)-1):
                    view_duration = time_slots[i+1] - time_slots[i]
                    page_type = determine_page_type(i, page_views)
                    product, category = get_page_content(page_type, products, categories, inventory)
                    if page_type == "product_detail" and product:
                        product_id = product["product_id"]
                        viewed_products.add(product_id)
                        if random.random() < 0.3:
                            if product_id not in cart_contents:
                                cart_contents[product_id] = {"quantity": 0, "price": product["base_price"]}
                            max_possible = min(3, inventory.get_product(product_id)["current_stock"] - cart_contents[product_id]["quantity"])
                            if max_possible > 0:
                                add_qty = random.randint(1, max_possible)
                                cart_contents[product_id]["quantity"] += add_qty
                    page_views.append({
                        "timestamp": (session_start + timedelta(seconds=time_slots[i])).isoformat(),
                        "page_type": page_type,
                        "product_id": product["product_id"] if product else None,
                        "category_id": category["category_id"] if category else None,
                        "view_duration": view_duration
                    })

                converted = False
                if cart_contents and any(p["page_type"] in ["checkout", "confirmation"] for p in page_views):
                    converted = random.random() < 0.7
                if converted:
                    converted_sessions.append(session_id)
                session_geo = user["geo_data"].copy()
                session_geo["ip_address"] = fake.ipv4()
                session_chunk.append({
                    "session_id": session_id,
                    "user_id": user["user_id"],
                    "start_time": session_start.isoformat(),
                    "end_time": (session_start + timedelta(seconds=session_duration)).isoformat(),
                    "duration_seconds": session_duration,
                    "geo_data": session_geo,
                    "device_profile": {
                        "type": random.choice(["mobile", "desktop", "tablet"]),
                        "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"])
                    },
                    "viewed_products": list(viewed_products),
                    "page_views": page_views,
                    "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
                    "conversion_status": "converted" if converted else "abandoned" if cart_contents else "browsed",
                    "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"])
                })
                session_counter += 1
                pbar.update(1)

                # Save session chunk
                if len(session_chunk) >= CHUNK_SIZE_SESSIONS:
                    chunk_index = session_counter // CHUNK_SIZE_SESSIONS
                    with open(f"{OUTPUT_DIR}/sessions_{chunk_index}.json", "w") as f:
                        json.dump(session_chunk, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))
                    session_chunk = []
                    print(f"Saved session chunk {chunk_index} ({session_counter:,} sessions)")

                # Transaction Generation from Session
                if converted and transaction_counter < NUM_TRANSACTIONS:
                    transaction_items = []
                    valid = True
                    for prod_id, details in cart_contents.items():
                        quantity = details["quantity"]
                        if quantity > 0:
                            if inventory.update_stock(prod_id, quantity):
                                transaction_items.append({
                                    "product_id": prod_id,
                                    "quantity": quantity,
                                    "unit_price": details["price"],
                                    "subtotal": round(quantity * details["price"], 2)
                                })
                            else:
                                valid = False
                                break
                    if valid and transaction_items:
                        subtotal = sum(item["subtotal"] for item in transaction_items)
                        discount = 0
                        if random.random() < 0.2:
                            discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                            discount = round(subtotal * discount_rate, 2)
                        total = round(subtotal - discount, 2)
                        transaction_chunk.append({
                            "transaction_id": generate_transaction_id(),
                            "session_id": session_id,
                            "user_id": user["user_id"],
                            "timestamp": (session_start + timedelta(seconds=session_duration)).isoformat(),
                            "items": transaction_items,
                            "subtotal": subtotal,
                            "discount": discount,
                            "total": total,
                            "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "crypto"]),
                            "status": "completed"
                        })
                        transaction_counter += 1
                        pbar.update(1)

            # Additional Transactions
            if transaction_counter < NUM_TRANSACTIONS and random.random() < 0.2:
                user = random.choice(users)
                products_in_txn = random.sample(products, k=min(3, len(products)))
                transaction_items = []
                valid = True
                for product in products_in_txn:
                    if product["is_active"]:
                        quantity = random.randint(1, 3)
                        if inventory.update_stock(product["product_id"], quantity):
                            transaction_items.append({
                                "product_id": product["product_id"],
                                "quantity": quantity,
                                "unit_price": product["base_price"],
                                "subtotal": round(quantity * product["base_price"], 2)
                            })
                        else:
                            valid = False
                            break
                if valid and transaction_items:
                    subtotal = sum(item["subtotal"] for item in transaction_items)
                    discount = 0
                    if random.random() < 0.2:
                        discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                        discount = round(subtotal * discount_rate, 2)
                    total = round(subtotal - discount, 2)
                    transaction_chunk.append({
                        "transaction_id": generate_transaction_id(),
                        "session_id": None,
                        "user_id": user["user_id"],
                        "timestamp": fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now").isoformat(),
                        "items": transaction_items,
                        "subtotal": subtotal,
                        "discount": discount,
                        "total": total,
                        "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "gift_card"]),
                        "status": random.choice(["completed", "processing", "shipped", "delivered"])
                    })
                    transaction_counter += 1
                    pbar.update(1)

            # Save transaction chunk
            if len(transaction_chunk) >= CHUNK_SIZE_TRANSACTIONS:
                chunk_index = transaction_counter // CHUNK_SIZE_TRANSACTIONS
                with open(f"{OUTPUT_DIR}/transactions_{chunk_index}.json", "w") as f:
                    json.dump(transaction_chunk, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))
                transaction_chunk = []
                print(f"Saved transaction chunk {chunk_index} ({transaction_counter:,} transactions)")

    # Save remaining chunks
    if session_chunk:
        chunk_index = session_counter // CHUNK_SIZE_SESSIONS
        with open(f"{OUTPUT_DIR}/sessions_{chunk_index}.json", "w") as f:
            json.dump(session_chunk, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))
        print(f"Saved final session chunk {chunk_index} ({session_counter:,} sessions)")

    if transaction_chunk:
        chunk_index = transaction_counter // CHUNK_SIZE_TRANSACTIONS
        with open(f"{OUTPUT_DIR}/transactions_{chunk_index}.json", "w") as f:
            json.dump(transaction_chunk, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))
        print(f"Saved final transaction chunk {chunk_index} ({transaction_counter:,} transactions)")

    # Save updated products
    with open(f"{OUTPUT_DIR}/products_updated.json", "w") as f:
        json.dump(list(inventory.products.values()), f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else TypeError(f"Type {type(o)} not serializable"))

    print(f"""
    Dataset generation complete!
    - Sessions: {session_counter:,}/{NUM_SESSIONS:,}
    - Transactions: {transaction_counter:,}/{NUM_TRANSACTIONS:,}
    - Users: {len(users):,}/{NUM_USERS:,}
    - Products: {len(products):,}/{NUM_PRODUCTS:,}
    - Categories: {len(categories):,}/{NUM_CATEGORIES:,}
    - Remaining products stock: {sum(p['current_stock'] for p in inventory.products.values()):,}
    """)

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting dataset generation...")
    users = generate_users()
    categories = generate_categories()
    products = generate_products(categories)
    generate_sessions_and_transactions(users, products, categories)