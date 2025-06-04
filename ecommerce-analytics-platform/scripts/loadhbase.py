import json
import os
import glob
import happybase
from tqdm import tqdm
from datetime import datetime

# Configuration
OUTPUT_DIR = "generated_data"  # Directory where session JSON files are stored
HBASE_HOST = "localhost"  # Adjust if using a remote or Dockerized HBase
HBASE_PORT = 9090  # Default Thrift port for HBase
TABLE_NAME = "sessions"
CHUNK_SIZE = 10000  # Number of rows to batch insert
SESSION_FILES = os.path.join(OUTPUT_DIR, "sessions_*.json")

# Connect to HBase
connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)

def load_json_file(filepath):
    """Load a JSON file and return its contents."""
    with open(filepath, "r") as f:
        return json.load(f)

def get_reverse_timestamp(timestamp_str):
    """Convert timestamp to reverse timestamp for row key sorting."""
    max_long = 2**63 - 1  # Max value for a 64-bit long
    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    timestamp_ms = int(dt.timestamp() * 1000)
    return max_long - timestamp_ms

def load_sessions_to_hbase():
    """Load session data from JSON files into existing HBase table."""
    table = connection.table(TABLE_NAME)
    session_files = glob.glob(SESSION_FILES)
    seen_row_keys = set()  # To avoid duplicate row keys

    for file in session_files:
        print(f"Processing file: {file}")
        data = load_json_file(file)
        
        batch = table.batch(batch_size=CHUNK_SIZE)
        for doc in tqdm(data, desc=f"Loading {file} into HBase"):
            session_id = doc.get("session_id")
            user_id = doc.get("user_id")
            start_time = doc.get("start_time")
            
            # Create row key: user_id:reverse_timestamp
            reverse_ts = get_reverse_timestamp(start_time)
            row_key = f"{user_id}:{reverse_ts}"
            
            if row_key in seen_row_keys:
                print(f"Skipping duplicate row key: {row_key}")
                continue
            seen_row_keys.add(row_key)
            
            # Prepare data for HBase
            data = {
                b'info:session_id': session_id.encode(),
                b'info:start_time': start_time.encode(),
                b'info:end_time': doc.get("end_time").encode(),
                b'info:duration_seconds': str(doc.get("duration_seconds")).encode(),
                b'info:conversion_status': doc.get("conversion_status").encode(),
                b'info:referrer': doc.get("referrer").encode(),
                b'geo:city': doc.get("geo_data", {}).get("city", "").encode(),
                b'geo:state': doc.get("geo_data", {}).get("state", "").encode(),
                b'geo:country': doc.get("geo_data", {}).get("country", "").encode(),
                b'geo:ip_address': doc.get("geo_data", {}).get("ip_address", "").encode(),
                b'device:type': doc.get("device_profile", {}).get("type", "").encode(),
                b'device:os': doc.get("device_profile", {}).get("os", "").encode(),
                b'device:browser': doc.get("device_profile", {}).get("browser", "").encode(),
                b'views:viewed_products': json.dumps(doc.get("viewed_products", [])).encode(),
                b'views:page_views': json.dumps(doc.get("page_views", [])).encode(),
                b'cart:cart_contents': json.dumps(doc.get("cart_contents", {})).encode()
            }
            
            # Add to batch
            batch.put(row_key.encode(), data)
        
        # Commit batch
        try:
            batch.send()
            print(f"Loaded {file} into HBase table {TABLE_NAME} ({len(data):,} sessions)")
        except Exception as e:
            print(f"Error loading {file} into HBase: {e}")
            raise

def verify_data():
    """Verify the number of rows loaded into HBase."""
    table = connection.table(TABLE_NAME)
    row_count = sum(1 for _ in table.scan())
    print(f"Total rows in HBase table {TABLE_NAME}: {row_count:,}")

def main():
    print("Starting session data loading into HBase...")
    
    # Load session data into existing table
    load_sessions_to_hbase()
    
    # Verify data
    verify_data()
    
    print("Session data loading complete!")

if __name__ == "__main__":
    try:
        main()
    finally:
        connection.close()