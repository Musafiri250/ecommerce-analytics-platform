import happybase
from datetime import datetime, timedelta
import json

# Configuration
HBASE_HOST = "localhost"  # Adjust if using a remote or Dockerized HBase
HBASE_PORT = 9090  # Default Thrift port for HBase
TABLE_NAME = "sessions"

# Connect to HBase
connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)

def get_reverse_timestamp(timestamp_str):
    """Convert timestamp to reverse timestamp for row key filtering."""
    max_long = 2**63 - 1  # Max value for a 64-bit long
    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    timestamp_ms = int(dt.timestamp() * 1000)
    return max_long - timestamp_ms

def query_user_sessions(user_id):
    """Retrieve all sessions for a given user_id."""
    table = connection.table(TABLE_NAME)
    rows = table.scan(row_prefix=user_id.encode())
    
    sessions = []
    for key, data in rows:
        session = {
            "row_key": key.decode(),
            "session_id": data.get(b'info:session_id', b'').decode(),
            "start_time": data.get(b'info:start_time', b'').decode(),
            "end_time": data.get(b'info:end_time', b'').decode(),
            "duration_seconds": int(data.get(b'info:duration_seconds', b'0').decode()),
            "conversion_status": data.get(b'info:conversion_status', b'').decode(),
            "referrer": data.get(b'info:referrer', b'').decode(),
            "geo_data": {
                "city": data.get(b'geo:city', b'').decode(),
                "state": data.get(b'geo:state', b'').decode(),
                "country": data.get(b'geo:country', b'').decode(),
                "ip_address": data.get(b'geo:ip_address', b'').decode()
            },
            "device_profile": {
                "type": data.get(b'device:type', b'').decode(),
                "os": data.get(b'device:os', b'').decode(),
                "browser": data.get(b'device:browser', b'').decode()
            },
            "viewed_products": json.loads(data.get(b'views:viewed_products', b'[]').decode()),
            "page_views": json.loads(data.get(b'views:page_views', b'[]').decode()),
            "cart_contents": json.loads(data.get(b'cart:cart_contents', b'{}').decode())
        }
        sessions.append(session)
    
    return sessions

def query_user_sessions_time_range(user_id, start_date, end_date):
    """Retrieve sessions for a user within a specific time range."""
    table = connection.table(TABLE_NAME)
    start_reverse_ts = get_reverse_timestamp(end_date)  # End date is earlier in reverse time
    end_reverse_ts = get_reverse_timestamp(start_date)  # Start date is later in reverse time
    start_row = f"{user_id}:{start_reverse_ts}"
    end_row = f"{user_id}:{end_reverse_ts + 1}"  # Exclusive end
    
    rows = table.scan(row_start=start_row.encode(), row_stop=end_row.encode())
    
    sessions = []
    for key, data in rows:
        session = {
            "row_key": key.decode(),
            "session_id": data.get(b'info:session_id', b'').decode(),
            "start_time": data.get(b'info:start_time', b'').decode(),
            "end_time": data.get(b'info:end_time', b'').decode(),
            "duration_seconds": int(data.get(b'info:duration_seconds', b'0').decode()),
            "conversion_status": data.get(b'info:conversion_status', b'').decode(),
            "referrer": data.get(b'info:referrer', b'').decode()
        }
        sessions.append(session)
    
    return sessions

def main():
    # Example 1: Retrieve all sessions for a specific user
    user_id = "user_000042"
    print(f"\nQuerying all sessions for {user_id}")
    sessions = query_user_sessions(user_id)
    for session in sessions[:5]:  # Limit to 5 for brevity
        print(f"Session ID: {session['session_id']}, Start Time: {session['start_time']}, "
              f"Conversion Status: {session['conversion_status']}")
    print(f"Total sessions found: {len(sessions)}")

    # Example 2: Retrieve sessions within a time range
    end_date = "2025-03-31T23:59:59"
    start_date = "2025-03-01T00:00:00"
    print(f"\nQuerying sessions for {user_id} between {start_date} and {end_date}")
    sessions = query_user_sessions_time_range(user_id, start_date, end_date)
    for session in sessions:
        print(f"Session ID: {session['session_id']}, Start Time: {session['start_time']}, "
              f"Conversion Status: {session['conversion_status']}")
    print(f"Total sessions in time range: {len(sessions)}")

if __name__ == "__main__":
    try:
        main()
    finally:
        connection.close()