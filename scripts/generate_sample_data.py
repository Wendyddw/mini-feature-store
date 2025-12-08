#!/usr/bin/env python3
"""Generate sample data for feature store demo."""
import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Check if pandas is installed
try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("Error: pandas and pyarrow are required. Install with: pip install pandas pyarrow")
    sys.exit(1)

# Create output directories
os.makedirs("/tmp/events_raw", exist_ok=True)
os.makedirs("/tmp/labels", exist_ok=True)

# Generate events_raw data - use recent dates (7 days ago to today)
# This ensures data is within the online-sync window
events = []
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = today - timedelta(days=6)  # Start 6 days ago, so we have 7 days total
for day in range(7):  # 7 days of data
    date = start_date + timedelta(days=day)
    for user_id in ["user1", "user2", "user3"]:
        # Generate 1-3 events per user per day
        num_events = (day % 3) + 1
        for i in range(num_events):
            event_type = ["click", "purchase", "view"][i % 3]
            ts = date + timedelta(hours=10 + i)
            events.append({
                "user_id": user_id,
                "event_type": event_type,
                "ts": ts.isoformat()
            })

events_df = pd.DataFrame(events)
events_df["ts"] = pd.to_datetime(events_df["ts"])

# Convert to PyArrow Table with explicit timestamp schema for Spark compatibility
table = pa.Table.from_pandas(events_df)
# Ensure timestamp column uses timestamp[us] type (microsecond precision)
schema = pa.schema([
    pa.field("user_id", pa.string()),
    pa.field("event_type", pa.string()),
    pa.field("ts", pa.timestamp("us"))  # Explicit timestamp[us] type
])
table = table.cast(schema)

# Write Parquet file
pq.write_table(table, "/tmp/events_raw/events.parquet")
print(f"Generated {len(events)} events")

# Generate labels data - use recent dates matching events
labels = []
for user_id in ["user1", "user2", "user3"]:
    # Create labels at different timestamps (days 2, 4, 6 relative to start_date)
    for day_offset in [2, 4, 6]:
        as_of_ts = start_date + timedelta(days=day_offset, hours=12)
        label = 1.0 if day % 2 == 0 else 0.0
        labels.append({
            "user_id": user_id,
            "label": label,
            "as_of_ts": as_of_ts.isoformat()
        })

labels_df = pd.DataFrame(labels)
labels_df["as_of_ts"] = pd.to_datetime(labels_df["as_of_ts"])

# Convert to PyArrow Table with explicit timestamp schema for Spark compatibility
table = pa.Table.from_pandas(labels_df)
# Ensure timestamp column uses timestamp[us] type (microsecond precision)
schema = pa.schema([
    pa.field("user_id", pa.string()),
    pa.field("label", pa.float64()),
    pa.field("as_of_ts", pa.timestamp("us"))  # Explicit timestamp[us] type
])
table = table.cast(schema)

# Write Parquet file
pq.write_table(table, "/tmp/labels/labels.parquet")
print(f"Generated {len(labels)} labels")

print("Sample data generation complete!")

