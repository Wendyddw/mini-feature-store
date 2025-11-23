#!/usr/bin/env python3
"""Generate sample data for feature store demo."""
import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# Check if pandas is installed
try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas pyarrow")
    sys.exit(1)

# Create output directories
os.makedirs("/tmp/events_raw", exist_ok=True)
os.makedirs("/tmp/labels", exist_ok=True)

# Generate events_raw data
events = []
start_date = datetime(2024, 1, 1)
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
events_df.to_parquet("/tmp/events_raw/events.parquet", index=False)
print(f"Generated {len(events)} events")

# Generate labels data
labels = []
for user_id in ["user1", "user2", "user3"]:
    # Create labels at different timestamps
    for day in [2, 4, 6]:
        as_of_ts = start_date + timedelta(days=day, hours=12)
        label = 1.0 if day % 2 == 0 else 0.0
        labels.append({
            "user_id": user_id,
            "label": label,
            "as_of_ts": as_of_ts.isoformat()
        })

labels_df = pd.DataFrame(labels)
labels_df["as_of_ts"] = pd.to_datetime(labels_df["as_of_ts"])
labels_df.to_parquet("/tmp/labels/labels.parquet", index=False)
print(f"Generated {len(labels)} labels")

print("Sample data generation complete!")

