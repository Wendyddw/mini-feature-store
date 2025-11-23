"""FastAPI service for feature serving.

Supports both offline (Iceberg) and online (Redis) feature retrieval.
"""
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

app = FastAPI(title="Feature Store API", version="1.0.0")

# Redis client for online serving
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

# Iceberg catalog for offline serving
catalog = load_catalog(
    name="spark_catalog",
    **{
        "type": "rest",
        "uri": "http://rest:8181",
        "warehouse": "s3://warehouse/",
    }
)


class FeatureResponse(BaseModel):
    """Feature response model."""
    user_id: str
    as_of: str
    features: dict
    source: str  # "online" or "offline"


@app.get("/features", response_model=FeatureResponse)
async def get_features(
    user_id: str,
    as_of: Optional[str] = None
):
    """Get features for a user.

    Args:
        user_id: User ID to get features for
        as_of: Timestamp for point-in-time query (ISO format).
               If None, returns latest from Redis (online).

    Returns:
        FeatureResponse with user_id, as_of, features, and source
    """
    if as_of is None:
        # Online: get from Redis
        key = f"features:{user_id}"
        features_json = redis_client.get(key)

        if features_json is None:
            raise HTTPException(
                status_code=404,
                detail=f"Features not found for user_id: {user_id}"
            )

        import json
        features = json.loads(features_json)

        return FeatureResponse(
            user_id=user_id,
            as_of=datetime.now().isoformat(),
            features=features,
            source="online"
        )
    else:
        # Offline: get from Iceberg with point-in-time query
        try:
            as_of_dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            as_of_date = as_of_dt.date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid as_of format: {as_of}. Use ISO format."
            )

        try:
            table = catalog.load_table("feature_store.features_daily")

            # Query: get features where day <= as_of_date, latest per user
            # This is simplified - in production you'd use proper Iceberg time travel
            scan = table.scan(
                row_filter=f"user_id = '{user_id}' AND day <= '{as_of_date}'"
            )

            rows = list(scan)
            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"Features not found for user_id: {user_id} at {as_of}"
                )

            # Get latest row (assuming sorted by day desc)
            latest_row = max(rows, key=lambda r: r["day"])

            features = {
                "day": str(latest_row["day"]),
                "event_count_7d": latest_row.get("event_count_7d"),
                "event_count_30d": latest_row.get("event_count_30d"),
                "last_event_days_ago": latest_row.get("last_event_days_ago"),
                "event_type_counts": latest_row.get("event_type_counts"),
            }

            return FeatureResponse(
                user_id=user_id,
                as_of=as_of,
                features=features,
                source="offline"
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error querying Iceberg: {str(e)}"
            )


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

