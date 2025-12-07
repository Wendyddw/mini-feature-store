"""Offline feature serving module.

Handles historical feature retrieval from Iceberg for training and batch inference.
"""
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from pyiceberg.catalog import load_catalog

from models import FeatureResponse

router = APIRouter(prefix="/features/offline", tags=["offline"])

# Iceberg catalog for offline serving
catalog = load_catalog(
    name="spark_catalog",
    **{
        "type": "rest",
        "uri": "http://rest:8181",
        "warehouse": "s3://warehouse/",
    }
)


@router.get("/{user_id}", response_model=FeatureResponse)
async def get_offline_features(
    user_id: str,
    as_of: str = Query(..., description="Timestamp for point-in-time query (ISO format)")
):
    """Get historical features for a user from Iceberg (offline serving).

    This endpoint provides point-in-time feature retrieval from the Iceberg
    data lake. Use this for training data generation, batch inference, and
    historical analysis.

    Args:
        user_id: User ID to get features for
        as_of: Timestamp for point-in-time query (ISO format, e.g., "2024-01-05T12:00:00")

    Returns:
        FeatureResponse with user_id, as_of, features, and source="offline"

    Raises:
        HTTPException: 400 if as_of format is invalid
        HTTPException: 404 if features not found for the user at the specified time
        HTTPException: 500 if error querying Iceberg
    """
    try:
        as_of_dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
        as_of_date = as_of_dt.date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid as_of format: {as_of}. Use ISO format (e.g., '2024-01-05T12:00:00')."
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying Iceberg: {str(e)}"
        )

