"""Online feature serving module.

Handles low-latency feature retrieval from Redis for real-time serving.
"""
from datetime import datetime
import json

from fastapi import APIRouter, HTTPException
import redis

from models import FeatureResponse

router = APIRouter(prefix="/features/online", tags=["online"])

# Redis client for online serving
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


@router.get("/{user_id}", response_model=FeatureResponse)
async def get_online_features(user_id: str):
    """Get latest features for a user from Redis (online serving).

    This endpoint provides low-latency access to the most recent features
    stored in Redis. Use this for real-time inference and online serving.

    Args:
        user_id: User ID to get features for

    Returns:
        FeatureResponse with user_id, as_of (current timestamp), features, and source="online"

    Raises:
        HTTPException: 404 if features not found for the user
    """
    key = f"features:{user_id}"
    features_json = redis_client.get(key)

    if features_json is None:
        raise HTTPException(
            status_code=404,
            detail=f"Features not found for user_id: {user_id}"
        )

    features = json.loads(features_json)

    return FeatureResponse(
        user_id=user_id,
        as_of=datetime.now().isoformat(),
        features=features,
        source="online"
    )

