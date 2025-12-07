"""Shared models for the feature serving API."""
from pydantic import BaseModel


class FeatureResponse(BaseModel):
    """Feature response model."""
    user_id: str
    as_of: str
    features: dict
    source: str  # "online" or "offline"

