"""FastAPI service for online feature serving.

Primary use case: Real-time feature retrieval from Redis for production model serving.

Note: Offline features (Iceberg) should be accessed programmatically via Spark/SQL
for training data generation and batch inference, not via REST API. The offline
endpoint is provided for debugging/exploration only.
"""
from fastapi import FastAPI

from online import router as online_router
from offline import router as offline_router  # Debugging/exploration only

app = FastAPI(
    title="Feature Store API",
    version="1.0.0",
    description="Online feature serving API for real-time inference. Offline features should be accessed via Spark/SQL."
)

# Include routers
app.include_router(online_router)  # Production endpoint for real-time serving
app.include_router(offline_router)  # Development/debugging only


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

