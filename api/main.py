"""FastAPI service for feature serving.

Supports both offline (Iceberg) and online (Redis) feature retrieval.
"""
from fastapi import FastAPI

from online import router as online_router
from offline import router as offline_router

app = FastAPI(
    title="Feature Store API",
    version="1.0.0",
    description="Feature serving API with separate endpoints for online and offline serving"
)

# Include routers for online and offline serving
app.include_router(online_router)
app.include_router(offline_router)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

