from fastapi import APIRouter
from datetime import datetime

router = APIRouter(tags=["Health"])


@router.get("/health")
def health_check():
    """
    Health check endpoint to verify the API is running
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Todo Application API"
    }


@router.get("/health/status")
def health_status():
    """
    Health check endpoint to verify the API is running
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Todo Application API"
    }