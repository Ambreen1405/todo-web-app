"""
Error reporting middleware for capturing and reporting errors in the application
"""
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from ..utils.monitoring import monitoring_service
from ..utils.log_config import logger


class ErrorReportingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to capture and report errors
    """
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            # Capture the error with context
            monitoring_service.capture_error(
                error=e,
                endpoint=f"{request.method} {request.url.path}",
                request=request
            )

            # Re-raise the exception to be handled by the exception handlers
            raise


# Alternative decorator approach for specific functions
def error_reporting_decorator(func):
    """
    Decorator to add error reporting to specific functions
    """
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # Extract endpoint info if available
            endpoint = getattr(func, '__name__', 'unknown')
            monitoring_service.capture_error(
                error=e,
                endpoint=endpoint
            )
            raise
    return wrapper