"""
Monitoring and error reporting utilities for the todo-web-app backend
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import traceback
from functools import wraps

from fastapi import Request
from pydantic import BaseModel

from .logging import logger


class ErrorReport(BaseModel):
    """Model for error reports"""
    timestamp: datetime
    error_type: str
    message: str
    traceback: str
    endpoint: Optional[str] = None
    user_id: Optional[str] = None
    request_details: Optional[Dict[str, Any]] = None


class MonitoringService:
    """Service class for monitoring and error reporting"""

    def __init__(self):
        self.logger = logger

    def capture_error(self, error: Exception, endpoint: Optional[str] = None,
                     user_id: Optional[str] = None, request: Optional[Request] = None):
        """Capture and log an error with additional context"""
        try:
            error_report = ErrorReport(
                timestamp=datetime.utcnow(),
                error_type=type(error).__name__,
                message=str(error),
                traceback=traceback.format_exc(),
                endpoint=endpoint,
                user_id=user_id,
                request_details=self._extract_request_details(request) if request else None
            )

            # Log the error
            self.logger.error(
                f"Error captured: {error_report.error_type} - {error_report.message}",
                extra={
                    "error_type": error_report.error_type,
                    "endpoint": error_report.endpoint,
                    "user_id": error_report.user_id,
                    "request_details": error_report.request_details
                }
            )

            # In a real implementation, you would send this to an external service like Sentry
            # self._send_to_external_monitoring(error_report)

            return error_report
        except Exception as e:
            # If error reporting fails, at least log that
            self.logger.error(f"Failed to capture error: {e}")

    def _extract_request_details(self, request: Request) -> Dict[str, Any]:
        """Extract relevant details from the request"""
        try:
            return {
                "method": request.method,
                "url": str(request.url),
                "headers": dict(request.headers),
                "client": request.client.host if request.client else None
            }
        except Exception:
            return {}

    def _send_to_external_monitoring(self, error_report: ErrorReport):
        """Send error report to external monitoring service (placeholder)"""
        # This would integrate with services like Sentry, DataDog, etc.
        # For now, this is a placeholder implementation
        pass

    def monitor_performance(self, func):
        """Decorator to monitor function performance"""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            try:
                result = await func(*args, **kwargs) if hasattr(func, '__await__') else func(*args, **kwargs)
                execution_time = (datetime.utcnow() - start_time).total_seconds()

                if execution_time > 1.0:  # Log slow operations (>1 second)
                    self.logger.warning(
                        f"Slow operation detected: {func.__name__} took {execution_time:.2f}s",
                        extra={
                            "function": func.__name__,
                            "execution_time": execution_time
                        }
                    )

                return result
            except Exception as e:
                execution_time = (datetime.utcnow() - start_time).total_seconds()
                self.capture_error(e, endpoint=func.__name__)
                raise
        return wrapper


# Global monitoring service instance
monitoring_service = MonitoringService()


def get_monitoring_service():
    """Get the global monitoring service instance"""
    return monitoring_service