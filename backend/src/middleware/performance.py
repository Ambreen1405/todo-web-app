import time
import functools
from typing import Callable, Dict, Any
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from ..utils.log_config import logger


class PerformanceMiddleware(BaseHTTPMiddleware):
    """
    Middleware to monitor and log performance metrics for API requests
    """
    def __init__(self, app):
        super().__init__(app)
        self.metrics: Dict[str, Any] = {}

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # Process the request
        response = await call_next(request)

        # Calculate duration
        duration = time.time() - start_time

        # Log performance metrics
        logger.info(
            f"PERFORMANCE: {request.method} {request.url.path} "
            f"took {duration:.3f}s, status={response.status_code}"
        )

        # Add timing header to response (only in development)
        from ..config import settings
        if settings.debug:
            response.headers["X-Response-Time"] = f"{duration:.3f}s"

        # Track metrics
        self._track_metric(request.method, request.url.path, duration, response.status_code)

        return response

    def _track_metric(self, method: str, path: str, duration: float, status_code: int):
        """
        Track performance metrics
        """
        key = f"{method}_{path}"

        if key not in self.metrics:
            self.metrics[key] = {
                'count': 0,
                'total_duration': 0,
                'avg_duration': 0,
                'min_duration': float('inf'),
                'max_duration': 0,
                'status_codes': {}
            }

        metric = self.metrics[key]
        metric['count'] += 1
        metric['total_duration'] += duration
        metric['avg_duration'] = metric['total_duration'] / metric['count']
        metric['min_duration'] = min(metric['min_duration'], duration)
        metric['max_duration'] = max(metric['max_duration'], duration)

        # Track status codes
        if status_code not in metric['status_codes']:
            metric['status_codes'][status_code] = 0
        metric['status_codes'][status_code] += 1


def performance_monitor(func: Callable) -> Callable:
    """
    Decorator to monitor performance of specific functions
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start_time

            logger.info(f"Function {func.__name__} took {duration:.3f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Function {func.__name__} failed after {duration:.3f}s: {str(e)}")
            raise

    return wrapper