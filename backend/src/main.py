from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import SQLModel

from .api.auth_router import router as auth_router
from .api.task_router import router as task_router
from .api.health_router import router as health_router
from .api.activities_router import router as activities_router
from .middleware.error_handler import (
    http_exception_handler,
    validation_exception_handler,
    general_exception_handler
)
from .config import settings
from .database.session import engine
from .utils.log_config import logger
from .utils.monitoring import monitoring_service
from .middleware.performance import PerformanceMiddleware




from .core.background_tasks import start_background_cleanup, stop_background_cleanup



def create_app():
    # Log application startup
    logger.info(f"Starting {settings.app_name} with debug={settings.debug}")

    app = FastAPI(
        title=settings.app_name,
        debug=settings.debug,
        version="1.0.0"
    )

    # Create all tables on startup
    logger.info("Initializing database...")
    try:
        SQLModel.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully!")

        # Start background cleanup tasks
        start_background_cleanup()

    except Exception as e:
        logger.error(f"Error during application startup: {e}")
        raise

    # Register shutdown handler
    def cleanup_on_exit():
        logger.info("Application shutting down...")
        try:
            from .core.background_tasks import stop_background_cleanup
            stop_background_cleanup()
            logger.info("Background tasks cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during application shutdown: {e}")

    import atexit
    atexit.register(cleanup_on_exit)

    # Add CORS middleware first (as per FastAPI requirements)
    # Parse allowed origins from settings (comma-separated string)
    allowed_origins = [origin.strip() for origin in settings.allowed_origins.split(",")]
    # Add localhost:3000 for development
    if "http://localhost:3000" not in allowed_origins:
        allowed_origins.append("http://localhost:3000")

    if settings.environment == "production":
        logger.warning(f"Running in production mode with allowed origins: {allowed_origins}")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add Error Reporting middleware
    from .middleware.error_reporting import ErrorReportingMiddleware
    app.add_middleware(ErrorReportingMiddleware)

    # Add Performance Monitoring middleware
    from .middleware.performance import PerformanceMiddleware
    app.add_middleware(PerformanceMiddleware)

    # Register exception handlers
    app.add_exception_handler(404, http_exception_handler)
    app.add_exception_handler(500, general_exception_handler)
    app.add_exception_handler(Exception, general_exception_handler)

    # Include routers
    app.include_router(auth_router)
    logger.info("Auth router included")

    app.include_router(task_router)
    logger.info("Task router included")

    app.include_router(health_router)
    logger.info("Health router included")

    app.include_router(activities_router)
    logger.info("Activities router included")

    @app.get("/")
    def read_root():
        logger.info("Root endpoint accessed")
        return {"message": "Todo Application API", "status": "running"}

    return app


app = create_app()
logger.info("Application created successfully")


if __name__ == "__main__":
    logger.info("Starting application server...")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)