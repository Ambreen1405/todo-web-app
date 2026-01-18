import logging.config
import os
from ..config import settings

def setup_comprehensive_logging():
    """
    Set up comprehensive logging configuration for the application
    """
    # Create logs directory if it doesn't exist
    if settings.log_file:
        log_dir = os.path.dirname(settings.log_file)
        os.makedirs(log_dir, exist_ok=True)

    # Define the logging configuration
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(levelprefix)s %(asctime)s %(message)s",
                "use_colors": None,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(levelprefix)s %(asctime)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            },
            "detailed": {
                "format": "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "handlers": {
            "console": {
                "formatter": "detailed",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout"
            },
            "file": {
                "formatter": "detailed",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": settings.log_file or "logs/app.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "encoding": "utf8"
            },
            "error_file": {
                "formatter": "detailed",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": settings.log_file.replace(".log", "_errors.log") if settings.log_file else "logs/errors.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "encoding": "utf8",
                "level": "ERROR"
            }
        },
        "root": {
            "level": settings.log_level,
            "handlers": ["console", "file", "error_file"]
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["console"],
                "level": settings.log_level,
            },
            "uvicorn.error": {
                "level": settings.log_level,
            },
            "uvicorn.access": {
                "handlers": ["console"],
                "level": settings.log_level,
                "propagate": False,
            },
        }
    }

    # Apply the logging configuration
    logging.config.dictConfig(LOGGING_CONFIG)

    # Get the root logger
    logger = logging.getLogger(__name__)
    logger.info("Logging configuration applied successfully")

    return logger

# Initialize logging
logger = setup_comprehensive_logging()