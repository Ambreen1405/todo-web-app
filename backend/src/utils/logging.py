import logging
from pathlib import Path
import sys
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    logger_name: str = "todo_app"
) -> logging.Logger:
    """
    Set up logging configuration for the application
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level.upper())

    # Prevent adding handlers multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


from ..config import settings

# Global logger instance
logger = setup_logging(
    log_level=settings.log_level,
    log_file=settings.log_file
)