from sqlmodel import create_engine
from typing import Generator

# Import the settings to get database URL
from ..config import settings

# Import all models to ensure they're registered with SQLAlchemy
from ..models.user import User
from ..models.task import Task
from ..models.activity_log import ActivityLog

# Create the database engine
# Use Neon database URL if available, otherwise fall back to default database URL
database_url = str(settings.neon_database_url) if settings.neon_database_url else str(settings.database_url)
engine = create_engine(
    database_url,
    echo=settings.db_echo,  # Set to True to see SQL queries in logs
    pool_pre_ping=True,     # Verify connections before using them
)

def get_session() -> Generator:
    """
    Dependency to get database session for FastAPI endpoints
    """
    from sqlmodel import Session

    with Session(engine) as session:
        yield session