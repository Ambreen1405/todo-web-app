import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from sqlmodel import SQLModel, create_engine
# Import all models to ensure they're registered with SQLAlchemy
from src.models.user import User
from src.models.task import Task
from src.models.activity_log import ActivityLog

def create_db_and_tables():
    """
    Create database tables
    """
    # Use SQLite directly for initialization, ignoring environment variables
    engine = create_engine("sqlite:///./todo_app.db")
    print("Creating database tables...")
    SQLModel.metadata.create_all(engine)
    print("Database tables created successfully!")

if __name__ == "__main__":
    create_db_and_tables()