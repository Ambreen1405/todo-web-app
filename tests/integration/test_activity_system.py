#!/usr/bin/env python3
"""
Test script to verify the Recent Activity system implementation
"""
import os
import sys
import asyncio
from datetime import datetime
from sqlmodel import Session, select

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend', 'src'))

from backend.src.config import settings
from sqlmodel import create_engine
from sqlmodel import SQLModel  # This is needed to register the models

# Import all models to register them with SQLModel metadata
from backend.src.models.user import User
from backend.src.models.task import Task
from backend.src.models.activity_log import ActivityLog, ActivityActionEnum

# Create engine using the same configuration as the app
# Models must be imported before creating the engine to register them
engine = create_engine(str(settings.database_url))

from backend.src.utils.activity_logger import log_activity, get_recent_activities_for_user

def test_activity_system():
    print("Testing Recent Activity System...")

    # Create a database session
    with Session(engine) as db:
        # Get a test user (assuming there's at least one user in the database)
        user = db.exec(select(User).limit(1)).first()
        if not user:
            print("No users found in database. Creating a test user...")
            # Create a test user
            test_user = User(
                email="test@example.com",
                name="Test User",
                hashed_password="hashed_test_password"
            )
            db.add(test_user)
            db.commit()
            db.refresh(test_user)
            user = test_user
            print(f"Created test user with ID: {user.id}")

        print(f"Using user ID: {user.id}")

        # Test logging different activities
        print("\n1. Testing task creation activity logging...")
        success = log_activity(
            db=db,
            user_id=user.id,
            action=ActivityActionEnum.TASK_CREATED,
            task_id=1,
            task_title="Test Task 1"
        )
        print(f"Task creation activity logged: {success}")

        print("\n2. Testing task completion activity logging...")
        success = log_activity(
            db=db,
            user_id=user.id,
            action=ActivityActionEnum.TASK_COMPLETED,
            task_id=1,
            task_title="Test Task 1"
        )
        print(f"Task completion activity logged: {success}")

        print("\n3. Testing task update activity logging...")
        success = log_activity(
            db=db,
            user_id=user.id,
            action=ActivityActionEnum.TASK_UPDATED,
            task_id=1,
            task_title="Test Task 1 (Updated)"
        )
        print(f"Task update activity logged: {success}")

        print("\n4. Testing task deletion activity logging...")
        success = log_activity(
            db=db,
            user_id=user.id,
            action=ActivityActionEnum.TASK_DELETED,
            task_id=1,
            task_title="Test Task 1 (Deleted)"
        )
        print(f"Task deletion activity logged: {success}")

        print("\n5. Testing retrieval of recent activities...")
        recent_activities = get_recent_activities_for_user(
            db=db,
            user_id=user.id,
            hours_back=24,
            limit=20
        )

        print(f"Found {len(recent_activities)} recent activities:")
        for i, activity in enumerate(recent_activities):
            print(f"  {i+1}. Action: {activity.action.value}, Task: '{activity.task_title}', Time: {activity.created_at}")

        print("\n6. Testing manual cleanup of old activities...")
        from backend.src.utils.activity_logger import cleanup_old_activities
        deleted_count = cleanup_old_activities(db, hours_old=24)
        print(f"Manually cleaned up {deleted_count} old activities (may be 0 if none were older than 24 hours)")

        print("\nRecent Activity System test completed successfully!")

if __name__ == "__main__":
    test_activity_system()