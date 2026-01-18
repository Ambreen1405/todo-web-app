from sqlmodel import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
from typing import Optional
import logging

from ..models.activity_log import ActivityLog, ActivityActionEnum
from ..models.task import Task

logger = logging.getLogger(__name__)


def log_activity(
    db: Session,
    user_id: int,
    action: ActivityActionEnum,
    task_id: Optional[int] = None,
    task_title: Optional[str] = None
) -> bool:
    """
    Log a user activity to the database

    Args:
        db: Database session
        user_id: ID of the user performing the action
        action: Type of action (ActivityActionEnum)
        task_id: ID of the task involved (if applicable)
        task_title: Title of the task involved (if applicable)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        activity = ActivityLog(
            user_id=user_id,
            action=action,
            task_id=task_id,
            task_title=task_title or ""
        )

        db.add(activity)
        db.commit()
        db.refresh(activity)

        logger.info(f"Activity logged: user_id={user_id}, action={action.value}, task_id={task_id}")
        return True

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error logging activity: {str(e)}")
        return False
    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error logging activity: {str(e)}")
        return False


def cleanup_old_activities(db: Session, hours_old: int = 24) -> int:
    """
    Delete activities older than specified hours

    Args:
        db: Database session
        hours_old: Number of hours after which activities are considered old

    Returns:
        int: Number of deleted records
    """
    try:
        from sqlmodel import select, delete
        from ..models.activity_log import ActivityLog

        cutoff_time = datetime.utcnow() - timedelta(hours=hours_old)

        # First, count the records that will be deleted
        stmt = select(ActivityLog).where(ActivityLog.created_at < cutoff_time)
        old_activities = db.exec(stmt).all()
        deleted_count = len(old_activities)

        # Then delete them
        for activity in old_activities:
            db.delete(activity)

        db.commit()

        logger.info(f"Cleaned up {deleted_count} old activity logs")
        return deleted_count

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error cleaning up old activities: {str(e)}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error during cleanup: {str(e)}")
        return 0


def get_recent_activities_for_user(
    db: Session,
    user_id: int,
    hours_back: int = 24,
    limit: int = 20
) -> list:
    """
    Get recent activities for a specific user

    Args:
        db: Database session
        user_id: ID of the user
        hours_back: Number of hours back to look for activities
        limit: Maximum number of activities to return

    Returns:
        list: List of recent activities
    """
    try:
        from sqlmodel import select

        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)

        activities = db.exec(
            select(ActivityLog)
            .where(ActivityLog.user_id == user_id)
            .where(ActivityLog.created_at >= cutoff_time)
            .order_by(ActivityLog.created_at.desc())
            .limit(limit)
        ).all()

        return activities

    except Exception as e:
        logger.error(f"Error fetching recent activities: {str(e)}")
        return []