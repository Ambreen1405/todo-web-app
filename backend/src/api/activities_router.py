from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session
from datetime import datetime, timedelta
from typing import List

from ..database.session import get_session
from ..schemas.activity import ActivityLogResponse
from ..models.activity_log import ActivityLog, ActivityActionEnum
from ..utils.auth import get_current_user
from ..models.user import User
from ..utils.activity_logger import get_recent_activities_for_user

router = APIRouter(tags=["activities"])


@router.get("/activities/recent", response_model=List[ActivityLogResponse])
async def get_recent_activities(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session)
):
    """
    Get recent activities for the current user (last 24 hours, limited to 20 records)
    """
    try:
        activities = get_recent_activities_for_user(
            db=db,
            user_id=current_user.id,
            hours_back=24,
            limit=20
        )

        return [
            ActivityLogResponse(
                id=activity.id,
                user_id=activity.user_id,
                action=activity.action,
                task_id=activity.task_id,
                task_title=activity.task_title,
                created_at=activity.created_at
            )
            for activity in activities
        ]

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching recent activities: {str(e)}"
        )