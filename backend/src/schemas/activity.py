from pydantic import BaseModel
from datetime import datetime
from typing import Optional
from ..models.activity_log import ActivityActionEnum


class ActivityLogResponse(BaseModel):
    id: int
    user_id: int
    action: ActivityActionEnum
    task_id: Optional[int] = None
    task_title: str
    created_at: datetime

    class Config:
        from_attributes = True