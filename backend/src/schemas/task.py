from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from enum import Enum


class PriorityEnum(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class TaskBase(BaseModel):
    title: str
    description: Optional[str] = None
    completed: bool = False
    priority: PriorityEnum = PriorityEnum.medium
    due_date: Optional[datetime] = None


class TaskCreate(TaskBase):
    title: str

    class Config:
        schema_extra = {
            "example": {
                "title": "Complete project proposal",
                "description": "Finish writing the project proposal document",
                "priority": "high",
                "due_date": "2023-12-31T23:59:59"
            }
        }


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    completed: Optional[bool] = None
    priority: Optional[PriorityEnum] = None
    due_date: Optional[datetime] = None


class TaskResponse(TaskBase):
    id: int
    created_at: datetime
    updated_at: datetime
    user_id: int

    class Config:
        from_attributes = True