from sqlmodel import SQLModel, Field, Relationship
from typing import Optional
from datetime import datetime
from enum import Enum

class ActivityActionEnum(str, Enum):
    TASK_CREATED = "task_created"
    TASK_UPDATED = "task_updated"
    TASK_DELETED = "task_deleted"
    TASK_COMPLETED = "task_completed"
    TASK_UNCOMPLETED = "task_uncompleted"


class ActivityLogBase(SQLModel):
    user_id: int = Field(nullable=False, foreign_key="users.id", index=True)
    action: ActivityActionEnum = Field(nullable=False, index=True)
    task_id: Optional[int] = Field(default=None, foreign_key="tasks.id", index=True)
    task_title: str = Field(max_length=255, nullable=False)


class ActivityLog(ActivityLogBase, table=True):
    """
    Activity log model representing user activities
    """
    __tablename__ = "activity_logs"  # Explicit table name to follow naming convention

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    # Relationships
    user: "User" = Relationship(back_populates="activities")
    task: "Task" = Relationship(back_populates="activities")


class ActivityLogRead(ActivityLogBase):
    """Schema for reading activity log data"""
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class ActivityLogCreate(ActivityLogBase):
    """Schema for creating a new activity log"""
    pass