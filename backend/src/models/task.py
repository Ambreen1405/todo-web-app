from sqlmodel import SQLModel, Field, Relationship
from typing import Optional
from datetime import datetime
from enum import Enum

from typing import TYPE_CHECKING, List
from .base import Base

if TYPE_CHECKING:
    from .activity_log import ActivityLog  # Import for type checking
    from .user import User


class PriorityEnum(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class TaskBase(SQLModel):
    title: str = Field(nullable=False, max_length=255)
    description: Optional[str] = Field(default=None, max_length=1000)
    completed: bool = Field(default=False)
    priority: PriorityEnum = Field(default=PriorityEnum.medium)
    due_date: Optional[datetime] = Field(default=None)


class Task(TaskBase, table=True):
    """
    Task model representing a user's task
    """
    __tablename__ = "tasks"  # Explicit table name to avoid PostgreSQL reserved keyword

    id: Optional[int] = Field(default=None, primary_key=True)
    title: str = Field(nullable=False, max_length=255)
    description: Optional[str] = Field(default=None, max_length=1000)
    completed: bool = Field(default=False)
    priority: PriorityEnum = Field(default=PriorityEnum.medium)
    due_date: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    user_id: int = Field(foreign_key="users.id", nullable=False)

    # Relationship to user
    user: "User" = Relationship(back_populates="tasks")
    # Relationship to activities
    activities: List["ActivityLog"] = Relationship(back_populates="task")

class TaskRead(TaskBase):
    """Schema for reading task data"""
    id: int
    created_at: datetime
    updated_at: datetime
    user_id: int


class TaskCreate(TaskBase):
    """Schema for creating a new task"""
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


class TaskUpdate(SQLModel):
    """Schema for updating task data"""
    title: Optional[str] = None
    description: Optional[str] = None
    completed: Optional[bool] = None
    priority: Optional[PriorityEnum] = None
    due_date: Optional[datetime] = None