from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import datetime
import uuid
from pydantic import validator

from typing import TYPE_CHECKING, List
from .base import Base

if TYPE_CHECKING:
    from .task import Task  # Only import for type checking to avoid circular import
    from .activity_log import ActivityLog  # Import for type checking




class UserBase(SQLModel):
    email: str = Field(unique=True, nullable=False)
    name: str = Field(nullable=False)
    is_active: bool = Field(default=True)


class User(UserBase, table=True):
    """
    User model representing a registered user of the system
    """
    __tablename__ = "users"  # Explicit table name to avoid PostgreSQL reserved keyword

    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(unique=True, nullable=False, max_length=255)
    name: str = Field(nullable=False, max_length=255)
    hashed_password: str = Field(nullable=False, max_length=255)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(default=True)

    # Relationship to tasks
    tasks: List["Task"] = Relationship(back_populates="user")
    # Relationship to activities
    activities: List["ActivityLog"] = Relationship(back_populates="user", sa_relationship_kwargs={"lazy": "select"})


    def verify_password(self, password: str) -> bool:
        """Verify a password against the stored hash"""
        from ..utils.security import verify_password as verify_pwd
        return verify_pwd(password, self.hashed_password)


class UserRead(UserBase):
    """Schema for reading user data"""
    id: int
    created_at: datetime
    updated_at: datetime
    is_active: bool


class UserCreate(UserBase):
    """Schema for creating a new user"""
    password: str


class UserUpdate(SQLModel):
    """Schema for updating user data"""
    name: Optional[str] = None
    email: Optional[str] = None
    is_active: Optional[bool] = None