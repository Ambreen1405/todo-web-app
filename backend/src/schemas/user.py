from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime


class UserBase(BaseModel):
    email: EmailStr
    name: str


class UserCreate(UserBase):
    password: str

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "name": "John Doe",
                "password": "securePassword123"
            }
        }


class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None


class UserInDB(UserBase):
    id: int
    hashed_password: str
    created_at: datetime
    updated_at: datetime
    is_active: bool = True

    class Config:
        from_attributes = True


class UserResponse(UserBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True