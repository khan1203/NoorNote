from sqlalchemy import Boolean, Column, Integer, String, DateTime, UniqueConstraint
from .database import Base
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from sqlalchemy.sql import func

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    username = Column(String(50), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(
    DateTime(timezone=True),
    server_default=func.now(),
    nullable=False
    )

    __table_args__ = (
        UniqueConstraint("email", name="uq_users_email"),
        UniqueConstraint("username", name="uq_users_username"),
    )

    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}', username='{self.username}')>"



"""----------------------------------------------mongodb Models------------------------------------------------"""
class NoteCreate(BaseModel):

    # user_id: will be auto generated during post-request by any user
    title: str = Field(..., min_length=1, max_length=200, description="Note title")
    content: str = Field(..., min_length=1, description="Note content")
    tags: Optional[list[str]] = Field(default=[], description="Optional tags for categorization")

    class Config:
        json_schema_extra = {
            "example": {
                "title": "Learn FastAPI",
                "content": "FastAPI is a modern web framework for building APIs",
                "tags": ["python", "fastapi", "tutorial"]
            }
        }


class NoteResponse(BaseModel):
    id: str = Field(..., description="Unique identifier (MongoDB ObjectId)")
    title: str
    content: str
    tags: list[str]
    created_at: datetime = Field(..., description="Timestamp when note was created")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "507f1f77bcf86cd799439011",
                "title": "Learn FastAPI",
                "content": "FastAPI is a modern web framework",
                "tags": ["python", "fastapi"],
                "created_at": "2024-01-15T10:30:00Z"
            }
        }


class NoteUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    content: Optional[str] = Field(None, min_length=1)
    tags: Optional[list[str]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "title": "Updated Title",
                "tags": ["python", "fastapi", "mongodb"]
            }
        }