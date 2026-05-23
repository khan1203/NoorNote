from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime


class UserCreate(BaseModel):
    email: EmailStr
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=6, description="Minimum 6 characters")

    class Config:
        json_schema_extra = {
            "example": {
                "email": "alice@example.com",
                "username": "alice",
                "password": "securepassword123"
            }
        }


class UserOut(BaseModel):
    id: int
    email: EmailStr
    username: str
    created_at: datetime

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "email": "alice@example.com",
                "username": "alice",
                "created_at": "2026-05-09T12:30:00"
            }
        }


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    email: Optional[str] = None


class ActivityLogCreate(BaseModel):
    action: str = Field(description="Action performed (e.g., login, profile_update)")
    metadata: Optional[dict] = Field(default={}, description="Additional information")

    class Config:
        json_schema_extra = {
            "example": {
                "action": "login",
                "metadata": {
                    "ip_address": "192.168.1.1",
                    "user_agent": "Mozilla/5.0"
                }
            }
        }


class ActivityLogOut(BaseModel):
    id: str = Field(alias="_id", description="MongoDB document ID")
    user_id: int
    action: str
    timestamp: datetime
    metadata: dict = {}

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "_id": "507f1f77bcf86cd799439011",
                "user_id": 1,
                "action": "login",
                "timestamp": "2024-12-17T10:30:00Z",
                "metadata": {"ip_address": "192.168.1.1"}
            }
        }

# Elasticsearch
class SearchResult(BaseModel):
    id: str
    title: str
    content: str
    tags: List[str]
    score: float
    highlight: Optional[dict] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "507f1f77bcf86cd799439011",
                "title": "FastAPI Tutorial",
                "content": "FastAPI is a modern web framework...",
                "tags": ["fastapi", "python"],
                "score": 8.5,
                "highlight": {
                    "title": ["<em>FastAPI</em> Tutorial"]
                }
            }
        }