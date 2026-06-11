from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_pg
from app.models import User
from app.auth import decode_access_token
import asyncio

security = HTTPBearer()


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security),db: Session = Depends(get_pg)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token = credentials.credentials
    email = decode_access_token(token)
    if email is None:
        raise credentials_exception

    user = db.query(User).filter(User.email == email).first()
    if user is None:
        raise credentials_exception

    return user


def note_helper(note) -> dict:
    """
    Convert MongoDB document to API response format.
    Converts ObjectId to string and structures data according to NoteResponse schema.
    """
    return {
        "id": str(note["_id"]),
        "title": note["title"],
        "content": note["content"],
        "tags": note.get("tags", []),
        "created_at": note["created_at"]
    }

async def connect_with_retry(connect_fn, name: str, retries=5, delay=3):
    for attempt in range(1, retries + 1):
        try:
            await connect_fn()
            return
        except Exception as e:
            print(f"[{name}] attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            await asyncio.sleep(delay)