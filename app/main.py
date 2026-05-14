"""
NoorNote

"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordRequestForm
from datetime import datetime,timedelta
from bson import ObjectId

from sqlalchemy.orm import Session
from app.models import User, NoteCreate, NoteResponse, NoteUpdate
from app.database import get_pg
from app.mongodb import connect_to_mongodb, close_mongodb_connection, get_mongodb
from app.schemas import UserCreate, UserOut, Token, ActivityLogCreate, ActivityLogOut
from app.auth import (
    hash_password,
    verify_password,
    create_access_token,
    decode_access_token,
    ACCESS_TOKEN_EXPIRE_MINUTES
)


from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os

load_dotenv()

# Create FastAPI application

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await connect_to_mongodb()

    yield

    # Shutdown
    await close_mongodb_connection()


app = FastAPI(
    title=os.getenv("APP_NAME", "FastAPI Lab 3"),
    description="Async MongoDB Integration with FastAPI",
    version="1.0.0",
    lifespan=lifespan
)

# PostgreSQL Dependency:

security = HTTPBearer()

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_pg)
) -> User:
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

# MongoDB Dependency: Helper function to convert MongoDB document to response format
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

"""=========================================    ENDPOINTS   ==============================================="""

@app.get("/ping")
def ping():
    return {"status": "ok", "message": "pong"}


@app.post("/auth/signup", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def signup(user_data: UserCreate, db: Session = Depends(get_pg)):
    existing_user = db.query(User).filter(
        (User.email == user_data.email) | (User.username == user_data.username)
    ).first()

    if existing_user:
        if existing_user.email == user_data.email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already taken"
            )

    hashed_password = hash_password(user_data.password)
    new_user = User(
        email=user_data.email,
        username=user_data.username,
        password_hash=hashed_password
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return new_user


@app.post("/auth/login", response_model=Token)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_pg)
):
    user = db.query(User).filter(User.username == form_data.username).first()

    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Log login activity to MongoDB
    mongodb = get_mongodb()
    await mongodb.activity_logs.insert_one({
        "user_id": user.id,
        "action": "login",
        "timestamp": datetime.utcnow(),
        "metadata": {}
    })

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.email},
        expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/profile", response_model=UserOut)
async def get_profile(current_user: User = Depends(get_current_user)):
    # Automatic logging: log profile view
    mongodb = get_mongodb()
    await mongodb.activity_logs.insert_one({
        "user_id": current_user.id,
        "action": "profile_view",
        "timestamp": datetime.utcnow(),
        "metadata": {}
    })

    return current_user


@app.get("/users", response_model=list[UserOut])
async def get_users(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_pg)
):
    # Automatic logging: log users list view
    mongodb = get_mongodb()
    await mongodb.activity_logs.insert_one({
        "user_id": current_user.id,
        "action": "users_list_view",
        "timestamp": datetime.utcnow(),
        "metadata": {}
    })

    users = db.query(User).all()
    return users


@app.post("/logs", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_custom_log(
    log_data: ActivityLogCreate,
    current_user: User = Depends(get_current_user)
):
    mongodb = get_mongodb()

    log_document = {
        "user_id": current_user.id,
        "action": log_data.action,
        "timestamp": datetime.utcnow(),
        "metadata": log_data.metadata
    }

    result = await mongodb.activity_logs.insert_one(log_document)

    return {
        "message": "Custom activity log created",
        "log_id": str(result.inserted_id)
    }


@app.get("/logs", response_model=list[ActivityLogOut])
async def get_my_logs(
    current_user: User = Depends(get_current_user),
    limit: int = 10
):
    mongodb = get_mongodb()

    cursor = mongodb.activity_logs.find(
        {"user_id": current_user.id}
    ).sort("timestamp", -1).limit(limit)

    logs = await cursor.to_list(length=limit)

    # Convert ObjectId to string for JSON serialization
    for log in logs:
        log["_id"] = str(log["_id"])

    return logs


@app.get("/users/{user_id}/logs", response_model=list[ActivityLogOut])
async def get_user_logs(
    user_id: int,
    current_user: User = Depends(get_current_user),
    limit: int = 10,
    db: Session = Depends(get_pg)
):
    # Check if user exists in PostgreSQL
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with id {user_id} not found"
        )

    # Get logs from MongoDB
    mongodb = get_mongodb()
    cursor = mongodb.activity_logs.find(
        {"user_id": user_id}
    ).sort("timestamp", -1).limit(limit)

    logs = await cursor.to_list(length=limit)

    # Convert ObjectId to string
    for log in logs:
        log["_id"] = str(log["_id"])

    return logs


@app.get("/")
async def root():
    return {
        "message": "Welcome to NoorNote",
        "endpoints": {
            "create_note": "POST /notes",
            "get_all_notes": "GET /notes",
            "get_note": "GET /notes/{id}",
            "update_note": "PUT /notes/{id}",
            "delete_note": "DELETE /notes/{id}"
        }
    }


@app.post("/notes", response_model=NoteResponse, status_code=status.HTTP_201_CREATED)
async def create_note(note: NoteCreate):
    db = get_mongodb()

    # Prepare note document
    note_dict = note.model_dump()
    note_dict["created_at"] = datetime.utcnow()

    # Insert into MongoDB
    result = await db.notes.insert_one(note_dict)

    # Retrieve the created note
    created_note = await db.notes.find_one({"_id": result.inserted_id})

    return note_helper(created_note)


@app.get("/notes", response_model=list[NoteResponse])
async def get_all_notes():
    db = get_mongodb()

    # Find all notes, sort by creation time (newest first)
    notes = await db.notes.find().sort("created_at", -1).to_list(length=100)

    return [note_helper(note) for note in notes]


@app.get("/notes/{note_id}", response_model=NoteResponse)
async def get_note(note_id: str):
    # Validate ObjectId format
    if not ObjectId.is_valid(note_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid note ID format"
        )

    db = get_mongodb()
    note = await db.notes.find_one({"_id": ObjectId(note_id)})

    if not note:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Note with id {note_id} not found"
        )

    return note_helper(note)


@app.put("/notes/{note_id}", response_model=NoteResponse)
async def update_note(note_id: str, note_update: NoteUpdate):
    # Validate ObjectId format
    if not ObjectId.is_valid(note_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid note ID format"
        )

    db = get_mongodb()

    # Only include fields that were provided
    update_data = note_update.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )

    # Update the note
    result = await db.notes.update_one(
        {"_id": ObjectId(note_id)},
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Note with id {note_id} not found"
        )

    # Retrieve and return updated note
    updated_note = await db.notes.find_one({"_id": ObjectId(note_id)})
    return note_helper(updated_note)


@app.delete("/notes/{note_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_note(note_id: str):
    # Validate ObjectId format
    if not ObjectId.is_valid(note_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid note ID format"
        )

    db = get_mongodb()
    result = await db.notes.delete_one({"_id": ObjectId(note_id)})

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Note with id {note_id} not found"
        )

    return None
