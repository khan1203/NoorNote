"""
NoorNote main.py

"""
import time
from fastapi import FastAPI, Depends, HTTPException, status, Header, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordRequestForm
from datetime import datetime,timedelta, UTC
from bson import ObjectId
from typing import List, Optional

from sqlalchemy.orm import Session
from app.database import get_pg
from app.helpers import get_current_user, note_helper, connect_with_retry
from app.mongodb import connect_to_mongodb, close_mongodb_connection, get_mongodb
from app.models import User, NoteCreate, NoteResponse, NoteUpdate
from app.redis_client import connect_to_redis, close_redis_connection, cache_get, cache_set, cache_delete, cache_delete_pattern
from app.schemas import (
    UserCreate, 
    UserOut, 
    Token, 
    ActivityLogCreate, 
    ActivityLogOut, 
    SearchResult, 
    EventLog
)

from app.auth import (
    hash_password,
    verify_password,
    create_access_token,
    decode_access_token,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from .elasticsearch import (
    connect_to_elasticsearch, 
    close_elasticsearch_connection, 
    get_elasticsearch, 
    ELASTICSEARCH_INDEX
)
from .kafka_producer import (
    start_kafka_producer, 
    stop_kafka_producer, 
    publish_log, 
    get_topic_name
)

import asyncio
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os

load_dotenv()


# Lifespan Context --------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        
        await connect_with_retry(connect_to_mongodb, "MongoDB")
        await connect_with_retry(connect_to_redis, "Redis")
        await connect_with_retry(connect_to_elasticsearch, "Elasticsearch")
        await start_kafka_producer()

    except Exception as e:
        print(f"✗ Startup failed: {type(e).__name__}: {e}")
        raise   # re-raise so FastAPI reports it properly

    yield

    # Shutdown
    await close_mongodb_connection()
    await close_redis_connection()
    await close_elasticsearch_connection()
    await stop_kafka_producer()

# FastAPI app ---------------------------
app = FastAPI(
    title=os.getenv("APP_NAME", "NoorNote"),
    description=os.getenv("APP_DESCRIPTION"),
    version=os.getenv("APP_VERSION"),
    lifespan=lifespan
)

# =====================================================
# Root & Health Check Endpoints
# =====================================================

@app.get("/")
async def root():
    return {
        "message": "Welcome to NoorNote"
    }


@app.get("/ping")
def ping():

    # Publish to kafka

    return {"status": "ok", "message": "pong"}

# =====================================================
# Authentication Endpoints
# =====================================================

@app.post("/auth/signup", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def signup(user_data: UserCreate, db: Session = Depends(get_pg)):
    
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

    #──────────────────────────────────────────────────────────────────────────
    # 3. Publish to Kafka
    #──────────────────────────────────────────────────────────────────────────
    event= EventLog(
        event_type="user.signup",
        user_id = new_user.id,
        resource_id= str(new_user.id),
        timestamp = datetime.now(UTC),
        metadata = {
            "username" : new_user.username,
            "email" : new_user.email
        }
    )

    asyncio.create_task(
        publish_log(event.model_dump(mode="json"))
    )

    return new_user


@app.post("/auth/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_pg)):

    user = db.query(User).filter(User.username == form_data.username).first()

    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    #──────────────────────────────────────────────────────────────────────────
    #  Publish to Kafka
    #──────────────────────────────────────────────────────────────────────────
    event = EventLog(
        event_type="user.login",
        user_id=user.id,
        resource_id=str(user.id),
        timestamp=datetime.now(UTC),
        metadata={
            "username": user.username,
            "email": user.email
        }
    )

    asyncio.create_task(
        publish_log(event.model_dump(mode="json"))
    )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.email},
        expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


# =====================================================
# Notes CRUD Endpoints
# =====================================================

@app.post("/notes", response_model=NoteResponse, status_code=status.HTTP_201_CREATED)
async def create_note(note: NoteCreate, current_user: User = Depends(get_current_user)):
    
    db = get_mongodb()
    es = get_elasticsearch()


    #  Prepare Note Document
    #──────────────────────────────────────────────────────────────────────────
    note_dict = note.model_dump()
    note_dict["user_id"] = current_user.id
    note_dict["created_at"] = datetime.now(UTC)


    #  Insert into MongoDB
    #──────────────────────────────────────────────────────────────────────────
    result = await db.notes.insert_one(note_dict)


    #  Publish to kafka
    #──────────────────────────────────────────────────────────────────────────
    note_id = str(result.inserted_id)
    event = EventLog(
        event_type="note.created",
        user_id=current_user.id,
        resource_id=note_id,
        timestamp= datetime.now(UTC),
        metadata={
            "title": note.title,
            "tags": note.tags,
        }
    )

    # fire-and-forget 
    asyncio.create_task(
        publish_log(event.model_dump(mode="json"))
    )

    # Pure fire-and-forget
    """
    kafka_producer.send(
    KAFKA_TOPIC,
    event.model_dump(mode="json")
    )
    """

    #  Index in ElasticSearch
    #──────────────────────────────────────────────────────────────────────────
    await es.index(
        index=ELASTICSEARCH_INDEX,
        id=note_id,
        document={
            "title": note.title,
            "content": note.content,
            "tags": note.tags,
            "created_at": note_dict["created_at"].isoformat(),
            "user_id": current_user.id
        }
    )

    # Retrieve the created note
    #──────────────────────────────────────────────────────────────────────────
    created_note = await db.notes.find_one({"_id": result.inserted_id})

    return note_helper(created_note)


@app.get("/notes", response_model=list[NoteResponse])
async def get_my_notes(current_user: User = Depends(get_current_user)):
    
    db = get_mongodb()

    # NOTE: datatype for user_id = int
    notes = await db.notes.find({"user_id": current_user.id}).sort("created_at", -1).to_list(length=100)
    
    return [note_helper(note) for note in notes]


@app.get("/notes/{note_id}", response_model=NoteResponse)
async def get_note(note_id: str,  x_cache_control: Optional[str] = Header(None)):

    start_time = time.time()

    # Check if cache should be bypassed
    bypass_cache = (x_cache_control == "no-cache")

    # Try cache first (unless bypassed)
    if not bypass_cache:
        cached_note = await cache_get(f"note:{note_id}")
        if cached_note:
            elapsed = (time.time() - start_time) * 1000
            print(f"Cache HIT for note:{note_id} ({elapsed:.2f}ms)")

            # Convert ISO string back to datetime for response
            cached_note["created_at"] = datetime.fromisoformat(cached_note["created_at"])
            return NoteResponse(**note_helper(cached_note))

    # Validate ObjectId format
    if not ObjectId.is_valid(note_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid note ID format"
        )

    # Cache miss & validates ObjectID - query MongoDB
    db = get_mongodb()
    note = await db.notes.find_one({"_id": ObjectId(note_id)})

    if not note:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Note with id {note_id} not found"
        )
    
    note["_id"] = str(note["_id"])

    # Prepare cache-friendly version (datetime → ISO string)
    cache_note = note.copy()
    cache_note["created_at"] = note["created_at"].isoformat()

    # Store in Redis cache for future requests
    await cache_set(f"note:{note_id}", cache_note)

    elapsed = (time.time() - start_time) * 1000
    print(f"Cache MISS for note:{note_id} ({elapsed:.2f}ms)")

    return note_helper(note)


@app.put("/notes/{note_id}", response_model=NoteResponse)
async def update_note(note_id: str, note_update: NoteUpdate, current_user: User = Depends(get_current_user)):

    # Validate ObjectId format
    #──────────────────────────────────────────────────────────────────────────
    if not ObjectId.is_valid(note_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid note ID format"
        )

    # Only include fields that were provided
    update_data = note_update.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )

    # Update the note to MongoDB
    #──────────────────────────────────────────────────────────────────────────
    db = get_mongodb()
    result = await db.notes.update_one(
        {"_id": ObjectId(note_id)},
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Note with id {note_id} not found"
        )

    # Publish to kafka
    #──────────────────────────────────────────────────────────────────────────
    event = EventLog(
        event_type="note.updated",
        user_id= current_user.id,
        resource_id=note_id,
        timestamp=datetime.now(UTC),
        metadata={
            "upgraded_fields": update_data
            }
    )

    asyncio.create_task(
        publish_log(event.model_dump(mode="json"))
    )

    # Invalidate Redis cache (CRITICAL!)
    #──────────────────────────────────────────────────────────────────────────
    await cache_delete(f"note:{note_id}")


    # Re-index in Elasticsearch
    #──────────────────────────────────────────────────────────────────────────
    es = get_elasticsearch()
    updated_note = await db.notes.find_one({"_id": ObjectId(note_id)})          # NOTE: `updated_note` is just a python dict, not any Pydantic Model Instance

    await es.index(
        index="notes",
        id=note_id,
        document={
            "title": updated_note["title"],
            "content": updated_note["content"],
            "tags": updated_note.get("tags", []),
            "created_at": updated_note["created_at"]
        }
    )

    # Return updated note
    return note_helper(updated_note)


@app.delete("/notes/{note_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_note(note_id: str, current_user: User = Depends(get_current_user)):

    db = get_mongodb()
    es = get_elasticsearch()

    # Validate ObjectId format
    if not ObjectId.is_valid(note_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid note ID format"
        )

    note = await db.notes.find_one({"_id": ObjectId(note_id)})
    # Delete from MongoDB
    result = await db.notes.delete_one({"_id": ObjectId(note_id)})

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Note with id {note_id} not found"
        )
    
    # Publish to kafka
    event = EventLog(
        event_type="note.deleted",
        user_id=current_user.id,
        resource_id=note_id,
        timestamp=datetime.now(UTC),
        metadata = {
            "title": note.get("title"),
            "tags": note.get("tags"),
            "deleted_at": datetime.now(UTC).isoformat()
        }
    )

    asyncio.create_task(
        publish_log(event.model_dump(mode="json"))
    )

    # Delete from Elasticsearch Index
    try:
        await es.delete(index=ELASTICSEARCH_INDEX, id=note_id)
    except Exception as e:
        print(f"Failed to delete from Elasticsearch: {e}")
    
    # Invalidate Redis cache (CRITICAL!)
    await cache_delete(f"note:{note_id}")

    # Return fokka :)
    return None


# =====================================================
# Search Endpoints
# =====================================================

@app.get("/search", response_model=List[SearchResult])
async def search_notes(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(default=10, le=100),
    current_user: User = Depends(get_current_user)
):
    es = get_elasticsearch()

    # Build Elasticsearch query
    search_body = {
    "query": {
        "bool": {
            "must": {
                "multi_match": {
                    "query": q,
                    "fields": ["title^3", "content"],
                    "fuzziness": "AUTO"
                }
            },
            "filter": [
                {"term": {"user_id": current_user.id}}
            ]
        }
    },
    "highlight": {
        "fields": {
            "title": {},
            "content": {"fragment_size": 150}
        }
    },
    "size": limit
    }

    # Execute search
    response = await es.search(
        index=ELASTICSEARCH_INDEX,
        body=search_body
    )
    print(response)
    # logger.debug(response)

    # Format results
    results = []
    for hit in response["hits"]["hits"]:
        result = SearchResult(
            id=hit["_id"],
            title=hit["_source"]["title"],
            content=hit["_source"]["content"],
            tags=hit["_source"]["tags"],
            score=hit["_score"],
            highlight=hit.get("highlight")
        )
        results.append(result)

    # =====================================================
    # Publish to Kafka
    # =====================================================
    event = EventLog(
        event_type="note.searched",
        user_id=current_user.id,
        resource_id="search",
        timestamp=datetime.now(UTC),
        metadata = search_body
    )

    asyncio.create_task(
        publish_log(event.model_dump(mode="json"))
    )

    return results


# =====================================================
# Profile & User Endpoints
# =====================================================

@app.get("/profile", response_model=UserOut)
async def get_profile(current_user: User = Depends(get_current_user)):
    
    ''' Publish to kafka
    ──────────────────────────────────────────────────────────────────────────'''
    event = EventLog(
    event_type="profile.view",
    user_id=current_user.id,
    resource_id=str(current_user.id), 
    timestamp=datetime.now(UTC),
    metadata={
        "username": current_user.username,
        "email": current_user.email,
        # "metadata": current_user.metadata
        }    
    )

    asyncio.create_task(
        publish_log(event.model_dump(mode="json"))
    )

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
        "timestamp": datetime.now(UTC),
        "metadata": {}
    })

    users = db.query(User).all()
    return users


# =====================================================
# Activity Log Endpoints
# =====================================================

@app.post("/log", status_code=202)
async def create_log(log: EventLog):
    """
    Publish activity log to Kafka

    Returns 202 Accepted because event is queued, not processed yet
    """
    # Add timestamp if not provided
    log_data = log.model_dump(mode="json")

    try:
        # Publish to Kafka (fast, async operation)
        await publish_log(log_data)

        return {
            "status": "accepted",
            "message": "Log event published to Kafka",
            "topic": get_topic_name(),
            "event": log_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish event: {str(e)}"
        )


@app.get("/activity", response_model=list[EventLog])
async def get_my_logs(
    current_user: User = Depends(get_current_user),
    limit: int = Query(default=10, le=100)
):
    mongodb = get_mongodb()
    cursor = mongodb.activity_logs.find(
        {"user_id": current_user.id},
        {"_id": 0}
    ).sort("timestamp", -1).limit(limit)
    
    logs = await cursor.to_list(length=limit)
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


# =====================================================
# Cache Management Endpoints
# =====================================================

@app.get("/cache/stats")
async def cache_stats():
    from .redis_client import get_redis
    redis = get_redis()

    info = await redis.info("stats")

    total_commands = info.get("total_commands_processed", 0)
    keyspace_hits = info.get("keyspace_hits", 0)
    keyspace_misses = info.get("keyspace_misses", 0)

    total_requests = keyspace_hits + keyspace_misses
    hit_rate = (keyspace_hits / total_requests * 100) if total_requests > 0 else 0

    return {
        "keyspace_hits": keyspace_hits,
        "keyspace_misses": keyspace_misses,
        "hit_rate_percentage": round(hit_rate, 2),
        "total_commands": total_commands
    }


@app.delete("/cache/clear")
async def clear_cache():
    await cache_delete_pattern("note:*")
    return {"message": "Cache cleared successfully"}

# =======================================================
# GRAPHQL ENDPOINT
# =======================================================

from app.graphql_schema import schema
from strawberry.fastapi import GraphQLRouter

# Create GraphQL router
graphql_app = GraphQLRouter(schema)

# Mount GraphQL endpoint
app.include_router(graphql_app, prefix="/graphql")

