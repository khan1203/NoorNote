"""GraphQL schema for NoorNote using Strawberry."""
import strawberry
import asyncio
from typing import List, Optional
from datetime import datetime, UTC
from bson import ObjectId

from app.models import User as UserModel
from app.database import get_pg
from app.mongodb import get_mongodb
from app.schemas import EventLog
from app.kafka_producer import publish_log


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

@strawberry.type
class ActivityLog:
    """ActivityLog type — resolved from MongoDB activity_logs collection."""
    id: strawberry.ID
    event_type: str
    user_id: strawberry.ID
    resource_id: strawberry.ID
    timestamp: str
    metadata: str  # serialized as JSON string


@strawberry.type
class User:
    """User type — resolved from PostgreSQL users table."""
    id: strawberry.ID
    username: str
    email: str
    created_at: str

    @strawberry.field
    async def notes(self) -> List["Note"]:    # NOTE: forward reference as Note type is not defined yet. Forward reference is written inside quotation.
        """Get all notes for this user — resolver → MongoDB notes collection."""
        mongo_db = get_mongodb()
        cursor = mongo_db.notes.find({"user_id": int(self.id)})
        docs = await cursor.to_list(length=100)

        return [
            Note(
                id=str(doc["_id"]),
                user_id=str(doc["user_id"]),
                title=doc["title"],
                content=doc["content"],
                tags=doc.get("tags", []),
                created_at=doc["created_at"].isoformat(),
            )
            for doc in docs
        ]

    @strawberry.field
    async def activity_logs(self) -> List[ActivityLog]:
        """Get all activity logs for this user — resolver → MongoDB activity_logs collection."""
        mongo_db = get_mongodb()
        cursor = mongo_db.activity_logs.find({"user_id": int(self.id)})
        docs = await cursor.to_list(length=100)

        return [
            ActivityLog(
                id=str(doc["_id"]),
                event_type=doc["event_type"],
                user_id=str(doc["user_id"]),
                resource_id=str(doc["resource_id"]),
                timestamp=doc["timestamp"].isoformat(),
                metadata=str(doc.get("metadata", {})),
            )
            for doc in docs
        ]


@strawberry.type
class Note:
    """Note type — resolved from MongoDB notes collection."""
    id: strawberry.ID
    user_id: strawberry.ID
    title: str
    content: str
    tags: List[str]
    created_at: str

    @strawberry.field
    async def author(self) -> Optional[User]:
        """Get the author of this note — resolver → PostgreSQL users table."""
        db = get_pg()
        
        user = db.query(UserModel).filter(UserModel.id == int(self.user_id)).first()

        if not user:
            return None

        return User(
            id=str(user.id),
            username=user.username,
            email=user.email,
            created_at=user.created_at.isoformat(),
        )


# ---------------------------------------------------------------------------
# Input Types
# ---------------------------------------------------------------------------

@strawberry.input
class CreateNoteInput:
    title: str
    content: str
    tags: List[str]


@strawberry.input
class UpdateNoteInput:
    title: Optional[str] = None
    content: Optional[str] = None
    tags: Optional[List[str]] = None

@strawberry.input
class UpdateUserInput:
    id: int
    username: Optional[str] = None
    email: Optional[str] = None


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------
from app.auth import decode_access_token

@strawberry.type
class Query:

    @strawberry.field
    async def me(self, info: strawberry.types.Info) -> Optional[User]:
        """Get the current authenticated user — reads JWT from context."""
        request = info.context["request"]
        token = request.headers.get("Authorization", "").replace("Bearer ", "")

        if not token:
            return None

        email = decode_access_token(token)

        if not email:
            return None
        try:
            db = next(get_pg())
            user = db.query(UserModel).filter(UserModel.email == email).first()

            if not user:
                return None

            return User(
                id=str(user.id),
                username=user.username,
                email=user.email,
                created_at=user.created_at.isoformat(),
            )
        
        finally:
            db.close()

    @strawberry.field
    async def user(self, id: strawberry.ID) -> Optional[User]:
        """Get a single user by ID — PostgreSQL."""
        try:
            db = next(get_pg())
            user = db.query(UserModel).filter(UserModel.id == int(id)).first()

            if not user:
                return None
        
            return User(
                id=str(user.id),
                username=user.username,
                email=user.email,
                created_at=user.created_at.isoformat(),
            )
        finally:
            db.close()
    
    @strawberry.field
    async def users(self) -> List[User]:
        """Get all users — PostgreSQL."""
        try:
            db = next(get_pg())
            users = db.query(UserModel).all()

            return [
                User(
                    id=str(user.id),
                    username=user.username,
                    email=user.email,
                    created_at=user.created_at.isoformat(),
                )
                for user in users
            ]
        finally:
            db.close()

    @strawberry.field
    async def note(self, id: strawberry.ID) -> Optional[Note]:
        """Get a single note by ID — MongoDB."""
        mongo_db = get_mongodb()
        doc = await mongo_db.notes.find_one({"_id": ObjectId(id)})

        if not doc:
            return None
        
        return Note(
            id=str(doc["_id"]),
            user_id=str(doc["user_id"]),
            title=doc["title"],
            content=doc["content"],
            tags=doc.get("tags", []),
            created_at=doc["created_at"].isoformat(),
        )

    @strawberry.field
    async def notes(self) -> List[Note]:
        """Get all notes for the authenticated user — MongoDB."""
        mongo_db = get_mongodb()
        cursor = mongo_db.notes.find({"user_id": int(self.id)})
        docs = await cursor.to_list(length=100)

        return [
            Note(
                id=str(doc["_id"]),
                user_id=str(doc["user_id"]),
                title=doc["title"],
                content=doc["content"],
                tags=doc.get("tags", []),
                created_at=doc["created_at"].isoformat(),
            )
            for doc in docs
        ]

    @strawberry.field
    async def activity_logs(self) -> List[ActivityLog]:
        """Get all activity logs — MongoDB."""
        mongo_db = get_mongodb()
        cursor = mongo_db.activity_logs.find()
        docs = await cursor.to_list(length=100)
    
        return [
            ActivityLog(
                id=str(doc["_id"]),
                event_type=doc["event_type"],
                user_id=str(doc["user_id"]),
                resource_id=str(doc["resource_id"]),
                timestamp=doc["timestamp"].isoformat(),
                metadata=str(doc.get("metadata", {})),
            )
            for doc in docs
        ]


# ---------------------------------------------------------------------------
# Mutation
# ---------------------------------------------------------------------------

@strawberry.type
class Mutation:

    @strawberry.mutation
    async def createNote(
        self,
        user_id: strawberry.ID,
        note: CreateNoteInput,
    ) -> Note:
        """Create a new note — MongoDB."""
        mongo_db = get_mongodb()

        note_data = {
            "user_id": int(user_id),
            "title": note.title,
            "content": note.content,
            "tags": note.tags,
            "created_at": datetime.now(UTC),
        }

        result = await mongo_db.notes.insert_one(note_data)

        #  Publish to kafka
        #──────────────────────────────────────────────────────────────────────────
        note_id = str(result.inserted_id)
        event = EventLog(
            event_type="note.created",
            user_id=user_id,
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

        #  Index in ElasticSearch
        #──────────────────────────────────────────────────────────────────────────
        from app.elasticsearch import (
            get_elasticsearch, 
            ELASTICSEARCH_INDEX
        )

        es = get_elasticsearch()
        await es.index(
            index=ELASTICSEARCH_INDEX,
            id=note_id,
            document={
                "title": note.title,
                "content": note.content,
                "tags": note.tags,
                "created_at": note_data["created_at"].isoformat(),
                "user_id": user_id
            }
        )

        # return the created note
        return Note(
            id=str(result.inserted_id),
            user_id=str(user_id),
            title=note.title,
            content=note.content,
            tags=note.tags,
            created_at=note_data["created_at"].isoformat(),
        )

    @strawberry.mutation
    async def updateNote(
        self,
        id: strawberry.ID,
        note: UpdateNoteInput,
    ) -> Optional[Note]:
        
        """Update an existing note — MongoDB."""
        mongo_db = get_mongodb()

        update_data = {}
        if note.title is not None:
            update_data["title"] = note.title
        if note.content is not None:
            update_data["content"] = note.content
        if note.tags is not None:
            update_data["tags"] = note.tags

        result = await mongo_db.notes.find_one_and_update(
            {"_id": ObjectId(id)},
            {"$set": update_data},
            return_document=True,
        )

        if not result:
            return None

        return Note(
            id=str(result["_id"]),
            user_id=str(result["user_id"]),
            title=result["title"],
            content=result["content"],
            tags=result.get("tags", []),
            created_at=result["created_at"].isoformat(),
        )

    @strawberry.mutation
    async def deleteNote(self, id: strawberry.ID) -> bool:
        """Delete a note by ID — MongoDB."""
        mongo_db = get_mongodb()
        result = await mongo_db.notes.delete_one({"_id": ObjectId(id)})
        return result.deleted_count == 1
    
    @strawberry.mutation
    async def update_user(self, input: UpdateUserInput) -> Optional[User]:
        """Update user profile — PostgreSQL."""
        db = next(get_pg())
        try:
            user = db.query(UserModel).filter(UserModel.id == input.id).first()

            if not user:
                return None

            if input.username is not None:
                user.username = input.username
            if input.email is not None:
                user.email = input.email

            db.commit()
            db.refresh(user)

            return User(
                id=str(user.id),
                username=user.username,
                email=user.email,
                created_at=user.created_at.isoformat(),
            )
        
        finally:
            db.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

schema = strawberry.Schema(query=Query, mutation=Mutation)