import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.database import Base, get_pg
from app.mongodb import get_mongodb

# Test PostgreSQL Database
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)

TestingSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Create tables
Base.metadata.create_all(bind=engine)


# Override PostgreSQL dependency
def override_get_pg():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


# Fake MongoDB
class FakeMongoDB:
    def __init__(self):
        self.notes = FakeCollection()
        self.activity_logs = FakeCollection()


class FakeCollection:
    def __init__(self):
        self.data = []

    async def insert_one(self, document):
        document["_id"] = str(len(self.data) + 1)
        self.data.append(document)

        class Result:
            inserted_id = document["_id"]

        return Result()

    async def find_one(self, query):
        for item in self.data:
            if item["_id"] == query["_id"]:
                return item
        return None

    async def delete_one(self, query):
        deleted = 0

        for item in self.data:
            if item["_id"] == query["_id"]:
                self.data.remove(item)
                deleted = 1
                break

        class Result:
            deleted_count = deleted

        return Result()

    async def update_one(self, query, update):
        matched = 0

        for item in self.data:
            if item["_id"] == query["_id"]:
                item.update(update["$set"])
                matched = 1

        class Result:
            matched_count = matched

        return Result()

    def find(self, query=None):
        return FakeCursor(self.data)


class FakeCursor:
    def __init__(self, data):
        self.data = data

    def sort(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    async def to_list(self, length=100):
        return self.data[:length]


fake_mongo = FakeMongoDB()

app.dependency_overrides[get_pg] = override_get_pg

@pytest.fixture
def client():
    app.mongodb = fake_mongo
    return TestClient(app)