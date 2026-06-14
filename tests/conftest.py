import pytest
import pytest_asyncio
from redis import asyncio as aioredis
from httpx import AsyncClient, ASGITransport
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import mongomock_motor

from app.database import get_pg, Base
import app.mongodb as mongo_module
from app.main import app

# ─────────────────────────────────────────────
# PostgreSQL — in-memory SQLite
# ─────────────────────────────────────────────
TEST_SQLITE_URL = "sqlite://"

test_engine = create_engine(
    TEST_SQLITE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSession = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


def override_get_pg():
    db = TestingSession()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────
# MongoDB — mongomock_motor, injected directly
# into the module so direct get_mongodb() calls
# are also intercepted
# ─────────────────────────────────────────────
mock_mongo_client = mongomock_motor.AsyncMongoMockClient()
mock_mongo_db = mock_mongo_client["test_db"]


def override_get_mongodb():
    return mock_mongo_db


# ─────────────────────────────────────────────
# Dependency overrides
# ─────────────────────────────────────────────
app.dependency_overrides[get_pg] = override_get_pg
app.dependency_overrides[mongo_module.get_mongodb] = override_get_mongodb


# ─────────────────────────────────────────────
# Prevent real MongoDB connect/disconnect
# ─────────────────────────────────────────────
@pytest.fixture(scope="session", autouse=True)
def patch_mongo():
    original = mongo_module.mongodb_db
    mongo_module.mongodb_db = mock_mongo_db
    yield
    mongo_module.mongodb_db = original


# ─────────────────────────────────────────────
# Create / drop tables once per session
# ─────────────────────────────────────────────
@pytest.fixture(scope="session", autouse=True)
def create_tables():
    Base.metadata.create_all(bind=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)


# ─────────────────────────────────────────────
# Wipe data between tests
# ─────────────────────────────────────────────
@pytest.fixture(autouse=True)
async def clean_mongo():
    await mock_mongo_db.activity_logs.drop()
    await mock_mongo_db.notes.drop()
    yield


@pytest.fixture(autouse=True)
def clean_postgres():
    db = TestingSession()
    for table in reversed(Base.metadata.sorted_tables):
        db.execute(table.delete())
    db.commit()
    db.close()
    yield


# ─────────────────────────────────────────────
# HTTP client
# ─────────────────────────────────────────────
@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac


# ─────────────────────────────────────────────
# Redis client
# ─────────────────────────────────────────────
from app.redis_client import redis_client as rc # noqa: E402

@pytest.fixture(autouse=True)
async def init_redis():
    rc.REDIS_URL = "redis://localhost:6379/0"
    rc.redis_client = await aioredis.from_url(
        "redis://localhost:6379/0",
        encoding="utf-8",
        decode_responses=True
    )
    yield
    await rc.close_redis_connection()


@pytest.fixture
async def redis_client():
    from app.redis_client import get_redis
    return get_redis()

# ─────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────
@pytest.fixture
def mongo_db():
    return mock_mongo_db


@pytest.fixture
def user_payload() -> dict:
    return {
        "username": "testuser",
        "email": "test@example.com",
        "password": "StrongPass123!",
    }


@pytest_asyncio.fixture
async def registered_user(client: AsyncClient, user_payload: dict) -> dict:
    resp = await client.post("/auth/signup", json=user_payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


@pytest_asyncio.fixture
async def auth_headers(
    client: AsyncClient,
    registered_user: dict,
    user_payload: dict,
) -> dict:
    resp = await client.post(
        "/auth/login",
        data={
            "username": user_payload["username"],
            "password": user_payload["password"],
        },
    )
    assert resp.status_code == 200, resp.text
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def note_payload() -> dict:
    return {
        "title": "Test Note",
        "content": "This is a test note.",
        "tags": ["pytest", "testing"],
    }


@pytest_asyncio.fixture
async def created_note(client: AsyncClient, note_payload: dict) -> dict:
    resp = await client.post("/notes", json=note_payload)
    assert resp.status_code == 201, resp.text
    return resp.json()