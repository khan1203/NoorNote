import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from unittest.mock import AsyncMock, MagicMock, patch
import mongomock_motor

from app.database import get_pg, Base          # adjust import path if needed
from app.mongodb import get_mongodb
from app.main import app

# ─────────────────────────────────────────────
# PostgreSQL — in-memory SQLite for tests
# ─────────────────────────────────────────────
TEST_SQLITE_URL = "sqlite://"   # pure in-memory, no file

test_engine = create_engine(
    TEST_SQLITE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,       # single shared connection for in-memory DB
)
TestingSession = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


def override_get_pg():
    db = TestingSession()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────
# MongoDB — mongomock_motor (no real Mongo needed)
# ─────────────────────────────────────────────
mock_mongo_client = mongomock_motor.AsyncMongoMockClient()
mock_mongo_db = mock_mongo_client["test_db"]


def override_get_mongodb():
    return mock_mongo_db


# ─────────────────────────────────────────────
# App-level dependency overrides
# ─────────────────────────────────────────────
app.dependency_overrides[get_pg] = override_get_pg
app.dependency_overrides[get_mongodb] = override_get_mongodb


# ─────────────────────────────────────────────
# Session-scoped: create / drop tables once per test run
# ─────────────────────────────────────────────
@pytest.fixture(scope="session", autouse=True)
def create_tables():
    Base.metadata.create_all(bind=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)


# ─────────────────────────────────────────────
# Function-scoped: wipe rows between tests
# ─────────────────────────────────────────────
@pytest.fixture(autouse=True)
async def clean_mongo():
    """Drop all mongo collections before each test for isolation."""
    await mock_mongo_db.activity_logs.drop()
    await mock_mongo_db.notes.drop()
    yield


@pytest.fixture(autouse=True)
def clean_postgres():
    """Truncate all postgres tables before each test for isolation."""
    db = TestingSession()
    for table in reversed(Base.metadata.sorted_tables):
        db.execute(table.delete())
    db.commit()
    db.close()
    yield


# ─────────────────────────────────────────────
# Async HTTP client
# ─────────────────────────────────────────────
@pytest_asyncio.fixture
async def client():
    """AsyncClient wired directly to the ASGI app — no real server needed."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac


# ─────────────────────────────────────────────
# Reusable user payload
# ─────────────────────────────────────────────
@pytest.fixture
def user_payload() -> dict:
    return {
        "username": "testuser",
        "email": "test@example.com",
        "password": "StrongPass123!",
    }


# ─────────────────────────────────────────────
# Registered user fixture (hits /auth/signup)
# ─────────────────────────────────────────────
@pytest_asyncio.fixture
async def registered_user(client: AsyncClient, user_payload: dict) -> dict:
    """Creates a user via the API and returns the response body."""
    resp = await client.post("/auth/signup", json=user_payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


# ─────────────────────────────────────────────
# Auth headers fixture (hits /auth/login)
# ─────────────────────────────────────────────
@pytest_asyncio.fixture
async def auth_headers(
    client: AsyncClient,
    registered_user: dict,   # ensures user exists first
    user_payload: dict,
) -> dict:
    """Returns Authorization headers with a valid Bearer token."""
    resp = await client.post(
        "/auth/login",
        data={                              # OAuth2PasswordRequestForm → form data
            "username": user_payload["username"],
            "password": user_payload["password"],
        },
    )
    assert resp.status_code == 200, resp.text
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


# ─────────────────────────────────────────────
# Reusable note payload
# ─────────────────────────────────────────────
@pytest.fixture
def note_payload() -> dict:
    return {
        "title": "Test Note",
        "content": "This is a test note.",
        "tags": ["pytest", "testing"],
    }


# ─────────────────────────────────────────────
# Created note fixture (hits POST /notes)
# ─────────────────────────────────────────────
@pytest_asyncio.fixture
async def created_note(
    client: AsyncClient,
    note_payload: dict,
) -> dict:
    """Creates a note via the API and returns the response body."""
    resp = await client.post("/notes", json=note_payload)
    assert resp.status_code == 201, resp.text
    return resp.json()