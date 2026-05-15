import pytest
from httpx import AsyncClient


# ─── Signup ───────────────────────────────────────────────────────────────────

async def test_signup_success(client: AsyncClient, user_payload):
    resp = await client.post("/auth/signup", json=user_payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["email"] == user_payload["email"]
    assert data["username"] == user_payload["username"]
    assert "password" not in data
    assert "password_hash" not in data


async def test_signup_duplicate_email(client: AsyncClient, user_payload, registered_user):
    dupe = {**user_payload, "username": "other"}
    resp = await client.post("/auth/signup", json=dupe)
    assert resp.status_code == 400
    assert "email" in resp.json()["detail"].lower()


async def test_signup_duplicate_username(client: AsyncClient, user_payload, registered_user):
    dupe = {**user_payload, "email": "other@example.com"}
    resp = await client.post("/auth/signup", json=dupe)
    assert resp.status_code == 400
    assert "username" in resp.json()["detail"].lower()


async def test_signup_missing_fields(client: AsyncClient):
    resp = await client.post("/auth/signup", json={"email": "x@x.com"})
    assert resp.status_code == 422


# ─── Login ────────────────────────────────────────────────────────────────────

async def test_login_success(client: AsyncClient, registered_user, user_payload):
    resp = await client.post(
        "/auth/login",
        data={"username": user_payload["username"], "password": user_payload["password"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


async def test_login_wrong_password(client: AsyncClient, registered_user, user_payload):
    resp = await client.post(
        "/auth/login",
        data={"username": user_payload["username"], "password": "wrongpass"},
    )
    assert resp.status_code == 401


async def test_login_nonexistent_user(client: AsyncClient):
    resp = await client.post(
        "/auth/login",
        data={"username": "ghost", "password": "irrelevant"},
    )
    assert resp.status_code == 401


async def test_login_logs_to_mongo(client: AsyncClient, registered_user, user_payload):
    from conftest import mock_mongo_db
    await client.post(
        "/auth/login",
        data={"username": user_payload["username"], "password": user_payload["password"]},
    )
    log = await mock_mongo_db.activity_logs.find_one({"action": "login"})
    assert log is not None
    assert log["action"] == "login"


# ─── Profile ──────────────────────────────────────────────────────────────────

async def test_get_profile_success(client: AsyncClient, auth_headers, user_payload):
    resp = await client.get("/profile", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["email"] == user_payload["email"]


async def test_get_profile_no_token(client: AsyncClient):
    resp = await client.get("/profile")
    assert resp.status_code == 403


async def test_get_profile_invalid_token(client: AsyncClient):
    resp = await client.get("/profile", headers={"Authorization": "Bearer invalidtoken"})
    assert resp.status_code == 401


async def test_get_profile_logs_to_mongo(client: AsyncClient, auth_headers):
    from conftest import mock_mongo_db
    await client.get("/profile", headers=auth_headers)
    log = await mock_mongo_db.activity_logs.find_one({"action": "profile_view"})
    assert log is not None