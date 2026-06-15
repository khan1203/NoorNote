async def test_get_logs_unauthorized(client):
    response = await client.get("/activity")
    assert response.status_code == 403


async def test_hybrid_endpoint(client, auth_headers, registered_user):
    user_id = registered_user["id"]
    response = await client.get(f"/users/{user_id}/logs", headers=auth_headers)
    assert response.status_code == 200
    assert isinstance(response.json(), list)