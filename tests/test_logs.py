def test_get_logs_unauthorized(client):
    response = client.get("/logs")

    assert response.status_code == 403


def test_hybrid_endpoint(client):
    payload = {
        "user_id": 1,
        "note_id": "123",
        "action": "view"
    }

    response = client.post("/hybrid-endpoint", json=payload)

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "success"
    assert data["user_id"] == payload["user_id"]
    assert data["note_id"] == payload["note_id"]