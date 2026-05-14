def test_create_note(client):
    payload = {
        "title": "Test Note",
        "content": "This is a test note",
        "tags": ["fastapi", "mongodb"]
    }

    response = client.post("/notes", json=payload)

    assert response.status_code == 201

    data = response.json()

    assert data["title"] == payload["title"]
    assert data["content"] == payload["content"]
    assert data["tags"] == payload["tags"]


def test_get_all_notes(client):
    response = client.get("/notes")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_single_note(client):
    payload = {
        "title": "Single Note",
        "content": "Get note test",
        "tags": []
    }

    create_response = client.post("/notes", json=payload)

    note_id = create_response.json()["id"]

    response = client.get(f"/notes/{note_id}")

    assert response.status_code == 200

    data = response.json()

    assert data["id"] == note_id


def test_update_note(client):
    payload = {
        "title": "Old Title",
        "content": "Old content",
        "tags": []
    }

    create_response = client.post("/notes", json=payload)

    note_id = create_response.json()["id"]

    update_payload = {
        "title": "Updated Title"
    }

    response = client.put(
        f"/notes/{note_id}",
        json=update_payload
    )

    assert response.status_code == 200

    data = response.json()

    assert data["title"] == "Updated Title"


def test_delete_note(client):
    payload = {
        "title": "Delete Me",
        "content": "Temporary",
        "tags": []
    }

    create_response = client.post("/notes", json=payload)

    note_id = create_response.json()["id"]

    response = client.delete(f"/notes/{note_id}")

    assert response.status_code == 204