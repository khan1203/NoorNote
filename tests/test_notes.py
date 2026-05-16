async def test_create_note(client):
    payload = {
        "title": "Test Note",
        "content": "This is a test note",
        "tags": ["fastapi", "mongodb"]
    }
    response = await client.post("/notes", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["title"] == payload["title"]
    assert data["content"] == payload["content"]
    assert data["tags"] == payload["tags"]


async def test_get_all_notes(client):
    response = await client.get("/notes")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


async def test_get_single_note(client):
    payload = {
        "title": "Single Note",
        "content": "Get note test",
        "tags": []
    }
    create_response = await client.post("/notes", json=payload)
    note_id = create_response.json()["id"]
    response = await client.get(f"/notes/{note_id}")
    assert response.status_code == 200
    assert response.json()["id"] == note_id


async def test_update_note(client):
    payload = {
        "title": "Old Title",
        "content": "Old content",
        "tags": []
    }
    create_response = await client.post("/notes", json=payload)
    note_id = create_response.json()["id"]
    response = await client.put(f"/notes/{note_id}", json={"title": "Updated Title"})
    assert response.status_code == 200
    assert response.json()["title"] == "Updated Title"


async def test_delete_note(client):
    payload = {
        "title": "Delete Me",
        "content": "Temporary",
        "tags": []
    }
    create_response = await client.post("/notes", json=payload)
    note_id = create_response.json()["id"]
    response = await client.delete(f"/notes/{note_id}")
    assert response.status_code == 204