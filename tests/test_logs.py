def test_get_logs_unauthorized(client):
    response = client.get("/logs")

    assert response.status_code == 403