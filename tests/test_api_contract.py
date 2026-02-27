import json


def test_health_endpoint_structure(client):
    response = client.get("/health")
    assert response.status_code == 200

    data = response.json()

    assert "capabilities" in data
    assert "schema_version" in data

    assert list(data.keys()) == sorted(data.keys())


def test_openapi_schema_available(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200

    schema = response.json()

    assert "paths" in schema
    assert "components" in schema


def test_document_upload_and_ingest(client):
    files = {"file": ("test.pdf", b"hello world", "application/pdf")}
    response = client.post("/documents/upload", files=files)

    assert response.status_code == 200

    data = response.json()

    assert "document_id" in data
    assert list(data.keys()) == sorted(data.keys())


def test_job_execution_endpoint_deterministic(client):
    files = {"file": ("test.pdf", b"hello world", "application/pdf")}
    upload = client.post("/documents/upload", files=files)

    doc_id = upload.json()["document_id"]

    payload = {
        "operation": "noop",
        "input_ref": {"document_id": doc_id},
        "params": {},
    }

    r1 = client.post("/jobs/execute", json=payload)
    r2 = client.post("/jobs/execute", json=payload)

    assert r1.status_code == 200
    assert r2.status_code == 200

    assert r1.json()["job_id"] == r2.json()["job_id"]


def test_error_envelope_structure(client):
    payload = {
        "operation": "unknown_operation",
        "input_ref": {},
        "params": {},
    }

    response = client.post("/jobs/execute", json=payload)

    assert response.status_code >= 400

    data = response.json()

    assert "code" in data
    assert "message" in data
    assert list(data.keys()) == sorted(data.keys())