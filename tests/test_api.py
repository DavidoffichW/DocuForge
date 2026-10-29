from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from docuforge.app import create_app
from docuforge.config import Settings


def test_api_upload_range_edit_save_and_reopen(tmp_path: Path, pdf_bytes: bytes) -> None:
    app = create_app(Settings(workspace_root=tmp_path / "managed", max_upload_bytes=2_000_000))
    with TestClient(app) as client:
        upload = client.post(
            "/api/workspaces/default/documents",
            files={"file": ("fixture.pdf", pdf_bytes, "application/pdf")},
            data={"title": "fixture.pdf"},
        )
        assert upload.status_code == 200
        imported = upload.json()
        version = imported["version"]

        ranged = client.get(
            f"/api/versions/{version['id']}/content",
            headers={"Range": "bytes=0-19"},
        )
        assert ranged.status_code == 206
        assert ranged.headers["content-range"] == f"bytes 0-19/{len(pdf_bytes)}"
        assert ranged.content == pdf_bytes[:20]

        invalid_range = client.get(
            f"/api/versions/{version['id']}/content",
            headers={"Range": f"bytes={len(pdf_bytes) + 1}-"},
        )
        assert invalid_range.status_code == 416

        session = client.post(
            f"/api/documents/{imported['document']['id']}/sessions",
            json={"version_id": version["id"]},
        ).json()
        command = client.post(
            f"/api/sessions/{session['id']}/commands",
            json={
                "expected_revision": 0,
                "command": {
                    "kind": "rotate",
                    "page_ids": [session["pages"][0]["id"]],
                    "delta": 90,
                },
            },
        )
        assert command.status_code == 200
        edited = command.json()
        saved = client.post(
            f"/api/sessions/{session['id']}/save",
            json={"expected_revision": edited["revision"]},
        )
        assert saved.status_code == 200
        assert saved.json()["version"]["parent_version_id"] == version["id"]

        document = client.get(f"/api/documents/{imported['document']['id']}").json()
        assert len(document["versions"]) == 2


def test_api_errors_are_controlled_and_do_not_leak_paths(tmp_path: Path) -> None:
    app = create_app(Settings(workspace_root=tmp_path / "private-folder"))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/documents/missing")
        assert response.status_code == 404
        assert response.json() == {"code": "not_found", "message": "document not found"}
        assert str(tmp_path) not in response.text

        response = client.post(
            "/api/workspaces/default/documents",
            files={"file": ("bad.pdf", b"%PDF-broken", "application/pdf")},
            data={"title": "bad.pdf"},
        )
        assert response.status_code == 400
        assert response.json()["code"] == "invalid_document"
        assert str(tmp_path) not in response.text


def test_editor_and_health_are_served_by_same_application(tmp_path: Path) -> None:
    app = create_app(Settings(workspace_root=tmp_path / "managed"))
    with TestClient(app) as client:
        editor = client.get("/")
        assert editor.status_code == 200
        assert "DocuForge" in editor.text
        health = client.get("/api/health").json()
        assert health["capabilities"]["pdf_engine"] == "pikepdf"
        assert health["capabilities"]["ocr"] is False
