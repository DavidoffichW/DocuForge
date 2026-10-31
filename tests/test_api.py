from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pikepdf
from fastapi.testclient import TestClient

from docuforge.app import create_app
from docuforge.config import Settings

LOCAL_API_HEADERS = {"X-DocuForge-Client": "local-api"}


def test_api_upload_range_edit_save_and_reopen(tmp_path: Path, pdf_bytes: bytes) -> None:
    app = create_app(Settings(workspace_root=tmp_path / "managed", max_upload_bytes=2_000_000))
    with TestClient(app, base_url="http://127.0.0.1", headers=LOCAL_API_HEADERS) as client:
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
    with TestClient(
        app,
        base_url="http://127.0.0.1",
        headers=LOCAL_API_HEADERS,
        raise_server_exceptions=False,
    ) as client:
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
    with TestClient(app, base_url="http://127.0.0.1") as client:
        editor = client.get("/")
        assert editor.status_code == 200
        assert "DocuForge" in editor.text
        health = client.get("/api/health").json()
        assert health["capabilities"]["pdf_engine"] == "pikepdf"
        assert health["capabilities"]["ocr"] is False


def test_local_host_and_browser_origin_boundary(tmp_path: Path) -> None:
    app = create_app(Settings(workspace_root=tmp_path / "managed"))

    for base_url in ("http://127.0.0.1:8123", "http://localhost:8123", "http://[::1]:8123"):
        with TestClient(app, base_url=base_url) as client:
            assert client.get("/api/health").status_code == 200

    with TestClient(app, base_url="http://127.0.0.1:8123") as client:
        assert client.get("/api/health", headers={"Host": "attacker.invalid"}).status_code == 400
        assert client.get(
            "/api/health", headers={"Host": "loopback-alias.invalid:8123"}
        ).status_code == 400

        same_origin = client.post(
            "/api/workspaces",
            json={"name": "Browser workspace"},
            headers={
                "X-DocuForge-Client": "browser",
                "Origin": "http://127.0.0.1:8123",
                "Sec-Fetch-Site": "same-origin",
            },
        )
        assert same_origin.status_code == 200

        hostile_origin = client.post(
            "/api/workspaces",
            json={"name": "Blocked"},
            headers={
                "X-DocuForge-Client": "browser",
                "Origin": "https://attacker.invalid",
                "Sec-Fetch-Site": "cross-site",
            },
        )
        assert hostile_origin.status_code == 403
        assert hostile_origin.json()["code"] == "browser_boundary_rejected"

        for headers in (
            {},
            {"X-DocuForge-Client": "browser", "Origin": "null"},
            {"X-DocuForge-Client": "browser"},
            {
                "X-DocuForge-Client": "browser",
                "Origin": "http://127.0.0.1:8123",
                "Sec-Fetch-Site": "same-site",
            },
        ):
            assert client.post(
                "/api/workspaces", json={"name": "Rejected"}, headers=headers
            ).status_code == 403

        local_api = client.post(
            "/api/workspaces",
            json={"name": "Local API workspace"},
            headers=LOCAL_API_HEADERS,
        )
        assert local_api.status_code == 200

        local_api_with_browser_metadata = client.post(
            "/api/workspaces",
            json={"name": "Rejected local API"},
            headers={
                **LOCAL_API_HEADERS,
                "Origin": "http://127.0.0.1:8123",
            },
        )
        assert local_api_with_browser_metadata.status_code == 403


def test_unsafe_action_save_fails_without_creating_a_version(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
) -> None:
    source = BytesIO(interactive_pdf_bytes)
    output = BytesIO()
    with pikepdf.Pdf.open(source) as pdf:
        link = next(
            annotation
            for annotation in pdf.pages[0].obj["/Annots"]
            if str(annotation.get("/Subtype")) == "/Link"
        )
        link["/A"] = pdf.make_indirect(
            pikepdf.Dictionary(
                S=pikepdf.Name("/JavaScript"),
                JS=pikepdf.String("app.alert('not executed')"),
            )
        )
        if "/Dest" in link:
            del link["/Dest"]
        pdf.save(output)

    app = create_app(Settings(workspace_root=tmp_path / "managed"))
    with TestClient(
        app,
        base_url="http://127.0.0.1",
        headers=LOCAL_API_HEADERS,
    ) as client:
        imported = client.post(
            "/api/workspaces/default/documents",
            files={"file": ("unsafe-action.pdf", output.getvalue(), "application/pdf")},
            data={"title": "unsafe-action.pdf"},
        ).json()
        document_id = imported["document"]["id"]
        session = client.post(
            f"/api/documents/{document_id}/sessions",
            json={"version_id": imported["version"]["id"]},
        ).json()
        edited = client.post(
            f"/api/sessions/{session['id']}/commands",
            json={
                "expected_revision": session["revision"],
                "command": {
                    "kind": "rotate",
                    "page_ids": [session["pages"][0]["id"]],
                    "delta": 90,
                },
            },
        ).json()
        blocked = client.post(
            f"/api/sessions/{session['id']}/save",
            json={"expected_revision": edited["revision"]},
        )
        assert blocked.status_code == 409
        assert blocked.json()["code"] == "fidelity_check_failed"
        document = client.get(f"/api/documents/{document_id}").json()
        assert len(document["versions"]) == 1
        current_session = client.get(f"/api/sessions/{session['id']}").json()
        assert current_session["dirty"] is True
