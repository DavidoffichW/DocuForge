from __future__ import annotations

import hashlib
import io

import pytest

from docuforge.errors import FidelityError, InvalidDocumentError, RevisionConflictError
from docuforge.pdf_engine import PdfEngine, inspect_semantics
from docuforge.repository import Repository
from docuforge.service import DocumentManager
from docuforge.storage import ManagedStorage


def import_fixture(manager: DocumentManager, data: bytes, workspace_id: str = "default"):
    return manager.import_pdf(
        workspace_id=workspace_id,
        title="fixture.pdf",
        stream=io.BytesIO(data),
    )


def test_open_edit_save_restart_reopen_and_original_unchanged(
    manager: DocumentManager,
    pdf_bytes: bytes,
) -> None:
    original_hash = hashlib.sha256(pdf_bytes).hexdigest()
    imported = import_fixture(manager, pdf_bytes)
    source_version = imported["version"]
    session = manager.create_session(imported["document"]["id"])
    first_id = session["pages"][0]["id"]
    last_id = session["pages"][-1]["id"]
    session = manager.apply_command(
        session["id"], session["revision"],
        {"kind": "move", "page_id": last_id, "to_index": 0},
    )
    session = manager.apply_command(
        session["id"], session["revision"],
        {"kind": "rotate", "page_ids": [first_id], "delta": 90},
    )
    result = manager.save(session["id"], session["revision"])
    assert result["version"]["parent_version_id"] == source_version["id"]
    assert result["session"]["dirty"] is False
    assert manager.storage.blob_path(source_version["content_sha256"]).read_bytes() == pdf_bytes
    assert source_version["content_sha256"] == original_hash

    repository = Repository(manager.repository.database_path)
    restarted = DocumentManager(
        manager.settings,
        repository,
        ManagedStorage(manager.settings.workspace_root),
        PdfEngine(),
    )
    versions = restarted.repository.list_versions(imported["document"]["id"])
    assert len(versions) == 2
    saved = restarted.storage.blob_path(versions[-1]["content_sha256"]).read_bytes()
    semantic = inspect_semantics(saved)
    assert semantic["page_count"] == 3
    assert semantic["pages"][1]["rotation"] == 90
    reopened = restarted.create_session(imported["document"]["id"], versions[-1]["id"])
    assert [page["source_index"] for page in reopened["pages"]] == [0, 1, 2]


def test_undo_redo_and_revision_conflicts(manager: DocumentManager, pdf_bytes: bytes) -> None:
    imported = import_fixture(manager, pdf_bytes)
    session = manager.create_session(imported["document"]["id"])
    session = manager.apply_command(
        session["id"], 0,
        {"kind": "rotate", "page_ids": [session["pages"][0]["id"]], "delta": 90},
    )
    with pytest.raises(RevisionConflictError):
        manager.apply_command(
            session["id"], 0,
            {"kind": "delete", "page_ids": [session["pages"][1]["id"]]},
        )
    undone = manager.undo(session["id"], session["revision"])
    assert undone["pages"][0]["rotation"] == 0
    assert undone["dirty"] is False
    redone = manager.redo(undone["id"], undone["revision"])
    assert redone["pages"][0]["rotation"] == 90


def test_workspace_and_document_identities_are_independent(
    manager: DocumentManager,
    pdf_bytes: bytes,
) -> None:
    workspace_a = manager.repository.create_workspace("A")
    workspace_b = manager.repository.create_workspace("B")
    document_a = import_fixture(manager, pdf_bytes, workspace_a["id"])["document"]
    document_b = import_fixture(manager, pdf_bytes, workspace_b["id"])["document"]
    assert document_a["id"] != document_b["id"]
    assert document_a["workspace_id"] != document_b["workspace_id"]


def test_merge_versions_creates_managed_document_and_preserves_sources(
    manager: DocumentManager,
    pdf_bytes: bytes,
    table_pdf_bytes: bytes,
) -> None:
    first = import_fixture(manager, pdf_bytes)
    second = manager.import_pdf(
        workspace_id="default",
        title="tables.pdf",
        stream=io.BytesIO(table_pdf_bytes),
    )
    source_hashes = [
        first["version"]["content_sha256"],
        second["version"]["content_sha256"],
    ]

    merged = manager.merge_versions(
        workspace_id="default",
        title="combined.pdf",
        version_ids=[second["version"]["id"], first["version"]["id"]],
    )

    assert merged["document"]["title"] == "combined.pdf"
    assert merged["version"]["reason"] == "merge"
    assert merged["version"]["page_count"] == 4
    assert merged["source_version_ids"] == [
        second["version"]["id"],
        first["version"]["id"],
    ]
    assert manager.repository.list_document_sources(merged["document"]["id"]) == [
        {"position": 0, "source_version_id": second["version"]["id"]},
        {"position": 1, "source_version_id": first["version"]["id"]},
    ]
    assert len(manager.repository.list_documents("default")) == 3
    for digest in source_hashes:
        assert manager.storage.blob_path(digest).exists()

    restarted = Repository(manager.repository.database_path)
    reopened = restarted.get_version(merged["version"]["id"])
    assert reopened["content_sha256"] == merged["version"]["content_sha256"]
    assert [
        source["source_version_id"]
        for source in restarted.list_document_sources(merged["document"]["id"])
    ] == merged["source_version_ids"]


def test_failed_fidelity_merge_does_not_create_document(
    manager: DocumentManager,
    interactive_pdf_bytes: bytes,
) -> None:
    imported = import_fixture(manager, interactive_pdf_bytes)
    before = manager.repository.list_documents("default")
    with pytest.raises(FidelityError):
        manager.merge_versions(
            workspace_id="default",
            title="blocked.pdf",
            version_ids=[imported["version"]["id"], imported["version"]["id"]],
        )
    assert manager.repository.list_documents("default") == before


def test_invalid_and_oversized_uploads_leave_no_document(manager: DocumentManager) -> None:
    with pytest.raises(InvalidDocumentError):
        import_fixture(manager, b"not a pdf")
    manager.settings = type(manager.settings)(
        workspace_root=manager.settings.workspace_root,
        max_upload_bytes=10,
        stream_chunk_bytes=4,
    )
    with pytest.raises(Exception) as exc:
        import_fixture(manager, b"%PDF-" + b"x" * 20)
    assert exc.value.__class__.__name__ == "DocumentTooLargeError"
    assert manager.repository.list_documents("default") == []


def test_failed_materialization_does_not_create_version(
    manager: DocumentManager,
    interactive_pdf_bytes: bytes,
) -> None:
    imported = import_fixture(manager, interactive_pdf_bytes)
    session = manager.create_session(imported["document"]["id"])
    session = manager.apply_command(
        session["id"], 0,
        {"kind": "delete", "page_ids": [session["pages"][1]["id"]]},
    )
    with pytest.raises(Exception) as exc:
        manager.save(session["id"], session["revision"])
    assert exc.value.__class__.__name__ == "FidelityError"
    assert len(manager.repository.list_versions(imported["document"]["id"])) == 1
    assert manager.repository.get_session(session["id"])["dirty"] is True
