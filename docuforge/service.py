from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, BinaryIO

from docuforge.config import Settings
from docuforge.errors import InvalidDocumentError
from docuforge.page_commands import apply_page_command
from docuforge.pdf_engine import PdfEngine
from docuforge.repository import Repository
from docuforge.storage import ManagedStorage


class DocumentManager:
    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        storage: ManagedStorage,
        engine: PdfEngine,
    ):
        self.settings = settings
        self.repository = repository
        self.storage = storage
        self.engine = engine

    def import_pdf(
        self,
        *,
        workspace_id: str,
        title: str,
        stream: BinaryIO,
    ) -> dict[str, Any]:
        self.repository.get_workspace(workspace_id)
        staged, digest, byte_size = self.storage.stage_stream(
            stream,
            max_bytes=self.settings.max_upload_bytes,
            chunk_bytes=self.settings.stream_chunk_bytes,
        )
        try:
            with staged.open("rb") as source:
                if source.read(5) != b"%PDF-":
                    raise InvalidDocumentError("uploaded file is not a PDF")
            info = self.engine.validate(staged)
            self.storage.commit_staged(staged, digest)
            document, version = self.repository.create_document(
                workspace_id=workspace_id,
                title=Path(title).name or "Untitled.pdf",
                digest=digest,
                byte_size=byte_size,
                page_count=info.page_count,
            )
            return {"document": document, "version": version, "pdf": info.__dict__}
        except Exception:
            staged.unlink(missing_ok=True)
            raise

    def create_session(self, document_id: str, version_id: str | None = None) -> dict[str, Any]:
        return self.repository.create_session(document_id, version_id)

    def apply_command(
        self,
        session_id: str,
        expected_revision: int,
        command: dict[str, Any],
    ) -> dict[str, Any]:
        session = self.repository.get_session(session_id)
        pages, record = apply_page_command(session["pages"], command)
        history = [*session["history"], record]
        return self.repository.update_session(
            session_id=session_id,
            expected_revision=expected_revision,
            pages=pages,
            history=history,
            redo=[],
            action="command",
            payload=command,
        )

    def undo(self, session_id: str, expected_revision: int) -> dict[str, Any]:
        session = self.repository.get_session(session_id)
        if not session["history"]:
            raise ValueError("nothing to undo")
        history = list(session["history"])
        record = history.pop()
        redo = [*session["redo"], record]
        return self.repository.update_session(
            session_id=session_id,
            expected_revision=expected_revision,
            pages=record["before"],
            history=history,
            redo=redo,
            action="undo",
            payload={"kind": record["kind"]},
        )

    def redo(self, session_id: str, expected_revision: int) -> dict[str, Any]:
        session = self.repository.get_session(session_id)
        if not session["redo"]:
            raise ValueError("nothing to redo")
        redo = list(session["redo"])
        record = redo.pop()
        history = [*session["history"], record]
        return self.repository.update_session(
            session_id=session_id,
            expected_revision=expected_revision,
            pages=record["after"],
            history=history,
            redo=redo,
            action="redo",
            payload={"kind": record["kind"]},
        )

    def save(self, session_id: str, expected_revision: int) -> dict[str, Any]:
        session = self.repository.get_session(session_id)
        if session["revision"] != expected_revision:
            from docuforge.errors import RevisionConflictError

            raise RevisionConflictError("editing session changed before save")
        if not session["dirty"]:
            return {
                "version": self.repository.get_version(session["base_version_id"]),
                "session": session,
                "unchanged": True,
                "byte_reproducible": True,
            }
        source_version = self.repository.get_version(session["base_version_id"])
        source_path = self.storage.blob_path(source_version["content_sha256"])
        materialized = self.engine.materialize(source_path, session["pages"])
        _, digest, byte_size = self.storage.store_bytes(materialized.pdf_bytes)
        version, updated_session = self.repository.commit_saved_version(
            session_id=session_id,
            expected_revision=expected_revision,
            digest=digest,
            byte_size=byte_size,
            page_count=materialized.page_count,
        )
        return {
            "version": version,
            "session": updated_session,
            "unchanged": False,
            "byte_reproducible": materialized.byte_reproducible,
            "semantic_sha256": materialized.semantic_sha256,
        }

    def extract(
        self,
        session_id: str,
        expected_revision: int,
        page_ids: list[str],
    ) -> dict[str, Any]:
        session = self.repository.get_session(session_id)
        if session["revision"] != expected_revision:
            from docuforge.errors import RevisionConflictError

            raise RevisionConflictError("editing session changed before extraction")
        selected = set(page_ids)
        pages = [page for page in session["pages"] if page["id"] in selected]
        if not pages or len(pages) != len(selected):
            raise ValueError("one or more selected pages are unavailable")
        source_version = self.repository.get_version(session["base_version_id"])
        source_path = self.storage.blob_path(source_version["content_sha256"])
        materialized = self.engine.materialize(source_path, pages)
        _, digest, byte_size = self.storage.store_bytes(materialized.pdf_bytes)
        artifact = self.repository.create_artifact(
            session_id=session_id,
            digest=digest,
            byte_size=byte_size,
            title="extracted-pages.pdf",
        )
        return {
            "artifact": artifact,
            "byte_reproducible": materialized.byte_reproducible,
            "semantic_sha256": materialized.semantic_sha256,
        }

    def copy_blob_to(self, digest: str, destination: Path) -> None:
        source = self.storage.blob_path(digest)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as source_file, destination.open("wb") as output:
            shutil.copyfileobj(source_file, output, self.settings.stream_chunk_bytes)
