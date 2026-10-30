from __future__ import annotations

import mimetypes
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit

import pikepdf
import pypdf
from fastapi import FastAPI, File, Form, Header, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.datastructures import parse_host_header
from starlette.middleware.trustedhost import TrustedHostMiddleware

from docuforge import __version__
from docuforge.config import Settings
from docuforge.errors import DocuForgeError
from docuforge.pdf_engine import PdfEngine
from docuforge.repository import Repository
from docuforge.service import DocumentManager
from docuforge.storage import ManagedStorage

LOCAL_HOSTS = ("127.0.0.1", "localhost", "[::1]")
MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
CLIENT_HEADER = "X-DocuForge-Client"


def _origin_matches_request(origin: str, request: Request) -> bool:
    try:
        parsed_origin = urlsplit(origin)
        origin_port = parsed_origin.port
    except ValueError:
        return False
    if (
        parsed_origin.scheme not in {"http", "https"}
        or parsed_origin.username is not None
        or parsed_origin.password is not None
        or parsed_origin.path not in {"", "/"}
        or parsed_origin.query
        or parsed_origin.fragment
        or parsed_origin.hostname is None
    ):
        return False
    parsed_host = parse_host_header(request.headers.get("host"))
    if parsed_host is None:
        return False
    request_host = parsed_host.host.strip("[]").lower()
    origin_host = parsed_origin.hostname.strip("[]").lower()
    request_port = int(parsed_host.port) if parsed_host.port else (
        443 if request.url.scheme == "https" else 80
    )
    if origin_port is None:
        origin_port = 443 if parsed_origin.scheme == "https" else 80
    return (
        parsed_origin.scheme == request.url.scheme
        and origin_host == request_host
        and origin_port == request_port
    )


def _browser_boundary_error(message: str) -> JSONResponse:
    return JSONResponse(
        status_code=403,
        content={"code": "browser_boundary_rejected", "message": message},
    )


class WorkspaceRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class SessionRequest(BaseModel):
    version_id: str | None = None


class CommandRequest(BaseModel):
    expected_revision: int = Field(ge=0)
    command: dict[str, Any]


class RevisionRequest(BaseModel):
    expected_revision: int = Field(ge=0)


class ExtractRequest(RevisionRequest):
    page_ids: list[str] = Field(min_length=1)


def _parse_range(range_header: str | None, size: int) -> tuple[int, int, int]:
    if not range_header:
        return 0, size - 1, 200
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", range_header.strip())
    if match is None:
        raise ValueError("unsupported byte range")
    start_text, end_text = match.groups()
    if not start_text and not end_text:
        raise ValueError("empty byte range")
    if not start_text:
        suffix = int(end_text)
        if suffix <= 0:
            raise ValueError("invalid suffix range")
        start = max(0, size - suffix)
        end = size - 1
    else:
        start = int(start_text)
        end = int(end_text) if end_text else size - 1
    if start < 0 or start >= size or end < start:
        raise ValueError("byte range is outside the document")
    return start, min(end, size - 1), 206


def create_app(settings: Settings | None = None) -> FastAPI:
    mimetypes.add_type("text/javascript", ".mjs")
    selected = settings or Settings.from_environment()
    storage = ManagedStorage(selected.workspace_root)
    repository = Repository(selected.workspace_root / "docuforge.sqlite3")
    engine = PdfEngine()
    manager = DocumentManager(selected, repository, storage, engine)
    editor_root = Path(__file__).with_name("editor")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield

    app = FastAPI(
        title="DocuForge",
        version=__version__,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    app.state.settings = selected
    app.state.storage = storage
    app.state.repository = repository
    app.state.engine = engine
    app.state.manager = manager
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=list(LOCAL_HOSTS),
        www_redirect=False,
    )

    @app.middleware("http")
    async def browser_boundary(request: Request, call_next):
        if request.method in MUTATING_METHODS and request.url.path.startswith("/api/"):
            client_kind = request.headers.get(CLIENT_HEADER)
            origin = request.headers.get("origin")
            fetch_site = request.headers.get("sec-fetch-site")
            if client_kind not in {"browser", "local-api"}:
                return _browser_boundary_error(
                    f"state-changing requests require {CLIENT_HEADER}"
                )
            if fetch_site is not None and fetch_site != "same-origin":
                return _browser_boundary_error("cross-site browser requests are not allowed")
            if client_kind == "browser":
                if origin is None or not _origin_matches_request(origin, request):
                    return _browser_boundary_error("browser requests must be same-origin")
            elif origin is not None or fetch_site is not None:
                return _browser_boundary_error(
                    "local API clients must not present browser-origin metadata"
                )
        return await call_next(request)

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; img-src 'self' data:; worker-src 'self' blob:; "
            "script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'"
        )
        return response

    @app.exception_handler(DocuForgeError)
    async def controlled_error(_: Request, exc: DocuForgeError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": exc.code, "message": str(exc)},
        )

    @app.exception_handler(ValueError)
    async def validation_error(_: Request, exc: ValueError):
        return JSONResponse(
            status_code=400,
            content={"code": "validation_error", "message": str(exc)},
        )

    @app.exception_handler(Exception)
    async def internal_error(_: Request, __: Exception):
        return JSONResponse(
            status_code=500,
            content={"code": "internal_error", "message": "operation failed"},
        )

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "version": __version__,
            "capabilities": {
                "editor": True,
                "ocr": False,
                "table_processing": False,
                "pdf_engine": "pikepdf",
                "pikepdf_version": pikepdf.__version__,
                "pypdf_version": pypdf.__version__,
            },
        }

    @app.get("/api/workspaces")
    def list_workspaces() -> list[dict[str, Any]]:
        return repository.list_workspaces()

    @app.post("/api/workspaces")
    def create_workspace(payload: WorkspaceRequest) -> dict[str, Any]:
        return repository.create_workspace(payload.name)

    @app.get("/api/workspaces/{workspace_id}/documents")
    def list_documents(workspace_id: str) -> list[dict[str, Any]]:
        repository.get_workspace(workspace_id)
        return repository.list_documents(workspace_id)

    @app.post("/api/workspaces/{workspace_id}/documents")
    def import_document(
        workspace_id: str,
        file: Annotated[UploadFile, File()],
        title: Annotated[str, Form()] = "",
    ) -> dict[str, Any]:
        return manager.import_pdf(
            workspace_id=workspace_id,
            title=title or file.filename or "Untitled.pdf",
            stream=file.file,
        )

    @app.get("/api/documents/{document_id}")
    def get_document(document_id: str) -> dict[str, Any]:
        document = repository.get_document(document_id)
        document["versions"] = repository.list_versions(document_id)
        return document

    @app.post("/api/documents/{document_id}/sessions")
    def create_session(document_id: str, payload: SessionRequest) -> dict[str, Any]:
        return manager.create_session(document_id, payload.version_id)

    @app.get("/api/sessions/{session_id}")
    def get_session(session_id: str) -> dict[str, Any]:
        return repository.get_session(session_id)

    @app.post("/api/sessions/{session_id}/commands")
    def apply_command(session_id: str, payload: CommandRequest) -> dict[str, Any]:
        return manager.apply_command(session_id, payload.expected_revision, payload.command)

    @app.post("/api/sessions/{session_id}/undo")
    def undo(session_id: str, payload: RevisionRequest) -> dict[str, Any]:
        return manager.undo(session_id, payload.expected_revision)

    @app.post("/api/sessions/{session_id}/redo")
    def redo(session_id: str, payload: RevisionRequest) -> dict[str, Any]:
        return manager.redo(session_id, payload.expected_revision)

    @app.post("/api/sessions/{session_id}/save")
    def save(session_id: str, payload: RevisionRequest) -> dict[str, Any]:
        return manager.save(session_id, payload.expected_revision)

    @app.post("/api/sessions/{session_id}/extract")
    def extract(session_id: str, payload: ExtractRequest) -> dict[str, Any]:
        return manager.extract(session_id, payload.expected_revision, payload.page_ids)

    def pdf_response(digest: str, byte_size: int, range_header: str | None):
        try:
            start, end, status = _parse_range(range_header, byte_size)
        except ValueError as exc:
            return JSONResponse(
                status_code=416,
                content={"code": "invalid_range", "message": str(exc)},
                headers={"Content-Range": f"bytes */{byte_size}"},
            )
        length = end - start + 1
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Cache-Control": "private, immutable, max-age=31536000",
        }
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{byte_size}"
        return StreamingResponse(
            storage.iter_range(digest, start, length, selected.stream_chunk_bytes),
            status_code=status,
            media_type="application/pdf",
            headers=headers,
        )

    @app.get("/api/versions/{version_id}/content")
    def version_content(
        version_id: str,
        range: str | None = Header(default=None),
    ):
        version = repository.get_version(version_id)
        return pdf_response(version["content_sha256"], version["byte_size"], range)

    @app.get("/api/artifacts/{artifact_id}/content")
    def artifact_content(
        artifact_id: str,
        range: str | None = Header(default=None),
    ):
        artifact = repository.get_artifact(artifact_id)
        response = pdf_response(artifact["content_sha256"], artifact["byte_size"], range)
        response.headers["Content-Disposition"] = (
            f'attachment; filename="{artifact["title"].replace(chr(34), "")}"'
        )
        return response

    @app.get("/")
    def editor() -> FileResponse:
        return FileResponse(editor_root / "index.html", media_type="text/html")

    app.mount("/assets", StaticFiles(directory=editor_root), name="assets")
    return app


app = create_app()
