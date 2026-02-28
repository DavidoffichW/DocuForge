from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from api.routes.artifacts import router as artifacts_router
from api.routes.documents import router as documents_router
from api.routes.jobs import router as jobs_router
from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from core.errors import ErrorCode
from core.ids import HybridDocumentIdStrategy
from core.ordering import sort_dict
from services.artifact_service import ArtifactService
from services.document_service import DocumentService
from services.job_service import JobService
from storage.local_fs import LocalFSStorage

from execution.pdf.preview import make_pdf_preview_execution
from execution.pdf.merge import make_pdf_merge_execution
from execution.pdf.reorder import make_pdf_reorder_execution
from execution.pdf.remove import make_pdf_remove_execution
from execution.pdf.extract import make_pdf_extract_execution
from execution.tables.detect import make_tables_detect_execution
from execution.tables.export_csv import make_tables_export_csv_execution
from execution.tables.export_jsonl import make_tables_export_jsonl_execution
from execution.tables.export_zip import make_tables_export_zip_execution


def _canonical_error_response(code: str, message: str, details: Optional[Dict[str, Any]], status: int):
    payload = {
        "code": code,
        "message": message,
        "details": details,
    }
    return JSONResponse(status_code=status, content=sort_dict(payload))


def create_app(
    storage: Optional[Any] = None,
    registry: Optional[Any] = None,
    policy: Optional[Any] = None,
    document_service: Optional[DocumentService] = None,
    artifact_service: Optional[ArtifactService] = None,
    job_service: Optional[JobService] = None,
) -> FastAPI:
    app = FastAPI()

    st = storage if storage is not None else LocalFSStorage("workspace")
    reg = registry if registry is not None else build_registry()
    pol = policy if policy is not None else ExecutionPolicy(reg)

    docs = document_service if document_service is not None else DocumentService(
        storage=st,
        id_strategy=HybridDocumentIdStrategy(),
        policy=pol,
        session_namespace="default_session",
    )

    arts = artifact_service if artifact_service is not None else ArtifactService(
        storage=st,
        policy=pol,
    )

    def _noop_exec(payload: Dict[str, Any]):
        return b"", {"kind": "bin", "media_type": "application/octet-stream", "manifest": {}}

    execution_map: Dict[str, Any] = {
        "noop": _noop_exec,
        "pdf.preview": make_pdf_preview_execution(docs),
        "pdf.merge": make_pdf_merge_execution(docs),
        "pdf.reorder": make_pdf_reorder_execution(docs),
        "pdf.remove": make_pdf_remove_execution(docs),
        "pdf.extract": make_pdf_extract_execution(docs),
        "tables.detect": make_tables_detect_execution(docs),
        "tables.export.csv": make_tables_export_csv_execution(),
        "tables.export.jsonl": make_tables_export_jsonl_execution(),
        "tables.export.zip": make_tables_export_zip_execution(),
    }

    jobs = job_service if job_service is not None else JobService(
        policy=pol,
        documents=docs,
        artifacts=arts,
        execution_map=execution_map,
    )

    app.state.storage = st
    app.state.registry = reg
    app.state.policy = pol
    app.state.document_service = docs
    app.state.artifact_service = arts
    app.state.job_service = jobs

    app.include_router(documents_router)
    app.include_router(jobs_router)
    app.include_router(artifacts_router)

    @app.get("/health")
    def health():
        report = reg.report().to_dict()
        payload = {
            "ok": True,
            "schema_version": "v1",
            "capabilities": report.get("capabilities"),
        }
        return sort_dict(payload)

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        detail = exc.detail
        if isinstance(detail, dict) and "code" in detail and "message" in detail:
            return _canonical_error_response(
                code=detail.get("code"),
                message=detail.get("message"),
                details=detail.get("details"),
                status=exc.status_code,
            )
        return _canonical_error_response(
            code=ErrorCode.INTERNAL_ERROR.value,
            message="internal error",
            details={"error": str(detail)},
            status=exc.status_code,
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        return _canonical_error_response(
            code=ErrorCode.INTERNAL_ERROR.value,
            message="internal error",
            details={"error": str(exc)},
            status=500,
        )

    return app