from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from services.artifact_service import ArtifactService
from services.document_service import DocumentService
from services.job_service import JobService
from storage.local_fs import LocalFSStorage

from api.routes.documents import router as documents_router
from api.routes.jobs import router as jobs_router
from api.routes.artifacts import router as artifacts_router


def create_app() -> FastAPI:
    storage = LocalFSStorage(root_dir="data")

    registry = build_registry()
    policy = ExecutionPolicy(registry)

    documents = DocumentService(storage=storage)
    artifacts = ArtifactService(storage=storage)

    execution_map = {}

    jobs = JobService(
        policy=policy,
        documents=documents,
        artifacts=artifacts,
        execution_map=execution_map,
    )

    app = FastAPI(title="DocuForge API", version="0.1.0")

    documents_router.state.document_service = documents
    jobs_router.state.job_service = jobs
    artifacts_router.state.storage = storage

    app.include_router(documents_router)
    app.include_router(jobs_router)
    app.include_router(artifacts_router)

    @app.get("/health")
    def health():
        return {
            "ok": True,
            "capabilities": registry.report().to_dict(),
        }

    @app.exception_handler(Exception)
    def unhandled_exception_handler(_, exc: Exception):
        return JSONResponse(
            status_code=500,
            content={
                "message": "unhandled exception",
                "details": {"error": str(exc)},
            },
        )

    return app


app = create_app()