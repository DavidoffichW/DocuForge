import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from storage.local_fs import LocalFSStorage
from services.document_service import DocumentService
from services.artifact_service import ArtifactService
from services.job_service import JobService
from core.ids import HybridDocumentIdStrategy


@pytest.fixture
def app(tmp_path):
    storage = LocalFSStorage(tmp_path)
    registry = build_registry()
    policy = ExecutionPolicy(registry)

    document_service = DocumentService(
        storage=storage,
        id_strategy=HybridDocumentIdStrategy(),
        policy=policy,
    )

    artifact_service = ArtifactService(
        storage=storage,
        policy=policy,
    )

    job_service = JobService(
        storage=storage,
        policy=policy,
        artifact_service=artifact_service,
    )

    app = create_app(
        document_service=document_service,
        artifact_service=artifact_service,
        job_service=job_service,
        capability_registry=registry,
    )

    return app


@pytest.fixture
def client(app):
    return TestClient(app)