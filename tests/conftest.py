import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.main import create_app
from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from core.ids import HybridDocumentIdStrategy
from services.artifact_service import ArtifactService
from services.document_service import DocumentService
from services.job_service import JobService
from storage.local_fs import LocalFSStorage


@pytest.fixture
def app(tmp_path):
    storage = LocalFSStorage(root_dir=str(tmp_path))
    registry = build_registry()
    policy = ExecutionPolicy(registry)

    document_service = DocumentService(storage=storage)
    artifact_service = ArtifactService(storage=storage)

    execution_map = {}

    job_service = JobService(
        policy=policy,
        documents=document_service,
        artifacts=artifact_service,
        execution_map=execution_map,
    )

    app = create_app()

    return app


@pytest.fixture
def client(app):
    return TestClient(app)