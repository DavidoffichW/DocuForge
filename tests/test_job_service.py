import pytest

from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from storage.local_fs import LocalFSStorage
from services.document_service import DocumentService
from services.artifact_service import ArtifactService
from services.job_service import JobService
from core.ids import HybridDocumentIdStrategy


def _build_services(tmp_path):
    storage = LocalFSStorage(tmp_path)
    registry = build_registry()
    policy = ExecutionPolicy(registry)

    doc_service = DocumentService(
        storage=storage,
        id_strategy=HybridDocumentIdStrategy(),
        policy=policy,
    )
    art_service = ArtifactService(storage=storage, policy=policy)
    job_service = JobService(
        storage=storage,
        policy=policy,
        artifact_service=art_service,
    )

    return doc_service, art_service, job_service, policy


def test_job_execution_deterministic(tmp_path):
    doc_service, art_service, job_service, policy = _build_services(tmp_path)

    data = b"abc"
    doc = doc_service.ingest(data, ingest_index=0)

    params = {"mode": "noop"}

    job1 = job_service.execute("noop", {"document_id": doc.document_id}, params)
    job2 = job_service.execute("noop", {"document_id": doc.document_id}, params)

    assert job1.job_id == job2.job_id


def test_job_blocks_missing_capability(tmp_path, monkeypatch):
    doc_service, art_service, job_service, policy = _build_services(tmp_path)

    def fake_require(name):
        class D:
            allowed = False
        return D()

    monkeypatch.setattr(policy, "require", fake_require)

    with pytest.raises(Exception):
        job_service.execute("noop", {"document_id": "x"}, {})