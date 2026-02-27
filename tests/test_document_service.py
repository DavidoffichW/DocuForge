import pytest

from core.ids import HybridDocumentIdStrategy
from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from storage.local_fs import LocalFSStorage
from services.document_service import DocumentService


def _build_service(tmp_path):
    storage = LocalFSStorage(tmp_path)
    registry = build_registry()
    policy = ExecutionPolicy(registry)
    strategy = HybridDocumentIdStrategy()
    return DocumentService(storage=storage, id_strategy=strategy, policy=policy)


def test_ingest_document_deterministic(tmp_path):
    service = _build_service(tmp_path)

    data = b"abc"
    doc1 = service.ingest(data, ingest_index=0)
    doc2 = service.ingest(data, ingest_index=0)

    assert doc1.document_id == doc2.document_id
    assert doc1.content_sha256 == doc2.content_sha256


def test_ingest_document_storage_written(tmp_path):
    service = _build_service(tmp_path)

    data = b"abc"
    doc = service.ingest(data, ingest_index=0)

    stored = service.storage.read_bytes(doc.storage_key)
    assert stored == data


def test_ingest_document_different_index(tmp_path):
    service = _build_service(tmp_path)

    data = b"abc"
    doc1 = service.ingest(data, ingest_index=0)
    doc2 = service.ingest(data, ingest_index=1)

    assert doc1.document_id != doc2.document_id