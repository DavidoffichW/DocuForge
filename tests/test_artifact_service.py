import pytest

from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from storage.local_fs import LocalFSStorage
from services.artifact_service import ArtifactService


def _build_service(tmp_path):
    storage = LocalFSStorage(tmp_path)
    registry = build_registry()
    policy = ExecutionPolicy(registry)
    return ArtifactService(storage=storage, policy=policy)


def test_create_artifact_deterministic(tmp_path):
    service = _build_service(tmp_path)

    input_ref = {"a": 1}
    params = {"b": 2}

    art1 = service.create("pdf", input_ref, params, b"data")
    art2 = service.create("pdf", input_ref, params, b"data")

    assert art1.artifact_id == art2.artifact_id


def test_artifact_written_to_storage(tmp_path):
    service = _build_service(tmp_path)

    input_ref = {"a": 1}
    params = {"b": 2}

    art = service.create("pdf", input_ref, params, b"data")

    stored = service.storage.read_bytes(art.storage_key)
    assert stored == b"data"