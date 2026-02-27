import pytest

from core.ids import (
    HybridDocumentIdStrategy,
    document_identity_from_bytes,
    make_job_id,
    make_artifact_id,
)


def test_document_identity_deterministic_same_inputs():
    data = b"hello world"
    strategy = HybridDocumentIdStrategy()

    id1 = document_identity_from_bytes(data, strategy, "session_x", 0)
    id2 = document_identity_from_bytes(data, strategy, "session_x", 0)

    assert id1.document_id == id2.document_id
    assert id1.content_sha256 == id2.content_sha256


def test_document_identity_changes_with_ingest_index():
    data = b"hello world"
    strategy = HybridDocumentIdStrategy()

    id1 = document_identity_from_bytes(data, strategy, "session_x", 0)
    id2 = document_identity_from_bytes(data, strategy, "session_x", 1)

    assert id1.document_id != id2.document_id
    assert id1.content_sha256 == id2.content_sha256


def test_job_id_deterministic():
    op = "merge"
    input_ref = {"a": 1}
    params = {"b": 2}

    j1 = make_job_id(op, input_ref, params)
    j2 = make_job_id(op, input_ref, params)

    assert j1 == j2


def test_artifact_id_deterministic():
    kind = "pdf"
    input_ref = {"x": 1}
    params = {"y": 2}

    a1 = make_artifact_id(kind, input_ref, params)
    a2 = make_artifact_id(kind, input_ref, params)

    assert a1 == a2