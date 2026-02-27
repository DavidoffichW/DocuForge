import pytest
from pydantic import ValidationError

from domain import (
    DocumentRecord as Document,
    JobRecord as Job,
    JobStatus,
    ArtifactRecord as Artifact,
)


def test_document_model_immutable():
    doc = Document(
        document_id="doc_1",
        content_sha256="abc",
        ingest_index=0,
        storage_key="documents/doc_1.pdf",
    )

    with pytest.raises(TypeError):
        doc.document_id = "changed"


def test_document_model_extra_fields_rejected():
    with pytest.raises(ValidationError):
        Document(
            document_id="doc_1",
            content_sha256="abc",
            ingest_index=0,
            storage_key="documents/doc_1.pdf",
            extra_field="not_allowed",
        )


def test_document_model_canonical_dict_order():
    doc = Document(
        document_id="doc_1",
        content_sha256="abc",
        ingest_index=0,
        storage_key="documents/doc_1.pdf",
    )

    d = doc.model_dump()
    keys = list(d.keys())

    assert keys == sorted(keys)


def test_job_model_validation_and_immutability():
    job = Job(
        job_id="job_1",
        operation="merge",
        input_ref={"a": 1},
        params={"b": 2},
    )

    with pytest.raises(TypeError):
        job.operation = "changed"

    d = job.model_dump()
    assert list(d.keys()) == sorted(d.keys())


def test_artifact_model_validation():
    artifact = Artifact(
        artifact_id="art_1",
        kind="pdf",
        storage_key="artifacts/art_1.pdf",
    )

    assert artifact.artifact_id == "art_1"

    with pytest.raises(TypeError):
        artifact.kind = "changed"