from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict


def sha256_hex(data: bytes) -> str:
    if not isinstance(data, (bytes, bytearray)):
        raise ValueError("data must be bytes")
    return hashlib.sha256(bytes(data)).hexdigest()


def _canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


@dataclass(frozen=True)
class DocumentIdentity:
    document_id: str
    content_sha256: str
    strategy: str
    ingest_index: int


class DocumentIdStrategy:
    def name(self) -> str:
        raise NotImplementedError


class HybridDocumentIdStrategy(DocumentIdStrategy):
    def name(self) -> str:
        return "hybrid_v1"


def document_identity_from_bytes(
    data: bytes,
    strategy: DocumentIdStrategy,
    session_namespace: str,
    ingest_index: int,
) -> DocumentIdentity:
    if not isinstance(data, (bytes, bytearray)):
        raise ValueError("data must be bytes")
    if not isinstance(session_namespace, str) or not session_namespace.strip():
        raise ValueError("session_namespace must be non-empty string")
    if not isinstance(ingest_index, int) or ingest_index < 0:
        raise ValueError("ingest_index must be non-negative int")

    content_hash = sha256_hex(bytes(data))
    basis = {
        "content_sha256": content_hash,
        "ingest_index": ingest_index,
        "session_namespace": session_namespace,
        "strategy": strategy.name(),
    }
    doc_id = sha256_hex(_canonical_json(basis))
    return DocumentIdentity(
        document_id=doc_id,
        content_sha256=content_hash,
        strategy=strategy.name(),
        ingest_index=ingest_index,
    )


def make_artifact_id(kind: str, input_ref: Dict[str, Any], params: Dict[str, Any]) -> str:
    if not isinstance(kind, str) or not kind.strip():
        raise ValueError("kind must be non-empty string")
    if not isinstance(input_ref, dict):
        raise ValueError("input_ref must be dict")
    if not isinstance(params, dict):
        raise ValueError("params must be dict")

    basis = {
        "kind": kind,
        "input_ref": input_ref,
        "params": params,
    }
    return sha256_hex(_canonical_json(basis))


def make_job_id(operation: str, input_ref: Dict[str, Any], params: Dict[str, Any]) -> str:
    if not isinstance(operation, str) or not operation.strip():
        raise ValueError("operation must be non-empty string")
    if not isinstance(input_ref, dict):
        raise ValueError("input_ref must be dict")
    if not isinstance(params, dict):
        raise ValueError("params must be dict")

    basis = {
        "operation": operation,
        "input_ref": input_ref,
        "params": params,
    }
    return sha256_hex(_canonical_json(basis))