from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
from dataclasses import dataclass
from typing import Any, Dict, Protocol, Tuple


@dataclass(frozen=True)
class DocumentIdentity:
    document_id: str
    content_sha256: str
    strategy: str
    ingest_index: int


class DocumentIdStrategy(Protocol):
    def derive(self, data: bytes, session_namespace: str, ingest_index: int) -> DocumentIdentity: ...


def canonical_json_bytes(obj: Any) -> bytes:
    s = json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return s.encode("utf-8")


def sha256_hex(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def make_session_namespace() -> str:
    payload = {
        "py": sys.version.split()[0],
        "exe": sys.executable,
        "plat": platform.platform(),
        "cwd": os.getcwd(),
        "pid": os.getpid(),
    }
    digest = sha256_hex(canonical_json_bytes(payload))
    return f"session_{digest}"


def make_job_id(operation: str, input_ref: Dict[str, Any], params: Dict[str, Any]) -> str:
    if not isinstance(operation, str) or not operation.strip():
        raise ValueError("operation must be a non-empty string")
    payload = {
        "operation": operation,
        "input_ref": input_ref,
        "params": params,
    }
    digest = sha256_hex(canonical_json_bytes(payload))
    return f"job_{digest}"


def make_artifact_id(artifact_kind: str, input_ref: Dict[str, Any], params: Dict[str, Any]) -> str:
    if not isinstance(artifact_kind, str) or not artifact_kind.strip():
        raise ValueError("artifact_kind must be a non-empty string")
    payload = {
        "artifact_kind": artifact_kind,
        "input_ref": input_ref,
        "params": params,
    }
    digest = sha256_hex(canonical_json_bytes(payload))
    return f"artifact_{digest}"


class HybridDocumentIdStrategy:
    strategy_name = "hybrid_v1"

    def derive(self, data: bytes, session_namespace: str, ingest_index: int) -> DocumentIdentity:
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")
        if not isinstance(session_namespace, str) or not session_namespace.strip():
            raise ValueError("session_namespace must be a non-empty string")
        if not isinstance(ingest_index, int) or ingest_index < 0:
            raise ValueError("ingest_index must be a non-negative int")

        content_sha = sha256_hex(bytes(data))
        doc_payload = {
            "session_namespace": session_namespace,
            "ingest_index": ingest_index,
            "content_sha256": content_sha,
            "strategy": self.strategy_name,
        }
        doc_id = f"doc_{sha256_hex(canonical_json_bytes(doc_payload))}"
        return DocumentIdentity(
            document_id=doc_id,
            content_sha256=content_sha,
            strategy=self.strategy_name,
            ingest_index=ingest_index,
        )


def document_identity_from_bytes(
    data: bytes,
    strategy: DocumentIdStrategy,
    session_namespace: str,
    ingest_index: int,
) -> DocumentIdentity:
    return strategy.derive(data=data, session_namespace=session_namespace, ingest_index=ingest_index)


def split_document_identity(identity: DocumentIdentity) -> Tuple[str, str, str, int]:
    return identity.document_id, identity.content_sha256, identity.strategy, identity.ingest_index