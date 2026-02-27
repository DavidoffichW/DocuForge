from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from core.ids import DocumentIdStrategy, HybridDocumentIdStrategy, document_identity_from_bytes
from domain.document import DocumentRecord
from storage.adapter import StorageAdapter


class DocumentService:
    def __init__(
        self,
        storage: StorageAdapter,
        id_strategy: Optional[DocumentIdStrategy] = None,
        policy: Optional[ExecutionPolicy] = None,
        session_namespace: str = "default_session",
    ):
        if not isinstance(session_namespace, str) or not session_namespace.strip():
            raise ValueError("session_namespace must be a non-empty string")
        self._storage = storage
        self._id_strategy = id_strategy if id_strategy is not None else HybridDocumentIdStrategy()
        self._policy = policy if policy is not None else ExecutionPolicy(build_registry())
        self._session_namespace = session_namespace
        self._docs_by_id: Dict[str, DocumentRecord] = {}

    @property
    def storage(self) -> StorageAdapter:
        return self._storage

    def ingest(
        self,
        data: bytes,
        ingest_index: int,
        filename: str = "",
        media_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DocumentRecord:
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")
        if not isinstance(ingest_index, int) or ingest_index < 0:
            raise ValueError("ingest_index must be a non-negative int")
        if not isinstance(filename, str):
            raise ValueError("filename must be a string")
        if not isinstance(media_type, str) or not media_type.strip():
            raise ValueError("media_type must be a non-empty string")
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError("metadata must be a dict if provided")

        identity = document_identity_from_bytes(
            data=bytes(data),
            strategy=self._id_strategy,
            session_namespace=self._session_namespace,
            ingest_index=ingest_index,
        )

        storage_key = f"documents/{identity.document_id}"
        self._storage.put_bytes(storage_key, bytes(data), overwrite=True)

        meta = dict(metadata) if metadata is not None else {}

        record = DocumentRecord(
            byte_size=len(bytes(data)),
            content_sha256=identity.content_sha256,
            document_id=identity.document_id,
            filename=filename,
            id_strategy=identity.strategy,
            ingest_index=identity.ingest_index,
            media_type=media_type,
            metadata=meta,
            storage_key=storage_key,
        )

        self._docs_by_id[record.document_id] = record
        return record

    def load_bytes(self, doc: DocumentRecord) -> bytes:
        return self._storage.get_bytes(doc.storage_key)

    def get_document(self, document_id: str) -> DocumentRecord:
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must be a non-empty string")
        if document_id not in self._docs_by_id:
            raise KeyError(document_id)
        return self._docs_by_id[document_id]

    def list_documents(self) -> List[DocumentRecord]:
        items = list(self._docs_by_id.values())
        items.sort(key=lambda d: (d.ingest_index, d.document_id))
        return items